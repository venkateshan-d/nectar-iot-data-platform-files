# Task 5 — Data Quality Framework

## 1. Principles

**Rules are data, not code.** A rule is a named row-level predicate plus
metadata (dimension, severity, threshold, description). Keeping them declarative
means the same objects run in batch and in streaming, every evaluation produces
an auditable row, and adding a rule is appending to a list rather than editing
pipeline logic.

**Quarantine, never drop.** Dropping bad rows destroys the evidence needed to
fix the device that produced them. Every rejected row is written to
`quarantine/<table>` partitioned by batch, annotated with the rule ids it broke,
and can be replayed after the upstream fix.

**Fail closed on blocking breaches.** When a BLOCKING rule exceeds its
threshold the batch is failed and downstream tasks skip. Stale dashboards beat
wrong dashboards.

**Absence is a first-class defect.** No row-level predicate can validate a
record that never arrived. Freshness and completeness are computed as separate
dataset-level signals.

## 2. Dimensions and severities

Rules are classified on the DAMA dimensions, which is also how the report groups
them:

| Dimension | Question |
|---|---|
| Completeness | Is the value there? |
| Uniqueness | Has this record been counted twice? |
| Validity | Is the value well-formed and in its allowed set? |
| Consistency | Does it agree with the rest of the system (referential integrity)? |
| Timeliness | Did it arrive in time to be counted? |
| Accuracy | Is the value physically possible? |

| Severity | Effect |
|---|---|
| `BLOCKING` | Row quarantined; batch fails if the rate breaches the threshold |
| `WARN` | Row kept and flagged in `_dq_warnings`; alerted, does not stop the run |
| `INFO` | Recorded for observability only |

The severity split is a judgement about *usability*, not about how bad the data
looks. A null `asset_id` makes a reading unattributable → BLOCKING. A null
`temperature` is a gap in one series → WARN, because the row's power reading is
still perfectly usable.

## 3. What the framework detects

Mapped directly onto the challenge's list:

| Required | Implementation |
|---|---|
| Missing records | `completeness_report` — expected vs received per asset/day from the sampling interval; plus `freshness_report` per-asset watermark lag |
| Duplicate events | `row_number()` over the business key ordered by ingestion time; rank > 1 is quarantined, so the *volume of gateway retries* is measurable rather than silently absorbed |
| Schema violations | Explicit schemas everywhere (inference is banned); `try_cast` yields NULL instead of throwing, so a bad value is quarantined rather than killing the batch |
| Null values | Per-column rules, BLOCKING on identity columns, WARN on measures |
| Outliers | Two layers — hard physical ranges from config, plus a robust (MAD-based) z-score against each asset's own baseline |
| Late arriving data | Landing-date lag vs event date, compared to a 24 h watermark; flagged per row and used by the maintenance DAG to restate affected aggregates |

**Why two kinds of outlier check.** A fixed range ("temperature < 120 °C")
cannot tell that a chiller running at 30 °C is broken while a boiler at 30 °C is
merely cold. The statistical check uses the *modified z-score*
`0.6745 × (x − median) / MAD` rather than a mean/stddev z-score, because a
handful of stuck-sensor values at 9999 would inflate the standard deviation
enough to mask themselves. Tested explicitly in
`tests/test_quality_rules.py::test_statistical_outlier_uses_the_assets_own_baseline`.

## 4. Error handling strategy

```
                     ┌── clean ──────────► silver (MERGE on business key)
incoming ── rules ───┤
                     └── any BLOCKING ───► quarantine/<table>/_batch_id=...
                                             + _quarantine_reasons[]
                     ▲
unparseable payload ─┴────────────────────► DLQ (streaming) / _corrupt_record (batch)
```

| Failure | Response | Why |
|---|---|---|
| One bad row | Quarantine, continue | One faulty device must not stop an estate |
| Rate above threshold | Fail the batch, skip downstream | A systemic problem, not noise |
| Unparseable payload | DLQ with raw bytes | There is no record to evaluate a rule against |
| Rule itself errors | Job fails loudly | A silently skipped rule is worse than no rule |
| Retry after failure | Safe — every write is idempotent | MERGE on the business key |

`AirflowFailException` is used for the quality gate specifically so a DQ breach
does **not** consume retries: it is not transient, and retrying it just burns
time and hides the problem.

## 5. Automated reporting

`python -m nectar.quality.report` emits, per batch:

* `data_quality_report_<batch>.json` — machine readable, consumed by CI and alerting
* `data_quality_report_<batch>.html` — self-contained, no CDN, opens from an S3
  link or an email attachment on a phone
* `..._latest.*` — stable filenames so a dashboard can bookmark one URL

Contents: verdict (PASS/WARN/FAIL), summary tiles, breakdown by dimension and by
table, every rule with counts/rate/threshold/verdict, stale assets by watermark
lag, and least-complete assets by missing readings. Status is never carried by
colour alone — every verdict ships an icon and a word.

## 6. It is verifiable end to end

The synthetic generator injects defects at known rates and records the counts;
the engine independently rediscovers them. On the shipped dataset the two
reconcile:

| Defect injected | Generator | Framework | Rule |
|---|---|---|---|
| Out-of-range values | 2,433 | 2,433 across four measures | `tel.accuracy.*_in_range` |
| Unknown asset ids | 1,216 | 1,700 | `tel.consistency.asset_registered` |
| Broken timestamps | 1,824 | 1,099 unparseable + 723 implausible = 1,822 | `tel.validity.timestamp_*` |
| Duplicates | 6,022 | 7,968 | `tel.uniqueness.business_key` |
| Silent devices | 4 | 4 stale assets | `freshness_report` |
| Unparseable payloads | 484 | 484 | DLQ |

That reconciliation is the point of the generator: it makes "the framework
works" a checkable claim rather than an assertion. Where the two columns differ,
they differ for a reason worth stating:

* **Unknown assets, 1,216 → 1,700.** The extra 484 are the unparseable payloads.
  They land as all-null rows, so their `asset_id` genuinely is not in the
  register. Correct, and a useful demonstration that one bad payload trips
  several rules rather than being invisible.
* **Duplicates, 6,022 → 7,968.** A randomly cloned row can collide with a key
  that already exists. The extra detections are real duplicates.
* **Timestamps, 1,824 → 1,822.** A row can receive more than one injected
  defect; two rows had their broken timestamp overwritten by a later injection
  pass.

Run summary on the shipped dataset: **611,885 rows evaluated (608,261 telemetry
+ 3,624 events), 35 rules, 11,946 rows quarantined (1.95%), 0 blocking breaches
→ PASS.** Full output in `docs/results/data_quality_report.html`.
