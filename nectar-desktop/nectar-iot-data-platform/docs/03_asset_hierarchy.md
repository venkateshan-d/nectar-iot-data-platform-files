# Task 4 — Multi-Asset Hierarchy & Connectivity

## 1. The problem

`assets.parent_asset_id` is an **adjacency list**. It is compact, cheap to
update, and useless for the questions that matter:

```
Site A
└── Building-1
    ├── Chiller-01
    │   ├── AHU-01
    │   │   └── Temp Sensor-14
    │   └── AHU-02
    └── Pump-01
        └── Flow Sensor-01
```

"Everything under Site A", "what breaks when Chiller-01 fails" — both are
recursive traversals. Spark SQL 3.5 has no `WITH RECURSIVE`, and even on an
engine that does, a recursive CTE per dashboard refresh is the wrong cost
profile for a read-heavy analytics platform.

## 2. Two models, both shipped

### Closure table (primary) — `src/nectar/hierarchy/closure_table.py`

Materialise the transitive closure once per batch:

```
asset_closure(ancestor_id, descendant_id, depth, path)
```

One row per ancestor/descendant pair, including the depth-0 self pair. Every
hierarchy query then becomes a single indexed join:

| Question | Predicate |
|---|---|
| Subtree of X | `WHERE ancestor_id = X` |
| Ancestors of X | `WHERE descendant_id = X` |
| Direct children / parent | `... AND depth = 1` |
| Blast radius of X | `WHERE ancestor_id = X AND depth >= 1` |
| Roll a metric up the tree | join facts on `descendant_id`, group by `ancestor_id` |

The last row is the one that earns the design its keep: attributing a
subtree's energy to its root is one join, not a traversal per row.

**Cost.** O(nodes × average depth) rows, rebuilt when topology changes. For
building services — tens of thousands of assets, depth 3–5, topology changing on
commissioning rather than continuously — that is a few hundred thousand rows
rebuilt nightly. In this dataset: 85 assets → 136 closure rows, max depth 2,
built in under a second.

**Cycle safety.** Expansion is bounded by `max_depth` and drops any pair where
ancestor = descendant beyond depth 0, so mis-entered master data (`A → B → A`)
logs a warning instead of looping forever. Tested in
`tests/test_hierarchy.py::test_a_cycle_is_detected_and_does_not_hang_the_closure`.

`dim_asset_hierarchy` is the denormalised companion — one row per asset with
`level`, `root_asset_id`, `hierarchy_path`, `child_count`, `descendant_count`
and the connectivity flags, so a tree view binds to it with no joins at all.

### Property graph (bonus) — `src/nectar/hierarchy/graph_model.py`

NetworkX in-process, plus a full Neo4j schema/loader/query pack
(`docs/diagrams/neo4j_asset_graph.cypher`).

The closure table stops being the right answer when relationships gain **types**
beyond containment — "Chiller-01 *feeds* AHU-02", "AHU-02 *is monitored by*
Sensor-14", "Pump-01 *shares a circuit with* Pump-02" — because the structure
becomes a mesh rather than a tree, queries become genuinely path-shaped
("shortest chilled-water path from the plant room to this VAV box"), and
rebuilding a closure over a frequently-changing mesh is wasteful.

That is the shape building-services topology takes as a platform matures, so
both models are implemented and the tests assert they **agree** — a divergence
would mean the two paths give an operator different blast radii.

## 3. The five required queries

| # | Question | Closure table | Graph |
|---|---|---|---|
| 1 | All assets under a site | `dim_asset_hierarchy WHERE site_id = ?` | `nx.descendants` from the site node |
| 2 | Parent and child assets | `asset_closure ... depth = 1`, both directions | `predecessors` / `successors` |
| 3 | Downstream impacted | `ancestor_id = ? AND depth >= 1`, returns hop distance | `single_source_shortest_path_length` |
| 4 | Orphan assets | `is_orphan` (computed in silver before the pointer is nulled) | evaluated on the raw records |
| 5 | Disconnected assets | `is_disconnected` + `connectivity_status` | isolated nodes (in- and out-degree 0) |

SQL: `sql/hierarchy/hierarchy_queries.sql` (with the recursive-CTE equivalent in
the appendix, for engines that support it). Cypher: `CYPHER_QUERIES` in
`graph_model.py`.

### Two subtleties

**Orphans must be detected before the data is cleaned.** An orphan has a
`parent_asset_id` pointing at an asset that does not exist. The silver layer
nulls that dangling pointer — otherwise every join to the parent silently drops
rows — which means by the time the graph is built, the evidence is gone. So the
verdict is computed at conform time and preserved as `is_orphan`. This is a
general rule: referential breaks have to be caught where the reference is still
visible.

**"Disconnected" is not automatically a defect.** A standalone boiler
legitimately has no asset-level parent. Isolation alone would flag it alongside
genuine commissioning gaps, so `connectivity_status` separates the cases:

| Status | Meaning | Action |
|---|---|---|
| `CONNECTED` | Has a parent and/or children | none |
| `STANDALONE` | Isolated in the asset graph, but assigned to a building | none — legitimate |
| `ORPHANED` | `parent_asset_id` pointed at a non-existent asset | fix the register |
| `UNASSIGNED` | No building | fix the register |

In the generated dataset: 85 assets, 6 orphaned, 18 isolated — of which 12 are
legitimately standalone.

## 4. Beyond the brief: criticality

The reason to model the hierarchy at all is to answer *which asset should be
fixed first*. `blast_radius` — the count of assets in a node's subtree — ranks
maintenance by consequence rather than by symptom count. Available as
`descendant_count` on `dim_asset_hierarchy`, as query 3b in the SQL file, and as
`graph_model.critical_assets()`.
