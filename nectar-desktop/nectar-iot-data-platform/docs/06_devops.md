# DevOps — how this gets from a laptop to production

Three layers, and they are often confused with each other:

| Layer | Tool here | Runs |
|---|---|---|
| Provision the platform — workspace, metastore, groups, network | Terraform (Databricks provider) | rarely, by the platform team |
| Package and deploy workspace assets — jobs, pipelines, schemas, volumes, grants | **Databricks Asset Bundle** | every merge |
| Run the tests and call the deploy | **GitHub Actions** | every push |

A bundle is not CI/CD. **CI/CD calls the bundle.** The bundle is the deploy
step, the same way `terraform apply` is a deploy step and not a pipeline.

## The promotion path

```
push to a branch   →  CI: tests + bundle validate
merge to main      →  deploy -t dev   (jobs PAUSED, resources prefixed per user)
tag v1.2.0         →  deploy -t prod  (jobs UNPAUSED, runs as a service principal)
```

The tag is the promotion gate. It is deliberate, reviewable and revertable —
unlike "whatever is on main right now", which is whatever someone merged last.
The `prod` GitHub Environment can require an approver, so production deploys
wait for a human without anyone needing workspace access.

## What CI actually checks

`.github/workflows/ci.yml`, on every push and pull request:

1. **Byte-compile** everything — a syntax error in a notebook should not reach a
   workspace.
2. **35 unit tests** on the rules, transformations and hierarchy. No Databricks
   account needed; the portable PySpark path exists partly for this reason.
3. **Bundle YAML** — `databricks.yml` and both resource files parse, no
   duplicate `task_key`, and every `depends_on` points at a task that exists.
   Job definitions are data, and data that ships broken is worse than code that
   ships broken, because nothing tries to parse it until deploy day.
4. **Docs links** — every relative link in every markdown file resolves.
5. **`databricks bundle validate`** — but only if workspace credentials are
   configured.

That last point is deliberate. The test job needs no credentials at all, so a
fork or an outside reviewer gets a green tick without being handed access to
anything. The bundle job **skips** rather than fails when the secrets are
absent, because a red X that only means "no secrets here" trains people to
ignore red Xs.

## Turning the deploy on

Three repository secrets — **Settings → Secrets and variables → Actions**:

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | `https://<your-workspace>.cloud.databricks.com` |
| `DATABRICKS_CLIENT_ID` | service principal application id |
| `DATABRICKS_CLIENT_SECRET` | its OAuth secret |

OAuth machine-to-machine, not a personal access token. A PAT is tied to a
person: it dies when they leave, and every deploy is attributed to them rather
than to the pipeline. Create the service principal under **Settings → Identity
and access → Service principals**, give it `CAN_MANAGE` on the target folder and
`USE CATALOG` plus `CREATE` on the catalog — nothing wider.

Free Edition has no service principals or account console, so the deploy
workflow is designed to skip cleanly there. That is why the repo also ships two
credential-free paths: `databricks/sql/00_deploy_all.sql`, and running the
notebooks directly from a Git folder.

## Secrets inside the pipeline

Nothing in this repo reads a credential. When it does — a Kafka API key, a JDBC
password — it goes in a **Databricks secret scope** backed by the cloud key
vault, read with `dbutils.secrets.get`. Never in `pipeline.yaml`, never in a
notebook widget default, never in the bundle. The rule is that a secret must not
be able to appear in a diff.

## What is deliberately not automated

**Terraform is not wired into this repo.** Creating a workspace, a metastore and
groups is a platform-team action that happens once and is reviewed by hand. Tying
it to an application repo's CI means an application change can alter the
platform, which is exactly the blast radius you do not want.

**`bundle destroy` is nowhere in CI.** There is no automated path that deletes
production resources.

**Data is never deployed.** Only definitions. Restoring data is Delta time
travel and `RESTORE`, which is a data-recovery decision, not a deployment one.
