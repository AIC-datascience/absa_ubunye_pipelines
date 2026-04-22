# absa_ubunye_pipelines

ABSA Databricks pipelines orchestrated by [ubunye-engine](https://pypi.org/project/ubunye-engine/) 0.1.7.

## Layout

```
absa_ubunye_pipelines/
├── databricks.yml                  # bundle + targets (docs/deployment.md pattern)
├── bundles/
│   ├── flood.yml                   # flood job: geocode -> flood_risk (chained)
│   └── telematics.yml              # telematics job: policy/device mapping
├── pipelines/
│   ├── flood/etl/
│   │   ├── geocode_addresses/      # config + transformations + dev notebook
│   │   └── flood_risk/
│   └── telematics/etl/
│       └── policy_device_mapping/
├── tests/                          # local Spark unit tests
└── .github/workflows/              # validate_pr + deploy_nonprod
```

Folder shape follows the canonical `pipelines/<usecase>/<package>/<task>/` layout
described in [ubunye docs — structure](https://github.com/thabangline/ubunye_engine/blob/main/docs/getting_started/structure.md).

## GitHub secrets

Secrets are scoped by **GitHub Environment** (`nonprod`, and later `prod`) —
the names below stay the same in both environments; only the values change.
The `deploy_nonprod.yml` workflow declares `environment: nonprod`, so it
resolves each secret from the nonprod environment's store.

| Secret | Purpose |
|---|---|
| `DATABRICKS_HOST` | Workspace URL, e.g. `https://adb-xxx.azuredatabricks.net` |
| `DATABRICKS_CLIENT_ID` | Service principal Application ID (OAuth) |
| `DATABRICKS_CLIENT_SECRET` | OAuth secret |
| `DATABRICKS_TOKEN` | PAT fallback — only needed if OAuth isn't configured |
| `TELM_CATALOG` | Unity Catalog catalog name |
| `TELM_SCHEMA` | Unity Catalog schema name |
| `ADDRESS_SOURCE_TABLE` | Unqualified source table name (`id`, `address` columns) |

> `TELM_CATALOG`, `TELM_SCHEMA`, and `ADDRESS_SOURCE_TABLE` can live as
> **repository** secrets if the values don't differ between nonprod and prod;
> put them on the environment when they do.

TomTom and JBA credentials go into a **Databricks secret scope** (default name
`absa-flood`), not GitHub. Create it once per workspace:

```bash
databricks secrets create-scope absa-flood
databricks secrets put-secret absa-flood tomtom_api_key    # TomTom key
databricks secrets put-secret absa-flood jba_basic_auth    # "Basic <base64 of user:pass>"
```

The bundle references them via `{{secrets/absa-flood/<key>}}` in
`bundles/flood.yml`, so the workflow never sees them.

## Adding a pipeline

1. Copy a task folder: `cp -r pipelines/telematics/etl/policy_device_mapping pipelines/<usecase>/<package>/<task>`
2. Edit `config.yaml` and `transformations.py` for the new logic.
3. Rename the dev notebook: `mv pipelines/.../policy_device_mapping_dev.py pipelines/.../<task>_dev.py`.
4. Add a job block to `bundles/<group>.yml` (or create a new file) with
   `notebook_task.notebook_path` pointing at the new dev notebook and
   `base_parameters.task_dir` pointing at the new task dir.
5. Open a PR — `validate_pr.yml` validates configs and runs tests.

## How deployment works

One workflow (`deploy.yml`) handles both environments. Each job is scoped to
its own GitHub Environment — secrets resolve from that environment's store,
and you can add required-reviewer protection on `prod` for a human approval
gate.

- **Pull request** → `validate_pr.yml` runs `ubunye validate -d pipelines -u <usecase> -p etl --all` and `pytest tests/` on a local SparkSession (no Databricks access).
- **Push to main** → `deploy.yml` auto-deploys to `nonprod` (target: `nonprod`). If any required secret is missing the deploy step is skipped with a `::warning` rather than failing.
- **Manual `workflow_dispatch`** → pick `target: nonprod` or `target: prod`. Optional `run_after_deploy: true` triggers both bundled jobs once after the deploy.
- **Prod promotion** → always manual: trigger `deploy.yml` with `target: prod`. The prod job fails hard if secrets are missing (unlike nonprod which skips silently), so misconfiguration can't silently drop a prod deploy.
