# Legacy notebooks (pre-Ubunye)

Original Databricks notebooks authored by the ABSA AIC Data Science team.
Kept here for reference only — they are the source material that the
pipelines under `pipelines/` were ported from. They are **not** part of the
deployable bundle and are not touched by any workflow.

| File | Maps to |
|---|---|
| `smart-flood-detection.py` | `pipelines/flood/etl/geocode_addresses` + `pipelines/flood/etl/flood_risk` |
| `smart-rewards-telematics.py` | `pipelines/telematics/etl/policy_device_mapping` |
| `item-etl-exposure-mappping.py` | Early draft of the telematics ETL |

Notable deviations between the ported pipelines and these originals are
documented in the per-pipeline config comments and in the transformations.
