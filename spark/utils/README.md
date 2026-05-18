# Spark Utilities

Shared, side-effect-free utility modules consumed by Bronze, Silver, and Gold Spark jobs. Centralising these here prevents drift between jobs and keeps the per-job code surface small.

## Modules

| Module | Responsibility |
|---|---|
| `spark_session.py` | SparkSession factory wired for the Iceberg catalog (`lakehouse`) and S3A access to MinIO. |
| `s3a_config.py` | Hadoop / S3A property factory; credentials from env vars, MinIO-safe defaults. |
| `iceberg_utils.py` | Namespace + table lifecycle helpers, append writer with default table properties. |
| `config_loader.py` | YAML loader + JSON Schema validation for the Bronze source registry. |
| `observability.py` | Structured JSON logging and a `timed()` context manager for paired start/end events. |
| `metadata.py` | Ingestion metadata column enrichment (`_ingested_at`, `_source`, `_source_type`, `_batch_id`, `_source_file`). |

## Design Conventions

* Modules import lazily and avoid touching Spark at import time. They are safe to import in tests without a cluster.
* Configuration flows through function arguments and environment variables — never module-level globals.
* The S3A and Iceberg property maps are returned as plain dicts so callers can introspect and override them.
* `observability.get_logger` is idempotent; repeated calls return the same configured logger.

## Adding a Utility

A new utility belongs here when:

* it is consumed by more than one layer (Bronze + Silver, or Spark + Airflow), and
* it is generic — no business logic, no domain knowledge.

Otherwise, keep the helper next to its caller. Premature centralisation is its own form of debt.
