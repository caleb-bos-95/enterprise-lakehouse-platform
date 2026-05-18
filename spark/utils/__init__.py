"""
spark.utils
===========

Shared utility modules consumed by Bronze, Silver, and Gold Spark jobs.

This package intentionally does **not** re-export Spark-dependent symbols
at the package level. Importing ``spark.utils`` must not require PySpark
to be on the path — that lets unit tests for the config loader, metadata
contract, and handler registry run in a lightweight environment (CI lint
job, local pre-commit hook) without provisioning a Spark distribution.

Sub-modules
-----------
- ``spark_session``     SparkSession factory wired for Iceberg + S3A
- ``s3a_config``        MinIO / S3A Hadoop configuration factory
- ``iceberg_utils``     Iceberg namespace / table lifecycle helpers
- ``config_loader``     YAML source-registry loader with schema validation
- ``observability``     Structured JSON logging and metric emission helpers
- ``metadata``          Ingestion metadata column enrichment

See ``docs/spark/ingestion-framework.md`` for the architectural contract.
"""
