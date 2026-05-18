"""
spark.utils.spark_session
=========================

Factory for SparkSession instances pre-configured for the lakehouse stack:

- Apache Iceberg catalog (Hadoop-backed warehouse on MinIO)
- S3A access to MinIO
- Sensible shuffle and adaptive-execution defaults

Building a SparkSession is centralised here so that every job — Bronze
ingestion, Silver cleansing, ad-hoc notebooks — runs with an identical
runtime contract. Drift between jobs is the single largest source of
"works on my laptop" failures in lakehouse environments.
"""

from __future__ import annotations

import os
from typing import Mapping

from pyspark.sql import SparkSession

from spark.utils.s3a_config import s3a_hadoop_conf


# ---------------------------------------------------------------------------
# Iceberg catalog defaults
# ---------------------------------------------------------------------------
DEFAULT_CATALOG_NAME = os.environ.get("ICEBERG_CATALOG", "lakehouse")
DEFAULT_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3a://bronze/")

# Iceberg + Spark 3.5 runtime. Pinning the version protects us from
# transitive dependency drift in CI and local builds.
ICEBERG_RUNTIME_PACKAGE = os.environ.get(
    "ICEBERG_RUNTIME_PACKAGE",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
)
S3A_HADOOP_PACKAGE = os.environ.get(
    "S3A_HADOOP_PACKAGE",
    "org.apache.hadoop:hadoop-aws:3.3.4",
)


def _iceberg_catalog_conf(
    catalog_name: str = DEFAULT_CATALOG_NAME,
    warehouse: str = DEFAULT_WAREHOUSE,
) -> dict[str, str]:
    """Return Spark properties wiring a Hadoop-backed Iceberg catalog."""
    prefix = f"spark.sql.catalog.{catalog_name}"
    return {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "hadoop",
        f"{prefix}.warehouse": warehouse,
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }


def build_spark_session(
    app_name: str,
    *,
    catalog_name: str = DEFAULT_CATALOG_NAME,
    warehouse: str = DEFAULT_WAREHOUSE,
    extra_conf: Mapping[str, str] | None = None,
    enable_hive_support: bool = False,
) -> SparkSession:
    """
    Create (or get) a SparkSession wired for Iceberg + S3A.

    Parameters
    ----------
    app_name
        The Spark application name. Convention: ``{layer}.{job_name}``
        (e.g. ``bronze.raw_ingest``). The name is what surfaces in the
        Spark UI and in OpenMetadata lineage events.
    catalog_name
        Iceberg catalog identifier. Defaults to ``lakehouse``.
    warehouse
        S3 (or S3A) URI for the Iceberg warehouse root.
    extra_conf
        Optional Spark properties merged last — useful for per-job
        overrides (e.g. shuffle partitions, dynamic allocation).
    enable_hive_support
        Off by default. Iceberg is the source of truth; Hive metastore
        support is opt-in for legacy interop.

    Returns
    -------
    SparkSession
        A fully configured session. Callers are responsible for stopping
        it (or letting the driver exit, which is the typical pattern).
    """
    builder = SparkSession.builder.appName(app_name)

    # Bundle the Iceberg + Hadoop AWS jars so that `spark-submit` does not
    # need to know about them — the job is self-describing.
    builder = builder.config(
        "spark.jars.packages",
        ",".join([ICEBERG_RUNTIME_PACKAGE, S3A_HADOOP_PACKAGE]),
    )

    # Adaptive execution + reasonable defaults
    base_conf: dict[str, str] = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    }

    for key, value in {
        **base_conf,
        **_iceberg_catalog_conf(catalog_name, warehouse),
        **s3a_hadoop_conf(),
        **(extra_conf or {}),
    }.items():
        builder = builder.config(key, value)

    if enable_hive_support:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    # Reduce log noise from the JVM during local development; production
    # observability is driven by the structured logger, not stderr.
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
