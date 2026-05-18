"""
spark.utils.iceberg_utils
=========================

Iceberg lifecycle helpers — namespace creation, table creation, and
append/merge writers — wrapped behind a stable interface so that
handlers never need to issue raw DDL.

Design intent
-------------
The Bronze layer is append-only by contract. We expose a single
``append_to_iceberg`` writer that:

  * creates the target namespace and table if they do not exist,
  * partitions the table according to the source registry,
  * writes via Iceberg's V2 ``DataFrameWriterV2`` API.

Schema evolution is **enabled** at the table property level so that new
columns at the source land in Bronze without manual intervention.
Removed columns are tolerated (Iceberg keeps them in the schema history)
and remain queryable as NULL.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:  # pragma: no cover — type-only import
    from pyspark.sql import DataFrame, SparkSession


# Table properties applied to every Bronze table at creation time.
DEFAULT_TABLE_PROPERTIES: dict[str, str] = {
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.metadata.compression-codec": "gzip",
    # Schema evolution defaults — additive changes flow through silently.
    "write.spark.accept-any-schema": "true",
    # Compact small files automatically on commit
    "write.target-file-size-bytes": str(128 * 1024 * 1024),  # 128 MB
    # Format version 2 unlocks row-level deletes and Merge-on-Read,
    # which Silver will rely on later.
    "format-version": "2",
}


def fully_qualified(catalog: str, namespace: str, table: str) -> str:
    """Build a ``catalog.namespace.table`` identifier (Iceberg convention)."""
    return f"{catalog}.{namespace}.{table}"


def ensure_namespace(spark: "SparkSession", catalog: str, namespace: str) -> None:
    """Create the namespace if it does not exist. Idempotent."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def append_to_iceberg(
    df: "DataFrame",
    *,
    catalog: str,
    namespace: str,
    table: str,
    partition_by: Iterable[str] | None = None,
    table_properties: dict[str, str] | None = None,
) -> str:
    """
    Append ``df`` to an Iceberg table, creating it on first write.

    Parameters
    ----------
    df
        DataFrame to append. Must already carry ingestion metadata columns.
    catalog, namespace, table
        Iceberg identifier triple. ``catalog`` corresponds to the catalog
        configured on the SparkSession (default: ``lakehouse``).
    partition_by
        Optional list of partition columns. Iceberg hidden partitioning
        is preferred over directory partitioning for Bronze.
    table_properties
        Optional overrides merged on top of ``DEFAULT_TABLE_PROPERTIES``.

    Returns
    -------
    str
        The fully qualified table identifier that was written to.
    """
    spark = df.sparkSession
    ensure_namespace(spark, catalog, namespace)

    fqn = fully_qualified(catalog, namespace, table)
    props = {**DEFAULT_TABLE_PROPERTIES, **(table_properties or {})}
    partition_cols = list(partition_by or [])

    # Iceberg V2 writer. ``createOrReplace`` is **not** used — we never
    # want to silently drop a table at the Bronze layer.
    writer = df.writeTo(fqn)
    for key, value in props.items():
        writer = writer.tableProperty(key, value)

    if not _table_exists(spark, fqn):
        # First write — create with partition spec.
        if partition_cols:
            partition_exprs = [_partition_expr(c) for c in partition_cols]
            writer = writer.partitionedBy(*partition_exprs)
        writer.create()
    else:
        writer.append()

    return fqn


def _table_exists(spark: "SparkSession", fqn: str) -> bool:
    """Return True if the Iceberg table at ``fqn`` exists."""
    try:
        spark.sql(f"DESCRIBE TABLE {fqn}").collect()
        return True
    except Exception:  # pragma: no cover — Spark throws AnalysisException
        return False


def _partition_expr(column: str):
    """
    Map a partition column name to a Spark Column expression.

    String column names are passed through verbatim, so the caller can
    write Iceberg-style hidden partitioning expressions in the YAML
    registry (e.g. ``days(_ingested_at)``) by quoting them in SQL form.
    For simplicity we currently only support identity partitioning;
    transform partitioning is a follow-up.
    """
    from pyspark.sql.functions import col
    return col(column)
