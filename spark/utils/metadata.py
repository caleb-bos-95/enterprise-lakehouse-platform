"""
spark.utils.metadata
====================

Ingestion metadata enrichment.

Every record landing in Bronze is annotated with a fixed set of lineage
columns. Standardising the schema at the framework layer (rather than
per-handler) guarantees that Silver, Gold, OpenMetadata, and ad-hoc
Trino queries can rely on the same audit contract regardless of source
type.

Schema contract
---------------
+----------------+-----------+----------------------------------------------+
| Column         | Type      | Meaning                                      |
+================+===========+==============================================+
| _ingested_at   | timestamp | UTC ingestion timestamp (Spark current_ts).  |
| _source        | string    | Logical source name from sources.yml.        |
| _source_type   | string    | Handler key (e.g. ``file``, ``jdbc``).       |
| _batch_id      | string    | Job-run UUID — joins every row of one batch. |
| _source_file   | string    | File URI for file sources, NULL otherwise.   |
+----------------+-----------+----------------------------------------------+

The contract is intentionally minimal; richer lineage (commit SHA, DAG
run ID) flows through OpenMetadata ingestion events, not row columns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover — type-only import
    from pyspark.sql import DataFrame


INGESTION_COLUMNS: tuple[str, ...] = (
    "_ingested_at",
    "_source",
    "_source_type",
    "_batch_id",
    "_source_file",
)


def enrich_with_ingestion_metadata(
    df: "DataFrame",
    *,
    source_name: str,
    source_type: str,
    batch_id: str,
    include_source_file: bool = False,
) -> "DataFrame":
    """
    Append the standard ingestion metadata columns to ``df``.

    Parameters
    ----------
    df
        Source DataFrame returned by a handler. Schema is otherwise
        untouched — Bronze preserves raw column names and types.
    source_name
        Logical source identifier from the YAML registry.
    source_type
        Handler key (``file``, ``jdbc``, ...). Surfaced for observability
        dashboards that group by source type.
    batch_id
        Job-run identifier (UUID4). Every row in a single ingest run
        carries the same value, enabling row-level rollback by batch.
    include_source_file
        When ``True``, populates ``_source_file`` from Spark's hidden
        ``input_file_name()`` (file sources only). When ``False``, the
        column is added as NULL to preserve the contract.

    Returns
    -------
    DataFrame
        A new DataFrame with the five ingestion columns appended.
    """
    from pyspark.sql import functions as F  # local import — keeps module pyspark-free at import time

    out = df
    if include_source_file:
        out = out.withColumn("_source_file", F.input_file_name())
    else:
        out = out.withColumn("_source_file", F.lit(None).cast("string"))

    return (
        out
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source_name))
        .withColumn("_source_type", F.lit(source_type))
        .withColumn("_batch_id", F.lit(batch_id))
    )
