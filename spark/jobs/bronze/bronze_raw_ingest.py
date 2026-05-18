"""
spark.jobs.bronze.bronze_raw_ingest
===================================

Entry point for the Bronze raw ingestion job.

Operational contract
--------------------
* **Trigger**     Airflow ``bronze_raw_ingest`` DAG (hourly) or manual
                  ``spark-submit``.
* **Idempotency** Each run carries a unique ``--batch-id``; downstream
                  Silver consumers can dedupe on it if reruns occur.
* **Atomicity**   Per-source. A failure on one source does not abort the
                  batch — failed sources are reported in the run summary
                  and Airflow re-tries individually.
* **Output**      Iceberg tables in the ``lakehouse.bronze.*`` namespace.
* **Schema**      Raw source columns + the five ingestion metadata columns
                  defined in :mod:`spark.utils.metadata`.

CLI
---
::

    spark-submit \\
        --master spark://spark-master:7077 \\
        spark/jobs/bronze/bronze_raw_ingest.py \\
        --registry spark/configs/sources/sources.yml \\
        --catalog lakehouse \\
        --batch-id 2026-05-18T12:00:00Z \\
        [--only source_name_a source_name_b]

Exit codes
----------
* ``0`` — all enabled sources ingested successfully.
* ``1`` — one or more sources failed. Failed source list is in the final
          log line under ``event=run.summary``.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:  # pragma: no cover — type-only import
    from pyspark.sql import SparkSession

from spark.jobs.bronze.handlers import get_handler
from spark.utils.config_loader import SourceConfig, load_source_registry
from spark.utils.iceberg_utils import append_to_iceberg
from spark.utils.metadata import enrich_with_ingestion_metadata
from spark.utils.observability import get_logger, timed

# Constants from spark_session — duplicated here as module-level defaults
# so CLI parsing (which `pytest` exercises) does not require pyspark.
# The full SparkSession factory is imported lazily inside main().
import os as _os
DEFAULT_CATALOG_NAME = _os.environ.get("ICEBERG_CATALOG", "lakehouse")
DEFAULT_WAREHOUSE = _os.environ.get("ICEBERG_WAREHOUSE", "s3a://bronze/")


LOGGER = get_logger("bronze.raw_ingest")


# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class JobArgs:
    registry: Path
    catalog: str
    warehouse: str
    batch_id: str
    only: tuple[str, ...]
    fail_fast: bool


def parse_args(argv: Sequence[str] | None = None) -> JobArgs:
    parser = argparse.ArgumentParser(
        description="Bronze raw ingestion (config-driven, multi-source).",
    )
    parser.add_argument(
        "--registry",
        type=Path,
        required=True,
        help="Path to sources.yml.",
    )
    parser.add_argument(
        "--catalog",
        default=DEFAULT_CATALOG_NAME,
        help="Iceberg catalog name (default: %(default)s).",
    )
    parser.add_argument(
        "--warehouse",
        default=DEFAULT_WAREHOUSE,
        help="Iceberg warehouse root URI (default: %(default)s).",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Stable identifier for this run. Auto-generated UUID if omitted.",
    )
    parser.add_argument(
        "--only",
        nargs="*",
        default=None,
        help="Optional subset of source names to ingest.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Abort the whole job on first source failure (default: continue).",
    )
    ns = parser.parse_args(argv)
    return JobArgs(
        registry=ns.registry,
        catalog=ns.catalog,
        warehouse=ns.warehouse,
        batch_id=ns.batch_id or str(uuid.uuid4()),
        only=tuple(ns.only) if ns.only else (),
        fail_fast=bool(ns.fail_fast),
    )


# ---------------------------------------------------------------------------
# Per-source ingestion
# ---------------------------------------------------------------------------
def ingest_source(
    spark: "SparkSession",
    config: SourceConfig,
    *,
    catalog: str,
    batch_id: str,
) -> dict[str, int | str]:
    """
    Ingest a single source end-to-end.

    Returns a small result dict suitable for inclusion in the run summary.
    Exceptions propagate to the caller; the orchestrator decides whether
    to abort or continue.
    """
    handler = get_handler(config.type)
    fields = {
        "source": config.name,
        "type": config.type,
        "target": f"{catalog}.{config.fqn_suffix}",
        "batch_id": batch_id,
    }

    with timed(LOGGER, "source.ingest", **fields) as ctx:
        # 1. Read raw frame from the source
        raw_df = handler.read(spark, config)

        # 2. Append ingestion metadata
        enriched = enrich_with_ingestion_metadata(
            raw_df,
            source_name=config.name,
            source_type=config.type,
            batch_id=batch_id,
            include_source_file=handler.captures_source_file,
        )

        # 3. Append into Iceberg (creates table on first run)
        fqn = append_to_iceberg(
            enriched,
            catalog=catalog,
            namespace=config.target_namespace,
            table=config.target_table,
            partition_by=config.partition_by or None,
        )
        # We intentionally do not call .count() here — counting forces a
        # second scan. Spark's metrics from the write itself are emitted
        # to the listener bus and surface in the Spark UI / OpenMetadata
        # lineage events.
        ctx["table"] = fqn
        ctx["status"] = "ok"

    return {"source": config.name, "table": fqn, "status": "ok"}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    sources = load_source_registry(args.registry, only=list(args.only) or None)

    LOGGER.info(
        "run.start",
        extra={
            "batch_id": args.batch_id,
            "catalog": args.catalog,
            "warehouse": args.warehouse,
            "registry": str(args.registry),
            "source_count": len(sources),
            "fail_fast": args.fail_fast,
        },
    )

    if not sources:
        LOGGER.warning("run.empty", extra={"batch_id": args.batch_id})
        return 0

    # Lazy import — only required at runtime under spark-submit. Keeping
    # this off the module's top level lets unit tests exercise CLI parsing
    # and source-registry loading without pyspark installed.
    from spark.utils.spark_session import build_spark_session

    spark = build_spark_session(
        app_name=f"bronze.raw_ingest::{args.batch_id}",
        catalog_name=args.catalog,
        warehouse=args.warehouse,
    )

    successes: list[dict] = []
    failures: list[dict] = []

    for source in sources:
        try:
            result = ingest_source(
                spark, source, catalog=args.catalog, batch_id=args.batch_id,
            )
            successes.append(result)
        except Exception as exc:  # noqa: BLE001 — orchestrator boundary
            failures.append(
                {"source": source.name, "status": "failed", "error": str(exc)},
            )
            LOGGER.exception(
                "source.failed",
                extra={"source": source.name, "batch_id": args.batch_id},
            )
            if args.fail_fast:
                break

    LOGGER.info(
        "run.summary",
        extra={
            "batch_id": args.batch_id,
            "successes": successes,
            "failures": failures,
            "success_count": len(successes),
            "failure_count": len(failures),
        },
    )

    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
