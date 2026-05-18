# Spark Bronze Jobs

Raw ingestion Spark jobs. Reads from supported source systems and writes unmodified data — plus a fixed ingestion metadata envelope — to Iceberg Bronze tables.

## Job Inventory

| Module | Purpose | Trigger |
|---|---|---|
| `bronze_raw_ingest.py` | Config-driven, multi-source raw ingestion. | Airflow `bronze_raw_ingest` DAG (hourly) or manual `spark-submit`. |

## Architectural Contract

* **Append-only.** No `MERGE`, no `OVERWRITE`, no `CREATE OR REPLACE` against existing Bronze tables.
* **No business logic.** Type casting, deduplication, and schema enforcement live in Silver.
* **Schema evolution** is enabled at the Iceberg table level (`write.spark.accept-any-schema=true`). Additive source-side changes flow through silently.
* **Output format**: Apache Iceberg (Parquet, zstd).
* **Metadata envelope**: every row carries `_ingested_at`, `_source`, `_source_type`, `_batch_id`, `_source_file`. See `spark/utils/metadata.py` for the authoritative definition.

## Onboarding a New Source

In the common case, onboarding is a YAML-only change:

1. Add an entry to `spark/configs/sources/sources.yml`.
2. Pick a `type` from the supported set (`file`, `jdbc`).
3. Set `target.namespace` and `target.table`.
4. Supply handler-specific `options`.
5. Open a PR. CI validates the registry against `sources.schema.json`.

For new *source types* (e.g. Kafka, REST), see the handler authoring guide in [`docs/spark/ingestion-framework.md`](../../../docs/spark/ingestion-framework.md).

## Running Locally

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --py-files spark/ \
  spark/jobs/bronze/bronze_raw_ingest.py \
    --registry spark/configs/sources/sources.yml \
    --catalog lakehouse \
    --batch-id "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

Optional flags:

* `--only banksim_transactions postgres_orders` — restrict to a subset.
* `--fail-fast` — abort the whole run on the first source failure.

## Further Reading

* [`docs/spark/ingestion-framework.md`](../../../docs/spark/ingestion-framework.md) — full architectural contract, observability, and roadmap.
* [`docs/architecture/lakehouse-strategy.md`](../../../../docs/architecture/lakehouse-strategy.md) — medallion architecture and Iceberg rationale.
