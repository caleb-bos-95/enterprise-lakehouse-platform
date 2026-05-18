# Bronze Ingestion Framework

## Purpose

The Bronze ingestion framework is the controlled entry point through which every external dataset becomes a queryable Iceberg table inside the lakehouse. It exists to enforce a uniform raw-layer contract — append-only writes, deterministic metadata enrichment, declarative source onboarding — across heterogeneous source systems without forcing engineers to write bespoke Spark jobs for each new feed.

The framework is operated by Airflow (`bronze_raw_ingest` DAG) on an hourly cadence and is also invokable manually via `spark-submit` for backfills, ad-hoc loads, and CI integration tests.

---

## Architectural Position

Bronze occupies the first layer of the medallion architecture. It is the immutable system-of-record for raw data. Silver depends on Bronze being lossless, late-binding, and append-only; Gold depends on Silver. Any business logic, schema enforcement, deduplication, or quality validation belongs strictly downstream — never inside this framework.

```text
   source systems        ┌─────────────────────────┐         silver/
   ─────────────┐        │  bronze_raw_ingest      │        ┌──────┐
   files / S3   │  ───▶  │  ┌─────────────────┐    │  ───▶  │ dbt  │
   JDBC         │        │  │ Source handlers │    │        │ +    │
   (kafka next) │        │  └─────────────────┘    │        │ GE   │
   ─────────────┘        │  ┌─────────────────┐    │        └──────┘
                         │  │ Metadata        │    │
                         │  └─────────────────┘    │
                         │  ┌─────────────────┐    │
                         │  │ Iceberg writer  │    │
                         │  └─────────────────┘    │
                         └─────────────────────────┘
                                Bronze layer
```

---

## Module Layout

```text
spark/
├── configs/
│   └── sources/
│       ├── sources.yml              # Source registry (the contract)
│       └── sources.schema.json      # JSON Schema validating the registry
├── jobs/
│   └── bronze/
│       ├── bronze_raw_ingest.py     # CLI entry point
│       └── handlers/
│           ├── __init__.py          # Handler registry
│           ├── base.py              # SourceHandler abstract base class
│           ├── file_handler.py      # CSV / JSON / Parquet
│           └── jdbc_handler.py      # JDBC relational sources
└── utils/
    ├── spark_session.py             # SparkSession factory (Iceberg + S3A)
    ├── s3a_config.py                # MinIO / S3A Hadoop configuration
    ├── iceberg_utils.py             # Append + table-create helpers
    ├── config_loader.py             # YAML loader + schema validation
    ├── observability.py             # JSON structured logging
    └── metadata.py                  # Ingestion metadata enrichment
```

The framework is split into three concerns that compose at run time. The **handler layer** owns *how* a source is read. The **utility layer** owns *how* the platform talks to Iceberg, S3A, and observability sinks. The **entry point** owns *what* to ingest and *in what order* — driven exclusively by `sources.yml`.

---

## The Source Registry

`sources.yml` is the single source of truth for which feeds the Bronze job ingests. It is the operational interface engineers and analysts use to onboard new data — code changes are not required for the common case.

A registry entry looks like:

```yaml
- name: banksim_transactions
  description: Synthetic banking transactions seed dataset.
  owner: data-platform-team
  type: file
  enabled: true
  tags: [payments, demo]
  target:
    namespace: bronze
    table: banksim_transactions
  partition_by: []
  options:
    path: "file:///opt/datasets/raw/bs140513_032310.csv"
    format: csv
    reader_options:
      header: "true"
```

The registry is validated against `sources.schema.json` at load time. Validation runs *before* a SparkSession is constructed, so malformed entries fail in milliseconds rather than after a multi-second cluster start. CI enforces the same validation on every pull request.

### Onboarding a new source

Onboarding a source that uses an existing handler type is a four-step operation:

1. Append a new entry to `sources.yml`.
2. Choose a handler `type` from the supported set (`file`, `jdbc`).
3. Define `target.namespace` and `target.table`; both follow lakehouse naming conventions.
4. Supply the handler-specific `options`.

The change is reviewed under the standard PR process. No Python code is touched. The framework picks up the new entry on its next run.

### Onboarding a new source *type*

When a new source system requires a new ingestion mechanism (Kafka, REST, file watching), the contract is:

1. Create a new module under `spark/jobs/bronze/handlers/` subclassing `SourceHandler`.
2. Implement `read(spark, config) -> DataFrame`.
3. Set a unique `type_key` and register the class in `handlers/__init__.py`.
4. Extend `sources.schema.json` with the conditional schema branch for the new type.
5. Add unit tests covering required options and error paths.

Every handler is a pure read function. It does not write to Iceberg, does not enrich metadata, and does not consume secrets directly — credentials are referenced by environment-variable name. This keeps handlers small, testable, and free of side effects.

---

## Ingestion Metadata Contract

Every Bronze row carries five framework-managed columns. The schema is enforced centrally so that Silver, Gold, OpenMetadata, and ad-hoc Trino queries can rely on a stable audit surface:

| Column          | Type      | Provenance                                          |
| --------------- | --------- | --------------------------------------------------- |
| `_ingested_at`  | timestamp | UTC `current_timestamp()` at write time             |
| `_source`       | string    | Logical source name from the registry               |
| `_source_type`  | string    | Handler key (`file`, `jdbc`, …)                     |
| `_batch_id`     | string    | Per-run UUID, identical across every row of a batch |
| `_source_file`  | string    | Input file URI for file sources; NULL otherwise     |

`_batch_id` is the join key for row-level rollback: a failed downstream job can re-emit a single batch by filtering on it. The contract is asserted by `tests/spark/test_metadata_contract.py`; changes require deliberate review.

---

## Write Semantics

Bronze is append-only. The framework never issues `MERGE INTO`, `OVERWRITE`, or `CREATE OR REPLACE` against an existing Bronze table. Deduplication is Silver's responsibility — the absence of dedup at Bronze is deliberate and preserves the auditable history of every record that arrived.

Iceberg table properties applied on creation:

* `format-version=2` — unlocks Merge-on-Read for Silver later.
* `write.format.default=parquet`, compression `zstd`.
* `write.target-file-size-bytes=128MiB` — keeps small-file pressure off the metadata service.
* `write.spark.accept-any-schema=true` — additive schema evolution flows through silently.

Schema *removals* at the source are tolerated: Iceberg retains the column in schema history, and queries return NULL.

---

## Observability

Every framework component emits structured JSON logs with a deterministic envelope: `timestamp`, `level`, `logger`, `event`, and event-specific fields. The `event` field is the indexable key in downstream log stores. Two paired events bracket every source ingest:

* `source.ingest.start` — emitted before any read.
* `source.ingest.end`   — emitted on success, with `duration_ms` and the resulting `table` FQN.
* `source.ingest.error` — emitted on failure, with the exception string.

A `run.summary` event closes every job run, listing successes and failures. Airflow surfaces it as the task log; downstream alerting (PagerDuty, Slack) keys off `failure_count > 0`.

Spark's own stage / task metrics are exported by the Spark listener bus to OpenMetadata as lineage events. We deliberately do not call `.count()` on enriched DataFrames before writing — counting forces a second scan and would double the cost of every ingest.

---

## Failure Semantics

Per-source isolation is the default. A failure on one source is logged, recorded in the run summary, and the job continues with the next source. The job exits non-zero if any source failed, so Airflow re-tries the *task* — but Airflow's re-run targets only the failed sources via `--only`, avoiding redundant work.

`--fail-fast` is available for environments where partial success is unacceptable (typically not Bronze, occasionally Silver).

---

## Operational Success Criteria

| Metric                                    | Target                                |
| ----------------------------------------- | ------------------------------------- |
| End-to-end latency (per source, hourly)   | < 10 minutes from source watermark    |
| Schema validation failures (per week)     | 0 in `main`; caught in PR CI          |
| Bronze write rollback events (per month)  | 0 (append-only contract intact)       |
| New source onboarding time                | < 1 PR (existing handler type)        |
| Run failure rate (per 1000 ingest tasks)  | < 0.5%                                |

---

## Engineering Tradeoffs

The framework is opinionated. A few decisions are worth surfacing:

**Config-driven over code-driven.** A central YAML registry trades raw flexibility for onboarding speed and reviewability. Edge cases that need bespoke logic are served by adding a handler type, not by overloading the registry. The result is that 95% of onboarding work is a YAML diff.

**Append-only at Bronze.** Allowing `MERGE INTO` at Bronze would shave latency off some downstream jobs but blur the layer's contract. We chose strict append; Silver merges with explicit business keys and is the system of *enrichment*.

**No row counts on the hot path.** Counting before write is conservative but doubles cost. Spark's listener bus carries authoritative input/output metrics that flow to OpenMetadata without a second scan.

**Credentials via environment variables, not YAML.** The registry is checked into git. Putting JDBC passwords in it is unacceptable; the framework enforces the pattern by reading `user_env` / `password_env`. Secret material lives in the orchestrator's secret store (Airflow Variables, Vault, K8s Secrets).

**Hadoop-backed Iceberg catalog in local dev.** A REST catalog is the production target. The Hadoop catalog is faster to stand up locally and aligned with the local-first lakehouse strategy documented in `docs/architecture/lakehouse-strategy.md`.

---

## Roadmap

The framework is designed to absorb the following extensions without re-architecting:

* **Kafka / Redpanda streaming handler** with Schema Registry integration and Iceberg streaming writes.
* **Transform partitioning** — surfacing Iceberg `days()`, `bucket()`, `truncate()` partitioning in `partition_by` entries.
* **OpenMetadata lineage emission** — explicit `LineageEvent` writes after each successful source, complementing the implicit Spark listener events.
* **Watermark-driven incremental reads** — `_high_water` tracking in registry state for JDBC sources.
* **Great Expectations gating at the Bronze→Silver boundary** — already planned in Silver; the Bronze contract is the upstream guarantee that makes it tractable.
