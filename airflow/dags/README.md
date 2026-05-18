# Airflow DAGs

Contains all DAG definitions for the Enterprise Lakehouse Platform.

## Conventions

- DAG IDs follow the pattern: `{layer}_{domain}_{action}` (e.g. `bronze_orders_ingest`)
- All DAGs must define `owner`, `start_date`, and `tags`
- Retry logic: 3 retries with 5-minute exponential backoff
- SLAs must be defined for production-grade DAGs

## Planned DAGs

| DAG | Layer | Frequency | Description |
|---|---|---|---|
| `bronze_raw_ingest` | Bronze | Hourly | Raw data ingestion from source systems |
| `silver_cleanse_validate` | Silver | Hourly | Cleansing, deduplication, and GE validation |
| `gold_kpi_refresh` | Gold | Daily | Aggregate KPI model refresh via dbt |