# Monitoring

Observability configurations for the lakehouse platform.

## Structure

- `dashboards/` — Grafana dashboard JSON exports
- `alerts/` — Alert rule definitions

## Observability Targets

- Airflow DAG SLA breaches
- Spark job failure rates
- Redpanda consumer lag
- Iceberg table freshness
- Trino query latency
- OpenMetadata metadata completeness