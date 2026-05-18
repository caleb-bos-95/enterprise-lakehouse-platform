# Spark Gold Jobs

Aggregation and KPI jobs. Reads Silver Iceberg tables and produces business-ready analytics models for Trino consumption.

## Conventions

- All Gold models must have corresponding dbt documentation
- Partitioning strategy must be defined for each table
- Gold tables are the authoritative source for dashboards and reporting