# Enterprise Lakehouse Platform

Modern open-source governed lakehouse architecture built using Airbyte, Kafka, Spark, Airflow, Iceberg, dbt, Trino, Great Expectations, and OpenMetadata.

## Business Problem

Modern enterprises struggle with fragmented reporting, inconsistent data quality, siloed analytics systems, and poor metadata visibility.

This project demonstrates how a modern open-source lakehouse architecture can centralize ingestion, transformation, governance, observability, and analytics into a unified data platform.


<img width="1530" height="581" alt="diagram-export-5-11-2026-6_14_10-PM" src="https://github.com/user-attachments/assets/9f1de4a1-d9db-44fb-bdad-585514c6eff4" />


Apache Iceberg was selected for:
- ACID-compliant analytics
- schema evolution
- partition evolution
- time travel support
- interoperability with Spark and Trino

OpenMetadata was selected for:
- metadata lineage
- governance
- observability
- dataset discovery
- ownership management


enterprise-lakehouse-platform/
│
├── airflow/
├── spark/
├── dbt/
├── iceberg/
├── metadata/
├── governance/
├── quality/
├── infra/
├── docker/
├── datasets/
└── docs/
