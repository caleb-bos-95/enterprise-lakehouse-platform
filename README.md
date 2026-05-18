# Enterprise Lakehouse Platform

A production-grade, governed lakehouse analytics platform built on open-source technologies. This project simulates the core data infrastructure of a modern enterprise — covering ingestion, streaming, distributed processing, analytics engineering, data quality, and metadata governance.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                             │
│              (APIs, Databases, Files, Streams)                  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │    Airbyte     │  Batch / CDC ingestion
                    └───────┬────────┘
                            │
                            ▼
                    ┌────────────────┐
                    │   Redpanda     │  Event streaming (Kafka-compatible)
                    └───────┬────────┘
                            │
                            ▼
                    ┌────────────────┐
                    │  Apache Spark  │  Distributed processing
                    └───────┬────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │  Apache Iceberg on MinIO    │  Bronze / Silver / Gold
              └──────────┬──────────────────┘
                         │
            ┌────────────┴─────────────┐
            ▼                          ▼
   ┌─────────────────┐      ┌──────────────────────┐
   │   dbt Core      │      │  Great Expectations  │
   │ (Transformations│      │   (Data Quality)     │
   └────────┬────────┘      └──────────────────────┘
            │
            ▼
   ┌─────────────────┐
   │     Trino       │  Federated analytics SQL
   └────────┬────────┘
            │
            ▼
   ┌─────────────────┐
   │  OpenMetadata   │  Lineage, cataloging, governance
   └─────────────────┘
```

---

## Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Airbyte | Batch and incremental data loading |
| Streaming | Redpanda | Kafka-compatible event streaming |
| Processing | Apache Spark 3.5 | Distributed batch and streaming |
| Orchestration | Apache Airflow 2.9 | Pipeline scheduling and monitoring |
| Storage | MinIO + Apache Iceberg | S3-compatible object store with ACID tables |
| Transformation | dbt Core | Modular SQL transformations (Bronze→Silver→Gold) |
| Quality | Great Expectations | Schema, freshness, and rule validation |
| Query Engine | Trino 442 | Federated SQL across Iceberg tables |
| Governance | OpenMetadata 1.3 | Metadata, lineage, and cataloging |
| Infrastructure | Docker Compose | Local containerised deployment |
| CI/CD | GitHub Actions | Linting, testing, validation |

---

## Data Architecture

The platform follows a medallion (Bronze / Silver / Gold) architecture backed by Apache Iceberg tables on MinIO object storage.

| Layer | Location | Purpose |
|---|---|---|
| Bronze | `s3a://bronze/` | Raw ingested data — no transformation |
| Silver | `s3a://silver/` | Cleansed, validated, conformed data |
| Gold | `s3a://gold/` | Business-ready analytics and KPI models |
| Checkpoints | `s3a://checkpoints/` | Spark Structured Streaming checkpoints |

---

## Service Ports

| Service | URL | Credentials |
|---|---|---|
| MinIO Console | http://localhost:9001 | admin / password123 |
| Redpanda Console | http://localhost:8080 | — |
| Redpanda Kafka API | localhost:19092 | — |
| Redpanda Schema Registry | localhost:18081 | — |
| Airflow UI | http://localhost:8085 | admin / admin |
| Spark Master UI | http://localhost:8181 | — |
| Trino UI | http://localhost:8090 | — |
| OpenMetadata UI | http://localhost:8585 | admin@open-metadata.org / admin |
| PostgreSQL | localhost:5432 | airflow / airflow |

---

## Repository Structure

```
enterprise-lakehouse-platform/
│
├── docker-compose.yml              # Core infrastructure (MinIO, Redpanda, PostgreSQL)
├── docker-compose.airflow.yml      # Airflow orchestration stack
├── docker-compose.spark.yml        # Spark processing stack
├── docker-compose.trino.yml        # Trino query engine
├── docker-compose.governance.yml   # OpenMetadata governance stack
├── Makefile                        # Convenience targets for all stacks
├── .env.example                    # Environment variable template
│
├── airflow/
│   ├── dags/                       # Airflow DAG definitions
│   └── plugins/                    # Custom operators and hooks
│
├── spark/
│   ├── jobs/
│   │   ├── bronze/                 # Raw ingestion jobs
│   │   ├── silver/                 # Cleansing and validation jobs
│   │   └── gold/                   # Aggregation and KPI jobs
│   └── utils/                      # Shared Spark utilities
│
├── dbt/
│   ├── models/
│   │   ├── bronze/                 # Source staging models
│   │   ├── silver/                 # Conformed dimension/fact models
│   │   └── gold/                   # Business KPI and mart models
│   ├── tests/                      # Custom dbt tests
│   ├── macros/                     # Reusable dbt macros
│   └── seeds/                      # Reference / lookup datasets
│
├── datasets/
│   ├── raw/                        # Sample source datasets
│   └── schemas/                    # Schema definitions and contracts
│
├── metadata/
│   ├── openmetadata/
│   │   ├── ingestion/              # OpenMetadata ingestion configs
│   │   └── glossary/               # Business glossary definitions
│   └── lineage/                    # Lineage mapping definitions
│
├── governance/
│   ├── policies/                   # Access and data policies
│   ├── rbac/                       # Role-based access control models
│   └── trust/                      # Trust scoring frameworks
│
├── monitoring/
│   ├── dashboards/                 # Observability dashboard configs
│   └── alerts/                     # Alert rule definitions
│
├── configs/
│   ├── trino/                      # Trino coordinator and catalog configs
│   └── redpanda/                   # Redpanda console config
│
├── infra/
│   ├── terraform/                  # Infrastructure-as-code (future)
│   └── kubernetes/                 # Kubernetes manifests (future)
│
├── tests/
│   ├── unit/                       # Python unit tests
│   └── integration/                # Integration and pipeline tests
│
├── scripts/                        # Operational utilities and setup scripts
├── architecture/                   # Architecture diagrams and ADRs
└── docs/
    ├── decisions/                  # Architecture Decision Records
    └── runbooks/                   # Operational runbooks
```

---

## Quickstart

### Prerequisites

- Docker Desktop (≥ 4.x) with at least 6 GB RAM allocated
- Docker Compose v2
- `make`

### 1. Clone and configure

```bash
git clone https://github.com/<your-username>/enterprise-lakehouse-platform.git
cd enterprise-lakehouse-platform

# Create your local environment file
cp .env.example .env
```

### 2. Start core infrastructure

```bash
make up-core
```

MinIO and Redpanda are now running. MinIO will automatically create the `bronze`, `silver`, `gold`, and `checkpoints` buckets.

### 3. Start additional stacks

```bash
make up-airflow      # Start Airflow
make up-spark        # Start Spark
make up-trino        # Start Trino

# Or start everything at once
make up-all
```

### 4. Start the governance stack (optional — requires ~4 GB RAM)

```bash
make up-governance
```

### 5. Verify services

```bash
make ps
```

### Tear down

```bash
make down        # Stop containers, keep volumes
make destroy     # Stop containers and delete all data
```

---

## CI/CD

GitHub Actions runs on every push to `main` / `develop` and on pull requests targeting `main`.

| Stage | What it checks |
|---|---|
| Lint | Python (ruff), YAML (yamllint), SQL (sqlfluff) |
| Unit Tests | pytest with coverage reporting |
| dbt Validate | dbt parse + compile (no warehouse required) |
| Docker Check | docker compose config validation for all stacks |

---

## Architecture Principles

This platform is built around the following engineering commitments:

- **Safety and security first** — least-privilege access patterns, no credentials in source control, environment-variable driven configuration
- **Modular infrastructure** — each service stack is independently deployable via its own compose file
- **Governance by default** — metadata, lineage, and ownership tracking are structural, not optional
- **Observability built in** — every layer exposes health checks; monitoring integrations are first-class citizens
- **Reproducibility** — fully containerised, infrastructure-as-code driven, no manual configuration steps

---

## Future Roadmap

- Kubernetes-native deployment via Helm charts
- CDC ingestion via Debezium
- Semantic layer integration (Cube or Metricflow)
- OpenTelemetry distributed tracing
- AI-assisted metadata generation via Claude
- Data contracts and schema registry enforcement
- Cost observability dashboards

---

## Author

**Caleb Bosman** — Cloud & Data Engineer  
Focused on lakehouse architecture, metadata governance, streaming analytics, and AI infrastructure engineering.
