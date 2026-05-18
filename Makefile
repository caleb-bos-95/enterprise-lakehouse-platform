# =============================================================================
# Enterprise Lakehouse Platform — Makefile
# =============================================================================
# Convenience targets for managing the multi-stack Docker Compose environment.
#
# Usage:
#   make help          Show all available targets
#   make up-all        Start every service stack
#   make up-core       Start core infrastructure only
#   make down          Stop and remove all containers
# =============================================================================

COMPOSE_CORE   := docker compose -f docker-compose.yml
COMPOSE_AIRFLOW  := $(COMPOSE_CORE) -f docker-compose.airflow.yml
COMPOSE_SPARK    := $(COMPOSE_CORE) -f docker-compose.spark.yml
COMPOSE_TRINO    := $(COMPOSE_CORE) -f docker-compose.trino.yml
COMPOSE_GOVERNANCE := $(COMPOSE_CORE) -f docker-compose.governance.yml
COMPOSE_ALL    := $(COMPOSE_CORE) -f docker-compose.airflow.yml -f docker-compose.spark.yml -f docker-compose.trino.yml

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
.PHONY: help
help:
	@echo ""
	@echo "  Enterprise Lakehouse Platform — Available Targets"
	@echo "  =================================================="
	@echo ""
	@echo "  Startup:"
	@echo "    make up-core         Start MinIO, Redpanda, PostgreSQL"
	@echo "    make up-airflow      Start core + Airflow"
	@echo "    make up-spark        Start core + Spark"
	@echo "    make up-trino        Start core + Trino"
	@echo "    make up-governance   Start core + OpenMetadata stack"
	@echo "    make up-all          Start core + Airflow + Spark + Trino"
	@echo ""
	@echo "  Teardown:"
	@echo "    make down            Stop all containers (keep volumes)"
	@echo "    make destroy         Stop all containers and delete volumes"
	@echo ""
	@echo "  Operations:"
	@echo "    make logs            Tail logs from all core services"
	@echo "    make ps              Show running containers"
	@echo "    make trino-shell     Open Trino CLI"
	@echo "    make airflow-shell   Open Airflow bash shell"
	@echo ""
	@echo "  Setup:"
	@echo "    make env             Copy .env.example to .env"
	@echo "    make init            Initialise environment (copy .env, pull images)"
	@echo ""

# ---------------------------------------------------------------------------
# Startup targets
# ---------------------------------------------------------------------------
.PHONY: up-core
up-core:
	$(COMPOSE_CORE) up -d
	@echo ""
	@echo "  Core services started:"
	@echo "    MinIO console:      http://localhost:9001"
	@echo "    Redpanda console:   http://localhost:8080"
	@echo "    PostgreSQL:         localhost:5432"
	@echo ""

.PHONY: up-airflow
up-airflow:
	$(COMPOSE_AIRFLOW) up -d
	@echo ""
	@echo "  Airflow services started:"
	@echo "    Airflow UI:         http://localhost:8085  (admin / admin)"
	@echo ""

.PHONY: up-spark
up-spark:
	$(COMPOSE_SPARK) up -d
	@echo ""
	@echo "  Spark services started:"
	@echo "    Spark Master UI:    http://localhost:8181"
	@echo ""

.PHONY: up-trino
up-trino:
	$(COMPOSE_TRINO) up -d
	@echo ""
	@echo "  Trino services started:"
	@echo "    Trino UI:           http://localhost:8090"
	@echo ""

.PHONY: up-governance
up-governance:
	$(COMPOSE_GOVERNANCE) up -d
	@echo ""
	@echo "  Governance services started:"
	@echo "    OpenMetadata UI:    http://localhost:8585"
	@echo "    Login:              admin@open-metadata.org / admin"
	@echo ""

.PHONY: up-all
up-all:
	$(COMPOSE_ALL) up -d
	@echo ""
	@echo "  All services started:"
	@echo "    MinIO console:      http://localhost:9001"
	@echo "    Redpanda console:   http://localhost:8080"
	@echo "    Airflow UI:         http://localhost:8085  (admin / admin)"
	@echo "    Spark Master UI:    http://localhost:8181"
	@echo "    Trino UI:           http://localhost:8090"
	@echo ""

# ---------------------------------------------------------------------------
# Teardown targets
# ---------------------------------------------------------------------------
.PHONY: down
down:
	$(COMPOSE_ALL) -f docker-compose.governance.yml down
	@echo "All containers stopped."

.PHONY: destroy
destroy:
	$(COMPOSE_ALL) -f docker-compose.governance.yml down -v --remove-orphans
	@echo "All containers and volumes removed."

# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------
.PHONY: logs
logs:
	$(COMPOSE_CORE) logs -f

.PHONY: ps
ps:
	$(COMPOSE_ALL) ps

.PHONY: trino-shell
trino-shell:
	docker exec -it lakehouse-trino trino

.PHONY: airflow-shell
airflow-shell:
	docker exec -it lakehouse-airflow-webserver bash

.PHONY: spark-shell
spark-shell:
	docker exec -it lakehouse-spark-master spark-shell \
	  --master spark://spark-master:7077 \
	  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
	  --conf spark.hadoop.fs.s3a.access.key=$(MINIO_ROOT_USER) \
	  --conf spark.hadoop.fs.s3a.secret.key=$(MINIO_ROOT_PASSWORD) \
	  --conf spark.hadoop.fs.s3a.path.style.access=true

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
.PHONY: env
env:
	@if [ ! -f .env ]; then cp .env.example .env && echo ".env created from .env.example"; else echo ".env already exists — skipped"; fi

.PHONY: init
init: env
	$(COMPOSE_ALL) pull
	@echo "Images pulled. Run 'make up-core' to start."
