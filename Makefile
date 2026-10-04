# ==============================================================================
# Makefile - E-Commerce ML System Orchestration & Automation
# ==============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Load environment variables from .env or .env.example
ifneq (,$(wildcard ./.env))
    include .env
    export
else ifneq (,$(wildcard ./.env.example))
    include .env.example
    export
endif

PYTHON_EXEC ?= python3

.PHONY: help install download-jars up-all down-all up-infra up-flink up-airflow up-datahub down \
        check reset all-small init-airflow gen-data gen-data-skewed gen-data-medium gen-data-full gen-stream \
        spark-baseline spark-opt spark-skew flink-baseline flink-opt dwh-setup \
        governance-sync governance-verify feast-apply test lint

help: ## Show this help message and exit
	@echo "========================================================================"
	@echo "  E-Commerce ML System - Available Make Targets"
	@echo "========================================================================"
	@grep -h -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo "========================================================================"

install: ## Install runtime dependencies
	@echo "--> Installing dependencies from requirements.txt..."
	$(PYTHON_EXEC) -m pip install -r requirements.txt

download-jars: ## Download required Apache Flink connectors (Kafka, S3 Hadoop)
	@echo "--> Downloading Flink connector JARs..."
	bash scripts/download_flink_jars.sh

up-all: ## Start all services across all 7 compose stacks in order of dependency
	@echo "--> Starting foundational infrastructure (MinIO, Postgres, Redis, Kafka)..."
	docker compose -f docker/docker-compose-minio.yml \
	               -f docker/docker-compose-postgres.yml \
	               -f docker/docker-compose-redis.yml \
	               -f docker/docker-compose-kafka.yml up -d
	@echo "--> Starting Flink cluster..."
	docker compose -f docker/docker-compose-flink.yml up -d
	@echo "--> Starting Airflow orchestrator..."
	docker compose -f docker/docker-compose-airflow.yml up -d
	@echo "--> Starting DataHub governance stack..."
	docker compose -f docker/docker-compose-datahub.yml up -d
	@echo "--> All services launched. Run 'make check' to verify readiness."

down-all: ## Stop all services across all 7 compose stacks in reverse order
	@echo "--> Stopping all services..."
	docker compose -f docker/docker-compose-datahub.yml down
	docker compose -f docker/docker-compose-airflow.yml down
	docker compose -f docker/docker-compose-flink.yml down
	docker compose -f docker/docker-compose-kafka.yml \
	               -f docker/docker-compose-redis.yml \
	               -f docker/docker-compose-postgres.yml \
	               -f docker/docker-compose-minio.yml down

up-infra: ## Start foundational infrastructure (MinIO, PostgreSQL DWH, Redis, Kafka, Zookeeper)
	@echo "--> Starting core infrastructure containers..."
	docker compose -f docker/docker-compose-minio.yml \
	               -f docker/docker-compose-postgres.yml \
	               -f docker/docker-compose-redis.yml \
	               -f docker/docker-compose-kafka.yml up -d

up-flink: ## Start Apache Flink JobManager & TaskManager
	@echo "--> Starting Flink cluster..."
	docker compose -f docker/docker-compose-flink.yml up -d

up-airflow: ## Start Apache Airflow Webserver, Scheduler, and Airflow DB
	@echo "--> Starting Airflow orchestrator..."
	docker compose -f docker/docker-compose-airflow.yml up -d

up-datahub: ## Start DataHub metadata & governance stack (GMS, Frontend, Neo4j, Elasticsearch)
	@echo "--> Starting DataHub governance platform..."
	docker compose -f docker/docker-compose-datahub.yml up -d

down: down-all ## Alias for down-all

check: ## Check health and readiness of all service ports and HTTP endpoints
	@$(PYTHON_EXEC) scripts/check_services.py

reset: ## Stop all stacks, wipe docker volumes and local generated data (with confirmation)
	@if [ "$$FORCE" = "1" ]; then \
	    ans="y"; \
	else \
	    read -p "Are you sure you want to wipe all containers, volumes and local data? [y/N] " ans; \
	fi; \
	if [ "$$ans" = "y" ] || [ "$$ans" = "Y" ]; then \
	    echo "--> Tearing down all stacks and wiping volumes..."; \
	    docker compose -f docker/docker-compose-datahub.yml down -v; \
	    docker compose -f docker/docker-compose-airflow.yml down -v; \
	    docker compose -f docker/docker-compose-flink.yml down -v; \
	    docker compose -f docker/docker-compose-kafka.yml \
	                   -f docker/docker-compose-redis.yml \
	                   -f docker/docker-compose-postgres.yml \
	                   -f docker/docker-compose-minio.yml down -v; \
	    rm -rf data/stream_manifest.json data/generation_manifest.json data/stream_events/ spark-warehouse/; \
	    echo "--> System reset complete. Run 'make up-all' to restart from clean state."; \
	else \
	    echo "--> Reset aborted."; \
	fi

all-small: ## Execute full automated rehearsal pipeline on small dataset (up -> init -> gen -> spark -> dags -> governance -> feast)
	@$(PYTHON_EXEC) scripts/rehearse_all_small.py

init-airflow: ## Provision Airflow connections and variables for MinIO, Postgres, Redis
	@echo "--> Initializing Airflow connections and variables..."
	docker compose -f docker/docker-compose-airflow.yml exec -T airflow-webserver python /opt/airflow/ecom_project/scripts/init_airflow_connections.py

gen-data: ## Generate synthetic batch dataset in small mode (bronze ingest)
	@echo "--> Generating small batch dataset..."
	$(PYTHON_EXEC) src/generator/batch_generator.py --mode small

gen-data-skewed: ## Generate batch dataset with injected key skew (opt-in)
	@echo "--> Generating skewed batch dataset..."
	$(PYTHON_EXEC) src/generator/batch_generator.py --mode small --skewed

gen-data-medium: ## Generate batch dataset in medium mode (~5GB)
	@echo "--> Generating medium batch dataset (~5GB)..."
	$(PYTHON_EXEC) src/generator/batch_generator.py --mode medium

TARGET_GB ?= 100
gen-data-full: ## Generate batch dataset in full benchmark mode (TARGET_GB, default 100)
	@echo "--> Running batch generator full mode (TARGET_GB=$(TARGET_GB))..."
	$(PYTHON_EXEC) src/generator/batch_generator.py --mode full --target-size-gb $(TARGET_GB)

gen-stream: ## Run real-time streaming event pusher to Kafka
	@echo "--> Starting streaming events pusher..."
	$(PYTHON_EXEC) src/generator/stream_generator.py

spark-baseline: ## Execute Spark Bronze-to-Silver baseline job (unoptimized)
	@echo "--> Running Spark baseline job..."
	$(PYTHON_EXEC) src/spark/spark_baseline.py

spark-opt: ## Execute Spark Bronze-to-Silver & Gold optimized job (salting, broadcast, AQE, Z-Order)
	@echo "--> Running Spark optimized job..."
	$(PYTHON_EXEC) src/spark/spark_optimized.py

spark-skew: ## Run Spark skew join benchmark comparing Baseline, AQE, and Salting
	@echo "--> Running Spark skew experiment..."
	$(PYTHON_EXEC) src/spark/skew_experiment.py

flink-baseline: ## Submit PyFlink streaming baseline aggregation job
	@echo "--> Submitting Flink baseline job..."
	bash scripts/submit_flink_job.sh baseline

flink-opt: ## Submit PyFlink streaming optimized aggregation job (RocksDB, minibatch)
	@echo "--> Submitting Flink optimized job..."
	bash scripts/submit_flink_job.sh optimized

dwh-setup: ## Initialize PostgreSQL DWH schemas, tables, and indexes
	@echo "--> Initializing PostgreSQL DWH schemas..."
	$(PYTHON_EXEC) scripts/setup_dwh_schemas.py

governance-sync: ## Synchronize metadata catalog, schemas, lineage, and contracts to DataHub GMS
	@echo "--> Emitting metadata catalog to DataHub..."
	PYTHONPATH=governance $(PYTHON_EXEC) governance/sync_catalog.py

governance-verify: ## Verify data contracts against active Delta Lakehouse tables
	@echo "--> Verifying data contracts..."
	PYTHONPATH=governance $(PYTHON_EXEC) governance/verify_contracts.py

feast-apply: ## Apply Feast feature store repository configuration
	@echo "--> Applying Feast feature definitions..."
	cd feature_store && feast apply

test: ## Run unit tests with pytest and coverage
	@echo "--> Running unit tests with pytest..."
	pytest --cov=src/generator --cov=governance --cov=feature_store --cov=src/spark --cov=src/flink tests/

lint: ## Run code linter with ruff
	@echo "--> Running ruff linter..."
	ruff check .
