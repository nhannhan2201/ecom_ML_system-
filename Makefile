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

.PHONY: help install download-jars up-infra up-flink up-airflow up-datahub down \
        init-airflow gen-data gen-data-skewed gen-data-full gen-stream \
        spark-baseline spark-opt flink-baseline flink-opt dwh-setup \
        governance-sync governance-verify feast-apply test lint docker-size profile-data

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

down: ## Stop all running service containers across all compose stacks
	@echo "--> Stopping all services..."
	docker compose -f docker/docker-compose-airflow.yml \
	               -f docker/docker-compose-flink.yml \
	               -f docker/docker-compose-datahub.yml \
	               -f docker/docker-compose-kafka.yml \
	               -f docker/docker-compose-minio.yml \
	               -f docker/docker-compose-postgres.yml \
	               -f docker/docker-compose-redis.yml down

init-airflow: ## Provision Airflow connections and variables for MinIO, Postgres, Redis
	@echo "--> Initializing Airflow connections and variables..."
	docker compose -f docker/docker-compose-airflow.yml exec -T airflow-webserver python scripts/init_airflow_connections.py || $(PYTHON_EXEC) scripts/init_airflow_connections.py

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
	@mkdir -p docs/evidence
	pytest --cov=src/generator --cov=governance --cov=feature_store --cov=src/spark --cov=src/flink tests/ | tee docs/evidence/coverage.txt

lint: ## Run code linter with ruff
	@echo "--> Running ruff linter..."
	ruff check .

docker-size: ## Compare and display Docker image sizes for Airflow Spark image
	@echo "--> Inspecting Docker image sizes and layer breakdown..."
	$(PYTHON_EXEC) scripts/measure_docker_sizes.py

profile-data: ## Profile generated data distributions, skew, duplicates, and schema
	@echo "--> Profiling generated dataset..."
	$(PYTHON_EXEC) scripts/profile_generated_data.py

profile-stream: ## Profile streaming generator manifest, late arrival and duplicates
	@echo "--> Profiling streaming dataset..."
	$(PYTHON_EXEC) scripts/profile_stream_data.py

measure-cardinality: ## Measure high-cardinality replica scaling and SCD2 versions
	@echo "--> Measuring high-cardinality replica scaling..."
	$(PYTHON_EXEC) scripts/measure_scale_cardinality.py
