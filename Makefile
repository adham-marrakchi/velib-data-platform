.PHONY: help build up down restart logs clean status test lint health rebuild

COMPOSE = docker compose

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ===========================
# Docker Lifecycle
# ===========================
build: ## Build all Docker images
	$(COMPOSE) build

up: ## Start all services (full stack, ~18GB RAM)
	$(COMPOSE) up -d

up-lite: ## Start core services only (~8GB RAM, disables ES/InfluxDB/MLflow)
	$(COMPOSE) -f docker-compose.yaml -f docker-compose.lite.yaml up -d

down: ## Stop all services
	$(COMPOSE) down

restart: ## Restart all services
	$(COMPOSE) down && $(COMPOSE) up -d

clean: ## Stop services and remove all volumes
	$(COMPOSE) down -v --remove-orphans

rebuild: ## Rebuild and restart all services from scratch
	$(COMPOSE) down && $(COMPOSE) build --no-cache && $(COMPOSE) up -d

# ===========================
# Observability
# ===========================
status: ## Show status of all services
	$(COMPOSE) ps

health: ## Check health of all services
	@echo "=== Service Health ===" && $(COMPOSE) ps --format "table {{.Name}}\t{{.Status}}"

logs: ## Tail logs for all services
	$(COMPOSE) logs -f

logs-%: ## Tail logs for a specific service (e.g., make logs-kafka)
	$(COMPOSE) logs -f $*

shell-%: ## Open a shell in a container (e.g., make shell-airflow-webserver)
	$(COMPOSE) exec $* /bin/bash 2>/dev/null || $(COMPOSE) exec $* /bin/sh

# ===========================
# Testing & Quality
# ===========================
test: ## Run Python tests
	python -m pytest tests/ -v --tb=short

lint: ## Lint Python code with flake8
	python -m flake8 airflow/ spark/ kafka/ storage/ governance/ ml/ monitoring/ \
		--max-line-length=120 --ignore=E501,W503,E203 --count --statistics

format: ## Format all code (Python + HTML/CSS/JS + C#)
	@echo "=== Formatting Python (black + isort) ==="
	black --line-length 120 airflow/ spark/ kafka/ storage/ governance/ ml/ monitoring/ bi_dashboards/ great_expectations/ snowflake/ tests/ serve_wiki.py
	isort --profile black --line-length 120 airflow/ spark/ kafka/ storage/ governance/ ml/ monitoring/ bi_dashboards/ great_expectations/ snowflake/ tests/ serve_wiki.py
	@echo "=== Formatting HTML/CSS/JS/JSON (prettier) ==="
	prettier --write "index.html" "packages/*.css" "packages/*.js" "sample_dotnet_backend/appsettings*.json" 2>/dev/null || echo "  prettier not installed (npm install -g prettier)"
	@echo "=== Formatting C# (dotnet format) ==="
	cd sample_dotnet_backend/src/DataPipelineApi && dotnet format --verbosity minimal 2>/dev/null || docker run --rm -v $(PWD)/sample_dotnet_backend:/src -w /src/src/DataPipelineApi mcr.microsoft.com/dotnet/sdk:8.0 dotnet format --verbosity minimal 2>/dev/null || echo "  dotnet format not available"
	@echo "=== All formatting complete ==="

format-check: ## Check formatting without modifying files
	black --check --line-length 120 airflow/ spark/ kafka/ storage/ governance/ ml/ monitoring/ bi_dashboards/ great_expectations/ snowflake/ tests/ serve_wiki.py
	isort --check-only --profile black --line-length 120 airflow/ spark/ kafka/ storage/ governance/ ml/ monitoring/ bi_dashboards/ great_expectations/ snowflake/ tests/ serve_wiki.py

validate: ## Validate docker-compose config
	$(COMPOSE) config --quiet && echo "docker-compose.yaml: VALID"

# ===========================
# Pipeline Operations
# ===========================
kafka-topics: ## List Kafka topics
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:29092 --list

spark-batch: ## Run Spark batch ETL job
	$(COMPOSE) exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark_jobs/spark_batch_job.py

spark-stream: ## Run Spark streaming job
	$(COMPOSE) exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark_jobs/spark_streaming_job.py

trigger-batch: ## Trigger batch ingestion DAG in Airflow
	$(COMPOSE) exec airflow-webserver airflow dags trigger batch_ingestion_dag

trigger-warehouse: ## Trigger warehouse transform DAG in Airflow
	$(COMPOSE) exec airflow-webserver airflow dags trigger warehouse_transform_dag

list-dags: ## List all Airflow DAGs
	$(COMPOSE) exec airflow-webserver airflow dags list

# ===========================
# Service UIs
# ===========================
airflow-ui: ## Show Airflow UI URL
	@echo "Airflow UI:    http://localhost:8080"

grafana-ui: ## Show Grafana UI URL
	@echo "Grafana UI:    http://localhost:3000"

minio-ui: ## Show MinIO Console URL
	@echo "MinIO Console: http://localhost:9001"

mlflow-ui: ## Show MLflow UI URL
	@echo "MLflow UI:     http://localhost:5001"

spark-ui: ## Show Spark Master UI URL
	@echo "Spark UI:      http://localhost:8081"

swagger-ui: ## Show .NET API Swagger URL
	@echo "Swagger UI:    http://localhost:5000/swagger"

urls: ## Show all service URLs
	@echo "=== Service URLs ==="
	@echo "Airflow UI:    http://localhost:8080"
	@echo "Grafana UI:    http://localhost:3000"
	@echo "MinIO Console: http://localhost:9001"
	@echo "MLflow UI:     http://localhost:5001"
	@echo "Spark UI:      http://localhost:8081"
	@echo "Swagger API:   http://localhost:5000/swagger"
	@echo "Prometheus:    http://localhost:9090"
	@echo "Elasticsearch: http://localhost:9200"
	@echo "Kafka:         localhost:9092"

# ===========================
# Deployment (Multi-Provider)
# ===========================
deploy-local: ## Deploy with Docker Compose (full stack)
	./scripts/deploy.sh local

deploy-lite: ## Deploy with Docker Compose (lite, ~8GB RAM)
	./scripts/deploy.sh local-lite

deploy-k8s: ## Deploy to any Kubernetes cluster via Helm
	./scripts/deploy.sh k8s

deploy-aws: ## Deploy to AWS EKS (Terraform + Helm)
	./scripts/deploy.sh aws

deploy-gcp: ## Deploy to GCP GKE via Helm
	./scripts/deploy.sh gcp

deploy-azure: ## Deploy to Azure AKS via Helm
	./scripts/deploy.sh azure

deploy-onprem: ## Deploy to on-prem Kubernetes (k3s, kubeadm, etc.)
	./scripts/deploy.sh k8s helm/e2e-pipeline/values-onprem.yaml

deploy-status: ## Show deployment status
	./scripts/deploy.sh status

deploy-teardown: ## Remove deployment
	./scripts/deploy.sh teardown
