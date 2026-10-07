.PHONY: help env build up down restart logs clean status test lint format kafka-topics list-dags urls

COMPOSE = docker compose
PY_DIRS = airflow kafka spark ml quality tests

help: ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

# --- Cycle de vie Docker -------------------------------------------------------
env: ## Crée .env à partir de .env.example (si absent)
	@test -f .env || cp .env.example .env

build: env ## Construit les images (Airflow, Spark, producteur, MLflow)
	$(COMPOSE) build

up: env ## Démarre la plateforme
	$(COMPOSE) up -d

down: ## Arrête la plateforme
	$(COMPOSE) down

restart: down up ## Redémarre la plateforme

logs: ## Suit les logs de tous les services
	$(COMPOSE) logs -f

status: ## État et santé des services
	$(COMPOSE) ps --format "table {{.Name}}\t{{.Status}}"

clean: ## Arrête tout et supprime les volumes (données perdues)
	$(COMPOSE) down -v

# --- Qualité du code -----------------------------------------------------------
test: ## Tests unitaires
	python -m pytest tests

lint: ## flake8 + black + isort (vérification)
	python -m flake8 $(PY_DIRS) --max-line-length 120 --extend-ignore E501,W503,E203
	python -m black --check $(PY_DIRS)
	python -m isort --check-only $(PY_DIRS)

format: ## Formate le code Python
	python -m isort $(PY_DIRS)
	python -m black $(PY_DIRS)

# --- Raccourcis ----------------------------------------------------------------
kafka-topics: ## Liste les topics Kafka
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

list-dags: ## Liste les DAGs Airflow
	$(COMPOSE) exec airflow-scheduler airflow dags list

urls: ## URLs des interfaces
	@echo "Airflow        http://localhost:8080"
	@echo "Spark master   http://localhost:8081"
	@echo "Spark streaming http://localhost:4040"
	@echo "MinIO console  http://localhost:9001"
	@echo "MLflow         http://localhost:5001"
