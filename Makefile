# FinTech Data Platform — common development tasks
#
# Most targets wrap the single consolidated compose file in infra/docker/.
# The orchestration layer (Airflow) sits behind the `orchestrate` profile, so
# ingest and orchestration can be brought up together or independently.

COMPOSE          ?= docker compose -f infra/docker/docker-compose.yml
ORCH_PROFILE     := --profile orchestrate
ENV_FILE         := .env
VENV             := .venv
PYTHON           := $(VENV)/bin/python

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## --- Ingest stack (Kafka + producer + consumer) ---

.PHONY: infra-up
infra-up: ## Start the full stack (ingest + orchestration)
	$(COMPOSE) $(ORCH_PROFILE) up -d

.PHONY: ingest-up
ingest-up: ## Start only the ingest stack (Kafka, producer, consumer)
	$(COMPOSE) up -d

.PHONY: orchestrate-up
orchestrate-up: ## Start only the orchestration layer (Airflow)
	$(COMPOSE) $(ORCH_PROFILE) up -d

.PHONY: down
down: ## Stop all services (removes containers, keeps volumes)
	$(COMPOSE) $(ORCH_PROFILE) down

.PHONY: logs
logs: ## Tail logs for the whole stack
	$(COMPOSE) $(ORCH_PROFILE) logs -f

.PHONY: logs-ingest
logs-ingest: ## Tail ingest logs (producer + consumer)
	$(COMPOSE) logs -f producer consumer

.PHONY: ps
ps: ## List running services
	$(COMPOSE) $(ORCH_PROFILE) ps

## --- Airflow helpers ---

airflow_exec := $(COMPOSE) $(ORCH_PROFILE) exec airflow-scheduler airflow

.PHONY: airflow-dags
airflow-dags: ## List Airflow DAGs
	$(airflow_exec) dags list

.PHONY: airflow-dag-details
airflow-dag-details: ## Show DAG details (schedule, pause state)
	$(airflow_exec) dags details fintech_dbt_pipeline

.PHONY: airflow-dag-unpause
airflow-dag-unpause: ## Unpause the pipeline
	$(airflow_exec) dags unpause fintech_dbt_pipeline

.PHONY: airflow-dag-pause
airflow-dag-pause: ## Pause the pipeline
	$(airflow_exec) dags pause fintech_dbt_pipeline

## --- dbt ---

.PHONY: dbt-build
dbt-build: ## Run the dbt project (local, uses dbt/profiles.yml)
	cd dbt && ../$(VENV)/bin/dbt build --profiles-dir .

.PHONY: dbt-test
dbt-test: ## Run dbt data tests
	cd dbt && ../$(VENV)/bin/dbt test --profiles-dir .

## --- Validation ---

.PHONY: validate
validate: ## Validate compose file and DAG import
	docker compose -f infra/docker/docker-compose.yml --profile orchestrate config --quiet && \
	echo "compose: OK"

.PHONY: clean
clean: ## Remove untracked build artifacts (dbt target, pycache)
	rm -rf dbt/target dbt/logs .pytest_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
