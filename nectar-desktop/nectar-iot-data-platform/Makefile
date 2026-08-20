# =============================================================================
# Nectar IoT Data Platform
#
#   make setup       install dependencies into a virtualenv
#   make all         generate data -> run the pipeline -> publish -> run the SQL
#   make demo        the above, plus the streaming pipeline (no Kafka needed)
#   make test        unit tests
#
# FORMAT=parquet runs without Delta jars (offline / CI). FORMAT=delta is default.
# =============================================================================

PY      ?= python3
VENV    ?= .venv
BIN     := $(VENV)/bin
PYTHON  := $(BIN)/python
FORMAT  ?= delta
DAYS    ?=
export PYTHONPATH := src

.DEFAULT_GOAL := help
.PHONY: help setup generate pipeline pipeline-offline serve sql stream demo all test lint clean clean-data kafka-up kafka-down airflow-up

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup: ## Create the virtualenv and install dependencies
	$(PY) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip "setuptools<80" wheel
	$(BIN)/pip install -r requirements.txt
	@echo "done. Delta jars are fetched from Maven Central on first run."

generate: ## Generate the synthetic dataset into data/raw
	$(PYTHON) -m nectar.generator.generate_data $(if $(DAYS),--days $(DAYS),)

pipeline: ## Run bronze -> silver -> gold -> hierarchy -> quality report
	$(PYTHON) -m nectar.pipeline.run_batch --format $(FORMAT) --optimize

pipeline-offline: ## Same, forced onto the Parquet fallback (no Maven access)
	$(PYTHON) -m nectar.pipeline.run_batch --format parquet

serve: ## Publish the gold layer to DuckDB
	$(PYTHON) -m nectar.serving.load_duckdb

sql: ## Execute the Task 6 + Task 4 SQL and write results to data/query_results
	$(PYTHON) -m nectar.serving.run_queries

stream: ## Bonus A: streaming pipeline over the raw zone (no Kafka required)
	$(PYTHON) -m nectar.streaming.consumer --source file --once --format $(FORMAT)

stream-kafka: ## Bonus A: streaming pipeline against a live Kafka broker
	$(PYTHON) -m nectar.streaming.consumer --source kafka --format $(FORMAT)

produce: ## Replay the raw zone into Kafka at 500 msg/s
	$(PYTHON) -m nectar.streaming.producer --rate 500

all: generate pipeline serve sql ## Full batch run, end to end

demo: all stream ## Everything, including the streaming pipeline
	@echo ""
	@echo "  quality report : data/lakehouse/quality/reports/data_quality_report_latest.html"
	@echo "  SQL results    : data/query_results/sql_results.md"
	@echo "  architecture   : docs/diagrams/architecture.html"

test: ## Run the unit tests
	$(PYTHON) -m pytest tests/ -q

lint: ## Byte-compile everything as a cheap syntax check
	$(PYTHON) -m compileall -q src tests alternatives/airflow

kafka-up: ## Start Kafka + Kafka UI (localhost:8085)
	docker compose up -d kafka kafka-init kafka-ui

kafka-down: ## Stop the local infrastructure
	docker compose down

airflow-up: ## Start Airflow (localhost:8080, admin/admin)
	docker compose --profile airflow up -d

clean-data: ## Remove generated data, keeping the code
	rm -rf data/raw data/lakehouse data/serving data/checkpoints data/query_results

clean: clean-data ## Remove data and all build artefacts
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
	find . -name '*.pyc' -delete
	rm -rf .pytest_cache
