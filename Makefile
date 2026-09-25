# Convenience targets. Every target is a thin wrapper around docker compose, so the
# same commands work without make (see README).

COMPOSE ?= docker compose
RATE ?= 1000
DATE ?= $(shell date -u +%Y-%m-%d)

.PHONY: help up down clean build logs ps produce benchmark reconcile test test-integration lint format psql kpis

help:  ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-18s %s\n", $$1, $$2}'

build:  ## Build the Spark and app images
	$(COMPOSE) build

up:  ## Start the whole stack (Kafka, Postgres, Spark cluster, pipeline, producer, dashboard)
	$(COMPOSE) up -d --build

down:  ## Stop the stack, keep data
	$(COMPOSE) down

clean:  ## Stop the stack and delete Kafka, lakehouse and warehouse volumes
	$(COMPOSE) down -v

ps:  ## Service status
	$(COMPOSE) ps -a

logs:  ## Follow the streaming driver logs
	$(COMPOSE) logs -f pipeline

produce:  ## Restart the producer at RATE events/second (make produce RATE=5000)
	PRODUCER_RATE=$(RATE) $(COMPOSE) up -d --no-deps --force-recreate producer

benchmark:  ## Print throughput / latency measured from ops.streaming_query_progress
	$(COMPOSE) exec -T postgres psql -U fraud -d fraud_dw -f - < scripts/benchmark.sql

reconcile:  ## Run the nightly reconciliation batch job for DATE (default: today UTC)
	$(COMPOSE) --profile batch run --rm reconcile /opt/spark/bin/spark-submit \
		--conf spark.driver.host=reconcile --conf spark.driver.bindAddress=0.0.0.0 \
		/opt/app/batch/historical_reconciliation.py --date $(DATE)

test:  ## Unit + Spark tests inside the Spark image
	$(COMPOSE) --profile test run --rm -e REQUIRE_SPARK=1 tests

test-integration:  ## Warehouse SQL tests against the compose Postgres
	$(COMPOSE) up -d postgres
	$(COMPOSE) --profile test run --rm -e TEST_POSTGRES_DSN=postgresql://fraud:fraud@postgres:5432/postgres \
		tests python3 -m pytest -q -m integration

lint:  ## Ruff lint + format check
	ruff check . && ruff format --check .

format:  ## Auto-format
	ruff check --fix . && ruff format .

psql:  ## Open psql on the warehouse
	$(COMPOSE) exec postgres psql -U fraud -d fraud_dw

kpis:  ## Run the analytical model library against the live warehouse
	$(COMPOSE) exec -T postgres psql -U fraud -d fraud_dw -f - < sql/analytics_kpis.sql
