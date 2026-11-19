# Webhook Delivery Lab — Makefile
# 
# Usage: make <target>
# Run `make help` to see all targets.

.PHONY: help build up down restart logs ps \
        migrate migrate-new db-shell \
        test test-unit test-integration \
        lint format typecheck \
        clean clean-volumes \
        submit-event submit-fail submit-flaky submit-slow submit-rate-limit \
        dlq-info deliveries health

# Default Python interpreter
PYTHON := python3

# Docker Compose
DC := docker compose
DC_RUN := $(DC) run --rm

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

build: ## Build all Docker images
	$(DC) build

up: ## Start all services (runs migrations first)
	$(DC) up -d
	@echo "Waiting for migrations to complete..."
	@sleep 5
	@echo ""
	@echo "✅ Services started. URLs:"
	@echo "   API Swagger:       http://localhost:8000/docs"
	@echo "   Receiver Swagger:  http://localhost:8001/docs"
	@echo "   RabbitMQ UI:       http://localhost:15672  (webhook/webhook)"
	@echo "   Prometheus:        http://localhost:9090"
	@echo "   Grafana:           http://localhost:3000   (admin/admin)"

down: ## Stop all services
	$(DC) down

restart: ## Restart all services
	$(DC) restart

logs: ## Follow logs for all services
	$(DC) logs -f

logs-api: ## Follow API service logs
	$(DC) logs -f api

logs-worker: ## Follow delivery worker logs
	$(DC) logs -f delivery-worker

logs-outbox: ## Follow outbox publisher logs
	$(DC) logs -f outbox-publisher

logs-receiver: ## Follow mock receiver logs
	$(DC) logs -f receiver

ps: ## Show running services
	$(DC) ps

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

migrate: ## Run Alembic migrations (head)
	$(DC_RUN) migrations alembic upgrade head

migrate-new: ## Create a new Alembic migration (NAME= required)
	@if [ -z "$(NAME)" ]; then echo "Usage: make migrate-new NAME=your_migration_name"; exit 1; fi
	$(DC_RUN) api alembic revision --autogenerate -m "$(NAME)"

migrate-history: ## Show migration history
	$(DC_RUN) api alembic history --verbose

db-shell: ## Open a PostgreSQL shell
	$(DC) exec postgres psql -U webhook -d webhookdb

db-events: ## List recent webhook events
	$(DC) exec postgres psql -U webhook -d webhookdb -c \
		"SELECT id, event_type, created_at FROM webhook_events ORDER BY created_at DESC LIMIT 10;"

db-deliveries: ## List recent deliveries with status
	$(DC) exec postgres psql -U webhook -d webhookdb -c \
		"SELECT id, status, attempts, last_error, created_at FROM webhook_deliveries ORDER BY created_at DESC LIMIT 20;"

db-outbox: ## Show unpublished outbox records
	$(DC) exec postgres psql -U webhook -d webhookdb -c \
		"SELECT id, event_type, published, attempts, last_error FROM outbox_events WHERE published = false ORDER BY created_at;"

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------

test: test-unit ## Run unit tests (default)

test-unit: ## Run unit tests only (no external deps)
	$(PYTHON) -m pytest tests/unit/ -v -m "not integration" --tb=short

test-integration: ## Run integration tests (requires running infrastructure)
	$(PYTHON) -m pytest tests/integration/ -v -m "integration or not integration" --tb=short

test-all: ## Run all tests
	$(PYTHON) -m pytest tests/ -v --tb=short

test-coverage: ## Run tests with coverage report
	$(PYTHON) -m pytest tests/ --cov=app --cov-report=html --cov-report=term-missing

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

lint: ## Run ruff linter
	$(PYTHON) -m ruff check app/ receiver/ tests/

format: ## Format code with ruff
	$(PYTHON) -m ruff format app/ receiver/ tests/

typecheck: ## Run mypy type checker
	$(PYTHON) -m mypy app/ --ignore-missing-imports

# ---------------------------------------------------------------------------
# API shortcuts (requires running API)
# ---------------------------------------------------------------------------

health: ## Check API health
	@curl -s http://localhost:8000/health | python3 -m json.tool

ready: ## Check API readiness
	@curl -s http://localhost:8000/ready | python3 -m json.tool

submit-event: ## Submit a success webhook event
	@curl -s -X POST http://localhost:8000/api/v1/events \
		-H "Content-Type: application/json" \
		-d '{"event_type":"order.created","payload":{"order_id":"ORD-001","amount":1500},"endpoint_url":"http://receiver:8001/webhooks/success"}' \
		| python3 -m json.tool

submit-fail: ## Submit a webhook that will always fail (triggers retries → DLQ)
	@curl -s -X POST http://localhost:8000/api/v1/events \
		-H "Content-Type: application/json" \
		-d '{"event_type":"payment.failed","payload":{},"endpoint_url":"http://receiver:8001/webhooks/fail"}' \
		| python3 -m json.tool

submit-flaky: ## Submit a webhook to the flaky endpoint (fails 3x then succeeds)
	@curl -s -X POST http://localhost:8000/api/v1/events \
		-H "Content-Type: application/json" \
		-d '{"event_type":"order.flaky","payload":{},"endpoint_url":"http://receiver:8001/webhooks/flaky"}' \
		| python3 -m json.tool

submit-slow: ## Submit a webhook to the slow endpoint (tests timeout)
	@curl -s -X POST http://localhost:8000/api/v1/events \
		-H "Content-Type: application/json" \
		-d '{"event_type":"order.slow","payload":{},"endpoint_url":"http://receiver:8001/webhooks/slow"}' \
		| python3 -m json.tool

submit-rate-limit: ## Submit a webhook to rate-limited endpoint
	@curl -s -X POST http://localhost:8000/api/v1/events \
		-H "Content-Type: application/json" \
		-d '{"event_type":"order.ratelimited","payload":{},"endpoint_url":"http://receiver:8001/webhooks/rate-limit"}' \
		| python3 -m json.tool

deliveries: ## List recent deliveries via API
	@curl -s "http://localhost:8000/api/v1/deliveries?page_size=10" | python3 -m json.tool

dlq-info: ## Show RabbitMQ DLQ message count
	@curl -s -u webhook:webhook \
		"http://localhost:15672/api/queues/%2F/webhook.dead_letter" \
		| python3 -m json.tool | grep -E '"messages"|"name"'

rabbitmq-queues: ## Show all RabbitMQ queue stats
	@curl -s -u webhook:webhook \
		"http://localhost:15672/api/queues" \
		| python3 -m json.tool

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

clean: ## Stop and remove containers
	$(DC) down --remove-orphans

clean-volumes: ## Stop containers and remove ALL data volumes (destructive!)
	@echo "⚠️  This will delete all PostgreSQL, RabbitMQ, Grafana, and monitoring data!"
	@read -p "Are you sure? [y/N] " confirm; \
		if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
			$(DC) down -v --remove-orphans; \
			echo "✅ Volumes removed"; \
		else \
			echo "Cancelled"; \
		fi

# ---------------------------------------------------------------------------
# Experiments
# ---------------------------------------------------------------------------

experiment-load: ## Run load test (1000 events)
	@$(PYTHON) experiments/load_test.py --count 1000

experiment-crash: ## Simulate worker crash (see experiments/README.md)
	@echo "See experiments/01_consumer_crash.md for instructions"

experiment-outbox: ## Demonstrate outbox pattern recovery
	@$(PYTHON) experiments/06_outbox_recovery.py

