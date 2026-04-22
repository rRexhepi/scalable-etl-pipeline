.PHONY: help install install-dev lint fmt test up down logs dashboards clean

help:
	@echo "Targets:"
	@echo "  install      - Install runtime deps"
	@echo "  install-dev  - Install runtime + dev deps (pytest, ruff)"
	@echo "  lint         - Run ruff"
	@echo "  fmt          - Auto-fix lint and format"
	@echo "  test         - Run pytest with coverage"
	@echo "  up           - docker compose up -d (Postgres, MinIO, Spark, Airflow, Prometheus, Grafana)"
	@echo "  down         - docker compose down"
	@echo "  logs         - Tail compose logs"
	@echo "  dashboards   - Print URLs for Airflow / Grafana / Prometheus / MinIO"
	@echo "  clean        - Remove __pycache__, .pytest_cache, logs"

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-dev.txt

lint:
	ruff check .

fmt:
	ruff check --fix .
	ruff format .

test:
	pytest --cov=etl --cov=utils --cov-report=term-missing

up:
	@test -f .env || (echo "ERROR: .env missing. Copy .env.example to .env and fill in secrets." && exit 1)
	docker compose --env-file .env up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

dashboards:
	@echo "Airflow     : http://localhost:8080"
	@echo "Grafana     : http://localhost:3000 (anonymous viewer enabled)"
	@echo "Prometheus  : http://localhost:9090"
	@echo "Pushgateway : http://localhost:9091"
	@echo "MinIO       : http://localhost:9001"

clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache logs/*.log
