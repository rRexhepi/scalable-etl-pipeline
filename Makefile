.PHONY: help install install-dev lint fmt test up down logs etl clean

help:
	@echo "Targets:"
	@echo "  install      - Install runtime deps"
	@echo "  install-dev  - Install runtime + dev deps (pytest, ruff)"
	@echo "  lint         - Run ruff"
	@echo "  fmt          - Auto-fix lint and format"
	@echo "  test         - Run pytest with coverage"
	@echo "  up           - docker compose up -d (Postgres, MinIO, Spark, Airflow)"
	@echo "  down         - docker compose down"
	@echo "  logs         - Tail compose logs"
	@echo "  etl          - Run the local ETL orchestrator (requires .env)"
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

etl:
	python run_all_etl.py

clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache logs/*.log
