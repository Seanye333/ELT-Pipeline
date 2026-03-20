# ─────────────────────────────────────────────────────────────────────────────
# ELT Pipeline — Developer Makefile
# Usage: make <target>
# ─────────────────────────────────────────────────────────────────────────────

.DEFAULT_GOAL := help
PYTHON        := python
PYTEST        := pytest
RUFF          := ruff

.PHONY: help install up down logs bootstrap lint format test test-unit test-integration clean

# ── Help ─────────────────────────────────────────────────────────────────────
help:
	@echo "ELT Pipeline Makefile"
	@echo ""
	@echo "  make install           Install Python dependencies"
	@echo "  make up                Start all local services (docker-compose)"
	@echo "  make down              Stop and remove containers"
	@echo "  make logs              Tail all container logs"
	@echo "  make bootstrap         Create MinIO buckets and Oracle tables"
	@echo "  make lint              Run ruff linter"
	@echo "  make format            Auto-format with ruff"
	@echo "  make test              Run all unit tests"
	@echo "  make test-unit         Run unit tests only"
	@echo "  make test-integration  Run integration tests"
	@echo "  make clean             Remove __pycache__, .pytest_cache, reports/"

# ── Environment ───────────────────────────────────────────────────────────────
install:
	pip install -r requirements.txt

# ── Docker ────────────────────────────────────────────────────────────────────
up:
	docker-compose up -d
	@echo ""
	@echo "Services starting..."
	@echo "  Airflow:    http://localhost:8080  (admin/admin)"
	@echo "  MinIO:      http://localhost:9001  (minioadmin/minioadmin)"
	@echo "  FastAPI:    http://localhost:8000/docs"
	@echo "  Streamlit:  http://localhost:8501"

down:
	docker-compose down

logs:
	docker-compose logs -f --tail=50

# ── Bootstrap ─────────────────────────────────────────────────────────────────
bootstrap:
	$(PYTHON) scripts/bootstrap_minio.py
	$(PYTHON) scripts/bootstrap_oracle.py

# ── Code Quality ─────────────────────────────────────────────────────────────
lint:
	$(RUFF) check src/ api/ dashboard/ airflow/ tests/

format:
	$(RUFF) format src/ api/ dashboard/ airflow/ tests/
	$(RUFF) check --fix src/ api/ dashboard/

# ── Tests ─────────────────────────────────────────────────────────────────────
test: test-unit

test-unit:
	$(PYTEST) tests/unit/ -v --cov=src --cov-report=term-missing

test-integration:
	$(PYTEST) tests/integration/ -v

test-all:
	$(PYTEST) tests/ -v --cov=src --cov-report=xml:reports/coverage.xml

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf reports/ htmlcov/ .coverage coverage.xml 2>/dev/null || true
	@echo "Clean complete."
