# ELT Pipeline

A production-grade ELT (Extract → Load → Transform) data pipeline that pulls Excel/CSV files from OneDrive, stores raw data in MinIO object storage, transforms and loads it into Oracle Database, and serves the data via a FastAPI layer and Streamlit dashboard.

---

## Architecture Overview

```
OneDrive (Excel/CSV)
       │
       ▼  Microsoft Graph API (MSAL OAuth2)
┌─────────────┐
│   Extract   │  graph_client.py · onedrive_scanner.py · file_downloader.py
└──────┬──────┘
       │ raw bytes
       ▼
┌─────────────┐
│   MinIO     │  Bronze bucket (raw files) → Silver bucket (Parquet)
│  (Bronze)   │  MD5 checksum idempotency — skips re-upload if unchanged
└──────┬──────┘
       │ Parquet
       ▼
┌─────────────┐
│  Transform  │  excel_transformer.py · csv_transformer.py · schema_validator.py
└──────┬──────┘
       │ DataFrame
       ▼
┌─────────────┐
│   Oracle    │  MERGE (upsert) — never plain INSERT; full idempotency
│  Database   │  python-oracledb thin mode (no Instant Client required)
└──────┬──────┘
       │
       ▼
┌─────────────┐   ┌───────────────┐
│   FastAPI   │   │   Streamlit   │
│  REST API   │──▶│   Dashboard   │
└─────────────┘   └───────────────┘
```

**Orchestration:** Apache Airflow 2.9 (daily schedule) + Argo Workflows (Kubernetes-native heavy jobs)
**CI/CD:** Jenkins → JFrog Artifactory → Kubernetes

---

## Tech Stack

| Layer | Technology |
|---|---|
| Source | OneDrive via Microsoft Graph API |
| Auth | MSAL OAuth2 client credentials |
| Orchestration | Apache Airflow 2.9 + Argo Workflows |
| Object Storage | MinIO (S3-compatible) |
| Database | Oracle DB on-prem (python-oracledb thin mode) |
| API | FastAPI + Uvicorn |
| Dashboard | Streamlit (multi-page) |
| CI/CD | Jenkins → JFrog Artifactory → Kubernetes |
| Containerization | Docker + docker-compose (local), Kubernetes (production) |

---

## Project Structure

```
ELT Pipeline/
├── src/
│   ├── config/
│   │   ├── settings.py          # Pydantic BaseSettings — single source of truth
│   │   └── logging_config.py    # structlog JSON logging
│   ├── extract/
│   │   ├── graph_client.py      # MSAL OAuth2 client with token caching
│   │   ├── onedrive_scanner.py  # Recursively lists OneDrive files
│   │   └── file_downloader.py   # Downloads files to buffer or disk
│   ├── load/
│   │   ├── minio_client.py      # boto3 S3 client pointed at MinIO
│   │   ├── minio_uploader.py    # Upload raw/Parquet with MD5 idempotency
│   │   └── oracle_loader.py     # Connection pool + bulk MERGE upsert
│   ├── transform/
│   │   ├── excel_transformer.py # openpyxl + pandas normalization
│   │   ├── csv_transformer.py   # chardet encoding detection + pandas
│   │   └── schema_validator.py  # Column spec validation
│   └── utils/
│       ├── checksum.py          # MD5 / SHA256 helpers
│       ├── retry.py             # tenacity-based retry decorator
│       └── notifications.py     # Slack webhook + SMTP email alerts
├── airflow/
│   ├── dags/
│   │   ├── dag_extract_to_minio.py    # DAG 1: OneDrive → MinIO (daily 06:00)
│   │   ├── dag_transform_to_oracle.py # DAG 2: MinIO → Oracle (triggered by DAG 1)
│   │   └── dag_full_pipeline.py       # DAG 3: Full pipeline Mon–Fri 05:00
│   └── plugins/
│       ├── hooks/               # Custom Airflow hooks (Graph, MinIO, Oracle)
│       └── operators/           # Custom operators wrapping hooks
├── api/
│   ├── main.py                  # FastAPI app factory
│   ├── routers/                 # health · datasets · records · pipeline
│   ├── models/                  # Pydantic request/response models
│   └── dependencies.py          # Singleton OracleLoader via lru_cache
├── dashboard/
│   ├── app.py                   # Streamlit entry point
│   └── pages/
│       ├── 1_Overview.py        # KPI metrics + trend charts
│       ├── 2_Data_Explorer.py   # Interactive data browser + CSV export
│       └── 3_Pipeline_Health.py # Run history + manual trigger
├── docker/
│   ├── airflow/Dockerfile
│   ├── api/Dockerfile
│   ├── dashboard/Dockerfile
│   └── pipeline/Dockerfile
├── k8s/                         # Kubernetes manifests (namespace, secrets, deployments)
├── argo/workflows/              # Argo WorkflowTemplates + CronWorkflow
├── scripts/
│   ├── bootstrap_minio.py       # Create bronze/silver buckets
│   ├── bootstrap_oracle.py      # Create PIPELINE_RUN, FILE_MANIFEST, ELT_DATA tables
│   └── smoke_test.py            # API health + Oracle row count checks
├── tests/
│   ├── conftest.py              # pytest fixtures
│   └── unit/                   # Unit tests for transformers, validators, uploaders
├── docker-compose.yml           # Local dev: 10 services
├── Makefile                     # Developer shortcuts
├── Jenkinsfile                  # CI/CD pipeline (8 stages)
├── requirements.txt
└── .env.example
```

---

## Quick Start (Local Development)

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- Python 3.11+
- Git

### 1. Clone and configure

```bash
git clone <repo-url>
cd "ELT Pipeline"

# Create your local environment file
cp .env.example .env
```

Edit `.env` and fill in your Azure App Registration credentials:

```env
GRAPH_TENANT_ID=your-tenant-id
GRAPH_CLIENT_ID=your-client-id
GRAPH_CLIENT_SECRET=your-client-secret
GRAPH_FOLDER_PATH=/path/to/your/onedrive/folder
```

Everything else (MinIO, Oracle, Airflow) uses safe local defaults and works out of the box.

### 2. Start all services

```bash
docker-compose up -d --build
```

This starts 10 services: PostgreSQL, Redis, MinIO, Oracle XE, Airflow (web + scheduler + worker), FastAPI, and Streamlit.

### 3. Initialize storage and database

```bash
python scripts/bootstrap_minio.py    # Creates bronze + silver buckets
python scripts/bootstrap_oracle.py   # Creates Oracle tables
```

### 4. Access the services

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| FastAPI Docs | http://localhost:8000/docs | — |
| Streamlit Dashboard | http://localhost:8501 | — |

### 5. Run a smoke test

```bash
python scripts/smoke_test.py
```

---

## Azure App Registration (OneDrive Access)

To connect to OneDrive you need a Microsoft Azure App Registration with the **client credentials** flow:

1. Go to [Azure Portal](https://portal.azure.com) → **Azure Active Directory** → **App registrations** → **New registration**
2. Name it (e.g. `elt-pipeline`), choose **Accounts in this organizational directory only**
3. Go to **Certificates & secrets** → **New client secret** — copy the value into `GRAPH_CLIENT_SECRET`
4. Go to **API permissions** → **Add a permission** → **Microsoft Graph** → **Application permissions**:
   - `Files.Read.All`
   - `Sites.Read.All`
5. Click **Grant admin consent**
6. Copy **Application (client) ID** → `GRAPH_CLIENT_ID`
7. Copy **Directory (tenant) ID** → `GRAPH_TENANT_ID`

---

## Airflow DAGs

| DAG | Schedule | Description |
|---|---|---|
| `dag_extract_to_minio` | Daily 06:00 | Scans OneDrive folder, downloads new/changed files, uploads raw to MinIO bronze. Uses dynamic task mapping (one task per file). Triggers DAG 2 on success. |
| `dag_transform_to_oracle` | Triggered | Reads bronze files, transforms to Parquet (silver), merges into Oracle via MERGE SQL. |
| `dag_full_pipeline` | Mon–Fri 05:00 | Full end-to-end pipeline with data quality checks. |

### Airflow Connections (set once via UI)

Go to **Admin → Connections** and add:

| Conn ID | Type | Details |
|---|---|---|
| `graph_api_default` | HTTP | Host: `graph.microsoft.com`, extras: `{"tenant_id": "...", "client_id": "...", "client_secret": "..."}` |
| `minio_default` | S3 | Host: `minio:9000`, login: `minioadmin`, password: `minioadmin` |
| `oracle_default` | Oracle | Host: `oracle`, schema: `ELT_USER`, login: `elt_user`, password: `elt_password` |

---

## Key Design Decisions

### Idempotency
- **MinIO**: MD5 checksum stored in object metadata. Re-running the extract DAG skips files that haven't changed.
- **Oracle**: All inserts use `MERGE` (upsert) — running the same data twice produces the same result.

### Bronze / Silver Layers
- **Bronze** (`bronze/` bucket): Raw files as downloaded from OneDrive (Excel, CSV). Never modified.
- **Silver** (`silver/` bucket): Cleaned, normalized Parquet files ready for Oracle load.

### Airflow Credential Storage
Credentials are stored in Airflow's encrypted connection store (Fernet-encrypted in PostgreSQL), not in `.env` files or DAG code.

### python-oracledb Thin Mode
No Oracle Instant Client installation required. The driver connects directly using the Oracle Net protocol.

---

## Running Tests

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=src --cov-report=term-missing
```

---

## CI/CD Pipeline (Jenkins)

The `Jenkinsfile` defines an 8-stage pipeline:

1. **Checkout** — clone repo
2. **Lint** — ruff + mypy
3. **Unit Tests** — pytest with coverage
4. **Integration Tests** — docker-compose up + smoke test
5. **Build** — parallel Docker image builds (api, airflow, dashboard)
6. **Push** — push images to JFrog Artifactory
7. **Deploy** — `kubectl apply` to Kubernetes namespace `elt-pipeline`
8. **Smoke Test** — API health check against production

---

## Production (Kubernetes)

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secrets/
kubectl apply -f k8s/minio/
kubectl apply -f k8s/api/
kubectl apply -f k8s/dashboard/
```

For Kubernetes-native scheduled workflows, submit Argo WorkflowTemplates:

```bash
argo submit argo/workflows/full-pipeline-workflow.yaml
```

---

## Troubleshooting

**`python-oracledb` not found during Docker build**
Your network/proxy blocks PyPI access to this package. The build is designed to continue without it. Oracle DAG tasks will raise a clear error when they run. To install manually after containers start:
```bash
docker-compose exec airflow-worker pip install python-oracledb==2.5.0
```

**Airflow webserver not starting**
Wait ~60 seconds for `airflow-init` to complete DB migrations first. Check logs:
```bash
docker-compose logs airflow-init
```

**MinIO buckets not found**
Run `python scripts/bootstrap_minio.py` after services are up.

**Oracle connection refused**
Oracle XE takes ~2–3 minutes to fully start. Check:
```bash
docker-compose logs oracle
```
Wait until you see `DATABASE IS READY TO USE!`
