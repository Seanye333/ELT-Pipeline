# Standalone ELT pipeline runner — used by Argo Workflow jobs
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY scripts/ ./scripts/

ENV PYTHONPATH=/app

# Default entrypoint — override in Argo workflow steps
ENTRYPOINT ["python", "-m"]
CMD ["src.extract.onedrive_scanner"]
