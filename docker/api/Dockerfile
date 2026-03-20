# FastAPI service image
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    fastapi==0.111.0 \
    uvicorn[standard]==0.29.0 \
    pydantic==2.7.1 \
    pydantic-settings==2.2.1 \
    httpx==0.27.0 \
    boto3==1.34.69 \
    structlog==24.1.0 \
    python-dotenv==1.0.1 \
    tenacity==8.3.0 \
    pandas==2.2.1

COPY src/ ./src/
COPY api/ ./api/

ENV PYTHONPATH=/app
EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
