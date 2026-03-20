# Streamlit dashboard image
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    streamlit==1.34.0 \
    plotly==5.21.0 \
    httpx==0.27.0 \
    pandas==2.2.1 \
    python-dotenv==1.0.1

COPY dashboard/ ./dashboard/
COPY src/config/ ./src/config/

ENV PYTHONPATH=/app
EXPOSE 8501

CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
