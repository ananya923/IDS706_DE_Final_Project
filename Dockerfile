# Multi-stage Dockerfile for Ethereum Fraud Detection Pipeline

# Base Stage
FROM python:3.10-slim as base

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

# Dependencies Stage
FROM base as dependencies

COPY requirements.txt requirements-dev.txt ./

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r requirements-dev.txt

# Testing Stage
FROM dependencies as testing

USER appuser

COPY --chown=appuser:appuser glue_jobs/ ./glue_jobs/
COPY --chown=appuser:appuser scripts/ ./scripts/
COPY --chown=appuser:appuser tests/ ./tests/
COPY --chown=appuser:appuser dashboard/ ./dashboard/

ENV PYTHONPATH=/app
ENV AWS_DEFAULT_REGION=us-east-1

CMD ["pytest", "tests/", "-v", "--tb=short"]

# Ingestion Stage
FROM dependencies as ingestion

USER appuser

COPY --chown=appuser:appuser glue_jobs/ ./glue_jobs/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "glue_jobs/etherscan_to_s3_glue_v3.py"]

# Feature Engineering Stage
FROM dependencies as feature-engineering

USER appuser

COPY --chown=appuser:appuser scripts/feature_engineering.py ./scripts/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "scripts/feature_engineering.py"]

# Modeling Stage
FROM dependencies as modeling

USER appuser

COPY --chown=appuser:appuser scripts/modeling.py ./scripts/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "scripts/modeling.py"]

# Dashboard Stage
FROM dependencies as dashboard

USER appuser

COPY --chown=appuser:appuser dashboard/ ./dashboard/

EXPOSE 8501

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]

# Production Stage
FROM python:3.10-slim as production

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

COPY --from=dependencies /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=dependencies /usr/local/bin /usr/local/bin

USER appuser

COPY --chown=appuser:appuser glue_jobs/ ./glue_jobs/
COPY --chown=appuser:appuser scripts/ ./scripts/
COPY --chown=appuser:appuser dashboard/ ./dashboard/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "--version"]
