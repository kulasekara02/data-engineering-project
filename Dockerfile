# ===================================
# DATA ENGINEERING PLATFORM - Docker
# Multi-stage build for production
# ===================================

# Stage 1: Builder
FROM python:3.11-slim AS builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Stage 2: Production
FROM python:3.11-slim AS production

LABEL maintainer="kulasekara02"
LABEL description="Data Engineering Platform - ETL, API, Dashboard"
LABEL version="1.0.0"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY src/ ./src/
COPY frontend/ ./frontend/
COPY run.py server.py ./
COPY requirements.txt .

# Create data directories
RUN mkdir -p data/raw data/processed/quality_reports

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser
RUN chown -R appuser:appuser /app
USER appuser

# Generate data and run ETL at build time
RUN python src/etl/generate_data.py && python src/etl/etl_pipeline.py

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/overview')" || exit 1

EXPOSE 8000

CMD ["python", "server.py"]
