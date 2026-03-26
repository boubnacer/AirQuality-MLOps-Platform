FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir \
    fastapi uvicorn httpx \
    mlflow psycopg2-binary \
    pandas numpy scikit-learn xgboost \
    loguru python-dotenv pydantic

COPY api/ ./api/
COPY ml/ ./ml/

ENV MLFLOW_TRACKING_URI=http://mlflow:5000
ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=airquality
ENV POSTGRES_USER=airquality
ENV POSTGRES_PASSWORD=airquality123

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
