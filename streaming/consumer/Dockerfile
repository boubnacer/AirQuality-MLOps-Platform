FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir confluent-kafka pydantic psycopg2-binary pymongo python-dotenv loguru pandas sqlalchemy
COPY . .
CMD ["python", "etl_consumer.py"]
