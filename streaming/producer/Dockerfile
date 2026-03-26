FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir confluent-kafka pydantic python-dotenv loguru
COPY . .
CMD ["python", "simulator.py"]
