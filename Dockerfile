FROM python:3.12-slim

# MySQL client libs needed by PyMySQL C extensions (mysqlclient)
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default: run a generic worker that listens to all queues.
# Override via docker-compose command: to target specific queues / concurrency.
CMD ["celery", "-A", "tasks.celery_app", "worker", "--loglevel=info"]
