FROM python:3.12-slim

WORKDIR /app

COPY services/api/pyproject.toml ./
COPY services/api/src ./src
COPY services/api/alembic.ini ./alembic.ini
COPY services/api/migrations ./migrations
COPY deploy/api-entrypoint.py /app/deploy/api-entrypoint.py
COPY deploy/market-worker-healthcheck.py /app/deploy/market-worker-healthcheck.py

RUN python -m pip install --no-cache-dir .

ENTRYPOINT ["python", "/app/deploy/api-entrypoint.py"]
CMD ["uvicorn", "crypto_research.api:app", "--host", "0.0.0.0", "--port", "8000"]
