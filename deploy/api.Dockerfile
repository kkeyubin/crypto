FROM python:3.12-slim

WORKDIR /app

COPY services/api/pyproject.toml ./
COPY services/api/src ./src

RUN python -m pip install --no-cache-dir .

CMD ["uvicorn", "crypto_research.api:app", "--host", "0.0.0.0", "--port", "8000"]
