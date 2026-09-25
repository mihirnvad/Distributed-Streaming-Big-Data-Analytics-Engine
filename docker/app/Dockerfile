# Lightweight Python image for the event producer, dimension seeder and dashboard.
FROM python:3.12-slim

ENV TZ=UTC \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN useradd --create-home --uid 10001 app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY --chown=app:app common ./common
COPY --chown=app:app producer ./producer
COPY --chown=app:app dashboard ./dashboard
COPY --chown=app:app config ./config

USER app
EXPOSE 8501
CMD ["python", "-m", "producer.transaction_producer"]
