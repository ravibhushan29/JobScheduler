# syntax=docker/dockerfile:1
# One image for every service; docker-compose.yml sets each service's command
# and health check.
FROM python:3.12-slim

# No .pyc files; unbuffered stdout so logs appear immediately.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /code

# Dependencies first, so code changes don't invalidate this layer.
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app

# Run as an unprivileged user; it owns only the writable data/log dirs.
RUN useradd --create-home --uid 1000 appuser \
    && mkdir -p /code/data /code/logs \
    && chown -R appuser:appuser /code/data /code/logs
USER appuser

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
