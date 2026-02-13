FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt pyproject.toml README.md ./
COPY src ./src
COPY dashboard ./dashboard
COPY sql ./sql
COPY docker ./docker

RUN pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip install -e . \
    && chmod +x /app/docker/entrypoint.sh

EXPOSE 8501

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["dashboard"]
