# API Analytics NBA

Standalone analytics platform for the NBA Daily local API. It ingests prediction, DFS, and backtest data from your API, stores normalized records in Postgres, and serves a Streamlit dashboard for trend analysis.

## Stack

- FastAPI source API (already running separately)
- Postgres (Neon or Render Postgres)
- Python ingestion service
- Streamlit dashboard

## Quick Start

1. Create a virtual environment and install dependencies:

```bash
pip install -r requirements.txt
pip install -e .
```

2. Copy `.env.example` to `.env` and fill values.

3. Initialize schema:

```bash
python -m nba_analytics.ingestion --init-only
```

4. Run ingestion:

```bash
python -m nba_analytics.ingestion
```

5. Start dashboard:

```bash
streamlit run dashboard/app.py
```

## Docker

1. Build and start Postgres + dashboard:

```bash
docker compose up --build dashboard
```

2. Run ingestion job manually:

```bash
docker compose --profile jobs run --rm ingest
```

3. Open dashboard:

```text
http://127.0.0.1:8501
```

Notes:
- Default Docker source API target is `http://host.docker.internal:8000`.
- Override `SOURCE_API_BASE_URL` in your shell or `.env` if your API is hosted elsewhere.

## Render Automation

This repo includes `render.yaml` that provisions:
- Managed Postgres database (`nba-analytics-db`)
- Web service for Streamlit dashboard
- Daily cron service for ingestion

This repo also includes `render.fullstack.yaml` that provisions:
- The source NBA API service (`nba-daily-source-api`) from `https://github.com/themvf/NBA_Daily`
- A persistent disk mounted at `/var/data` for `nba_stats.db`
- Managed Postgres + dashboard + cron ingestion

Deploy steps:
1. Push this repo to GitHub.
2. In Render, create a new Blueprint and select the repo.
3. Choose which Blueprint spec to deploy:
- `render.yaml` for analytics-only (source API hosted elsewhere)
- `render.fullstack.yaml` for full stack (source API + analytics)
4. If using `render.yaml`, set `SOURCE_API_BASE_URL` for web and cron to your reachable source API endpoint.
5. Deploy.

Render behavior:
- Dashboard service auto-runs schema initialization on deploy.
- Cron service runs ingestion daily at `10:00 UTC`.
- Full-stack Blueprint auto-populates `SOURCE_API_BASE_URL` from the source API service URL.
- Full-stack Blueprint expects source SQLite file at `/var/data/nba_stats.db` in the source API service.

## Key Files

- `src/nba_analytics/api_client.py`: typed API client with pagination and resilient response parsing.
- `src/nba_analytics/ingestion.py`: incremental ingestion and upsert pipeline.
- `src/nba_analytics/db.py`: schema creation and upsert helpers.
- `dashboard/app.py`: Streamlit analytics dashboard.
- `render.yaml`: Render deployment blueprint.
- `Dockerfile`: production container image.
- `docker-compose.yml`: local container orchestration.
- `docker/entrypoint.sh`: mode-based runtime entrypoint (`dashboard`, `ingest`, `init-db`).

## Environment Variables

See `.env.example`.
