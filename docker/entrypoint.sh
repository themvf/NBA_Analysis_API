#!/usr/bin/env sh
set -eu

MODE="${1:-dashboard}"
shift || true

case "${MODE}" in
  dashboard)
    python -m nba_analytics.ingestion --init-only
    exec streamlit run dashboard/app.py --server.port "${PORT:-8501}" --server.address 0.0.0.0 "$@"
    ;;
  ingest)
    exec python -m nba_analytics.ingestion "$@"
    ;;
  init-db)
    exec python -m nba_analytics.ingestion --init-only "$@"
    ;;
  *)
    exec "${MODE}" "$@"
    ;;
esac
