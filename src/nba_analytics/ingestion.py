from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from nba_analytics.api_client import ApiClientError, NbaDailyApiClient
from nba_analytics.config import Settings


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def filter_dates(
    date_values: list[str],
    start_date: date | None,
    end_date: date | None,
    lookback_days: int | None,
) -> list[date]:
    parsed = sorted({date.fromisoformat(v[:10]) for v in date_values if v})
    if not parsed:
        return []

    if start_date is None and lookback_days is not None:
        start_date = date.today() - timedelta(days=lookback_days)

    filtered: list[date] = []
    for d in parsed:
        if start_date and d < start_date:
            continue
        if end_date and d > end_date:
            continue
        filtered.append(d)
    return filtered


@dataclass
class IngestionStats:
    prediction_rows: int = 0
    accuracy_rows: int = 0
    dfs_slate_rows: int = 0
    dfs_projection_rows: int = 0
    backtest_top3_rows: int = 0
    backtest_portfolio_rows: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "prediction_rows": self.prediction_rows,
            "accuracy_rows": self.accuracy_rows,
            "dfs_slate_rows": self.dfs_slate_rows,
            "dfs_projection_rows": self.dfs_projection_rows,
            "backtest_top3_rows": self.backtest_top3_rows,
            "backtest_portfolio_rows": self.backtest_portfolio_rows,
        }


def _get_or_none(client: NbaDailyApiClient, path: str, params: dict[str, Any] | None = None) -> Any | None:
    try:
        return client.get(path, params=params)
    except ApiClientError as exc:
        if "(404)" in str(exc):
            return None
        raise


def run_ingestion(
    settings: Settings,
    start_date: date | None = None,
    end_date: date | None = None,
    page_size: int | None = None,
    lookback_days: int | None = None,
) -> dict[str, int]:
    from nba_analytics import db

    page_size = min(page_size or settings.source_api_page_size, settings.ingestion_max_page_size)
    lookback_days = settings.ingestion_default_lookback_days if lookback_days is None else lookback_days

    stats = IngestionStats()
    client = NbaDailyApiClient(
        base_url=settings.source_api_base_url,
        timeout_seconds=settings.source_api_timeout_seconds,
    )

    run_id: int | None = None
    conn = db.connect(settings.database_url)
    try:
        db.initialize_schema(conn)
        run_id = db.begin_run(conn)

        prediction_dates = filter_dates(client.fetch_prediction_dates(), start_date, end_date, lookback_days)
        dfs_dates = filter_dates(client.fetch_dfs_dates(), start_date, end_date, lookback_days)
        backtest_dates = filter_dates(client.fetch_backtest_dates(), start_date, end_date, lookback_days)

        for game_date in prediction_dates:
            rows = client.paginated_records("/predictions", {"date": game_date.isoformat()}, page_size=page_size)
            db.store_raw_snapshot(conn, "/predictions", game_date, rows)
            stats.prediction_rows += db.upsert_prediction_rows(conn, game_date, rows)

        if prediction_dates:
            accuracy_payload = client.get(
                "/accuracy/daily-summary",
                {"start_date": prediction_dates[0].isoformat(), "end_date": prediction_dates[-1].isoformat()},
            )
            accuracy_rows = client.records(accuracy_payload)
            db.store_raw_snapshot(conn, "/accuracy/daily-summary", None, accuracy_payload)
            stats.accuracy_rows += db.upsert_accuracy_rows(conn, accuracy_rows)

        for slate_date in dfs_dates:
            slate_payload = _get_or_none(client, f"/dfs/slate-results/{slate_date.isoformat()}")
            if slate_payload:
                db.store_raw_snapshot(conn, "/dfs/slate-results/{slate_date}", slate_date, slate_payload)
                stats.dfs_slate_rows += db.upsert_dfs_slate_row(conn, slate_date, slate_payload)

            projection_rows = client.paginated_records(
                f"/dfs/projections/{slate_date.isoformat()}",
                page_size=page_size,
            )
            db.store_raw_snapshot(conn, "/dfs/projections", slate_date, projection_rows)
            stats.dfs_projection_rows += db.upsert_dfs_projection_rows(conn, slate_date, projection_rows)

        for bt_date in backtest_dates:
            params = {"start_date": bt_date.isoformat(), "end_date": bt_date.isoformat()}
            top3_rows = client.paginated_records("/backtest/top3", params=params, page_size=page_size)
            db.store_raw_snapshot(conn, "/backtest/top3", bt_date, top3_rows)
            stats.backtest_top3_rows += db.upsert_backtest_rows(conn, "backtest_top3_daily", top3_rows)

            portfolio_rows = client.paginated_records("/backtest/portfolio", params=params, page_size=page_size)
            db.store_raw_snapshot(conn, "/backtest/portfolio", bt_date, portfolio_rows)
            stats.backtest_portfolio_rows += db.upsert_backtest_rows(conn, "backtest_portfolio_daily", portfolio_rows)

        conn.commit()
        db.finish_run(conn, run_id, "success", "Ingestion completed.", stats.as_dict())
        return stats.as_dict()
    except Exception as exc:
        conn.rollback()
        if run_id is not None:
            db.finish_run(conn, run_id, "failed", str(exc), stats.as_dict())
        raise
    finally:
        client.close()
        conn.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest NBA Daily API data into Postgres.")
    parser.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=None, help="YYYY-MM-DD")
    parser.add_argument("--page-size", type=int, default=None, help="Endpoint page size (max from settings).")
    parser.add_argument("--lookback-days", type=int, default=None, help="Used when start-date is not set.")
    parser.add_argument("--init-only", action="store_true", help="Only initialize database schema.")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    settings = Settings.from_env()

    if args.init_only:
        from nba_analytics import db

        conn = db.connect(settings.database_url)
        try:
            db.initialize_schema(conn)
            print("Schema initialized.")
        finally:
            conn.close()
        return

    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date)
    stats = run_ingestion(
        settings=settings,
        start_date=start_date,
        end_date=end_date,
        page_size=args.page_size,
        lookback_days=args.lookback_days,
    )
    print(stats)


if __name__ == "__main__":
    main()
