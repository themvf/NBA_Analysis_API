from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


def connect(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url)


def initialize_schema(conn: psycopg.Connection) -> None:
    schema_sql = """
    CREATE TABLE IF NOT EXISTS ingestion_runs (
        run_id BIGSERIAL PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        status TEXT NOT NULL DEFAULT 'running',
        message TEXT,
        stats JSONB NOT NULL DEFAULT '{}'::jsonb
    );

    CREATE TABLE IF NOT EXISTS raw_endpoint_snapshot (
        snapshot_id BIGSERIAL PRIMARY KEY,
        endpoint TEXT NOT NULL,
        snapshot_date DATE,
        payload JSONB NOT NULL,
        retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        payload_hash TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_endpoint_snapshot
        ON raw_endpoint_snapshot (endpoint, snapshot_date, payload_hash);

    CREATE TABLE IF NOT EXISTS prediction_daily (
        row_key TEXT PRIMARY KEY,
        game_date DATE NOT NULL,
        player_name TEXT,
        team TEXT,
        projected_ppg DOUBLE PRECISION,
        actual_ppg DOUBLE PRECISION,
        absolute_error DOUBLE PRECISION,
        payload JSONB NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS ix_prediction_daily_game_date ON prediction_daily (game_date);
    CREATE INDEX IF NOT EXISTS ix_prediction_daily_team ON prediction_daily (team);

    CREATE TABLE IF NOT EXISTS accuracy_daily (
        game_date DATE PRIMARY KEY,
        mean_absolute_error DOUBLE PRECISION,
        rmse DOUBLE PRECISION,
        hit_rate_floor_ceiling DOUBLE PRECISION,
        mean_error DOUBLE PRECISION,
        payload JSONB NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS dfs_slate_daily (
        slate_date DATE PRIMARY KEY,
        proj_mae DOUBLE PRECISION,
        proj_correlation DOUBLE PRECISION,
        lineup_efficiency_pct DOUBLE PRECISION,
        value_correlation DOUBLE PRECISION,
        payload JSONB NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS dfs_projection_daily (
        row_key TEXT PRIMARY KEY,
        slate_date DATE NOT NULL,
        player_name TEXT,
        team TEXT,
        proj_fpts DOUBLE PRECISION,
        actual_fpts DOUBLE PRECISION,
        absolute_error DOUBLE PRECISION,
        payload JSONB NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS ix_dfs_projection_daily_slate_date ON dfs_projection_daily (slate_date);

    CREATE TABLE IF NOT EXISTS backtest_top3_daily (
        row_key TEXT PRIMARY KEY,
        slate_date DATE,
        strategy TEXT,
        payload JSONB NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS ix_backtest_top3_daily_slate_date ON backtest_top3_daily (slate_date);

    CREATE TABLE IF NOT EXISTS backtest_portfolio_daily (
        row_key TEXT PRIMARY KEY,
        slate_date DATE,
        strategy TEXT,
        payload JSONB NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS ix_backtest_portfolio_daily_slate_date ON backtest_portfolio_daily (slate_date);
    """
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()


def begin_run(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO ingestion_runs DEFAULT VALUES RETURNING run_id")
        run_id = cur.fetchone()[0]
    conn.commit()
    return int(run_id)


def finish_run(
    conn: psycopg.Connection,
    run_id: int,
    status: str,
    message: str,
    stats: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion_runs
            SET completed_at = NOW(),
                status = %s,
                message = %s,
                stats = %s
            WHERE run_id = %s
            """,
            (status, message, Jsonb(stats), run_id),
        )
    conn.commit()


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def stable_hash(payload: Any) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def store_raw_snapshot(
    conn: psycopg.Connection,
    endpoint: str,
    snapshot_date: date | None,
    payload: Any,
) -> None:
    payload_hash = stable_hash(payload)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_endpoint_snapshot (endpoint, snapshot_date, payload, payload_hash)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (endpoint, snapshot_date, payload_hash) DO NOTHING
            """,
            (endpoint, snapshot_date, Jsonb(payload), payload_hash),
        )


def upsert_prediction_rows(conn: psycopg.Connection, game_date: date, rows: list[dict[str, Any]]) -> int:
    inserted = 0
    sql = """
        INSERT INTO prediction_daily (
            row_key, game_date, player_name, team, projected_ppg, actual_ppg, absolute_error, payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (row_key) DO UPDATE SET
            projected_ppg = EXCLUDED.projected_ppg,
            actual_ppg = EXCLUDED.actual_ppg,
            absolute_error = EXCLUDED.absolute_error,
            payload = EXCLUDED.payload,
            ingested_at = NOW()
    """
    with conn.cursor() as cur:
        for row in rows:
            player_name = (row.get("player_name") or row.get("name") or "").strip() or None
            team = (row.get("team") or row.get("team_name") or "").strip() or None
            projected = to_float(row.get("projected_ppg") or row.get("projection_ppg") or row.get("projected_points"))
            actual = to_float(row.get("actual_ppg") or row.get("actual_points"))
            abs_error = abs(actual - projected) if actual is not None and projected is not None else None
            row_key = stable_hash([game_date.isoformat(), player_name, team, row])
            cur.execute(
                sql,
                (row_key, game_date, player_name, team, projected, actual, abs_error, Jsonb(row)),
            )
            inserted += 1
    return inserted


def upsert_accuracy_rows(conn: psycopg.Connection, rows: list[dict[str, Any]]) -> int:
    inserted = 0
    sql = """
        INSERT INTO accuracy_daily (
            game_date, mean_absolute_error, rmse, hit_rate_floor_ceiling, mean_error, payload
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_date) DO UPDATE SET
            mean_absolute_error = EXCLUDED.mean_absolute_error,
            rmse = EXCLUDED.rmse,
            hit_rate_floor_ceiling = EXCLUDED.hit_rate_floor_ceiling,
            mean_error = EXCLUDED.mean_error,
            payload = EXCLUDED.payload,
            ingested_at = NOW()
    """
    with conn.cursor() as cur:
        for row in rows:
            game_date = to_date(row.get("game_date") or row.get("date"))
            if game_date is None:
                continue
            cur.execute(
                sql,
                (
                    game_date,
                    to_float(row.get("mean_absolute_error") or row.get("mae")),
                    to_float(row.get("rmse")),
                    to_float(row.get("hit_rate_floor_ceiling") or row.get("hit_rate")),
                    to_float(row.get("mean_error")),
                    Jsonb(row),
                ),
            )
            inserted += 1
    return inserted


def upsert_dfs_slate_row(conn: psycopg.Connection, slate_date: date, row: dict[str, Any]) -> int:
    sql = """
        INSERT INTO dfs_slate_daily (
            slate_date, proj_mae, proj_correlation, lineup_efficiency_pct, value_correlation, payload
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (slate_date) DO UPDATE SET
            proj_mae = EXCLUDED.proj_mae,
            proj_correlation = EXCLUDED.proj_correlation,
            lineup_efficiency_pct = EXCLUDED.lineup_efficiency_pct,
            value_correlation = EXCLUDED.value_correlation,
            payload = EXCLUDED.payload,
            ingested_at = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                slate_date,
                to_float(row.get("proj_mae")),
                to_float(row.get("proj_correlation")),
                to_float(row.get("lineup_efficiency_pct")),
                to_float(row.get("value_correlation")),
                Jsonb(row),
            ),
        )
    return 1


def upsert_dfs_projection_rows(conn: psycopg.Connection, slate_date: date, rows: list[dict[str, Any]]) -> int:
    inserted = 0
    sql = """
        INSERT INTO dfs_projection_daily (
            row_key, slate_date, player_name, team, proj_fpts, actual_fpts, absolute_error, payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (row_key) DO UPDATE SET
            proj_fpts = EXCLUDED.proj_fpts,
            actual_fpts = EXCLUDED.actual_fpts,
            absolute_error = EXCLUDED.absolute_error,
            payload = EXCLUDED.payload,
            ingested_at = NOW()
    """
    with conn.cursor() as cur:
        for row in rows:
            player_name = (row.get("player_name") or row.get("name") or "").strip() or None
            team = (row.get("team") or row.get("team_name") or "").strip() or None
            proj = to_float(row.get("proj_fpts") or row.get("projected_fpts"))
            actual = to_float(row.get("actual_fpts"))
            abs_error = abs(actual - proj) if actual is not None and proj is not None else None
            row_key = stable_hash([slate_date.isoformat(), player_name, team, row])
            cur.execute(
                sql,
                (row_key, slate_date, player_name, team, proj, actual, abs_error, Jsonb(row)),
            )
            inserted += 1
    return inserted


def upsert_backtest_rows(
    conn: psycopg.Connection,
    table_name: str,
    rows: list[dict[str, Any]],
) -> int:
    if table_name not in {"backtest_top3_daily", "backtest_portfolio_daily"}:
        raise ValueError(f"Unsupported table: {table_name}")

    inserted = 0
    sql = f"""
        INSERT INTO {table_name} (row_key, slate_date, strategy, payload)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (row_key) DO UPDATE SET
            strategy = EXCLUDED.strategy,
            payload = EXCLUDED.payload,
            ingested_at = NOW()
    """
    with conn.cursor() as cur:
        for row in rows:
            slate_date = to_date(row.get("slate_date") or row.get("date"))
            strategy = (row.get("strategy") or row.get("strategy_name") or "").strip() or None
            row_key = stable_hash([table_name, slate_date.isoformat() if slate_date else "", strategy, row])
            cur.execute(sql, (row_key, slate_date, strategy, Jsonb(row)))
            inserted += 1
    return inserted

