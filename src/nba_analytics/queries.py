from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import psycopg


def _df(conn: psycopg.Connection, sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        columns = [desc.name for desc in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)


def accuracy_trend(conn: psycopg.Connection, start_date: date, end_date: date) -> pd.DataFrame:
    return _df(
        conn,
        """
        SELECT
            game_date,
            mean_absolute_error,
            rmse,
            hit_rate_floor_ceiling,
            mean_error
        FROM accuracy_daily
        WHERE game_date BETWEEN %s AND %s
        ORDER BY game_date
        """,
        (start_date, end_date),
    )


def dfs_trend(conn: psycopg.Connection, start_date: date, end_date: date) -> pd.DataFrame:
    return _df(
        conn,
        """
        SELECT
            slate_date,
            proj_mae,
            proj_correlation,
            lineup_efficiency_pct,
            value_correlation
        FROM dfs_slate_daily
        WHERE slate_date BETWEEN %s AND %s
        ORDER BY slate_date
        """,
        (start_date, end_date),
    )


def top_prediction_misses(conn: psycopg.Connection, start_date: date, end_date: date, limit: int = 25) -> pd.DataFrame:
    return _df(
        conn,
        """
        SELECT
            game_date,
            player_name,
            team,
            projected_ppg,
            actual_ppg,
            absolute_error
        FROM prediction_daily
        WHERE game_date BETWEEN %s AND %s
          AND absolute_error IS NOT NULL
        ORDER BY absolute_error DESC
        LIMIT %s
        """,
        (start_date, end_date, limit),
    )


def backtest_activity(conn: psycopg.Connection, start_date: date, end_date: date) -> pd.DataFrame:
    return _df(
        conn,
        """
        SELECT
            slate_date,
            COUNT(*) AS top3_rows
        FROM backtest_top3_daily
        WHERE slate_date BETWEEN %s AND %s
        GROUP BY slate_date
        ORDER BY slate_date
        """,
        (start_date, end_date),
    )

