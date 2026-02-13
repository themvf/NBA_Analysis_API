from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import psycopg
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from nba_analytics.config import Settings
from nba_analytics import queries


st.set_page_config(page_title="NBA Analytics", layout="wide")
st.title("NBA Model Analytics")

settings = Settings.from_env()

default_end = date.today()
default_start = default_end - timedelta(days=30)

with st.sidebar:
    st.header("Filters")
    start_date = st.date_input("Start Date", value=default_start)
    end_date = st.date_input("End Date", value=default_end)
    refresh = st.button("Refresh")

if start_date > end_date:
    st.error("Start date must be before end date.")
    st.stop()


@st.cache_data(ttl=60, show_spinner=False)
def load_data(start_dt: date, end_dt: date) -> dict[str, pd.DataFrame]:
    with psycopg.connect(settings.database_url) as conn:
        return {
            "accuracy": queries.accuracy_trend(conn, start_dt, end_dt),
            "dfs": queries.dfs_trend(conn, start_dt, end_dt),
            "misses": queries.top_prediction_misses(conn, start_dt, end_dt, limit=25),
            "backtest": queries.backtest_activity(conn, start_dt, end_dt),
        }


if refresh:
    load_data.clear()

data = load_data(start_date, end_date)
accuracy_df = data["accuracy"]
dfs_df = data["dfs"]
misses_df = data["misses"]
backtest_df = data["backtest"]

st.subheader("Accuracy Overview")
if accuracy_df.empty:
    st.info("No accuracy data found for this range.")
else:
    latest = accuracy_df.iloc[-1]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest MAE", f"{latest['mean_absolute_error']:.2f}" if pd.notna(latest["mean_absolute_error"]) else "n/a")
    c2.metric("Latest RMSE", f"{latest['rmse']:.2f}" if pd.notna(latest["rmse"]) else "n/a")
    c3.metric(
        "Latest Hit Rate",
        f"{latest['hit_rate_floor_ceiling']:.1%}" if pd.notna(latest["hit_rate_floor_ceiling"]) else "n/a",
    )
    c4.metric("Rows", f"{len(accuracy_df)}")

    st.line_chart(
        accuracy_df.set_index("game_date")[["mean_absolute_error", "rmse"]],
        use_container_width=True,
    )
    st.line_chart(
        accuracy_df.set_index("game_date")[["hit_rate_floor_ceiling"]],
        use_container_width=True,
    )

st.subheader("DFS Slate Quality")
if dfs_df.empty:
    st.info("No DFS slate results found for this range.")
else:
    st.line_chart(
        dfs_df.set_index("slate_date")[["proj_mae", "proj_correlation", "lineup_efficiency_pct"]],
        use_container_width=True,
    )

st.subheader("Backtest Activity")
if backtest_df.empty:
    st.info("No backtest rows found for this range.")
else:
    st.bar_chart(backtest_df.set_index("slate_date")[["top3_rows"]], use_container_width=True)

st.subheader("Top Prediction Misses")
if misses_df.empty:
    st.info("No prediction errors found for this range.")
else:
    st.dataframe(misses_df, hide_index=True, use_container_width=True)

