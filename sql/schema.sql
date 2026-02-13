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

CREATE TABLE IF NOT EXISTS backtest_top3_daily (
    row_key TEXT PRIMARY KEY,
    slate_date DATE,
    strategy TEXT,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS backtest_portfolio_daily (
    row_key TEXT PRIMARY KEY,
    slate_date DATE,
    strategy TEXT,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
