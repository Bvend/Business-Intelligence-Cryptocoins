-- Connect to crypto_bi before running.
-- DWH: dimensional model for Power BI.

CREATE SCHEMA IF NOT EXISTS dwh;

-- Dimensions
CREATE TABLE IF NOT EXISTS dwh.dim_provider (
    provider_sk   SERIAL PRIMARY KEY,
    provider_code TEXT NOT NULL UNIQUE,
    provider_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_crypto (
    crypto_sk     SERIAL PRIMARY KEY,
    symbol        TEXT NOT NULL UNIQUE,
    name          TEXT,
    coingecko_id  TEXT,
    cmc_id        BIGINT
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_sk     INTEGER PRIMARY KEY, -- YYYYMMDD
    full_date   DATE NOT NULL UNIQUE,
    year        INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    day         INTEGER NOT NULL,
    week        INTEGER NOT NULL,
    is_weekend  BOOLEAN NOT NULL
);

-- Facts
CREATE TABLE IF NOT EXISTS dwh.fact_market_snapshot (
    fact_id             BIGSERIAL PRIMARY KEY,
    date_sk             INTEGER NOT NULL REFERENCES dwh.dim_date (date_sk),
    provider_sk         INTEGER NOT NULL REFERENCES dwh.dim_provider (provider_sk),
    crypto_sk           INTEGER NOT NULL REFERENCES dwh.dim_crypto (crypto_sk),
    price_usd           NUMERIC,
    price_eur           NUMERIC,
    market_cap_usd      NUMERIC,
    market_cap_eur      NUMERIC,
    volume_usd          NUMERIC,
    volume_eur          NUMERIC,
    market_rank         INTEGER,
    pct_change_7d       NUMERIC,
    pct_change_30d      NUMERIC,
    pct_change_90d      NUMERIC,
    pct_change_1y       NUMERIC,
    snapshot_ts         TIMESTAMPTZ NOT NULL,
    UNIQUE (provider_sk, crypto_sk, snapshot_ts)
);

CREATE INDEX IF NOT EXISTS idx_fact_mkt_snap_ts
    ON dwh.fact_market_snapshot (snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS idx_fact_mkt_snap_rank
    ON dwh.fact_market_snapshot (provider_sk, snapshot_ts DESC, market_rank);

CREATE TABLE IF NOT EXISTS dwh.fact_global_market (
    fact_id                   BIGSERIAL PRIMARY KEY,
    date_sk                   INTEGER NOT NULL REFERENCES dwh.dim_date (date_sk),
    provider_sk               INTEGER NOT NULL REFERENCES dwh.dim_provider (provider_sk),
    total_market_cap_usd      NUMERIC,
    total_market_cap_eur      NUMERIC,
    total_volume_usd          NUMERIC,
    total_volume_eur          NUMERIC,
    active_cryptocurrencies   INTEGER,
    active_exchanges          INTEGER,
    pct_cap_top10             NUMERIC,
    pct_cap_top100            NUMERIC,
    snapshot_ts               TIMESTAMPTZ NOT NULL,
    UNIQUE (provider_sk, snapshot_ts)
);

CREATE TABLE IF NOT EXISTS dwh.fact_price_history (
    fact_id          BIGSERIAL PRIMARY KEY,
    date_sk          INTEGER NOT NULL REFERENCES dwh.dim_date (date_sk),
    crypto_sk        INTEGER NOT NULL REFERENCES dwh.dim_crypto (crypto_sk),
    provider_sk      INTEGER NOT NULL REFERENCES dwh.dim_provider (provider_sk),
    price_date       DATE NOT NULL,
    price_usd        NUMERIC,
    price_eur        NUMERIC,
    market_cap_usd   NUMERIC,
    market_cap_eur   NUMERIC,
    volume_usd       NUMERIC,
    volume_eur       NUMERIC,
    UNIQUE (crypto_sk, price_date)
);

CREATE INDEX IF NOT EXISTS idx_fact_price_hist_crypto_date
    ON dwh.fact_price_history (crypto_sk, price_date);
