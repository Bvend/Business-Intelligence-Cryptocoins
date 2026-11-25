-- Connect to crypto_bi before running.
-- ODS: raw landing zone filled by Apache NiFi.

CREATE SCHEMA IF NOT EXISTS ods;

-- CoinGecko Top 100 markets snapshot
CREATE TABLE IF NOT EXISTS ods.cg_markets (
    id                  BIGSERIAL PRIMARY KEY,
    coin_id             TEXT NOT NULL,
    symbol              TEXT,
    name                TEXT,
    current_price       NUMERIC,
    market_cap          NUMERIC,
    total_volume        NUMERIC,
    market_cap_rank     INTEGER,
    price_change_pct_7d NUMERIC,
    price_change_pct_30d NUMERIC,
    price_change_pct_1y NUMERIC,
    last_updated        TEXT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cg_markets_ingested ON ods.cg_markets (ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_cg_markets_coin ON ods.cg_markets (coin_id, ingested_at DESC);

-- CoinMarketCap Top 100 listings snapshot
CREATE TABLE IF NOT EXISTS ods.cmc_listings (
    id                  BIGSERIAL PRIMARY KEY,
    cmc_id              BIGINT,
    symbol              TEXT,
    name                TEXT,
    cmc_rank            INTEGER,
    price_usd           NUMERIC,
    market_cap          NUMERIC,
    volume_24h          NUMERIC,
    percent_change_7d   NUMERIC,
    percent_change_30d  NUMERIC,
    percent_change_90d  NUMERIC,
    last_updated        TEXT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cmc_listings_ingested ON ods.cmc_listings (ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_cmc_listings_symbol ON ods.cmc_listings (symbol, ingested_at DESC);

-- CoinGecko global metrics
CREATE TABLE IF NOT EXISTS ods.cg_global (
    id                      BIGSERIAL PRIMARY KEY,
    active_cryptocurrencies INTEGER,
    markets                 INTEGER,
    total_market_cap_usd    NUMERIC,
    total_volume_usd        NUMERIC,
    market_cap_pct_btc      NUMERIC,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cg_global_ingested ON ods.cg_global (ingested_at DESC);

-- CoinMarketCap global metrics
CREATE TABLE IF NOT EXISTS ods.cmc_global (
    id                        BIGSERIAL PRIMARY KEY,
    active_cryptocurrencies   INTEGER,
    active_exchanges          INTEGER,
    total_market_cap_usd      NUMERIC,
    total_volume_24h_usd      NUMERIC,
    ingested_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cmc_global_ingested ON ods.cmc_global (ingested_at DESC);

-- USD → EUR FX rate (from CoinGecko exchange_rates)
CREATE TABLE IF NOT EXISTS ods.fx_rate (
    id          BIGSERIAL PRIMARY KEY,
    usd_eur     NUMERIC NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fx_rate_ingested ON ods.fx_rate (ingested_at DESC);

-- CoinGecko daily history (Top 10, ~5 years)
CREATE TABLE IF NOT EXISTS ods.cg_history (
    id            BIGSERIAL PRIMARY KEY,
    coin_id       TEXT NOT NULL,
    ts_ms         BIGINT NOT NULL,
    price_usd     NUMERIC,
    market_cap    NUMERIC,
    total_volume  NUMERIC,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (coin_id, ts_ms)
);

CREATE INDEX IF NOT EXISTS idx_cg_history_coin_ts ON ods.cg_history (coin_id, ts_ms);
