-- Connect to crypto_bi before running.
-- Creates dwh.run_etl() and analytical views. Re-runnable.
-- NiFi ExecuteSQL calls: SELECT dwh.run_etl();

CREATE OR REPLACE FUNCTION dwh._ensure_date(p_date DATE)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_sk INTEGER;
BEGIN
    IF p_date IS NULL THEN
        RETURN NULL;
    END IF;
    v_sk := to_char(p_date, 'YYYYMMDD')::INTEGER;
    INSERT INTO dwh.dim_date (date_sk, full_date, year, month, day, week, is_weekend)
    VALUES (
        v_sk,
        p_date,
        EXTRACT(YEAR FROM p_date)::INTEGER,
        EXTRACT(MONTH FROM p_date)::INTEGER,
        EXTRACT(DAY FROM p_date)::INTEGER,
        EXTRACT(WEEK FROM p_date)::INTEGER,
        EXTRACT(ISODOW FROM p_date) IN (6, 7)
    )
    ON CONFLICT (date_sk) DO NOTHING;
    RETURN v_sk;
END;
$$;

CREATE OR REPLACE FUNCTION dwh.run_etl()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_fx NUMERIC;
    v_cg_provider INTEGER;
    v_cmc_provider INTEGER;
    v_inserted_snap BIGINT := 0;
    v_inserted_glob BIGINT := 0;
    v_upserted_hist BIGINT := 0;
BEGIN
    -- 1) FX rate (fallback 0.92)
    SELECT usd_eur INTO v_fx
    FROM ods.fx_rate
    ORDER BY ingested_at DESC
    LIMIT 1;
    IF v_fx IS NULL OR v_fx <= 0 THEN
        v_fx := 0.92;
    END IF;

    -- 2) Providers
    INSERT INTO dwh.dim_provider (provider_code, provider_name)
    VALUES
        ('coingecko', 'CoinGecko'),
        ('coinmarketcap', 'CoinMarketCap')
    ON CONFLICT (provider_code) DO UPDATE
        SET provider_name = EXCLUDED.provider_name;

    SELECT provider_sk INTO v_cg_provider FROM dwh.dim_provider WHERE provider_code = 'coingecko';
    SELECT provider_sk INTO v_cmc_provider FROM dwh.dim_provider WHERE provider_code = 'coinmarketcap';

    -- 3) dim_crypto from CoinGecko
    INSERT INTO dwh.dim_crypto (symbol, name, coingecko_id)
    SELECT DISTINCT ON (upper(symbol))
        upper(symbol),
        name,
        coin_id
    FROM ods.cg_markets
    WHERE symbol IS NOT NULL AND symbol <> ''
    ORDER BY upper(symbol), ingested_at DESC
    ON CONFLICT (symbol) DO UPDATE
        SET name = COALESCE(EXCLUDED.name, dwh.dim_crypto.name),
            coingecko_id = COALESCE(EXCLUDED.coingecko_id, dwh.dim_crypto.coingecko_id);

    -- Merge CMC ids on symbol
    INSERT INTO dwh.dim_crypto (symbol, name, cmc_id)
    SELECT DISTINCT ON (upper(symbol))
        upper(symbol),
        name,
        cmc_id
    FROM ods.cmc_listings
    WHERE symbol IS NOT NULL AND symbol <> ''
    ORDER BY upper(symbol), ingested_at DESC
    ON CONFLICT (symbol) DO UPDATE
        SET name = COALESCE(EXCLUDED.name, dwh.dim_crypto.name),
            cmc_id = COALESCE(EXCLUDED.cmc_id, dwh.dim_crypto.cmc_id);

    -- History rows join dim_crypto via coingecko_id (populated by cg_markets first).

    -- 4) dim_date from all ODS timestamps
    PERFORM dwh._ensure_date(d)
    FROM (
        SELECT DISTINCT ingested_at::date AS d FROM ods.cg_markets
        UNION
        SELECT DISTINCT ingested_at::date FROM ods.cmc_listings
        UNION
        SELECT DISTINCT ingested_at::date FROM ods.cg_global
        UNION
        SELECT DISTINCT ingested_at::date FROM ods.cmc_global
        UNION
        SELECT DISTINCT ingested_at::date FROM ods.fx_rate
        UNION
        SELECT DISTINCT to_timestamp(ts_ms / 1000.0)::date FROM ods.cg_history
    ) s
    WHERE d IS NOT NULL;

    -- 5) fact_market_snapshot — CoinGecko (insert-only for new snapshot_ts)
    INSERT INTO dwh.fact_market_snapshot (
        date_sk, provider_sk, crypto_sk,
        price_usd, price_eur, market_cap_usd, market_cap_eur,
        volume_usd, volume_eur, market_rank,
        pct_change_7d, pct_change_30d, pct_change_90d, pct_change_1y,
        snapshot_ts
    )
    SELECT
        dwh._ensure_date(m.ingested_at::date),
        v_cg_provider,
        c.crypto_sk,
        m.current_price,
        m.current_price * v_fx,
        m.market_cap,
        m.market_cap * v_fx,
        m.total_volume,
        m.total_volume * v_fx,
        m.market_cap_rank,
        m.price_change_pct_7d,
        m.price_change_pct_30d,
        NULL,
        m.price_change_pct_1y,
        m.ingested_at
    FROM ods.cg_markets m
    JOIN dwh.dim_crypto c ON c.coingecko_id = m.coin_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM dwh.fact_market_snapshot f
        WHERE f.provider_sk = v_cg_provider
          AND f.crypto_sk = c.crypto_sk
          AND f.snapshot_ts = m.ingested_at
    );
    GET DIAGNOSTICS v_inserted_snap = ROW_COUNT;

    -- CoinMarketCap snapshots
    INSERT INTO dwh.fact_market_snapshot (
        date_sk, provider_sk, crypto_sk,
        price_usd, price_eur, market_cap_usd, market_cap_eur,
        volume_usd, volume_eur, market_rank,
        pct_change_7d, pct_change_30d, pct_change_90d, pct_change_1y,
        snapshot_ts
    )
    SELECT
        dwh._ensure_date(m.ingested_at::date),
        v_cmc_provider,
        c.crypto_sk,
        m.price_usd,
        m.price_usd * v_fx,
        m.market_cap,
        m.market_cap * v_fx,
        m.volume_24h,
        m.volume_24h * v_fx,
        m.cmc_rank,
        m.percent_change_7d,
        m.percent_change_30d,
        m.percent_change_90d,
        NULL,
        m.ingested_at
    FROM ods.cmc_listings m
    JOIN dwh.dim_crypto c ON c.symbol = upper(m.symbol)
    WHERE NOT EXISTS (
        SELECT 1
        FROM dwh.fact_market_snapshot f
        WHERE f.provider_sk = v_cmc_provider
          AND f.crypto_sk = c.crypto_sk
          AND f.snapshot_ts = m.ingested_at
    );
    GET DIAGNOSTICS v_inserted_snap = ROW_COUNT; -- last statement only; ok for summary

    -- 6) fact_global_market — CoinGecko
    INSERT INTO dwh.fact_global_market (
        date_sk, provider_sk,
        total_market_cap_usd, total_market_cap_eur,
        total_volume_usd, total_volume_eur,
        active_cryptocurrencies, active_exchanges,
        pct_cap_top10, pct_cap_top100,
        snapshot_ts
    )
    SELECT
        dwh._ensure_date(g.ingested_at::date),
        v_cg_provider,
        g.total_market_cap_usd,
        g.total_market_cap_usd * v_fx,
        g.total_volume_usd,
        g.total_volume_usd * v_fx,
        g.active_cryptocurrencies,
        g.markets,
        CASE
            WHEN g.total_market_cap_usd > 0 THEN
                (
                    SELECT COALESCE(SUM(m.market_cap), 0)
                    FROM ods.cg_markets m
                    WHERE m.ingested_at = (
                        SELECT max(ingested_at) FROM ods.cg_markets WHERE ingested_at <= g.ingested_at
                    )
                      AND m.market_cap_rank <= 10
                ) / g.total_market_cap_usd * 100
            ELSE NULL
        END,
        CASE
            WHEN g.total_market_cap_usd > 0 THEN
                (
                    SELECT COALESCE(SUM(m.market_cap), 0)
                    FROM ods.cg_markets m
                    WHERE m.ingested_at = (
                        SELECT max(ingested_at) FROM ods.cg_markets WHERE ingested_at <= g.ingested_at
                    )
                      AND m.market_cap_rank <= 100
                ) / g.total_market_cap_usd * 100
            ELSE NULL
        END,
        g.ingested_at
    FROM ods.cg_global g
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.fact_global_market f
        WHERE f.provider_sk = v_cg_provider AND f.snapshot_ts = g.ingested_at
    );

    -- CoinMarketCap global
    INSERT INTO dwh.fact_global_market (
        date_sk, provider_sk,
        total_market_cap_usd, total_market_cap_eur,
        total_volume_usd, total_volume_eur,
        active_cryptocurrencies, active_exchanges,
        pct_cap_top10, pct_cap_top100,
        snapshot_ts
    )
    SELECT
        dwh._ensure_date(g.ingested_at::date),
        v_cmc_provider,
        g.total_market_cap_usd,
        g.total_market_cap_usd * v_fx,
        g.total_volume_24h_usd,
        g.total_volume_24h_usd * v_fx,
        g.active_cryptocurrencies,
        g.active_exchanges,
        CASE
            WHEN g.total_market_cap_usd > 0 THEN
                (
                    SELECT COALESCE(SUM(m.market_cap), 0)
                    FROM ods.cmc_listings m
                    WHERE m.ingested_at = (
                        SELECT max(ingested_at) FROM ods.cmc_listings WHERE ingested_at <= g.ingested_at
                    )
                      AND m.cmc_rank <= 10
                ) / g.total_market_cap_usd * 100
            ELSE NULL
        END,
        CASE
            WHEN g.total_market_cap_usd > 0 THEN
                (
                    SELECT COALESCE(SUM(m.market_cap), 0)
                    FROM ods.cmc_listings m
                    WHERE m.ingested_at = (
                        SELECT max(ingested_at) FROM ods.cmc_listings WHERE ingested_at <= g.ingested_at
                    )
                      AND m.cmc_rank <= 100
                ) / g.total_market_cap_usd * 100
            ELSE NULL
        END,
        g.ingested_at
    FROM ods.cmc_global g
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.fact_global_market f
        WHERE f.provider_sk = v_cmc_provider AND f.snapshot_ts = g.ingested_at
    );
    GET DIAGNOSTICS v_inserted_glob = ROW_COUNT;

    -- 7) fact_price_history upsert
    INSERT INTO dwh.fact_price_history (
        date_sk, crypto_sk, provider_sk, price_date,
        price_usd, price_eur, market_cap_usd, market_cap_eur,
        volume_usd, volume_eur
    )
    SELECT
        dwh._ensure_date(to_timestamp(h.ts_ms / 1000.0)::date),
        c.crypto_sk,
        v_cg_provider,
        to_timestamp(h.ts_ms / 1000.0)::date,
        h.price_usd,
        h.price_usd * v_fx,
        h.market_cap,
        h.market_cap * v_fx,
        h.total_volume,
        h.total_volume * v_fx
    FROM ods.cg_history h
    JOIN dwh.dim_crypto c ON c.coingecko_id = h.coin_id
    ON CONFLICT (crypto_sk, price_date) DO UPDATE
        SET price_usd = EXCLUDED.price_usd,
            price_eur = EXCLUDED.price_eur,
            market_cap_usd = EXCLUDED.market_cap_usd,
            market_cap_eur = EXCLUDED.market_cap_eur,
            volume_usd = EXCLUDED.volume_usd,
            volume_eur = EXCLUDED.volume_eur,
            date_sk = EXCLUDED.date_sk;
    GET DIAGNOSTICS v_upserted_hist = ROW_COUNT;

    RETURN format(
        'ETL ok | fx=%s | hist_upserts=%s | last_global_inserts=%s',
        v_fx, v_upserted_hist, v_inserted_glob
    );
END;
$$;

-- Analytical views (recreated each time this script is applied)
CREATE OR REPLACE VIEW dwh.v_latest_snapshot AS
SELECT f.*
FROM dwh.fact_market_snapshot f
JOIN (
    SELECT provider_sk, max(snapshot_ts) AS max_ts
    FROM dwh.fact_market_snapshot
    GROUP BY provider_sk
) latest
  ON f.provider_sk = latest.provider_sk
 AND f.snapshot_ts = latest.max_ts;

CREATE OR REPLACE VIEW dwh.v_top10 AS
SELECT
    p.provider_code,
    c.symbol,
    c.name,
    f.market_rank,
    f.price_usd,
    f.price_eur,
    f.market_cap_usd,
    f.volume_usd,
    f.pct_change_7d,
    f.snapshot_ts
FROM dwh.v_latest_snapshot f
JOIN dwh.dim_crypto c ON c.crypto_sk = f.crypto_sk
JOIN dwh.dim_provider p ON p.provider_sk = f.provider_sk
WHERE f.market_rank <= 10
ORDER BY p.provider_code, f.market_rank;

CREATE OR REPLACE VIEW dwh.v_top100 AS
SELECT
    p.provider_code,
    c.symbol,
    c.name,
    f.market_rank,
    f.price_usd,
    f.price_eur,
    f.market_cap_usd,
    f.volume_usd,
    f.pct_change_7d,
    f.snapshot_ts
FROM dwh.v_latest_snapshot f
JOIN dwh.dim_crypto c ON c.crypto_sk = f.crypto_sk
JOIN dwh.dim_provider p ON p.provider_sk = f.provider_sk
WHERE f.market_rank <= 100
ORDER BY p.provider_code, f.market_rank;

CREATE OR REPLACE VIEW dwh.v_provider_gap AS
WITH latest AS (
    SELECT provider_sk, max(snapshot_ts) AS max_ts
    FROM dwh.fact_market_snapshot
    GROUP BY provider_sk
),
cg AS (
    SELECT c.symbol, f.market_cap_usd, f.volume_usd, f.market_rank, f.snapshot_ts
    FROM dwh.fact_market_snapshot f
    JOIN latest l ON l.provider_sk = f.provider_sk AND l.max_ts = f.snapshot_ts
    JOIN dwh.dim_provider p ON p.provider_sk = f.provider_sk AND p.provider_code = 'coingecko'
    JOIN dwh.dim_crypto c ON c.crypto_sk = f.crypto_sk
),
cmc AS (
    SELECT c.symbol, f.market_cap_usd, f.volume_usd, f.market_rank, f.snapshot_ts
    FROM dwh.fact_market_snapshot f
    JOIN latest l ON l.provider_sk = f.provider_sk AND l.max_ts = f.snapshot_ts
    JOIN dwh.dim_provider p ON p.provider_sk = f.provider_sk AND p.provider_code = 'coinmarketcap'
    JOIN dwh.dim_crypto c ON c.crypto_sk = f.crypto_sk
)
SELECT
    COALESCE(cg.symbol, cmc.symbol) AS symbol,
    cg.market_rank AS cg_rank,
    cmc.market_rank AS cmc_rank,
    cg.market_cap_usd AS cg_market_cap,
    cmc.market_cap_usd AS cmc_market_cap,
    CASE
        WHEN cmc.market_cap_usd IS NULL OR cmc.market_cap_usd = 0 THEN NULL
        ELSE (cg.market_cap_usd - cmc.market_cap_usd) / cmc.market_cap_usd * 100
    END AS pct_gap_market_cap,
    cg.volume_usd AS cg_volume,
    cmc.volume_usd AS cmc_volume,
    CASE
        WHEN cmc.volume_usd IS NULL OR cmc.volume_usd = 0 THEN NULL
        ELSE (cg.volume_usd - cmc.volume_usd) / cmc.volume_usd * 100
    END AS pct_gap_volume
FROM cg
FULL OUTER JOIN cmc ON cg.symbol = cmc.symbol
ORDER BY COALESCE(cg.market_rank, cmc.market_rank);

CREATE OR REPLACE VIEW dwh.v_best_performers AS
WITH bounds AS (
    SELECT
        crypto_sk,
        max(price_date) AS max_date,
        max(price_usd) FILTER (WHERE price_date = (SELECT max(price_date) FROM dwh.fact_price_history h2 WHERE h2.crypto_sk = h.crypto_sk)) AS price_now
    FROM dwh.fact_price_history h
    GROUP BY crypto_sk
),
priced AS (
    SELECT
        b.crypto_sk,
        b.max_date,
        (
            SELECT price_usd FROM dwh.fact_price_history x
            WHERE x.crypto_sk = b.crypto_sk AND x.price_date = b.max_date
            LIMIT 1
        ) AS price_now,
        (
            SELECT price_usd FROM dwh.fact_price_history x
            WHERE x.crypto_sk = b.crypto_sk AND x.price_date <= b.max_date - 7
            ORDER BY x.price_date DESC LIMIT 1
        ) AS price_7d,
        (
            SELECT price_usd FROM dwh.fact_price_history x
            WHERE x.crypto_sk = b.crypto_sk AND x.price_date <= b.max_date - 90
            ORDER BY x.price_date DESC LIMIT 1
        ) AS price_3m,
        (
            SELECT price_usd FROM dwh.fact_price_history x
            WHERE x.crypto_sk = b.crypto_sk AND x.price_date <= b.max_date - 180
            ORDER BY x.price_date DESC LIMIT 1
        ) AS price_6m,
        (
            SELECT price_usd FROM dwh.fact_price_history x
            WHERE x.crypto_sk = b.crypto_sk AND x.price_date <= b.max_date - 365
            ORDER BY x.price_date DESC LIMIT 1
        ) AS price_12m,
        (
            SELECT price_usd FROM dwh.fact_price_history x
            WHERE x.crypto_sk = b.crypto_sk AND x.price_date <= b.max_date - 1825
            ORDER BY x.price_date DESC LIMIT 1
        ) AS price_5y
    FROM bounds b
)
SELECT
    c.symbol,
    c.name,
    c.coingecko_id,
    p.price_now,
    CASE WHEN p.price_7d  > 0 THEN (p.price_now - p.price_7d)  / p.price_7d  * 100 END AS perf_7d_pct,
    CASE WHEN p.price_3m  > 0 THEN (p.price_now - p.price_3m)  / p.price_3m  * 100 END AS perf_3m_pct,
    CASE WHEN p.price_6m  > 0 THEN (p.price_now - p.price_6m)  / p.price_6m  * 100 END AS perf_6m_pct,
    CASE WHEN p.price_12m > 0 THEN (p.price_now - p.price_12m) / p.price_12m * 100 END AS perf_12m_pct,
    CASE WHEN p.price_5y  > 0 THEN (p.price_now - p.price_5y)  / p.price_5y  * 100 END AS perf_5y_pct
FROM priced p
JOIN dwh.dim_crypto c ON c.crypto_sk = p.crypto_sk
ORDER BY perf_7d_pct DESC NULLS LAST;

-- Smoke-test call (safe if ODS empty)
-- SELECT dwh.run_etl();
