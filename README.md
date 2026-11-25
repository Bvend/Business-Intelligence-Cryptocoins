# Business-Intelligence-Cryptocoins

Pipeline BI : **CoinGecko + CoinMarketCap → Apache NiFi → PostgreSQL (`ods`) → SQL ETL (`dwh`) → Power BI**.

## Prérequis

- PostgreSQL local + DBeaver
- Apache NiFi **2.12** (comme le TD weather)
- Driver JDBC déjà présent côté NiFi Homebrew  
  `/opt/homebrew/Cellar/nifi/2.12.0/libexec/lib/postgresql-42.7.14.jar`
- Clé CoinMarketCap dans [`.env`](.env) (`API_KEY=...`) — **ne pas la committer**

## 1. Créer la base (DBeaver)

1. Connecte-toi à la base `postgres`, exécute [`sql/01_create_db.sql`](sql/01_create_db.sql).
2. Reconnecte-toi à **`crypto_bi`**, puis dans l’ordre :
   - [`sql/02_ods_tables.sql`](sql/02_ods_tables.sql)
   - [`sql/03_dwh_tables.sql`](sql/03_dwh_tables.sql)
   - [`sql/04_etl_ods_to_dwh.sql`](sql/04_etl_ods_to_dwh.sql)

La fonction `dwh.run_etl()` convertit ODS → DWH (dimensions, faits, vues).

Test manuel :

```sql
SELECT dwh.run_etl();
```

## 2. Importer le flux NiFi

1. NiFi UI → importer [`NiFi/project.json`](NiFi/project.json) (process group `project`).
2. Activer les **Controller Services** :
   - `StandardRestrictedSSLContextService`
   - `DBCPConnectionPool_crypto_bi` → renseigner le **Password** PostgreSQL
   - `JsonTreeReader`, `CSVRecordSetWriter`, `CSVReader`
3. Sur les `InvokeHTTP_CMC_*`, remplacer `REPLACE_WITH_CMC_API_KEY` par la clé de `.env` (propriété dynamique `X-CMC_PRO_API_KEY`).
4. Démarrer la **branche A** (snapshots, timer 5 min) : CG markets/global, CMC listings/global, FX.
5. Vérifier dans DBeaver : `SELECT count(*) FROM ods.cg_markets;`
6. Démarrer la **branche C** `ExecuteSQL_run_etl` (15 min) — ou lancer `SELECT dwh.run_etl();` une fois à la main.
7. **Branche B (historique 5 ans, Top 10)** : processors en `DISABLED`. Les démarrer **une fois** après les snapshots (rate limits CoinGecko ≈ 12 s entre appels). Vérifier `ods.cg_history`, puis relancer l’ETL.

### Branches

| Branche | Rôle | Schedule |
|---------|------|----------|
| A — Snapshots | Top 100 + global CG/CMC + FX → `ods.*` | 5 min |
| B — History | Top 10 `market_chart` 1825 j → `ods.cg_history` | Manuel |
| C — ETL | `SELECT dwh.run_etl();` → `dwh.*` | 15 min |

Pattern processors (comme [`NiFi/td_weather_3.json`](NiFi/td_weather_3.json)) :

`InvokeHTTP → UpdateAttribute(ingested_at) → [SplitJson] → EvaluateJsonPath → AttributesToJSON → ConvertRecord → PutDatabaseRecord`

## 3. Vues utiles pour Power BI

- `dwh.v_top10` / `dwh.v_top100`
- `dwh.v_provider_gap` (écarts cap / volume CG vs CMC)
- `dwh.v_best_performers` (perfs 7j / 3m / 6m / 12m / 5y)
- Tables : `dwh.fact_market_snapshot`, `dwh.fact_global_market`, `dwh.fact_price_history`

## Régénérer le flux NiFi

```bash
python3 scripts/generate_nifi_flow.py
```

## Documentation

- Architecture / plan détaillé : [`docs/architecture-nifi-ods-dwh.md`](docs/architecture-nifi-ods-dwh.md)
- Sujet PDF : [`docs/Projet BI.pdf`](docs/Projet%20BI.pdf)
- Flux weather (TD) : [`NiFi/td_weather_3.json`](NiFi/td_weather_3.json)
