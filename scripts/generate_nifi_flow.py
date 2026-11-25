#!/usr/bin/env python3
"""Generate NiFi/project.json for the crypto BI pipeline (NiFi 2.12 export format)."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

ROOT_ID = "07f478a7-a17f-30b9-9190-dea19c7c5f3d"
ROOT_INSTANCE = "12f0ef90-01a2-1000-665a-21cf922adb75"
BUNDLE_STD = {"group": "org.apache.nifi", "artifact": "nifi-standard-nar", "version": "2.12.0"}
BUNDLE_DBCP = {"group": "org.apache.nifi", "artifact": "nifi-dbcp-service-nar", "version": "2.12.0"}
BUNDLE_RECORD = {
    "group": "org.apache.nifi",
    "artifact": "nifi-record-serialization-services-nar",
    "version": "2.12.0",
}
BUNDLE_SSL = {"group": "org.apache.nifi", "artifact": "nifi-ssl-context-service-nar", "version": "2.12.0"}
BUNDLE_API = {
    "group": "org.apache.nifi",
    "artifact": "nifi-standard-services-api-nar",
    "version": "2.12.0",
}
BUNDLE_JOLT = {"group": "org.apache.nifi", "artifact": "nifi-standard-nar", "version": "2.12.0"}

SSL_ID = str(uuid.uuid4())
DBCP_ID = str(uuid.uuid4())
JSON_READER_ID = str(uuid.uuid4())
CSV_WRITER_ID = str(uuid.uuid4())
CSV_READER_ID = str(uuid.uuid4())

CACERTS = "/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home/lib/security/cacerts"
PG_JAR = "/opt/homebrew/Cellar/nifi/2.12.0/libexec/lib/postgresql-42.7.14.jar"
CMC_KEY = "REPLACE_WITH_CMC_API_KEY"

TOP10 = "\n".join(
    [
        "bitcoin",
        "ethereum",
        "tether",
        "binancecoin",
        "solana",
        "ripple",
        "usd-coin",
        "cardano",
        "dogecoin",
        "tron",
    ]
)

JOLT_HISTORY = json.dumps(
    [
        {
            "operation": "shift",
            "spec": {
                "prices": {"*": {"0": "points[&1].ts_ms", "1": "points[&1].price_usd"}},
                "market_caps": {"*": {"1": "points[&1].market_cap"}},
                "total_volumes": {"*": {"1": "points[&1].total_volume"}},
            },
        }
    ]
)


def uid() -> str:
    return str(uuid.uuid4())


def pdesc(name: str, *, sensitive: bool = False, dynamic: bool = False, cs: bool = False, resource=None):
    d = {
        "name": name,
        "displayName": name,
        "identifiesControllerService": cs,
        "sensitive": sensitive,
        "dynamic": dynamic,
    }
    if resource is not None:
        d["resourceDefinition"] = resource
    return d


def base_processor(
    name: str,
    typ: str,
    properties: dict,
    position: dict,
    *,
    scheduling_period: str = "0 sec",
    auto_terminated: list | None = None,
    scheduled_state: str = "ENABLED",
    comments: str = "",
    bundle=None,
):
    pid = uid()
    iid = uid()
    props = {k: v for k, v in properties.items()}
    descriptors = {
        k: pdesc(k, dynamic=k not in (
            # leave all non-known as possibly dynamic; NiFi accepts extra descriptors
        ), cs="Service" in k or k in (
            "SSL Context Service",
            "Record Reader",
            "Record Writer",
            "Database Connection Pooling Service",
            "Database Dialect Service",
            "Proxy Configuration Service",
            "Request OAuth2 Access Token Provider",
            "Kerberos User Service",
            "Schema Registry",
            "Schema Reference Reader",
            "Schema Reference Writer",
            "Schema Cache",
            "Schema Inference Cache",
        ))
        for k in props
    }
    # Fix CS detection properly
    cs_keys = {
        "SSL Context Service",
        "Record Reader",
        "Record Writer",
        "Database Connection Pooling Service",
        "Database Dialect Service",
        "Proxy Configuration Service",
        "Request OAuth2 Access Token Provider",
    }
    descriptors = {k: pdesc(k, cs=k in cs_keys, dynamic=False) for k in props}
    for k in props:
        # Dynamic: JSONPath attrs, UpdateAttribute fields, HTTP headers (X-*), ExtractText groups
        if (
            k[0].islower()
            or k.startswith("Header:")
            or k.startswith("X-")
            or k in ("coin_id", "ingested_at", "ts_ms", "price_usd", "market_cap", "total_volume")
        ):
            descriptors[k] = pdesc(k, dynamic=True)

    return {
        "identifier": pid,
        "instanceIdentifier": iid,
        "name": name,
        "comments": comments,
        "position": position,
        "type": typ,
        "bundle": bundle or BUNDLE_STD,
        "properties": props,
        "propertyDescriptors": descriptors,
        "style": {},
        "schedulingPeriod": scheduling_period,
        "schedulingStrategy": "TIMER_DRIVEN",
        "executionNode": "ALL",
        "penaltyDuration": "30 sec",
        "yieldDuration": "1 sec",
        "bulletinLevel": "WARN",
        "runDurationMillis": 0,
        "concurrentlySchedulableTaskCount": 1,
        "autoTerminatedRelationships": auto_terminated or [],
        "scheduledState": scheduled_state,
        "retryCount": 10,
        "retriedRelationships": [],
        "backoffMechanism": "PENALIZE_FLOWFILE",
        "maxBackoffPeriod": "10 mins",
        "componentType": "PROCESSOR",
        "groupIdentifier": ROOT_ID,
    }, pid, iid


def connection(source, dest, relationship: str, z: int = 0):
    return {
        "identifier": uid(),
        "instanceIdentifier": uid(),
        "name": "",
        "source": {
            "id": source["identifier"],
            "type": "PROCESSOR",
            "groupId": ROOT_ID,
            "name": source["name"],
            "comments": source.get("comments", ""),
            "instanceIdentifier": source["instanceIdentifier"],
        },
        "destination": {
            "id": dest["identifier"],
            "type": "PROCESSOR",
            "groupId": ROOT_ID,
            "name": dest["name"],
            "comments": dest.get("comments", ""),
            "instanceIdentifier": dest["instanceIdentifier"],
        },
        "labelIndex": 0,
        "zIndex": z,
        "selectedRelationships": [relationship],
        "backPressureObjectThreshold": 10000,
        "backPressureDataSizeThreshold": "1 GB",
        "flowFileExpiration": "0 sec",
        "prioritizers": [],
        "bends": [],
        "loadBalanceStrategy": "DO_NOT_LOAD_BALANCE",
        "partitioningAttribute": "",
        "loadBalanceCompression": "DO_NOT_COMPRESS",
        "componentType": "CONNECTION",
        "groupIdentifier": ROOT_ID,
    }


def invoke_http(name: str, url: str, position: dict, *, schedule: str, headers: dict | None = None, state="ENABLED"):
    props = {
        "Request Content-Encoding": "DISABLED",
        "Request Multipart Form-Data Filename Enabled": "true",
        "Request Chunked Transfer-Encoding Enabled": "false",
        "HTTP/2 Disabled": "False",
        "Connection Timeout": "5 secs",
        "Response Cookie Strategy": "DISABLED",
        "Socket Read Timeout": "60 secs",
        "Socket Idle Connections": "5",
        "Request Body Enabled": "true",
        "HTTP URL": url,
        "Socket Idle Timeout": "5 mins",
        "Response Redirects Enabled": "True",
        "Socket Write Timeout": "15 secs",
        "Response FlowFile Naming Strategy": "RANDOM",
        "Response Cache Enabled": "false",
        "Request Date Header Enabled": "True",
        "Request Failure Penalization Enabled": "false",
        "Response Body Attribute Size": "256",
        "SSL Context Service": SSL_ID,
        "Response Generation Required": "false",
        "Response Header Request Attributes Enabled": "false",
        "HTTP Method": "GET",
        "Request Content-Type": "${mime.type}",
        "Request Digest Authentication Enabled": "false",
        "Response Cache Size": "10MB",
        "Response Body Ignored": "false",
    }
    if headers:
        # InvokeHTTP dynamic properties are sent as HTTP headers (name = header name)
        for hk, hv in headers.items():
            props[hk] = hv
    return base_processor(
        name,
        "org.apache.nifi.processors.standard.InvokeHTTP",
        props,
        position,
        scheduling_period=schedule,
        auto_terminated=["No Retry", "Retry", "Original", "Failure"],
        scheduled_state=state,
        comments=name,
    )


def update_attr(name: str, position: dict, attrs: dict):
    props = {
        "Store State": "Do not store state",
        "canonical-name-lookup-cache-size": "100",
        **attrs,
    }
    return base_processor(
        name,
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        props,
        position,
        auto_terminated=[],
        bundle={"group": "org.apache.nifi", "artifact": "nifi-update-attribute-nar", "version": "2.12.0"},
    )


def split_json(name: str, position: dict, expr: str = "$"):
    return base_processor(
        name,
        "org.apache.nifi.processors.standard.SplitJson",
        {
            "JsonPath Expression": expr,
            "Null Value Representation Strategy": "empty string",
            "Max String Length": "20 MB",
        },
        position,
        auto_terminated=["failure", "original"],
    )


def evaluate_json(name: str, position: dict, paths: dict):
    props = {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "Path Not Found Behavior": "ignore",
        "Null Value Representation": "empty string",
        "Max String Length": "20 MB",
        **paths,
    }
    return base_processor(
        name,
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        props,
        position,
        auto_terminated=["failure", "unmatched"],
    )


def attributes_to_json(name: str, position: dict, attrs: str):
    return base_processor(
        name,
        "org.apache.nifi.processors.standard.AttributesToJSON",
        {
            "Destination": "flowfile-content",
            "Null Value": "false",
            "Attributes List": attrs,
            "Pretty Print": "false",
            "Include Core Attributes": "false",
            "JSON Handling Strategy": "ESCAPED",
        },
        position,
        auto_terminated=["failure"],
    )


def convert_record(name: str, position: dict):
    return base_processor(
        name,
        "org.apache.nifi.processors.standard.ConvertRecord",
        {
            "Include Zero Record FlowFiles": "true",
            "Record Writer": CSV_WRITER_ID,
            "Record Reader": JSON_READER_ID,
        },
        position,
        auto_terminated=["failure"],
    )


def put_db(name: str, position: dict, table: str, *, statement="INSERT", update_keys=None):
    props = {
        "Table Name": table,
        "Quote Column Identifiers": "false",
        "Max Wait Time": "0 seconds",
        "Database Connection Pooling Service": DBCP_ID,
        "Record Reader": CSV_READER_ID,
        "Quote Table Identifiers": "false",
        "Schema Name": "ods",
        "Database Session AutoCommit": "false",
        "Unmatched Field Behavior": "Ignore Unmatched Fields",
        "Allow Multiple SQL Statements": "false",
        "Rollback On Failure": "false",
        "Column Name Translation Strategy": "REMOVE_UNDERSCORE",
        "Binary String Format": "UTF-8",
        "Unmatched Column Behavior": "Ignore Unmatched Columns",
        "Database Type": "PostgreSQL",
        "Translate Field Names": "true",
        "Statement Type": statement,
        "Table Schema Cache Size": "100",
        "Maximum Batch Size": "100",
    }
    if update_keys:
        props["Update Keys"] = update_keys
    return base_processor(
        name,
        "org.apache.nifi.processors.standard.PutDatabaseRecord",
        props,
        position,
        auto_terminated=["success", "failure", "retry"],
        comments=f"Insert into ods.{table}",
    )


def chain_flat(processors_rels):
    """processors_rels: list of (proc, out_rel) ending with last that has no next."""
    conns = []
    for i in range(len(processors_rels) - 1):
        src, rel = processors_rels[i][0], processors_rels[i][1]
        dst = processors_rels[i + 1][0]
        conns.append(connection(src, dst, rel, z=i))
    return conns


def make_snapshot_branch(y: float, label: str, url: str, schedule: str, split_expr, paths: dict, attr_list: str, table: str, headers=None, state="ENABLED"):
    procs = []
    conns = []
    x0 = 0.0
    inv, *_ = invoke_http(f"InvokeHTTP_{label}", url, {"x": x0, "y": y}, schedule=schedule, headers=headers, state=state)
    procs.append(inv)
    ua, *_ = update_attr(
        f"UpdateAttribute_{label}_ts",
        {"x": x0 + 400, "y": y},
        {"ingested_at": "${now():format('yyyy-MM-dd HH:mm:ss')}"},
    )
    procs.append(ua)
    conns.append(connection(inv, ua, "Response"))

    prev = ua
    prev_rel = "success"
    if split_expr is not None:
        sp, *_ = split_json(f"SplitJson_{label}", {"x": x0 + 800, "y": y}, split_expr)
        procs.append(sp)
        conns.append(connection(prev, sp, prev_rel))
        prev, prev_rel = sp, "split"

    ej, *_ = evaluate_json(f"EvaluateJsonPath_{label}", {"x": x0 + 1200, "y": y}, paths)
    procs.append(ej)
    conns.append(connection(prev, ej, prev_rel))

    a2j, *_ = attributes_to_json(f"AttributesToJSON_{label}", {"x": x0 + 1600, "y": y}, attr_list)
    procs.append(a2j)
    conns.append(connection(ej, a2j, "matched"))

    cr, *_ = convert_record(f"ConvertRecord_{label}", {"x": x0 + 2000, "y": y})
    procs.append(cr)
    conns.append(connection(a2j, cr, "success"))

    put, *_ = put_db(f"PutDatabaseRecord_{label}", {"x": x0 + 2400, "y": y}, table)
    procs.append(put)
    conns.append(connection(cr, put, "success"))

    return procs, conns


def main():
    processors = []
    connections = []

    # --- Branch A1: CoinGecko markets ---
    p, c = make_snapshot_branch(
        y=0,
        label="CG_markets",
        url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&price_change_percentage=7d,30d,1y",
        schedule="5 min",
        split_expr="$",
        paths={
            "coin_id": "$.id",
            "symbol": "$.symbol",
            "name": "$.name",
            "current_price": "$.current_price",
            "market_cap": "$.market_cap",
            "total_volume": "$.total_volume",
            "market_cap_rank": "$.market_cap_rank",
            "price_change_pct_7d": "$.price_change_percentage_7d_in_currency",
            "price_change_pct_30d": "$.price_change_percentage_30d_in_currency",
            "price_change_pct_1y": "$.price_change_percentage_1y_in_currency",
            "last_updated": "$.last_updated",
        },
        attr_list="coin_id,symbol,name,current_price,market_cap,total_volume,market_cap_rank,price_change_pct_7d,price_change_pct_30d,price_change_pct_1y,last_updated,ingested_at",
        table="cg_markets",
    )
    processors += p
    connections += c

    # --- A2: CG global ---
    p, c = make_snapshot_branch(
        y=280,
        label="CG_global",
        url="https://api.coingecko.com/api/v3/global",
        schedule="5 min",
        split_expr=None,
        paths={
            "active_cryptocurrencies": "$.data.active_cryptocurrencies",
            "markets": "$.data.markets",
            "total_market_cap_usd": "$.data.total_market_cap.usd",
            "total_volume_usd": "$.data.total_volume.usd",
            "market_cap_pct_btc": "$.data.market_cap_percentage.btc",
        },
        attr_list="active_cryptocurrencies,markets,total_market_cap_usd,total_volume_usd,market_cap_pct_btc,ingested_at",
        table="cg_global",
    )
    processors += p
    connections += c

    # --- A3: CMC listings ---
    p, c = make_snapshot_branch(
        y=560,
        label="CMC_listings",
        url="https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=100&convert=USD",
        schedule="5 min",
        split_expr="$.data",
        paths={
            "cmc_id": "$.id",
            "symbol": "$.symbol",
            "name": "$.name",
            "cmc_rank": "$.cmc_rank",
            "price_usd": "$.quote.USD.price",
            "market_cap": "$.quote.USD.market_cap",
            "volume_24h": "$.quote.USD.volume_24h",
            "percent_change_7d": "$.quote.USD.percent_change_7d",
            "percent_change_30d": "$.quote.USD.percent_change_30d",
            "percent_change_90d": "$.quote.USD.percent_change_90d",
            "last_updated": "$.quote.USD.last_updated",
        },
        attr_list="cmc_id,symbol,name,cmc_rank,price_usd,market_cap,volume_24h,percent_change_7d,percent_change_30d,percent_change_90d,last_updated,ingested_at",
        table="cmc_listings",
        headers={"X-CMC_PRO_API_KEY": CMC_KEY},
    )
    processors += p
    connections += c

    # --- A4: CMC global ---
    p, c = make_snapshot_branch(
        y=840,
        label="CMC_global",
        url="https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest",
        schedule="5 min",
        split_expr=None,
        paths={
            "active_cryptocurrencies": "$.data.active_cryptocurrencies",
            "active_exchanges": "$.data.active_exchanges",
            "total_market_cap_usd": "$.data.quote.USD.total_market_cap",
            "total_volume_24h_usd": "$.data.quote.USD.total_volume_24h",
        },
        attr_list="active_cryptocurrencies,active_exchanges,total_market_cap_usd,total_volume_24h_usd,ingested_at",
        table="cmc_global",
        headers={"X-CMC_PRO_API_KEY": CMC_KEY},
    )
    processors += p
    connections += c

    # --- A5: FX ---
    p, c = make_snapshot_branch(
        y=1120,
        label="FX",
        url="https://api.coingecko.com/api/v3/exchange_rates",
        schedule="5 min",
        split_expr=None,
        paths={"usd_eur": "$.rates.eur.value"},
        attr_list="usd_eur,ingested_at",
        table="fx_rate",
    )
    processors += p
    connections += c

    # --- Branch B: History Top 10 (DISABLED by default) ---
    y = 1400
    gen, *_ = base_processor(
        "GenerateFlowFile_Top10",
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        {
            "File Size": "0B",
            "Batch Size": "1",
            "Data Format": "Text",
            "Unique FlowFiles": "false",
            "Custom Text": TOP10,
            "Mime Type": "text/plain",
        },
        {"x": 0, "y": y},
        scheduling_period="1 day",
        auto_terminated=[],
        scheduled_state="DISABLED",
        comments="Start manually once: Top 10 CoinGecko ids for 5y history",
    )
    processors.append(gen)

    split_txt, *_ = base_processor(
        "SplitText_coin_ids",
        "org.apache.nifi.processors.standard.SplitText",
        {
            "Line Split Mode": "Split",
            "Header Line Count": "0",
            "Remove Trailing Newlines": "true",
        },
        {"x": 400, "y": y},
        auto_terminated=["original", "failure"],
        scheduled_state="DISABLED",
    )
    processors.append(split_txt)
    connections.append(connection(gen, split_txt, "success"))

    extract, *_ = base_processor(
        "ExtractText_coin_id",
        "org.apache.nifi.processors.standard.ExtractText",
        {
            "Character Set": "UTF-8",
            "Maximum Capture Group Length": "1024",
            "Enable Canonical Equivalence": "false",
            "Enable Case-insensitive Matching": "false",
            "Permit Overlapping Capture Groups": "false",
            "Enable Multiline Mode": "false",
            "Enable Dotall Mode": "true",
            "Enable Unicode Support": "false",
            "Enable Unix Lines Mode": "false",
            "Include Capture Group 0": "true",
            "Extract to multiple attributes": "false",
            "coin_id": "(?<coin_id>[^\\r\\n]+)",
        },
        {"x": 800, "y": y},
        auto_terminated=["unmatched"],
        scheduled_state="DISABLED",
    )
    processors.append(extract)
    connections.append(connection(split_txt, extract, "splits"))

    inv_h, *_ = invoke_http(
        "InvokeHTTP_CG_history",
        "https://api.coingecko.com/api/v3/coins/${coin_id}/market_chart?vs_currency=usd&days=1825&interval=daily",
        {"x": 1200, "y": y},
        schedule="0 sec",
        state="DISABLED",
    )
    # Slow down to respect rate limits
    inv_h["yieldDuration"] = "12 sec"
    inv_h["penaltyDuration"] = "12 sec"
    processors.append(inv_h)
    connections.append(connection(extract, inv_h, "matched"))

    jolt, *_ = base_processor(
        "JoltTransformJSON_history",
        "org.apache.nifi.processors.standard.JoltTransformJSON",
        {
            "Jolt Transformation DSL": "jolt-transform-chain",
            "Jolt Specification": JOLT_HISTORY,
            "Transform Cache Size": "1",
            "Pretty Print": "false",
            "Max String Length": "50 MB",
        },
        {"x": 1600, "y": y},
        auto_terminated=["failure"],
        scheduled_state="DISABLED",
    )
    processors.append(jolt)
    connections.append(connection(inv_h, jolt, "Response"))

    split_pts, *_ = split_json("SplitJson_history_points", {"x": 2000, "y": y}, "$.points")
    split_pts["scheduledState"] = "DISABLED"
    processors.append(split_pts)
    connections.append(connection(jolt, split_pts, "success"))

    ej_h, *_ = evaluate_json(
        "EvaluateJsonPath_history",
        {"x": 2400, "y": y},
        {
            "ts_ms": "$.ts_ms",
            "price_usd": "$.price_usd",
            "market_cap": "$.market_cap",
            "total_volume": "$.total_volume",
        },
    )
    ej_h["scheduledState"] = "DISABLED"
    processors.append(ej_h)
    connections.append(connection(split_pts, ej_h, "split"))

    a2j_h, *_ = attributes_to_json(
        "AttributesToJSON_history",
        {"x": 2800, "y": y},
        "coin_id,ts_ms,price_usd,market_cap,total_volume",
    )
    a2j_h["scheduledState"] = "DISABLED"
    processors.append(a2j_h)
    connections.append(connection(ej_h, a2j_h, "matched"))

    cr_h, *_ = convert_record("ConvertRecord_history", {"x": 3200, "y": y})
    cr_h["scheduledState"] = "DISABLED"
    processors.append(cr_h)
    connections.append(connection(a2j_h, cr_h, "success"))

    put_h, *_ = put_db(
        "PutDatabaseRecord_history",
        {"x": 3600, "y": y},
        "cg_history",
        statement="UPSERT",
        update_keys="coin_id,ts_ms",
    )
    put_h["scheduledState"] = "DISABLED"
    processors.append(put_h)
    connections.append(connection(cr_h, put_h, "success"))

    # --- Branch C: ETL ExecuteSQL ---
    y = 1680
    etl, *_ = base_processor(
        "ExecuteSQL_run_etl",
        "org.apache.nifi.processors.standard.ExecuteSQL",
        {
            "Database Connection Pooling Service": DBCP_ID,
            "SQL select query": "SELECT dwh.run_etl();",
            "Max Wait Time": "0 seconds",
            "Normalize Table/Column Names": "false",
            "Use Avro Logical Types": "false",
            "Default Decimal Precision": "10",
            "Default Decimal Scale": "0",
            "esql-max-rows": "0",
            "esql-output-batch-size": "0",
            "esql-fetch-size": "0",
        },
        {"x": 0, "y": y},
        scheduling_period="15 min",
        auto_terminated=["success", "failure"],
        comments="Automates ODS → DWH conversion via dwh.run_etl()",
    )
    # Fix property name - NiFi uses "dbcp-service" in some versions
    etl["properties"]["Database Connection Pooling Service"] = DBCP_ID
    processors.append(etl)

    # Labels
    labels = [
        {
            "identifier": uid(),
            "instanceIdentifier": uid(),
            "label": "A — Snapshots (5 min)\nCoinGecko + CoinMarketCap + FX → ods.*",
            "position": {"x": -320, "y": -40},
            "width": 280,
            "height": 80,
            "style": {"font-size": "14px"},
            "zIndex": 0,
            "componentType": "LABEL",
            "groupIdentifier": ROOT_ID,
        },
        {
            "identifier": uid(),
            "instanceIdentifier": uid(),
            "label": "B — History 5y Top10 (DISABLED)\nStart manually once — rate limits",
            "position": {"x": -320, "y": 1360},
            "width": 280,
            "height": 80,
            "style": {"font-size": "14px"},
            "zIndex": 0,
            "componentType": "LABEL",
            "groupIdentifier": ROOT_ID,
        },
        {
            "identifier": uid(),
            "instanceIdentifier": uid(),
            "label": "C — ETL ODS→DWH (15 min)\nExecuteSQL: SELECT dwh.run_etl();",
            "position": {"x": -320, "y": 1640},
            "width": 280,
            "height": 80,
            "style": {"font-size": "14px"},
            "zIndex": 0,
            "componentType": "LABEL",
            "groupIdentifier": ROOT_ID,
        },
    ]

    controller_services = [
        {
            "identifier": SSL_ID,
            "instanceIdentifier": uid(),
            "name": "StandardRestrictedSSLContextService",
            "comments": "",
            "type": "org.apache.nifi.ssl.StandardRestrictedSSLContextService",
            "bundle": BUNDLE_SSL,
            "properties": {
                "Truststore Type": "JKS",
                "Truststore Filename": CACERTS,
                "TLS Protocol": "TLS",
            },
            "propertyDescriptors": {
                "Truststore Type": pdesc("Truststore Type"),
                "Truststore Filename": pdesc(
                    "Truststore Filename",
                    resource={"cardinality": "SINGLE", "resourceTypes": ["FILE"]},
                ),
                "TLS Protocol": pdesc("TLS Protocol"),
            },
            "controllerServiceApis": [
                {"type": "org.apache.nifi.ssl.RestrictedSSLContextService", "bundle": BUNDLE_API},
                {"type": "org.apache.nifi.ssl.SSLContextService", "bundle": BUNDLE_API},
                {"type": "org.apache.nifi.ssl.SSLContextProvider", "bundle": BUNDLE_API},
            ],
            "scheduledState": "DISABLED",
            "bulletinLevel": "WARN",
            "componentType": "CONTROLLER_SERVICE",
            "groupIdentifier": ROOT_ID,
        },
        {
            "identifier": DBCP_ID,
            "instanceIdentifier": uid(),
            "name": "DBCPConnectionPool_crypto_bi",
            "comments": "Set Password after import. DB crypto_bi / user bernardo",
            "type": "org.apache.nifi.dbcp.DBCPConnectionPool",
            "bundle": BUNDLE_DBCP,
            "properties": {
                "Maximum Idle Connections": "8",
                "Database Driver Locations": PG_JAR,
                "Max Wait Time": "500 millis",
                "Database Driver Class Name": "org.postgresql.Driver",
                "Minimum Idle Connections": "0",
                "Max Total Connections": "8",
                "Validation Query": "SELECT 1",
                "Database Connection URL": "jdbc:postgresql://localhost:5432/crypto_bi",
                "Password Source": "PASSWORD",
                "Time Between Eviction Runs": "-1",
                "Database User": "bernardo",
                "Soft Minimum Evictable Idle Time": "-1",
                "Maximum Connection Lifetime": "-1",
                "Minimum Evictable Idle Time": "30 mins",
            },
            "propertyDescriptors": {
                "Database Driver Locations": pdesc(
                    "Database Driver Locations",
                    resource={"cardinality": "MULTIPLE", "resourceTypes": ["URL", "FILE", "DIRECTORY"]},
                ),
                "Database Connection URL": pdesc("Database Connection URL"),
                "Database User": pdesc("Database User"),
                "Database Driver Class Name": pdesc("Database Driver Class Name"),
                "Password": pdesc("Password", sensitive=True),
            },
            "controllerServiceApis": [
                {"type": "org.apache.nifi.dbcp.DBCPService", "bundle": BUNDLE_API},
            ],
            "scheduledState": "DISABLED",
            "bulletinLevel": "WARN",
            "componentType": "CONTROLLER_SERVICE",
            "groupIdentifier": ROOT_ID,
        },
        {
            "identifier": JSON_READER_ID,
            "instanceIdentifier": uid(),
            "name": "JsonTreeReader",
            "comments": "",
            "type": "org.apache.nifi.json.JsonTreeReader",
            "bundle": BUNDLE_RECORD,
            "properties": {
                "Max String Length": "20 MB",
                "Starting Field Strategy": "ROOT_NODE",
                "Schema Access Strategy": "infer-schema",
                "Parsing Strategy": "STANDARD",
            },
            "propertyDescriptors": {
                "Schema Access Strategy": pdesc("Schema Access Strategy"),
                "Parsing Strategy": pdesc("Parsing Strategy"),
            },
            "controllerServiceApis": [
                {"type": "org.apache.nifi.serialization.RecordReaderFactory", "bundle": BUNDLE_API},
            ],
            "scheduledState": "DISABLED",
            "bulletinLevel": "WARN",
            "componentType": "CONTROLLER_SERVICE",
            "groupIdentifier": ROOT_ID,
        },
        {
            "identifier": CSV_WRITER_ID,
            "instanceIdentifier": uid(),
            "name": "CSVRecordSetWriter",
            "comments": "",
            "type": "org.apache.nifi.csv.CSVRecordSetWriter",
            "bundle": BUNDLE_RECORD,
            "properties": {
                "CSV Format": "custom",
                "Include Header Line": "true",
                "Schema Access Strategy": "inherit-record-schema",
                "CSV Writer": "commons-csv",
                "Escape Character": "\\",
                "Trim Fields": "true",
                "Quote Character": "\"",
                "Value Separator": ",",
                "Record Separator": "\\n",
                "Schema Write Strategy": "no-schema",
                "Character Set": "UTF-8",
                "Quote Mode": "MINIMAL",
                "Include Trailing Delimiter": "false",
            },
            "propertyDescriptors": {"CSV Format": pdesc("CSV Format")},
            "controllerServiceApis": [
                {"type": "org.apache.nifi.serialization.RecordSetWriterFactory", "bundle": BUNDLE_API},
            ],
            "scheduledState": "DISABLED",
            "bulletinLevel": "WARN",
            "componentType": "CONTROLLER_SERVICE",
            "groupIdentifier": ROOT_ID,
        },
        {
            "identifier": CSV_READER_ID,
            "instanceIdentifier": uid(),
            "name": "CSVReader",
            "comments": "",
            "type": "org.apache.nifi.csv.CSVReader",
            "bundle": BUNDLE_RECORD,
            "properties": {
                "CSV Format": "custom",
                "Quote Character": "\"",
                "Value Separator": ",",
                "Treat First Line as Header": "true",
                "Schema Access Strategy": "infer-schema",
                "Ignore CSV Header Column Names": "false",
                "Record Separator": "\\n",
                "Timestamp Format": "yyyy-MM-dd HH:mm:ss",
                "Escape Character": "\\",
                "Trim Double Quote": "true",
                "Trim Fields": "true",
                "Allow Duplicate Header Names": "true",
                "CSV Parser": "commons-csv",
                "Character Set": "UTF-8",
            },
            "propertyDescriptors": {
                "Treat First Line as Header": pdesc("Treat First Line as Header"),
                "Timestamp Format": pdesc("Timestamp Format"),
            },
            "controllerServiceApis": [
                {"type": "org.apache.nifi.serialization.RecordReaderFactory", "bundle": BUNDLE_API},
            ],
            "scheduledState": "DISABLED",
            "bulletinLevel": "WARN",
            "componentType": "CONTROLLER_SERVICE",
            "groupIdentifier": ROOT_ID,
        },
    ]

    flow = {
        "flowContents": {
            "identifier": ROOT_ID,
            "instanceIdentifier": ROOT_INSTANCE,
            "name": "project",
            "comments": (
                "Crypto BI: A) CG/CMC snapshots+FX → ods (5min) | "
                "B) CG history Top10 5y (manual) | "
                "C) ExecuteSQL dwh.run_etl() (15min). "
                "Replace CMC API key + set DB password after import."
            ),
            "position": {"x": 1192.0, "y": -520.0},
            "processGroups": [],
            "remoteProcessGroups": [],
            "processors": processors,
            "inputPorts": [],
            "outputPorts": [],
            "connections": connections,
            "labels": labels,
            "funnels": [],
            "controllerServices": controller_services,
            "defaultFlowFileExpiration": "0 sec",
            "defaultBackPressureObjectThreshold": 10000,
            "defaultBackPressureDataSizeThreshold": "1 GB",
            "scheduledState": "ENABLED",
            "executionEngine": "INHERITED",
            "maxConcurrentTasks": 1,
            "statelessFlowTimeout": "1 min",
            "flowFileConcurrency": "UNBOUNDED",
            "flowFileOutboundPolicy": "STREAM_WHEN_AVAILABLE",
            "componentType": "PROCESS_GROUP",
        },
        "externalControllerServices": {},
        "parameterContexts": {},
        "flowEncodingVersion": "1.0",
        "parameterProviders": {},
        "latest": False,
    }

    out = Path(__file__).resolve().parents[1] / "NiFi" / "project.json"
    out.write_text(json.dumps(flow, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {out}")
    print(f"Processors: {len(processors)} | Connections: {len(connections)} | CS: {len(controller_services)}")


if __name__ == "__main__":
    main()
