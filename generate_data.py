#!/usr/bin/env python3
"""
Pipeline Doctor - Data Pipeline Schema-Change Fault Generator

Generates realistic ETL / data-pipeline observability data with a
SCHEMA-CHANGE failure scenario, and pushes it to Splunk via HEC.

Domain (e-commerce data pipeline):

  AFFECTED LINEAGE (inventory):
    source.orders_db.inventory --[ingest_inventory]--> raw.inventory_snapshot
      --[transform_inventory]--> mart.product_availability
      --[refresh_inventory_dashboard]--> dashboard.inventory_health

  HEALTHY LINEAGE (revenue, proves blast radius is confined):
    source.orders_db.orders --[ingest_orders]--> raw.orders
      --[transform_revenue]--> mart.daily_revenue --> dashboard.revenue_overview

Root cause:
  Source table `inventory` renames column stock_count -> available_qty
  (schema v2.2.0 -> v2.3.1). The ingestion job still SELECTs stock_count,
  so raw.inventory_snapshot.stock_count goes 0% -> 100% NULL. Jobs keep
  "succeeding" (status=success, normal rows, normal duration, normal CPU),
  so this is NOT a resource/volume problem -- it is a broken schema contract.
  Nulls propagate downstream to mart.product_availability and the dashboard.

Sourcetypes:
  pipeline:job_log         job runs: status, rows_in/out, duration_ms, worker_cpu_pct
  pipeline:schema_registry schema version / column change events
  pipeline:data_quality    DQ checks: null_rate / row_count / freshness
  pipeline:lineage         lineage edges (upstream_table -> downstream_table)
  pipeline:alerts          DQ and business alerts
"""

import os
import json
import random
import time
import argparse
import uuid
from datetime import datetime, timedelta

import requests
import urllib3

try:
    from dotenv import load_dotenv
    load_dotenv()  # load .env from the current directory if present
except ImportError:
    pass  # dotenv optional; env vars / CLI args still work

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# CONFIGURATION  (pass --token / --host on the CLI; do NOT hardcode secrets)
# ============================================================
SPLUNK_HOST = os.environ.get("SPLUNK_HEC_HOST", "")
SPLUNK_HEC_PORT = 8088
SPLUNK_HEC_TOKEN = ""          # set via --token or PIPELINE_HEC_TOKEN env var
SPLUNK_HEC_URL = f"https://{SPLUNK_HOST}:{SPLUNK_HEC_PORT}/services/collector/event"

INDEX = "main"

# ============================================================
# PIPELINE TOPOLOGY
# ============================================================
# Each job: stage, source table, target table, the worker host it runs on.
JOBS = {
    "ingest_inventory":   {"stage": "ingest",    "src": "source.orders_db.inventory", "dst": "raw.inventory_snapshot", "host": "etl-worker-01"},
    "ingest_orders":      {"stage": "ingest",    "src": "source.orders_db.orders",    "dst": "raw.orders",            "host": "etl-worker-01"},
    "transform_inventory":{"stage": "transform", "src": "raw.inventory_snapshot",      "dst": "mart.product_availability", "host": "etl-worker-02"},
    "transform_revenue":  {"stage": "transform", "src": "raw.orders",                  "dst": "mart.daily_revenue",    "host": "etl-worker-02"},
    "refresh_inventory_dashboard": {"stage": "serve", "src": "mart.product_availability", "dst": "dashboard.inventory_health",  "host": "bi-worker-01"},
    "refresh_revenue_dashboard":   {"stage": "serve", "src": "mart.daily_revenue",        "dst": "dashboard.revenue_overview",  "host": "bi-worker-01"},
}

# Tables we run data-quality checks on, and the columns that matter.
# The "broken" column is stock_count (renamed away at the source).
TABLES = {
    "raw.inventory_snapshot":   {"columns": ["product_id", "warehouse_id", "stock_count"], "chain": "inventory", "base_rows": 12000},
    "mart.product_availability":{"columns": ["product_id", "available"],                   "chain": "inventory", "base_rows": 11800},
    "raw.orders":               {"columns": ["order_id", "user_id", "amount"],             "chain": "revenue",   "base_rows": 8400},
    "mart.daily_revenue":       {"columns": ["day", "revenue"],                            "chain": "revenue",   "base_rows": 30},
}

INVENTORY_JOBS = ["ingest_inventory", "transform_inventory", "refresh_inventory_dashboard"]
REVENUE_JOBS = ["ingest_orders", "transform_revenue", "refresh_revenue_dashboard"]


# ============================================================
# HEC TRANSPORT
# ============================================================
def send_to_splunk(events: list) -> bool:
    headers = {"Authorization": f"Splunk {SPLUNK_HEC_TOKEN}"}
    payload = "".join(json.dumps(e) + "\n" for e in events)
    try:
        resp = requests.post(SPLUNK_HEC_URL, headers=headers, data=payload, verify=False, timeout=10)
        if resp.status_code == 200:
            return True
        print(f"  [!] HEC error: {resp.status_code} - {resp.text}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"  [!] Connection error: {e}")
        return False


def hec(ts: datetime, sourcetype: str, source: str, host: str, event: dict) -> dict:
    event.setdefault("timestamp", ts.isoformat())
    event.setdefault("environment", "production")
    return {
        "time": ts.timestamp(),
        "sourcetype": sourcetype,
        "source": source,
        "host": host,
        "index": INDEX,
        "event": event,
    }


def run_id() -> str:
    return uuid.uuid4().hex[:12]


# ============================================================
# EVENT GENERATORS
# ============================================================
def gen_job_log(ts, job_name, status, rows_in, rows_out, duration_ms,
                level="INFO", message="", worker_cpu=None, error=None):
    job = JOBS[job_name]
    ev = {
        "job_id": run_id(),
        "job_name": job_name,
        "stage": job["stage"],
        "source_table": job["src"],
        "target_table": job["dst"],
        "status": status,
        "rows_in": int(rows_in),
        "rows_out": int(rows_out),
        "duration_ms": int(duration_ms),
        "worker_cpu_pct": round(worker_cpu if worker_cpu is not None else random.gauss(28, 6), 1),
        "worker_mem_pct": round(random.gauss(41, 8), 1),
        "level": level,
        "message": message or f"Job {job_name} {status}",
    }
    if error:
        ev["error_message"] = error
    return hec(ts, "pipeline:job_log", f"airflow/{job_name}", job["host"], ev)


def gen_schema_event(ts, table, schema_version, change_type,
                     old_field=None, new_field=None, ticket=None):
    ev = {
        "table": table,
        "schema_version": schema_version,
        "change_type": change_type,   # baseline | rename_column | add_column | drop_column | type_change
        "changed_by": "platform-data-team",
    }
    if old_field:
        ev["old_field"] = old_field
    if new_field:
        ev["new_field"] = new_field
    if ticket:
        ev["migration_ticket"] = ticket
    return hec(ts, "pipeline:schema_registry", "schema-registry", "schema-registry-01", ev)


def gen_dq_check(ts, table, check_name, value, threshold, status, column=None):
    ev = {
        "table": table,
        "check_name": check_name,     # null_rate | row_count | freshness_lag_min
        "value": round(value, 4),
        "threshold": threshold,
        "status": status,             # pass | fail
    }
    if column:
        ev["column"] = column
    return hec(ts, "pipeline:data_quality", "great_expectations", "dq-runner-01", ev)


def gen_lineage(ts, job_name):
    job = JOBS[job_name]
    ev = {
        "upstream_table": job["src"],
        "downstream_table": job["dst"],
        "produced_by_job": job_name,
    }
    return hec(ts, "pipeline:lineage", "lineage-catalog", "lineage-01", ev)


def gen_alert(ts, target, severity, alert_name, description):
    ev = {
        "target": target,
        "severity": severity,
        "alert_name": alert_name,
        "description": description,
        "status": "firing",
    }
    return hec(ts, "pipeline:alerts", "monitoring/alerts", "monitor-01", ev)


# ============================================================
# PER-TABLE DATA-QUALITY EMITTER
# ============================================================
def emit_dq_for_table(ts, table, null_rate_overrides=None, row_count_override=None, freshness_override=None):
    """Emit null_rate / row_count / freshness checks for a table.

    null_rate_overrides: {column: null_rate} to force a broken column.
    row_count_override: force a specific row count (fails if < base_rows * 0.7).
    freshness_override: force a specific freshness_lag_min (fails if > 20).
    """
    events = []
    meta = TABLES[table]
    overrides = null_rate_overrides or {}

    # null_rate per column
    for col in meta["columns"]:
        nr = overrides.get(col, max(0.0, random.gauss(0.002, 0.001)))
        status = "fail" if nr > 0.05 else "pass"
        events.append(gen_dq_check(ts, table, "null_rate", nr, 0.05, status, column=col))

    # row_count
    threshold_rows = meta["base_rows"] * 0.7
    if row_count_override is not None:
        rows = row_count_override
        rc_status = "fail" if rows < threshold_rows else "pass"
    else:
        rows = meta["base_rows"] * random.gauss(1.0, 0.02)
        rc_status = "pass" if rows > threshold_rows else "fail"
    events.append(gen_dq_check(ts, table, "row_count", rows, threshold_rows, rc_status))

    # freshness
    if freshness_override is not None:
        lag = freshness_override
        f_status = "fail" if lag > 20 else "pass"
    else:
        lag = max(0.0, random.gauss(6, 3))
        f_status = "pass" if lag < 20 else "fail"
    events.append(gen_dq_check(ts, table, "freshness_lag_min", lag, 20, f_status))
    return events


def emit_lineage(ts):
    return [gen_lineage(ts, j) for j in JOBS]


# ============================================================
# PHASE 0: NORMAL
# ============================================================
def phase_normal(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 0] Normal baseline at {cur.isoformat()}")
    while cur < start + timedelta(minutes=minutes):
        # All jobs succeed with normal rows / duration / cpu
        for job_name, job in JOBS.items():
            base = TABLES.get(job["dst"], {}).get("base_rows", 1000)
            rows = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success",
                                      rows_in=rows, rows_out=rows,
                                      duration_ms=random.gauss(45000, 8000),
                                      worker_cpu=random.gauss(28, 6),
                                      message="completed"))
        # DQ all green
        for table in TABLES:
            events.extend(emit_dq_for_table(cur, table))
        # Lineage + occasional schema baseline ping
        events.extend(emit_lineage(cur))
        if random.random() < 0.3:
            events.append(gen_schema_event(cur, "source.orders_db.inventory",
                                           "2.2.0", "baseline"))
        cur += timedelta(seconds=random.randint(20, 40))
    return events


# ============================================================
# PHASE 1: SCHEMA CHANGE PUBLISHED (root cause event)
# ============================================================
def phase_schema_change(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 1] Source schema change published at {cur.isoformat()}")

    # The defining event: stock_count -> available_qty on the source table.
    events.append(gen_schema_event(
        cur, "source.orders_db.inventory", "2.3.1", "rename_column",
        old_field="stock_count", new_field="available_qty", ticket="DATA-1847"))
    events.append(gen_alert(
        cur, "source.orders_db.inventory", "info", "SchemaVersionBump",
        "inventory schema upgraded 2.2.0 -> 2.3.1 (column rename: stock_count -> available_qty, ticket DATA-1847)"))

    # Pipeline is still on the OLD schema; everything still looks fine this phase
    # because the next ingest run hasn't fired yet.
    while cur < start + timedelta(minutes=minutes):
        for job_name in JOBS:
            base = TABLES.get(JOBS[job_name]["dst"], {}).get("base_rows", 1000)
            rows = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success", rows, rows,
                                      random.gauss(45000, 8000)))
        for table in TABLES:
            events.extend(emit_dq_for_table(cur, table))
        events.extend(emit_lineage(cur))
        cur += timedelta(seconds=random.randint(20, 40))
    return events


# ============================================================
# PHASE 2: INGEST PRODUCES NULLS (raw layer breaks)
# ============================================================
def phase_ingest_breaks(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 2] Ingest produces NULL stock_count at {cur.isoformat()}")
    while cur < start + timedelta(minutes=minutes):
        # ingest_inventory: still SELECTs stock_count -> column missing at source.
        # The job does NOT crash (insidious): it "succeeds", normal rows, normal
        # duration, normal cpu -- but logs a WARN and the column is now all NULL.
        rows = TABLES["raw.inventory_snapshot"]["base_rows"] * random.gauss(1.0, 0.02)
        events.append(gen_job_log(
            cur, "ingest_inventory", "success", rows, rows,
            duration_ms=random.gauss(46000, 8000),
            worker_cpu=random.gauss(29, 6),
            level="WARN",
            message="Column 'stock_count' not found in source.orders_db.inventory; "
                    "filling NULL. (source schema now 2.3.1)"))

        # Healthy jobs unaffected
        for job_name in ["ingest_orders", "transform_revenue", "refresh_revenue_dashboard"]:
            base = TABLES.get(JOBS[job_name]["dst"], {}).get("base_rows", 1000)
            r = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success", r, r, random.gauss(45000, 8000)))

        # raw.inventory_snapshot: stock_count null_rate jumps to ~100%, rows still normal
        events.extend(emit_dq_for_table(cur, "raw.inventory_snapshot",
                                        null_rate_overrides={"stock_count": random.gauss(0.99, 0.01)}))
        # other tables still healthy this phase (downstream not refreshed yet)
        for table in ["mart.product_availability", "raw.orders", "mart.daily_revenue"]:
            events.extend(emit_dq_for_table(cur, table))

        events.extend(emit_lineage(cur))
        if random.random() < 0.4:
            events.append(gen_alert(
                cur, "raw.inventory_snapshot", "warning", "NullRateHigh",
                "null_rate for column stock_count = 99% (threshold 5%)"))
        cur += timedelta(seconds=random.randint(20, 40))
    return events


# ============================================================
# PHASE 3: DOWNSTREAM CASCADE (mart + dashboard break)
# ============================================================
def phase_downstream_cascade(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 3] Downstream cascade (mart + dashboard) at {cur.isoformat()}")
    while cur < start + timedelta(minutes=minutes):
        # ingest still NULL
        rows = TABLES["raw.inventory_snapshot"]["base_rows"] * random.gauss(1.0, 0.02)
        events.append(gen_job_log(cur, "ingest_inventory", "success", rows, rows,
                                  random.gauss(46000, 8000), level="WARN",
                                  message="Column 'stock_count' not found in source; filling NULL."))

        # transform_inventory reads all-NULL stock_count -> mart.available all 0.
        # Job 'succeeds' but produces garbage; rows normal.
        rows_m = TABLES["mart.product_availability"]["base_rows"] * random.gauss(1.0, 0.02)
        events.append(gen_job_log(
            cur, "transform_inventory", "success", rows, rows_m,
            duration_ms=random.gauss(38000, 6000), worker_cpu=random.gauss(31, 6),
            level="WARN",
            message="Computed available=0 for all rows: upstream stock_count is NULL. "
                    "Check raw.inventory_snapshot."))

        # dashboard refresh runs but shows everything out of stock
        events.append(gen_job_log(
            cur, "refresh_inventory_dashboard", "success", rows_m, rows_m,
            duration_ms=random.gauss(12000, 3000), level="WARN",
            message="Refreshed dashboard.inventory_health: 100% of SKUs report available=0 (out of stock)."))

        # Healthy revenue chain keeps running clean
        for job_name in ["ingest_orders", "transform_revenue", "refresh_revenue_dashboard"]:
            base = TABLES.get(JOBS[job_name]["dst"], {}).get("base_rows", 1000)
            r = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success", r, r, random.gauss(45000, 8000)))

        # DQ: raw stock_count NULL, mart.available NULL/0 -- but row_count normal everywhere
        events.extend(emit_dq_for_table(cur, "raw.inventory_snapshot",
                                        null_rate_overrides={"stock_count": random.gauss(0.99, 0.01)}))
        events.extend(emit_dq_for_table(cur, "mart.product_availability",
                                        null_rate_overrides={"available": random.gauss(0.97, 0.02)}))
        # revenue chain healthy
        for table in ["raw.orders", "mart.daily_revenue"]:
            events.extend(emit_dq_for_table(cur, table))

        events.extend(emit_lineage(cur))

        # Alerts
        events.append(gen_alert(cur, "mart.product_availability", "critical",
                                "DataQualityFailure",
                                "null_rate for column 'available' = 97% (threshold 5%); row_count normal"))
        events.append(gen_alert(cur, "dashboard.inventory_health", "critical",
                                "BusinessImpact",
                                "Inventory dashboard reports ALL products out of stock; "
                                "merchandising + checkout availability affected"))
        cur += timedelta(seconds=random.randint(15, 30))
    return events


# ============================================================
# PHASE 4: RECOVERY (pipeline patched to new schema)
# ============================================================
def phase_recovery(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 4] Recovery (pipeline patched to 2.3.1) at {cur.isoformat()}")
    events.append(gen_schema_event(
        cur, "raw.inventory_snapshot", "2.3.1", "rename_column",
        old_field="stock_count", new_field="available_qty", ticket="DATA-1851"))
    events.append(gen_alert(cur, "ingest_inventory", "info", "PipelinePatched",
                            "ingest_inventory updated to read available_qty (alias stock_count); backfill running"))

    total = minutes * 60
    while cur < start + timedelta(minutes=minutes):
        pct = (cur - start).total_seconds() / total           # 0 -> 1
        null_rate = max(0.001, 0.99 * (1 - pct))

        rows = TABLES["raw.inventory_snapshot"]["base_rows"] * random.gauss(1.0, 0.02)
        rows_m = TABLES["mart.product_availability"]["base_rows"] * random.gauss(1.0, 0.02)
        lvl = "INFO" if pct > 0.5 else "WARN"
        events.append(gen_job_log(cur, "ingest_inventory", "success", rows, rows,
                                  random.gauss(45000, 8000), level=lvl,
                                  message=f"Backfilling stock_count from available_qty; null_rate={null_rate:.2f}"))
        events.append(gen_job_log(cur, "transform_inventory", "success", rows, rows_m,
                                  random.gauss(38000, 6000), level=lvl,
                                  message="Recomputing availability from patched upstream"))
        events.append(gen_job_log(cur, "refresh_inventory_dashboard", "success", rows_m, rows_m,
                                  random.gauss(12000, 3000)))
        for job_name in ["ingest_orders", "transform_revenue", "refresh_revenue_dashboard"]:
            base = TABLES.get(JOBS[job_name]["dst"], {}).get("base_rows", 1000)
            r = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success", r, r, random.gauss(45000, 8000)))

        events.extend(emit_dq_for_table(cur, "raw.inventory_snapshot",
                                        null_rate_overrides={"stock_count": null_rate}))
        events.extend(emit_dq_for_table(cur, "mart.product_availability",
                                        null_rate_overrides={"available": max(0.001, null_rate * 0.9)}))
        for table in ["raw.orders", "mart.daily_revenue"]:
            events.extend(emit_dq_for_table(cur, table))
        events.extend(emit_lineage(cur))

        if pct > 0.8:
            events.append(gen_alert(cur, "mart.product_availability", "info",
                                    "RecoveryDetected", "null_rate back under threshold; dashboard recovering"))
        cur += timedelta(seconds=random.randint(20, 35))
    return events


# ============================================================
# VOLUME DROP PHASES
# ============================================================
def phase_volume_source_breaks(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 1/VD] Source extraction breaks at {cur.isoformat()}")

    # Root cause event: extraction returns only ~10% of rows; job still "succeeds".
    events.append(gen_job_log(
        cur, "ingest_inventory", "success",
        rows_in=1247, rows_out=1247,
        duration_ms=random.gauss(45000, 8000),
        worker_cpu=random.gauss(28, 6),
        level="WARN",
        message="Extraction query returned 1,247 rows (expected ~12,000). "
                "source.orders_db.inventory may have incomplete data. Job completed successfully."))

    cur += timedelta(seconds=random.randint(20, 40))

    while cur < start + timedelta(minutes=minutes):
        for job_name in JOBS:
            if job_name == "ingest_inventory":
                continue
            base = TABLES.get(JOBS[job_name]["dst"], {}).get("base_rows", 1000)
            rows = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success", rows, rows,
                                      random.gauss(45000, 8000)))
        for table in TABLES:
            events.extend(emit_dq_for_table(cur, table))
        events.extend(emit_lineage(cur))
        cur += timedelta(seconds=random.randint(20, 40))
    return events


def phase_volume_raw_drops(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 2/VD] Raw layer row_count drops at {cur.isoformat()}")
    while cur < start + timedelta(minutes=minutes):
        low_rows = int(random.gauss(1247, 50))

        events.append(gen_job_log(
            cur, "ingest_inventory", "success",
            rows_in=low_rows, rows_out=low_rows,
            duration_ms=random.gauss(45000, 8000),
            worker_cpu=random.gauss(28, 6),
            level="WARN",
            message=f"Ingested {low_rows:,} rows from source.orders_db.inventory "
                    f"(baseline: 12,000). Possible upstream data loss."))

        # transform/serve inventory: rows_in must match what's actually in raw (~1,247)
        low_mart = int(low_rows * 0.983)
        events.append(gen_job_log(cur, "transform_inventory", "success",
                                  rows_in=low_rows, rows_out=low_mart,
                                  duration_ms=random.gauss(38000, 6000),
                                  level="WARN",
                                  message=f"Transformed {low_mart:,} rows (expected ~11,800). "
                                          f"Output significantly below baseline."))
        events.append(gen_job_log(cur, "refresh_inventory_dashboard", "success",
                                  rows_in=low_mart, rows_out=low_mart,
                                  duration_ms=random.gauss(12000, 3000)))

        for job_name in ["ingest_orders", "transform_revenue", "refresh_revenue_dashboard"]:
            base = TABLES.get(JOBS[job_name]["dst"], {}).get("base_rows", 1000)
            rows = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success", rows, rows,
                                      random.gauss(45000, 8000)))

        # raw.inventory_snapshot: row_count fails, null_rate and freshness normal
        events.extend(emit_dq_for_table(cur, "raw.inventory_snapshot",
                                        row_count_override=low_rows))
        events.extend(emit_dq_for_table(cur, "mart.product_availability",
                                        row_count_override=low_mart))
        for table in ["raw.orders", "mart.daily_revenue"]:
            events.extend(emit_dq_for_table(cur, table))

        events.extend(emit_lineage(cur))

        if random.random() < 0.5:
            events.append(gen_alert(
                cur, "raw.inventory_snapshot", "warning", "RowCountAnomaly",
                f"row_count {low_rows:,} is 89% below baseline 12,000"))

        cur += timedelta(seconds=random.randint(20, 40))
    return events


def phase_volume_downstream(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 3/VD] Downstream cascade at {cur.isoformat()}")
    while cur < start + timedelta(minutes=minutes):
        low_raw = int(random.gauss(1247, 50))
        low_mart = int(random.gauss(1180, 50))

        events.append(gen_job_log(
            cur, "ingest_inventory", "success",
            rows_in=low_raw, rows_out=low_raw,
            duration_ms=random.gauss(45000, 8000),
            worker_cpu=random.gauss(28, 6),
            level="WARN",
            message=f"Ingested {low_raw:,} rows from source.orders_db.inventory "
                    f"(baseline: 12,000). Possible upstream data loss."))

        events.append(gen_job_log(
            cur, "transform_inventory", "success",
            rows_in=low_raw, rows_out=low_mart,
            duration_ms=random.gauss(38000, 6000),
            worker_cpu=random.gauss(28, 6),
            level="WARN",
            message=f"Transformed {low_mart:,} rows (expected ~11,800). "
                    f"Output significantly below baseline."))

        events.append(gen_job_log(
            cur, "refresh_inventory_dashboard", "success",
            rows_in=low_mart, rows_out=low_mart,
            duration_ms=random.gauss(12000, 3000),
            level="WARN",
            message=f"Refreshed dashboard.inventory_health with {low_mart:,} product rows "
                    f"(expected ~11,800). Dashboard counts significantly below normal."))

        for job_name in ["ingest_orders", "transform_revenue", "refresh_revenue_dashboard"]:
            base = TABLES.get(JOBS[job_name]["dst"], {}).get("base_rows", 1000)
            rows = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success", rows, rows,
                                      random.gauss(45000, 8000)))

        events.extend(emit_dq_for_table(cur, "raw.inventory_snapshot",
                                        row_count_override=low_raw))
        events.extend(emit_dq_for_table(cur, "mart.product_availability",
                                        row_count_override=low_mart))
        for table in ["raw.orders", "mart.daily_revenue"]:
            events.extend(emit_dq_for_table(cur, table))

        events.extend(emit_lineage(cur))

        events.append(gen_alert(
            cur, "raw.inventory_snapshot", "warning", "RowCountAnomaly",
            f"row_count {low_raw:,} is 89% below baseline 12,000"))
        events.append(gen_alert(
            cur, "dashboard.inventory_health", "critical", "BusinessImpact",
            "Inventory dashboard showing 90% fewer products than expected; "
            "potential data loss in pipeline"))

        cur += timedelta(seconds=random.randint(15, 30))
    return events


def phase_volume_recovery(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 4/VD] Recovery at {cur.isoformat()}")
    total = minutes * 60
    recovery_alerted = False
    threshold = int(TABLES["raw.inventory_snapshot"]["base_rows"] * 0.7)  # 8400

    while cur < start + timedelta(minutes=minutes):
        pct = (cur - start).total_seconds() / total  # 0 -> 1
        recovered_rows = int(1200 + (12000 - 1200) * pct)
        mart_rows = int(recovered_rows * 0.983)

        lvl = "INFO" if pct > 0.5 else "WARN"
        msg = ("Extraction restored: source.orders_db.inventory returning full dataset"
               if pct > 0.7 else
               f"Extraction recovering: {recovered_rows:,} rows from source.orders_db.inventory")

        events.append(gen_job_log(cur, "ingest_inventory", "success",
                                  rows_in=recovered_rows, rows_out=recovered_rows,
                                  duration_ms=random.gauss(45000, 8000),
                                  worker_cpu=random.gauss(28, 6),
                                  level=lvl, message=msg))
        events.append(gen_job_log(cur, "transform_inventory", "success",
                                  rows_in=recovered_rows, rows_out=mart_rows,
                                  duration_ms=random.gauss(38000, 6000), level=lvl))
        events.append(gen_job_log(cur, "refresh_inventory_dashboard", "success",
                                  rows_in=mart_rows, rows_out=mart_rows,
                                  duration_ms=random.gauss(12000, 3000)))
        for job_name in ["ingest_orders", "transform_revenue", "refresh_revenue_dashboard"]:
            base = TABLES.get(JOBS[job_name]["dst"], {}).get("base_rows", 1000)
            rows = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success", rows, rows,
                                      random.gauss(45000, 8000)))

        events.extend(emit_dq_for_table(cur, "raw.inventory_snapshot",
                                        row_count_override=recovered_rows))
        events.extend(emit_dq_for_table(cur, "mart.product_availability",
                                        row_count_override=mart_rows))
        for table in ["raw.orders", "mart.daily_revenue"]:
            events.extend(emit_dq_for_table(cur, table))
        events.extend(emit_lineage(cur))

        if recovered_rows >= threshold and not recovery_alerted:
            events.append(gen_alert(
                cur, "raw.inventory_snapshot", "info", "RowCountRecovered",
                f"row_count {recovered_rows:,} back above threshold {threshold:,}; "
                f"pipeline data volume restoring"))
            recovery_alerted = True

        cur += timedelta(seconds=random.randint(20, 35))
    return events


# ============================================================
# SCENARIO BUILDERS
# ============================================================
def build_schema_change(base_time):
    events = []
    events += phase_normal(base_time, 15)                                       # 0-15
    events += phase_schema_change(base_time + timedelta(minutes=15), 5)         # 15-20
    events += phase_ingest_breaks(base_time + timedelta(minutes=20), 10)        # 20-30
    events += phase_downstream_cascade(base_time + timedelta(minutes=30), 20)   # 30-50
    events += phase_recovery(base_time + timedelta(minutes=50), 10)             # 50-60
    return events


def build_volume_drop(base_time):
    events = []
    events += phase_normal(base_time, 15)                                          # 0-15
    events += phase_volume_source_breaks(base_time + timedelta(minutes=15), 5)     # 15-20
    events += phase_volume_raw_drops(base_time + timedelta(minutes=20), 10)        # 20-30
    events += phase_volume_downstream(base_time + timedelta(minutes=30), 20)       # 30-50
    events += phase_volume_recovery(base_time + timedelta(minutes=50), 10)         # 50-60
    return events


# ============================================================
# FRESHNESS DELAY PHASES
# ============================================================
def phase_freshness_slowdown(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 1/FD] Ingest slows down at {cur.isoformat()}")
    total = minutes * 60
    while cur < start + timedelta(minutes=minutes):
        pct = (cur - start).total_seconds() / total  # 0 -> 1
        duration = random.gauss(45000 + (300000 - 45000) * pct, 10000)
        lag = max(0.0, 6 + 9 * pct + random.gauss(0, 1))

        base_r = TABLES["raw.inventory_snapshot"]["base_rows"]
        rows_r = base_r * random.gauss(1.0, 0.02)
        events.append(gen_job_log(
            cur, "ingest_inventory", "success",
            rows_in=rows_r, rows_out=rows_r,
            duration_ms=max(10000, duration),
            worker_cpu=random.gauss(35, 5),
            level="WARN",
            message=f"Job completed in {int(duration):,}ms (baseline: 45,000ms). "
                    f"Slow read from source.orders_db.inventory."))

        for job_name in ["ingest_orders", "transform_revenue", "refresh_revenue_dashboard",
                         "transform_inventory", "refresh_inventory_dashboard"]:
            base = TABLES.get(JOBS[job_name]["dst"], {}).get("base_rows", 1000)
            rows = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success", rows, rows,
                                      random.gauss(45000, 8000)))

        events.extend(emit_dq_for_table(cur, "raw.inventory_snapshot",
                                        freshness_override=lag))
        for table in ["mart.product_availability", "raw.orders", "mart.daily_revenue"]:
            events.extend(emit_dq_for_table(cur, table))
        events.extend(emit_lineage(cur))
        cur += timedelta(seconds=random.randint(20, 40))
    return events


def phase_freshness_stall(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 2/FD] Ingest stalls at {cur.isoformat()}")
    total = minutes * 60
    while cur < start + timedelta(minutes=minutes):
        pct = (cur - start).total_seconds() / total  # 0 -> 1
        lag = 15 + 25 * pct + random.gauss(0, 2)
        stall_ms = random.randint(600000, 900000)

        # ingest_inventory stalled: still "running", no output written
        events.append(gen_job_log(
            cur, "ingest_inventory", "running",
            rows_in=0, rows_out=0,
            duration_ms=stall_ms,
            worker_cpu=random.gauss(45, 8),
            level="WARN",
            message=f"Job still running after {stall_ms:,}ms. No output written yet. "
                    f"Source query may be blocked."))

        base_m = TABLES["mart.product_availability"]["base_rows"]
        rows_m = base_m * random.gauss(1.0, 0.02)
        events.append(gen_job_log(
            cur, "transform_inventory", "success",
            rows_in=rows_m, rows_out=rows_m,
            duration_ms=random.gauss(38000, 6000),
            level="WARN",
            message=f"Input table raw.inventory_snapshot last updated {int(lag)} min ago. "
                    f"Processing stale data."))
        events.append(gen_job_log(
            cur, "refresh_inventory_dashboard", "success",
            rows_in=rows_m, rows_out=rows_m,
            duration_ms=random.gauss(12000, 3000)))

        for job_name in ["ingest_orders", "transform_revenue", "refresh_revenue_dashboard"]:
            base = TABLES.get(JOBS[job_name]["dst"], {}).get("base_rows", 1000)
            rows = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success", rows, rows,
                                      random.gauss(45000, 8000)))

        # freshness FAILS; null_rate and row_count stay normal
        events.extend(emit_dq_for_table(cur, "raw.inventory_snapshot",
                                        freshness_override=lag))
        mart_lag = max(0.0, lag * 0.9 + random.gauss(0, 2))
        events.extend(emit_dq_for_table(cur, "mart.product_availability",
                                        freshness_override=mart_lag))
        for table in ["raw.orders", "mart.daily_revenue"]:
            events.extend(emit_dq_for_table(cur, table))
        events.extend(emit_lineage(cur))

        if random.random() < 0.5:
            events.append(gen_alert(
                cur, "raw.inventory_snapshot", "warning", "FreshnessViolation",
                f"freshness_lag_min {lag:.1f} exceeds threshold 20 min. "
                f"ingest_inventory has been running for {stall_ms // 60000} min with no output."))

        cur += timedelta(seconds=random.randint(20, 40))
    return events


def phase_freshness_stale_dashboard(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 3/FD] Dashboard serving stale data at {cur.isoformat()}")
    total = minutes * 60
    while cur < start + timedelta(minutes=minutes):
        pct = (cur - start).total_seconds() / total  # 0 -> 1
        lag = 40 + 20 * pct + random.gauss(0, 2)
        stall_ms = random.randint(600000, 900000)

        events.append(gen_job_log(
            cur, "ingest_inventory", "running",
            rows_in=0, rows_out=0,
            duration_ms=stall_ms,
            worker_cpu=random.gauss(45, 8),
            level="WARN",
            message=f"Job still running after {stall_ms:,}ms. No output written yet. "
                    f"Source query may be blocked."))

        base_m = TABLES["mart.product_availability"]["base_rows"]
        rows_m = base_m * random.gauss(1.0, 0.02)
        events.append(gen_job_log(
            cur, "transform_inventory", "success",
            rows_in=rows_m, rows_out=rows_m,
            duration_ms=random.gauss(38000, 6000),
            level="WARN",
            message=f"Input table raw.inventory_snapshot last updated {int(lag)} min ago. "
                    f"Processing stale data."))
        events.append(gen_job_log(
            cur, "refresh_inventory_dashboard", "success",
            rows_in=rows_m, rows_out=rows_m,
            duration_ms=random.gauss(12000, 3000),
            level="WARN",
            message=f"Refreshed dashboard.inventory_health from stale data "
                    f"({int(lag)} min old). Dashboard may not reflect current inventory."))

        for job_name in ["ingest_orders", "transform_revenue", "refresh_revenue_dashboard"]:
            base = TABLES.get(JOBS[job_name]["dst"], {}).get("base_rows", 1000)
            rows = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success", rows, rows,
                                      random.gauss(45000, 8000)))

        events.extend(emit_dq_for_table(cur, "raw.inventory_snapshot",
                                        freshness_override=lag))
        mart_lag = max(0.0, lag * 0.9 + random.gauss(0, 2))
        events.extend(emit_dq_for_table(cur, "mart.product_availability",
                                        freshness_override=mart_lag))
        for table in ["raw.orders", "mart.daily_revenue"]:
            events.extend(emit_dq_for_table(cur, table))
        events.extend(emit_lineage(cur))

        events.append(gen_alert(
            cur, "raw.inventory_snapshot", "warning", "FreshnessViolation",
            f"freshness_lag_min {lag:.1f} exceeds threshold 20 min"))
        events.append(gen_alert(
            cur, "dashboard.inventory_health", "critical", "StaleData",
            f"dashboard.inventory_health serving data from {int(lag)} minutes ago; "
            f"business decisions may be based on outdated inventory levels"))

        cur += timedelta(seconds=random.randint(15, 30))
    return events


def phase_freshness_recovery(start, minutes):
    events = []
    cur = start
    print(f"  [Phase 4/FD] Recovery at {cur.isoformat()}")
    total = minutes * 60
    recovery_alerted = False
    while cur < start + timedelta(minutes=minutes):
        pct = (cur - start).total_seconds() / total  # 0 -> 1
        lag = max(0.0, 45 - 39 * pct + random.gauss(0, 1))
        duration = max(10000, random.gauss(45000 + (1 - pct) * 50000, 8000))

        lvl = "INFO" if pct > 0.5 else "WARN"
        base_r = TABLES["raw.inventory_snapshot"]["base_rows"]
        rows_r = base_r * random.gauss(1.0, 0.02)
        base_m = TABLES["mart.product_availability"]["base_rows"]
        rows_m = base_m * random.gauss(1.0, 0.02)

        events.append(gen_job_log(
            cur, "ingest_inventory", "success",
            rows_in=rows_r, rows_out=rows_r,
            duration_ms=duration,
            worker_cpu=random.gauss(28 + (1 - pct) * 15, 6),
            level=lvl,
            message="Source query unblocked. Backfill completing. Freshness recovering."))
        events.append(gen_job_log(
            cur, "transform_inventory", "success",
            rows_in=rows_r, rows_out=rows_m,
            duration_ms=random.gauss(38000, 6000), level=lvl))
        events.append(gen_job_log(
            cur, "refresh_inventory_dashboard", "success",
            rows_in=rows_m, rows_out=rows_m,
            duration_ms=random.gauss(12000, 3000)))

        for job_name in ["ingest_orders", "transform_revenue", "refresh_revenue_dashboard"]:
            base = TABLES.get(JOBS[job_name]["dst"], {}).get("base_rows", 1000)
            rows = base * random.gauss(1.0, 0.02)
            events.append(gen_job_log(cur, job_name, "success", rows, rows,
                                      random.gauss(45000, 8000)))

        events.extend(emit_dq_for_table(cur, "raw.inventory_snapshot",
                                        freshness_override=lag))
        mart_lag = max(0.0, lag * 0.9 + random.gauss(0, 1))
        events.extend(emit_dq_for_table(cur, "mart.product_availability",
                                        freshness_override=mart_lag))
        for table in ["raw.orders", "mart.daily_revenue"]:
            events.extend(emit_dq_for_table(cur, table))
        events.extend(emit_lineage(cur))

        if lag < 20 and not recovery_alerted:
            events.append(gen_alert(
                cur, "raw.inventory_snapshot", "info", "FreshnessRecovered",
                f"freshness_lag_min {lag:.1f} back below threshold 20 min; "
                f"pipeline freshness restored"))
            recovery_alerted = True

        cur += timedelta(seconds=random.randint(20, 35))
    return events


def build_freshness_delay(base_time):
    events = []
    events += phase_normal(base_time, 15)                                             # 0-15
    events += phase_freshness_slowdown(base_time + timedelta(minutes=15), 10)         # 15-25
    events += phase_freshness_stall(base_time + timedelta(minutes=25), 10)            # 25-35
    events += phase_freshness_stale_dashboard(base_time + timedelta(minutes=35), 15)  # 35-50
    events += phase_freshness_recovery(base_time + timedelta(minutes=50), 10)         # 50-60
    return events


# ============================================================
# ORCHESTRATOR
# ============================================================
SCENARIO_BUILDERS = {
    "schema_change": build_schema_change,
    "volume_drop": build_volume_drop,
    "freshness_delay": build_freshness_delay,
}
SCENARIOS = set(SCENARIO_BUILDERS.keys())


def build_events(scenario: str, base_time: datetime) -> list:
    builder = SCENARIO_BUILDERS.get(scenario)
    if not builder:
        raise SystemExit(f"Unknown scenario '{scenario}'. Available: {sorted(SCENARIOS)}")
    return builder(base_time)


def main():
    global SPLUNK_HEC_URL, SPLUNK_HEC_TOKEN

    parser = argparse.ArgumentParser(description="Pipeline Doctor - Data Pipeline Fault Generator")
    parser.add_argument("--host", default=os.environ.get("SPLUNK_HEC_HOST", SPLUNK_HOST), help="Splunk host")
    parser.add_argument("--port", type=int, default=SPLUNK_HEC_PORT, help="HEC port")
    parser.add_argument("--token", default=os.environ.get("PIPELINE_HEC_TOKEN", SPLUNK_HEC_TOKEN),
                        help="HEC token (or set PIPELINE_HEC_TOKEN env var)")
    parser.add_argument("--scenario", default="schema_change", choices=sorted(SCENARIOS),
                        help="Failure scenario to generate")
    parser.add_argument("--dry-run", action="store_true", help="Print sample events without sending")
    parser.add_argument("--batch-size", type=int, default=50, help="Events per HEC batch")
    args = parser.parse_args()

    SPLUNK_HEC_URL = f"https://{args.host}:{args.port}/services/collector/event"
    SPLUNK_HEC_TOKEN = args.token

    base_time = datetime.now() - timedelta(minutes=60)

    print("=" * 60)
    print("Pipeline Doctor - Data Pipeline Fault Simulator")
    print("=" * 60)
    print(f"Scenario : {args.scenario}")
    print(f"Target   : {SPLUNK_HEC_URL}")
    print(f"Base time: {base_time.isoformat()}\n")

    all_events = build_events(args.scenario, base_time)
    all_events.sort(key=lambda e: e["time"])
    print(f"\nTotal events generated: {len(all_events)}")

    if args.dry_run:
        print("\n[DRY RUN] Sample events:")
        for evt in all_events[:6]:
            print(json.dumps(evt, indent=2))
        print(f"... and {len(all_events) - 6} more events")
        return

    if not SPLUNK_HEC_TOKEN:
        raise SystemExit("No HEC token. Pass --token or set PIPELINE_HEC_TOKEN. (Use --dry-run to preview.)")

    print(f"\nSending in batches of {args.batch_size}...")
    sent = failed = 0
    for i in range(0, len(all_events), args.batch_size):
        batch = all_events[i:i + args.batch_size]
        if send_to_splunk(batch):
            sent += len(batch)
        else:
            failed += len(batch)
        time.sleep(0.1)
        pct = min(100, int((i + len(batch)) / len(all_events) * 100))
        print(f"\r  Progress: {pct}% ({sent} sent, {failed} failed)", end="", flush=True)

    print(f"\n\nDone! {sent} sent, {failed} failed.")
    print("Verify in Splunk: index=main sourcetype=pipeline:*")


if __name__ == "__main__":
    main()
