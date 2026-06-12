# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T21:54:57.822308

**Problem:** The inventory dashboard numbers dropped dramatically overnight — product counts are a fraction of what they should be. But no jobs have failed and no alerts fired until just now. What happened?

---

I have everything needed for a complete, high-confidence diagnosis. Here is the full report:

---

# 🩺 Pipeline Doctor — Diagnosis Report

## 1. Summary

**The inventory dashboard did not lose data inside the pipeline. The pipeline worked perfectly. The problem is that `source.orders_db.inventory` — the upstream operational database — delivered only ~1,200 rows per cycle instead of its normal ~12,000 (an 89% drop) for a roughly 42-minute window overnight.** Every downstream job faithfully ingested, transformed, and served whatever the source gave it, causing `raw.inventory_snapshot`, `mart.product_availability`, and ultimately `dashboard.inventory_health` to show a fraction of the expected product counts. The source self-recovered by 01:45, and row counts are back to normal.

**User-visible impact:** The inventory health dashboard showed ~90% fewer products than expected from ~01:03 to ~01:45 (≈42 minutes). No inventory business decision made during that window should be trusted.

---

## 2. Root Cause

> **Upstream source truncation at `source.orders_db.inventory` — NOT a pipeline failure.**

This is a **source-side data availability incident**, not a schema break, transform bug, resource outage, or scheduling gap. The source table delivered ~1,200 rows when ~12,000 were expected. The pipeline had no schema changes whatsoever — the schema registry shows only `baseline` events at version `2.2.0` throughout the incident.

**The pipeline's ingest job detected this but did not block it.** The `ingest_inventory` job logged a `WARN`-level message — _"Extraction query returned 1,247 rows (expected ~12,000). source.orders_db.inventory may have incomplete data."_ — but its status remained `success`, so no alert fired until the row-count DQ threshold was breached at 01:08.

**Likely source-side causes to investigate** (outside this pipeline's observability scope):
- A scheduled `DELETE`/`TRUNCATE` + re-load job on `orders_db.inventory` that briefly empties the table mid-cycle
- A database maintenance window, partition swap, or bulk-load operation
- A replication lag that caused the replica being queried to temporarily fall behind
- A misconfigured snapshot/export query with a restrictive `WHERE` clause added temporarily

---

## 3. Lineage / Propagation

The break flowed through the entire inventory chain, top to bottom:

```
source.orders_db.inventory  ← ROOT: Only ~1,200 rows available (normal: ~12,000)
         │  01:03 — ingest_inventory fires WARN, ingests 1,247 rows, status=success
         ▼
raw.inventory_snapshot       ← ~1,200–1,350 rows (normal: ~12,000)
         │  01:08 — row_count DQ check fails (1,240 vs threshold 8,400)
         │  01:36 — RowCountAnomaly alert fires ("89% below baseline")
         ▼
mart.product_availability    ← row_count fails (e.g. 3,656 vs threshold 8,260)
         ▼
dashboard.inventory_health   ← BusinessImpact CRITICAL alert fires at 01:37
                               "Inventory dashboard showing 90% fewer products than expected"

         ✅ 01:45:38 — RowCountRecovered: raw.inventory_snapshot back to 8,814+ rows
```

**The revenue pipeline (`source.orders_db.orders` → `raw.orders` → `mart.daily_revenue` → `dashboard.revenue_overview`) was completely unaffected** throughout the incident — all `ingest_orders` runs show INFO/success with normal row counts (~8,000–8,500). This confirms the problem is **inventory-source-specific**, not a platform-wide issue.

---

## 4. Evidence

| Signal | Value | Conclusion |
|---|---|---|
| `ingest_inventory` job status | `success` (all runs) | Pipeline did NOT fail |
| `ingest_inventory` CPU | 19–35% (normal range) | NOT a resource exhaustion |
| `ingest_inventory` duration_ms | 23,000–57,000ms (normal) | NOT slow / hung |
| `ingest_inventory` rows_in at 01:03 | **1,247** (baseline: ~12,000) | ⚠️ Source delivered 89% fewer rows |
| `ingest_inventory` WARN message | *"Extraction query returned 1,247 rows (expected ~12,000). source.orders_db.inventory may have incomplete data."* | Job itself flagged the upstream shortfall |
| `raw.inventory_snapshot` row_count DQ | **1,240 vs threshold 8,400** (first fail: 01:08) | Confirmed propagation to raw layer |
| `raw.inventory_snapshot` null_rate | **0 fails** for `stock_count`, `warehouse_id`, `product_id` | ✅ Rules out column rename / schema break |
| `raw.inventory_snapshot` freshness_lag_min | Passing throughout | ✅ Rules out scheduling / job-not-running |
| Schema registry | Only `baseline` events, schema version `2.2.0` | ✅ Definitively rules out schema change |
| `ingest_orders` throughout | INFO, 8,000–8,500 rows, normal CPU | ✅ Confirms blast radius is inventory-only |
| Recovery alert at 01:45:38 | `RowCountRecovered`: 8,814 rows above threshold 8,400 | Source self-healed |

---

## 5. Remediation

### Immediate Actions
1. **Validate the recovered data is complete.** Confirm `source.orders_db.inventory` now has its full ~12,000 rows and that the rows present during the outage window are not a permanent partial state.
2. **Backfill the affected window.** Re-run `ingest_inventory` → `transform_inventory` → `refresh_inventory_dashboard` for the 01:03–01:45 window to replace the truncated snapshots with correct data.
3. **Audit `orders_db.inventory`** for what happened between ~01:00 and ~01:45: check database audit logs for `TRUNCATE`, `DELETE`, bulk-load jobs, or maintenance operations.
4. **Flag any business decisions** made from the dashboard during the 01:03–01:45 window as potentially based on incomplete data.

### Prevent Recurrence

| Prevention Layer | Action |
|---|---|
| **Source contract** | Add a source-level row-count SLA: if `source.orders_db.inventory` returns fewer than 8,400 rows, `ingest_inventory` should **HALT** (not complete with `success`) and raise an alert immediately. The job's WARN-but-succeed behavior delayed alerting by ~33 minutes. |
| **Ingest job guardrail** | Implement a configurable `min_rows_in` threshold in `ingest_inventory`. If breached, fail the job (or write to a quarantine table) rather than propagating corrupted volume downstream. |
| **DQ alert latency** | The `RowCountAnomaly` alert only fired at 01:36 — 33 minutes after the first WARN at 01:03. Tighten the DQ check frequency and alerting threshold so a row-count drop of >20% triggers a `critical` alert within one check cycle. |
| **Source monitoring** | Add observability directly on `source.orders_db.inventory` (e.g., a pre-ingest probe query) so source-side anomalies are caught before any pipeline job runs. |
| **Lineage-aware blast radius** | Wire the existing lineage graph to auto-suppress or watermark dashboard panels when an upstream source check fails, rather than showing users silently incorrect data. |

---

## 6. Confidence

**🟢 HIGH**

All five evidence streams are mutually consistent and unambiguous:
- ✅ Schema registry: zero non-baseline changes — schema break definitively ruled out
- ✅ Null rates: all passing — column-level data quality intact
- ✅ Job metrics: success status, normal CPU/memory/duration — resource/infrastructure failure ruled out
- ✅ Freshness checks: passing — scheduling failure ruled out
- ✅ Revenue pipeline: fully healthy — platform-wide outage ruled out
- ✅ Direct WARN log message names the source table and quantifies the shortfall

**What would further raise confidence:** Direct access to `orders_db` database audit logs to identify the specific operation (TRUNCATE, bulk-load, partition swap, etc.) that caused the source-side truncation between ~01:00 and ~01:45. That's the one piece of evidence that lies outside this pipeline's observability boundary.