# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T21:33:07.809307

**Problem:** The inventory dashboard numbers dropped dramatically overnight — product counts are a fraction of what they should be. But no jobs have failed and no alerts fired until just now. What happened?

---

This is the complete picture. I now have everything needed for a definitive diagnosis. The schema registry confirms schema stayed at `v2.2.0` (baseline only, no column changes registered in our registry), the null_rate never triggered, duration and CPU were normal throughout, and the revenue pipeline was completely unaffected. The cause is a **transient upstream source truncation on `source.orders_db.inventory`** — a source-side data availability problem, not a pipeline code or schema issue.

---

# 🩺 Pipeline Doctor — Diagnosis Report

## 1. Summary

**What broke:** Between **00:42 and 01:24 UTC-4 on 2026-06-07**, the `dashboard.inventory_health` dashboard displayed **~90% fewer products than expected** (~1,200 rows vs baseline ~12,000). The entire inventory pipeline (`ingest_inventory → raw.inventory_snapshot → mart.product_availability → dashboard.inventory_health`) faithfully propagated a severely truncated dataset sourced from `source.orders_db.inventory`. The problem has **self-resolved** as of 01:24, with full row counts restoring organically.

**Why it was silent so long:** Every pipeline job reported `status=success`. The jobs weren't broken — they were accurately ingesting what the source gave them. The row_count DQ threshold (8,400) is set too close to the *post-crash* floor (~1,200), so the alert didn't fire until approximately 01:14. No null_rate or schema violations occurred at all.

---

## 2. Root Cause

> **The source database `source.orders_db.inventory` began returning only ~10% of its expected rows at 00:42 UTC-4. This was a transient upstream data availability incident — a source-side truncation, partial export failure, or query filter applied at the DB layer — not a pipeline code defect, schema change, or infrastructure overload.**

There were **no schema changes** at any layer. Schema version `2.2.0` was the only entry across all registry checks (all tagged `baseline`). This rules out a column rename, type change, or drop as the cause.

---

## 3. Lineage / Propagation

The truncation propagated faithfully and instantly through every hop:

| Time (UTC-4) | Event | Detail |
|---|---|---|
| `00:27–00:41` | ✅ **Normal operation** | `ingest_inventory` pulling ~12,000–12,400 rows/run, all INFO |
| **`00:42`** | 🔴 **Source truncation begins** | First WARN: *"Extraction query returned 1,247 rows (expected ~12,000). source.orders_db.inventory may have incomplete data."* `rows_in` crashes from ~12,000 → **1,247** |
| `00:47 onward` | 🔴 **Crash confirmed & sustained** | DQ `row_count` on `raw.inventory_snapshot` drops from ~12,000 avg → **~1,219 avg** (per timechart). Stable at ~1,200 for 38 minutes |
| `00:47–01:14` | ⚠️ **Silent impact window** | `mart.product_availability` and `dashboard.inventory_health` show 90% fewer products; NO alert fires yet because DQ threshold wasn't breached immediately |
| `01:14–01:16` | 🚨 **Alerts fire** | `RowCountAnomaly` (WARNING) on `raw.inventory_snapshot` and `BusinessImpact` (CRITICAL) on `dashboard.inventory_health` begin firing |
| `01:24` | ✅ **Self-recovery** | `ingest_inventory` logs *"Extraction restored: source.orders_db.inventory returning full dataset"*; rows climb back through ~8,940 and continue recovering |
| `01:27` | ✅ **Full restoration** | All three jobs back to ~11,900–12,000 rows, all INFO, no warnings |

**Blast radius:** Inventory chain only. The revenue pipeline (`ingest_orders → raw.orders → mart.daily_revenue → dashboard.revenue_overview`) showed zero disruption — consistent ~8,200–8,600 rows throughout the entire window, confirming this was not a platform-wide outage.

---

## 4. Evidence

### ✅ What RULES OUT resource/infra/schema causes:

| Signal | Value During Incident | Interpretation |
|---|---|---|
| `ingest_inventory` `status` | Always `success` | No job failure — pipeline logic intact |
| `duration_ms` | 24,248–57,486 ms (normal range) | No timeout or slowdown |
| `worker_cpu_pct` | 19–33% (normal) | No CPU saturation |
| `null_rate` (all columns) | **No failures registered** | No schema mismatch, no renamed/dropped columns |
| `schema_registry` non-baseline events | **0 records** | No column renames, drops, or type changes |
| `ingest_orders` row counts | Steady 8,200–8,600 | Platform workers and network are healthy |

### 🔴 What PROVES the source-side truncation:

| Signal | Value | Interpretation |
|---|---|---|
| First WARN @ `00:42` | *"Extraction query returned 1,247 rows (expected ~12,000). source.orders_db.inventory may have incomplete data."* | The source itself returned 90% fewer rows |
| `rows_in = rows_out` throughout crash | 1,172–1,293 (perfectly matched) | `ingest_inventory` wrote exactly what the source gave — no pipeline-side filtering |
| DQ `row_count` on `raw.inventory_snapshot` | Drops from ~12,000 → ~1,219 avg at exactly `00:47` | Confirms source truncation propagated directly |
| Recovery message | *"Extraction restored: source.orders_db.inventory returning full dataset"* | The source resolved itself; no pipeline change was made |
| `RowCountRecovered` alert @ `01:24` | row_count 8,940 back above 8,400 | Confirms organic recovery from source side |

---

## 5. Remediation

### Immediate (already resolved, but verify):
- ✅ Confirm `mart.product_availability` and `dashboard.inventory_health` have fully refreshed with the restored ~12,000-row dataset — check the latest `transform_inventory` and `refresh_inventory_dashboard` runs.
- 🔍 **Investigate the source database:** Contact the `source.orders_db.inventory` DB owners. The truncation window was 00:42–01:24 (~42 minutes). Likely causes: a maintenance job, partial export script, a scheduled DELETE/TRUNCATE with a bad filter, a replication lag event, or a DB-side connection pool issue that silently returned partial results.
- 📊 **Validate mart data integrity:** Run a point-in-time backfill for `mart.product_availability` for the 00:42–01:24 window if downstream consumers (BI reports, SLAs) may have cached the bad numbers.

### Structural Fixes (prevent recurrence):

| Problem | Fix |
|---|---|
| **Alert threshold too loose** | The DQ `row_count` threshold on `raw.inventory_snapshot` is 8,400 — the crash hit ~1,200 for 32 minutes before the alert fired. Lower the threshold to, e.g., **10,800 (90% of baseline)** for a faster trigger. |
| **Alert fired 32 minutes late** | Add a **`RowCountDrop` alert** that fires on ANY single run where `rows_in < previous_run_rows_in * 0.5` — a per-run delta check, not just an absolute threshold. |
| **No source-layer DQ** | Add a DQ check directly on `source.orders_db.inventory` before ingestion. If the source returns < 10,000 rows, **halt the pipeline and raise an alert** rather than propagating bad data downstream. |
| **Pipeline ingested bad data silently** | Implement a **row count circuit breaker** in `ingest_inventory`: if `rows_in` drops >50% from a rolling average, abort the run and keep the last-known-good snapshot in place rather than overwriting with truncated data. |
| **No schema contract monitoring** | Even though this wasn't a schema break, the registry had no non-baseline entries during the incident. Add proactive schema contract tests (e.g., dbt tests, Great Expectations) that run at the *source* layer before ingestion begins. |
| **Dashboard showed bad data** | Consider a **"stale data" banner** on `dashboard.inventory_health` that activates when the upstream DQ check fails, rather than silently displaying wrong numbers. |

---

## 6. Confidence

**🟢 HIGH confidence** on the root cause (upstream source truncation).

**Supporting the confidence:**
- The WARN messages from `ingest_inventory` explicitly name `source.orders_db.inventory` and quantify the shortfall
- `rows_in = rows_out` proves no pipeline-side filtering
- Zero schema registry changes, zero null_rate failures, zero CPU/duration anomalies — all alternative explanations are ruled out with direct evidence
- The revenue pipeline (same workers, same platform) was completely unaffected throughout
- The recovery was spontaneous with no pipeline change, consistent with a transient source-side event

**What would further raise it to 100%:**
- DB audit logs from `source.orders_db.inventory` showing exactly what ran at ~00:40 (a batch job, maintenance window, or replication event that partially wiped or temporarily hid rows)
- Confirmation from the DB team of what caused the partial result set