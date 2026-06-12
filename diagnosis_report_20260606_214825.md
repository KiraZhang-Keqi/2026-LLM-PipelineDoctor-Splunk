# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T21:48:25.131081

**Problem:** The inventory dashboard numbers dropped dramatically overnight — product counts are a fraction of what they should be. But no jobs have failed and no alerts fired until just now. What happened?

---

I have everything I need. The lineage is confirmed and the complete incident timeline is fully reconstructed. Here is the full diagnosis:

---

# 🏥 Pipeline Doctor — Incident Diagnosis Report

## 1. Summary

**What broke:** The `dashboard.inventory_health` dashboard showed ~90% fewer products than expected overnight due to a dramatic row volume collapse in `source.orders_db.inventory` — the root source database table. This is **not a pipeline failure** — every single job (`ingest_inventory`, `transform_inventory`, `refresh_inventory_dashboard`) reported `status=success` throughout the entire incident. The pipeline did exactly what it was designed to do: it faithfully ingested, transformed, and served whatever data the source gave it — which turned out to be only ~1,200 rows instead of the normal ~12,000.

**User-visible impact:** `dashboard.inventory_health` showed ~90% of products as unavailable, corrupted, or missing for approximately **35–40 minutes** (00:56 AM → ~01:38 AM). It is now fully recovering.

---

## 2. Root Cause

> **An upstream source truncation or partial-load event occurred in `source.orders_db.inventory` at ~00:56 AM on June 7.**

This is a **source data contract violation** — not a schema change, not a resource failure, not a code bug. The source database delivered only ~1,200 rows per extraction cycle (≈10% of the expected ~12,000 baseline) for ~35 minutes. The cause is most likely one of:

- A **botched ETL/batch job on the source side** that truncated and only partially reloaded the inventory table
- A **database maintenance window** (e.g., a `TRUNCATE` followed by a slow `INSERT` backfill) that the ingestion job caught mid-operation
- A **failed data migration or deployment** on `source.orders_db` that left inventory in a partial state

**Ruled out — definitively:**
| Hypothesis | Evidence Against |
|---|---|
| Schema change / rename / drop | Schema registry shows **zero** non-baseline events. All null_rates for `product_id`, `warehouse_id`, `stock_count` were **clean throughout** (≤0.3%) |
| Pipeline job failure | All 6 jobs show `last_status=success`. CPU max 43%, memory max 67% — both completely normal |
| Infrastructure / resource pressure | `duration_ms` and `worker_cpu_pct`/`worker_mem_pct` are flat and unremarkable across all jobs, including during the incident |
| Revenue pipeline impacted | `ingest_orders` rows_in stable at ~8,200–8,600 rows throughout; `mart.daily_revenue` null_rates all passing — **revenue chain is perfectly healthy** |

---

## 3. Lineage / Propagation

The failure propagated through the full inventory chain within minutes of each extraction cycle:

```
source.orders_db.inventory   ← 💥 ROOT CAUSE: ~10% of rows present (~1,200 vs ~12,000 expected)
        │
        │  [ingest_inventory]  ← faithfully pulled 1,247 rows, status=success, WARN logged
        ▼
raw.inventory_snapshot        ← row_count crashed to ~1,195–1,331 (was ~12,000)
        │                        DQ check threshold=8,400 → FAIL
        │  [transform_inventory]  ← transformed whatever was there, status=success
        ▼
mart.product_availability     ← row_count crashed to ~1,100–1,300 (was ~8,260+)
        │                        DQ check threshold=8,260 → FAIL
        │  [refresh_inventory_dashboard]  ← served degraded data, status=success
        ▼
dashboard.inventory_health    ← 💥 VISIBLE IMPACT: "90% fewer products than expected"
                                  Critical BusinessImpact alert fired
```

**Revenue chain (unaffected, confirmed healthy control group):**
```
source.orders_db.orders → raw.orders → mart.daily_revenue → dashboard.revenue_overview ✅
```

---

## 4. Evidence — Key Data Points

### 🔴 Crash Onset
| Time | Event |
|---|---|
| **00:56:28 AM** | First anomalous `ingest_inventory` run: **1,247 rows** ingested (vs. ~12,000 expected). Message: *"Extraction query returned 1,247 rows (expected ~12,000). source.orders_db.inventory may have incomplete data."* |
| **01:01 → 01:28 AM** | Sustained crash: every extraction returns **1,150–1,350 rows**, WARNs log *"Possible upstream data loss"* |
| **01:28:49 AM** | DQ alert fires: `raw.inventory_snapshot` row_count = **1,331** (baseline 12,000), **89% below baseline** |
| **01:28:49 AM** | `dashboard.inventory_health` Critical BusinessImpact alert fires |

### 🟡 Recovery
| Time | Event |
|---|---|
| **01:32 → 01:38 AM** | `ingest_inventory` logs *"Extraction recovering"* — rows climbing: 1,722 → 2,766 → 3,630 → ... → 8,526 |
| **01:38:15 AM** | `RowCountRecovered` alert: row_count 8,526 back above threshold 8,400 |
| **01:38 → 01:41 AM** | `ingest_inventory` logs *"Extraction restored: source.orders_db.inventory returning full dataset"* — rows at 9,120 → 10,020 → 11,658 |

### ✅ Schema & Null-Rate Evidence (all clean)
- `raw.inventory_snapshot` columns `product_id`, `warehouse_id`, `stock_count` — null_rate ≤ 0.003 throughout the entire incident
- Schema registry: **0 non-baseline events** for any table, ever
- `ingest_orders` (revenue chain): steady ~8,300–8,600 rows throughout — proves this is **inventory-source-specific**, not a platform-wide event

---

## 5. Remediation

### 🚨 Immediate Actions
1. **Investigate `source.orders_db.inventory`** — pull DB audit logs for the 00:50–00:56 AM window. Look for `TRUNCATE`, `DELETE`, mass `UPDATE`, or a migration script. Identify the responsible team/deployment.
2. **Validate data completeness is fully restored** — current row counts (~11,650) look healthy, but confirm totals match the pre-incident baseline exactly. Check for any rows that were permanently deleted vs. temporarily unavailable.
3. **Check `mart.product_availability` for stale snapshots** — if the transform runs incrementally, some product records may have been overwritten with the low-volume snapshot and not yet repopulated. A full rerun of `transform_inventory` may be needed.

### 🛡️ Prevention — Closing the Detection Gap
The most critical finding here: **the pipeline WARNed at 00:56 AM but no alert fired until 01:28 AM — a 32-minute blind spot.** The jobs all returned `success`, so no failure-based alert triggered.

| Fix | Detail |
|---|---|
| **Source-side row count gate** | Add a pre-ingestion check: if `rows_in < 50% of rolling_7d_avg`, **abort the run** and fire a `critical` alert immediately — do not silently ingest degraded data |
| **WARN-level alert escalation** | The job logged `WARN` at 00:56 AM. Wire WARN-level job log messages containing "expected ~12,000" or "upstream data loss" directly to an alert with ≤5 min latency |
| **DQ threshold tightening** | The row_count DQ threshold is 8,400 — but the actual baseline is ~12,000. Tighten it to ≥10,000 (or use a dynamic ±20% window) so the DQ check catches a 10% source delivery, not just an 89% one |
| **Source DB contract SLA** | Establish a formal data contract with the `source.orders_db` team: the inventory table must have ≥ N rows before any downstream pipeline runs. Use a coordinator/sensor pattern to check this before `ingest_inventory` starts |
| **Separate freshness from volume alerting** | Add a `min_rows_delivered` check to `pipeline:data_quality` at the `raw.*` layer that fires independently of the downstream DQ checks |

---

## 6. Confidence

**🟢 HIGH confidence** in the root cause (upstream source truncation/partial load event).

The evidence is unambiguous and self-consistent across all four data sources:
- ✅ Job logs explicitly name the source table and row count at every cycle
- ✅ Schema registry is clean — zero non-baseline changes
- ✅ Null rates are clean — rules out column-level data contract break
- ✅ CPU/memory/duration are normal — rules out resource pressure
- ✅ Revenue chain is healthy — rules out platform-wide event
- ✅ Recovery message says *"source.orders_db.inventory returning full dataset"* — confirms the source itself recovered

**What would raise it to 100%:** A DB-side audit log confirming the exact operation (TRUNCATE, failed migration, etc.) that caused the partial table state between 00:56 and 01:38 AM.