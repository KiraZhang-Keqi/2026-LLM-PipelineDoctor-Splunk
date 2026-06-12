# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T21:41:18.199161

**Problem:** The inventory dashboard numbers dropped dramatically overnight — product counts are a fraction of what they should be. But no jobs have failed and no alerts fired until just now. What happened?

---

This is the critical timing correlation. Notice: the DQ row_count check **still showed 12,000 rows as passing** at `00:52:38` and `00:53:01` — but `ingest_inventory` first WARN'd about only 1,247 rows at `00:48:18`. That ~4 minute lag is exactly the DQ check reporting on the *previous* batch still in the table. The DQ fail hit at `00:53:18` when the bad batch fully replaced the table. Now I have everything needed for the complete diagnosis.

---

# 🩺 Pipeline Doctor — Diagnosis Report

## 1. Summary

**What broke:** The `dashboard.inventory_health` dashboard dropped to ~10% of its normal product count overnight, showing ~1,200 rows instead of the expected ~12,000.

**User-visible impact:** Inventory health metrics are severely under-reported. Any business decision based on product availability counts (replenishment, stock alerts, merchandising) made during the window **00:48 – ~01:32 UTC-4 on June 7** was based on corrupted data.

**The deceptive part:** Every pipeline job reported `status=success` throughout the incident. No job failed. No alert fired until `01:20`. This is because the extraction query completed successfully — it just returned 90% fewer rows than expected, and the pipeline has no hard-fail guard on row-volume drops.

---

## 2. Root Cause

**The source database `source.orders_db.inventory` began returning only ~10% of its rows starting at `00:48:18`.**

This is a **source-side data contract breach** — specifically an **upstream filter or view change on `source.orders_db.inventory`** that the schema registry did not capture. The evidence points to one of these two scenarios (both requiring the same fix):

| Scenario | Mechanism | Explains |
|---|---|---|
| **A) Source table predicate change** | A `WHERE` clause or default view filter was added/changed on the source DB (e.g., `WHERE is_active = 1` or `WHERE warehouse_region = 'US'`), returning only ~10% of rows | Row count drops 90%, column null rates unchanged ✅ |
| **B) Source table was replaced with a partitioned/filtered snapshot** | The `inventory` table was swapped with a filtered view representing only one warehouse segment | Same symptoms ✅ |

**The schema registry completely stopped emitting events for `source.orders_db.inventory` after `00:46:47`** — the last heartbeat before the incident. A new schema version (`2.3.0` or similar) was never registered, which means the schema contract monitor lost visibility at the exact moment the source changed. The `ingest_inventory` job's extraction query continued to execute successfully against whatever the new shape of the source was.

**The `available` column is ruled out as the cause** — null rates on all columns including `available`, `stock_count`, `product_id`, and `warehouse_id` remained flat at <0.3% throughout the incident. This is not a null-propagation or column-rename problem.

---

## 3. Lineage / Propagation

```
source.orders_db.inventory
   │
   │  ⚠️ 00:46:47  — Last schema registry heartbeat (v2.2.0)
   │  💥 00:47:47  — Source silently begins returning ~10% of rows
   │                 (gap between last good run 00:47:46 and first bad run 00:48:18)
   ▼
[ingest_inventory]  ← WARN logged at 00:48:18: "1,247 rows (expected ~12,000)"
   │                  status=success, cpu=28%, mem=40% (all normal — job itself healthy)
   ▼
raw.inventory_snapshot
   │
   │  ❌ 00:53:18  — DQ row_count FAIL: value=1,234 (threshold=8,400)
   │                 (4-min lag: DQ checks previous batch until replacement completes)
   ▼
[transform_inventory]  ← Faithfully passes rows_in → rows_out (no amplification)
   ▼
mart.product_availability
   │
   │  ❌ 00:53:18  — DQ row_count FAIL: value=1,153 (threshold=8,260)
   ▼
[refresh_inventory_dashboard]
   ▼
dashboard.inventory_health
   │
   │  🚨 01:20:31  — CRITICAL alert: "90% fewer products than expected"
   │                 (32-minute alert delay after first bad data hit the dashboard)
   ▼
⚠️  RECOVERING from ~01:23:18 onward  →  ✅ Fully recovered ~01:32:51
     (source.orders_db.inventory restored to full dataset)
```

**The revenue pipeline (`raw.orders` → `mart.daily_revenue` → `dashboard.revenue_overview`) was completely unaffected throughout.** This confirms the problem is isolated to the inventory source and is not a platform-wide resource or scheduler issue.

---

## 4. Evidence

| Signal | Value | Interpretation |
|---|---|---|
| **Job status — all jobs** | `success` throughout | ❌ Rules out job failure as cause |
| **Worker CPU (all jobs)** | ~28% avg | ❌ Rules out CPU saturation |
| **Worker memory (all jobs)** | ~40–42% avg | ❌ Rules out memory pressure |
| **`ingest_orders` row count** | stable 8,031–8,915 rows | ❌ Rules out platform-wide issue |
| **Null rates — all columns** | <0.3% flat, no spike | ❌ Rules out column rename/drop/type change |
| **Last good `ingest_inventory` run** | `00:47:46`, rows_in=12,003 | ✅ Last clean run |
| **First bad `ingest_inventory` run** | `00:48:18`, rows_in=1,247 (-90%) | ✅ Onset timestamp |
| **Schema registry last heartbeat** | `00:46:47` (v2.2.0 baseline) | ✅ Schema monitor lost sight at T-1 min |
| **Schema registry — new version** | *Never logged* | ✅ Source changed without registry notification |
| **First DQ row_count FAIL** | `00:53:18`, value=1,234 | ✅ DQ check 4.5 min behind actual onset |
| **First business alert** | `01:20:31` (CRITICAL, BusinessImpact) | ✅ Alert fired 32 min after DQ failure |
| **Recovery confirmed** | `01:30:32` onward | ✅ Source restored, all checks passing by 01:32 |

---

## 5. Remediation

### 🔥 Immediate (now)

1. **Audit `source.orders_db.inventory` on the source DB** — run:
   ```sql
   SELECT COUNT(*), COUNT(DISTINCT warehouse_id), COUNT(DISTINCT product_id)
   FROM source.orders_db.inventory;
   ```
   Compare against the baseline (~12,000 rows, full warehouse set). If counts are still off, the source filter/view change may not be fully reverted.

2. **Contact the source DB team** and request the change log for `source.orders_db.inventory` between `00:46` and `00:48` UTC-4 on June 7. Ask specifically about: view redefinitions, added WHERE clauses, table swaps, or partition pruning changes.

3. **Backfill** `raw.inventory_snapshot`, `mart.product_availability`, and `dashboard.inventory_health` for the affected window (`00:48`–`01:32`) once the source is confirmed healthy.

4. **Confirm `available` column** in `mart.product_availability` — since rows were silently dropped, any aggregate that relied on `available=True` row counts during the window will need recalculation.

### 🛡️ Short-term (this sprint)

5. **Add a hard-fail row-volume guard to `ingest_inventory`:** If `rows_in < 8,000` (70% of baseline), the job should `FAIL`, not `WARN` and `SUCCESS`. A 90% row drop is never a transient blip — it must be a blocking error.

6. **Register `source.orders_db.inventory` v2.2.0 schema formally** and enforce schema change notifications from the source team before any DDL runs in production. The schema registry blind spot (no events after `00:46:47`) means the source DB is not integrated into the schema contract workflow.

7. **Tighten the DQ alert threshold**: the current `row_count` threshold is 8,400 (70% of 12,000). Lower it to no more than 85% of baseline (~10,200) so failures are caught faster.

### 🏗️ Long-term (next quarter)

8. **Implement source-side schema contracts**: use a tool like Great Expectations, dbt tests, or Soda to validate `COUNT(*)` and key column cardinality on `source.orders_db.inventory` *before* `ingest_inventory` runs — not after.

9. **Reduce alert latency**: the gap between first bad data (`00:48`) and first alert (`01:20`) was **32 minutes**. Configure an alerting rule directly on `ingest_inventory` WARN messages so that any single run returning <70% of baseline rows pages on-call immediately.

10. **Add a cross-pipeline volume sanity check**: `COUNT(raw.inventory_snapshot)` should always be within 20% of the previous run. A Splunk alert on `pipeline:data_quality check_name=row_count` with a tighter threshold would have caught this 30 minutes sooner.

---

## 6. Confidence

**Confidence: HIGH (85%)** for the core finding; **MEDIUM (65%)** for the precise mechanism.

| Finding | Confidence |
|---|---|
| Row drop originated at `source.orders_db.inventory`, not in any pipeline job | ✅ **High** — directly evidenced by `rows_in = rows_out` at ingest and no null-rate spikes |
| Onset time: `00:47:47`–`00:48:18` on June 7 | ✅ **High** — pinned to the job-log transition |
| Schema registry lost tracking at `00:46:47` (T-1 min before onset) | ✅ **High** — only 12 baseline events, hard stop exactly at onset |
| Mechanism is a source-side filter/view change (not a column rename/type change) | ⚠️ **Medium** — consistent with all evidence but cannot confirm without source DB access |
| Recovery was due to source DB reverting the change | ⚠️ **Medium** — recovery messages say "returning full dataset" but source reason unknown |

**What would raise confidence to High across the board:** Access to the source DB change log or git history for the `inventory` view definition between `00:46`–`00:48`. If a `WHERE` clause, partition filter, or view swap is found there, this becomes a closed case.