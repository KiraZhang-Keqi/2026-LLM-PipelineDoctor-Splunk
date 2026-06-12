# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T21:09:35.593835

**Problem:** The inventory dashboard shows every product as out of stock, but sales data looks fine. Please investigate and diagnose the root cause.

---

All the evidence is in. The picture is crystal clear. Let me compile the complete diagnosis report.

---

# 🩺 Pipeline Doctor — Diagnosis Report

**Incident:** `dashboard.inventory_health` — All Products Showing Out of Stock
**Investigation Time:** 2026-06-07 ~00:19 → 01:04 UTC-4
**Severity:** CRITICAL (merchandising and checkout availability impacted)

---

## 1. 📋 Summary

The `dashboard.inventory_health` dashboard showed every product as out of stock due to a **broken schema contract in the upstream source database**. The column used to carry stock quantity was renamed from `stock_count` → `available_qty` in `source.orders_db.inventory`, but the `ingest_inventory` pipeline job was never updated to reflect this change. As a result, the job silently filled `stock_count` with `NULL` for every row it ingested, and those NULLs propagated downstream through `raw.inventory_snapshot` → `mart.product_availability` → `dashboard.inventory_health`, causing the dashboard to interpret all inventory as zero/unavailable.

The revenue pipeline (`source.orders_db.orders → raw.orders → mart.daily_revenue → dashboard.revenue_overview`) was **completely unaffected** — confirming this is an inventory-chain-only, schema-specific break, not a platform-wide outage.

---

## 2. 🔍 Root Cause

**A column rename in the source database was not communicated to or handled by the consuming pipeline.**

| Attribute | Detail |
|---|---|
| **Schema Change** | `stock_count` → `available_qty` |
| **Where it originated** | `source.orders_db.inventory` (schema v2.3.1, ticket **DATA-1847**) |
| **When it landed** | **2026-06-07 00:19 UTC-4** |
| **Breakage propagated to** | `raw.inventory_snapshot` (schema v2.3.1, ticket **DATA-1851**) at **00:20** |
| **Offending job** | `ingest_inventory` — hardcoded to read column `stock_count`; the source no longer had that column |
| **Job behavior** | Silently continued with `status=success`, but logged `WARN: Column 'stock_count' not found in source; filling NULL` from **00:20 onward** |

---

## 3. 🔗 Lineage / Propagation

The break flowed downstream table-by-table with measurable delays:

```
source.orders_db.inventory
  ↓ 00:19 UTC-4 — Schema change: stock_count → available_qty (DATA-1847)
  
[ingest_inventory] ← BROKE HERE — still reading "stock_count" → NULLs written
  ↓ 00:20 UTC-4 — null_rate on stock_count spikes to ~17.8% → 98.6% by 00:25

raw.inventory_snapshot  ← stock_count column now ~99% NULL
  ↓
[transform_inventory] — computes availability from NULL stock_count
  ↓ 00:30 UTC-4 — null_rate on "available" crosses threshold (~19.8%)
  ↓ 00:35 UTC-4 — null_rate reaches 96% (fully saturated)

mart.product_availability  ← "available" column ~97% NULL
  ↓
[refresh_inventory_dashboard]

dashboard.inventory_health  ← ALL products shown as out-of-stock
  ↓ 00:53 UTC-4 — Critical BusinessImpact alert fires
```

**Propagation lag breakdown:**
- Source rename → raw null spike: **~1 min** (next ingest cycle)
- raw null spike → mart saturation: **~10–15 min** (transform cycle)
- mart saturation → dashboard alert: **~15–20 min** (dashboard refresh + alert threshold)

**Patch applied at 00:54 UTC-4:** `ingest_inventory` updated to read `available_qty` aliased as `stock_count`. Backfill began immediately, and null_rate trended back down from ~99% to ~3% by 01:04 UTC-4. Recovery alerts (`RecoveryDetected`) confirmed at 01:02–01:04 UTC-4.

---

## 4. 🧾 Evidence

### ✅ Schema Registry — Smoking Gun
| Time | Table | Change | Old Field | New Field | Ticket |
|---|---|---|---|---|---|
| 00:19 | `source.orders_db.inventory` | `rename_column` | `stock_count` | `available_qty` | DATA-1847 |
| 00:54 | `raw.inventory_snapshot` | `rename_column` | `stock_count` | `available_qty` | DATA-1851 |

### ✅ null_rate Timeline (avg per 5-min window)
| Window | `raw.inventory_snapshot` (stock_count) | `mart.product_availability` (available) |
|---|---|---|
| 00:00–00:15 | ~0.1–0.2% ✅ | ~0.1–0.2% ✅ |
| **00:20** | **17.8%** 🚨 | ~0.17% (lag) |
| **00:25** | **98.6%** 🔴 | ~0.15% (lag) |
| **00:30** | 99.4% 🔴 | **19.8%** 🚨 |
| **00:35–00:50** | ~98–99% 🔴 | **96–97%** 🔴 |
| 00:55 | 66% ↓ (patch) | 59% ↓ |
| 01:00–01:04 | 20% → 7.6% ↓↓ | 18% → 6.8% ↓↓ |

### ❌ RULED OUT: Resource / Volume / Scheduling Issues
| Signal | Value | Verdict |
|---|---|---|
| `row_count` — `raw.inventory_snapshot` | ~11,600–12,200 rows ✅ PASS | **Normal volume** — no data loss |
| `row_count` — `mart.product_availability` | ~11,400–11,900 rows ✅ PASS | **Normal volume** |
| `freshness_lag_min` — both tables | 3–10 min, well under 20-min threshold ✅ PASS | **No scheduling delay** |
| `worker_cpu_pct` — `ingest_inventory` | 18–47%, no saturation ✅ | **No CPU pressure** |
| `worker_mem_pct` — `ingest_inventory` | 28–59%, no saturation ✅ | **No memory pressure** |
| `duration_ms` — `ingest_inventory` | 34k–55k ms, consistent ✅ | **No slowdown** |
| `status` — both jobs | `success` throughout ✅ | **Jobs never failed** — silent data corruption |

> 🔑 **The job never errored. It succeeded with corrupted data.** This is the hallmark of a schema-contract violation: the pipeline code was silently schema-mismatched while the infrastructure ran perfectly.

### ✅ Job Logs Confirm the Missing Column by Name
```
00:20 WARN ingest_inventory: "Column 'stock_count' not found in source; filling NULL."
00:54 WARN ingest_inventory: "Backfilling stock_count from available_qty; null_rate=0.99"  ← patch applied
01:04 WARN ingest_inventory: "Backfilling stock_count from available_qty; null_rate=0.03"  ← recovering
```

---

## 5. 🔧 Remediation

### Immediate Fix (already in progress ✅)
- `ingest_inventory` has been patched to read `available_qty` and alias it as `stock_count` — confirmed by job logs and declining null_rate.
- A full backfill is running; monitor `null_rate` until it drops below 5% threshold consistently.
- Validate `mart.product_availability` recovery and manually verify dashboard before closing the incident.

### Prevent Recurrence — Schema Contract Enforcement

| Layer | Action |
|---|---|
| **Schema Registry CI gate** | Block source-side `rename_column` / `drop_column` changes (DATA-1847 class) from merging unless all downstream consumers have been patched and their jobs' `schema_version` updated. |
| **Consumer compatibility check** | Add a pre-flight check in `ingest_inventory` startup that validates expected column names against the live source schema. Fail fast with `ERROR` instead of filling NULLs. |
| **DQ alert tuning** | The null_rate alert on `raw.inventory_snapshot.stock_count` should fire at the raw layer (which it did at ~00:20) and trigger a **pipeline hold** on downstream jobs before the mart and dashboard are corrupted. |
| **Migration ticket linking** | DATA-1847 (source change) and DATA-1851 (raw snapshot update) should have been a single coordinated ticket that includes a job-code PR for `ingest_inventory`. Enforce this via your change-management workflow. |
| **Column-level lineage tracking** | Extend `pipeline:lineage` to track field-level lineage (`stock_count` flows through these 4 tables), so a schema registry change automatically flags all consuming jobs. |

---

## 6. 🎯 Confidence: **HIGH**

| Factor | Support |
|---|---|
| Schema change timestamp aligns exactly with null_rate onset | ✅ |
| Job logs name the missing column explicitly | ✅ |
| Null_rate cascades from raw → mart with expected delay | ✅ |
| Row count, freshness, CPU, memory all normal throughout | ✅ |
| Revenue pipeline (different source) completely healthy | ✅ |
| Patch applied + declining null_rate confirms the fix | ✅ |

The only thing that would further raise confidence is confirming that **no other schema changes** were made around this time period (the registry only shows these two rename events — consistent), and validating that `mart.product_availability.available` is derived directly from `raw.inventory_snapshot.stock_count` in the `transform_inventory` job code (consistent with the observed ~10-minute propagation lag).