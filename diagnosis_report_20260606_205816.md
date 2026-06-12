# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T20:58:16.344241

**Problem:** The inventory dashboard shows every product as out of stock, but sales data looks fine. Please investigate and diagnose the root cause.

---

All the evidence is in. I have a complete, high-confidence picture. Here is the full diagnosis report.

---

# 🩺 Pipeline Doctor — Diagnosis Report

## 1. Summary

**What broke:** The `dashboard.inventory_health` dashboard displayed every product as "out of stock" because the critical stock quantity column — `stock_count` — was being written as `NULL` into `raw.inventory_snapshot`, which propagated downstream into `mart.product_availability` and ultimately the dashboard.

**User-visible impact:** Between ~**00:08 UTC** and ~**00:43 UTC** (≈35 minutes), the inventory dashboard falsely showed all products as out-of-stock, affecting merchandising and checkout availability decisions. The revenue/sales pipeline was entirely unaffected.

---

## 2. Root Cause

**A column rename in the source database broke the ingest job's field mapping — with no coordinated pipeline update.**

| Layer | Event | Ticket | Time |
|---|---|---|---|
| `source.orders_db.inventory` | Column renamed: **`stock_count` → `available_qty`** (schema v2.3.1) | DATA-1847 | 2026-06-07 **00:08 UTC** |
| `raw.inventory_snapshot` | Same rename propagated/registered: **`stock_count` → `available_qty`** (schema v2.3.1) | DATA-1851 | 2026-06-07 **00:43 UTC** |

The `ingest_inventory` job was **never updated** to read the new column name `available_qty`. It continued requesting the now-deleted column `stock_count`, found nothing, and silently filled every row with `NULL`. This is confirmed by the job's own WARN logs (firing continuously from 00:13 UTC onward):

> `"Column 'stock_count' not found in source.orders_db.inventory; filling NULL. (source schema now 2.3.1)"`

The `transform_inventory` job then faithfully propagated those NULLs downstream, and the dashboard interpreted NULL stock quantity as zero/out-of-stock.

---

## 3. Lineage / Propagation

```
source.orders_db.inventory
  ← column renamed stock_count → available_qty at 00:08 UTC (DATA-1847)
        │
        ▼  [ingest_inventory]  ← reads old name "stock_count" → writes NULL
raw.inventory_snapshot
  ← null_rate on stock_count spikes from ~0.2% to 97% starting ~00:13 UTC
        │
        ▼  [transform_inventory]  ← inherits NULLs, no transformation logic can fix it
mart.product_availability
  ← null_rate on "available" column spikes in lockstep (97% peak)
        │
        ▼  [refresh_inventory_dashboard]
dashboard.inventory_health
  ← ALL products show as out-of-stock (NULL qty interpreted as 0)
  ← Critical BusinessImpact alert fires at 00:41 UTC
```

**Timeline of propagation:**

| Time (UTC) | Event |
|---|---|
| **00:08** | `source.orders_db.inventory` renames `stock_count` → `available_qty` (DATA-1847) |
| **00:13** | `ingest_inventory` WARN logs begin: "Column 'stock_count' not found… filling NULL" |
| **00:13+** | `raw.inventory_snapshot.stock_count` null_rate rises from <1% toward 97% |
| **00:13+** | `mart.product_availability.available` null_rate rises in lockstep |
| **00:41** | Critical `BusinessImpact` alert fires on `dashboard.inventory_health` |
| **00:43** | `ingest_inventory` patched to read `available_qty` (aliased as `stock_count`); backfill begins |
| **00:43** | `raw.inventory_snapshot` schema registry updated to v2.3.1 (DATA-1851) |
| **00:51–00:52** | `RecoveryDetected` alerts fire; null_rates falling (97% → 14% → 6%) |

---

## 4. Evidence

### ✅ Schema-contract break confirmed
- **Schema registry:** `source.orders_db.inventory` registered `change_type=rename_column`, `old_field=stock_count`, `new_field=available_qty` at **00:08 UTC**, ticket DATA-1847.
- **Job WARN logs:** `ingest_inventory` emitted continuous WARNs from **00:13 UTC**: *"Column 'stock_count' not found in source.orders_db.inventory; filling NULL."*

### ✅ Null-rate spike is real and column-specific
- `raw.inventory_snapshot.stock_count`: null_rate rose from **~0.2% (baseline)** to **~97% (peak)**, first exceeding the 5% threshold around 00:13 UTC.
- `mart.product_availability.available`: null_rate mirrored the spike with a slight lag, confirming downstream propagation.

### ❌ Resource/volume causes ruled out — all healthy throughout
| Check | Value | Threshold | Status |
|---|---|---|---|
| `raw.inventory_snapshot` row_count | ~12,000 | 8,400 | ✅ PASS |
| `mart.product_availability` row_count | ~11,500 | 8,260 | ✅ PASS |
| `raw.inventory_snapshot` freshness_lag_min | 2.4 min | 20 min | ✅ PASS |
| `mart.product_availability` freshness_lag_min | 5.9 min | 20 min | ✅ PASS |
| `ingest_inventory` CPU | 26–37% | — | ✅ Normal |
| `ingest_inventory` duration_ms | 42,000–47,000 ms | — | ✅ Normal |
| All three jobs `status` | `success` | — | ✅ No failures |

> **This is the definitive schema-break signature:** rows are arriving on time, in normal volume, jobs complete successfully — but a specific column is all-NULL. No scheduler issue, no worker overload, no data volume drop.

### ✅ Revenue pipeline unaffected
- No alerts, DQ failures, or WARN logs on any `orders` / `daily_revenue` / `revenue_overview` assets — consistent with an isolated inventory schema contract break.

---

## 5. Remediation

### 🔴 Immediate Fix (already in progress)
1. **`ingest_inventory` job update** — confirmed applied at 00:43 UTC: read `available_qty` from source, alias it as `stock_count` in `raw.inventory_snapshot` to maintain downstream compatibility.
2. **Backfill** — trigger a full backfill of `raw.inventory_snapshot` and `mart.product_availability` to replace NULL rows written between 00:08–00:43 UTC with correct values from `available_qty`. Verify null_rate returns to <1% baseline before closing the incident.
3. **Schema registry sync** — DATA-1851 registered the rename on `raw.inventory_snapshot`; confirm `mart.product_availability` and dashboard contracts are updated to reflect `available_qty` as the canonical name going forward.

### 🟡 Short-Term (within 1 sprint)
4. **Remove the silent-NULL fallback** — `ingest_inventory` should **fail fast** (raise an exception and halt) when a required column is missing, rather than filling NULLs silently. A 35-minute undetected propagation window is the direct consequence of silent failure.
5. **Add a column-presence check** in the DQ suite on `source.orders_db.inventory` *before* ingestion runs — catch the missing column at the source, not two hops downstream.

### 🟢 Long-Term (data contract governance)
6. **Schema contract enforcement:** Require that any breaking change to `source.orders_db.inventory` (rename/drop/type change) must be accompanied by a co-deployed update to `ingest_inventory` — gated by CI. DATA-1847 was filed but the pipeline was not updated in lockstep.
7. **Automated breaking-change alerts:** Add a Splunk alert on `sourcetype=pipeline:schema_registry change_type=rename_column OR change_type=drop_column` that immediately pages the pipeline-owning team so human review happens within minutes, not 35 minutes later via a business-impact alarm.
8. **Column-level DQ on the source table:** Monitor `source.orders_db.inventory` null_rate *pre-ingestion* so breaks are caught before they enter the warehouse at all.

---

## 6. Confidence: 🟢 HIGH

All four evidence streams are fully corroborating:

| Signal | Confirms |
|---|---|
| Schema registry rename event at 00:08 UTC | Root cause timing and exact field |
| `ingest_inventory` WARN logs naming `stock_count` exactly | Job was never updated; confirms mechanism |
| null_rate spike starting at 00:13 UTC, first in `raw`, then `mart` | Propagation direction and timing |
| row_count, freshness, CPU, duration all normal | Resource/volume causes definitively ruled out |

**What would further raise confidence (already high):** A direct query of `source.orders_db.inventory` confirming `available_qty` exists and `stock_count` is absent — but this is source-system access outside the Splunk observability plane and is not needed given the corroborating evidence above.