# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T21:20:39.131463

**Problem:** The inventory dashboard shows every product as out of stock, but sales data looks fine. Please investigate and diagnose the root cause.

---

All evidence is in. The diagnosis is complete and unambiguous. Here is the full report:

---

# 🩺 Pipeline Doctor — Diagnosis Report

## 1. Summary

The `dashboard.inventory_health` dashboard was reporting **every product as out of stock** due to a **broken schema contract** originating in the source database. Every inventory pipeline stage was executing successfully in terms of scheduling, row volume, and compute resources — but the **stock quantity column was silently null-filled** all the way from raw ingestion through to the dashboard, causing downstream systems (merchandising, checkout availability) to see zero/null stock for all SKUs.

A backfill patch is actively in progress as of ~01:05 UTC, and null rates are recovering.

---

## 2. Root Cause

> **A column rename in `source.orders_db.inventory` was not communicated to the `ingest_inventory` job.**

| | Detail |
|---|---|
| **Schema change** | `source.orders_db.inventory` column **`stock_count` → `available_qty`** |
| **Schema version** | `2.2.0` → `2.3.1` |
| **Change type** | `rename_column` |
| **Migration ticket** | `DATA-1847` |
| **Change applied at** | `2026-06-07 00:30:05 UTC` |
| **Offending job** | `ingest_inventory` — hardcoded to read `stock_count`, which no longer exists |
| **Job behavior** | Silently wrote `NULL` for every row instead of raising a hard failure |

The source DB team renamed `stock_count` to `available_qty` (ticket DATA-1847) without notifying the pipeline team or enforcing a migration contract. `ingest_inventory` kept running but found no column named `stock_count`, and null-filled the entire field.

---

## 3. Lineage / Propagation

The null values cascaded downstream through the full inventory chain:

```
source.orders_db.inventory
  ← Column rename: stock_count → available_qty @ 00:30:05
  ↓ [ingest_inventory] — reads "stock_count" → finds nothing → writes NULL
raw.inventory_snapshot          ← stock_count null_rate spikes to ~99% @ 00:35
  ↓ [transform_inventory] — propagates NULLs into derived "available" column
mart.product_availability       ← available null_rate spikes to ~97% @ 01:03
  ↓ [refresh_inventory_dashboard]
dashboard.inventory_health      ← 🚨 ALL products show "out of stock" @ 01:03
```

| Timestamp | Event |
|---|---|
| `00:30:05` | Schema rename applied to `source.orders_db.inventory` (v2.3.1, ticket DATA-1847) |
| `00:35:00` | `raw.inventory_snapshot.stock_count` null_rate explodes from **~0.2% → 99%** |
| `01:02+` | `ingest_inventory` begins emitting `WARN: Column 'stock_count' not found in source; filling NULL` |
| `01:03:31` | First **CRITICAL** alert fires: `dashboard.inventory_health` — all products out of stock |
| `01:03–01:05` | `mart.product_availability.available` null_rate confirmed at **97%** |
| `01:05:00` | Patch applied: `ingest_inventory` updated to read `available_qty` aliased as `stock_count` |
| `01:05+` | Backfill begins; null_rate declining (`0.99 → 0.77 → 0.28 → 0.10…`) |
| `01:13–01:14` | `RecoveryDetected` alerts firing; null_rate approaching threshold |

**Revenue pipeline:** Zero schema changes detected in `source.orders_db.orders`, `raw.orders`, or `mart.daily_revenue` — all DQ checks passing. Confirms the outage is **isolated to the inventory chain**.

---

## 4. Evidence

### ✅ Resource/Volume Causes — RULED OUT

| Signal | Value | Verdict |
|---|---|---|
| `ingest_inventory` status | `success` | ✅ Job completed normally |
| `ingest_inventory` CPU | 38.9% | ✅ Not saturated |
| `ingest_inventory` rows_in / rows_out | 12,236 / 12,236 | ✅ No row loss at ingest |
| `raw.inventory_snapshot` row_count DQ | **PASS** (12,429 rows, threshold 8,400) | ✅ Volume normal |
| `mart.product_availability` row_count DQ | **PASS** (12,059 rows, threshold 8,260) | ✅ Volume normal |
| `raw.inventory_snapshot` freshness_lag_min | **PASS** (7.9 min, threshold 20 min) | ✅ Not stale |
| `mart.product_availability` freshness_lag_min | **PASS** (6.6 min, threshold 20 min) | ✅ Not stale |

**Conclusion:** The pipeline ran on schedule, processed all rows, and finished quickly. This is a **data-contract/schema failure, not an infrastructure or load problem.**

### 🔴 Schema Break Evidence

| Signal | Value |
|---|---|
| Schema registry change | `rename_column`: `stock_count` → `available_qty` at `00:30:05` |
| Schema version bump | `2.2.0` → `2.3.1` (DATA-1847) |
| `raw.inventory_snapshot.stock_count` null_rate | **~0.2% before 00:30 → 99% after 00:35** (step-change, not gradual) |
| `mart.product_availability.available` null_rate | **97% at peak** |
| `ingest_inventory` WARN log | *"Column 'stock_count' not found in source; filling NULL."* |
| Revenue pipeline schema changes | **Zero** — no changes detected |

---

## 5. Remediation

### Immediate (In Progress)
- ✅ `ingest_inventory` already patched at `01:05` to read `available_qty` and alias it as `stock_count`
- ✅ Backfill is running and null_rate is recovering — monitor until it drops below the 5% threshold across all tables
- 🔲 Confirm `dashboard.inventory_health` shows correct stock levels once backfill completes
- 🔲 Validate `mart.product_availability.available` null_rate returns to <0.5% baseline

### Short-term (This Sprint)
1. **Schema contract enforcement:** Register all pipeline column dependencies in the schema registry. Block `change_type=rename_column|drop_column|type_change` deploys unless all downstream consumers are updated first.
2. **Hard-fail on missing columns:** `ingest_inventory` (and all ingest jobs) should raise a hard `ERROR` and halt when an expected column is absent — **never silently null-fill**. A null-fill here masked the break for ~30 minutes before an alert fired.
3. **Column-level DQ alerts at ingestion:** Add a `null_rate` alert on `raw.inventory_snapshot.stock_count` with a tight threshold (e.g., >5% → CRITICAL). This would have fired at `00:35` instead of `01:03` — saving 28 minutes of customer impact.

### Long-term (Preventive)
4. **Schema registry CI gate:** Any source DB migration (like DATA-1847) must query the registry for downstream consumers and auto-open change tickets before the migration is applied.
5. **Contract testing in staging:** Run the full pipeline in staging against the new schema version before promoting to production. A rename would have caused an immediate test failure.
6. **Cross-team schema change notification:** The DATA-1847 ticket should have triggered automatic notification to the pipeline team with a mandatory sign-off before execution.

---

## 6. Confidence

**🟢 HIGH**

All four diagnostic pillars align perfectly:
- **Schema event** at `00:30:05` (exact column, exact table, ticket number)
- **Null_rate step-change** precisely at the next 5-minute window after the rename (`00:35`)
- **Ingestion WARN logs** explicitly naming the missing column (`stock_count`)
- **Row counts, freshness, CPU all normal** — no alternative resource/scheduling explanation possible
- **Revenue pipeline clean** — confirms blast radius is isolated to the inventory chain

The only data that would further increase confidence would be the actual source DB migration log confirming the exact moment `available_qty` replaced `stock_count` at the Postgres/MySQL level, but the schema registry event is a reliable proxy for that.