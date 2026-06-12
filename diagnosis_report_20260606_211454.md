# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T21:14:54.631150

**Problem:** The inventory dashboard shows every product as out of stock, but sales data looks fine. Please investigate and diagnose the root cause.

---

I now have a complete, fully-corroborated picture. I have everything I need to deliver a definitive diagnosis. Here is the full report:

---

# 🩺 Pipeline Doctor — Diagnosis Report

## 1. Summary

**What broke:** The `dashboard.inventory_health` dashboard showed every product as "out of stock" due to a cascading null-value failure flowing through the entire inventory pipeline chain.

**User-visible impact:** 100% of products appeared out of stock on the inventory dashboard, directly affecting merchandising and checkout availability decisions. The `BusinessImpact` alert fired at **00:57 – 00:59 UTC-4** on 2026-06-07. A patch was deployed at **00:59:46** and recovery was confirmed by **01:08–01:09**.

---

## 2. Root Cause

> **A breaking schema rename in the upstream source database was not propagated to the `ingest_inventory` pipeline job.**

| Detail | Value |
|---|---|
| **Schema Change** | `stock_count` → `available_qty` (column rename) |
| **Source Table** | `source.orders_db.inventory` |
| **Schema Version** | `2.3.1` |
| **Migration Ticket** | `DATA-1847` |
| **Change Applied At** | `2026-06-07 00:24:46 UTC-4` |
| **Offending Job** | `ingest_inventory` |
| **Job's Bad Behavior** | Kept reading `stock_count` — a column that no longer exists — and silently filled `NULL` for every row |

The `ingest_inventory` job had a hard-coded column reference to `stock_count`. When the source DB team renamed it to `available_qty` (ticket `DATA-1847`), the pipeline was **never updated**. The job continued to run successfully (exit code 0) but produced an entirely null `stock_count` column in `raw.inventory_snapshot`. This null then propagated downstream as null `available` values in `mart.product_availability`, rendering every product as out-of-stock on the dashboard.

---

## 3. Lineage / Propagation

```
source.orders_db.inventory
  ── [schema rename: stock_count → available_qty at 00:24:46] ──▶  BREAK ORIGIN
        │
        ▼  [ingest_inventory — reads missing 'stock_count', fills NULL]
raw.inventory_snapshot          (column: stock_count  → null_rate spikes from 00:29:46)
        │
        ▼  [transform_inventory]
mart.product_availability       (column: available    → null_rate spikes, ~97% at peak)
        │
        ▼  [refresh_inventory_dashboard]
dashboard.inventory_health      ← 💥 BusinessImpact alert fires (00:57–00:59)
                                   "ALL products out of stock"
```

| Timestamp | Event |
|---|---|
| `00:24:46` | Schema rename committed to `source.orders_db.inventory` (ticket DATA-1847) |
| `00:29:46` | **First WARN** from `ingest_inventory`: *"Column 'stock_count' not found... filling NULL"* |
| `~00:30–00:57` | `raw.inventory_snapshot.stock_count` null_rate climbs from ~0% → 97% |
| `~00:57–00:59` | `mart.product_availability.available` null_rate hits 97%; critical alerts fire |
| `00:59:46` | Patch deployed: `ingest_inventory` updated to read `available_qty` (aliased as `stock_count`); backfill starts |
| `01:08–01:09` | Null rate falls back below threshold; `RecoveryDetected` alerts confirm restoration |

---

## 4. Evidence

### ✅ Schema Change — The Smoking Gun
```
sourcetype=pipeline:schema_registry
  _time=00:24:46  table=source.orders_db.inventory
  change_type=rename_column  old_field=stock_count  new_field=available_qty
  schema_version=2.3.1  migration_ticket=DATA-1847
```
A second registry event at `00:59:46` shows `raw.inventory_snapshot` also updated to v2.3.1 post-patch.

### ✅ Job WARN Logs — Named the Missing Column Explicitly
```
sourcetype=pipeline:job_log  job_name=ingest_inventory  level=WARN
  _time=00:29:46  (and every run thereafter)
  message="Column 'stock_count' not found in source.orders_db.inventory;
           filling NULL. (source schema now 2.3.1)"
```

### ✅ Null Rate Cascade
| Table | Column | Peak null_rate | Threshold |
|---|---|---|---|
| `raw.inventory_snapshot` | `stock_count` | **97%** | 5% |
| `mart.product_availability` | `available` | **97%** | 5% |

### ❌ Resource / Volume Cause — RULED OUT
All of the following were normal throughout the incident, definitively ruling out a capacity, scheduling, or data-volume problem:

| Check | Result |
|---|---|
| `ingest_inventory` job status | ✅ `success` on every run (no crash, no timeout) |
| `duration_ms` | ✅ Normal range (~24k–55k ms, consistent with baseline) |
| `worker_cpu_pct` | ✅ 13–38% — well within normal bounds |
| `raw.inventory_snapshot` row_count | ✅ PASS (~11,600–12,700 rows, above 8,400 threshold) |
| `mart.product_availability` row_count | ✅ PASS (~12,000 rows, above 8,260 threshold) |
| `freshness_lag_min` (both tables) | ✅ PASS (< 11 min, well under 20-min threshold) |
| Revenue pipeline (orders chain) | ✅ Entirely unaffected — isolates fault to inventory branch |

**The job processed the right number of rows on time with healthy CPU — but the values were null. That is the signature of a schema contract break, not a resource failure.**

---

## 5. Remediation

### 🔥 Immediate Fix (already applied)
- `ingest_inventory` was patched at `00:59:46` to read `available_qty` and alias it as `stock_count` for downstream compatibility.
- A backfill was triggered; null_rate recovered below threshold by `01:09`.

### 🔧 Short-Term (within 1 sprint)
1. **Rename the column consistently end-to-end:** Update `transform_inventory` and `mart.product_availability` to use `available_qty` natively rather than relying on an alias, so the schema contract is explicit at every layer.
2. **Open a follow-up ticket** (reference `DATA-1847`/`DATA-1851`) to audit all other jobs for any remaining references to `stock_count`.

### 🛡️ Long-Term Prevention
| Mechanism | Description |
|---|---|
| **Schema Contract Tests in CI** | Before deploying any DB schema change, a CI gate should validate that all consuming pipeline jobs reference only currently-valid column names. Tools: dbt tests, Great Expectations, or a custom schema contract linter. |
| **Schema Change Alerts** | Fire an alert the moment a `rename_column` or `drop_column` event lands in `pipeline:schema_registry` so the pipeline team is notified *before* the null_rate spikes — not 30 minutes later. |
| **Fail-Fast on Missing Columns** | `ingest_inventory` should throw a hard **ERROR** (not a WARN) and halt when an expected column is absent. Silent null-filling turns a loud, fast failure into a slow, invisible data-corruption event. |
| **Schema Version Pinning** | Pipeline jobs should declare the schema version they were written against. A version mismatch at startup should block execution. |
| **Lineage-aware Impact Analysis** | Before approving ticket `DATA-1847`, the schema registry should surface which downstream jobs and dashboards depend on `stock_count` — this blast radius was predictable. |

---

## 6. Confidence: 🟢 HIGH

Every layer of evidence agrees and points to the same cause:
- ✅ Schema registry records the exact rename with a timestamp **before** the first WARN log
- ✅ Job WARN logs name `stock_count` as the missing column verbatim
- ✅ Null_rate spike is isolated to `stock_count` / `available` — no other columns failed
- ✅ Row counts, freshness, CPU, and job status were all normal — resource causes fully excluded
- ✅ The revenue pipeline (separate lineage branch) was completely healthy throughout
- ✅ The patch message confirms the fix: *"Backfilling stock_count from available_qty"*

The only thing that would further increase confidence is a direct query log from `source.orders_db.inventory` confirming the rename timestamp — but the schema registry event at `00:24:46` is a sufficient and authoritative source.