# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T21:04:07.714744

**Problem:** The inventory dashboard shows every product as out of stock, but sales data looks fine. Please investigate and diagnose the root cause.

---

I now have a complete, high-confidence picture. Here is the full diagnosis report.

---

# 🔬 Pipeline Doctor — Diagnosis Report

## 1. Summary

**`dashboard.inventory_health` is showing every product as out of stock.** The root cause is a **broken schema contract**: the source database renamed the stock quantity column from `stock_count` → `available_qty`, but the `ingest_inventory` pipeline job was never updated to consume the new column name. As a result, `ingest_inventory` silently filled the column with `NULL` for every row, and that null propagated downstream through `raw.inventory_snapshot` → `mart.product_availability` → `dashboard.inventory_health`. The dashboard interprets NULL stock quantities as zero/out-of-stock.

**The revenue/orders pipeline is completely unaffected.** This is an isolated, inventory-chain-only schema break.

---

## 2. Root Cause

| Attribute | Detail |
|---|---|
| **Schema change** | Column renamed: `stock_count` → `available_qty` |
| **Source table** | `source.orders_db.inventory` |
| **Schema version bump** | `2.2.0` → `2.3.1` |
| **Migration ticket** | `DATA-1847` |
| **Change timestamp** | **2026-06-07 00:13:27** |
| **Offending job** | `ingest_inventory` — still hardcoded to read `stock_count` |
| **Failure mode** | Job ran successfully but wrote `NULL` into `raw.inventory_snapshot.stock_count` for every row because the source column no longer exists under that name |

The job did **not crash** — it silently degraded. This is the most dangerous kind of schema break. The job logs emitted `WARN`-level messages starting at **00:18:27**: `"Column 'stock_count' not found in source.orders_db.inventory; filling NULL. (source schema now 2.3.1)"` — but no alert was wired to job-log WARNs.

A second registry entry (`DATA-1851`) at **00:48:27** shows that the `raw.inventory_snapshot` schema was eventually updated to reflect the rename, but by that point the damage had already propagated all the way to the dashboard.

---

## 3. Lineage / Propagation

```
source.orders_db.inventory
  [schema change: stock_count → available_qty @ 00:13:27, ticket DATA-1847]
        │
        ▼  ingest_inventory (WARN @ 00:18:27: col not found, writing NULL)
raw.inventory_snapshot  ← null_rate(stock_count) spikes to 97%+
        │
        ▼  transform_inventory
mart.product_availability  ← null_rate(available) spikes to 97%+
        │
        ▼  refresh_inventory_dashboard
dashboard.inventory_health  ← ALL products show "out of stock" 🔴
```

| Timestamp | Event |
|---|---|
| **00:13:27** | `source.orders_db.inventory` schema changes: `stock_count` → `available_qty` (v2.2.0 → v2.3.1, DATA-1847) |
| **00:18:27** | `ingest_inventory` first WARN: `"Column 'stock_count' not found"` — null-fill begins |
| **~00:18–00:45** | `raw.inventory_snapshot.stock_count` null_rate climbs from <0.5% to 97%+ |
| **~00:46** | `mart.product_availability.available` null_rate breaches 5% threshold; critical alerts fire |
| **00:46–00:48** | `dashboard.inventory_health` BusinessImpact alerts: all products out of stock |
| **00:48:13** | Critical DataQualityFailure alert: `null_rate = 97%`, `row_count normal` |
| **00:48:27** | `ingest_inventory` patched to read `available_qty` (alias `stock_count`); backfill starts (DATA-1851) |
| **00:56–00:58** | null_rate falls back below threshold; RecoveryDetected alerts fire |

---

## 4. Evidence

### ✅ Ruling Out Resource / Volume / Scheduling Causes

| Signal | Observation | Conclusion |
|---|---|---|
| `ingest_inventory` job status | `success` throughout — **no failures or crashes** | Not a job crash |
| `rows_in` / `rows_out` | Consistent ~11,700–12,400 rows in and out; **no volume drop** | Not a data volume issue |
| `duration_ms` | Normal range (36,000–62,000 ms) — **no slowdown** | Not a resource saturation issue |
| `worker_cpu_pct` | 10–38% — well within normal | Not CPU starvation |
| `raw.inventory_snapshot` row_count DQ | **All passing** (12,000+ rows vs 8,400 threshold) | Rows are arriving; only the column is broken |
| Revenue pipeline DQ | **Zero failures** across `raw.orders`, `mart.daily_revenue` | Blast radius is inventory chain only |

> **These signals unambiguously rule out an infrastructure, scheduling, or volume problem.** Full row counts with a column-level null_rate spike is the textbook fingerprint of a schema/contract break.

### 🔴 The Schema Smoking Gun

```
Schema Registry Event @ 2026-06-07 00:13:27
  table:        source.orders_db.inventory
  change_type:  rename_column
  old_field:    stock_count
  new_field:    available_qty
  ticket:       DATA-1847
  version:      2.2.0 → 2.3.1
```

```
Job Log WARNs @ 2026-06-07 00:18:27 (and every run thereafter)
  job_name: ingest_inventory
  level:    WARN
  message:  "Column 'stock_count' not found in source.orders_db.inventory;
             filling NULL. (source schema now 2.3.1)"
  status:   success  ← job did NOT fail, so no pager alert fired
```

```
DQ Check (null_rate peak)
  table:      raw.inventory_snapshot / mart.product_availability
  column:     stock_count / available
  null_rate:  97%  (threshold: 5%)
  row_count:  NORMAL (passing) ← confirms this is not a missing-data problem
```

---

## 5. Remediation

### 🚑 Immediate Fix (already in progress)
The patch alert (`PipelinePatched @ 00:48:27`) confirms `ingest_inventory` has been updated to read `available_qty` (aliased back to `stock_count`). A backfill is running. Monitor `raw.inventory_snapshot.stock_count` null_rate — the `RecoveryDetected` alerts already show it declining toward baseline. **Verify `mart.product_availability` refreshes fully before declaring the dashboard recovered.**

### 🛡️ Prevention — Short Term
| Action | Detail |
|---|---|
| **Alert on job-log WARNs** | Wire a Splunk alert: `sourcetype=pipeline:job_log level=WARN message="not found"` → PagerDuty. This incident went undetected for ~5 minutes because the job status remained `success`. |
| **Break the job on missing critical columns** | `ingest_inventory` should raise an exception (not a warning) when a required column is absent. Fail fast; don't silently null-fill. |
| **Schema-change gate** | Require all `rename_column` / `drop_column` events in `pipeline:schema_registry` to trigger a downstream consumer notification before the change is applied. Ticket DATA-1847 should have blocked until `ingest_inventory` was updated. |

### 🏗️ Prevention — Long Term
| Action | Detail |
|---|---|
| **Schema contracts / compatibility checks in CI** | Use a schema registry (e.g. Confluent Schema Registry, dbt `contract:` blocks) with **BACKWARD compatibility enforcement**. A rename would be rejected until all downstream consumers acknowledge it. |
| **Column-level DQ alerting** | The null_rate DQ check existed but only fired ~33 minutes after the schema change. Set the alert threshold check interval to ≤2 minutes for critical columns like `stock_count`. |
| **Lineage-aware impact analysis** | Before applying any source schema change, query the lineage graph to enumerate all downstream consumers (as done in this investigation) — and send an automated impact report to owning teams. |

---

## 6. Confidence

**🟢 HIGH**

Every link in the causal chain is directly evidenced:

- ✅ Exact schema change event with timestamp, old/new field names, and ticket number
- ✅ Job WARN logs naming the missing column by name, beginning 5 minutes after the schema change
- ✅ null_rate spike correlated precisely to the schema change — not to any resource, volume, or scheduling anomaly
- ✅ row_count DQ checks passing throughout — eliminating all volume/infrastructure hypotheses
- ✅ Revenue pipeline 100% clean — confirming blast radius is inventory-chain-only
- ✅ Recovery already underway, consistent with the patch described in the alert log

The only data point that would add marginal confidence would be a sample of actual NULL rows from `raw.inventory_snapshot` during the incident window, but the available evidence is already conclusive.