# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T22:21:47.337262

**Problem:** Business users are complaining that the inventory dashboard hasn't updated since yesterday. The numbers look plausible but appear to be stale. No jobs are showing as failed. Please investigate.

---

I now have a complete, fully-evidenced picture. Here is the definitive diagnosis report.

---

# 🩺 Pipeline Doctor — Diagnosis Report
## `dashboard.inventory_health` — Stale Data Incident

---

## 1. Summary

**The inventory dashboard served stale data for approximately 30–35 minutes** (roughly 01:40–02:12 UTC-4 on 2026-06-07) because the `ingest_inventory` job was held up waiting for data from the upstream source database (`source.orders_db.inventory`). The source query became progressively slower starting at **01:30**, then fully stalled at **01:40:23** — producing **zero rows** and no output for ~30 minutes. Jobs remained in `status=running` (not `failed`), which is why no failure alerts fired. The pipeline has since self-recovered and is backfilling.

**This is NOT a schema/data-contract problem.** It is a **source database query performance event** — most likely a lock contention, long-running transaction, or index stall on `source.orders_db.inventory` that blocked all reads.

---

## 2. Root Cause

> **A query block on `source.orders_db.inventory`** caused `ingest_inventory` to stall with 0 rows in/out, starving the entire downstream inventory pipeline of fresh data.

| Attribute | Value |
|---|---|
| **Affected source** | `source.orders_db.inventory` |
| **Affected ingest job** | `ingest_inventory` |
| **Degradation onset** | 01:30:23 — slow reads begin (36s–280s, baseline 45s) |
| **Full block onset** | **01:40:23** — `rows_in=0, rows_out=0`, status=`running` |
| **Block duration** | ~30 minutes (until ~02:10–02:12) |
| **Peak job duration** | **895,479 ms** (~15 minutes per attempt) |
| **Recovery confirmed** | 02:12:03 — "Source query unblocked. Backfill completing." |

No schema changes were found in `pipeline:schema_registry` — the registry is clean with zero non-baseline events.

---

## 3. Lineage / Propagation

The stall propagated downstream through the full inventory chain:

```
source.orders_db.inventory  ← BLOCKED HERE (source DB query stall)
        │
        ▼ [ingest_inventory]  ← Stalled at 01:40:23, status=running, 0 rows
raw.inventory_snapshot        ← Freshness lag hit 61.9 min (threshold: 20 min)
        │
        ▼ [transform_inventory]  ← Starved of input; downstream lag grew
mart.product_availability     ← Freshness lag hit ~44 min
        │
        ▼ [refresh_inventory_dashboard]
dashboard.inventory_health    ← CRITICAL StaleData alert: "serving data from 61 minutes ago"
```

**Timestamp cascade:**

| Time (UTC-4) | Event |
|---|---|
| 01:30:23 | First WARN: slow reads from `source.orders_db.inventory` (~37s vs 45s baseline) |
| 01:35–01:40 | Reads progressively slower (178s → 280s); freshness lag creeps up |
| **01:40:23** | **Full block begins**: `ingest_inventory` status=`running`, rows_in=0, rows_out=0 |
| ~01:40 | `raw.inventory_snapshot` freshness_lag_min crosses 20-min threshold → DQ FAIL |
| ~01:45 | `mart.product_availability` freshness lag > 30 min |
| ~02:00–02:05 | Peak lag: `raw.inventory_snapshot` ~57 min, `mart.product_availability` ~51 min |
| ~02:03–02:05 | `FreshnessViolation` (warning) on `raw.inventory_snapshot`; `StaleData` (critical) on `dashboard.inventory_health` |
| **02:12:03** | **Block clears**: "Source query unblocked. Backfill completing. Freshness recovering." |
| 02:12:03 | `FreshnessRecovered` alert on `raw.inventory_snapshot` (lag back to 19.6 min ✅) |
| ~02:15 | All freshness lags back to normal (5–7 min), pipeline healthy |

---

## 4. Evidence

### ✅ Signals that RULE OUT schema / data-contract cause
| Check | Finding | Conclusion |
|---|---|---|
| `pipeline:schema_registry` non-baseline changes | **0 events** — completely empty | No column renames, drops, or type changes |
| `null_rate` across all inventory tables | **Flat throughout** (~0.002, never spiked) | No column-level data contract breakage |
| `row_count` on `raw.inventory_snapshot` & `mart.product_availability` | **Stable at ~12,000 / ~11,800** throughout incident and recovery | Not a volume/truncation problem |
| Worker CPU & memory during block | **Normal to low** (17–46% CPU, 24–52% mem) | Not a compute resource exhaustion |
| Revenue pipeline (`ingest_orders`, `transform_revenue`, `refresh_revenue_dashboard`) | **Healthy throughout** — all `status=success`, normal durations | Platform-wide outage ruled out; problem is inventory-source-specific |

### 🔴 Signals that CONFIRM the source query block
| Evidence | Detail |
|---|---|
| WARN logs starting 01:30 | "Slow read from `source.orders_db.inventory`" — 19 WARN events, duration escalating from 36s to 280s |
| Status=`running` with 0 rows | At 01:40:23, `ingest_inventory` enters a hung state: `rows_in=0, rows_out=0`, duration climbing to **895,479 ms** |
| Peak freshness lag | `raw.inventory_snapshot` hit **61.9 min** (threshold: 20 min); `mart.product_availability` hit **~44 min** |
| Recovery message | Exact log: *"Source query unblocked. Backfill completing. Freshness recovering."* — appeared on 23 consecutive successful runs starting ~02:10 |
| Alert timeline | `FreshnessViolation` + `StaleData` (critical) fired from ~02:03 to 02:12; `FreshnessRecovered` (info) at 02:12:03 |

---

## 5. Remediation

### Immediate (Resolved — but verify)
- ✅ The pipeline has self-recovered. Confirm with business users that the dashboard now reflects current data.
- 🔍 **Investigate `source.orders_db.inventory`** — check the DB's slow query log, lock wait events, or VACUUM/ANALYZE history for the 01:30–02:10 window. A long-running DML transaction, a missing index, or an unvacuumed table are the most likely culprits.
- 🔍 Check whether any batch job, reporting query, or schema migration ran against `source.orders_db.inventory` around 01:30.

### Short-term (Within 1 sprint)
- **Add a `duration_ms` threshold alert** on `ingest_inventory`. If a run exceeds 2× baseline (e.g., >90s), fire a warning *before* freshness breaches. The escalation from first slow read to full block took only ~10 minutes — early alerting would have given operators time to intervene.
- **Add a `rows_in=0` / `status=running` alert**: a job that is running but producing zero output for >2 minutes should fire an immediate page, not silently wait.
- **Set a query timeout** on the source DB connection in `ingest_inventory`. A stalled query that times out and retries will surface as a `status=fail` (visible and alertable) rather than silently blocking as `status=running`.

### Longer-term (Prevention)
- **Source DB read replica**: route `ingest_inventory`'s reads to a read replica of `orders_db` to insulate the pipeline from lock contention caused by OLTP writes or maintenance operations.
- **Circuit breaker pattern**: if `ingest_inventory` fails to produce rows for N consecutive attempts, automatically raise a `CRITICAL` alert and stop downstream jobs rather than letting stale data silently flow to dashboards.
- **Dashboard staleness indicator**: display a "last updated" timestamp prominently on `dashboard.inventory_health` so business users can self-identify staleness without needing to file a ticket.

---

## 6. Confidence

**🟢 HIGH**

Every signal aligns cleanly with a single root cause:
- The schema registry is completely clean (zero non-baseline events) — schema break is fully excluded.
- Null rates are flat — data contract problems are fully excluded.
- Row counts and worker resources are normal — volume and compute problems are fully excluded.
- The revenue pipeline was healthy throughout — platform-wide outage is fully excluded.
- The job logs name the cause explicitly: *"Slow read from source.orders_db.inventory"* and *"Source query may be blocked"*, followed by *"Source query unblocked"* on recovery.

**The only open question** is the precise DB-side mechanism (lock contention, long transaction, maintenance job, etc.) — that requires inspection of `source.orders_db` server logs, which are outside this Splunk observability dataset. That investigation should be the next step for the database/platform team.