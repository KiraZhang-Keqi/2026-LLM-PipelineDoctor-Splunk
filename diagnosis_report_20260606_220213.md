# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T22:02:13.766903

**Problem:** Business users are complaining that the inventory dashboard hasn't updated since yesterday. The numbers look plausible but appear to be stale. No jobs are showing as failed. Please investigate.

---

I now have the complete timeline. I can see the full escalation curve. Let me check the recovery point and confirm the pipeline is back to normal now.

The data is complete. Here is the full diagnosis report.

---

# 🩺 Pipeline Doctor — Diagnosis Report
**Case:** Stale `dashboard.inventory_health` | **Date:** 2026-06-07 | **Confidence: HIGH**

---

## 1. 📋 Summary

The `dashboard.inventory_health` dashboard went stale for approximately **~35 minutes** (roughly 01:20–01:52 UTC-4). Every job in the inventory pipeline continued reporting `status=success`, which is why no failure alerts fired — this is a **silent slowdown**, not a crash. The root cause is a **source-database query block on `source.orders_db.inventory`** that caused `ingest_inventory` run durations to balloon from a ~45-second baseline to over **13 minutes at peak**, starving all downstream transforms and the dashboard refresh of fresh data. The pipeline has since self-recovered.

---

## 2. 🔍 Root Cause

> **A query-level lock/block on `source.orders_db.inventory` caused `ingest_inventory` to stall, propagating staleness across the entire inventory pipeline. No schema change was involved.**

This is a **source-side contention event**, not a schema/contract break, not a resource (CPU/memory) problem on the pipeline workers, and not a job failure.

Key proof:
- `ingest_inventory` logs explicitly state: *"Slow read from `source.orders_db.inventory`"* — repeatedly from **01:10 onwards**
- After the block cleared: *"Source query unblocked. Backfill completing. Freshness recovering."*
- The schema registry returned **zero non-baseline changes** — no rename, drop, or type change occurred
- Worker CPU stayed between **20–34%** throughout — workers were idle, waiting on I/O from the source DB, not under load

---

## 3. 🔗 Lineage / Propagation

```
source.orders_db.inventory
        │
        │  ingest_inventory ← BLOCKED HERE (slow source read)
        ▼
raw.inventory_snapshot          ← freshness_lag breaches 20-min threshold at ~01:20
        │
        │  transform_inventory  ← stalled, waiting on stale upstream
        ▼
mart.product_availability       ← freshness_lag breaches threshold ~5 min later
        │
        │  refresh_inventory_dashboard
        ▼
dashboard.inventory_health      ← CRITICAL StaleData alert fires; lag peaks at ~59 min
```

| Time (UTC-4) | Event |
|---|---|
| 00:55–01:10 | ✅ Normal — `ingest_inventory` avg duration ~40–52 sec (baseline) |
| **01:10** | ⚠️ First WARN: *"Slow read from source.orders_db.inventory"* (24,586 ms) |
| 01:11–01:19 | ⚠️ Escalating WARNs — duration climbs: 63k → 99k → 128k → 276k ms |
| **01:20** | 🔴 Duration spikes to **~732 seconds avg** — full lock contention |
| 01:20–01:26 | 🔴 `raw.inventory_snapshot` freshness_lag hits 20+ min; DQ fail fires |
| 01:25–01:45 | 🔴 `mart.product_availability` freshness_lag climbs to ~50 min |
| 01:43–01:44 | 🔴 `dashboard.inventory_health` CRITICAL StaleData alert: **~57–59 min stale** |
| **01:52** | ✅ `FreshnessRecovered` alert fires: *"freshness_lag_min 17.7 back below threshold"* |
| Post-01:52 | ✅ All jobs back to normal durations; row_counts, null_rates all passing |

The **revenue pipeline was completely unaffected throughout** — `mart.daily_revenue` and `raw.orders` freshness_lag never exceeded ~8 minutes and stayed green the entire time. This confirms the problem was isolated to `source.orders_db.inventory` — not a shared platform issue.

---

## 4. 🧪 Evidence

### ✅ What RULES OUT schema / data-quality / resource causes:

| Signal | Value | Interpretation |
|---|---|---|
| `schema_registry` non-baseline changes | **0 events** | No rename, drop, type-change on any table |
| `null_rate` checks (all inventory columns) | **All PASSING** — values ~0.001–0.004 | No column went null; data contract is intact |
| `row_count` checks (inventory chain) | **All PASSING** — raw ~11,800, mart ~12,000 | Volume is normal; not a source data loss |
| Worker CPU (`ingest_inventory`) | **20–34%** throughout | Workers were idle/blocked on I/O, not overloaded |
| Revenue pipeline freshness | **< 8 min throughout** (green) | Shared infrastructure was healthy |

### 🔴 What CONFIRMS the source-query block:

| Signal | Value |
|---|---|
| WARN log first appearance | **01:10:08** — *"Slow read from source.orders_db.inventory"* |
| Duration escalation | 45 sec → **847 sec** (18× baseline) by 01:27 |
| Peak avg duration | **~848,000 ms (~14 min)** at 01:27 |
| Ingest recovery message | *"Source query unblocked. Backfill completing. Freshness recovering."* ~01:52 |
| `FreshnessViolation` alert | `raw.inventory_snapshot`, threshold 20 min, peak **59.5 min** |
| `StaleData` CRITICAL alert | `dashboard.inventory_health`, **59 min stale** at 01:44 |

---

## 5. 🛠️ Remediation

### Immediate (Done — pipeline self-recovered)
- ✅ The block has cleared; `FreshnessRecovered` alert fired at 01:52
- ✅ Confirm with business users that dashboard numbers are now current
- 🔍 **Investigate `source.orders_db.inventory`** — identify what caused the lock (long-running analytical query, batch ETL, schema migration, index rebuild, or VACUUM on the source table). Check DB query logs / pg_stat_activity / slow-query logs around 01:10–01:52

### Short-Term (Within 1–2 sprints)
1. **Source query timeout / circuit breaker**: Set a hard query timeout on `ingest_inventory`'s source read (e.g., 90 seconds). If the read exceeds it, fail fast, raise a proper `ERROR` alert, and retry — rather than silently succeeding with stale data after 14 minutes
2. **Job duration SLA alert**: Add a Splunk alert: if `avg(duration_ms) > 120000` for `ingest_inventory` in any 5-minute window → page on-call. This would have fired at 01:11, 35+ minutes before the CRITICAL StaleData alert
3. **Source DB read-replica / dedicated ingest user**: Route `ingest_inventory` through a read-replica or a connection with resource limits to isolate it from transactional locks on the primary

### Long-Term (Prevention)
4. **Source-side observability**: Instrument the source DB (pg_locks, pg_stat_activity) and emit events to Splunk so lock contention is visible before it propagates into the pipeline
5. **Freshness SLA tightening**: The 20-minute freshness threshold at `raw.inventory_snapshot` means the WARN fires ~10 minutes after the ingest stall. A 10-minute threshold would give an earlier signal
6. **Pipeline topology documentation**: The lineage data (`pipeline:lineage`) is clean and accurate — make sure it's surfaced in dashboards so on-call engineers can quickly identify that a `dashboard.inventory_health` staleness event means "look at `ingest_inventory` → `source.orders_db.inventory`" without a full investigation

---

## 6. 🎯 Confidence: HIGH

All four evidence pillars are satisfied:
- ✅ Root cause explicitly named in job logs (*"Slow read from source.orders_db.inventory"*)
- ✅ Schema change conclusively ruled out (zero registry events)
- ✅ Data quality (null_rate, row_count) conclusively ruled out (all passing throughout)
- ✅ Resource/CPU conclusively ruled out (20–34% CPU, workers were idle not overloaded)
- ✅ Revenue pipeline as a clean control group confirms it was isolated to the inventory source DB

**What would raise it further:** Access to the source DB's own query logs / lock-wait logs to identify *what specifically* held the lock (e.g., a long-running migration, an unscheduled analytics query, a batch job), and the name of the offending session. That context would turn this from *"source DB lock"* to *"DBA team's nightly index rebuild overran its window"* — and enable scheduling controls to prevent recurrence.