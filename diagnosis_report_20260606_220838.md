# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T22:08:38.424397

**Problem:** Business users are complaining that the inventory dashboard hasn't updated since yesterday. The numbers look plausible but appear to be stale. No jobs are showing as failed. Please investigate.

---

I now have a complete, coherent picture of the incident. Here is the full diagnosis report.

---

# 🩺 Pipeline Doctor — Diagnosis Report

**Incident:** `dashboard.inventory_health` serving stale data
**Investigation Time:** 2026-06-07 ~01:17–01:58 UTC-4
**Status:** ✅ **Resolved / Recovering** at time of investigation

---

## 1. Summary

The `dashboard.inventory_health` dashboard served stale inventory data for approximately **40 minutes** (roughly 01:25–01:58). The staleness peaked at **61 minutes of lag** against a 20-minute freshness threshold. **No jobs failed outright**, which is why no failure alerts fired — the pipeline kept completing, but each run was taking far longer than normal, causing data to fall behind the refresh cadence. The issue is now fully resolved and backfilling.

---

## 2. Root Cause

**A source-side query slowdown on `source.orders_db.inventory` caused the `ingest_inventory` job's read duration to escalate from a normal ~45,000ms to a peak of ~780,000ms (13× baseline).**

This is a **source database contention / slow query event** — not a schema change, not a worker resource exhaustion, and not a pipeline code bug. The `ingest_inventory` job itself logged exactly this:

> *"Slow read from source.orders_db.inventory"* — first appearing at **01:17:24**

Because each run took so long, the pipeline fell further and further behind its schedule, causing the downstream chain (`raw.inventory_snapshot` → `mart.product_availability` → `dashboard.inventory_health`) to serve progressively older data. Eventually the source contention cleared, and the job recovered with *"Source query unblocked. Backfill completing. Freshness recovering."* messages from ~01:55 onward.

---

## 3. Lineage / Propagation

The staleness followed the lineage chain exactly as expected, with `raw.inventory_snapshot` lagging first (it's closest to the source), then `mart.product_availability` lagging ~5 minutes behind it:

| Time Window | `raw.inventory_snapshot` Lag | `mart.product_availability` Lag | Revenue Pipeline Lag |
|---|---|---|---|
| 01:00–01:15 | ~4–7 min ✅ normal | ~5–7 min ✅ normal | ~5–8 min ✅ normal |
| 01:25 | **16 min** ⚠️ rising | **12 min** ⚠️ rising | 5.7 min ✅ unaffected |
| 01:30 | **29 min** 🔴 breach | **25 min** 🔴 breach | 6.1 min ✅ unaffected |
| 01:35–01:45 | **41–53 min** 🔴 critical | **37–48 min** 🔴 critical | ~5–6 min ✅ unaffected |
| 01:50 (peak alert) | **61 min** 🔴 peak alert fired | **58 min** 🔴 | 5.1 min ✅ unaffected |
| 01:55–02:02 | Recovering ↓25 min → 10 min | Recovering ↓23 min → 9 min | ✅ stable throughout |

**Critical observation:** `raw.orders` / revenue pipeline stayed flat at 5–8 min throughout the entire incident. This **rules out a shared infrastructure problem** — the slowdown was isolated purely to `source.orders_db.inventory`.

---

## 4. Evidence

### ✅ What RULES OUT a schema break
- **Zero schema registry events:** `sourcetype=pipeline:schema_registry change_type!=baseline` returned **0 results**. No renames, drops, type changes, or additions occurred.
- **Zero null_rate DQ failures:** All `null_rate` checks on `raw.inventory_snapshot` and `mart.product_availability` were **passing** throughout — columns like `stock_count`, `product_id`, `warehouse_id` remained healthy. A schema break would have caused null spikes.
- **Row counts were normal:** `row_count` DQ checks on `raw.inventory_snapshot` and `mart.product_availability` consistently passed throughout the incident (~11,000–12,000 rows), ruling out a data volume drop.

### ✅ What RULES OUT a worker/infrastructure outage
- **Revenue pipeline was completely unaffected**, staying at 5–8 min freshness throughout — a shared infrastructure problem would have hit both pipelines.
- **Worker CPU was within normal range** during the early phase of the incident (25–40%), and even at peak only reached ~47% — not CPU-bound.
- **No jobs failed** — the job runner kept completing each run, it was just slow.

### 🔴 What CONFIRMS the source database contention cause
| Data Point | Value |
|---|---|
| First WARN log | `2026-06-07T01:17:24` — *"Slow read from source.orders_db.inventory"* |
| Ingest duration at baseline | ~45,000ms (01:00–01:15) |
| Ingest duration at peak contention | ~780,000ms avg (01:30–01:45) — **17× baseline** |
| Recovery log message | *"Source query unblocked. Backfill completing."* from ~01:55 |
| Freshness alert fired | `01:49` — `dashboard.inventory_health` stale by 61 min (critical) |
| Freshness recovered alert | `01:58:58` — `raw.inventory_snapshot` freshness back below 20 min threshold |
| Revenue pipeline throughout | ✅ 5–8 min lag — **completely isolated to inventory source** |

### Timeline Reconstruction

```
01:17  → ingest_inventory starts logging WARN: "Slow read from source.orders_db.inventory"
         Duration creeping up: 38s → 75s → 98s → 139s → 172s...
01:25  → Freshness lag on raw.inventory_snapshot crosses ~16 min (approaching 20 min threshold)
01:30  → Freshness lag breaches 20-min SLA on both raw.inventory_snapshot (29 min) AND
         mart.product_availability (25 min) — downstream propagation confirmed
01:30–01:50 → ingest_inventory duration averaging 730–780 seconds (12–13 min per run!)
              Pipeline can't keep up. Freshness lag compounds with each slow cycle.
01:49–01:52 → FreshnessViolation (WARNING) + StaleData (CRITICAL) alerts fire for
              dashboard.inventory_health; peak lag 61 minutes reported
~01:52  → Source contention begins to clear; ingest_inventory duration starts dropping
01:55  → ingest_inventory logs "Source query unblocked. Backfill completing."
          Freshness lag already dropping: raw.inventory_snapshot ~25 min, mart ~23 min
01:58:58 → FreshnessRecovered alert fires: freshness_lag_min 18.9 < 20 min threshold ✅
02:02  → All jobs reporting INFO "success" with normal durations (~37–54s). Full recovery.
```

---

## 5. Remediation

### Immediate (already auto-resolved, but verify)
- ✅ Confirm `dashboard.inventory_health` is now showing current data (freshness lag < 20 min — confirmed by `FreshnessRecovered` alert at 01:58).
- 📣 Notify business users that the dashboard is live and the data gap (01:25–01:58) has been backfilled.
- 🔍 **Investigate `source.orders_db.inventory`** for what caused the 40-minute read slowdown: look for long-running transactions, lock contention, a missing index, a poorly-timed batch job, or a maintenance window on the source DB side. This is the next investigation that needs to happen — it is outside the pipeline itself.

### Short-Term (prevent recurrence)
| Action | Detail |
|---|---|
| **Add a source-query duration alert** | Alert when `ingest_inventory` `duration_ms` exceeds 2× baseline (90,000ms) for 2+ consecutive runs. Currently you only get a freshness alert after lag has already compounded — you want earlier warning at the *source query* level. |
| **Implement a circuit breaker / timeout** | If `ingest_inventory` source read exceeds e.g. 5 minutes, surface a `WARN` alert immediately rather than silently retrying. The current behaviour let the problem run for ~35 minutes before any alert fired. |
| **Review source DB query plan** | The source read is against `source.orders_db.inventory`. Ensure the query has appropriate indexes and isn't doing a full table scan. Examine whether any other DB activity (VACUUM, ETL, batch jobs) runs at 01:15–01:52 that could cause lock contention. |
| **Add a job-duration DQ check** | Treat `duration_ms` as a first-class data quality signal alongside `null_rate`, `row_count`, and `freshness_lag_min`. A duration check at the ingest stage would have fired ~35 minutes before the freshness alert. |

### Long-Term (systemic)
| Action | Detail |
|---|---|
| **Source DB read replica** | Route `ingest_inventory` reads to a read replica to isolate pipeline reads from OLTP write contention. |
| **Freshness SLA tightening** | If 61-minute staleness is business-critical, consider tightening the alert threshold or adding a tiered alert (warn at 15 min, critical at 30 min) to give more reaction time. |
| **Runbook documentation** | Document this incident pattern: "jobs succeed but data is stale → check ingest duration → check source DB contention". The symptom (no failures, stale data) is non-obvious and tripped up initial triage. |

---

## 6. Confidence

**Confidence: HIGH** 🟢

The evidence is unambiguous and fully corroborated across four independent data sources:

1. **Schema registry** — no changes whatsoever, definitively ruling out a schema break
2. **DQ null_rate checks** — all passing, ruling out column-level data contract issues
3. **DQ row_count checks** — all passing, ruling out a data volume problem
4. **Job logs** — exact WARN messages naming `source.orders_db.inventory` as the slow component, with duration escalating from 45s → 780s and then explicit *"Source query unblocked"* recovery messages
5. **Revenue pipeline comparison** — completely unaffected throughout, isolating the cause to the inventory source specifically

The only thing that would increase confidence further is access to the **`source.orders_db` database-level logs** (slow query log, lock wait events) to confirm *what* caused the contention at 01:17 — that is the one question the pipeline observability data cannot answer by itself.