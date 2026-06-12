# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T22:15:12.346524

**Problem:** Business users are complaining that the inventory dashboard hasn't updated since yesterday. The numbers look plausible but appear to be stale. No jobs are showing as failed. Please investigate.

---

I now have a complete, corroborated picture. Here is the full diagnosis report.

---

# 🩺 Pipeline Doctor — Diagnosis Report
**Case:** Stale `dashboard.inventory_health` | **Date:** 2026-06-07 | **Status:** ✅ Recovering (incident resolved)

---

## 1. Summary

The inventory dashboard served stale data for approximately **~25 minutes** (freshness lag peaked at ~60 minutes above the 20-minute threshold). The root cause was **not a schema break, a job failure, or a pipeline code defect**. Instead, the `ingest_inventory` job's source queries against `source.orders_db.inventory` became progressively slower and eventually fully blocked, starving the entire downstream inventory pipeline of fresh data. All jobs technically reported `success`, which is why no failure alerts fired — the problem was silently masked.

**User-visible impact:** `dashboard.inventory_health` was serving inventory numbers that were up to ~60 minutes old. The data looked *plausible* because the last good snapshot was structurally intact — it was simply not updating.

---

## 2. Root Cause

**A source-side query performance degradation / lock event on `source.orders_db.inventory`.**

This is an **infrastructure / source DB problem**, not a schema contract break. The sequence:

| Time | Event |
|------|-------|
| ~01:23 | First WARN: `"Slow read from source.orders_db.inventory"` — duration 48s vs 45s baseline |
| 01:23–01:33 | Escalating slow reads: duration climbing from 48s → 192s, each run still succeeding |
| ~01:33 | Source query fully blocked — job enters `status=running` with `rows_in=0, rows_out=0` |
| 01:33–01:59 | **26-minute full block** — 63 consecutive `running` heartbeats, avg blocked duration ~764 seconds each, zero rows flowing |
| ~01:55–02:00 | Block releases — freshness lag begins declining |
| ~02:05 | `FreshnessRecovered` alert fires; freshness drops back below 20-minute threshold |
| ~02:07 onward | `ingest_inventory` back to normal: 40-60s duration, ~12k rows/run, INFO-level logs |

The most likely database-side cause is a **long-running write transaction, DDL lock, or table-level lock** held on `source.orders_db.inventory` during that 26-minute window. This is outside the pipeline itself.

---

## 3. Lineage / Propagation

```
source.orders_db.inventory   ← BLOCKED HERE (source query stalled)
        │
        │  [ingest_inventory]  ← rows_in/out = 0 during block; still reports "success"
        ▼
raw.inventory_snapshot        ← freshness_lag_min spiked from ~6 min → 60 min
        │
        │  [transform_inventory]  ← starved of new input; ran on stale data
        ▼
mart.product_availability     ← freshness_lag_min mirrored raw.inventory_snapshot lag
        │
        │  [refresh_inventory_dashboard]
        ▼
dashboard.inventory_health    ← CRITICAL StaleData alert; users see yesterday's numbers
```

The revenue pipeline (`ingest_orders` → `raw.orders` → `mart.daily_revenue` → `dashboard.revenue_overview`) was **completely unaffected** throughout the incident — confirming the problem was isolated to `source.orders_db.inventory` specifically.

---

## 4. Evidence

### ✅ What RULES OUT a schema break
| Signal | Value | Interpretation |
|--------|-------|----------------|
| `pipeline:schema_registry` non-baseline changes | **0 events** | No column renames, drops, or type changes occurred |
| `null_rate` on all inventory columns | All **pass** (max ~0.004) | No column went missing or null |
| `row_count` checks | Not failing | Data volume is normal when flowing |
| `ingest_inventory` CPU during block | ~38% average overall | No runaway compute; worker was idle waiting on I/O |

### ✅ What RULES OUT a job/code failure
| Signal | Value | Interpretation |
|--------|-------|----------------|
| All job statuses | `success` | Pipeline orchestrator saw no failures |
| `ingest_orders` anomalies | **None** — INFO only, avg 45s, zero WARNs | Orders DB table unaffected; problem is inventory-table-specific |
| Post-incident job behaviour | Normal rows (~12k), normal duration (~50s) | No code regression; pipeline self-healed once source unblocked |

### 🔴 The direct evidence
| Signal | Value |
|--------|-------|
| First WARN timestamp | **01:23:49** — `"Slow read from source.orders_db.inventory"` |
| Slow-read escalation | Duration: 48s → 73s → 88s → 114s → 142s → 192s in ~6 minutes |
| Block start | **~01:33** — `"Job still running after 760,105ms. No output written yet. Source query may be blocked."` |
| Block duration | **~26 minutes** (63 heartbeat events, `status=running`) |
| Block end / recovery | **~01:59–02:05** — freshness recovering, "Backfill completing" logs |
| Peak freshness lag | **~60 minutes** on `raw.inventory_snapshot`; **~34 minutes** on `mart.product_availability` |
| Dashboard staleness alert | **57–59 minutes** stale at peak |

---

## 5. Remediation

### Immediate (incident is already self-healed, but verify)
1. **Confirm full recovery** — check that `raw.inventory_snapshot` freshness lag is consistently below 20 minutes and that `dashboard.inventory_health` is serving current data.
2. **Investigate `source.orders_db.inventory`** — review the DB slow-query log, lock history, or DBA audit trail for what held a lock/transaction from ~01:23 to ~01:59. Common suspects: a bulk `UPDATE`/`DELETE`, an unindexed analytics query, a schema migration run directly on the source, or a deployment rollout.
3. **Communicate to business users** that the dashboard is current as of ~02:05 and historical numbers for 01:23–01:59 are stale (no data was corrupted — it just wasn't ingested).

### Short-term (within this sprint)
4. **Add a source-query timeout** to `ingest_inventory` — if the source query doesn't return within, say, 120 seconds, fail fast and alert rather than hanging silently as `status=running`. A hung-but-succeeding job is a monitoring blind spot.
5. **Add a `rows_out=0` guard** — if `ingest_inventory` completes with zero rows written, it should emit a **failure** or at minimum a high-severity alert, not a silent `success`. This was the core reason the on-call team wasn't paged.
6. **Add a freshness alert at a lower threshold** (e.g., warn at 10 minutes, critical at 20 minutes) to give earlier signal before the dashboard becomes visibly stale.

### Long-term (process)
7. **Source DB change management** — any maintenance, migrations, or bulk operations on `source.orders_db.inventory` should be coordinated with the data engineering team and scheduled during low-impact windows. The orders DB DBA team may not know that a lock on that table starves the real-time inventory dashboard.
8. **Add a circuit breaker / read replica** — route `ingest_inventory` source queries to a read replica of `orders_db` to isolate the pipeline from write-side contention.
9. **Differentiate job "success" from "data success"** — the orchestrator marking a job `success` when it wrote 0 rows is a misleading signal. Consider adding a row-count assertion post-run (e.g., `rows_out > 0` is required for a true success status).

---

## 6. Confidence

**High (≈90%)** that this is a source DB query block/lock event on `source.orders_db.inventory`.

The evidence is internally consistent and multi-layered: escalating slow-read WARNs → full block with `rows_in=0` → freshness lag climbing in exact lockstep → self-resolution with backfill → normal behaviour restored. No schema changes, no null_rate spikes, no code changes, and the parallel revenue pipeline (sharing the same orchestrator and workers) was entirely clean throughout.

**What would raise confidence to 100%:** A matching entry in the `source.orders_db` slow-query log or lock-wait history confirming the blocking transaction's start time (~01:23) and release time (~01:59). That data lives outside this Splunk instance and should be obtained from the DBA team.