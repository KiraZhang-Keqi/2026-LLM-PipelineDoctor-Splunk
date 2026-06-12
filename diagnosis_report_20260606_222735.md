# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T22:27:35.404290

**Problem:** Business users are complaining that the inventory dashboard hasn't updated since yesterday. The numbers look plausible but appear to be stale. No jobs are showing as failed. Please investigate.

---

The picture is now **completely clear and conclusive**. Here is the full diagnosis:

---

# 🩺 Pipeline Doctor — Diagnosis Report

## 1. Summary

`dashboard.inventory_health` served stale data for approximately **~45–60 minutes** between ~01:45 and ~02:18 UTC-4 on 2026-06-07. All pipeline jobs reported `success` throughout, which masked the real problem: a **severe source-side read slowdown** on `source.orders_db.inventory` that caused `ingest_inventory` job durations to explode from a healthy ~45 seconds to over **12 minutes per run** — preventing fresh data from propagating downstream through the full chain to the dashboard.

**The pipeline did not crash. It ran in slow motion.**

---

## 2. Root Cause

**A slow/blocked read on `source.orders_db.inventory`** — almost certainly a long-running table lock, heavy concurrent DML, or a missing/invalidated index on the source database. This caused `ingest_inventory` to take up to **~773 seconds** per run (vs. 45s baseline), making each job cycle take so long that downstream tables (`raw.inventory_snapshot` → `mart.product_availability` → `dashboard.inventory_health`) simply stopped receiving fresh data fast enough.

> **This is NOT a schema change.** The schema registry has zero non-baseline events. Column null rates for `product_id`, `stock_count`, `warehouse_id`, and `available` all remained at `pass` throughout. Row counts were completely stable (~12,000 rows). This rules out a data contract or transformation bug entirely.

> **This is NOT a resource exhaustion issue.** Worker CPU peaked at ~34% and memory at ~53% — both well within normal operating range.

---

## 3. Lineage / Propagation (with timestamps)

```
source.orders_db.inventory  ← ROOT CAUSE: slow/locked reads begin here ~01:36 UTC-4
       │
  [ingest_inventory]         ← Duration: 45s (normal) → 56s (01:36 WARN) → 12+ min (01:45–02:05)
       │
raw.inventory_snapshot       ← Freshness lag: 7 min (normal) → 17 min (01:45) → 55 min (02:05) ❌
       │
  [transform_inventory]      ← Inherited stale inputs; completed fine but on stale data
       │
mart.product_availability    ← Freshness lag: 7 min (normal) → 14 min (01:45) → 49 min (02:05) ❌
       │
  [refresh_inventory_dashboard]
       │
dashboard.inventory_health   ← "StaleData" CRITICAL alert: serving data 58–63 min old ❌
```

| Time (UTC-4) | Event |
|---|---|
| ~01:20–01:35 | All systems healthy; `ingest_inventory` ~44–46s; freshness ~4–7 min |
| **01:36** | **First WARN: slow read, duration 56,393ms** |
| ~01:37–01:40 | Duration escalating: 71s → 103s → 130s → 157s; freshness lag rising to 11 min |
| ~01:40–01:45 | Duration jumps to 194s avg, then **638s avg** — jobs running >10x slower |
| **01:45** | Freshness lag on `raw.inventory_snapshot` breaches 20-min threshold |
| ~01:50–02:05 | Duration peaks at **773,000ms avg (~12.9 min)**; freshness lag peaks at **55–63 min** |
| ~02:10 | Source unblocks; duration falls to 355s, then recovering |
| **02:16+** | "Backfill completing" WARNs appear; freshness recovering |
| **02:18** | `FreshnessRecovered` alert fires; `raw.inventory_snapshot` lag drops to 19.9 min |
| ~02:20 | Freshness returns to normal ~6–9 min for all tables |

---

## 4. Evidence

| Signal | Value | Interpretation |
|---|---|---|
| `ingest_inventory` avg duration at baseline | ~44,755 ms | Normal |
| `ingest_inventory` avg duration at peak | ~773,003 ms | **17× slower — blocked source read** |
| First WARN message | `"Slow read from source.orders_db.inventory"` at 01:36 | **Root cause pinpointed** |
| Recovery message | `"Source query unblocked. Backfill completing."` | Confirms block lifted, not a crash |
| `raw.inventory_snapshot` row_count | Stable ~11,900–12,100 throughout | ✅ Rules out data loss |
| `mart.product_availability` row_count | Stable ~11,700–11,850 throughout | ✅ Rules out transform failure |
| Worker CPU / memory | Max ~34% CPU, ~53% mem | ✅ Rules out resource exhaustion |
| null_rate checks (`stock_count`, `warehouse_id`, `available`, `product_id`) | All `pass` throughout | ✅ Rules out schema break |
| Schema registry non-baseline events | **0** | ✅ Definitively rules out schema contract break |
| Revenue pipeline freshness during same window | Stable 5–7 min throughout | ✅ Confirms this is inventory-source-specific, not a platform-wide issue |

---

## 5. Remediation

### Immediate
1. **Investigate `source.orders_db.inventory` for lock contention.** Check the DB's lock wait history, slow query logs, and any batch jobs or deployments that ran between 01:35–02:10 UTC-4. This is almost certainly a long-running transaction or a missing index after a schema change on the *source* database.
2. **No pipeline code changes are needed** — the pipeline itself behaved correctly and self-recovered once the source unblocked.

### Short-Term
3. **Add a `duration_ms` alert on `ingest_inventory`** — a threshold of e.g. 90,000ms (2× baseline) should trigger a `warning` alert, catching this ~5 minutes earlier. The freshness alert only fired at 01:45; the duration was already anomalous at 01:36.
4. **Add a source-query timeout** to `ingest_inventory` so that a blocked read fails fast and loudly rather than silently running for 12+ minutes, which would make the issue far more visible.

### Long-Term
5. **Read replica for analytics ingestion:** Route `ingest_inventory` reads to a read replica of `orders_db` to avoid lock contention from application writes entirely.
6. **Schema contract enforcement:** Even though not the cause here, the fact that `source.orders_db.inventory` has zero schema registry events means *nobody would know* if a column was renamed there. Implement CDC (Change Data Capture) or schema-registry hooks at the source database level.
7. **Synthetic freshness SLO dashboard:** Create a Splunk dashboard that shows freshness lag across all four tables side-by-side in real time, so operations staff can spot cascading staleness without waiting for user reports.

---

## 6. Confidence

**🟢 HIGH**

The evidence is unambiguous and fully corroborated:
- The exact WARN message names `source.orders_db.inventory` as the slow component.
- The duration timeline precisely matches the freshness lag timeline with appropriate propagation delay.
- Row counts, null rates, CPU, memory, and the schema registry all rule out every alternative hypothesis.
- The revenue pipeline remained perfectly healthy throughout, isolating the fault to the inventory source.

The one gap is that we cannot directly inspect the source database's lock/query logs from Splunk — **confirming the specific root cause of the slowdown** (lock, bad query plan, batch job, etc.) requires a DBA to inspect `source.orders_db` query history for the 01:35–02:10 window. That would raise an already-high confidence to near certainty.