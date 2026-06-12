# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T21:26:52.096845

**Problem:** The inventory dashboard numbers dropped dramatically overnight — product counts are a fraction of what they should be. But no jobs have failed and no alerts fired until just now. What happened?

---

I now have a complete, high-confidence picture. All the evidence is assembled. Let me compile the definitive diagnosis.

---

# 🩺 Pipeline Doctor — Diagnosis Report

## 1. Summary

**What broke:** The `source.orders_db.inventory` database table experienced a severe **upstream data loss event** between **00:35 and ~01:17** on 2026-06-07, returning only ~1,200 rows per poll instead of its normal ~12,000 (~89% fewer records). This propagated faithfully through the entire inventory pipeline — no job had a reason to fail or raise an error — resulting in `dashboard.inventory_health` showing ~90% fewer products than expected.

**User-visible impact:** The inventory health dashboard displayed drastically deflated product counts for approximately **42 minutes** (00:35–01:17). The pipeline has since self-recovered as the source data returned to full volume.

---

## 2. Root Cause

> **An upstream source data outage — not a schema break, not a resource problem.**

The root cause is that **`source.orders_db.inventory`** began returning only ~10% of its normal row volume starting at **00:35:49**. The schema contract (v2.2.0) was **not violated** — all columns (`product_id`, `warehouse_id`, `stock_count`) remained present and non-null. The ingestion job (`ingest_inventory`) correctly ingested whatever the source returned, logged `WARN` messages about the shortfall, but completed with `status=success` because no pipeline-level threshold for failing the job on low volume was configured.

**The ingest job itself was behaving correctly — it was faithfully mirroring a source that was broken.**

The most likely source-side causes (requiring DB-side investigation):
- A partial database maintenance window, migration, or truncation at the source
- A misconfigured WHERE clause or view change on `source.orders_db.inventory` that silently filtered out ~90% of rows
- A replication or snapshot failure on the upstream orders_db leaving the table partially populated

---

## 3. Lineage / Propagation

The failure flowed perfectly along the inventory lineage chain, each step passing along the reduced row count:

```
source.orders_db.inventory        ← ROOT: ~89% row loss begins at 00:35:49
        │  [ingest_inventory — WARN at 00:35:49]
        ▼
raw.inventory_snapshot            ← 1,157–1,306 rows (vs. baseline ~12,000) | DQ row_count FAIL ~01:08
        │  [transform_inventory — SUCCESS, no warnings]
        ▼
mart.product_availability         ← mirrors the row collapse | DQ row_count FAIL ~01:08+
        │  [refresh_inventory_dashboard — SUCCESS]
        ▼
dashboard.inventory_health        ← CRITICAL alert: "90% fewer products" at 01:08:12
                                    RowCountRecovered alert at 01:17:35 ✅
```

| Time | Event |
|------|-------|
| **00:20–00:34** | `ingest_inventory` healthy: ~12,000 rows, all INFO |
| **00:35:49** | 🔴 First WARN: only 1,247 rows extracted from `source.orders_db.inventory` |
| **00:36–00:39** | 4-minute **gap** — no ingest_inventory runs at all (possible source unavailability) |
| **00:40+** | Ingest resumes at ~1,150–1,300 rows, sustained WARN cadence |
| **~01:08** | DQ `row_count` failures surface on `raw.inventory_snapshot` and `mart.product_availability`; CRITICAL business alert fires on `dashboard.inventory_health` |
| **01:17:35** | ✅ `RowCountRecovered` alert fires; source has restored to 8,508+ rows and climbing |
| **01:20:34** | All DQ checks passing again, rows back to 11,530–11,730 |

---

## 4. Evidence

### ✅ What rules out a resource / infrastructure outage:
| Signal | Value | Verdict |
|--------|-------|---------|
| `ingest_inventory` CPU | 22–32% throughout incident | ✅ Normal — no compute pressure |
| `ingest_inventory` Memory | 18–54% | ✅ Normal |
| `ingest_inventory` Status | `success` on every single run | ✅ No job failure |
| `duration_ms` | 35K–51K ms — normal range | ✅ No slowdown/timeout |
| `null_rate` on `stock_count`, `product_id`, `warehouse_id` | 0.001–0.002 (passing, threshold 0.05) | ✅ Column structure intact — **not a schema rename or drop** |
| `schema_version` | v2.2.0 `baseline` — no `rename_column`, `drop_column`, `type_change` events | ✅ No schema contract violation |
| Revenue pipeline (`ingest_orders`, `transform_revenue`) | Healthy 8,300–8,600 rows throughout | ✅ Confirms this is **inventory-specific**, not a platform-wide outage |

### 🔴 What confirms the source data collapse:
| Signal | Value |
|--------|-------|
| First WARN log at **00:35:49** | *"Extraction query returned 1,247 rows (expected ~12,000). source.orders_db.inventory may have incomplete data."* |
| Timechart cliff at **00:35** | avg_rows_out drops from ~12,049 → 6,704 in one minute bucket, then **zero rows** for 4 minutes (00:36–00:39) |
| Sustained WARN cadence 00:40–01:17 | ~1,150–1,300 rows per run across 30+ consecutive runs |
| `RowCountAnomaly` alert at 01:08:12 | "row_count 1,161 is **89% below baseline 12,000**" |
| `BusinessImpact` CRITICAL at 01:08:12 | "Inventory dashboard showing **90% fewer products** than expected" |
| Recovery confirmed 01:17:35 | "row_count 8,508 back above threshold 8,400; pipeline data volume restoring" |

---

## 5. Remediation

### Immediate Actions
1. **Investigate `source.orders_db.inventory` on the source DB side** — check for:
   - Any DDL changes (ALTER TABLE, CREATE VIEW replacements) between 00:33 and 00:35
   - DB maintenance jobs, TRUNCATE/DELETE statements, or replication lag events in that window
   - Whether a filter predicate (WHERE clause) was added to a view that `ingest_inventory` queries
   - Query the source directly: `SELECT COUNT(*) FROM inventory` — is it back to ~12,000?
2. **Validate the recovered data** — the source returned to full volume, but confirm the ~42 minutes of thin loads didn't leave gaps in `raw.inventory_snapshot` (check for missing `warehouse_id` / `product_id` ranges from the incident window)
3. **Re-run a full backfill** of `raw.inventory_snapshot` → `mart.product_availability` for the incident window to repair the mart data if analytical queries cover that period

### Prevention (Structural Fixes)
| Fix | Description |
|-----|-------------|
| **Fail-fast on volume anomaly** | Configure `ingest_inventory` to **fail the job** (not just WARN) when `rows_in < N * 0.5` of the rolling baseline. A silent WARN with `status=success` hid the incident for 33 minutes before DQ alerted. |
| **Add a source-level DQ check** | Run a `row_count` check directly on `source.orders_db.inventory` *before* ingestion begins — this would have caught the problem at the source without waiting for DQ checks on `raw.inventory_snapshot`. |
| **Tighten DQ thresholds** | The `row_count` threshold on `raw.inventory_snapshot` was 8,400 (70% of baseline). At 1,250 rows (10%), it should have fired immediately — check why DQ alerts lagged until 01:08 when the first WARN was at 00:35. Consider a tighter threshold or a dedicated "catastrophic drop" alert at >50% loss. |
| **Schema registry coverage** | This incident was NOT schema-related, but the schema registry had no events after 00:34. Ensure source DDL changes (especially view redefinitions) are captured as `change_type` events — a view filter change would look exactly like this incident but would *not* self-recover. |
| **Circuit breaker on ingestion** | Don't write a catastrophically thin snapshot to `raw.*` — gate the write behind a volume assertion, or write to a staging area and promote only on pass. |

---

## 6. Confidence

**Confidence: HIGH** ✅

The evidence is unambiguous:
- Exact incident start timestamp pinpointed: **00:35:49**
- No schema changes, no column nulls, no resource exhaustion — all alternative hypotheses eliminated by data
- WARN messages explicitly name `source.orders_db.inventory` as the source of incomplete data
- Consistent ~89% row loss across 30+ consecutive runs, self-recovering at 01:17
- Revenue pipeline (same DB host, different table) was healthy throughout — narrows the fault to the `inventory` table specifically, not the DB server itself

**What would further increase confidence:**
- Direct query of `source.orders_db.inventory` row count at time of incident (requires DB access)
- DB audit logs showing any DDL, DML, or maintenance activity on `inventory` between 00:33–00:35
- Confirmation of whether the 00:36–00:39 gap represents a complete source outage vs. ingest job scheduler gap