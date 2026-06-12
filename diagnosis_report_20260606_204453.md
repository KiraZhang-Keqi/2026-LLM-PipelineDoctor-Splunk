# Pipeline Doctor - Diagnosis Report

**Generated:** 2026-06-06T20:44:53.030453

**Problem:** The inventory dashboard shows every product as out of stock, but sales data looks fine. Please investigate and diagnose the root cause.

---

All the evidence I need is now in hand. The picture is complete and unambiguous. Let me compile the full diagnosis report.

---

# 🩺 Pipeline Doctor — Diagnosis Report

## 1. Summary

**What broke:** Every product in `dashboard.inventory_health` showed as "out of stock" because the `stock_count` column — the field that drives availability calculations — was silently replaced with NULLs throughout the entire inventory pipeline.

**User-visible impact:** `dashboard.inventory_health` reported 100% out-of-stock across all SKUs, affecting merchandising and checkout availability (confirmed by `BusinessImpact` critical alert at **23:53–23:55 UTC-4**). The revenue/sales pipeline was completely unaffected.

**Status at time of analysis:** A patch is in progress (backfill running as of ~00:04), with null_rate declining and `RecoveryDetected` alerts firing. Full recovery is not yet complete.

---

## 2. Root Cause

**A breaking column rename in the inventory source database was not propagated to the `ingest_inventory` job.**

| Detail | Value |
|---|---|
| **Source table** | `source.orders_db.inventory` |
| **Schema change** | `stock_count` → `available_qty` |
| **Schema version** | 2.3.1 |
| **Change time** | **2026-06-06 23:20:15 UTC-4** (migration ticket `DATA-1847`) |
| **Offending job** | `ingest_inventory` |
| **Mechanism** | `ingest_inventory` continued requesting the old column name `stock_count`; since it no longer existed in the source, the job's fault-tolerance filled every row with `NULL` instead of failing loudly. Those NULLs then propagated downstream. |

The job logs are explicit — starting at **23:25:15**, every run emitted:
> *"Column 'stock_count' not found in source.orders_db.inventory; filling NULL. (source schema now 2.3.1)"*

---

## 3. Lineage & Propagation (with timestamps)

```
source.orders_db.inventory
  ↓ [schema change: stock_count → available_qty @ 23:20]
  ↓
[ingest_inventory] ← ❌ BREAK POINT: still reads 'stock_count', gets NULLs
  ↓  first WARN log @ 23:25:15
  ↓
raw.inventory_snapshot  ← null_rate(stock_count) spikes to ~89% @ 23:25 window
  ↓  ~1 transform cycle later
  ↓
mart.product_availability  ← null_rate(available) spikes to ~97.5% @ 23:35 window
  ↓
dashboard.inventory_health  ← BusinessImpact CRITICAL alert @ 23:53–23:55
                               "ALL products out of stock"
```

**Propagation lag:** ~10 minutes from source rename → first raw null spike; ~10 more minutes for the mart to reflect it; ~20 more minutes before dashboard alerts fired. Total end-to-end: ~33 minutes from schema change to business-visible impact.

---

## 4. Evidence

### 🔴 Schema Change — The Trigger
| Time | Table | Change | Old Field | New Field | Ticket |
|---|---|---|---|---|---|
| 23:20:15 | `source.orders_db.inventory` | rename_column | `stock_count` | `available_qty` | DATA-1847 |
| 23:55:15 | `raw.inventory_snapshot` | rename_column | `stock_count` | `available_qty` | DATA-1851 |

*(The second entry at 23:55 is the downstream schema registry catching up to the patch.)*

### 🔴 Null Rate Spike — Precise Timeline (avg null_rate per 5-min bucket)
| Window | `raw.inventory_snapshot` | `mart.product_availability` |
|---|---|---|
| 23:05–23:20 | ~0.17–0.26% ✅ | ~0.16–0.25% ✅ |
| **23:25** | **89.2% 🚨** | 0.18% (not yet propagated) |
| **23:30** | **98.7% 🚨** | 0.18% |
| **23:35** | **98.9% 🚨** | **97.6% 🚨** |
| 23:40–23:50 | ~99% 🚨 | ~96–97% 🚨 |
| 23:55 | ~79.6% (patch begins) | ~72.1% |
| 00:00+ | ~27.5% (recovering) | ~24.7% (recovering) |

### ✅ Resource/Volume — Definitively Ruled Out
| Signal | Finding | Conclusion |
|---|---|---|
| `row_count` DQ check | **PASS** throughout — ~11,000–12,000 rows per run | Data volume is normal; no dropped batches |
| `freshness_lag_min` DQ check | **PASS** throughout — well under 20-min threshold | Jobs are running on schedule |
| `duration_ms` | Normal range (28–60 sec) across all three inventory jobs | No slowdowns |
| `worker_cpu_pct` | 20–44% — no saturation | No compute pressure |
| `worker_mem_pct` | 26–52% — no saturation | No memory pressure |
| Revenue pipeline DQ failures | **Zero** — `raw.orders`, `mart.daily_revenue` fully healthy | Proves this is inventory-specific, not a platform-wide outage |

The combination of **normal row_count + normal freshness + 97% null_rate on a single column** is the definitive fingerprint of a **schema contract break**, not a resource or scheduling failure.

### 🔴 Job Log — Smoking Gun
```
2026-06-06 23:25:15 | ingest_inventory | WARN | status=success
"Column 'stock_count' not found in source.orders_db.inventory; filling NULL. (source schema now 2.3.1)"
```
This WARN fired continuously from **23:25 through the entire incident window**.

---

## 5. Remediation

### Immediate Fix (In Progress ✅)
1. **`ingest_inventory` has been patched** to read `available_qty` (aliased as `stock_count` for downstream compatibility) — confirmed by alert `PipelinePatched` at 23:55 and job log message *"Backfilling stock_count from available_qty"*.
2. **Monitor backfill completion**: null_rate is declining (97% → 8% as of 00:04). Watch `RecoveryDetected` alerts. Confirm null_rate drops below the 5% threshold and stays there.
3. **Validate the mart**: Run a manual DQ check on `mart.product_availability.available` and spot-check `dashboard.inventory_health` against known inventory counts before declaring recovery.

### Prevent Recurrence
| Layer | Action |
|---|---|
| **Schema contracts** | Register `ingest_inventory`'s column dependency on `stock_count` in a schema contract system (e.g., Great Expectations, dbt contracts, or a custom schema registry). Any rename to that column should require a migration ticket that includes a pipeline update. |
| **Fail loudly, don't fill NULLs** | Change `ingest_inventory`'s error-handling policy: if an expected column is missing, **fail the job** (status=error) rather than silently NULLing rows. A failed job triggers immediate alerting; silent NULLs don't. |
| **CI/CD schema validation** | Add a pre-deploy schema compatibility check in the source DB's deployment pipeline. Ticket `DATA-1847` should have been blocked until downstream consumers were updated. |
| **Column-level DQ alerting** | Add a null_rate alert specifically on `raw.inventory_snapshot.stock_count` with a threshold of 5% and a P1 page — this would have fired ~30 minutes earlier than the dashboard alert did. |
| **Cross-team change notification** | Enforce that the DB team notifies pipeline owners (via DATA ticket + Slack) before any column rename lands in production. `DATA-1847` existed but `ingest_inventory` was not updated in lockstep. |

---

## 6. Confidence

**🟢 HIGH**

Every link in the causal chain is directly evidenced in Splunk:
- ✅ Schema rename event with exact timestamp and ticket number
- ✅ First WARN log within 5 minutes of the schema change, naming the missing column explicitly
- ✅ Null_rate spike originating in `raw.inventory_snapshot` *before* `mart.product_availability` — proving directionality
- ✅ Row count, freshness, CPU, and memory all normal throughout — all resource/volume hypotheses eliminated
- ✅ Revenue pipeline (parallel, unaffected) confirms this is not a platform-wide issue
- ✅ Patch and recovery already observed in the data

The only data that would marginally strengthen the case further would be the `source.orders_db.inventory` migration runbook to confirm `DATA-1847` was intentional and not an accidental rename — but from a pipeline diagnosis perspective, the root cause is conclusively identified.