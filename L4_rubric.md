# Pipeline Doctor — Diagnostic Capability Rubric (L1–L4)

After the agent finishes a scenario, read its `DIAGNOSIS REPORT` and assign the **highest level it reaches** (L1/L2/L3/L4; record L0 if it fails L1). Levels are cumulative: to earn L4, the requirements of L1–L3 must also be met.

---

## Level Definitions

| Level | Name | Requirement |
|---|---|---|
| **L1** | Detection | Correctly states that an incident occurred and that the **inventory** line / `dashboard.inventory_health` is affected. Does not misattribute the problem to the revenue line. |
| **L2** | Localization | Correctly identifies the failing job/stage as **`ingest_inventory` (ingest stage)** and describes the propagation path `raw.inventory_snapshot → mart.product_availability → dashboard.inventory_health`. Correctly determines the **revenue line is unaffected** (blast radius is inventory-only). |
| **L3** | Root-cause class | Correctly classifies the failure into **the type matching this scenario**, and does **not** misclassify it as one of the other two scenarios (see answer key below). This is where the three scenarios are most easily confused and is the core scoring watershed. |
| **L4** | Exact root cause + evidence + remediation | On top of L3: (1) states the **precise mechanism** (down to the scenario-specific specifics: column name / ticket / row counts / status, etc.); (2) gives **correct, actionable remediation**; (3) states the blast radius correctly; (4) has **no major false root cause** (does not promote a distractor signal to the main cause). Confidence is supported by multiple independent signals. |

> Scoring note: the L3 vs L4 difference is "right class" vs "precise with closed-loop evidence." If the agent gets the class right but writes another scenario's trace as the main cause (e.g., a freshness_delay report naming the schema rename as the ultimate root cause), cap it at **L3** and note "mixed in scenario X signal" — this usually means the index was not cleaned.

---

## Per-Scenario Ground Truth (Answer Key)

### Scenario 1: `schema_change`
- **Exact root cause**: The column `stock_count` on source table `source.orders_db.inventory` was renamed to `available_qty` (schema v2.2.0 → 2.3.1, ticket **DATA-1847**), but the `ingest_inventory` job was not updated and still reads the old column `stock_count` → null-fills.
- **Failure class (L3 must get right)**: schema contract break / un-propagated column rename. **Not** a volume drop, **not** a stall.
- **Typical symptoms**: dashboard shows every product as out of stock; null_rate on `stock_count` in `raw.inventory_snapshot` spikes (~0.29).
- **L4 evidence points**: schema_registry rename event; job log WARN "Column 'stock_count' not found ... filling NULL"; null_rate jump; ticket DATA-1847.
- **L4 remediation**: update `ingest_inventory` to read `available_qty`; backfill the affected window; add a schema-compatibility check / CI gate.
- **Blast radius**: inventory only; revenue line clean.

### Scenario 2: `volume_drop`
- **Exact root cause**: source extraction returns only ~**10%** of rows (~**1,247** rows, expected ~12,000), but `ingest_inventory` still reports `success` (WARN only, not fail) → row counts collapse downstream.
- **Failure class (L3 must get right)**: partial extraction / volume collapse (job falsely succeeds). **Not** a schema rename, **not** a job stall/staleness.
- **Typical symptoms**: row_count drops sharply across tables, but all job statuses are success, no failed status, no alert fires first.
- **L4 evidence points**: job log WARN "returned 1,247 rows (expected ~12,000)"; row_count DQ check fail; downstream row counts shrink proportionally; status stays success throughout.
- **L4 remediation**: investigate the source extraction query / upstream data completeness; make "row count far below baseline" a **hard failure** instead of WARN; add a row_count threshold alert.
- **Blast radius**: inventory only; revenue line clean.

### Scenario 3: `freshness_delay`
- **Exact root cause**: `ingest_inventory` slows down then **stalls completely** (status=`running`, rows_in/out=0, source query blocked for ~35 minutes), data stops updating → dashboard goes stale; then self-recovers.
- **Failure class (L3 must get right)**: job stall / data staleness (freshness). **Not** a schema rename, **not** a volume drop. Note: the rows that did arrive are normal — it is "stale," not "fewer."
- **Typical symptoms**: dashboard numbers look plausible but do not update; freshness_lag_min exceeds threshold; StaleData alert; no failed status.
- **L4 evidence points**: job shows `running` with rows_in/out=0; duration spikes; freshness_lag_min fail; StaleData critical alert; FreshnessRecovered recovery alert.
- **L4 remediation**: investigate source query blocking/locks; add a duration timeout alert on `ingest_inventory` (>3× baseline); add a freshness SLA alert so you don't wait until the dashboard is stale.
- **Blast radius**: inventory only; revenue line clean.

---

## Scoring Sheet (one row per run)

| scenario | run | Level (L0–L4) | Class correct? | Mixed other scenario? | Notes |
|---|---|---|---|---|---|
| schema_change | 1 | | | | |
| schema_change | 2 | | | | |
| schema_change | 3 | | | | |
| schema_change | 4 | | | | |
| schema_change | 5 | | | | |
| volume_drop | 1 | | | | |
| volume_drop | 2 | | | | |
| volume_drop | 3 | | | | |
| volume_drop | 4 | | | | |
| volume_drop | 5 | | | | |
| freshness_delay | 1 | | | | |
| freshness_delay | 2 | | | | |
| freshness_delay | 3 | | | | |
| freshness_delay | 4 | | | | |
| freshness_delay | 5 | | | | |

**Suggested summary metrics**: L4 hit rate per scenario (how many of 5 reach L4), average level, and stability (variance across the 5). For the demo, reporting something like "L4 hit rate X/5, average L3.x" is clear and concrete.
