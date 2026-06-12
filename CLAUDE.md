# Pipeline Doctor — Project Context

## What this project is
AI-powered data pipeline fault diagnosis agent for the Splunk Agentic Ops Hackathon (Observability track). The agent queries Splunk and reasons like a data engineer to diagnose schema breaks, data-lineage problems, and data-quality regressions.

## Architecture
```
generate_data.py  →  Simulates a fault scenario, pushes events to Splunk via HEC
agent.py (+Claude) →  Queries Splunk REST API, reasons through the fault, outputs diagnosis
diagnosis_report_*.md → Output report
```

## Data pipeline topology
```
INVENTORY chain (affected):
  source.orders_db.inventory → [ingest_inventory] → raw.inventory_snapshot
    → [transform_inventory] → mart.product_availability
    → [refresh_inventory_dashboard] → dashboard.inventory_health

REVENUE chain (healthy control group):
  source.orders_db.orders → [ingest_orders] → raw.orders
    → [transform_revenue] → mart.daily_revenue
    → [refresh_revenue_dashboard] → dashboard.revenue_overview
```

## 5 sourcetypes in Splunk (index=main)
- `pipeline:job_log` — ETL job runs (job_name, stage, status, rows_in/out, duration_ms, worker_cpu_pct, message)
- `pipeline:schema_registry` — Schema versions & column changes (table, change_type, old_field, new_field)
- `pipeline:data_quality` — DQ checks: null_rate / row_count / freshness_lag_min (table, column, check_name, value, threshold, status)
- `pipeline:lineage` — Lineage edges (upstream_table, downstream_table, produced_by_job)
- `pipeline:alerts` — Alerts (target, severity, alert_name, description)

## Scenario design principles
- All scenarios: jobs report status="success", CPU/memory normal. The failure is in the DATA, not the infrastructure.
- Each scenario breaks exactly ONE DQ dimension; the other two stay healthy.
- The healthy revenue chain always runs alongside the affected inventory chain as a control group.
- Error messages should be realistic (what a real ETL framework would log), NOT editorial ("possible schema mismatch" is banned — the agent must reason that out itself).
- Timestamps must respect causality: root cause event BEFORE first symptom, symptoms propagate downstream with realistic delays.

## Code conventions
- Secrets in .env (never hardcoded). Both scripts use `load_dotenv()`.
- `generate_data.py` uses a `SCENARIO_BUILDERS` registry dict; `SCENARIOS = set(SCENARIO_BUILDERS.keys())`. Each scenario has a `build_<name>(base_time)` function and four `phase_*` functions.
- Event generators (`gen_job_log`, `gen_dq_check`, etc.) are shared across scenarios.
- DQ checks use `emit_dq_for_table(ts, table, null_rate_overrides=None, row_count_override=None, freshness_override=None)` — pass only the overrides needed for the broken dimension.
- `agent.py` system prompt contains ONLY general diagnostic methodology, never scenario-specific hints.

## Current state
- `schema_change` scenario: fully implemented and tested
- `volume_drop` scenario: fully implemented and tested
- `freshness_delay` scenario: fully implemented and tested
- `agent.py --scenario` flag: implemented — selects a default `--question` per scenario; `--question` overrides it
