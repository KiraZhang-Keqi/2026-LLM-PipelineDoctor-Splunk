# 🩺 Pipeline Doctor — Data Observability

**AI-Powered Data Pipeline Fault Diagnosis Agent**

An intelligent agent that uses an LLM + Splunk to diagnose **data pipeline failures** —
broken schema contracts, data-lineage breaks, and data-quality regressions — by querying
Splunk and reasoning like an experienced data engineer. Built for the Splunk Agentic Ops Hackathon.

## Track: Observability

## Problem

When a dashboard suddenly shows wrong numbers ("every product is out of stock"), the cause
is rarely an outage — jobs keep "succeeding." The real culprit is often an upstream **schema
change** that silently breaks a downstream column. Tracing that through a multi-stage ETL
pipeline by hand takes a data engineer hours of cross-referencing job logs, schema history,
and data-quality checks. Pipeline Doctor automates that investigation: it follows data lineage
upstream, rules out resource/volume causes, and pinpoints the exact field change at the root.

## Architecture

See [`architecture.svg`](architecture.svg) in the repo root for the full diagram (AI/agent
integration, app ↔ Splunk interaction, and data flow). At a glance:

```
User (natural language)
    → Agent (Python + Claude API)
        → LLM decides what to investigate
            → Queries Splunk via MCP (default) or REST API (--use-rest fallback)
                → Analyzes results
                    → Decides: need more data?
                        → Yes: query again (loop)
                        → No: output diagnosis report
```

### Splunk Connection: MCP-first, REST fallback

The agent connects to Splunk through the **Splunk MCP Server App** over Streamable HTTP
(the MCP protocol). This is the default path. Pass `--use-rest` to fall back to the
direct Splunk REST API (port 8089).

```
Default (MCP):
  agent.py → MCP Client → Splunk MCP Server (/services/mcp) → Splunk index=main

Fallback (--use-rest):
  agent.py → REST Client → Splunk REST API (:8089) → Splunk index=main
```

### Simulated Pipeline (data lineage)

```
AFFECTED chain (inventory):
  source.orders_db.inventory ──ingest_inventory──▶ raw.inventory_snapshot
        ──transform_inventory──▶ mart.product_availability
              ──refresh_inventory_dashboard──▶ dashboard.inventory_health

HEALTHY chain (revenue — proves blast radius is confined):
  source.orders_db.orders ──ingest_orders──▶ raw.orders
        ──transform_revenue──▶ mart.daily_revenue ──▶ dashboard.revenue_overview
```

**Example failure (schema_change):** Source column renamed (`stock_count` → `available_qty`)
→ ingest job still selects the old name → `raw.inventory_snapshot.stock_count` goes 100% NULL
(job still "succeeds") → `mart.product_availability.available` computes to 0 → inventory
dashboard shows everything out of stock. Row counts, durations, and CPU stay normal throughout
— the tell that this is a **schema/contract break, not a resource problem**.

## Fault Scenarios

The agent is tested against three pipeline-failure scenarios, each with a distinct
data-quality signature so the agent must reason rather than pattern-match:

| Scenario          | Failing signal                                              | Root cause                                                           |
| ----------------- | ----------------------------------------------------------- | -------------------------------------------------------------------- |
| `schema_change`   | `null_rate` spikes (row_count & freshness normal)           | Source column renamed; ingest job not updated, null-fills downstream |
| `volume_drop`     | `row_count` collapses (null_rate & freshness normal)        | Source returns ~10% of rows; job still reports success               |
| `freshness_delay` | `freshness_lag_min` breaches (row_count & null_rate normal) | Source query stalls; data stops updating, dashboard goes stale       |

## Splunk Dashboard

A pre-built Splunk dashboard (`splunk_dashboard.xml`) is included for visual monitoring.
It shows all three DQ dimensions side by side — during a `schema_change` incident, the
null_rate chart spikes while row_count and freshness stay flat; during `volume_drop` the
pattern inverts, and so on.

**To import:** In Splunk Web → Search & Reporting → Dashboards → Create New Dashboard →
choose Classic Dashboard → Edit → Source → paste the contents of `splunk_dashboard.xml` → Save.

Dashboard panels:

- **Status overview** — DQ failure count, active alerts, schema changes, job success rate
- **Three DQ dimensions** — null_rate / row_count / freshness_lag over time (the core diagnostic view)
- **Blast radius** — inventory chain vs revenue chain DQ failures
- **Job performance** — ingest duration comparison (inventory vs orders)
- **Detail tables** — recent job runs, alert feed, schema registry

## Data Sources (5 sourcetypes in Splunk)

| Sourcetype                 | Description                                                                                |
| -------------------------- | ------------------------------------------------------------------------------------------ |
| `pipeline:job_log`         | ETL job runs: status, rows_in/out, duration_ms, worker_cpu_pct, stage, source/target table |
| `pipeline:schema_registry` | Schema version & column-change events (rename/add/drop/type_change)                        |
| `pipeline:data_quality`    | DQ checks: null_rate (per column), row_count, freshness_lag_min                            |
| `pipeline:lineage`         | Lineage edges: upstream_table → downstream_table, produced_by_job                          |
| `pipeline:alerts`          | Data-quality and business-impact alerts                                                    |

## Quick Start

### Prerequisites

- Python 3.10+
- Splunk Enterprise with HEC enabled (HTTP Event Collector)
- Splunk MCP Server App installed (provides `/services/mcp` endpoint)
- An Anthropic API key

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure secrets

Copy the template and fill in your real values:

```bash
cp .env.example .env
# then edit .env with your editor
```

`.env` holds the Anthropic API key, the Splunk HEC token, and Splunk host/login. It is
git-ignored and never committed. Both scripts load it automatically.

### 3. Generate and ingest simulated data

```bash
python generate_data.py --scenario schema_change
```

Host and token come from `.env`. To preview events without sending (no token needed):

```bash
python generate_data.py --scenario schema_change --dry-run
```

### 4. Verify in Splunk

```
index=main sourcetype=pipeline:*
```

Confirm the root-cause event landed:

```
index=main sourcetype=pipeline:schema_registry change_type=rename_column
```

You should see the `stock_count` → `available_qty` rename.

### 5. Run the diagnostic agent

```bash
# Default: connects via Splunk MCP Server (requires MCP_TOKEN in .env)
python agent.py --scenario schema_change -v

# Fallback: connects via Splunk REST API
python agent.py --scenario schema_change -v --use-rest
```

`--scenario` accepts `schema_change`, `volume_drop`, or `freshness_delay` (each sets a
matching symptom prompt). To investigate a custom symptom instead, use `--question "..."`.
`-v` prints the agent's step-by-step reasoning. The final report is also saved to
`diagnosis_report_<timestamp>.md`.

On startup, the MCP path prints the available tools and auto-mapped names:

```
MCP tools: ['splunk_run_query', 'splunk_get_indexes', 'splunk_get_metadata', ...]
Mapped:    {'run_query': 'splunk_run_query', 'get_indexes': 'splunk_get_indexes', ...}
```

If the MCP connection fails, the agent prints the error and suggests `--use-rest`.

## Preventive Alerting (closing the loop)

Diagnosis tells you what broke this time; an alert stops it from going unnoticed next
time. `generate_alerts.py` provisions, for each fault scenario, the Splunk alert that
would have caught it earlier — turning each post-mortem into a guardrail.

Each rule maps 1:1 to a scenario, because each scenario breaks exactly one data-quality
dimension on the inventory chain (the thresholds mirror `generate_data.py`):

| Alert                          | Detects                                  | Catches scenario  |
| ------------------------------ | ---------------------------------------- | ----------------- |
| Inventory `null_rate` breach   | `null_rate` DQ check fails (> 0.05)      | `schema_change`   |
| Inventory `row_count` collapse | `row_count` DQ check fails (< 70% base)  | `volume_drop`     |
| Inventory freshness SLA breach | `freshness_lag_min` DQ check fails (> 20)| `freshness_delay` |

Rules live in [`alert_rules.json`](alert_rules.json) (edit the SPL, cron, or severity
there). The generator reuses the **same REST login as `agent.py`** (`SPLUNK_HOST` /
`SPLUNK_USERNAME` / `SPLUNK_PASSWORD` from `.env`) — no new dependency, env var, or token.

```bash
# Preview the SPL without touching Splunk (no connection needed)
python generate_alerts.py --dry-run

# Create / update all three scheduled alerts
python generate_alerts.py

# Optional: list or remove them
python generate_alerts.py --list
python generate_alerts.py --delete
```

Created alerts appear under **Settings → Searches, reports, and alerts** (filter
`PipelineDoctor`); firings show under **Activity → Triggered Alerts**. A static export is
also committed as [`savedsearches.conf`](savedsearches.conf) for direct import or review.

> Note: alerts are provisioned in the authenticated user's namespace within the `search`
> app. Use `--share` to set app-level sharing so the whole app sees them. As with
> `can_delete`, alert ownership/visibility is governed by Splunk roles — assign at the
> user/app level if a teammate needs to see them.

## Validation & Reliability

To show the agent is reliable — not lucky on a single run — each scenario was run 5 times
(15 runs total) and each diagnosis was scored against a 4-level capability rubric
([`L4_rubric.md`](L4_rubric.md)), where **L4** = correct exact root cause + supporting
evidence + actionable remediation, with the other two scenarios explicitly ruled out.

| Scenario          | L4 hit rate                           |
| ----------------- | ------------------------------------- |
| `schema_change`   | 5 / 5                                 |
| `volume_drop`     | 5 / 5                                 |
| `freshness_delay` | 5 / 5                                 |
| **Total**         | **15 / 15 (avg L4.0, zero variance)** |

Per-run reports and scores are in [`validation_runs/`](validation_runs/) (see
`validation_runs/scores.csv`). The diagnostic principle is **lineage-first, not
resource-first**: normal row_count / duration / CPU combined with a single column-level
signal is the fingerprint of a schema/contract break rather than an outage.

## Configuration

All config is read from `.env` (see `.env.example`). CLI flags override `.env`:

`generate_data.py`

- `--host` — Splunk HEC host IP (default from `SPLUNK_HEC_HOST`)
- `--port` — HEC port (default 8088)
- `--token` — HEC token (default from `PIPELINE_HEC_TOKEN`)
- `--scenario` — failure scenario: `schema_change` (default), `volume_drop`, `freshness_delay`
- `--dry-run` — preview events without sending
- `--batch-size` — events per HEC batch (default 50)

`agent.py`

- `--scenario` — `schema_change` / `volume_drop` / `freshness_delay`; sets a default symptom prompt
- `--question` — override with a custom symptom to investigate
- `--verbose` / `-v` — show agent reasoning
- `--use-rest` — skip MCP, connect via Splunk REST API instead
- `--mcp-token` — MCP Bearer token (overrides `MCP_TOKEN` in `.env`)
- `--mcp-url` — MCP server URL (default: `{SPLUNK_HOST}/services/mcp`)
- `--api-key`, `--splunk-host`, `--splunk-user`, `--splunk-pass` — override `.env`

`generate_alerts.py`

- `--rules` — path to the alert definitions (default `alert_rules.json`)
- `--dry-run` — print the SPL without creating anything (no Splunk connection)
- `--share` — set app-level sharing after creating
- `--list` — list provisioned Pipeline Doctor alerts
- `--delete` — remove the alerts defined in the rules file
- `--splunk-host`, `--splunk-user`, `--splunk-pass` — override `.env`

## License

MIT
