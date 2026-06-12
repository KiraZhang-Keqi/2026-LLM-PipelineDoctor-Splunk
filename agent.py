#!/usr/bin/env python3
"""
Pipeline Doctor - AI-Powered Data Pipeline Fault Diagnosis Agent

Diagnoses data-pipeline failures (schema breaks, broken data lineage,
data-quality regressions) by querying Splunk like an experienced data engineer.

Flow:
  1. User describes a symptom in natural language
  2. Agent (Claude) decides what SPL query to run
  3. Tool calls are executed against Splunk (via MCP or REST)
  4. Results are fed back to Claude for analysis
  5. Repeat until enough info -> output diagnosis report

Security note: the Anthropic API key, Splunk password, and MCP token are read
from environment variables / CLI args. Do NOT hardcode secrets in this file.
"""

import os
import json
import time
import argparse
import asyncio
import threading
import contextlib
import xml.etree.ElementTree as ET
from datetime import datetime

import requests
import urllib3
import anthropic

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import httpx
    from mcp.client.streamable_http import streamable_http_client
    from mcp.client.session import ClientSession
    _MCP_AVAILABLE = True
except ImportError:
    _MCP_AVAILABLE = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# CONFIGURATION  (no secrets hardcoded)
# ============================================================
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
SPLUNK_HOST       = os.environ.get("SPLUNK_HOST", "https://localhost:8089")
SPLUNK_USERNAME   = os.environ.get("SPLUNK_USERNAME", "admin")
SPLUNK_PASSWORD   = os.environ.get("SPLUNK_PASSWORD", "")
MCP_TOKEN         = os.environ.get("MCP_TOKEN", "")
MCP_URL           = os.environ.get("MCP_URL", "")   # if empty, falls back to {SPLUNK_HOST}/services/mcp at runtime

MODEL = "claude-sonnet-4-6"
MAX_TURNS = 15

DEFAULT_MAX_RESULTS  = 20
MAX_TOOL_RESULT_CHARS = 4000
TURN_DELAY_SEC       = 8


# ============================================================
# SPLUNK REST CLIENT  (fallback, --use-rest)
# ============================================================
class SplunkClient:
    """Simple Splunk REST API client."""

    def __init__(self, host, username, password):
        self.host = host.rstrip("/")
        self.session_key = self._login(username, password)

    def _login(self, username, password):
        resp = requests.post(
            f"{self.host}/services/auth/login",
            data={"username": username, "password": password},
            verify=False, timeout=15,
        )
        if resp.status_code != 200:
            raise Exception(f"Splunk login failed: {resp.status_code} - {resp.text}")
        root = ET.fromstring(resp.text)
        session_key = root.findtext("sessionKey")
        if not session_key:
            raise Exception("Could not extract session key from login response")
        return session_key

    def _headers(self):
        return {"Authorization": f"Splunk {self.session_key}"}

    def run_query(self, spl_query, max_results=DEFAULT_MAX_RESULTS):
        q = spl_query.strip()
        search = q if (q.startswith("|") or q.lower().startswith("search ")) else f"search {q}"
        resp = requests.post(
            f"{self.host}/services/search/jobs",
            headers=self._headers(),
            data={"search": search, "exec_mode": "oneshot",
                  "output_mode": "json", "count": max_results},
            verify=False, timeout=60,
        )
        if resp.status_code != 200:
            return {"error": f"Search failed: {resp.status_code} - {resp.text[:500]}"}
        try:
            data = resp.json()
            results = data.get("results", [])
            return {"result_count": len(results), "results": results[:max_results]}
        except Exception as e:
            return {"error": f"Failed to parse results: {e}"}

    def get_indexes(self):
        resp = requests.get(
            f"{self.host}/services/data/indexes",
            headers=self._headers(),
            params={"output_mode": "json", "count": 50},
            verify=False, timeout=15,
        )
        if resp.status_code != 200:
            return {"error": f"Failed to get indexes: {resp.status_code}"}
        out = []
        for entry in resp.json().get("entry", []):
            out.append({
                "name": entry["name"],
                "totalEventCount": entry["content"].get("totalEventCount", 0),
                "currentDBSizeMB": entry["content"].get("currentDBSizeMB", 0),
            })
        return {"indexes": out}

    def get_metadata(self, index="main", metadata_type="sourcetypes"):
        return self.run_query(f"| metadata type={metadata_type} index={index} | sort -totalCount")


# ============================================================
# SPLUNK MCP CLIENT  (primary path)
# ============================================================
class MCPSplunkClient:
    """Calls the Splunk MCP Server over Streamable HTTP.
    Exposes the same run_query / get_indexes / get_metadata interface as
    SplunkClient so the agent loop needs no changes.

    Async internals are bridged to the synchronous agent loop via a daemon
    thread running its own event loop."""

    def __init__(self, mcp_url: str, token: str):
        if not _MCP_AVAILABLE:
            raise ImportError("mcp package not installed. Run: pip install mcp httpx")
        self.mcp_url = mcp_url
        self.token   = token
        self._loop   = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._session    = None
        self._exit_stack = None
        self._tools      = {}   # name -> Tool object (from list_tools)
        self._tool_map   = {}   # logical op -> actual MCP tool name

        future = asyncio.run_coroutine_threadsafe(self._connect(), self._loop)
        future.result(timeout=30)

    # ── async internals ──────────────────────────────────────

    async def _connect(self):
        self._exit_stack = contextlib.AsyncExitStack()

        # We own the httpx client and manage its lifetime via the exit stack.
        # Passing it via http_client= tells streamable_http_client not to close it.
        http_client = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {self.token}"},
            verify=False,
            follow_redirects=True,
            timeout=httpx.Timeout(30.0, read=300.0),
        )
        await self._exit_stack.enter_async_context(http_client)

        read, write, _ = await self._exit_stack.enter_async_context(
            streamable_http_client(self.mcp_url, http_client=http_client)
        )
        self._session = await self._exit_stack.enter_async_context(
            ClientSession(read, write)
        )
        await self._session.initialize()

        tools_result = await self._session.list_tools()
        self._tools    = {t.name: t for t in tools_result.tools}
        self._tool_map = self._map_tools(tools_result.tools)
        print(f"  MCP tools: {list(self._tools.keys())}")
        if self._tool_map:
            print(f"  Mapped:    {self._tool_map}")
        else:
            print("  ⚠️  Could not auto-map any tools — check tool names above")

    def _map_tools(self, tools: list) -> dict:
        """Heuristically map logical ops to actual MCP tool names."""
        mapping: dict = {}
        SEARCH_KW = {"search", "query", "spl", "run"}
        INDEX_KW  = {"index", "indexes", "indices"}
        META_KW   = {"meta", "metadata", "sourcetype", "host"}

        def score(t, keywords):
            name = t.name.lower()
            desc = (getattr(t, "description", "") or "").lower()
            return sum(1 for kw in keywords if kw in name or kw in desc)

        ranked_search = sorted(tools, key=lambda t: score(t, SEARCH_KW), reverse=True)
        ranked_index  = sorted(tools, key=lambda t: score(t, INDEX_KW),  reverse=True)
        ranked_meta   = sorted(tools, key=lambda t: score(t, META_KW),   reverse=True)

        if ranked_search and score(ranked_search[0], SEARCH_KW) > 0:
            mapping["run_query"] = ranked_search[0].name

        for t in ranked_index:
            if t.name != mapping.get("run_query") and score(t, INDEX_KW) > 0:
                mapping["get_indexes"] = t.name
                break

        for t in ranked_meta:
            if t.name not in mapping.values() and score(t, META_KW) > 0:
                mapping["get_metadata"] = t.name
                break

        return mapping

    async def _call(self, tool_name: str, arguments: dict) -> dict:
        result = await self._session.call_tool(tool_name, arguments)
        if result.isError:
            texts = [c.text for c in result.content if hasattr(c, "text")]
            return {"error": "\n".join(texts) or "MCP tool returned an error"}
        texts = [c.text for c in result.content if hasattr(c, "text")]
        combined = "\n".join(texts)
        try:
            return json.loads(combined)
        except Exception:
            return {"result": combined}

    # ── argument builder ─────────────────────────────────────

    def _search_args(self, spl_query: str, max_results: int) -> dict:
        """Build call_tool arguments by inspecting the tool's input schema."""
        tool_name = self._tool_map.get("run_query")
        if not tool_name:
            return {"query": spl_query}
        schema = getattr(self._tools[tool_name], "inputSchema", None) or {}
        props  = schema.get("properties", {}) if isinstance(schema, dict) else {}

        args: dict = {}
        # Find the query parameter
        for candidate in ["query", "search", "spl", "search_query", "q"]:
            if not props or candidate in props:
                args[candidate] = spl_query
                break
        else:
            args["query"] = spl_query

        # Find the count/limit parameter
        for candidate in ["count", "max_count", "limit", "max_results", "num_results"]:
            if candidate in props:
                args[candidate] = max_results
                break

        return args

    # ── sync public interface (matches SplunkClient) ─────────

    def _sync(self, coro, timeout=60):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout=timeout)

    def run_query(self, spl_query: str, max_results: int = DEFAULT_MAX_RESULTS) -> dict:
        tool_name = self._tool_map.get("run_query")
        if not tool_name:
            return {"error": f"No search tool found. Available MCP tools: {list(self._tools.keys())}"}
        args = self._search_args(spl_query, max_results)
        return self._sync(self._call(tool_name, args))

    def get_indexes(self) -> dict:
        tool_name = self._tool_map.get("get_indexes")
        if tool_name:
            return self._sync(self._call(tool_name, {}))
        # Fallback: SPL via the search tool
        return self.run_query(
            "| rest /services/data/indexes output_mode=json "
            "| table title totalEventCount currentDBSizeMB",
            max_results=50,
        )

    def get_metadata(self, index: str = "main", metadata_type: str = "sourcetypes") -> dict:
        tool_name = self._tool_map.get("get_metadata")
        if tool_name:
            return self._sync(self._call(tool_name, {"index": index, "type": metadata_type}))
        return self.run_query(
            f"| metadata type={metadata_type} index={index} | sort -totalCount"
        )

    def close(self):
        if self._exit_stack:
            fut = asyncio.run_coroutine_threadsafe(self._exit_stack.aclose(), self._loop)
            try:
                fut.result(timeout=10)
            except Exception:
                pass
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


# ============================================================
# TOOL DEFINITIONS (Claude function calling)
# ============================================================
TOOLS = [
    {
        "name": "splunk_run_query",
        "description": (
            "Execute a Splunk SPL search and return results. Use it to query pipeline "
            "job logs, schema-change events, data-quality checks, lineage edges, and alerts. "
            "Always specify index=main. Examples: "
            "'index=main sourcetype=pipeline:data_quality status=fail | stats count by table check_name'; "
            "'index=main sourcetype=pipeline:schema_registry change_type=rename_column | table _time table old_field new_field'; "
            "'index=main sourcetype=pipeline:lineage | table upstream_table downstream_table produced_by_job | dedup upstream_table downstream_table'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string",
                          "description": "SPL query. Do NOT prefix with 'search'; it is added automatically."},
                "max_results": {"type": "integer", "description": "Max results (default 20)", "default": DEFAULT_MAX_RESULTS},
            },
            "required": ["query"],
        },
    },
    {
        "name": "splunk_get_indexes",
        "description": "List all Splunk indexes with event counts and sizes.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "splunk_get_metadata",
        "description": "Get metadata about hosts, sources, or sourcetypes in an index.",
        "input_schema": {
            "type": "object",
            "properties": {
                "index": {"type": "string", "description": "Index (default: main)", "default": "main"},
                "metadata_type": {"type": "string", "enum": ["sourcetypes", "sources", "hosts"],
                                  "default": "sourcetypes"},
            },
        },
    },
]


# ============================================================
# SYSTEM PROMPT  (data-pipeline / schema / lineage diagnosis)
# ============================================================
SYSTEM_PROMPT = """You are Pipeline Doctor, an expert AI Data Engineer that diagnoses DATA PIPELINE failures by querying Splunk. You specialize in broken schema contracts and data-lineage problems -- not infrastructure outages.

You have access to a Splunk instance with observability data from an e-commerce data platform.

PIPELINE TOPOLOGY (data lineage):
  source.orders_db.inventory --[ingest_inventory]--> raw.inventory_snapshot
    --[transform_inventory]--> mart.product_availability
    --[refresh_inventory_dashboard]--> dashboard.inventory_health

  source.orders_db.orders --[ingest_orders]--> raw.orders
    --[transform_revenue]--> mart.daily_revenue
    --[refresh_revenue_dashboard]--> dashboard.revenue_overview

Data lives in index=main with these sourcetypes:
- pipeline:job_log        - Job runs. Fields: job_name, stage (ingest/transform/serve), status,
                            rows_in, rows_out, duration_ms, worker_cpu_pct, worker_mem_pct,
                            level, message, source_table, target_table
- pipeline:schema_registry- Schema versions/changes. Fields: table, schema_version,
                            change_type (baseline/rename_column/add_column/drop_column/type_change),
                            old_field, new_field, migration_ticket
- pipeline:data_quality   - DQ checks. Fields: table, column, check_name (null_rate/row_count/freshness_lag_min),
                            value, threshold, status (pass/fail)
- pipeline:lineage        - Lineage edges. Fields: upstream_table, downstream_table, produced_by_job
- pipeline:alerts         - Alerts. Fields: target, severity, alert_name, description

YOUR DIAGNOSTIC APPROACH (lineage-first, NOT resource-first):
1. Locate the symptom: which table / dashboard is failing DQ or firing alerts?
   (e.g. sourcetype=pipeline:data_quality status=fail; sourcetype=pipeline:alerts)
2. RULE OUT the ordinary causes before assuming a schema break. Check the failing job's
   status, duration_ms, worker_cpu_pct, and the table's row_count + freshness_lag_min checks.
   - If duration/cpu/row_count/freshness are all NORMAL, it is NOT a resource, volume, or
     scheduling problem. A column-level null_rate spike with normal row_count points to a
     SCHEMA / data-contract problem.
3. Follow lineage UPSTREAM: use pipeline:lineage to find which job produces the bad table and
   what its upstream table is. Walk up the chain to find where the bad column FIRST goes null
   (earliest null_rate spike = closest to root cause).
4. Check the schema registry for that upstream source table around the incident start:
   sourcetype=pipeline:schema_registry change_type!=baseline. Look for rename/drop/type changes.
5. Correlate timestamps: schema-change event time vs the first null_rate spike vs job WARN logs.
   Read job_log messages -- ingestion often logs the missing column by name.
6. Root cause = the specific column change the pipeline did not adapt to. Name the exact field
   (old -> new) and the job that needs updating.

SPL TIPS:
- Always index=main; filter with sourcetype=.
- Use | stats, | timechart span=5m, | table, | dedup, | sort.
- Trace a column over time: sourcetype=pipeline:data_quality check_name=null_rate column=stock_count | timechart span=5m avg(value) by table
- Find schema changes: sourcetype=pipeline:schema_registry change_type=rename_column | table _time table old_field new_field migration_ticket
- Confirm blast radius: compare the inventory chain vs the (healthy) revenue chain.

When you have enough information, output a diagnosis report with:
1. **Summary** - what is broken and the user-visible impact
2. **Root Cause** - the schema/contract change, with the exact field rename and offending job
3. **Lineage / Propagation** - how the break flowed downstream table by table (with timestamps)
4. **Evidence** - specific Splunk data points (schema event, null_rate spikes, normal row_count/cpu)
5. **Remediation** - immediate fix + how to prevent recurrence (schema contracts, CI checks, alerts)
6. **Actionable Splunk Alerts** - 2-4 ready-to-use SPL saved-search alert definitions tailored to
   the specific root cause just diagnosed. For each alert provide:
   - **Title** - a short descriptive name
   - **SPL** - a complete runnable query using index=main and the exact table/column/job names
     found in the evidence (no generic placeholders)
   - **Trigger** - condition that fires the alert (e.g. "count > 0", "avg(value) > 0.05")
   - **Schedule** - how often the search runs (e.g. "every 5 minutes", "cron: */5 * * * *")
   - **Severity** - info / warning / critical
   Cover: (a) the earliest detectable signal of this root-cause type, (b) the business-impact
   signal that actually paged, and (c) a cross-chain comparison alert to catch blast-radius
   expansion to other pipelines.
7. **Confidence** - high/medium/low, and what data would raise it.

Be methodical. Explicitly note the signals that RULE OUT a resource/volume cause -- that is what
distinguishes a schema break from an outage. Show your reasoning at each step."""


# ============================================================
# AGENT LOOP
# ============================================================
def execute_tool(splunk, tool_name, tool_input):
    try:
        if tool_name == "splunk_run_query":
            return splunk.run_query(tool_input.get("query", ""), tool_input.get("max_results", DEFAULT_MAX_RESULTS))
        if tool_name == "splunk_get_indexes":
            return splunk.get_indexes()
        if tool_name == "splunk_get_metadata":
            return splunk.get_metadata(tool_input.get("index", "main"),
                                       tool_input.get("metadata_type", "sourcetypes"))
        return {"error": f"Unknown tool: {tool_name}"}
    except Exception as e:
        return {"error": str(e)}


def run_agent(question, verbose=False, use_rest=False):
    global SPLUNK_PASSWORD

    if not ANTHROPIC_API_KEY:
        print("❌ No Anthropic API key. Set ANTHROPIC_API_KEY env var or pass --api-key.")
        return

    # ── connect to Splunk ──────────────────────────────────────
    if use_rest:
        if not SPLUNK_PASSWORD:
            import getpass
            SPLUNK_PASSWORD = getpass.getpass(f"Enter Splunk password for '{SPLUNK_USERNAME}': ")
        print("\n🔌 Connecting to Splunk via REST API...")
        try:
            splunk = SplunkClient(SPLUNK_HOST, SPLUNK_USERNAME, SPLUNK_PASSWORD)
            print("✅ Connected to Splunk (REST)!")
        except Exception as e:
            print(f"❌ Failed to connect to Splunk: {e}")
            return
    else:
        if not _MCP_AVAILABLE:
            print("❌ mcp package not installed. Run: pip install mcp httpx")
            print("💡 Or use --use-rest to fall back to Splunk REST API")
            return
        if not MCP_TOKEN:
            print("❌ No MCP token. Set MCP_TOKEN in .env or pass --mcp-token.")
            print("💡 Or use --use-rest to fall back to Splunk REST API")
            return
        mcp_endpoint = MCP_URL or f"{SPLUNK_HOST}/services/mcp"
        print(f"\n🔌 Connecting to Splunk MCP Server at {mcp_endpoint}...")
        try:
            splunk = MCPSplunkClient(mcp_endpoint, MCP_TOKEN)
            print("✅ Connected via MCP!")
        except Exception as e:
            print(f"❌ MCP connection failed: {e}")
            print("💡 Try again with --use-rest to fall back to Splunk REST API")
            return

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    print("\n" + "=" * 70)
    print("🩺 Pipeline Doctor - AI Data Pipeline Fault Diagnosis")
    print("=" * 70)
    print(f"\n📋 Problem: {question}\n" + "-" * 70)

    messages = [{"role": "user", "content": question}]

    try:
        for turn in range(MAX_TURNS):
            print(f"\n🔄 Agent Turn {turn + 1}/{MAX_TURNS}")
            try:
                response = client.messages.create(
                    model=MODEL, max_tokens=4096,
                    system=SYSTEM_PROMPT, tools=TOOLS, messages=messages,
                )
            except anthropic.APIError as e:
                print(f"\n❌ API Error: {e}")
                return

            if response.stop_reason == "end_turn":
                final_text = "".join(b.text for b in response.content if b.type == "text")
                print("\n" + "=" * 70 + "\n📋 DIAGNOSIS REPORT\n" + "=" * 70)
                print(final_text)
                print("=" * 70)
                report_file = f"diagnosis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
                with open(report_file, "w", encoding="utf-8") as f:
                    f.write("# Pipeline Doctor - Diagnosis Report\n\n")
                    f.write(f"**Generated:** {datetime.now().isoformat()}\n\n")
                    f.write(f"**Problem:** {question}\n\n---\n\n")
                    f.write(final_text)
                print(f"\n💾 Report saved to: {report_file}")
                return

            if response.stop_reason == "tool_use":
                messages.append({"role": "assistant", "content": response.content})
                tool_results = []
                for block in response.content:
                    if block.type == "text" and block.text and verbose:
                        print(f"\n💭 Agent reasoning:\n{block.text}")
                    elif block.type == "tool_use":
                        print(f"\n  🔍 Tool: {block.name}")
                        if block.name == "splunk_run_query":
                            print(f"     Query: {block.input.get('query', '')}")
                        result = execute_tool(splunk, block.name, block.input)
                        result_str = json.dumps(result, indent=2, default=str)
                        print(f"  📊 Result: {result_str[:500]}" + ("..." if len(result_str) > 500 else ""))
                        stored_str = result_str
                        if len(stored_str) > MAX_TOOL_RESULT_CHARS:
                            stored_str = (stored_str[:MAX_TOOL_RESULT_CHARS]
                                          + f"\n... [truncated to {MAX_TOOL_RESULT_CHARS} chars]")
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": stored_str,
                        })
                messages.append({"role": "user", "content": tool_results})
                if turn < MAX_TURNS - 1:
                    time.sleep(TURN_DELAY_SEC)
            else:
                print(f"\n⚠️ Unexpected stop reason: {response.stop_reason}")
                break

        print("\n⚠️ Max turns reached without completing diagnosis.")

    finally:
        if hasattr(splunk, "close"):
            splunk.close()


# ============================================================
# SCENARIO DEFAULTS
# ============================================================
SCENARIO_QUESTIONS = {
    "schema_change": (
        "The inventory dashboard shows every product as out of stock, "
        "but sales data looks fine. Please investigate and diagnose the root cause."
    ),
    "volume_drop": (
        "The inventory dashboard numbers dropped dramatically overnight — "
        "product counts are a fraction of what they should be. "
        "But no jobs have failed and no alerts fired until just now. What happened?"
    ),
    "freshness_delay": (
        "Business users are complaining that the inventory dashboard hasn't updated "
        "since yesterday. The numbers look plausible but appear to be stale. "
        "No jobs are showing as failed. Please investigate."
    ),
}

_DEFAULT_QUESTION = SCENARIO_QUESTIONS["schema_change"]


# ============================================================
# ENTRY POINT
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Pipeline Doctor - AI Data Pipeline Fault Diagnosis Agent")
    parser.add_argument("--scenario", type=str, choices=list(SCENARIO_QUESTIONS.keys()), default=None,
                        help="Scenario to diagnose; sets a default --question for that scenario")
    parser.add_argument("--question", type=str, default=None,
                        help="Override the diagnostic question (takes precedence over --scenario)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed agent reasoning")
    parser.add_argument("--use-rest", action="store_true",
                        help="Use Splunk REST API instead of MCP (fallback mode)")
    parser.add_argument("--api-key",    type=str, default=None, help="Anthropic API key (overrides env)")
    parser.add_argument("--splunk-host", type=str, default=None)
    parser.add_argument("--splunk-user", type=str, default=None)
    parser.add_argument("--splunk-pass", type=str, default=None)
    parser.add_argument("--mcp-token",  type=str, default=None, help="MCP Bearer token (overrides MCP_TOKEN env)")
    parser.add_argument("--mcp-url",    type=str, default=None, help="MCP server URL (overrides MCP_URL env)")
    args = parser.parse_args()

    if args.question is not None:
        question = args.question
    elif args.scenario is not None:
        question = SCENARIO_QUESTIONS[args.scenario]
    else:
        question = _DEFAULT_QUESTION

    global ANTHROPIC_API_KEY, SPLUNK_HOST, SPLUNK_USERNAME, SPLUNK_PASSWORD, MCP_TOKEN, MCP_URL
    if args.api_key:     ANTHROPIC_API_KEY = args.api_key
    if args.splunk_host: SPLUNK_HOST       = args.splunk_host
    if args.splunk_user: SPLUNK_USERNAME   = args.splunk_user
    if args.splunk_pass: SPLUNK_PASSWORD   = args.splunk_pass
    if args.mcp_token:   MCP_TOKEN         = args.mcp_token
    if args.mcp_url:     MCP_URL           = args.mcp_url

    run_agent(question, verbose=args.verbose, use_rest=args.use_rest)


if __name__ == "__main__":
    main()
