#!/usr/bin/env python3
"""
Pipeline Doctor - Splunk Alert Rule Generator

Closes the loop: after the agent diagnoses a fault, this script provisions the
preventive Splunk alert that would have caught it earlier. It reads alert
definitions from `alert_rules.json` and creates them as scheduled saved
searches via the Splunk REST API.

Each rule maps 1:1 to one fault scenario, because each scenario breaks exactly
one data-quality dimension on the inventory chain:

    schema_change   -> null_rate breach
    volume_drop     -> row_count collapse
    freshness_delay -> freshness_lag_min breach

Connection reuses the same REST login as agent.py (SPLUNK_HOST / SPLUNK_USERNAME
/ SPLUNK_PASSWORD from .env) -- no new dependency, env var, or token.

Usage:
    python generate_alerts.py --dry-run        # preview SPL, create nothing
    python generate_alerts.py                  # create / update all alerts
    python generate_alerts.py --share          # also set app-level sharing
    python generate_alerts.py --list           # list provisioned alerts
    python generate_alerts.py --delete         # remove all alerts in the file

Security note: secrets are read from environment / .env. Do NOT hardcode them.
"""

import os
import sys
import json
import argparse
import xml.etree.ElementTree as ET
from urllib.parse import quote

import requests
import urllib3

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# CONFIGURATION  (no secrets hardcoded -- same env vars as agent.py)
# ============================================================
SPLUNK_HOST     = os.environ.get("SPLUNK_HOST", "https://localhost:8089")
SPLUNK_USERNAME = os.environ.get("SPLUNK_USERNAME", "admin")
SPLUNK_PASSWORD = os.environ.get("SPLUNK_PASSWORD", "")

RULES_FILE = "alert_rules.json"


# ============================================================
# SPLUNK REST CLIENT  (same login flow as agent.py SplunkClient)
# ============================================================
class SplunkAlertClient:
    def __init__(self, host, username, password):
        self.host = host.rstrip("/")
        self.username = username
        self.session_key = self._login(username, password)

    def _login(self, username, password):
        resp = requests.post(
            f"{self.host}/services/auth/login",
            data={"username": username, "password": password},
            verify=False, timeout=15,
        )
        if resp.status_code != 200:
            raise Exception(f"Splunk login failed: {resp.status_code} - {resp.text}")
        session_key = ET.fromstring(resp.text).findtext("sessionKey")
        if not session_key:
            raise Exception("Could not extract session key from login response")
        return session_key

    def _headers(self):
        return {"Authorization": f"Splunk {self.session_key}"}

    def _ns(self, app):
        # Provision in the user's namespace inside the given app (e.g. "search")
        # so the alerts show up under Search & Reporting -> Alerts.
        return f"{self.host}/servicesNS/{quote(self.username)}/{quote(app)}/saved/searches"

    def upsert_alert(self, app, rule):
        """Create the saved search; if it already exists, update it in place."""
        name = rule["name"]
        payload = {
            "name": name,
            "search": rule["search"],
            "is_scheduled": "1",
            "cron_schedule": rule.get("cron_schedule", "*/5 * * * *"),
            "dispatch.earliest_time": rule.get("earliest_time", "-30m"),
            "dispatch.latest_time": rule.get("latest_time", "now"),
            "alert_type": "number of events",
            "alert_comparator": rule.get("alert_comparator", "greater than"),
            "alert_threshold": str(rule.get("alert_threshold", "0")),
            "alert.severity": str(rule.get("severity", 4)),
            "alert.track": "1",
            "description": rule.get("description", ""),
            "disabled": "1" if rule.get("disabled", False) else "0",
            "output_mode": "json",
        }

        # Try create first.
        resp = requests.post(self._ns(app), headers=self._headers(),
                             data=payload, verify=False, timeout=30)
        if resp.status_code in (200, 201):
            return "created"

        # Already exists -> update via the per-object endpoint (name in the path,
        # and not repeated in the body).
        if resp.status_code == 409:
            update_url = f"{self._ns(app)}/{quote(name)}"
            update_payload = {k: v for k, v in payload.items() if k != "name"}
            resp = requests.post(update_url, headers=self._headers(),
                                 data=update_payload, verify=False, timeout=30)
            if resp.status_code in (200, 201):
                return "updated"

        raise Exception(f"{resp.status_code} - {resp.text[:300]}")

    def set_sharing_app(self, app, name):
        url = f"{self._ns(app)}/{quote(name)}/acl"
        resp = requests.post(url, headers=self._headers(),
                             data={"sharing": "app", "owner": self.username,
                                   "output_mode": "json"},
                             verify=False, timeout=30)
        return resp.status_code in (200, 201)

    def delete_alert(self, app, name):
        url = f"{self._ns(app)}/{quote(name)}"
        resp = requests.delete(url, headers=self._headers(),
                               params={"output_mode": "json"},
                               verify=False, timeout=30)
        return resp.status_code in (200, 201)

    def list_alerts(self, app):
        resp = requests.get(self._ns(app), headers=self._headers(),
                            params={"output_mode": "json", "count": 100,
                                    "search": "PipelineDoctor"},
                            verify=False, timeout=30)
        if resp.status_code != 200:
            return []
        out = []
        for entry in resp.json().get("entry", []):
            c = entry.get("content", {})
            out.append({
                "name": entry.get("name"),
                "cron": c.get("cron_schedule"),
                "scheduled": c.get("is_scheduled"),
                "disabled": c.get("disabled"),
            })
        return out


# ============================================================
# HELPERS
# ============================================================
def load_rules(path):
    if not os.path.exists(path):
        raise SystemExit(f"Rules file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("app", "search"), data.get("alerts", [])


def connect():
    global SPLUNK_PASSWORD
    if not SPLUNK_PASSWORD:
        import getpass
        SPLUNK_PASSWORD = getpass.getpass(f"Enter Splunk password for '{SPLUNK_USERNAME}': ")
    print(f"\nConnecting to Splunk at {SPLUNK_HOST} ...")
    client = SplunkAlertClient(SPLUNK_HOST, SPLUNK_USERNAME, SPLUNK_PASSWORD)
    print("Connected (REST).")
    return client


# ============================================================
# ENTRY POINT
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Pipeline Doctor - Splunk Alert Rule Generator")
    parser.add_argument("--rules", default=RULES_FILE, help="Path to alert_rules.json")
    parser.add_argument("--dry-run", action="store_true", help="Print the SPL without creating anything")
    parser.add_argument("--share", action="store_true", help="Set app-level sharing after creating")
    parser.add_argument("--list", action="store_true", help="List provisioned Pipeline Doctor alerts")
    parser.add_argument("--delete", action="store_true", help="Delete the alerts defined in the rules file")
    parser.add_argument("--splunk-host", default=None)
    parser.add_argument("--splunk-user", default=None)
    parser.add_argument("--splunk-pass", default=None)
    args = parser.parse_args()

    global SPLUNK_HOST, SPLUNK_USERNAME, SPLUNK_PASSWORD
    if args.splunk_host: SPLUNK_HOST     = args.splunk_host
    if args.splunk_user: SPLUNK_USERNAME = args.splunk_user
    if args.splunk_pass: SPLUNK_PASSWORD = args.splunk_pass

    app, rules = load_rules(args.rules)
    print("=" * 64)
    print("Pipeline Doctor - Alert Rule Generator")
    print("=" * 64)
    print(f"Rules file : {args.rules}")
    print(f"App        : {app}")
    print(f"Alerts     : {len(rules)}")

    # --dry-run needs no connection.
    if args.dry_run:
        for r in rules:
            print("\n" + "-" * 64)
            print(f"[{r.get('scenario', '?')}] {r['name']}")
            print(f"  cron     : {r.get('cron_schedule')}  window: {r.get('earliest_time')}..{r.get('latest_time')}")
            print(f"  trigger  : results {r.get('alert_comparator')} {r.get('alert_threshold')}  severity={r.get('severity')}")
            print(f"  search   : {r['search']}")
        print("\n[DRY RUN] Nothing was created.")
        return

    client = connect()

    if args.list:
        found = client.list_alerts(app)
        if not found:
            print("\nNo Pipeline Doctor alerts found.")
        else:
            print(f"\nProvisioned alerts ({len(found)}):")
            for a in found:
                state = "disabled" if str(a["disabled"]).lower() in ("1", "true") else "enabled"
                print(f"  - {a['name']}  [{state}, cron={a['cron']}]")
        return

    if args.delete:
        print("\nDeleting alerts...")
        for r in rules:
            ok = client.delete_alert(app, r["name"])
            print(f"  {'removed ' if ok else 'missing '} {r['name']}")
        return

    print("\nProvisioning alerts...")
    failures = 0
    for r in rules:
        try:
            action = client.upsert_alert(app, r)
            shared = ""
            if args.share:
                shared = " (shared:app)" if client.set_sharing_app(app, r["name"]) else " (share failed)"
            print(f"  {action:8s} {r['name']}{shared}")
        except Exception as e:
            failures += 1
            print(f"  FAILED   {r['name']} -> {e}")

    print("\nDone.")
    print("View in Splunk: Settings -> Searches, reports, and alerts (filter: PipelineDoctor)")
    print("Triggered alerts appear under Activity -> Triggered Alerts.")
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()

