"""
Hourly self-diagnostic: checks the bot's own health and logs issues.
Runs automatically every hour during the main loop.
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta

MARKETS_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")
SCORES_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "filter_scores.sqlite")
DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")
DIAG_LOG = os.path.join(os.path.dirname(__file__), "..", "logs", "diagnostics.log")


def run_self_check() -> list[str]:
    """Run all diagnostics and return list of issues found."""
    issues = []
    stats = []

    now = datetime.now(timezone.utc).isoformat()

    # 1. Are we scanning markets?
    if os.path.exists(MARKETS_DB):
        conn = sqlite3.connect(MARKETS_DB)
        total = conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
        active = conn.execute(
            "SELECT COUNT(*) FROM markets WHERE status IN ('open','active')"
        ).fetchone()[0]
        with_price = conn.execute(
            "SELECT COUNT(*) FROM markets WHERE last_price IS NOT NULL AND last_price > 0"
        ).fetchone()[0]
        conn.close()
        stats.append(f"Markets: {total} total, {active} active, {with_price} with prices")

        if total == 0:
            issues.append("CRITICAL: No markets in DB. Scanner may be failing.")
        elif with_price == 0:
            issues.append("HIGH: All markets have null/zero prices. Scanner parsing issue.")
        elif with_price < 10:
            issues.append(f"MEDIUM: Only {with_price} markets have prices. Expected 100+.")
    else:
        issues.append("CRITICAL: Markets DB doesn't exist. Scanner hasn't run.")

    # 2. Are we scoring markets?
    if os.path.exists(SCORES_DB):
        conn = sqlite3.connect(SCORES_DB)
        total_scores = conn.execute("SELECT COUNT(*) FROM filter_scores").fetchone()[0]
        recent = conn.execute(
            "SELECT COUNT(*) FROM filter_scores WHERE scored_at > datetime('now', '-1 hour')"
        ).fetchone()[0]
        passed = conn.execute(
            "SELECT COUNT(*) FROM filter_scores WHERE passed_filter = 1"
        ).fetchone()[0]
        conn.close()
        stats.append(f"Filter: {total_scores} scored, {recent} in last hour, {passed} passed")

        if total_scores == 0:
            issues.append("HIGH: No markets scored. Local model filter may be broken.")
        elif recent == 0 and total_scores > 0:
            issues.append("MEDIUM: No scoring in last hour. Ollama may be down or all markets cached.")
    else:
        stats.append("Filter: no scores DB yet")

    # 3. Are we making decisions?
    if os.path.exists(DECISIONS_DB):
        conn = sqlite3.connect(DECISIONS_DB)
        total_decisions = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        recent_decisions = conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE decided_at > datetime('now', '-6 hours')"
        ).fetchone()[0]
        conn.close()
        stats.append(f"Decisions: {total_decisions} total, {recent_decisions} in last 6 hours")
    else:
        stats.append("Decisions: no DB yet")

    # 4. Is Ollama responding?
    try:
        import requests
        r = requests.get("http://localhost:11434/api/tags", timeout=5)
        if r.status_code == 200:
            models = [m["name"] for m in r.json().get("models", [])]
            stats.append(f"Ollama: OK ({len(models)} models loaded)")
        else:
            issues.append(f"HIGH: Ollama returned HTTP {r.status_code}")
    except Exception as e:
        issues.append(f"CRITICAL: Ollama not responding: {e}")

    # 5. Check disk space
    try:
        import shutil
        usage = shutil.disk_usage(os.path.dirname(__file__))
        free_gb = usage.free / (1024 ** 3)
        stats.append(f"Disk: {free_gb:.1f} GB free")
        if free_gb < 5:
            issues.append(f"MEDIUM: Low disk space ({free_gb:.1f} GB free)")
    except Exception:
        pass

    # Log results
    os.makedirs(os.path.dirname(DIAG_LOG), exist_ok=True)
    with open(DIAG_LOG, "a") as f:
        f.write(f"\n--- Self-check {now} ---\n")
        for s in stats:
            f.write(f"  {s}\n")
        if issues:
            for i in issues:
                f.write(f"  !! {i}\n")
        else:
            f.write("  All checks passed.\n")

    # Print summary
    print(f"\n  Self-check: {len(issues)} issues found")
    for s in stats:
        print(f"    {s}")
    for i in issues:
        print(f"    !! {i}")

    return issues


if __name__ == "__main__":
    issues = run_self_check()
    if not issues:
        print("\nAll systems healthy.")
