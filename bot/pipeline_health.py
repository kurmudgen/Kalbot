"""
Pipeline health monitor — operational self-diagnosis.

Detects and fixes pipeline problems that prevent trades from executing:
- API budget exhaustion before validated categories are scored
- Filter→ensemble gaps (markets passing filter but never reaching analyst)
- Zero-trade periods despite available markets
- Gates blocking 100% of markets (miscalibrated thresholds)
- Stale caches preventing re-scoring after config changes

Runs every hour from dual_strategy.py. Auto-fixes what it can,
alerts for what it can't.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

BASE = os.path.join(os.path.dirname(__file__), "..")
DECISIONS_DB = os.path.join(BASE, "logs", "decisions.sqlite")
MARKETS_DB = os.path.join(BASE, "data", "live", "markets.sqlite")
FILTER_DB = os.path.join(BASE, "data", "live", "filter_scores.sqlite")
ANALYST_DB = os.path.join(BASE, "data", "live", "analyst_scores.sqlite")
API_CACHE_DB = os.path.join(BASE, "logs", "api_cache.sqlite")

API_LIMITS = {
    "gemini": int(os.getenv("GEMINI_DAILY_BUDGET", "200")),
    "deepseek": int(os.getenv("DEEPSEEK_DAILY_BUDGET", "100")),
    "claude": int(os.getenv("CLAUDE_DAILY_BUDGET", "75")),
    "perplexity": int(os.getenv("PERPLEXITY_DAILY_BUDGET", "50")),
}

VALIDATED_CATEGORIES = {"weather", "tsa", "inflation"}


def _alert(message: str, level: str = "warning"):
    """Send alert via Telegram."""
    print(f"  [HEALTH] {level.upper()}: {message}")
    try:
        from telegram_alerts import system_alert
        system_alert(f"Pipeline Health: {message}", level)
    except Exception:
        pass


def _log(conn: sqlite3.Connection, observation: str, action: str):
    """Log to calibration_reflections."""
    try:
        conn.execute(
            "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, ?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), "0.5", "pipeline_health",
             observation[:500], action),
        )
        conn.commit()
    except Exception:
        pass


def check_api_budget_waste() -> list[str]:
    """Check if non-validated categories are consuming API budget."""
    fixes = []
    if not os.path.exists(API_CACHE_DB):
        return fixes

    conn = sqlite3.connect(API_CACHE_DB)
    conn.row_factory = sqlite3.Row
    today = datetime.now().strftime("%Y-%m-%d")

    rows = conn.execute(
        "SELECT model, call_count FROM daily_call_counts WHERE date = ?", (today,)
    ).fetchall()

    exhausted = []
    for r in rows:
        limit = API_LIMITS.get(r["model"], 999)
        if r["call_count"] >= limit:
            exhausted.append(r["model"])

    if not exhausted:
        conn.close()
        return fixes

    # Check if non-validated markets are in the pipeline
    if os.path.exists(MARKETS_DB):
        mconn = sqlite3.connect(MARKETS_DB)
        non_valid = mconn.execute(
            """SELECT COUNT(*) FROM markets WHERE status IN ('open','active')
               AND ticker NOT LIKE 'KXHIGH%' AND ticker NOT LIKE 'KXLOW%'
               AND ticker NOT LIKE 'KXTSA%' AND ticker NOT LIKE 'TSA%'
               AND ticker NOT LIKE 'KXCPI%' AND ticker NOT LIKE 'KXPCE%'"""
        ).fetchone()[0]
        mconn.close()

        if non_valid > 0:
            # Auto-fix: purge non-validated from caches
            for db_path, table in [
                (MARKETS_DB, "markets"),
                (FILTER_DB, "filter_scores"),
                (ANALYST_DB, "analyst_scores"),
            ]:
                if os.path.exists(db_path):
                    c = sqlite3.connect(db_path)
                    col = "ticker"
                    deleted = c.execute(
                        f"""DELETE FROM {table} WHERE {col} NOT LIKE 'KXHIGH%'
                            AND {col} NOT LIKE 'KXLOW%' AND {col} NOT LIKE 'KXTSA%'
                            AND {col} NOT LIKE 'TSA%' AND {col} NOT LIKE 'KXCPI%'
                            AND {col} NOT LIKE 'KXPCE%'"""
                    ).rowcount
                    c.commit()
                    c.close()
                    if deleted > 0:
                        fixes.append(f"Purged {deleted} non-validated from {table}")

            # Reset exhausted budgets
            conn.execute("DELETE FROM daily_call_counts WHERE date = ?", (today,))
            conn.commit()
            fixes.append(f"Reset exhausted API budgets: {exhausted}")

    conn.close()
    return fixes


def check_filter_ensemble_gap() -> list[str]:
    """Check if markets pass filter but never reach ensemble."""
    issues = []
    if not os.path.exists(FILTER_DB) or not os.path.exists(ANALYST_DB):
        return issues

    fconn = sqlite3.connect(FILTER_DB)
    filter_passed = {r[0] for r in fconn.execute(
        "SELECT ticker FROM filter_scores WHERE category IN ('weather','tsa','inflation') "
        "AND passed_filter=1 AND scored_at > datetime('now', '-6 hours')"
    ).fetchall()}
    fconn.close()

    aconn = sqlite3.connect(ANALYST_DB)
    analyzed = {r[0] for r in aconn.execute(
        "SELECT ticker FROM analyst_scores WHERE category IN ('weather','tsa','inflation')"
    ).fetchall()}
    aconn.close()

    gap = filter_passed - analyzed
    if len(gap) > 5:
        issues.append(f"{len(gap)} weather/tsa markets passed filter but not analyzed (API budget issue?)")

    return issues


def check_zero_trade_period() -> list[str]:
    """Check if bot has gone too long without executing."""
    issues = []
    if not os.path.exists(DECISIONS_DB):
        return issues

    conn = sqlite3.connect(DECISIONS_DB)
    conn.row_factory = sqlite3.Row

    # Last LIVE execution
    last_exec = conn.execute(
        "SELECT decided_at FROM decisions WHERE executed=1 AND mode='LIVE' ORDER BY id DESC LIMIT 1"
    ).fetchone()

    # Last execution of any kind
    last_any = conn.execute(
        "SELECT decided_at FROM decisions WHERE executed=1 ORDER BY id DESC LIMIT 1"
    ).fetchone()

    # How many weather markets are available?
    if os.path.exists(MARKETS_DB):
        mconn = sqlite3.connect(MARKETS_DB)
        wx_count = mconn.execute(
            "SELECT COUNT(*) FROM markets WHERE ticker LIKE 'KXHIGH%' AND status IN ('open','active')"
        ).fetchone()[0]
        mconn.close()
    else:
        wx_count = 0

    if wx_count > 5 and not last_exec:
        issues.append(f"Never executed a LIVE trade. {wx_count} weather markets available.")
    elif wx_count > 5 and last_exec:
        hours_ago = (datetime.now(timezone.utc) -
                     datetime.fromisoformat(last_exec[0].replace("Z", "+00:00"))).total_seconds() / 3600
        if hours_ago > 12:
            issues.append(f"No LIVE execution in {hours_ago:.0f}h despite {wx_count} weather markets")

    conn.close()
    return issues


def check_gate_blockage() -> list[str]:
    """Check if any gate is blocking 100% of weather markets."""
    issues = []
    if not os.path.exists(DECISIONS_DB):
        return issues

    conn = sqlite3.connect(DECISIONS_DB)
    conn.row_factory = sqlite3.Row

    # Weather decisions in last 6 hours
    rows = conn.execute(
        """SELECT error, COUNT(*) as cnt FROM decisions
           WHERE category='weather' AND decided_at > datetime('now', '-6 hours')
           AND error IS NOT NULL
           GROUP BY error ORDER BY cnt DESC"""
    ).fetchall()

    total_wx = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE category='weather' AND decided_at > datetime('now', '-6 hours')"
    ).fetchone()[0]

    executed_wx = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE category='weather' AND decided_at > datetime('now', '-6 hours') AND executed=1"
    ).fetchone()[0]

    conn.close()

    if total_wx > 10 and executed_wx == 0:
        # Find the dominant blocker
        if rows:
            top_error = rows[0]["error"][:60]
            top_pct = rows[0]["cnt"] / total_wx * 100
            issues.append(f"0/{total_wx} weather trades executed. Top blocker ({top_pct:.0f}%): {top_error}")

    return issues


def run_health_check() -> dict:
    """Run all health checks. Auto-fix what's possible, alert the rest."""
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fixes_applied": [],
        "issues_found": [],
        "status": "healthy",
    }

    # Check 1: API budget waste
    fixes = check_api_budget_waste()
    if fixes:
        results["fixes_applied"].extend(fixes)
        for f in fixes:
            print(f"  [HEALTH] AUTO-FIX: {f}")

    # Check 2: Filter→ensemble gap
    issues = check_filter_ensemble_gap()
    results["issues_found"].extend(issues)

    # Check 3: Zero-trade period
    issues = check_zero_trade_period()
    results["issues_found"].extend(issues)

    # Check 4: Gate blockage
    issues = check_gate_blockage()
    results["issues_found"].extend(issues)

    # Alert if issues found
    if results["issues_found"]:
        results["status"] = "degraded"
        msg = "Pipeline issues:\n" + "\n".join(f"- {i}" for i in results["issues_found"])
        _alert(msg)

    if results["fixes_applied"]:
        msg = "Auto-fixes applied:\n" + "\n".join(f"- {f}" for f in results["fixes_applied"])
        print(f"  [HEALTH] {msg}")

    # Log to calibration_reflections
    if os.path.exists(DECISIONS_DB):
        conn = sqlite3.connect(DECISIONS_DB)
        _log(conn, json.dumps(results, default=str)[:500],
             "health_check_" + results["status"])
        conn.close()

    if not results["fixes_applied"] and not results["issues_found"]:
        print("  [HEALTH] All checks passed")

    return results


if __name__ == "__main__":
    print("=== Pipeline Health Check ===")
    results = run_health_check()
    print(f"\nStatus: {results['status']}")
    print(f"Fixes: {len(results['fixes_applied'])}")
    print(f"Issues: {len(results['issues_found'])}")
