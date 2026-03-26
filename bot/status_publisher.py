"""
Generates dashboard/status.json from all SQLite DBs and pushes to GitHub.
Called at the end of every cycle by the main bot loop.
"""

import json
import os
import sqlite3
import subprocess
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
STATUS_PATH = os.path.join(BASE_DIR, "dashboard", "status.json")
PUBLIC_STATUS_PATH = os.path.join(BASE_DIR, "dashboard", "public", "status.json")
MARKETS_DB = os.path.join(BASE_DIR, "data", "live", "markets.sqlite")
SCORES_DB = os.path.join(BASE_DIR, "data", "live", "filter_scores.sqlite")
ANALYST_DB = os.path.join(BASE_DIR, "data", "live", "analyst_scores.sqlite")
DECISIONS_DB = os.path.join(BASE_DIR, "logs", "decisions.sqlite")
SESSIONS_DB = os.path.join(BASE_DIR, "logs", "sessions.sqlite")
NEWS_DB = os.path.join(BASE_DIR, "logs", "news_pool.sqlite")


def _query(db_path, sql, params=()):
    if not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _count(db_path, table, where=""):
    if not os.path.exists(db_path):
        return 0
    conn = sqlite3.connect(db_path)
    sql = f"SELECT COUNT(*) FROM {table}"
    if where:
        sql += f" WHERE {where}"
    n = conn.execute(sql).fetchone()[0]
    conn.close()
    return n


def _build_pnl(total_pnl, pnl_history):
    """Build P&L section including resolved trade stats."""
    result = {
        "today": total_pnl,
        "all_time": total_pnl,
        "history": pnl_history,
        "resolved": {"total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0, "actual_pnl": 0},
    }
    try:
        from resolution_tracker import get_lifetime_stats
        stats = get_lifetime_stats()
        if stats["trades"] > 0:
            result["resolved"] = {
                "total_trades": stats["trades"],
                "wins": stats["wins"],
                "losses": stats["losses"],
                "win_rate": stats["win_rate"],
                "actual_pnl": stats["total_pnl"],
            }
            result["all_time"] = stats["total_pnl"]
    except Exception:
        pass
    return result


def generate_status() -> dict:
    now = datetime.now(timezone.utc).isoformat()

    # Session
    sessions = _query(SESSIONS_DB,
        "SELECT * FROM sessions ORDER BY start_time DESC LIMIT 1")
    session = sessions[0] if sessions else {}

    # Accounts
    kalshi_balance = 50  # TODO: pull from Kalshi API
    alpaca = {"cash": 100000, "portfolio_value": 100000, "paper_mode": True}
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(BASE_DIR, ".env"), override=True)
        paper_kalshi = os.getenv("PAPER_TRADE", "true").lower() == "true"
        paper_alpaca = os.getenv("ALPACA_PAPER", "true").lower() == "true"
    except Exception:
        paper_kalshi = True
        paper_alpaca = True

    # Trades
    trades = _query(DECISIONS_DB,
        "SELECT * FROM decisions ORDER BY decided_at DESC LIMIT 50")
    executed = [t for t in trades if t.get("executed")]
    skipped = [t for t in trades if not t.get("executed")]

    # P&L (simplified — counts executed trades)
    total_pnl = sum(t.get("amount", 0) * 0.18 for t in executed)  # Rough estimate

    # P&L history
    pnl_history = []
    daily = _query(DECISIONS_DB,
        """SELECT DATE(decided_at) as date, COUNT(*) as trades,
           SUM(CASE WHEN executed = 1 THEN amount ELSE 0 END) as deployed
           FROM decisions GROUP BY DATE(decided_at) ORDER BY date""")
    for d in daily:
        pnl_history.append({
            "date": d["date"],
            "trades": d["trades"],
            "deployed": d["deployed"] or 0,
        })

    # Strategies
    strategy_data = {}
    strategy_trades = _query(DECISIONS_DB,
        """SELECT session_id, COUNT(*) as trades,
           SUM(CASE WHEN executed = 1 THEN 1 ELSE 0 END) as executed,
           SUM(CASE WHEN executed = 1 THEN amount ELSE 0 END) as deployed
           FROM decisions GROUP BY session_id""")
    for st in strategy_trades:
        sid = st.get("session_id", "")
        for name in ["WEATHER", "SNIPER", "EMERGING", "SP500", "ECON",
                      "BITCOIN", "TREASURY", "GAS", "FOREX", "QUANT"]:
            if name in sid.upper():
                if name not in strategy_data:
                    strategy_data[name] = {"trades": 0, "executed": 0, "deployed": 0}
                strategy_data[name]["trades"] += st["trades"]
                strategy_data[name]["executed"] += st["executed"]
                strategy_data[name]["deployed"] += st["deployed"] or 0

    # Ensemble
    ensemble_data = {"models": {}, "consensus_rate": 0, "recent_votes": []}
    details = _query(ANALYST_DB,
        "SELECT * FROM ensemble_details ORDER BY analyzed_at DESC LIMIT 20")
    if details:
        consensus_count = sum(1 for d in details if d.get("consensus"))
        ensemble_data["consensus_rate"] = consensus_count / len(details) if details else 0

        # Per-model stats
        for provider in ["perplexity", "claude", "deepseek"]:
            prob_key = f"{provider}_prob"
            conf_key = f"{provider}_conf"
            probs = [d[prob_key] for d in details if d.get(prob_key) is not None]
            ensemble_data["models"][provider] = {
                "analyses": len(probs),
                "avg_probability": sum(probs) / len(probs) if probs else 0,
            }

        # Recent votes
        for d in details[:10]:
            ensemble_data["recent_votes"].append({
                "perplexity": d.get("perplexity_prob"),
                "claude": d.get("claude_prob"),
                "deepseek": d.get("deepseek_prob"),
                "consensus": bool(d.get("consensus")),
                "time": d.get("analyzed_at", "")[:19],
            })

    # Scanner
    scanner = {
        "total_markets": _count(MARKETS_DB, "markets"),
        "with_prices": _count(MARKETS_DB, "markets", "last_price > 0"),
        "passed_filter": _count(SCORES_DB, "filter_scores", "passed_filter = 1"),
        "total_scored": _count(SCORES_DB, "filter_scores"),
    }

    # Categories
    cats = _query(MARKETS_DB,
        "SELECT category, COUNT(*) as n FROM markets GROUP BY category")
    scanner["categories"] = {c["category"]: c["n"] for c in cats}

    # News pool
    news = _query(NEWS_DB,
        "SELECT * FROM research ORDER BY created_at DESC LIMIT 10")
    breaking = _query(NEWS_DB,
        "SELECT * FROM breaking_news ORDER BY created_at DESC LIMIT 5") if os.path.exists(NEWS_DB) else []

    # Alerts
    alerts = []
    if session.get("errors", 0) > 0:
        error_count = session["errors"]
        cycles = session.get("cycles_completed", 1) or 1
        per_cycle = error_count / cycles
        detail = f"{error_count} non-fatal exceptions across {cycles} cycles (~{per_cycle:.1f}/cycle). "
        detail += "Typical causes: API timeouts, CoinGecko rate limits, model inference delays. "
        detail += "Not trade rejections (those are tracked separately in the executor)."
        alerts.append({
            "level": "warning" if error_count < 200 else "critical",
            "message": f"{error_count} API retries this session",
            "detail": detail,
        })
    if os.path.exists(os.path.join(BASE_DIR, "STOP")):
        alerts.append({"level": "critical", "message": "Kill switch is ACTIVE", "detail": "Delete S:\\kalbot\\STOP to resume trading."})

    # Sniper markets (expiring soon)
    sniper_markets = _query(MARKETS_DB,
        """SELECT ticker, title, category, last_price, close_time
           FROM markets WHERE status IN ('open', 'active')
           AND close_time != '' AND last_price > 0
           ORDER BY close_time ASC LIMIT 10""")

    # System
    system = {
        "ollama": "unknown",
        "errors_24h": session.get("errors", 0),
        "cycles": session.get("cycles_completed", 0),
        "uptime_hours": 0,
    }
    try:
        import requests
        r = requests.get("http://localhost:11434/api/tags", timeout=3)
        system["ollama"] = "ok" if r.status_code == 200 else "error"
    except Exception:
        system["ollama"] = "down"

    if session.get("start_time"):
        try:
            start = datetime.fromisoformat(session["start_time"])
            system["uptime_hours"] = round(
                (datetime.now(timezone.utc) - start).total_seconds() / 3600, 1)
        except Exception:
            pass

    # Treasury / Mercury
    treasury_data = {
        "checking_balance": 0,
        "savings_balance": 0,
        "total": 0,
        "last_updated": now,
        "daily_burn": 0,
        "runway_days": 0,
        "ok": False,
    }
    try:
        from treasury import get_all_balances, get_burn_rate
        balances = get_all_balances()
        if balances.get("ok"):
            treasury_data["checking_balance"] = balances.get("checking") or 0
            treasury_data["savings_balance"] = balances.get("savings") or 0
            treasury_data["total"] = balances.get("total") or 0
            treasury_data["ok"] = True
            burn = get_burn_rate(days=7)
            treasury_data["daily_burn"] = round(burn.get("daily_burn", 0), 2)
            treasury_data["runway_days"] = int(burn.get("runway_days", 0)) if burn.get("runway_days", 0) != float("inf") else 9999
    except Exception:
        pass

    return {
        "updated_at": now,
        "bot_status": session.get("status", "unknown"),
        "accounts": {
            "kalshi": {"balance": kalshi_balance, "paper_mode": paper_kalshi},
            "alpaca": {**alpaca, "paper_mode": paper_alpaca},
        },
        "treasury": treasury_data,
        "pnl": _build_pnl(total_pnl, pnl_history),
        "trades": {
            "total": len(trades),
            "executed": len(executed),
            "skipped": len(skipped),
            "recent": [{
                "ticker": t.get("ticker", ""),
                "title": t.get("title", ""),
                "side": t.get("side", ""),
                "amount": t.get("amount", 0),
                "probability": t.get("cloud_probability", 0),
                "confidence": t.get("cloud_confidence", 0),
                "market_price": t.get("market_price", 0),
                "price_gap": t.get("price_gap", 0),
                "mode": t.get("mode", ""),
                "executed": bool(t.get("executed")),
                "reasoning": t.get("reasoning", "")[:200],
                "category": t.get("category", ""),
                "time": t.get("decided_at", "")[:19],
            } for t in trades[:50]],
        },
        "strategies": strategy_data,
        "ensemble": ensemble_data,
        "scanner": scanner,
        "sniper_markets": [{
            "ticker": m["ticker"],
            "title": m["title"][:80],
            "price": m["last_price"],
            "close_time": m["close_time"],
            "category": m["category"],
        } for m in sniper_markets],
        "news": [{
            "text": n.get("research_text", "")[:200],
            "category": n.get("category", ""),
            "source": n.get("source", ""),
            "time": n.get("created_at", "")[:19],
        } for n in news],
        "alerts": alerts,
        "system": system,
    }


def publish():
    """Generate status.json and push to GitHub."""
    os.makedirs(os.path.dirname(STATUS_PATH), exist_ok=True)

    status = generate_status()

    # Write to both locations
    with open(STATUS_PATH, "w") as f:
        json.dump(status, f, indent=2, default=str)

    # Also write to public folder for Vercel serving
    os.makedirs(os.path.dirname(PUBLIC_STATUS_PATH), exist_ok=True)
    with open(PUBLIC_STATUS_PATH, "w") as f:
        json.dump(status, f, indent=2, default=str)

    # Push to GitHub (Vercel disconnected — no rebuild trigger)
    try:
        cwd = BASE_DIR
        subprocess.run(["git", "add", "dashboard/status.json"],
                       cwd=cwd, capture_output=True, timeout=10)
        subprocess.run(["git", "commit", "-m", "status update"],
                       cwd=cwd, capture_output=True, timeout=10)
        subprocess.run(["git", "push"],
                       cwd=cwd, capture_output=True, timeout=30)
    except Exception:
        pass


if __name__ == "__main__":
    publish()
    print(f"Status published to {STATUS_PATH}")
