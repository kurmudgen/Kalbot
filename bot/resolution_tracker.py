"""
Resolution tracker: checks if markets we traded on have resolved,
calculates actual P&L, and updates the trade memory.

This is how we know if the system actually works.
Runs every cycle and checks all open positions against Kalshi API.
"""

import os
import sqlite3
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")
RESOLUTIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "resolutions.sqlite")


def init_resolutions_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(RESOLUTIONS_DB), exist_ok=True)
    conn = sqlite3.connect(RESOLUTIONS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS resolved_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            title TEXT,
            category TEXT,
            side TEXT,
            amount REAL,
            entry_price REAL,
            our_probability REAL,
            our_confidence REAL,
            result TEXT,
            pnl REAL,
            pnl_pct REAL,
            strategy TEXT,
            resolved_at TEXT,
            decided_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS performance_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            total_trades INTEGER,
            wins INTEGER,
            losses INTEGER,
            win_rate REAL,
            total_pnl REAL,
            best_trade REAL,
            worst_trade REAL,
            updated_at TEXT
        )
    """)
    conn.commit()
    return conn


def get_unresolved_trades() -> list[dict]:
    """Get executed trades that haven't been resolved yet."""
    if not os.path.exists(DECISIONS_DB):
        return []

    conn = sqlite3.connect(DECISIONS_DB)
    conn.row_factory = sqlite3.Row

    # Get executed trades (exclude early exit entries — those have EXIT in side)
    rows = conn.execute("""
        SELECT * FROM decisions WHERE executed = 1
        AND side NOT LIKE '%EXIT%'
        AND amount > 0
    """).fetchall()
    conn.close()

    # Check which are already resolved (only count entries with actual category data)
    resolved_tickers = set()
    if os.path.exists(RESOLUTIONS_DB):
        rconn = sqlite3.connect(RESOLUTIONS_DB)
        resolved = rconn.execute(
            "SELECT ticker FROM resolved_trades WHERE category != '' AND category IS NOT NULL"
        ).fetchall()
        resolved_tickers = {r[0] for r in resolved}
        rconn.close()

    return [dict(r) for r in rows if r["ticker"] not in resolved_tickers]


def check_market_resolution(ticker: str) -> dict | None:
    """Check if a Kalshi market has resolved."""
    try:
        url = f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}"
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return None

        market = r.json().get("market", {})
        result = market.get("result", "")
        status = market.get("status", "")

        if result in ("yes", "no") or status in ("settled", "finalized", "determined"):
            return {
                "result": result or ("yes" if market.get("settlement_value", 0) > 50 else "no"),
                "status": status,
                "settlement_value": market.get("settlement_value", 0),
            }
    except Exception:
        pass

    return None


def calculate_pnl(trade: dict, resolution: dict) -> dict:
    """Calculate P&L for a resolved trade."""
    side = trade.get("side", "YES")
    amount = trade.get("amount", 0)
    entry_price = trade.get("market_price", 0.5)
    result = resolution.get("result", "")

    # Did we win?
    if side == "YES" and result == "yes":
        won = True
    elif side == "NO" and result == "no":
        won = True
    else:
        won = False

    if won:
        # Payout is $1 per contract, we paid entry_price
        if side == "YES":
            pnl = amount * ((1.0 / entry_price) - 1) if entry_price > 0 else 0
        else:
            pnl = amount * ((1.0 / (1 - entry_price)) - 1) if entry_price < 1 else 0
    else:
        pnl = -amount

    pnl_pct = (pnl / amount * 100) if amount > 0 else 0

    return {
        "won": won,
        "pnl": round(pnl, 4),
        "pnl_pct": round(pnl_pct, 1),
    }


def resolve_trades() -> dict:
    """Check all unresolved trades and update P&L."""
    unresolved = get_unresolved_trades()
    if not unresolved:
        return {"checked": 0, "resolved": 0, "pnl": 0}

    conn = init_resolutions_db()
    stats = {"checked": len(unresolved), "resolved": 0, "wins": 0, "losses": 0, "pnl": 0}

    for trade in unresolved:
        ticker = trade["ticker"]
        resolution = check_market_resolution(ticker)

        if resolution is None:
            continue  # Not resolved yet

        result = calculate_pnl(trade, resolution)
        stats["resolved"] += 1
        stats["pnl"] += result["pnl"]

        if result["won"]:
            stats["wins"] += 1
        else:
            stats["losses"] += 1

        conn.execute(
            """INSERT INTO resolved_trades
               (ticker, title, category, side, amount, entry_price,
                our_probability, our_confidence, result, pnl, pnl_pct,
                strategy, resolved_at, decided_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, trade.get("title", ""), trade.get("category", ""),
             trade.get("side", ""), trade.get("amount", 0),
             trade.get("market_price", 0),
             trade.get("cloud_probability", 0), trade.get("cloud_confidence", 0),
             resolution["result"], result["pnl"], result["pnl_pct"],
             trade.get("session_id", ""),
             datetime.now(timezone.utc).isoformat(),
             trade.get("decided_at", "")),
        )

        status = "WIN" if result["won"] else "LOSS"
        print(f"  RESOLVED: {status} ${result['pnl']:+.2f} ({result['pnl_pct']:+.1f}%) — {trade.get('title', ticker)[:50]}")

        # Telegram alert
        try:
            from telegram_alerts import resolution_alert
            total_resolved = stats["wins"] + stats["losses"]
            wr = stats["wins"] / total_resolved if total_resolved > 0 else 0
            resolution_alert(ticker, trade.get("title", ""), result["won"],
                           result["pnl"], wr, stats["pnl"])
        except Exception:
            pass

        # Update trade memory with outcome
        try:
            import sys
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "stock_bot"))
            from trade_memory import reflect_on_trade
            reflect_on_trade(
                symbol=ticker,
                action=trade.get("side", "YES"),
                entry_price=trade.get("market_price", 0.5),
                exit_price=1.0 if result["won"] else 0.0,
                reasoning=trade.get("reasoning", ""),
                strategy=trade.get("session_id", ""),
            )
        except Exception:
            pass

        # Update adaptive model weights with actual outcome
        try:
            from adaptive_weights import record_prediction
            record_prediction(
                model="ensemble",
                category=trade.get("category", ""),
                predicted_prob=trade.get("cloud_probability", 0.5),
                actual_outcome=1 if result["won"] else 0,
            )
        except Exception:
            pass

        # Update performance score for dynamic capital management
        try:
            from executor import update_performance_score, DECISIONS_DB as EXEC_DECISIONS_DB
            dconn = sqlite3.connect(EXEC_DECISIONS_DB)
            new_score = update_performance_score(
                dconn, result["won"], trade.get("cloud_confidence", 0)
            )
            dconn.close()
            print(f"  Performance score: {new_score:.1f} ({'↑' if result['won'] else '↓'})")
        except Exception:
            pass

        # Trigger self-calibration cascade on each resolution
        try:
            from self_calibrator import on_resolution
            on_resolution({
                "ticker": ticker,
                "category": trade.get("category", ""),
                "won": result["won"],
                "pnl": result["pnl"],
                "confidence": trade.get("cloud_confidence", 0),
            })
        except Exception:
            pass

    # Update daily summary
    if stats["resolved"] > 0:
        today = datetime.now().strftime("%Y-%m-%d")
        total = stats["wins"] + stats["losses"]
        wr = stats["wins"] / total if total > 0 else 0

        conn.execute(
            """INSERT OR REPLACE INTO performance_summary
               (date, total_trades, wins, losses, win_rate, total_pnl,
                best_trade, worst_trade, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (today, total, stats["wins"], stats["losses"], wr, stats["pnl"],
             0, 0, datetime.now(timezone.utc).isoformat()),
        )

    conn.commit()
    conn.close()

    if stats["resolved"] > 0:
        wr = stats["wins"] / stats["resolved"] * 100 if stats["resolved"] > 0 else 0
        print(f"  Resolved {stats['resolved']} trades: {stats['wins']}W/{stats['losses']}L ({wr:.0f}% WR) P&L: ${stats['pnl']:+.2f}")

    return stats


def get_lifetime_stats() -> dict:
    """Get overall lifetime performance stats."""
    if not os.path.exists(RESOLUTIONS_DB):
        return {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0, "total_pnl": 0}

    conn = sqlite3.connect(RESOLUTIONS_DB)
    total = conn.execute("SELECT COUNT(*) FROM resolved_trades").fetchone()[0]
    wins = conn.execute("SELECT COUNT(*) FROM resolved_trades WHERE pnl > 0").fetchone()[0]
    losses = conn.execute("SELECT COUNT(*) FROM resolved_trades WHERE pnl <= 0").fetchone()[0]
    pnl = conn.execute("SELECT COALESCE(SUM(pnl), 0) FROM resolved_trades").fetchone()[0]
    conn.close()

    return {
        "trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": wins / total if total > 0 else 0,
        "total_pnl": pnl,
    }


if __name__ == "__main__":
    stats = resolve_trades()
    print(f"\nChecked: {stats['checked']}, Resolved: {stats['resolved']}, P&L: ${stats['pnl']:+.2f}")
    lifetime = get_lifetime_stats()
    print(f"Lifetime: {lifetime['trades']} trades, {lifetime['win_rate']:.0%} WR, ${lifetime['total_pnl']:+.2f}")
