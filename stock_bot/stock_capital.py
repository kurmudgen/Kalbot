"""
Stock bot capital management and performance scoring.
Mirrors the Kalshi executor's dynamic capital system for stocks.

Performance score (10-100, starts 50):
  Win: +3 points (high conf) or +2 (normal)
  Loss: -5 points
  Position multiplier = score/50 (1.0x at 50, 2.0x at 100)

Capital management:
  Available = portfolio_value - floor
  Floor = peak * (1 - ACCOUNT_FLOOR_PCT)
  Daily cap = (score/100) * available
  Per-trade max = available * MAX_SINGLE_TRADE_PCT
"""

import os
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

STOCK_DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "stock_decisions.sqlite")

# Configurable via .env
ACCOUNT_FLOOR_PCT = float(os.getenv("STOCK_ACCOUNT_FLOOR_PCT", "0.30"))
MAX_SINGLE_TRADE_PCT = float(os.getenv("STOCK_MAX_SINGLE_TRADE_PCT", "0.02"))  # 2% of portfolio per trade
PERF_SCORE_START = 50
PERF_SCORE_MIN = 10
PERF_SCORE_MAX = 100
DRAWDOWN_HALT_PCT = float(os.getenv("STOCK_DRAWDOWN_HALT_PCT", "0.15"))


def _init_capital_tracker(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_capital_tracker (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            portfolio_value REAL,
            cash REAL,
            peak_value REAL,
            floor_value REAL,
            available_capital REAL,
            performance_score REAL DEFAULT 50,
            score_multiplier REAL DEFAULT 1.0,
            daily_deployed REAL DEFAULT 0,
            daily_cap REAL,
            cycle_status TEXT
        )
    """)
    conn.commit()


def get_capital_state(conn: sqlite3.Connection, portfolio_value: float, cash: float) -> dict:
    """Compute current capital state for stock trading."""
    _init_capital_tracker(conn)

    # Get previous peak and score
    row = conn.execute(
        "SELECT peak_value, performance_score, daily_deployed FROM stock_capital_tracker ORDER BY id DESC LIMIT 1"
    ).fetchone()

    if row:
        prev_peak = row[0] or portfolio_value
        performance_score = row[1] or PERF_SCORE_START
        # Reset daily deployed if new day
        daily_deployed = row[2] or 0
    else:
        prev_peak = portfolio_value
        performance_score = PERF_SCORE_START
        daily_deployed = 0

    peak_value = max(prev_peak, portfolio_value)
    floor_value = peak_value * (1 - ACCOUNT_FLOOR_PCT)
    available_capital = max(0, portfolio_value - floor_value)

    score_multiplier = performance_score / 50.0
    daily_cap = (performance_score / 100.0) * available_capital

    # Check halt
    halt_reason = None
    drawdown = (peak_value - portfolio_value) / peak_value if peak_value > 0 else 0
    if drawdown >= DRAWDOWN_HALT_PCT:
        halt_reason = f"drawdown_halt ({drawdown:.1%} >= {DRAWDOWN_HALT_PCT:.0%})"

    # Per-trade max
    max_per_trade = min(
        available_capital * MAX_SINGLE_TRADE_PCT * score_multiplier,
        500  # Hard cap $500 per stock trade
    )

    state = {
        "portfolio_value": round(portfolio_value, 2),
        "cash": round(cash, 2),
        "peak_value": round(peak_value, 2),
        "floor_value": round(floor_value, 2),
        "available_capital": round(available_capital, 2),
        "performance_score": round(performance_score, 1),
        "score_multiplier": round(score_multiplier, 2),
        "daily_deployed": round(daily_deployed, 2),
        "daily_cap": round(daily_cap, 2),
        "max_per_trade": round(max_per_trade, 2),
        "halt_reason": halt_reason,
        "drawdown_pct": round(drawdown * 100, 1),
    }

    # Log this cycle
    conn.execute(
        """INSERT INTO stock_capital_tracker
           (timestamp, portfolio_value, cash, peak_value, floor_value,
            available_capital, performance_score, score_multiplier,
            daily_deployed, daily_cap, cycle_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (datetime.now(timezone.utc).isoformat(),
         state["portfolio_value"], state["cash"], state["peak_value"],
         state["floor_value"], state["available_capital"],
         state["performance_score"], state["score_multiplier"],
         state["daily_deployed"], state["daily_cap"],
         halt_reason or "active"),
    )
    conn.commit()

    return state


def update_performance_score(conn: sqlite3.Connection, won: bool, confidence: float):
    """Update performance score after an exit."""
    _init_capital_tracker(conn)
    row = conn.execute(
        "SELECT performance_score FROM stock_capital_tracker ORDER BY id DESC LIMIT 1"
    ).fetchone()
    score = row[0] if row else PERF_SCORE_START

    if won:
        score += 3 if confidence >= 0.85 else 2
    else:
        score -= 5

    score = max(PERF_SCORE_MIN, min(PERF_SCORE_MAX, score))

    conn.execute(
        "UPDATE stock_capital_tracker SET performance_score = ? WHERE id = (SELECT MAX(id) FROM stock_capital_tracker)",
        (score,),
    )
    conn.commit()
    return score


def get_stock_pnl_summary() -> dict:
    """Get stock trading P&L summary."""
    if not os.path.exists(STOCK_DECISIONS_DB):
        return {"total_trades": 0, "exits": 0, "wins": 0, "losses": 0,
                "total_pnl": 0, "win_rate": 0, "error_rate": 0}

    conn = sqlite3.connect(STOCK_DECISIONS_DB)

    total = conn.execute("SELECT COUNT(*) FROM stock_trades").fetchone()[0]
    errors = conn.execute("SELECT COUNT(*) FROM stock_trades WHERE status LIKE 'error%'").fetchone()[0]
    exits = conn.execute("SELECT COUNT(*) FROM stock_trades WHERE exit_price IS NOT NULL").fetchone()[0]
    wins = conn.execute("SELECT COUNT(*) FROM stock_trades WHERE pnl > 0 AND exit_price IS NOT NULL").fetchone()[0]
    losses = conn.execute("SELECT COUNT(*) FROM stock_trades WHERE pnl <= 0 AND exit_price IS NOT NULL").fetchone()[0]
    total_pnl = conn.execute("SELECT COALESCE(SUM(pnl), 0) FROM stock_trades WHERE exit_price IS NOT NULL").fetchone()[0]

    # By strategy
    strategies = {}
    for row in conn.execute(
        """SELECT strategy, COUNT(*) total,
           SUM(CASE WHEN status LIKE 'error%' THEN 1 ELSE 0 END) errs,
           SUM(CASE WHEN exit_price IS NOT NULL AND pnl > 0 THEN 1 ELSE 0 END) wins,
           SUM(CASE WHEN exit_price IS NOT NULL AND pnl <= 0 THEN 1 ELSE 0 END) losses,
           COALESCE(SUM(CASE WHEN exit_price IS NOT NULL THEN pnl ELSE 0 END), 0) pnl
           FROM stock_trades GROUP BY strategy"""
    ):
        strategies[row[0]] = {
            "total": row[1], "errors": row[2], "wins": row[3],
            "losses": row[4], "pnl": round(row[5], 2),
        }

    conn.close()
    return {
        "total_trades": total,
        "errors": errors,
        "error_rate": round(errors / total * 100, 1) if total > 0 else 0,
        "exits": exits,
        "wins": wins,
        "losses": losses,
        "total_pnl": round(total_pnl, 2),
        "win_rate": round(wins / exits * 100, 1) if exits > 0 else 0,
        "strategies": strategies,
    }
