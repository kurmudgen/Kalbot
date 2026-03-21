"""
Trade executor: reads analyst scores, applies safety checks, places trades.
Supports PAPER_TRADE mode (logs only) and LIVE mode (actual API calls).
"""

import os
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

ANALYST_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "analyst_scores.sqlite")
DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")

CONFIDENCE_MIN = 0.75
PRICE_GAP_MIN = 0.08


def get_config() -> dict:
    return {
        "paper_trade": os.getenv("PAPER_TRADE", "true").lower() == "true",
        "max_trade_size": float(os.getenv("MAX_TRADE_SIZE", "10")),
        "max_nightly_spend": float(os.getenv("MAX_NIGHTLY_SPEND", "50")),
    }


def init_decisions_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DECISIONS_DB), exist_ok=True)
    conn = sqlite3.connect(DECISIONS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            title TEXT,
            category TEXT,
            cloud_probability REAL,
            cloud_confidence REAL,
            market_price REAL,
            price_gap REAL,
            side TEXT,
            amount REAL,
            reasoning TEXT,
            mode TEXT,
            executed INTEGER,
            error TEXT,
            decided_at TEXT,
            session_id TEXT
        )
    """)
    conn.commit()
    return conn


def get_tonight_spend(conn: sqlite3.Connection, session_id: str) -> float:
    row = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM decisions WHERE session_id = ? AND executed = 1",
        (session_id,),
    ).fetchone()
    return row[0] if row else 0.0


def get_open_positions(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT ticker FROM decisions WHERE executed = 1"
    ).fetchall()
    return {r[0] for r in rows}


def get_analyst_scores() -> list[dict]:
    if not os.path.exists(ANALYST_DB):
        return []
    conn = sqlite3.connect(ANALYST_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM analyst_scores").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def execute_trades(scores: list[dict] | None = None, session_id: str = "") -> list[dict]:
    if scores is None:
        scores = get_analyst_scores()

    config = get_config()
    mode = "PAPER" if config["paper_trade"] else "LIVE"
    conn = init_decisions_db()

    if not session_id:
        session_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    open_positions = get_open_positions(conn)
    tonight_spend = get_tonight_spend(conn, session_id)

    trades = []
    print(f"Executor mode: {mode}")
    print(f"Tonight spend so far: ${tonight_spend:.2f} / ${config['max_nightly_spend']:.2f}")
    print(f"Evaluating {len(scores)} scored markets...\n")

    for score in scores:
        ticker = score["ticker"]
        title = score["title"]
        category = score["category"]
        cloud_prob = score["cloud_probability"]
        cloud_conf = score["cloud_confidence"]
        market_price = score["market_price"]
        price_gap = score["price_gap"]
        reasoning = score.get("cloud_reasoning", "")

        # Determine side
        side = "YES" if cloud_prob > market_price else "NO"
        amount = min(config["max_trade_size"], config["max_nightly_spend"] - tonight_spend)

        # Safety checks
        skip_reason = None
        if cloud_conf < CONFIDENCE_MIN:
            skip_reason = f"confidence {cloud_conf:.2f} < {CONFIDENCE_MIN}"
        elif price_gap < PRICE_GAP_MIN:
            skip_reason = f"price gap {price_gap:.2f} < {PRICE_GAP_MIN}"
        elif ticker in open_positions:
            skip_reason = "already in open positions"
        elif tonight_spend >= config["max_nightly_spend"]:
            skip_reason = f"nightly spend limit reached (${tonight_spend:.2f})"
        elif amount <= 0:
            skip_reason = "no budget remaining"

        if skip_reason:
            print(f"  SKIP {title[:50]}... — {skip_reason}")
            conn.execute(
                """INSERT INTO decisions
                   (ticker, title, category, cloud_probability, cloud_confidence,
                    market_price, price_gap, side, amount, reasoning,
                    mode, executed, error, decided_at, session_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)""",
                (ticker, title, category, cloud_prob, cloud_conf,
                 market_price, price_gap, side, 0, reasoning,
                 mode, skip_reason,
                 datetime.now(timezone.utc).isoformat(), session_id),
            )
            conn.commit()
            continue

        # Execute trade
        executed = False
        error = None

        if config["paper_trade"]:
            executed = True
            print(f"  PAPER TRADE: {side} ${amount:.2f} on {title[:50]}...")
            print(f"    conf={cloud_conf:.2f} gap={price_gap:.2f} price={market_price:.2f}")
        else:
            try:
                from pykalshi import HttpClient

                client = HttpClient(
                    api_key=os.getenv("KALSHI_API_KEY", ""),
                    private_key=os.getenv("KALSHI_PRIVATE_KEY", ""),
                )
                # Place market order
                contracts = max(1, int(amount / (market_price * 100)))
                order_side = "yes" if side == "YES" else "no"
                client.create_order(
                    ticker=ticker,
                    side=order_side,
                    action="buy",
                    type="market",
                    count=contracts,
                )
                executed = True
                print(f"  LIVE TRADE: {side} ${amount:.2f} ({contracts} contracts) on {title[:50]}...")
            except Exception as e:
                error = str(e)
                print(f"  TRADE FAILED: {title[:50]}... — {error}")

        conn.execute(
            """INSERT INTO decisions
               (ticker, title, category, cloud_probability, cloud_confidence,
                market_price, price_gap, side, amount, reasoning,
                mode, executed, error, decided_at, session_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, title, category, cloud_prob, cloud_conf,
             market_price, price_gap, side, amount, reasoning,
             mode, int(executed), error,
             datetime.now(timezone.utc).isoformat(), session_id),
        )
        conn.commit()

        if executed:
            tonight_spend += amount
            trades.append({
                "ticker": ticker,
                "title": title,
                "side": side,
                "amount": amount,
                "mode": mode,
            })

    conn.close()
    print(f"\n{len(trades)} trades {'placed' if not config['paper_trade'] else 'logged (paper)'}.")
    print(f"Total deployed tonight: ${tonight_spend:.2f}")
    return trades


if __name__ == "__main__":
    execute_trades()
