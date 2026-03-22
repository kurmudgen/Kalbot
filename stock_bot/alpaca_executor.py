"""
Alpaca stock trading executor.
Handles buy/sell execution, position tracking, and paper/live modes.

Setup:
1. Sign up at alpaca.markets
2. Get API key + secret from dashboard
3. Add to .env: ALPACA_API_KEY, ALPACA_SECRET_KEY
4. Set ALPACA_PAPER=true for paper trading

pip install alpaca-trade-api
"""

import os
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

STOCK_DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "stock_decisions.sqlite")

# Alpaca URLs
ALPACA_PAPER_URL = "https://paper-api.alpaca.markets"
ALPACA_LIVE_URL = "https://api.alpaca.markets"


def get_alpaca_client():
    """Create Alpaca API client."""
    api_key = os.getenv("ALPACA_API_KEY", "")
    secret_key = os.getenv("ALPACA_SECRET_KEY", "")
    paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"

    if not api_key or not secret_key:
        raise ValueError("ALPACA_API_KEY and ALPACA_SECRET_KEY required in .env")

    import alpaca_trade_api as tradeapi

    base_url = ALPACA_PAPER_URL if paper else ALPACA_LIVE_URL
    return tradeapi.REST(api_key, secret_key, base_url, api_version="v2")


def init_stock_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(STOCK_DECISIONS_DB), exist_ok=True)
    conn = sqlite3.connect(STOCK_DECISIONS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            qty REAL,
            price REAL,
            strategy TEXT,
            confidence REAL,
            reasoning TEXT,
            order_id TEXT,
            status TEXT,
            pnl REAL,
            traded_at TEXT,
            session_id TEXT
        )
    """)
    conn.commit()
    return conn


def get_account_info() -> dict:
    """Get current account balance and positions."""
    try:
        api = get_alpaca_client()
        account = api.get_account()
        positions = api.list_positions()

        return {
            "cash": float(account.cash),
            "portfolio_value": float(account.portfolio_value),
            "buying_power": float(account.buying_power),
            "positions": [
                {
                    "symbol": p.symbol,
                    "qty": float(p.qty),
                    "avg_entry": float(p.avg_entry_price),
                    "current_price": float(p.current_price),
                    "unrealized_pnl": float(p.unrealized_pl),
                    "pnl_pct": float(p.unrealized_plpc) * 100,
                }
                for p in positions
            ],
        }
    except Exception as e:
        print(f"Alpaca account error: {e}")
        return {}


def execute_stock_trade(
    symbol: str,
    side: str,  # "buy" or "sell"
    qty: float,
    strategy: str = "",
    confidence: float = 0.5,
    reasoning: str = "",
    session_id: str = "",
) -> dict | None:
    """Execute a stock trade on Alpaca."""
    conn = init_stock_db()

    try:
        api = get_alpaca_client()

        # Get current price
        quote = api.get_latest_quote(symbol)
        price = float(quote.ap) if quote.ap else float(quote.bp)

        # Check buying power
        account = api.get_account()
        if side == "buy" and float(account.buying_power) < price * qty:
            print(f"  Insufficient buying power for {symbol}")
            return None

        # Place order
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type="market",
            time_in_force="day",
        )

        result = {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "price": price,
            "order_id": order.id,
            "status": order.status,
        }

        conn.execute(
            """INSERT INTO stock_trades
               (symbol, side, qty, price, strategy, confidence, reasoning,
                order_id, status, traded_at, session_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (symbol, side, qty, price, strategy, confidence, reasoning,
             order.id, order.status,
             datetime.now(timezone.utc).isoformat(), session_id),
        )
        conn.commit()

        print(f"  {'PAPER ' if os.getenv('ALPACA_PAPER', 'true').lower() == 'true' else ''}TRADE: {side.upper()} {qty} {symbol} @ ${price:.4f}")
        return result

    except Exception as e:
        print(f"  Trade error: {e}")
        conn.execute(
            """INSERT INTO stock_trades
               (symbol, side, qty, price, strategy, confidence, reasoning,
                order_id, status, traded_at, session_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)""",
            (symbol, side, qty, 0, strategy, confidence, reasoning,
             f"error: {e}",
             datetime.now(timezone.utc).isoformat(), session_id),
        )
        conn.commit()
        return None
    finally:
        conn.close()


if __name__ == "__main__":
    info = get_account_info()
    if info:
        print(f"Cash: ${info['cash']:.2f}")
        print(f"Portfolio: ${info['portfolio_value']:.2f}")
        print(f"Positions: {len(info['positions'])}")
        for p in info["positions"]:
            print(f"  {p['symbol']}: {p['qty']} shares @ ${p['avg_entry']:.4f} (P&L: {p['pnl_pct']:.1f}%)")
    else:
        print("Add ALPACA_API_KEY and ALPACA_SECRET_KEY to .env")
