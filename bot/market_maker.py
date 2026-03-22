"""
Market maker: places limit orders on both sides of thin markets to earn the spread.
Works best on weather markets with low liquidity where we can be the market maker.

Strategy:
- Find markets with wide bid-ask spreads (>$0.06)
- Place buy limit at bid+1 and sell limit at ask-1
- Earn the spread regardless of outcome
- Cancel stale orders after 10 minutes
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

MARKETS_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")
MM_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "market_making.sqlite")

MIN_SPREAD_CENTS = 6   # Only market-make if spread is >= 6 cents
MAX_EXPOSURE = 20       # Max contracts outstanding per market
ORDER_TTL_MINUTES = 10  # Cancel orders after this long


def init_mm_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(MM_DB), exist_ok=True)
    conn = sqlite3.connect(MM_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mm_orders (
            order_id TEXT PRIMARY KEY,
            ticker TEXT,
            side TEXT,
            price INTEGER,
            count INTEGER,
            status TEXT,
            placed_at TEXT,
            filled_at TEXT,
            cancelled_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mm_pnl (
            ticker TEXT,
            buy_price INTEGER,
            sell_price INTEGER,
            spread_earned INTEGER,
            contracts INTEGER,
            realized_at TEXT
        )
    """)
    conn.commit()
    return conn


def find_wide_spread_markets() -> list[dict]:
    """Find markets with spreads wide enough to market-make."""
    if not os.path.exists(MARKETS_DB):
        return []

    conn = sqlite3.connect(MARKETS_DB)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT * FROM markets
        WHERE status IN ('open', 'active')
        AND yes_bid IS NOT NULL AND yes_ask IS NOT NULL
        AND yes_ask > yes_bid
        AND (yes_ask - yes_bid) >= ?
        AND category = 'weather'
        ORDER BY (yes_ask - yes_bid) DESC
    """, (MIN_SPREAD_CENTS,)).fetchall()
    conn.close()

    return [dict(r) for r in rows]


def place_mm_orders(market: dict, session_id: str = "") -> dict | None:
    """Place buy and sell limit orders to capture the spread."""
    paper_trade = os.getenv("PAPER_TRADE", "true").lower() == "true"

    ticker = market["ticker"]
    yes_bid = market["yes_bid"]
    yes_ask = market["yes_ask"]
    spread = yes_ask - yes_bid

    # Place buy at bid+1, sell at ask-1
    buy_price = yes_bid + 1
    sell_price = yes_ask - 1

    # Make sure we still have a positive spread after our orders
    if sell_price <= buy_price:
        return None

    our_spread = sell_price - buy_price
    contracts = min(5, MAX_EXPOSURE)  # Start small

    result = {
        "ticker": ticker,
        "buy_price": buy_price,
        "sell_price": sell_price,
        "spread": our_spread,
        "contracts": contracts,
        "market_spread": spread,
    }

    conn = init_mm_db()
    now = datetime.now(timezone.utc).isoformat()

    if paper_trade:
        print(f"  PAPER MM: {ticker} buy@{buy_price}¢ sell@{sell_price}¢ spread={our_spread}¢ x{contracts}")

        # Log paper orders
        for side, price in [("BUY_YES", buy_price), ("SELL_YES", sell_price)]:
            conn.execute(
                """INSERT INTO mm_orders
                   (order_id, ticker, side, price, count, status, placed_at)
                   VALUES (?, ?, ?, ?, ?, 'paper', ?)""",
                (f"paper_{ticker}_{side}_{now}", ticker, side, price, contracts, now),
            )
    else:
        try:
            from pykalshi import KalshiClient, Side as KalshiSide, OrderType

            pk_path = os.getenv("KALSHI_PRIVATE_KEY", "")
            if not os.path.isabs(pk_path):
                pk_path = os.path.join(os.path.dirname(__file__), "..", pk_path)

            client = KalshiClient(
                api_key_id=os.getenv("KALSHI_API_KEY", ""),
                private_key_path=pk_path,
            )

            # Place buy limit order (YES side)
            buy_order = client.portfolio.create_order(
                ticker=ticker,
                side=KalshiSide.YES,
                type=OrderType.LIMIT,
                count=contracts,
                yes_price=buy_price,
            )

            # Place sell limit order (NO side at 100-sell_price)
            sell_order = client.portfolio.create_order(
                ticker=ticker,
                side=KalshiSide.NO,
                type=OrderType.LIMIT,
                count=contracts,
                no_price=100 - sell_price,
            )

            client.close()

            print(f"  LIVE MM: {ticker} buy@{buy_price}¢ sell@{sell_price}¢ spread={our_spread}¢ x{contracts}")

            for oid, side, price in [
                (getattr(buy_order, 'order_id', 'unknown'), "BUY_YES", buy_price),
                (getattr(sell_order, 'order_id', 'unknown'), "SELL_YES", sell_price),
            ]:
                conn.execute(
                    """INSERT INTO mm_orders
                       (order_id, ticker, side, price, count, status, placed_at)
                       VALUES (?, ?, ?, ?, ?, 'open', ?)""",
                    (oid, ticker, side, price, contracts, now),
                )

        except Exception as e:
            print(f"  MM FAILED: {ticker} — {e}")
            conn.close()
            return None

    conn.commit()
    conn.close()
    return result


def run_market_maker(session_id: str = "") -> list[dict]:
    """Find wide-spread markets and place market-making orders."""
    markets = find_wide_spread_markets()

    if not markets:
        print("  No wide-spread markets found for market making")
        return []

    print(f"  Found {len(markets)} wide-spread weather markets")

    results = []
    for m in markets[:5]:  # Limit to 5 markets at a time
        result = place_mm_orders(m, session_id)
        if result:
            results.append(result)

    return results


if __name__ == "__main__":
    results = run_market_maker()
    if results:
        print(f"\n{len(results)} market-making positions placed")
        total_spread = sum(r["spread"] * r["contracts"] for r in results)
        print(f"Total spread exposure: {total_spread}¢")
    else:
        print("No market-making opportunities")
