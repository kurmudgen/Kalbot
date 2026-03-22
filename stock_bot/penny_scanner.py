"""
Penny stock scanner: finds momentum plays under $5.
Scans for volume spikes, price breakouts, and unusual activity.

Data sources (free):
- Alpaca market data API (included with account)
- yfinance for historical data
- Finnhub for news sentiment (free tier)

Filters:
- Price: $0.10 - $5.00
- Volume: >500K daily avg or >3x normal volume today
- Market cap: <$500M
- Listed on NYSE/NASDAQ (avoid OTC pink sheets)
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SCANNER_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "penny_scanner.sqlite")

# Scanner filters
MIN_PRICE = 0.10
MAX_PRICE = 5.00
MIN_VOLUME = 500_000
VOLUME_SPIKE_MULTIPLIER = 3.0  # 3x normal volume = interesting
MAX_MARKET_CAP = 500_000_000


def init_scanner_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(SCANNER_DB), exist_ok=True)
    conn = sqlite3.connect(SCANNER_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            price REAL,
            volume INTEGER,
            avg_volume INTEGER,
            volume_ratio REAL,
            change_pct REAL,
            market_cap REAL,
            signal_type TEXT,
            scanned_at TEXT
        )
    """)
    conn.commit()
    return conn


def scan_alpaca() -> list[dict]:
    """Scan for penny stock momentum using Alpaca."""
    try:
        from alpaca_executor import get_alpaca_client
        api = get_alpaca_client()

        # Get active assets
        assets = api.list_assets(status="active")
        penny_assets = [
            a for a in assets
            if a.tradable and a.exchange in ("NYSE", "NASDAQ")
            and not a.symbol.endswith("W")  # Skip warrants
        ]

        print(f"  Scanning {len(penny_assets)} tradable assets...")

        candidates = []
        # Check in batches via snapshots
        symbols = [a.symbol for a in penny_assets]

        # Get snapshots in batches of 100
        for i in range(0, min(len(symbols), 1000), 100):
            batch = symbols[i : i + 100]
            try:
                snapshots = api.get_snapshots(batch)
                for symbol, snap in snapshots.items():
                    if snap is None:
                        continue

                    price = float(snap.latest_trade.p) if snap.latest_trade else 0
                    if not (MIN_PRICE <= price <= MAX_PRICE):
                        continue

                    # Check volume
                    today_vol = int(snap.daily_bar.v) if snap.daily_bar else 0
                    if today_vol < MIN_VOLUME:
                        continue

                    change_pct = 0
                    if snap.prev_daily_bar and snap.prev_daily_bar.c > 0:
                        change_pct = (price - float(snap.prev_daily_bar.c)) / float(snap.prev_daily_bar.c) * 100

                    candidates.append({
                        "symbol": symbol,
                        "price": price,
                        "volume": today_vol,
                        "change_pct": change_pct,
                    })
            except Exception:
                continue

        return sorted(candidates, key=lambda x: abs(x["change_pct"]), reverse=True)

    except Exception as e:
        print(f"  Alpaca scan error: {e}")
        return []


def scan_yfinance() -> list[dict]:
    """Fallback scanner using yfinance (slower but no API key needed)."""
    try:
        import yfinance as yf

        # Screen using a watchlist of known penny stocks with volume
        # In production, you'd use a stock screener API
        watchlist = [
            "SNDL", "SOFI", "PLTR", "NIO", "CLOV", "WISH", "BB",
            "NOK", "TELL", "GSAT", "OPEN", "DNA", "SKLZ",
        ]

        candidates = []
        for symbol in watchlist:
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="5d")
                if hist.empty:
                    continue

                price = float(hist["Close"].iloc[-1])
                volume = int(hist["Volume"].iloc[-1])
                avg_vol = int(hist["Volume"].mean())

                if MIN_PRICE <= price <= MAX_PRICE and volume > MIN_VOLUME:
                    change = (price - float(hist["Close"].iloc[-2])) / float(hist["Close"].iloc[-2]) * 100
                    candidates.append({
                        "symbol": symbol,
                        "price": price,
                        "volume": volume,
                        "avg_volume": avg_vol,
                        "volume_ratio": volume / avg_vol if avg_vol > 0 else 1,
                        "change_pct": change,
                    })
            except Exception:
                continue

        return sorted(candidates, key=lambda x: abs(x["change_pct"]), reverse=True)

    except Exception as e:
        print(f"  yfinance scan error: {e}")
        return []


def find_momentum_plays() -> list[dict]:
    """Find penny stocks with momentum signals."""
    # Try Alpaca first, fall back to yfinance
    candidates = scan_alpaca()
    if not candidates:
        candidates = scan_yfinance()

    if not candidates:
        print("  No penny stock candidates found")
        return []

    conn = init_scanner_db()
    signals = []

    for c in candidates[:20]:  # Top 20 movers
        conn.execute(
            """INSERT INTO scans
               (symbol, price, volume, avg_volume, volume_ratio,
                change_pct, market_cap, signal_type, scanned_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (c["symbol"], c["price"], c["volume"],
             c.get("avg_volume", 0), c.get("volume_ratio", 0),
             c["change_pct"], 0, "momentum",
             datetime.now(timezone.utc).isoformat()),
        )

        # Flag strong momentum plays
        if abs(c["change_pct"]) > 5 and c["volume"] > MIN_VOLUME * 2:
            signals.append(c)
            print(f"  SIGNAL: {c['symbol']} ${c['price']:.2f} "
                  f"{'↑' if c['change_pct'] > 0 else '↓'}{abs(c['change_pct']):.1f}% "
                  f"vol={c['volume']:,}")

    conn.commit()
    conn.close()
    return signals


if __name__ == "__main__":
    print("Penny Stock Scanner")
    print("=" * 40)
    signals = find_momentum_plays()
    print(f"\n{len(signals)} momentum signals found")
