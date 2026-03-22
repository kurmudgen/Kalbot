"""
Enhanced penny stock scanner with multiple signal types.
Integrates patterns from top open-source repos:

1. Volume spike detection (from UnusualVolumeDetector)
2. Gap-up scanner (premarket gaps)
3. Short squeeze candidates (high short interest + volume)
4. Reddit/social sentiment trending
5. SEC filing alerts (insider buying via EdgarTools)

Each signal type outputs candidates that feed into the ensemble analyst.
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta

import numpy as np
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

SCANNER_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "enhanced_scanner.sqlite")


def init_scanner_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(SCANNER_DB), exist_ok=True)
    conn = sqlite3.connect(SCANNER_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, signal_type TEXT,
            price REAL, change_pct REAL, volume INTEGER,
            score REAL, details TEXT, scanned_at TEXT
        )
    """)
    conn.commit()
    return conn


# ── 1. Volume Spike Detection ──────────────────────────────────────

def scan_volume_spikes(min_price: float = 0.10, max_price: float = 5.0,
                        stddev_threshold: float = 5.0) -> list[dict]:
    """Find stocks with unusual volume (X standard deviations above mean).
    Based on UnusualVolumeDetector repo pattern."""
    candidates = []
    try:
        from finvizfinance.screener.overview import Overview

        screener = Overview()
        filters = {
            "Price": f"Under $5",
            "Average Volume": "Over 500K",
            "Relative Volume": "Over 3",
            "Change": "Up",
        }
        screener.set_filter(filters_dict=filters)
        df = screener.screener_view()

        if df is not None and len(df) > 0:
            for _, row in df.iterrows():
                try:
                    price = float(str(row.get("Price", "0")).replace(",", ""))
                    change = float(str(row.get("Change", "0%")).replace("%", ""))
                    volume = int(str(row.get("Volume", "0")).replace(",", ""))
                    rel_vol = float(str(row.get("Relative Volume", "1")).replace(",", ""))

                    if min_price <= price <= max_price and rel_vol >= stddev_threshold / 2:
                        candidates.append({
                            "symbol": row["Ticker"],
                            "price": price,
                            "change_pct": change,
                            "volume": volume,
                            "signal_type": "volume_spike",
                            "score": rel_vol,
                            "details": f"Rel Vol: {rel_vol:.1f}x",
                        })
                except (ValueError, TypeError):
                    continue

        print(f"  Volume spikes: {len(candidates)} found")
    except Exception as e:
        print(f"  Volume scan error: {e}")

    return sorted(candidates, key=lambda x: x.get("score", 0), reverse=True)[:10]


# ── 2. Gap-Up Scanner ──────────────────────────────────────────────

def scan_gap_ups(min_gap_pct: float = 5.0) -> list[dict]:
    """Find stocks gapping up in premarket."""
    candidates = []
    try:
        from finvizfinance.screener.overview import Overview

        screener = Overview()
        filters = {
            "Price": "Under $10",
            "Change": "Up 5%",
            "Average Volume": "Over 200K",
        }
        screener.set_filter(filters_dict=filters)
        df = screener.screener_view()

        if df is not None and len(df) > 0:
            for _, row in df.iterrows():
                try:
                    price = float(str(row.get("Price", "0")).replace(",", ""))
                    change = float(str(row.get("Change", "0%")).replace("%", ""))
                    volume = int(str(row.get("Volume", "0")).replace(",", ""))

                    if change >= min_gap_pct and price <= 10:
                        candidates.append({
                            "symbol": row["Ticker"],
                            "price": price,
                            "change_pct": change,
                            "volume": volume,
                            "signal_type": "gap_up",
                            "score": change,
                            "details": f"Gap: +{change:.1f}%",
                        })
                except (ValueError, TypeError):
                    continue

        print(f"  Gap-ups: {len(candidates)} found")
    except Exception as e:
        print(f"  Gap scan error: {e}")

    return sorted(candidates, key=lambda x: x.get("score", 0), reverse=True)[:10]


# ── 3. Short Squeeze Scanner ──────────────────────────────────────

def scan_short_squeezes() -> list[dict]:
    """Find high short interest stocks with volume spikes."""
    candidates = []
    try:
        from finvizfinance.screener.overview import Overview

        screener = Overview()
        filters = {
            "Price": "Under $10",
            "Short Float": "Over 15%",
            "Relative Volume": "Over 2",
            "Change": "Up",
        }
        screener.set_filter(filters_dict=filters)
        df = screener.screener_view()

        if df is not None and len(df) > 0:
            for _, row in df.iterrows():
                try:
                    price = float(str(row.get("Price", "0")).replace(",", ""))
                    change = float(str(row.get("Change", "0%")).replace("%", ""))
                    volume = int(str(row.get("Volume", "0")).replace(",", ""))

                    candidates.append({
                        "symbol": row["Ticker"],
                        "price": price,
                        "change_pct": change,
                        "volume": volume,
                        "signal_type": "short_squeeze",
                        "score": change,
                        "details": f"High short interest + volume spike",
                    })
                except (ValueError, TypeError):
                    continue

        print(f"  Short squeezes: {len(candidates)} found")
    except Exception as e:
        print(f"  Squeeze scan error: {e}")

    return sorted(candidates, key=lambda x: x.get("score", 0), reverse=True)[:5]


# ── 4. Reddit Sentiment Scanner ───────────────────────────────────

def scan_reddit_sentiment() -> list[dict]:
    """Check Reddit for trending penny stock mentions."""
    candidates = []
    try:
        import requests
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        analyzer = SentimentIntensityAnalyzer()

        # Check r/pennystocks hot posts
        headers = {"User-Agent": "KalBot/1.0"}
        for sub in ["pennystocks", "RobinHoodPennyStocks", "smallstreetbets"]:
            try:
                r = requests.get(
                    f"https://www.reddit.com/r/{sub}/hot.json?limit=25",
                    headers=headers, timeout=10,
                )
                if r.status_code != 200:
                    continue

                posts = r.json().get("data", {}).get("children", [])
                for post in posts:
                    data = post.get("data", {})
                    title = data.get("title", "")
                    score = data.get("score", 0)

                    # Extract ticker symbols ($XXX or all-caps 2-5 letter words)
                    import re
                    tickers = re.findall(r'\$([A-Z]{2,5})\b', title)
                    tickers += [w for w in title.split() if w.isupper() and 2 <= len(w) <= 5 and w.isalpha()]
                    tickers = list(set(tickers))

                    if tickers and score > 10:
                        sentiment = analyzer.polarity_scores(title)
                        for ticker in tickers[:2]:
                            candidates.append({
                                "symbol": ticker,
                                "signal_type": "reddit_trending",
                                "score": sentiment["compound"],
                                "details": f"r/{sub}: '{title[:50]}...' (upvotes: {score}, sentiment: {sentiment['compound']:+.2f})",
                                "price": 0,
                                "change_pct": 0,
                                "volume": 0,
                            })
            except Exception:
                continue

        # Deduplicate by symbol, keep highest score
        seen = {}
        for c in candidates:
            sym = c["symbol"]
            if sym not in seen or c["score"] > seen[sym]["score"]:
                seen[sym] = c
        candidates = list(seen.values())

        print(f"  Reddit trending: {len(candidates)} tickers found")
    except Exception as e:
        print(f"  Reddit scan error: {e}")

    return sorted(candidates, key=lambda x: x.get("score", 0), reverse=True)[:10]


# ── 5. SEC Insider Buying Scanner ─────────────────────────────────

def scan_insider_buying() -> list[dict]:
    """Check for recent insider purchases on penny stocks."""
    candidates = []
    try:
        from finvizfinance.screener.overview import Overview

        screener = Overview()
        filters = {
            "Price": "Under $5",
            "Insider Transactions": "Buy",
            "Average Volume": "Over 100K",
        }
        screener.set_filter(filters_dict=filters)
        df = screener.screener_view()

        if df is not None and len(df) > 0:
            for _, row in df.head(10).iterrows():
                try:
                    price = float(str(row.get("Price", "0")).replace(",", ""))
                    candidates.append({
                        "symbol": row["Ticker"],
                        "price": price,
                        "change_pct": float(str(row.get("Change", "0%")).replace("%", "")),
                        "volume": int(str(row.get("Volume", "0")).replace(",", "")),
                        "signal_type": "insider_buying",
                        "score": 0.7,
                        "details": "Recent insider purchase",
                    })
                except (ValueError, TypeError):
                    continue

        print(f"  Insider buys: {len(candidates)} found")
    except Exception as e:
        print(f"  Insider scan error: {e}")

    return candidates


# ── Main Scanner ──────────────────────────────────────────────────

def run_enhanced_scan() -> list[dict]:
    """Run all scanner modules and return combined candidates."""
    print("  Running enhanced penny stock scan...")

    all_candidates = []
    all_candidates.extend(scan_volume_spikes())
    all_candidates.extend(scan_gap_ups())
    all_candidates.extend(scan_short_squeezes())
    all_candidates.extend(scan_reddit_sentiment())
    all_candidates.extend(scan_insider_buying())

    # Deduplicate — if a stock shows up in multiple scans, boost its score
    symbol_signals = {}
    for c in all_candidates:
        sym = c["symbol"]
        if sym not in symbol_signals:
            symbol_signals[sym] = {**c, "signal_count": 1, "all_signals": [c["signal_type"]]}
        else:
            symbol_signals[sym]["signal_count"] += 1
            symbol_signals[sym]["all_signals"].append(c["signal_type"])
            symbol_signals[sym]["score"] = max(symbol_signals[sym]["score"], c["score"])
            if c["price"] > 0:
                symbol_signals[sym]["price"] = c["price"]
            if c["volume"] > 0:
                symbol_signals[sym]["volume"] = c["volume"]
            if c["change_pct"] != 0:
                symbol_signals[sym]["change_pct"] = c["change_pct"]

    # Sort by signal count (multi-signal = higher priority), then score
    results = sorted(
        symbol_signals.values(),
        key=lambda x: (x["signal_count"], x["score"]),
        reverse=True,
    )

    # Log to DB
    conn = init_scanner_db()
    for r in results:
        conn.execute(
            "INSERT INTO signals VALUES (NULL,?,?,?,?,?,?,?,?)",
            (r["symbol"], ",".join(r["all_signals"]), r["price"],
             r["change_pct"], r["volume"], r["score"],
             f"{r['signal_count']} signals: {', '.join(r['all_signals'])}",
             datetime.now(timezone.utc).isoformat()),
        )
    conn.commit()
    conn.close()

    print(f"  Total unique candidates: {len(results)}")
    for r in results[:5]:
        sigs = ", ".join(r["all_signals"])
        print(f"    {r['symbol']}: {r['signal_count']} signals ({sigs})")

    return results


if __name__ == "__main__":
    results = run_enhanced_scan()
    print(f"\n{len(results)} penny stock candidates")
