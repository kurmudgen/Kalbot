"""
Free market intelligence feeds:
1. Fear & Greed Index — contrarian signal (buy fear, sell greed)
2. Congressional trades — copy what politicians are buying
3. Alternative.me crypto Fear & Greed
"""

import os
import requests
from datetime import datetime


def get_fear_greed() -> dict:
    """CNN Fear & Greed Index via Alternative.me (free, no key)."""
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        if r.status_code == 200:
            data = r.json()["data"][0]
            score = int(data["value"])
            label = data["value_classification"]
            return {
                "score": score,
                "label": label,
                "signal": "buy" if score < 25 else "sell" if score > 75 else "hold",
                "description": f"Market sentiment: {label} ({score}/100)",
            }
    except Exception:
        pass
    return {"score": 50, "label": "neutral", "signal": "hold", "description": ""}


def get_crypto_fear_greed() -> dict:
    """Crypto-specific Fear & Greed (same API, crypto focused)."""
    return get_fear_greed()  # Alternative.me is crypto by default


def get_congressional_trades() -> list[dict]:
    """Recent congressional stock trades from Quiver Quant (free)."""
    try:
        r = requests.get(
            "https://api.quiverquant.com/beta/live/congresstrading",
            headers={"Accept": "application/json"},
            timeout=15,
        )
        if r.status_code == 200:
            trades = r.json()
            recent = []
            for t in trades[:20]:
                recent.append({
                    "politician": t.get("Representative", ""),
                    "symbol": t.get("Ticker", ""),
                    "action": t.get("Transaction", ""),
                    "amount": t.get("Range", ""),
                    "date": t.get("TransactionDate", ""),
                    "party": t.get("Party", ""),
                })
            return recent
    except Exception:
        pass

    # Fallback: scrape from public sources
    try:
        r = requests.get(
            "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json",
            timeout=15,
        )
        if r.status_code == 200:
            all_trades = r.json()
            # Get last 20 trades
            recent = sorted(all_trades, key=lambda x: x.get("transaction_date", ""), reverse=True)[:20]
            return [{
                "politician": t.get("representative", ""),
                "symbol": t.get("ticker", ""),
                "action": t.get("type", ""),
                "amount": t.get("amount", ""),
                "date": t.get("transaction_date", ""),
                "party": t.get("party", ""),
            } for t in recent if t.get("ticker")]
    except Exception:
        pass

    return []


def get_congress_buys() -> list[str]:
    """Get symbols that congress members recently bought — these tend to outperform."""
    trades = get_congressional_trades()
    buys = set()
    for t in trades:
        action = (t.get("action", "") or "").lower()
        symbol = t.get("symbol", "")
        if symbol and ("purchase" in action or "buy" in action):
            buys.add(symbol)
    return list(buys)


def get_market_context() -> str:
    """Build a market context string for LLM prompts."""
    fg = get_fear_greed()

    lines = []
    if fg["score"] != 50:
        lines.append(f"Market Fear & Greed: {fg['score']}/100 ({fg['label']})")
        if fg["signal"] == "buy":
            lines.append("Signal: EXTREME FEAR — contrarian buy opportunity")
        elif fg["signal"] == "sell":
            lines.append("Signal: EXTREME GREED — consider taking profits")

    congress = get_congress_buys()
    if congress:
        lines.append(f"Congress recently bought: {', '.join(congress[:5])}")

    return "\n".join(lines) if lines else ""


if __name__ == "__main__":
    fg = get_fear_greed()
    print(f"Fear & Greed: {fg['score']}/100 ({fg['label']}) -> {fg['signal']}")

    trades = get_congressional_trades()
    print(f"\nCongressional trades: {len(trades)}")
    for t in trades[:5]:
        print(f"  {t['politician']}: {t['action']} {t['symbol']} ({t['amount']}) on {t['date']}")

    buys = get_congress_buys()
    print(f"\nCongress buying: {buys[:10]}")
