"""
Crypto trading via Alpaca — blue chip + micro-cap momentum.

Two modes:
1. BLUE CHIP: BTC, ETH, SOL swing trades (hold 1-7 days)
2. MICRO-CAP SNIPER: find pumping coins, get in early, get out fast

Alpaca supports: BTC, ETH, SOL, DOGE, SHIB, AVAX, DOT, LINK, UNI,
AAVE, LTC, BCH, XLM, ETC, ATOM, ALGO, and more.

Uses CoinGecko for trending detection + Perplexity for news.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

CRYPTO_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "crypto_strategy.sqlite")

# Blue chip crypto (Alpaca supports 73 crypto pairs)
# Only symbols Alpaca actually supports — LINK/USD, DOT/USD, AVAX/USD, ADA/USD, XRP/USD
# return "invalid symbol" errors and have been removed
BLUE_CHIPS = {
    "BTC/USD": "Bitcoin",
    "ETH/USD": "Ethereum",
    "SOL/USD": "Solana",
    "LTC/USD": "Litecoin",
    "DOGE/USD": "Dogecoin",
}

# Micro-cap / meme coins
# Micro-cap / meme coins — only Alpaca-supported symbols
MICRO_CAPS = {
    "SHIB/USD": "Shiba Inu",
    "UNI/USD": "Uniswap",
    "AAVE/USD": "Aave",
}

STOP_LOSS_PCT = 0.05     # 5% stop loss (tighter — cut losers fast)
TAKE_PROFIT_PCT = 0.12   # 12% take profit
SNIPER_TAKE_PROFIT = 0.08  # 8% quick flip for sniping


def init_crypto_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(CRYPTO_DB), exist_ok=True)
    conn = sqlite3.connect(CRYPTO_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crypto_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, name TEXT, strategy TEXT,
            action TEXT, confidence REAL,
            price REAL, change_24h REAL, volume_24h REAL,
            reasoning TEXT, created_at TEXT
        )
    """)
    conn.commit()
    return conn


def get_coingecko_data() -> list[dict]:
    """Get market data from CoinGecko (free, no API key)."""
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "volume_desc",
            "per_page": 50,
            "page": 1,
            "sparkline": False,
            "price_change_percentage": "1h,24h,7d",
        }
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"  CoinGecko error: {e}")
    return []


def get_trending_coins() -> list[dict]:
    """Get trending coins from CoinGecko."""
    try:
        r = requests.get("https://api.coingecko.com/api/v3/search/trending", timeout=10)
        if r.status_code == 200:
            return [c["item"] for c in r.json().get("coins", [])]
    except Exception:
        pass
    return []


def scan_blue_chips() -> list[dict]:
    """Scan blue chip cryptos for swing trade opportunities."""
    signals = []
    market_data = get_coingecko_data()

    if not market_data:
        return signals

    # Map CoinGecko IDs to our symbols
    cg_map = {
        "bitcoin": "BTC/USD", "ethereum": "ETH/USD", "solana": "SOL/USD",
        "litecoin": "LTC/USD", "dogecoin": "DOGE/USD",
    }

    for coin in market_data:
        cg_id = coin.get("id", "")
        if cg_id not in cg_map:
            continue

        symbol = cg_map[cg_id]
        price = coin.get("current_price", 0)
        change_24h = coin.get("price_change_percentage_24h", 0) or 0
        change_7d = coin.get("price_change_percentage_7d_in_currency", 0) or 0
        volume = coin.get("total_volume", 0)

        # Simple momentum signal
        score = 0
        reasons = []

        if change_24h > 3:
            score += 1
            reasons.append(f"up {change_24h:.1f}% today")
        elif change_24h < -5:
            score += 1
            reasons.append(f"dip {change_24h:.1f}% (buy the dip?)")

        if change_7d and change_7d > 10:
            score += 1
            reasons.append(f"up {change_7d:.1f}% this week")

        # RSI proxy: if it dropped hard then bounced = potential reversal
        if change_24h > 2 and (change_7d or 0) < -5:
            score += 2
            reasons.append("bounce after weekly dip")

        if score >= 2:
            signals.append({
                "symbol": symbol,
                "name": BLUE_CHIPS.get(symbol, cg_id),
                "strategy": "blue_chip",
                "price": price,
                "change_24h": change_24h,
                "volume_24h": volume,
                "score": score,
                "reasons": reasons,
            })

    return sorted(signals, key=lambda x: x["score"], reverse=True)


def scan_micro_caps() -> list[dict]:
    """Scan for micro-cap momentum — pumping coins to snipe."""
    signals = []
    market_data = get_coingecko_data()
    trending = get_trending_coins()

    if not market_data:
        return signals

    # Find coins with big moves in the last 24h
    for coin in market_data:
        price = coin.get("current_price", 0)
        change_24h = coin.get("price_change_percentage_24h", 0) or 0
        volume = coin.get("total_volume", 0) or 0
        market_cap = coin.get("market_cap", 0) or 0
        symbol_raw = coin.get("symbol", "").upper()

        # Map to Alpaca symbols
        alpaca_symbol = f"{symbol_raw}/USD"
        if alpaca_symbol not in MICRO_CAPS:
            continue

        score = 0
        reasons = []

        # Big pump
        if change_24h > 10:
            score += 2
            reasons.append(f"pumping {change_24h:.1f}%")
        elif change_24h > 5:
            score += 1
            reasons.append(f"up {change_24h:.1f}%")

        # Trending on CoinGecko
        trending_ids = {t.get("id", "") for t in trending}
        if coin.get("id") in trending_ids:
            score += 2
            reasons.append("trending on CoinGecko")

        # Volume spike (volume > 2x market cap = unusual)
        if market_cap > 0 and volume > market_cap * 0.5:
            score += 1
            reasons.append("high volume relative to mcap")

        if score >= 2:
            signals.append({
                "symbol": alpaca_symbol,
                "name": MICRO_CAPS.get(alpaca_symbol, symbol_raw),
                "strategy": "micro_cap_sniper",
                "price": price,
                "change_24h": change_24h,
                "volume_24h": volume,
                "score": score,
                "reasons": reasons,
            })

    return sorted(signals, key=lambda x: x["score"], reverse=True)


def analyze_with_llm(signal: dict) -> dict | None:
    """Run ensemble LLM analysis on a crypto signal."""
    symbol = signal["symbol"]
    name = signal["name"]
    strategy = signal["strategy"]
    reasons_text = ", ".join(signal.get("reasons", []))

    prompt = f"""You are a crypto analyst. Analyze {name} ({symbol}) for a {'swing trade (hold 1-7 days)' if strategy == 'blue_chip' else 'quick flip (hold hours to 1 day)'}.

Current price: ${signal['price']:,.2f}
24h change: {signal['change_24h']:+.1f}%
24h volume: ${signal['volume_24h']:,.0f}
Signal reasons: {reasons_text}

Search for the latest news about {name}. Is there a real catalyst (partnership, upgrade, listing, regulation) or is this just speculation/pump-and-dump?

For micro-caps: is this a pump that still has room to run, or is it exhausted?
For blue chips: is this a good entry point for a swing trade?

Respond with JSON:
{{"action": "buy" or "avoid", "confidence": <0.0-1.0>, "target_pct": <expected % gain>, "reasoning": "<2 sentences citing specific news>"}}
"""

    estimates = []

    # Perplexity
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if api_key:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai", timeout=60.0)
            r = client.chat.completions.create(
                model="sonar-pro",
                messages=[
                    {"role": "system", "content": "You are a crypto analyst. Search for latest news. Respond with JSON only."},
                    {"role": "user", "content": prompt},
                ], temperature=0.2,
            )
            raw = r.choices[0].message.content
            start, end = raw.find("{"), raw.rfind("}") + 1
            if start >= 0 and end > 0:
                result = json.loads(raw[start:end])
                result["_provider"] = "perplexity"
                estimates.append(result)
        except Exception as e:
            print(f"    Perplexity: {e}")

    # Claude
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if api_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key, timeout=60.0)
            r = client.messages.create(
                model="claude-sonnet-4-20250514", max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = r.content[0].text
            start, end = raw.find("{"), raw.rfind("}") + 1
            if start >= 0 and end > 0:
                result = json.loads(raw[start:end])
                result["_provider"] = "claude"
                estimates.append(result)
        except Exception as e:
            print(f"    Claude: {e}")

    if len(estimates) < 1:
        return None

    # Check agreement (even 1 model is ok for crypto — it's fast-moving)
    actions = [e.get("action", "avoid") for e in estimates]
    buy_votes = sum(1 for a in actions if a == "buy")

    if buy_votes == 0:
        return None

    avg_conf = sum(e.get("confidence", 0.5) for e in estimates) / len(estimates)

    return {
        "symbol": symbol,
        "name": name,
        "action": "buy",
        "confidence": avg_conf,
        "strategy": strategy,
        "reasoning": estimates[0].get("reasoning", ""),
        "target_pct": estimates[0].get("target_pct", 10),
        "price": signal["price"],
    }


def run_crypto_scan(session_id: str = "") -> list[dict]:
    """Run both crypto strategies and return trade signals."""
    from alpaca_executor import execute_stock_trade, get_account_info

    print("  --- Crypto Blue Chip ---")
    blue_signals = scan_blue_chips()
    print(f"  {len(blue_signals)} blue chip signals")

    print("  --- Crypto Micro-Cap Sniper ---")
    micro_signals = scan_micro_caps()
    print(f"  {len(micro_signals)} micro-cap signals")

    conn = init_crypto_db()
    trades = []

    all_signals = blue_signals[:3] + micro_signals[:3]  # Top 3 each

    # Get currently held crypto to avoid duplicate buys
    try:
        info = get_account_info()
        held = {p["symbol"] for p in info.get("positions", [])}
    except Exception:
        held = set()

    for sig in all_signals:
        # Skip if already holding this crypto
        alpaca_sym = sig["symbol"].replace("/", "")
        if alpaca_sym in held or sig["symbol"] in held:
            continue

        print(f"  Analyzing {sig['name']}...", end=" ")
        analysis = analyze_with_llm(sig)

        if analysis is None:
            print("skip")
            continue

        if analysis["action"] != "buy" or analysis["confidence"] < 0.6:
            print(f"avoid (conf={analysis['confidence']:.2f})")
            continue

        print(f"BUY conf={analysis['confidence']:.2f}")

        # Position size
        max_position = float(os.getenv("STOCK_MAX_POSITION", "10"))
        amount = max_position * analysis["confidence"]
        qty = max(0.001, amount / sig["price"])  # Crypto allows fractional

        conn.execute(
            "INSERT INTO crypto_signals VALUES (NULL,?,?,?,?,?,?,?,?,?,?)",
            (sig["symbol"], sig["name"], sig["strategy"],
             analysis["action"], analysis["confidence"],
             sig["price"], sig["change_24h"], sig["volume_24h"],
             analysis["reasoning"],
             datetime.now(timezone.utc).isoformat()),
        )

        # Execute via Alpaca
        result = execute_stock_trade(
            symbol=sig["symbol"],  # Alpaca uses BTC/USD with the slash
            side="buy",
            qty=round(qty, 6),
            strategy=f"crypto_{sig['strategy']}",
            confidence=analysis["confidence"],
            reasoning=analysis["reasoning"],
            session_id=session_id,
        )

        if result:
            trades.append(result)

    conn.commit()
    conn.close()
    return trades


if __name__ == "__main__":
    print("Crypto Scanner")
    print("=" * 40)
    trades = run_crypto_scan()
    print(f"\n{len(trades)} crypto trades placed")
