"""
Blue chip / ETF strategy using ai-hedge-fund multi-agent pattern.
Trades liquid stocks and ETFs with fundamental + technical analysis.

Avoids Pattern Day Trader (PDT) rule issues:
- PDT triggers at 4+ day trades in 5 business days on accounts <$25K
- Alpaca paper accounts start at $100K so PDT doesn't apply
- For real money accounts <$25K: we hold positions overnight (swing trades)
- SWING_MODE=true: only buys, no same-day sells, holds 2-5 days

Watchlist: high-liquidity stocks and sector ETFs where LLM analysis
has the most data to work with.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

BLUECHIP_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "bluechip_strategy.sqlite")

# Account size threshold for PDT protection
PDT_THRESHOLD = 25000
SWING_MODE = os.getenv("STOCK_SWING_MODE", "true").lower() == "true"

# Core watchlist — high liquidity, well-covered by analysts, LLMs have deep knowledge
WATCHLIST = {
    # Mega caps (most data, most LLM knowledge)
    "tech": ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"],
    # Sector ETFs (diversified, liquid, less volatile)
    "etfs": ["SPY", "QQQ", "IWM", "XLF", "XLE", "XLK", "XLV", "ARKK", "GLD", "TLT"],
    # High-vol momentum names (bigger moves, more opportunity)
    "momentum": ["AMD", "PLTR", "SOFI", "COIN", "MARA", "RIOT", "SQ", "SNAP", "UBER"],
    # Dividend / defensive (steady, lower risk)
    "defensive": ["JNJ", "KO", "PG", "VZ", "T", "MO"],
}

# Position sizing by category
CATEGORY_MAX_POSITION_PCT = {
    "tech": 0.08,       # 8% of portfolio per tech stock
    "etfs": 0.12,       # 12% per ETF (safer)
    "momentum": 0.05,   # 5% per momentum name (volatile)
    "defensive": 0.10,  # 10% per defensive
}

# From ai-hedge-fund risk manager
VOLATILITY_LIMITS = {
    # annualized_vol: max_position_pct
    "low": (0, 0.15, 0.12),      # <15% vol → up to 12%
    "medium": (0.15, 0.30, 0.08), # 15-30% vol → up to 8%
    "high": (0.30, 0.50, 0.05),   # 30-50% vol → up to 5%
    "extreme": (0.50, 1.0, 0.03), # >50% vol → max 3%
}


def init_bluechip_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(BLUECHIP_DB), exist_ok=True)
    conn = sqlite3.connect(BLUECHIP_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, category TEXT,
            action TEXT, confidence REAL,
            price REAL, target REAL, stop_loss REAL,
            technical_signal REAL, fundamental_score REAL,
            reasoning TEXT, created_at TEXT
        )
    """)
    conn.commit()
    return conn


def get_stock_data(symbol: str) -> dict | None:
    """Pull price history + fundamentals via yfinance."""
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1y")
        if hist.empty or len(hist) < 50:
            return None

        info = ticker.info or {}
        prices = hist["Close"].tolist()
        volumes = hist["Volume"].tolist()

        return {
            "symbol": symbol,
            "prices": prices,
            "volumes": volumes,
            "current_price": prices[-1],
            "pe_ratio": info.get("trailingPE"),
            "forward_pe": info.get("forwardPE"),
            "market_cap": info.get("marketCap"),
            "revenue_growth": info.get("revenueGrowth"),
            "profit_margin": info.get("profitMargins"),
            "debt_to_equity": info.get("debtToEquity"),
            "roe": info.get("returnOnEquity"),
            "dividend_yield": info.get("dividendYield"),
            "52w_high": info.get("fiftyTwoWeekHigh"),
            "52w_low": info.get("fiftyTwoWeekLow"),
            "avg_volume": info.get("averageVolume"),
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
        }
    except Exception as e:
        return None


def check_fundamentals(data: dict) -> dict:
    """Score fundamentals using ai-hedge-fund thresholds."""
    score = 0
    reasons = []

    # Profitability
    roe = data.get("roe")
    if roe and roe > 0.15:
        score += 1
        reasons.append(f"ROE {roe:.0%}")
    margin = data.get("profit_margin")
    if margin and margin > 0.15:
        score += 1
        reasons.append(f"Margin {margin:.0%}")

    # Growth
    growth = data.get("revenue_growth")
    if growth and growth > 0.10:
        score += 1
        reasons.append(f"Rev growth {growth:.0%}")

    # Valuation (penalty for overvalued)
    pe = data.get("pe_ratio")
    if pe and pe > 40:
        score -= 1
        reasons.append(f"High P/E {pe:.0f}")
    elif pe and pe < 15 and pe > 0:
        score += 1
        reasons.append(f"Low P/E {pe:.0f}")

    # Financial health
    de = data.get("debt_to_equity")
    if de and de < 50:
        score += 1
        reasons.append("Low debt")
    elif de and de > 200:
        score -= 1
        reasons.append("High debt")

    return {"score": score, "max_score": 5, "reasons": reasons}


def analyze_with_ensemble(symbol: str, data: dict, tech_result: dict,
                           fund_result: dict) -> dict | None:
    """Run ensemble LLM analysis on a stock candidate."""
    price = data["current_price"]

    prompt = f"""You are analyzing {symbol} for a swing trade (hold 2-5 days).

Current price: ${price:.2f}
Sector: {data.get('sector', 'Unknown')}
P/E: {data.get('pe_ratio', 'N/A')}
Revenue growth: {data.get('revenue_growth', 'N/A')}
Profit margin: {data.get('profit_margin', 'N/A')}

Technical signal: {tech_result['signal']:+.2f} ({tech_result['direction']})
RSI: {tech_result['rsi']:.0f}
Fundamental score: {fund_result['score']}/{fund_result['max_score']} ({', '.join(fund_result['reasons'][:3])})

Search for any breaking news, earnings expectations, or analyst upgrades/downgrades for {symbol}.

Should a swing trader BUY, SELL, or HOLD this stock for the next 2-5 days?

Respond with JSON:
{{"action": "buy" or "sell" or "hold", "confidence": <0.0-1.0>, "target_price": <float>, "stop_loss": <float>, "reasoning": "<2-3 sentences>"}}
"""

    estimates = []

    # Perplexity (with web search for latest news)
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if api_key:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai")
            r = client.chat.completions.create(
                model="sonar-pro",
                messages=[
                    {"role": "system", "content": "You are a stock analyst. Search for latest news. Respond with JSON only."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )
            raw = r.choices[0].message.content
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > 0:
                result = json.loads(raw[start:end])
                result["_provider"] = "perplexity"
                estimates.append(result)
        except Exception:
            pass

    # Claude
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if api_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            r = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = r.content[0].text
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > 0:
                result = json.loads(raw[start:end])
                result["_provider"] = "claude"
                estimates.append(result)
        except Exception:
            pass

    # DeepSeek (third opinion for stocks — require unanimous)
    api_key = os.getenv("DEEPSEEK_API_KEY", "")
    if api_key:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com", timeout=60.0)
            r = client.chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            raw = r.choices[0].message.content
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > 0:
                result = json.loads(raw[start:end])
                result["_provider"] = "deepseek"
                estimates.append(result)
        except Exception:
            pass

    # Require ALL models to agree for stocks (higher bar than Kalshi)
    if len(estimates) < 2:
        return None

    actions = [e.get("action", "hold") for e in estimates]
    if not all(a == actions[0] for a in actions):
        return None  # Any disagreement = skip

    if actions[0] == "hold":
        return None  # No trade

    avg_conf = sum(e.get("confidence", 0.5) for e in estimates) / len(estimates)

    return {
        "symbol": symbol,
        "action": actions[0],
        "confidence": avg_conf,
        "target_price": estimates[0].get("target_price", price * 1.05),
        "stop_loss": estimates[0].get("stop_loss", price * 0.95),
        "reasoning": estimates[0].get("reasoning", ""),
    }


def check_pdt_safe(account_value: float, recent_day_trades: int) -> bool:
    """Check if we can day trade without PDT violation."""
    if account_value >= PDT_THRESHOLD:
        return True  # Above $25K, no PDT restriction
    if recent_day_trades >= 3:
        return False  # Would trigger PDT on next day trade
    return not SWING_MODE  # In swing mode, we never day trade


def scan_and_analyze() -> list[dict]:
    """Scan watchlist, run technicals + fundamentals, ensemble analyze top picks."""
    from technicals import analyze_technicals

    conn = init_bluechip_db()
    signals = []

    all_symbols = []
    symbol_categories = {}
    for category, symbols in WATCHLIST.items():
        for s in symbols:
            all_symbols.append(s)
            symbol_categories[s] = category

    # Add congressional buys to watchlist (politicians beat the market)
    try:
        from market_intelligence import get_congress_buys
        congress = get_congress_buys()
        for s in congress[:5]:
            if s not in symbol_categories:
                all_symbols.append(s)
                symbol_categories[s] = "congress"
    except Exception:
        pass

    print(f"  Blue chip scan: {len(all_symbols)} symbols")

    # Score all stocks technically
    scored = []
    for symbol in all_symbols:
        data = get_stock_data(symbol)
        if data is None:
            continue

        tech = analyze_technicals(data["prices"], data["volumes"])
        fund = check_fundamentals(data)

        # Momentum confirmation: skip if price is trending down recently
        prices = data["prices"]
        if len(prices) >= 5:
            recent_trend = (prices[-1] - prices[-5]) / prices[-5]
            if recent_trend < -0.02:  # Down more than 2% in last 5 days = skip
                continue

        # Combined score: technicals (60%) + fundamentals (40%)
        combined = tech["signal"] * 0.6 + (fund["score"] / max(fund["max_score"], 1)) * 0.4

        scored.append({
            "symbol": symbol,
            "category": symbol_categories[symbol],
            "data": data,
            "tech": tech,
            "fund": fund,
            "combined_score": combined,
        })

    # Sort by absolute combined score (strongest signals first)
    scored.sort(key=lambda x: abs(x["combined_score"]), reverse=True)

    # Analyze top 5 with ensemble LLMs
    for item in scored[:5]:
        symbol = item["symbol"]
        print(f"  Analyzing {symbol} (score: {item['combined_score']:+.2f})...")

        analysis = analyze_with_ensemble(symbol, item["data"], item["tech"], item["fund"])
        if analysis is None:
            print(f"    No consensus or hold")
            continue

        if analysis["confidence"] < 0.6:
            print(f"    Low confidence ({analysis['confidence']:.2f})")
            continue

        print(f"    {analysis['action'].upper()} conf={analysis['confidence']:.2f}")

        conn.execute(
            "INSERT INTO signals VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?)",
            (symbol, item["category"], analysis["action"], analysis["confidence"],
             item["data"]["current_price"], analysis["target_price"], analysis["stop_loss"],
             item["tech"]["signal"], item["fund"]["score"],
             analysis["reasoning"], datetime.now(timezone.utc).isoformat()),
        )

        signals.append({
            "symbol": symbol,
            "category": item["category"],
            "action": analysis["action"],
            "confidence": analysis["confidence"],
            "price": item["data"]["current_price"],
            "target_price": analysis["target_price"],
            "stop_loss": analysis["stop_loss"],
            "reasoning": analysis["reasoning"],
        })

    conn.commit()
    conn.close()
    return signals


if __name__ == "__main__":
    signals = scan_and_analyze()
    print(f"\n{len(signals)} blue chip signals")
    for s in signals:
        print(f"  {s['action'].upper()} {s['symbol']} @ ${s['price']:.2f} → ${s['target_price']:.2f} (conf: {s['confidence']:.2f})")
