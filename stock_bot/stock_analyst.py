"""
Stock analyst: uses the same ensemble LLM approach as our prediction market bot.
Perplexity researches → Claude + DeepSeek evaluate → consensus required.

For penny stocks, the analysis focuses on:
- Is the volume spike driven by real news or just noise?
- Is this a pump-and-dump pattern?
- What's the short interest / float?
- Any insider activity or SEC filings?
"""

import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)


RESEARCH_PROMPT = """You are a stock analyst evaluating a penny stock momentum play.

Stock: {symbol}
Current price: ${price:.4f}
Today's change: {change_pct:+.1f}%
Today's volume: {volume:,}
{technicals_summary}
{memory_context}

Research this stock NOW and determine:
1. WHY is it moving? (news, earnings, FDA, contract, or just noise?)
2. Is this a pump-and-dump pattern? (check social media mentions, recent promotions)
3. What is the float / shares outstanding?
4. Any recent SEC filings or insider transactions?
5. What is the company's actual business and revenue?

Based on your research, should a trader:
- BUY (momentum will continue)
- AVOID (pump-and-dump, no catalyst, or exhausted move)

Respond with JSON:
{{"action": "buy" or "avoid", "confidence": <0.0-1.0>, "target_price": <float>, "stop_loss": <float>, "reasoning": "<2-3 sentences citing specific findings>"}}
"""


def analyze_stock(stock: dict) -> dict | None:
    """Run ensemble analysis on a penny stock candidate."""
    symbol = stock["symbol"]
    price = stock["price"]
    change_pct = stock["change_pct"]
    volume = stock["volume"]

    # Technical analysis
    technicals_summary = ""
    try:
        import yfinance as yf
        from technicals import analyze_technicals

        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1y")
        if len(hist) >= 50:
            prices = hist["Close"].tolist()
            volumes = hist["Volume"].tolist()
            ta = analyze_technicals(prices, volumes)
            technicals_summary = (
                f"\nTechnical Analysis: signal={ta['signal']:+.2f} ({ta['direction']}), "
                f"confidence={ta['confidence']:.2f}, RSI={ta['rsi']:.0f}, "
                f"Hurst={ta['hurst']:.2f}"
            )
            # If technicals say strong sell, add a warning
            if ta["signal"] < -0.4:
                technicals_summary += " ⚠️ TECHNICAL WARNING: Strong sell signal"
    except Exception:
        pass

    # Trade memory — recall similar past situations
    memory_context = ""
    try:
        from trade_memory import recall_similar, format_memories_for_prompt

        situation = f"{symbol} {change_pct:+.1f}% on {volume:,} volume"
        memories = recall_similar(situation, top_k=3)
        memory_context = format_memories_for_prompt(memories)
    except Exception:
        pass

    prompt = RESEARCH_PROMPT.format(
        symbol=symbol, price=price, change_pct=change_pct, volume=volume,
        technicals_summary=technicals_summary,
        memory_context=memory_context,
    )

    estimates = []

    # Perplexity (with web search)
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if api_key:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai")
            r = client.chat.completions.create(
                model="sonar-pro",
                messages=[
                    {"role": "system", "content": "You are a penny stock analyst. Search for real-time news about this stock. Respond with JSON only."},
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
        except Exception as e:
            print(f"    Perplexity error: {e}")

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
        except Exception as e:
            print(f"    Claude error: {e}")

    if len(estimates) < 2:
        return None

    # Check consensus
    actions = [e.get("action", "avoid") for e in estimates]
    if not all(a == actions[0] for a in actions):
        print(f"    {symbol}: NO CONSENSUS (disagreement)")
        return None

    # Average confidence
    avg_conf = sum(e.get("confidence", 0.5) for e in estimates) / len(estimates)

    return {
        "symbol": symbol,
        "action": actions[0],
        "confidence": avg_conf,
        "price": price,
        "target_price": estimates[0].get("target_price", price * 1.1),
        "stop_loss": estimates[0].get("stop_loss", price * 0.9),
        "reasoning": estimates[0].get("reasoning", ""),
        "providers": [e.get("_provider") for e in estimates],
    }


if __name__ == "__main__":
    # Test with a sample
    test = {"symbol": "SNDL", "price": 1.50, "change_pct": 8.5, "volume": 5000000}
    result = analyze_stock(test)
    if result:
        print(f"{result['symbol']}: {result['action'].upper()} (conf: {result['confidence']:.2f})")
        print(f"  Target: ${result['target_price']:.2f}, Stop: ${result['stop_loss']:.2f}")
        print(f"  {result['reasoning']}")
