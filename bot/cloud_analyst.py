"""
Cloud analyst: sends filtered markets to Perplexity Sonar Pro (primary)
with DeepSeek and Gemini as fallbacks.
Perplexity's built-in web search gives us real-time data for each market.
Writes to data/live/analyst_scores.sqlite.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SCORES_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "filter_scores.sqlite")
ANALYST_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "analyst_scores.sqlite")

ANALYSIS_PROMPT = """You are an expert prediction market analyst. Analyze this Kalshi market and provide your probability estimate.

Market: {title}
Category: {category}
Current market price (implied probability): {market_price:.2f}
Local model estimate: {local_prob:.2f} (confidence: {local_conf:.2f})
Local model reasoning: {reasoning}

INSTRUCTIONS:
1. Search for the latest relevant data — current forecasts, recent economic releases, official reports.
2. Compare what you find to what the market is pricing.
3. Consider base rates, seasonal patterns, and consensus expectations.
4. Give your honest probability estimate. If the market looks efficient, say so.
5. Only flag a mispricing if you have specific evidence the market is wrong.

Respond ONLY with valid JSON:
{{"probability": <float 0.0-1.0>, "confidence": <float 0.0-1.0>, "reasoning": "<2-3 sentences citing specific data>"}}
"""


def init_analyst_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(ANALYST_DB), exist_ok=True)
    conn = sqlite3.connect(ANALYST_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analyst_scores (
            ticker TEXT PRIMARY KEY,
            title TEXT,
            category TEXT,
            local_probability REAL,
            local_confidence REAL,
            cloud_probability REAL,
            cloud_confidence REAL,
            cloud_reasoning TEXT,
            market_price REAL,
            price_gap REAL,
            cloud_provider TEXT,
            analyzed_at TEXT
        )
    """)
    conn.commit()
    return conn


def get_filtered_markets() -> list[dict]:
    if not os.path.exists(SCORES_DB):
        return []
    conn = sqlite3.connect(SCORES_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM filter_scores WHERE passed_filter = 1"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def call_perplexity(prompt: str) -> dict | None:
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if not api_key:
        return None

    try:
        from openai import OpenAI

        client = OpenAI(
            api_key=api_key,
            base_url="https://api.perplexity.ai",
        )
        response = client.chat.completions.create(
            model="sonar-pro",
            messages=[
                {
                    "role": "system",
                    "content": "You are a calibrated prediction market analyst. "
                    "Search for the latest data relevant to the market question. "
                    "Respond only with valid JSON.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )
        raw = response.choices[0].message.content
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start == -1 or end == 0:
            return None
        result = json.loads(raw[start:end])
        result["_provider"] = "perplexity"
        return result
    except Exception as e:
        print(f"  Perplexity error: {e}")
        return None


def call_deepseek(prompt: str) -> dict | None:
    api_key = os.getenv("DEEPSEEK_API_KEY", "")
    if not api_key:
        return None

    try:
        from openai import OpenAI

        client = OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com",
        )
        response = client.chat.completions.create(
            model="deepseek-reasoner",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        raw = response.choices[0].message.content
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start == -1 or end == 0:
            return None
        result = json.loads(raw[start:end])
        result["_provider"] = "deepseek"
        return result
    except Exception as e:
        print(f"  DeepSeek error: {e}")
        return None


def call_gemini(prompt: str) -> dict | None:
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        return None

    try:
        import google.generativeai as genai

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.0-flash")
        response = model.generate_content(prompt)
        raw = response.text
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start == -1 or end == 0:
            return None
        result = json.loads(raw[start:end])
        result["_provider"] = "gemini"
        return result
    except Exception as e:
        print(f"  Gemini error: {e}")
        return None


def analyze_markets(markets: list[dict] | None = None) -> list[dict]:
    if markets is None:
        markets = get_filtered_markets()

    if not markets:
        print("No markets to analyze.")
        return []

    conn = init_analyst_db()
    analyzed = []

    print(f"Sending {len(markets)} markets to cloud analyst...")

    for i, m in enumerate(markets):
        ticker = m["ticker"]
        title = m["title"]
        category = m["category"]
        market_price = m["market_price"]
        local_prob = m["model_probability"]
        local_conf = m["confidence"]
        reasoning = m.get("reasoning", "")

        prompt = ANALYSIS_PROMPT.format(
            title=title,
            category=category,
            market_price=market_price,
            local_prob=local_prob,
            local_conf=local_conf,
            reasoning=reasoning,
        )

        print(f"[{i+1}/{len(markets)}] {title[:60]}...", end=" ", flush=True)

        # Perplexity first (has web search), then DeepSeek, then Gemini
        result = call_perplexity(prompt)
        if result is None:
            result = call_deepseek(prompt)
        if result is None:
            result = call_gemini(prompt)
        if result is None:
            print("SKIP (all providers failed)")
            continue

        cloud_prob = float(result.get("probability", 0.5))
        cloud_conf = float(result.get("confidence", 0.5))
        cloud_reasoning = str(result.get("reasoning", ""))
        provider = result.get("_provider", "unknown")
        price_gap = abs(cloud_prob - market_price)

        conn.execute(
            """INSERT OR REPLACE INTO analyst_scores
               (ticker, title, category, local_probability, local_confidence,
                cloud_probability, cloud_confidence, cloud_reasoning,
                market_price, price_gap, cloud_provider, analyzed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, title, category, local_prob, local_conf,
             cloud_prob, cloud_conf, cloud_reasoning,
             market_price, price_gap, provider,
             datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()

        print(f"prob={cloud_prob:.2f} conf={cloud_conf:.2f} via {provider}")

        analyzed.append({
            "ticker": ticker,
            "title": title,
            "category": category,
            "cloud_probability": cloud_prob,
            "cloud_confidence": cloud_conf,
            "market_price": market_price,
            "price_gap": price_gap,
            "cloud_reasoning": cloud_reasoning,
        })

    conn.close()
    print(f"\n{len(analyzed)} markets analyzed by cloud model")
    return analyzed


if __name__ == "__main__":
    analyze_markets()
