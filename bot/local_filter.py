"""
Local filter: scores markets using dual Ollama models sequentially.
Uses qwen2.5:7b then llama3.1:8b — both must agree to pass.
Peak VRAM ~5GB (Ollama auto-swaps models).

Reads from data/live/markets.sqlite, writes scores to data/live/filter_scores.sqlite.
Only passes markets with relevance=true AND confidence > 0.6 from BOTH models.
"""

import json
import os
import sqlite3
import time
from datetime import datetime, timezone

import feedparser
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

OLLAMA_URL = "http://localhost:11434/api/generate"
MODELS = ["qwen2.5:7b"]  # Single model for speed (dual was too slow for 6K+ markets)
MODEL = MODELS[0]
PROMPT_PATH = os.path.join(os.path.dirname(__file__), "..", "prompts", "local_filter.txt")
MARKETS_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")
SCORES_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "filter_scores.sqlite")

RSS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/economicsNews",
    "https://rss.app/feeds/v1.1/ts8Wr9NfGWjR2Nzl.xml",  # AP Economy
]

CONFIDENCE_THRESHOLD = 0.6


def init_scores_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(SCORES_DB), exist_ok=True)
    conn = sqlite3.connect(SCORES_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS filter_scores (
            ticker TEXT PRIMARY KEY,
            title TEXT,
            category TEXT,
            model_probability REAL,
            confidence REAL,
            relevant INTEGER,
            reasoning TEXT,
            market_price REAL,
            price_gap REAL,
            passed_filter INTEGER,
            scored_at TEXT
        )
    """)
    conn.commit()
    return conn


def fetch_headlines() -> list[str]:
    headlines = []
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:10]:
                headlines.append(entry.get("title", ""))
        except Exception:
            continue
    return headlines[:30]


def load_prompt_template() -> str:
    with open(PROMPT_PATH, "r") as f:
        return f.read()


def get_open_markets() -> list[dict]:
    if not os.path.exists(MARKETS_DB):
        return []
    conn = sqlite3.connect(MARKETS_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM markets WHERE status = 'open'").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def query_ollama(prompt: str, model: str = None) -> dict | None:
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": model or MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3, "num_predict": 150},
            },
            timeout=120,
        )
        resp.raise_for_status()
        raw = resp.json().get("response", "")
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start == -1 or end == 0:
            return None
        return json.loads(raw[start:end])
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"  Ollama error: {e}")
        return None


def run_filter() -> list[dict]:
    """Score all open markets and return those that pass the filter."""
    template = load_prompt_template()
    markets = get_open_markets()
    headlines = fetch_headlines()
    headline_text = "\n".join(f"- {h}" for h in headlines) if headlines else "No recent headlines available."

    scores_conn = init_scores_db()
    passed = []

    use_dual = len(MODELS) > 1
    print(f"Scoring {len(markets)} markets with {'dual' if use_dual else 'single'} local model{'s' if use_dual else ''}...")
    print(f"Models: {', '.join(MODELS)}")
    print(f"Headlines fetched: {len(headlines)}")

    for i, market in enumerate(markets):
        ticker = market["ticker"]
        title = market["title"]
        category = market["category"]
        market_price = (market.get("last_price") or 50) / 100.0

        prompt = f"""{template}

Market question: "{title}"
Category: {category}
Current YES price: {market_price:.2f}
Recent relevant headlines:
{headline_text}
"""

        print(f"[{i+1}/{len(markets)}] {title[:60]}...", end=" ", flush=True)

        # Query first model
        result = query_ollama(prompt, model=MODELS[0])
        if result is None:
            print("SKIP")
            continue

        prob = float(result.get("probability", 0.5))
        conf = float(result.get("confidence", 0.5))
        relevant = bool(result.get("relevant", False))
        reasoning = str(result.get("reasoning", ""))

        # If first model says not relevant or low confidence, skip second model
        if not relevant or conf <= CONFIDENCE_THRESHOLD:
            price_gap = abs(prob - market_price)
            passed_filter = False
        elif use_dual:
            # Query second model for confirmation
            result2 = query_ollama(prompt, model=MODELS[1])
            if result2 is None:
                # Second model failed — trust first model alone
                price_gap = abs(prob - market_price)
                passed_filter = relevant and conf > CONFIDENCE_THRESHOLD
            else:
                prob2 = float(result2.get("probability", 0.5))
                conf2 = float(result2.get("confidence", 0.5))
                relevant2 = bool(result2.get("relevant", False))

                # Both must agree on relevance
                both_relevant = relevant and relevant2
                avg_conf = (conf + conf2) / 2
                prob = (prob + prob2) / 2  # Average probability
                conf = avg_conf
                relevant = both_relevant
                reasoning = f"[dual] {reasoning}"
                price_gap = abs(prob - market_price)
                passed_filter = both_relevant and avg_conf > CONFIDENCE_THRESHOLD
        else:
            price_gap = abs(prob - market_price)
            passed_filter = relevant and conf > CONFIDENCE_THRESHOLD

        scores_conn.execute(
            """INSERT OR REPLACE INTO filter_scores
               (ticker, title, category, model_probability, confidence,
                relevant, reasoning, market_price, price_gap, passed_filter, scored_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, title, category, prob, conf, int(relevant), reasoning,
             market_price, price_gap, int(passed_filter),
             datetime.now(timezone.utc).isoformat()),
        )
        scores_conn.commit()

        status = "PASS" if passed_filter else "FILTERED"
        print(f"{status} prob={prob:.2f} conf={conf:.2f}")

        if passed_filter:
            passed.append({
                "ticker": ticker,
                "title": title,
                "category": category,
                "model_probability": prob,
                "confidence": conf,
                "market_price": market_price,
                "price_gap": price_gap,
                "reasoning": reasoning,
            })

    scores_conn.close()
    print(f"\n{len(passed)}/{len(markets)} markets passed local filter")
    return passed


if __name__ == "__main__":
    run_filter()
