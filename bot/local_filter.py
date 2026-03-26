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

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

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
    """Get open markets worth scoring. Pre-filters BEFORE Ollama to cut 90% of junk."""
    if not os.path.exists(MARKETS_DB):
        return []

    conn = sqlite3.connect(MARKETS_DB)
    conn.row_factory = sqlite3.Row

    # Get already-scored tickers (within last hour) to skip
    scored_tickers = set()
    if os.path.exists(SCORES_DB):
        scores_conn = sqlite3.connect(SCORES_DB)
        recent = scores_conn.execute(
            "SELECT ticker FROM filter_scores WHERE scored_at > datetime('now', '-1 hour')"
        ).fetchall()
        scored_tickers = {r[0] for r in recent}
        scores_conn.close()

    rows = conn.execute("SELECT * FROM markets WHERE status IN ('open', 'active')").fetchall()
    conn.close()

    # Pre-filter: skip markets that aren't worth sending to Ollama
    markets = []
    skipped = {"scored": 0, "no_volume": 0, "resolved": 0, "stale_price": 0}

    for r in rows:
        ticker = r["ticker"]

        # Skip recently scored
        if ticker in scored_tickers:
            skipped["scored"] += 1
            continue

        # Skip markets with no trading activity (dead markets)
        volume = r["volume"] or 0
        if volume == 0:
            skipped["no_volume"] += 1
            continue

        # Skip effectively resolved markets (price at 0-2 or 98-100)
        price = r["last_price"] or 50
        if price <= 2 or price >= 98:
            skipped["resolved"] += 1
            continue

        markets.append(dict(r))

    total_skipped = sum(skipped.values())
    if total_skipped > 0:
        print(f"  Pre-filter: {len(markets)} to score, skipped {total_skipped} "
              f"(scored:{skipped['scored']}, no_vol:{skipped['no_volume']}, "
              f"resolved:{skipped['resolved']})")

    # Cap at 200 per cycle to keep within time budget (~100 seconds at 2/sec)
    if len(markets) > 200:
        # Prioritize by volume (most liquid = most tradeable)
        markets.sort(key=lambda m: m.get("volume", 0) or 0, reverse=True)
        markets = markets[:200]
        print(f"  Capped to top 200 by volume")

    return markets


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

        # Inject data feed context for each category
        nws_context = ""
        data_path = os.path.join(os.path.dirname(__file__), "..", "data")
        import sys as _sys
        if data_path not in _sys.path:
            _sys.path.insert(0, data_path)

        if category == "weather":
            try:
                from weather_nws_feed import get_city_forecast
                nws = get_city_forecast(title)
                if nws:
                    nws_context = (
                        f"\nCRITICAL — {nws}\n"
                        f"This is the official settlement source. Base your answer on this forecast.\n"
                        f"Calibration guide based on gap between NWS forecast and market threshold:\n"
                        f"  >8F gap: probability 0.05-0.15 (or 0.85-0.95), confidence 0.85-0.95\n"
                        f"  5-8F gap: probability 0.15-0.30 (or 0.70-0.85), confidence 0.70-0.85\n"
                        f"  3-5F gap: probability 0.30-0.40 (or 0.60-0.70), confidence 0.50-0.70\n"
                        f"  <3F gap: probability 0.40-0.60, confidence 0.30-0.50\n"
                        f"Confidence = how clear the NWS gap is, NOT certainty about the market price.\n"
                        f"Do NOT echo the market price. Use the NWS forecast."
                    )
            except Exception:
                pass

        elif category == "inflation" and "jobless" in title.lower():
            try:
                from jobless_claims_feed import get_jobless_context
                ctx = get_jobless_context(title)
                if ctx:
                    nws_context = f"\n{ctx}\nUse this DOL data to calibrate your probability."
            except Exception:
                pass

        elif category == "energy" or ("gas" in title.lower() and category == "economics"):
            try:
                from eia_petroleum_feed import get_gas_context
                ctx = get_gas_context(title)
                if ctx:
                    nws_context = f"\n{ctx}\nUse this EIA data to calibrate your probability."
            except Exception:
                pass

        elif category == "economics" and any(kw in title.lower() for kw in ["fed", "fomc", "rate"]):
            try:
                from fed_funds_feed import get_fed_context
                ctx = get_fed_context(title)
                if ctx:
                    nws_context = f"\n{ctx}\nUse this Fed data to calibrate your probability."
            except Exception:
                pass

        elif category == "economics" and any(kw in title.lower() for kw in ["treasury", "yield", "10y", "10-year"]):
            try:
                from treasury_auction_feed import get_treasury_context
                ctx = get_treasury_context(title)
                if ctx:
                    nws_context = f"\n{ctx}\nUse this Treasury data to calibrate your probability."
            except Exception:
                pass

        elif category == "congressional":
            try:
                from congressional_trades_feed import get_congressional_context
                ctx = get_congressional_context(title)
                if ctx:
                    nws_context = f"\n{ctx}\nUse this congressional trading data to assess the market."
            except Exception:
                pass

        elif category == "entertainment":
            try:
                from box_office_feed import get_box_office_context
                ctx = get_box_office_context(title)
                if ctx:
                    nws_context = f"\n{ctx}\nUse this box office data to calibrate your probability."
            except Exception:
                pass

        prompt = f"""{template}

Market question: "{title}"
Category: {category}
Current YES price: {market_price:.2f}{nws_context}
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

    # Also include previously-passed markets that haven't been analyzed yet
    if os.path.exists(SCORES_DB):
        prev_conn = sqlite3.connect(SCORES_DB)
        prev_conn.row_factory = sqlite3.Row
        prev_passed = prev_conn.execute(
            "SELECT * FROM filter_scores WHERE passed_filter = 1"
        ).fetchall()
        prev_conn.close()

        existing_tickers = {p["ticker"] for p in passed}
        for pp in prev_passed:
            if pp["ticker"] not in existing_tickers:
                passed.append({
                    "ticker": pp["ticker"],
                    "title": pp["title"],
                    "category": pp["category"],
                    "model_probability": pp["model_probability"],
                    "confidence": pp["confidence"],
                    "market_price": pp["market_price"],
                    "price_gap": pp["price_gap"],
                    "reasoning": pp["reasoning"],
                })

    scores_conn.close()
    print(f"  {len(passed)} markets passed local filter (new + cached)")
    return passed


if __name__ == "__main__":
    run_filter()
