"""
Data release sniper: pre-positions on economic data releases.
Knows the exact schedule of BLS/BEA/Fed releases and has Perplexity
research ready before the number drops.

Key releases:
- CPI: ~8:30am ET, monthly (usually 2nd or 3rd Tuesday)
- Nonfarm Payrolls: 8:30am ET, first Friday of month
- Initial Jobless Claims: 8:30am ET, every Thursday
- GDP: 8:30am ET, quarterly (advance/preliminary/final)
- FOMC Decision: 2:00pm ET, 8 meetings/year
- PCE: 8:30am ET, monthly
"""

import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

SNIPER_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "sniper.sqlite")

# BLS release schedule keywords to detect upcoming releases
RELEASE_KEYWORDS = {
    "cpi": {
        "market_keywords": ["cpi", "consumer price", "inflation rate"],
        "time": "08:30",
        "source": "BLS",
        "prep_minutes": 30,
    },
    "payrolls": {
        "market_keywords": ["nonfarm", "payroll", "jobs report", "employment situation"],
        "time": "08:30",
        "source": "BLS",
        "prep_minutes": 30,
    },
    "jobless_claims": {
        "market_keywords": ["jobless claim", "initial claim", "unemployment claim"],
        "time": "08:30",
        "source": "DOL",
        "prep_minutes": 15,
    },
    "gdp": {
        "market_keywords": ["gdp", "gross domestic product"],
        "time": "08:30",
        "source": "BEA",
        "prep_minutes": 30,
    },
    "fomc": {
        "market_keywords": ["fomc", "federal reserve", "fed rate", "rate cut", "rate hike", "fed funds"],
        "time": "14:00",
        "source": "FOMC",
        "prep_minutes": 60,
    },
    "pce": {
        "market_keywords": ["pce", "personal consumption"],
        "time": "08:30",
        "source": "BEA",
        "prep_minutes": 30,
    },
}


def init_sniper_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(SNIPER_DB), exist_ok=True)
    conn = sqlite3.connect(SNIPER_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sniper_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            release_type TEXT,
            ticker TEXT,
            title TEXT,
            pre_release_price REAL,
            perplexity_estimate REAL,
            action TEXT,
            acted_at TEXT
        )
    """)
    conn.commit()
    return conn


def find_release_markets(markets: list[dict]) -> list[dict]:
    """Find markets that correspond to upcoming data releases."""
    matched = []
    now = datetime.now()

    for m in markets:
        title = m.get("title", "").lower()
        close_time = m.get("close_time", "")

        for release_type, config in RELEASE_KEYWORDS.items():
            if any(kw in title for kw in config["market_keywords"]):
                # Check if market closes today or tomorrow (near a release)
                try:
                    close_dt = datetime.fromisoformat(
                        close_time.replace("Z", "+00:00")
                    ).replace(tzinfo=None)
                    hours_until_close = (close_dt - now).total_seconds() / 3600

                    if 0 < hours_until_close < 48:
                        matched.append({
                            **m,
                            "release_type": release_type,
                            "release_config": config,
                            "hours_until_close": hours_until_close,
                        })
                except (ValueError, TypeError):
                    pass

    return sorted(matched, key=lambda x: x.get("hours_until_close", 999))


def pre_research(market: dict) -> dict | None:
    """Use Perplexity to research a market before the data release."""
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if not api_key:
        return None

    release_type = market.get("release_type", "")
    title = market.get("title", "")

    prompt = f"""You are a prediction market analyst preparing for an upcoming economic data release.

Market: {title}
Release type: {release_type}
Current market price: {market.get('last_price', 50) / 100:.2f}

Search for:
1. The consensus/median forecast for this specific release
2. Any leading indicators or nowcasts that predict the number
3. Historical surprise rate (how often does actual beat/miss consensus?)
4. Any unusual signals this time (survey data, other indicators)

Respond with JSON:
{{"probability": <float>, "confidence": <float>, "consensus_estimate": "<the consensus number>", "reasoning": "<cite specific data>"}}
"""

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai")
        response = client.chat.completions.create(
            model="sonar-pro",
            messages=[
                {"role": "system", "content": "Research upcoming economic data releases. Respond with JSON only."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        raw = response.choices[0].message.content
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > 0:
            return json.loads(raw[start:end])
    except Exception as e:
        print(f"  Sniper research error: {e}")

    return None


def run_sniper(all_markets: list[dict], session_id: str = "") -> list[dict]:
    """Find release-related markets, research them, flag for trading."""
    release_markets = find_release_markets(all_markets)

    if not release_markets:
        return []

    print(f"  Data sniper: {len(release_markets)} release-related markets found")
    conn = init_sniper_db()
    flagged = []

    for m in release_markets[:3]:  # Limit research calls
        release_type = m["release_type"]
        hours = m["hours_until_close"]
        print(f"  [{release_type}] {m['title'][:60]}... (closes in {hours:.1f}hr)")

        research = pre_research(m)
        if research:
            prob = research.get("probability", 0.5)
            conf = research.get("confidence", 0.5)
            consensus = research.get("consensus_estimate", "unknown")
            market_price = m.get("last_price", 50) / 100.0

            print(f"    Consensus: {consensus}")
            print(f"    Our estimate: {prob:.2f} (conf: {conf:.2f}) vs market: {market_price:.2f}")

            conn.execute(
                """INSERT INTO sniper_actions
                   (release_type, ticker, title, pre_release_price,
                    perplexity_estimate, action, acted_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (release_type, m.get("ticker", ""), m.get("title", ""),
                 market_price, prob, "researched",
                 datetime.now(timezone.utc).isoformat()),
            )

            # Pass to the main pipeline if there's a significant gap
            gap = abs(prob - market_price)
            if gap > 0.08 and conf > 0.7:
                flagged.append({
                    **m,
                    "model_probability": prob,
                    "confidence": conf,
                    "market_price": market_price,
                    "price_gap": gap,
                    "reasoning": research.get("reasoning", ""),
                })
                print(f"    FLAGGED for trading (gap: {gap:.2f})")

    conn.commit()
    conn.close()
    return flagged


if __name__ == "__main__":
    # Test with current markets
    from market_scanner import init_db, scan_markets
    import sqlite3 as sql

    markets_db = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")
    if os.path.exists(markets_db):
        conn = sql.connect(markets_db)
        conn.row_factory = sql.Row
        markets = [dict(r) for r in conn.execute("SELECT * FROM markets").fetchall()]
        conn.close()
        run_sniper(markets)
    else:
        print("No markets DB found. Run market_scanner first.")
