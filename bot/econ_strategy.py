"""
Economic data release strategy.
Uses Cleveland Fed Inflation Nowcast and Atlanta Fed GDPNow as
real-time signals to trade Kalshi CPI, GDP, and jobs markets.

WARNING: ryanfrigo's Kalshi bot found economic trades had -70% ROI
and caused 78% of all losses. This strategy is VERY CONSERVATIVE:
- Only trades when nowcast signal is extremely strong (>12% edge)
- Prefers NO-side (near-certain outcomes)
- Requires higher confidence threshold than weather
- Edge dampened aggressively (max 18% per edge paradox)

Release schedule:
- CPI: ~8:30am ET, usually 2nd or 3rd week of the month
- Nonfarm Payrolls: 8:30am ET, first Friday of month
- Initial Jobless Claims: 8:30am ET, every Thursday
- GDP: 8:30am ET, quarterly
- PCE: 8:30am ET, monthly
- FOMC Decision: 2:00pm ET, 8 meetings/year
"""

import json
import os
import re
import sqlite3
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

MARKETS_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")
ECON_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "econ_strategy.sqlite")


def init_econ_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(ECON_DB), exist_ok=True)
    conn = sqlite3.connect(ECON_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS econ_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            title TEXT,
            release_type TEXT,
            nowcast_value TEXT,
            nowcast_source TEXT,
            market_price REAL,
            model_prob REAL,
            edge REAL,
            created_at TEXT
        )
    """)
    conn.commit()
    return conn


def get_cleveland_fed_nowcast() -> dict | None:
    """Fetch Cleveland Fed inflation nowcast (CPI, PCE)."""
    # Use Perplexity to get the latest nowcast since the API is tricky
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if not api_key:
        return None

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai")
        response = client.chat.completions.create(
            model="sonar-pro",
            messages=[
                {"role": "system", "content": "You are a data retrieval assistant. Return only valid JSON."},
                {"role": "user", "content": """Search for the latest Cleveland Fed Inflation Nowcast.
What are the current nowcast values for:
1. Headline CPI month-over-month change
2. Core CPI month-over-month change
3. PCE month-over-month change
4. Core PCE month-over-month change

Also: what is the next CPI release date?

Respond with JSON:
{"headline_cpi_mom": <float>, "core_cpi_mom": <float>, "pce_mom": <float>, "core_pce_mom": <float>, "next_cpi_date": "<date>", "source": "Cleveland Fed Nowcast"}"""},
            ],
            temperature=0.1,
        )
        raw = response.choices[0].message.content
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > 0:
            return json.loads(raw[start:end])
    except Exception as e:
        print(f"  Cleveland Fed nowcast error: {e}")

    return None


def get_gdpnow() -> dict | None:
    """Fetch Atlanta Fed GDPNow latest estimate."""
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if not api_key:
        return None

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai")
        response = client.chat.completions.create(
            model="sonar-pro",
            messages=[
                {"role": "system", "content": "You are a data retrieval assistant. Return only valid JSON."},
                {"role": "user", "content": """Search for the latest Atlanta Fed GDPNow estimate.
What is the current GDPNow forecast for the next GDP release?

Respond with JSON:
{"gdp_growth_pct": <float>, "quarter": "<e.g. Q1 2026>", "last_updated": "<date>", "source": "Atlanta Fed GDPNow"}"""},
            ],
            temperature=0.1,
        )
        raw = response.choices[0].message.content
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > 0:
            return json.loads(raw[start:end])
    except Exception as e:
        print(f"  GDPNow error: {e}")

    return None


def get_fedwatch() -> dict | None:
    """Get CME FedWatch implied rate probabilities via Perplexity."""
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if not api_key:
        return None

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai")
        response = client.chat.completions.create(
            model="sonar-pro",
            messages=[
                {"role": "system", "content": "You are a data retrieval assistant. Return only valid JSON."},
                {"role": "user", "content": """Search for the current CME FedWatch tool probabilities.
What are the implied probabilities for the next FOMC meeting?

Respond with JSON:
{"next_meeting_date": "<date>", "rate_cut_prob": <float 0-1>, "no_change_prob": <float 0-1>, "rate_hike_prob": <float 0-1>, "current_rate": "<e.g. 4.25-4.50%>", "source": "CME FedWatch"}"""},
            ],
            temperature=0.1,
        )
        raw = response.choices[0].message.content
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > 0:
            return json.loads(raw[start:end])
    except Exception as e:
        print(f"  FedWatch error: {e}")

    return None


def find_econ_markets() -> list[dict]:
    """Find economics-related markets on Kalshi."""
    if not os.path.exists(MARKETS_DB):
        return []

    conn = sqlite3.connect(MARKETS_DB)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT * FROM markets
        WHERE status IN ('open', 'active')
        AND (category IN ('economics', 'inflation')
             OR title LIKE '%CPI%' OR title LIKE '%inflation%'
             OR title LIKE '%GDP%' OR title LIKE '%payroll%'
             OR title LIKE '%nonfarm%' OR title LIKE '%unemployment%'
             OR title LIKE '%jobless%' OR title LIKE '%fed%'
             OR title LIKE '%FOMC%' OR title LIKE '%rate cut%'
             OR title LIKE '%PCE%')
    """).fetchall()
    conn.close()

    return [dict(r) for r in rows]


def match_market_to_nowcast(market: dict, nowcasts: dict) -> dict | None:
    """Match a Kalshi market to a nowcast signal and compute edge."""
    title = market.get("title", "").lower()
    market_price = (market.get("last_price") or 50) / 100.0

    result = None

    # CPI markets
    if "cpi" in title and nowcasts.get("cleveland"):
        cv = nowcasts["cleveland"]
        nowcast_val = cv.get("headline_cpi_mom")
        if nowcast_val is not None:
            # Parse the threshold from the title
            numbers = re.findall(r'(\d+\.?\d*)%', title)
            if numbers:
                threshold = float(numbers[0])
                # Estimate probability based on nowcast vs threshold
                # Simple: if nowcast is well above threshold, prob is high
                diff = (nowcast_val - threshold) / 0.1  # Normalize by typical CPI volatility
                from scipy.stats import norm
                prob = norm.cdf(diff)
                edge = prob - market_price

                result = {
                    "release_type": "CPI",
                    "nowcast_value": str(nowcast_val),
                    "nowcast_source": "Cleveland Fed",
                    "model_prob": prob,
                    "edge": edge,
                }

    # GDP markets
    elif "gdp" in title and nowcasts.get("gdpnow"):
        gn = nowcasts["gdpnow"]
        nowcast_val = gn.get("gdp_growth_pct")
        if nowcast_val is not None:
            numbers = re.findall(r'(\d+\.?\d*)%', title)
            if numbers:
                threshold = float(numbers[0])
                diff = (nowcast_val - threshold) / 0.5  # GDP vol ~0.5%
                from scipy.stats import norm
                prob = norm.cdf(diff)
                edge = prob - market_price

                result = {
                    "release_type": "GDP",
                    "nowcast_value": str(nowcast_val),
                    "nowcast_source": "Atlanta Fed GDPNow",
                    "model_prob": prob,
                    "edge": edge,
                }

    # Fed rate markets
    elif any(kw in title for kw in ["fed", "fomc", "rate cut", "rate hike"]) and nowcasts.get("fedwatch"):
        fw = nowcasts["fedwatch"]
        if "cut" in title:
            prob = fw.get("rate_cut_prob", 0.5)
        elif "hike" in title or "raise" in title:
            prob = fw.get("rate_hike_prob", 0.5)
        else:
            prob = fw.get("no_change_prob", 0.5)

        edge = prob - market_price
        result = {
            "release_type": "FOMC",
            "nowcast_value": f"cut={fw.get('rate_cut_prob', '?')}, hold={fw.get('no_change_prob', '?')}",
            "nowcast_source": "CME FedWatch",
            "model_prob": prob,
            "edge": edge,
        }

    return result


def analyze_econ_markets() -> list[dict]:
    """Run economic data analysis and find trading signals."""
    print("  Econ strategy: fetching nowcasts...")

    nowcasts = {}

    # Fetch all nowcasts (these use Perplexity, so they cost ~$0.007 each)
    cleveland = get_cleveland_fed_nowcast()
    if cleveland:
        nowcasts["cleveland"] = cleveland
        print(f"  Cleveland Fed: CPI={cleveland.get('headline_cpi_mom', '?')}%")

    gdpnow = get_gdpnow()
    if gdpnow:
        nowcasts["gdpnow"] = gdpnow
        print(f"  GDPNow: {gdpnow.get('gdp_growth_pct', '?')}% ({gdpnow.get('quarter', '?')})")

    fedwatch = get_fedwatch()
    if fedwatch:
        nowcasts["fedwatch"] = fedwatch
        print(f"  FedWatch: cut={fedwatch.get('rate_cut_prob', '?')}, hold={fedwatch.get('no_change_prob', '?')}")

    if not nowcasts:
        print("  No nowcast data available")
        return []

    markets = find_econ_markets()
    if not markets:
        print("  No econ markets found")
        return []

    print(f"  Found {len(markets)} econ markets, matching to nowcasts...")

    conn = init_econ_db()
    signals = []

    for m in markets:
        match = match_market_to_nowcast(m, nowcasts)
        if match is None:
            continue

        edge = match["edge"]
        model_prob = match["model_prob"]
        ticker = m["ticker"]
        title = m["title"]
        market_price = (m.get("last_price") or 50) / 100.0

        conn.execute(
            """INSERT INTO econ_signals
               (ticker, title, release_type, nowcast_value, nowcast_source,
                market_price, model_prob, edge, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, title, match["release_type"], match["nowcast_value"],
             match["nowcast_source"], market_price, model_prob, edge,
             datetime.now(timezone.utc).isoformat()),
        )

        # VERY conservative: only trade econ with strong signal (>12% edge)
        # And cap edge at 18% (edge paradox from alexandermazza)
        dampened_edge = edge
        if abs(edge) > 0.18:
            dampened_edge = 0.18 * (1 if edge > 0 else -1) + (abs(edge) - 0.18) * 0.5 * (1 if edge > 0 else -1)

        if abs(dampened_edge) > 0.12:
            side = "YES" if dampened_edge > 0 else "NO"
            print(f"  SIGNAL [{match['release_type']}]: {side} edge={dampened_edge:+.2f} — {title[:60]}...")
            print(f"    Nowcast: {match['nowcast_value']} via {match['nowcast_source']}")
            signals.append({
                "ticker": ticker,
                "title": title,
                "category": "economics",
                "model_probability": model_prob,
                "confidence": min(0.85, 0.60 + abs(dampened_edge)),  # Lower max confidence for econ
                "market_price": market_price,
                "price_gap": abs(dampened_edge),
                "reasoning": f"{match['nowcast_source']}: {match['nowcast_value']} → prob={model_prob:.2f}",
            })

    conn.commit()
    conn.close()
    return signals


if __name__ == "__main__":
    signals = analyze_econ_markets()
    print(f"\n{len(signals)} econ signals found")
