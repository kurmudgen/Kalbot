"""
Self-calibration engine — resolution-count-driven trigger cascade.

Primary triggers fire based on resolution count (not time):
  Tier 0:   every 1 resolution  — instant sanity check on new trade
  Tier 1.5: every 3 resolutions — market movement reflector
  Tier 2:   every 10            — pattern analyzer (category + city)
  Tier 2.5: every 25            — confidence recalibration
  Tier 3:   every 50            — adjustment executor (trial + auto-revert)
  Tier 3.5: every 200           — deep pattern review
  Tier 4:   every 500           — benchmark comparison
  Tier 5:   every 2000          — full system review

Time-based fallback: if any tier goes 72hr without firing, it fires
anyway on whatever data exists (logged as time_fallback_trigger).

Safety: never modifies financial risk params, max 0.10 threshold change,
full audit trail in calibration_history table.
"""

import json
import os
import sqlite3
import requests
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

ENABLED = os.getenv("SELF_CALIBRATION_ENABLED", "true").lower() == "true"
MIN_TRADES = int(os.getenv("SELF_CAL_MIN_TRADES", "5"))
MIN_TRADES_CITY = int(os.getenv("SELF_CAL_MIN_TRADES_CITY", "3"))
TRIAL_HOURS = int(os.getenv("SELF_CAL_TRIAL_HOURS", "24"))
MAX_THRESHOLD_CHANGE = float(os.getenv("SELF_CAL_MAX_THRESHOLD_CHANGE", "0.10"))

DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")
RESOLUTIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "resolutions.sqlite")
BASE_RATES_PATH = os.path.join(os.path.dirname(__file__), "..", "calibration", "base_rates.txt")

OLLAMA_URL = "http://localhost:11434/api/generate"
LOCAL_MODEL = os.getenv("LOCAL_FILTER_MODEL", "qwen2.5:32b")

# Resolution-count thresholds for each tier
TIER_THRESHOLDS = {
    "0":   1,
    "1.5": 3,
    "2":   10,
    "2.5": 25,
    "3":   50,
    "3.5": 200,
    "4":   500,
    "5":   2000,
}

# Time-based fallback: fire if tier hasn't run in this many hours
FALLBACK_HOURS = 72

# Protected parameters — self-calibrator can NEVER modify these
PROTECTED_PARAMS = {
    "MAX_TRADE_SIZE", "MAX_NIGHTLY_SPEND", "MAX_DAILY_SPEND",
    "BALANCE_FLOOR", "KELLY_FRACTION",
}


def _init_tables(conn: sqlite3.Connection):
    """Create calibration tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calibration_reflections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            tier INTEGER,
            trade_id TEXT,
            observation TEXT,
            action_taken TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calibration_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            tier INTEGER,
            parameter TEXT,
            before_value TEXT,
            after_value TEXT,
            reasoning TEXT,
            supporting_trades INTEGER,
            confidence TEXT,
            status TEXT DEFAULT 'active'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calibration_trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hypothesis_id INTEGER,
            change_description TEXT,
            before_value TEXT,
            after_value TEXT,
            trial_start TEXT,
            trial_end TEXT,
            win_rate_before REAL,
            win_rate_during REAL,
            verdict TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calibration_counters (
            tier TEXT PRIMARY KEY,
            last_fired_at_count INTEGER DEFAULT 0,
            current_count INTEGER DEFAULT 0,
            threshold INTEGER,
            last_fired_at TEXT
        )
    """)
    # Seed tier rows if missing
    for tier, threshold in TIER_THRESHOLDS.items():
        conn.execute(
            """INSERT OR IGNORE INTO calibration_counters
               (tier, last_fired_at_count, current_count, threshold, last_fired_at)
               VALUES (?, 0, 0, ?, ?)""",
            (tier, threshold, datetime.now(timezone.utc).isoformat()),
        )
    conn.commit()


def _query_local_model(prompt: str) -> dict | None:
    """Query local 32b model and parse JSON response."""
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": LOCAL_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.2, "num_predict": 500},
            },
            timeout=120,
        )
        resp.raise_for_status()
        raw = resp.json().get("response", "")
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > 0:
            return json.loads(raw[start:end])
    except Exception as e:
        print(f"  Self-cal model error: {e}")
    return None


# ── Resolution Counter + Cascade ───────────────────────────────

def _increment_counter(conn: sqlite3.Connection) -> int:
    """Increment resolution count across all tiers. Returns new count."""
    conn.execute("UPDATE calibration_counters SET current_count = current_count + 1")
    count = conn.execute(
        "SELECT current_count FROM calibration_counters LIMIT 1"
    ).fetchone()[0]
    conn.commit()
    return count


def _get_due_tiers(conn: sqlite3.Connection) -> list[str]:
    """Return list of tier names that should fire based on count or time fallback."""
    rows = conn.execute(
        "SELECT tier, last_fired_at_count, current_count, threshold, last_fired_at FROM calibration_counters"
    ).fetchall()

    due = []
    now = datetime.now(timezone.utc)

    for row in rows:
        tier = row[0]
        last_count = row[1]
        current = row[2]
        threshold = row[3]
        last_fired = row[4]

        # Count-based trigger
        if current - last_count >= threshold:
            due.append(tier)
            continue

        # Time-based fallback
        if last_fired:
            try:
                last_dt = datetime.fromisoformat(last_fired.replace("Z", "+00:00"))
                if (now - last_dt).total_seconds() > FALLBACK_HOURS * 3600:
                    due.append(tier)
                    print(f"  Self-cal: time_fallback_trigger for tier {tier} ({FALLBACK_HOURS}hr)")
            except Exception:
                pass

    return sorted(due, key=lambda t: float(t))


def _mark_tier_fired(conn: sqlite3.Connection, tier: str):
    """Update counter after a tier fires."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """UPDATE calibration_counters
           SET last_fired_at_count = current_count, last_fired_at = ?
           WHERE tier = ?""",
        (now, tier),
    )
    conn.commit()


# ── Tier dispatch ─────────────────────────────────────────────

# Map tier names to their handler functions (defined below)
_TIER_HANDLERS = {}  # populated after function definitions


def _run_tier(tier: str, is_fallback: bool = False):
    """Execute a single tier's handler."""
    handler = _TIER_HANDLERS.get(tier)
    if not handler:
        return

    trigger_type = "time_fallback_trigger" if is_fallback else "count_trigger"
    print(f"  Self-cal: firing tier {tier} ({trigger_type})")

    try:
        handler()
    except Exception as e:
        print(f"  Self-cal tier {tier} error: {e}")

    # Log the trigger
    try:
        conn = sqlite3.connect(DECISIONS_DB)
        _init_tables(conn)
        conn.execute(
            "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, ?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), tier, f"tier_{tier}_trigger",
             f"Tier {tier} fired via {trigger_type}", trigger_type),
        )
        _mark_tier_fired(conn, tier)
        conn.commit()
        conn.close()
    except Exception:
        pass


def on_resolution(resolved_trade: dict | None = None):
    """Called by resolution_tracker after each new resolved trade.

    Increments the global counter, checks which tiers are due,
    and fires them in order. This is the primary trigger mechanism.
    """
    if not ENABLED:
        return

    conn = sqlite3.connect(DECISIONS_DB)
    _init_tables(conn)

    count = _increment_counter(conn)
    due = _get_due_tiers(conn)
    conn.close()

    if due:
        print(f"  Self-cal: resolution #{count}, firing tiers: {due}")

    for tier in due:
        _run_tier(tier)


def check_time_fallbacks():
    """Called from dual_strategy.py on each cycle as safety net.

    Only fires tiers that haven't run in FALLBACK_HOURS due to
    insufficient resolution count.
    """
    if not ENABLED or not os.path.exists(DECISIONS_DB):
        return

    conn = sqlite3.connect(DECISIONS_DB)
    _init_tables(conn)

    now = datetime.now(timezone.utc)
    rows = conn.execute(
        "SELECT tier, last_fired_at FROM calibration_counters"
    ).fetchall()
    conn.close()

    for row in rows:
        tier, last_fired = row
        if not last_fired:
            continue
        try:
            last_dt = datetime.fromisoformat(last_fired.replace("Z", "+00:00"))
            if (now - last_dt).total_seconds() > FALLBACK_HOURS * 3600:
                _run_tier(tier, is_fallback=True)
        except Exception:
            pass


# ── TIER 0: Instant Sanity Check ──────────────────────────────

def tier0_sanity_check():
    """Quick check on the most recent resolution.

    Fires on every single resolution. Lightweight — no model calls.
    Checks if the result contradicts a high-confidence prediction.
    """
    if not os.path.exists(RESOLUTIONS_DB):
        return

    conn = sqlite3.connect(RESOLUTIONS_DB)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM resolved_trades ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()

    if not row:
        return

    pnl = row["pnl"]
    conf = row["our_confidence"]
    ticker = row["ticker"]
    category = row["category"] or "unknown"

    # Flag high-confidence losses — the model was very sure and wrong
    if pnl <= 0 and conf >= 0.85:
        print(f"  Self-cal T0: HIGH-CONF LOSS — {ticker} conf={conf:.2f} pnl=${pnl:.2f} [{category}]")
        dconn = sqlite3.connect(DECISIONS_DB)
        _init_tables(dconn)
        dconn.execute(
            "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 0, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), ticker,
             f"High-confidence loss: conf={conf:.2f}, pnl=${pnl:.2f}, cat={category}",
             "high_conf_loss_flagged"),
        )
        dconn.commit()
        dconn.close()


# ── TIER 1.5: Market Movement Reflector (was Tier 1) ──────────

def tier1_market_movement() -> list[dict]:
    """Check open positions for market price movement contradicting prediction.

    If YES price on a NO bet rises >20 points since entry, flag for review.
    Runs on local 32b model. No API calls.
    """
    if not ENABLED or not os.path.exists(DECISIONS_DB):
        return []

    conn = sqlite3.connect(DECISIONS_DB)
    conn.row_factory = sqlite3.Row
    _init_tables(conn)

    # Get open positions (executed, not exited)
    positions = conn.execute("""
        SELECT ticker, title, side, market_price, cloud_probability, decided_at
        FROM decisions
        WHERE executed = 1 AND side NOT LIKE '%EXIT%'
        AND decided_at > datetime('now', '-48 hours')
    """).fetchall()

    if not positions:
        conn.close()
        return []

    # Get current market prices
    markets_db = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")
    current_prices = {}
    if os.path.exists(markets_db):
        mconn = sqlite3.connect(markets_db)
        for row in mconn.execute("SELECT ticker, last_price FROM markets").fetchall():
            current_prices[row[0]] = (row[1] or 50) / 100.0
        mconn.close()

    flags = []
    now = datetime.now(timezone.utc).isoformat()

    for pos in positions:
        ticker = pos["ticker"]
        entry_price = pos["market_price"] or 0.5
        side = pos["side"]
        current = current_prices.get(ticker)

        if current is None:
            continue

        # Check if market moved against us
        if side == "NO":
            # We bet NO — YES price rising means market disagrees
            movement = (current - entry_price) * 100  # In cents
            if movement > 20:
                observation = f"NO bet on {ticker}: YES price rose {movement:.0f}c since entry ({entry_price:.2f} -> {current:.2f})"
                flags.append({"ticker": ticker, "observation": observation, "movement": movement})
                conn.execute(
                    "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 1.5, ?, ?, ?)",
                    (now, ticker, observation, "flagged_for_review"),
                )
        elif side == "YES":
            # We bet YES — YES price dropping means market disagrees
            movement = (entry_price - current) * 100
            if movement > 20:
                observation = f"YES bet on {ticker}: YES price dropped {movement:.0f}c since entry ({entry_price:.2f} -> {current:.2f})"
                flags.append({"ticker": ticker, "observation": observation, "movement": movement})
                conn.execute(
                    "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 1.5, ?, ?, ?)",
                    (now, ticker, observation, "flagged_for_review"),
                )

    conn.commit()
    conn.close()

    if flags:
        print(f"  Self-cal T1: {len(flags)} positions with adverse movement >20c")

    return flags


# ── TIER 2: Pattern Analyzer (every 10 resolutions) ─────────

def _extract_city_from_title(title: str) -> str | None:
    """Extract city name from a weather market title for grouping."""
    title_lower = title.lower()
    cities = ["austin", "chicago", "denver", "houston", "miami", "nyc",
              "new york", "los angeles", "phoenix", "seattle", "philadelphia"]
    for city in cities:
        if city in title_lower:
            return "nyc" if city == "new york" else city
    return None


def tier2_pattern_analyzer() -> list[dict]:
    """Analyze last 24hr of resolved trades for loss patterns.

    Groups losses by category AND city (for weather). Runs city-level
    analysis at MIN_TRADES_CITY threshold (default 3) since city-specific
    patterns have clearer causal mechanisms. Category-level and threshold-level
    patterns use the standard MIN_TRADES threshold (default 5).
    """
    if not ENABLED or not os.path.exists(RESOLUTIONS_DB):
        return []

    conn = sqlite3.connect(RESOLUTIONS_DB)
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    losses = conn.execute("""
        SELECT ticker, title, category, side, our_confidence, pnl, resolved_at
        FROM resolved_trades
        WHERE resolved_at > ? AND pnl <= 0 AND our_confidence > 0.70
    """, (cutoff,)).fetchall()

    wins = conn.execute("""
        SELECT ticker, title, category, side, our_confidence, pnl
        FROM resolved_trades
        WHERE resolved_at > ? AND pnl > 0 AND our_confidence > 0.70
    """, (cutoff,)).fetchall()
    conn.close()

    # Group losses by city for weather trades
    city_losses = {}  # city -> [losses]
    category_losses = {}  # category -> [losses]
    for l in losses:
        cat = l["category"] or "unknown"
        category_losses.setdefault(cat, []).append(l)
        if cat == "weather":
            city = _extract_city_from_title(l["title"])
            if city:
                city_losses.setdefault(city, []).append(l)

    # Group wins by city for context
    city_wins = {}
    for w in wins:
        if (w["category"] or "") == "weather":
            city = _extract_city_from_title(w["title"])
            if city:
                city_wins.setdefault(city, []).append(w)

    hypotheses = []
    now = datetime.now(timezone.utc).isoformat()

    # City-level analysis (lower threshold — clearer causal mechanisms)
    for city, city_loss_list in city_losses.items():
        if len(city_loss_list) < MIN_TRADES_CITY:
            continue

        city_win_list = city_wins.get(city, [])
        loss_lines = [
            f"  LOSS: {l['title'][:60]} | conf={l['our_confidence']:.2f} | pnl=${l['pnl']:.2f}"
            for l in city_loss_list
        ]
        win_lines = [
            f"  WIN: {w['title'][:60]} | conf={w['our_confidence']:.2f}"
            for w in city_win_list[:5]
        ]

        prompt = f"""You are analyzing trading performance for a prediction market bot.
This analysis is specifically for the city of {city.upper()}.

{city.upper()} LOSSES with confidence above 0.70 (last 24 hours):
{chr(10).join(loss_lines)}

{city.upper()} WINS with confidence above 0.70 (last 24 hours):
{chr(10).join(win_lines) if win_lines else '  (none)'}

Total: {len(city_loss_list)} losses, {len(city_win_list)} wins for {city.upper()}.

Focus on city-specific factors: Is this city harder to predict due to geography,
coastal effects, altitude, or microclimate? Should this city require a wider NWS
gap or higher confidence threshold?

Answer in JSON format:
{{
  "city": "{city}",
  "common_loss_pattern": "<what characteristic do the {city} losses share?>",
  "causal_mechanism": "<why is {city} specifically harder to predict?>",
  "recommended_change": "<one specific threshold adjustment for this city>",
  "affected_parameter": "<e.g. CITY_MIN_NWS_GAP, CITY_MIN_CONFIDENCE, SUSPENDED_CITIES>",
  "recommended_value": "<new value as string>",
  "confidence": "<low/medium/high>",
  "scope": "city"
}}"""

        result = _query_local_model(prompt)
        if result:
            hypothesis = {
                "pattern": result.get("common_loss_pattern", ""),
                "recommendation": result.get("recommended_change", ""),
                "parameter": result.get("affected_parameter", ""),
                "value": result.get("recommended_value", ""),
                "confidence": result.get("confidence", "low"),
                "scope": "city",
                "city": city,
                "supporting_trades": len(city_loss_list),
                "timestamp": now,
            }
            hypotheses.append(hypothesis)

            dconn = sqlite3.connect(DECISIONS_DB)
            _init_tables(dconn)
            dconn.execute(
                "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 2, ?, ?, ?)",
                (now, f"city_pattern_{city}",
                 json.dumps(result)[:500], "city_hypothesis_generated"),
            )
            dconn.commit()
            dconn.close()

            print(f"  Self-cal T2: city pattern [{city}] — {hypothesis['pattern'][:50]}")
            print(f"    Mechanism: {result.get('causal_mechanism', 'unknown')[:60]}")
            print(f"    Recommendation: {hypothesis['recommendation'][:60]}")
            print(f"    Confidence: {hypothesis['confidence']}, trades: {hypothesis['supporting_trades']}")

    # Category-level analysis (standard threshold)
    if len(losses) >= MIN_TRADES:
        loss_lines = []
        for l in losses:
            city = _extract_city_from_title(l["title"]) if l["category"] == "weather" else None
            city_tag = f" [{city}]" if city else ""
            loss_lines.append(f"  LOSS: {l['title'][:60]}{city_tag} | conf={l['our_confidence']:.2f} | cat={l['category']} | pnl=${l['pnl']:.2f}")

        win_lines = [
            f"  WIN: {w['title'][:60]} | conf={w['our_confidence']:.2f} | cat={w['category']}"
            for w in wins[:10]
        ]

        prompt = f"""You are analyzing trading performance for a prediction market bot.

LOSSES with confidence above 0.70 (last 24 hours):
{chr(10).join(loss_lines)}

WINS with confidence above 0.70 (last 24 hours):
{chr(10).join(win_lines) if win_lines else '  (none)'}

Answer in JSON format:
{{
  "common_loss_pattern": "<what characteristic do the losses share?>",
  "data_gap": "<what data was available that should have predicted the correct outcome?>",
  "recommended_change": "<one specific threshold or prompt adjustment>",
  "affected_parameter": "<which parameter to change, e.g. WEATHER_CONFIDENCE>",
  "recommended_value": "<new value as string>",
  "confidence": "<low/medium/high>",
  "scope": "category",
  "min_trades_to_validate": <integer>
}}"""

        result = _query_local_model(prompt)
        if result:
            hypothesis = {
                "pattern": result.get("common_loss_pattern", ""),
                "recommendation": result.get("recommended_change", ""),
                "parameter": result.get("affected_parameter", ""),
                "value": result.get("recommended_value", ""),
                "confidence": result.get("confidence", "low"),
                "scope": "category",
                "supporting_trades": len(losses),
                "timestamp": now,
            }
            hypotheses.append(hypothesis)

            dconn = sqlite3.connect(DECISIONS_DB)
            _init_tables(dconn)
            dconn.execute(
                "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 2, ?, ?, ?)",
                (now, "pattern_analysis",
                 json.dumps(result)[:500], "hypothesis_generated"),
            )
            dconn.commit()
            dconn.close()

            print(f"  Self-cal T2: category pattern — {hypothesis['pattern'][:60]}")
            print(f"    Recommendation: {hypothesis['recommendation'][:60]}")
            print(f"    Confidence: {hypothesis['confidence']}, trades: {hypothesis['supporting_trades']}")

    # Options-specific analysis (every 10 new observations)
    try:
        options_db = os.path.join(os.path.dirname(__file__), "..", "logs", "options_paper.sqlite")
        if os.path.exists(options_db):
            oconn = sqlite3.connect(options_db)
            obs_count = oconn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
            oconn.close()
            # Fire options analysis every 10 observations
            if obs_count > 0 and obs_count % 10 == 0:
                _run_options_pattern_analysis(obs_count)
    except Exception:
        pass

    return hypotheses


def _run_options_pattern_analysis(obs_count: int):
    """Analyze options observation patterns using local model."""
    options_db = os.path.join(os.path.dirname(__file__), "..", "logs", "options_paper.sqlite")
    conn = sqlite3.connect(options_db)
    conn.row_factory = sqlite3.Row

    # Get recent observations summary
    rows = conn.execute("""
        SELECT release_type, COUNT(*) as n,
               ROUND(AVG(options_edge_gap), 4) as avg_gap,
               ROUND(AVG(implied_vol), 4) as avg_iv,
               ROUND(AVG(straddle_implied_prob), 4) as avg_straddle_prob
        FROM observations
        GROUP BY release_type
    """).fetchall()
    conn.close()

    if not rows:
        return

    summary = "\n".join(
        f"  {r['release_type']}: {r['n']} obs, avg_gap={r['avg_gap']:.2%}, avg_iv={r['avg_iv']:.2%}, avg_straddle_prob={r['avg_straddle_prob']:.2%}"
        for r in rows
    )

    prompt = f"""You are analyzing options market observations for a prediction market bot.

The bot observes SPY/QQQ options chains around economic releases and compares
the options-implied probability against its Kalshi probability estimate.

Observations by release type:
{summary}

Total observations: {obs_count}

Analyze:
1. Strike selection accuracy — are ATM strikes providing useful signal?
2. Expiration timing — weekly vs monthly, which shows more edge?
3. Edge gap threshold — what minimum gap predicts profitable trades?
4. Which release types generate the strongest options signals?

Answer in JSON format:
{{
  "strike_selection": "<assessment>",
  "best_expiration": "<weekly or monthly>",
  "min_edge_gap": "<recommended minimum>",
  "strongest_release": "<cpi, fed, jobless, or tsa>",
  "confidence": "<low/medium/high>"
}}"""

    result = _query_local_model(prompt)
    if result:
        dconn = sqlite3.connect(DECISIONS_DB)
        _init_tables(dconn)
        dconn.execute(
            "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 2, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), "options_pattern_analysis",
             json.dumps(result)[:500], "options_hypothesis_generated"),
        )
        dconn.commit()
        dconn.close()
        print(f"  Self-cal T2: options pattern — strongest={result.get('strongest_release', '?')}, gap={result.get('min_edge_gap', '?')}")


# ── TIER 2.5: Confidence Recalibration (every 25 resolutions) ─

def tier25_confidence_recalibration():
    """Analyze confidence calibration across recent resolutions.

    Checks if high-confidence trades are actually winning at a higher
    rate than low-confidence trades. If not, confidence scoring is
    miscalibrated and thresholds need adjustment.
    """
    if not os.path.exists(RESOLUTIONS_DB):
        return

    conn = sqlite3.connect(RESOLUTIONS_DB)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT our_confidence, pnl, category
        FROM resolved_trades
        ORDER BY id DESC LIMIT 25
    """).fetchall()
    conn.close()

    if len(rows) < 10:
        return

    # Bucket by confidence
    high_conf = [r for r in rows if r["our_confidence"] >= 0.85]
    mid_conf = [r for r in rows if 0.70 <= r["our_confidence"] < 0.85]
    low_conf = [r for r in rows if r["our_confidence"] < 0.70]

    def wr(bucket):
        if not bucket:
            return 0, 0
        wins = sum(1 for r in bucket if r["pnl"] > 0)
        return wins / len(bucket), len(bucket)

    high_wr, high_n = wr(high_conf)
    mid_wr, mid_n = wr(mid_conf)
    low_wr, low_n = wr(low_conf)

    observation = (
        f"Confidence calibration (last 25): "
        f"high(≥0.85)={high_wr:.0%} n={high_n}, "
        f"mid(0.70-0.85)={mid_wr:.0%} n={mid_n}, "
        f"low(<0.70)={low_wr:.0%} n={low_n}"
    )
    print(f"  Self-cal T2.5: {observation}")

    # Flag if high-confidence is NOT beating mid-confidence
    miscalibrated = high_n >= 3 and mid_n >= 3 and high_wr <= mid_wr
    action = "miscalibration_flagged" if miscalibrated else "calibration_ok"

    if miscalibrated:
        print(f"  Self-cal T2.5: WARNING — high-conf WR ({high_wr:.0%}) <= mid-conf WR ({mid_wr:.0%})")

    dconn = sqlite3.connect(DECISIONS_DB)
    _init_tables(dconn)
    dconn.execute(
        "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 2.5, ?, ?, ?)",
        (datetime.now(timezone.utc).isoformat(), "conf_recalibration", observation, action),
    )
    dconn.commit()
    dconn.close()


# ── TIER 3: Adjustment Executor (every 50 resolutions) ───────

def tier3_daily_executor() -> list[dict]:
    """Review T2 hypotheses, apply qualifying adjustments with 24hr trial.

    Only applies changes to category-level thresholds (not financial risk).
    Max change: 0.10 per parameter per cycle.
    """
    if not ENABLED or not os.path.exists(DECISIONS_DB):
        return []

    conn = sqlite3.connect(DECISIONS_DB)
    _init_tables(conn)

    # Get recent T2 hypotheses
    rows = conn.execute("""
        SELECT id, timestamp, observation
        FROM calibration_reflections
        WHERE tier = 2 AND action_taken = 'hypothesis_generated'
        AND timestamp > datetime('now', '-24 hours')
    """).fetchall()

    applied = []

    for row in rows:
        try:
            hypothesis = json.loads(row[1] if isinstance(row[1], str) else "")
        except Exception:
            # observation column has the JSON
            try:
                hypothesis = json.loads(row[2] if len(row) > 2 else "")
            except Exception:
                continue

        param = hypothesis.get("affected_parameter", "")
        value = hypothesis.get("recommended_value", "")
        confidence = hypothesis.get("confidence", "low")
        supporting = hypothesis.get("min_trades_to_validate", 10)

        # Safety checks
        if not param or not value:
            continue
        if param.upper() in PROTECTED_PARAMS:
            print(f"  Self-cal T3: BLOCKED — {param} is a protected parameter")
            continue
        if confidence == "low":
            continue

        # Check max threshold change
        current_val = os.getenv(param, "")
        if current_val:
            try:
                change = abs(float(value) - float(current_val))
                if change > MAX_THRESHOLD_CHANGE:
                    print(f"  Self-cal T3: BLOCKED — change {change:.2f} exceeds max {MAX_THRESHOLD_CHANGE}")
                    continue
            except ValueError:
                pass

        # Get win rate before trial
        rconn = sqlite3.connect(RESOLUTIONS_DB) if os.path.exists(RESOLUTIONS_DB) else None
        wr_before = 0
        if rconn:
            try:
                total = rconn.execute("SELECT COUNT(*) FROM resolved_trades WHERE resolved_at > datetime('now', '-48 hours')").fetchone()[0]
                wins = rconn.execute("SELECT COUNT(*) FROM resolved_trades WHERE resolved_at > datetime('now', '-48 hours') AND pnl > 0").fetchone()[0]
                wr_before = wins / total if total > 0 else 0
            except Exception:
                pass
            finally:
                rconn.close()

        # Log the trial
        now = datetime.now(timezone.utc).isoformat()
        trial_end = (datetime.now(timezone.utc) + timedelta(hours=TRIAL_HOURS)).isoformat()

        conn.execute(
            """INSERT INTO calibration_trials
               (hypothesis_id, change_description, before_value, after_value,
                trial_start, trial_end, win_rate_before, verdict)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'in_trial')""",
            (row[0], f"Change {param} from {current_val} to {value}",
             current_val, value, now, trial_end, wr_before),
        )

        conn.execute(
            """INSERT INTO calibration_history
               (timestamp, tier, parameter, before_value, after_value,
                reasoning, supporting_trades, confidence, status)
               VALUES (?, 3, ?, ?, ?, ?, ?, ?, 'trial')""",
            (now, param, current_val, value,
             hypothesis.get("recommended_change", ""),
             hypothesis.get("min_trades_to_validate", 5),
             confidence),
        )

        applied.append({"parameter": param, "before": current_val, "after": value})
        print(f"  Self-cal T3: TRIAL started — {param}: {current_val} -> {value} (24hr trial)")

    # Check expired trials
    expired = conn.execute("""
        SELECT id, hypothesis_id, change_description, before_value, after_value,
               win_rate_before, trial_start
        FROM calibration_trials
        WHERE verdict = 'in_trial' AND trial_end < datetime('now')
    """).fetchall()

    for trial in expired:
        # Get win rate during trial
        rconn = sqlite3.connect(RESOLUTIONS_DB) if os.path.exists(RESOLUTIONS_DB) else None
        wr_during = 0
        if rconn:
            try:
                total = rconn.execute(
                    "SELECT COUNT(*) FROM resolved_trades WHERE resolved_at > ?",
                    (trial[6],)
                ).fetchone()[0]
                wins = rconn.execute(
                    "SELECT COUNT(*) FROM resolved_trades WHERE resolved_at > ? AND pnl > 0",
                    (trial[6],)
                ).fetchone()[0]
                wr_during = wins / total if total > 0 else 0
            except Exception:
                pass
            finally:
                rconn.close()

        wr_before = trial[5] or 0
        improvement = wr_during - wr_before

        if improvement > 0.03:
            verdict = "promoted"
            print(f"  Self-cal T3: PROMOTED — {trial[2]} (WR: {wr_before:.0%} -> {wr_during:.0%})")
        else:
            verdict = "reverted"
            print(f"  Self-cal T3: REVERTED — {trial[2]} (no improvement: {wr_before:.0%} -> {wr_during:.0%})")

        conn.execute(
            "UPDATE calibration_trials SET win_rate_during = ?, verdict = ? WHERE id = ?",
            (wr_during, verdict, trial[0]),
        )
        conn.execute(
            "UPDATE calibration_history SET status = ? WHERE id = ?",
            (verdict, trial[1]),
        )

    conn.commit()
    conn.close()
    return applied


# ── TIER 3.5: Deep Pattern Review (every 200 resolutions) ────

def tier35_deep_pattern_review():
    """Deep analysis across all resolved trades using local model.

    Looks at category-level performance trends, systematic biases,
    and whether specific market types are consistently unprofitable.
    """
    if not os.path.exists(RESOLUTIONS_DB):
        return

    conn = sqlite3.connect(RESOLUTIONS_DB)
    conn.row_factory = sqlite3.Row

    # Category breakdown across all resolutions
    rows = conn.execute("""
        SELECT category,
               COUNT(*) as total,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
               ROUND(SUM(pnl), 2) as total_pnl,
               ROUND(AVG(our_confidence), 3) as avg_conf
        FROM resolved_trades
        GROUP BY category
        ORDER BY total DESC
    """).fetchall()
    conn.close()

    if not rows:
        return

    summary_lines = []
    unprofitable = []
    for r in rows:
        cat = r["category"] or "unknown"
        wr = r["wins"] / r["total"] if r["total"] > 0 else 0
        summary_lines.append(
            f"  {cat}: {r['total']} trades, {wr:.0%} WR, ${r['total_pnl']} PnL, avg_conf={r['avg_conf']}"
        )
        if r["total"] >= 10 and wr < 0.40:
            unprofitable.append(cat)

    prompt = f"""You are reviewing the complete trading history of a prediction market bot.

Category performance (all time):
{chr(10).join(summary_lines)}

Unprofitable categories (>10 trades, <40% WR): {unprofitable if unprofitable else 'none'}

Analyze the overall system health. Which categories should be:
1. Expanded (consistently profitable)?
2. Tightened (profitable but noisy)?
3. Suspended (consistently unprofitable)?

Answer in JSON format:
{{
  "expand": ["<category names>"],
  "tighten": ["<category names>"],
  "suspend": ["<category names>"],
  "reasoning": "<brief explanation>",
  "confidence": "<low/medium/high>"
}}"""

    result = _query_local_model(prompt)
    observation = json.dumps(result)[:500] if result else "model_unavailable"

    print(f"  Self-cal T3.5: Deep review — {len(rows)} categories analyzed")
    if unprofitable:
        print(f"  Self-cal T3.5: Unprofitable categories: {unprofitable}")

    dconn = sqlite3.connect(DECISIONS_DB)
    _init_tables(dconn)
    dconn.execute(
        "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 3.5, ?, ?, ?)",
        (datetime.now(timezone.utc).isoformat(), "deep_pattern_review",
         observation, "deep_review_completed"),
    )
    dconn.commit()
    dconn.close()


# ── TIER 4: Benchmark Comparison (every 500 resolutions) ─────

def tier4_weekly_benchmark() -> dict:
    """Compare live win rates against training benchmarks.

    Alerts if any category drifts >15 points below benchmark.
    """
    if not ENABLED or not os.path.exists(RESOLUTIONS_DB):
        return {}

    # Load base rates
    base_rates = {}
    if os.path.exists(BASE_RATES_PATH):
        try:
            with open(BASE_RATES_PATH) as f:
                for line in f:
                    line = line.strip()
                    if ":" in line and not line.startswith("#"):
                        parts = line.split(":")
                        cat = parts[0].strip().lower()
                        rate = float(parts[1].strip().rstrip("%")) / 100
                        base_rates[cat] = rate
        except Exception:
            pass

    if not base_rates:
        # Default training benchmarks
        base_rates = {
            "weather": 0.89,
            "sp500": 0.50,
            "economics": 0.60,
            "inflation": 0.60,
        }

    # Get live win rates per category (last 7 days)
    conn = sqlite3.connect(RESOLUTIONS_DB)
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    rows = conn.execute("""
        SELECT category, COUNT(*) as total,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins
        FROM resolved_trades
        WHERE resolved_at > ?
        GROUP BY category
    """, (cutoff,)).fetchall()
    conn.close()

    alerts = []
    results = {}

    for row in rows:
        cat = (row["category"] or "").lower()
        total = row["total"]
        wins = row["wins"]
        live_wr = wins / total if total > 0 else 0
        benchmark = base_rates.get(cat)

        results[cat] = {
            "live_wr": live_wr,
            "benchmark": benchmark,
            "total": total,
            "wins": wins,
        }

        if benchmark and total >= 5:
            drift = benchmark - live_wr
            if drift > 0.15:
                alert = f"{cat}: live {live_wr:.0%} vs benchmark {benchmark:.0%} (drift: {drift:.0%})"
                alerts.append(alert)
                print(f"  Self-cal T4: DRIFT ALERT — {alert}")

    if alerts:
        try:
            import sys
            sys.path.insert(0, os.path.dirname(__file__))
            from telegram_alerts import system_alert
            system_alert(
                f"Weekly benchmark drift:\n" + "\n".join(alerts),
                "warning",
            )
        except Exception:
            pass
    elif results:
        print(f"  Self-cal T4: All categories within benchmark tolerance")

    # Log the benchmark run
    if os.path.exists(DECISIONS_DB):
        dconn = sqlite3.connect(DECISIONS_DB)
        _init_tables(dconn)
        dconn.execute(
            "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 4, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), "weekly_benchmark",
             json.dumps(results, default=str)[:500],
             f"{len(alerts)} drift alerts" if alerts else "all within tolerance"),
        )
        dconn.commit()
        dconn.close()

    return {"results": results, "alerts": alerts}


# ── TIER 5: Full System Review (every 2000 resolutions) ──────

def tier5_full_system_review():
    """Comprehensive system review at major milestones.

    Evaluates overall system ROI, category allocation efficiency,
    and whether the system should scale up, down, or restructure.
    """
    if not os.path.exists(RESOLUTIONS_DB):
        return

    conn = sqlite3.connect(RESOLUTIONS_DB)
    conn.row_factory = sqlite3.Row

    total = conn.execute("SELECT COUNT(*) FROM resolved_trades").fetchone()[0]
    wins = conn.execute("SELECT COUNT(*) FROM resolved_trades WHERE pnl > 0").fetchone()[0]
    total_pnl = conn.execute("SELECT COALESCE(SUM(pnl), 0) FROM resolved_trades").fetchone()[0]

    # Per-category stats
    cats = conn.execute("""
        SELECT category,
               COUNT(*) as n,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as w,
               ROUND(SUM(pnl), 2) as pnl
        FROM resolved_trades GROUP BY category
    """).fetchall()
    conn.close()

    wr = wins / total if total > 0 else 0
    observation = (
        f"MILESTONE: {total} resolutions, {wr:.1%} WR, ${total_pnl:.2f} PnL. "
        f"Categories: {', '.join(str(r['category']) + '=' + str(r['n']) + 't/' + str(r['w']) + 'w/$' + str(r['pnl']) for r in cats)}"
    )

    print(f"  Self-cal T5: FULL SYSTEM REVIEW — {total} trades, {wr:.1%} WR, ${total_pnl:.2f} PnL")

    dconn = sqlite3.connect(DECISIONS_DB)
    _init_tables(dconn)
    dconn.execute(
        "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 5, ?, ?, ?)",
        (datetime.now(timezone.utc).isoformat(), f"system_review_{total}",
         observation[:500], "full_review_completed"),
    )
    dconn.commit()
    dconn.close()

    # Telegram alert for milestone
    try:
        from telegram_alerts import system_alert
        system_alert(f"System milestone: {total} resolutions\n{wr:.1%} WR, ${total_pnl:.2f} PnL", "info")
    except Exception:
        pass


# ── Tier Handler Registry ─────────────────────────────────────

_TIER_HANDLERS.update({
    "0":   tier0_sanity_check,
    "1.5": tier1_market_movement,
    "2":   tier2_pattern_analyzer,
    "2.5": tier25_confidence_recalibration,
    "3":   tier3_daily_executor,
    "3.5": tier35_deep_pattern_review,
    "4":   tier4_weekly_benchmark,
    "5":   tier5_full_system_review,
})


# ── Runner (legacy compat — kept for dual_strategy.py calls) ──

def run_tier1():
    """Legacy: run tier 1.5 market movement."""
    if not ENABLED:
        return
    try:
        tier1_market_movement()
    except Exception as e:
        print(f"  Self-cal T1.5 error: {e}")


def run_tier2():
    """Legacy: run tier 2 pattern analyzer."""
    if not ENABLED:
        return
    try:
        tier2_pattern_analyzer()
    except Exception as e:
        print(f"  Self-cal T2 error: {e}")


def run_tier3():
    """Legacy: run tier 3 daily executor."""
    if not ENABLED:
        return
    try:
        tier3_daily_executor()
    except Exception as e:
        print(f"  Self-cal T3 error: {e}")


def run_tier4():
    """Legacy: run tier 4 weekly benchmark."""
    if not ENABLED:
        return
    try:
        tier4_weekly_benchmark()
    except Exception as e:
        print(f"  Self-cal T4 error: {e}")


if __name__ == "__main__":
    print("=== Self-Calibration Engine ===")
    print(f"Enabled: {ENABLED}")
    print(f"Local model: {LOCAL_MODEL}")
    print(f"Min trades (category): {MIN_TRADES}")
    print(f"Min trades (city): {MIN_TRADES_CITY}")
    print(f"Trial period: {TRIAL_HOURS}hr")
    print(f"Max threshold change: {MAX_THRESHOLD_CHANGE}")
    print(f"Tier thresholds: {TIER_THRESHOLDS}")
    print(f"Fallback hours: {FALLBACK_HOURS}")
    print()

    print("--- Counter Status ---")
    if os.path.exists(DECISIONS_DB):
        conn = sqlite3.connect(DECISIONS_DB)
        _init_tables(conn)
        rows = conn.execute("SELECT tier, last_fired_at_count, current_count, threshold, last_fired_at FROM calibration_counters ORDER BY CAST(tier AS REAL)").fetchall()
        for r in rows:
            delta = r[2] - r[1]
            print(f"  Tier {r[0]:>3}: count={r[2]}, last_fired_at={r[1]}, threshold={r[3]}, delta={delta}, last={r[4][:16] if r[4] else 'never'}")
        conn.close()

    print("\n--- Tier 1.5: Market Movement ---")
    flags = tier1_market_movement()
    print(f"Flags: {len(flags)}")

    print("\n--- Tier 2: Pattern Analyzer ---")
    hypotheses = tier2_pattern_analyzer()
    print(f"Hypotheses: {len(hypotheses)}")

    print("\n--- Tier 2.5: Confidence Recalibration ---")
    tier25_confidence_recalibration()

    print("\n--- Tier 4: Weekly Benchmark ---")
    benchmark = tier4_weekly_benchmark()
    print(f"Alerts: {len(benchmark.get('alerts', []))}")
