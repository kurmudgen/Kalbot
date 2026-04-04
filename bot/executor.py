"""
Trade executor: reads analyst scores, applies safety checks, places trades.
Supports PAPER_TRADE mode (logs only) and LIVE mode (actual API calls).
Uses Kelly criterion for position sizing — bet more when edge is larger.
Prioritizes close-to-expiry markets and weather category.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

ANALYST_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "analyst_scores.sqlite")
DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")

CONFIDENCE_MIN = float(os.getenv("CONFIDENCE_MIN", "0.75"))
PRICE_GAP_MIN = float(os.getenv("PRICE_GAP_MIN", "0.08"))

# City-specific minimum NWS gap (Fahrenheit) for weather trades
_city_gap_raw = os.getenv("CITY_MIN_NWS_GAP", '{"default": 3}')
try:
    CITY_MIN_NWS_GAP = json.loads(_city_gap_raw)
except Exception:
    CITY_MIN_NWS_GAP = {"default": 3}

# Suspended cities — skip all weather trades for these cities pending calibration
_suspended_raw = os.getenv("SUSPENDED_CITIES", "")
SUSPENDED_CITIES = [c.strip().lower() for c in _suspended_raw.split(",") if c.strip()]

# City-specific minimum confidence (override category default for harder-to-predict cities)
_city_conf_raw = os.getenv("CITY_MIN_CONFIDENCE", '{}')
try:
    CITY_MIN_CONFIDENCE = json.loads(_city_conf_raw)
except Exception:
    CITY_MIN_CONFIDENCE = {}

# Category-specific confidence thresholds (lower = more aggressive)
CATEGORY_CONFIDENCE = {
    "weather": float(os.getenv("WEATHER_CONFIDENCE", "0.70")),
    "economics": float(os.getenv("ECON_CONFIDENCE", "0.80")),
    "inflation": float(os.getenv("INFLATION_CONFIDENCE", "0.80")),
    "tsa": float(os.getenv("TSA_CONFIDENCE", "0.85")),
    "congressional": float(os.getenv("CONGRESSIONAL_CONFIDENCE", "0.80")),
    "energy": float(os.getenv("ENERGY_CONFIDENCE", "0.80")),
    "entertainment": float(os.getenv("ENTERTAINMENT_CONFIDENCE", "0.85")),
}

# Kelly fraction — use fractional Kelly to reduce variance
KELLY_FRACTION = 0.25  # Quarter-Kelly: conservative but still edge-proportional

# Dynamic capital management
PAPER_STARTING_BALANCE = float(os.getenv("PAPER_STARTING_BALANCE", "350"))
ACCOUNT_FLOOR_PCT = float(os.getenv("ACCOUNT_FLOOR_PCT", "0.30"))
DRAWDOWN_HALT_PCT = float(os.getenv("DRAWDOWN_HALT_PCT", "0.25"))
MAX_SINGLE_TRADE_PCT = float(os.getenv("MAX_SINGLE_TRADE_PCT", "0.20"))
MIN_POSITION_SIZE = float(os.getenv("MIN_POSITION_SIZE", "15"))

# Category-specific liquidity caps
MAX_POS_WEATHER = float(os.getenv("MAX_POSITION_SIZE_WEATHER", "150"))
MAX_POS_TSA = float(os.getenv("MAX_POSITION_SIZE_TSA", "200"))
MAX_POS_CPI = float(os.getenv("MAX_POSITION_SIZE_CPI", "400"))
CATEGORY_POS_CAPS = {
    "weather": MAX_POS_WEATHER,
    "tsa": MAX_POS_TSA,
    "inflation": MAX_POS_CPI,
    "economics": MAX_POS_CPI,
}

# Performance score (10-100, starts at 50)
PERF_SCORE_FLOOR = 10
PERF_SCORE_CEILING = 100
PERF_SCORE_START = 50
PERF_HIGH_CONF_WIN = 3   # +3 for win with conf >= 0.85
PERF_STANDARD_WIN = 2    # +2 for standard win
PERF_LOSS_PENALTY = 5    # -5 for any loss

RESOLUTIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "resolutions.sqlite")

# Load historical market bias data
BIAS_PATH = os.path.join(os.path.dirname(__file__), "..", "calibration", "kalshi_market_bias.json")
MARKET_BIAS = {}
if os.path.exists(BIAS_PATH):
    with open(BIAS_PATH) as f:
        MARKET_BIAS = json.load(f).get("bins", {})


import re

# Known weather cities for extraction from titles
_WEATHER_CITIES = ["austin", "chicago", "denver", "houston", "miami", "nyc",
                   "new york", "los angeles", "phoenix", "seattle", "philadelphia"]


def extract_weather_city(title: str) -> str | None:
    """Extract the primary city from a weather market title."""
    title_lower = title.lower()
    for city in _WEATHER_CITIES:
        if city in title_lower:
            # Normalize "new york" -> "nyc" for config lookup
            return "nyc" if city == "new york" else city
    return None


def extract_threshold_temp(title: str) -> float | None:
    """Extract the temperature threshold from a weather market title.
    Handles formats like '90-91°', '>84°', '<77°', '83-84°', '<40�'."""
    # Match patterns: 90-91, >84, <77, etc. (before °, F, or mangled degree symbol)
    m = re.search(r'[<>]?(\d+)(?:-(\d+))?[°F�\ufffd]', title)
    if not m:
        # Fallback: try matching bare number after < or > in weather context
        m = re.search(r'[<>](\d+)', title)
        if not m:
            return None
        return float(m.group(1))
    lo = int(m.group(1))
    hi = int(m.group(2)) if m.group(2) else lo
    return (lo + hi) / 2.0


def check_city_nws_gap(title: str) -> tuple[bool, str | None, str | None]:
    """Check if a weather trade meets the city-specific NWS gap minimum.
    Returns (passed, skip_reason, city_used)."""
    city = extract_weather_city(title)
    if not city:
        return True, None, None

    threshold = extract_threshold_temp(title)
    if threshold is None:
        return True, None, city

    # Load NWS forecast for this city
    try:
        import sys as _sys
        data_path = os.path.join(os.path.dirname(__file__), "..", "data")
        if data_path not in _sys.path:
            _sys.path.insert(0, data_path)
        from weather_nws_feed import load_forecasts
        forecasts = load_forecasts()
    except Exception:
        return True, None, city  # Can't check, allow through

    # Find matching forecast
    nws_temp = None
    for city_key, data in forecasts.items():
        city_name = data.get("city", "").lower()
        if city == city_key or city == city_name or (city == "nyc" and "new york" in city_name):
            nws_temp = data.get("high_temp")
            break

    if nws_temp is None:
        return True, None, city

    gap = abs(nws_temp - threshold)
    min_gap = CITY_MIN_NWS_GAP.get(city, CITY_MIN_NWS_GAP.get("default", 3))

    if gap < min_gap:
        reason = (f"city_nws_gap: {city} gap={gap:.1f}F < min={min_gap}F "
                  f"(NWS={nws_temp}F, threshold={threshold:.0f}F)")
        return False, reason, city

    return True, None, city


def get_historical_bias(market_price_cents: int) -> float:
    """Get historical calibration bias for a given price level.
    Returns the bias in percentage points (positive = YES underpriced)."""
    for bin_key, data in MARKET_BIAS.items():
        lo, hi = bin_key.split("-")
        if int(lo) <= market_price_cents <= int(hi):
            return data.get("bias_pct", 0) / 100.0
    return 0.0


def get_config() -> dict:
    return {
        "paper_trade": os.getenv("PAPER_TRADE", "true").lower() == "true",
    }


def _init_capital_tracker(conn: sqlite3.Connection):
    """Create capital_tracker table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS capital_tracker (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            balance REAL,
            peak_balance REAL,
            floor_balance REAL,
            available_capital REAL,
            performance_score REAL DEFAULT 50,
            score_multiplier REAL DEFAULT 1.0,
            daily_deployed REAL DEFAULT 0,
            daily_cap REAL,
            cycle_status TEXT
        )
    """)
    conn.commit()


def get_live_kalshi_balance() -> float | None:
    """Get current Kalshi account balance via API. Returns dollars."""
    try:
        from pykalshi import KalshiClient
        pk_path = os.getenv("KALSHI_PRIVATE_KEY", "")
        if not os.path.isabs(pk_path):
            pk_path = os.path.join(os.path.dirname(__file__), "..", pk_path)
        client = KalshiClient(
            api_key_id=os.getenv("KALSHI_API_KEY", ""),
            private_key_path=pk_path,
        )
        bal = client.portfolio.get_balance()
        client.close()
        # bal.balance is in cents
        dollars = bal.balance / 100.0
        print(f"  Capital: Kalshi balance = ${dollars:.2f} (portfolio_value=${bal.portfolio_value / 100.0:.2f})")
        return dollars
    except Exception as e:
        print(f"  Capital: Kalshi balance API error: {type(e).__name__}: {e}")
        return None


def get_paper_balance() -> float:
    """Simulate balance for paper trading by summing resolved P&L."""
    balance = PAPER_STARTING_BALANCE
    if os.path.exists(RESOLUTIONS_DB):
        try:
            conn = sqlite3.connect(RESOLUTIONS_DB)
            pnl = conn.execute("SELECT COALESCE(SUM(pnl), 0) FROM resolved_trades").fetchone()[0]
            conn.close()
            balance += pnl
        except Exception:
            pass
    return balance


def get_capital_state(conn: sqlite3.Connection, is_paper: bool) -> dict:
    """Compute current capital state for this cycle.

    Returns dict with: balance, peak_balance, floor_balance, available_capital,
    performance_score, score_multiplier, daily_deployed, daily_cap, halt_reason
    """
    _init_capital_tracker(conn)

    # Get current balance
    if is_paper:
        balance = get_paper_balance()
    else:
        balance = get_live_kalshi_balance()
        if balance is None:
            # Do NOT fall back to paper balance in live mode — wrong numbers
            # Use last known live balance from tracker instead
            last = conn.execute(
                "SELECT balance FROM capital_tracker WHERE cycle_status != 'api_fallback' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if last:
                balance = last[0]
                print(f"  Capital: API failed, using last known balance ${balance:.2f}")
            else:
                balance = float(os.getenv("PAPER_STARTING_BALANCE", "50"))
                print(f"  Capital: API failed, no history, using starting balance ${balance:.2f}")

    # Get previous peak from tracker
    row = conn.execute(
        "SELECT peak_balance, performance_score, daily_deployed, cycle_status FROM capital_tracker ORDER BY id DESC LIMIT 1"
    ).fetchone()

    if row:
        prev_peak = row[0]
        performance_score = row[1] or PERF_SCORE_START

        # Auto-reset peak on paper-to-live transition: if previous peak is from
        # paper trading (much higher than live balance), reset to current balance
        if not is_paper and prev_peak > balance * 2:
            print(f"  Capital: peak reset {prev_peak:.2f} -> {balance:.2f} (paper-to-live transition)")
            prev_peak = balance
            performance_score = PERF_SCORE_START  # Reset score too

        peak_balance = max(prev_peak, balance)

        # Reset daily deployed if new day
        last_row = conn.execute(
            "SELECT timestamp FROM capital_tracker ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if last_row and last_row[0][:10] != datetime.now(timezone.utc).isoformat()[:10]:
            daily_deployed = 0
        else:
            daily_deployed = row[2] or 0
    else:
        peak_balance = balance
        performance_score = PERF_SCORE_START
        daily_deployed = 0

    # Floor protection — floor_pct of peak is protected (untouchable)
    # Lower floor % for small accounts so more capital is available
    floor_pct = 0.20 if balance < 200 else ACCOUNT_FLOOR_PCT
    floor_balance = peak_balance * floor_pct
    available_capital = max(0, balance - floor_balance)

    # Performance score multiplier: score/50 (1.0 at 50, 2.0 at 100)
    score_multiplier = performance_score / 50.0

    # Daily deployment cap: (score/100) * 1.2 * available_capital
    daily_cap = (performance_score / 100.0) * 1.2 * available_capital

    # Check halt conditions
    halt_reason = None
    if available_capital <= 0:
        halt_reason = "floor_protection_halt"
    elif balance < peak_balance * (1 - DRAWDOWN_HALT_PCT):
        halt_reason = "catastrophic_drawdown_halt"

    state = {
        "balance": round(balance, 2),
        "peak_balance": round(peak_balance, 2),
        "floor_balance": round(floor_balance, 2),
        "available_capital": round(available_capital, 2),
        "performance_score": round(performance_score, 1),
        "score_multiplier": round(score_multiplier, 2),
        "daily_deployed": round(daily_deployed, 2),
        "daily_cap": round(daily_cap, 2),
        "halt_reason": halt_reason,
    }

    # Log this cycle's capital state
    conn.execute(
        """INSERT INTO capital_tracker
           (timestamp, balance, peak_balance, floor_balance, available_capital,
            performance_score, score_multiplier, daily_deployed, daily_cap, cycle_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (datetime.now(timezone.utc).isoformat(),
         state["balance"], state["peak_balance"], state["floor_balance"],
         state["available_capital"], state["performance_score"],
         state["score_multiplier"], state["daily_deployed"], state["daily_cap"],
         halt_reason or "active"),
    )
    conn.commit()

    return state


def update_performance_score(conn: sqlite3.Connection, won: bool, confidence: float):
    """Update performance score after a resolution."""
    _init_capital_tracker(conn)
    row = conn.execute(
        "SELECT performance_score FROM capital_tracker ORDER BY id DESC LIMIT 1"
    ).fetchone()
    score = row[0] if row else PERF_SCORE_START

    if won:
        delta = PERF_HIGH_CONF_WIN if confidence >= 0.85 else PERF_STANDARD_WIN
    else:
        delta = -PERF_LOSS_PENALTY

    score = max(PERF_SCORE_FLOOR, min(PERF_SCORE_CEILING, score + delta))

    # Update the most recent tracker row
    conn.execute(
        "UPDATE capital_tracker SET performance_score = ? WHERE id = (SELECT MAX(id) FROM capital_tracker)",
        (score,),
    )
    conn.commit()
    return score


def init_decisions_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DECISIONS_DB), exist_ok=True)
    conn = sqlite3.connect(DECISIONS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            title TEXT,
            category TEXT,
            cloud_probability REAL,
            cloud_confidence REAL,
            market_price REAL,
            price_gap REAL,
            side TEXT,
            amount REAL,
            reasoning TEXT,
            mode TEXT,
            executed INTEGER,
            error TEXT,
            decided_at TEXT,
            session_id TEXT
        )
    """)
    conn.commit()
    return conn


def get_open_positions(conn: sqlite3.Connection) -> set[str]:
    """Get tickers with open (unresolved) positions only.

    Excludes resolved trades and early exits. Only blocks re-entry
    on markets that are actually still open.
    """
    # Get all executed tickers (non-exit)
    all_executed = conn.execute(
        "SELECT DISTINCT ticker FROM decisions WHERE executed = 1 AND side NOT LIKE '%EXIT%'"
    ).fetchall()
    all_tickers = {r[0] for r in all_executed}

    # Remove resolved tickers
    resolved_tickers = set()
    if os.path.exists(RESOLUTIONS_DB):
        try:
            rconn = sqlite3.connect(RESOLUTIONS_DB)
            rows = rconn.execute("SELECT DISTINCT ticker FROM resolved_trades").fetchall()
            resolved_tickers = {r[0] for r in rows}
            rconn.close()
        except Exception:
            pass

    # Remove exited tickers (take-profit / stop-loss)
    exited = conn.execute(
        "SELECT DISTINCT ticker FROM decisions WHERE executed = 1 AND side LIKE '%EXIT%'"
    ).fetchall()
    exited_tickers = set()
    for r in exited:
        # Extract base ticker from exit entries (they use the same ticker)
        exited_tickers.add(r[0])

    open_positions = all_tickers - resolved_tickers - exited_tickers
    return open_positions


def get_analyst_scores() -> list[dict]:
    if not os.path.exists(ANALYST_DB):
        return []
    conn = sqlite3.connect(ANALYST_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM analyst_scores").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def execute_trades(scores: list[dict] | None = None, session_id: str = "") -> list[dict]:
    # Kill switch check
    try:
        from kill_switch import should_trade
        ok, reason = should_trade()
        if not ok:
            print(f"  HALTED: {reason}")
            return []
    except Exception:
        pass

    if scores is None:
        scores = get_analyst_scores()

    config = get_config()
    mode = "PAPER" if config["paper_trade"] else "LIVE"
    conn = init_decisions_db()

    if not session_id:
        session_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Dynamic capital state — replaces fixed MAX_NIGHTLY_SPEND
    capital = get_capital_state(conn, config["paper_trade"])

    # Check for halt conditions
    if capital["halt_reason"]:
        print(f"  CAPITAL HALT: {capital['halt_reason']} — balance=${capital['balance']}, floor=${capital['floor_balance']}")
        if capital["halt_reason"] == "catastrophic_drawdown_halt":
            try:
                from telegram_alerts import system_alert
                system_alert(
                    f"CATASTROPHIC DRAWDOWN HALT\n"
                    f"Balance: ${capital['balance']}\n"
                    f"Peak: ${capital['peak_balance']}\n"
                    f"Drawdown: {(1 - capital['balance']/capital['peak_balance'])*100:.1f}%\n"
                    f"Manual restart required.",
                    "critical",
                )
            except Exception:
                pass
        conn.close()
        return []

    open_positions = get_open_positions(conn)
    print(f"  Open positions blocking dedup: {len(open_positions)}")
    if open_positions and len(open_positions) <= 10:
        for t in open_positions:
            print(f"    {t}")

    # Track events traded in THIS batch to prevent bracket flooding
    _traded_events_this_batch = set()
    try:
        from bracket_guard import extract_event_key
        existing = conn.execute(
            "SELECT ticker, title FROM decisions WHERE executed = 1 AND session_id = ?",
            (session_id,),
        ).fetchall()
        for row in existing:
            _traded_events_this_batch.add(extract_event_key(row[0], row[1] or ""))
    except Exception:
        pass

    trades = []
    effective_capital = capital["available_capital"] * capital["score_multiplier"]
    daily_remaining = capital["daily_cap"] - capital["daily_deployed"]
    print(f"Executor mode: {mode}")
    print(f"Capital: ${capital['balance']} (peak=${capital['peak_balance']}, floor=${capital['floor_balance']})")
    print(f"Available: ${capital['available_capital']} x{capital['score_multiplier']:.1f} = ${effective_capital:.2f} effective")
    print(f"Score: {capital['performance_score']}, Daily: ${capital['daily_deployed']:.2f}/${capital['daily_cap']:.2f}")
    print(f"Evaluating {len(scores)} scored markets...\n")

    for score in scores:
        ticker = score["ticker"]
        title = score["title"]
        category = score["category"]
        cloud_prob = score.get("cloud_probability", score.get("model_probability", 0.5))
        cloud_conf = score.get("cloud_confidence", score.get("confidence", 0.5))
        market_price = score.get("market_price", 0.5)
        price_gap = score.get("price_gap", 0)
        reasoning = score.get("cloud_reasoning", score.get("reasoning", ""))

        # NUCLEAR GUARD: never execute on null/empty signals
        if cloud_prob == 0.0 or cloud_conf == 0.0 or market_price == 0.0:
            continue  # Silently skip — these are broken signals

        skip_reason = None

        # Order book depth check — skip thin markets
        try:
            from orderbook_analyzer import is_safe_to_trade
            if not is_safe_to_trade(ticker):
                skip_reason = "thin order book (low liquidity)"
        except Exception:
            pass

        # Seasonal adjustment for weather markets
        if category == "weather" and not skip_reason:
            try:
                from seasonal_adjuster import get_seasonal_multiplier
                seasonal_mult = get_seasonal_multiplier(title)
                cloud_conf = cloud_conf * seasonal_mult
            except Exception:
                pass

        # Extreme gap override for weather — obvious calls where model undershoots confidence
        # 30F gap with p=0.05 is near-certain, low confidence is model hedging not genuine uncertainty
        extreme_gap_applied = False
        if category == "weather" and not skip_reason:
            try:
                _city = extract_weather_city(title)
                _threshold = extract_threshold_temp(title)
                if _city and _threshold:
                    from weather_nws_feed import load_forecasts
                    _forecasts = load_forecasts()
                    _nws_temp = None
                    for _ck, _cd in _forecasts.items():
                        _cn = _cd.get("city", "").lower()
                        if _city == _ck or _city == _cn or (_city == "nyc" and "new york" in _cn):
                            _nws_temp = _cd.get("high_temp")
                            break
                    if _nws_temp is not None:
                        _gap = abs(_nws_temp - _threshold)
                        if _gap >= 15 and (cloud_prob < 0.15 or cloud_prob > 0.85):
                            cloud_conf = 0.90
                            extreme_gap_applied = True
                            print(f"  [extreme gap] {_city} gap={_gap:.0f}F p={cloud_prob:.2f} -> conf overridden to 0.90")
            except Exception:
                pass

        # Hard block — disabled tickers that must never trade
        BLOCKED_TICKERS = ["KXEURUSD", "KXUSDJPY", "KXINX", "KXGDP"]  # forex, S&P, GDP disabled
        if not skip_reason:
            for blocked in BLOCKED_TICKERS:
                if ticker.startswith(blocked):
                    skip_reason = f"blocked ticker: {blocked} disabled (no simulation validation)"
                    break

        # Category whitelist — only execute on simulation-validated categories
        # CPI maps to both "inflation" and "economics" in the scanner, so allow both
        VALIDATED_CATEGORIES = {"weather", "tsa", "inflation"}
        # Also allow economics only for CPI tickers (KXCPI/KXPCE), block all other economics
        if not skip_reason:
            if category in VALIDATED_CATEGORIES:
                pass  # Allowed
            elif category == "economics" and any(ticker.startswith(p) for p in ["KXCPI", "KXPCE"]):
                pass  # CPI under economics category — allowed
            else:
                skip_reason = f"unvalidated category: {category} (only weather/tsa/cpi have simulation backing)"

        # Bracket deduplication — one trade per underlying event
        if not skip_reason:
            try:
                from bracket_guard import extract_event_key, already_traded_event
                event_key = extract_event_key(ticker, title)
                # Check against both DB history AND current batch
                if event_key in _traded_events_this_batch:
                    skip_reason = "bracket dedup (same batch)"
                elif already_traded_event(ticker, title, session_id):
                    skip_reason = "bracket dedup (prior session)"
            except Exception:
                pass

        # Confidence floor — ensemble average must be above 0.65
        if not skip_reason and cloud_conf < 0.65:
            skip_reason = f"confidence {cloud_conf:.2f} below ensemble floor (0.65)"

        # Check historical bias — boost confidence when model agrees with history
        price_cents = int(market_price * 100)
        hist_bias = get_historical_bias(price_cents)
        # hist_bias > 0 means YES is historically underpriced (buy YES)
        # hist_bias < 0 means YES is historically overpriced (buy NO)

        # Determine side and Kelly-sized bet
        side = "YES" if cloud_prob > market_price else "NO"

        # Boost confidence if historical bias agrees with our direction
        bias_aligned = (side == "YES" and hist_bias > 0) or (side == "NO" and hist_bias < 0)
        if bias_aligned:
            cloud_conf = min(1.0, cloud_conf * 1.1)  # 10% confidence boost

        # Kelly criterion: f* = (bp - q) / b
        # where b = odds, p = our probability, q = 1-p
        if side == "YES":
            cost = market_price  # Cost to buy YES
            payout = 1.0        # Pays $1 if YES
            our_prob = cloud_prob
        else:
            cost = 1.0 - market_price  # Cost to buy NO
            payout = 1.0
            our_prob = 1.0 - cloud_prob

        b = (payout / cost) - 1  # Net odds
        q = 1.0 - our_prob
        kelly_raw = (b * our_prob - q) / b if b > 0 else 0
        kelly_bet = max(0, kelly_raw * KELLY_FRACTION)

        # Regime adjustment — scale Kelly by market state
        try:
            from regime_detector import detect_regime
            regime = detect_regime()
            kelly_bet *= regime.get("kelly_multiplier", 1.0)
        except Exception:
            pass

        # Dynamic position sizing against effective available capital
        # Position size multiplier scales with performance score (score/50)
        pos_multiplier = capital["score_multiplier"]
        max_single = effective_capital * MAX_SINGLE_TRADE_PCT * pos_multiplier
        category_cap = CATEGORY_POS_CAPS.get(category, MAX_POS_WEATHER)

        amount = min(
            kelly_bet * effective_capital,   # Quarter-Kelly against effective capital
            max_single,                       # % of effective capital per trade
            category_cap,                     # Category liquidity cap
            daily_remaining,                  # Daily deployment cap remaining
        )
        amount = round(max(0, amount), 2)

        # Enforce minimum position size — don't bother with $3 bets
        # Minimum position — lower for small accounts
        min_pos = 10 if capital["balance"] < 100 else MIN_POSITION_SIZE
        if 0 < amount < min_pos:
            amount = min_pos if effective_capital >= min_pos * 2 else 0

        # Category-specific confidence threshold
        cat_conf_min = CATEGORY_CONFIDENCE.get(category, CONFIDENCE_MIN)

        # Safety checks (use <= threshold - epsilon to avoid floating point rounding rejections)
        if not skip_reason and cloud_conf < cat_conf_min - 0.001:
            skip_reason = f"confidence {cloud_conf:.2f} < {cat_conf_min} ({category})"
        elif price_gap < PRICE_GAP_MIN:
            skip_reason = f"price gap {price_gap:.2f} < {PRICE_GAP_MIN}"
        elif ticker in open_positions:
            skip_reason = "already in open positions"

        # Gate 0.5: Suspended cities — skip entirely pending calibration
        city_used = None
        if not skip_reason and category == "weather":
            city_used = extract_weather_city(title)
            if city_used and city_used in SUSPENDED_CITIES:
                skip_reason = f"suspended_city: {city_used} pending calibration"
                print(f"  [city gate] SUSPENDED: {city_used} — {ticker}")

        # Gate 0.7: City-specific minimum confidence (Miami=0.85, etc.)
        if not skip_reason and category == "weather" and city_used:
            city_conf_min = CITY_MIN_CONFIDENCE.get(city_used)
            if city_conf_min and cloud_conf < city_conf_min - 0.001:
                skip_reason = f"city_confidence: {city_used} conf={cloud_conf:.2f} < {city_conf_min}"
                print(f"  [city gate] CONFIDENCE: {city_used} conf={cloud_conf:.2f} < {city_conf_min} — {ticker}")

        # Gate 1: Weather markets with borderline probability need NWS data
        if not skip_reason and category == "weather":
            if 0.20 < cloud_prob < 0.80:
                # Check if NWS forecast data is actually available for this city
                # (NWS data is injected into the prompt by local_filter, models don't always echo "NWS" in response)
                _nws_city = extract_weather_city(title)
                has_nws = False
                if _nws_city:
                    try:
                        import sys as _sys
                        _dp = os.path.join(os.path.dirname(__file__), "..", "data")
                        if _dp not in _sys.path:
                            _sys.path.insert(0, _dp)
                        from weather_nws_feed import load_forecasts
                        _fc = load_forecasts()
                        has_nws = any(
                            _nws_city == k or _nws_city in v.get("city", "").lower()
                            for k, v in _fc.items()
                        ) if _fc else False
                    except Exception:
                        has_nws = True  # Can't check — allow through
                else:
                    has_nws = True  # Can't identify city — allow through
                if not has_nws:
                    skip_reason = "nws_data_missing: no NWS forecast available for this city"

        # Gate 1.5: City-specific NWS gap minimum (Austin=6F, default=3F)
        if not skip_reason and category == "weather":
            passed, gap_reason, city_checked = check_city_nws_gap(title)
            if not passed:
                skip_reason = gap_reason
            if city_checked and not city_used:
                city_used = city_checked

        # Gate 2: Borderline probability EV trades need moderate confidence
        # Lowered from 0.85 to 0.70 — the 0.85 requirement after seasonal
        # adjustment (0.88x) required 0.97 raw confidence which is unreachable.
        # Other gates (NWS gap, city confidence, ensemble floor) provide protection.
        if not skip_reason:
            if 0.20 < cloud_prob < 0.80 and cloud_conf < 0.65 - 0.001:
                skip_reason = f"borderline EV trade: prob={cloud_prob:.2f} needs conf>0.65 (has {cloud_conf:.2f})"

        # Correlation check
        if not skip_reason:
            try:
                from correlation_guard import check_correlation
                corr = check_correlation(score)
                if not corr["allowed"]:
                    skip_reason = corr["reason"]
            except Exception:
                pass

        # Daily deployment cap check
        if not skip_reason and daily_remaining <= 0:
            skip_reason = f"daily deployment cap reached (${capital['daily_deployed']:.2f}/${capital['daily_cap']:.2f})"
        elif not skip_reason and amount <= 0:
            skip_reason = "no budget remaining (insufficient capital)"

        # Append city and extreme gap to reasoning for weather trades (audit trail)
        if category == "weather" and city_used:
            reasoning = f"[city={city_used}] {reasoning}"
        if extreme_gap_applied:
            reasoning = f"[extreme_gap_override] {reasoning}"

        if skip_reason:
            print(f"  SKIP {title[:50]}... — {skip_reason}")
            conn.execute(
                """INSERT INTO decisions
                   (ticker, title, category, cloud_probability, cloud_confidence,
                    market_price, price_gap, side, amount, reasoning,
                    mode, executed, error, decided_at, session_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)""",
                (ticker, title, category, cloud_prob, cloud_conf,
                 market_price, price_gap, side, 0, reasoning,
                 mode, skip_reason,
                 datetime.now(timezone.utc).isoformat(), session_id),
            )
            conn.commit()
            continue

        # Execute trade
        executed = False
        error = None

        if config["paper_trade"]:
            executed = True
            # Track this event in the batch to prevent bracket flooding
            try:
                from bracket_guard import extract_event_key
                _traded_events_this_batch.add(extract_event_key(ticker, title))
            except Exception:
                pass
            print(f"  PAPER TRADE: {side} ${amount:.2f} on {title[:50]}...")
            try:
                from telegram_alerts import trade_alert
                trade_alert(ticker, title, side, amount, cloud_conf, price_gap, "PAPER")
            except Exception:
                pass
            print(f"    conf={cloud_conf:.2f} gap={price_gap:.2f} price={market_price:.2f}")
        else:
            try:
                from pykalshi import KalshiClient, Side as KalshiSide, Action as KalshiAction

                pk_path = os.getenv("KALSHI_PRIVATE_KEY", "")
                if not os.path.isabs(pk_path):
                    pk_path = os.path.join(os.path.dirname(__file__), "..", pk_path)

                client = KalshiClient(
                    api_key_id=os.getenv("KALSHI_API_KEY", ""),
                    private_key_path=pk_path,
                )
                # Contracts cost market_price each (in dollars, 0-1 range)
                # For YES: cost per contract = market_price
                # For NO: cost per contract = 1 - market_price
                cost_per = market_price if side == "YES" else (1.0 - market_price)
                contracts = max(1, int(amount / cost_per)) if cost_per > 0 else 1
                order_side = KalshiSide.YES if side == "YES" else KalshiSide.NO
                # Use limit order at current price with slippage buffer
                # YES price = market_price, NO price = 1 - market_price
                # Add 2c slippage buffer for guaranteed fill
                if side == "YES":
                    limit_price = min(0.99, market_price + 0.02)
                    price_str = f"{limit_price:.2f}"
                    client.portfolio.place_order(
                        ticker=ticker,
                        action=KalshiAction.BUY,
                        side=order_side,
                        count_fp=f"{contracts}.00",
                        yes_price_dollars=price_str,
                        buy_max_cost_dollars=f"{amount:.2f}",
                    )
                else:
                    limit_price = min(0.99, (1.0 - market_price) + 0.02)
                    price_str = f"{limit_price:.2f}"
                    client.portfolio.place_order(
                        ticker=ticker,
                        action=KalshiAction.BUY,
                        side=order_side,
                        count_fp=f"{contracts}.00",
                        no_price_dollars=price_str,
                        buy_max_cost_dollars=f"{amount:.2f}",
                    )
                client.close()
                executed = True
                print(f"  LIVE TRADE: {side} ${amount:.2f} ({contracts} contracts @ ~${cost_per:.2f}/ea) on {title[:50]}...")
                try:
                    from telegram_alerts import trade_alert
                    trade_alert(ticker, title, side, amount, cloud_conf, price_gap, "LIVE")
                except Exception:
                    pass
            except Exception as e:
                error = str(e)
                print(f"  TRADE FAILED: {title[:50]}... — {error}")

        conn.execute(
            """INSERT INTO decisions
               (ticker, title, category, cloud_probability, cloud_confidence,
                market_price, price_gap, side, amount, reasoning,
                mode, executed, error, decided_at, session_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, title, category, cloud_prob, cloud_conf,
             market_price, price_gap, side, amount, reasoning,
             mode, int(executed), error,
             datetime.now(timezone.utc).isoformat(), session_id),
        )
        conn.commit()

        if executed:
            daily_remaining -= amount
            capital["daily_deployed"] += amount
            # Update daily_deployed in latest tracker row
            conn.execute(
                "UPDATE capital_tracker SET daily_deployed = ? WHERE id = (SELECT MAX(id) FROM capital_tracker)",
                (capital["daily_deployed"],),
            )
            conn.commit()
            trades.append({
                "ticker": ticker,
                "title": title,
                "side": side,
                "amount": amount,
                "mode": mode,
            })

    conn.close()
    print(f"\n{len(trades)} trades {'placed' if not config['paper_trade'] else 'logged (paper)'}.")
    print(f"Deployed this cycle: ${capital['daily_deployed']:.2f} / ${capital['daily_cap']:.2f} daily cap")
    print(f"Score: {capital['performance_score']} | Effective capital: ${effective_capital:.2f}")
    return trades


if __name__ == "__main__":
    execute_trades()
