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
# Full Kelly is too aggressive; half-Kelly is standard practice
KELLY_FRACTION = 0.25  # Quarter-Kelly: conservative but still edge-proportional

# Load historical market bias data
BIAS_PATH = os.path.join(os.path.dirname(__file__), "..", "calibration", "kalshi_market_bias.json")
MARKET_BIAS = {}
if os.path.exists(BIAS_PATH):
    with open(BIAS_PATH) as f:
        MARKET_BIAS = json.load(f).get("bins", {})


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
        "max_trade_size": float(os.getenv("MAX_TRADE_SIZE", "10")),
        "max_nightly_spend": float(os.getenv("MAX_NIGHTLY_SPEND", "50")),
    }


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


def get_tonight_spend(conn: sqlite3.Connection, session_id: str) -> float:
    row = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM decisions WHERE session_id = ? AND executed = 1",
        (session_id,),
    ).fetchone()
    return row[0] if row else 0.0


def get_open_positions(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT ticker FROM decisions WHERE executed = 1"
    ).fetchall()
    return {r[0] for r in rows}


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

    open_positions = get_open_positions(conn)
    tonight_spend = get_tonight_spend(conn, session_id)

    # Track events traded in THIS batch to prevent bracket flooding
    # Pre-populate from any trades already in DB for this session (crash recovery)
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
    print(f"Executor mode: {mode}")
    print(f"Tonight spend so far: ${tonight_spend:.2f} / ${config['max_nightly_spend']:.2f}")
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

        # Order book depth check — skip thin markets
        try:
            from orderbook_analyzer import is_safe_to_trade
            if not is_safe_to_trade(ticker):
                skip_reason = "thin order book (low liquidity)"
        except Exception:
            pass

        if skip_reason:
            pass  # Already have a reason to skip
        else:
            pass  # Continue to other checks

        # Seasonal adjustment for weather markets
        if category == "weather" and not skip_reason:
            try:
                from seasonal_adjuster import get_seasonal_multiplier
                seasonal_mult = get_seasonal_multiplier(title)
                cloud_conf = cloud_conf * seasonal_mult
            except Exception:
                pass

        # Hard block — disabled categories that must never trade
        BLOCKED_TICKERS = ["KXEURUSD", "KXUSDJPY", "KXINX"]  # forex + S&P disabled
        if not skip_reason:
            for blocked in BLOCKED_TICKERS:
                if ticker.startswith(blocked):
                    skip_reason = f"blocked category: {blocked} disabled"
                    break

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

        # Scale Kelly fraction to dollar amount, capped by max trade size
        budget_remaining = config["max_nightly_spend"] - tonight_spend
        amount = min(
            kelly_bet * config["max_nightly_spend"],  # Kelly-sized
            config["max_trade_size"],                  # Per-trade cap
            budget_remaining,                          # Budget remaining
        )
        amount = round(max(0, amount), 2)

        # Category-specific confidence threshold
        cat_conf_min = CATEGORY_CONFIDENCE.get(category, CONFIDENCE_MIN)

        # Safety checks (use <= threshold - epsilon to avoid floating point rounding rejections)
        skip_reason = None
        if cloud_conf < cat_conf_min - 0.001:
            skip_reason = f"confidence {cloud_conf:.2f} < {cat_conf_min} ({category})"
        elif price_gap < PRICE_GAP_MIN:
            skip_reason = f"price gap {price_gap:.2f} < {PRICE_GAP_MIN}"
        elif ticker in open_positions:
            skip_reason = "already in open positions"

        # Gate 1: Weather markets with borderline probability need NWS data
        if not skip_reason and category == "weather":
            if 0.20 < cloud_prob < 0.80:
                # Check if NWS data was available (injected via cloud_reasoning)
                reasoning = score.get("cloud_reasoning", "")
                has_nws = "NWS" in reasoning or "nws" in reasoning or "official forecast" in reasoning.lower()
                if not has_nws:
                    skip_reason = "nws_data_missing: borderline weather trade without settlement source"

        # Gate 2: Borderline probability EV trades need higher confidence
        if not skip_reason:
            if 0.20 < cloud_prob < 0.80 and cloud_conf < 0.85 - 0.001:
                # This is an EV trade on borderline probability — not an obvious call
                # Require higher confidence to filter noise
                skip_reason = f"borderline EV trade: prob={cloud_prob:.2f} needs conf>0.85 (has {cloud_conf:.2f})"

        # Correlation check
        if not skip_reason:
            try:
                from correlation_guard import check_correlation
                corr = check_correlation(score)
                if not corr["allowed"]:
                    skip_reason = corr["reason"]
            except Exception:
                pass
        elif tonight_spend >= config["max_nightly_spend"]:
            skip_reason = f"nightly spend limit reached (${tonight_spend:.2f})"
        elif amount <= 0:
            skip_reason = "no budget remaining"

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
                from pykalshi import KalshiClient, Side as KalshiSide, OrderType

                pk_path = os.getenv("KALSHI_PRIVATE_KEY", "")
                if not os.path.isabs(pk_path):
                    pk_path = os.path.join(os.path.dirname(__file__), "..", pk_path)

                client = KalshiClient(
                    api_key_id=os.getenv("KALSHI_API_KEY", ""),
                    private_key_path=pk_path,
                )
                contracts = max(1, int(amount / (market_price * 100)))
                order_side = KalshiSide.YES if side == "YES" else KalshiSide.NO
                client.portfolio.create_order(
                    ticker=ticker,
                    side=order_side,
                    type=OrderType.MARKET,
                    count=contracts,
                )
                client.close()
                executed = True
                print(f"  LIVE TRADE: {side} ${amount:.2f} ({contracts} contracts) on {title[:50]}...")
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
            tonight_spend += amount
            trades.append({
                "ticker": ticker,
                "title": title,
                "side": side,
                "amount": amount,
                "mode": mode,
            })

    conn.close()
    print(f"\n{len(trades)} trades {'placed' if not config['paper_trade'] else 'logged (paper)'}.")
    print(f"Total deployed tonight: ${tonight_spend:.2f}")
    return trades


if __name__ == "__main__":
    execute_trades()
