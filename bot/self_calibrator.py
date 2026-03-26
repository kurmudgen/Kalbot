"""
Self-calibration engine — 4-tier reflection loop.

Tier 1: Hourly market movement reflector (local 32b, no API cost)
Tier 2: 6-hour pattern analyzer (local 32b, structured JSON)
Tier 3: Daily adjustment executor (2am, 24hr trial + auto-revert)
Tier 4: Weekly benchmark (Sunday 3am, compare vs training base rates)

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
TRIAL_HOURS = int(os.getenv("SELF_CAL_TRIAL_HOURS", "24"))
MAX_THRESHOLD_CHANGE = float(os.getenv("SELF_CAL_MAX_THRESHOLD_CHANGE", "0.10"))

DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")
RESOLUTIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "resolutions.sqlite")
BASE_RATES_PATH = os.path.join(os.path.dirname(__file__), "..", "calibration", "base_rates.txt")

OLLAMA_URL = "http://localhost:11434/api/generate"
LOCAL_MODEL = os.getenv("LOCAL_FILTER_MODEL", "qwen2.5:32b")

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


# ── TIER 1: Hourly Market Movement Reflector ─────────────────

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
                    "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 1, ?, ?, ?)",
                    (now, ticker, observation, "flagged_for_review"),
                )
        elif side == "YES":
            # We bet YES — YES price dropping means market disagrees
            movement = (entry_price - current) * 100
            if movement > 20:
                observation = f"YES bet on {ticker}: YES price dropped {movement:.0f}c since entry ({entry_price:.2f} -> {current:.2f})"
                flags.append({"ticker": ticker, "observation": observation, "movement": movement})
                conn.execute(
                    "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 1, ?, ?, ?)",
                    (now, ticker, observation, "flagged_for_review"),
                )

    conn.commit()
    conn.close()

    if flags:
        print(f"  Self-cal T1: {len(flags)} positions with adverse movement >20c")

    return flags


# ── TIER 2: 6-Hour Pattern Analyzer ─────────────────────────

def tier2_pattern_analyzer() -> list[dict]:
    """Analyze last 24hr of resolved trades for loss patterns.

    Groups losses by category, city (weather), confidence bucket.
    Sends structured summary to local 32b model for pattern ID.
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

    if len(losses) < MIN_TRADES:
        return []

    # Build structured input for local model
    loss_lines = []
    for l in losses:
        loss_lines.append(f"  LOSS: {l['title'][:60]} | conf={l['our_confidence']:.2f} | cat={l['category']} | pnl=${l['pnl']:.2f}")

    win_lines = []
    for w in wins[:10]:
        win_lines.append(f"  WIN: {w['title'][:60]} | conf={w['our_confidence']:.2f} | cat={w['category']}")

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
  "min_trades_to_validate": <integer>
}}"""

    result = _query_local_model(prompt)
    hypotheses = []

    if result:
        hypothesis = {
            "pattern": result.get("common_loss_pattern", ""),
            "recommendation": result.get("recommended_change", ""),
            "parameter": result.get("affected_parameter", ""),
            "value": result.get("recommended_value", ""),
            "confidence": result.get("confidence", "low"),
            "supporting_trades": len(losses),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        hypotheses.append(hypothesis)

        # Log hypothesis
        dconn = sqlite3.connect(DECISIONS_DB)
        _init_tables(dconn)
        dconn.execute(
            "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 2, ?, ?, ?)",
            (hypothesis["timestamp"], "pattern_analysis",
             json.dumps(result)[:500], "hypothesis_generated"),
        )
        dconn.commit()
        dconn.close()

        print(f"  Self-cal T2: pattern found — {hypothesis['pattern'][:60]}")
        print(f"    Recommendation: {hypothesis['recommendation'][:60]}")
        print(f"    Confidence: {hypothesis['confidence']}, trades: {hypothesis['supporting_trades']}")

    return hypotheses


# ── TIER 3: Daily Adjustment Executor (2am) ──────────────────

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


# ── TIER 4: Weekly Benchmark (Sunday 3am) ────────────────────

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


# ── Runner ────────────────────────────────────────────────────

def run_tier1():
    """Run from hourly cycle in dual_strategy.py."""
    if not ENABLED:
        return
    try:
        tier1_market_movement()
    except Exception as e:
        print(f"  Self-cal T1 error: {e}")


def run_tier2():
    """Run from 6-hour cycle in dual_strategy.py."""
    if not ENABLED:
        return
    try:
        tier2_pattern_analyzer()
    except Exception as e:
        print(f"  Self-cal T2 error: {e}")


def run_tier3():
    """Run at 2am from dual_strategy.py."""
    if not ENABLED:
        return
    try:
        tier3_daily_executor()
    except Exception as e:
        print(f"  Self-cal T3 error: {e}")


def run_tier4():
    """Run Sunday 3am from dual_strategy.py."""
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
    print(f"Min trades: {MIN_TRADES}")
    print(f"Trial period: {TRIAL_HOURS}hr")
    print(f"Max threshold change: {MAX_THRESHOLD_CHANGE}")
    print()

    print("--- Tier 1: Market Movement ---")
    flags = tier1_market_movement()
    print(f"Flags: {len(flags)}")

    print("\n--- Tier 2: Pattern Analyzer ---")
    hypotheses = tier2_pattern_analyzer()
    print(f"Hypotheses: {len(hypotheses)}")

    print("\n--- Tier 4: Weekly Benchmark ---")
    benchmark = tier4_weekly_benchmark()
    print(f"Alerts: {len(benchmark.get('alerts', []))}")
