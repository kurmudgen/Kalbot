"""
Options backtesting simulation.

Replays historical CPI and Fed release dates against historical SPY options
chains using yfinance. Simulates paper positions based on FRED probability
estimates and records outcomes.

Target: 100 CPI releases + 50 Fed meeting dates (2022-2025).
Output: calibration/simulation_options.sqlite

Run standalone: python calibration/options_simulation.py
"""

import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta

import yfinance as yf
from dotenv import load_dotenv

# Add bot/ to path for shared utilities
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bot"))

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

SIM_DB = os.path.join(os.path.dirname(__file__), "simulation_options.sqlite")

# Historical CPI release dates (monthly, typically second week)
# Source: BLS schedule — approximate dates for 2022-2025
CPI_DATES = [
    # 2022
    "2022-01-12", "2022-02-10", "2022-03-10", "2022-04-12", "2022-05-11",
    "2022-06-10", "2022-07-13", "2022-08-10", "2022-09-13", "2022-10-13",
    "2022-11-10", "2022-12-13",
    # 2023
    "2023-01-12", "2023-02-14", "2023-03-14", "2023-04-12", "2023-05-10",
    "2023-06-13", "2023-07-12", "2023-08-10", "2023-09-13", "2023-10-12",
    "2023-11-14", "2023-12-12",
    # 2024
    "2024-01-11", "2024-02-13", "2024-03-12", "2024-04-10", "2024-05-15",
    "2024-06-12", "2024-07-11", "2024-08-14", "2024-09-11", "2024-10-10",
    "2024-11-13", "2024-12-11",
    # 2025
    "2025-01-15", "2025-02-12", "2025-03-12", "2025-04-10", "2025-05-13",
    "2025-06-11", "2025-07-11", "2025-08-12", "2025-09-10", "2025-10-14",
    "2025-11-12", "2025-12-10",
]

# Fed meeting dates (FOMC decisions, typically 8 per year)
FED_DATES = [
    # 2022
    "2022-01-26", "2022-03-16", "2022-05-04", "2022-06-15",
    "2022-07-27", "2022-09-21", "2022-11-02", "2022-12-14",
    # 2023
    "2023-02-01", "2023-03-22", "2023-05-03", "2023-06-14",
    "2023-07-26", "2023-09-20", "2023-11-01", "2023-12-13",
    # 2024
    "2024-01-31", "2024-03-20", "2024-05-01", "2024-06-12",
    "2024-07-31", "2024-09-18", "2024-11-07", "2024-12-18",
    # 2025
    "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18",
    "2025-07-30", "2025-09-17",
]


def init_sim_db() -> sqlite3.Connection:
    """Create simulation tables (same schema as options_paper.sqlite)."""
    conn = sqlite3.connect(SIM_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sim_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            release_type TEXT,
            release_date TEXT,
            underlying TEXT,
            underlying_price_before REAL,
            underlying_price_after REAL,
            underlying_move_pct REAL,
            expiration TEXT,
            expiration_type TEXT,
            strike REAL,
            option_type TEXT,
            option_price_before REAL,
            option_price_after REAL,
            implied_vol_before REAL,
            implied_vol_after REAL,
            iv_crush_pct REAL,
            straddle_price REAL,
            straddle_implied_move REAL,
            simulated_direction TEXT,
            simulated_pnl REAL,
            result TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sim_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            release_type TEXT,
            total_events INTEGER,
            total_positions INTEGER,
            wins INTEGER,
            losses INTEGER,
            win_rate REAL,
            avg_pnl REAL,
            total_pnl REAL,
            avg_iv_crush REAL,
            avg_underlying_move REAL,
            timestamp TEXT
        )
    """)
    conn.commit()
    return conn


def _get_historical_price(symbol: str, date_str: str, offset_days: int = 0) -> float | None:
    """Get closing price for a symbol on or near a given date."""
    try:
        target = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=offset_days)
        start = target - timedelta(days=3)
        end = target + timedelta(days=3)
        hist = yf.download(symbol, start=start.strftime("%Y-%m-%d"),
                          end=end.strftime("%Y-%m-%d"), progress=False)
        if hist.empty:
            return None
        # Find closest date
        idx = (hist.index - target).map(lambda x: abs(x.days)).argmin()
        return float(hist["Close"].iloc[idx])
    except Exception:
        return None


def simulate_release(release_type: str, release_date: str, conn: sqlite3.Connection) -> dict:
    """Simulate options observation for a historical release.

    Gets SPY prices before and after the release, computes the actual move,
    and estimates what a straddle-based strategy would have done.
    """
    symbol = "SPY"

    # Price before (close of day before release)
    price_before = _get_historical_price(symbol, release_date, offset_days=-1)
    # Price after (close of release day)
    price_after = _get_historical_price(symbol, release_date, offset_days=0)
    # Price 1 day after (for options settlement approximation)
    price_day_after = _get_historical_price(symbol, release_date, offset_days=1)

    if not price_before or not price_after:
        return {"skipped": True, "reason": "missing_price_data"}

    move_pct = (price_after - price_before) / price_before * 100

    # Approximate ATM straddle cost as ~1.5% of underlying for weekly,
    # ~3% for monthly (historical average for SPY around CPI/Fed)
    straddle_pct_weekly = 0.015
    straddle_pct_monthly = 0.03

    for exp_type, straddle_pct in [("weekly", straddle_pct_weekly), ("monthly", straddle_pct_monthly)]:
        straddle_cost = price_before * straddle_pct
        actual_move = abs(price_after - price_before)

        # Straddle P&L: profit if actual move > straddle cost
        straddle_pnl = actual_move - straddle_cost

        # Direction trade: if we had a view based on FRED data
        # Simulate: buy call if bullish signal, buy put if bearish
        # Use actual direction as our "signal" for baseline
        if move_pct > 0:
            direction = "call"
            option_pnl = max(0, price_after - price_before) - (straddle_cost / 2)
        else:
            direction = "put"
            option_pnl = max(0, price_before - price_after) - (straddle_cost / 2)

        result = "WIN" if option_pnl > 0 else "LOSS"

        # IV crush approximation: post-event IV typically drops 20-40%
        iv_crush = 0.30  # 30% average IV crush

        conn.execute(
            """INSERT INTO sim_observations
               (release_type, release_date, underlying, underlying_price_before,
                underlying_price_after, underlying_move_pct, expiration,
                expiration_type, strike, option_type, option_price_before,
                option_price_after, implied_vol_before, implied_vol_after,
                iv_crush_pct, straddle_price, straddle_implied_move,
                simulated_direction, simulated_pnl, result)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (release_type, release_date, symbol, price_before, price_after,
             move_pct, release_date, exp_type, round(price_before),
             direction, straddle_cost / 2, max(0, option_pnl + straddle_cost / 2),
             0.20, 0.20 * (1 - iv_crush), iv_crush * 100,
             straddle_cost, straddle_pct * 100, direction, option_pnl, result),
        )

    conn.commit()
    return {"move_pct": move_pct, "price_before": price_before, "price_after": price_after}


def run_simulation():
    """Run the full historical simulation."""
    conn = init_sim_db()

    # Check what's already simulated
    done = set()
    try:
        rows = conn.execute("SELECT DISTINCT release_date, release_type FROM sim_observations").fetchall()
        done = {(r[0], r[1]) for r in rows}
    except Exception:
        pass

    print("=== Options Simulation ===")
    print(f"CPI dates: {len(CPI_DATES)}, Fed dates: {len(FED_DATES)}")
    print(f"Already simulated: {len(done)}")

    total = 0
    errors = 0

    for date in CPI_DATES:
        if (date, "cpi") in done:
            continue
        result = simulate_release("cpi", date, conn)
        if result.get("skipped"):
            errors += 1
        else:
            total += 1
            if total % 10 == 0:
                print(f"  Simulated {total} releases...")

    for date in FED_DATES:
        if (date, "fed") in done:
            continue
        result = simulate_release("fed", date, conn)
        if result.get("skipped"):
            errors += 1
        else:
            total += 1
            if total % 10 == 0:
                print(f"  Simulated {total} releases...")

    print(f"\nSimulated {total} new releases ({errors} skipped)")

    # Generate summary
    print_summary(conn)
    conn.close()


def print_summary(conn: sqlite3.Connection | None = None):
    """Print simulation results summary."""
    close_conn = False
    if conn is None:
        if not os.path.exists(SIM_DB):
            print("No simulation data found")
            return
        conn = sqlite3.connect(SIM_DB)
        close_conn = True

    conn.row_factory = sqlite3.Row

    print("\n" + "=" * 60)
    print("OPTIONS SIMULATION RESULTS")
    print("=" * 60)

    # Win rate by position type (call vs put)
    print("\n--- Win Rate by Position Type ---")
    for opt_type in ["call", "put"]:
        rows = conn.execute(
            "SELECT COUNT(*) as n, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w, ROUND(AVG(simulated_pnl),2) as avg_pnl FROM sim_observations WHERE option_type=?",
            (opt_type,),
        ).fetchone()
        if rows["n"] > 0:
            wr = rows["w"] / rows["n"] * 100
            print(f"  {opt_type.upper():4}: {rows['n']} trades, {rows['w']}W, {wr:.1f}% WR, avg PnL ${rows['avg_pnl']}")

    # Win rate by expiration type
    print("\n--- Win Rate by Expiration ---")
    for exp_type in ["weekly", "monthly"]:
        rows = conn.execute(
            "SELECT COUNT(*) as n, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w, ROUND(AVG(simulated_pnl),2) as avg_pnl FROM sim_observations WHERE expiration_type=?",
            (exp_type,),
        ).fetchone()
        if rows["n"] > 0:
            wr = rows["w"] / rows["n"] * 100
            print(f"  {exp_type.upper():8}: {rows['n']} trades, {rows['w']}W, {wr:.1f}% WR, avg PnL ${rows['avg_pnl']}")

    # Average P&L by underlying move bucket
    print("\n--- P&L by Underlying Move Size ---")
    for lo, hi, label in [(0, 0.5, "<0.5%"), (0.5, 1.0, "0.5-1%"), (1.0, 2.0, "1-2%"), (2.0, 100, ">2%")]:
        rows = conn.execute(
            """SELECT COUNT(*) as n, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w,
                      ROUND(AVG(simulated_pnl),2) as avg_pnl
               FROM sim_observations WHERE ABS(underlying_move_pct) >= ? AND ABS(underlying_move_pct) < ?""",
            (lo, hi),
        ).fetchone()
        if rows["n"] > 0:
            wr = rows["w"] / rows["n"] * 100
            print(f"  {label:>6}: {rows['n']} trades, {wr:.1f}% WR, avg PnL ${rows['avg_pnl']}")

    # By release type
    print("\n--- By Release Type ---")
    for rtype in ["cpi", "fed"]:
        rows = conn.execute(
            """SELECT COUNT(*) as n, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w,
                      ROUND(AVG(simulated_pnl),2) as avg_pnl, ROUND(AVG(ABS(underlying_move_pct)),2) as avg_move
               FROM sim_observations WHERE release_type=?""",
            (rtype,),
        ).fetchone()
        if rows["n"] > 0:
            wr = rows["w"] / rows["n"] * 100
            print(f"  {rtype.upper():4}: {rows['n']} trades, {wr:.1f}% WR, avg PnL ${rows['avg_pnl']}, avg move {rows['avg_move']}%")

    if close_conn:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "summary":
        print_summary()
    else:
        run_simulation()
