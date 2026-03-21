"""
Night runner: orchestrates the full KalBot loop.
Runs market_scanner → local_filter → cloud_analyst → executor
Loops until 6am or until stopped.
"""

import os
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

sys.path.insert(0, os.path.dirname(__file__))

SESSIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "sessions.sqlite")
LOOP_INTERVAL = 300  # 5 minutes between full cycles
STOP_HOUR = 6  # Stop at 6am local time


def init_sessions_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(SESSIONS_DB), exist_ok=True)
    conn = sqlite3.connect(SESSIONS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            start_time TEXT,
            end_time TEXT,
            cycles_completed INTEGER,
            markets_scanned INTEGER,
            markets_filtered INTEGER,
            markets_analyzed INTEGER,
            trades_placed INTEGER,
            errors INTEGER,
            status TEXT
        )
    """)
    conn.commit()
    return conn


def run_cycle(session_id: str) -> dict:
    """Run one full cycle: scan → filter → analyze → execute."""
    from market_scanner import init_db as init_markets_db, scan_markets
    from local_filter import run_filter
    from cloud_analyst import analyze_markets
    from executor import execute_trades

    stats = {
        "markets_scanned": 0,
        "markets_filtered": 0,
        "markets_analyzed": 0,
        "trades_placed": 0,
        "errors": 0,
    }

    # Step 1: Scan markets
    try:
        print("\n--- SCANNING MARKETS ---")
        markets_conn = init_markets_db()
        stats["markets_scanned"] = scan_markets(markets_conn)
        markets_conn.close()
        print(f"Scanned {stats['markets_scanned']} markets")
    except Exception as e:
        print(f"Scanner error: {e}")
        traceback.print_exc()
        stats["errors"] += 1

    # Step 2: Local filter
    try:
        print("\n--- LOCAL FILTER ---")
        passed = run_filter()
        stats["markets_filtered"] = len(passed)
        print(f"{len(passed)} markets passed filter")
    except Exception as e:
        print(f"Filter error: {e}")
        traceback.print_exc()
        stats["errors"] += 1
        passed = []

    # Step 3: Cloud analyst
    try:
        print("\n--- CLOUD ANALYST ---")
        analyzed = analyze_markets(passed)
        stats["markets_analyzed"] = len(analyzed)
        print(f"{len(analyzed)} markets analyzed")
    except Exception as e:
        print(f"Analyst error: {e}")
        traceback.print_exc()
        stats["errors"] += 1
        analyzed = []

    # Step 4: Execute
    try:
        print("\n--- EXECUTOR ---")
        trades = execute_trades(analyzed, session_id=session_id)
        stats["trades_placed"] = len(trades)
    except Exception as e:
        print(f"Executor error: {e}")
        traceback.print_exc()
        stats["errors"] += 1

    return stats


def should_stop() -> bool:
    now = datetime.now()
    return now.hour >= STOP_HOUR and now.hour < 12


def main():
    session_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    start_time = datetime.now(timezone.utc).isoformat()

    print(f"{'='*60}")
    print(f"  KALBOT NIGHT SESSION: {session_id}")
    print(f"  Started: {start_time}")
    print(f"  Will stop at {STOP_HOUR}:00 local time")
    print(f"{'='*60}")

    sess_conn = init_sessions_db()
    sess_conn.execute(
        """INSERT INTO sessions
           (session_id, start_time, cycles_completed, markets_scanned,
            markets_filtered, markets_analyzed, trades_placed, errors, status)
           VALUES (?, ?, 0, 0, 0, 0, 0, 0, 'running')""",
        (session_id, start_time),
    )
    sess_conn.commit()

    totals = {
        "cycles": 0,
        "markets_scanned": 0,
        "markets_filtered": 0,
        "markets_analyzed": 0,
        "trades_placed": 0,
        "errors": 0,
    }

    try:
        while not should_stop():
            print(f"\n{'='*40}")
            print(f"  CYCLE {totals['cycles'] + 1}")
            print(f"  {datetime.now(timezone.utc).isoformat()}")
            print(f"{'='*40}")

            stats = run_cycle(session_id)
            totals["cycles"] += 1
            totals["markets_scanned"] += stats["markets_scanned"]
            totals["markets_filtered"] += stats["markets_filtered"]
            totals["markets_analyzed"] += stats["markets_analyzed"]
            totals["trades_placed"] += stats["trades_placed"]
            totals["errors"] += stats["errors"]

            # Update session record
            sess_conn.execute(
                """UPDATE sessions SET
                   cycles_completed = ?, markets_scanned = ?,
                   markets_filtered = ?, markets_analyzed = ?,
                   trades_placed = ?, errors = ?
                   WHERE session_id = ?""",
                (totals["cycles"], totals["markets_scanned"],
                 totals["markets_filtered"], totals["markets_analyzed"],
                 totals["trades_placed"], totals["errors"], session_id),
            )
            sess_conn.commit()

            if should_stop():
                break

            print(f"\nSleeping {LOOP_INTERVAL}s until next cycle...")
            time.sleep(LOOP_INTERVAL)

    except KeyboardInterrupt:
        print("\n\nSession stopped by user")

    end_time = datetime.now(timezone.utc).isoformat()
    sess_conn.execute(
        "UPDATE sessions SET end_time = ?, status = 'completed' WHERE session_id = ?",
        (end_time, session_id),
    )
    sess_conn.commit()
    sess_conn.close()

    print(f"\n{'='*60}")
    print(f"  SESSION COMPLETE: {session_id}")
    print(f"  Duration: {start_time} to {end_time}")
    print(f"  Cycles: {totals['cycles']}")
    print(f"  Markets scanned: {totals['markets_scanned']}")
    print(f"  Passed filter: {totals['markets_filtered']}")
    print(f"  Cloud analyzed: {totals['markets_analyzed']}")
    print(f"  Trades placed: {totals['trades_placed']}")
    print(f"  Errors: {totals['errors']}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
