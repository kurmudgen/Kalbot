"""
Morning review: prints a clean summary of last night's KalBot session.
Supports --days flag for multi-day rolling summaries.
"""

import argparse
import os
import sys
import sqlite3
from datetime import datetime, timedelta, timezone

DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")
SESSIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "sessions.sqlite")


def get_sessions(days: int = 1) -> list[dict]:
    if not os.path.exists(SESSIONS_DB):
        return []
    conn = sqlite3.connect(SESSIONS_DB)
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = conn.execute(
        "SELECT * FROM sessions WHERE start_time > ? ORDER BY start_time DESC",
        (cutoff,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_decisions(days: int = 1) -> list[dict]:
    if not os.path.exists(DECISIONS_DB):
        return []
    conn = sqlite3.connect(DECISIONS_DB)
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = conn.execute(
        "SELECT * FROM decisions WHERE decided_at > ? ORDER BY decided_at DESC",
        (cutoff,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def main():
    parser = argparse.ArgumentParser(description="KalBot morning review")
    parser.add_argument("--days", type=int, default=1, help="Number of days to review")
    args = parser.parse_args()

    sessions = get_sessions(args.days)
    decisions = get_decisions(args.days)

    executed = [d for d in decisions if d["executed"]]
    skipped = [d for d in decisions if not d["executed"]]

    print()
    print("=" * 50)
    print("  KALBOT OVERNIGHT SUMMARY")
    print("=" * 50)

    if not sessions:
        print(f"  No sessions found in the last {args.days} day(s).")
        print("=" * 50)
        return

    # Session info
    latest = sessions[0]
    print(f"  Date:     {latest.get('start_time', 'N/A')[:10]}")
    print(f"  Session:  {latest.get('start_time', 'N/A')[:19]} to {(latest.get('end_time') or 'ongoing')[:19]}")
    print(f"  Cycles:   {latest.get('cycles_completed', 0)}")
    print(f"  Markets scanned:       {latest.get('markets_scanned', 0)}")
    print(f"  Passed local filter:   {latest.get('markets_filtered', 0)}")
    print(f"  Passed cloud analyst:  {latest.get('markets_analyzed', 0)}")

    mode = executed[0]["mode"] if executed else "PAPER"
    total_deployed = sum(d["amount"] for d in executed)

    print(f"  Trades placed:         {len(executed)} ({mode.lower()})")
    print(f"  Total deployed:        ${total_deployed:.2f}")

    # Trade log
    if executed:
        print()
        print("--- TRADE LOG ---")
        for d in executed:
            title = d["title"][:40]
            conf = d["cloud_confidence"]
            gap = d["price_gap"]
            side = d["side"]
            amount = d["amount"]
            print(f"  {title:<40} | conf: {conf:.2f} | gap: {gap:.2f} | bet: ${amount:.2f} {side}")

    # By category
    if executed:
        print()
        print("--- BY CATEGORY ---")
        cats = {}
        for d in executed:
            cat = d["category"]
            if cat not in cats:
                cats[cat] = {"trades": 0, "deployed": 0.0}
            cats[cat]["trades"] += 1
            cats[cat]["deployed"] += d["amount"]

        for cat, stats in sorted(cats.items()):
            print(f"  {cat:<15} {stats['trades']} trades | ${stats['deployed']:.2f} deployed")

    # Flags
    print()
    print("--- FLAGS ---")
    flags = []

    errors = sum(s.get("errors", 0) for s in sessions)
    if errors > 0:
        flags.append(f"  {errors} error(s) during session")

    if not executed:
        flags.append("  No trades placed — all markets filtered out or skipped")

    for d in skipped:
        if d.get("error") and "limit" in d.get("error", "").lower():
            flags.append(f"  Spend limit reached: {d['error']}")
            break

    if total_deployed > float(os.getenv("MAX_NIGHTLY_SPEND", "50")) * 0.9:
        flags.append(f"  Near nightly spend limit (${total_deployed:.2f})")

    low_conf = [d for d in executed if d["cloud_confidence"] < 0.8]
    if len(low_conf) > len(executed) * 0.5 and executed:
        flags.append(f"  {len(low_conf)}/{len(executed)} trades had confidence < 0.80")

    if not flags:
        flags.append("  No flags.")

    for f in flags:
        print(f)

    # Multi-day summary
    if args.days > 1 and len(sessions) > 1:
        print()
        print(f"--- {args.days}-DAY ROLLING SUMMARY ---")
        all_executed = [d for d in decisions if d["executed"]]
        total = sum(d["amount"] for d in all_executed)
        print(f"  Sessions:        {len(sessions)}")
        print(f"  Total trades:    {len(all_executed)}")
        print(f"  Total deployed:  ${total:.2f}")

    # Data feed status check
    print()
    print("--- DATA FEEDS ---")
    data_path = os.path.join(os.path.dirname(__file__), "..", "data")
    sys.path.insert(0, data_path)

    feed_status = {}
    try:
        from weather_nws_feed import get_city_forecast
        nws = get_city_forecast("New York high temperature")
        feed_status["NWS Weather"] = "OK" if nws else "no data"
    except Exception as e:
        feed_status["NWS Weather"] = f"error: {str(e)[:30]}"

    try:
        from jobless_claims_feed import fetch_claims_data
        data = fetch_claims_data()
        feed_status["DOL Jobless"] = "OK" if data.get("initial_claims") else "no FRED key"
    except Exception as e:
        feed_status["DOL Jobless"] = f"error: {str(e)[:30]}"

    try:
        from eia_petroleum_feed import fetch_gas_prices
        data = fetch_gas_prices()
        feed_status["EIA Petroleum"] = "OK" if data.get("retail_gas") else "no EIA key"
    except Exception as e:
        feed_status["EIA Petroleum"] = f"error: {str(e)[:30]}"

    try:
        from fed_funds_feed import fetch_fed_data
        data = fetch_fed_data()
        feed_status["Fed/FRED"] = "OK" if data.get("fed_funds_rate") else "no FRED key"
    except Exception as e:
        feed_status["Fed/FRED"] = f"error: {str(e)[:30]}"

    try:
        from treasury_auction_feed import fetch_recent_auctions
        data = fetch_recent_auctions(days=7)
        feed_status["Treasury Auctions"] = f"OK ({len(data)} recent)" if data else "no data"
    except Exception as e:
        feed_status["Treasury Auctions"] = f"error: {str(e)[:30]}"

    try:
        from congressional_trades_feed import fetch_congressional_trades
        data = fetch_congressional_trades(days=7)
        feed_status["Congressional"] = f"OK ({len(data)} trades)" if data else "no data"
    except Exception as e:
        feed_status["Congressional"] = f"error: {str(e)[:30]}"

    for name, status in feed_status.items():
        icon = "+" if status.startswith("OK") else "-"
        print(f"  [{icon}] {name:<20} {status}")

    # Treasury / Mercury report
    try:
        from treasury import print_treasury_report
        print()
        print_treasury_report()
    except Exception as e:
        print()
        print("--- TREASURY ---")
        print(f"  Mercury: error ({str(e)[:40]})")

    print()
    print("=" * 50)


if __name__ == "__main__":
    main()
