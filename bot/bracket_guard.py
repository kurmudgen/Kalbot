"""
Bracket deduplication guard.
Only allows ONE trade per underlying event per session.
Prevents correlated S&P bracket exposure (trading every bracket = same bet).

Also handles macro event lockout — on CPI/Fed/jobs days, only one venue trades.
"""

import os
import sqlite3
import re
from datetime import datetime, timezone

DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")


def extract_event_key(ticker: str, title: str) -> str:
    """Extract the underlying event from a bracket market ticker.
    KXINX-26MAR27H1600-T6949 and KXINX-26MAR27H1600-B6912 are the SAME event."""
    # For S&P brackets: strip the bracket suffix
    # KXINX-26MAR27H1600-T6949.9999 → KXINX-26MAR27H1600
    parts = ticker.split("-")
    if len(parts) >= 3 and ticker.startswith("KXINX"):
        return "-".join(parts[:2])

    # For BTC brackets
    if ticker.startswith("KXBTC"):
        return "-".join(parts[:2])

    # For CPI/GDP/Fed: group by release date
    if any(ticker.startswith(p) for p in ["KXCPI", "KXGDP", "KXFED", "KXFOMC", "KXJOBLESS"]):
        return "-".join(parts[:2])

    # Default: use the ticker as-is
    return ticker


def already_traded_event(ticker: str, title: str, session_id: str) -> bool:
    """Check if we already have a position on this underlying event."""
    if not os.path.exists(DECISIONS_DB):
        return False

    event_key = extract_event_key(ticker, title)
    conn = sqlite3.connect(DECISIONS_DB)

    # Check all executed trades in this session for the same event
    rows = conn.execute(
        "SELECT ticker FROM decisions WHERE executed = 1 AND session_id LIKE ?",
        (f"%{session_id.split('_')[0]}%",),
    ).fetchall()
    conn.close()

    for row in rows:
        existing_key = extract_event_key(row[0], "")
        if existing_key == event_key:
            return True

    return False


# Macro event dates that affect all venues
MACRO_EVENTS = {
    # CPI release dates 2026 (approximate — 2nd or 3rd Tuesday)
    "cpi": ["2026-01-14", "2026-02-11", "2026-03-11", "2026-04-14",
            "2026-05-12", "2026-06-10", "2026-07-14", "2026-08-11",
            "2026-09-15", "2026-10-13", "2026-11-10", "2026-12-15"],
    # FOMC decision dates 2026
    "fomc": ["2026-01-29", "2026-03-19", "2026-05-07", "2026-06-18",
             "2026-07-30", "2026-09-17", "2026-11-05", "2026-12-17"],
    # Jobs report (first Friday of month)
    "nfp": ["2026-01-02", "2026-02-06", "2026-03-06", "2026-04-03",
            "2026-05-01", "2026-06-05", "2026-07-02", "2026-08-07",
            "2026-09-04", "2026-10-02", "2026-11-06", "2026-12-04"],
}


def is_macro_event_day() -> str | None:
    """Check if today is a major macro event day."""
    today = datetime.now().strftime("%Y-%m-%d")
    for event_type, dates in MACRO_EVENTS.items():
        if today in dates:
            return event_type
    return None


def should_block_venue(venue: str) -> bool:
    """On macro event days, only allow ONE venue to trade.
    Priority: Kalshi > Alpaca > Coinbase (most calibrated first)."""
    event = is_macro_event_day()
    if event is None:
        return False

    # On macro days, only Kalshi trades (most calibrated for economic events)
    if venue == "kalshi":
        return False
    return True  # Block Alpaca and Coinbase on macro days


if __name__ == "__main__":
    event = is_macro_event_day()
    print(f"Macro event today: {event or 'None'}")

    # Test bracket dedup
    key1 = extract_event_key("KXINX-26MAR27H1600-T6949.9999", "")
    key2 = extract_event_key("KXINX-26MAR27H1600-B6912", "")
    print(f"Bracket test: {key1} == {key2} ? {key1 == key2}")
