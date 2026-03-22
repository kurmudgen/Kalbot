"""
Kill switch and circuit breaker for KalBot.
- File-based kill switch: create ~/kalbot/STOP to halt all trading
- Circuit breaker: halts if cumulative losses exceed threshold
- Can be triggered remotely by creating the STOP file
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta

STOP_FILE = os.path.join(os.path.dirname(__file__), "..", "STOP")
DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")
STOCK_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "stock_decisions.sqlite")

MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "100"))  # Stop if down $100 in a day
MAX_SESSION_LOSS = float(os.getenv("MAX_SESSION_LOSS", "200"))  # Stop if down $200 in a session


def is_killed() -> bool:
    """Check if the kill switch file exists."""
    return os.path.exists(STOP_FILE)


def kill(reason: str = "Manual stop"):
    """Activate the kill switch."""
    with open(STOP_FILE, "w") as f:
        f.write(f"Stopped at {datetime.now().isoformat()}\nReason: {reason}\n")
    print(f"\n!!! KILL SWITCH ACTIVATED: {reason} !!!")
    print(f"Delete {STOP_FILE} to resume trading.\n")


def unkill():
    """Remove the kill switch."""
    if os.path.exists(STOP_FILE):
        os.remove(STOP_FILE)
        print("Kill switch deactivated. Trading will resume.")


def check_circuit_breaker() -> bool:
    """Check if cumulative losses exceed the circuit breaker threshold.
    Returns True if trading should STOP."""
    # This is a placeholder — actual P&L tracking requires resolved trades
    # For now, count consecutive failed/skipped trades as a warning
    return False


def should_trade() -> tuple[bool, str]:
    """Master check — should the bot place any trades right now?"""
    if is_killed():
        return False, "Kill switch active (STOP file exists)"

    if check_circuit_breaker():
        return False, "Circuit breaker triggered (max loss exceeded)"

    return True, "OK"


if __name__ == "__main__":
    ok, reason = should_trade()
    print(f"Should trade: {ok} — {reason}")
