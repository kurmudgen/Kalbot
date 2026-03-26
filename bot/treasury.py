"""
Treasury module — Mercury bank integration.

Checking (...3906): operational account for bot expenses and trading deposits.
Savings (...4242): reserve account — profits sweep here, never touched for operations.

Daily sweep: moves any checking balance above $500 to savings automatically.
Balance monitoring: feeds into morning review and hourly API cost monitor.
"""

import os
import sqlite3
import requests
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

MERCURY_TOKEN = os.getenv("MERCURY_API_TOKEN", "")
CHECKING_ID = os.getenv("MERCURY_CHECKING_ID", "")
SAVINGS_ID = os.getenv("MERCURY_SAVINGS_ID", "")
MERCURY_BASE = "https://api.mercury.com/api/v1"

TREASURY_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "treasury.sqlite")

# Sweep threshold — keep $500 in checking for operations, rest goes to savings
SWEEP_THRESHOLD = 500.00
MIN_SWEEP_AMOUNT = 10.00  # Don't bother sweeping less than $10


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {MERCURY_TOKEN}",
        "Content-Type": "application/json",
    }


def init_treasury_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(TREASURY_DB), exist_ok=True)
    conn = sqlite3.connect(TREASURY_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS balance_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            checking_balance REAL,
            savings_balance REAL,
            total_balance REAL,
            snapshot_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sweeps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            amount REAL,
            from_account TEXT,
            to_account TEXT,
            status TEXT,
            swept_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_burns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            api_costs REAL DEFAULT 0,
            trading_deposits REAL DEFAULT 0,
            trading_returns REAL DEFAULT 0,
            net_pnl REAL DEFAULT 0,
            updated_at TEXT
        )
    """)
    conn.commit()
    return conn


def get_account_balance(account_id: str) -> float | None:
    """Get current balance for a Mercury account."""
    if not MERCURY_TOKEN or not account_id:
        return None
    try:
        r = requests.get(
            f"{MERCURY_BASE}/account/{account_id}",
            headers=_headers(),
            timeout=15,
        )
        if r.status_code == 200:
            return float(r.json().get("currentBalance", 0))
    except Exception:
        pass
    return None


def get_all_balances() -> dict:
    """Get balances for both accounts."""
    checking = get_account_balance(CHECKING_ID)
    savings = get_account_balance(SAVINGS_ID)

    result = {
        "checking": checking,
        "savings": savings,
        "total": None,
        "ok": checking is not None,
    }
    if checking is not None and savings is not None:
        result["total"] = checking + savings

    return result


def get_recent_transactions(account_id: str, days: int = 1) -> list[dict]:
    """Get recent transactions for an account."""
    if not MERCURY_TOKEN or not account_id:
        return []
    try:
        r = requests.get(
            f"{MERCURY_BASE}/account/{account_id}/transactions",
            headers=_headers(),
            params={"limit": 50},
            timeout=15,
        )
        if r.status_code == 200:
            txns = r.json().get("transactions", [])
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            return [t for t in txns if (t.get("createdAt", "") or "") >= cutoff]
    except Exception:
        pass
    return []


def snapshot_balances() -> dict:
    """Take a balance snapshot and save to DB."""
    balances = get_all_balances()
    if not balances["ok"]:
        return balances

    conn = init_treasury_db()
    conn.execute(
        """INSERT INTO balance_snapshots
           (checking_balance, savings_balance, total_balance, snapshot_at)
           VALUES (?, ?, ?, ?)""",
        (balances["checking"], balances["savings"], balances["total"],
         datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()
    return balances


def daily_sweep(dry_run: bool = False) -> dict | None:
    """Sweep checking balance above threshold to savings.

    Returns sweep details or None if no sweep needed.
    """
    checking = get_account_balance(CHECKING_ID)
    if checking is None:
        return None

    excess = checking - SWEEP_THRESHOLD
    if excess < MIN_SWEEP_AMOUNT:
        return None

    sweep = {
        "amount": round(excess, 2),
        "from": "checking",
        "to": "savings",
        "checking_before": checking,
        "checking_after": SWEEP_THRESHOLD,
    }

    if dry_run:
        sweep["status"] = "dry_run"
        print(f"  SWEEP (dry run): ${excess:.2f} checking -> savings")
        return sweep

    # Execute the internal transfer via Mercury API
    try:
        r = requests.post(
            f"{MERCURY_BASE}/account/{CHECKING_ID}/transactions",
            headers=_headers(),
            json={
                "amount": sweep["amount"],
                "recipientId": SAVINGS_ID,
                "note": f"KalBot daily sweep - {datetime.now().strftime('%Y-%m-%d')}",
                "paymentMethod": "internalTransfer",
            },
            timeout=15,
        )
        if r.status_code in (200, 201):
            sweep["status"] = "completed"
            print(f"  SWEEP: ${excess:.2f} checking -> savings")
        else:
            sweep["status"] = f"failed_{r.status_code}"
            sweep["error"] = r.text[:200]
            print(f"  SWEEP FAILED: {r.status_code} {r.text[:100]}")
    except Exception as e:
        sweep["status"] = f"error"
        sweep["error"] = str(e)[:200]
        print(f"  SWEEP ERROR: {e}")

    # Log the sweep
    conn = init_treasury_db()
    conn.execute(
        "INSERT INTO sweeps (amount, from_account, to_account, status, swept_at) VALUES (?, ?, ?, ?, ?)",
        (sweep["amount"], "checking", "savings", sweep["status"],
         datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()

    # Telegram alert
    try:
        from telegram_alerts import send
        send(f"Treasury sweep: ${sweep['amount']:.2f} checking -> savings ({sweep['status']})")
    except Exception:
        pass

    return sweep


def get_burn_rate(days: int = 7) -> dict:
    """Calculate daily burn rate from recent transactions."""
    txns = get_recent_transactions(CHECKING_ID, days=days)

    outflows = 0
    inflows = 0
    transfers = []

    for t in txns:
        amount = float(t.get("amount", 0))
        if amount < 0:
            outflows += abs(amount)
        else:
            inflows += amount
        if abs(amount) > 0:
            transfers.append({
                "amount": amount,
                "counterparty": t.get("counterpartyName", ""),
                "date": (t.get("createdAt", "") or "")[:10],
            })

    daily_burn = outflows / max(days, 1)
    balances = get_all_balances()
    checking = balances.get("checking") or 0

    runway_days = checking / daily_burn if daily_burn > 0 else float("inf")

    return {
        "outflows": outflows,
        "inflows": inflows,
        "daily_burn": daily_burn,
        "runway_days": runway_days,
        "transfers": transfers[:10],
    }


def get_weekly_pnl() -> dict:
    """Weekly P&L: trading returns vs operational costs."""
    conn = init_treasury_db()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT * FROM daily_burns WHERE date >= ?", (cutoff,)
    ).fetchall()
    conn.close()

    total_api = sum(r[2] for r in rows) if rows else 0
    total_deposits = sum(r[3] for r in rows) if rows else 0
    total_returns = sum(r[4] for r in rows) if rows else 0

    # Also pull from resolution tracker for actual trading P&L
    try:
        resolutions_db = os.path.join(os.path.dirname(__file__), "..", "logs", "resolutions.sqlite")
        if os.path.exists(resolutions_db):
            rconn = sqlite3.connect(resolutions_db)
            pnl_row = rconn.execute(
                "SELECT COALESCE(SUM(pnl), 0) FROM resolved_trades WHERE resolved_at >= ?",
                (cutoff,)
            ).fetchone()
            total_returns = pnl_row[0] if pnl_row else 0
            rconn.close()
    except Exception:
        pass

    return {
        "api_costs": total_api,
        "trading_deposits": total_deposits,
        "trading_returns": total_returns,
        "net_pnl": total_returns - total_api - total_deposits,
        "period_days": 7,
    }


def print_treasury_report():
    """Print formatted treasury report for morning review."""
    balances = get_all_balances()

    print("--- TREASURY ---")
    if not balances["ok"]:
        print("  Mercury API: UNREACHABLE")
        return

    checking = balances["checking"] or 0
    savings = balances["savings"] or 0
    total = balances["total"] or 0

    print(f"  Checking (...3906): ${checking:,.2f} -- operational")
    print(f"  Savings  (...4242): ${savings:,.2f} -- reserve")
    print(f"  Total:              ${total:,.2f}")

    # Today's burns
    burn = get_burn_rate(days=1)
    print(f"  Burns today:        ${burn['outflows']:,.2f} API costs")

    if burn["transfers"]:
        print("  Transfers today:")
        for t in burn["transfers"]:
            print(f"    {t['date']} {t['counterparty'][:30]}: ${t['amount']:+,.2f}")

    # Runway
    burn_7d = get_burn_rate(days=7)
    if burn_7d["daily_burn"] > 0:
        print(f"  Runway at current burn: {burn_7d['runway_days']:.0f} days")
    else:
        print(f"  Runway at current burn: unlimited (no outflows)")

    # Weekly P&L
    weekly = get_weekly_pnl()
    if weekly["trading_returns"] != 0 or weekly["api_costs"] != 0:
        print(f"  Weekly P&L: ${weekly['net_pnl']:+,.2f} "
              f"(returns ${weekly['trading_returns']:+,.2f} - costs ${weekly['api_costs']:,.2f})")


def run_treasury_cycle():
    """Run full treasury cycle: snapshot + sweep check."""
    if not MERCURY_TOKEN:
        return

    # Snapshot balances
    balances = snapshot_balances()

    # Check for daily sweep (once per day)
    conn = init_treasury_db()
    today = datetime.now().strftime("%Y-%m-%d")
    swept_today = conn.execute(
        "SELECT COUNT(*) FROM sweeps WHERE swept_at LIKE ?", (f"{today}%",)
    ).fetchone()[0]
    conn.close()

    if swept_today == 0 and balances.get("ok"):
        daily_sweep()


if __name__ == "__main__":
    print("=== Mercury Treasury ===")
    print()
    print_treasury_report()
    print()

    # Test sweep in dry run mode
    result = daily_sweep(dry_run=True)
    if result:
        print(f"\nSweep available: ${result['amount']:.2f}")
    else:
        print("\nNo sweep needed")
