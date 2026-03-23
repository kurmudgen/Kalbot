"""
Weekly performance report. Runs every Friday at 8am.
Sends summary to Telegram and saves to dashboard.
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")
RESOLUTIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "resolutions.sqlite")
STOCK_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "stock_decisions.sqlite")


def generate_weekly_report() -> str:
    """Generate a full weekly performance report."""
    lines = ["📊 <b>KALBOT WEEKLY REPORT</b>"]
    lines.append(f"Week ending {datetime.now().strftime('%B %d, %Y')}")
    lines.append("")

    # Alpaca P&L
    try:
        from alpaca.trading.client import TradingClient
        client = TradingClient(
            os.getenv("ALPACA_API_KEY"), os.getenv("ALPACA_SECRET_KEY"), paper=True,
        )
        account = client.get_account()
        positions = client.get_all_positions()
        portfolio = float(account.portfolio_value)
        pnl = portfolio - 100000

        winners = sum(1 for p in positions if float(p.unrealized_pl) > 0)
        losers = sum(1 for p in positions if float(p.unrealized_pl) <= 0)
        total = winners + losers
        wr = winners / total * 100 if total > 0 else 0

        lines.append(f"<b>STOCKS & CRYPTO (Alpaca)</b>")
        lines.append(f"  Portfolio: ${portfolio:,.2f}")
        lines.append(f"  P&L: ${pnl:+,.2f} ({pnl/1000:.2f}%)")
        lines.append(f"  Positions: {total} ({winners}W/{losers}L, {wr:.0f}% WR)")
        lines.append("")

        # Top winners/losers
        sorted_pos = sorted(positions, key=lambda x: float(x.unrealized_pl), reverse=True)
        if sorted_pos:
            best = sorted_pos[0]
            worst = sorted_pos[-1]
            lines.append(f"  Best: {best.symbol} ${float(best.unrealized_pl):+.2f}")
            lines.append(f"  Worst: {worst.symbol} ${float(worst.unrealized_pl):+.2f}")
            lines.append("")
    except Exception as e:
        lines.append(f"Alpaca error: {e}")
        lines.append("")

    # Kalshi
    if os.path.exists(DECISIONS_DB):
        conn = sqlite3.connect(DECISIONS_DB)
        total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        executed = conn.execute("SELECT COUNT(*) FROM decisions WHERE executed = 1").fetchone()[0]
        conn.close()

        lines.append(f"<b>KALSHI (Prediction Markets)</b>")
        lines.append(f"  Decisions: {total} evaluated")
        lines.append(f"  Trades: {executed} placed")

    if os.path.exists(RESOLUTIONS_DB):
        conn = sqlite3.connect(RESOLUTIONS_DB)
        try:
            wins = conn.execute("SELECT COUNT(*) FROM resolved_trades WHERE pnl > 0").fetchone()[0]
            losses = conn.execute("SELECT COUNT(*) FROM resolved_trades WHERE pnl <= 0").fetchone()[0]
            pnl = conn.execute("SELECT COALESCE(SUM(pnl), 0) FROM resolved_trades").fetchone()[0]
            total = wins + losses
            wr = wins / total * 100 if total > 0 else 0
            lines.append(f"  Resolved: {wins}W/{losses}L ({wr:.0f}% WR)")
            lines.append(f"  Kalshi P&L: ${pnl:+.2f}")
        except Exception:
            lines.append(f"  No resolved trades yet")
        conn.close()
    lines.append("")

    # Market context
    try:
        import requests
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
        if r.status_code == 200:
            fg = r.json()["data"][0]
            lines.append(f"<b>MARKET</b>")
            lines.append(f"  Fear & Greed: {fg['value']}/100 ({fg['value_classification']})")
    except Exception:
        pass

    # Recommendation
    lines.append("")
    lines.append("<b>RECOMMENDATION</b>")
    try:
        if pnl > 0 and wr > 55:
            lines.append("  ✅ System is profitable. Consider increasing budget.")
        elif pnl > 0:
            lines.append("  ⚠️ Profitable but thin edge. Keep paper trading.")
        else:
            lines.append("  ❌ Not profitable yet. Continue paper trading.")
    except Exception:
        lines.append("  📊 Need more data. Continue paper trading.")

    return "\n".join(lines)


def send_weekly_report():
    """Generate and send the weekly report via Telegram."""
    report = generate_weekly_report()

    try:
        from telegram_alerts import send
        send(report)
        print("Weekly report sent to Telegram")
    except Exception as e:
        print(f"Failed to send report: {e}")

    # Save to file
    report_path = os.path.join(os.path.dirname(__file__), "..", "logs", "weekly_report.txt")
    with open(report_path, "w") as f:
        # Strip HTML tags for file version
        import re
        clean = re.sub(r"<[^>]+>", "", report)
        f.write(clean)

    print(report.replace("<b>", "").replace("</b>", ""))


if __name__ == "__main__":
    send_weekly_report()
