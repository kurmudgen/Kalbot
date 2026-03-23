"""
Telegram alerts: sends push notifications for trades, wins/losses, and system events.
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def send(message: str):
    """Send a Telegram message."""
    if not TOKEN or not CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        pass


def trade_alert(ticker: str, title: str, side: str, amount: float,
                confidence: float, edge: float, mode: str = "PAPER"):
    """Alert when a trade is placed."""
    emoji = "🟢" if side == "YES" else "🔴"
    send(
        f"{emoji} <b>{mode} TRADE</b>\n"
        f"{side} ${amount:.2f} on:\n"
        f"<i>{title[:80]}</i>\n"
        f"Confidence: {confidence:.0%} | Edge: {edge:.0%}"
    )


def resolution_alert(ticker: str, title: str, won: bool, pnl: float,
                      win_rate: float, total_pnl: float):
    """Alert when a trade resolves."""
    emoji = "💰" if won else "💸"
    result = "WIN" if won else "LOSS"
    send(
        f"{emoji} <b>{result}</b> ${pnl:+.2f}\n"
        f"<i>{title[:80]}</i>\n"
        f"Win rate: {win_rate:.0%} | Total P&L: ${total_pnl:+.2f}"
    )


def system_alert(message: str, level: str = "info"):
    """Alert for system events."""
    emoji = {"info": "ℹ️", "warning": "⚠️", "critical": "🚨"}.get(level, "ℹ️")
    send(f"{emoji} {message}")


def daily_summary(trades: int, wins: int, pnl: float, portfolio: float):
    """Daily summary at 8am."""
    wr = wins / trades * 100 if trades > 0 else 0
    send(
        f"📊 <b>Daily Summary</b>\n"
        f"Trades: {trades} | Wins: {wins} ({wr:.0f}%)\n"
        f"P&L: ${pnl:+.2f}\n"
        f"Portfolio: ${portfolio:,.2f}"
    )


if __name__ == "__main__":
    send("🧪 Test alert from KalBot")
    print("Sent test alert")
