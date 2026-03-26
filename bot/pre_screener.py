"""
Pre-screener: uses llama3.2:3b via Ollama to quickly reject markets
where public data can't help predict the outcome.

Runs BEFORE the main local filter (7b/32b model). Markets that fail
are skipped entirely — no further processing, no cloud API calls.
Saves significant cost on junk markets.
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3.2:3b"
ENABLED = os.getenv("PRESCREENER_ENABLED", "true").lower() == "true"

PROMPT = """You are a quick market screener. Answer only YES or NO.

Is this a prediction market where publicly available data like weather forecasts, economic releases, government reports, or official statistics could help predict the outcome before it resolves?

Market: {title}
Category: {category}

Answer YES or NO only."""


def screen_market(title: str, category: str) -> bool:
    """Returns True if market should proceed to full analysis.
    Returns True on timeout/error to avoid blocking valid markets.
    """
    if not ENABLED:
        return True

    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL,
                "prompt": PROMPT.format(title=title, category=category),
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 10},
            },
            timeout=10,
        )
        resp.raise_for_status()
        raw = resp.json().get("response", "").strip().upper()
        return "YES" in raw
    except Exception:
        # Timeout or error — let it through to avoid blocking valid markets
        return True


if __name__ == "__main__":
    tests = [
        ("Will the high temp in NYC be above 60F tomorrow?", "weather"),
        ("Will BTC be above $100,000 on March 30?", "economics"),
        ("Will Team A beat Team B in tonight's game?", "sports"),
        ("Will initial jobless claims exceed 220,000?", "inflation"),
        ("Who will win the Oscar for Best Picture?", "entertainment"),
    ]

    for title, cat in tests:
        result = screen_market(title, cat)
        print(f"{'PASS' if result else 'REJECT'}: {title[:50]}")
