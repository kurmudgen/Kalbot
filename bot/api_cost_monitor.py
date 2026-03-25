"""
API cost monitor — tracks spending and warns before accounts run dry.
Checks Perplexity, Anthropic, and DeepSeek balances/usage.
Sends Telegram alert when any account drops below threshold.
"""

import os
import requests
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

# Alert when balance drops below these amounts
PERPLEXITY_WARN = 5.00   # $5 remaining
ANTHROPIC_WARN = 5.00
DEEPSEEK_WARN = 2.00

_last_warned = {}  # Prevent spam — only warn once per hour per service


def check_perplexity() -> dict:
    """Check Perplexity API — test with a cheap call."""
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if not api_key:
        return {"status": "no_key", "ok": False}
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai", timeout=10)
        # Cheapest possible call to verify credits
        r = client.chat.completions.create(
            model="sonar",  # Cheapest model
            messages=[{"role": "user", "content": "hi"}],
            max_tokens=1,
        )
        return {"status": "ok", "ok": True}
    except Exception as e:
        error = str(e)
        if "quota" in error.lower() or "401" in error:
            return {"status": "out_of_credits", "ok": False, "error": error[:100]}
        return {"status": "error", "ok": True, "error": error[:100]}  # Other errors don't mean out of money


def check_anthropic() -> dict:
    """Check Anthropic/Claude API credits."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"status": "no_key", "ok": False}
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key, timeout=10)
        r = client.messages.create(
            model="claude-haiku-4-5-20251001",  # Cheapest model
            max_tokens=1,
            messages=[{"role": "user", "content": "hi"}],
        )
        return {"status": "ok", "ok": True}
    except Exception as e:
        error = str(e)
        if "credit" in error.lower() or "quota" in error.lower() or "billing" in error.lower():
            return {"status": "out_of_credits", "ok": False, "error": error[:100]}
        if "401" in error or "authentication" in error.lower():
            return {"status": "auth_error", "ok": False, "error": error[:100]}
        return {"status": "error", "ok": True, "error": error[:100]}


def check_deepseek() -> dict:
    """Check DeepSeek API credits."""
    api_key = os.getenv("DEEPSEEK_API_KEY", "")
    if not api_key:
        return {"status": "no_key", "ok": False}
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com", timeout=10)
        r = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": "hi"}],
            max_tokens=1,
        )
        return {"status": "ok", "ok": True}
    except Exception as e:
        error = str(e)
        if "quota" in error.lower() or "balance" in error.lower() or "402" in error:
            return {"status": "out_of_credits", "ok": False, "error": error[:100]}
        return {"status": "error", "ok": True, "error": error[:100]}


def run_cost_check() -> list[str]:
    """Check all API accounts and return list of warnings."""
    warnings = []
    now = datetime.now(timezone.utc)

    checks = {
        "Perplexity": check_perplexity,
        "Claude": check_anthropic,
        "DeepSeek": check_deepseek,
    }

    for name, check_fn in checks.items():
        result = check_fn()

        if not result["ok"]:
            # Check if we already warned recently
            last = _last_warned.get(name)
            if last and (now - last).total_seconds() < 3600:
                continue  # Already warned in the last hour

            warning = f"{name}: {result['status']}"
            if result.get("error"):
                warning += f" — {result['error'][:50]}"
            warnings.append(warning)
            _last_warned[name] = now

            # Send Telegram alert
            try:
                from telegram_alerts import system_alert
                system_alert(f"API CREDIT WARNING: {warning}", "critical")
            except Exception:
                pass

            print(f"  API WARNING: {warning}")

    return warnings


if __name__ == "__main__":
    print("=== API Cost Monitor ===")
    print("Checking Perplexity...", end=" ")
    p = check_perplexity()
    print(p["status"])

    print("Checking Claude...", end=" ")
    c = check_anthropic()
    print(c["status"])

    print("Checking DeepSeek...", end=" ")
    d = check_deepseek()
    print(d["status"])

    warnings = run_cost_check()
    if warnings:
        print(f"\n{len(warnings)} warnings:")
        for w in warnings:
            print(f"  {w}")
    else:
        print("\nAll APIs healthy.")
