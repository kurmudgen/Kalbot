"""
Gemini dual-model integration for ensemble analyst.

Fast model (Tier 2): gemini-2.0-flash-lite — unlimited RPD, 4K RPM, cheapest.
Quality model (Tier 3): gemini-2.5-flash — 10K RPD, better reasoning.

Fast model handles obvious markets. Quality model only fires when fast
returns borderline confidence (0.65-0.80).
"""

import json
import os
import warnings

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

GEMINI_FAST_MODEL = os.getenv("GEMINI_FAST_MODEL", "gemini-2.5-flash-lite")
GEMINI_QUALITY_MODEL = os.getenv("GEMINI_QUALITY_MODEL", "gemini-2.5-flash")


def _call_model(model_name: str, prompt: str) -> dict | None:
    """Call a specific Gemini model."""
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        return None

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            import google.generativeai as genai

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name)

        response = model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                temperature=0.3,
                max_output_tokens=300,
            ),
        )

        raw = response.text
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start == -1 or end == 0:
            return None

        result = json.loads(raw[start:end])
        result["_provider"] = "gemini"
        result["_model"] = model_name
        return result
    except Exception as e:
        print(f"    Gemini ({model_name}) error: {e}")
        return None


def call_gemini(prompt: str) -> dict | None:
    """Call Gemini fast model (Tier 2 screening). Unlimited daily calls."""
    return _call_model(GEMINI_FAST_MODEL, prompt)


def call_gemini_quality(prompt: str) -> dict | None:
    """Call Gemini quality model (Tier 3). 10K/day limit — use sparingly."""
    return _call_model(GEMINI_QUALITY_MODEL, prompt)


if __name__ == "__main__":
    print(f"Fast model: {GEMINI_FAST_MODEL}")
    print(f"Quality model: {GEMINI_QUALITY_MODEL}")

    print("\nTesting fast model...")
    result = call_gemini(
        'Estimate probability: "Will BTC be above $100,000 on March 30, 2026?"\n'
        'Respond ONLY with JSON: {"probability": <float>, "confidence": <float>, "reasoning": "<one sentence>"}'
    )
    if result:
        print(f"  OK: prob={result.get('probability')}, conf={result.get('confidence')}, model={result.get('_model')}")
    else:
        print("  FAILED")

    print("\nTesting quality model...")
    result2 = call_gemini_quality(
        'Estimate probability: "Will initial jobless claims be above 220,000 next week?"\n'
        'Respond ONLY with JSON: {"probability": <float>, "confidence": <float>, "reasoning": "<one sentence>"}'
    )
    if result2:
        print(f"  OK: prob={result2.get('probability')}, conf={result2.get('confidence')}, model={result2.get('_model')}")
    else:
        print("  FAILED")
