"""
Gemini Flash integration for ensemble analyst.
Uses google-generativeai SDK. Cheapest cloud model in the ensemble.
"""

import json
import os

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)


def call_gemini(prompt: str) -> dict | None:
    """Call Gemini 2.0 Flash via Google GenerativeAI SDK."""
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        return None

    try:
        import google.generativeai as genai

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.0-flash")

        response = model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                temperature=0.3,
                max_output_tokens=300,
            ),
        )

        raw = response.text
        # Parse JSON from response
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start == -1 or end == 0:
            return None

        result = json.loads(raw[start:end])
        result["_provider"] = "gemini"
        return result
    except Exception as e:
        print(f"    Gemini error: {e}")
        return None


if __name__ == "__main__":
    result = call_gemini(
        'Estimate the probability of this resolving YES: "Will BTC be above $100,000 on March 30, 2026?"\n'
        'Respond ONLY with JSON: {"probability": <float>, "confidence": <float>, "reasoning": "<one sentence>"}'
    )
    if result:
        print(f"Gemini: prob={result.get('probability')}, conf={result.get('confidence')}")
        print(f"Reasoning: {result.get('reasoning')}")
    else:
        print("Gemini call failed")
