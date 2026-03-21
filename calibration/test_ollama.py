"""Test Ollama local model connectivity and JSON response parsing."""

import json
import requests
import sys


OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "qwen2.5:14b"

PROMPT = """You are a prediction market analyst. Estimate the probability of the following event resolving YES.

Market question: "Will the Federal Reserve cut interest rates at the next FOMC meeting?"
Category: Economics / Federal Reserve
Current YES price: 0.35

Respond ONLY with valid JSON:
{"probability": <float 0.0-1.0>, "confidence": <float 0.0-1.0>, "relevant": true, "reasoning": "<one sentence>"}
"""


def test_ollama():
    print(f"Testing Ollama model: {MODEL}")
    print("-" * 50)

    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL,
                "prompt": PROMPT,
                "stream": False,
                "options": {"temperature": 0.3},
            },
            timeout=120,
        )
        response.raise_for_status()
    except requests.ConnectionError:
        print("FAIL: Cannot connect to Ollama. Is it running?")
        print("Start it with: ollama serve")
        sys.exit(1)
    except requests.Timeout:
        print("FAIL: Ollama request timed out (120s)")
        sys.exit(1)

    raw = response.json().get("response", "")
    print(f"Raw response:\n{raw}\n")

    # Try to extract JSON from the response
    try:
        # Try direct parse first
        result = json.loads(raw.strip())
    except json.JSONDecodeError:
        # Try to find JSON in the response
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start == -1 or end == 0:
            print("FAIL: No JSON object found in response")
            sys.exit(1)
        try:
            result = json.loads(raw[start:end])
        except json.JSONDecodeError as e:
            print(f"FAIL: Could not parse JSON: {e}")
            sys.exit(1)

    # Validate fields
    required = {"probability", "confidence", "relevant", "reasoning"}
    missing = required - set(result.keys())
    if missing:
        print(f"FAIL: Missing fields: {missing}")
        sys.exit(1)

    prob = result["probability"]
    conf = result["confidence"]
    if not (0.0 <= prob <= 1.0):
        print(f"FAIL: probability {prob} not in [0, 1]")
        sys.exit(1)
    if not (0.0 <= conf <= 1.0):
        print(f"FAIL: confidence {conf} not in [0, 1]")
        sys.exit(1)
    if not isinstance(result["relevant"], bool):
        print(f"FAIL: relevant should be bool, got {type(result['relevant'])}")
        sys.exit(1)

    print("Parsed result:")
    print(f"  probability: {prob}")
    print(f"  confidence:  {conf}")
    print(f"  relevant:    {result['relevant']}")
    print(f"  reasoning:   {result['reasoning']}")
    print()
    print("PASS: Ollama model responds with valid JSON")


if __name__ == "__main__":
    test_ollama()
