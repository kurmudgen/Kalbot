"""
Ensemble analyst: Perplexity researches, then Claude + DeepSeek evaluate independently.
Only passes markets where models reach consensus. Replaces cloud_analyst.py.

Pipeline:
1. Perplexity sonar-pro: searches web for real-time data, produces research brief + probability
2. Claude sonnet: reads research brief, gives independent probability
3. DeepSeek reasoner: reads research brief, gives independent probability
4. Consensus check: all must agree on direction, average probability used

Cost: ~$0.015/market
"""

import json
import os
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

SCORES_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "filter_scores.sqlite")
ANALYST_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "analyst_scores.sqlite")

RESEARCH_PROMPT = """You are a prediction market research analyst. Search for the latest data relevant to this market.

Market: {title}
Category: {category}
Current market price (implied probability): {market_price:.2f}

Search for:
- Latest forecasts, data releases, or official reports relevant to this question
- Consensus expectations from analysts or forecasters
- Any recent news that could shift the probability

Provide your findings and probability estimate.
Respond ONLY with valid JSON:
{{"probability": <float 0.0-1.0>, "confidence": <float 0.0-1.0>, "research": "<key findings from your search>", "reasoning": "<your analysis>"}}
"""

EVALUATE_PROMPT = """You are a calibrated prediction market analyst. Based on the research below, estimate the probability of this market resolving YES.

Market: {title}
Category: {category}
Current market price (implied probability): {market_price:.2f}

Research findings:
{research}

IMPORTANT: Give YOUR independent probability estimate. Do not simply agree with the researcher.
Consider: base rates, whether the evidence is strong enough to deviate from the market price,
and whether the research findings actually change the probability meaningfully.

Respond ONLY with valid JSON:
{{"probability": <float 0.0-1.0>, "confidence": <float 0.0-1.0>, "reasoning": "<one sentence>"}}
"""

CONSENSUS_THRESHOLD = 0.6  # All models must agree this side of 0.5
MIN_MODELS_REQUIRED = 2    # At least 2 models must succeed


def init_analyst_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(ANALYST_DB), exist_ok=True)
    conn = sqlite3.connect(ANALYST_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analyst_scores (
            ticker TEXT PRIMARY KEY,
            title TEXT,
            category TEXT,
            local_probability REAL,
            local_confidence REAL,
            cloud_probability REAL,
            cloud_confidence REAL,
            cloud_reasoning TEXT,
            market_price REAL,
            price_gap REAL,
            cloud_provider TEXT,
            analyzed_at TEXT
        )
    """)
    # Extended table for ensemble details
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ensemble_details (
            ticker TEXT PRIMARY KEY,
            perplexity_prob REAL,
            perplexity_conf REAL,
            perplexity_research TEXT,
            claude_prob REAL,
            claude_conf REAL,
            deepseek_prob REAL,
            deepseek_conf REAL,
            consensus INTEGER,
            consensus_prob REAL,
            consensus_conf REAL,
            models_used TEXT,
            analyzed_at TEXT
        )
    """)
    conn.commit()
    return conn


def get_filtered_markets() -> list[dict]:
    if not os.path.exists(SCORES_DB):
        return []
    conn = sqlite3.connect(SCORES_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM filter_scores WHERE passed_filter = 1"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _call_openai_compatible(api_key: str, base_url: str, model: str,
                             messages: list, temperature: float = 0.3) -> str | None:
    """Generic OpenAI-compatible API call with timeout."""
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key, base_url=base_url, timeout=60.0)
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"    API error ({base_url}): {e}")
        return None


def _parse_json(raw: str) -> dict | None:
    if not raw:
        return None
    start = raw.find("{")
    end = raw.rfind("}") + 1
    if start == -1 or end == 0:
        return None
    try:
        return json.loads(raw[start:end])
    except json.JSONDecodeError:
        return None


def call_perplexity(prompt: str, title: str = "") -> dict | None:
    # Cache check
    try:
        from api_cache import get_cached, store_cached, check_daily_budget
        cached = get_cached("perplexity", title)
        if cached:
            result = _parse_json(cached)
            if result:
                result["_provider"] = "perplexity"
                result["_cached"] = True
                return result
        if not check_daily_budget("perplexity"):
            print("    Perplexity: daily budget exhausted")
            return None
    except Exception:
        pass

    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if not api_key:
        return None
    raw = _call_openai_compatible(
        api_key, "https://api.perplexity.ai", "sonar-pro",
        [
            {"role": "system", "content": "You are a prediction market research analyst. Search for relevant data. Respond only with valid JSON."},
            {"role": "user", "content": prompt},
        ],
    )
    # Store in cache
    if raw and title:
        try:
            from api_cache import store_cached
            store_cached("perplexity", title, raw)
        except Exception:
            pass
    result = _parse_json(raw)
    if result:
        result["_provider"] = "perplexity"
    return result


def call_claude(prompt: str, title: str = "") -> dict | None:
    # Cache check
    try:
        from api_cache import get_cached, store_cached, check_daily_budget
        cached = get_cached("claude", title)
        if cached:
            result = _parse_json(cached)
            if result:
                result["_provider"] = "claude"
                result["_cached"] = True
                return result
        if not check_daily_budget("claude"):
            print("    Claude: daily budget exhausted")
            return None
    except Exception:
        pass

    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key, timeout=60.0)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text
        # Store in cache
        if raw and title:
            try:
                from api_cache import store_cached
                store_cached("claude", title, raw)
            except Exception:
                pass
        result = _parse_json(raw)
        if result:
            result["_provider"] = "claude"
        return result
    except Exception as e:
        print(f"    Claude error: {e}")
        return None


def call_deepseek(prompt: str, title: str = "") -> dict | None:
    # Cache check
    try:
        from api_cache import get_cached, store_cached, check_daily_budget
        cached = get_cached("deepseek", title)
        if cached:
            result = _parse_json(cached)
            if result:
                result["_provider"] = "deepseek"
                result["_cached"] = True
                return result
        if not check_daily_budget("deepseek"):
            print("    DeepSeek: daily budget exhausted")
            return None
    except Exception:
        pass

    api_key = os.getenv("DEEPSEEK_API_KEY", "")
    if not api_key:
        return None
    raw = _call_openai_compatible(
        api_key, "https://api.deepseek.com", "deepseek-chat",
        [{"role": "user", "content": prompt}],
    )
    # Store in cache
    if raw and title:
        try:
            from api_cache import store_cached
            store_cached("deepseek", title, raw)
        except Exception:
            pass
    result = _parse_json(raw)
    if result:
        result["_provider"] = "deepseek"
    return result


# Categories that need live web search (Perplexity)
LIVE_DATA_CATEGORIES = {"weather", "tsa"}

def needs_perplexity(category: str, local_conf: float, price_gap: float) -> bool:
    """Perplexity priority gate: only fire for time-sensitive data with strong signal."""
    # Always skip if confidence or gap too low
    if local_conf < 0.70 or price_gap < 0.10:
        return False
    # Only fire for categories with rapidly changing data
    if category in LIVE_DATA_CATEGORIES:
        return True
    # Economic data between releases doesn't need web search
    return False


def check_consensus(estimates: list[dict], market_price: float,
                     category: str = "", title: str = "") -> dict | None:
    """Check if models agree on direction, apply adaptive weighting and
    calibration pipeline, return consensus estimate."""
    if len(estimates) < MIN_MODELS_REQUIRED:
        return None

    probs = [e["probability"] for e in estimates]
    confs = [e["confidence"] for e in estimates]

    # Check direction agreement: 2/3 majority (not unanimous)
    above = sum(1 for p in probs if p > market_price)
    below = sum(1 for p in probs if p < market_price)
    majority = len(probs) / 2

    if not (above > majority or below > majority):
        return None  # No majority on direction

    # Confidence floor: average confidence must be above 0.65
    avg_conf_check = sum(confs) / len(confs)
    if avg_conf_check < 0.65:
        return None  # Models agree but aren't confident — skip

    # Check outcome side: 2/3 majority
    yes_votes = sum(1 for p in probs if p > 0.5)
    no_votes = sum(1 for p in probs if p <= 0.5)

    if not (yes_votes > majority or no_votes > majority):
        return None  # No majority on outcome

    # Get adaptive weights based on historical per-model performance
    try:
        from adaptive_weights import get_adaptive_weights
        weights = get_adaptive_weights(category)
    except Exception:
        weights = {"perplexity": 0.40, "claude": 0.35, "deepseek": 0.25}

    # Weighted average using adaptive weights
    weighted_prob = 0.0
    weighted_conf = 0.0
    total_weight = 0.0
    providers = []

    for est in estimates:
        provider = est.get("_provider", "unknown")
        w = weights.get(provider, 0.2) * max(est["confidence"], 0.1)
        weighted_prob += est["probability"] * w
        weighted_conf += est["confidence"] * w
        total_weight += w
        providers.append(provider)

    if total_weight > 0:
        avg_prob = weighted_prob / total_weight
        avg_conf = weighted_conf / total_weight
    else:
        avg_prob = sum(probs) / len(probs)
        avg_conf = sum(confs) / len(confs)

    # Ensemble spread (disagreement measure)
    spread = max(probs) - min(probs)

    # Run calibration pipeline
    try:
        from calibration_pipeline import calibrate_probability
        cal = calibrate_probability(
            raw_prob=avg_prob,
            confidence=avg_conf,
            market_price=market_price,
            ensemble_spread=spread,
            title=title,
        )
        final_prob = cal["calibrated_probability"]
        dampened_edge = cal["dampened_edge"]
    except Exception:
        final_prob = avg_prob
        dampened_edge = avg_prob - market_price

    return {
        "probability": final_prob,
        "confidence": avg_conf,
        "providers": providers,
        "individual_probs": probs,
        "spread": spread,
        "dampened_edge": dampened_edge,
        "weights_used": weights,
    }


def analyze_market_ensemble(market: dict) -> dict | None:
    """Run the full ensemble pipeline on a single market.

    Perplexity priority gate: only called for time-sensitive categories
    (weather, tsa) when local model shows strong signal. Otherwise
    Claude + DeepSeek evaluate using pooled research context.
    All calls are cached — repeated markets within TTL window get instant results.
    """
    title = market["title"]
    category = market["category"]
    market_price = market["market_price"]
    local_conf = market.get("confidence", 0.5)
    local_prob = market.get("model_probability", 0.5)
    price_gap = abs(local_prob - market_price)

    research_text = ""
    estimates = []
    pplx_result = None
    escalation_tier = "claude_deepseek"

    # Perplexity priority gate: only for live-data categories with strong signal
    if needs_perplexity(category, local_conf, price_gap):
        research_prompt = RESEARCH_PROMPT.format(
            title=title, category=category, market_price=market_price,
        )
        pplx_result = call_perplexity(research_prompt, title=title)

        if pplx_result:
            research_text = pplx_result.get("research", pplx_result.get("reasoning", ""))
            estimates.append(pplx_result)
            escalation_tier = "full_ensemble"

            try:
                from news_pool import store_research
                store_research(title, category, research_text, "perplexity")
            except Exception:
                pass

    if not research_text:
        research_text = "No real-time research available."

    # Inject recent pooled research as additional context
    try:
        from news_pool import format_context
        pooled = format_context(category)
        if pooled:
            research_text += "\n\n" + pooled
    except Exception:
        pass

    # Claude and DeepSeek evaluate independently
    eval_prompt = EVALUATE_PROMPT.format(
        title=title, category=category,
        market_price=market_price, research=research_text,
    )

    claude_result = call_claude(eval_prompt, title=title)
    if claude_result:
        estimates.append(claude_result)

    deepseek_result = call_deepseek(eval_prompt, title=title)
    if deepseek_result:
        estimates.append(deepseek_result)

    if len(estimates) < MIN_MODELS_REQUIRED:
        return None

    # Check consensus with adaptive weighting + calibration
    consensus = check_consensus(estimates, market_price, category=category, title=title)

    return {
        "perplexity": pplx_result,
        "claude": claude_result,
        "deepseek": deepseek_result,
        "consensus": consensus,
        "research": research_text,
        "estimates": estimates,
        "escalation_tier": escalation_tier,
    }


def analyze_markets(markets: list[dict] | None = None) -> list[dict]:
    """Run ensemble analysis on all filtered markets."""
    if markets is None:
        markets = get_filtered_markets()

    if not markets:
        print("No markets to analyze.")
        return []

    conn = init_analyst_db()
    analyzed = []

    print(f"Ensemble analysis: {len(markets)} markets (Perplexity -> Claude + DeepSeek)")

    for i, m in enumerate(markets):
        ticker = m["ticker"]
        title = m["title"]
        category = m["category"]
        market_price = m["market_price"]
        local_prob = m.get("model_probability", 0.5)
        local_conf = m.get("confidence", 0.5)

        print(f"\n[{i+1}/{len(markets)}] {title[:60]}...")

        result = analyze_market_ensemble(m)

        if result is None:
            print("  SKIP (insufficient model responses)")
            continue

        # Log individual estimates
        for est in result["estimates"]:
            provider = est.get("_provider", "?")
            print(f"  {provider}: prob={est['probability']:.2f} conf={est['confidence']:.2f}")

        # Store ensemble details
        pplx = result.get("perplexity") or {}
        claude = result.get("claude") or {}
        ds = result.get("deepseek") or {}

        conn.execute(
            """INSERT OR REPLACE INTO ensemble_details
               (ticker, perplexity_prob, perplexity_conf, perplexity_research,
                claude_prob, claude_conf, deepseek_prob, deepseek_conf,
                consensus, consensus_prob, consensus_conf, models_used, analyzed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker,
             pplx.get("probability"), pplx.get("confidence"), result.get("research", ""),
             claude.get("probability"), claude.get("confidence"),
             ds.get("probability"), ds.get("confidence"),
             1 if result["consensus"] else 0,
             result["consensus"]["probability"] if result["consensus"] else None,
             result["consensus"]["confidence"] if result["consensus"] else None,
             ",".join(e.get("_provider", "?") for e in result["estimates"]),
             datetime.now(timezone.utc).isoformat()),
        )

        if result["consensus"]:
            cons = result["consensus"]
            price_gap = abs(cons["probability"] - market_price)
            reasoning = result.get("research", "")

            print(f"  CONSENSUS: prob={cons['probability']:.2f} conf={cons['confidence']:.2f} gap={price_gap:.2f}")

            # Write to the main analyst_scores table (executor reads this)
            conn.execute(
                """INSERT OR REPLACE INTO analyst_scores
                   (ticker, title, category, local_probability, local_confidence,
                    cloud_probability, cloud_confidence, cloud_reasoning,
                    market_price, price_gap, cloud_provider, analyzed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ticker, title, category, local_prob, local_conf,
                 cons["probability"], cons["confidence"], reasoning,
                 market_price, price_gap,
                 "+".join(cons["providers"]),
                 datetime.now(timezone.utc).isoformat()),
            )

            analyzed.append({
                "ticker": ticker,
                "title": title,
                "category": category,
                "cloud_probability": cons["probability"],
                "cloud_confidence": cons["confidence"],
                "market_price": market_price,
                "price_gap": price_gap,
                "cloud_reasoning": reasoning,
            })
        else:
            print("  NO CONSENSUS — skipping")

        conn.commit()

    conn.close()
    print(f"\n{len(analyzed)}/{len(markets)} markets reached consensus")
    return analyzed


if __name__ == "__main__":
    analyze_markets()
