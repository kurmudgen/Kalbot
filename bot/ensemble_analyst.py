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


import time
import random

# Retry on transient errors (429, 500, 502, 503, 529)
RETRYABLE_STATUSES = {429, 500, 502, 503, 529}
MAX_RETRIES = 2


def _should_retry(e: Exception) -> bool:
    """Check if an API error is retryable."""
    status = getattr(e, "status_code", None) or getattr(e, "status", None)
    if status and int(status) in RETRYABLE_STATUSES:
        return True
    err_str = str(e).lower()
    if any(code in err_str for code in ("overloaded", "rate_limit", "529", "429", "502", "503")):
        return True
    return False


def _retry_delay(attempt: int, err: Exception) -> float:
    """Exponential backoff with jitter. Respects Retry-After header."""
    retry_after = getattr(err, "headers", {})
    if hasattr(retry_after, "get"):
        ra = retry_after.get("retry-after")
        if ra:
            try:
                return float(ra)
            except ValueError:
                pass
    base = min(2 ** attempt, 30)
    return base * (0.5 + random.random() * 0.5)


def _call_openai_compatible(api_key: str, base_url: str, model: str,
                             messages: list, temperature: float = 0.3) -> str | None:
    """Generic OpenAI-compatible API call with timeout and retry."""
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key, base_url=base_url, timeout=60.0)

        last_error = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=temperature,
                )
                return response.choices[0].message.content
            except Exception as e:
                last_error = e
                if attempt < MAX_RETRIES and _should_retry(e):
                    delay = _retry_delay(attempt, e)
                    print(f"    Retry {attempt + 1}/{MAX_RETRIES} for {base_url} after {delay:.1f}s — {e}")
                    time.sleep(delay)
                else:
                    raise
        return None
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

        last_error = None
        for attempt in range(MAX_RETRIES + 1):
            try:
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
                last_error = e
                if attempt < MAX_RETRIES and _should_retry(e):
                    delay = _retry_delay(attempt, e)
                    print(f"    Claude retry {attempt + 1}/{MAX_RETRIES} after {delay:.1f}s — {e}")
                    time.sleep(delay)
                else:
                    raise
        return None
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
    """Tiered ensemble escalation pipeline.

    Tier 1: Local-only fast path (conf>0.92, prob extreme) -> skip all cloud
    Tier 2: Gemini only (agrees with local within 0.15) -> skip Claude/DeepSeek
    Tier 3: Gemini + DeepSeek (agree within 0.10) -> skip Claude
    Tier 4: Full ensemble (Gemini + DeepSeek + Claude tiebreaker)
    Tier 5: Full research (+ Perplexity for live-data categories)

    All calls are cached. Each tier only fires if the previous tier couldn't resolve.
    """
    title = market["title"]
    category = market["category"]
    market_price = market["market_price"]
    local_conf = market.get("confidence", 0.5)
    local_prob = market.get("model_probability", 0.5)
    price_gap = abs(local_prob - market_price)

    # ── TIER 1: Fast path — local model is extremely confident ──
    if local_conf > 0.92 and (local_prob < 0.08 or local_prob > 0.92):
        # Obvious market — local model alone is sufficient
        return {
            "perplexity": None, "claude": None, "deepseek": None, "gemini": None,
            "consensus": {
                "probability": local_prob,
                "confidence": local_conf,
                "providers": ["local"],
                "individual_probs": [local_prob],
                "spread": 0,
                "dampened_edge": local_prob - market_price,
                "weights_used": {"local": 1.0},
            },
            "research": "Fast path: local model high confidence on obvious market.",
            "estimates": [{"probability": local_prob, "confidence": local_conf, "_provider": "local"}],
            "escalation_tier": "local_only",
        }

    # ── TIER 2: Gemini Fast (unlimited daily, cheapest) ──
    gemini_result = None
    try:
        from ensemble_gemini import call_gemini, call_gemini_quality
        from api_cache import get_cached, store_cached, check_daily_budget

        # Cache check for fast model
        cached_raw = get_cached("gemini", title)
        if cached_raw:
            gemini_result = _parse_json(cached_raw)
            if gemini_result:
                gemini_result["_provider"] = "gemini"
                gemini_result["_cached"] = True
        elif check_daily_budget("gemini"):
            eval_prompt = EVALUATE_PROMPT.format(
                title=title, category=category,
                market_price=market_price, research="Use your general knowledge.",
            )
            gemini_result = call_gemini(eval_prompt)
            if gemini_result:
                store_cached("gemini", title, json.dumps(gemini_result))
    except Exception as e:
        print(f"    Gemini tier error: {e}")

    if gemini_result:
        g_prob = gemini_result.get("probability", 0.5)
        g_conf = gemini_result.get("confidence", 0.5)

        # Weather with NWS data: accept Gemini at 0.60+ without escalation
        # NWS forecast is the settlement source — multi-model consensus is
        # redundant when the answer is deterministic from NWS data.
        if category == "weather" and g_conf >= 0.60:
            try:
                import sys as _sys
                _dp = os.path.join(os.path.dirname(__file__), "..", "data")
                if _dp not in _sys.path:
                    _sys.path.insert(0, _dp)
                from weather_nws_feed import load_forecasts
                _fc = load_forecasts()
                if _fc and len(_fc) > 0:
                    avg_prob = (g_prob * 0.6 + local_prob * 0.4)
                    avg_conf = (g_conf * 0.6 + local_conf * 0.4)
                    return {
                        "perplexity": None, "claude": None, "deepseek": None,
                        "gemini": gemini_result,
                        "consensus": {
                            "probability": avg_prob,
                            "confidence": avg_conf,
                            "providers": ["local", "gemini"],
                            "individual_probs": [local_prob, g_prob],
                            "spread": abs(g_prob - local_prob),
                            "dampened_edge": avg_prob - market_price,
                            "weights_used": {"local": 0.4, "gemini": 0.6},
                        },
                        "research": "Weather NWS tier: Gemini + local with NWS data.",
                        "estimates": [
                            {"probability": local_prob, "confidence": local_conf, "_provider": "local"},
                            gemini_result,
                        ],
                        "escalation_tier": "weather_nws_gemini",
                    }
            except Exception:
                pass

        # Fast model conf > 0.80 and agrees with local -> accept, done
        if g_conf > 0.80 and abs(g_prob - local_prob) < 0.15:
            avg_prob = (g_prob + local_prob) / 2
            avg_conf = (g_conf + local_conf) / 2
            return {
                "perplexity": None, "claude": None, "deepseek": None, "gemini": gemini_result,
                "consensus": {
                    "probability": avg_prob,
                    "confidence": avg_conf,
                    "providers": ["local", "gemini"],
                    "individual_probs": [local_prob, g_prob],
                    "spread": abs(g_prob - local_prob),
                    "dampened_edge": avg_prob - market_price,
                    "weights_used": {"local": 0.4, "gemini": 0.6},
                },
                "research": "Gemini-fast tier: high confidence, agrees with local.",
                "estimates": [
                    {"probability": local_prob, "confidence": local_conf, "_provider": "local"},
                    gemini_result,
                ],
                "escalation_tier": "gemini_fast_only",
            }

        # Fast model conf 0.65-0.80 -> escalate to Gemini quality model
        if 0.65 <= g_conf <= 0.80:
            try:
                quality_prompt = EVALUATE_PROMPT.format(
                    title=title, category=category,
                    market_price=market_price,
                    research="Use your general knowledge. Provide a careful analysis.",
                )
                gemini_quality = call_gemini_quality(quality_prompt)
                if gemini_quality:
                    q_prob = gemini_quality.get("probability", 0.5)
                    q_conf = gemini_quality.get("confidence", 0.5)

                    # Quality model agrees with fast within 0.10 -> accept average
                    if abs(q_prob - g_prob) < 0.10:
                        avg_prob = (q_prob + g_prob + local_prob) / 3
                        avg_conf = (q_conf + g_conf + local_conf) / 3
                        return {
                            "perplexity": None, "claude": None, "deepseek": None,
                            "gemini": gemini_quality,
                            "consensus": {
                                "probability": avg_prob,
                                "confidence": avg_conf,
                                "providers": ["local", "gemini_fast", "gemini_quality"],
                                "individual_probs": [local_prob, g_prob, q_prob],
                                "spread": max(local_prob, g_prob, q_prob) - min(local_prob, g_prob, q_prob),
                                "dampened_edge": avg_prob - market_price,
                                "weights_used": {"local": 0.3, "gemini_fast": 0.3, "gemini_quality": 0.4},
                            },
                            "research": "Gemini-dual tier: quality confirmed fast model.",
                            "estimates": [
                                {"probability": local_prob, "confidence": local_conf, "_provider": "local"},
                                gemini_result,
                                gemini_quality,
                            ],
                            "escalation_tier": "gemini_dual",
                        }
                    # Quality model disagrees -> fall through to DeepSeek (Tier 3)
            except Exception:
                pass

        # Fast model conf < 0.65 OR quality disagreed -> fall through to DeepSeek

    # ── TIER 3: Gemini + DeepSeek ──
    # Build research context for evaluation
    research_text = "No real-time research available."
    try:
        from news_pool import format_context
        pooled = format_context(category)
        if pooled:
            research_text = pooled
    except Exception:
        pass

    eval_prompt = EVALUATE_PROMPT.format(
        title=title, category=category,
        market_price=market_price, research=research_text,
    )

    deepseek_result = call_deepseek(eval_prompt, title=title)

    if gemini_result and deepseek_result:
        g_prob = gemini_result.get("probability", 0.5)
        d_prob = deepseek_result.get("probability", 0.5)

        # If Gemini and DeepSeek agree within 0.10, accept their average
        if abs(g_prob - d_prob) < 0.10:
            estimates = [gemini_result, deepseek_result]
            consensus = check_consensus(estimates, market_price, category=category, title=title)
            if consensus:
                return {
                    "perplexity": None, "claude": None,
                    "deepseek": deepseek_result, "gemini": gemini_result,
                    "consensus": consensus,
                    "research": research_text,
                    "estimates": estimates,
                    "escalation_tier": "gemini_deepseek",
                }

    # ── TIER 4: Full ensemble (+ Claude tiebreaker) ──
    claude_result = call_claude(eval_prompt, title=title)

    estimates = []
    if gemini_result:
        estimates.append(gemini_result)
    if deepseek_result:
        estimates.append(deepseek_result)
    if claude_result:
        estimates.append(claude_result)

    pplx_result = None
    escalation_tier = "full_ensemble"

    # ── TIER 5: Full research (+ Perplexity) ──
    # Only for live-data categories with borderline confidence across all models
    if needs_perplexity(category, local_conf, price_gap):
        all_confs = [e.get("confidence", 0.5) for e in estimates]
        all_borderline = all(0.65 <= c <= 0.80 for c in all_confs) if all_confs else False

        if all_borderline:
            research_prompt = RESEARCH_PROMPT.format(
                title=title, category=category, market_price=market_price,
            )
            pplx_result = call_perplexity(research_prompt, title=title)
            if pplx_result:
                research_text = pplx_result.get("research", pplx_result.get("reasoning", ""))
                estimates.append(pplx_result)
                escalation_tier = "full_research"

                try:
                    from news_pool import store_research
                    store_research(title, category, research_text, "perplexity")
                except Exception:
                    pass

    if len(estimates) < MIN_MODELS_REQUIRED:
        return None

    consensus = check_consensus(estimates, market_price, category=category, title=title)

    return {
        "perplexity": pplx_result,
        "claude": claude_result,
        "deepseek": deepseek_result,
        "gemini": gemini_result,
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

    print(f"Tiered ensemble: {len(markets)} markets (local -> gemini -> deepseek -> claude -> perplexity)")

    tier_counts = {"local_only": 0, "gemini_only": 0, "gemini_deepseek": 0,
                   "full_ensemble": 0, "full_research": 0, "skip": 0}

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
            tier_counts["skip"] += 1
            continue

        tier = result.get("escalation_tier", "unknown")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

        # Log individual estimates
        for est in result["estimates"]:
            provider = est.get("_provider", "?")
            cached = " (cached)" if est.get("_cached") else ""
            print(f"  {provider}: prob={est['probability']:.2f} conf={est['confidence']:.2f}{cached}")
        print(f"  TIER: {tier}")

        # Store ensemble details
        pplx = result.get("perplexity") or {}
        claude = result.get("claude") or {}
        ds = result.get("deepseek") or {}
        gem = result.get("gemini") or {}

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
                "execution_tier": tier,
            })
        else:
            print("  NO CONSENSUS — skipping")

        conn.commit()

    conn.close()
    print(f"\n{len(analyzed)}/{len(markets)} markets reached consensus")
    print(f"Tier distribution: {' | '.join(f'{k}:{v}' for k, v in tier_counts.items() if v > 0)}")
    return analyzed


if __name__ == "__main__":
    analyze_markets()
