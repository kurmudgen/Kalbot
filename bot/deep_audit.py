"""Deep end-to-end pipeline trace. Picks a real market and walks it through every gate."""
import sqlite3, os, sys, json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "data"))
os.chdir(os.path.dirname(__file__))

BASE = os.path.join(os.path.dirname(__file__), "..")

print("=" * 70)
print("DEEP END-TO-END TRACE — " + datetime.now().strftime("%Y-%m-%d %H:%M"))
print("=" * 70)

# ── STEP 1: Scanner ──
print("\n[1] SCANNER — markets.sqlite")
mconn = sqlite3.connect(os.path.join(BASE, "data/live/markets.sqlite"))
mconn.row_factory = sqlite3.Row
# Tradeable range (not 0/100)
candidates = mconn.execute(
    "SELECT * FROM markets WHERE ticker LIKE 'KXHIGH%' AND status IN ('open','active') "
    "AND last_price > 5 AND last_price < 95 ORDER BY volume DESC LIMIT 5"
).fetchall()
all_wx = mconn.execute(
    "SELECT ticker, last_price, yes_bid, yes_ask, volume FROM markets "
    "WHERE ticker LIKE 'KXHIGH%' AND status IN ('open','active') ORDER BY volume DESC LIMIT 15"
).fetchall()
mconn.close()

print(f"  Tradeable (5-95c): {len(candidates)}")
print(f"  All weather: {len(all_wx)}")
for w in all_wx[:10]:
    tag = " <-- TARGET" if candidates and w["ticker"] == candidates[0]["ticker"] else ""
    print(f"    {w['ticker']:<38} price={w['last_price']:>3}c bid={w['yes_bid'] or 0} ask={w['yes_ask'] or 0} vol={w['volume'] or 0}{tag}")

if not candidates:
    print("\n  PROBLEM: No weather markets in 5-95c range.")
    print("  All markets are at 0-5c or 95-100c (already decided).")
    print("  Need fresh markets with uncertain outcomes.")
    # Still continue with best available
    if all_wx:
        candidates = [all_wx[0]]

if not candidates:
    print("  NO WEATHER MARKETS AT ALL")
    sys.exit()

target = dict(candidates[0])
ticker = target["ticker"]
market_price = target["last_price"] / 100.0
print(f"\n  TARGET: {ticker} at {market_price:.2f}")

# ── STEP 2: Local Filter ──
print("\n[2] LOCAL FILTER — filter_scores.sqlite")
fconn = sqlite3.connect(os.path.join(BASE, "data/live/filter_scores.sqlite"))
fconn.row_factory = sqlite3.Row
fscore = fconn.execute(
    "SELECT * FROM filter_scores WHERE ticker=? ORDER BY scored_at DESC LIMIT 1", (ticker,)
).fetchone()
if fscore:
    print(f"  Passed: {bool(fscore['passed_filter'])}")
    print(f"  Local p={fscore['model_probability']:.2f} c={fscore['confidence']:.2f} gap={fscore['price_gap']:.2f}")
    print(f"  Scored: {fscore['scored_at'][:19]}")
    if not fscore["passed_filter"]:
        print(f"  BLOCKED: Local filter rejected this market")
        print(f"  Reasoning: {(fscore.get('reasoning') or '')[:120]}")
else:
    print(f"  NOT IN FILTER CACHE — hasn't been scored by Ollama yet")
    print(f"  This means scanner found it but filter queue hasn't reached it")
fconn.close()

# ── STEP 3: Ensemble Analyst ──
print("\n[3] ENSEMBLE ANALYST — analyst_scores.sqlite")
aconn = sqlite3.connect(os.path.join(BASE, "data/live/analyst_scores.sqlite"))
aconn.row_factory = sqlite3.Row
ascore = aconn.execute(
    "SELECT * FROM analyst_scores WHERE ticker=? ORDER BY analyzed_at DESC LIMIT 1", (ticker,)
).fetchone()
if ascore:
    print(f"  Cloud p={ascore['cloud_probability']:.2f} c={ascore['cloud_confidence']:.2f}")
    print(f"  Provider: {ascore['cloud_provider']}")
    print(f"  Gap: {ascore['price_gap']:.2f}")
    print(f"  Analyzed: {ascore['analyzed_at'][:19]}")
else:
    print(f"  NOT SCORED by ensemble")
    # Check API budget
    try:
        bcconn = sqlite3.connect(os.path.join(BASE, "logs/api_cache.sqlite"))
        bcconn.row_factory = sqlite3.Row
        rows = bcconn.execute(
            "SELECT model, call_count FROM daily_call_counts WHERE date >= '2026-04-03'"
        ).fetchall()
        limits = {"gemini": 200, "deepseek": 100, "claude": 75, "perplexity": 50}
        print("  API Budget:")
        for r in rows:
            lim = limits.get(r["model"], 999)
            exhausted = r["call_count"] >= lim
            print(f"    {r['model']:<12} {r['call_count']}/{lim} {'EXHAUSTED' if exhausted else 'ok'}")
        bcconn.close()
    except Exception as e:
        print(f"  Budget check error: {e}")
aconn.close()

# ── STEP 4: Executor Gate Chain ──
print("\n[4] EXECUTOR GATE CHAIN")

# Use best available score
if ascore:
    cloud_prob = ascore["cloud_probability"]
    cloud_conf = ascore["cloud_confidence"]
    price_gap = ascore["price_gap"]
    source = "ensemble"
elif fscore and fscore["passed_filter"]:
    cloud_prob = fscore["model_probability"]
    cloud_conf = fscore["confidence"]
    price_gap = fscore["price_gap"]
    source = "filter_only"
else:
    print("  No scores — can't trace gates")
    sys.exit()

print(f"  Using {source} scores: p={cloud_prob:.2f} c={cloud_conf:.2f} gap={price_gap:.2f}")

from executor import (extract_weather_city, extract_threshold_temp,
                       CATEGORY_CONFIDENCE, CONFIDENCE_MIN, PRICE_GAP_MIN,
                       SUSPENDED_CITIES, CITY_MIN_CONFIDENCE, CITY_MIN_NWS_GAP,
                       check_city_nws_gap, get_open_positions, init_decisions_db,
                       MIN_POSITION_SIZE, MAX_SINGLE_TRADE_PCT, KELLY_FRACTION,
                       ACCOUNT_FLOOR_PCT)
from orderbook_analyzer import MIN_SPREAD_SAFE, MIN_VOLUME_SAFE
from seasonal_adjuster import get_seasonal_multiplier
from weather_nws_feed import load_forecasts

title = target.get("title", "")
city = extract_weather_city(title or ticker)
threshold = extract_threshold_temp(title or "")
side = "YES" if cloud_prob > market_price else "NO"

# Orderbook
bid = target.get("yes_bid", 0) or 0
ask = target.get("yes_ask", 0) or 0
spread = ask - bid
volume = target.get("volume", 0) or 0
ob_pass = not (spread > MIN_SPREAD_SAFE or volume < MIN_VOLUME_SAFE)
if bid == 0 and ask == 0:
    ob_pass = True  # No data = assume safe (per orderbook_analyzer logic)
print(f"  [{'PASS' if ob_pass else 'FAIL'}] Orderbook: spread={spread}c (max {MIN_SPREAD_SAFE}), vol={volume} (min {MIN_VOLUME_SAFE})")

# Seasonal adjustment
seasonal = get_seasonal_multiplier(title or ticker)
adjusted_conf = cloud_conf * seasonal
print(f"  [INFO] Seasonal: {cloud_conf:.2f} x {seasonal:.2f} = {adjusted_conf:.2f}")

# Blocked tickers
BLOCKED = ["KXEURUSD", "KXUSDJPY", "KXINX", "KXGDP"]
blocked = any(ticker.startswith(b) for b in BLOCKED)
print(f"  [{'FAIL' if blocked else 'PASS'}] Blocked ticker")

# Category whitelist
print(f"  [PASS] Category whitelist (weather)")

# Extreme gap override
forecasts = load_forecasts()
nws_temp = None
if city and forecasts:
    for k, v in forecasts.items():
        if city == k or city in v.get("city", "").lower():
            nws_temp = v.get("high_temp")
            break
gap_f = abs(nws_temp - threshold) if nws_temp is not None and threshold is not None else None
if gap_f is not None and gap_f >= 15 and (cloud_prob < 0.15 or cloud_prob > 0.85):
    print(f"  [OVERRIDE] Extreme gap: {gap_f:.0f}F -> conf overridden to 0.90")
    adjusted_conf = 0.90

# Ensemble floor
ef_pass = adjusted_conf >= 0.65
print(f"  [{'PASS' if ef_pass else 'FAIL'}] Ensemble floor: {adjusted_conf:.2f} >= 0.65")

# Category confidence
cat_conf = CATEGORY_CONFIDENCE.get("weather", CONFIDENCE_MIN)
cc_pass = adjusted_conf >= cat_conf - 0.001
print(f"  [{'PASS' if cc_pass else 'FAIL'}] Category conf: {adjusted_conf:.2f} >= {cat_conf}")

# Price gap
pg_pass = price_gap >= PRICE_GAP_MIN
print(f"  [{'PASS' if pg_pass else 'FAIL'}] Price gap: {price_gap:.2f} >= {PRICE_GAP_MIN}")

# Open positions
dconn = init_decisions_db()
open_pos = get_open_positions(dconn)
op_pass = ticker not in open_pos
print(f"  [{'PASS' if op_pass else 'FAIL'}] Open positions: {len(open_pos)} open, this ticker {'not ' if op_pass else ''}in set")
dconn.close()

# Suspended cities
susp = city and city in SUSPENDED_CITIES
print(f"  [{'FAIL' if susp else 'PASS'}] Suspended: city={city}, SUSPENDED={SUSPENDED_CITIES}")

# City confidence
city_conf_val = CITY_MIN_CONFIDENCE.get(city) if city else None
if city_conf_val:
    cc2_pass = adjusted_conf >= city_conf_val - 0.001
    print(f"  [{'PASS' if cc2_pass else 'FAIL'}] City conf: {adjusted_conf:.2f} >= {city_conf_val} ({city})")
else:
    cc2_pass = True
    print(f"  [PASS] City conf: no override for {city}")

# NWS data available
has_nws = True
if city and forecasts:
    has_nws = any(city == k or city in v.get("city", "").lower() for k, v in forecasts.items())
print(f"  [{'PASS' if has_nws else 'FAIL'}] NWS data available for {city}: {has_nws}")

# NWS gap
if city:
    nws_pass, nws_reason, _ = check_city_nws_gap(title or "")
    min_gap = CITY_MIN_NWS_GAP.get(city, CITY_MIN_NWS_GAP.get("default", 3))
    print(f"  [{'PASS' if nws_pass else 'FAIL'}] NWS gap: NWS={nws_temp}F thresh={threshold} gap={gap_f}F min={min_gap}F")
    if nws_reason:
        print(f"    Reason: {nws_reason}")
else:
    nws_pass = True

# Borderline EV
borderline = 0.20 < cloud_prob < 0.80
if borderline:
    bev_pass = adjusted_conf >= 0.85 - 0.001
    print(f"  [{'PASS' if bev_pass else 'FAIL'}] Borderline EV: prob={cloud_prob:.2f} needs conf>=0.85, has {adjusted_conf:.2f}")
else:
    bev_pass = True
    print(f"  [PASS] Borderline EV: prob={cloud_prob:.2f} (not borderline)")

# ── STEP 5: Position Sizing ──
print("\n[5] POSITION SIZING")
if side == "YES":
    cost_per = market_price
    our_prob = cloud_prob
else:
    cost_per = 1.0 - market_price
    our_prob = 1.0 - cloud_prob

b = (1.0 / cost_per) - 1 if cost_per > 0 else 0
q = 1.0 - our_prob
kelly = (b * our_prob - q) / b if b > 0 else 0
kelly_bet = max(0, kelly * KELLY_FRACTION)
effective_capital = 40.0  # $50 balance - $10 floor
amount = min(kelly_bet * effective_capital, effective_capital * MAX_SINGLE_TRADE_PCT, 150)
min_pos = 10  # balance < 100
if 0 < amount < min_pos:
    amount = min_pos if effective_capital >= min_pos * 2 else 0
amount = round(max(0, amount), 2)

contracts = max(1, int(amount / cost_per)) if cost_per > 0 and amount > 0 else 0
print(f"  Side: {side}, cost/contract: ${cost_per:.2f}")
print(f"  Kelly raw: {kelly:.4f}, fraction: {kelly_bet:.4f}")
print(f"  Amount: ${amount:.2f} ({contracts} contracts)")

# ── VERDICT ──
all_pass = all([ob_pass, not blocked, ef_pass, cc_pass, pg_pass, op_pass,
                not susp, cc2_pass, has_nws, nws_pass, bev_pass])
print(f"\n{'=' * 70}")
if all_pass and amount > 0:
    print(f"VERDICT: WOULD EXECUTE — {side} ${amount:.2f} ({contracts} contracts)")
else:
    fails = []
    if not ob_pass: fails.append("orderbook")
    if blocked: fails.append("blocked_ticker")
    if not ef_pass: fails.append(f"ensemble_floor({adjusted_conf:.2f}<0.65)")
    if not cc_pass: fails.append(f"category_conf({adjusted_conf:.2f}<{cat_conf})")
    if not pg_pass: fails.append(f"price_gap({price_gap:.2f}<{PRICE_GAP_MIN})")
    if not op_pass: fails.append("open_position")
    if susp: fails.append("suspended_city")
    if not cc2_pass: fails.append("city_confidence")
    if not has_nws: fails.append("nws_missing")
    if not nws_pass: fails.append(f"nws_gap({gap_f}F<{min_gap}F)")
    if not bev_pass: fails.append(f"borderline_ev({adjusted_conf:.2f}<0.85)")
    if amount <= 0: fails.append("no_budget")
    print(f"VERDICT: BLOCKED — {fails}")
print("=" * 70)
