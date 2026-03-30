"""
Options observer — paper trading observation module.

Purely observational. No execution, no capital deployment.
Watches economics-category Kalshi trade signals (CPI, Fed, jobless, TSA)
and records corresponding SPY/QQQ options chain data for correlation analysis.

Activated automatically when the scanner identifies upcoming economic releases.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta

import yfinance as yf
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

ENABLED = os.getenv("OPTIONS_OBSERVER_ENABLED", "false").lower() == "true"
OPTIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "options_paper.sqlite")
DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")

# ETFs to observe
TICKERS = ["SPY", "QQQ"]
# Sector ETFs by release type
SECTOR_ETFS = {
    "cpi": ["XLP", "TLT"],      # Consumer staples, bonds (inflation-sensitive)
    "fed": ["XLF", "TLT"],      # Financials, bonds (rate-sensitive)
    "jobless": ["XLI", "IWM"],   # Industrials, small-cap (labor-sensitive)
    "tsa": ["JETS"],             # Airlines (travel-sensitive)
}

# Map Kalshi prefixes to release types
RELEASE_TYPES = {
    "KXCPI": "cpi", "KXPCE": "cpi",
    "KXFED": "fed", "KXFOMC": "fed",
    "KXJOBLESS": "jobless", "KXNFP": "jobless",
    "KXTSA": "tsa", "TSA": "tsa",
}


def init_options_db() -> sqlite3.Connection:
    """Create options observation tables."""
    os.makedirs(os.path.dirname(OPTIONS_DB), exist_ok=True)
    conn = sqlite3.connect(OPTIONS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            release_type TEXT,
            release_date TEXT,
            kalshi_ticker TEXT,
            kalshi_probability REAL,
            kalshi_confidence REAL,
            underlying TEXT,
            underlying_price REAL,
            expiration TEXT,
            expiration_type TEXT,
            strike REAL,
            option_type TEXT,
            option_price REAL,
            implied_vol REAL,
            delta REAL,
            gamma REAL,
            theta REAL,
            vega REAL,
            straddle_price REAL,
            straddle_implied_prob REAL,
            options_edge_gap REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS simulated_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER,
            timestamp TEXT,
            underlying TEXT,
            expiration TEXT,
            strike REAL,
            option_type TEXT,
            direction TEXT,
            entry_price REAL,
            current_price REAL,
            underlying_entry REAL,
            underlying_current REAL,
            unrealized_pnl REAL,
            status TEXT DEFAULT 'open',
            exit_price REAL,
            exit_timestamp TEXT,
            FOREIGN KEY (observation_id) REFERENCES observations(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER,
            release_type TEXT,
            release_date TEXT,
            actual_outcome TEXT,
            kalshi_was_correct INTEGER,
            options_direction_correct INTEGER,
            options_pnl REAL,
            underlying_move_pct REAL,
            iv_crush_pct REAL,
            timestamp TEXT,
            FOREIGN KEY (observation_id) REFERENCES observations(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS greeks_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id INTEGER,
            timestamp TEXT,
            underlying_price REAL,
            option_price REAL,
            implied_vol REAL,
            delta REAL,
            gamma REAL,
            theta REAL,
            vega REAL,
            time_to_expiry_hours REAL,
            FOREIGN KEY (position_id) REFERENCES simulated_positions(id)
        )
    """)
    conn.commit()
    return conn


def _get_release_type(ticker: str) -> str | None:
    """Extract release type from Kalshi ticker prefix."""
    for prefix, rtype in RELEASE_TYPES.items():
        if ticker.startswith(prefix):
            return rtype
    return None


def _get_options_chain(symbol: str) -> dict | None:
    """Pull options chain data for a symbol using yfinance."""
    try:
        stock = yf.Ticker(symbol)
        current_price = stock.info.get("regularMarketPrice") or stock.info.get("currentPrice")
        if not current_price:
            hist = stock.history(period="1d")
            if not hist.empty:
                current_price = float(hist["Close"].iloc[-1])
        if not current_price:
            return None

        expirations = stock.options
        if not expirations:
            return None

        return {
            "symbol": symbol,
            "price": current_price,
            "expirations": list(expirations),
            "ticker_obj": stock,
        }
    except Exception as e:
        print(f"  Options observer: error fetching {symbol}: {e}")
        return None


def _select_expirations(expirations: list[str], release_date: str) -> dict:
    """Select nearest weekly and monthly expirations around the release date."""
    from datetime import date

    try:
        rel_date = datetime.strptime(release_date, "%Y-%m-%d").date()
    except Exception:
        rel_date = datetime.now().date() + timedelta(days=7)

    weekly = None
    monthly = None

    for exp_str in expirations:
        try:
            exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
        except Exception:
            continue

        days_after = (exp_date - rel_date).days
        if days_after < 0:
            continue

        # Weekly: closest expiration within 7 days after release
        if days_after <= 7 and (weekly is None or days_after < (datetime.strptime(weekly, "%Y-%m-%d").date() - rel_date).days):
            weekly = exp_str

        # Monthly: closest expiration 14-45 days after release
        if 14 <= days_after <= 45 and (monthly is None or days_after < (datetime.strptime(monthly, "%Y-%m-%d").date() - rel_date).days):
            monthly = exp_str

    return {"weekly": weekly, "monthly": monthly}


def _select_strikes(chain_data: dict, expiration: str) -> list[dict]:
    """Select ATM and 1% OTM strikes in each direction."""
    try:
        stock = chain_data["ticker_obj"]
        price = chain_data["price"]
        opts = stock.option_chain(expiration)

        calls = opts.calls
        puts = opts.puts

        atm_strike = round(price)  # Nearest whole dollar
        otm_call_strike = round(price * 1.01)
        otm_put_strike = round(price * 0.99)

        strikes_info = []
        for strike, opt_type in [
            (atm_strike, "call"), (atm_strike, "put"),
            (otm_call_strike, "call"), (otm_put_strike, "put"),
        ]:
            df = calls if opt_type == "call" else puts
            if df.empty:
                continue

            # Find nearest available strike
            idx = (df["strike"] - strike).abs().idxmin()
            row = df.loc[idx]

            strikes_info.append({
                "strike": float(row["strike"]),
                "option_type": opt_type,
                "price": float(row.get("lastPrice", 0)),
                "bid": float(row.get("bid", 0)),
                "ask": float(row.get("ask", 0)),
                "implied_vol": float(row.get("impliedVolatility", 0)),
                "volume": int(row.get("volume", 0) or 0),
                "open_interest": int(row.get("openInterest", 0) or 0),
            })

        return strikes_info
    except Exception as e:
        print(f"  Options observer: strike selection error: {e}")
        return []


def _compute_greeks_approx(option_data: dict, underlying_price: float, days_to_expiry: float) -> dict:
    """Approximate Greeks from available data."""
    iv = option_data.get("implied_vol", 0)
    price = option_data.get("price", 0)
    strike = option_data.get("strike", underlying_price)

    # Basic approximations (Black-Scholes-like)
    moneyness = underlying_price / strike if strike > 0 else 1
    t = max(days_to_expiry / 365, 0.001)

    # Delta approximation from moneyness
    if option_data["option_type"] == "call":
        delta = max(0.01, min(0.99, 0.5 + 0.5 * (moneyness - 1) / (iv * t**0.5 + 0.001)))
    else:
        delta = max(-0.99, min(-0.01, -0.5 + 0.5 * (moneyness - 1) / (iv * t**0.5 + 0.001)))

    # Theta approximation (daily)
    theta = -price / max(days_to_expiry, 1) if price > 0 else 0

    # Gamma approximation
    gamma = 0.01 / (underlying_price * iv * t**0.5 + 0.001) if iv > 0 else 0

    # Vega approximation
    vega = underlying_price * t**0.5 * 0.01 if iv > 0 else 0

    return {
        "delta": round(delta, 4),
        "gamma": round(gamma, 6),
        "theta": round(theta, 4),
        "vega": round(vega, 4),
    }


def observe_release(kalshi_ticker: str, kalshi_prob: float, kalshi_conf: float,
                    release_date: str | None = None) -> list[dict]:
    """Main observation function — called when economic release market is detected.

    Pulls options chains, computes implied probabilities, simulates paper positions.
    Returns list of observations recorded.
    """
    if not ENABLED:
        return []

    release_type = _get_release_type(kalshi_ticker)
    if not release_type:
        return []

    if not release_date:
        # Default to 7 days from now if no specific date
        release_date = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")

    conn = init_options_db()
    observations = []

    # Determine which ETFs to observe
    symbols = TICKERS + SECTOR_ETFS.get(release_type, [])

    for symbol in symbols:
        chain = _get_options_chain(symbol)
        if not chain:
            continue

        expirations = _select_expirations(chain["expirations"], release_date)

        for exp_type, exp_date in expirations.items():
            if not exp_date:
                continue

            strikes = _select_strikes(chain, exp_date)
            if not strikes:
                continue

            # Compute days to expiry
            try:
                exp_dt = datetime.strptime(exp_date, "%Y-%m-%d")
                days_to_exp = max(1, (exp_dt - datetime.now()).days)
            except Exception:
                days_to_exp = 7

            # Compute ATM straddle implied probability
            atm_call = next((s for s in strikes if s["option_type"] == "call" and abs(s["strike"] - chain["price"]) < chain["price"] * 0.02), None)
            atm_put = next((s for s in strikes if s["option_type"] == "put" and abs(s["strike"] - chain["price"]) < chain["price"] * 0.02), None)

            straddle_price = 0
            straddle_implied_prob = 0
            if atm_call and atm_put:
                straddle_price = atm_call["price"] + atm_put["price"]
                straddle_implied_prob = straddle_price / chain["price"] if chain["price"] > 0 else 0

            # Options edge gap: difference between our Kalshi probability and options-implied probability
            options_edge_gap = abs(kalshi_prob - straddle_implied_prob) if straddle_implied_prob > 0 else 0

            now = datetime.now(timezone.utc).isoformat()

            for strike_data in strikes:
                greeks = _compute_greeks_approx(strike_data, chain["price"], days_to_exp)

                obs_id = conn.execute(
                    """INSERT INTO observations
                       (timestamp, release_type, release_date, kalshi_ticker,
                        kalshi_probability, kalshi_confidence, underlying, underlying_price,
                        expiration, expiration_type, strike, option_type,
                        option_price, implied_vol, delta, gamma, theta, vega,
                        straddle_price, straddle_implied_prob, options_edge_gap)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (now, release_type, release_date, kalshi_ticker,
                     kalshi_prob, kalshi_conf, symbol, chain["price"],
                     exp_date, exp_type, strike_data["strike"], strike_data["option_type"],
                     strike_data["price"], strike_data["implied_vol"],
                     greeks["delta"], greeks["gamma"], greeks["theta"], greeks["vega"],
                     straddle_price, straddle_implied_prob, options_edge_gap),
                ).lastrowid

                observations.append({
                    "id": obs_id,
                    "symbol": symbol,
                    "strike": strike_data["strike"],
                    "type": strike_data["option_type"],
                    "price": strike_data["price"],
                    "iv": strike_data["implied_vol"],
                    "edge_gap": options_edge_gap,
                })

                # Simulate paper position if edge gap >= 5%
                if options_edge_gap >= 0.05:
                    # Bullish Kalshi signal -> buy call, bearish -> buy put
                    if kalshi_prob > straddle_implied_prob:
                        sim_type = "call"
                        direction = "long"
                    else:
                        sim_type = "put"
                        direction = "long"

                    if strike_data["option_type"] == sim_type:
                        conn.execute(
                            """INSERT INTO simulated_positions
                               (observation_id, timestamp, underlying, expiration,
                                strike, option_type, direction, entry_price,
                                current_price, underlying_entry, underlying_current,
                                unrealized_pnl, status)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'open')""",
                            (obs_id, now, symbol, exp_date,
                             strike_data["strike"], sim_type, direction,
                             strike_data["price"], strike_data["price"],
                             chain["price"], chain["price"]),
                        )

    conn.commit()
    conn.close()

    if observations:
        print(f"  Options observer: {len(observations)} observations for {kalshi_ticker} ({release_type})")

    return observations


def update_open_positions():
    """Update all open simulated positions with current prices.

    Called periodically (every 30 min) to track position values.
    """
    if not ENABLED or not os.path.exists(OPTIONS_DB):
        return

    conn = sqlite3.connect(OPTIONS_DB)
    conn.row_factory = sqlite3.Row
    positions = conn.execute(
        "SELECT * FROM simulated_positions WHERE status = 'open'"
    ).fetchall()

    if not positions:
        conn.close()
        return

    now = datetime.now(timezone.utc).isoformat()
    updated = 0

    for pos in positions:
        try:
            chain = _get_options_chain(pos["underlying"])
            if not chain:
                continue

            # Check if expired
            try:
                exp_dt = datetime.strptime(pos["expiration"], "%Y-%m-%d")
                if datetime.now() > exp_dt + timedelta(days=1):
                    # Position expired
                    conn.execute(
                        "UPDATE simulated_positions SET status = 'expired', exit_price = 0, exit_timestamp = ?, unrealized_pnl = ? WHERE id = ?",
                        (now, -pos["entry_price"], pos["id"]),
                    )
                    updated += 1
                    continue
            except Exception:
                pass

            # Get current option price
            stock = chain["ticker_obj"]
            opts = stock.option_chain(pos["expiration"])
            df = opts.calls if pos["option_type"] == "call" else opts.puts

            if df.empty:
                continue

            idx = (df["strike"] - pos["strike"]).abs().idxmin()
            current_price = float(df.loc[idx].get("lastPrice", 0))

            pnl = current_price - pos["entry_price"]
            days_to_exp = max(1, (exp_dt - datetime.now()).days) if 'exp_dt' in dir() else 7

            conn.execute(
                "UPDATE simulated_positions SET current_price = ?, underlying_current = ?, unrealized_pnl = ? WHERE id = ?",
                (current_price, chain["price"], pnl, pos["id"]),
            )

            # Record Greeks checkpoint
            greeks = _compute_greeks_approx(
                {"strike": pos["strike"], "option_type": pos["option_type"],
                 "price": current_price, "implied_vol": float(df.loc[idx].get("impliedVolatility", 0))},
                chain["price"], days_to_exp
            )
            conn.execute(
                """INSERT INTO greeks_history
                   (position_id, timestamp, underlying_price, option_price,
                    implied_vol, delta, gamma, theta, vega, time_to_expiry_hours)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (pos["id"], now, chain["price"], current_price,
                 float(df.loc[idx].get("impliedVolatility", 0)),
                 greeks["delta"], greeks["gamma"], greeks["theta"], greeks["vega"],
                 days_to_exp * 24),
            )
            updated += 1
        except Exception as e:
            print(f"  Options observer: update error for {pos['underlying']}: {e}")

    conn.commit()
    conn.close()

    if updated:
        print(f"  Options observer: updated {updated} open positions")


def record_outcome(release_type: str, release_date: str, actual_outcome: str):
    """Record the actual outcome after a release.

    Called when CPI/Fed/etc data is published and Kalshi market resolves.
    """
    if not ENABLED or not os.path.exists(OPTIONS_DB):
        return

    conn = sqlite3.connect(OPTIONS_DB)
    conn.row_factory = sqlite3.Row

    # Find observations for this release
    obs = conn.execute(
        """SELECT DISTINCT id, kalshi_probability, underlying, underlying_price
           FROM observations
           WHERE release_type = ? AND release_date = ?""",
        (release_type, release_date),
    ).fetchall()

    if not obs:
        conn.close()
        return

    now = datetime.now(timezone.utc).isoformat()

    for ob in obs:
        # Get current underlying price to compute move
        chain = _get_options_chain(ob["underlying"])
        current_price = chain["price"] if chain else ob["underlying_price"]
        move_pct = ((current_price - ob["underlying_price"]) / ob["underlying_price"] * 100) if ob["underlying_price"] > 0 else 0

        # Close open positions for this observation
        positions = conn.execute(
            "SELECT id, entry_price, current_price FROM simulated_positions WHERE observation_id = ? AND status = 'open'",
            (ob["id"],),
        ).fetchall()

        total_pnl = 0
        for pos in positions:
            pnl = (pos["current_price"] or 0) - pos["entry_price"]
            total_pnl += pnl
            conn.execute(
                "UPDATE simulated_positions SET status = 'closed', exit_price = ?, exit_timestamp = ?, unrealized_pnl = ? WHERE id = ?",
                (pos["current_price"], now, pnl, pos["id"]),
            )

        conn.execute(
            """INSERT INTO outcomes
               (observation_id, release_type, release_date, actual_outcome,
                kalshi_was_correct, options_direction_correct, options_pnl,
                underlying_move_pct, iv_crush_pct, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ob["id"], release_type, release_date, actual_outcome,
             None, None, total_pnl, move_pct, None, now),
        )

    conn.commit()
    conn.close()
    print(f"  Options observer: recorded outcome for {release_type} {release_date}")


def get_observation_stats() -> dict:
    """Get summary stats of all observations."""
    if not os.path.exists(OPTIONS_DB):
        return {}

    conn = sqlite3.connect(OPTIONS_DB)
    stats = {}
    stats["total_observations"] = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    stats["total_positions"] = conn.execute("SELECT COUNT(*) FROM simulated_positions").fetchone()[0]
    stats["open_positions"] = conn.execute("SELECT COUNT(*) FROM simulated_positions WHERE status='open'").fetchone()[0]
    stats["total_outcomes"] = conn.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]

    # By release type
    rows = conn.execute(
        "SELECT release_type, COUNT(*) as n FROM observations GROUP BY release_type"
    ).fetchall()
    stats["by_release_type"] = {r[0]: r[1] for r in rows}

    conn.close()
    return stats


if __name__ == "__main__":
    print("=== Options Observer ===")
    print(f"Enabled: {ENABLED}")
    print(f"Database: {OPTIONS_DB}")
    print(f"Tickers: {TICKERS}")
    print(f"Sector ETFs: {SECTOR_ETFS}")

    if ENABLED:
        # Test observation
        print("\nTest: observing KXCPI-26APR-T0.3...")
        obs = observe_release("KXCPI-26APR-T0.3", 0.85, 0.80, "2026-04-15")
        print(f"Observations: {len(obs)}")
        for o in obs[:5]:
            print(f"  {o['symbol']} {o['type']} strike={o['strike']} price=${o['price']:.2f} iv={o['iv']:.2%} edge_gap={o['edge_gap']:.2%}")

        stats = get_observation_stats()
        print(f"\nStats: {stats}")
    else:
        print("\nSet OPTIONS_OBSERVER_ENABLED=true in .env to activate")
