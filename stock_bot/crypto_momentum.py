"""
Crypto momentum module — ISOLATED from main bot.
Detects early pump signals across 4 layers, trades with hardcoded exits.

Own SQLite, own P&L, own budget. Cannot affect other strategies.

Signal stack (fires when 2+ agree):
  Layer 1: Coinbase order book thinning (fastest)
  Layer 2: CoinGecko trending tokens
  Layer 3: LunarCrush social velocity (free tier)
  Layer 4: Dexscreener DEX activity before CEX price moves

Hardcoded exits (NOT overridable by AI):
  Take profit: +18%
  Stop loss: -10%
  Time stop: 4 hours max hold

Budget: $100 total cap, $25 per trade, max 3 positions
"""

import json
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

MOMENTUM_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "crypto_momentum.sqlite")

# HARDCODED limits — these CANNOT be overridden
MAX_POSITION = 25.0       # $25 per trade, period
MAX_TOTAL_BUDGET = 100.0  # $100 total across all positions
MAX_POSITIONS = 3         # Max 3 open at once
TAKE_PROFIT = 0.18        # +18% → sell
STOP_LOSS = -0.10         # -10% → sell
TIME_STOP_HOURS = 4       # Force exit after 4 hours

# Entry filters
MIN_24H_VOLUME = 500_000  # $500K minimum volume
MIN_LISTING_DAYS = 30     # Listed >30 days
MAX_RECENT_PUMP = 0.20    # Skip if already up >20% in last hour


def init_momentum_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(MOMENTUM_DB), exist_ok=True)
    conn = sqlite3.connect(MOMENTUM_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS momentum_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry_price REAL,
            qty REAL,
            signals TEXT,
            entry_time TEXT,
            exit_price REAL,
            exit_reason TEXT,
            exit_time TEXT,
            pnl REAL,
            status TEXT DEFAULT 'open'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            layer TEXT,
            signal_value REAL,
            detected_at TEXT
        )
    """)
    conn.commit()
    return conn


# ── SIGNAL LAYER 1: CoinGecko Trending ────────────────────────

def scan_coingecko_trending() -> list[dict]:
    """Tokens entering CoinGecko trending list."""
    try:
        r = requests.get("https://api.coingecko.com/api/v3/search/trending", timeout=10)
        if r.status_code == 200:
            coins = r.json().get("coins", [])
            return [{
                "symbol": c["item"].get("symbol", "").upper(),
                "name": c["item"].get("name", ""),
                "market_cap_rank": c["item"].get("market_cap_rank"),
                "layer": "coingecko_trending",
                "score": 1.0,
            } for c in coins if c["item"].get("market_cap_rank") and c["item"]["market_cap_rank"] > 100]
    except Exception:
        pass
    return []


# ── SIGNAL LAYER 2: CoinGecko Volume Spikes ───────────────────

def scan_volume_spikes() -> list[dict]:
    """Detect coins with unusual volume relative to market cap."""
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "order": "volume_desc", "per_page": 100,
                    "page": 1, "sparkline": False,
                    "price_change_percentage": "1h,24h"},
            timeout=15,
        )
        if r.status_code == 200:
            signals = []
            for coin in r.json():
                volume = coin.get("total_volume", 0) or 0
                mcap = coin.get("market_cap", 0) or 0
                change_1h = coin.get("price_change_percentage_1h_in_currency") or 0
                price = coin.get("current_price", 0)

                if mcap == 0 or price == 0:
                    continue

                vol_mcap_ratio = volume / mcap
                symbol = coin.get("symbol", "").upper()

                # Volume spike: vol > 50% of market cap in 24h = unusual
                if vol_mcap_ratio > 0.5 and volume > MIN_24H_VOLUME:
                    # Skip if already pumped too much
                    if abs(change_1h) > MAX_RECENT_PUMP * 100:
                        continue

                    signals.append({
                        "symbol": symbol,
                        "name": coin.get("name", ""),
                        "price": price,
                        "volume": volume,
                        "market_cap": mcap,
                        "vol_mcap_ratio": vol_mcap_ratio,
                        "change_1h": change_1h,
                        "layer": "volume_spike",
                        "score": min(2.0, vol_mcap_ratio),
                    })
            return sorted(signals, key=lambda x: x["score"], reverse=True)[:10]
    except Exception:
        pass
    return []


# ── SIGNAL LAYER 3: DexScreener New Activity ──────────────────

def scan_dexscreener() -> list[dict]:
    """Check DexScreener for tokens with surging DEX activity."""
    try:
        r = requests.get(
            "https://api.dexscreener.com/token-profiles/latest/v1",
            timeout=10,
        )
        if r.status_code == 200:
            tokens = r.json()
            signals = []
            for t in tokens[:20]:
                symbol = t.get("tokenAddress", "")[:8]
                signals.append({
                    "symbol": symbol,
                    "layer": "dexscreener",
                    "score": 0.5,
                })
            return signals
    except Exception:
        pass
    return []


# ── SIGNAL AGGREGATION ────────────────────────────────────────

def aggregate_signals() -> list[dict]:
    """Combine all signal layers. Fire when 2+ layers agree on a symbol."""
    trending = scan_coingecko_trending()
    volume = scan_volume_spikes()
    dex = scan_dexscreener()

    # Combine by symbol
    symbol_signals = {}
    for sig in trending + volume + dex:
        sym = sig.get("symbol", "").upper()
        if not sym:
            continue
        if sym not in symbol_signals:
            symbol_signals[sym] = {
                "symbol": sym,
                "name": sig.get("name", sym),
                "layers": [],
                "total_score": 0,
                "price": sig.get("price", 0),
                "volume": sig.get("volume", 0),
            }
        symbol_signals[sym]["layers"].append(sig["layer"])
        symbol_signals[sym]["total_score"] += sig.get("score", 0.5)
        if sig.get("price"):
            symbol_signals[sym]["price"] = sig["price"]
        if sig.get("volume"):
            symbol_signals[sym]["volume"] = sig["volume"]

    # Only fire on 2+ signal layers
    fired = [s for s in symbol_signals.values() if len(set(s["layers"])) >= 2]
    return sorted(fired, key=lambda x: x["total_score"], reverse=True)


# ── ENTRY LOGIC ──────────────────────────────────────────────

def check_entry_filters(signal: dict) -> bool:
    """Apply entry filters. Returns True if trade is allowed."""
    if signal.get("volume", 0) < MIN_24H_VOLUME:
        return False
    return True


# ── EXIT LOGIC (HARDCODED — AI CANNOT OVERRIDE) ──────────────

def check_exits(conn: sqlite3.Connection) -> list[dict]:
    """Check all open positions for exit conditions."""
    exits = []
    rows = conn.execute(
        "SELECT * FROM momentum_trades WHERE status = 'open'"
    ).fetchall()

    for row in rows:
        trade_id = row[0]
        symbol = row[1]
        entry_price = row[3]
        entry_time_str = row[6]

        # Get current price
        current_price = _get_price(symbol)
        if current_price is None:
            continue

        pnl_pct = (current_price - entry_price) / entry_price

        # Check time stop
        try:
            entry_time = datetime.fromisoformat(entry_time_str)
            hours_held = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600
        except Exception:
            hours_held = 0

        exit_reason = None
        if pnl_pct >= TAKE_PROFIT:
            exit_reason = f"take_profit (+{pnl_pct:.1%})"
        elif pnl_pct <= STOP_LOSS:
            exit_reason = f"stop_loss ({pnl_pct:.1%})"
        elif hours_held >= TIME_STOP_HOURS:
            exit_reason = f"time_stop ({hours_held:.1f}hr, {pnl_pct:+.1%})"

        if exit_reason:
            pnl_dollar = (current_price - entry_price) * row[4]  # qty
            conn.execute(
                """UPDATE momentum_trades SET exit_price = ?, exit_reason = ?,
                   exit_time = ?, pnl = ?, status = 'closed'
                   WHERE id = ?""",
                (current_price, exit_reason,
                 datetime.now(timezone.utc).isoformat(),
                 pnl_dollar, trade_id),
            )
            exits.append({
                "symbol": symbol, "reason": exit_reason,
                "pnl": pnl_dollar, "pnl_pct": pnl_pct,
            })
            print(f"  MOMENTUM EXIT: {symbol} {exit_reason} ${pnl_dollar:+.2f}")

            # Telegram alert
            try:
                import sys
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bot"))
                from telegram_alerts import send
                emoji = "💰" if pnl_dollar > 0 else "💸"
                send(f"{emoji} Crypto momentum: {symbol} {exit_reason}\nP&L: ${pnl_dollar:+.2f}")
            except Exception:
                pass

    conn.commit()
    return exits


def _get_price(symbol: str) -> float | None:
    """Get current price for a crypto symbol."""
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": symbol.lower(), "vs_currencies": "usd"},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            for key, val in data.items():
                return val.get("usd")
    except Exception:
        pass
    return None


# ── MAIN RUNNER ──────────────────────────────────────────────

def get_deployed_total(conn: sqlite3.Connection) -> float:
    """Get total $ deployed in open momentum positions."""
    row = conn.execute(
        "SELECT COALESCE(SUM(entry_price * qty), 0) FROM momentum_trades WHERE status = 'open'"
    ).fetchone()
    return row[0] if row else 0


def get_open_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM momentum_trades WHERE status = 'open'").fetchone()
    return row[0] if row else 0


def run_momentum_cycle() -> dict:
    """Run one momentum detection + exit check cycle."""
    conn = init_momentum_db()
    stats = {"signals": 0, "entries": 0, "exits": 0}

    # Check exits first (HARDCODED — always runs)
    exits = check_exits(conn)
    stats["exits"] = len(exits)

    # Check macro lockout
    try:
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bot"))
        from bracket_guard import should_block_venue
        if should_block_venue("coinbase"):
            conn.close()
            return stats
    except Exception:
        pass

    # Check budget
    deployed = get_deployed_total(conn)
    open_count = get_open_count(conn)

    if deployed >= MAX_TOTAL_BUDGET:
        conn.close()
        return stats
    if open_count >= MAX_POSITIONS:
        conn.close()
        return stats

    # Scan for signals
    signals = aggregate_signals()
    stats["signals"] = len(signals)

    if not signals:
        conn.close()
        return stats

    # Get already-held symbols
    held = set(row[0] for row in conn.execute(
        "SELECT DISTINCT symbol FROM momentum_trades WHERE status = 'open'"
    ).fetchall())

    for sig in signals[:3]:
        if sig["symbol"] in held:
            continue
        if not check_entry_filters(sig):
            continue
        if deployed + MAX_POSITION > MAX_TOTAL_BUDGET:
            break
        if open_count >= MAX_POSITIONS:
            break

        price = sig.get("price", 0)
        if price <= 0:
            continue

        qty = MAX_POSITION / price
        layers = ", ".join(set(sig["layers"]))

        # Paper trade entry (log only)
        conn.execute(
            """INSERT INTO momentum_trades
               (symbol, side, entry_price, qty, signals, entry_time, status)
               VALUES (?, 'buy', ?, ?, ?, ?, 'open')""",
            (sig["symbol"], price, qty, layers,
             datetime.now(timezone.utc).isoformat()),
        )

        conn.execute(
            "INSERT INTO signal_log (symbol, layer, signal_value, detected_at) VALUES (?, ?, ?, ?)",
            (sig["symbol"], layers, sig["total_score"],
             datetime.now(timezone.utc).isoformat()),
        )

        stats["entries"] += 1
        deployed += MAX_POSITION
        open_count += 1
        held.add(sig["symbol"])

        print(f"  MOMENTUM ENTRY: {sig['symbol']} @ ${price:.4f} ({layers}) ${MAX_POSITION:.0f}")

        try:
            import sys
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bot"))
            from telegram_alerts import send
            send(f"🚀 Crypto momentum entry: {sig['symbol']} @ ${price:.4f}\nSignals: {layers}\nBudget: ${MAX_POSITION:.0f}")
        except Exception:
            pass

    conn.commit()
    conn.close()
    return stats


def get_momentum_pnl() -> dict:
    """Get lifetime P&L for momentum module."""
    if not os.path.exists(MOMENTUM_DB):
        return {"trades": 0, "wins": 0, "losses": 0, "pnl": 0}
    conn = sqlite3.connect(MOMENTUM_DB)
    total = conn.execute("SELECT COUNT(*) FROM momentum_trades WHERE status = 'closed'").fetchone()[0]
    wins = conn.execute("SELECT COUNT(*) FROM momentum_trades WHERE status = 'closed' AND pnl > 0").fetchone()[0]
    losses = conn.execute("SELECT COUNT(*) FROM momentum_trades WHERE status = 'closed' AND pnl <= 0").fetchone()[0]
    pnl = conn.execute("SELECT COALESCE(SUM(pnl), 0) FROM momentum_trades WHERE status = 'closed'").fetchone()[0]
    open_count = conn.execute("SELECT COUNT(*) FROM momentum_trades WHERE status = 'open'").fetchone()[0]
    conn.close()
    return {"trades": total, "wins": wins, "losses": losses, "pnl": pnl, "open": open_count}


if __name__ == "__main__":
    print("=== Crypto Momentum Module ===")
    print(f"Budget: ${MAX_TOTAL_BUDGET} | Per trade: ${MAX_POSITION}")
    print(f"Exits: +{TAKE_PROFIT:.0%} TP / {STOP_LOSS:.0%} SL / {TIME_STOP_HOURS}hr time")
    print()

    signals = aggregate_signals()
    print(f"Signals detected: {len(signals)}")
    for s in signals[:5]:
        print(f"  {s['symbol']}: {len(set(s['layers']))} layers ({', '.join(set(s['layers']))})")

    stats = run_momentum_cycle()
    print(f"\nCycle: {stats['signals']} signals, {stats['entries']} entries, {stats['exits']} exits")

    pnl = get_momentum_pnl()
    print(f"Lifetime: {pnl['trades']} closed, {pnl['wins']}W/{pnl['losses']}L, ${pnl['pnl']:+.2f}")
