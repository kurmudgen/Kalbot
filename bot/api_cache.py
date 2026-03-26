"""
Universal API response cache — prevents duplicate cloud API calls.
Stores responses by model + market title + date hash.
Each model has its own TTL. Cache hits are logged for cost tracking.
"""

import hashlib
import os
import sqlite3
from datetime import datetime, timezone, timedelta

CACHE_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "api_cache.sqlite")

# TTL per model (hours)
MODEL_TTL = {
    "perplexity": 4,   # Web search data changes frequently
    "claude": 3,
    "deepseek": 3,
    "gemini": 3,
}

def init_cache_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(CACHE_DB), exist_ok=True)
    conn = sqlite3.connect(CACHE_DB)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS api_response_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model TEXT,
            cache_key TEXT,
            market_title TEXT,
            prompt_hash TEXT,
            response TEXT,
            created_at TEXT,
            expires_at TEXT,
            UNIQUE(model, cache_key)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model TEXT,
            cache_key TEXT,
            hit INTEGER,
            timestamp TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_call_counts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model TEXT,
            date TEXT,
            call_count INTEGER DEFAULT 0,
            cache_hits INTEGER DEFAULT 0,
            UNIQUE(model, date)
        )
    """)
    conn.commit()
    return conn


def _make_cache_key(model: str, title: str) -> str:
    """Hash of model + title + today's date."""
    today = datetime.now().strftime("%Y-%m-%d")
    raw = f"{model}:{title}:{today}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def get_cached(model: str, title: str) -> str | None:
    """Check cache. Returns cached response or None."""
    conn = init_cache_db()
    key = _make_cache_key(model, title)
    now = datetime.now(timezone.utc).isoformat()

    row = conn.execute(
        "SELECT response FROM api_response_cache WHERE model=? AND cache_key=? AND expires_at > ?",
        (model, key, now),
    ).fetchone()

    # Log the hit/miss
    conn.execute(
        "INSERT INTO cache_stats (model, cache_key, hit, timestamp) VALUES (?, ?, ?, ?)",
        (model, key, 1 if row else 0, now),
    )

    if row:
        # Increment cache hit counter
        today = datetime.now().strftime("%Y-%m-%d")
        conn.execute(
            """INSERT INTO daily_call_counts (model, date, call_count, cache_hits)
               VALUES (?, ?, 0, 1)
               ON CONFLICT(model, date) DO UPDATE SET cache_hits = cache_hits + 1""",
            (model, today),
        )
        conn.commit()
        conn.close()
        return row[0]

    conn.commit()
    conn.close()
    return None


def store_cached(model: str, title: str, response: str):
    """Store a response in cache."""
    conn = init_cache_db()
    key = _make_cache_key(model, title)
    now = datetime.now(timezone.utc)
    ttl_hours = MODEL_TTL.get(model, 3)
    expires = (now + timedelta(hours=ttl_hours)).isoformat()

    conn.execute(
        """INSERT OR REPLACE INTO api_response_cache
           (model, cache_key, market_title, prompt_hash, response, created_at, expires_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (model, key, title, key, response, now.isoformat(), expires),
    )

    # Increment call counter
    today = datetime.now().strftime("%Y-%m-%d")
    conn.execute(
        """INSERT INTO daily_call_counts (model, date, call_count, cache_hits)
           VALUES (?, ?, 1, 0)
           ON CONFLICT(model, date) DO UPDATE SET call_count = call_count + 1""",
        (model, today),
    )

    conn.commit()
    conn.close()


def check_daily_budget(model: str) -> bool:
    """Returns True if model is within daily budget."""
    budgets = {
        "perplexity": int(os.getenv("PERPLEXITY_DAILY_BUDGET", "50")),
        "claude": int(os.getenv("CLAUDE_DAILY_BUDGET", "75")),
        "deepseek": int(os.getenv("DEEPSEEK_DAILY_BUDGET", "100")),
        "gemini": int(os.getenv("GEMINI_DAILY_BUDGET", "200")),
    }
    budget = budgets.get(model, 100)

    conn = init_cache_db()
    today = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT call_count FROM daily_call_counts WHERE model=? AND date=?",
        (model, today),
    ).fetchone()
    conn.close()

    count = row[0] if row else 0
    return count < budget


def get_daily_stats() -> dict:
    """Get today's call counts and cache stats per model."""
    conn = init_cache_db()
    today = datetime.now().strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT model, call_count, cache_hits FROM daily_call_counts WHERE date=?",
        (today,),
    ).fetchall()
    conn.close()

    stats = {}
    for r in rows:
        stats[r[0]] = {"calls": r[1], "cache_hits": r[2]}
    return stats


def cleanup_expired():
    """Remove expired cache entries."""
    conn = init_cache_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("DELETE FROM api_response_cache WHERE expires_at < ?", (now,))
    conn.commit()
    conn.close()
