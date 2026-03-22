"""
Shared news/research pool. Perplexity's research gets stored here
so all strategies and future cycles can reference it.

Stores:
- Research findings per market
- Breaking news detected
- Data release results
- Anything Perplexity finds that might be useful later
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta

NEWS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "news_pool.sqlite")


def init_news_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(NEWS_DB), exist_ok=True)
    conn = sqlite3.connect(NEWS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS research (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            category TEXT,
            research_text TEXT,
            source TEXT,
            relevance_score REAL,
            created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS breaking_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            headline TEXT,
            category TEXT,
            impact TEXT,
            source TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    return conn


def store_research(ticker: str, category: str, research: str,
                    source: str = "perplexity"):
    """Store research findings from any analysis."""
    if not research or len(research) < 10:
        return
    conn = init_news_db()
    conn.execute(
        """INSERT INTO research (ticker, category, research_text, source,
           relevance_score, created_at) VALUES (?, ?, ?, ?, ?, ?)""",
        (ticker, category, research[:2000], source, 1.0,
         datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()


def store_breaking_news(headline: str, category: str, impact: str = "",
                         source: str = "perplexity"):
    """Store a breaking news item."""
    conn = init_news_db()
    conn.execute(
        """INSERT INTO breaking_news (headline, category, impact, source, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (headline, category, impact, source,
         datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()


def get_recent_research(category: str = "", hours: int = 24,
                         limit: int = 10) -> list[dict]:
    """Get recent research findings, optionally filtered by category."""
    if not os.path.exists(NEWS_DB):
        return []

    conn = sqlite3.connect(NEWS_DB)
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    if category:
        rows = conn.execute(
            """SELECT * FROM research WHERE category = ? AND created_at > ?
               ORDER BY created_at DESC LIMIT ?""",
            (category, cutoff, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT * FROM research WHERE created_at > ?
               ORDER BY created_at DESC LIMIT ?""",
            (cutoff, limit),
        ).fetchall()

    conn.close()
    return [dict(r) for r in rows]


def get_breaking_news(hours: int = 6) -> list[dict]:
    """Get recent breaking news."""
    if not os.path.exists(NEWS_DB):
        return []

    conn = sqlite3.connect(NEWS_DB)
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    rows = conn.execute(
        "SELECT * FROM breaking_news WHERE created_at > ? ORDER BY created_at DESC",
        (cutoff,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def format_context(category: str = "") -> str:
    """Format recent research as context for LLM prompts."""
    research = get_recent_research(category, hours=12, limit=5)
    news = get_breaking_news(hours=6)

    lines = []
    if news:
        lines.append("Recent breaking news:")
        for n in news[:3]:
            lines.append(f"- [{n['category']}] {n['headline']}")

    if research:
        lines.append("\nRecent research findings:")
        for r in research[:5]:
            lines.append(f"- [{r['category']}] {r['research_text'][:150]}")

    return "\n".join(lines) if lines else ""


if __name__ == "__main__":
    # Show current pool
    research = get_recent_research(hours=48)
    news = get_breaking_news(hours=48)
    print(f"Research items (48hr): {len(research)}")
    print(f"Breaking news (48hr): {len(news)}")
    if research:
        for r in research[:3]:
            print(f"  [{r['category']}] {r['research_text'][:80]}...")
