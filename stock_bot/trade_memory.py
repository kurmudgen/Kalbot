"""
Trade memory system from TradingAgents repo (35K stars).
Uses BM25 lexical matching to recall similar past trades.
No embeddings API needed — runs locally, zero cost.

After each trade resolves, stores what happened.
Before each new trade, retrieves similar past situations.
The bot learns from its own mistakes and successes.
"""

import json
import math
import os
import sqlite3
from collections import Counter
from datetime import datetime, timezone

MEMORY_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "trade_memory.sqlite")


def init_memory_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(MEMORY_DB), exist_ok=True)
    conn = sqlite3.connect(MEMORY_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            situation TEXT,
            action TEXT,
            outcome TEXT,
            pnl REAL,
            lesson TEXT,
            symbol TEXT,
            strategy TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    return conn


def _tokenize(text: str) -> list[str]:
    """Simple tokenizer for BM25."""
    return [w.lower().strip(".,!?()[]{}\"'") for w in text.split() if len(w) > 2]


def _bm25_score(query_tokens: list[str], doc_tokens: list[str],
                avg_dl: float, k1: float = 1.5, b: float = 0.75) -> float:
    """BM25 relevance score."""
    dl = len(doc_tokens)
    doc_freq = Counter(doc_tokens)
    score = 0.0

    for qt in set(query_tokens):
        tf = doc_freq.get(qt, 0)
        if tf == 0:
            continue
        numerator = tf * (k1 + 1)
        denominator = tf + k1 * (1 - b + b * dl / max(avg_dl, 1))
        score += numerator / denominator

    return score


def store_memory(situation: str, action: str, outcome: str, pnl: float,
                  lesson: str, symbol: str = "", strategy: str = ""):
    """Store a trade outcome for future recall."""
    conn = init_memory_db()
    conn.execute(
        """INSERT INTO memories
           (situation, action, outcome, pnl, lesson, symbol, strategy, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (situation, action, outcome, pnl, lesson, symbol, strategy,
         datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()


def recall_similar(situation: str, top_k: int = 3) -> list[dict]:
    """Find the most similar past trades using BM25."""
    if not os.path.exists(MEMORY_DB):
        return []

    conn = init_memory_db()
    rows = conn.execute("SELECT * FROM memories ORDER BY created_at DESC").fetchall()
    conn.close()

    if not rows:
        return []

    query_tokens = _tokenize(situation)
    if not query_tokens:
        return []

    # Compute BM25 scores
    all_docs = []
    for row in rows:
        doc_text = f"{row[1]} {row[2]} {row[3]} {row[5]}"  # situation + action + outcome + lesson
        doc_tokens = _tokenize(doc_text)
        all_docs.append((row, doc_tokens))

    avg_dl = sum(len(dt) for _, dt in all_docs) / len(all_docs)

    scored = []
    for row, doc_tokens in all_docs:
        score = _bm25_score(query_tokens, doc_tokens, avg_dl)
        if score > 0:
            scored.append((score, row))

    scored.sort(key=lambda x: -x[0])

    results = []
    for score, row in scored[:top_k]:
        results.append({
            "situation": row[1],
            "action": row[2],
            "outcome": row[3],
            "pnl": row[4],
            "lesson": row[5],
            "symbol": row[6],
            "relevance_score": score,
        })

    return results


def format_memories_for_prompt(memories: list[dict]) -> str:
    """Format recalled memories as context for LLM prompts."""
    if not memories:
        return ""

    lines = ["Reflections from similar past trades:"]
    for m in memories:
        outcome = "WIN" if m["pnl"] > 0 else "LOSS"
        lines.append(f"- {m['symbol']}: {m['action']} → {outcome} (${m['pnl']:+.2f}). Lesson: {m['lesson']}")

    return "\n".join(lines)


def reflect_on_trade(symbol: str, action: str, entry_price: float,
                      exit_price: float, reasoning: str, strategy: str = ""):
    """After a trade closes, reflect and store the lesson."""
    pnl = exit_price - entry_price if action == "buy" else entry_price - exit_price
    pnl_pct = pnl / entry_price * 100

    situation = f"{symbol} at ${entry_price:.4f}, {action} based on: {reasoning}"

    if pnl > 0:
        outcome = f"Won ${pnl:.4f} ({pnl_pct:+.1f}%)"
        lesson = f"Pattern worked: {reasoning[:100]}"
    else:
        outcome = f"Lost ${abs(pnl):.4f} ({pnl_pct:+.1f}%)"
        lesson = f"Pattern failed: {reasoning[:100]}. Avoid similar setups."

    store_memory(situation, action, outcome, pnl, lesson, symbol, strategy)


if __name__ == "__main__":
    # Test
    store_memory(
        "SNDL spiked 15% on high volume, cannabis sector momentum",
        "buy", "Lost 8%", -0.12, "Cannabis momentum fades quickly", "SNDL", "penny_momentum",
    )
    store_memory(
        "NIO gapped up 10% on delivery numbers, EV sector strong",
        "buy", "Won 12%", 0.18, "EV delivery beats drive sustained moves", "NIO", "penny_momentum",
    )

    results = recall_similar("cannabis stock spiking on volume")
    print(f"Found {len(results)} similar memories:")
    for r in results:
        print(f"  [{r['symbol']}] {r['lesson']} (relevance: {r['relevance_score']:.2f})")

    print("\n" + format_memories_for_prompt(results))
