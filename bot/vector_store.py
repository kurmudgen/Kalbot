"""
Vector store — ChromaDB-backed market memory.
Embeds resolved market titles and stores outcomes.
Provides get_similar_markets() for context injection.
"""

import os
from datetime import datetime

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

CHROMA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "chromadb")
COLLECTION_NAME = "kalbot_market_history"
ENABLED = os.getenv("VECTOR_STORE_ENABLED", "true").lower() == "true"

_client = None
_collection = None


def _get_collection():
    """Lazy-init ChromaDB collection."""
    global _client, _collection
    if _collection is not None:
        return _collection

    if not ENABLED:
        return None

    try:
        import chromadb

        os.makedirs(CHROMA_DIR, exist_ok=True)
        _client = chromadb.PersistentClient(path=CHROMA_DIR)
        _collection = _client.get_or_create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )
        return _collection
    except Exception as e:
        print(f"  Vector store init error: {e}")
        return None


def add_resolved_market(ticker: str, title: str, category: str,
                        outcome: str, confidence: float, pnl: float):
    """Add a resolved market to the vector store."""
    collection = _get_collection()
    if collection is None:
        return

    try:
        collection.upsert(
            ids=[ticker],
            documents=[title],
            metadatas=[{
                "category": category,
                "outcome": outcome,
                "confidence": str(round(confidence, 2)),
                "pnl": str(round(pnl, 2)),
                "added_at": datetime.utcnow().isoformat(),
            }],
        )
    except Exception as e:
        print(f"  Vector store add error: {e}")


def get_similar_markets(title: str, n: int = 5) -> list[dict]:
    """Find the N most similar historical markets by title."""
    collection = _get_collection()
    if collection is None:
        return []

    try:
        results = collection.query(
            query_texts=[title],
            n_results=n,
        )

        similar = []
        if results and results["documents"]:
            for i, doc in enumerate(results["documents"][0]):
                meta = results["metadatas"][0][i] if results["metadatas"] else {}
                distance = results["distances"][0][i] if results["distances"] else 0
                similar.append({
                    "title": doc,
                    "outcome": meta.get("outcome", "?"),
                    "category": meta.get("category", "?"),
                    "confidence": meta.get("confidence", "?"),
                    "pnl": meta.get("pnl", "?"),
                    "similarity": round(1 - distance, 3),
                })
        return similar
    except Exception as e:
        print(f"  Vector store query error: {e}")
        return []


def format_similar_context(title: str, n: int = 5) -> str:
    """Format similar markets as injectable context for the local model."""
    similar = get_similar_markets(title, n)
    if not similar:
        return ""

    lines = ["SIMILAR HISTORICAL OUTCOMES:"]
    for m in similar:
        lines.append(
            f"  [{m['outcome']}] {m['title'][:60]} "
            f"(conf={m['confidence']}, sim={m['similarity']:.2f})"
        )
    return "\n".join(lines)


def load_from_resolutions():
    """Bulk load resolved trades into vector store."""
    import sqlite3

    resolutions_db = os.path.join(os.path.dirname(__file__), "..", "logs", "resolutions.sqlite")
    if not os.path.exists(resolutions_db):
        print("  No resolutions DB found")
        return 0

    conn = sqlite3.connect(resolutions_db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT ticker, title, category, result, our_confidence, pnl FROM resolved_trades"
    ).fetchall()
    conn.close()

    loaded = 0
    for r in rows:
        if r["title"]:
            add_resolved_market(
                r["ticker"], r["title"], r["category"] or "",
                r["result"] or "?", r["our_confidence"] or 0.5, r["pnl"] or 0,
            )
            loaded += 1

    print(f"  Vector store: loaded {loaded} resolved markets")
    return loaded


if __name__ == "__main__":
    print("=== Vector Store ===")
    loaded = load_from_resolutions()
    print(f"Loaded: {loaded}")

    # Test query
    similar = get_similar_markets("Will the high temp in Denver be above 80F?")
    print(f"\nSimilar to Denver weather:")
    for s in similar:
        print(f"  [{s['outcome']}] {s['title'][:50]} (sim={s['similarity']:.2f})")
