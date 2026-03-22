"""
Adaptive model weighting: tracks per-model, per-category Brier scores
and reweights the ensemble based on which models perform best.

Based on dylanpersonguy's Polymarket bot pattern.
Uses inverse-Brier weighting with Bayesian blending.
"""

import os
import sqlite3
from datetime import datetime, timezone

WEIGHTS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "model_performance.sqlite")

# Default weights (before we have enough data)
DEFAULT_WEIGHTS = {
    "perplexity": 0.40,
    "claude": 0.35,
    "deepseek": 0.25,
}

MIN_SAMPLES_FOR_LEARNED = 5   # Minimum samples per model per category
FULL_TRUST_SAMPLES = 50       # Fully trust learned weights after this many


def init_weights_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(WEIGHTS_DB), exist_ok=True)
    conn = sqlite3.connect(WEIGHTS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS model_predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model TEXT,
            category TEXT,
            predicted_prob REAL,
            actual_outcome INTEGER,
            brier_score REAL,
            recorded_at TEXT
        )
    """)
    conn.commit()
    return conn


def record_prediction(model: str, category: str, predicted_prob: float,
                       actual_outcome: int):
    """Record a model's prediction and actual outcome for tracking."""
    conn = init_weights_db()
    brier = (predicted_prob - actual_outcome) ** 2
    conn.execute(
        """INSERT INTO model_predictions
           (model, category, predicted_prob, actual_outcome, brier_score, recorded_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (model, category, predicted_prob, actual_outcome, brier,
         datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()


def get_adaptive_weights(category: str = "") -> dict[str, float]:
    """Get model weights based on historical Brier scores for this category.
    Uses inverse-Brier weighting with Bayesian blending toward defaults."""

    if not os.path.exists(WEIGHTS_DB):
        return DEFAULT_WEIGHTS.copy()

    conn = sqlite3.connect(WEIGHTS_DB)

    # Get per-model Brier scores for this category
    if category:
        rows = conn.execute("""
            SELECT model, AVG(brier_score) as avg_brier, COUNT(*) as n
            FROM model_predictions
            WHERE category = ?
            GROUP BY model
        """, (category,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT model, AVG(brier_score) as avg_brier, COUNT(*) as n
            FROM model_predictions
            GROUP BY model
        """).fetchall()

    conn.close()

    if not rows:
        return DEFAULT_WEIGHTS.copy()

    # Compute inverse-Brier weights
    learned_weights = {}
    min_samples = float("inf")

    for model, avg_brier, n in rows:
        if n >= MIN_SAMPLES_FOR_LEARNED:
            learned_weights[model] = 1.0 / max(avg_brier, 0.001)
            min_samples = min(min_samples, n)

    if not learned_weights:
        return DEFAULT_WEIGHTS.copy()

    # Normalize learned weights
    total = sum(learned_weights.values())
    for model in learned_weights:
        learned_weights[model] /= total

    # Bayesian blending: trust learned weights more as we get more data
    blend = min(1.0, min_samples / FULL_TRUST_SAMPLES)

    final_weights = {}
    for model in DEFAULT_WEIGHTS:
        default_w = DEFAULT_WEIGHTS.get(model, 0.2)
        learned_w = learned_weights.get(model, default_w)
        final_weights[model] = blend * learned_w + (1 - blend) * default_w

    # Re-normalize
    total = sum(final_weights.values())
    for model in final_weights:
        final_weights[model] /= total

    return final_weights


def print_model_performance():
    """Print current model performance summary."""
    if not os.path.exists(WEIGHTS_DB):
        print("No performance data yet.")
        return

    conn = sqlite3.connect(WEIGHTS_DB)
    rows = conn.execute("""
        SELECT model, category, AVG(brier_score) as avg_brier, COUNT(*) as n
        FROM model_predictions
        GROUP BY model, category
        ORDER BY model, category
    """).fetchall()
    conn.close()

    print(f"{'Model':<15} {'Category':<12} {'Brier':>8} {'Samples':>8}")
    print("-" * 45)
    for model, category, brier, n in rows:
        print(f"{model:<15} {category:<12} {brier:>8.4f} {n:>8}")

    # Show current weights
    for cat in set(r[1] for r in rows):
        weights = get_adaptive_weights(cat)
        print(f"\nAdaptive weights for {cat}:")
        for model, w in sorted(weights.items(), key=lambda x: -x[1]):
            print(f"  {model}: {w:.1%}")


if __name__ == "__main__":
    print_model_performance()
