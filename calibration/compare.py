"""
Compare training and validation calibration results side by side.
Flags overfitting if training win rate exceeds validation by >5 points.
"""

import os
import sqlite3
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from run_calibration import compute_metrics

TRAIN_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "calibration_train.sqlite")
VAL_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "calibration_val.sqlite")


def load_results(db_path: str) -> pd.DataFrame:
    if not os.path.exists(db_path):
        print(f"WARNING: {db_path} not found. Run calibration first.")
        return pd.DataFrame()
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM calibration_results", conn)
    conn.close()
    return df


def main():
    train_df = load_results(TRAIN_DB)
    val_df = load_results(VAL_DB)

    if train_df.empty or val_df.empty:
        print("Cannot compare — one or both result sets are empty.")
        sys.exit(1)

    train_metrics = compute_metrics(train_df)
    val_metrics = compute_metrics(val_df)

    print("=" * 60)
    print("  CALIBRATION COMPARISON: TRAIN vs VALIDATION")
    print("=" * 60)
    print(f"{'Metric':<25} {'Train':>12} {'Validation':>12} {'Delta':>10}")
    print("-" * 60)

    rows = [
        ("Total markets", train_metrics["total_markets"], val_metrics["total_markets"]),
        ("Win rate", train_metrics["win_rate"], val_metrics["win_rate"]),
        ("Brier score", train_metrics["brier_score"], val_metrics["brier_score"]),
        ("Avg confidence", train_metrics["avg_confidence"], val_metrics["avg_confidence"]),
    ]

    for label, t, v in rows:
        if isinstance(t, int):
            print(f"  {label:<23} {t:>12} {v:>12} {v - t:>+10}")
        else:
            print(f"  {label:<23} {t:>12.4f} {v:>12.4f} {v - t:>+10.4f}")

    # Per-category comparison
    all_cats = set(train_df["category"].unique()) | set(val_df["category"].unique())
    print(f"\n{'='*60}")
    print("  BY CATEGORY")
    print(f"{'='*60}")
    print(f"{'Category':<15} {'Train WR':>10} {'Val WR':>10} {'Train Brier':>12} {'Val Brier':>12}")
    print("-" * 60)

    for cat in sorted(all_cats):
        t_cat = train_df[train_df["category"] == cat]
        v_cat = val_df[val_df["category"] == cat]
        t_m = compute_metrics(t_cat)
        v_m = compute_metrics(v_cat)
        t_wr = f"{t_m['win_rate']:.3f}" if t_m else "N/A"
        v_wr = f"{v_m['win_rate']:.3f}" if v_m else "N/A"
        t_br = f"{t_m['brier_score']:.4f}" if t_m else "N/A"
        v_br = f"{v_m['brier_score']:.4f}" if v_m else "N/A"
        print(f"  {cat:<13} {t_wr:>10} {v_wr:>10} {t_br:>12} {v_br:>12}")

    # Overfitting check
    print(f"\n{'='*60}")
    print("  DIAGNOSIS")
    print(f"{'='*60}")

    gap = train_metrics["win_rate"] - val_metrics["win_rate"]
    if gap > 0.05:
        print(f"  ⚠ OVERFITTING WARNING: Training win rate exceeds validation by {gap:.1%}")
        print("  Recommendation: Simplify prompt, reduce temperature, or use fewer features.")
    elif val_metrics["brier_score"] > 0.30:
        print(f"  ⚠ HIGH BRIER SCORE ({val_metrics['brier_score']:.4f}): Model is poorly calibrated.")
        print("  Recommendation: Continue tuning prompt template and model parameters.")
    else:
        print(f"  ✓ No overfitting detected (train-val gap: {gap:.1%})")
        print(f"  ✓ Brier score: {val_metrics['brier_score']:.4f}")
        print("  Recommendation: Safe to proceed with test set evaluation.")


if __name__ == "__main__":
    main()
