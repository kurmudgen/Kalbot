"""
Enrich calibration data with trade history price snapshots.
For each market, sample prices at different time points to create
multiple calibration data points from a single market.

This multiplies our effective dataset without fabricating outcomes.
"""

import os
import sys

import numpy as np
import pandas as pd

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
TRADES_DIR = os.path.join(BASE_DIR, "data", "raw", "data", "kalshi", "trades")
SPLITS_DIR = os.path.join(BASE_DIR, "data", "splits")


def load_trades_for_tickers(tickers: set[str]) -> pd.DataFrame:
    """Load trade data for specific tickers from parquet files."""
    if not os.path.exists(TRADES_DIR):
        print(f"ERROR: {TRADES_DIR} not found")
        return pd.DataFrame()

    trade_files = [f for f in os.listdir(TRADES_DIR) if f.endswith(".parquet")]
    print(f"Scanning {len(trade_files)} trade files for {len(tickers)} tickers...")

    all_trades = []
    for i, f in enumerate(trade_files):
        if i % 500 == 0:
            print(f"  Scanning file {i}/{len(trade_files)}...", end="\r")
        try:
            df = pd.read_parquet(os.path.join(TRADES_DIR, f))
            matched = df[df["ticker"].isin(tickers)]
            if len(matched) > 0:
                all_trades.append(matched)
        except Exception:
            continue

    print(f"\nFound trades in {len(all_trades)} files")
    if not all_trades:
        return pd.DataFrame()

    return pd.concat(all_trades, ignore_index=True)


def create_price_snapshots(markets_df: pd.DataFrame, trades_df: pd.DataFrame,
                           snapshots_per_market: int = 3) -> pd.DataFrame:
    """Create multiple calibration points per market using historical prices."""
    enriched_rows = []

    for _, market in markets_df.iterrows():
        ticker = market["market_id"]
        market_trades = trades_df[trades_df["ticker"] == ticker].sort_values("created_time")

        if len(market_trades) < snapshots_per_market:
            # Not enough trades, use the single close price
            enriched_rows.append(market.to_dict())
            continue

        # Sample prices at different time quantiles
        indices = np.linspace(0, len(market_trades) - 1, snapshots_per_market + 2, dtype=int)[1:-1]

        for j, idx in enumerate(indices):
            trade = market_trades.iloc[idx]
            row = market.to_dict()
            row["market_id"] = f"{ticker}_snap{j}"
            row["market_price_at_close"] = trade["yes_price"]  # Price at this snapshot
            row["last_price"] = trade["yes_price"]
            row["_snapshot_time"] = str(trade["created_time"])
            enriched_rows.append(row)

    return pd.DataFrame(enriched_rows)


def main():
    # Load training data
    train_path = os.path.join(SPLITS_DIR, "train.parquet")
    if not os.path.exists(train_path):
        print("ERROR: train.parquet not found")
        sys.exit(1)

    train_df = pd.read_parquet(train_path)
    print(f"Training markets: {len(train_df)}")

    tickers = set(train_df["market_id"].unique())
    trades = load_trades_for_tickers(tickers)

    if trades.empty:
        print("No matching trades found. Skipping enrichment.")
        return

    print(f"Total matching trades: {len(trades)}")
    tickers_with_trades = trades["ticker"].nunique()
    print(f"Markets with trade data: {tickers_with_trades}/{len(tickers)}")

    # Create enriched dataset with 3 snapshots per market
    enriched = create_price_snapshots(train_df, trades, snapshots_per_market=3)
    print(f"\nEnriched dataset: {len(enriched)} data points (from {len(train_df)} markets)")

    # Save
    out_path = os.path.join(SPLITS_DIR, "train_enriched.parquet")
    enriched.to_parquet(out_path, index=False)
    print(f"Saved to {out_path}")

    # Stats
    print(f"\nPer-category breakdown:")
    for cat in enriched["category"].value_counts().index:
        count = len(enriched[enriched["category"] == cat])
        print(f"  {cat}: {count}")


if __name__ == "__main__":
    main()
