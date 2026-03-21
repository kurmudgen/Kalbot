"""
Phase 2: Extract data.tar.zst, inspect Kalshi dataset, filter, and split.

WARNING: Do NOT load or inspect data/splits/test.parquet after splitting.
"""

import io
import os
import tarfile

import pandas as pd
import zstandard as zstd
from sklearn.model_selection import train_test_split

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
SPLITS_DIR = os.path.join(BASE_DIR, "data", "splits")
ARCHIVE_PATH = os.path.join(RAW_DIR, "data.tar.zst")

CATEGORY_KEYWORDS = {
    "economics": ["fed ", "federal reserve", "interest rate", "fomc", "gdp",
                   "economic growth", "fed funds", "rate cut", "rate hike",
                   "monetary policy"],
    "inflation": ["inflation", " cpi ", "consumer price", "pce ", "personal consumption",
                  "jobless claim", "unemployment", "nonfarm", "payroll",
                  "jobs report", "initial claims"],
    "tsa": ["tsa", "passenger", "airport checkpoint", "travel volume",
            "air travel"],
    "weather": ["temperature", "weather", "precipitation", "rain", "snow",
                "heat", "cold", "hurricane", "tornado", "degree", "fahrenheit",
                "celsius", "high of", "low of"],
}


def extract_archive():
    """Extract data.tar.zst using Python zstandard library."""
    if not os.path.exists(ARCHIVE_PATH):
        print(f"ERROR: {ARCHIVE_PATH} not found. Download it first.")
        return False

    print(f"Extracting {ARCHIVE_PATH}...")
    dctx = zstd.ZstdDecompressor()

    with open(ARCHIVE_PATH, "rb") as fh:
        reader = dctx.stream_reader(fh)
        with tarfile.open(fileobj=reader, mode="r|") as tar:
            tar.extractall(path=RAW_DIR)

    print("Extraction complete.")
    return True


def detect_category(title: str, event_ticker: str) -> str | None:
    text = f" {title} {event_ticker} ".lower()
    for cat, keywords in CATEGORY_KEYWORDS.items():
        if any(kw.lower() in text for kw in keywords):
            return cat
    return None


def inspect_and_split():
    """Inspect Kalshi markets data, filter to target categories, and split."""
    # Find the parquet files
    kalshi_markets_dir = os.path.join(RAW_DIR, "data", "kalshi", "markets")
    if not os.path.exists(kalshi_markets_dir):
        # Try without 'data' prefix
        kalshi_markets_dir = os.path.join(RAW_DIR, "kalshi", "markets")
    if not os.path.exists(kalshi_markets_dir):
        print(f"Looking for Kalshi markets data...")
        # Search for parquet files
        for root, dirs, files in os.walk(RAW_DIR):
            for f in files:
                if f.endswith(".parquet") and "kalshi" in root.lower() and "market" in root.lower():
                    kalshi_markets_dir = root
                    print(f"  Found: {root}")
                    break

    if not os.path.exists(kalshi_markets_dir):
        print(f"ERROR: Cannot find Kalshi markets directory under {RAW_DIR}")
        print("Available contents:")
        for item in os.listdir(RAW_DIR):
            print(f"  {item}")
        return

    # Load all parquet files
    parquet_files = [
        os.path.join(kalshi_markets_dir, f)
        for f in os.listdir(kalshi_markets_dir)
        if f.endswith(".parquet")
    ]

    if not parquet_files:
        print(f"No parquet files in {kalshi_markets_dir}")
        return

    print(f"Loading {len(parquet_files)} parquet file(s)...")
    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)

    print(f"\n{'='*60}")
    print(f"  DATA INSPECTION")
    print(f"{'='*60}")
    print(f"  Total markets: {len(df)}")
    print(f"  Columns: {list(df.columns)}")

    # Resolved markets
    if "result" in df.columns:
        resolved = df[df["result"].isin(["yes", "no"])]
        print(f"  Resolved markets: {len(resolved)}")
    elif "status" in df.columns:
        resolved = df[df["status"] == "finalized"]
        print(f"  Finalized markets: {len(resolved)}")
    else:
        resolved = df
        print(f"  Could not determine resolution status")

    # Date range
    for col in ["created_time", "open_time", "close_time"]:
        if col in df.columns:
            print(f"  {col} range: {df[col].min()} to {df[col].max()}")

    # Categorize
    print("\n  Categorizing markets...")
    df["category"] = df.apply(
        lambda r: detect_category(
            str(r.get("title", "")),
            str(r.get("event_ticker", ""))
        ),
        axis=1,
    )

    all_cats = df["category"].value_counts(dropna=False)
    print(f"\n  Markets per category (all):")
    for cat, count in all_cats.items():
        label = cat if cat else "uncategorized"
        print(f"    {label}: {count}")

    # Filter to target categories only, resolved markets only
    target = df[df["category"].notna()].copy()
    if "result" in target.columns:
        target = target[target["result"].isin(["yes", "no"])]
        target["actual_outcome"] = (target["result"] == "yes").astype(int)
    else:
        print("WARNING: No 'result' column. Cannot determine outcomes.")
        return

    # Add market_id
    if "ticker" in target.columns:
        target["market_id"] = target["ticker"]
    else:
        target["market_id"] = target.index.astype(str)

    # Get last_price for calibration
    if "last_price" not in target.columns:
        if "yes_bid" in target.columns and "yes_ask" in target.columns:
            target["last_price"] = (target["yes_bid"].fillna(50) + target["yes_ask"].fillna(50)) / 2
        else:
            target["last_price"] = 50

    # Keep price as market_price_at_close
    target["market_price_at_close"] = target["last_price"].fillna(50)

    print(f"\n  Filtered resolved target markets: {len(target)}")
    print(f"  By category:")
    for cat, count in target["category"].value_counts().items():
        print(f"    {cat}: {count}")

    # Write data summary
    summary_path = os.path.join(BASE_DIR, "calibration", "data_summary.txt")
    with open(summary_path, "w") as f:
        f.write(f"Dataset Summary\n{'='*40}\n")
        f.write(f"Total markets in raw data: {len(df)}\n")
        f.write(f"Resolved markets: {len(resolved)}\n")
        f.write(f"Filtered target markets: {len(target)}\n\n")
        f.write(f"Columns: {list(df.columns)}\n\n")
        for col in ["created_time", "open_time", "close_time"]:
            if col in df.columns:
                f.write(f"{col}: {df[col].min()} to {df[col].max()}\n")
        f.write(f"\nMarkets by category (target only):\n")
        for cat, count in target["category"].value_counts().items():
            f.write(f"  {cat}: {count}\n")
    print(f"\n  Summary written to {summary_path}")

    if len(target) == 0:
        print("ERROR: No target markets found. Check category keywords.")
        return

    # Split 60/20/20 stratified by category
    os.makedirs(SPLITS_DIR, exist_ok=True)

    # Keep relevant columns
    keep_cols = ["market_id", "title", "event_ticker", "category",
                 "market_price_at_close", "last_price", "actual_outcome",
                 "volume", "result"]
    keep_cols = [c for c in keep_cols if c in target.columns]
    target = target[keep_cols]

    train_val, test = train_test_split(
        target, test_size=0.2, random_state=42, stratify=target["category"]
    )
    train, val = train_test_split(
        train_val, test_size=0.25, random_state=42, stratify=train_val["category"]
    )  # 0.25 of 0.8 = 0.2

    train.to_parquet(os.path.join(SPLITS_DIR, "train.parquet"), index=False)
    val.to_parquet(os.path.join(SPLITS_DIR, "val.parquet"), index=False)
    test.to_parquet(os.path.join(SPLITS_DIR, "test.parquet"), index=False)

    print(f"\n  Split sizes:")
    print(f"    Train: {len(train)} ({len(train)/len(target):.0%})")
    print(f"    Val:   {len(val)} ({len(val)/len(target):.0%})")
    print(f"    Test:  {len(test)} ({len(test)/len(target):.0%})")

    # Write split summary
    split_summary_path = os.path.join(BASE_DIR, "calibration", "split_summary.txt")
    with open(split_summary_path, "w") as f:
        f.write(f"Split Summary (seed=42, stratified by category)\n{'='*50}\n\n")
        for name, split_df in [("Train", train), ("Val", val), ("Test", test)]:
            f.write(f"{name} ({len(split_df)} markets):\n")
            for cat, count in split_df["category"].value_counts().items():
                f.write(f"  {cat}: {count}\n")
            f.write("\n")
    print(f"  Split summary written to {split_summary_path}")


if __name__ == "__main__":
    if not os.path.exists(os.path.join(RAW_DIR, "data")) and \
       not os.path.exists(os.path.join(RAW_DIR, "kalshi")):
        print("Extracting archive first...")
        if extract_archive():
            inspect_and_split()
    else:
        print("Data already extracted, running inspect and split...")
        inspect_and_split()
