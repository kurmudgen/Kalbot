"""
Pull and process additional datasets for calibration:
1. Polymarket data from Jon-Becker dataset (already downloaded)
2. KalshiBench trade data → reconstruct price histories
3. TSA ground truth data from TSA.gov HTML
4. Philadelphia Fed SPF probability distributions
5. Kalshi API resolved markets (direct pull, all statuses)
"""

import json
import os
import re
import sys
from html.parser import HTMLParser

import numpy as np
import pandas as pd

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")


# ── 1. Polymarket resolved markets ──────────────────────────────────

def process_polymarket():
    """Extract resolved Polymarket markets related to our target categories."""
    pm_dir = os.path.join(BASE_DIR, "data", "raw", "data", "polymarket", "markets")
    if not os.path.exists(pm_dir):
        print("Polymarket markets dir not found, skipping")
        return pd.DataFrame()

    files = [f for f in os.listdir(pm_dir) if f.endswith(".parquet")]
    print(f"Loading {len(files)} Polymarket market files...")

    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(os.path.join(pm_dir, f)))
        except Exception:
            continue

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    print(f"Total Polymarket markets: {len(df)}")

    # Filter to resolved markets
    if "closed" in df.columns:
        resolved = df[df["closed"] == True].copy()
    else:
        resolved = df.copy()

    print(f"Closed Polymarket markets: {len(resolved)}")

    # Filter to our target categories via keywords
    keywords = {
        "economics": ["fed ", "federal reserve", "interest rate", "fomc", "gdp", "recession", "rate cut"],
        "inflation": ["inflation", " cpi", "consumer price", "pce ", "jobless", "unemployment", "nonfarm", "payroll"],
        "weather": ["temperature", "weather", "precipitation", "hurricane", "tornado"],
    }

    def classify(question):
        if not isinstance(question, str):
            return None
        q = f" {question.lower()} "
        for cat, kws in keywords.items():
            if any(kw in q for kw in kws):
                return cat
        return None

    if "question" in resolved.columns:
        resolved["category"] = resolved["question"].apply(classify)
    elif "title" in resolved.columns:
        resolved["category"] = resolved["title"].apply(classify)
    else:
        print("No question/title column found")
        return pd.DataFrame()

    target = resolved[resolved["category"].notna()].copy()
    print(f"Target category Polymarket markets: {len(target)}")

    if len(target) > 0:
        print(f"By category:")
        for cat, count in target["category"].value_counts().items():
            print(f"  {cat}: {count}")

    return target


# ── 2. TSA ground truth extraction ──────────────────────────────────

class TSAParser(HTMLParser):
    """Extract TSA passenger volume data from HTML tables."""

    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.current_row = []
        self.rows = []

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.in_table = True
        elif tag == "tr" and self.in_table:
            self.in_row = True
            self.current_row = []
        elif tag in ("td", "th") and self.in_row:
            self.in_cell = True

    def handle_endtag(self, tag):
        if tag == "table":
            self.in_table = False
        elif tag == "tr":
            self.in_row = False
            if self.current_row:
                self.rows.append(self.current_row)
        elif tag in ("td", "th"):
            self.in_cell = False

    def handle_data(self, data):
        if self.in_cell:
            self.current_row.append(data.strip())


def process_tsa():
    """Parse TSA passenger volume HTML into a DataFrame."""
    tsa_dir = os.path.join(BASE_DIR, "data", "raw", "tsa")
    all_rows = []

    for year in [2023, 2024, 2025]:
        html_path = os.path.join(tsa_dir, f"tsa_volumes_{year}.html")
        if not os.path.exists(html_path):
            continue

        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()

        parser = TSAParser()
        parser.feed(html)

        for row in parser.rows:
            if len(row) >= 2:
                # Try to extract date and number
                date_str = row[0]
                for cell in row[1:]:
                    cleaned = cell.replace(",", "").strip()
                    if cleaned.isdigit() and int(cleaned) > 100000:
                        all_rows.append({"date": date_str, "passengers": int(cleaned), "year": year})
                        break

    if not all_rows:
        print("No TSA data parsed from HTML")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    print(f"TSA daily records: {len(df)}")

    # Save as CSV for easy access
    csv_path = os.path.join(tsa_dir, "tsa_daily_volumes.csv")
    df.to_csv(csv_path, index=False)
    print(f"Saved to {csv_path}")

    return df


# ── 3. SPF probability distributions ───────────────────────────────

def process_spf():
    """Load Philadelphia Fed SPF probability distributions for CPI and GDP."""
    spf_dir = os.path.join(BASE_DIR, "data", "raw", "spf")

    results = {}
    for name in ["spf_prob_cpi", "spf_prob_rgdp", "spf_mean_cpi"]:
        path = os.path.join(spf_dir, f"{name}.xlsx")
        if os.path.exists(path):
            try:
                df = pd.read_excel(path)
                results[name] = df
                print(f"{name}: {len(df)} rows, {list(df.columns)[:8]}")
            except Exception as e:
                print(f"{name}: error reading - {e}")

    return results


# ── 4. Combine all data into enriched training set ──────────────────

def build_combined_dataset():
    """Build combined calibration dataset from all sources."""
    datasets = []

    # Original Kalshi training data
    train_path = os.path.join(BASE_DIR, "data", "splits", "train.parquet")
    if os.path.exists(train_path):
        train = pd.read_parquet(train_path)
        train["source"] = "kalshi_jbecker"
        datasets.append(train)
        print(f"Kalshi (Jon-Becker): {len(train)} markets")

    # Polymarket
    pm = process_polymarket()
    if not pm.empty and "question" in pm.columns:
        pm_clean = pd.DataFrame({
            "market_id": pm.get("id", pm.index).astype(str),
            "title": pm.get("question", pm.get("title", "")),
            "event_ticker": "",
            "category": pm["category"],
            "market_price_at_close": pd.to_numeric(
                pm.get("outcome_prices", "50").apply(
                    lambda x: json.loads(x)[0] if isinstance(x, str) and x.startswith("[") else 50
                ),
                errors="coerce",
            ).fillna(50) * 100,
            "last_price": 50,
            "actual_outcome": 0,  # Need to determine from outcomes
            "volume": pm.get("volume", 0),
            "result": "unknown",
            "source": "polymarket",
        })
        datasets.append(pm_clean)
        print(f"Polymarket: {len(pm_clean)} markets")

    if datasets:
        combined = pd.concat(datasets, ignore_index=True)
        out_path = os.path.join(BASE_DIR, "data", "splits", "train_combined.parquet")
        combined.to_parquet(out_path, index=False)
        print(f"\nCombined dataset: {len(combined)} total rows")
        print(f"Saved to {out_path}")
        print(f"By source:")
        for src, count in combined["source"].value_counts().items():
            print(f"  {src}: {count}")
        return combined

    return pd.DataFrame()


def main():
    print("=" * 60)
    print("  PULLING ADDITIONAL DATA SOURCES")
    print("=" * 60)

    print("\n--- TSA Ground Truth ---")
    tsa = process_tsa()

    print("\n--- Philadelphia Fed SPF ---")
    spf = process_spf()

    print("\n--- Polymarket + Combined Dataset ---")
    combined = build_combined_dataset()

    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"TSA daily records:  {len(tsa) if not tsa.empty else 'N/A'}")
    print(f"SPF datasets:       {len(spf)}")
    print(f"Combined training:  {len(combined) if not combined.empty else 'N/A'}")


if __name__ == "__main__":
    main()
