"""
Multi-category simulation runner: replays historical Kalshi markets through the full pipeline.

Supports: TSA, CPI/Inflation, Jobless Claims, Fed Rate
Skips: EIA (0 markets), Box Office (0 markets)

Reads from data/splits/train.parquet, fetches historical data from FRED/local CSV,
scores with Ollama qwen2.5:32b, applies executor gates, writes results to
category-specific SQLite files in calibration/.

Completely standalone — does not touch live databases or running processes.
"""

import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

# Setup paths
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(ROOT, ".env"), override=True)

TRAIN_PATH = os.path.join(ROOT, "data", "splits", "train.parquet")
PROMPT_PATH = os.path.join(ROOT, "prompts", "local_filter.txt")
BIAS_PATH = os.path.join(ROOT, "calibration", "kalshi_market_bias.json")
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = os.getenv("LOCAL_FILTER_MODEL", "qwen2.5:32b")

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
EIA_API_KEY = os.getenv("EIA_API_KEY", "")

# Executor gate thresholds
CATEGORY_CONFIDENCE = {
    "tsa": float(os.getenv("TSA_CONFIDENCE", "0.85")),
    "inflation": float(os.getenv("INFLATION_CONFIDENCE", "0.80")),
    "economics": float(os.getenv("ECON_CONFIDENCE", "0.80")),
}
PRICE_GAP_MIN = float(os.getenv("PRICE_GAP_MIN", "0.08"))
KELLY_FRACTION = 0.25
MAX_TRADE_SIZE = float(os.getenv("MAX_TRADE_SIZE", "10"))
MAX_NIGHTLY_SPEND = float(os.getenv("MAX_NIGHTLY_SPEND", "50"))

# Historical bias
MARKET_BIAS = {}
if os.path.exists(BIAS_PATH):
    with open(BIAS_PATH) as f:
        MARKET_BIAS = json.load(f).get("bins", {})


def get_historical_bias(market_price_cents: int) -> float:
    for bin_key, data in MARKET_BIAS.items():
        lo, hi = bin_key.split("-")
        if int(lo) <= market_price_cents <= int(hi):
            return data.get("bias_pct", 0) / 100.0
    return 0.0


# ── Ollama ───────────────────────────────────────────────────

def load_prompt_template() -> str:
    with open(PROMPT_PATH) as f:
        return f.read().strip()


def query_ollama(prompt: str, max_retries: int = 2) -> dict | None:
    for attempt in range(max_retries + 1):
        try:
            r = requests.post(OLLAMA_URL, json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3, "num_predict": 256},
            }, timeout=120)
            if r.status_code != 200:
                if attempt < max_retries:
                    time.sleep(2)
                    continue
                return None
            text = r.json().get("response", "")
            json_match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            if attempt < max_retries:
                time.sleep(1)
                continue
            return None
        except Exception:
            if attempt < max_retries:
                time.sleep(2)
                continue
            return None
    return None


# ══════════════════════════════════════════════════════════════
# TSA SIMULATION
# ══════════════════════════════════════════════════════════════

class TSASimulator:
    """Simulates TSA passenger volume markets using local CSV data."""

    TSA_CSV = os.path.join(ROOT, "data", "raw", "tsa", "tsa_daily_volumes.csv")

    def __init__(self):
        self.daily_data = {}  # {date_str: passengers}
        self._load_csv()

    def _load_csv(self):
        if not os.path.exists(self.TSA_CSV):
            print(f"  WARNING: TSA CSV not found at {self.TSA_CSV}")
            return
        df = pd.read_csv(self.TSA_CSV)
        for _, row in df.iterrows():
            try:
                dt = pd.to_datetime(row["date"])
                self.daily_data[dt.strftime("%Y-%m-%d")] = int(row["passengers"])
            except Exception:
                continue
        print(f"  TSA CSV loaded: {len(self.daily_data)} days")

    def filter_markets(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to actual TSA screening markets."""
        tsa = df[df["category"] == "tsa"]
        return tsa[tsa["title"].str.contains("screened by the TSA", case=False, na=False)]

    def parse_market(self, market: dict) -> dict | None:
        """Extract week date and threshold from market."""
        ticker = market["event_ticker"]
        market_id = market["market_id"]
        title = market["title"]

        # Extract week start date from event_ticker: KXTSAW-25MAR02 or TSAW-24JUL14
        date_match = re.search(r'[A-Z]*TSAW-(\d{2})([A-Z]{3})(\d{2})', ticker)
        if not date_match:
            return None
        year = int("20" + date_match.group(1))
        month_str = date_match.group(2)
        day = int(date_match.group(3))
        months = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                   "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
        month = months.get(month_str)
        if not month:
            return None
        try:
            week_start = datetime(year, month, day)
        except ValueError:
            return None

        # Extract threshold from title: "more than 1950000"
        thresh_match = re.search(r'more than (\d+)', title)
        if not thresh_match:
            # Try market_id: A1.95 -> 1,950,000 or A2.55 -> 2,550,000
            id_match = re.search(r'A(\d+)\.(\d+)', market_id)
            if id_match:
                threshold = int(id_match.group(1)) * 1_000_000 + int(id_match.group(2)) * 100_000
            else:
                return None
        else:
            threshold = int(thresh_match.group(1))

        return {
            "week_start": week_start,
            "week_start_str": week_start.strftime("%Y-%m-%d"),
            "threshold": threshold,
        }

    def get_weekly_average(self, week_start: datetime) -> float | None:
        """Get actual weekly average TSA passengers for the week starting at week_start."""
        total = 0
        count = 0
        for i in range(7):
            day = week_start + timedelta(days=i)
            day_str = day.strftime("%Y-%m-%d")
            if day_str in self.daily_data:
                total += self.daily_data[day_str]
                count += 1
        if count < 5:  # Need at least 5 days for a valid average
            return None
        return total / count

    def build_context(self, parsed: dict, actual_avg: float) -> tuple[str, float]:
        """Build TSA data context and compute gap."""
        threshold = parsed["threshold"]
        gap = abs(actual_avg - threshold)
        gap_pct = gap / threshold * 100  # Gap as percentage of threshold

        above_below = "ABOVE" if actual_avg > threshold else "BELOW"

        context = (
            f"\nOFFICIAL TSA DATA: Week of {parsed['week_start_str']} "
            f"average daily passenger volume was {actual_avg:,.0f}. "
            f"This is the Kalshi settlement source.\n"
            f"Market threshold: {threshold:,}\n"
            f"Actual vs threshold: {actual_avg:,.0f} vs {threshold:,} ({above_below})\n"
            f"Gap: {gap:,.0f} passengers ({gap_pct:.1f}% of threshold)\n"
            f"Calibration guide:\n"
            f"  >5% gap: high confidence (0.85-0.95)\n"
            f"  2-5% gap: moderate confidence (0.65-0.80)\n"
            f"  <2% gap: low confidence (0.40-0.60)\n"
            f"Base your probability on the actual TSA data, not the market price."
        )

        return context, gap_pct


# ══════════════════════════════════════════════════════════════
# CPI / INFLATION SIMULATION
# ══════════════════════════════════════════════════════════════

class CPISimulator:
    """Simulates CPI/Inflation markets using FRED API data."""

    # FRED series for CPI data
    SERIES = {
        "CPI": "CPIAUCSL",       # CPI-U All Items
        "CPIFOOD": "CPIUFDSL",   # CPI Food
        "CPICORE": "CPILFESL",   # Core CPI (less food and energy)
    }

    def __init__(self):
        self.cpi_data = {}  # {series: {date: value}}
        self._fetch_all()

    def _fetch_fred(self, series_id: str, start: str = "2022-01-01") -> dict[str, float]:
        """Fetch FRED series data."""
        if not FRED_API_KEY:
            print(f"  WARNING: No FRED_API_KEY for {series_id}")
            return {}
        url = f"https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": start,
            "sort_order": "asc",
        }
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code != 200:
                print(f"  FRED error for {series_id}: HTTP {r.status_code}")
                return {}
            obs = r.json().get("observations", [])
            return {o["date"]: float(o["value"]) for o in obs if o["value"] != "."}
        except Exception as e:
            print(f"  FRED error for {series_id}: {e}")
            return {}

    def _fetch_all(self):
        print("  Fetching CPI data from FRED...")
        for name, series_id in self.SERIES.items():
            data = self._fetch_fred(series_id)
            self.cpi_data[name] = data
            print(f"    {name} ({series_id}): {len(data)} observations")
            time.sleep(0.5)

    def filter_markets(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to CPI/inflation markets."""
        inflation = df[df["category"] == "inflation"]
        return inflation[inflation["title"].str.contains(
            "CPI|core inflation|food CPI", case=False, na=False
        )]

    def parse_market(self, market: dict) -> dict | None:
        """Extract date, threshold, and CPI type from market."""
        title = market["title"]
        event = market["event_ticker"]
        market_id = market["market_id"]

        # Determine CPI type from event ticker
        if "CPIFOOD" in event:
            cpi_type = "CPIFOOD"
        elif "CPICORE" in event:
            cpi_type = "CPICORE"
        elif "CPIYOY" in event or "CPI" in event:
            cpi_type = "CPI"
        else:
            return None

        # Determine if YoY or MoM
        is_yoy = "CPIYOY" in event or "year ending" in title.lower() or "rate of" in title.lower()

        # Extract threshold from title
        thresh_match = re.search(r'(?:above|more than|rise more than)\s+(-?\d+\.?\d*)%', title)
        if not thresh_match:
            return None
        threshold = float(thresh_match.group(1))

        # Extract date from event ticker: CPI-23APR, CPIYOY-24APR
        date_match = re.search(r'(\d{2})([A-Z]{3})', event)
        if not date_match:
            return None
        year = int("20" + date_match.group(1))
        months = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                   "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
        month = months.get(date_match.group(2))
        if not month:
            return None

        return {
            "cpi_type": cpi_type,
            "is_yoy": is_yoy,
            "threshold": threshold,
            "year": year,
            "month": month,
            "date_str": f"{year}-{month:02d}-01",
        }

    def get_actual_value(self, parsed: dict) -> float | None:
        """Get actual CPI change (MoM % or YoY %) for the given month."""
        series_name = parsed["cpi_type"]
        data = self.cpi_data.get(series_name, {})
        if not data:
            return None

        target = f"{parsed['year']}-{parsed['month']:02d}-01"

        if parsed["is_yoy"]:
            # YoY: compare this month to same month last year
            prev_year = f"{parsed['year']-1}-{parsed['month']:02d}-01"
            curr = data.get(target)
            prev = data.get(prev_year)
            if curr is None or prev is None:
                return None
            return ((curr - prev) / prev) * 100
        else:
            # MoM: compare to previous month
            prev_month = parsed["month"] - 1
            prev_year = parsed["year"]
            if prev_month == 0:
                prev_month = 12
                prev_year -= 1
            prev_date = f"{prev_year}-{prev_month:02d}-01"
            curr = data.get(target)
            prev = data.get(prev_date)
            if curr is None or prev is None:
                return None
            return ((curr - prev) / prev) * 100

    def build_context(self, parsed: dict, actual: float) -> tuple[str, float]:
        """Build CPI context and compute gap."""
        threshold = parsed["threshold"]
        gap = abs(actual - threshold)
        change_type = "YoY" if parsed["is_yoy"] else "MoM"
        cpi_name = {"CPI": "CPI-U All Items", "CPICORE": "Core CPI", "CPIFOOD": "Food CPI"}
        name = cpi_name.get(parsed["cpi_type"], "CPI")
        above_below = "ABOVE" if actual > threshold else "BELOW"

        context = (
            f"\nOFFICIAL BLS DATA: {name} {change_type} change for "
            f"{parsed['year']}-{parsed['month']:02d} was {actual:.2f}%. "
            f"This is the Kalshi settlement source.\n"
            f"Market threshold: {threshold}%\n"
            f"Actual vs threshold: {actual:.2f}% vs {threshold}% ({above_below})\n"
            f"Gap: {gap:.2f} percentage points\n"
            f"Calibration guide:\n"
            f"  >0.3pp gap: high confidence (0.85-0.95)\n"
            f"  0.1-0.3pp gap: moderate confidence (0.65-0.80)\n"
            f"  <0.1pp gap: low confidence (0.40-0.60)\n"
            f"Base your probability on the actual CPI data, not the market price."
        )

        return context, gap


# ══════════════════════════════════════════════════════════════
# JOBLESS CLAIMS SIMULATION
# ══════════════════════════════════════════════════════════════

class JoblessSimulator:
    """Simulates initial jobless claims markets using FRED ICSA data."""

    def __init__(self):
        self.claims_data = {}  # {date: value}
        self._fetch()

    def _fetch(self):
        print("  Fetching ICSA data from FRED...")
        if not FRED_API_KEY:
            print("  WARNING: No FRED_API_KEY")
            return
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "ICSA",
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": "2020-01-01",
            "sort_order": "asc",
        }
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code != 200:
                print(f"  FRED error: HTTP {r.status_code}")
                return
            obs = r.json().get("observations", [])
            for o in obs:
                if o["value"] != ".":
                    self.claims_data[o["date"]] = float(o["value"])
            print(f"    ICSA: {len(self.claims_data)} observations loaded")
        except Exception as e:
            print(f"  FRED error: {e}")

    def filter_markets(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to initial jobless claims markets."""
        return df[df["title"].str.contains("initial jobless claims", case=False, na=False)]

    def parse_market(self, market: dict) -> dict | None:
        """Extract week ending date and threshold."""
        title = market["title"]
        event = market["event_ticker"]

        # Extract date from event_ticker: JOBLESS-22OCT08 or KXJOBLESSCLAIMS-25SEP18
        date_match = re.search(r'(\d{2})([A-Z]{3})(\d{2})', event)
        if not date_match:
            return None
        year = int("20" + date_match.group(1))
        months = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                   "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
        month = months.get(date_match.group(2))
        day = int(date_match.group(3))
        if not month:
            return None

        try:
            report_date = datetime(year, month, day)
        except ValueError:
            return None

        # Extract threshold from title: "higher than 300,000" or "more than 215,000"
        thresh_match = re.search(r'(?:higher than|more than)\s+([\d,]+)', title)
        if thresh_match:
            threshold = int(thresh_match.group(1).replace(",", ""))
        else:
            # Try market_id: JOBLESS-22OCT08-C215 -> 215,000
            id_match = re.search(r'C(\d+)', market["market_id"])
            if id_match:
                threshold = int(id_match.group(1)) * 1000
            else:
                # Try: KXJOBLESSCLAIMS-25SEP18-230000
                id_match2 = re.search(r'-(\d{6})$', market["market_id"])
                if id_match2:
                    threshold = int(id_match2.group(1))
                else:
                    return None

        return {
            "report_date": report_date,
            "report_date_str": report_date.strftime("%Y-%m-%d"),
            "threshold": threshold,
        }

    def get_actual_value(self, parsed: dict) -> float | None:
        """Get actual ICSA value for the report date (or closest Thursday)."""
        target = parsed["report_date"]
        # ICSA is reported on Thursdays, search nearby dates
        for offset in range(-7, 8):
            check = target + timedelta(days=offset)
            check_str = check.strftime("%Y-%m-%d")
            if check_str in self.claims_data:
                return self.claims_data[check_str]
        return None

    def build_context(self, parsed: dict, actual: float) -> tuple[str, float]:
        """Build jobless claims context and compute gap."""
        threshold = parsed["threshold"]
        gap = abs(actual - threshold)
        gap_pct = gap / threshold * 100
        above_below = "ABOVE" if actual > threshold else "BELOW"

        context = (
            f"\nOFFICIAL BLS DATA: Initial jobless claims for week ending "
            f"{parsed['report_date_str']} was {actual:,.0f}. "
            f"This is the Kalshi settlement source.\n"
            f"Market threshold: {threshold:,}\n"
            f"Actual vs threshold: {actual:,.0f} vs {threshold:,} ({above_below})\n"
            f"Gap: {gap:,.0f} claims ({gap_pct:.1f}% of threshold)\n"
            f"Calibration guide:\n"
            f"  >5% gap: high confidence (0.85-0.95)\n"
            f"  2-5% gap: moderate confidence (0.65-0.80)\n"
            f"  <2% gap: low confidence (0.40-0.60)\n"
            f"Base your probability on the actual BLS data, not the market price."
        )

        return context, gap_pct


# ══════════════════════════════════════════════════════════════
# FED RATE SIMULATION
# ══════════════════════════════════════════════════════════════

class FedSimulator:
    """Simulates Fed rate decision markets using FRED DFEDTARU data."""

    def __init__(self):
        self.rate_data = {}  # {date: rate}
        self._fetch()

    def _fetch(self):
        print("  Fetching DFEDTARU data from FRED...")
        if not FRED_API_KEY:
            print("  WARNING: No FRED_API_KEY")
            return
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "DFEDTARU",
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": "2020-01-01",
            "sort_order": "asc",
        }
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code != 200:
                print(f"  FRED error: HTTP {r.status_code}")
                return
            obs = r.json().get("observations", [])
            for o in obs:
                if o["value"] != ".":
                    self.rate_data[o["date"]] = float(o["value"])
            print(f"    DFEDTARU: {len(self.rate_data)} observations loaded")
        except Exception as e:
            print(f"  FRED error: {e}")

    def filter_markets(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to clean Fed rate decision markets."""
        fed = df[df["title"].str.contains(
            "fed.*rate|fomc|funds rate|fed.*cut|fed.*hike", case=False, na=False
        )]
        # Remove misclassified sports markets
        fed = fed[~fed["title"].str.contains(
            "yes:|Mike|Tirico|Collinsworth|Michaels|Herbstreit|Total Points",
            case=False, na=False
        )]
        return fed

    def parse_market(self, market: dict) -> dict | None:
        """Extract meeting date context and threshold from Fed market."""
        title = market["title"]

        # These markets are more varied — "cut rates by December", "cut rates 10 times", etc.
        # Extract what we can

        # Try to find a specific date
        date_match = re.search(
            r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}',
            title
        )
        if date_match:
            try:
                dt = pd.to_datetime(date_match.group())
                target_date = dt.strftime("%Y-%m-%d")
            except Exception:
                target_date = None
        else:
            # Try "March 2024", "December 31, 2023" etc.
            date_match2 = re.search(r'(\w+ \d{4})', title)
            if date_match2:
                try:
                    dt = pd.to_datetime(date_match2.group(), format="%B %Y")
                    target_date = dt.strftime("%Y-%m-%d")
                except Exception:
                    target_date = None
            else:
                target_date = None

        if not target_date:
            return None

        # Extract basis points or rate threshold
        bps_match = re.search(r'(\d+)\s*bps', title)
        rate_match = re.search(r'(\d+\.?\d*)\s*%', title)

        is_cut = "cut" in title.lower()
        is_hike = "hike" in title.lower() or "raise" in title.lower()

        return {
            "target_date": target_date,
            "is_cut": is_cut,
            "is_hike": is_hike,
            "bps": int(bps_match.group(1)) if bps_match else None,
            "rate_threshold": float(rate_match.group(1)) if rate_match else None,
        }

    def get_actual_rate(self, parsed: dict) -> float | None:
        """Get actual fed funds rate around the target date."""
        target = datetime.strptime(parsed["target_date"], "%Y-%m-%d")
        # Search for rate on or after target date
        for offset in range(0, 30):
            check = target + timedelta(days=offset)
            check_str = check.strftime("%Y-%m-%d")
            if check_str in self.rate_data:
                return self.rate_data[check_str]
        # Search before
        for offset in range(1, 30):
            check = target - timedelta(days=offset)
            check_str = check.strftime("%Y-%m-%d")
            if check_str in self.rate_data:
                return self.rate_data[check_str]
        return None

    def build_context(self, parsed: dict, actual_rate: float) -> tuple[str, float]:
        """Build Fed rate context."""
        action = "CUT" if parsed["is_cut"] else "HIKE" if parsed["is_hike"] else "DECISION"
        gap = 0.0

        context = (
            f"\nOFFICIAL FED DATA: Federal funds target rate (upper bound) "
            f"as of {parsed['target_date']} was {actual_rate:.2f}%. "
            f"This is the Kalshi settlement source.\n"
            f"Market is about a rate {action}.\n"
        )

        if parsed["rate_threshold"]:
            gap = abs(actual_rate - parsed["rate_threshold"])
            above_below = "ABOVE" if actual_rate > parsed["rate_threshold"] else "BELOW"
            context += (
                f"Rate threshold: {parsed['rate_threshold']}%\n"
                f"Actual vs threshold: {actual_rate:.2f}% vs {parsed['rate_threshold']}% ({above_below})\n"
                f"Gap: {gap:.2f} percentage points\n"
            )

        context += (
            f"Calibration guide:\n"
            f"  Clear direction: high confidence (0.85-0.95)\n"
            f"  Uncertain timing: moderate confidence (0.65-0.80)\n"
            f"  Coin flip: low confidence (0.40-0.60)\n"
            f"Base your probability on the actual Fed data, not the market price."
        )

        return context, gap


# ══════════════════════════════════════════════════════════════
# GENERALIZED SIMULATION ENGINE
# ══════════════════════════════════════════════════════════════

def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            title TEXT,
            category TEXT,
            cloud_probability REAL,
            cloud_confidence REAL,
            market_price REAL,
            price_gap REAL,
            side TEXT,
            amount REAL,
            reasoning TEXT,
            mode TEXT,
            executed INTEGER,
            error TEXT,
            decided_at TEXT,
            session_id TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS simulation_meta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            actual_outcome INTEGER,
            result TEXT,
            data_gap REAL,
            actual_value TEXT,
            resolution_date TEXT,
            sim_timestamp TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS resolved_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            title TEXT,
            category TEXT,
            side TEXT,
            amount REAL,
            entry_price REAL,
            our_probability REAL,
            our_confidence REAL,
            result TEXT,
            pnl REAL,
            pnl_pct REAL,
            strategy TEXT,
            resolved_at TEXT,
            decided_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calibration_reflections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            tier INTEGER,
            trade_id TEXT,
            observation TEXT,
            action_taken TEXT
        )
    """)
    conn.commit()
    return conn


def apply_gates(score: dict, category: str) -> tuple[bool, str, dict]:
    """Apply executor gates. Returns (execute, skip_reason, trade_details)."""
    cloud_prob = score["probability"]
    cloud_conf = score["confidence"]
    market_price = score["market_price"]
    reasoning = score.get("reasoning", "")

    if cloud_prob == 0.0 or cloud_conf == 0.0 or market_price == 0.0:
        return False, "null_signal", {}

    price_gap = abs(cloud_prob - market_price)
    side = "YES" if cloud_prob > market_price else "NO"

    # Historical bias
    price_cents = int(market_price * 100)
    hist_bias = get_historical_bias(price_cents)
    bias_aligned = (side == "YES" and hist_bias > 0) or (side == "NO" and hist_bias < 0)
    if bias_aligned:
        cloud_conf = min(1.0, cloud_conf * 1.1)

    # Kelly sizing
    cost = market_price if side == "YES" else 1.0 - market_price
    our_prob = cloud_prob if side == "YES" else 1.0 - cloud_prob
    b = (1.0 / cost) - 1 if cost > 0 else 0
    q = 1.0 - our_prob
    kelly_raw = (b * our_prob - q) / b if b > 0 else 0
    kelly_bet = max(0, kelly_raw * KELLY_FRACTION)
    amount = round(min(kelly_bet * MAX_NIGHTLY_SPEND, MAX_TRADE_SIZE), 2)

    trade = {
        "side": side,
        "amount": amount,
        "cloud_probability": cloud_prob,
        "cloud_confidence": cloud_conf,
        "price_gap": price_gap,
        "market_price": market_price,
        "reasoning": reasoning,
    }

    # Gate: Category confidence threshold
    cat_conf_min = CATEGORY_CONFIDENCE.get(category, 0.75)
    if cloud_conf < cat_conf_min - 0.001:
        return False, f"confidence {cloud_conf:.2f} < {cat_conf_min} ({category})", trade

    # Gate: Price gap minimum
    if price_gap < PRICE_GAP_MIN:
        return False, f"price gap {price_gap:.2f} < {PRICE_GAP_MIN}", trade

    # Gate: Borderline EV floor
    if 0.20 < cloud_prob < 0.80 and cloud_conf < 0.85 - 0.001:
        return False, f"borderline EV: prob={cloud_prob:.2f} needs conf>0.85", trade

    # Gate: No edge
    if amount <= 0:
        return False, "kelly_says_no_edge", trade

    return True, "", trade


def run_simulation(
    sim_name: str,
    simulator,
    db_path: str,
    limit: int = 1000,
    skip_analysis: bool = False,
):
    """Run simulation for a category."""
    print("\n" + "=" * 70)
    print(f"SIMULATION: {sim_name.upper()}")
    print(f"Model: {MODEL}")
    print(f"Target: {limit} markets")
    print(f"Output: {db_path}")
    print("=" * 70)

    # Load and filter training data
    print(f"\nLoading {TRAIN_PATH}...")
    df = pd.read_parquet(TRAIN_PATH)
    filtered = simulator.filter_markets(df)
    print(f"Filtered markets: {len(filtered)}")

    if len(filtered) == 0:
        print(f"SKIP: No markets found for {sim_name}")
        return None

    # Sample
    if len(filtered) > limit:
        filtered = filtered.sample(n=limit, random_state=42)
    print(f"Selected for simulation: {len(filtered)}")

    # Initialize
    conn = init_db(db_path)
    template = load_prompt_template()
    session_id = f"sim_{sim_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    results = []
    start_time = time.time()
    zero_executed_check = 200  # Check for zero trades after this many

    for i, (_, market) in enumerate(filtered.iterrows()):
        ticker = market["market_id"]
        title = market["title"]
        category = market["category"]
        actual_outcome = int(market["actual_outcome"])
        actual_result = market.get("result", "yes" if actual_outcome == 1 else "no")

        # Market price
        raw_price = market.get("market_price_at_close", market.get("last_price", 50))
        if raw_price is None:
            raw_price = 50
        market_price = raw_price / 100.0 if raw_price > 1 else raw_price
        if market_price < 0.03 or market_price > 0.97:
            market_price = 0.50

        # Skip if already done
        existing = conn.execute("SELECT 1 FROM decisions WHERE ticker = ?", (ticker,)).fetchone()
        if existing:
            continue

        elapsed = time.time() - start_time
        rate = (i + 1) / elapsed * 60 if elapsed > 0 else 0
        eta = (len(filtered) - i - 1) / (rate / 60) / 60 if rate > 0 else 0
        print(f"\n[{i+1}/{len(filtered)}] ({rate:.1f}/min, ETA {eta:.0f}min) ", end="")

        # Parse market
        parsed = simulator.parse_market(dict(market))
        if parsed is None:
            print("SKIP (parse failure)")
            continue

        # Get actual data
        if hasattr(simulator, 'get_weekly_average'):
            actual = simulator.get_weekly_average(parsed.get("week_start"))
        elif hasattr(simulator, 'get_actual_value'):
            actual = simulator.get_actual_value(parsed)
        elif hasattr(simulator, 'get_actual_rate'):
            actual = simulator.get_actual_rate(parsed)
        else:
            actual = None

        if actual is None:
            print("SKIP (no historical data)")
            continue

        # Build context
        data_context, data_gap = simulator.build_context(parsed, actual)

        # Build prompt
        prompt = f"""{template}

Market question: "{title}"
Category: {category}
Current YES price: {market_price:.2f}{data_context}
Recent relevant headlines:
No recent headlines available.
"""

        # Score with Ollama
        result = query_ollama(prompt)
        if result is None:
            print("SKIP (ollama failure)")
            continue

        prob = float(result.get("probability", 0.5))
        conf = float(result.get("confidence", 0.5))
        reasoning = result.get("reasoning", "")

        score = {
            "probability": prob,
            "confidence": conf,
            "market_price": market_price,
            "reasoning": reasoning,
        }

        # Apply gates
        executed, skip_reason, trade = apply_gates(score, category)

        side = trade.get("side", "YES" if prob > market_price else "NO")
        amount = trade.get("amount", 0)
        final_prob = trade.get("cloud_probability", prob)
        final_conf = trade.get("cloud_confidence", conf)
        price_gap = trade.get("price_gap", abs(prob - market_price))

        # P&L
        pnl = 0.0
        won = None
        if executed and amount > 0:
            if side == "YES":
                pnl = (1.0 - market_price) * amount if actual_outcome == 1 else -market_price * amount
            else:
                pnl = market_price * amount if actual_outcome == 0 else -(1.0 - market_price) * amount
            pnl = round(pnl, 2)
            won = (side == "YES" and actual_outcome == 1) or (side == "NO" and actual_outcome == 0)

        # Write decision
        resolution_date = parsed.get("week_start_str", parsed.get("report_date_str",
                          parsed.get("date_str", parsed.get("target_date", ""))))

        conn.execute(
            """INSERT INTO decisions
               (ticker, title, category, cloud_probability, cloud_confidence,
                market_price, price_gap, side, amount, reasoning,
                mode, executed, error, decided_at, session_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'SIM', ?, ?, ?, ?)""",
            (ticker, title, category, final_prob, final_conf,
             market_price, price_gap, side, amount if executed else 0, reasoning,
             int(executed), skip_reason if not executed else None,
             resolution_date, session_id),
        )

        conn.execute(
            """INSERT INTO simulation_meta
               (ticker, actual_outcome, result, data_gap, actual_value,
                resolution_date, sim_timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (ticker, actual_outcome, actual_result, data_gap,
             str(actual), resolution_date,
             datetime.now(timezone.utc).isoformat()),
        )

        if executed and amount > 0:
            entry_price = market_price if side == "YES" else (1.0 - market_price)
            pnl_pct = (pnl / (entry_price * amount) * 100) if entry_price * amount > 0 else 0
            conn.execute(
                """INSERT INTO resolved_trades
                   (ticker, title, category, side, amount, entry_price,
                    our_probability, our_confidence, result, pnl, pnl_pct,
                    strategy, resolved_at, decided_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ticker, title, category, side, amount, entry_price,
                 final_prob, final_conf, actual_result, pnl, round(pnl_pct, 2),
                 f"SIM_{sim_name.upper()}", resolution_date, resolution_date),
            )

        conn.commit()

        result_dict = {
            "ticker": ticker,
            "executed": executed,
            "skip_reason": skip_reason,
            "side": side,
            "prob": final_prob,
            "conf": final_conf,
            "market_price": market_price,
            "price_gap": price_gap,
            "amount": amount if executed else 0,
            "actual_outcome": actual_outcome,
            "data_gap": data_gap,
            "pnl": pnl if executed else None,
            "won": won,
        }
        results.append(result_dict)

        if executed:
            status = "WIN" if won else "LOSS"
            print(f"{status} {side} gap={data_gap:.1f} conf={final_conf:.2f} pnl=${pnl:.2f}")
        else:
            print(f"GATE: {skip_reason[:50]}")

        # Zero-trade bailout check
        if i + 1 >= zero_executed_check:
            exec_count = sum(1 for r in results if r["executed"])
            if exec_count == 0:
                print(f"\n*** BAILOUT: {zero_executed_check} markets processed, 0 executed trades ***")
                print(f"*** Skipping {sim_name} — likely insufficient actionable markets ***")
                conn.close()
                return None

    # ── Summary ──────────────────────────────────────────────
    total = len(results)
    executed_list = [r for r in results if r["executed"]]
    wins = [r for r in executed_list if r.get("won")]
    losses = [r for r in executed_list if r.get("won") is False]

    win_rate = len(wins) / len(executed_list) * 100 if executed_list else 0
    total_pnl = sum(r.get("pnl", 0) for r in executed_list if r.get("pnl") is not None)
    avg_gap = 0
    gap_count = sum(1 for r in executed_list if r.get("data_gap") is not None)
    if gap_count:
        avg_gap = sum(r["data_gap"] for r in executed_list if r["data_gap"] is not None) / gap_count

    # Skip reasons
    skip_reasons = {}
    for r in results:
        if not r["executed"] and r.get("skip_reason"):
            reason = r["skip_reason"]
            if "confidence" in reason:
                reason = "confidence_too_low"
            elif "price gap" in reason:
                reason = "price_gap_too_small"
            elif "borderline" in reason:
                reason = "borderline_ev_floor"
            elif "kelly" in reason:
                reason = "kelly_no_edge"
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1

    top_skip = max(skip_reasons, key=skip_reasons.get) if skip_reasons else "N/A"

    # Tier 2 analysis
    hypothesis = "N/A"
    if not skip_analysis and len(executed_list) >= 3:
        hypothesis = run_tier2(conn, sim_name)

    # Gap breakdown
    gap_breakdown = compute_gap_breakdown(conn, sim_name)

    print(f"\n{'='*70}")
    print(f"SIMULATION {sim_name.upper()} COMPLETE")
    print(f"{'='*70}")
    print(f"Markets processed: {total}")
    print(f"Executed trades: {len(executed_list)}")
    print(f"Win rate: {win_rate:.1f}%")
    print(f"Total P&L: ${total_pnl:.2f}")
    print(f"Gap breakdown: {gap_breakdown}")
    print(f"Top rejection reason: {top_skip}")
    print(f"Tier 2 hypothesis: {hypothesis}")
    print(f"Committing to calibration/{sim_name}_results.txt")

    # Write results file
    results_path = os.path.join(ROOT, "calibration", f"{sim_name}_results.txt")
    with open(results_path, "w") as f:
        f.write(f"SIMULATION {sim_name.upper()} RESULTS\n")
        f.write(f"{'='*50}\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"Markets processed: {total}\n")
        f.write(f"Executed trades: {len(executed_list)}\n")
        f.write(f"Wins: {len(wins)}, Losses: {len(losses)}\n")
        f.write(f"Win rate: {win_rate:.1f}%\n")
        f.write(f"Total P&L: ${total_pnl:.2f}\n")
        f.write(f"Avg data gap: {avg_gap:.2f}\n")
        f.write(f"Gap breakdown: {gap_breakdown}\n")
        f.write(f"Top skip reason: {top_skip}\n")
        f.write(f"Tier 2 hypothesis: {hypothesis}\n")

    elapsed_total = time.time() - start_time
    print(f"\nCompleted in {elapsed_total/60:.1f} minutes.")

    conn.close()

    return {
        "name": sim_name,
        "trades": len(executed_list),
        "win_rate": win_rate,
        "avg_gap": avg_gap,
        "hypothesis": hypothesis,
        "total_pnl": total_pnl,
    }


def compute_gap_breakdown(conn: sqlite3.Connection, sim_name: str) -> str:
    """Compute gap distance breakdown string."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT d.executed, d.side, m.actual_outcome, m.data_gap
        FROM decisions d
        JOIN simulation_meta m ON d.ticker = m.ticker
        WHERE d.executed = 1 AND d.amount > 0
    """).fetchall()

    if not rows:
        return "No executed trades"

    # For TSA/Jobless: gap is %, for CPI: gap is pp, for Fed: gap is pp
    # Use generic buckets
    buckets = {"low": [0, 0], "med": [0, 0], "high": [0, 0]}

    for row in rows:
        gap = row["data_gap"] or 0
        outcome = row["actual_outcome"]
        side = row["side"]
        won = (side == "YES" and outcome == 1) or (side == "NO" and outcome == 0)
        idx = 0 if won else 1

        if gap < 2:
            buckets["low"][idx] += 1
        elif gap < 5:
            buckets["med"][idx] += 1
        else:
            buckets["high"][idx] += 1

    parts = []
    for name, (w, l) in buckets.items():
        total = w + l
        if total > 0:
            wr = w / total * 100
            parts.append(f"{name}={w}W/{l}L ({wr:.0f}%)")

    return " | ".join(parts) if parts else "No data"


def run_tier2(conn: sqlite3.Connection, sim_name: str) -> str:
    """Run Tier 2 pattern analysis."""
    conn.row_factory = sqlite3.Row
    losses = conn.execute("""
        SELECT ticker, title, category, side, our_confidence, pnl
        FROM resolved_trades WHERE pnl <= 0 AND our_confidence > 0.70
        ORDER BY pnl ASC LIMIT 30
    """).fetchall()

    wins = conn.execute("""
        SELECT ticker, title, category, side, our_confidence, pnl
        FROM resolved_trades WHERE pnl > 0 AND our_confidence > 0.70
        ORDER BY pnl DESC LIMIT 15
    """).fetchall()

    if len(losses) < 2:
        return "Insufficient losses for analysis"

    loss_lines = [f"  LOSS: {l['title'][:60]} | conf={l['our_confidence']:.2f} | pnl=${l['pnl']:.2f}" for l in losses]
    win_lines = [f"  WIN: {w['title'][:60]} | conf={w['our_confidence']:.2f}" for w in wins[:10]]

    prompt = f"""You are analyzing {sim_name} trading performance for a prediction market bot.

LOSSES:
{chr(10).join(loss_lines)}

WINS:
{chr(10).join(win_lines) if win_lines else '  (none)'}

Answer in JSON:
{{"common_loss_pattern": "<pattern>", "recommended_change": "<change>", "confidence": "<low/medium/high>"}}"""

    result = query_ollama(prompt)
    if result:
        pattern = result.get("common_loss_pattern", "N/A")
        conn.execute(
            "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 2, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), f"sim_{sim_name}",
             json.dumps(result)[:500], "hypothesis_generated"),
        )
        conn.commit()
        return pattern[:80]
    return "Analysis failed"


# ══════════════════════════════════════════════════════════════
# MAIN — Sequential simulation runner
# ══════════════════════════════════════════════════════════════

def wait_for_weather_sim():
    """Wait for the weather simulation to complete."""
    weather_db = os.path.join(ROOT, "calibration", "simulation_decisions.sqlite")
    print("Checking weather simulation status...")
    while True:
        if not os.path.exists(weather_db):
            print("  Weather DB not found — waiting 60s...")
            time.sleep(60)
            continue
        try:
            conn = sqlite3.connect(weather_db)
            total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
            conn.close()
            if total >= 950:  # Close enough to 1000
                print(f"  Weather simulation complete: {total} markets processed")
                return
            print(f"  Weather: {total}/1000 — waiting 120s...")
            time.sleep(120)
        except Exception:
            time.sleep(60)


def check_ollama():
    """Verify Ollama is responsive."""
    for attempt in range(3):
        try:
            r = requests.get("http://localhost:11434/api/tags", timeout=10)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        print(f"  Ollama not responding, waiting 60s (attempt {attempt+1}/3)...")
        time.sleep(60)
    return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Multi-category simulation runner")
    parser.add_argument("--skip-weather-wait", action="store_true",
                        help="Skip waiting for weather simulation")
    parser.add_argument("--category", type=str, default=None,
                        help="Run only a specific category (tsa, cpi, jobless, fed)")
    parser.add_argument("--limit", type=int, default=1000,
                        help="Max markets per category (default 1000)")
    args = parser.parse_args()

    print("=" * 70)
    print("KALBOT MULTI-CATEGORY SIMULATION RUNNER")
    print(f"Model: {MODEL}")
    print("=" * 70)

    # Wait for weather to finish unless skipped
    if not args.skip_weather_wait:
        wait_for_weather_sim()

    # Define simulations
    simulations = [
        {
            "name": "tsa",
            "simulator": TSASimulator,
            "db": os.path.join(ROOT, "calibration", "simulation_tsa.sqlite"),
        },
        {
            "name": "cpi",
            "simulator": CPISimulator,
            "db": os.path.join(ROOT, "calibration", "simulation_cpi.sqlite"),
        },
        {
            "name": "jobless",
            "simulator": JoblessSimulator,
            "db": os.path.join(ROOT, "calibration", "simulation_jobless.sqlite"),
        },
        {
            "name": "fed",
            "simulator": FedSimulator,
            "db": os.path.join(ROOT, "calibration", "simulation_fed.sqlite"),
        },
    ]

    # Filter to specific category if requested
    if args.category:
        simulations = [s for s in simulations if s["name"] == args.category]

    all_results = []

    for sim_config in simulations:
        name = sim_config["name"]
        db_path = sim_config["db"]

        print(f"\n{'#' * 70}")
        print(f"# Starting {name.upper()} simulation")
        print(f"{'#' * 70}")

        # Check Ollama between runs
        if not check_ollama():
            print(f"  FATAL: Ollama not responding. Stopping.")
            break

        # Initialize simulator
        simulator = sim_config["simulator"]()

        # Run simulation
        result = run_simulation(
            sim_name=name,
            simulator=simulator,
            db_path=db_path,
            limit=args.limit,
        )

        if result:
            all_results.append(result)
        else:
            all_results.append({
                "name": name,
                "trades": 0,
                "win_rate": 0,
                "avg_gap": 0,
                "hypothesis": "SKIPPED — 0 executed trades after 200 markets",
                "total_pnl": 0,
            })

        # Brief pause between simulations
        print("\nPausing 30s before next simulation...")
        time.sleep(30)

    # ── Master Summary ───────────────────────────────────────
    if all_results:
        print("\n\n" + "=" * 70)
        print("SIMULATION RESULTS SUMMARY")
        print("=" * 70)
        print(f"\n{'Category':<12} {'Trades':>8} {'Win Rate':>10} {'Avg Gap':>10} {'P&L':>10} {'Hypothesis'}")
        print("-" * 90)

        for r in all_results:
            hyp = r.get("hypothesis", "N/A")[:40]
            print(f"{r['name']:<12} {r['trades']:>8} {r['win_rate']:>9.1f}% {r['avg_gap']:>10.2f} ${r.get('total_pnl', 0):>8.2f} {hyp}")

        # Save master summary
        summary_path = os.path.join(ROOT, "calibration", "simulation_master_summary.txt")
        with open(summary_path, "w") as f:
            f.write("SIMULATION RESULTS SUMMARY\n")
            f.write(f"Date: {datetime.now().isoformat()}\n")
            f.write("=" * 70 + "\n\n")
            for r in all_results:
                f.write(f"{r['name'].upper()}: {r['trades']} trades, "
                        f"{r['win_rate']:.1f}% WR, ${r.get('total_pnl', 0):.2f} P&L\n")
                f.write(f"  Hypothesis: {r.get('hypothesis', 'N/A')}\n\n")

        print(f"\nMaster summary saved to: {summary_path}")

    print("\nAll simulations complete.")


if __name__ == "__main__":
    main()
