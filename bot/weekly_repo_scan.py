"""
Weekly repo scanner: runs every Sunday to check tracked repos for updates.
Pulls latest commits, looks for new strategies/data, and logs findings.

Tracked repos:
- Weather bots and calibration data
- Kalshi trading strategies
- Prediction market analysis
- Forecasting benchmarks
"""

import os
import subprocess
import sqlite3
from datetime import datetime, timezone

REPOS_DIR = os.path.join(os.path.dirname(__file__), "..", "repos")
SCAN_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "repo_scans.sqlite")

# All repos we track
TRACKED_REPOS = [
    # Core analysis
    "prediction-market-analysis",
    "tools-and-analysis",
    "forecasting-tools",
    "kalshi-data-collector",
    "Awesome-Prediction-Market-Tools",
    # Trading bots
    "weatherbots",
    "kalshi-ai-trading-bot",
    "kalshi-trading-mcp",
    "Fully-Autonomous-Polymarket-AI-Trading-Bot",
    "llm-forecast",
    # Quant strategies
    "kalshi-trading",
    "kalshi-market-making",
    "Kalshi_Crypto_Monte_Carlo",
    "kalshi_crypto",
    "polymarket-kalshi-weather-bot",
]

# Repos to watch (not cloned, just check for updates)
WATCH_REPOS = [
    "https://github.com/ryanfrigo/kalshi-ai-trading-bot",
    "https://github.com/JHenzi/weatherbots",
    "https://github.com/alexandermazza/kalshi-trading-mcp",
    "https://github.com/quantgalore/kalshi-trading",
    "https://github.com/forecastingresearch/forecastbench-datasets",
]


def init_scan_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(SCAN_DB), exist_ok=True)
    conn = sqlite3.connect(SCAN_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo TEXT,
            last_commit_before TEXT,
            last_commit_after TEXT,
            new_commits INTEGER,
            interesting_files TEXT,
            scanned_at TEXT
        )
    """)
    conn.commit()
    return conn


def pull_repo(repo_name: str) -> dict:
    """Git pull a repo and check for new commits."""
    repo_path = os.path.join(REPOS_DIR, repo_name)
    if not os.path.exists(repo_path):
        return {"repo": repo_name, "status": "not_found"}

    # Get current HEAD
    try:
        before = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path, capture_output=True, text=True, timeout=10,
        ).stdout.strip()
    except Exception:
        before = "unknown"

    # Pull latest
    try:
        result = subprocess.run(
            ["git", "pull", "--ff-only"],
            cwd=repo_path, capture_output=True, text=True, timeout=60,
        )
        pull_output = result.stdout.strip()
    except Exception as e:
        return {"repo": repo_name, "status": f"pull_failed: {e}"}

    # Get new HEAD
    try:
        after = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path, capture_output=True, text=True, timeout=10,
        ).stdout.strip()
    except Exception:
        after = "unknown"

    # Count new commits
    new_commits = 0
    interesting_files = []
    if before != after and before != "unknown":
        try:
            log = subprocess.run(
                ["git", "log", f"{before}..{after}", "--oneline"],
                cwd=repo_path, capture_output=True, text=True, timeout=10,
            ).stdout.strip()
            new_commits = len(log.split("\n")) if log else 0

            # Check what files changed
            diff = subprocess.run(
                ["git", "diff", "--name-only", before, after],
                cwd=repo_path, capture_output=True, text=True, timeout=10,
            ).stdout.strip()
            changed = diff.split("\n") if diff else []

            # Flag interesting changes
            for f in changed:
                fl = f.lower()
                if any(kw in fl for kw in [
                    "strategy", "calibrat", "weight", "config", "prompt",
                    "model", "signal", "trade", "backtest", "result",
                    ".json", ".csv", ".parquet",
                ]):
                    interesting_files.append(f)
        except Exception:
            pass

    return {
        "repo": repo_name,
        "status": "updated" if before != after else "up_to_date",
        "before": before[:8],
        "after": after[:8],
        "new_commits": new_commits,
        "interesting_files": interesting_files,
    }


def run_weekly_scan():
    """Pull all tracked repos and report updates."""
    print(f"{'='*60}")
    print(f"  WEEKLY REPO SCAN — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    conn = init_scan_db()
    updated = []
    interesting = []

    for repo in TRACKED_REPOS:
        result = pull_repo(repo)
        status = result.get("status", "unknown")

        if status == "updated":
            n = result["new_commits"]
            files = result["interesting_files"]
            print(f"  UPDATED: {repo} — {n} new commits")
            for f in files[:5]:
                print(f"    * {f}")
            updated.append(repo)
            if files:
                interesting.append((repo, files))
        elif status == "up_to_date":
            print(f"  OK: {repo}")
        else:
            print(f"  SKIP: {repo} ({status})")

        conn.execute(
            """INSERT INTO scans
               (repo, last_commit_before, last_commit_after,
                new_commits, interesting_files, scanned_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (repo, result.get("before", ""), result.get("after", ""),
             result.get("new_commits", 0),
             ",".join(result.get("interesting_files", [])),
             datetime.now(timezone.utc).isoformat()),
        )

    conn.commit()
    conn.close()

    # Summary
    print(f"\n{'='*60}")
    print(f"  {len(updated)}/{len(TRACKED_REPOS)} repos had updates")
    if interesting:
        print(f"\n  WORTH REVIEWING:")
        for repo, files in interesting:
            print(f"    {repo}: {', '.join(files[:3])}")
    print(f"{'='*60}")

    return updated, interesting


if __name__ == "__main__":
    run_weekly_scan()
