"""
SEC EDGAR real-time filing monitor.
Catches 8-K filings (material events) within seconds of publication.
Most market-moving filing types:
- Item 1.01: Material agreements
- Item 2.01: Asset acquisitions
- Item 5.02: Executive departures
- Item 8.01: Other events (catch-all for big news)
"""

import os
import requests
from datetime import datetime, timezone


EDGAR_RSS = "https://efts.sec.gov/LATEST/search-index?q=%228-K%22&dateRange=custom&startdt={date}&enddt={date}&forms=8-K"
EDGAR_FULL = "https://efts.sec.gov/LATEST/search-index?forms=8-K&dateRange=custom&startdt={date}&enddt={date}"


def get_recent_8k_filings(limit: int = 10) -> list[dict]:
    """Get recent 8-K filings from EDGAR."""
    filings = []
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        url = f"https://efts.sec.gov/LATEST/search-index?forms=8-K&dateRange=custom&startdt={today}&enddt={today}"

        headers = {
            "User-Agent": "KalBot research@formationlabs.com",
            "Accept": "application/json",
        }

        r = requests.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={"forms": "8-K", "dateRange": "custom", "startdt": today, "enddt": today},
            headers=headers,
            timeout=10,
        )

        if r.status_code == 200:
            data = r.json()
            hits = data.get("hits", {}).get("hits", [])
            for hit in hits[:limit]:
                source = hit.get("_source", {})
                filings.append({
                    "company": source.get("display_names", [""])[0] if source.get("display_names") else "",
                    "ticker": source.get("tickers", [""])[0] if source.get("tickers") else "",
                    "form": source.get("form_type", ""),
                    "filed": source.get("file_date", ""),
                    "description": source.get("display_description", ""),
                })
    except Exception as e:
        # Fallback: use EDGAR RSS feed
        try:
            r = requests.get(
                "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&dateb=&owner=include&count=10&search_text=&output=atom",
                headers={"User-Agent": "KalBot research@formationlabs.com"},
                timeout=10,
            )
            if r.status_code == 200:
                # Parse Atom XML roughly
                import re
                titles = re.findall(r"<title[^>]*>(.*?)</title>", r.text)
                for t in titles[1:limit+1]:  # Skip feed title
                    filings.append({
                        "company": t.split(" - ")[0] if " - " in t else t,
                        "ticker": "",
                        "form": "8-K",
                        "filed": datetime.now().strftime("%Y-%m-%d"),
                        "description": t,
                    })
        except Exception:
            pass

    return filings


def check_for_trading_signals(filings: list[dict]) -> list[dict]:
    """Check if any 8-K filings are tradeable."""
    signals = []

    # Keywords that indicate market-moving events
    bullish_keywords = ["acquisition", "merger", "partnership", "contract", "award",
                       "dividend", "buyback", "repurchase", "upgrade"]
    bearish_keywords = ["resignation", "termination", "layoff", "restructuring",
                       "breach", "investigation", "lawsuit", "default", "bankruptcy",
                       "restatement", "going concern", "cybersecurity incident"]

    for f in filings:
        desc = (f.get("description", "") or "").lower()
        company = f.get("company", "")
        ticker = f.get("ticker", "")

        if not ticker:
            continue

        signal = None
        for kw in bullish_keywords:
            if kw in desc:
                signal = {"direction": "buy", "keyword": kw}
                break

        if not signal:
            for kw in bearish_keywords:
                if kw in desc:
                    signal = {"direction": "sell", "keyword": kw}
                    break

        if signal:
            signals.append({
                "ticker": ticker,
                "company": company,
                "direction": signal["direction"],
                "trigger": signal["keyword"],
                "description": f.get("description", "")[:100],
            })

    return signals


def scan_edgar() -> list[dict]:
    """Full EDGAR scan — get filings, check for signals."""
    filings = get_recent_8k_filings(limit=20)
    if not filings:
        return []

    signals = check_for_trading_signals(filings)
    if signals:
        print(f"  EDGAR: {len(signals)} tradeable 8-K filings found")
        for s in signals:
            print(f"    {s['direction'].upper()} {s['ticker']} — {s['trigger']}: {s['description'][:60]}")

    return signals


if __name__ == "__main__":
    print("SEC EDGAR Monitor")
    print("=" * 40)
    filings = get_recent_8k_filings()
    print(f"{len(filings)} recent 8-K filings:")
    for f in filings[:5]:
        print(f"  {f['ticker'] or '?'} | {f['company'][:30]} | {f['description'][:50]}")

    signals = check_for_trading_signals(filings)
    print(f"\n{len(signals)} trading signals")
