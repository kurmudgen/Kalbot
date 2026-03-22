# KalBot Dashboard — Build Plan

## Tech Stack
- Next.js 14 (App Router)
- Tailwind CSS + shadcn/ui components
- Recharts for charts/graphs
- Deployed on Vercel
- Data: bot pushes status.json to GitHub every cycle

## Data Flow
```
Bot (S:\kalbot) → writes dashboard/status.json → git push → GitHub
Vercel dashboard → fetches raw.githubusercontent.com/.../status.json → renders
```

## Pages

### 1. Main Dashboard (/)
- **Header**: KalBot logo, bot status indicator (green/red), last update time
- **P&L Cards**: Today, This Week, All-Time (with +/- color)
- **Account Cards**: Kalshi balance, Alpaca balance
- **Cumulative P&L Chart**: line graph over time
- **Active Positions**: table with live P&L per position
- **Alerts Banner**: errors, kill switch status, API issues

### 2. Trade Log (/trades)
- **Sortable/filterable table**: every trade decision
- Columns: time, market, side, amount, confidence, edge, outcome, P&L
- Filters: executed/skipped, by strategy, by category, date range
- Click to expand: full reasoning, model votes, research text

### 3. Strategy Performance (/strategies)
- **Strategy cards**: each of the 9 strategies with WR, Brier, P&L
- **Allocation pie chart**: live percentages from rebalancer
- **Allocation history**: line chart showing how allocation shifted over time
- **Per-strategy P&L chart**: which strategies are making/losing money

### 4. Ensemble View (/ensemble)
- **Model leaderboard**: Perplexity vs Claude vs DeepSeek accuracy
- **Consensus rate chart**: % of analyses reaching consensus over time
- **Disagreement log**: when models disagree, show each vote
- **Per-model per-category heatmap**: who's best at what

### 5. Market Scanner (/markets)
- **Active markets table**: ticker, price, volume, category
- **Filter scores**: passed/rejected with scores
- **Sniper countdown**: markets expiring soon with timers
- **Price movement alerts**: markets with recent sharp moves

### 6. News Pool (/news)
- **Research feed**: Perplexity findings, timestamped
- **Breaking news**: highlighted
- **Category tabs**: filter by economics, weather, etc

### 7. Analytics (/analytics)
- **Profitability heatmap**: hours × days of week
- **Cost tracker**: API spend vs trading revenue
- **Win rate over time**: rolling 7-day WR chart
- **Confidence vs outcome**: calibration curve
- **Cycle timing**: how long each step takes

### 8. System (/system)
- **Health indicators**: Ollama, Kalshi API, Alpaca API, Perplexity, Claude, DeepSeek
- **GPU/VRAM**: current usage
- **Error log**: recent errors with timestamps
- **Kill switch**: toggle button
- **Weekly repo scan results**: what repos updated

## status.json Structure
```json
{
  "updated_at": "ISO timestamp",
  "bot_status": "running|stopped",
  "accounts": {
    "kalshi": {"balance": 50, "paper_mode": true},
    "alpaca": {"cash": 100000, "portfolio_value": 100000, "paper_mode": true}
  },
  "pnl": {
    "today": 0.15,
    "week": 0.15,
    "all_time": 0.15,
    "history": [{"date": "2026-03-22", "pnl": 0.15}]
  },
  "positions": [],
  "recent_trades": [{...last 50 trades...}],
  "strategies": {
    "WEATHER": {"allocation": 0.30, "trades": 0, "pnl": 0, "win_rate": 0},
    ...
  },
  "ensemble": {
    "models": {
      "perplexity": {"analyses": 10, "accuracy": 0.7},
      "claude": {"analyses": 10, "accuracy": 0.65},
      "deepseek": {"analyses": 10, "accuracy": 0.6}
    },
    "consensus_rate": 0.5,
    "recent_votes": [{...}]
  },
  "scanner": {
    "total_markets": 573,
    "with_prices": 429,
    "passed_filter": 4,
    "categories": {"economics": 400, "inflation": 173}
  },
  "news_pool": [{...recent research...}],
  "alerts": [],
  "system": {
    "ollama": "ok",
    "gpu_vram_used": "5GB",
    "errors_24h": 0,
    "cycle_time_avg": "120s",
    "api_costs_today": 0.05
  },
  "heatmap": {
    "hour_day_pnl": [[0,0,0,...], ...]
  }
}
```

## Design
- Dark mode default (dark gray/navy background)
- Green for profits, red for losses
- Accent color: electric blue (#3B82F6)
- Cards with subtle borders and shadows
- Responsive: works on phone
- Auto-refresh every 60 seconds
