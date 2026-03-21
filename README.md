# KalBot - Kalshi Prediction Market Trading Bot

A multi-stage prediction market trading bot that uses local and cloud AI models to identify mispriced markets on Kalshi.

## Project Structure

```
kalbot/
├── bot/                    # Core bot files
│   ├── market_scanner.py   # Polls Kalshi API for open markets
│   ├── local_filter.py     # Scores markets with local Ollama model
│   ├── cloud_analyst.py    # Deep analysis via DeepSeek/Gemini
│   ├── executor.py         # Trade execution with safety limits
│   ├── run_night.py        # Overnight orchestration loop
│   └── morning_review.py   # Post-session summary report
├── calibration/            # Calibration and evaluation
│   ├── test_ollama.py      # Ollama connectivity test
│   ├── run_calibration.py  # Run against training data
│   ├── run_validation.py   # Run against validation data
│   ├── compare.py          # Train vs val comparison
│   └── extract_and_process.py  # Data extraction and splitting
├── data/
│   ├── raw/                # Downloaded datasets
│   ├── splits/             # train/val/test parquet files
│   └── live/               # Live market snapshots
├── logs/                   # SQLite databases and run logs
├── prompts/                # Prompt templates for local model
│   └── local_filter.txt    # Base forecasting prompt
└── results/                # Calibration output CSVs
```

## Setup

1. **Clone and enter the project:**
   ```bash
   cd ~/kalbot
   ```

2. **Activate the virtual environment:**
   ```bash
   source .venv/Scripts/activate  # Windows
   source .venv/bin/activate       # Linux/Mac
   ```

3. **Copy and fill in environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys
   ```

4. **Ensure Ollama is running with the model loaded:**
   ```bash
   ollama serve
   ollama pull qwen2.5:14b
   python calibration/test_ollama.py  # Should print PASS
   ```

5. **Extract and process the dataset (if not already done):**
   ```bash
   python calibration/extract_and_process.py
   ```

## Running Calibration

Run calibration against the training set to evaluate model performance:

```bash
# Full training set
python calibration/run_calibration.py

# Single category (faster)
python calibration/run_calibration.py --category tsa

# Limited sample
python calibration/run_calibration.py --limit 50
```

Then validate:

```bash
python calibration/run_validation.py
```

Compare train vs validation:

```bash
python calibration/compare.py
```

The compare script flags overfitting if training win rate exceeds validation by more than 5 percentage points.

## Running the Bot (Paper Mode)

With `PAPER_TRADE=true` in `.env`, the bot logs all decisions without placing real trades:

```bash
python bot/run_night.py
```

The bot runs until 6am local time, cycling every 5 minutes through:
1. **Scanner** - pulls open markets from Kalshi
2. **Local Filter** - scores each market with Ollama (qwen2.5:14b)
3. **Cloud Analyst** - deep analysis via DeepSeek/Gemini
4. **Executor** - applies safety checks and logs trades

## Morning Review

Check what happened overnight:

```bash
# Last night's summary
python bot/morning_review.py

# Last 7 days
python bot/morning_review.py --days 7
```

## Hard Limits

These limits protect against runaway losses:

| Limit | Default | What it protects |
|-------|---------|------------------|
| `PAPER_TRADE` | `true` | Prevents real trades until explicitly enabled |
| `MAX_TRADE_SIZE` | `$10` | Maximum per-trade exposure |
| `MAX_NIGHTLY_SPEND` | `$50` | Total deployment cap per session |
| Confidence threshold | `0.75` | Minimum cloud analyst confidence to trade |
| Price gap threshold | `0.08` | Minimum mispricing to justify a trade |
| Category whitelist | economics, tsa, weather, inflation | Only trades in analyzed categories |

## Target Categories

KalBot only trades in these well-defined, data-driven categories:
- **Economics** - Federal Reserve, interest rates, FOMC, GDP
- **Inflation** - CPI, PCE, jobless claims, payroll data
- **TSA** - Airport passenger volume
- **Weather** - Temperature, precipitation

All geopolitical, military, election, and regime-change markets are excluded.
