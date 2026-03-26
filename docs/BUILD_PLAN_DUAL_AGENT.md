# Dual-Agent Coinbase Momentum Architecture

## Status: PLANNED — waiting for 7 days of baseline data + GPU upgrade

## Overview
Two specialized agents with opposite timing profiles working the same market:
- **Hunter**: enters at pump start, lower confidence, smaller size, earlier exit
- **Sniper**: enters after confirmation, higher confidence, larger size

## Architecture

### CoinbaseHunter
- Budget: $40 of $100 total
- Max position: $15
- Max simultaneous: 3
- Hold time: 2 hours max
- Signal requirement: 2/4 layers, at least 1 pre-price-movement signal
- Confidence floor: 0.65
- Decay exit: first signal (tight)

### CoinbaseSniper (current module, refined)
- Budget: $60 of $100 total
- Max position: $25
- Max simultaneous: 2
- Hold time: 4 hours max
- Signal requirement: 3/4 layers
- Confidence floor: 0.82
- Decay exit: second signal (standard)

## Hunter Pre-Price-Movement Signals (NEW — requires infrastructure)

### 1. Order Book Wall Detector
- Coinbase WebSocket feed for real-time order book
- Fires when buy wall > 3x average order size appears
- Requires: persistent WebSocket connection, VRAM for continuous processing

### 2. Cross-Exchange Arbitrage Gap
- Dexscreener API for DEX prices
- Fires when DEX price diverges >2% from Coinbase (Coinbase lower)
- Requires: polling loop, price normalization across exchanges

### 3. Watchlist Velocity Detector
- CoinGecko API watchlist count changes
- Fires when watchlist count increases >15% in 30 minutes
- Requires: frequent polling, baseline tracking per token

### 4. Wallet Accumulation (stretch goal)
- Etherscan/Solscan API for transfer counts
- Unusual accumulation by small number of wallets before price move
- Requires: on-chain API access, pattern detection

## Agent Communication
- Hunter entry writes to shared_signals table in crypto_momentum.sqlite
- Sniper checks shared_signals every 60 seconds
- If Hunter position exists AND Sniper confirmation fires → Sniper adds position
- First decay signal from either agent closes BOTH positions

## P&L Tracking
- Separate columns in crypto_momentum.sqlite: agent_type = 'hunter' or 'sniper'
- Daily summary: Hunter P&L vs Sniper P&L vs combined
- Self-calibrator Tier 2 analyzes each agent independently

## Prerequisites
- [ ] 7 days of Sniper-only baseline data (current system running)
- [ ] GPU upgrade for persistent model loading + WebSocket processing
- [ ] Coinbase WebSocket SDK integration
- [ ] Etherscan/Solscan API keys (free tier)
- [ ] Self-calibrator producing reliable pattern analysis

## GPU Considerations
Current: 16GB VRAM handles qwen2.5:32b but with swapping
Ideal: 24GB+ VRAM for persistent 32b + 3b pre-screener simultaneously
The Hunter's real-time signal processing adds continuous load on top of
the existing model inference. With 16GB this means model swapping delays
that could miss sub-minute entry windows. 24GB eliminates this bottleneck.

## Build Order
1. Dynamic decay exit logic for existing Sniper
2. Hunter signal detectors (order book, cross-exchange, watchlist)
3. Agent communication via shared_signals table
4. Combined position management
5. Separate P&L tracking
6. Self-calibrator comparison: Hunter vs Sniper vs combined
