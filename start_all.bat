@echo off
REM KalBot Auto-Start Script
REM Starts both Kalshi and Stock bots
REM Called by Windows Task Scheduler on login

cd /d S:\kalbot

REM Start Kalshi bot (runs 24/7)
start /min "KalBot-Kalshi" .venv\Scripts\python.exe bot\dual_strategy.py

REM Start Stock bot (only trades during market hours)
start /min "KalBot-Stocks" .venv\Scripts\python.exe stock_bot\run_stock_bot.py

echo KalBot started at %date% %time% >> logs\autostart.log
