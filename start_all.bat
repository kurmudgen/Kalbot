@echo off
REM KalBot Auto-Start Script
REM Starts both Kalshi and Stock bots plus watchdog
REM Called by Windows Task Scheduler on login

cd /d S:\kalbot

REM Clean stale lock files from prior shutdown
del /q dual_strategy.py.lock 2>nul
del /q run_stock_bot.py.lock 2>nul

REM Start watchdog (monitors and auto-restarts bots)
start /min "KalBot-Watchdog" .venv\Scripts\python.exe bot\watchdog.py

REM Small delay then start bots directly too (watchdog will manage restarts)
timeout /t 3 /nobreak >nul

REM Start Kalshi bot
start /min "KalBot-Kalshi" .venv\Scripts\python.exe bot\dual_strategy.py

REM Start Stock bot
start /min "KalBot-Stocks" .venv\Scripts\python.exe stock_bot\run_stock_bot.py

echo KalBot started at %date% %time% >> logs\autostart.log
