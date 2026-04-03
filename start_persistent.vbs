' KalBot Persistent Launcher
' Runs bot processes detached from any terminal using pythonw.exe
' Survives terminal closures, shell resets, and SSH disconnects
' Called by start_all.bat or Task Scheduler directly

Set WshShell = CreateObject("WScript.Shell")

' Clean stale lock files
Set fso = CreateObject("Scripting.FileSystemObject")
If fso.FileExists("S:\kalbot\dual_strategy.py.lock") Then fso.DeleteFile "S:\kalbot\dual_strategy.py.lock"
If fso.FileExists("S:\kalbot\run_stock_bot.py.lock") Then fso.DeleteFile "S:\kalbot\run_stock_bot.py.lock"

' Start watchdog (monitors and restarts bots)
WshShell.Run "S:\kalbot\.venv\Scripts\pythonw.exe S:\kalbot\bot\watchdog.py", 0, False

' Small delay
WScript.Sleep 2000

' Start Kalshi bot
WshShell.Run "S:\kalbot\.venv\Scripts\pythonw.exe S:\kalbot\bot\dual_strategy.py", 0, False

' Start Stock bot
WshShell.Run "S:\kalbot\.venv\Scripts\pythonw.exe S:\kalbot\stock_bot\run_stock_bot.py", 0, False
