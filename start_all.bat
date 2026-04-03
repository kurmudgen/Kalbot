@echo off
REM KalBot Auto-Start Script
REM Uses VBS launcher for terminal-independent persistent processes
REM Called by Windows Task Scheduler on login

cd /d S:\kalbot
cscript //nologo start_persistent.vbs
echo KalBot started at %date% %time% >> logs\autostart.log
