@echo off
:: daily_refresh.bat — Runs the daily market data refresh
:: Scheduled via Windows Task Scheduler to fire at 3:30 PM IST (Mon-Fri)

cd /d "c:\Users\Sumit meena\OneDrive\Desktop\Claude Code\Market Price Dashboard"

echo [%DATE% %TIME%] Starting daily market refresh...
"C:\Users\Sumit meena\AppData\Local\Programs\Python\Python311\python.exe" backend\daily_refresh.py >> logs\refresh.log 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [%DATE% %TIME%] Refresh completed successfully.
) else (
    echo [%DATE% %TIME%] Refresh FAILED with exit code %ERRORLEVEL%. Check logs\refresh.log
)
