@echo off
:: run_news_refresh.bat — Runs the news feed refresh (RSS parser + DB upsert)
:: Scheduled via Windows Task Scheduler to fire every 30 minutes (Mon-Fri).

cd /d "c:\Users\Sumit meena\OneDrive\Desktop\Claude Code\Market Price Dashboard"

echo [%DATE% %TIME%] Starting news refresh...
"C:\Users\Sumit meena\AppData\Local\Programs\Python\Python311\python.exe" backend\news_fetcher.py >> logs\news_refresh.log 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [%DATE% %TIME%] News refresh completed successfully.
) else (
    echo [%DATE% %TIME%] News refresh FAILED with exit code %ERRORLEVEL%. Check logs\news_refresh.log
)
