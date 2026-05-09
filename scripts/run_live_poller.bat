@echo off
:: run_live_poller.bat — Start the Zerodha live price poller for quarterly result stocks.
:: Double-click this file before market open (09:15 IST) on result announcement days.
:: The poller automatically exits at 15:35 IST.

cd /d "c:\Users\Sumit meena\OneDrive\Desktop\Claude Code\Market Price Dashboard"

echo [%DATE% %TIME%] Starting Zerodha live price poller...
"C:\Users\Sumit meena\AppData\Local\Programs\Python\Python311\python.exe" backend\live_price_poller.py

echo.
echo [%DATE% %TIME%] Poller has stopped.
pause
