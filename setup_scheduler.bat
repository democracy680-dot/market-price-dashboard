@echo off
:: setup_scheduler.bat — Registers a Windows Task Scheduler task to run
:: daily_refresh.py at 3:30 PM IST (Mon–Fri).
::
:: Run this ONCE as Administrator to install the scheduled task.
:: To remove: schtasks /Delete /TN "MarketPriceDashboard_DailyRefresh" /F

set TASK_NAME=MarketPriceDashboard_DailyRefresh
set SCRIPT_PATH=c:\Users\Sumit meena\OneDrive\Desktop\Claude Code\Market Price Dashboard\run_refresh.bat

echo Registering scheduled task "%TASK_NAME%"...

schtasks /Create /F ^
  /TN "%TASK_NAME%" ^
  /TR "\"%SCRIPT_PATH%\"" ^
  /SC WEEKLY ^
  /D MON,TUE,WED,THU,FRI ^
  /ST 15:30 ^
  /RL HIGHEST

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Task registered successfully!
    echo It will run every weekday at 3:30 PM.
    echo.
    echo To verify:  schtasks /Query /TN "%TASK_NAME%" /FO LIST
    echo To remove:  schtasks /Delete /TN "%TASK_NAME%" /F
    echo To run now: schtasks /Run /TN "%TASK_NAME%"
) else (
    echo.
    echo ERROR: Failed to register task. Make sure you are running as Administrator.
)

pause
