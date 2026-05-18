@echo off
:: setup_scheduler.bat — Registers a Windows Task Scheduler task to run
:: daily_refresh.py at 3:30 PM IST (Mon–Fri).
::
:: Run this ONCE as the logged-in user (no Administrator required).
:: Uses task_definition.xml which handles paths with spaces correctly.
::
:: To remove: schtasks /Delete /TN "MarketPriceDashboard_DailyRefresh" /F
:: To verify: schtasks /Query /TN "MarketPriceDashboard_DailyRefresh" /FO LIST /V
:: To run now: schtasks /Run /TN "MarketPriceDashboard_DailyRefresh"

set TASK_NAME=MarketPriceDashboard_DailyRefresh
set XML_PATH=%~dp0task_definition.xml

echo Registering scheduled task "%TASK_NAME%"...

schtasks /Create /TN "%TASK_NAME%" /XML "%XML_PATH%" /F

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Task registered successfully!
    echo It will run every weekday at 3:30 PM.
    echo Runs on battery: YES
    echo Starts when available if missed: YES
    echo.
    echo To verify:  schtasks /Query /TN "%TASK_NAME%" /FO LIST /V
    echo To remove:  schtasks /Delete /TN "%TASK_NAME%" /F
    echo To run now: schtasks /Run /TN "%TASK_NAME%"
) else (
    echo.
    echo ERROR: Failed to register task. Check that task_definition.xml exists next to this file.
)

pause
