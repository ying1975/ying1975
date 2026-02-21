@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem ============================================================
rem TradingSystem V7 - RUN_REPORT (Stable)
rem Usage: run_report.bat RUN_ID [BT_ROOT]
rem Produces:
rem   equity_compare.csv / equity_compare_summary.json
rem   equity_compare.html
rem   reports\report_<RUN_ID>.html / reports\report_<RUN_ID>.pdf
rem ============================================================

set "ROOT=C:\TradingSystem"
set "PY_EXE=C:\Python312\python.exe"
set "PYTHONPATH=%ROOT%"

if "%~1"=="" (
  echo Usage: %~nx0 RUN_ID [BT_ROOT]
  exit /b 2
)

set "RUN_ID=%~1"
set "RUN_DIR=%ROOT%\data\out\runs\%RUN_ID%"

set "BT_ROOT=%ROOT%\data\out\_bt_tmp"
if not "%~2"=="" set "BT_ROOT=%~2"

echo ===============================
echo [V7] RUN_REPORT
echo RUN_ID=%RUN_ID%
echo RUN_DIR=%RUN_DIR%
echo PYTHONPATH=%PYTHONPATH%
echo BT_ROOT=%BT_ROOT%
echo ===============================

if not exist "%RUN_DIR%" (
  echo [RUN_REPORT] FAILED: RUN_DIR not found: "%RUN_DIR%"
  exit /b 3
)

if not exist "%BT_ROOT%" (
  echo [RUN_REPORT] FAILED: BT_ROOT not found: "%BT_ROOT%"
  exit /b 11
)

rem Count valid YYYYMMDD dirs that contain both daily_out.csv and daily_top20.csv
set /a BT_DAYS_NUM=0
for /f "delims=" %%D in ('dir /b /ad "%BT_ROOT%\????????" 2^>nul') do (
  if exist "%BT_ROOT%\%%D\daily_out.csv" if exist "%BT_ROOT%\%%D\daily_top20.csv" (
    set /a BT_DAYS_NUM+=1
  )
)

echo [RUN_REPORT] BT_DAYS_NUM=!BT_DAYS_NUM!
if !BT_DAYS_NUM! GEQ 2 goto :BT_OK

echo [RUN_REPORT] FAILED: Not enough prepared snapshots in "%BT_ROOT%" (need ^>=2, got !BT_DAYS_NUM!)
exit /b 12

:BT_OK
echo [RUN_REPORT] BT_ROOT OK: days=!BT_DAYS_NUM!

call :__STEP EQUITY_COMPARE ^
  "%PY_EXE%" "%ROOT%\app\backtest\strategy_with_risk.py" ^
  --run_id "%RUN_ID%" ^
  --out_dir "%RUN_DIR%" ^
  --bt_root "%BT_ROOT%" ^
  --strategy trade_value ^
  --topN 5 ^
  --risk_on 0.55 ^
  --risk_mid 0.48
if errorlevel 1 goto :FAILED

call :__STEP PLOT_EQUITY ^
  "%PY_EXE%" "%ROOT%\app\backtest\plot_equity_compare.py" ^
  --run_id "%RUN_ID%" ^
  --out_dir "%RUN_DIR%"
if errorlevel 1 goto :FAILED

call :__STEP GEN_REPORT_HTML ^
  "%PY_EXE%" "%ROOT%\app\backtest\generate_report.py" ^
  --run_id "%RUN_ID%" ^
  --out_dir "%RUN_DIR%"
if errorlevel 1 goto :FAILED

call :__STEP GEN_REPORT_PDF ^
  "%PY_EXE%" "%ROOT%\app\backtest\generate_report_pdf.py" ^
  --run_id "%RUN_ID%" ^
  --out_dir "%RUN_DIR%"
if errorlevel 1 goto :FAILED

echo ===============================
echo [RUN_REPORT] OK
echo ===============================
exit /b 0


:__STEP
setlocal EnableExtensions EnableDelayedExpansion

set "STEP_NAME=%~1"
set "ALL=%*"
set "CMDLINE=!ALL:*%STEP_NAME% =!"

echo ===============================
echo [V7] STEP: !STEP_NAME!
echo ===============================
echo [CMD] !CMDLINE!

call !CMDLINE!
set "RC=!ERRORLEVEL!"
if not "!RC!"=="0" echo [RC]=!RC!

endlocal & exit /b %RC%


:FAILED
echo ===============================
echo [RUN_REPORT] FAILED rc=%ERRORLEVEL%
echo ===============================
exit /b %ERRORLEVEL%