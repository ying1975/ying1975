@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem ============================================================
rem TradingSystem V7 - RUN_ALL (Stable)
rem Stages:
rem   fetch -> prepare -> quality -> oneclick -> report
rem ============================================================

set "BASE=C:\TradingSystem"
set "OUT=%BASE%\data\out"
set "RUNS=%OUT%\runs"
set "LOGDIR=%OUT%\logs"

if not exist "%RUNS%" mkdir "%RUNS%" 1>nul 2>nul
if not exist "%LOGDIR%" mkdir "%LOGDIR%" 1>nul 2>nul

for /f %%A in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "RUN_ID=%%A"

set "RUN_DIR=%RUNS%\%RUN_ID%"
set "LOG=%LOGDIR%\run_all_%RUN_ID%.log"

echo ============================================================
echo [RUN_ALL] RUN_ID=%RUN_ID%
echo [RUN_ALL] RUN_DIR=%RUN_DIR%
echo ============================================================

echo ============================================================ >> "%LOG%"
echo [RUN_ALL] RUN_ID=%RUN_ID% >> "%LOG%"
echo [RUN_ALL] RUN_DIR=%RUN_DIR% >> "%LOG%"
echo [RUN_ALL] started=%DATE% %TIME% >> "%LOG%"
echo ============================================================ >> "%LOG%"

set "FETCH_MAX=1"
set "ONECLICK_MAX=1"
set "REPORT_MAX=2"

call :RUN_STAGE fetch "%BASE%\run_daily_parts.bat" fetch %FETCH_MAX%
if errorlevel 1 goto :FAIL

call :RUN_STAGE prepare "%BASE%\run_daily_parts.bat" prepare 1
if errorlevel 1 goto :FAIL

call "%BASE%\app\ops\quality_gate.py"
if errorlevel 1 goto :FAIL

call :RUN_STAGE oneclick "%BASE%\run_daily_parts.bat" oneclick %ONECLICK_MAX%
if errorlevel 1 goto :FAIL

rem Scheme #2: pass RUN_ID + BT_ROOT
call :RUN_STAGE report "%BASE%\run_report.bat" "%RUN_ID%" "%OUT%\_bt_tmp" %REPORT_MAX%
if errorlevel 1 goto :FAIL

echo ============================================================
echo [RUN_ALL] SUCCESS
echo ============================================================
echo [RUN_ALL] SUCCESS >> "%LOG%"
exit /b 0


:FAIL
echo ============================================================
echo [RUN_ALL] FAILED. log=%LOG%
echo ============================================================
echo [RUN_ALL] FAILED >> "%LOG%"
exit /b 1


rem ============================================================
rem RUN_STAGE
rem   %1 = stage name
rem   %2 = script path
rem   %3 = arg1
rem   %4 = arg2 (optional)
rem   %5 = max retry
rem ============================================================
:RUN_STAGE
setlocal EnableDelayedExpansion

set "STAGE=%~1"
set "SCRIPT=%~2"
set "ARG1=%~3"
set "ARG2=%~4"
set "MAX=%~5"

if "%MAX%"=="" set "MAX=1"
set /a TRY=0

:RETRY
set /a TRY+=1

echo [RUN_ALL] STAGE=!STAGE! TRY=!TRY!/!MAX!
echo [RUN_ALL] STAGE=!STAGE! TRY=!TRY!/!MAX! >> "%LOG%"

if not "%ARG2%"=="" (
  call "%SCRIPT%" "%ARG1%" "%ARG2%"
) else (
  call "%SCRIPT%" "%ARG1%"
)

set "RC=!ERRORLEVEL!"

echo [RUN_ALL] STAGE=!STAGE! RC=!RC!
echo [RUN_ALL] STAGE=!STAGE! RC=!RC! >> "%LOG%"

if "!RC!"=="0" (
  endlocal & exit /b 0
)

if !TRY! GEQ !MAX! (
  endlocal & exit /b !RC!
)

echo [RUN_ALL] retry in 15 sec...
echo [RUN_ALL] retry in 15 sec... >> "%LOG%"
timeout /t 15 >nul
goto :RETRY