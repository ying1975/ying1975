@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem ============================================================
rem TradingSystem V7 - Daily parts (run_id isolated)
rem Requires env vars:
rem   RUN_ID, RUN_DIR
rem Writes only into RUN_DIR (no "latest" coupling)
rem Outputs (expected):
rem   %RUN_DIR%\inbound\...
rem   %RUN_DIR%\prepared\daily_input.csv
rem   %RUN_DIR%\daily_out.csv
rem   %RUN_DIR%\daily_top20.csv
rem ============================================================

set "BASE=C:\TradingSystem"
set "PY=C:\Python312\python.exe"

rem Make "app" importable for python scripts if needed
set "PYTHONPATH=%BASE%"

rem Validate required env vars
if "%RUN_ID%"=="" (
  echo [PARTS] ERROR: RUN_ID is empty
  exit /b 2
)

if "%RUN_DIR%"=="" (
  echo [PARTS] ERROR: RUN_DIR is empty
  exit /b 2
)

rem Ensure run directories exist
if not exist "%RUN_DIR%" mkdir "%RUN_DIR%" 1>nul 2>nul
if not exist "%RUN_DIR%\inbound" mkdir "%RUN_DIR%\inbound" 1>nul 2>nul
if not exist "%RUN_DIR%\prepared" mkdir "%RUN_DIR%\prepared" 1>nul 2>nul
if not exist "%RUN_DIR%\reports" mkdir "%RUN_DIR%\reports" 1>nul 2>nul

set "MODE=%~1"
if "%MODE%"=="" (
  echo Usage: run_daily_parts.bat fetch^|prepare^|oneclick
  exit /b 2
)

echo ===============================
echo [V7] PARTS  RUN_ID=%RUN_ID%
echo [V7] PARTS  RUN_DIR=%RUN_DIR%
echo ===============================

if /I "%MODE%"=="fetch" goto :FETCH
if /I "%MODE%"=="prepare" goto :PREPARE
if /I "%MODE%"=="oneclick" goto :ONECLICK

echo [PARTS] ERROR: unknown mode=%MODE%
exit /b 2


:FETCH
echo ===============================
echo [V7] FETCH (RUN_ID=%RUN_ID%)
echo ===============================

set "LOG=%RUN_DIR%\fetch.log"
> "%LOG%" echo [FETCH] start

rem Fetch: copy newest raw files into %RUN_DIR%\inbound (source: %BASE%\data\inbound)

if not exist "%BASE%\data\inbound" (
  echo [FETCH] missing %BASE%\data\inbound >> "%LOG%"
  echo [FETCH] missing inbound source dir
  exit /b 2
)

xcopy /E /I /Y "%BASE%\data\inbound" "%RUN_DIR%\inbound" >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FETCH] xcopy failed >> "%LOG%"
  echo [FETCH] xcopy failed. see "%LOG%"
  exit /b 2
)

echo [FETCH] ok. log="%LOG%"
exit /b 0


:PREPARE
echo ===============================
echo [V7] PREPARE (RUN_ID=%RUN_ID%)
echo ===============================

set "LOG=%RUN_DIR%\prepare.log"
> "%LOG%" echo [PREPARE] start

rem Prepare: copy daily_input.csv into run-scoped prepared folder

if not exist "%BASE%\data\daily_input.csv" (
  echo [PREPARE] missing source daily_input.csv >> "%LOG%"
  echo [PREPARE] missing source daily_input.csv
  exit /b 2
)

copy /Y "%BASE%\data\daily_input.csv" "%RUN_DIR%\prepared\daily_input.csv" >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [PREPARE] copy failed >> "%LOG%"
  echo [PREPARE] copy failed. see "%LOG%"
  exit /b 1
)

echo [PREPARE] ok. log="%LOG%"
exit /b 0


:ONECLICK
echo ===============================
echo [V7] ONECLICK (RUN_ID=%RUN_ID%)
echo ===============================

set "LOG=%RUN_DIR%\oneclick.log"
> "%LOG%" echo [ONECLICK] start

rem Oneclick: run pre mode to generate daily_out.csv and daily_top20.csv

if not exist "%RUN_DIR%\prepared\daily_input.csv" (
  echo [ONECLICK] missing prepared daily_input.csv >> "%LOG%"
  echo [ONECLICK] missing prepared daily_input.csv
  exit /b 2
)

"%PY%" "%BASE%\oneclick_daily_run.py" --mode pre ^
  --input "%RUN_DIR%\prepared\daily_input.csv" ^
  --output "%RUN_DIR%\daily_out.csv" ^
  --top20 "%RUN_DIR%\daily_top20.csv" >> "%LOG%" 2>&1

set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  echo [ONECLICK] oneclick_daily_run failed rc=%RC% >> "%LOG%"
  echo [ONECLICK] oneclick_daily_run failed rc=%RC%
  exit /b %RC%
)

if not exist "%RUN_DIR%\daily_out.csv" (
  echo [ONECLICK] missing daily_out.csv >> "%LOG%"
  echo [ONECLICK] missing daily_out.csv
  exit /b 2
)

if not exist "%RUN_DIR%\daily_top20.csv" (
  echo [ONECLICK] missing daily_top20.csv >> "%LOG%"
  echo [ONECLICK] missing daily_top20.csv
  exit /b 2
)

echo [ONECLICK] ok. log="%LOG%"
exit /b 0