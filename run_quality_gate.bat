@echo off
setlocal

set "PY=C:\Python312\python.exe"
set "BASE=C:\TradingSystem"

REM mode: fail | degrade
REM thresholds: close<=1% , trade_value<=0.5%
REM min_rows: at least 800 rows remain after degradation
%PY% "%BASE%\app\ops\quality_gate.py" --mode fail --max_bad_close_pct 0.01 --max_bad_trade_value_pct 0.005 --min_rows 800
exit /b %ERRORLEVEL%
