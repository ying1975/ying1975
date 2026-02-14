@echo off
setlocal

set BASE=C:\TradingSystem
set PY=C:\Python312\python.exe

echo ===============================
echo [V7] FETCH TWSE
%PY% %BASE%\app\pipeline\fetch_twse.py
if errorlevel 1 exit /b %errorlevel%

echo ===============================
echo [V7] FETCH TPEX
%PY% %BASE%\app\pipeline\fetch_tpex.py
if errorlevel 1 exit /b %errorlevel%

echo ===============================
echo [V7] PREPARE INPUT
%PY% %BASE%\app\pipeline\prepare_input.py
if errorlevel 1 exit /b %errorlevel%

echo ===============================
echo [V7] RUN ONECLICK
%PY% %BASE%\oneclick_daily_run.py --mode pre --input %BASE%\data\daily_input.csv --output %BASE%\data\out\daily_out.csv --top20 %BASE%\data\out\daily_top20.csv
if errorlevel 1 exit /b %errorlevel%

echo ===============================
echo [V7] RETENTION CLEANUP
%PY% %BASE%\app\ops\cleanup_retention.py

echo ===============================
echo [V7] HEALTH GATE
REM 建議門檻：7天 fallback >40% 才告警；FAILED 48h 內直接 fail
%PY% %BASE%\app\ops\health_gate.py --window_days 7 --fb_ratio 0.40 --min_samples 5 --recent_hours 48
exit /b %errorlevel%
