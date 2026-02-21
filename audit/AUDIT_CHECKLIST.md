\# TradingSystem V7.0 Stable

\## Internal Audit Checklist



---



\## 1. Environment Verification



\- \[ ] Python version is 3.12

\- \[ ] reportlab installed

\- \[ ] pandas installed

\- \[ ] numpy installed

\- \[ ] All .bat files saved as ANSI or UTF-8 without BOM



---



\## 2. Isolation Verification



\- \[ ] RUN\_ID auto-generated

\- \[ ] No output written outside data\\out\\runs\\<RUN\_ID>\\

\- \[ ] No dependency on "latest" folder

\- \[ ] \_bt\_tmp used only as historical snapshot source



---



\## 3. Data Integrity



\- \[ ] daily\_out.csv generated

\- \[ ] daily\_top20.csv generated

\- \[ ] equity\_compare.csv generated

\- \[ ] equity\_compare\_summary.json generated



---



\## 4. Report Verification



HTML:

\- \[ ] 10 columns visible

\- \[ ] Name auto-wrap works

\- \[ ] All metrics show 5 decimal precision



PDF:

\- \[ ] Chinese characters render correctly

\- \[ ] No truncated name

\- \[ ] Page break logic correct

\- \[ ] All numeric fields 5 decimals



---



\## 5. Financial Metric Verification



\- \[ ] Sharpe Ratio formula correct

\- \[ ] Maximum Drawdown calculation correct

\- \[ ] Equity curve monotonic formula correct



---



\## 6. Reproducibility Test



\- \[ ] Same RUN\_ID produces identical output

\- \[ ] No timestamp leakage inside report content



---



Audit Result:



APPROVED / REJECTED



Auditor:

Date:

