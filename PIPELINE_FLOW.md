\# TradingSystem V7.0 Stable

\## Pipeline Flow Specification



---



\## Execution State Machine



START  

&nbsp; -> FETCH  

&nbsp; -> PREPARE  

&nbsp; -> QUALITY  

&nbsp; -> ONECLICK  

&nbsp; -> REPORT  

&nbsp; -> END  



---



\## Stage Rules



FETCH  

\- Requires inbound folder  



PREPARE  

\- Requires daily\_input.csv  



QUALITY  

\- Validates data integrity  



ONECLICK  

\- Produces daily\_out.csv  

\- Produces daily\_top20.csv  



REPORT  

\- Requires >= 2 historical days  

\- Generates equity, HTML, PDF  



---



\## Failure Handling



If RC != 0  

Pipeline stops  

RUN\_STATUS updated

