\# TradingSystem V7.0 Stable

\## Architecture Document



---



\## 1. Layered Architecture



Batch Layer

\- run\_all.bat

\- run\_daily\_parts.bat

\- run\_report.bat



Execution Layer

\- oneclick\_daily\_run.py

\- strategy\_with\_risk.py



Reporting Layer

\- plot\_equity\_compare.py

\- generate\_report.py

\- generate\_report\_pdf.py



Data Layer

\- data\\inbound

\- data\\daily\_input.csv

\- data\\out\\\_bt\_tmp

\- data\\out\\runs\\<RUN\_ID>



---



\## 2. Dependency Flow



run\_all.bat  

&nbsp; -> run\_daily\_parts.bat fetch  

&nbsp; -> run\_daily\_parts.bat prepare  

&nbsp; -> run\_daily\_parts.bat oneclick  

&nbsp; -> run\_report.bat  



run\_report.bat  

&nbsp; -> strategy\_with\_risk.py  

&nbsp; -> plot\_equity\_compare.py  

&nbsp; -> generate\_report.py  

&nbsp; -> generate\_report\_pdf.py  



---



\## 3. Isolation Rule



Each RUN\_ID must write only inside:



data\\out\\runs\\<RUN\_ID>\\



No shared-state writing allowed.

