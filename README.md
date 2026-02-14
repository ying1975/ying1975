\# TradingSystem v7 – Production Architecture



Production-grade automated Taiwan market analysis system.



This system provides a fully automated daily pipeline for:



\- Fetching TWSE \& TPEX data

\- Normalizing and filtering stock universe

\- Running strategy core

\- Producing daily outputs

\- Logging execution status

\- Performing health checks

\- Automatically cleaning old data



---



\# 📁 Project Structure



```

C:\\TradingSystem

│

├── app/

│   ├── core/          # Strategy logic (actual core engine)

│   ├── pipeline/      # Data fetch + normalization layer

│   ├── ops/           # Health check + retention + batch scripts

│   └── config/        # config.prod.json

│

├── data/

│   ├── inbound/       # Raw TWSE/TPEX snapshots (runtime only)

│   ├── prepared/      # Normalized snapshots (runtime only)

│   └── out/           # daily\_out / daily\_top20 outputs

│

├── logs/              # Execution logs (runtime only)

│

├── oneclick\_daily\_run.py      # Main orchestrator

├── daily\_auto\_run\_final.py    # Wrapper for core

├── strategy\_score.py          # Wrapper for core

├── BUILD\_INFO.json            # Version metadata

├── .gitignore

└── README.md

```



---



\# 🚀 Daily Automated Flow



1\. Fetch TWSE data

2\. Fetch TPEX data

3\. Normalize schema

4\. Filter to common stocks only

5\. Dynamic sanity validation

6\. Run strategy core

7\. Generate:

&nbsp;  - `data/out/daily\_out.csv`

&nbsp;  - `data/out/daily\_top20.csv`

&nbsp;  - `run\_summary.csv`

&nbsp;  - `RUN\_STATUS.txt`

8\. Cleanup old logs and snapshots

9\. Run health gate validation



---



\# ▶ Manual Execution



Navigate to:



```

C:\\TradingSystem\\app\\ops

```



Run:



```

run\_daily.bat

```



---



\# ⏰ Windows Task Scheduler Setup



\*\*Program/script\*\*

```

C:\\Windows\\System32\\cmd.exe

```



\*\*Arguments\*\*

```

/c C:\\TradingSystem\\app\\ops\\run\_daily.bat

```



\*\*Start in\*\*

```

C:\\TradingSystem\\app\\ops

```



---



\# 🛡 Health Gate Logic



The system will raise an alert if:



\- A FAILED run is detected within recent window

\- Fallback ratio exceeds configured threshold



Alert file:

```

HEALTH\_ALERT.txt

```



Exit code will reflect failure state for scheduler monitoring.



---



\# ⚙ Configuration



Main config file:



```

app/config/config.prod.json

```



You may adjust:



\- Sanity thresholds

\- Retention days

\- Health gate fallback ratio

\- Python path

\- Output locations



---



\# 🏷 Version Control



Version metadata stored in:



```

BUILD\_INFO.json

```



Each execution writes version info into:



```

run\_summary.csv

```



Create release tag:



```

git tag v7.0.0

git push origin v7.0.0

```



---



\# 🧹 What Is NOT Tracked by Git



The following are ignored:



\- data/inbound/

\- data/prepared/

\- data/out/

\- logs/

\- runtime CSV outputs

\- RUN\_STATUS files

\- HEALTH\_ALERT.txt



Git tracks only:



\- Source code

\- Structure

\- Configuration

\- Version metadata



---



\# 📌 Production Design Principles



\- Clear separation of pipeline / core / ops

\- Git is for code, not data

\- Runtime artifacts are excluded

\- Sanity validation matches stock universe definition

\- Health gate protects operational stability

\- Retention prevents disk growth



---



\# 🔄 Upgrade Path



Future possible enhancements:



\- Anomaly detection layer

\- Historical backtest module

\- Market breadth indicators

\- Volatility regime classifier

\- Capital allocation module

\- Performance analytics dashboard



---



System status:



\*\*Production-ready automated Taiwan market pipeline (v7)\*\*



