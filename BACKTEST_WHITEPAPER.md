\# TradingSystem V7.0 Stable

\## Backtest Whitepaper



---



\## 1. Daily Return



Return\_t = (P\_t+1 / P\_t) - 1



Portfolio return:



R\_t = average(selected stock returns)



---



\## 2. Equity Curve



Equity\_t = Equity\_t-1 × (1 + R\_t)



Initial equity = 1.0



---



\## 3. Sharpe Ratio



Sharpe = mean(daily\_return) / std(daily\_return) × sqrt(252)



Risk-free rate assumed 0.



---



\## 4. Maximum Drawdown



MDD = max((peak - trough) / peak)



---



\## 5. Breadth Ratio



breadth = positive\_light\_count / total\_count

