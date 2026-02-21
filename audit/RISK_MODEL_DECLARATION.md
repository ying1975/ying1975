\# Risk Model Declaration



TradingSystem V7.0 uses:



\- Equal-weight portfolio

\- Exposure model: configurable (none / tier / continuous)

\- Risk-free rate assumed 0

\- 252 trading days annualization



Sharpe Ratio:

mean(daily\_return) / std(daily\_return) \* sqrt(252)



Maximum Drawdown:

max((peak - trough) / peak)



This model is deterministic and does not use random components.

