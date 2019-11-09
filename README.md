# barnacle
obligate commensalism for personal finance

```
python -m luigi \
  --module barnacle.jobs.pipeline DownloadFilings
  --company-name "AQR CAPITAL MANAGEMENT LLC"
  --filing-type "13F-*"
  --local-scheduler
```
