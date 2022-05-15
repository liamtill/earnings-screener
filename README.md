## Earnings Screener

This code uses the [Yahoo Earnings Calender](https://pypi.org/project/yahoo-earnings-calendar/) to screen stocks for earnings dates and [yfinance](https://pypi.org/project/yfinance/) to fetch stock data. 

**Due to dependencies on 3rd party packages this may break at times if Yahoo modify their website and the scarpers in the dependencies are not updated.**

**This repository is for archive purposes only and will not be updated. You may still find some of the code useful for your own project.**

Install the requirements:

```
pip install -r requirements.txt
```

Simply select the start and end date to screen for, enter some basic filter criteria in `filters.yaml` which currently uses:

* Minimum price: `minprice`
* Maximum Price: `maxprice`
* Minimum market cap: `minmcap`
* Maximum market cap: `maxmcap`
* Minimum average volume: `minavgvol`

You will then be presented with a table of stocks matching the criteria with earnings in the dates you selected. You can sort this table by technical or fundamental (EPS) values. 