import datetime as dt
from yahoo_earnings_calendar import YahooEarningsCalendar
import yfinance as yf
import multiprocessing as mp
from itertools import repeat

def screen_earnings(d, tickers_first_filter):

    # first filter params #
    minprice = 1.
    maxprice = 100.
    minmcap = 300e6
    minavgvol = 300000
    minvol = 300000
    ## ##

    ## loop over list
    #for d in earnings:
    data = d[1]
    ticker = data['ticker']
    print("Checking: ", ticker)
    try:
        share = yf.Ticker(ticker)
        info = share.info
        # print(ticker)
        # print(share.info)
        close = info['previousClose']
        mcap = info['marketCap']
        ma200 = info['twoHundredDayAverage']
        ma50 = info['fiftyDayAverage']
        avgvol = info['averageVolume']
        vol = info['volume']
        if (close >= minprice) and (close <= maxprice) and (mcap >= minmcap) and (close >= ma200) and (
                close >= ma50) \
                and (avgvol >= minavgvol) and (vol >= minvol):
            # tickers_first_filter.append(ticker)
            earn_date = dt.datetime.strptime(data['startdatetime'], '%Y-%m-%dT%H:%M:%S.%fZ')
            date_str = earn_date.strftime('%Y-%m-%d')
            tickers_first_filter[ticker] = {'name': data['companyshortname'], 'date': date_str, \
                                            'epsest': data['epsestimate']}
    except Exception as e:
        pass


if __name__ == '__main__':

    date_from = dt.datetime.strptime('2020-11-23', '%Y-%m-%d')
    date_to = dt.datetime.strptime('2020-11-27', '%Y-%m-%d')
    yec = YahooEarningsCalendar()
    #print(yec.earnings_on(date_from))
    #print(yec.earnings_between(date_from, date_to))

    #{'ticker': 'WMG', 'companyshortname': 'Warner Music Group Corp', 'startdatetime': '2020-11-25T13:30:00.000Z',
    # 'startdatetimetype': 'BMO', 'epsestimate': 0.1, 'epsactual': None, 'epssurprisepct': None, 'timeZoneShortName': 'EST',
    # 'gmtOffsetMilliSeconds': -18000000, 'quoteType': 'EQUITY'},

    earnings = yec.earnings_between(date_from, date_to)
    #earnings = yec.earnings_on(date_from)
    print('Number of earnings: ', len(earnings))


    ## yf info dict keys ##
    #dict_keys(['zip', 'sector', 'fullTimeEmployees', 'longBusinessSummary', 'city', 'phone', 'state', 'country',
    #           'companyOfficers', 'website', 'maxAge', 'address1', 'industry', 'previousClose', 'regularMarketOpen',
    #           'twoHundredDayAverage', 'trailingAnnualDividendYield', 'payoutRatio', 'volume24Hr', 'regularMarketDayHigh',
    #           'navPrice', 'averageDailyVolume10Day', 'totalAssets', 'regularMarketPreviousClose', 'fiftyDayAverage',
    #           'trailingAnnualDividendRate', 'open', 'toCurrency', 'averageVolume10days', 'expireDate', 'yield', 'algorithm',
    #           'dividendRate', 'exDividendDate', 'beta', 'circulatingSupply', 'startDate', 'regularMarketDayLow', 'priceHint',
    #           'currency', 'regularMarketVolume', 'lastMarket', 'maxSupply', 'openInterest', 'marketCap',
    #           'volumeAllCurrencies', 'strikePrice', 'averageVolume', 'priceToSalesTrailing12Months', 'dayLow', 'ask',
    #           'ytdReturn', 'askSize', 'volume', 'fiftyTwoWeekHigh', 'forwardPE', 'fromCurrency', 'fiveYearAvgDividendYield',
    #           'fiftyTwoWeekLow', 'bid', 'tradeable', 'dividendYield', 'bidSize', 'dayHigh', 'exchange', 'shortName',
    #           'longName', 'exchangeTimezoneName', 'exchangeTimezoneShortName', 'isEsgPopulated', 'gmtOffSetMilliseconds',
    #           'quoteType', 'symbol', 'messageBoardId', 'market', 'annualHoldingsTurnover', 'enterpriseToRevenue',
    #           'beta3Year', 'profitMargins', 'enterpriseToEbitda', '52WeekChange', 'morningStarRiskRating', 'forwardEps',
    #           'revenueQuarterlyGrowth', 'sharesOutstanding', 'fundInceptionDate', 'annualReportExpenseRatio', 'bookValue',
    #           'sharesShort', 'sharesPercentSharesOut', 'fundFamily', 'lastFiscalYearEnd', 'heldPercentInstitutions',
    #           'netIncomeToCommon', 'trailingEps', 'lastDividendValue', 'SandP52WeekChange', 'priceToBook',
    #           'heldPercentInsiders', 'nextFiscalYearEnd', 'mostRecentQuarter', 'shortRatio', 'sharesShortPreviousMonthDate',
    #           'floatShares', 'enterpriseValue', 'threeYearAverageReturn', 'lastSplitDate', 'lastSplitFactor', 'legalType',
    #           'lastDividendDate', 'morningStarOverallRating', 'earningsQuarterlyGrowth', 'dateShortInterest', 'pegRatio',
    #           'lastCapGain', 'shortPercentOfFloat', 'sharesShortPriorMonth', 'category', 'fiveYearAverageReturn',
    #           'regularMarketPrice', 'logo_url'])
    ## ##

    #print(yf.Ticker('WMG').info)

    manager = mp.Manager()
    tickers_first_filter = manager.dict()

    # dict to store tickers from initial filtering
    #tickers_first_filter = {}

    # set up processing pool
    mp.freeze_support()
    pool = mp.Pool(processes=10)

    try:
        res = pool.starmap(screen_earnings, zip(enumerate(earnings), repeat(tickers_first_filter)))
    except Exception as e:
        print("Error starting multiprocess: " + str(e))

    pool.close()
    pool.join()

    print('Number of earnings filtered: ', len(tickers_first_filter.keys()))
    print(tickers_first_filter)