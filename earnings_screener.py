import datetime as dt
from yahoo_earnings_calendar import YahooEarningsCalendar
import yfinance as yf
import multiprocessing as mp
from itertools import repeat
from datetime import date
import dash
from dash import html
from dash import dcc
from dash import dash_table
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import pandas as pd
import os
import numpy as np
import yaml
import sys


def screen_earnings(d, start_date, end_date, tickers_first_filter):

    # first filter params #
    minprice = float(filters['minprice'])
    maxprice = float(filters['maxprice'])
    minmcap = float(filters['minmcap'])
    maxmcap = float(filters['maxmcap'])
    minavgvol = float(filters['minavgvol'])
    ## ##

    ## yf info dict keys ##
    # dict_keys(['zip', 'sector', 'fullTimeEmployees', 'longBusinessSummary', 'city', 'phone', 'state', 'country',
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

    earn = d[1]
    ticker = earn['ticker']
    print("Checking: ", ticker)
    try:
        share = yf.Ticker(ticker)
        info = share.info
        open = info['open']
        close = info['previousClose']
        mcap = info['marketCap']
        ma200 = info['twoHundredDayAverage']
        ma50 = info['fiftyDayAverage']
        avgvol = info['averageVolume']
        vol = info['volume']
        if (close >= minprice) and (close <= maxprice) and (mcap >= minmcap) and (mcap <= maxmcap) and (close >= ma200) \
                and (close >= ma50) and (avgvol >= minavgvol):
            # tickers_first_filter.append(ticker)
            print(ticker, 'MEETS CRITERIA')
            earn_date = dt.datetime.strptime(earn['startdatetime'], '%Y-%m-%dT%H:%M:%S.%fZ')
            date_str = earn_date.strftime('%Y-%m-%d')
            prev_close = info['previousClose']
            fiftytwo_whi = check_round(info['fiftyTwoWeekHigh'])
            fiftytwo_wlo = check_round(info['fiftyTwoWeekLow'])
            #print(prev_close, open, ma50, ma200)
            day_pct_change = ((prev_close - open) / open) * 100.
            pct_of_50dma = ((close - ma50) / ma50) * 100.
            pct_of_200dma = ((close - ma200) / ma200) * 100.
            pct_of_52whi = ((close - fiftytwo_whi) / fiftytwo_whi) * 100.
            pct_of_52wlo = ((close - fiftytwo_wlo) / fiftytwo_whi) * 100.
            #print(day_pct_change, pct_of_50dma, pct_of_200dma)
            tickers_first_filter[d[0]] = {'Date': date_str,
                                            'Ticker': ticker,
                                            'Name': earn['companyshortname'],
                                            'Sector': info['sector'],
                                            'MCAP [M]': check_round(mcap) / 1e6,
                                            'Open [$]': open,
                                            'Close [$]': close,
                                            '% Change': check_round(day_pct_change),
                                            'Vol. [M]': check_round(info['volume']) / 1e6,
                                            'Avg. Vol. [M]': check_round(info['averageVolume']) / 1e6,
                                            'Avg. 10D Vol. [M]': check_round(info['averageDailyVolume10Day']) / 1e6,
                                            'EPS est.': check_round(earn['epsestimate']),
                                            'Trailing EPS': check_round(info['trailingEps']),
                                            'Forward EPS': check_round(info['forwardEps']),
                                            'EPS QoQ': check_round(info['earningsQuarterlyGrowth']),
                                            'Rev. Q Growth': check_round(info['revenueQuarterlyGrowth']),
                                            'Profit Marg.': check_round(info['profitMargins']),
                                            '% Inst. Holdings': check_round(info['heldPercentInstitutions']),
                                            '% of 200DMA': check_round(pct_of_200dma),
                                            '% of 50DMA': check_round(pct_of_50dma),
                                            #'Forward PE': check_round(info['forwardPE']),
                                            'Float [M]': check_round(info['floatShares']) / 1e6,
                                            '% Short Float': check_round(info['shortPercentOfFloat']),
                                            '% off 52W High': check_round(pct_of_52whi),
                                            '% of 52W Low': check_round(pct_of_52wlo)
                                          }
    except Exception as e:
        print('error checking ticker: ', ticker, e)
        pass


def check_round(val):

    if val is None:
        return ''

    return np.round(val, 2)


def get_earnings_calendar(start_date, end_date):

    # {'ticker': 'WMG', 'companyshortname': 'Warner Music Group Corp', 'startdatetime': '2020-11-25T13:30:00.000Z',
    # 'startdatetimetype': 'BMO', 'epsestimate': 0.1, 'epsactual': None, 'epssurprisepct': None, 'timeZoneShortName': 'EST',
    # 'gmtOffsetMilliSeconds': -18000000, 'quoteType': 'EQUITY'},

    date_from = dt.datetime.strptime(start_date, '%Y-%m-%d')
    date_to = dt.datetime.strptime(end_date, '%Y-%m-%d')
    yec = YahooEarningsCalendar()
    try:
        earnings = yec.earnings_between(date_from, date_to)
    except Exception as e:
        return 'error fetching earnings: ' + str(e)
    return earnings


def run_screener(start_date, end_date):
    print('Running earnings screener...')
    print(start_date, end_date)
    filename = start_date.replace('-','')+'_'+end_date.replace('-','')+'.csv' #start_date.strftime('%Y%m%d')+'_'+end_date.strftime('%Y%m%d')+'.csv'

    if os.path.exists('data/'+filename):
        print(filename, ' exists')
        data = pd.read_csv('data/'+filename)
        return data, 'loaded: ' + str(filename)

    earnings = get_earnings_calendar(start_date, end_date)
    if isinstance(earnings, str):
        return None, earnings

    num_earnings = len(earnings)
    print('Number of earnings: ', num_earnings)

    manager = mp.Manager()
    tickers_first_filter = manager.dict()

    # set up processing pool
    mp.freeze_support()
    pool = mp.Pool(processes=20)

    try:
        res = pool.starmap(screen_earnings, zip(enumerate(earnings), repeat(start_date), repeat(end_date),
                                                repeat(tickers_first_filter)))
    except Exception as e:
        print("Error starting multiprocess: " + str(e))

    pool.close()
    pool.join()

    num_matched = len(tickers_first_filter.keys())
    print('Number of earnings filtered: ', num_matched)

    msg = str(num_matched) + ' screened from ' + str(num_earnings) + ' earnings'

    data_df = pd.DataFrame.from_dict(tickers_first_filter, orient='index')
    data_df.to_csv('data/'+filename)

    return data_df, msg


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

# load filter config
with open('filters.yaml', 'r') as file:
    filters = yaml.safe_load(file)

current_date = dt.datetime.now()

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div([

    html.H1(children='Earnings Screener'),

    dcc.DatePickerRange(
        id='my-date-picker-range',
        min_date_allowed=date(current_date.year, current_date.month, current_date.day),
        max_date_allowed=date(2050, 12, 31),
        display_format='DD-MM-YYYY'
    ),
    html.Div(id='output-container-date-picker-range'),

    html.Div(id='output'),

    dcc.Loading(id="loading-1",
                type="default",
                children=html.Div(id="loading-output-1")
            ),

    html.Div(id='earn-table')

])

#    [Output('output-container-date-picker-range', 'children'),
@app.callback(
    Output('earn-table', 'children'),
    Output('loading-output-1', 'children'),
    Output('output', 'children'),
    [Input('my-date-picker-range', 'start_date'),
     Input('my-date-picker-range', 'end_date')])
def update_output(start_date, end_date):
    selected_dates = 'SELECT DATES'
    prev_start, prev_end = None, None
    if (start_date is not None) and (end_date is not None):
        selected_dates = 'Start Date: ' + start_date + ' | ' + 'End Date: ' + end_date

    if len(selected_dates) == len('SELECT DATES'):
        raise PreventUpdate
    if (prev_start == start_date) or (prev_end == end_date):
        raise PreventUpdate
    else:
        data, msg = run_screener(start_date, end_date)
        if data is None:
            return None, '', msg

        table = dash_table.DataTable(
            id='table',
            sort_action="native",
            sort_mode="multi",
            columns=[{"name": i, "id": i} for i in data.columns],
            data=data.to_dict('records')
        )

        prev_start, prev_end = start_date, end_date

        return table, '', msg


if __name__ == '__main__':
    app.run_server(debug=True, port='8050')