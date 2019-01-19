import os, sys
import json
import numpy as np
import pandas as pd
from .. import core as bundles
from tiingo import TiingoClient
from trading_calendars import get_calendar

@bundles.register('tiingo')
def quandl_bundle(environ,
                  asset_db_writer,
                  minute_bar_writer,
                  daily_bar_writer,
                  adjustment_writer,
                  calendar,
                  start_session,
                  end_session,
                  cache,
                  show_progress,
                  output_dir):

    ## load tickers from json 
    dir__ = os.path.dirname(os.path.abspath(__file__))
    path_ = os.path.join(dir__,"ticker.json")
    with open(path_) as f:
        tickers = json.load(f)

    ## build calendar
    start_session = pd.Timestamp("1990-01-01")
    end_session = pd.Timestamp.today()
    calendar = get_calendar("NYSE")

    ## build tiigo clinet with api_key
    api_key = "16deb934fe26b4780df9becffdada8302e0b2f5f"
    config = {}
    config['session'] = True
    config['api_key'] = api_key
    client = TiingoClient(config)

    ## process ticker
    bar_data = []
    asset_df = None
    dfSplit = None
    dfDiv = None
    sid = 0
    for ticker in tickers:
        print ("Processing sid: " + str(sid) + " Symbol: " + ticker)
        df = client.get_dataframe(ticker,
                            frequency='daily',
                            startDate=start_session.strftime("%Y-%m-%d"),
                            endDate=end_session.strftime("%Y-%m-%d"))
        df.index = df.index.tz_localize('UTC')
        dfOHL = df[["open","high","low","close","volume"]].copy()
        # new_index = calendar.sessions_in_range(df.index[0],df.index[-1])
        # dfOHL_new = dfOHL.reindex(new_index)
        bar_data.append((sid,dfOHL))
        if asset_df is None:
            asset_df = pd.DataFrame([[ticker,dfOHL.index[0],dfOHL.index[-1]]],columns=['symbol','start_date','end_date'])
        else:
            asset_df.loc[sid] = [ticker,dfOHL.index[0],dfOHL.index[-1]]

        ## process split:
        dfSplit = df[df['splitFactor']!=1].copy()
        dfSplit = dfSplit.reset_index()
        dfSplit['sid'] = sid
        dfSplit['ratio'] = 1.0 / dfSplit['splitFactor']
        dfSplit['effective_date'] = dfSplit['date']
        dfSplit = dfSplit[["sid","ratio","effective_date"]].copy()

        ## process dividends
        dfDiv = df[df['divCash']!=0.0].copy()
        dfDiv = dfDiv.reset_index()
        dfDiv['sid'] = sid
        dfDiv['amount'] = dfDiv['divCash']*1.0
        dfDiv['ex_date'] = dfDiv['date']
        dfDiv['record_date'] = dfDiv['date']
        dfDiv['declared_date'] = dfDiv['date']
        dfDiv['pay_date'] = dfDiv['date']
        dfDiv = dfDiv[['sid', 'amount', 'ex_date', 'record_date', 'declared_date', 'pay_date']].copy()

        sid += 1
    
    ## Write bar_data and asset_df to DB
    asset_df['exchange'] = "NYSE"
    daily_bar_writer.write(bar_data,show_progress = True)
    asset_db_writer.write(equities=asset_df)
    adjustment_writer.write(splits=dfSplit, dividends=dfDiv)