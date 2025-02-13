#!/usr/bin/python3
#
# get-stock-data.py
#
# connect to stock data API and obtain tickers available
# then download available data, augment with computed
# indicators (moving averages, rsi, etc), write to 
# mysql database for other parts of the project to mine
#
# currently uses only polygon.io .. may enhance later to
# add redundant sources to improve coverage.  need paid
# service to get (near) realtime data for hourly or faster
# updates. current version only needs close data daily.
#
# syntax - specify:
# 1. scope (all, database, symbol), and
# 2. mode (incremental, full)
#
#  get all available tickers and all available
#  data - used to do initial population of database
#  expected run time:  2.5 hours
#   ./get-stock-data.py --full --all
#
#  update all available tickers
#   ./get-stock-data.py --incremental --all
#
#  get data for a single ticker
#   ./get-stock-data.py --symbol AAPL --full
#   ./get-stock-data.py --symbol AAPL --incremental
#
#  output status/debugging messages
#   ./get-stock-data.py --symbol AAPL --incremental --debuglevel 5
#  0=minimal (default), 5=maximum (huge amounts of output)
#   
#
# v2.5 2025/02/12
# - added calculation corrections to expand data available
#   for rolling window calculations
# - refactored most functions to accept and return pandas
#   dataframes, including db query and updates
# - added function frame to log_messages > debuglevel 1
# v1.8 2025/02/09
# - updated incremental calculation approach to populate
#   the data structure with all values by database query,
#   add the incremental rows of data downloaded, then
#   update all the downloaded rows by using all data to
#   calculate the moving averages and others based on
#   the entire data set to update the incremental rows with
#   the averages then commit to insert_data
# v1.3 2025/02/06
# - added macd, bollinger band technical indicators
# - pulls historical data from db to properly calculate
#   long term moving averages
#
# (c) Allen Pomeroy, 2025, MIT License
#
# TODO and Caveats:
# - to extend this script to update hourly vs daily,
#   timestamp handling need to be tightened up to 
#   respect and use correct hour of day values
# - clean up url and parameter handling to enable
#   multiple api sources
#
# Overall process:
# 1 build list of tickers to operate on
#    --all           all tickers from source
#    --symbol        limit to single ticker
#    --database      build ticker list from database
# 2 cycle through ticker list and get ticker info
#   to write to database
#    --full          all data available
#    --incremental   build ticker list from database
# 3 calculate indicators
# 4 write changes to database (full == all, incremental ==
#   new rows)
#

import argparse
import MySQLdb
import requests
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
import logging
from decimal import Decimal
import time
import inspect
import json

# constants
version = "2.5"
dbhost = "localhost"
dbuser = "aitrade"
dbpass = "aitrade1"
dbname = "aitrade"
dbtable = "stock_data"
global debuglevel
tickers_updated = 0

polygon_api_key     = "UPDATE_WITH_YOUR_POLYGON_KEY"
polygon_base_url    = "https://api.polygon.io"
polygon_api_tickers = "/v3/reference/tickers"
polygon_api_data    = "/v2/aggs/ticker"

#polygon_ticker_params = {"market": "stocks", "active": "true", "apiKey": polygon_api_key, "limit": 1000}
polygon_ticker_params = {"apiKey": polygon_api_key, "market": "stocks", "active": "true", "limit": "1000"}
polygon_data_params = {"apiKey": polygon_api_key, "adjusted": "true", "sort": "asc", "limit": 50000}

# define indicator periods
indicators = {
    'rsi': {'window': 14},
    'ma50': {'window': 50},
    'ma200': {'window': 200},
    'macd': {'window': 12},
    'macd_signal': {'window': 26},
    'bb_upper': {'window': 20},
    'bb_middle': {'window': 20},
    'bb_lower': {'window': 20},
    'adx': {'window': 14}
}


def log_message(level, message, function_frame=None):
    if debuglevel >= level:
        #print(severity + str(level) + ": " + message)
        #print(message, flush=True)
        if level <= 1:
            print(message, flush=True)
            return
        if function_frame is not None:
            current_function = function_frame.f_code.co_name
            line_number = function_frame.f_lineno
            print(f"{message} ({current_function}:{line_number})", flush=True)
        else:
            print(f"{message}", flush=True)


def download_all_tickers():
    """Fetch all active stock tickers from Polygon.io, handling pagination."""

    current_frame = inspect.currentframe()

    url = "https://api.polygon.io/v3/reference/tickers"
    params = {"market": "stocks", "active": "true", "apiKey": polygon_api_key, "limit": 1000}
    tickers = []
    page_count = 0

    while True:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            page_tickers = [result['ticker'] for result in data.get('results', [])]
            tickers.extend(page_tickers)
            page_count += 1

            log_message(2, f"  fetched page {page_count} with {len(page_tickers)} tickers - total: {len(tickers)}",
                        current_frame)

            next_url = data.get("next_url")
            if not next_url:
                break

            url = next_url
            params = {"apiKey": polygon_api_key}

            time.sleep(0.1)  # 100ms delay between requests

        except requests.exceptions.RequestException as e:
            log_message(0, f"  error fetching tickers from Polygon.io: {e}")
            break

    log_message(1, f"  fetched a total of {len(tickers)} tickers from Polygon.io.",current_frame)
    return sorted(tickers)


def get_tickers(scope):
    #current_function = inspect.currentframe().f_code.co_name
    current_frame = inspect.currentframe()
    if scope == 'all':
        # use API to get all available tickers
        tickers = download_all_tickers()
    elif scope == 'database':
        # get list of tickers to operate on from database
        db = MySQLdb.connect(host=dbhost, user=dbuser, password=dbpass, database=dbname)
        cursor = db.cursor()
        query = f'SELECT DISTINCT symbol FROM {dbtable}'
        cursor.execute(query)
        tickers = [row[0] for row in cursor.fetchall()]
        db.close()
    elif scope == 'symbol':
        tickers = [args.symbol]
    log_message(1, f"Found {len(tickers)} tickers", current_frame)
    return sorted(tickers)


def download_daily_ticker_info(ticker, start_date, end_date):
    current_frame = inspect.currentframe()
    url = f"{polygon_base_url}{polygon_api_data}/{ticker}/range/1/day/{start_date}/{end_date}"

    try:
        log_message(5, f"  url={url}", current_frame)
        log_message(5, f"  params={polygon_data_params}", current_frame)
        response = requests.get(url, params=polygon_data_params)
        response.raise_for_status()

        response_json = response.json()
        log_message(5, f"  response={json.dumps(response_json, indent=2)}", current_frame)

        if "results" not in response_json or not response_json["results"]:
            log_message(4, "  no results found in API response.", current_frame)
            return pd.DataFrame()

        df_api = pd.DataFrame(response_json['results'])

        # Ensure 'vwap' column exists, and set to Decimal(0) if missing
        if 'vw' not in df_api.columns:
            df_api['vw'] = 0.0

        # Rename columns
        df_api = df_api.rename(columns={
            'v': 'volume',
            'vw': 'vwap',  
            'o': 'open',
            'c': 'close',
            'h': 'high',
            'l': 'low',
            't': 'timestamp',
            'n': 'transactions'
        })

        # Convert floating point columns to Decimal
        decimal_columns = ['volume', 'vwap', 'open', 'close', 'high', 'low']
        for col in decimal_columns:
            df_api[col] = df_api[col].apply(lambda x: Decimal(x) if pd.notnull(x) else Decimal(0))

        # Return DataFrame
        log_message(2, f"  downloaded {len(df_api)} rows", current_frame)
        return df_api

    except requests.exceptions.RequestException as e:
        log_message(0, f"  error fetching data for ticker {ticker} from Polygon.io: {e}", current_frame)
        return pd.DataFrame()


def process_api_response(data):
    current_frame = inspect.currentframe()

    # process API response and return a pandas DataFrame
    df = pd.DataFrame(data['results'])
    df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
    df['close']  = Decimal(df['c'])
    df['open']   = Decimal(df['o'])
    df['high']   = Decimal(df['h'])
    df['low']    = Decimal(df['l'])
    df['volume'] = Decimal(df['v'])
    df = df[['timestamp', 'close', 'open', 'high', 'low', 'volume']]
    return df


def get_historical_data(ticker):
    current_frame = inspect.currentframe()

    # connect to mysql database and retrieve all rows from database for ticker
    # TODO current query will not limit rows returned, bring it all back vs only 400
    #      consider design change to limit rows returned for processing efficiency
    try:
        # Connect to MySQL database
        db = MySQLdb.connect(host=dbhost, user=dbuser, passwd=dbpass, db=dbname, charset='utf8mb4')
        cursor = db.cursor()

        query = f"""
            SELECT timestamp, close, open, high, low, volume, rsi, ma50, ma200,
                   macd, macd_signal, bb_upper, bb_middle, bb_lower, adx
            FROM {dbtable}
            WHERE symbol = %s
            ORDER BY timestamp DESC
        """

        cursor.execute(query, (ticker,))
        rows = cursor.fetchall()

        # convert query result to DataFrame
        if rows:
            columns = ['timestamp', 'close', 'open', 'high', 'low', 'volume', 
               'rsi', 'ma50', 'ma200', 'macd', 'macd_signal', 
               'bb_upper', 'bb_middle', 'bb_lower', 'adx']
            df_db = pd.DataFrame(rows, columns=columns)
        else:
            # no row data retrieved, return empty dataframe
            log_message(2, f"  no history data retrieved for {ticker}",current_frame)
            return pd.DataFrame()

        # ensure columns that need to be Decimal are properly converted
        decimal_columns = ['close', 'open', 'high', 'low', 'volume',
                           'rsi', 'ma50', 'ma200', 'macd', 'macd_signal',
                           'bb_upper', 'bb_middle', 'bb_lower', 'adx']
        for col in decimal_columns:
            df_db[col] = df_db[col].astype(float).apply(Decimal)

        log_message(5, f"  fetched history dataframe from database for {ticker}:\n{df_db.head()}",current_frame)

    except Exception as e:
        log_message(0, f"  error retrieving history data for {ticker}: {e}",current_frame)
        # return an empty DataFrame on error
        return pd.DataFrame()

    finally:
        # Ensure the connection is closed
        db.close()

    log_message(2, f"  retrieved {len(df_db)} historical rows", current_frame)
    return df_db


def insert_data(symbol, df):
    """Insert or update data into the database using a DataFrame structure.
    Expecting columns: ['timestamp', 'close', 'open', 'high', 'low', 'volume',
                       'rsi', 'ma50', 'ma200', 'macd', 'macd_signal',
                       'bb_upper', 'bb_middle', 'bb_lower', 'adx']
    """
    current_frame = inspect.currentframe()

    global tickers_updated
    rows_updated = 0

    if df.empty:
        log_message(1, f"  no data available to insert for ticker {symbol}",current_frame)
        return

    try:
        # Connect to MySQL database
        db = MySQLdb.connect(host=dbhost, user=dbuser, passwd=dbpass, db=dbname, charset='utf8mb4')
        cursor = db.cursor()

        # Prepare the insert query with ON DUPLICATE KEY UPDATE
        insert_query = f"""
            INSERT INTO {dbtable} (symbol, timestamp, close, open, high, low, volume, rsi, ma50, ma200, 
                                   macd, macd_signal, bb_upper, bb_middle, bb_lower, adx)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                close = VALUES(close), open = VALUES(open), high = VALUES(high), low = VALUES(low), 
                volume = VALUES(volume), rsi = VALUES(rsi), ma50 = VALUES(ma50), ma200 = VALUES(ma200), 
                macd = VALUES(macd), macd_signal = VALUES(macd_signal), bb_upper = VALUES(bb_upper), 
                bb_middle = VALUES(bb_middle), bb_lower = VALUES(bb_lower), adx = VALUES(adx)
        """

        # Clean DataFrame to replace NaN with None for MySQL
        df = df.where(pd.notnull(df), None)

        # Convert Unix timestamp (in milliseconds) to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Format it as a string that MySQL understands (YYYY-MM-DD HH:MM:SS)
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Insert or update each row in the DataFrame
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                symbol, row['timestamp'], row['close'], row['open'], row['high'], row['low'], row['volume'],
                row['rsi'], row['ma50'], row['ma200'], row['macd'], row['macd_signal'], row['bb_upper'],
                row['bb_middle'], row['bb_lower'], row['adx']
            ))
            rows_updated += 1

        # Commit the changes
        db.commit()
        tickers_updated += 1
        log_message(2, f"  {rows_updated} rows inserted or updated for {symbol}",current_frame)

    except Exception as e:
        log_message(0, f"  error inserting data for {symbol}: {e}",current_frame)

    finally:
        # Ensure the connection is closed
        if 'db' in locals() and db.open:
            db.close()

    return rows_updated


def calculate_indicators(data):
    # calculate all indicators
    df = data.copy()
    df['rsi'] = calculate_rsi(df['close'], indicators['rsi']['window'])
    df['ma50'] = calculate_ma(df['close'], indicators['ma50']['window'])
    df['ma200'] = calculate_ma(df['close'], indicators['ma200']['window'])
    df['macd'] = calculate_macd(df['close'], indicators['macd']['window'])
    df['macd_signal'] = calculate_macd_signal(df['close'], indicators['macd_signal']['window'])
    df['bb_upper'] = calculate_bb_upper(df['close'], indicators['bb_upper']['window'])
    df['bb_middle'] = calculate_bb_middle(df['close'], indicators['bb_middle']['window'])
    df['bb_lower'] = calculate_bb_lower(df['close'], indicators['bb_lower']['window'])
    df['adx'] = calculate_adx(df['high'], df['low'], df['close'], indicators['adx']['window'])

    # Fill NaN values with 0, or another appropriate value
    df = df.fillna(0) # or df = df.fillna(method='backfill') or df = df.fillna(method='ffill')

    return df


def calculate_rsi(data, window):
    delta = data.diff(1)
    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    roll_up = up.rolling(window).mean()
    roll_down = down.rolling(window).mean().abs()
    RS = roll_up / roll_down
    RSI = 100.0 - (100.0 / (1.0 + RS))
    return RSI


def calculate_ma(data, window):
    return data.rolling(window).mean()


def calculate_macd(data, window):
    ema12 = data.ewm(span=window, adjust=False).mean()
    ema26 = data.ewm(span=26, adjust=False).mean()
    return ema12 - ema26


def calculate_macd_signal(data, window):
    macd = calculate_macd(data, indicators['macd']['window'])
    return macd.ewm(span=window, adjust=False).mean()


def calculate_bb_upper(data, window):
    ma = calculate_ma(data, window)
    std = data.rolling(window).std()
    return ma + 2 * std


def calculate_bb_middle(data, window):
    return calculate_ma(data, window)


def calculate_bb_lower(data, window):
    ma = calculate_ma(data, window)
    std = data.rolling(window).std()
    return ma - 2 * std


def calculate_adx(high, low, close, window):
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    plus_dm = high.diff().clip(lower=0)
    minus_dm = -low.diff().clip(upper=0)

    tr_smooth = tr.rolling(window).mean()
    plus_di = 100 * (plus_dm.rolling(window).mean() / tr_smooth)
    minus_di = 100 * (minus_dm.rolling(window).mean() / tr_smooth)
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.rolling(window).mean()
    return adx


def dataframe_to_list(df):
    """Convert a DataFrame to a list of tuples for easier insertion into the database."""
    df = df.fillna(None)  # Replace NaN with None for compatibility with MySQL
    data_list = [
        (
            row['timestamp'].to_pydatetime(),  # Convert timestamp to Python datetime
            row['close'], row['open'], row['high'], row['low'], row['volume'],
            row.get('rsi'), row.get('ma50'), row.get('ma200'),
            row.get('macd'), row.get('macd_signal'),
            row.get('bb_upper'), row.get('bb_middle'), row.get('bb_lower'),
            row.get('adx')
        )
        for _, row in df.iterrows()
    ]
    return data_list


def main():
    global debuglevel
    global args
    global tickers_updated
    tickers_updated = 0

    current_frame = inspect.currentframe()

    #
    parser = argparse.ArgumentParser()
    #
    # scope command line arguments
    parser.add_argument('--all', action='store_true', help='Update all tickers from Polygon.io.')
    parser.add_argument('--database', action='store_true', help='Update tickers already in the database.')
    parser.add_argument('--symbol', type=str, help='A single ticker symbol to update.')
    #
    # mode command line arguments
    parser.add_argument('--full', action='store_true', help='Fetch all available data for the tickers.')
    parser.add_argument('--incremental', action='store_true', help='Fetch only incremental data for the tickers.')
    #
    # other
    parser.add_argument('--debuglevel', type=int, default=0, help='Set the debug level (0-5).')

    args = parser.parse_args()
    debuglevel = args.debuglevel

    log_message(0, f"{__file__} {version} started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", current_frame)

    # set scope of run
    if args.all:
        scope = "all"
    elif args.database:
        scope = "database"
    elif args.symbol:
        scope = "symbol"
    else:
        parser.print_help()
        exit()

    # set mode of run
    mode = "full" if args.full else "incremental"

    log_message(0, f"Executing {mode} run with scope {scope}")
    start_time = datetime.now()

    # get tickers to be processed
    tickers = get_tickers(scope)

    if not tickers:
        print("No tickers to process.")
        exit()

    # retrieve all data available, calculate indicators and write to database
    if mode == 'full':
        # cycle through each ticker in scope
        for ticker in tickers:
            log_message(1, f'Processing {ticker}')
            log_message(3, f'  requesting daily data from 1980-01-01',current_frame)
            data = download_daily_ticker_info(ticker, '1980-01-01', datetime.now().strftime('%Y-%m-%d'))
            if not data.empty:
                data = calculate_indicators(data)
                insert_data(ticker, data)

    # retrieve any new data since most recent timestamp
    elif mode == 'incremental':
        # calculate end_date based on today's date if time of day > 16:30ET
        # otherwise use last business day or easiest, yesterday

        # get the current time in Eastern Time (ET)
        now_utc = datetime.now(pytz.utc)
        now_et = now_utc.astimezone(pytz.timezone('US/Eastern'))

        # determine the end date (today if after 16:30 ET, otherwise yesterday)
        if now_et.hour > 16 or (now_et.hour == 16 and now_et.minute >= 30):
            end_date = now_et.date()
        else:
            end_date = (now_et - timedelta(days=1)).date()

        # cycle through each ticker in scope
        for ticker in tickers:
            log_message(1, f'Processing {ticker}')
            historical_data = get_historical_data(ticker)
            log_message(5, f"  historical_data return from get_historical_data: {historical_data.head()}", current_frame)
            if historical_data.empty:
                log_message(1, f'  no historical data retrieved for {ticker}, skipping.')
                continue

            # obtain latest timestamp for ticker in database (max_timestamp)
            # start_date will be max_timestamp + 1
            max_timestamp = historical_data['timestamp'].max()

            # may not need?  is max_timestamp already pd object?
            max_timestamp = pd.to_datetime(max_timestamp)

            # set default if null
            if pd.isnull(max_timestamp):
                start_date = pd.Timestamp('1980-01-01')
            else:
                start_date = max_timestamp + pd.Timedelta(days=1)

            start_date_str = start_date.strftime('%Y-%m-%d')

            # convert start_date and end_date to datetime objects
            start_date_dt = start_date
            end_date_dt = pd.to_datetime(end_date)

            # ensure start_date not later than end_date
            if start_date_dt > end_date_dt:
                log_message(5, f"  start_date {start_date_str} > end_date {end_date}, overriding to end_date", current_frame)
                start_date_str = end_date

            # if max_timestamp date == end_date date then nothing to do this run
            log_message(5, f"  end_date_dt={end_date_dt}, max_timestamp={max_timestamp}", current_frame)
            if end_date_dt.date() == max_timestamp.date():
                # skip
                log_message(3, f"  historical data up to date, skipping download", current_frame)
                continue

            log_message(2, f'  requesting daily data from {start_date_str} to {end_date}',current_frame)
            new_data = download_daily_ticker_info(ticker, start_date_str, end_date)
            if not new_data.empty:
                log_message(5, f"  new_data return from download_daily_ticker_info: {new_data.head()}", current_frame)


                # Ensure max_timestamp is a pandas Timestamp
                if not isinstance(max_timestamp, pd.Timestamp):
                    max_timestamp = pd.to_datetime(max_timestamp, errors='coerce')
                    if pd.isna(max_timestamp):
                        raise ValueError(f"Invalid max_timestamp: {max_timestamp}")
                
                # Ensure 'timestamp' in both historical_data and new_data is a pandas Timestamp
                #historical_data['timestamp'] = pd.to_datetime(historical_data['timestamp'], errors='coerce')
                #new_data['timestamp'] = pd.to_datetime(new_data['timestamp'], errors='coerce')

                # Convert 'timestamp' in historical_data and new_data to pandas datetime
                historical_data['timestamp'] = pd.to_datetime(historical_data['timestamp'], errors='coerce')
                new_data['timestamp'] = pd.to_datetime(new_data['timestamp'], unit='ms', errors='coerce')
                
                # Concatenate historical and new data
                all_data = pd.concat([historical_data[['timestamp', 'close', 'open', 'high', 'low', 'volume']], new_data], ignore_index=True)
                
                # Check and log data types for debugging
                log_message(5, f"  all_data['timestamp'] dtype: {all_data['timestamp'].dtype}", current_frame)
                
                # Ensure 'timestamp' is sorted (if needed for calculate_indicators)
                all_data = all_data.sort_values(by='timestamp').reset_index(drop=True)
                
                # Calculate indicators
                log_message(5, f"  calling calculate_indicators with: {all_data.head()}", current_frame)
                all_data = calculate_indicators(all_data)
                
                # Ensure max_timestamp is still valid after potential conversion
                log_message(5, f"  max_timestamp: {max_timestamp}", current_frame)
                
                # Filter for new rows where 'timestamp' is greater than max_timestamp
                new_rows = all_data[all_data['timestamp'] > max_timestamp]
                
                # Log the resulting new_rows
                log_message(5, f"  new_rows after filter: {new_rows.shape[0]} rows", current_frame)
                if not new_rows.empty:
                    log_message(5, f"  calling insert_data with new_rows: {new_rows.head()}", current_frame)
                    insert_data(ticker, new_rows)
                else:
                    log_message(4, "  no new rows to insert, new_rows is empty", current_frame)


            else:
                log_message(2, f'  no new data found for {ticker}, new_data is empty.')


    end_time = datetime.now()
    log_message(0, f"{tickers_updated} tickers updated out of {len(tickers)} total")
    log_message(0, f"Script ended at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log_message(0, f"Total run time: {end_time - start_time}")


if __name__ == '__main__':
    main()
