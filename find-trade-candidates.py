#!/usr/bin/python3
#
# find-trade-candidates.py
#
# run sql query against stock_data table to find
# potential candidates for long trade.  outputs
# candidate and history in JSON format to feed
# downstream apps. 
#
# (c) Allen Pomeroy, 2025, MIT License
#
# v2.0 2025/02/17
# - added cli options for analysis limits
#
# syntax:
# find-trade-candidates.py
# - will use all defaults and output to stdout candidates
#   found in the last 5 days with 14 days of history
#
# scope:
# --ticker-file {path-to-file}   limits query to a list
#   of tickers, otherwise query entire stock_data table
# --min-price        floor for stock price
# --max-price        ceiling for stock price
#
# analysis parameters:
# --rsilimit         must be below this value
# --ma50ma200delta   max separation of two ma values
# --adxminlimit      min adx value
# --adxmaxlimit      max adx value
# --lookbackdays     last x days to consider
#
# output parameters:
# --history-days     output this many days of history
# --webhook          send findings to webhook as well as stdout
# --debuglevel       0-5 0=min, 5=max
#
# example use:
# concise (one day output) of candidates found in the last 7 days
# ./find-trade-candidates2.py --lookbackdays 7 --history-days 1
#{
#  "candidates": {
#    "EVGO": [
#      {
#        "date": "2025-02-14",
#        "close": 3.03,
#        "rsi": 33.870968,
#        "ma50": 4.238756,
#        "ma200": 4.224339,
#        "macd": -0.346368,
#        "macd_signal": -0.398811,
#        "bb_upper": 3.714767,
#        "bb_middle": 3.24189,
#        "bb_lower": 2.769013,
#        "adx": 39.822473
#      }
#    ],
#    "HIVE": [
#      {
#        "date": "2025-02-14",
#        "close": 2.85,
#        "rsi": 54.023914,
#        "ma50": 3.227702,
#        "ma200": 3.262201,
#        "macd": -0.114654,
#        "macd_signal": -0.135644,
#        "bb_upper": 3.219743,
#        "bb_middle": 2.922005,
#        "bb_lower": 2.624267,
#        "adx": 20.60974
#      }
#    ]
#  }
#}

# imports
import argparse
import MySQLdb
import json
import requests
import inspect
from datetime import datetime

# constants
version = "2.0"
dbhost = "localhost"
dbuser = "aitrade"
dbpass = "aitrade1"
dbname = "aitrade"
dbtable = "stock_data"
webhookurl = "https://hook.us2.make.com/tlen5q8nfsk5e51g2vi5lgo3jbfsookm"
global debuglevel

# example output
example_payload = {
  "candidates": {
    "BSL": [
      {
        "date": "2024-12-30",
        "close": 14.51,
        "rsi": 54.036355,
        "ma50": 14.148898,
        "ma200": 13.585734
      },
      {
        "date": "2024-12-31",
        "close": 14.34,
        "rsi": 49.887282,
        "ma50": 14.160151,
        "ma200": 13.591394
      }
    ],
    "SONO": [
      {
        "date": "2024-12-30",
        "close": 15.07,
        "rsi": 53.877543,
        "ma50": 13.8022,
        "ma200": 14.46765
      },
      {
        "date": "2024-12-31",
        "close": 15.04,
        "rsi": 62.26414,
        "ma50": 13.8432,
        "ma200": 14.4488
      }
    ]
  }
}


def log_message(level, message, function_frame=None):
    if debuglevel >= level:
        if level <= 1:
            print(message, flush=True)
            return
        if function_frame is not None:
            current_function = function_frame.f_code.co_name
            line_number = function_frame.f_lineno
            print(f"{message} ({current_function}:{line_number})", flush=True)
        else:
            print(f"{message}", flush=True)


def get_db_connection():
    return MySQLdb.connect(
        host="localhost",
        user="aitrade",
        password="aitrade1",
        database="aitrade"
    )


def read_tickers_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            tickers = [line.strip() for line in file.readlines() if line.strip()]
        return tickers
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        exit(1)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        exit(1)


def read_tickers_from_database():
    # get list of tickers to operate on from database
    db = MySQLdb.connect(host=dbhost, user=dbuser, password=dbpass, database=dbname)
    cursor = db.cursor()
    query = f'SELECT DISTINCT symbol FROM {dbtable}'
    cursor.execute(query)
    tickers = [row[0] for row in cursor.fetchall()]
    db.close()

    if tickers:
        return tickers
    else:
        return None
        

def find_trading_candidates(cursor, tickers, minclose, maxclose, rsilimit, ma50ma200delta, adxminlimit, adxmaxlimit, lookbackdays):
    current_frame = inspect.currentframe()

    placeholders = ','.join(['%s'] * len(tickers))
    log_message(1, f"Finding trade candidates for provided tickers with min {minclose} and max {maxclose}", current_frame)
    query = f"""
    SELECT symbol, timestamp, close, rsi, ma50, ma200, macd, macd_signal, bb_upper, bb_middle, bb_lower, adx
    FROM {dbtable}
    WHERE symbol IN ({placeholders})
      AND close BETWEEN %s AND %s
      AND rsi <= %s
      AND ma50 > ma200
      AND ma50 - ma200 <= %s
      AND macd > macd_signal
      AND close < bb_middle
      AND adx BETWEEN %s AND %s
      AND timestamp >= NOW() - INTERVAL %s DAY
    ORDER BY timestamp DESC
    """
    
    cursor.execute(query, (*tickers, minclose, maxclose, rsilimit, ma50ma200delta, adxminlimit, adxmaxlimit, lookbackdays))
    rows = cursor.fetchall()
    if not rows:
        log_message(2, f"  no tickers match query criteria", current_frame)
        return {}

    # convert the results into a structured dictionary format
    result = {"candidates": {}}
    for row in rows:
        symbol, timestamp, close, rsi, ma50, ma200, macd, macd_signal, bb_upper, bb_middle, bb_lower, adx = row
        date_str = timestamp.strftime('%Y-%m-%d')

        if symbol not in result["candidates"]:
            result["candidates"][symbol] = []

        result["candidates"][symbol].append({
            "date": date_str,
            "close": float(close),
            "rsi": float(rsi),
            "ma50": float(ma50),
            "ma200": float(ma200),
            "macd": float(macd),
            "macd_signal": float(macd_signal),
            "bb_upper": float(bb_upper),
            "bb_middle": float(bb_middle),
            "bb_lower": float(bb_lower),
            "adx": float(adx),
        })

    return result


def get_history_data(cursor, symbol, days):
    current_frame = inspect.currentframe()

    log_message(1, f"  getting {days} days history for {symbol}", current_frame)

    # query to get the most recent {days} days of data for a given symbol
    query = f"""
    SELECT timestamp, close, rsi, ma50, ma200, macd, macd_signal, bb_upper, bb_middle, bb_lower, adx
    FROM {dbtable}
    WHERE symbol = %s
    ORDER BY timestamp DESC
    LIMIT %s
    """
    cursor.execute(query, (symbol, days))
    rows = cursor.fetchall()

    # If no rows returned, return an empty list
    if not rows:
        log_message(1, f"  no history found", current_frame)
        return []

    # Convert the results into a list of dictionaries
    data = []
    for row in rows:
        # Ensure all fields are unpacked correctly, filling missing fields with None or default values
        (timestamp, close, rsi, ma50, ma200, macd, macd_signal, bb_upper, bb_middle, bb_lower, adx) = row

        # Handle missing or None values with defaults (e.g., 0.0 for floats)
        data.append({
            "date": timestamp.strftime('%Y-%m-%d'),
            "close": float(close) if close is not None else 0.0,
            "rsi": float(rsi) if rsi is not None else 0.0,
            "ma50": float(ma50) if ma50 is not None else 0.0,
            "ma200": float(ma200) if ma200 is not None else 0.0,
            "macd": float(macd) if macd is not None else 0.0,
            "macd_signal": float(macd_signal) if macd_signal is not None else 0.0,
            "bb_upper": float(bb_upper) if bb_upper is not None else 0.0,
            "bb_middle": float(bb_middle) if bb_middle is not None else 0.0,
            "bb_lower": float(bb_lower) if bb_lower is not None else 0.0,
            "adx": float(adx) if adx is not None else 0.0,
        })

    return data


def send_webhook(url, payload):
    """
    Send a JSON payload to a webhook URL.
    
    :param url: The webhook URL to send the payload to
    :param payload: A dictionary containing the JSON payload
    :return: The response from the webhook receiver
    """
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx
        return response
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def main():
    global debuglevel

    current_frame = inspect.currentframe()
    parser = argparse.ArgumentParser(description="Stock Analysis Script")

    # scope and input limits
    parser.add_argument("--min-price", type=float, default=2.0, help="Min stock price (default: 2.0).")
    parser.add_argument("--max-price", type=float, default=22.0, help="Max stock price (default: 22.0).")
    parser.add_argument("--ticker-file", type=str, help="Path to the file containing tickers.")
    # analysis control
    parser.add_argument("--rsilimit", type=float, default=30.0, help="Max RSI level (default: 30.0).")
    parser.add_argument("--ma50ma200delta", type=float, default=0.3, help="Max delta between ma50 and ma200 level (default: 0.3).")
    parser.add_argument("--adxminlimit", type=float, default=20.0, help="Min ADX value (default: 20.0).")
    parser.add_argument("--adxmaxlimit", type=float, default=40.0, help="Max ADX value (default: 40.0).")
    parser.add_argument("--lookbackdays", type=float, default=5.0, help="Max days to look back (default: 5.0).")
    # output control
    parser.add_argument("--history-days", type=int, default=14, help="Number of days of history per candidate to emit (default: 14).")
    parser.add_argument("--webhook", action="store_true", help="When set, send to configured webhook destinations")
    # other
    parser.add_argument('--debuglevel', type=int, default=0, help='Set the debug level (0-5).')

    args = parser.parse_args()
    debuglevel = args.debuglevel
    log_message(1, f"{__file__} {version} started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", current_frame)

    conn = get_db_connection()
    cursor = conn.cursor()

    min_price = args.min_price
    max_price = args.max_price
    history_days = args.history_days

    log_message(1, f"Min price: {min_price}, Max price: {max_price}, History days: {history_days}")

    # set scope for analysis - all symbols in db or just HDO tickers in list file
    if args.ticker_file:
        tickers = read_tickers_from_file(args.ticker_file)

    else:
        tickers = read_tickers_from_database()

    conn = get_db_connection()
    cursor = conn.cursor()

    log_message(1, f"Finding trading candidates", current_frame)
    # run query on scope of tickers
    candidates = find_trading_candidates(cursor, tickers, args.min_price, args.max_price, args.rsilimit,
                                         args.ma50ma200delta, args.adxminlimit, args.adxmaxlimit,
                                         args.lookbackdays)
    
    if candidates:
        log_message(1, f"Found {len(candidates)} trading candidates", current_frame)
        log_message(1, f"{json.dumps(candidates, indent=2)}\n", current_frame)
        
        # Create a new dictionary to hold the full data for candidates
        full_data = {"candidates": {}}
        
        # Fetch last history_days days of data for each candidate
        log_message(1, f"Fetching {history_days} for each candidates", current_frame)
        for symbol in candidates["candidates"]:
            log_message(2, f"  fetching history data for {symbol}")
            history_data = get_history_data(cursor, symbol, history_days)
            full_data["candidates"][symbol] = history_data
            
        # Print the final JSON object with both candidates and their historical data
        log_message(1, f"Final JSON object with all data:", current_frame)
        log_message(0, f"{json.dumps(full_data,indent=2)}", current_frame)
        
        if args.webhook:
            log_message(1, f"Sending data to webhook {webhookurl}")
            send_webhook(webhookurl, json.dumps(full_data))

    else:
        log_message(1, "No trading candidates found.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
