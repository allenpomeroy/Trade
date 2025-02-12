#!/usr/bin/python3
#
# find-trade-candidates.py
#
# v1.2 2025/02/12
# - updated sql logic to find candidates

import argparse
import MySQLdb
import json
import requests
import inspect
from datetime import datetime

# constants
version = "1.2"
dbhost = "localhost"
dbuser = "aitrade"
dbpass = "aitrade1"
dbname = "aitrade"
dbtable = "stock_data"
webhookurl = "https://example.com/webhook/040cdc"
global debuglevel


test_payload = {
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


def get_tickers_from_db(cursor, min_price, max_price, max_tickers):
    current_frame = inspect.currentframe()
    query = f"""
    SELECT DISTINCT symbol
    FROM {dbtable}
    WHERE close BETWEEN %s AND %s
    AND timestamp = (SELECT MAX(timestamp) FROM {dbtable})
    LIMIT %s
    """
    cursor.execute(query, (min_price, max_price, max_tickers))
    return [row[0] for row in cursor.fetchall()]


def find_trading_candidates(cursor, minclose, maxclose):
    current_frame = inspect.currentframe()

    log_message(1, f"  finding trade candidates with min {minclose} and max {maxclose}", current_frame)
    query = f"""
    SELECT symbol, timestamp, close, rsi, ma50, ma200, macd, macd_signal, bb_upper, bb_middle, bb_lower, adx
    FROM {dbtable}
    WHERE close BETWEEN %s AND %s
      AND rsi <= 30
      AND ma50 > ma200
      AND ma50 - ma200 <= 0.5
      AND macd > macd_signal
      AND close < bb_middle
      AND adx BETWEEN 20 AND 40
      AND timestamp >= NOW() - INTERVAL 5 DAY
    ORDER BY timestamp DESC
    """

    cursor.execute(query, (minclose, maxclose))
    rows = cursor.fetchall()
    if not rows:
        log_message(2, f"  no tickers match query criteria", current_frame)
        return {}

    # Convert the results into a structured dictionary format
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

    # Query to get the most recent {days} days of data for a given symbol
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
    parser.add_argument("--min-price", type=float, default=2.0, help="Min stock price (default: 2.0).")
    parser.add_argument("--max-price", type=float, default=22.0, help="Max stock price (default: 22.0).")
    parser.add_argument("--max-tickers", type=int, default=10000, help="Max number of tickers to analyze (default: 10000).")
    parser.add_argument("--history-days", type=int, default=14, help="Number of days of history per candidate to emit (default: 14).")
    parser.add_argument("--webhook", action="store_true", help="When set, send to configured webhook destinations")
    parser.add_argument("--sample", action="store_true", help="When set, send sample payload to configured webhook destinations")
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

    if not args.sample:
        log_message(1, f"Finding trading candidates", current_frame)
        candidates = find_trading_candidates(cursor, min_price, max_price)
    
        if candidates:
            log_message(1, f"Found {len(candidates)} trading candidates", current_frame)
            log_message(1, f"{json.dumps(candidates, indent=2)}\n", current_frame)
            #print(json.dumps(candidates, indent=2))
        
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
            #print(json.dumps(full_data, indent=2))
        
            if args.webhook:
                print(f"Sending data to webhook {webhookurl}...")
                send_webhook(webhookurl, full_data)

        else:
            print("No trading candidates found.")

    # Send to webhook destination
    if args.webhook:
        if args.sample:
            print(f"Sending sample to webhook {webhookurl}")
            send_webhook(webhookurl, test_payload)
        else:
            print(f"Sending to webhook {webhookurl}")
            send_webhook(webhookurl, full_data)


    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
