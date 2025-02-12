AITrade project contains code and design for experimental trading platform

Example output of the selection code:

./find-trade-candidates.py --history-days 2
{
  "candidates": {
    "HIVE": [
      {
        "date": "2025-02-12",
        "close": 2.7101,
        "rsi": 30.77219,
        "ma50": 3.276902,
        "ma200": 3.263051,
        "macd": -0.133886,
        "macd_signal": -0.138434,
        "bb_upper": 3.273734,
        "bb_middle": 2.951005,
        "bb_lower": 2.628276,
        "adx": 20.939091
      },
      {
        "date": "2025-02-11",
        "close": 2.72,
        "rsi": 29.906542,
        "ma50": 3.3035,
        "ma200": 3.26485,
        "macd": -0.130004,
        "macd_signal": -0.138798,
        "bb_upper": 3.264278,
        "bb_middle": 2.9615,
        "bb_lower": 2.658722,
        "adx": 20.026712
      }
    ],
    "GOODO": [
      {
        "date": "2025-02-12",
        "close": 20.75,
        "rsi": 25.142045,
        "ma50": 20.946556,
        "ma200": 20.736829,
        "macd": -0.070019,
        "macd_signal": -0.098258,
        "bb_upper": 21.150938,
        "bb_middle": 20.81907,
        "bb_lower": 20.487202,
        "adx": 18.090424
      },
      {
        "date": "2025-02-11",
        "close": 20.67,
        "rsi": 35.125154,
        "ma50": 20.961556,
        "ma200": 20.728029,
        "macd": -0.07633,
        "macd_signal": -0.100517,
        "bb_upper": 21.163336,
        "bb_middle": 20.80807,
        "bb_lower": 20.452804,
        "adx": 19.53189
      }
    ]
  }
}
