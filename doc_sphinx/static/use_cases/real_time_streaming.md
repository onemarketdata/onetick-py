---
file_format: mystnb
---

# Real Time Streaming

This section contains 3 examples for Real Time Streaming using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Streaming Ticks

Stream individual trade ticks in real-time using OneTick WebAPI with batch size configuration.  
Demonstrates continuous streaming with
{meth}`process_ticks <onetick.py.CallbackBase.process_ticks>` callback for per-tick analysis.  
Configures ``bs_ticks=1`` for minimal batch size to get frequent updates.

```python
import onetick.py as otp

class InfoCallback(otp.CallbackBase):
    def __init__(self):
        super().__init__()
        self.tick_count = 0

    def process_ticks(self, ticks):
        self.tick_count += 1
        print(f"[Tick #{self.tick_count}] {ticks['Time']}")

    def done(self):
        print(f"\n[Complete] Streaming ended. Total ticks received: {self.tick_count}")

trades = otp.DataSource(db='US_COMP_REPLAY', tick_type='TRD', schema_policy='manual')
try:
    otp.run(
        trades,
        symbols='CSCO',
        start=otp.now(),
        end=otp.now() + otp.Second(4),
        timezone='UTC',
        running=True,
        callback=InfoCallback(),
        bs_ticks=1,
    )
except Exception as e:
    print(f"Error during streaming: {e}")
```

```text
[Tick #1] ['2026-08-03T13:46:04.373470000']
[Tick #2] ['2026-08-03T13:46:04.444361000']
[Tick #3] ['2026-08-03T13:46:04.474041000']
[Tick #4] ['2026-08-03T13:46:04.474046000']
[Tick #5] ['2026-08-03T13:46:04.507209000']
...
[Tick #121] ['2026-08-03T13:46:07.676835000']
[Tick #122] ['2026-08-03T13:46:07.832010000']
[Tick #123] ['2026-08-03T13:46:07.948852000']
[Tick #124] ['2026-08-03T13:46:08.233410000']
[Tick #125] ['2026-08-03T13:46:08.233411000']

[Complete] Streaming ended. Total ticks received: 125
```

## Streaming Bars

Stream aggregated trade bars in real-time with 5-second bucketing.  
Calculates OHLC and volume statistics on each bar using continuous aggregation.  
Demonstrates real-time OHLC bar generation with callback-based streaming updates.

```python
import onetick.py as otp

class InfoCallback(otp.CallbackBase):
    def __init__(self):
        super().__init__()
        self.tick_count = 0

    def process_ticks(self, ticks):
        self.tick_count += 1
        print({k: str(v[0]) for k, v in ticks.items()})

    def done(self):
        print(f"\n[Complete] Streaming ended. Total ticks received: {self.tick_count}")

trades = otp.DataSource(db='US_COMP_REPLAY', tick_type='TRD', schema={'PRICE': float, 'SIZE': int})

# Calculate real-time bars for 5-second buckets
trades = trades.agg({
    'FIRST_PRICE': otp.agg.first('PRICE'),
    'HIGH_PRICE': otp.agg.max('PRICE'),
    'LOW_PRICE': otp.agg.min('PRICE'),
    'LAST_PRICE': otp.agg.last('PRICE'),
    'SUM_SIZE': otp.agg.sum('SIZE'),
    'TRADE_COUNT': otp.agg.count()
}, bucket_interval=5)

try:
    print("Starting continuous tick streaming...\n")
    otp.run(
        trades,
        symbols='CSCO',
        start=otp.now(),
        end=otp.now() + otp.Minute(1),
        timezone='UTC',
        running=True,
        callback=InfoCallback(),
        bs_ticks=1,
    )
except Exception as e:
    print(f"Error during streaming: {e}")
```

```text
Starting continuous tick streaming...

{'Time': '2026-08-03T13:59:42.056000000', 'FIRST_PRICE': '121.79', 'HIGH_PRICE': '121.8142', 'LOW_PRICE': '121.694', 'LAST_PRICE': '121.76', 'SUM_SIZE': '2114', 'TRADE_COUNT': '49'}
{'Time': '2026-08-03T13:59:47.056000000', 'FIRST_PRICE': '121.78', 'HIGH_PRICE': '121.81', 'LOW_PRICE': '121.695', 'LAST_PRICE': '121.79', 'SUM_SIZE': '1747', 'TRADE_COUNT': '27'}
{'Time': '2026-08-03T13:59:52.056000000', 'FIRST_PRICE': '121.6956', 'HIGH_PRICE': '121.7828', 'LOW_PRICE': '121.6956', 'LAST_PRICE': '121.78', 'SUM_SIZE': '612', 'TRADE_COUNT': '13'}
{'Time': '2026-08-03T13:59:57.056000000', 'FIRST_PRICE': '121.76', 'HIGH_PRICE': '121.76', 'LOW_PRICE': '121.64', 'LAST_PRICE': '121.65', 'SUM_SIZE': '4776', 'TRADE_COUNT': '105'}
{'Time': '2026-08-03T14:00:02.056000000', 'FIRST_PRICE': '121.64', 'HIGH_PRICE': '121.6947', 'LOW_PRICE': '121.62', 'LAST_PRICE': '121.6942', 'SUM_SIZE': '3455', 'TRADE_COUNT': '66'}
{'Time': '2026-08-03T14:00:07.056000000', 'FIRST_PRICE': '121.65', 'HIGH_PRICE': '121.7158', 'LOW_PRICE': '121.65', 'LAST_PRICE': '121.715', 'SUM_SIZE': '803', 'TRADE_COUNT': '15'}
{'Time': '2026-08-03T14:00:12.056000000', 'FIRST_PRICE': '121.71', 'HIGH_PRICE': '121.7839', 'LOW_PRICE': '121.6796', 'LAST_PRICE': '121.715', 'SUM_SIZE': '646', 'TRADE_COUNT': '27'}
{'Time': '2026-08-03T14:00:17.056000000', 'FIRST_PRICE': '121.69', 'HIGH_PRICE': '121.7', 'LOW_PRICE': '121.657', 'LAST_PRICE': '121.685', 'SUM_SIZE': '1350', 'TRADE_COUNT': '26'}
{'Time': '2026-08-03T14:00:22.056000000', 'FIRST_PRICE': '121.69', 'HIGH_PRICE': '121.7093', 'LOW_PRICE': '121.59', 'LAST_PRICE': '121.6', 'SUM_SIZE': '2593', 'TRADE_COUNT': '61'}
{'Time': '2026-08-03T14:00:27.056000000', 'FIRST_PRICE': '121.61', 'HIGH_PRICE': '121.7101', 'LOW_PRICE': '121.6', 'LAST_PRICE': '121.635', 'SUM_SIZE': '785', 'TRADE_COUNT': '21'}
{'Time': '2026-08-03T14:00:32.056000000', 'FIRST_PRICE': '121.635', 'HIGH_PRICE': '121.7108', 'LOW_PRICE': '121.6', 'LAST_PRICE': '121.675', 'SUM_SIZE': '1565', 'TRADE_COUNT': '29'}
{'Time': '2026-08-03T14:00:37.056000000', 'FIRST_PRICE': '121.6825', 'HIGH_PRICE': '121.7765', 'LOW_PRICE': '121.59', 'LAST_PRICE': '121.595', 'SUM_SIZE': '1477', 'TRADE_COUNT': '42'}

[Complete] Streaming ended. Total ticks received: 12
```

## Conflated Ticks

Stream conflated (aggregated) trade ticks using `last_tick` bucketing to reduce data volume.  
Demonstrates continuous streaming using method {meth}`last <onetick.py.Source.last>`
with parameter `bucket_interval=1` for 1-second conflation.  
Useful for reducing noise while maintaining time-series continuity.

```python
import onetick.py as otp

class InfoCallback(otp.CallbackBase):
    def __init__(self):
        super().__init__()
        self.tick_count = 0

    def process_ticks(self, ticks):
        self.tick_count += 1
        print({k: str(v[0]) for k, v in ticks.items()})

    def done(self):
        print(f"\n[Complete] Streaming ended. Total ticks received: {self.tick_count}")

# Stream conflated trade ticks using aggregation
trades = otp.DataSource(db='US_COMP_REPLAY', tick_type='TRD', schema_policy='manual')

# Apply last_tick aggregation with 1-second buckets for conflation
trades = trades.last(bucket_interval=1, bucket_units='seconds', keep_timestamp=False)

# Stream with continuous query mode for real-time conflation
try:
    print("Starting continuous tick streaming...\n")
    otp.run(
        trades,
        symbols='CSCO',
        start=otp.now(),
        end=otp.now() + otp.Second(10),
        timezone='UTC',
        running=True,
        callback=InfoCallback(),
        bs_ticks=1,
    )
except Exception as e:
    print(f"Error during streaming: {e}")
```

```text
Starting continuous tick streaming...

{'Time': '2026-08-03T13:58:37.094000000', 'TICK_TIME': '2026-08-03T13:58:37.054817000', 'EXCH_TIME': '2026-08-03T13:57:32.604188190', 'TRF_TIME': '2026-08-03T13:57:32.604596184', 'EXCHANGE': 'D', 'TRF': 'Q', 'SOURCE': 'N', 'PRICE': '121.74', 'SIZE': '5', 'FRACTIONAL_SIZE': '5.0', 'AGGRESSOR_SIDE': '', 'TRADE_TYPE': '@  I', 'TRADE_PERIOD': '-', 'BOOK_TYPE': '9', 'STOP_STOCK': '', 'TTE': '0', 'TRADE_ID': '7530', 'PARTICIPANT_TIME': '2026-08-03T13:57:32.604188190', 'COND': '@  I', 'TICK_STATUS': '0', 'DELETED_TIME': '1970-01-01T00:00:00.000', 'OMDSEQ': '5'}
{'Time': '2026-08-03T13:58:38.094000000', 'TICK_TIME': '2026-08-03T13:58:38.059776000', 'EXCH_TIME': '2026-08-03T13:57:33.609691740', 'TRF_TIME': '1970-01-01T00:00:00.000000000', 'EXCHANGE': 'P', 'TRF': '', 'SOURCE': 'N', 'PRICE': '121.76', 'SIZE': '20', 'FRACTIONAL_SIZE': '20.0', 'AGGRESSOR_SIDE': '', 'TRADE_TYPE': '@F I', 'TRADE_PERIOD': '-', 'BOOK_TYPE': '0', 'STOP_STOCK': '', 'TTE': '1', 'TRADE_ID': '4089', 'PARTICIPANT_TIME': '2026-08-03T13:57:33.609691740', 'COND': '@F I', 'TICK_STATUS': '0', 'DELETED_TIME': '1970-01-01T00:00:00.000', 'OMDSEQ': '28'}
{'Time': '2026-08-03T13:58:39.094000000', 'TICK_TIME': '2026-08-03T13:58:38.926000000', 'EXCH_TIME': '2026-08-03T13:57:34.399050519', 'TRF_TIME': '1970-01-01T00:00:00.000000000', 'EXCHANGE': 'P', 'TRF': '', 'SOURCE': 'N', 'PRICE': '121.77', 'SIZE': '20', 'FRACTIONAL_SIZE': '20.0', 'AGGRESSOR_SIDE': '', 'TRADE_TYPE': '@  I', 'TRADE_PERIOD': '-', 'BOOK_TYPE': '0', 'STOP_STOCK': '', 'TTE': '0', 'TRADE_ID': '4091', 'PARTICIPANT_TIME': '2026-08-03T13:57:34.399050519', 'COND': '@  I', 'TICK_STATUS': '0', 'DELETED_TIME': '1970-01-01T00:00:00.000', 'OMDSEQ': '24'}
{'Time': '2026-08-03T13:58:40.094000000', 'TICK_TIME': '2026-08-03T13:58:39.561876000', 'EXCH_TIME': '2026-08-03T13:57:35.111131172', 'TRF_TIME': '1970-01-01T00:00:00.000000000', 'EXCHANGE': 'P', 'TRF': '', 'SOURCE': 'N', 'PRICE': '121.77', 'SIZE': '20', 'FRACTIONAL_SIZE': '20.0', 'AGGRESSOR_SIDE': '', 'TRADE_TYPE': '@F I', 'TRADE_PERIOD': '-', 'BOOK_TYPE': '0', 'STOP_STOCK': '', 'TTE': '1', 'TRADE_ID': '4093', 'PARTICIPANT_TIME': '2026-08-03T13:57:35.111131172', 'COND': '@F I', 'TICK_STATUS': '0', 'DELETED_TIME': '1970-01-01T00:00:00.000', 'OMDSEQ': '8'}
{'Time': '2026-08-03T13:58:41.094000000', 'TICK_TIME': '2026-08-03T13:58:40.517142000', 'EXCH_TIME': '2026-08-03T13:57:36.055403000', 'TRF_TIME': '2026-08-03T13:57:36.068068065', 'EXCHANGE': 'D', 'TRF': 'Q', 'SOURCE': 'N', 'PRICE': '121.79', 'SIZE': '4', 'FRACTIONAL_SIZE': '4.0', 'AGGRESSOR_SIDE': '', 'TRADE_TYPE': '@  I', 'TRADE_PERIOD': '-', 'BOOK_TYPE': '9', 'STOP_STOCK': '', 'TTE': '0', 'TRADE_ID': '7534', 'PARTICIPANT_TIME': '2026-08-03T13:57:36.055403000', 'COND': '@  I', 'TICK_STATUS': '0', 'DELETED_TIME': '1970-01-01T00:00:00.000', 'OMDSEQ': '9'}
{'Time': '2026-08-03T13:58:42.094000000', 'TICK_TIME': '2026-08-03T13:58:42.078141000', 'EXCH_TIME': '2026-08-03T13:57:37.628308324', 'TRF_TIME': '1970-01-01T00:00:00.000000000', 'EXCHANGE': 'P', 'TRF': '', 'SOURCE': 'N', 'PRICE': '121.8', 'SIZE': '20', 'FRACTIONAL_SIZE': '20.0', 'AGGRESSOR_SIDE': '', 'TRADE_TYPE': '@  I', 'TRADE_PERIOD': '-', 'BOOK_TYPE': '0', 'STOP_STOCK': '', 'TTE': '0', 'TRADE_ID': '4097', 'PARTICIPANT_TIME': '2026-08-03T13:57:37.628308324', 'COND': '@  I', 'TICK_STATUS': '0', 'DELETED_TIME': '1970-01-01T00:00:00.000', 'OMDSEQ': '196'}
{'Time': '2026-08-03T13:58:43.094000000', 'TICK_TIME': '2026-08-03T13:58:42.552404000', 'EXCH_TIME': '2026-08-03T13:57:38.101000000', 'TRF_TIME': '2026-08-03T13:57:38.102657236', 'EXCHANGE': 'D', 'TRF': 'Q', 'SOURCE': 'N', 'PRICE': '121.785', 'SIZE': '1', 'FRACTIONAL_SIZE': '1.0', 'AGGRESSOR_SIDE': '', 'TRADE_TYPE': '@  I', 'TRADE_PERIOD': '-', 'BOOK_TYPE': '9', 'STOP_STOCK': '', 'TTE': '0', 'TRADE_ID': '7538', 'PARTICIPANT_TIME': '2026-08-03T13:57:38.101000000', 'COND': '@  I', 'TICK_STATUS': '0', 'DELETED_TIME': '1970-01-01T00:00:00.000', 'OMDSEQ': '6'}
{'Time': '2026-08-03T13:58:44.094000000', 'TICK_TIME': '2026-08-03T13:58:43.961014000', 'EXCH_TIME': '2026-08-03T13:57:39.510929042', 'TRF_TIME': '2026-08-03T13:57:39.511146731', 'EXCHANGE': 'D', 'TRF': 'Q', 'SOURCE': 'N', 'PRICE': '121.825', 'SIZE': '400', 'FRACTIONAL_SIZE': '400.0', 'AGGRESSOR_SIDE': '', 'TRADE_TYPE': '@', 'TRADE_PERIOD': '-', 'BOOK_TYPE': '9', 'STOP_STOCK': '', 'TTE': '0', 'TRADE_ID': '7539', 'PARTICIPANT_TIME': '2026-08-03T13:57:39.510929042', 'COND': '@', 'TICK_STATUS': '0', 'DELETED_TIME': '1970-01-01T00:00:00.000', 'OMDSEQ': '16'}
{'Time': '2026-08-03T13:58:45.094000000', 'TICK_TIME': '2026-08-03T13:58:44.776292000', 'EXCH_TIME': '2026-08-03T13:57:40.326256430', 'TRF_TIME': '1970-01-01T00:00:00.000000000', 'EXCHANGE': 'P', 'TRF': '', 'SOURCE': 'N', 'PRICE': '121.88', 'SIZE': '16', 'FRACTIONAL_SIZE': '16.0', 'AGGRESSOR_SIDE': '', 'TRADE_TYPE': '@F I', 'TRADE_PERIOD': '-', 'BOOK_TYPE': '0', 'STOP_STOCK': '', 'TTE': '1', 'TRADE_ID': '4139', 'PARTICIPANT_TIME': '2026-08-03T13:57:40.326256430', 'COND': '@F I', 'TICK_STATUS': '0', 'DELETED_TIME': '1970-01-01T00:00:00.000', 'OMDSEQ': '9'}

[Complete] Streaming ended. Total ticks received: 9
```
