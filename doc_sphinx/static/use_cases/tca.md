---
file_format: mystnb
---

# TCA (Trade Cost Analysis)

This section contains 18 examples for TCA using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## CSV Load for Order Msgs

Load CSV with Trade Data and aggregate in OneTick.  
Data supplied in CSV format with Time, ID and Symbol.  
When the trade data is combined with market data it becomes more valuable.

```{code-cell} ipython3

import onetick.py as otp

# CSV with TIMESTAMP field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,STATE,ID,SIDE,PRICE,ORIG_QTY,LEAVES_QTY,FILL_QTY,FILL_PRICE
2024-01-03 14:33:00.000,N,1,BUY,50.05,100,100,0,nan
2024-01-03 14:33:02.000,PF,1,BUY,50.05,100,50,50,50.05
2024-01-03 14:33:32.000,F,1,BUY,50.05,100,0,50,50.05
2024-01-03 14:34:42.000,N,2,SELL,50.30,100,0,0,nan
2024-01-03 14:34:52.000,C,2,SELL,50.30,100,0,0,nan
2024-01-03 14:36:00.000,N,3,BUY,49.98,100,100,0,nan
2024-01-03 14:36:02.000,PF,3,BUY,49.98,100,80,20,49.97
2024-01-03 14:36:03.000,PF,3,BUY,49.98,100,60,20,49.97
2024-01-03 14:36:03.250,PF,3,BUY,49.98,100,40,20,49.97
2024-01-03 14:36:03.260,PF,3,BUY,49.98,100,20,20,49.97
2024-01-03 14:36:04.000,PF,3,BUY,49.98,100,10,10,49.97
2024-01-03 14:36:12.000,F,3,BUY,49.98,100,0,10,49.95
"""

# Load CSV using otp.CSV
orders = otp.CSV(
    file_contents=csv_input,
    timestamp_name='TIME',
    converters={'TIME': lambda c: c.apply(otp.nsectime)},
)

# Add Buy and Sell Qty Fields
orders['BUY_QTY'] = orders.if_else(orders['SIDE'] == 'BUY', orders['ORIG_QTY'], 0)
orders['SELL_QTY'] = orders.if_else(orders['SIDE'] == 'SELL', orders['ORIG_QTY'], 0)

# Aggregate order msgs on Order ID field
orders = orders.agg(
    {
        'MSG_COUNT': otp.agg.count(),
        'ORDER_QTY': otp.agg.first('ORIG_QTY'),
        'FILL_QTY': otp.agg.sum('FILL_QTY'),
        'BUY_QTY': otp.agg.first('BUY_QTY'),
        'SELL_QTY': otp.agg.first('SELL_QTY'),
    },
    group_by='ID'
)

# Execute query, and return aggregated orders
result = otp.run(
    orders,
    start=otp.dt(2024, 1, 3),
    end=otp.dt(2024, 1, 4),
    symbols='US_COMP_SAMPLE::AAPL',
    timezone='America/New_York'
)
result
```

## Prevailing Prices for Trade Set

Retrieve Prevailing NBBO from supplied symbols and timestamps.  
Data supplied in CSV format with Time, ID and Symbol.

```{code-cell} ipython3

import onetick.py as otp

otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# CSV with TIME field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,ID,SYMBOL_NAME
2024-01-03 14:33:02.000,1,AAPL
2024-01-03 14:33:32.000,2,CSCO
2024-01-03 14:36:02.000,3,MSFT
2024-01-03 14:36:03.000,4,MSFT
2024-01-03 14:36:03.250,5,AA
2024-01-03 14:36:03.260,6,ZION
2024-01-03 14:36:04.000,7,AMZN
2024-01-03 14:36:12.000,8,AA
"""

# Load CSV into Sym List
sym_list = otp.CSV(
    file_contents=csv_input,
    timestamp_name='TIME',
    converters={'TIME': lambda c: c.apply(otp.nsectime)},
)

# Set the start time and end time of each sub query
sym_list['_PARAM_START_TIME'] = sym_list['TIMESTAMP']
sym_list['_PARAM_END_TIME'] = sym_list['TIMESTAMP']

# Retrieve Bid and Ask from NBBO, looking back up to 1 day for prevailing NBBO
nbbo = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO', back_to_first_tick=86400)
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE']]
# Expose the ID parameter from the symbol list
nbbo['ID'] = nbbo.Symbol['ID', str]

# Retrieve NBBO for Each Symbol and Prevailing Time
merged_query = otp.merge(nbbo, symbols=sym_list, identify_input_ts=True)

result = otp.run(merged_query,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='America/New_York')
result
```

## Arrival Mid Price for an Order

Retrieve order messages (new orders) and join with prevailing quotes from ``US_COMP_SAMPLE``.  
Uses ``otp.CSV`` to load order data from CSV format, filters for new orders (*STATE='N'*).  
Joins with ``US_COMP_SAMPLE`` quote data to calculate arrival mid price (mid-point of best bid/ask).  
Returns order ID and arrival mid price for TCA (Trade Cost Analysis).

```{code-cell} ipython3

import onetick.py as otp

otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# CSV with TIME field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,STATE,ID,SIDE,PRICE,ORIG_QTY,LEAVES_QTY,FILL_QTY,FILL_PRICE
2024-01-03 14:33:00.000,N,1,BUY,50.05,100,100,0,nan
2024-01-03 14:33:02.000,PF,1,BUY,50.05,100,50,50,50.05
2024-01-03 14:33:32.000,F,1,BUY,50.05,100,0,50,50.05
2024-01-03 14:34:42.000,N,2,SELL,50.30,100,0,0,nan
2024-01-03 14:34:52.000,C,2,SELL,50.30,100,0,0,nan
2024-01-03 14:36:00.000,N,3,BUY,49.98,100,100,0,nan
2024-01-03 14:36:02.000,PF,3,BUY,49.98,100,80,20,49.97
2024-01-03 14:36:03.000,PF,3,BUY,49.98,100,60,20,49.97
2024-01-03 14:36:03.250,PF,3,BUY,49.98,100,40,20,49.97
2024-01-03 14:36:03.260,PF,3,BUY,49.98,100,20,20,49.97
2024-01-03 14:36:04.000,PF,3,BUY,49.98,100,10,10,49.97
2024-01-03 14:36:12.000,F,3,BUY,49.98,100,0,10,49.95
"""

# Load CSV as orders
orders = otp.CSV(
    file_contents=csv_input,
    timestamp_name='TIME',
    converters={'TIME': lambda c: c.apply(otp.nsectime)},
)

# Filter for new orders (STATE == 'N')
new_orders, _ = orders[orders['STATE'] == 'N']

# Retrieve prevailing NBBO prices
quotes = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO')

# Join orders to prevailing quotes at order arrival
joined = otp.join_by_time([new_orders, quotes])

# Add mid price
joined['ARRIVAL_MID_PRICE'] = (joined['ASK_PRICE'] + joined['BID_PRICE']) / 2

# Limit returned fields to Order Id and Arrival Mid Price
data = joined[['ID', 'ARRIVAL_MID_PRICE']]

# Run the query for the relevant interval (covering all order times)
result = otp.run(
    data,
    symbols='CSCO',
    start=otp.dt(2024, 1, 3),
    end=otp.dt(2024, 1, 4),
    timezone='America/New_York'
)

result
```

### Arrival Mid Price for an Order LSE

Retrieve order messages (new orders) and join with prevailing quotes from ``LSE_SAMPLE``.  
Uses ``otp.CSV`` to load order data from CSV format, filters for new orders (*STATE='N'*).  
Joins with ``LSE_SAMPLE`` quote data to calculate arrival mid price (mid-point of best bid/ask).  
Returns order ID and arrival mid price for TCA (Trade Cost Analysis).

```{code-cell} ipython3

import onetick.py as otp

otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# CSV with TIME field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,STATE,ID,SIDE,PRICE,ORIG_QTY,LEAVES_QTY,FILL_QTY,FILL_PRICE
2024-01-03 14:33:00.000,N,1,BUY,50.05,100,100,0,nan
2024-01-03 14:33:02.000,PF,1,BUY,50.05,100,50,50,50.05
2024-01-03 14:33:32.000,F,1,BUY,50.05,100,0,50,50.05
2024-01-03 14:34:42.000,N,2,SELL,50.30,100,0,0,nan
2024-01-03 14:34:52.000,C,2,SELL,50.30,100,0,0,nan
2024-01-03 14:36:00.000,N,3,BUY,49.98,100,100,0,nan
2024-01-03 14:36:02.000,PF,3,BUY,49.98,100,80,20,49.97
2024-01-03 14:36:03.000,PF,3,BUY,49.98,100,60,20,49.97
2024-01-03 14:36:03.250,PF,3,BUY,49.98,100,40,20,49.97
2024-01-03 14:36:03.260,PF,3,BUY,49.98,100,20,20,49.97
2024-01-03 14:36:04.000,PF,3,BUY,49.98,100,10,10,49.97
2024-01-03 14:36:12.000,F,3,BUY,49.98,100,0,10,49.95
"""

# Load CSV as orders
orders = otp.CSV(
    file_contents=csv_input,
    timestamp_name='TIME',
    converters={'TIME': lambda c: c.apply(otp.nsectime)},
)

# Filter for new orders (STATE == 'N')
new_orders, _ = orders[orders['STATE'] == 'N']

# Retrieve prevailing quotes
quotes = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')

# Join orders to prevailing quotes at order arrival
joined = otp.join_by_time([new_orders, quotes])

# Add mid price
joined['ARRIVAL_MID_PRICE'] = (joined['ASK_PRICE'] + joined['BID_PRICE']) / 2

# Limit returned fields to Order Id and Arrival Mid Price
data = joined[['ID', 'ARRIVAL_MID_PRICE']]

# Run the query for the relevant interval (covering all order times)
result = otp.run(
    data,
    symbols='VOD',
    start=otp.dt(2024, 1, 3),
    end=otp.dt(2024, 1, 4),
    timezone='Europe/London'
)

result
```

## Load DataFrame with Prevailing Prices

Load symbol list from DataFrame and retrieve prevailing prices.  
Uses pandas DataFrame to load symbol list with timestamps.  
Joins symbol list with prevailing NBBO prices using parameter-based merging.  
Returns symbol, ID, and bid/ask prices at each timestamp.

```{code-cell} ipython3

import onetick.py as otp
import pandas as pd
from io import StringIO

otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# CSV with TIME field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,ID,SYMBOL_NAME
2024-01-03 14:33:02.000,1,AAPL
2024-01-03 14:33:32.000,2,CSCO
2024-01-03 14:36:02.000,3,MSFT
2024-01-03 14:36:03.000,4,MSFT
2024-01-03 14:36:03.250,5,AA
2024-01-03 14:36:03.260,6,ZION
2024-01-03 14:36:04.000,7,AMZN
2024-01-03 14:36:12.000,8,AA
"""

df = pd.read_csv(StringIO(csv_input), parse_dates=['TIME'])

# Load symbol list from DataFrame with timestamps
sym_list = otp.LoadTicksFromDataFrame(df)

# Set the start time and end time of each sub query
sym_list['_PARAM_START_TIME'] = sym_list['TIMESTAMP']
sym_list['_PARAM_END_TIME'] = sym_list['TIMESTAMP']

nbbo = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO', back_to_first_tick=86400)
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE']]
# Expose the ID parameter from the symbol list
nbbo['ID'] = nbbo.Symbol['ID', str]

merged_query = otp.merge(nbbo, symbols=sym_list, identify_input_ts=True)

result = otp.run(
    merged_query,
    start=otp.dt(2024, 1, 3),
    end=otp.dt(2024, 1, 4),
    timezone='America/New_York',
)

result
```

## Prevailing Prices for Trade Set with Offsets

Retrieve Prevailing NBBO with Offsets from supplied symbols and timestamps.  
Data supplied in CSV format with Time, ID and Symbol.  
Set of Offsets supplied in Milliseconds both backwards and forwards looking.

```{code-cell} ipython3

import onetick.py as otp

otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# CSV with TIME field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,ID,SYMBOL_NAME
2024-01-03 14:33:02.000,1,AAPL
2024-01-03 14:33:32.000,2,CSCO
2024-01-03 14:36:02.000,3,MSFT
2024-01-03 14:36:03.000,4,MSFT
2024-01-03 14:36:03.250,5,AA
2024-01-03 14:36:03.260,6,ZION
2024-01-03 14:36:04.000,7,AMZN
2024-01-03 14:36:12.000,8,AA
"""

# Load CSV into Sym List
sym_list = otp.CSV(
    file_contents=csv_input,
    timestamp_name='TIME',
    converters={'TIME': lambda c: c.apply(otp.nsectime)},
)

sym_list['OFFSET'] = 0
sym_list['_PARAM_START_TIME'] = sym_list['TIMESTAMP']

# define Offsets
def add_offsets(tick):
    for offset in [-1000,-100, -10, -1, 1, 100, 1000,5000,10000,30000,60000]:
        yield
        tick['OFFSET'] = offset
        tick['_PARAM_START_TIME'] = tick['TIMESTAMP'] + offset
    return True

# Apply the offsets
sym_list = sym_list.script(add_offsets)

# Set the end time of each sub query to the start time
sym_list['_PARAM_END_TIME'] = sym_list['_PARAM_START_TIME']

nbbo = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO', back_to_first_tick=86400)
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE']]
nbbo['ID'] = nbbo.Symbol['ID', str]          # Expose the ID parameter from the symbol list
nbbo['OFFSET'] = nbbo.Symbol['OFFSET', int]  # Expose the OFFSET parameter from the symbol list

# Retrieve NBBO for Each Symbol and Prevailing Time
merged_query = otp.merge(nbbo, symbols=sym_list, identify_input_ts=True)

result = otp.run(merged_query,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='America/New_York')
result
```

## Combined Prevailing Trades Based on Supplied Dataframe

Retrieve Combined Set of Prevailing Trades based on Input Dataframe with Symbols and Timestamps.

```{code-cell} ipython3

import onetick.py as otp
import pandas as pd
from datetime import timedelta
from io import StringIO

# Default Symbols and Database are defined to load data using otp.LoadTicksFromDataFrame
otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# To Demonstrate the use of a Dataframe a CSV is loaded with TIME, ID and SYMBOL_NAME fields.
# CSV with TIMESTAMP field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,ID,SYMBOL_NAME
2024-01-03 14:33:02.000,1,AAPL
2024-01-03 14:33:32.000,2,CSCO
2024-01-03 14:36:02.000,3,MSFT
2024-01-03 14:36:03.000,4,MSFT
2024-01-03 14:36:03.250,5,AMZN
2024-01-03 14:36:03.260,6,HD
2024-01-03 14:36:04.000,7,AMZN
2024-01-03 14:36:12.000,8,AMZN
"""

# Loaded into the Dataframe using the pandas read_csv method.
df = pd.read_csv(StringIO(csv_input), parse_dates=['TIME'])

# The Dataframe is updated to include two specifial fields:
# _PARAM_START_TIME - The Start Time of the sub query specific to this symbol
# _PARAM_END_TIME - The End Time of the sub query specific to this symbol

df['_PARAM_START_TIME'] = df['TIME']
df['_PARAM_END_TIME'] = df['TIME']
df = df[['TIME', 'SYMBOL_NAME', 'ID', '_PARAM_START_TIME', '_PARAM_END_TIME']]

# Calculate the Start and End Time for the parent query based on the Dataframe Time Range
start_time = min(df['TIME'])
end_time = max(df['TIME']) + timedelta(seconds=1)

# Convert Dataframe into the Symbol List
sym_list = otp.LoadTicksFromDataFrame(df)

# The DataSource is defined with the Database and Tick Type specified
# As there may not be prevailng trades at the specified timestamp a lookback period is defined using "back_to_first_tick"
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', back_to_first_tick=86400)
data = data[['PRICE', 'SIZE', 'EXCHANGE']]

# Fields are added from the supplied Dataframe to each output
data['SYMBOL'] = data.Symbol.name
data['ID'] = data.Symbol['ID', str]

# Merge the results across the input symbols, binding symbols
merged_data = otp.merge([data], symbols=sym_list, identify_input_ts=True)

# Run Query setting symbols equal to the dataframe
# Producing a dictionary of output results dataframes, one per input
result = otp.run(merged_data,
                 start=start_time,
                 end=end_time,
                 timezone='America/New_York')
result
```

## Prevailing Trades Based on Supplied Dataframe

Retrieve Prevailing Trades based on Input Dataframe with Symbols and Timestamps.

```{code-cell} ipython3

import onetick.py as otp
import pandas as pd
from io import StringIO

# To Demonstrate the use of a Dataframe a CSV is loaded with TIME, ID and SYMBOL_NAME fields.
# CSV with TIMESTAMP field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,ID,SYMBOL_NAME
2024-01-03 14:33:02.000,1,AAPL
2024-01-03 14:33:32.000,2,CSCO
2024-01-03 14:36:02.000,3,MSFT
2024-01-03 14:36:03.000,4,MSFT
2024-01-03 14:36:03.250,5,AMZN
2024-01-03 14:36:03.260,6,HD
2024-01-03 14:36:04.000,7,AMZN
2024-01-03 14:36:12.000,8,AMZN
"""

# Loaded into the Dataframe using the pandas read_csv method.
df = pd.read_csv(StringIO(csv_input), parse_dates=['TIME'])

# The Dataframe is updated to include two specifial fields:
# _PARAM_START_TIME - The Start Time of the sub query specific to this symbol
# _PARAM_END_TIME - The End Time of the sub query specific to this symbol
# As there are multiple records for the same symbol an ID column is also defined.

df['_PARAM_START_TIME'] = df['TIME']
df['_PARAM_END_TIME'] = df['TIME']
df = df[['SYMBOL_NAME', 'ID', '_PARAM_START_TIME', '_PARAM_END_TIME']]

# The DataSource is defined with the Database and Tick Type specified
# As there may not be prevailng trades at the specified timestamp a lookback period is defined using "back_to_first_tick"
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', back_to_first_tick=86400)
data = data[['PRICE', 'SIZE']]

# Fields are added from the supplied Dataframe to each output
data['SYMBOL'] = data.Symbol.name
data['ID'] = data.Symbol['ID', str]

# Run Query setting symbols equal to the dataframe
# Producing a dictionary of output results dataframes, one per input symbol
result = otp.run(
    data,
    start=otp.dt(2024, 1, 3),
    end=otp.dt(2024, 1, 4),
    symbols=df,
    timezone='America/New_York'
)
result
```

## Combined Prevailing Trades & NBBO Based on Supplied Dataframe

Retrieve Combined Set of Prevailing Trades and NBBO Quotes based on Input Dataframe with Symbols and Timestamps.

```{code-cell} ipython3

import onetick.py as otp
import pandas as pd
from datetime import timedelta
from io import StringIO

otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# CSV with TIMESTAMP field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,ID,SYMBOL_NAME
2024-01-03 14:33:02.000,1,AAPL
2024-01-03 14:33:32.000,2,CSCO
2024-01-03 14:36:02.000,3,MSFT
2024-01-03 14:36:03.000,4,MSFT
2024-01-03 14:36:03.250,5,AMZN
2024-01-03 14:36:03.260,6,HD
2024-01-03 14:36:04.000,7,AMZN
2024-01-03 14:36:12.000,8,AMZN
"""
df = pd.read_csv(StringIO(csv_input), parse_dates=['TIME'])

# The Dataframe is updated to include two specifial fields:
# _PARAM_START_TIME - The Start Time of the sub query specific to this symbol
# _PARAM_END_TIME - The End Time of the sub query specific to this symbol
df['_PARAM_START_TIME'] = df['TIME']
df['_PARAM_END_TIME'] = df['TIME']
df = df[['TIME', 'ID', 'SYMBOL_NAME', '_PARAM_START_TIME', '_PARAM_END_TIME']]

# Calculate the Start and End Time for the parent query based on the Dataframe Time Range
start_time = min(df['TIME'])
end_time = max(df['TIME']) + timedelta(seconds=1)

# Convert Dataframe into the Symbol List
sym_list = otp.LoadTicksFromDataFrame(df)

# Define Data Source, in this case for Trade records, to retrieve the prevailing Trade.
# As there may not be prevailng trades at the specified timestamp a lookback period is defined using "back_to_first_tick"
trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', back_to_first_tick=86400)
trd = trd[['PRICE', 'SIZE', 'EXCHANGE']]

# Fields are added from the supplied Dataframe
trd['ID'] = trd.Symbol['ID', str]

# Define Data Source, in this case for NBBO records, to retrieve the prevailing NBBO quote.
# As there may not be prevailng NBBO quotes at the specified timestamp a lookback period is defined using "back_to_first_tick"
nbbo = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO', back_to_first_tick=86400)
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE', 'BID_EXCHANGE', 'ASK_EXCHANGE']]

# Join the Trades & NBBO together.
joined = otp.join_by_time(sources=[trd, nbbo], match_if_identical_times=True)

# Merge data into a single result across symbols
merged_data = otp.merge(joined, symbols=sym_list, identify_input_ts=True)

# Run query
result = otp.run(merged_data,
                 start=start_time,
                 end=end_time,
                 timezone='America/New_York')
result
```

## Combined European Composite Prevailing Trades & NBBO Based on Supplied Dataframe of Symbols

Retrieve Combined Set of European Composite Prevailing Trades and NBBO
based on Input Dataframe with Symbols and Timestamps.

```{code-cell} ipython3

import onetick.py as otp
import pandas as pd
from datetime import timedelta
from io import StringIO

# Default Symbols and Database are defined to load data using otp.LoadTicksFromDataFrame
otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# Input Symbols for the European Composite are ISINs, as the tickers for each venue can be different.
# To Demonstrate the use of a Dataframe a CSV is loaded with TIME, ID and SYMBOL_NAME fields.

# CSV with TIMESTAMP field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,ID,SYMBOL_NAME
2024-01-03 14:33:02.000,1,GB00BH4HKS39
2024-01-03 14:33:32.000,2,FR001400J770
2024-01-03 14:36:02.000,3,DE0005140008
2024-01-03 14:36:03.000,4,GB00BP6MXD84
2024-01-03 14:36:03.250,5,GB00BVZK7T90
2024-01-03 14:36:03.260,6,FR0000120578
2024-01-03 14:36:04.000,7,GB00BH4HKS39
2024-01-03 14:36:12.000,8,DE000ENAG999
"""
df = pd.read_csv(StringIO(csv_input), parse_dates=['TIME'])

# The Dataframe is updated to include two specifial fields:
# _PARAM_START_TIME - The Start Time of the sub query specific to this symbol
# _PARAM_END_TIME - The End Time of the sub query specific to this symbol
df['_PARAM_START_TIME'] = df['TIME']
df['_PARAM_END_TIME'] = df['TIME']
df = df[['TIME', 'SYMBOL_NAME', 'ID', '_PARAM_START_TIME', '_PARAM_END_TIME']]

# Calculate the Start and End Time for the parent query based on the Dataframe Time Range
start_time = min(df['TIME'])
end_time = max(df['TIME']) + timedelta(seconds=1)

# Convert Dataframe into the Symbol List
sym_list = otp.LoadTicksFromDataFrame(df)

# Define Data Source, in this case for Trade records, to retrieve the prevailing Trade.
# As there may not be prevailng trades at the specified timestamp a lookback period is defined using "back_to_first_tick"
trd = otp.DataSource(db='EU_COMP', tick_type='TRD', back_to_first_tick=86400)
trd = trd[['PRICE', 'SIZE', 'TRADE_VENUE']]

# Fields are added from the supplied Dataframe
trd['ID'] = trd.Symbol['ID', str]

# Define Data Source, in this case for NBBO records, to retrieve the prevailing NBBO quote.
# As there may not be prevailng NBBO quotes at the specified timestamp a lookback period is defined using "back_to_first_tick"
nbbo = otp.DataSource(db='EU_COMP', tick_type='NBBO', back_to_first_tick=86400)
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE', 'BID_EXCHANGE', 'ASK_EXCHANGE']]

# Join the Trades & NBBO together.
joined = otp.join_by_time(sources=[trd, nbbo], match_if_identical_times=True)

# Merge data into a single result across symbols
merged_data = otp.merge(joined, symbols=sym_list, identify_input_ts=True)

# Run query
result = otp.run(merged_data,
                 start=start_time,
                 end=end_time,
                 timezone='Europe/London')
result
```

## Combined Trade Statistics Based on Supplied Dataframe of Database & Symbols

Retrieve Combined Set of Period Trade Statistics based on Input Dataframe with Databases, Symbols and Time Ranges.

```{code-cell} ipython3

import onetick.py as otp
import pandas as pd
from datetime import timedelta
from io import StringIO

# Default Symbols and Database are defined to load data using otp.LoadTicksFromDataFrame
otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# To Demonstrate the use of a Dataframe a CSV is loaded with START_TIME, END_TIME, ID and SYMBOL_NAME fields.
# The SYMBOL_NAME field includes both Database and Symbol
# using the syntax [DB Name]::[Ticker Symbol]
# A time window is defined for each record through START_TIME and END_TIME

# CSV with TIMESTAMP field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """START_TIME,END_TIME,ID,SYMBOL_NAME
2024-01-03 14:33:02.000,2024-01-03 15:23:02.000,1,LSE::VOD
2024-01-03 14:33:32.000,2024-01-03 15:33:32.000,2,EURONEXT::AF
2024-01-03 14:36:02.000,2024-01-03 15:46:02.000,3,XETRA::DBK
2024-01-03 14:36:03.000,2024-01-03 15:26:03.000,4,LSE::TSCO
2024-01-03 14:36:03.250,2024-01-03 15:36:03.250,5,LSE::SHEL
2024-01-03 14:36:03.260,2024-01-03 15:46:03.260,6,EURONEXT::AF
2024-01-03 14:36:04.000,2024-01-03 15:16:04.000,7,LSE::VOD
2024-01-03 14:36:12.000,2024-01-03 15:26:12.000,8,XETRA::DBK
"""

# Loaded into the Dataframe using the pandas read_csv method.
df = pd.read_csv(StringIO(csv_input))

# The Dataframe is updated to include two specifial fields:
# _PARAM_START_TIME - The Start Time of the sub query specific to this symbol
# _PARAM_END_TIME - The End Time of the sub query specific to this symbol

df['_PARAM_START_TIME'] = pd.to_datetime(df['START_TIME'])
df['_PARAM_END_TIME'] = pd.to_datetime(df['END_TIME'])
df = df[['ID', 'SYMBOL_NAME', '_PARAM_START_TIME', '_PARAM_END_TIME']]

# Calculate the Start and End Time for the parent query based on the Dataframe Time Range
start_time = min(df['_PARAM_START_TIME'])
end_time = max(df['_PARAM_END_TIME']) + timedelta(seconds=1)

# Convert Dataframe into the Symbol List
sym_list = otp.LoadTicksFromDataFrame(df)

# The Trade DataSource is defined with Tick Type specified, but without the Database
# As there may not be prevailng trades at the specified timestamp a lookback period is defined using "back_to_first_tick"
# As the Database is not specified, the schema policy is set to "manual", and the schema is defined
trd = otp.DataSource(tick_type='TRD', back_to_first_tick=86400, schema_policy='manual')
# Define the output schema
trd.schema.set(
    PRICE=float,
    SIZE=int,
    TRADE_VENUE=str,
    BOOK_TYPE=str,
    TRADE_PERIOD=str
    )
trd = trd[['PRICE', 'SIZE', 'TRADE_VENUE', 'BOOK_TYPE', 'TRADE_PERIOD']]

# Filter on Lit Order Book
trd = trd.where(trd['BOOK_TYPE'] == '0')

# Filter on Continuous Tradng
trd = trd.where(trd['TRADE_PERIOD'] == '-')

# Aggregates All Trades
data = trd.agg({
    'FIRST_PRICE': otp.agg.first('PRICE'),
    'HIGH_PRICE': otp.agg.max('PRICE'),
    'LOW_PRICE': otp.agg.min('PRICE'),
    'LAST_PRICE': otp.agg.last('PRICE'),
    'VWAP_PRICE': otp.agg.vwap('PRICE', 'SIZE'),
    'SUM_SIZE': otp.agg.sum('SIZE'),
    'TRADE_COUNT': otp.agg.count()
})

# Create a single output, merging all the inputs into a single resultset.
merged = otp.merge([data], symbols=sym_list, identify_input_ts=True, separate_db_name=True)

# Run the query returning the data in the selected timezone
result = otp.run(merged,
                 start=start_time,
                 end=end_time,
                 timezone='Europe/London')
result
```

## Combined Prevailing Trades & NBBO Based on Supplied Dataframe of ISIN Symbology, Database & Symbols

Retrieve Combined Set of Prevailing Trades and NBBO Quotes based on Input Dataframe with Symbols and Timestamps.

```{code-cell} ipython3

import onetick.py as otp
import pandas as pd
from datetime import timedelta
from io import StringIO

otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# CSV with TIMESTAMP field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,ID,SYMBOL_NAME
2024-01-03 14:33:02.000,1,AAPL
2024-01-03 14:33:32.000,2,CSCO
2024-01-03 14:36:02.000,3,MSFT
2024-01-03 14:36:03.000,4,MSFT
2024-01-03 14:36:03.250,5,AMZN
2024-01-03 14:36:03.260,6,HD
2024-01-03 14:36:04.000,7,AMZN
2024-01-03 14:36:12.000,8,AMZN
"""
df = pd.read_csv(StringIO(csv_input))
df['Time'] = pd.to_datetime(df['TIME'])

# The Dataframe is updated to include two specifial fields:
# _PARAM_START_TIME - The Start Time of the sub query specific to this symbol
# _PARAM_END_TIME - The End Time of the sub query specific to this symbol
df['_PARAM_START_TIME'] = df['Time']
df['_PARAM_END_TIME'] = df['Time']
df = df[['Time', 'ID', 'SYMBOL_NAME', '_PARAM_START_TIME', '_PARAM_END_TIME']]

# Calculate the Start and End Time for the parent query based on the Dataframe Time Range
start_time = min(df['Time'])
end_time = max(df['Time']) + timedelta(seconds=1)

# Convert Dataframe into the Symbol List
sym_list = otp.LoadTicksFromDataFrame(df)

# Define Data Source, in this case for Trade records, to retrieve the prevailing Trade.
# As there may not be prevailng trades at the specified timestamp a lookback period is defined using "back_to_first_tick"
trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', back_to_first_tick=86400)
trd = trd[['PRICE', 'SIZE', 'EXCHANGE']]

# Fields are added from the supplied Dataframe
trd['ID'] = trd.Symbol['ID', str]

# Define Data Source, in this case for NBBO records, to retrieve the prevailing NBBO quote.
# As there may not be prevailng NBBO quotes at the specified timestamp a lookback period is defined using "back_to_first_tick"
nbbo = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO', back_to_first_tick=86400)
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE', 'BID_EXCHANGE', 'ASK_EXCHANGE']]

# Join the Trades & NBBO together.
joined = otp.join_by_time(sources=[trd, nbbo], match_if_identical_times=True)

# Merge data into a single result across symbols
merged_data = otp.merge(joined, symbols=sym_list, identify_input_ts=True)

# Run query
result = otp.run(merged_data,
                 start=start_time,
                 end=end_time,
                 timezone='America/New_York')
result
```

## Combined Prevailing Trades & NBBO Based on Supplied Dataframe of Bloomberg Symbology, Database & Symbols

Retrieve Combined Set of Period Trade Statistics based on Input Dataframe
with Bloomberg Symbology, Databases, Symbols and Time Ranges.

```{code-cell} ipython3

import onetick.py as otp
import pandas as pd
from datetime import timedelta
from io import StringIO

# Default Symbols and Database are defined to load data using otp.LoadTicksFromDataFrame
otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# To Demonstrate the use of a Dataframe a CSV is loaded with START_TIME, END_TIME, ID and SYMBOL_NAME fields.
# The SYMBOL_NAME field includes both Symbology, Database and Symbol
# using the syntax [Symbology]::[DB Name]::[Ticker Symbol]
# BSYM is used to specify the Bloomberg Symbology
# A time window is defined for each record through START_TIME and END_TIME

# CSV with TIMESTAMP field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """START_TIME,END_TIME,ID,SYMBOL_NAME
2024-01-03 14:33:02.000,2024-01-03 15:23:02.000,1,BSYM::LSE::VOD LN Equity
2024-01-03 14:33:32.000,2024-01-03 15:33:32.000,2,BSYM::EURONEXT::AF FP Equity
2024-01-03 14:36:02.000,2024-01-03 15:46:02.000,3,BSYM::XETRA::DBK GY Equity
2024-01-03 14:36:03.000,2024-01-03 15:26:03.000,4,BSYM::LSE::TSCO LN Equity
2024-01-03 14:36:03.250,2024-01-03 15:36:03.250,5,BSYM::LSE::SHEL LN Equity
2024-01-03 14:36:03.260,2024-01-03 15:46:03.260,6,BSYM::EURONEXT::AF FP Equity
2024-01-03 14:36:04.000,2024-01-03 15:16:04.000,7,BSYM::LSE::VOD LN Equity
2024-01-03 14:36:12.000,2024-01-03 15:26:12.000,8,BSYM::XETRA::DBK GY Equity
"""

# Loaded into the Dataframe using the pandas read_csv method.
df = pd.read_csv(StringIO(csv_input))

# The Dataframe is updated to include two specifial fields:
# _PARAM_START_TIME - The Start Time of the sub query specific to this symbol
# _PARAM_END_TIME - The End Time of the sub query specific to this symbol
df['_PARAM_START_TIME'] = pd.to_datetime(df['START_TIME'])
df['_PARAM_END_TIME'] = pd.to_datetime(df['END_TIME'])
df = df[['ID', 'SYMBOL_NAME', '_PARAM_START_TIME', '_PARAM_END_TIME']]

# Calculate the Start and End Time for the parent query based on the Dataframe Time Range
start_time = min(df['_PARAM_START_TIME'])
end_time = max(df['_PARAM_END_TIME']) + timedelta(seconds=1)

# Convert Dataframe into the Symbol List
sym_list = otp.LoadTicksFromDataFrame(df)

# The Trade DataSource is defined with Tick Type specified, but without the Database
# As there may not be prevailng trades at the specified timestamp a lookback period is defined using "back_to_first_tick"
# As the Database is not specified, the schema policy is set to "manual", and the schema is defined
trd = otp.DataSource(tick_type='TRD', back_to_first_tick=86400, schema_policy='manual')
# Define the output schema
trd.schema.set(
    PRICE=float,
    SIZE=int,
    TRADE_VENUE=str,
    BOOK_TYPE=str,
    TRADE_PERIOD=str
)
trd = trd[['PRICE', 'SIZE', 'TRADE_VENUE', 'BOOK_TYPE', 'TRADE_PERIOD']]

# Filter on Lit Order Book
trd = trd.where(trd['BOOK_TYPE'] == '0')

# Filter on Continuous Tradng
trd = trd.where(trd['TRADE_PERIOD'] == '-')

# Aggregates All Trades
data = trd.agg({
    'FIRST_PRICE': otp.agg.first('PRICE'),
    'HIGH_PRICE': otp.agg.max('PRICE'),
    'LOW_PRICE': otp.agg.min('PRICE'),
    'LAST_PRICE': otp.agg.last('PRICE'),
    'VWAP_PRICE': otp.agg.vwap('PRICE', 'SIZE'),
    'SUM_SIZE': otp.agg.sum('SIZE'),
    'TRADE_COUNT': otp.agg.count()
})

# Add the Database Symbol
data = data.show_symbol_name_in_db()

# Create a single output, merging all the inputs into a single resultset.
merged = otp.merge([data], symbols=sym_list, identify_input_ts=True, separate_db_name=True)

# Run the query returning the data in the selected timezone
result = otp.run(merged,
                 start=start_time,
                 end=end_time,
                 timezone='Europe/London',
                 # Specifying symbol_date to perform the symbology lookup
                 symbol_date=end_time)
result
```

## Combined European Composite Trade Statistics for Composite and Primary based on Supplied Dataframe

Retrieve Combined Set of EU Composite and Primary Period Trade Statistics
based on Input Dataframe with Symbols, Primary MIC and Time Ranges.  
Period Statistics are calculated both across the European Composite, and across the specified Primary venue.  
Both Sets of Statistics are joined, and returned as a single results set.

```{code-cell} ipython3

import onetick.py as otp
import pandas as pd
from datetime import timedelta
from io import StringIO

# Default Symbols and Database are defined to load data using otp.LoadTicksFromDataFrame
otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# To Demonstrate the use of a Dataframe a CSV is loaded with START_TIME, END_TIME, ID, SYMBOL_NAME and PMIC fields.
# The PMIC field represents the MIC of the specified primary venue
# A time window is defined for each record through START_TIME and END_TIME

# CSV with TIMESTAMP field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """START_TIME,END_TIME,ID,SYMBOL_NAME,PMIC
2024-01-03 14:33:02.000,2024-01-03 15:23:02.000,1,GB00BH4HKS39,XLON
2024-01-03 14:33:32.000,2024-01-03 15:33:32.000,2,FR001400J770,XPAR
2024-01-03 14:36:02.000,2024-01-03 15:46:02.000,3,DE0005140008,XETA
2024-01-03 14:36:03.000,2024-01-03 15:26:03.000,4,GB00BP6MXD84,XLON
2024-01-03 14:36:03.250,2024-01-03 15:36:03.250,5,GB00BLGZ9862,XLON
2024-01-03 14:36:03.260,2024-01-03 15:46:03.260,6,FR0000120578,XPAR
2024-01-03 14:36:04.000,2024-01-03 15:16:04.000,7,GB00BH4HKS39,XLON
2024-01-03 14:36:12.000,2024-01-03 15:26:12.000,8,DE000ENAG999,XETA
"""

# Loaded into the Dataframe using the pandas read_csv method.
df = pd.read_csv(StringIO(csv_input))

# The Dataframe is updated to include two specifial fields:
# _PARAM_START_TIME - The Start Time of the sub query specific to this symbol
# _PARAM_END_TIME - The End Time of the sub query specific to this symbol
# Time - Required to allow conversion using otp.LoadTicksFromDataFrame

df['_PARAM_START_TIME'] = pd.to_datetime(df['START_TIME'])
df['_PARAM_END_TIME'] = pd.to_datetime(df['END_TIME'])
df = df[['ID', 'SYMBOL_NAME', '_PARAM_START_TIME', '_PARAM_END_TIME', 'PMIC']]

# Calculate the Start and End Time for the parent query based on the Dataframe Time Range
start_time = min(df['_PARAM_START_TIME'])
end_time = max(df['_PARAM_END_TIME']) + timedelta(seconds=1)

# Convert Dataframe into the Symbol List
sym_list = otp.LoadTicksFromDataFrame(df)

# Define the Trade Data Source for the European Composite
trd = otp.DataSource(db='EU_COMP', tick_type='TRD', back_to_first_tick=86400)
trd = trd[['PRICE', 'SIZE', 'TRADE_VENUE', 'BOOK_TYPE', 'TRADE_PERIOD']]

# Filter on Lit Order Book
trd = trd.where(trd['BOOK_TYPE'] == '0')

# Filter on Continuous Tradng
trd = trd.where(trd['TRADE_PERIOD'] == '-')

# Aggregates All Trades
data = trd.agg({
    'FIRST_PRICE': otp.agg.first('PRICE'),
    'HIGH_PRICE': otp.agg.max('PRICE'),
    'LOW_PRICE': otp.agg.min('PRICE'),
    'LAST_PRICE': otp.agg.last('PRICE'),
    'SUM_SIZE': otp.agg.sum('SIZE'),
    'TRADE_COUNT': otp.agg.count()
})

# Add the Primary MIC associated with each input
trd['PMIC'] = trd.Symbol['PMIC', str]

# Filter on Trades associated with the primary mic
trd_primary = trd.where(trd['TRADE_VENUE'] == trd['PMIC'])

# Aggregate the Trades from the Primary Mic
data_primary = trd_primary.agg({
    'FIRST_PRICE_PRIM': otp.agg.first('PRICE'),
    'HIGH_PRICE_PRIM': otp.agg.max('PRICE'),
    'LOW_PRICE_PRIM': otp.agg.min('PRICE'),
    'LAST_PRICE_PRIM': otp.agg.last('PRICE'),
    'SUM_SIZE_PRIM': otp.agg.sum('SIZE'),
    'TRADE_COUNT_PRIM': otp.agg.count()
})

# Join the aggregates trades across EU_COMP to those filtered to just the Primary
joined = otp.join_by_time([data, data_primary], match_if_identical_times=True)

# Add back the ID and Primary Mic Fields
joined['ID'] = joined.Symbol['ID', str]
joined['PMIC'] = joined.Symbol['PMIC', str]

# Create a single output, merging all the inputs into a single resultset.
merged = otp.merge([joined], symbols=sym_list, identify_input_ts=True, separate_db_name=True)

# Run the query returning the data in the selected timezone
result = otp.run(merged,
                 start=start_time,
                 end=end_time,
                 timezone='Europe/London')
result
```

## Combined European Composite Prevailing Values for Composite and Primary based on Supplied Dataframe

Retrieve Combined Set of EU Composite and Primary Prevailing Trades, Quotes and NBBO
based on Input Dataframe with Symbols, Primary MIC and Time Ranges.  
Prevailing values are retrieved across the European Composite, and across the specified Primary venue.  
Prevailng Trades across the Composite, and for the Primary Venue.  
Prevailing Quotes for the Primary Venue.  
Prevailing NBBO across the Composite.  
Both Sets of Statistics are joined, and returned as a single results set.

```{code-cell} ipython3

import onetick.py as otp
from datetime import timedelta
from io import StringIO

# Default Symbols and Database are defined to load data using otp.LoadTicksFromDataFrame
otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# To Demonstrate the use of a Dataframe a CSV is loaded with TIME, ID, SYMBOL_NAME and PMIC fields.
# The PMIC field represents the MIC of the specified primary venue

# CSV with TIMESTAMP field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,ID,SYMBOL_NAME,PMIC
2024-01-03 14:33:02.000,1,GB00BH4HKS39,XLON
2024-01-03 14:33:32.000,2,FR001400J770,XPAR
2024-01-03 14:36:02.000,3,DE0005140008,XETA
2024-01-03 14:36:03.000,4,GB00BP6MXD84,XLON
2024-01-03 14:36:03.250,5,GB00BVZK7T90,XLON
2024-01-03 14:36:03.260,6,FR0000120578,XPAR
2024-01-03 14:36:04.000,7,GB00BH4HKS39,XLON
2024-01-03 14:36:12.000,8,DE000ENAG999,XETA
"""

# Loaded into the Dataframe using the pandas read_csv method.
df = pd.read_csv(StringIO(csv_input), parse_dates=['TIME'])

# The Dataframe is updated to include two specifial fields:
# _PARAM_START_TIME - The Start Time of the sub query specific to this symbol
# _PARAM_END_TIME - The End Time of the sub query specific to this symbol
# Time - Required to allow conversion using otp.LoadTicksFromDataFrame
df['_PARAM_START_TIME'] = df['TIME']
df['_PARAM_END_TIME'] = df['TIME']
df = df[['TIME', 'SYMBOL_NAME', '_PARAM_START_TIME', '_PARAM_END_TIME', 'PMIC']]

# Calculate the Start and End Time for the parent query based on the Dataframe Time Range
start_time = min(df['TIME'])
end_time = max(df['TIME']) + timedelta(seconds=1)

# Convert Dataframe into the Symbol List
sym_list = otp.LoadTicksFromDataFrame(df)

# Define the Trade Data Source for the European Composite
trd = otp.DataSource(db='EU_COMP', tick_type='TRD', back_to_first_tick=86400)
trd = trd[['PRICE', 'SIZE', 'TRADE_VENUE']]

# Define Primary Trade Data Source, looking back for prevailing values filtered on the specified Primary MIC (PMIC)
trd_primary = otp.DataSource(db='EU_COMP', tick_type='TRD', back_to_first_tick=86400,
                             where_clause_for_back_ticks=otp.raw('TRADE_VENUE=_SYMBOL_PARAM.PMIC', dtype=bool))

trd_primary['PRICE_PRIMARY'] = trd_primary['PRICE']
trd_primary['SIZE_PRIMARY'] = trd_primary['SIZE']
trd_primary['TRADE_VENUE_PRIMARY'] = trd_primary['TRADE_VENUE']
trd_primary = trd_primary[['PRICE_PRIMARY', 'SIZE_PRIMARY', 'TRADE_VENUE_PRIMARY']]

# Define Primary Quote Data Source, looking back for prevailing values filtered on the specified Primary MIC (PMIC)
qte = otp.DataSource(db='EU_COMP', tick_type='QTE', back_to_first_tick=86400,
                     where_clause_for_back_ticks=otp.raw('QUOTE_VENUE=_SYMBOL_PARAM.PMIC', dtype=bool))

qte['BID_PRIMARY'] = qte['BID_PRICE']
qte['ASK_PRIMARY'] = qte['ASK_PRICE']
qte['QUOTE_VENUE_PRIMARY'] = qte['QUOTE_VENUE']
qte = qte[['BID_PRIMARY', 'ASK_PRIMARY', 'QUOTE_VENUE_PRIMARY']]

# Define NBBO Data Source, looking back for prevailing values
nbbo = otp.DataSource(db='EU_COMP', tick_type='NBBO', back_to_first_tick=86400)
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE', 'BID_EXCHANGE', 'ASK_EXCHANGE']]

# Join Prevailing Trades to Prevailing Trades from the Primary, Quotes from the Primary and NBBO
joined = otp.join_by_time(sources=[trd, trd_primary, qte, nbbo], match_if_identical_times=True)

# Merge data into a single result across symbols
merged_data = otp.merge(joined, symbols=sym_list, identify_input_ts=True)

# Run the query returning the data in the selected timezone
result = otp.run(merged_data,
                 start=start_time,
                 end=end_time,
                 timezone='Europe/London')
result
```

## Calculate Period Trade Statistics

Aggregating Trades for a list of Symbols across Venues, including Calendar Information
using {meth}`~onetick.py.Source.mkt_activity`.  
MktActivity returns:
``Rb`` [Pre Market], ``Rr`` [Trading], ``Ra`` [Post Market],
``R1`` [Morning], ``Rx`` [Lunch], ``R2`` [Afternoon].  
Combining Output into a single result set.

```{code-cell} ipython3

import onetick.py as otp

otp.config.default_db = 'US_COMP'
otp.config.default_symbol = 'AAPL'

# Define Symbol List with Symbols including the Database using syntax [Db Name]::[Ticker Symbol]
sym_list = [
    'LSE::VOD',
    'EURONEXT::AF',
    'XETRA::DBK',
    'LSE::TSCO',
    'LSE::SHEL'
]

# Define Data Source, in this case without specifying the symbol name.
# As the schema is not yet known, set the schema policy to manual
trd = otp.DataSource(tick_type='TRD', schema_policy='manual')
# Define the output schema
trd.schema.set(
    PRICE=float,
    SIZE=int,
    TRADE_VENUE=str,
    BOOK_TYPE=str,
    TRADE_PERIOD=str
)
trd = trd[['PRICE', 'SIZE', 'TRADE_VENUE', 'BOOK_TYPE', 'TRADE_PERIOD']]

# Add the Symbol to the DataSource
trd['SYMBOL_NAME'] = trd['_SYMBOL_NAME']
# Extract The Calendar Name from the Database component of the Symbol
trd['CALENDAR_NAME'] = 'CLOUD_DB_' + trd['SYMBOL_NAME'].str.extract(r'([^:]+)', rewrite=r'\1')

# Add the Market Activity field, based on the selected Calendar
trd = trd.mkt_activity(calendar_name=trd['CALENDAR_NAME'])

# Aggregates All Trades, grouped by MKT_ACTIVITY value
data = trd.agg({
    'FIRST_PRICE': otp.agg.first('PRICE'),
    'HIGH_PRICE': otp.agg.max('PRICE'),
    'LOW_PRICE': otp.agg.min('PRICE'),
    'LAST_PRICE': otp.agg.last('PRICE'),
    'VWAP_PRICE': otp.agg.vwap('PRICE', 'SIZE'),
    'SUM_SIZE': otp.agg.sum('SIZE'),
    'TRADE_COUNT': otp.agg.count()
}, group_by=trd['MKT_ACTIVITY'])

# Create a single output, merging all the inputs into a single resultset.
merged = otp.merge([data], symbols=sym_list, identify_input_ts=True, separate_db_name=True)

# Run the query returning the data in the selected timezone
result = otp.run(merged,
                 start=otp.datetime(2024, 1, 3),
                 end=otp.datetime(2024, 1, 4),
                 timezone='Europe/London')
result
```

## Calculate Period Trade Statistics for a Symbol List

Calculate Period Trade Statistics for a Symbol List, and combine into a single Output.

```{code-cell} ipython3

import onetick.py as otp

# Define the Symbol List
sym_list = ['CSCO', 'AAPL', 'AMZN', 'MSFT']

# Define Trade Data Source for specified Database
trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

# Limit Output fields
trd = trd[['PRICE', 'SIZE', 'COND']]

# Aggregates All Trades
data = trd.agg({
    'FIRST_PRICE': otp.agg.first('PRICE'),
    'HIGH_PRICE': otp.agg.max('PRICE'),
    'LOW_PRICE': otp.agg.min('PRICE'),
    'LAST_PRICE': otp.agg.last('PRICE'),
    'VWAP_PRICE': otp.agg.vwap('PRICE', 'SIZE'),
    'SUM_SIZE': otp.agg.sum('SIZE'),
    'TRADE_COUNT': otp.agg.count()
})

# Create a single output, merging all the input symbols into a single resultset.
merged_data = otp.merge([data], symbols=sym_list, identify_input_ts=True)

# Run the query returning the data in the selected timezone, Time period, and Symbol.
result = otp.run(merged_data,
                 start=otp.dt(2024, 1, 3, 9, 45),
                 end=otp.dt(2024, 1, 3, 10, 13),
                 timezone='America/New_York')
result
```

## Calculate Period Trade Statistics based on a Supplied Dataframe

Retrieve Prevailing Trades based on Input Dataframe with Symbols and Timestamps.

```{code-cell} ipython3

import onetick.py as otp
import pandas as pd
from io import StringIO

# To Demonstrate the use of a Dataframe a CSV is loaded with TIME, ID and SYMBOL_NAME fields.
# CSV with TIMESTAMP field set to %Y-%m-%d %H:%M:%S.%f format in Specified Time Zone
csv_input = """TIME,ID,SYMBOL_NAME
2024-01-03 14:33:02.000,1,AAPL
2024-01-03 14:33:32.000,2,CSCO
2024-01-03 14:36:02.000,3,MSFT
2024-01-03 14:36:03.000,4,MSFT
2024-01-03 14:36:03.250,5,AMZN
2024-01-03 14:36:03.260,6,HD
2024-01-03 14:36:04.000,7,AMZN
2024-01-03 14:36:12.000,8,AMZN
"""

# Loaded into the Dataframe using the pandas read_csv method.
df = pd.read_csv(StringIO(csv_input), parse_dates=['TIME'])

# The Dataframe is updated to include two specifial fields:
# _PARAM_START_TIME - The Start Time of the sub query specific to this symbol
# _PARAM_END_TIME - The End Time of the sub query specific to this symbol
# As there are multiple records for the same symbol an ID column is also defined.

df['_PARAM_START_TIME'] = df['TIME']
df['_PARAM_END_TIME'] = df['TIME']
df = df[['SYMBOL_NAME', 'ID', '_PARAM_START_TIME', '_PARAM_END_TIME']]

# The DataSource is defined with the Database and Tick Type specified
# As there may not be prevailng trades at the specified timestamp a lookback period is defined using "back_to_first_tick"
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', back_to_first_tick=86400)
data = data[['PRICE', 'SIZE']]

# Fields are added from the supplied Dataframe to each output
data['SYMBOL'] = data.Symbol.name
data['ID'] = data.Symbol['ID', str]

# Run Query setting symbols equal to the dataframe
# Producing a dictionary of output results dataframes, one per input symbol
result = otp.run(
    data,
    start=otp.dt(2024, 1, 3),
    end=otp.dt(2024, 1, 4),
    symbols=df,
    timezone='America/New_York'
)
result
```
