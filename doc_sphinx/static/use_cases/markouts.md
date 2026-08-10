---
file_format: mystnb
---

# Markouts and Time Shifts

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Prevailing quote at the time of a trade

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]

qte = otp.DataSource('US_COMP_SAMPLE',
                     tick_type='NBBO',
                     back_to_first_tick=otp.Minute(10),
                     keep_first_tick_timestamp='NBBO_TIME')
qte = qte[['ASK_PRICE', 'BID_PRICE', 'NBBO_TIME']]

enriched_trades = otp.join_by_time([trd, qte])

result = otp.run(enriched_trades,
                 symbols='AAPL',
                 start=otp.dt(2024, 2, 1, 9, 30),
                 end=otp.dt(2024, 2, 1, 9, 30, 1),
                 timezone='America/New_York')
result
```

## Point-in-time benchmarks: BBO at different markouts

Find the prevailing quote at different time intervals (markouts) before/after each trade in seconds.  
Also add the timestamp of joined quote.

```{code-cell} ipython3
markouts = [-1, 1]

trd = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]

qte_by_markout = []
for m in markouts:
    mr = str(m).replace('-', 'B') if m < 0 else f'A{m}'
    qte = otp.DataSource('US_COMP_SAMPLE',
                         tick_type='NBBO',
                         back_to_first_tick=otp.Hour(24),
                         keep_first_tick_timestamp='NBBO_TIME')
    qte = qte[['ASK_PRICE', 'BID_PRICE', 'NBBO_TIME']]
    qte = qte.add_suffix(f'_{mr}')

    # shift the data by m seconds
    qte = qte.time_interval_shift(m * 1000)
    qte_by_markout.append(qte)

data = otp.join_by_time([trd] + qte_by_markout)
result = otp.run(
    data,
    symbols='AAPL',
    start=otp.dt(2024, 2, 1, 9, 30),
    end=otp.dt(2024, 2, 1, 9, 30, 1),
    timezone='America/New_York',
)
result
```

## Time Shifts

Query ``VOD`` (Vodafone) trade data with time-shifted versions.  
Returns original prices plus shifted prices (1s, 10s, 60s backward and forward).

```{code-cell} ipython3

import onetick.py as otp

# Define the time range
start = otp.dt(2024, 1, 3, 8)
end = otp.dt(2024, 1, 4, 16)

# Create base trades source
trades = otp.DataSource(
    db='LSE_SAMPLE',
    tick_type='TRD',
    symbols='VOD'
)

# Define offsets in milliseconds (backward and forward)
offsets = {
    'BACK_1S': -1000,
    'BACK_10S': -10000,
    'BACK_60S': -60000,
    'FWD_1S': 1000,
    'FWD_10S': 10000,
    'FWD_60S': 60000
}

# Generate shifted data sources using for loop
shifted_sources = [trades]
shifted_column_names = []

for suffix, shift_ms in offsets.items():
    shifted = otp.DataSource(
        db='LSE_SAMPLE',
        tick_type='TRD',
        symbols='VOD'
    ).time_interval_shift(shift=shift_ms).add_suffix(f'_{suffix}')
    shifted_sources.append(shifted)
    shifted_column_names.append(f'PRICE_{suffix}')

# Join all shifted prices by TIMESTAMP
data = otp.join_by_time(shifted_sources)

# Select and order columns
columns = ['TIMESTAMP', 'PRICE', 'SIZE', 'TRADE_ID', 'TRADE_VENUE']
columns.extend(shifted_column_names)

data = data[columns]

# Return first 1000 Rows
data = data.limit(1000)

# Run the query
result = otp.run(data, start=start, end=end, timezone='UTC')
result
```

## Time Shifts - Nested

Query ``VOD`` (Vodafone) quote data with nested time-shifted mid-prices.  
Returns original mid-price plus shifted mid-prices (1s, 10s, 60s backward and forward).

```{code-cell} ipython3

import onetick.py as otp

# Define the time range
start = otp.dt(2024, 1, 3, 8)
end = otp.dt(2024, 1, 4, 16)

# Base quotes source
quotes = otp.DataSource(
    db='LSE_SAMPLE',
    tick_type='QTE',
    symbols='VOD'
)
quotes['MID_PRICE'] = (quotes['BID_PRICE'] + quotes['ASK_PRICE']) / 2

# Define offsets in milliseconds (backward and forward)
offsets = {
    'BACK_1S': -1000,
    'BACK_10S': -10000,
    'BACK_60S': -60000,
    'FWD_1S': 1000,
    'FWD_10S': 10000,
    'FWD_60S': 60000
}

# Generate independent time-shifted data sources
shifted_sources = [quotes]

for suffix, shift_ms in offsets.items():
    shifted = otp.DataSource(
        db='LSE_SAMPLE',
        tick_type='QTE',
        symbols='VOD'
    )
    shifted['MID_PRICE'] = (shifted['BID_PRICE'] + shifted['ASK_PRICE']) / 2
    shifted = shifted.time_interval_shift(shift=shift_ms).add_suffix(f'_{suffix}')
    shifted_sources.append(shifted)

# Join all shifted data by TIMESTAMP
data = otp.join_by_time(shifted_sources)

# Select final columns
columns = ['TIMESTAMP', 'MID_PRICE']
columns.extend([f'MID_PRICE_{suffix}' for suffix in offsets.keys()])

data = data[columns]

# Return first 1000 Rows
data = data.limit(1000)

# Run the query
result = otp.run(data, start=start, end=end, timezone='UTC')
result
```
