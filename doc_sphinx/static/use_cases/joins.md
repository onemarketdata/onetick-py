---
file_format: mystnb
---

# Joins

This section contains examples for Joins using the `onetick.py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Calculates Period Statistics and Join them to Trading Activity

Period statistics calculate the Volume Weighted Average Price (VWAP), High, Low and total Volume across the
whole period as a single tick (`bucket_interval=0`),
which is then joined to every trade with {func}`otp.join(on='all') <onetick.py.join>`.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE', 'EXCHANGE', 'COND']]

# Compute Aggregates across the whole Period as a single tick.
stats_src = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
stats = stats_src.agg({
    'VWAP_PRICE': otp.agg.vwap('PRICE', 'SIZE'),
    'FIRST_PRICE': otp.agg.first('PRICE'),
    'HIGH_PRICE': otp.agg.max('PRICE'),
    'LOW_PRICE': otp.agg.min('PRICE'),
    'LAST_PRICE': otp.agg.last('PRICE'),
    'VOLUME': otp.agg.sum('SIZE'),
})

# Join every trade to the single period-statistics record.
data = otp.join(trd, stats, on='all')

data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Join Trades to Multiple Tables

Returns trade price/size with prevailing bid/ask prices from both the `NBBO` and `NBBO_COMP` tables.  
Joins three tables together: `TRD`, `NBBO` and `NBBO_COMP`.  
`NBBO` provides the standard NBBO;
`NBBO_COMP` provides an improved NBBO that includes the Best Odd Lot order (BOLO).  
Each side's fields are renamed before the join, since `NBBO` and `NBBO_COMP` share column names.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]

nbbo = otp.DataSource(db='US_COMP', tick_type='NBBO')
nbbo['NBBO_TIME'] = nbbo['TIMESTAMP']
nbbo = nbbo[['NBBO_TIME', 'BID_PRICE', 'ASK_PRICE', 'BID_SIZE', 'ASK_SIZE']]

nbbo_comp = otp.DataSource(db='US_COMP', tick_type='NBBO_COMP')
nbbo_comp = nbbo_comp.add_suffix('_COMP')
nbbo_comp['NBBO_COMP_TIME'] = nbbo_comp['TIMESTAMP']

data = otp.join_by_time([trd, nbbo, nbbo_comp])
data = data.limit(10)

result = otp.run(data,
                 start=otp.dt(2026, 7, 23, 9, 30),
                 end=otp.dt(2026, 7, 23, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Join Trades to Prevailing Quotes

Returns trade price/size with prevailing bid/ask prices at time of trade.  
Joins trades and quotes by symbol using {func}`otp.join_by_time <onetick.py.join_by_time>`.  
As the US Composite includes trades and quotes across multiple exchanges, the `NBBO` tick type is used.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]

nbbo = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO')
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE']]

data = otp.join_by_time([trd, nbbo])
data = data.limit(100)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='UTC',
                 symbols='CSCO')
result
```

### Join Trades to Prevailing Exchange Quotes

Returns trade price/size with prevailing bid/ask prices at time of trade,
matched to the quote from the same exchange as the trade.  
The `on` parameter of {func}`otp.join_by_time <onetick.py.join_by_time>` adds this extra match key alongside the
time-based "as of" join, so the trade is joined to the prevailing quote for its own exchange.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE', 'EXCHANGE']]

qte = otp.DataSource(db='US_COMP_SAMPLE', tick_type='QTE')
qte = qte[['EXCHANGE', 'BID_PRICE', 'ASK_PRICE']]

# Match on EXCHANGE in addition to the time-based "as of" join.
data = otp.join_by_time([trd, qte], on=['EXCHANGE'])
data = data.limit(100)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 4, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Join Trades to Static to Calc Futures Notional Value

Notional Traded Value is calculated from both the Trade and Static (reference) record.  
The static record for the futures contract is fetched once (`back_to_first_tick` plus {meth}`~onetick.py.Source.last`),
then joined to every trade with {func}`otp.join(on='all') <onetick.py.join>`,
since the static side is a single constant tick.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='CME_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]

stat = otp.DataSource(db='CME_SAMPLE', tick_type='STAT', back_to_first_tick=86400)
stat = stat[['TICK_VALUE', 'TICK_SIZE', 'NAME']]
# Ensure exactly one static record, the prevailing one for the contract.
stat = stat.last()

# Join every trade to the single static record.
data = otp.join(trd, stat, on='all')
data['TRADED_VALUE'] = data['PRICE'] * data['SIZE'] * data['TICK_VALUE'] / data['TICK_SIZE'].astype(float)
data = data[['PRICE', 'SIZE', 'TICK_VALUE', 'TICK_SIZE', 'TRADED_VALUE', 'NAME']]

data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 7, 17),
                 end=otp.dt(2024, 1, 8, 17),
                 timezone='America/New_York',
                 symbols=r'ES\H24')
result
```

## Join Trades to Sub Query of Prevailing Mid

Returns trade price/size with prevailing mid-price at time of trade.  
Joins trades and quotes with valid size using {func}`otp.join_by_time <onetick.py.join_by_time>`.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
trd = trd.where(trd['SIZE'] > 0)
trd = trd[['PRICE', 'SIZE']]

qte = otp.DataSource(db='US_COMP_SAMPLE', tick_type='QTE')
qte = qte.where((qte['BID_SIZE'] > 0) & (qte['ASK_SIZE'] > 0))
qte['MID'] = (qte['ASK_PRICE'] + qte['BID_PRICE']) / 2
qte = qte[['MID']]

data = otp.join_by_time([trd, qte])
data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Join Trades to US Composite Day Volume

Returns individual trade price/size with the daily total volume (all exchanges composite).  
The day's volume is fetched once
(`back_to_first_tick` plus {meth}`~onetick.py.Source.last`, filtered to the composite `EXCHANGE`),
then joined to every trade with {func}`otp.join(on='all') <onetick.py.join>`,
since the daily side is a single constant tick for the query day.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]

day = otp.DataSource(db='US_COMP_SAMPLE', tick_type='DAY', back_to_first_tick=86400)
day = day.where(day['EXCHANGE'] == '')
day['VOL_TIME'] = day['TIMESTAMP']
day = day[['VOLUME', 'VOL_TIME']]
day = day.last()

# Join every trade to the single daily volume record.
data = otp.join(trd, day, on='all')
data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```
