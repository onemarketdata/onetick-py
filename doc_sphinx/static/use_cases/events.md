---
file_format: mystnb
---

# Earnings Events Analysis

This section contains 6 examples for Events using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

Earnings announcements are recorded in the ``EVENT`` tick type
and can be combined with market data to analyze market reactions and trading patterns around these events.

## Event Data Sources

Earnings announcement events are available in the daily market data databases:

* ``US_COMP_DAILY`` / ``EVENT`` - US earnings events

## Event Types

The EVENT tick type records two primary event types, held in the ``EVENT_TYPE`` field:

* ``EARNING_DATE`` - Earnings announcement dates
* ``COMPANY_CONFERENCE_CALL`` - Scheduled conference call dates

These events are useful for analyzing market behavior around significant corporate announcements.

## Earnings Event History for Specified Symbol

Retrieve all earnings events recorded for a specific symbol within a time range.  
This shows all historical earnings announcements for a company.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_DAILY', tick_type='EVENT')

result = otp.run(data,
                 start=otp.dt(2026, 1, 1),
                 end=otp.dt(2026, 6, 12),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Earnings Event History for all US Companies across a time range

Retrieve all earnings events across every symbol in the database within a specified time range,
by merging across {class}`otp.Symbols <onetick.py.Symbols>`, the equivalent of SQL's ``SYMBOL_NAME LIKE '%'``.  
This provides a comprehensive view of earnings announcements during a particular period.  
``identify_input_ts`` adds the ``SYMBOL_NAME`` of each tick to the output.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_DAILY', tick_type='EVENT')

# Merge the events for every symbol in the database into a single stream.
# identify_input_ts adds the SYMBOL_NAME of each tick to the output.
data = otp.merge([data],
                 symbols=otp.Symbols(db='US_COMP_DAILY', for_tick_type='EVENT'),
                 identify_input_ts=True)

result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 1, 5),
                 timezone='America/New_York')
result
```

## Joining Daily Pricing to US Earning Events

Correlate daily pricing data with earnings events to analyze closing prices and volume
on earnings announcement dates.  
Earnings events (excluding conference calls) are joined to the daily price/volume record
for the same symbol with {func}`otp.join <onetick.py.join>`, keyed on the date.  
As closing prices and events occur at different times of day,
both sides are truncated to the day with the
{meth}`.dt.date_trunc <onetick.py.core.column_operations.accessors.dt_accessor._DtAccessor.date_trunc>`
accessor before joining.  
A left outer join keeps every daily record, with ``EVENT_TYPE`` populated only on event days.

```{code-cell} ipython3

import onetick.py as otp

# Daily pricing, filtered to the composite (empty EXCHANGE).
day = otp.DataSource(db='US_COMP_DAILY', tick_type='DAY')
day = day.where(day['EXCHANGE'] == '')
day = day[['CLOSE', 'VOLUME']]

# Earnings events only, excluding the conference calls.
event = otp.DataSource(db='US_COMP_DAILY', tick_type='EVENT',
                       schema_policy='manual', schema={'EVENT_TYPE': str})
event = event.where(event['EVENT_TYPE'] == 'EARNING_DATE')
event = event[['EVENT_TYPE']]

# Truncate both sides to the day so the differing intraday times still match.
day['DAY_KEY'] = day['TIMESTAMP'].dt.date_trunc('day')
event['DAY_KEY'] = event['TIMESTAMP'].dt.date_trunc('day')

# Left outer join keeps every daily record, with EVENT_TYPE populated only on event days.
data = otp.join(day, event, on=day['DAY_KEY'] == event['DAY_KEY'], how='left_outer')
data = data[['CLOSE', 'VOLUME', 'EVENT_TYPE']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Combining or Unioning Daily Pricing and US Earnings Events

Union daily price data with earnings events, creating a combined dataset that shows both daily price bars and
discrete event occurrences, using {func}`otp.merge <onetick.py.merge>`.  
This is useful for time series visualization and analysis spanning both continuous pricing and discrete events.  
The output schema is standardized across both branches:
setting ``CLOSE`` as {class}`otp.nan <onetick.py.nan>` and ``VOLUME`` as 0 for the event rows,
and ``EVENT_TYPE`` as an empty {class}`otp.string[40] <onetick.py.string>` for the daily pricing rows.

```{code-cell} ipython3

import onetick.py as otp

# Daily pricing, filtered to the composite (empty EXCHANGE).
day = otp.DataSource(db='US_COMP_DAILY', tick_type='DAY')
day = day.where(day['EXCHANGE'] == '')
day = day[['CLOSE', 'VOLUME']]
day['EVENT'] = 0
day['EVENT_TYPE'] = otp.string[40]('')

# Earnings events only, shaped to the same schema as the daily pricing.
event = otp.DataSource(db='US_COMP_DAILY', tick_type='EVENT',
                       schema_policy='manual', schema={'EVENT_TYPE': otp.string[40]})
event = event.where(event['EVENT_TYPE'] == 'EARNING_DATE')
event['CLOSE'] = otp.nan
event['VOLUME'] = 0
event['EVENT'] = 1
event = event[['CLOSE', 'VOLUME', 'EVENT', 'EVENT_TYPE']]

# Union the two streams.
data = otp.merge([day, event])

result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Combining or Unioning 1 Minute Trade Bars and US Earnings Events

Merge pre-calculated 1 minute trade bar data with earnings events on the same day,
allowing analysis of intraday price movement patterns around earnings announcements,
using {func}`otp.merge <onetick.py.merge>`.  
The output schema is standardized across both branches:
setting ``LAST`` as {class}`otp.nan <onetick.py.nan>` and ``VOLUME`` as 0 for the event rows,
and ``EVENT_TYPE`` as an empty {class}`otp.string[40] <onetick.py.string>` for the bar rows.

```{code-cell} ipython3

import onetick.py as otp

# Pre-calculated 1 minute trade bars.
bars = otp.DataSource(db='US_COMP_BARS', tick_type='TRD_1M')
bars = bars[['LAST', 'VOLUME']]
bars['EVENT'] = 0
bars['EVENT_TYPE'] = otp.string[40]('')

# Earnings events only, shaped to the same schema as the bars.
event = otp.DataSource(db='US_COMP_DAILY', tick_type='EVENT',
                       schema_policy='manual', schema={'EVENT_TYPE': otp.string[40]})
event = event.where(event['EVENT_TYPE'] == 'EARNING_DATE')
event['LAST'] = otp.nan
event['VOLUME'] = 0
event['EVENT'] = 1
event = event[['LAST', 'VOLUME', 'EVENT', 'EVENT_TYPE']]

# Union the two streams.
data = otp.merge([bars, event])

result = otp.run(data,
                 start=otp.dt(2024, 2, 14),
                 end=otp.dt(2024, 2, 15),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Combining or Unioning Trades and US Earnings Events

Combine individual trade data with earnings events into a unified stream, creating a single data source that
captures both transaction-level activity and corporate announcements, using {func}`otp.merge <onetick.py.merge>`.  
The output schema is standardized across both branches:
setting ``PRICE`` as {class}`otp.nan <onetick.py.nan>` and ``SIZE`` as 0 for the event rows,
and ``EVENT_TYPE`` as an empty {class}`otp.string[40] <onetick.py.string>` for the trade rows.

```{code-cell} ipython3

import onetick.py as otp

# Trades from the US Composite.
trd = otp.DataSource(db='US_COMP', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]
trd['EVENT'] = 0
trd['EVENT_TYPE'] = otp.string[40]('')

# Earnings events only, shaped to the same schema as the trades.
event = otp.DataSource(db='US_COMP_DAILY', tick_type='EVENT',
                       schema_policy='manual', schema={'EVENT_TYPE': otp.string[40]})
event = event.where(event['EVENT_TYPE'] == 'EARNING_DATE')
event['PRICE'] = otp.nan
event['SIZE'] = 0
event['EVENT'] = 1
event = event[['PRICE', 'SIZE', 'EVENT', 'EVENT_TYPE']]

# Union the two streams.
data = otp.merge([trd, event])

# get only first 1000 rows
data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 2, 14),
                 end=otp.dt(2024, 2, 15),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```
