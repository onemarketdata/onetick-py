---
file_format: mystnb
---

# Symbol Selection

This section contains 9 examples for Symbol Selection using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Data Retrieval with Single Symbol

Retrieve Data for a single symbol, by passing a single symbol string into {func}`otp.run <onetick.py.run>`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
# Return first 100 Rows
data = data.limit(100)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Data Retrieval with List of Symbols

Retrieve Data for a set of symbols, by passing a List of Symbols in {func}`otp.run <onetick.py.run>`.

```{code-cell} ipython3

import onetick.py as otp

# Define the DataSource
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

# Limit to 1000 rows
data = data.limit(1000)

# Specify the Symbol List and Time Range
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols=['CSCO', 'MSFT'])
result
```

## Data Retrieval with Symbol Mask

Retrieve Data for symbols matched by pattern.

```{code-cell} ipython3

import onetick.py as otp

# Define your symbol mask, e.g., all symbols starting with 'AA'
symbol_mask = 'AA%'

# Get all symbols matching the mask
symbols = otp.Symbols(db='US_COMP_SAMPLE_DAILY', pattern=symbol_mask)

# Define Data Source, in this case for DAY records
data = otp.DataSource(db='US_COMP_SAMPLE_DAILY', tick_type='DAY')

# Merge data into a single result across symbols
merged_data = otp.merge(data, symbols=symbols, identify_input_ts=True)

# Run query, with defined time range and time zone
result = otp.run(merged_data,
                 start=otp.dt(2024, 1, 2),
                 end=otp.dt(2024, 1, 3),
                 timezone='America/New_York')
result
```

## Data Retrieval Across Databases

Retrieve Trades for Symbols across Databases for the specified time range.  
The initial {class}`otp.DataSource <onetick.py.DataSource>` is defined without specifying the Database or symbol.  
The schema of the Data Source is specified manually.  
Symbols are specified including the Database name, with format ``[Database]::[Symbol]`` e.g. ``LSE::VOD``.

```{code-cell} ipython3

import onetick.py as otp

# Define the Symbol List
sym_list = ['LSE::VOD', 'EURONEXT::AF', 'XETRA::DBK', 'LSE::TSCO',
            'LSE::SHEL', 'EURONEXT::AF', 'LSE::VOD', 'XETRA::DBK']

# Define Data Source, in this case without specifying the Database or symbol name.
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
# Specify Output Fields
trd = trd[['PRICE', 'SIZE', 'TRADE_VENUE', 'BOOK_TYPE', 'TRADE_PERIOD']]

# Filter on Lit Order Book
trd = trd.where(trd['BOOK_TYPE'] == '0')

# Filter on Continuous Trading
trd = trd.where(trd['TRADE_PERIOD'] == '-')

# Create a single output, merging all the inputs into a single resultset.
merged = otp.merge([trd], symbols=sym_list, identify_input_ts=True, separate_db_name=True)

# Return first 1000 Rows
merged = merged.limit(1000)

# Run the query returning the data in the selected timezone
result = otp.run(merged,
                 start=otp.datetime(2024, 1, 3, 8),
                 end=otp.datetime(2024, 1, 4, 16),
                 timezone='Europe/London')
result
```

## Data Retrieval across Symbol Changes

Ceridian HCM Holding rebranded as Dayforce, Inc on 1st Feb 2024, changing its symbol from ``CDAY`` to ``DAY``.

Full History can be retrieved by querying for both symbols.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_DAILY', tick_type='DAY')
data = data[['EXCHANGE', 'OPEN', 'HIGH', 'LOW', 'CLOSE']]
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='America/New_York',
                 symbols=['CDAY', 'DAY'])
result
```

## Data Retrieval with New Symbol

Ceridian HCM Holding rerbanded as Dayforce, Inc on 1st Feb 2024, changing its symbol from ``CDAY`` to ``DAY``.  
Full History can be retrieved by specifying a single symbol and selecting the ``SYMBOL_DATE`` to when it is active.  
For example ``DAY`` after Feb 2024.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_DAILY', tick_type='DAY')
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='America/New_York',
                 symbols='DAY',
                 symbol_date=otp.dt(2024, 4, 1))
result
```

## Data Retrieval with Old Symbol

Ceridian HCM Holding rebanded as Dayforce, Inc on 1st Feb 2024, changing its symbol from ``CDAY`` to ``DAY``.  
Full History can be retrieved by specifying a single symbol and selecting the ``SYMBOL_DATE`` to when it is active.  
For example ``CDAY`` before Feb 2024.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_DAILY', tick_type='DAY')
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='America/New_York',
                 symbols='CDAY',
                 symbol_date=otp.dt(2024, 1, 1))
result
```
