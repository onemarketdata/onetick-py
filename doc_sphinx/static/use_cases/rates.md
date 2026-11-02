---
file_format: mystnb
---

# Rates

This section contains examples for Rates using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Data Retrieval for 1-year SONIA Interest Rate

Query SONIA (Sterling Overnight Index Average) interest rate data.  
Returns closing rates from January 1, 2024 to today.

```python
import onetick.py as otp

data = otp.DataSource(db='RATES', tick_type='DAY')  # RATES database, daily data

# Optionally, limit the number of rows
data = data.limit(1000)

# Run the query for the SONIA_RATE symbol, selecting only the CLOSE column
result = otp.run(
    data[['CLOSE']],           # Select only closing price
    symbols='SONIA_RATE',      # SONIA interest rate symbol
    start=otp.dt(2024, 1, 1),  # Query start time: January 1, 2024
    end=otp.now(),             # Query end time, this will use the current date and time at runtime
)

result  # Display results
```

{.dataframe}
|     |                Time |  CLOSE |
|----:|--------------------:|-------:|
| 0   | 2024-01-01 19:00:00 | 5.1863 |
| 1   | 2024-01-02 19:00:00 | 5.1863 |
| 2   | 2024-01-03 19:00:00 | 5.1870 |
| 3   | 2024-01-04 19:00:00 | 5.1869 |
| 4   | 2024-01-07 19:00:00 | 5.1869 |
| ..  |                 ... |    ... |
| 625 | 2026-07-21 20:00:00 | 3.7303 |
| 626 | 2026-07-23 20:00:00 | 3.7307 |
| 627 | 2026-07-26 20:00:00 | 3.7312 |
| 628 | 2026-07-27 20:00:00 | 3.7316 |
| 629 | 2026-07-28 20:00:00 | 3.7313 |

630 rows x 2 columns

## Return All Interest Rate Symbols

Returning all Interest Rate Symbols and their associated Name that are available on a specific date.

```python
import onetick.py as otp

data = otp.DataSource(db='RATES', tick_type='STAT')

# Merge every symbol in the database into a single stream, the equivalent of SYMBOL_NAME LIKE '%'.
data = otp.merge([data],
                 symbols=otp.Symbols(db='RATES', for_tick_type='STAT'),
                 identify_input_ts=True)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='UTC')
result
```

{.dataframe}
|    |       Time |                           NAME |    OID | OMDSEQ | SEC_TYPE | SYMBOL_NAME | TICK_TYPE |
|---:|-----------:|-------------------------------:|-------:|-------:|---------:|------------:|----------:|
|  0 | 2024-01-03 |           BGCR(1ST PERCENTILE) | 741852 |      0 |     RATE |     BGCR_1P |      STAT |
|  1 | 2024-01-03 |          BGCR(25TH PERCENTILE) | 741853 |      0 |     RATE |    BGCR_25P |      STAT |
|  2 | 2024-01-03 |          BGCR(75TH PERCENTILE) | 742081 |      0 |     RATE |    BGCR_75P |      STAT |
|  3 | 2024-01-03 |          BGCR(99TH PERCENTILE) | 742170 |      0 |     RATE |    BGCR_99P |      STAT |
|  4 | 2024-01-03 | BROAD GENERAL COLL RATE (BGCR) | 741857 |      0 |     RATE |   BGCR_RATE |      STAT |
| .. |        ... |                            ... |    ... |    ... |      ... |         ... |       ... |
| 31 | 2024-01-03 |          TGCR(25TH PERCENTILE) | 742089 |      0 |     RATE |    TGCR_25P |      STAT |
| 32 | 2024-01-03 |          TGCR(75TH PERCENTILE) | 742090 |      0 |     RATE |    TGCR_75P |      STAT |
| 33 | 2024-01-03 |          TGCR(99TH PERCENTILE) | 742173 |      0 |     RATE |    TGCR_99P |      STAT |
| 34 | 2024-01-03 | TRI-PARTY GEN COLL RATE (TGCR) | 741861 |      0 |     RATE |   TGCR_RATE |      STAT |
| 35 | 2024-01-03 | TOKYO OVERNIGHT AVG RATE (TONA | 742868 |      0 |     RATE |   TONA_RATE |      STAT |
