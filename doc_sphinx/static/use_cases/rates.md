---
file_format: mystnb
---

# Rates

This section contains 1 example for Rates using the `onetick-py`.  
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
