---
file_format: mystnb
---

# Symbol Universe

This section contains 1 example for Symbol_Universe using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Symbol Universe with Mask

Query trades for all symbols matching a pattern for today.  
Retrieves trade data for symbols starting with ``AAA`` from the US equities composite database.

```python
import onetick.py as otp

# Define the masked symbol list (e.g., all symbols starting with 'AAA')
symbols = otp.Symbols(db='US_COMP', pattern='AAA%', for_tick_type='TRD')

# Define the trades data source (do not specify symbols here)
trades = otp.DataSource(db='US_COMP', tick_type='TRD')

# merge all symbols
trades = otp.merge([trades], symbols=symbols, identify_input_ts=True)

# Run the query for the masked symbol list for today
result = otp.run(
    trades,
    date=otp.dt.now().date(),
    timezone='America/New_York',
)
result
```

{.dataframe}
|     |                          Time | AGGRESSOR_SIDE | BOOK_TYPE | COND | CORR | EXCHANGE | ... | TRADE_PERIOD | TRADE_TYPE | TRF |                      TRF_TIME | TTE | SYMBOL_NAME | TICK_TYPE |
|----:|------------------------------:|---------------:|----------:|-----:|-----:|---------:|----:|-------------:|-----------:|----:|------------------------------:|----:|------------:|----------:|
|   0 | 2026-08-06 04:00:00.031017295 |                |         0 |   FT |    0 |        P | ... |            E |         FT |     | 1969-12-31 19:00:00.000000000 |   1 |        AAAU |       TRD |
|   1 | 2026-08-06 04:00:02.621367162 |                |         9 |    T |    0 |        D | ... |            E |          T |   N | 2026-08-06 04:00:02.621345555 |   0 |        AAAU |       TRD |
|   2 | 2026-08-06 04:00:06.891092937 |                |         9 |   TI |    0 |        D | ... |            E |         TI |   T | 2026-08-06 04:00:06.890724538 |   0 |        AAAU |       TRD |
|   3 | 2026-08-06 04:00:07.001389565 |                |         9 |   TI |    0 |        D | ... |            E |         TI |   T | 2026-08-06 04:00:07.001029603 |   0 |        AAAU |       TRD |
|   4 | 2026-08-06 04:00:07.331599873 |                |         9 |   TI |    0 |        D | ... |            E |         TI |   T | 2026-08-06 04:00:07.331239278 |   0 |        AAAU |       TRD |
| ... |                           ... |            ... |       ... |  ... |  ... |      ... | ... |          ... |        ... | ... |                           ... | ... |         ... |       ... |
| 431 | 2026-08-06 09:33:24.818020093 |                |         0 |    F |    0 |        K | ... |            - |          F |     | 1969-12-31 19:00:00.000000000 |   1 |        AAAU |       TRD |
| 432 | 2026-08-06 09:33:24.818022049 |                |         0 |    F |    0 |        Z | ... |            - |          F |     | 1969-12-31 19:00:00.000000000 |   0 |        AAAU |       TRD |
| 433 | 2026-08-06 09:33:24.818025613 |                |         0 |    F |    0 |        Z | ... |            - |          F |     | 1969-12-31 19:00:00.000000000 |   0 |        AAAU |       TRD |
| 434 | 2026-08-06 09:33:24.818259569 |                |         0 |    I |    0 |        T | ... |            - |          I |     | 1969-12-31 19:00:00.000000000 |   0 |        AAAU |       TRD |
| 435 | 2026-08-06 09:33:24.818265881 |                |         0 |    I |    0 |        T | ... |            - |          I |     | 1969-12-31 19:00:00.000000000 |   0 |        AAAU |       TRD |

436 rows x 26 columns
