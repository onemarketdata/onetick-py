---
file_format: mystnb
---

# Real Time and Intraday

This section contains 6 examples for Real Time and Intraday using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Latest NBBO Market Snapshot - NBBO Retrieval Across All Symbols in Database

The *LATEST* databases provide last value caches, storing the latest prices for each instrument.  
*LATEST* databases are available for all real time sources.  
They can only be accessed by those who are entitled to access real time data.  
The ``SNAP_NBBO`` table includes latest NBBO quote for every symbol.
It is only available for Composite databases.
Other databases provide ``SNAP_QTE``.  
Querying by ``SYMBOL_NAME`` returns all symbols.
To filter by symbol, please use the ``SYMBOL`` field.

```python
import onetick.py as otp

data = otp.DataSource(db='US_COMP_LATEST', tick_type='SNAP_NBBO')
data = data.limit(1000)
result = otp.run(data,
                 # The Start and End Times are set using NOW
                 start=otp.now() - otp.Second(1),
                 end=otp.now(),
                 timezone='America/New_York',
                 symbols='-')
result
```

{.dataframe}
|     |                    Time | SYMBOL_NAME | BID_PRICE | BID_SIZE | ASK_PRICE | ASK_SIZE | QUOTE_CURRENCY | SYMBOL |                  TICK_TIME |
|----:|------------------------:|------------:|----------:|---------:|----------:|---------:|---------------:|-------:|---------------------------:|
|   0 | 2026-08-04 09:40:14.955 |         SAP |    192.21 |      100 |    192.33 |      200 |            USD |    SAP | 2026-08-04 09:40:15.749750 |
|   1 | 2026-08-04 09:40:14.955 |         XXI |      4.42 |      100 |      4.45 |      100 |            USD |    XXI | 2026-08-04 09:40:12.181681 |
|   2 | 2026-08-04 09:40:14.955 |       WSTNU |     10.04 |     5000 |     10.75 |     3000 |            USD |  WSTNU | 2026-08-04 09:30:01.997604 |
|   3 | 2026-08-04 09:40:14.955 |        YLDE |     56.75 |      200 |     56.92 |     2100 |            USD |   YLDE | 2026-08-04 09:40:15.856680 |
|   4 | 2026-08-04 09:40:14.955 |        VBNK |     19.59 |      300 |     19.91 |      100 |            USD |   VBNK | 2026-08-04 09:40:14.922921 |
| ... |                     ... |         ... |       ... |      ... |       ... |      ... |            ... |    ... |                        ... |
| 995 | 2026-08-04 09:40:14.955 |        UMDD |     35.84 |      100 |     36.06 |      100 |            USD |   UMDD | 2026-08-04 09:40:14.794579 |
| 996 | 2026-08-04 09:40:14.955 |        ULST |     40.29 |      500 |     40.30 |      100 |            USD |   ULST | 2026-08-04 09:39:59.998443 |
| 997 | 2026-08-04 09:40:14.955 |         UGE |     18.80 |      200 |     18.91 |     2100 |            USD |    UGE | 2026-08-04 09:40:14.118550 |
| 998 | 2026-08-04 09:40:14.955 |         UMC |     20.02 |      900 |     20.03 |      100 |            USD |    UMC | 2026-08-04 09:40:15.810499 |
| 999 | 2026-08-04 09:40:14.955 |         ULS |     85.92 |      100 |     86.82 |      200 |            USD |    ULS | 2026-08-04 09:40:12.596457 |

[1000 rows x 9 columns]

## Latest Quote Market Snapshot - Quote Retrieval Across All Symbols in Database

The *LATEST* databases provide last value caches, storing the latest prices for each instrument.  
*LATEST* databases are available for all real time sources.  
They can only be accessed by those who are entitled to access real time data.  
The ``SNAP_QTE`` table includes latest quote for every symbol.
It is not available for Composite databases which use ``SNAP_NBBO``.  
Querying by ``SYMBOL_NAME`` returns all symbols.
To filter by symbol, please use the ``SYMBOL`` field.

```python
import onetick.py as otp

data = otp.DataSource(db='CME_GLOBEX_LATEST', tick_type='SNAP_QTE')
data = data.limit(1000)
result = otp.run(data,
                 # The Start and End Times are set using NOW
                 start=otp.now() - otp.Second(1),
                 end=otp.now(),
                 timezone='America/New_York',
                 # The Symbol is set to any non-empty value to return all symbols.
                 symbols='-')
result
```

{.dataframe}
|     |                    Time |     SYMBOL_NAME | BID_PRICE | BID_SIZE | ASK_PRICE | ASK_SIZE | QUOTE_CURRENCY |          SYMBOL |                  TICK_TIME |
|----:|------------------------:|----------------:|----------:|---------:|----------:|---------:|---------------:|----------------:|---------------------------:|
|   0 | 2026-08-04 09:50:03.932 |         EAD\M26 |    1.6414 |        4 |     1.642 |        4 |            AUD |         EAD\M26 | 2026-06-15 10:16:00.018016 |
|   1 | 2026-08-04 09:50:03.932 |     EMD\M26\M27 |   25.0500 |        1 |   199.950 |        1 |            USD |     EMD\M26\M27 | 2026-06-17 17:57:48.893272 |
|   2 | 2026-08-04 09:50:03.932 |     EMD\M26\U26 |   32.0000 |        1 |   159.250 |        1 |            USD |     EMD\M26\U26 | 2026-06-18 09:23:36.429287 |
|   3 | 2026-08-04 09:50:03.932 |     EMD\M26\H27 |   25.0500 |        1 |   124.950 |        1 |            USD |     EMD\M26\H27 | 2026-06-17 17:57:48.732984 |
|   4 | 2026-08-04 09:50:03.932 |     EMD\M26\Z26 |   25.0500 |        1 |    99.900 |        1 |            USD |     EMD\M26\Z26 | 2026-06-17 17:57:48.773010 |
| ... |                     ... |             ... |       ... |      ... |       ... |      ... |            ... |             ... |                        ... |
| 995 | 2026-08-04 09:50:03.932 |      DC\Z26\Z27 |   -0.3300 |        2 |    -0.030 |        1 |            USD |      DC\Z26\Z27 | 2026-08-04 09:39:02.454668 |
| 996 | 2026-08-04 09:50:03.932 |     CSC\N26\X26 |   -0.1730 |        1 |    -0.167 |        1 |            USD |     CSC\N26\X26 | 2026-07-31 14:54:55.028661 |
| 997 | 2026-08-04 09:50:03.932 |      DC\Z26\G27 |    0.1200 |        3 |     0.190 |        2 |            USD |      DC\Z26\G27 | 2026-08-04 09:47:11.891593 |
| 998 | 2026-08-04 09:50:03.932 |     CSC\N26\F27 |   -0.1590 |        1 |    -0.153 |        1 |            USD |     CSC\N26\F27 | 2026-07-31 14:23:51.287183 |
| 999 | 2026-08-04 09:50:03.932 | GNF\N26\Q26\U26 |   10.6000 |        1 |    13.350 |        1 |            USX | GNF\N26\Q26\U26 | 2026-07-31 14:53:20.953447 |

1000 rows x 9 columns

## Latest Market Snapshot -  Trade and Quote / NBBO Retrieval Across All Symbols in Database

The *LATEST* databases provide last value caches, storing the latest prices for each instrument.  
*LATEST* databases are available for all real time sources.  
They can only be accessed by those who are entitled to access real time data.  
The ``SNAP`` table includes the combined latest trade and quote or NBBO for every symbol.  
Querying by ``SYMBOL_NAME`` returns all symbols.
To filter by symbol, please use the ``SYMBOL`` field.

```python
import onetick.py as otp

data = otp.DataSource(db='US_COMP_LATEST', tick_type='SNAP')
data = data.limit(1000)
result = otp.run(data,
                 # The Start and End Times are set using NOW
                 start=otp.now() - otp.Second(1),
                 end=otp.now(),
                 timezone='America/New_York',
                 # The Symbol is set to any non-empty value to return all symbols.
                 symbols='-')
result
```

{.dataframe}
|     |                    Time | SYMBOL_NAME |   PRICE | SIZE | TRADE_CURRENCY |  OPEN | CLOSE | ... |    HIGH |    LOW | SYMBOL |            LAST_TRADE_TIME | BID_PRICE | ASK_PRICE |            LAST_QUOTE_TIME |
|----:|------------------------:|------------:|--------:|-----:|---------------:|------:|------:|----:|--------:|-------:|-------:|---------------------------:|----------:|----------:|---------------------------:|
|   0 | 2026-08-04 09:41:33.048 |        XXRP | 21.3400 |  240 |            USD | 21.20 | 21.34 | ... | 21.3400 | 21.176 |   XXRP | 2026-08-04 09:40:06.871326 |     21.29 |     21.34 | 2026-08-04 09:41:32.757718 |
|   1 | 2026-08-04 09:41:33.048 |        SMUP |  5.3036 |  500 |            USD |  5.03 |  4.87 | ... |  5.3900 |  5.030 |   SMUP | 2026-08-04 09:38:59.441185 |      5.32 |      5.34 | 2026-08-04 09:41:33.976781 |
|   2 | 2026-08-04 09:41:33.048 |         SMX | 17.4000 |  284 |            USD | 15.01 | 16.53 | ... |     NaN |    NaN |    SMX | 2026-08-04 09:28:41.845362 |     16.26 |     17.40 | 2026-08-04 09:35:02.753155 |
|   3 | 2026-08-04 09:41:33.048 |         SMU |  6.7600 |  100 |            USD |  6.41 |  6.18 | ... |  6.8795 |  6.370 |    SMU | 2026-08-04 09:41:25.569900 |      6.75 |      6.77 | 2026-08-04 09:41:33.977176 |
|   4 | 2026-08-04 09:41:33.048 |         SMR |  9.4500 |  200 |            USD |  9.19 |  9.01 | ... |  9.5400 |  9.190 |    SMR | 2026-08-04 09:41:33.523574 |      9.44 |      9.45 | 2026-08-04 09:41:34.003923 |
| ... |                     ... |         ... |     ... |  ... |            ... |   ... |   ... | ... |     ... |    ... |    ... |                        ... |       ... |       ... |                        ... |
| 995 | 2026-08-04 09:41:33.048 |        ACDC |  4.0450 |  436 |            USD |  4.05 |  4.11 | ... |  4.0600 |  4.000 |   ACDC | 2026-08-04 09:40:39.174113 |      4.03 |      4.05 | 2026-08-04 09:41:32.571879 |
| 996 | 2026-08-04 09:41:33.048 |        ACCS |  6.6500 |  100 |            USD |  6.74 |  6.64 | ... |  6.7400 |  6.650 |   ACCS | 2026-08-04 09:32:46.931936 |      6.60 |      7.00 | 2026-08-04 09:41:30.002186 |
| 997 | 2026-08-04 09:41:33.048 |        SPHL |  2.6171 |  189 |            USD |  2.64 |  2.60 | ... |     NaN |    NaN |   SPHL | 2026-08-04 09:01:57.854878 |      2.61 |      2.70 | 2026-08-04 09:30:17.000319 |
| 998 | 2026-08-04 09:41:33.048 |        ATPC |  2.3400 |  132 |            USD |  2.30 |  2.32 | ... |  2.4000 |  2.190 |   ATPC | 2026-08-04 09:41:05.238407 |      2.33 |      2.36 | 2026-08-04 09:40:46.479959 |
| 999 | 2026-08-04 09:41:33.048 |        ATOS |  2.2216 |  250 |            USD |  2.18 |  2.17 | ... |  2.2800 |  2.180 |   ATOS | 2026-08-04 09:36:02.981678 |      2.23 |      2.34 | 2026-08-04 09:41:18.537418 |

1000 rows x 20 columns

## Latest Trade Market Snapshot - Trade Retrieval Across All Symbols in Database

The *LATEST* databases provide last value caches, storing the latest prices for each instrument.  
*LATEST* databases are available for all real time sources.  
They can only be accessed by those who are entitled to access real time data.  
The ``SNAP_TRD`` table includes latest trade for every symbol.  
Querying by ``SYMBOL_NAME`` returns all symbols.
To filter by symbol, please use the ``SYMBOL`` field.

```python
import onetick.py as otp

data = otp.DataSource(db='US_COMP_LATEST', tick_type='SNAP_TRD')
data = data.limit(1000)
result = otp.run(data,
                 # The Start and End Times are set using NOW
                 start=otp.now() - otp.Second(1),
                 end=otp.now(),
                 timezone='America/New_York',
                 symbols='-')
result
```

{.dataframe}
|     |                    Time | SYMBOL_NAME |   PRICE | SIZE | TRADE_CURRENCY |  OPEN | CLOSE | ... |  VOLUME | VOLUME_MAIN_SESSION | VOLUME_EXTENDED |    HIGH |    LOW | SYMBOL |                  TICK_TIME |
|----:|------------------------:|------------:|--------:|-----:|---------------:|------:|------:|----:|--------:|--------------------:|----------------:|--------:|-------:|-------:|---------------------------:|
|   0 | 2026-08-04 09:49:14.298 |        XXRP | 21.1600 |  200 |            USD | 21.20 | 21.34 | ... |   51628 |               42505 |            9123 | 21.3400 | 21.145 |   XXRP | 2026-08-04 09:48:54.555459 |
|   1 | 2026-08-04 09:49:14.298 |        SMUP |  5.2500 |  100 |            USD |  5.03 |  4.87 | ... |   29120 |               17116 |           12004 |  5.3900 |  5.030 |   SMUP | 2026-08-04 09:44:20.531986 |
|   2 | 2026-08-04 09:49:14.298 |         SMX | 17.4000 |  284 |            USD | 15.01 | 16.53 | ... |     384 |                   0 |             384 |     NaN |    NaN |    SMX | 2026-08-04 09:28:41.845362 |
|   3 | 2026-08-04 09:49:14.298 |         SMU |  6.5700 |  100 |            USD |  6.41 |  6.18 | ... |  174156 |              120947 |           53209 |  6.8795 |  6.370 |    SMU | 2026-08-04 09:49:12.550636 |
|   4 | 2026-08-04 09:49:14.298 |         SMR |  9.2950 |  400 |            USD |  9.19 |  9.01 | ... | 3241770 |             2754069 |          487701 |  9.5400 |  9.190 |    SMR | 2026-08-04 09:49:14.166112 |
| ... |                     ... |         ... |     ... |  ... |            ... |   ... |   ... | ... |     ... |                 ... |             ... |     ... |    ... |    ... |                        ... |
| 995 | 2026-08-04 09:49:14.298 |        ACDC |  4.0600 |  100 |            USD |  4.05 |  4.11 | ... |   29697 |               26503 |            3194 |  4.0600 |  4.000 |   ACDC | 2026-08-04 09:49:15.084007 |
| 996 | 2026-08-04 09:49:14.298 |        ACCS |  6.8000 |  100 |            USD |  6.74 |  6.64 | ... |     436 |                 436 |               0 |  6.8000 |  6.640 |   ACCS | 2026-08-04 09:44:14.519060 |
| 997 | 2026-08-04 09:49:14.298 |        SPHL |  2.6171 |  189 |            USD |  2.64 |  2.60 | ... |     189 |                   0 |             189 |     NaN |    NaN |   SPHL | 2026-08-04 09:01:57.854878 |
| 998 | 2026-08-04 09:49:14.298 |        ATPC |  2.3700 |  100 |            USD |  2.30 |  2.32 | ... | 4472663 |              136504 |         4336159 |  2.4200 |  2.190 |   ATPC | 2026-08-04 09:48:58.605412 |
| 999 | 2026-08-04 09:49:14.298 |        ATOS |  2.2800 |  700 |            USD |  2.18 |  2.17 | ... |   13582 |               13165 |             417 |  2.3321 |  2.180 |   ATOS | 2026-08-04 09:44:28.298270 |

1000 rows x 17 columns

## Returns Todays Trades

Data will only be returned for those who are entitled to access real time data.

```python
import onetick.py as otp

# The US_COMP_REPLAY database replays older data, and is available to all.
data = otp.DataSource(db='US_COMP_REPLAY', tick_type='TRD')

# Limit to First 1000 Trades
data = data.limit(1000)

result = otp.run(data,
                 # get the current date == start of day
                 start=otp.now().dt.date(),
                 end=otp.now(),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

{.dataframe}
|     |                       Time |                     EXCH_TIME |                      TRF_TIME | EXCHANGE | ... | COND | TICK_STATUS |        DELETED_TIME | OMDSEQ |
|----:|---------------------------:|------------------------------:|------------------------------:|---------:|----:|-----:|------------:|--------------------:|-------:|
|   0 | 2026-08-03 04:01:04.954767 | 2026-08-03 04:00:00.504781214 | 1969-12-31 19:00:00.000000000 |        P | ... | @FTI |           0 | 1969-12-31 19:00:00 |     19 |
|   1 | 2026-08-03 04:01:04.995332 | 2026-08-03 04:00:00.545682770 | 1969-12-31 19:00:00.000000000 |        P | ... | @ TI |           0 | 1969-12-31 19:00:00 |     10 |
|   2 | 2026-08-03 04:01:04.995334 | 2026-08-03 04:00:00.545682770 | 1969-12-31 19:00:00.000000000 |        P | ... | @ TI |           0 | 1969-12-31 19:00:00 |     11 |
|   3 | 2026-08-03 04:01:05.059417 | 2026-08-03 04:00:00.609225364 | 1969-12-31 19:00:00.000000000 |        K | ... | @FTI |           0 | 1969-12-31 19:00:00 |      0 |
|   4 | 2026-08-03 04:01:05.083970 | 2026-08-03 04:00:00.546032382 | 2026-08-03 04:00:00.634145519 |        D | ... | @ TI |           0 | 1969-12-31 19:00:00 |      8 |
| ... |                        ... |                           ... |                           ... |      ... | ... |  ... |         ... |                 ... |    ... |
| 995 | 2026-08-03 07:35:04.553968 | 2026-08-03 07:34:00.095450601 | 2026-08-03 07:34:00.103777380 |        D | ... | @ TI |           0 | 1969-12-31 19:00:00 |      0 |
| 996 | 2026-08-03 07:35:35.714718 | 2026-08-03 07:34:31.264478680 | 2026-08-03 07:34:31.264696817 |        D | ... | @ TI |           0 | 1969-12-31 19:00:00 |      0 |
| 997 | 2026-08-03 07:36:14.709390 | 2026-08-03 07:35:09.695728000 | 2026-08-03 07:35:10.259392851 |        D | ... | C TI |           0 | 1969-12-31 19:00:00 |      0 |
| 998 | 2026-08-03 07:36:15.090211 | 2026-08-03 07:35:10.509138000 | 2026-08-03 07:35:10.640662687 |        D | ... | C TI |           0 | 1969-12-31 19:00:00 |      0 |
| 999 | 2026-08-03 07:36:16.067989 | 2026-08-03 07:35:10.597008000 | 2026-08-03 07:35:11.617630548 |        D | ... | C TI |           0 | 1969-12-31 19:00:00 |      0 |

1000 rows x 21 columns

## Returns Recent Trades

Data will only be returned for those who are entitled to access real time data.

```python
import onetick.py as otp

# The US_COMP_REPLAY database replays older data, and is available to all.
data = otp.DataSource(db='US_COMP_REPLAY', tick_type='TRD')

# Limit to First 1000 Trades
data = data.limit(1000)

result = otp.run(data,
                 # The Start and End Times are set using NOW, and TIMEDELTA and apply the timezone
                 start=otp.now() - otp.Minute(5),
                 end=otp.now(),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

{.dataframe}
|     |                       Time |                     EXCH_TIME |                      TRF_TIME | EXCHANGE | ... | COND | TICK_STATUS |        DELETED_TIME | OMDSEQ |
|----:|---------------------------:|------------------------------:|------------------------------:|---------:|----:|-----:|------------:|--------------------:|-------:|
|   0 | 2026-08-03 10:16:13.252999 | 2026-08-03 10:15:08.797520000 | 2026-08-03 10:15:08.802919026 |        D | ... | @  I |           0 | 1969-12-31 19:00:00 |      5 |
|   1 | 2026-08-03 10:16:13.253007 | 2026-08-03 10:15:08.799097000 | 2026-08-03 10:15:08.804125110 |        D | ... | @  I |           0 | 1969-12-31 19:00:00 |     13 |
|   2 | 2026-08-03 10:16:13.552707 | 2026-08-03 10:15:09.103285249 | 1969-12-31 19:00:00.000000000 |        Q | ... | @F I |           0 | 1969-12-31 19:00:00 |     43 |
|   3 | 2026-08-03 10:16:13.555496 | 2026-08-03 10:15:09.104806783 | 2026-08-03 10:15:09.105502853 |        D | ... |    @ |           0 | 1969-12-31 19:00:00 |    837 |
|   4 | 2026-08-03 10:16:13.556629 | 2026-08-03 10:15:09.104876699 | 2026-08-03 10:15:09.105857660 |        D | ... |    @ |           0 | 1969-12-31 19:00:00 |    103 |
| ... |                        ... |                           ... |                           ... |      ... | ... |  ... |         ... |                 ... |    ... |
| 995 | 2026-08-03 10:18:13.720910 | 2026-08-03 10:17:09.270806368 | 2026-08-03 10:17:09.271032293 |        D | ... | @  I |           0 | 1969-12-31 19:00:00 |     45 |
| 996 | 2026-08-03 10:18:14.218489 | 2026-08-03 10:17:09.768635898 | 2026-08-03 10:17:09.769076138 |        D | ... | @4 I |           0 | 1969-12-31 19:00:00 |     20 |
| 997 | 2026-08-03 10:18:14.404376 | 2026-08-03 10:17:09.954062918 | 1969-12-31 19:00:00.000000000 |        V | ... |    @ |           0 | 1969-12-31 19:00:00 |      2 |
| 998 | 2026-08-03 10:18:14.407793 | 2026-08-03 10:17:09.957284178 | 2026-08-03 10:17:09.957672243 |        D | ... | @4 I |           0 | 1969-12-31 19:00:00 |     60 |
| 999 | 2026-08-03 10:18:14.483209 | 2026-08-03 10:17:10.033782135 | 2026-08-03 10:17:10.034163060 |        D | ... | @4 W |           0 | 1969-12-31 19:00:00 |     63 |

1000 rows x 21 columns
