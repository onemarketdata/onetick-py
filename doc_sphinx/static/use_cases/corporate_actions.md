---
file_format: mystnb
---

# Corporate Actions

This section contains 7 examples for Corporate Actions using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Adjusting for Additive Dividends by PRICE

Retrieve corporate action adjusted daily price history for additive cash dividends, including the original prices
using {meth}`~onetick.py.Source.corp_actions` with parameters `adjust_rule='PRICE',  apply_cash_dividend=True`.  
Additive cash dividends can produce negative adjusted price histories across long time horizons.  
Adjusting for Multiplicative Dividends does not have this problem and is recommended instead.  

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_DAILY', tick_type='DAY')
data = data[['CLOSE', 'VOLUME', 'EXCHANGE']]
data = data.where(data['EXCHANGE'] == '')
data['ORIG_CLOSE'] = data['CLOSE']
data = data.corp_actions(fields='CLOSE', adjust_rule='PRICE', apply_cash_dividend=True)
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='America/New_York',
                 symbols='AAPL',
                 symbol_date=otp.dt.now())
result
```

## Adjusting for Corporate Actions by PRICE and SIZE

Retrieve corporate action adjusted daily price history for splits, including the original prices
where the closing price and volume should be adjusted in different ways due to the split
using {meth}`~onetick.py.Source.corp_actions` with parameters `adjust_rule='...', apply_split=True`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_DAILY', tick_type='DAY')
data = data[['CLOSE', 'VOLUME', 'EXCHANGE']]
data = data.where(data['EXCHANGE'] == '')
data['ORIG_CLOSE'] = data['CLOSE']
data['ORIG_VOLUME'] = data['VOLUME']
data = data.corp_actions(fields='CLOSE', adjust_rule='PRICE', apply_split=True)
data = data.corp_actions(fields='VOLUME', adjust_rule='SIZE', apply_split=True)
result = otp.run(data,
                 start=otp.dt(2024, 2, 10),
                 end=otp.dt(2024, 3, 10),
                 timezone='America/New_York',
                 symbols='WMT',
                 symbol_date=otp.dt.now())
result
```

## Adjusting for Multiplicative Dividends by PRICE

Retrieve corporate action adjusted daily price history for additive cash dividends, including the original prices
using {meth}`~onetick.py.Source.corp_actions` with parameters `adjust_rule='PRICE', apply_others='MULTI_ADJ_CASH'`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_DAILY', tick_type='DAY')
data = data[['CLOSE', 'VOLUME', 'EXCHANGE']]
data = data.where(data['EXCHANGE'] == '')
data['ORIG_CLOSE'] = data['CLOSE']
data = data.corp_actions(fields='CLOSE', adjust_rule='PRICE', apply_others='MULTI_ADJ_CASH')
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='America/New_York',
                 symbols='AAPL',
                 symbol_date=otp.dt.now())
result
```

## Adjusting for Splits by PRICE

Retrieve corporate action adjusted daily price history for splits, including the original prices
using {meth}`~onetick.py.Source.corp_actions` with parameters `adjust_rule='PRICE', apply_split=True`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_DAILY', tick_type='DAY')
data = data[['CLOSE', 'VOLUME', 'EXCHANGE']]
data = data.where(data['EXCHANGE'] == '')
data['ORIG_CLOSE'] = data['CLOSE']
data = data.corp_actions(fields='CLOSE', adjust_rule='PRICE', apply_split=True)
result = otp.run(data,
                 start=otp.dt(2024, 2, 10),
                 end=otp.dt(2024, 3, 10),
                 timezone='America/New_York',
                 symbols='WMT',
                 symbol_date=otp.dt.now())
result
```

## Retrieving Adjustment Factors

Retrieving corporate action adjustment factors across a specified time range.  
Using {meth}`~onetick.py.db._inspection.DB.ref_data` with parameter `ref_data_type='corp_actions'`.

```{code-cell} ipython3

import onetick.py as otp

dbs = otp.databases()
db = otp.databases()['US_COMP_SAMPLE_DAILY']
result = db.ref_data(ref_data_type='corp_actions',
                     symbol='WMT',
                     start=otp.dt(2024, 2, 10),
                     end=otp.dt(2024, 3, 10),
                     symbol_date=otp.dt.now())
result
```

## Retrieving all OQD Corporate Actions for Period

Quering corporate action history from OQD for all symbols.  
Specifically ``OQD_CACT_SAMPLE``, and table ``CACT``.  
The Symbol is specified as ``PAY_DATE``, ``ANN_DATE``, ``EX_DATE`` or ``REC_DATE``.

```python
import onetick.py as otp

data = otp.DataSource(db='OQD_CACT_SAMPLE', tick_type='CACT')
result = otp.run(data,
                 start=otp.dt(2024, 2, 1),
                 end=otp.dt(2024, 2, 2),
                 timezone='UTC',
                 # Or ANN_DATE, EX_DATE or REC_DATE
                 symbols=['PAY_DATE'])
result
```

{.dataframe}
|      |       Time |        OID | ACTION_ID |   ACTION_TYPE | ACTION_ADJUST | ACTION_CURRENCY | ACTION_DATE | DELETED_TIME | TICK_STATUS | OMDSEQ |
|-----:|-----------:|-----------:|----------:|--------------:|--------------:|-----------------|------------:|-------------:|------------:|-------:|
|    0 | 2024-02-01 | 1000012662 |   7197356 | CASH_DIVIDEND |      0.266044 |             USD |    20240201 |   1970-01-01 |           0 |  21116 |
|    1 | 2024-02-01 | 1000012663 |   7197357 | CASH_DIVIDEND |      0.090344 |             EUR |    20240201 |   1970-01-01 |           0 |  21117 |
|    2 | 2024-02-01 | 1000012667 |   7197358 | CASH_DIVIDEND |      0.347669 |             USD |    20240201 |   1970-01-01 |           0 |  21118 |
|    3 | 2024-02-01 | 1000013831 |   7198677 | CASH_DIVIDEND |      0.266044 |             USD |    20240201 |   1970-01-01 |           0 |  21119 |
|    4 | 2024-02-01 | 1000013832 |   7198678 | CASH_DIVIDEND |      0.347669 |             USD |    20240201 |   1970-01-01 |           0 |  21120 |
|  ... |        ... |        ... |       ... |           ... |           ... |             ... |         ... |          ... |         ... |    ... |
| 2864 | 2024-02-01 |      97063 |  17951542 | CASH_DIVIDEND |      0.350394 |             USD |    20240201 |   1970-01-01 |           0 |  23980 |
| 2865 | 2024-02-01 |      99218 |  17997558 | CASH_DIVIDEND |      0.028032 |             USD |    20240201 |   1970-01-01 |           0 |  23981 |
| 2866 | 2024-02-01 |      99220 |  17997559 | CASH_DIVIDEND |      0.018951 |             USD |    20240201 |   1970-01-01 |           0 |  23982 |
| 2867 | 2024-02-01 |      99221 |  17997560 | CASH_DIVIDEND |      0.028018 |             USD |    20240201 |   1970-01-01 |           0 |  23983 |
| 2868 | 2024-02-01 |      99318 |  17937167 | CASH_DIVIDEND |      0.450000 |             USD |    20240201 |   1970-01-01 |           0 |  23984 |

2869 rows x 10 columns

## Retrieving OQD Corporate Actions for Bloomberg Symbol

Quering corporate action history from OQD.  
Specifically ``OQD_CACT_SAMPLE``, and table ``CACS``, for two symbols.  
Bloomberg symbols have been specified using the prefix ``BSYM::::``.

```python
import onetick.py as otp

data = otp.DataSource(db='OQD_CACT_SAMPLE', tick_type='CACS')
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='UTC',
                 symbols=['BSYM::::WMT US Equity', 'BSYM::::AAPL US Equity'],
                 symbol_date=otp.dt.now())
result
```

```text
{'BSYM::::WMT US Equity':
        Time     OID  ACTION_ID    ACTION_TYPE  ACTION_ADJUST ACTION_CURRENCY  ANN_DATE   EX_DATE  PAY_DATE  REC_DATE                TERM_NOTE TERM_RECORD_TYPE ACTION_STATUS DELETED_TIME  TICK_STATUS  OMDSEQ
0 2024-02-26  203776   17992444          SPLIT       0.333333                  20240130  20240226  20240223  20240222  STOCK:2.00000000@203776                         NORMAL   1970-01-01            0     380
1 2024-03-14  203776   18004769  CASH_DIVIDEND       0.207500             USD  20240220  20240314  20240401  20240315          CASH:0.2075@USD                         NORMAL   1970-01-01            0    2864,
 'BSYM::::AAPL US Equity':
        Time   OID  ACTION_ID    ACTION_TYPE  ACTION_ADJUST ACTION_CURRENCY  ANN_DATE   EX_DATE  PAY_DATE  REC_DATE      TERM_NOTE TERM_RECORD_TYPE ACTION_STATUS DELETED_TIME  TICK_STATUS  OMDSEQ
0 2024-02-09  9706   17996883  CASH_DIVIDEND           0.24             USD  20240201  20240209  20240215  20240212  CASH:0.24@USD                         NORMAL   1970-01-01            0     559
}
```
