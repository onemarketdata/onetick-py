---
file_format: mystnb
---

# Data Retrieval with Continuous Contracts

This section contains 6 examples for Data Retrieval with Continuous Contracts using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## DAY Retrieval Cont Contract by Max Open Interest

Retrieve the Front Month Continuous Contract based on Maximum Open Interest.  
Using Symbol Syntax: ``[Product Code]_r_oi``.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='ICE_EU_COM_SAMPLE_DAILY', tick_type='DAY')
data = data.where(data['UPDATE_TYPE'] == 'Summary')
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='Europe/London',
                 symbols='BRN_r_oi',
                 symbol_date=otp.dt(2024, 4, 1))
result
```

## DAY Retrieval Cont Contract by Max Volume

Retrieve the Front Month Continuous Contract based on Maximum Volume.  
Using Symbol Syntax: ``[Product Code]_r_vol``.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='ICE_EU_COM_SAMPLE_DAILY', tick_type='DAY')
data = data.where(data['UPDATE_TYPE'] == 'Summary')
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='Europe/London',
                 symbols='BRN_r_vol',
                 symbol_date=otp.dt(2024, 4, 1))
result
```

## DAY Retrieval Cont Contract by Max Volume with Bloomberg Symbology

Front Month Continous Contract by Max Volume can be specified with Bloomberg ``BSYM`` symbology:
``[Bloomberg Product code]A``.  
For example Brent Crude (exchange symbol ``BRN``), has Bloomberg Product code ``CO``.  
Instead of ``BRN_r_vol``, ``COA Comdty`` can be specified.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='ICE_EU_COM_SAMPLE_DAILY', tick_type='DAY')
data = data.where(data['UPDATE_TYPE'] == 'Summary')
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='Europe/London',
                 symbols='BSYM::::COA Comdty',
                 symbol_date=otp.dt(2024, 4, 1))
result
```

## DAY Retrieval Cont Contract by Month (1-12)

Front Month to Twelve Month Continuous Contract can be specified
``[Product code]\1`` to ``[Product code]\12``.  
Rolls based on contract expiry.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='ICE_EU_COM_SAMPLE_DAILY', tick_type='DAY')
data = data.where(data['UPDATE_TYPE'] == 'Summary')
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='Europe/London',
                 symbols='BRN\\1',
                 symbol_date=otp.dt(2024, 4, 1))
result
```

## DAY Retrieval Cont Contract by Tick Data Method

Retrieve the Front Month Continuous Contract based on Tick Data Methology (only relevant for ``TDI_FUT``).  
Using Symbol Syntax: ``[Product Code]_r_tdi``.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='TDI_FUT_SAMPLE_DAILY', tick_type='DAY')
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='Europe/London',
                 symbols='CO_r_tdi',
                 symbol_date=otp.dt(2024, 4, 1))
result
```

## DAY Retrieval Cont Contract with Bloomberg Symbology

Front Month to Twelve Month Continuous Contract can be specified with Bloomberg ``BSYM`` symbology
``[Product code]1`` to ``[Product code]12``.  
Rolls based on contract expiry.  
For example Brent Crude (exchange symbol ``BRN``), has Bloomberg Product code ``CO``.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='ICE_EU_COM_SAMPLE_DAILY', tick_type='DAY')
data = data.where(data['UPDATE_TYPE'] == 'Summary')
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 4, 1),
                 timezone='Europe/London',
                 symbols='BSYM::::CO1 Comdty',
                 symbol_date=otp.dt(2024, 4, 1))
result
```
