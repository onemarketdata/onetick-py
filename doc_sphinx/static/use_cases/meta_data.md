---
file_format: mystnb
---

# Meta Data

This section contains 5 examples for Meta Data using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## DB List

Use {func}`otp.databases <onetick.py.databases>` to retrieve the list of available databases, and filter on the first 5.

```{code-cell} ipython3

import onetick.py as otp

data = otp.databases()
list(data)[:5]
```

## DB Symbol List

Use {class}`otp.Symbols <onetick.py.Symbols>` to retrieve the list of available symbols for a given database.

```{code-cell} ipython3

import onetick.py as otp

data = otp.Symbols(db='US_COMP_SAMPLE')
result = otp.run(data,
                 start=otp.dt(2024, 1, 2),
                 end=otp.dt(2024, 1, 3),
                 timezone='America/New_York')
result
```

## Field Schema

Retrieve the schema from a specified data source where the database and tick type are selected.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='CSCO',
                      start=otp.dt(2024, 1, 3), end=otp.dt(2024, 1, 4))
data.schema
```

## Masked DB Symbol List

Apply a pattern using the wildcard ``%`` to retrieve all symbols starting with `C`
with {class}`otp.Symbols <onetick.py.Symbols>`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.Symbols(db='US_COMP_SAMPLE', pattern='C%')
result = otp.run(data,
                 start=otp.dt(2024, 1, 2),
                 end=otp.dt(2024, 1, 3),
                 timezone='America/New_York')
result
```

## Tick Type - Table List

Retrieve the list of databases, and then for a specified database, retrieve the available tick types / tables.

```{code-cell} ipython3

import onetick.py as otp

dbs = otp.databases()
result = dbs['US_COMP_SAMPLE'].tick_types()
result
```
