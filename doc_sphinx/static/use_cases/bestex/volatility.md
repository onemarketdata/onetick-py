---
file_format: mystnb
---

# Volatility

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

A simple realized volatility proxy for an instrument over the interval,
computed as the standard deviation of trade prices divided by their average, in percent.

```text
VOLATILITY = STDDEV(PRICE) / AVERAGE(PRICE) * 100
```

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='TRD')
data = data[['PRICE']]

data = data.agg({
    'STDDEV_PRICE': otp.agg.stddev('PRICE'),
    'AVERAGE_PRICE': otp.agg.average('PRICE'),
})
data['VOLATILITY'] = data['STDDEV_PRICE'] / data['AVERAGE_PRICE'] * 100

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8, 0),
                 end=otp.dt(2024, 1, 3, 16, 30),
                 timezone='Europe/London',
                 symbols='VOD')
result
```
