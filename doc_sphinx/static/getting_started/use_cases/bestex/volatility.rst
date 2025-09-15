Volatility
==========
To calculate the `volatility` of a financial instrument using ``onetick.py``, you'll need to retrieve trade data (typically with the 'TRD' tick type) from the US_COMP database and then apply your defined function to calculate the volatility. The volatility is computed as the standard deviation of the trade prices divided by their average, multiplied by 100 (to get a percentage).


.. testcode::

    import onetick.py as otp

    # Define your symbol and the database
    symbol = 'TSLA'
    database = 'US_COMP'
    date = otp.dt(2023, 3, 2)

    # Load trades data from US_COMP database
    trades = otp.DataSource(database, tick_type='TRD', symbol=symbol)

    def add_volatility(md: otp.Source):
        md = md.agg({
            'STDDEV': otp.agg.stddev('PRICE'),
            'AVERAGE': otp.agg.average('PRICE'),
        })
        md['VOLATILITY'] = md['STDDEV'] / md['AVERAGE'] * 100
        return md

    # Apply the volatility function
    volatility_data = add_volatility(trades)

    # Run the query for the specified date
    df = otp.run(volatility_data, date=date)
