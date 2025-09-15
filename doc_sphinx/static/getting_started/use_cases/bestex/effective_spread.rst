Effective spread
================
To calculate the `effective spread` of your orders, you'll need to first calculate the Volume Weighted Average Price (VWAP) for each order and determine the prevailing mid-price (average of bid and ask prices) at the time of each order execution. Then, you can use the formula for the effective

::

    Effective_Spread := Executed_QTY x (VWAP - Mid_Price) x 2 x Direction

Here's a full code example to calculate the effective spread using ``onetick.py``:


.. testcode::

    import onetick.py as otp

    # Define your symbol, orders database, and quotes database
    symbol = 'TSLA'
    orders_db = 'ORDERS_DB'
    quotes_db = 'US_COMP'
    date = otp.dt(2022, 3, 2)

    # Load orders and quotes data
    orders = otp.DataSource(orders_db, tick_type='ORDER', symbol=symbol)
    quotes = otp.DataSource(quotes_db, tick_type='QTE', symbol=symbol)

    # Join orders with quotes based on timestamp
    joined_data = otp.join_by_time([orders, quotes])

    # Calculate the mid-price of prevailing quotes
    joined_data['MID_PRICE'] = (joined_data['ASK_PRICE'] + joined_data['BID_PRICE']) / 2

    # Calculate VWAP for each order grouped by ID
    # Take the first MID_PRICE because effective spread uses the price prevailing at the time the order arrived
    # Propagate BUY_FLAG to calculate then DIRECTION
    joined_data = joined_data.agg({'VWAP': otp.agg.vwap('PRICE_FILLED', 'QTY_FILLED'),
                                   'EXEC_QTY': otp.agg.sum('QTY_FILLED'),
                                   'MID_PRICE': otp.agg.first('MID_PRICE'),
                                   'BUY_FLAG': otp.agg.first('BUY_FLAG')},
                                   group_by='ID')

    # Add direction (1 for buy orders and -1 for sell orders)
    joined_data['DIRECTION'] = joined_data['BUY_FLAG'] * 2 - 1

    # Calculate effective spread
    joined_data['EFFECTIVE_SPREAD'] = 2 * joined_data['DIRECTION'] * joined_data['EXEC_QTY'] * (joined_data['VWAP'] - joined_data['MID_PRICE'])

    # Run the query for the specified date
    df = otp.run(joined_data, date=date)
