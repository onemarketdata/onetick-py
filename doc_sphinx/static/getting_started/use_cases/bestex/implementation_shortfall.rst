Implementation shortfall (slippage)
===================================
IS -- `implementation shortfall`, or slippage.  Large positive values imply a good execution while large negative values imply a poor execution.

::

    IS := Direction * Executed_QTY * (Arrival_Mid_Price - VWAP)

To calculate the Implementation Shortfall (IS) for each order, we need to determine the direction of the order, the executed quantity (sum of QTY_FILLED), the arrival mid-price, and the Volume Weighted Average Price (VWAP) of the order. We will then apply the provided formula to calculate the IS. Here's how you can do it using ``onetick.py``:

.. testcode::

    import onetick.py as otp

    # Define the symbol, orders database, quotes database, and date
    symbol = 'TSLA'
    orders_db = 'ORDERS_DB'
    quotes_db = 'US_COMP'
    date = otp.dt(2022, 3, 2)

    # Load orders and quotes data
    orders = otp.DataSource(orders_db, tick_type='ORDER', symbol=symbol)
    quotes = otp.DataSource(quotes_db, tick_type='QTE', symbol=symbol)

    # Filter for new (arrival) orders
    arrival_orders, other_orders = orders[(orders['STATE'] == 'N')]

    # Join arrival orders with quotes based on timestamp
    arrival_orders_with_quotes = otp.join_by_time([arrival_orders, quotes])

    # Calculate current mid price for each quote tick
    arrival_orders_with_quotes['MID_PRICE'] = (arrival_orders_with_quotes['ASK_PRICE'] + arrival_orders_with_quotes['BID_PRICE']) / 2

    # Merge all ticks back to apply aggregation properly
    merged_orders = arrival_orders_with_quotes + other_orders

    # Aggregate to carry forward arrival mid price, VWAP, 'SIDE' field, and sum of 'QTY_FILLED'
    orders_agg = merged_orders.agg({
        'ARRIVAL_MID_PRICE': otp.agg.first('MID_PRICE'),
        'SIDE': otp.agg.first('SIDE'),
        'VWAP': otp.agg.vwap('PRICE_FILLED', 'QTY_FILLED'),
        'EXECUTED_QTY': otp.agg.sum('QTY_FILLED')
    }, group_by='ID')

    # Calculate Direction (1 for BUY, -1 for SELL)
    orders_agg['DIRECTION'] = orders_agg.apply(lambda tick: 1 if tick['SIDE'] == 'BUY' else -1)

    # Calculate IS (Implementation Shortfall)
    orders_agg['IS'] = orders_agg['DIRECTION'] * orders_agg['EXECUTED_QTY'] * (orders_agg['ARRIVAL_MID_PRICE'] - orders_agg['VWAP'])

    # Select relevant fields
    orders_with_is = orders_agg[['ID', 'IS']]

    # Run the query for the specified date
    df = otp.run(orders_with_is, date=date)
