Notional spread (quoted spread)
===============================
To calculate the `notional spread` for every client's order in onetick.py, you first need to determine the Arrival_Ask_Price and Arrival_Bid_Price for each order at its arrival time, then calculate the notional spread using the formula:

::

    Notional_Spread := Order_Executed_Qty * (Arrival_Ask_Price - Arrival_Bid_Price)


Example of code in ``onetick.py``:

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

    # merge all ticks back with goal to apply aggregation properly then
    # especially to calculate 'QTY_FILLED' correctly
    merged_orders = arrival_orders_with_quotes + other_orders

    # Aggregate to get total executed quantity (QTY_FILLED) for each order
    # and carry forward arrival ask and bid prices
    orders_agg = merged_orders.agg({
        'EXEC_QTY': otp.agg.sum('QTY_FILLED'),
        'ARRIVAL_ASK_PRICE': otp.agg.first('ASK_PRICE'),
        'ARRIVAL_BID_PRICE': otp.agg.first('BID_PRICE')
    }, group_by='ID')

    # Calculate the notional spread for each order
    orders_agg['NOTIONAL_SPREAD'] = orders_agg['EXEC_QTY'] * (orders_agg['ARRIVAL_ASK_PRICE'] - orders_agg['ARRIVAL_BID_PRICE'])

    # Select relevant fields
    orders_with_notional_spread = orders_agg[['ID', 'NOTIONAL_SPREAD']]

    # Run the query for the specified date
    df = otp.run(orders_with_notional_spread, date=date)
