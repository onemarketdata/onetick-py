Offside value
=============
`Offside value` is a measure of the impact of an aggressive order. It is the distance in notional units that the impacted side moved from the best price prevailing at the time the order arrived.
The formula to get offside value

::

    Offside_Value := Executed_QTY * (VWAP - FT) * Direction

To calculate the offside value for orders, you need to determine the executed quantity (sum of QTY_FILLED), the VWAP, the Far Touch (FT), and the direction of each order. Here's how you can do it using ``onetick.py``:

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

    # Merge all ticks back to apply aggregation properly
    merged_orders = arrival_orders_with_quotes + other_orders

    # Aggregate to carry forward arrival ask and bid prices, along with VWAP, 'SIDE' field, and sum of 'QTY_FILLED'
    orders_agg = merged_orders.agg({
        'ARRIVAL_ASK_PRICE': otp.agg.first('ASK_PRICE'),
        'ARRIVAL_BID_PRICE': otp.agg.first('BID_PRICE'),
        'SIDE': otp.agg.first('SIDE'),
        'VWAP': otp.agg.vwap('PRICE_FILLED', 'QTY_FILLED'),
        'EXECUTED_QTY': otp.agg.sum('QTY_FILLED')
    }, group_by='ID')

    # Calculate FT for each order using lambda functions
    orders_agg['FT'] = orders_agg.apply(lambda tick: orders_agg['ARRIVAL_ASK_PRICE'] if tick['SIDE'] == 'BUY' else orders_agg['ARRIVAL_BID_PRICE'])

    # Calculate Direction (1 for BUY, -1 for SELL)
    orders_agg['DIRECTION'] = orders_agg.apply(lambda tick: 1 if tick['SIDE'] == 'BUY' else -1)

    # Calculate Offside Value
    orders_agg['OFFSIDE_VALUE'] = orders_agg['EXECUTED_QTY'] * (orders_agg['VWAP'] - orders_agg['FT']) * orders_agg['DIRECTION']

    # Select relevant fields
    orders_with_offside_value = orders_agg[['ID', 'OFFSIDE_VALUE']]

    # Run the query for the specified date
    df = otp.run(orders_with_offside_value, date=date)
