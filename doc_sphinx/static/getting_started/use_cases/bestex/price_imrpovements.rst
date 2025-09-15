Price improvement
=================
`Price improvement` -- distance to the far touch (FT) in bps (basis points). Measures the improvement over the aggressive touch at the time the order is placed. Positive values indicate a good execution, negative - a poor one.
Formula to get price improvement:

::

    PriceImprovements_bps := Direction * 10000 * (FT - VWAP) /  (FT)

Where the VWAP is order execution VWAP, FT -- far touch.

To calculate PriceImprovements_bps for each order, you need to determine the VWAP and Far Touch (FT) for each order and then apply the given formula. The formula measures the price improvement in basis points (bps), considering the order direction.

Here's the ``onetick.py`` code:

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

    # Aggregate to carry forward arrival ask and bid prices, along with VWAP and 'SIDE' field
    orders_agg = merged_orders.agg({
        'ARRIVAL_ASK_PRICE': otp.agg.first('ASK_PRICE'),
        'ARRIVAL_BID_PRICE': otp.agg.first('BID_PRICE'),
        'SIDE': otp.agg.first('SIDE'),
        'VWAP': otp.agg.vwap('PRICE_FILLED', 'QTY_FILLED')
    }, group_by='ID')

    # Calculate FT for each order using lambda functions
    orders_agg['FT'] = orders_agg.apply(lambda tick: orders_agg['ARRIVAL_ASK_PRICE'] if tick['SIDE'] == 'BUY' else orders_agg['ARRIVAL_BID_PRICE'])

    # Calculate Direction (1 for BUY, -1 for SELL)
    orders_agg['DIRECTION'] = orders_agg.apply(lambda tick: 1 if tick['SIDE'] == 'BUY' else -1)

    # Calculate PriceImprovements_bps
    orders_agg['PriceImprovements_bps'] = orders_agg['DIRECTION'] * 10000 * (orders_agg['FT'] - orders_agg['VWAP']) / orders_agg['FT']

    # Select relevant fields
    orders_with_price_improvement = orders_agg[['ID', 'PriceImprovements_bps']]

    # Run the query for the specified date
    df = otp.run(orders_with_price_improvement, date=date)
