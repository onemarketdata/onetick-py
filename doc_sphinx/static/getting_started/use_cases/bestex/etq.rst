Effective to quoted spread (ETQ)
================================
`ETQ` is a widely-used metric for small order instantaneous price impact. It is essentially an easier-to-interpret Effective Spread that (when aggregated) normalizes for the usual spread differences between instruments. ETQ is based on the regulatory Effective Spread Value, though ETQ is not itself a regulator-mandated metric. Like Effective Spread, ETQ is meaningful only for aggressive orders that execute immediately.

Formula to get ETQ

::

    ETQ := (VWAP - Mid_Price) * 2 * Direction / (Ask_Price - Bid_Price)

To calculate the Effective to Quoted Spread Ratio (ETQ) for every order, we need to determine the VWAP, Mid_Price, Ask_Price, Bid_Price, and the direction of each order. The ETQ is then calculated using the provided formula. Here's the onetick.py code to do this:

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

    # Filter for new (arrival) orders and get other orders
    arrival_orders, other_orders = orders[(orders['STATE'] == 'N')]

    # Join arrival orders with quotes based on timestamp
    arrival_orders_with_quotes = otp.join_by_time([arrival_orders, quotes])

    # Merge all ticks back to apply aggregation properly
    merged_orders = arrival_orders_with_quotes + other_orders

    # Aggregate to carry forward ask and bid prices, along with VWAP, 'SIDE' field, and mid-price
    orders_agg = merged_orders.agg({
        'ASK_PRICE': otp.agg.first('ASK_PRICE'),
        'BID_PRICE': otp.agg.first('BID_PRICE'),
        'SIDE': otp.agg.first('SIDE'),
        'VWAP': otp.agg.vwap('PRICE_FILLED', 'QTY_FILLED')
    }, group_by='ID')

    # Calculate Mid_Price
    orders_agg['MID_PRICE'] = (orders_agg['ASK_PRICE'] + orders_agg['BID_PRICE']) / 2

    # Calculate Direction (1 for BUY, -1 for SELL)
    orders_agg['DIRECTION'] = orders_agg.apply(lambda tick: 1 if tick['SIDE'] == 'BUY' else -1)

    # Calculate ETQ
    orders_agg['ETQ'] = (orders_agg['VWAP'] - orders_agg['MID_PRICE']) * 2 * orders_agg['DIRECTION'] / (orders_agg['ASK_PRICE'] - orders_agg['BID_PRICE'])

    # Select relevant fields
    orders_with_etq = orders_agg[['ID', 'ETQ']]

    # Run the query for the specified date
    df = otp.run(orders_with_etq, date=date)
