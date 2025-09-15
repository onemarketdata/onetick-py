Number spreads
==============
The `number spreads` is a metric that defines how far an order is executed from the best bid and best ask according to the following formula:

::

    Num_Spreads := abs((VWAP - FT) / (FT - NT)), and nan() when (FT - NT) = 0

To calculate the Num_Spreads metric for each order, you need to first determine the VWAP, Near Touch (NT), and Far Touch (FT) for each order, and then use these values to compute Num_Spreads according to the provided formula. Here's how you can do it in ``onetick.py``:

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

    # Calculate NT and FT for each order using lambda functions
    orders_agg['NT'] = orders_agg.apply(lambda tick: orders_agg['ARRIVAL_BID_PRICE'] if tick['SIDE'] == 'BUY' else orders_agg['ARRIVAL_ASK_PRICE'])
    orders_agg['FT'] = orders_agg.apply(lambda tick: orders_agg['ARRIVAL_ASK_PRICE'] if tick['SIDE'] == 'BUY' else orders_agg['ARRIVAL_BID_PRICE'])

    # Calculate NUM_SPREADS
    orders_agg['NUM_SPREADS'] = orders_agg.apply(lambda tick: abs((tick['VWAP'] - tick['FT']) / (tick['FT'] - tick['NT'])) if (tick['FT'] - tick['NT']) != 0 else otp.nan)

    # Select relevant fields
    orders_with_num_spreads = orders_agg[['ID', 'NUM_SPREADS']]

    # Run the query for the specified date
    df = otp.run(orders_with_num_spreads, date=date)
