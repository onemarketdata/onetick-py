FT (far touch) and NT (near touch)
===================================
To find the `Near Touch` (NT) and `Far Touch` (FT) prices for every order, considering the definitions for buy and sell orders, you'll need to process the order data to determine the arrival bid and ask prices, and then calculate NT and FT based on the side of the order. Here's how you can do it using ``onetick.py``

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

    # Aggregate to carry forward arrival ask and bid prices, along with the 'SIDE' field
    orders_agg = merged_orders.agg({
        'ARRIVAL_ASK_PRICE': otp.agg.first('ASK_PRICE'),
        'ARRIVAL_BID_PRICE': otp.agg.first('BID_PRICE'),
        'SIDE': otp.agg.first('SIDE')
    }, group_by='ID')

    # Calculate NT and FT for each order
    # For buy orders: NT = Arrival_Bid_Price, FT = Arrival_Ask_Price
    # For sell orders: NT = Arrival_Ask_Price, FT = Arrival_Bid_Price
    orders_agg['NT'] = orders_agg.apply(lambda tick: orders_agg['ARRIVAL_BID_PRICE'] if tick['SIDE'] == 'BUY' else orders_agg['ARRIVAL_ASK_PRICE'])
    orders_agg['FT'] = orders_agg.apply(lambda tick: orders_agg['ARRIVAL_ASK_PRICE'] if tick['SIDE'] == 'BUY' else orders_agg['ARRIVAL_BID_PRICE'])

    # Select relevant fields
    orders_with_nt_ft = orders_agg[['ID', 'NT', 'FT']]

    # Run the query for the specified date
    df = otp.run(orders_with_nt_ft, date=date)
