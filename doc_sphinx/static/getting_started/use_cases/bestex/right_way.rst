Right way
=========
`Right way` is set to 1 if the mid price moved in the favorable (unfavorable) direction following the execution of an order.

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

    # Calculate NEXT_MID_PRICE for each quote tick
    quotes['NEXT_MID_PRICE'] = (quotes['ASK_PRICE'][+1] + quotes['BID_PRICE'][+1]) / 2

    # Calculate current mid price for each quote tick
    quotes['MID_PRICE'] = (quotes['ASK_PRICE'] + quotes['BID_PRICE']) / 2

    # Join orders with quotes based on timestamp
    joined_data = otp.join_by_time([orders, quotes])

    # Determine Right_Way for each order
    joined_data['Right_Way'] = joined_data.apply(
        lambda tick: 1 if (tick['STATE'] == 'BUY' and tick['NEXT_MID_PRICE'] > tick['MID_PRICE']) or
                        (tick['STATE'] == 'SELL' and tick['NEXT_MID_PRICE'] < tick['MID_PRICE']) else 0
    )

    # Select relevant fields
    orders_with_right_way = joined_data[['ID', 'Right_Way']]

    # Run the query for the specified date
    df = otp.run(orders_with_right_way, date=date)
