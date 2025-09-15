Arrival ask / bid / mid prices for an order
===========================================
To find the `arrival mid-price` (which is the average of the ask and bid prices at the time of order arrival) for every order, you need to join the order data with the quote data at the time when each order was initially placed. In trading, the "arrival" of an order typically refers to the time when the order is first submitted, usually indicated by an order state of 'N' (New).

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
    arrival_orders, _ = orders[(orders['STATE'] == 'N')]

    # Join arrival orders with quotes based on timestamp
    arrival_orders_with_quotes = otp.join_by_time([arrival_orders, quotes])

    # Calculate the mid-price at the time of order arrival
    arrival_orders_with_quotes['ARRIVAL_MID_PRICE'] = (arrival_orders_with_quotes['ASK_PRICE'] + arrival_orders_with_quotes['BID_PRICE']) / 2

    # Select relevant fields, if necessary
    arrival_orders_with_quotes = arrival_orders_with_quotes[['ID', 'ARRIVAL_MID_PRICE']]

    # Run the query for the specified date
    df = otp.run(arrival_orders_with_quotes, date=date)
