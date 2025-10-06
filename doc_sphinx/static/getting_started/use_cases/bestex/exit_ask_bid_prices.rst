Exit ask/bid prices for an order
================================
To find the EXIT_ASK_PRICE and EXIT_BID_PRICE for each order in ``onetick.py``, where 'exit' means the order was fully executed (``STATE='F'``) or canceled (``STATE='C'``)

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

    # Filter orders to get only those that are fully executed or cancelled
    exit_orders = orders.where((orders['STATE'] == 'F') | (orders['STATE'] == 'C'))

    # Join exit orders with quotes based on timestamp
    exit_orders_with_quotes = otp.join_by_time([exit_orders, quotes])

    # Aggregate to get EXIT_ASK_PRICE and EXIT_BID_PRICE for each order ID
    exit_orders_with_quotes = exit_orders_with_quotes.agg({
        'EXIT_ASK_PRICE': otp.agg.first('ASK_PRICE'),
        'EXIT_BID_PRICE': otp.agg.first('BID_PRICE')
    }, group_by='ID')

    # Run the query for the specified date
    df = otp.run(exit_orders_with_quotes, date=date)
