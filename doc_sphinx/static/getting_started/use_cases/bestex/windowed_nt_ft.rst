Windowed FT (far touch) and Windowed NT (far touch)
===================================================
To calculate the `Windowed Near Touch` (Window_NT) and `Windowed Far Touch` (Window_FT) prices for every order, considering the side of the order (buy or sell) and using the windowed bid and ask prices, you need to first determine these windowed prices and then apply the logic based on the order side. Here's how you can do it using ``onetick.py``:

.. testcode::

    import onetick.py as otp

    # Define your symbols, orders, and quotes database
    symbol = 'TSLA'
    orders_db = 'ORDERS_DB'
    quotes_db = 'US_COMP'
    date = otp.dt(2022, 3, 2)

    # Define the time window parameters
    window_before = 5.0  # seconds before the order
    window_after = 7.0   # seconds after the order

    # Load orders and quotes data
    orders = otp.DataSource(orders_db, tick_type='ORDER', symbol=symbol)
    quotes = otp.DataSource(quotes_db, tick_type='QTE', symbol=symbol)

    def get_window_ask_bid(orders: otp.Source, quotes: otp.Source, window_before: float, window_after: float) -> otp.Source:
        # Aggregate ask and bid prices
        quotes_agg = quotes.agg({'WINDOW_ASK': otp.agg.max('ASK_PRICE'),
                                 'WINDOW_BID': otp.agg.min('BID_PRICE')})

        # Define the start and end of the window
        window_start = orders['Time'] - otp.Nano(int(window_before * 1e9))
        window_end = orders['Time'] + otp.Nano(int(window_after * 1e9))

        # Join orders with the aggregated quotes based on the time window
        return orders.join_with_query(quotes_agg,
                                      start=window_start,
                                      end=window_end)

    # Apply the function to get windowed ask and bid prices
    windowed_ask_bid = get_window_ask_bid(orders, quotes, window_before, window_after)

    # Calculate Window_NT and Window_FT for each order
    windowed_ask_bid['Window_NT'] = windowed_ask_bid.apply(lambda tick: windowed_ask_bid['WINDOW_BID'] if tick['SIDE'] == 'BUY' else windowed_ask_bid['WINDOW_ASK'])
    windowed_ask_bid['Window_FT'] = windowed_ask_bid.apply(lambda tick: windowed_ask_bid['WINDOW_ASK'] if tick['SIDE'] == 'BUY' else windowed_ask_bid['WINDOW_BID'])

    # Select relevant fields
    orders_with_window_nt_ft = windowed_ask_bid[['ID', 'Window_NT', 'Window_FT']]

    # Run the query for the specified date
    df = otp.run(orders_with_window_nt_ft, date=date)
