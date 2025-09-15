Window ask and window bid
=========================
To find the windowed ask and bid prices for every order tick, where the window is defined as window_before seconds before an order and window_after seconds after an order, you can use the :meth:`join_with_query <onetick.py.Source.join_with_query>` method in ``onetick.py``. This method allows you to join two data sources based on a time window.

Here's a full code example that implements the function get_window_ask_bid to achieve this:


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
        # Aggregate ask and bid prices over the window
        quotes = quotes.agg({'WINDOW_ASK': otp.agg.max('ASK_PRICE'),
                             'WINDOW_BID': otp.agg.min('BID_PRICE')})

        # Define the start and end of the window
        window_start = orders['Time'] - otp.Nano(int(window_before * 1e9))
        window_end = orders['Time'] + otp.Nano(int(window_after * 1e9))

        # Join orders with the aggregated quotes based on the time window
        return orders.join_with_query(quotes,
                                      start=window_start,
                                      end=window_end)

    # Apply the function to get windowed ask and bid prices
    windowed_ask_bid = get_window_ask_bid(orders, quotes, window_before, window_after)

    # Run the query for the specified date
    df = otp.run(windowed_ask_bid, date=date)
