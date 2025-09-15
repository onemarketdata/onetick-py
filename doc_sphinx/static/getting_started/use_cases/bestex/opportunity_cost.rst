Opportunity cost
================
OC -- `opportunity cost`.

Formula for it:

::

    OC := Direction * Unexecuted_QTY * (Arrival_Mid_Price - Exit_Mid_Price)

To calculate the Opportunity Cost (OC) for orders, we need to determine the direction of each order, the unexecuted quantity (Unexecuted_QTY), the arrival mid-price (Arrival_Mid_Price), and the exit mid-price (Exit_Mid_Price). The OC is then calculated using the provided formula. Here's how you can do it using ``onetick.py``:

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

    # Add mid-price for every quote
    quotes['MID_PRICE'] = (quotes['ASK_PRICE'] + quotes['BID_PRICE']) / 2

    # Join quotes to orders without any filtration
    joined_data = otp.join_by_time([orders, quotes])

    # Roll up order, save the first mid price (arrival mid price) and the last mid price (exit mid price)
    orders_agg = joined_data.agg({
        'ARRIVAL_MID_PRICE': otp.agg.first('MID_PRICE'),
        'EXIT_MID_PRICE': otp.agg.last('MID_PRICE'),
        'SIDE': otp.agg.first('SIDE'),
        'UNEXECUTED_QTY': otp.agg.sum(joined_data['QTY'] - joined_data['QTY_FILLED'])
    }, group_by='ID')

    # Calculate Direction (1 for BUY, -1 for SELL)
    orders_agg['DIRECTION'] = orders_agg.apply(lambda tick: 1 if tick['SIDE'] == 'BUY' else -1)

    # Calculate OC (Opportunity Cost)
    orders_agg['OC'] = orders_agg['DIRECTION'] * orders_agg['UNEXECUTED_QTY'] * (orders_agg['ARRIVAL_MID_PRICE'] - orders_agg['EXIT_MID_PRICE'])

    # Select relevant fields
    orders_with_oc = orders_agg[['ID', 'OC']]

    # Run the query for the specified date
    df = otp.run(orders_with_oc, date=date)
