Market impact
==============

MI -- `market impact`. Positive values indicate that the price moved in a favorable direction (i.e., the price is more attractive at the markout time than at the arrival time). Positive values of markouts into the future indicate that the order’s impact did not result in an adverse price movement. Positive values for future markouts may be an indication of a toxic client flow

Formula to calculate MI

::

    MI := Direction * Executed_QTY * (Arrival_Mid_Price - <RefPoint>_Mid_Price)

where ``<RefPoint>_Mid_Price`` -- is reference offset point (markout) on market data.

.. testcode::

    import onetick.py as otp

    # Define your symbols, orders, and quotes database
    symbol = 'TSLA'
    orders_db = 'ORDERS_DB'
    quotes_db = 'US_COMP'
    date = otp.dt(2022, 3, 2)

    # Load orders and quotes data
    orders = otp.DataSource(orders_db, tick_type='ORDER')
    quotes = otp.DataSource(quotes_db, tick_type='QTE')

    # Add mid-price for every quote
    quotes['MID_PRICE'] = (quotes['ASK_PRICE'] + quotes['BID_PRICE']) / 2
    # select only MID_PRICE, other fields we won't use
    quotes = quotes[['MID_PRICE']]

    # Points at with offsets from the order arrival time
    arrival_markouts = [-30, -10, 10, 30]

    # Prepare quotes by markout relative to order arrival
    qte_by_markout_arrival = [quotes.deepcopy()]  # Include original quotes
    for m in arrival_markouts:
        mr = str(m).replace('-', 'm')  # Replace minus sign with 'm'
        qte_shifted = quotes.deepcopy()
        qte_shifted = qte_shifted.rename({'MID_PRICE': f'MID_PRICE_{mr}_arrival'})
        qte_shifted = qte_shifted.time_interval_shift(m * 1000)
        qte_by_markout_arrival.append(qte_shifted)

    # Join orders with original and shifted quotes
    joined_orders_with_quotes = otp.join_by_time([orders] + qte_by_markout_arrival)

    # Roll up order to calculate Executed_QTY, Arrival_Mid_Price, and Direction
    # Propagate MID_PRICE at each markout from order arrival
    agg_fields = {
        'ARRIVAL_MID_PRICE': otp.agg.first('MID_PRICE'),
        'SIDE': otp.agg.first('SIDE'),
        'EXECUTED_QTY': otp.agg.sum('QTY_FILLED')
    }
    for m in arrival_markouts:
        mr = str(m).replace('-', 'm')
        agg_fields[f'MID_PRICE_{mr}_arrival'] = otp.agg.first(f'MID_PRICE_{mr}_arrival')

    orders_agg = joined_orders_with_quotes.agg(agg_fields, group_by='ID')

    # Calculate Direction (1 for BUY, -1 for SELL)
    orders_agg['DIRECTION'] = orders_agg.apply(lambda tick: 1 if tick['SIDE'] == 'BUY' else -1)

    # Calculate Market Impact for each markout
    for m in arrival_markouts:
        mr = str(m).replace('-', 'm')
        orders_agg[f'MI_{mr}'] = orders_agg['DIRECTION'] * orders_agg['EXECUTED_QTY'] * (orders_agg['ARRIVAL_MID_PRICE'] - orders_agg[f'MID_PRICE_{mr}_arrival'])

    # Select relevant fields
    orders_with_mi = orders_agg[['ID', 'EXECUTED_QTY'] + [f'MI_{mr}' for mr in [str(m).replace('-', 'm') for m in arrival_markouts]]]

    # Run the query for the specified date
    df = otp.run(orders_with_mi, date=date, symbols=symbol)
