Takeout success
===============
`Takeout success` based on capturing the available size in the market for an order at a certain level takeout success flag:

::

    Takeout_Success := SizeFilled >= min(Size, Ask_Size), when Side = 'BUY', and
    Takeout_Success := SizeFilled >= min(Size, Bid_Size), otherwise

Calculate `takeout success` for orders and US_COMP databases using for the ``TSLA`` ticker in ``onetick.py``

.. testcode::

    import onetick.py as otp

    orders = otp.DataSource('ORDERS_DB', tick_type='ORDER')

    # add the direction that is equl to 1 for buy orders and -1 for sell orders
    orders['DIRECTION'] = 2 * orders['BUY_FLAG'] - 1

    quotes = otp.DataSource('US_COMP', tick_type='QTE')

    res = otp.join_by_time([orders, quotes])

    # initialize a field where we could put the takeout sucess flag
    res['TAKEOUT_SUCCESS'] = 0

    res = res.update(
                {'TAKEOUT_SUCCESS': 1},
                where=(res['DIRECTION'] == 1) & (res['QTY_FILLED'] >= res['ASK_SIZE'])
    )
    res = res.update(
                {'TAKEOUT_SUCCESS': 1},
                where=(res['DIRECTION'] == -1) & (res['QTY_FILLED'] >= res['BID_SIZE'])
    )

    otp.run(res, date=otp.dt(2022, 3, 2), symbols='TSLA')
