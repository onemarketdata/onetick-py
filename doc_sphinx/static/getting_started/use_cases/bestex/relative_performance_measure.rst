Relative Performance Measure (RPM)
==================================
`RPM` indicates the percentage of total market activity outperformed by order execution. Fair and reasonable execution is around 50%. Values above 50% indicate superior execution. Converges to 50% as the executed size approaches the market volume.

::

    RPM = V_o/V_t + 0.5 * V_e / V_t

``V_t`` -- total market volume between order arrival and exit
``V_o`` -- market volume traded at prices worse ('higher' for buy orders, 'lower' for sell order) than the order VWAP. In other words, ``V_o/V_t`` is the percentage of market activity that the execution outperformed.
``V_e`` -- market volume traded at prices equal to the order VWAP

Here's the complete code to achieve this:

.. testcode::

    import onetick.py as otp
    import operator

    # Define your symbols, orders, and trades database
    symbol = 'TSLA'
    orders_db = 'ORDERS_DB'
    trades_db = 'US_COMP'
    date = otp.dt(2022, 3, 2)

    # Load orders data
    orders = otp.DataSource(orders_db, tick_type='ORDER', symbol=symbol)

    # Roll up orders to get VWAP, ARRIVAL_TIME (first tick time), and EXIT_TIME (last tick time)
    orders_agg = orders.agg({
        'VWAP': otp.agg.vwap('PRICE_FILLED', 'QTY_FILLED'),
        'ARRIVAL_TIME': otp.agg.first_time(),
        'EXIT_TIME': otp.agg.last_time(),
        'SIDE': otp.agg.first('SIDE')
    }, group_by='ID')

    # Calculate Direction (1 for BUY, -1 for SELL)
    orders_agg['DIRECTION'] = orders_agg.apply(lambda tick: 1 if tick['SIDE'] == 'BUY' else -1)

    # Define the function to add state variable aggregation
    def add_state_var_aggr(
            src: otp.Source,
            state_var: str,
            column: str,
            condition=lambda row: True,
            action=operator.add
        ):
        src.state_vars[state_var] = 0.0
        src.state_vars[state_var] = src.apply(
            lambda row: action(src.state_vars[state_var], row[column])
            if condition(row) else src.state_vars[state_var]
        )
        return src.state_vars[state_var]

    # Define the function to calculate RPM
    def rpm(vwap, direction) -> otp.Source:
        # Get trades
        md = otp.DataSource(trades_db, tick_type='TRD')

        # Sum up all qty ('SIZE') of trades
        total = add_state_var_aggr(md, 'TOTAL', 'SIZE')

        # Sum up qty of trades which price was worse than order vwap
        worse = add_state_var_aggr(
            md,
            'WORSE',
            'SIZE',
            lambda row: (row['PRICE'] > vwap and direction == 1) or (row['PRICE'] < vwap and direction == -1)
        )

        # Sum up qty of trades which prices are equal to order's vwap
        equal = add_state_var_aggr(md, 'EQUAL', 'SIZE', lambda row: row['PRICE'] == vwap)

        # Calculate RPM
        md = md.last()
        md['RPM'] = (worse + 0.5 * equal) / total
        md['RPM'] = md.apply(lambda row: otp.nan if vwap == otp.nan else md['RPM'])
        return md[['RPM']]

    # Join orders with RPM calculation
    orders_with_rpm = orders_agg.join_with_query(
        rpm,
        params=dict(vwap=orders_agg['VWAP'], direction=orders_agg['DIRECTION']),
        start_time=orders_agg['ARRIVAL_TIME'],
        end_time=orders_agg['EXIT_TIME']
    )

    # Run the query for the specified date
    df = otp.run(orders_with_rpm, date=date)
