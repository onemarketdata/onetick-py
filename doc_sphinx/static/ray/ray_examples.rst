.. _ray-examples:

Ray usage examples
==================

.. _ray-example-function:

Example remote function
:::::::::::::::::::::::

::

    import ray
    import onetick.py as otp

    # Special decorator to run code remotely
    @ray.remote(max_retries=1)
    def example_otp_code():

        # here goes OTP code you want to run
        data = otp.DataSource(db='US_COMP',
                              tick_type='TRD',
                              start=otp.dt(2022, 4, 1),
                              end=otp.dt(2022, 4, 2))

        data['VOLUME'] = data['PRICE'] * data['SIZE']
        return otp.run(data)

    # Initialize Ray connection
    ray.init()

    # Run your code on Ray and get results back
    df = ray.get(example_otp_code.remote())

    # Shutdown Ray connection
    ray.shutdown()

    # Continue using df just as local pandas.DataFrame object
    print(df.head())

::

                                Time EXCHANGE  COND STOP_STOCK SOURCE TRF TTE TICKER   PRICE        DELETED_TIME  TICK_STATUS  SIZE  CORR  SEQ_NUM TRADE_ID           PARTICIPANT_TIME            TRF_TIME  OMDSEQ   VOLUME
    0 2022-04-01 04:00:00.018381502        K  @ TI                 N       0   AAPL  175.00 1969-12-31 19:00:00            0     1     0     1970        1 2022-04-01 04:00:00.000186 1969-12-31 19:00:00       0   175.00
    1 2022-04-01 04:00:00.018693590        K  @ TI                 N       0   AAPL  175.00 1969-12-31 19:00:00            0     3     0     1971        2 2022-04-01 04:00:00.000186 1969-12-31 19:00:00       1   525.00
    2 2022-04-01 04:00:00.018702708        K  @ TI                 N       0   AAPL  175.01 1969-12-31 19:00:00            0     3     0     1972        3 2022-04-01 04:00:00.000186 1969-12-31 19:00:00       2   525.03
    3 2022-04-01 04:00:00.018876909        K  @ TI                 N       0   AAPL  175.03 1969-12-31 19:00:00            0     1     0     1973        4 2022-04-01 04:00:00.000186 1969-12-31 19:00:00       3   175.03
    4 2022-04-01 04:00:00.059225208        K  @FTI                 N       1   AAPL  175.08 1969-12-31 19:00:00            0    49     0     2024        5 2022-04-01 04:00:00.058673 1969-12-31 19:00:00       0  8578.92

Example function with arguments
:::::::::::::::::::::::::::::::

You may define arguments for remote functions and call it similarly with specific arguments.
The only difference is that you must put arguments inside ``function.remote()`` method.

::

    # Remote function with arguments
    @ray.remote(max_retries=1)
    def get_BBO_offset(start, num_orders, offset):
        # Create order flow.
        # In practice, it can be take from a CSV file for from a DataFrame.
        order = otp.Ticks(timezone_for_time='EST5EDT',
                          start=start,
                          end=start + otp.Hour(1),
                          offset = [otp.Milli(x * 500) for x in range(0, num_orders)],
                          ID = [x for x in range (0, num_orders)])
        order['ARRIVAL'] = order['Time']
        order['SYMBOL'] = 'NQ\H22'
        order()
        q = order.join_with_query(
            otp.DataSource('CME', tick_type='QTE', back_to_first_tick=600),
            symbol=(order['SYMBOL']),
            start_time=order['ARRIVAL'] + otp.Milli(int(offset * 1000)),
            end_time=order['ARRIVAL'] + otp.Milli(int(offset * 1000)),
        )
        return otp.run(q)

    # Initialize Ray connection
    ray.init()

    # Call remote function with specific arguments
    df = ray.get(get_BBO_offset.remote(start=otp.dt(2022, 3, 2, 10), num_orders=5, offset=.5))
    print(df.head())

    # Call it again with other arguments
    df_other_arguments = ray.get(get_BBO_offset.remote(start=otp.dt(2022, 3, 2, 10), num_orders=10, offset=-2))
    print(df_other_arguments.head())

    # Shutdown Ray connection
    ray.shutdown()

::

                        Time  ID                 ARRIVAL  SYMBOL  BID_PRICE  BID_SIZE  BID_NUM_ORDERS  BID_SIZE_IMPLIED  ASK_PRICE  ASK_SIZE  ASK_NUM_ORDERS  ASK_SIZE_IMPLIED  OMDSEQ
    0 2022-03-02 10:00:00.000   0 2022-03-02 10:00:00.000  NQ\H22   14076.75         3               3                 0   14077.75         1               1                 0       1
    1 2022-03-02 10:00:00.500   1 2022-03-02 10:00:00.500  NQ\H22   14084.00         1               1                 0   14084.75         1               1                 0       4
    2 2022-03-02 10:00:01.000   2 2022-03-02 10:00:01.000  NQ\H22   14083.75         2               2                 0   14084.75         1               1                 0       4
    3 2022-03-02 10:00:01.500   3 2022-03-02 10:00:01.500  NQ\H22   14080.25         4               3                 0   14081.25         3               2                 0       1
    4 2022-03-02 10:00:02.000   4 2022-03-02 10:00:02.000  NQ\H22   14078.25         1               1                 0   14079.00         3               3                 0       1
                        Time  ID                 ARRIVAL  SYMBOL  BID_PRICE  BID_SIZE  BID_NUM_ORDERS  BID_SIZE_IMPLIED  ASK_PRICE  ASK_SIZE  ASK_NUM_ORDERS  ASK_SIZE_IMPLIED  OMDSEQ
    0 2022-03-02 10:00:00.000   0 2022-03-02 10:00:00.000  NQ\H22   14079.25         1               1                 0   14080.00         2               2                 0      10
    1 2022-03-02 10:00:00.500   1 2022-03-02 10:00:00.500  NQ\H22   14079.50         1               1                 0   14080.25         1               1                 0       7
    2 2022-03-02 10:00:01.000   2 2022-03-02 10:00:01.000  NQ\H22   14080.00         1               1                 0   14080.75         2               2                 0       1
    3 2022-03-02 10:00:01.500   3 2022-03-02 10:00:01.500  NQ\H22   14073.25         1               1                 0   14074.00         1               1                 0       1
    4 2022-03-02 10:00:02.000   4 2022-03-02 10:00:02.000  NQ\H22   14075.25         1               1                 0   14076.00         1               1                 0       4

Limitations
:::::::::::

Remote run approach leads to some usage limitations:

- You cannot use custom/imported modules inside remote functions - compute all arguments before calling remote function.
- Ray instance is isolated from global Internet.
- Run only ``onetick.py`` specific code to reduce Ray instance resource (memory, CPU) consumption.
- You cannot use file pointers as arguments - call remote functions with file content as argument.

.. _apply-remote-context:

Using apply() method in remote context
--------------------------------------

Technical implementation of :doc:`/api/source/apply` method requires user to use :doc:`/api/misc/remote` decorator
with functions and lambda expressions that will be used as arguments to :doc:`/api/source/apply` method.

::

    import ray
    import onetick.py as otp

    @otp.remote
    def match_condition(row):
       if row['COND'].str.contains('O'):
           return 1
       if row['COND'].str.contains('6') == True:
           return 1
       if row['COND'].str.contains('9') == True:
           return 1
       else:
           return 0

    @ray.remote(max_retries=1)
    def quicktest(start, end, symbol):
        ds_trd = otp.DataSource(db='US_COMP', tick_type='TRD', start=start, end=end)
        ds_trd.schema['COND'] = str
        ds_trd['OC_TRD'] = ds_trd.apply(match_condition)
        return otp.run(ds_trd, symbol=[symbol])

    start = otp.dt(2022, 8, 25, 9, 29)
    end = otp.dt(2022, 8, 25, 16, 30)
    symbol = 'BAC'
    ray.init()
    result = ray.get(quicktest.remote(start, end, symbol))
    print(result)
    ray.shutdown()
