Data inspection
***************

``onetick-py`` allows to obtain information about available databases and perform data inspection,
such as getting available symbols, tick types, dates and data schemas.

This helps to create more complex data processing scenarios or simplify your queries.

Basics
======

Data inspection available through :func:`otp.databases <onetick.py.databases>` function that returns ``dict``
with database names as keys, and :class:`otp.inspection.DB <onetick.py.db._inspection.DB>` objects as values
through which database related properties can be obtained.

Let's look at the available methods for data inspection.

First of all you could list available databases.

.. doctest::

   >>> otp.databases()  # doctest: +ELLIPSIS
   {..., 'US_COMP_SAMPLE': ...}

To list dates for which data is available in the database, you can use
:meth:`DB.dates <onetick.py.db._inspection.DB.dates>` method.

.. doctest::

   >>> otp.databases()['US_COMP_SAMPLE'].dates()  # doctest: +ELLIPSIS
   [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3), ...]

You can retrieve available tick types for each database with
:meth:`DB.tick_types <onetick.py.db._inspection.DB.tick_types>` method.

.. doctest::

   >>> otp.databases()['US_COMP_SAMPLE'].tick_types()
   ['DAY', 'IND', 'LULD', 'MKT', 'NBBO', 'QTE', 'STAT', 'TRD']

:meth:`otp.inspection.DB.schema <onetick.py.db._inspection.DB.schema>` method provides the ability to obtain
the data schema for a selected database.

If more than one tick type is available for a database, you should specify needed tick type via ``tick_type`` parameter.

If you need to get schema for a specific date, it could be set via ``date`` parameter.
Otherwise, the schema for the last available day,
obtained by :meth:`DB.last_date <onetick.py.db._inspection.DB.last_date>`, will be returned.

.. doctest::

   >>> otp.databases()['US_COMP_SAMPLE'].schema(tick_type='TRD')
   {'COND': string[4],
    'CORR': <class 'onetick.py.types._int'>,
    'DELETED_TIME': <class 'onetick.py.types.msectime'>,
    'EXCHANGE': string[1],
    'OMDSEQ': <class 'onetick.py.types.uint'>,
    'PARTICIPANT_TIME': <class 'onetick.py.types.nsectime'>,
    'PRICE': <class 'float'>,
    'SEQ_NUM': <class 'int'>,
    'SIZE': <class 'int'>,
    'SOURCE': string[1],
    'STOP_STOCK': string[1],
    'TICKER': string[16],
    'TICK_STATUS': <class 'onetick.py.types._int'>,
    'TRADE_ID': string[20],
    'TRF': string[1],
    'TRF_TIME': <class 'onetick.py.types.nsectime'>,
    'TTE': string[1]}

:meth:`DB.symbols <onetick.py.db._inspection.DB.symbols>` method is available to retrieve
the list of symbols for a specific date.

If date not specified, the last available for selected database date will be used.

.. doctest::

   >>> otp.databases()['US_COMP_SAMPLE'].symbols(tick_type='TRD', date=otp.dt(2024, 2, 1))  # doctest: +ELLIPSIS
   ['A', 'AAL', 'AAPL', ...]

For further details about available data inspection methods follow
:class:`otp.inspection.DB <onetick.py.db._inspection.DB>` documentation.

Limitations
===========

Using of data inspection not recommended in complex production queries.
It's better to use OneTick's EPs or construct multi-stage queries,
in most cases it will allow OneTick to process symbols/ticks more efficiently.
For example, it's better to use :class:`otp.Symbols <onetick.py.Symbols>` EP
instead of :meth:`DB.symbols <onetick.py.db._inspection.DB.symbols>`.

For each data inspection method call, a query to the OneTick server is made.
Therefore, if you need to make frequent data inspection method calls, for example,
to enumerate a large number of dates, you may experience performance issues.

:meth:`DB.dates <onetick.py.db._inspection.DB.dates>` method loads time ranges for database as one year chunks.
For databases that have data for several years, loading may be performed a bit slower.

More complex scenarios
======================

With the combination of these simple methods, we can build more complex data inspection logic.

Data existence check
--------------------

Let's say that we need to check for the presence of data in the database `US_COMP_SAMPLE` for a symbol `AAPL`
with tick type `TRD` for the last available date.

.. doctest::

   >>> if otp.databases()['US_COMP_SAMPLE'].symbols(tick_type='TRD', pattern='^AAPL$'):
   ...     print('Symbol AAPL::TRD exists in US_COMP_SAMPLE')
   Symbol AAPL::TRD exists in US_COMP_SAMPLE

By making a small change to this code we can check for the presence of the selected symbol for a specific date.

.. doctest::

   >>> if otp.databases()['US_COMP_SAMPLE'].symbols(date=otp.dt(2024, 2, 1), tick_type='TRD', pattern='^AAPL$'):
   ...     print('Symbol AAPL::TRD exists in US_COMP_SAMPLE for 2024/02/01')
   Symbol AAPL::TRD exists in US_COMP_SAMPLE for 2024/02/01

You can go further and get for each symbol a list of dates for which there is data for it.

.. testcode::
   :skipif: True

   from collections import defaultdict
   databases = otp.databases()
   db = databases['US_COMP_SAMPLE']
   symbols_to_dates = defaultdict(list)
   for _date in db.dates():
       for symbol in db.symbols(date=_date, tick_type='TRD'):
           symbols_to_dates[symbol].append(_date)
   print(dict(sorted(symbols_to_dates.items())))

.. testoutput::

   {'A': [datetime.date(2024, 1, 1), ...],
    'AAL': [datetime.date(2024, 1, 1), ...],
    'AAPL': [datetime.date(2024, 1, 1), ...]
    ...}

However, if there are a large number of dates for which data is available, performance issues may arise,
as a query to OneTick is made for each ``symbols`` method call.

Last date for each symbol
-------------------------

Retrieving the last available day for symbols with `TRD` tick type.

.. testcode::
   :skipif: True

   db = otp.databases()['US_COMP_SAMPLE']
   tick_type = 'TRD'
   symbols_last_dates = {}
   for _date in reversed(db.dates()):
       unique_date_symbols = set(db.symbols(date=_date, tick_type=tick_type)) - symbols_last_dates.keys()
       symbols_last_dates.update({_symbol: _date for _symbol in unique_date_symbols})
   dict(sorted(symbols_last_dates.items()))

.. testoutput::

   {'A': datetime.date(2024, 3, 28),
    'AAL': datetime.date(2024, 3, 28),
    'AAPL': datetime.date(2024, 3, 28),
    ...}

Adding missing fields
---------------------

Let's imagine that you have a database in which, for some reason, the data field you need for
your query is not available for every day. However you can calculate its value during query execution.

.. doctest::

   >>> db = 'US_COMP_SAMPLE'
   >>> _date = otp.dt(2024, 2, 1)
   >>>
   >>> source = otp.DataSource(db=db, tick_type='TRD', symbols='AAPL')
   >>>
   >>> if 'VOLUME' not in otp.databases()[db].schema(date=_date, tick_type='TRD'):
   ...     source['VOLUME'] = source['PRICE'] * source['SIZE']
   >>>
   >>> source = source[['PRICE', 'SIZE', 'VOLUME']][:5]
   >>> otp.run(source, date=_date)
                                   Time   PRICE  SIZE    VOLUME
   0      2024-02-01 04:00:00.008283417  186.50     6   1119.00
   1      2024-02-01 04:00:00.008290927  185.59     1    185.59
   2      2024-02-01 04:00:00.008291153  185.49   107  19847.43
   3      2024-02-01 04:00:00.010381671  185.49     1    185.49
   4      2024-02-01 04:00:00.011224206  185.50     2    371.00
