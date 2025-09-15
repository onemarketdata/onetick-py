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

.. testsetup::

    >>> session.dbs['SOME_DB'].add(
    ...     otp.Ticks(
    ...         SIZE=[40, 10, 40, 350],
    ...         PRICE=[147.66] * 2 + [148.33] * 2,
    ...         offset=[otp.Second(1)] * 4,
    ...     ),
    ...     symbol='A',
    ...     tick_type='TRD',
    ...     date=otp.dt(2003, 12, 1)
    ... )
    >>> session.dbs['SOME_DB'].add(
    ...     otp.Ticks(
    ...         SIZE=[20, 6, 8, 300],
    ...         PRICE=[94.31] * 4,
    ...         offset=[otp.Second(1)] * 4,
    ...     ),
    ...     symbol='AA',
    ...     tick_type='TRD',
    ...     date=otp.dt(2003, 12, 1)
    ... )
    >>> session.dbs['SOME_DB'].add(
    ...     otp.Ticks(
    ...         SIZE=[6, 250, 40, 40],
    ...         PRICE=[51.21] + [50.44] * 2 + [51.02],
    ...         offset=[otp.Second(1)] * 4,
    ...     ),
    ...     symbol='AAA',
    ...     tick_type='TRD',
    ...     date=otp.dt(2003, 12, 1)
    ... )
    >>> session.dbs['SOME_DB'].add(
    ...     otp.Ticks(
    ...         SIZE=[10, 20, 30, 40],
    ...         PRICE=[51.44] * 4,
    ...         offset=[otp.Second(1)] * 4,
    ...     ),
    ...     symbol='AAA',
    ...     tick_type='TRD',
    ...     date=otp.dt(2003, 12, 2)
    ... )

.. doctest::

   >>> databases = otp.databases()
   >>> [database_name for database_name in databases]
   ['COMMON', 'DEMO_L1', 'SOME_DB']

To list dates for which data is available in the database, you can use
:meth:`DB.dates <onetick.py.db._inspection.DB.dates>` method.

.. doctest::

   >>> otp.databases()['SOME_DB'].dates()
   [datetime.date(2003, 12, 1), datetime.date(2003, 12, 2)]

You can retrieve available tick types for each database with
:meth:`DB.tick_types <onetick.py.db._inspection.DB.tick_types>` method.

.. doctest::

   >>> otp.databases()['DEMO_L1'].tick_types()
   ['QTE', 'TRD']

:meth:`otp.inspection.DB.schema <onetick.py.db._inspection.DB.schema>` method provides the ability to obtain
the data schema for a selected database.

If more than one tick type is available for a database, you should specify needed tick type via ``tick_type`` parameter.

If you need to get schema for a specific date, it could be set via ``date`` parameter.
Otherwise, the schema for the last available day,
obtained by :meth:`DB.last_date <onetick.py.db._inspection.DB.last_date>`, will be returned.

.. doctest::

   >>> otp.databases()['SOME_DB'].schema(tick_type='TRD')
   {'PRICE': <class 'float'>, 'SIZE': <class 'int'>}

:meth:`DB.symbols <onetick.py.db._inspection.DB.symbols>` method is available to retrieve
the list of symbols for a specific date.

If date not specified, the last available for selected database date will be used.

.. doctest::

   >>> databases['SOME_DB'].symbols(tick_type='TRD', date=otp.dt(2003, 12, 1))
   ['A', 'AA', 'AAA']

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

Let's say that we need to check for the presence of data in the database `SOME_DB` for a symbol `AAA`
with tick type `TRD` for the last available date.

.. doctest::

   >>> databases = otp.databases()
   >>> if databases['SOME_DB'].symbols(tick_type='TRD', pattern='^AAA$'):
   ...     print('Symbol AAA::TRD exists in DB')
   Symbol AAA::TRD exists in DB

By making a small change to this code we can check for the presence of the selected symbol for a specific date.

.. doctest::

   >>> if databases['SOME_DB'].symbols(date=otp.dt(2003, 12, 1), tick_type='TRD', pattern='^AAA$'):
   ...     print('Symbol AAA::TRD exists in DB for 2003/12/01')
   Symbol AAA::TRD exists in DB for 2003/12/01

You can go further and get for each symbol a list of dates for which there is data for it.

.. doctest::

   >>> from collections import defaultdict
   >>>
   >>> databases = otp.databases()
   >>> db = databases['SOME_DB']
   >>> symbols_to_dates = defaultdict(list)
   >>>
   >>> for _date in db.dates():
   ...     for symbol in db.symbols(date=_date, tick_type='TRD'):
   ...         symbols_to_dates[symbol].append(_date)
   >>>
   >>> dict(sorted(symbols_to_dates.items()))
   {'A': [datetime.date(2003, 12, 1)],
    'AA': [datetime.date(2003, 12, 1)],
    'AAA': [datetime.date(2003, 12, 1),
     datetime.date(2003, 12, 2)]}

However, if there are a large number of dates for which data is available, performance issues may arise,
as a query to OneTick is made for each ``symbols`` method call.

Last date for each symbol
-------------------------

Retrieving the last available day for symbols with `TRD` tick type.

.. doctest::

   >>> db = otp.databases()['SOME_DB']
   >>> tick_type = "TRD"
   >>> symbols_last_dates = {}
   >>>
   >>> for _date in reversed(db.dates()):
   ...     unique_date_symbols = set(db.symbols(date=_date, tick_type=tick_type)) - symbols_last_dates.keys()
   ...     symbols_last_dates.update({_symbol: _date for _symbol in unique_date_symbols})
   >>>
   >>> dict(sorted(symbols_last_dates.items()))
   {'A': datetime.date(2003, 12, 1), 'AA': datetime.date(2003, 12, 1), 'AAA': datetime.date(2003, 12, 2)}

Adding missing fields
---------------------

Let's imagine that you have a database in which, for some reason, the data field you need for
your query is not available for every day. However you can calculate its value during query execution.

.. doctest::

   >>> db = 'SOME_DB'
   >>> _date = otp.dt(2003, 12, 1)
   >>>
   >>> source = otp.DataSource(db=db, tick_type='TRD', symbols='AAA')
   >>>
   >>> if 'VOLUME' not in otp.databases()[db].schema(date=_date):
   ...     source['VOLUME'] = source['PRICE'] * source['SIZE']
   >>>
   >>> source = source[['PRICE', 'SIZE', 'VOLUME']]
   >>> otp.run(source, start=_date, end=_date + otp.Day(1))
                    Time  PRICE  SIZE    VOLUME
   0 2003-12-01 00:00:01  51.21     6    307.26
   1 2003-12-01 00:00:01  50.44   250  12610.00
   2 2003-12-01 00:00:01  50.44    40   2017.60
   3 2003-12-01 00:00:01  51.02    40   2040.80
