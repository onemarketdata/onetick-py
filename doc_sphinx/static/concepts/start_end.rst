Query start / end flow
***********************

Query interval
==============

``onetick.py`` runs a query for a specified *query interval*. The query interval can be set implicitly or explicitly.

The query interval ``[start, end)`` is specified using the ``start`` and ``end`` parameters (or the ``date`` parameter).
The are two ways of defining start and end for a query: on a source or for the whole query (e.g.,
in :func:`otp.run <onetick.py.run>`). Both methods can be combined for some cases.

If different query intervals set for multiple data sources, or query interval set both for data source and for the whole
query, then `MODIFY_QUERY_TIMES` EP will be applied to each data source with their start/end times as its parameters.

Query interval on query execution
---------------------------------
Query interval can be set when the query is executed:

::

    # trades are retrieved for the interval [2022/3/1, 2022/3/2) specified when the query is executed on line 2
    trades = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL')
    otp.run(trades, start=otp.dt(2022, 3, 1), end=otp.dt(2022, 3, 2))


The query interval specified when executing the query applies to every source that does not specify its own interval:

::

    # trades are retrieved for the interval [2022/3/1, 2022/3/2) specified when the query is executed
    trades = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL')
    # quotes are retrieved for the interval [2022/3/1, 2022/3/2) specified when the query is executed
    quotes = otp.DataSource(db='US_COMP', tick_type='QTE', symbols='AAPL')

    res = otp.join([trades, quotes])
    otp.run(res, start=otp.dt(2022, 3, 1), end=otp.dt(2022, 3, 2))

Query interval on a source
--------------------------
Query interval can be specified when a source is defined:

::

    trades = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL', start=otp.dt(2022, 3, 1), end=otp.dt(2022, 3, 2))
    otp.run(trades)


Every source can specify its own interval and different sources can have different intervals.
For example, below we specify the intervals to compute the volume on March 1
in one source and the volume on March 2 in another source. We then merge the two sources and the user does not need to worry
about setting the interval for the resulting query.


.. testsetup:: *

    >>> session.dbs['US_COMP'].add(
    ...     otp.Tick(SIZE=62351689),
    ...     symbol='AAPL',
    ...     tick_type='TRD',
    ...     date=otp.dt(2023, 3, 1)
    ... )
    >>> session.dbs['US_COMP'].add(
    ...     otp.Tick(SIZE=60242644),
    ...     symbol='AAPL',
    ...     tick_type='TRD',
    ...     date=otp.dt(2023, 3, 2)
    ... )

.. doctest::

    >>> day1_trades = otp.DataSource(db='US_COMP', symbol='AAPL', tick_type='TRD', start=otp.dt(2023, 3, 1), end=otp.dt(2023, 3, 2))
    >>> day1_volume = day1_trades.agg({'VOLUME': otp.agg.sum(day1_trades['SIZE'])}, bucket_time='start')
    >>> otp.run(day1_volume)  # volume on March 1
            Time    VOLUME
    0 2023-03-01  62351689

.. doctest::

    >>> day2_trades = otp.DataSource(db='US_COMP', symbol='AAPL', tick_type='TRD', start=otp.dt(2023, 3, 2), end=otp.dt(2023, 3, 3))
    >>> day2_volume = day2_trades.agg({'VOLUME': otp.agg.sum(day2_trades['SIZE'])}, bucket_time='start')
    >>> otp.run(day2_volume)  # volume on March 2
            Time    VOLUME
    0 2023-03-02  60242644

.. doctest::

    >>> res = day1_volume + day2_volume  # merge ticks
    >>> otp.run(res)
            Time    VOLUME
    0 2023-03-01  62351689
    1 2023-03-01  60242644

The interval can also be specified using the ``date`` parameter of the :class:`otp.DataSource <onetick.py.DataSource>`, that sets
the start and end parameters to ``00:00:00`` and next day's ``00:00:00`` respectively.


Default query interval
----------------------

``onetick.py`` uses the default values ``onetick.py.config.default_start_time`` and ``onetick.py.config.default_end_time``
for the ``start`` and ``end`` parameters when they are not set. The default values are useful in :ref:`here <Tests>`.

The :class:`otp.dt <onetick.py.datetime>` class
================================================
.. _datetime_guide:

The ``start`` and ``end`` parameters take the standard `datetime.datetime` values as well as
:class:`otp.dt <onetick.py.datetime>` values. The :class:`otp.dt <onetick.py.datetime>` class is introduced to support
nanoseconds and DST as the standard python ``datetime.datetime`` class does not support them.

:class:`otp.dt <onetick.py.datetime>` could be used in any ``onetick.py`` api call that allows date or time as an input:

.. doctest::

    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> data['TIME_VALUE'] = otp.dt(2022, 1, 1, nanosecond=456)
    >>> otp.run(data)
                         Time  X                    TIME_VALUE
    0 2003-12-01 00:00:00.000  1 2022-01-01 00:00:00.000000456
    1 2003-12-01 00:00:00.001  2 2022-01-01 00:00:00.000000456
    2 2003-12-01 00:00:00.002  3 2022-01-01 00:00:00.000000456

Timezone
========

The timezone can be specified in the :func:`otp.run <onetick.py.run>` using the ``timezone`` parameter. If it is not set then default timezone is used.

It is possible to change the default timezone using the ``OTP_DEFAULT_TZ`` environment variable or using the ``otp.config['tz']`` config variable:

::

    otp.config['tz'] = 'GMT'
