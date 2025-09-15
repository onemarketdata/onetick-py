Databases, symbols, and tick types
**********************************

.. _symbols_concept:


The data is organized by database (e.g., ``US_COMP`` or ``CME``), symbol (e.g., ``AAPL``), and tick type (e.g., ``TRD``
or ``QTE``). A data source must specify these (possibly providing multiple values for each) to retrieve data.

A tick type can be set explicitly via the ``tick_type`` parameter of a data source.

The database name can be specified via the ``db`` parameter or as part of a symbol using the ``::`` separator.
For example

.. testsetup::

    >>> session.dbs['US_COMP'].add(
    ...     otp.Ticks(
    ...         SIZE=[4, 1, 4, 62351680],
    ...         PRICE=[147.66] * 4,
    ...         offset=[otp.Nano(5165425), otp.Nano(5173198)] +  [otp.Nano(14521032)] * 2,
    ...     ),
    ...     symbol='AAPL',
    ...     tick_type='TRD',
    ...     date=otp.dt(2023, 3, 1)
    ... )
    >>> session.dbs['US_COMP'].add(
    ...     otp.Ticks(
    ...         SIZE=[10, 3, 4, 56099671],
    ...         PRICE=[94.31] * 4,
    ...         offset=[otp.Nano(62175376), otp.Nano(63719476), otp.Nano(64597172), otp.Nano(64597180)]
    ...     ),
    ...     symbol='AMZN',
    ...     tick_type='TRD',
    ...     date=otp.dt(2023, 3, 1)
    ... )
    >>> session.dbs['US_COMP'].add(
    ...     otp.Tick(SIZE=31619403),
    ...     symbol='MSFT',
    ...     tick_type='TRD',
    ...     date=otp.dt(2023, 3, 1)
    ... )

.. doctest::

   >>> trades = otp.DataSource(db='US_COMP', symbol='AAPL', tick_type='TRD', date=otp.dt(2023, 3, 1))
   >>> volume = trades.agg({'VOLUME': otp.agg.sum(trades['SIZE'])})
   >>> otp.run(volume)
           Time    VOLUME
   0 2023-03-02  62351689

is equivalent to

.. doctest::

   >>> trades = otp.DataSource(symbol='US_COMP::AAPL', tick_type='TRD', date=otp.dt(2023, 3, 1))

The same technique applies to the database and tick type:

.. doctest::

   >>> trades = otp.DataSource(symbol='AAPL', tick_type='US_COMP::TRD', date=otp.dt(2023, 3, 1))

Note that symbol name and tick type can be accessed as pseudo-fields in OneTick.
Those pseudo-fields are always present and you can access them like that:

.. doctest::

   >>> trades = otp.DataSource(db='US_COMP', tick_type='TRD', date=otp.dt(2023, 3, 1))
   >>> trades['SYMBOL_NAME'] = trades['_SYMBOL_NAME']
   >>> trades['TICK_TYPE'] = trades['_TICK_TYPE']
   >>> trades = trades.first()
   >>> trades = trades[['PRICE', 'SIZE', 'SYMBOL_NAME', 'TICK_TYPE']]
   >>> otp.run(trades, symbols='AAPL')
                              Time   PRICE  SIZE SYMBOL_NAME TICK_TYPE
   0 2023-03-01 00:00:00.005165425  147.66     4        AAPL       TRD


Parameter ``identify_input_ts`` in :class:`otp.DataSource <onetick.py.DataSource>`
and :meth:`otp.merge <onetick.py.merge>` can also be used to add them as new columns.


Symbols: bound and unbound
===========================

There are two ways of setting symbols: **bound** and **unbound**.

Bound symbols are specified when defining a source or with :meth:`otp.merge <onetick.py.merge>`.
Unbound symbols are set when the query is executed and apply to all sources that do not specify bound symbols.
Here is an simple example of an unbound symbol:

.. doctest::

   >>> trades = otp.DataSource(db='US_COMP', tick_type='TRD', date=otp.dt(2023, 3, 1))
   >>> volume = trades.agg({'VOLUME': otp.agg.sum(trades['SIZE'])})
   >>> otp.run(volume, symbols=['AAPL'])  # unbound symbol is set here
           Time    VOLUME
   0 2023-03-02  62351689

There is no difference between bound and unbound when we talk about a single symbol. To appreciate the difference, let's
look at two symbols.

.. doctest::

   >>> trades = otp.DataSource(db='US_COMP', tick_type='TRD', date=otp.dt(2023, 3, 1))
   >>> volume = trades.agg({'VOLUME': otp.agg.sum(trades['SIZE'])})
   >>> dict(sorted(otp.run(volume, symbols=['AAPL', 'MSFT']).items()))
    {'AAPL':         Time    VOLUME
     0 2023-03-02  62351689,
     'MSFT':         Time    VOLUME
     0 2023-03-02  31619403}

The results for each unbound symbol are processed separately and returned in a separate pandas DataFrame. The result of the run
method above is a dict with symbols as keys and pandas DataFrames as values.

Contrast this with bound symbols where the ticks are merged into a single flow:

.. doctest::

   >>> trades = otp.DataSource(db='US_COMP', tick_type='TRD', date=otp.dt(2023, 3, 1), symbols=['AAPL', 'MSFT'])
   >>> volume = trades.agg({'VOLUME': otp.agg.sum(trades['SIZE'])})
   >>> otp.run(volume)
           Time    VOLUME
   0 2023-03-02  93971092

Specifying bound symbols on a source is just a shorthand for the :meth:`otp.merge <onetick.py.merge>` method added right
after the source definition.

.. doctest::

    >>> trades = otp.DataSource(db='US_COMP', tick_type='TRD', date=otp.dt(2023, 3, 1))
    >>> cross_symbol_trades = otp.merge([trades], symbols=['AAPL', 'MSFT'])
    >>> volume = cross_symbol_trades.agg({'VOLUME': otp.agg.sum('SIZE')})
    >>> otp.run(volume)
            Time    VOLUME
    0 2023-03-02  93971092



Concurrency
-----------

Unbound symbols can be processed in parallel by specifying the ``concurrency`` parameter of :func:`otp.run <onetick.py.run>`.
You have to explicitly use :func:`otp.run <onetick.py.run>` to execute the query if you want to specify ``concurrency``.

::

    otp.run(volume, symbols=['AAPL', 'MSFT', 'AMZN', 'META'], concurrency=8)

For bound symbols, all calculations passed into :meth:`otp.merge <onetick.py.merge>` (``trades`` in example above) run in parallel.
Calculations can be made faster by computing as much as possible per symbol before merging them.
The following  example has the same result as the previous one but it finds the total volume faster as it calculates
the volume for every symbol independently which can be done in parallel and then  adds up the total.

.. doctest::

    >>> trades = otp.DataSource(db='US_COMP', tick_type='TRD', date=otp.dt(2023, 3, 1))
    >>> volume = trades.agg({'VOLUME': otp.agg.sum('SIZE')})
    >>> cross_symbol_volumes = otp.merge([volume], symbols=['AAPL', 'MSFT'], presort=True, identify_input_ts=True)
    >>> otp.run(cross_symbol_volumes, concurrency=8)
            Time    VOLUME SYMBOL_NAME TICK_TYPE
    0 2023-03-02  62351689        AAPL       TRD
    1 2023-03-02  31619403        MSFT       TRD

    >>> total_volume = cross_symbol_volumes.agg({'TOTAL_VOLUME': otp.agg.sum('VOLUME')})
    >>> otp.run(total_volume, concurrency=8)
            Time  TOTAL_VOLUME
    0 2023-03-02      93971092

.. note::
   All sources passed into :meth:`merge <onetick.py.merge>` are considered bound symbol sources.


Specifying symbols dynamically
==============================

In many cases it is necessary to select symbols from a databases according to some logic.
The :class:`otp.Symbols <onetick.py.Symbols>` source makes this easy.

.. testsetup::

    >>> session.dbs['US_COMP'].add(
    ...     otp.Ticks(
    ...         SIZE=[10, 10, 1],
    ...         PRICE=[24.56, 24.56, 24.53],
    ...         offset=[otp.Milli(441), otp.Milli(441), otp.Milli(735)]
    ...     ),
    ...     symbol='AAA',
    ...     tick_type='TRD',
    ...     date=otp.dt(2023, 3, 1)
    ... )
    >>> session.dbs['US_COMP'].add(
    ...     otp.Ticks(
    ...         SIZE=[40, 100, 549],
    ...         PRICE=[18.17, 18.17, 18.22],
    ...         offset=[otp.Milli(17), otp.Milli(71), otp.Milli(802)]
    ...     ),
    ...     symbol='AAAU',
    ...     tick_type='TRD',
    ...     date=otp.dt(2023, 3, 1)
    ... )

.. doctest::

    >>> symbols = otp.Symbols(db='US_COMP', date=otp.dt(2023, 3, 1), pattern='AAA%')
    >>> otp.run(symbols)
            Time SYMBOL_NAME
    0 2023-03-01         AAA
    1 2023-03-01        AAAU

The result of :class:`otp.Symbols <onetick.py.Symbols>` can be used as bound/unbound symbols.

.. doctest::

    >>> data = otp.DataSource(db='US_COMP', tick_type='TRD', start=otp.dt(2023, 3, 1), end=otp.dt(2023, 3, 2))
    >>> data = data.first(3)
    >>> data = data[['PRICE','SIZE']]
    >>> dict(sorted(otp.run(data, symbols=otp.Symbols(db='US_COMP', pattern='AAA%')).items()))
    {'AAA':                      Time    PRICE  SIZE
     0 2023-03-01 00:00:00.441  24.56    10
     1 2023-03-01 00:00:00.441  24.56    10
     2 2023-03-01 00:00:00.735  24.53     1,
     'AAAU':                     Time  PRICE  SIZE
     0 2023-03-01 00:00:00.017  18.17    40
     1 2023-03-01 00:00:00.071  18.17   100
     2 2023-03-01 00:00:00.802  18.22   549}


Note that the interval of the main query is implicitly used in ``otp.Symbols(db='US_COMP', pattern='AAA%')``.


The ``symbols`` parameter can be any type of calculation that contains the ``SYMBOL_NAME`` field in the resulting ticks.
Every tick provides a separate symbol name in the ``SYMBOL_NAME`` field while the other fields are passed as
'symbol parameters'.


Symbol parameters
=================

Symbol parameters are any fields other than ``SYMBOL_NAME`` that are constant for an instrument. We illustrate this below.
First, consider a query that finds 2 most traded symbols among symbols starting with the letter 'A'. We'll later use the
output of this query to specify symbols and symbol params.

.. doctest::

   >>> trd = otp.DataSource(db='US_COMP', tick_type='TRD', start=otp.dt(2023, 3, 1), end=otp.dt(2023, 3, 2))
   >>> trd = trd.agg({'VOLUME': otp.agg.sum('SIZE')})
   >>> count = otp.merge([trd], symbols=otp.Symbols(db='US_COMP', pattern='A%'), presort=True, identify_input_ts=True)
   >>> most_traded = count.high('VOLUME', n=2)
   >>> otp.run(most_traded)
            Time    VOLUME  SYMBOL_NAME  TICK_TYPE
    0 2023-03-02  62351689         AAPL        TRD
    1 2023-03-02  56099688         AMZN        TRD

The output of ``most_traded`` provides symbols in the ``SYMBOL_NAME`` column while the other columns  (``VOLUME`` and ``TICK_TYPE``)
provide symbol parameters. The following code retrieves the value of ``VOLUME`` for use in tick-by-tick analytics.

.. doctest::

   >>> trd = otp.DataSource('US_COMP', tick_type='TRD')
   >>> trd = trd.agg({'VOLUME': otp.agg.sum('SIZE')})
   >>> count = otp.merge([trd], symbols=otp.Symbols('US_COMP', pattern='A%'), identify_input_ts=True)
   >>> most_traded = count.high('VOLUME', n=2)
   >>>
   >>> data = otp.DataSource('US_COMP', tick_type='TRD')
   >>> data = data.first(3)
   >>> data = data[['PRICE','SIZE']]
   >>> data['VOLUME'] = data.Symbol['VOLUME', int]
   >>> dict(sorted(otp.run(data, symbols=most_traded, start=otp.dt(2023, 3, 1), end=otp.dt(2023, 3, 2)).items()))
   {'AAPL':                            Time   PRICE  SIZE    VOLUME
    0 2023-03-01 00:00:00.005165425  147.66     4  62351689
    1 2023-03-01 00:00:00.005173198  147.66     1  62351689
    2 2023-03-01 00:00:00.014521032  147.66     4  62351689,
    'AMZN':                            Time  PRICE  SIZE    VOLUME
    0 2023-03-01 00:00:00.062175376  94.31    10  56099688
    1 2023-03-01 00:00:00.063719476  94.31     3  56099688
    2 2023-03-01 00:00:00.064597172  94.31     4  56099688}

Equivalently, symbol parameters can be accessed by wrapping a function around the query as illustrated below.

.. doctest::

   >>> trd = otp.DataSource('US_COMP', tick_type='TRD')
   >>> trd = trd.agg({'VOLUME': otp.agg.sum('SIZE')})
   >>> count = otp.merge([trd], symbols=otp.Symbols('US_COMP', pattern='A%'), identify_input_ts=True)
   >>> most_traded = count.high('VOLUME', n=2)
   >>>
   >>> def query(sym):
   ...     data = otp.DataSource('US_COMP', tick_type='TRD')
   ...     data = data.first(3)
   ...     data = data[['PRICE','SIZE']]
   ...     data['VOLUME'] = sym['VOLUME']
   ...     return data
   >>>
   >>> dict(sorted(otp.run(query, symbols=most_traded, start=otp.dt(2023, 3, 1), end=otp.dt(2023, 3, 2)).items()))
   {'AAPL':                            Time   PRICE  SIZE    VOLUME
    0 2023-03-01 00:00:00.005165425  147.66     4  62351689
    1 2023-03-01 00:00:00.005173198  147.66     1  62351689
    2 2023-03-01 00:00:00.014521032  147.66     4  62351689,
    'AMZN':                            Time  PRICE  SIZE    VOLUME
    0 2023-03-01 00:00:00.062175376  94.31    10  56099688
    1 2023-03-01 00:00:00.063719476  94.31     3  56099688
    2 2023-03-01 00:00:00.064597172  94.31     4  56099688}

Time interval per symbol
========================

It is allowed to specify query interval per symbol using special fields ``_PARAM_START_TIME_NANOS`` and ``_PARAM_START_TIME_NANOS``


.. testsetup::

    >>> session.dbs['US_COMP'].add(
    ...     otp.Ticks(
    ...         SIZE=[10, 3539360, 10, 100],
    ...         offset=[0, otp.Minute(10 * 60 + 40), otp.Minute(10 * 60 + 45), otp.Minute(11 * 60)]
    ...     ),
    ...     symbol='AAPL',
    ...     tick_type='TRD',
    ...     date=otp.dt(2023, 3, 2)
    ... )
    >>> session.dbs['US_COMP'].add(
    ...     otp.Ticks(
    ...         SIZE=[99, 1679700, 16, 100],
    ...         offset=[0, otp.Minute(11 * 60 + 1), otp.Minute(11 * 60 + 29), otp.Minute(12 * 60 )]
    ...     ),
    ...     symbol='MSFT',
    ...     tick_type='TRD',
    ...     date=otp.dt(2023, 3, 2)
    ... )

.. doctest::

   >>> custom_symbols = otp.Ticks(SYMBOL_NAME=['AAPL', 'MSFT'],
   ...                            _PARAM_START_TIME_NANOS=[otp.dt(2023, 3, 2, 10, 30), otp.dt(2023, 3, 2, 11)],
   ...                            _PARAM_END_TIME_NANOS=[otp.dt(2023, 3, 2, 11), otp.dt(2023, 3, 2, 11, 30)])
   >>> data = otp.DataSource(db='US_COMP', tick_type='TRD')
   >>> data = data.agg({'VOLUME': otp.agg.sum('SIZE')})
   >>> data['SYMBOL'] = data.Symbol.name
   >>> dict(sorted(otp.run(data, start=otp.dt(2023, 3, 2), end=otp.dt(2023, 3, 3), symbols=custom_symbols).items()))
    {'AAPL':                  Time   VOLUME SYMBOL
    0 2023-03-02 11:00:00  3539370   AAPL,
    'MSFT':                  Time   VOLUME SYMBOL
    0 2023-03-02 11:30:00  1679716   MSFT}

Note that per symbol intervals should be inside the :ref:`query interval <static/concepts/start_end:Query interval>`.

Associated symbols
==================

Associated symbols is a technique when unbound symbols are used to define bound symbols or symbols in related queries.
It is expressed using the :func:`otp.eval <onetick.py.eval>` and a `symbol` parameter. More details can be
found in the API doc for the :func:`otp.eval <onetick.py.eval>`
