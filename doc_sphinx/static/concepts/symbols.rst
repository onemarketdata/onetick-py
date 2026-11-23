Databases, symbols, and tick types
**********************************

.. _symbols_concept:


The data is organized by database (e.g., ``US_COMP`` or ``CME``), symbol (e.g., ``AAPL``), and tick type (e.g., ``TRD``
or ``QTE``). A data source must specify these (possibly providing multiple values for each) to retrieve data.

A tick type can be set explicitly via the ``tick_type`` parameter of a data source.

The database name can be specified via the ``db`` parameter or as part of a symbol using the ``::`` separator.
For example:

.. doctest::

   >>> trades = otp.DataSource(db='US_COMP_SAMPLE', symbol='AAPL', tick_type='TRD', date=otp.dt(2024, 2, 1))
   >>> volume = trades.agg({'VOLUME': otp.agg.sum(trades['SIZE'])})
   >>> otp.run(volume)
           Time    VOLUME
   0 2024-02-02  70610921

is equivalent to

.. doctest::

   >>> trades = otp.DataSource(symbol='US_COMP_SAMPLE::AAPL', tick_type='TRD', date=otp.dt(2024, 2, 1))

The same technique applies to the database and tick type:

.. doctest::

   >>> trades = otp.DataSource(symbol='AAPL', tick_type='US_COMP_SAMPLE::TRD', date=otp.dt(2024, 2, 1))

Note that symbol name and tick type can be accessed as pseudo-fields in OneTick.
Those pseudo-fields are always present and you can access them like that:

.. doctest::

   >>> trades = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', date=otp.dt(2024, 2, 1))
   >>> trades['SYMBOL_NAME'] = trades['_SYMBOL_NAME']
   >>> trades['TICK_TYPE'] = trades['_TICK_TYPE']
   >>> trades = trades.first()
   >>> trades = trades[['PRICE', 'SIZE', 'SYMBOL_NAME', 'TICK_TYPE']]
   >>> otp.run(trades, symbols='AAPL')
                              Time  PRICE  SIZE SYMBOL_NAME TICK_TYPE
   0 2024-02-01 04:00:00.008283417  186.5     6        AAPL       TRD


Parameter ``identify_input_ts`` in :class:`otp.DataSource <onetick.py.DataSource>`
and :func:`otp.merge <onetick.py.merge>` can also be used to add them as new columns.


Symbols: bound and unbound
===========================

There are two ways of setting symbols: **bound** and **unbound**.

Bound symbols are specified when defining a source or with :func:`otp.merge <onetick.py.merge>`.
Unbound symbols are set when the query is executed and apply to all sources that do not specify bound symbols.
Here is an simple example of an unbound symbol:

.. doctest::

   >>> trades = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', date=otp.dt(2024, 2, 1))
   >>> volume = trades.agg({'VOLUME': otp.agg.sum(trades['SIZE'])})
   >>> otp.run(volume, symbols=['AAPL'])  # unbound symbol is set here
           Time    VOLUME
   0 2024-02-02  70610921

There is no difference between bound and unbound when we talk about a single symbol. To appreciate the difference, let's
look at two symbols.

.. doctest::

   >>> trades = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', date=otp.dt(2024, 2, 1))
   >>> volume = trades.agg({'VOLUME': otp.agg.sum(trades['SIZE'])})
   >>> dict(sorted(otp.run(volume, symbols=['AAPL', 'MSFT']).items()))
   {'AAPL':         Time    VOLUME
            0 2024-02-02  70610921,
    'MSFT':         Time    VOLUME
            0 2024-02-02  33577286}

The results for each unbound symbol are processed separately and returned in a separate pandas DataFrame. The result of the run
method above is a dict with symbols as keys and pandas DataFrames as values.

Contrast this with bound symbols where the ticks are merged into a single flow:

.. doctest::

   >>> trades = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', date=otp.dt(2024, 2, 1), symbols=['AAPL', 'MSFT'])
   >>> volume = trades.agg({'VOLUME': otp.agg.sum(trades['SIZE'])})
   >>> otp.run(volume)
            Time     VOLUME
    0 2024-02-02  104188207

Specifying bound symbols on a source is just a shorthand for the :func:`otp.merge <onetick.py.merge>` method added right
after the source definition.

.. doctest::

    >>> trades = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', date=otp.dt(2024, 2, 1))
    >>> cross_symbol_trades = otp.merge([trades], symbols=['AAPL', 'MSFT'])
    >>> volume = cross_symbol_trades.agg({'VOLUME': otp.agg.sum('SIZE')})
    >>> otp.run(volume)
            Time     VOLUME
    0 2024-02-02  104188207


Concurrency
-----------

Unbound symbols can be processed in parallel by specifying the ``concurrency`` parameter of :func:`otp.run <onetick.py.run>`.
You have to explicitly use :func:`otp.run <onetick.py.run>` to execute the query if you want to specify ``concurrency``.

::

    otp.run(volume, symbols=['AAPL', 'MSFT', 'AMZN', 'META'], concurrency=8)

For bound symbols, all calculations passed into :func:`otp.merge <onetick.py.merge>` (``trades`` in example above) run in parallel.
Calculations can be made faster by computing as much as possible per symbol before merging them.
The following  example has the same result as the previous one but it finds the total volume faster as it calculates
the volume for every symbol independently which can be done in parallel and then  adds up the total.

.. doctest::

    >>> trades = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', date=otp.dt(2024, 2, 1))
    >>> volume = trades.agg({'VOLUME': otp.agg.sum('SIZE')})
    >>> cross_symbol_volumes = otp.merge([volume], symbols=['AAPL', 'MSFT'], presort=True, identify_input_ts=True)
    >>> otp.run(cross_symbol_volumes, concurrency=8)
            Time    VOLUME SYMBOL_NAME TICK_TYPE
    0 2024-02-02  70610921        AAPL       TRD
    1 2024-02-02  33577286        MSFT       TRD

    >>> total_volume = cross_symbol_volumes.agg({'TOTAL_VOLUME': otp.agg.sum('VOLUME')})
    >>> otp.run(total_volume, concurrency=8)
            Time  TOTAL_VOLUME
    0 2024-02-02     104188207

.. note::
   All sources passed into :meth:`merge <onetick.py.merge>` are considered bound symbol sources.


Specifying symbols dynamically
==============================

In many cases it is necessary to select symbols from a databases according to some logic.
The :class:`otp.Symbols <onetick.py.Symbols>` source makes this easy.

.. doctest::

    >>> symbols = otp.Symbols(db='US_COMP_SAMPLE', date=otp.dt(2024, 2, 1), pattern='AA%')
    >>> otp.run(symbols)
            Time SYMBOL_NAME
    0 2024-02-01         AAL
    1 2024-02-01        AAPL

The result of :class:`otp.Symbols <onetick.py.Symbols>` can be used as bound/unbound symbols.

.. doctest::

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', date=otp.dt(2024, 2, 1))
    >>> data = data.first(3)
    >>> data = data[['PRICE','SIZE']]
    >>> dict(sorted(otp.run(data, symbols=otp.Symbols(db='US_COMP_SAMPLE', pattern='AA%')).items()))
    {'AAL':                             Time  PRICE  SIZE
             0 2024-02-01 04:00:00.097381367  14.33     1
             1 2024-02-01 04:00:00.138908789  14.37     1
             2 2024-02-01 04:00:00.726613365  14.36    10,
    'AAPL':                             Time   PRICE  SIZE
             0 2024-02-01 04:00:00.008283417  186.50     6
             1 2024-02-01 04:00:00.008290927  185.59     1
             2 2024-02-01 04:00:00.008291153  185.49   107}


Note that the interval of the main query is implicitly used in ``otp.Symbols(db='US_COMP_SAMPLE', pattern='AA%')``.


The ``symbols`` parameter can be any type of calculation that contains the ``SYMBOL_NAME`` field in the resulting ticks.
Every tick provides a separate symbol name in the ``SYMBOL_NAME`` field while the other fields are passed as
'symbol parameters'.


Symbol parameters
=================

Symbol parameters are any fields other than ``SYMBOL_NAME`` that are constant for an instrument. We illustrate this below.
First, consider a query that finds 2 most traded symbols among symbols starting with the letter 'A'. We'll later use the
output of this query to specify symbols and symbol params.

.. doctest::

   >>> trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', date=otp.dt(2024, 2, 1))
   >>> trd = trd.agg({'VOLUME': otp.agg.sum('SIZE')})
   >>> count = otp.merge([trd], symbols=otp.Symbols(db='US_COMP_SAMPLE', pattern='AA%'), presort=True, identify_input_ts=True)
   >>> most_traded = count.high('VOLUME', n=2)
   >>> otp.run(most_traded)
           Time    VOLUME SYMBOL_NAME TICK_TYPE
   0 2024-02-02  70610921        AAPL       TRD
   1 2024-02-02  41012294         AAL       TRD

The output of ``most_traded`` provides symbols in the ``SYMBOL_NAME`` column while the other columns  (``VOLUME`` and ``TICK_TYPE``)
provide symbol parameters. The following code retrieves the value of ``VOLUME`` for use in tick-by-tick analytics.

.. doctest::

   >>> trd = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
   >>> trd = trd.agg({'VOLUME': otp.agg.sum('SIZE')})
   >>> count = otp.merge([trd], symbols=otp.Symbols('US_COMP_SAMPLE', pattern='AA%'), identify_input_ts=True)
   >>> most_traded = count.high('VOLUME', n=2)
   >>>
   >>> data = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
   >>> data = data.first(3)
   >>> data = data[['PRICE','SIZE']]
   >>> data['VOLUME'] = data.Symbol['VOLUME', int]
   >>> dict(sorted(otp.run(data, symbols=most_traded, date=otp.dt(2024, 2, 1)).items()))
   {'AAL':                             Time   PRICE  SIZE    VOLUME
            0 2024-02-01 04:00:00.097381367   14.33     1  41012294
            1 2024-02-01 04:00:00.138908789   14.37     1  41012294
            2 2024-02-01 04:00:00.726613365   14.36    10  41012294,
    'AAPL':                            Time   PRICE  SIZE    VOLUME
            0 2024-02-01 04:00:00.008283417  186.50     6  70610921
            1 2024-02-01 04:00:00.008290927  185.59     1  70610921
            2 2024-02-01 04:00:00.008291153  185.49   107  70610921}

Equivalently, symbol parameters can be accessed by wrapping a function around the query as illustrated below.

.. doctest::

   >>> trd = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
   >>> trd = trd.agg({'VOLUME': otp.agg.sum('SIZE')})
   >>> count = otp.merge([trd], symbols=otp.Symbols('US_COMP_SAMPLE', pattern='AA%'), identify_input_ts=True)
   >>> most_traded = count.high('VOLUME', n=2)
   >>>
   >>> def query(sym):
   ...     data = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
   ...     data = data.first(3)
   ...     data = data[['PRICE','SIZE']]
   ...     data['VOLUME'] = sym['VOLUME']
   ...     return data
   >>>
   >>> dict(sorted(otp.run(query, symbols=most_traded, date=otp.dt(2024, 2, 1)).items()))
   {'AAL':                          Time   PRICE  SIZE    VOLUME
         0 2024-02-01 04:00:00.097381367   14.33     1  41012294
         1 2024-02-01 04:00:00.138908789   14.37     1  41012294
         2 2024-02-01 04:00:00.726613365   14.36    10  41012294,
    'AAPL':                         Time   PRICE  SIZE    VOLUME
         0 2024-02-01 04:00:00.008283417  186.50     6  70610921
         1 2024-02-01 04:00:00.008290927  185.59     1  70610921
         2 2024-02-01 04:00:00.008291153  185.49   107  70610921}

Time interval per symbol
========================

It is allowed to specify query interval per symbol using special fields ``_PARAM_START_TIME_NANOS`` and ``_PARAM_END_TIME_NANOS``
(``_PARAM_START_TIME`` and ``_PARAM_END_TIME`` can be used as well)


.. doctest::

   >>> custom_symbols = otp.Ticks(SYMBOL_NAME=['AAPL', 'MSFT'],
   ...                            _PARAM_START_TIME_NANOS=[otp.dt(2024, 2, 1, 10, 30), otp.dt(2024, 2, 1, 11)],
   ...                            _PARAM_END_TIME_NANOS=[otp.dt(2024, 2, 1, 11), otp.dt(2024, 2, 1, 11, 30)])
   >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
   >>> data = data.agg({'VOLUME': otp.agg.sum('SIZE')})
   >>> data['SYMBOL'] = data.Symbol.name
   >>> dict(sorted(otp.run(data, date=otp.dt(2024, 2, 1), symbols=custom_symbols).items()))
   {'AAPL':                  Time   VOLUME SYMBOL
            0 2024-02-01 11:00:00  3283224   AAPL,
    'MSFT':                  Time   VOLUME SYMBOL
            0 2024-02-01 11:30:00  1587813   MSFT}

Note that per symbol intervals should be inside the :ref:`query interval <static/concepts/start_end:Query interval>`.

The same can be done with a :pandas:`pandas.DataFrame` passed as a symbol list to
:func:`otp.merge <onetick.py.merge>`. Note that timezone-naive values are not localized when the
query is built: they are interpreted in the timezone passed to :func:`otp.run <onetick.py.run>`.
Timezone-aware values keep their own timezone instead.

.. doctest::

   >>> custom_symbols = pd.DataFrame({
   ...     'SYMBOL_NAME': ['AAPL', 'MSFT'],
   ...     '_PARAM_START_TIME': [otp.dt(2024, 2, 1, 10, 30), otp.dt(2024, 2, 1, 11)],
   ...     '_PARAM_END_TIME': [otp.dt(2024, 2, 1, 11), otp.dt(2024, 2, 1, 11, 30)],
   ... })
   >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
   >>> data = data.agg({'VOLUME': otp.agg.sum('SIZE')})
   >>> data = otp.merge([data], symbols=custom_symbols, identify_input_ts=True)
   >>> otp.run(data, date=otp.dt(2024, 2, 1))
                    Time   VOLUME SYMBOL_NAME TICK_TYPE
   0 2024-02-01 11:00:00  3283224        AAPL       TRD
   1 2024-02-01 11:30:00  1587813        MSFT       TRD

Associated symbols
==================

Associated symbols is a technique when unbound symbols are used to define bound symbols or symbols in related queries.
It is expressed using the :func:`otp.eval <onetick.py.eval>` and a `symbol` parameter. More details can be
found in the API doc for the :func:`otp.eval <onetick.py.eval>`

Special symbols
===============

OneTick reserves two symbol names that address the data of all symbols of a database:
``_ALL_SYMBOLS_`` and ``_ALL_DATA_``.
They are specified like regular symbols, usually with the database name as a prefix,
for example ``US_COMP_SAMPLE::_ALL_SYMBOLS_``.

.. note::
   These names are handled by the OneTick server and are available only in recent OneTick versions.
   A server that does not support them treats them as regular symbol names,
   so the query returns no data and no error.

``_ALL_SYMBOLS_``
-----------------

``_ALL_SYMBOLS_`` is a placeholder that is replaced with the list of all symbols
present in the database during the query interval.
It is a shorter alternative to :class:`otp.Symbols <onetick.py.Symbols>` when all symbols of the database are needed.

As an unbound symbol it produces a separate result for each symbol of the database:

.. doctest::

   >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
   >>> data = data.agg({'VOLUME': otp.agg.sum('SIZE')})
   >>> result = otp.run(data, symbols='US_COMP_SAMPLE::_ALL_SYMBOLS_',
   ...                  start=otp.dt(2024, 2, 1, 9, 30), end=otp.dt(2024, 2, 1, 9, 31))
   >>> len(result)
   501
   >>> result['US_COMP_SAMPLE::AAPL']
                    Time   VOLUME
   0 2024-02-01 09:31:00  1912066

To merge the ticks of all symbols into a single flow, pass the placeholder to :func:`otp.merge <onetick.py.merge>`:

.. doctest::

   >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
   >>> data = data.agg({'VOLUME': otp.agg.sum('SIZE')})
   >>> data = otp.merge([data], symbols='US_COMP_SAMPLE::_ALL_SYMBOLS_', presort=True, identify_input_ts=True)
   >>> data = data.high('VOLUME', n=3)
   >>> otp.run(data, start=otp.dt(2024, 2, 1, 9, 30), end=otp.dt(2024, 2, 1, 9, 31))
                    Time   VOLUME           SYMBOL_NAME TICK_TYPE
   0 2024-02-01 09:31:00  6583442     US_COMP_SAMPLE::T       TRD
   1 2024-02-01 09:31:00  2354286    US_COMP_SAMPLE::VZ       TRD
   2 2024-02-01 09:31:00  2191265  US_COMP_SAMPLE::AMZN       TRD

The placeholder can also be specified without the database prefix, as plain ``_ALL_SYMBOLS_``.
The database is then taken from the tick types bound in the query if they all name a single database
(e.g. ``US_COMP_SAMPLE::TRD``), or from the default database otherwise,
and the expanded symbol names have no database prefix:

.. doctest::

   >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
   >>> data = data.agg({'VOLUME': otp.agg.sum('SIZE')})
   >>> result = otp.run(data, symbols='_ALL_SYMBOLS_',
   ...                  start=otp.dt(2024, 2, 1, 9, 30), end=otp.dt(2024, 2, 1, 9, 31))
   >>> result['AAPL']
                    Time   VOLUME
   0 2024-02-01 09:31:00  1912066

``_ALL_DATA_``
--------------

``_ALL_DATA_`` retrieves the ticks of a tick type for all symbols of the database at once, as a single time series.
The ticks are returned in the order they are stored, not sorted by time,
and every tick has the query start time as its timestamp.
Two fields are added to each tick: ``SYMBOL_NAME`` with the symbol of the tick
and ``TICK_TIME`` with its original timestamp.
These fields are not in the schema of the database, so add them to the schema of the source to use them in the query.
As with ``_ALL_SYMBOLS_``, the database prefix can be omitted when the database is set in the source:

.. doctest::

   >>> data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
   >>> data.schema.update(SYMBOL_NAME=str, TICK_TIME=otp.nsectime)
   >>> data = data[['SYMBOL_NAME', 'TICK_TIME', 'LAST', 'VOLUME']]
   >>> data = data.limit(10)
   >>> otp.run(data, symbols='_ALL_DATA_',
   ...         start=otp.dt(2024, 2, 1, 9, 30), end=otp.dt(2024, 2, 1, 9, 35))
                    Time SYMBOL_NAME           TICK_TIME      LAST  VOLUME
   0 2024-02-01 09:30:00         AZO 2024-02-01 09:31:00  2750.000     600
   1 2024-02-01 09:30:00         AZO 2024-02-01 09:32:00  2750.000       0
   2 2024-02-01 09:30:00         AZO 2024-02-01 09:33:00  2750.000       0
   3 2024-02-01 09:30:00         AZO 2024-02-01 09:34:00  2751.215     400
   4 2024-02-01 09:30:00         DFS 2024-02-01 09:31:00   106.065    1943
   5 2024-02-01 09:30:00         DFS 2024-02-01 09:32:00   106.490    2101
   6 2024-02-01 09:30:00         DFS 2024-02-01 09:33:00   106.290    1000
   7 2024-02-01 09:30:00         DFS 2024-02-01 09:34:00   106.320    1178
   8 2024-02-01 09:30:00         CVS 2024-02-01 09:31:00    74.325   34849
   9 2024-02-01 09:30:00         CVS 2024-02-01 09:32:00    74.160   22384

The ``SYMBOL_NAME`` field can be used to calculate per-symbol statistics for the whole database in a single query:

.. doctest::

   >>> data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
   >>> data.schema.update(SYMBOL_NAME=str, TICK_TIME=otp.nsectime)
   >>> data = data.agg({'VOLUME': otp.agg.sum('VOLUME')}, group_by='SYMBOL_NAME')
   >>> data = data.high('VOLUME', n=3)
   >>> otp.run(data, symbols='_ALL_DATA_',
   ...         start=otp.dt(2024, 2, 1, 9, 30), end=otp.dt(2024, 2, 1, 9, 35))
                    Time SYMBOL_NAME   VOLUME
   0 2024-02-01 09:35:00           T  2587281
   1 2024-02-01 09:35:00        TSLA  1880226
   2 2024-02-01 09:35:00       GOOGL  1636029

As all ticks have the same timestamp, any time-based logic (e.g. aggregation with buckets)
sees them all at the query start time.
To restore the original time order, sort the ticks by ``TICK_TIME`` and assign it to the timestamp:

.. doctest::

   >>> data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
   >>> data.schema.update(SYMBOL_NAME=str, TICK_TIME=otp.nsectime)
   >>> data = data.where(data['SYMBOL_NAME'].isin('AAPL', 'MSFT'))
   >>> data = data.sort(['TICK_TIME', 'SYMBOL_NAME'])
   >>> data['TIMESTAMP'] = data['TICK_TIME']
   >>> data = data[['SYMBOL_NAME', 'LAST', 'VOLUME']]
   >>> otp.run(data, symbols='_ALL_DATA_',
   ...         start=otp.dt(2024, 2, 1, 9, 30), end=otp.dt(2024, 2, 1, 9, 33))
                    Time SYMBOL_NAME      LAST  VOLUME
   0 2024-02-01 09:31:00        AAPL  184.2850  405208
   1 2024-02-01 09:31:00        MSFT  401.9450  294540
   2 2024-02-01 09:32:00        AAPL  184.2099  269872
   3 2024-02-01 09:32:00        MSFT  403.8700  215629

.. note::
   ``_ALL_DATA_`` must be the only symbol of the query, and the query must run forward and without merging.
   Data quality events are not reported for it.

   The symbol is recognized by the query alone, so a database may contain a symbol literally named ``_ALL_DATA_``,
   whose own data is then unreachable under that name.

   Some databases do not support ``_ALL_DATA_``, in this case the query fails with an error.
