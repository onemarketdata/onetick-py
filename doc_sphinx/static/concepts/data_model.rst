.. _data structures concept:

Data structures and functions
******************************

Tick sources
=============

Every piece of ``onetick.py`` code starts with specifying a data source.

::

    data = otp.DataSource(db='US_COMP', tick_type='TRD', symbol='AAPL', start=otp.dt(2003, 12, 1), end=otp.dt(2003, 12, 2))

:class:`onetick.py.Source` is the base abstract class. We provide several predefined inherited
classes to cover various use cases: e.g., :class:`onetick.py.DataSource` for retrieving data from OneTick databases,
:class:`onetick.py.Ticks` for creating ticks on the fly, and :class:`onetick.py.Empty` for creating a data source
with no ticks.

Every source should be associated with a data schema (it can be deduced or
specified  manually; see :ref:`the schema concept <schema concept>`).
The schema is available through the :attr:`onetick.py.Source.schema` property and behaves
like a Python `dict`.

.. testsetup::

   >>> sample_trd_tick = otp.Tick(PRICE=float(0))
   >>> sample_trd_tick.table(**session.real_db_schemas['us_comp_trd'], inplace=True)
   >>> session.dbs['US_COMP'].add(sample_trd_tick)

.. doctest::

   >>> data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL', start=otp.dt(2022, 3, 1), end=otp.dt(2022, 3, 2))
   >>> data.schema
    {'COND': string[4], 'CORR': <class 'onetick.py.types._int'>, 'DELETED_TIME': <class 'onetick.py.types.msectime'>, 'EXCHANGE': string[1], 'OMDSEQ': <class 'onetick.py.types.uint'>, 'PARTICIPANT_TIME': <class 'onetick.py.types.nsectime'>, 'PRICE': <class 'float'>, 'SEQ_NUM': <class 'int'>, 'SIZE': <class 'int'>, 'SOURCE': string[1], 'STOP_STOCK': string[1], 'TICKER': string[16], 'TICK_STATUS': <class 'onetick.py.types._int'>, 'TRADE_ID': string[20], 'TRF': string[1], 'TRF_TIME': <class 'onetick.py.types.nsectime'>, 'TTE': string[1]}

Next we discuss the ``Column`` and ``Operation`` classes that make it easy to work with the fields in data sources
and to create new ones. We then talk about the methods and functions that operate on
individual fields and on entire data sources.

Column and Operation
======================

A **column** (:class:`onetick.py.Column`) represents a data series for a single field of the data source
(The relationship between a column and a data source is similar to the one between pandas's Series and
DataFrame). Columns are accessed via the :meth:`onetick.py.Source.__getitem__` method:
i.e., you can refer to a field using brackets so the ``PRICE`` field is accessed as ``data['PRICE']``.
Note that only the fields that are specified in the schema can be accessed.

An **operation** is a generalization of a column that represents columns as
well as results of operations involving one or more columns.
Formally, :class:`onetick.py.Column` is a subclass of :class:`onetick.py.Operation`. Any operation between instances of
:class:`onetick.py.Column` return an instance of :class:`onetick.py.Operation`, e.g.:

::

    <an Operation instance> = <a Column instance> * <a Column instance>

Similarly, any operation between the instances of :class:`onetick.py.Operation` or :class:`onetick.py.Column` return
an instance of  :class:`onetick.py.Operation`:

::

    <an Operation instance> =
            <an Operation or Column instance> / <an Operation or Column instance>


In most cases, a user does not need to make a distinction between a column and an operation.
A new column can be created based on an existing column or an operation using
the *assignment* operator:

::

    <a Source instance>[<column name>] = <an Operation instance>

for example

::

    data['VOLUME'] = data['PRICE'] * data['SIZE']
    data['FLAG'] = (data['PRICE'] > 3.5) & (data['SIZE'] == 100)


Some functions operate on columns only but it's clear from the context that the use of operations is not applicable
there (e.g., the ``apply`` method that casts a column to a different type):

.. testcode::

   data = otp.Ticks({'A': ['1', '2', '3']})
   data['B'] = data['A'].apply(int) + 10
   print(otp.run(data))

.. testoutput::

                        Time  A   B
   0 2003-12-01 00:00:00.000  1  11
   1 2003-12-01 00:00:00.001  2  12
   2 2003-12-01 00:00:00.002  3  13

Field names
-----------

Onetick allows using field names that:

- have length between 1 and 127 characters
- contain upper- and lowercase Latin characters
- contain symbols "_" and "."

Any other character is not allowed in a field name.

In addition to that, Onetick does not allow lowercase Latin characters in field names stored in a database.
However, it allows using lowercase characters in field names in analytics:

.. testcode::

   data = otp.Ticks({'LowercaseField': [1, 2, 3]})
   print(otp.run(data))

.. testoutput::

                        Time  LowercaseField
   0 2003-12-01 00:00:00.000               1
   1 2003-12-01 00:00:00.001               2
   2 2003-12-01 00:00:00.002               3

If you try to save lowercase field name to a database, Onetick will silently convert it to upper case:

::

    test_db = otp.db.DB('TEST_DB')
    test_db.add(otp.Tick(FieldName=1), symbol='TEST', tick_type='TEST')
    session.use(test_db)
    otp.run(otp.DataSource(db='TEST_DB', tick_type='TEST'), symbols='TEST')

::

           Time  FIELDNAME
   0 2003-12-01          1


Functions and methods
=====================

There are various functions that can be applied to operations and sources.


Methods/functions on Operations
--------------------------------

The *column / operation based* functions and methods return an instance of :class:`onetick.py.Operation`

::

    otp.math.min(data['BID_SIZE'], data['ASK_SIZE'])

that can then be used for further operations (no pun intended):

::

    data['TAKEOUT_SUCCESS'] = \
        data['QTY_FILLED'] >= otp.math.min(data['BID_SIZE'], data['ASK_SIZE'])


The :class:`onetick.py.Operation` class also has methods, some of which are collected into *accessors*.
An *accessor* is a special property that collects methods for a certain data type. For example, the
:attr:`onetick.py.Operation.str` accessor collects the methods for working with strings:

::

    data['IS_ORDER_EXECUTED'] = data['STATE'].str.find('F')


Methods/functions on Sources
--------------------------------

:class:`onetick.py.Source` has methods that operate on entire ticks (rather than on particular columns) like
aggregations :attr:`onetick.py.Source.agg` or :meth:`onetick.py.Source.sort`. Usually
the result of such methods is a new instance(s) of :class:`onetick.py.Source` but for some methods it is an
instance of :class:`onetick.py.Operation` (e.g., :meth:`onetick.py.Source.apply`).

Retrieving ticks that satisfy a given condition (aka filtering) is done as follows:

::

    passed, not_passed = data[(data['FLAG'] > 0) & (data['STATE'] == 'F')]

Note that two new sources are returned: first is for the ticks that satisfy the condition
and the second for the ones that do not.

A typical filtering case looks like this:

::

    data, _ = data[(data['FLAG'] > 0) & (data['STATE'] == 'F')]



There are also functions that combine multiple sources such
as :func:`onetick.py.merge` or :func:`onetick.py.join_by_time`

::

    trades = otp.DataSource(db='US_COMP', tick_type='TRD', symbol='APPL')
    quotes = otp.DataSource(db='US_COMP', tick_type='QTE', symbol='AAPL')

    data = otp.join_by_time([trades, quotes])
