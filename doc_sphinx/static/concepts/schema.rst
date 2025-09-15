.. _schema concept:

Schema
======

Every source comes with fields and field types, i.e., the tick *schema*.
A developer could access and modify the schema using :attr:`onetick.py.Source.schema` of any source.

The schema for a data source looks like this:

.. testsetup::

   >>> sample_trd_tick = otp.Tick(PRICE=float(0))
   >>> sample_trd_tick.table(**session.real_db_schemas['us_comp_trd'], inplace=True)
   >>> session.dbs['US_COMP'].add(sample_trd_tick)

.. doctest::

   >>> data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL', start=otp.dt(2022, 3, 1), end=otp.dt(2022, 3, 2))
   >>> data.schema
    {'COND': string[4], 'CORR': <class 'onetick.py.types._int'>, 'DELETED_TIME': <class 'onetick.py.types.msectime'>, 'EXCHANGE': string[1], 'OMDSEQ': <class 'onetick.py.types.uint'>, 'PARTICIPANT_TIME': <class 'onetick.py.types.nsectime'>, 'PRICE': <class 'float'>, 'SEQ_NUM': <class 'int'>, 'SIZE': <class 'int'>, 'SOURCE': string[1], 'STOP_STOCK': string[1], 'TICKER': string[16], 'TICK_STATUS': <class 'onetick.py.types._int'>, 'TRADE_ID': string[20], 'TRF': string[1], 'TRF_TIME': <class 'onetick.py.types.nsectime'>, 'TTE': string[1]}


The schema is updated when a new column is added:

.. testcode::

   data = otp.Ticks({'X': [7, 4, 11] })
   data['Y'] = data['X'] * 0.5
   print(data.schema)

.. testoutput::

   {'X': <class 'int'>, 'Y': <class 'float'>}

Here is the list of some of the supported types (see full list in :ref:`Types <otp types>`):

- ``int`` (maps to the ``int64`` in OneTick)
- ``float`` (maps to the ``double`` in OneTick)
- :class:`onetick.py.nsectime` for nanosecond precision timestamps (the ``nsectime`` in OneTick)
- :class:`onetick.py.msectime` for milliseconds precision timestamp (the ``msectime`` in OneTick)
- ``str`` for string not more than 64 characters (the ``string`` in OneTick)
- :class:`onetick.py.string` for any kind of strings, ``string[64]`` is equal to the ``str`` (the ``string[N]`` in OneTick)

.. note::
   The `onetick.py` package doesn't have the **boolean** type. Condition expressions use the `float` type with `1.0` and `0.0` values.

The schema allows being explicit about the fields available for analytics when the data is not available.
This is important as the construction of the calculation graph is separate  from the execution (i.e., from when the data becomes available).
The ticks in a database could have different set of columns or types from what the code assumes.

By default the schema is deduced automatically for each source but this may not always work
(and as you'll see below we'll recommend setting
it explicitly anyway). For example, it is impossible to deduce a single schema for a data source when
different symbols have different schemas which will lead to errors when updating a field that does not exist or adding
a field that exists (note that the operations to add and modify fields look the same to the user `data['X'] = 1`)

Another example is when  your logic aims to analyze trades and you set the schema to expect `PRICE=float` and `SIZE=int` but
quotes are passed as a source leading to a runtime error.

Schema deduction mechanism
--------------------------

:class:`onetick.py.DataSource` is the main interface to read from the OneTick database.

This is one of the few places where ``onetick-py`` can't easily get the schema of the data,
so this class has the schema *guessing* / *deduction* mechanism
based on the passed `db`, `tick_type`, `symbol(-s)`, `start` and `end` parameters.

It is convenient and fits well with the Jupyter style of code writing and with tests,
however this mechanism *does not guarantee* calculating correct schema in all cases and it also requires
making an additional query to get the schema, thus *affecting performance* of the query generation.

That is why
we **strongly recommend to explicitly disable this mechanism and specify the schema manually for production cases**.

The schema deduction takes place in the constructor of the :class:`onetick.py.DataSource`,
and it is enabled by default when the `db` parameter is specified.
It is possible to control this behaviour with the `schema_policy` parameter.

.. testcode::

    data = otp.DataSource(db='US_COMP',
                          tick_type='QTE',
                          symbol='AAPL',
                          schema_policy='manual')
    data.schema.set(ASK_PRICE=float, BID_PRICE=float, ASK_SIZE=int, BID_SIZE=int)

**schema_policy=manual** means that the source object has an empty schema
and it is expected that the schema will be set manually by the user
using the :attr:`onetick.py.Source.schema` methods.

That is the recommended way for production code.

Fields not in the schema
------------------------

It is possible that the source has more fields than needed for the use case. Defining only the necessary fields in the
schema does not actually remove them: they are still propagated. However, the :meth:`onetick.py.Source.table`
method can be used to propagate only the specified fields and to set the schema accordingly.

.. testcode::

   data = data.table(ASK_PRICE=float, BID_PRICE=float, ASK_SIZE=int, BID_SIZE=int)

:meth:`onetick.py.Source.table` guarantees the fields will be present during runtime even if
the fields are not present in the data. In this case, a field is filled with the default value for the corresponding
field type.

Types change
------------

The field type can be modified. This is done implicitly when values of a different type are assigned to the field

.. testcode::

    data['X'] = 1              # it is the `int` type
    data['X'] = data['X'] / 2  # here it becomes `float`

or it could be done explicitly using the :meth:`onetick.py.Source.apply` method
(or equivalently -- :meth:`onetick.py.Source.astype`)

.. testcode::

    data['X'] = data['X'].apply(str)
