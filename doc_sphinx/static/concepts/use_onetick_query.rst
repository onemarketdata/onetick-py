.. _use_onetick_query:

How to use onetick.query with onetick.py
========================================

``onetick.py`` is a wrapper around OneTick's python interface library ``onetick.query``.

But unlike ``onetick.query``, ``onetick.py`` is developed and supported separately,
so it doesn't cover 100% of functionality of ``onetick.query``.

Support for new OneTick functionality is being constantly added in the new releases of ``onetick.py``,
but in case of need it is possible to bypass some of the ``onetick.py`` interfaces
to use ``onetick.query`` classes and functions directly.

In all examples below this code is used to import ``onetick.query`` library:

.. testcode::

    from onetick.py.otq import otq


Implementing EP that is the source of ticks
-------------------------------------------

Some OneTick event processors are the source of ticks and they have a special interface in ``onetick.py``:
:py:class:`onetick.py.Source`.

Let's use the simplest source of ticks ``otq.TickGenerator`` as an example.
For already implemented and more powerful version see class :py:class:`onetick.py.Tick`.

.. doctest::

    >>> data = otp.Source(otq.TickGenerator(fields='long A = 1'))
    >>> data.schema
    {}


When using ``onetick.query`` the user is expected to set :ref:`the schema <schema concept>` manually.
That will let ``onetick.py`` internal logic know about the fields added or deleted from the tick schema
by the ``onetick.query`` classes and functions.


.. doctest::

    >>> node = otq.TickGenerator(fields='long A = 1', bucket_interval=0, bucket_time='BUCKET_START')
    >>> data = otp.Source(node, schema={'A': otp.long})
    >>> data.schema
    {'A': <class 'onetick.py.types.long'>}
    >>> data['B'] = data['A'] + 1
    >>> otp.run(data, symbols=f'{otp.config.default_db}::')
            Time  A  B
    0 2003-12-01  1  2


Implementing EP that can be sinked
----------------------------------

If OneTick's event processor is not a source of ticks then it can be sinked.

We can use method :meth:`onetick.py.Source.sink` to do this.
Let's use event processor ``otq.AddField`` as an example.
For already implemented and more powerful version see method :py:meth:`onetick.py.Source.__setitem__`.

Do not forget to update :ref:`the schema <schema concept>` if needed.

.. doctest::

    >>> data = otp.Tick(A=1)
    >>> data.sink(otq.AddField('B', '2'))
    >>> data.schema['B'] = otp.long
    >>> otp.run(data, symbols=f'{otp.config.default_db}::')
            Time  A  B
    0 2003-12-01  1  2


Implementing OneTick built-in functions
---------------------------------------

Another thing that can be inserted directly is OneTick built-in functions.
These functions are used in expressions when adding or updating fields.
These functions do not have special representation in ``onetick.query``, so we can just use strings.

Let's use OneTick's function ``REPLACE`` that replaces some substring in a string.
For already implemented and more powerful version see method
:py:meth:`onetick.py.Operation.str.replace <onetick.py.core.column_operations.accessors.str_accessor.replace>`.

Special class :py:class:`otp.raw <onetick.py.core.column_operations.base.Raw>`
can be used to represent arbitrary OneTick expression.
You only need to pass parameter ``dtype`` to let ``onetick.py`` internal logic know
what is the returned type of this function
and you will need to check that the types of arguments passed to this function are correct.


.. doctest::

    >>> data = otp.Tick(A='Hello world!')
    >>> data['B'] = otp.raw('REPLACE(A, "Hello", "Hi")', dtype=otp.string[64])
    >>> otp.run(data)
            Time             A          B
    0 2003-12-01  Hello world!  Hi world!
