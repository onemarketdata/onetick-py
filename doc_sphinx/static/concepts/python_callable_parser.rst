.. _python callable parser:

Python callable parsing
***********************

There are currently several methods that implement complex translation of python code
to OneTick's expressions:

- :meth:`onetick.py.Operation.apply`
- :meth:`onetick.py.Source.apply`
- :meth:`onetick.py.Source.script`.


Translation
===========

The main feature of these methods is that, unlike most of the other :class:`onetick.py.Source` methods,
using callables for them is not resulting in these callables being called in python, but rather
these callables being translated from python code to OneTick expressions.

Therefore, these callables will not be called on each tick and they will not be called even once.

Methods :meth:`onetick.py.Operation.apply` and :meth:`onetick.py.Source.apply` work the same
and return a OneTick's ``CASE`` expression as a new :class:`onetick.py.Column` object,
but the first parameter of the callable for :meth:`onetick.py.Operation.apply` represents the current column
and for :meth:`onetick.py.Source.apply` it represents the whole source object.

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> t['A'].apply(lambda x: x + 1 if x >= 0 else x - 1)
   Column(CASE((A) >= (0), 1, (A) + (1), (A) - (1)), <class 'int'>)

   >>> t.apply(lambda x: x['A'] + 1 if x['A'] >= 0 else x['A'] - 1)
   Column(CASE((A) >= (0), 1, (A) + (1), (A) - (1)), <class 'int'>)


Using functions for these methods works the same.

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> def fun(x):
   ...     if x >= 0:
   ...         return x + 1
   ...     return x - 1
   >>> t['A'].apply(fun)
   Column(CASE((A) >= (0), 1, (A) + (1), (A) - (1)), <class 'int'>)

   >>> def fun(x):
   ...     if x['A'] >= 0:
   ...         return x['A'] + 1
   ...     return x['A'] - 1
   >>> t.apply(fun)
   Column(CASE((A) >= (0), 1, (A) + (1), (A) - (1)), <class 'int'>)


Method :meth:`onetick.py.Source.script` works differently and in this case
python's callable is translated to OneTick's per-tick script language and
this script is used with ``SCRIPT`` event processor and method returns new :class:`onetick.py.Source` object.
Also the first argument of the callable represents the special input tick object.

.. doctest::

   >>> t = otp.Ticks(A=[-1, 1])
   >>> def fun(tick):
   ...     tick['B'] = 0
   ...     if tick['A'] >= 0:
   ...         tick['B'] = 1
   >>> t = t.script(fun)
   >>> otp.run(t)
                        Time  A  B
   0 2003-12-01 00:00:00.000 -1  0
   1 2003-12-01 00:00:00.001  1  1



Apply methods
=============

:meth:`onetick.py.Operation.apply` and :meth:`onetick.py.Source.apply`

For apply methods the main idea is that the logic of the python callable
should be convertible to OneTick's ``CASE`` expression.

Python's lambda-expressions have the same semantic capabilities as OneTick's ``CASE`` expressions,
so there are no limitations for them.

For functions some restrictions should be taken into consideration.

Only these operators are supported:

- ``if`` statement
- ``return`` statement

.. doctest::

   >>> t = otp.Ticks(A=[-1, 1])
   >>> def fun(x):
   ...     if x['A'] >= 0:
   ...         return x['A'] + 1
   ...     return x['A'] - 1
   >>> t['X'] = t.apply(fun)
   >>> otp.run(t)
                        Time  A   X
   0 2003-12-01 00:00:00.000 -1  -2
   1 2003-12-01 00:00:00.001  1   2

Also, the python code translation is very flexible and allows
some of the python operators and methods to be translated into simpler ones or
to be executed in python and then be translated to OneTick.

- Simple ``for`` statement can be replaced with it's duplicated body:

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> def fun(x):
   ...     for i in [999, 55, 1]:
   ...         if x['A'] > i:
   ...             return i
   ...     return -333
   >>> t.apply(fun)
   Column(CASE((A) > (999), 1, 999, CASE((A) > (55), 1, 55, CASE((A) > (1), 1, 1, -333))), <class 'int'>)

- If the first parameter is propagated to inner python callables,
  the code of these callables will also be translated (they won't be called):

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> def inner_fun(x):
   ...     if x['A'] >= 10:
   ...         return 10
   ...     return 0
   >>> t.apply(lambda x: inner_fun(x) if x['A'] > 0 else -10)
   Column(CASE((A) > (0), 1, CASE((A) >= (10), 1, 10, 0), -10), <class 'int'>)

- All other inner python callables will be called once and their result will be inserted in OneTick expression:

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> t.apply(lambda x: x['A'] + sum([1, 2, 3, 4, 5]))
   Column((A) + (15), <class 'int'>)


Per-tick script
===============

For :meth:`onetick.py.Source.script` method more python operators may also be used in the function:


Adding new and modifying existing columns
-----------------------------------------

It can be done by modifying first parameter of the function.
This parameter represents input tick.

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> def fun(tick):
   ...     tick['A'] += 1
   ...     tick['B'] = 'B'
   >>> t = t.script(fun)
   >>> otp.run(t)
           Time  A B
   0 2003-12-01  2 B


Filtering ticks
---------------

By default, all input ticks are returned in script.
To filter tick out you can use ``return False`` statement.
If some ``return`` statements are specified, then all ticks
are filtered out by default, and the user is expected to control
all cases where ticks should be propagated or not.

.. doctest::

   >>> t = otp.Ticks(A=[1, -1])
   >>> def fun(tick):
   ...     if tick['A'] < 0:
   ...         return False
   ...     return True
   >>> t = t.script(fun)
   >>> otp.run(t)
           Time  A
   0 2003-12-01  1


Propagating ticks
-----------------

``yield`` statement allows to propagate tick more than once.

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> def fun(tick):
   ...     yield
   ...     yield
   ...     return False
   >>> t = t.script(fun)
   >>> otp.run(t)
           Time  A
   0 2003-12-01  1
   1 2003-12-01  1


Local variables
---------------

Simple local variables are redefined on each arrived input tick.
Static local variables are defined once and their values are saved
between the arrival of input ticks.

.. doctest::

   >>> t = otp.Ticks(A=[0, 1])
   >>> def fun(tick):
   ...     a = 1234
   ...     b = otp.static(0)
   ...     a = a + 1
   ...     b = b + 1
   ...     tick['A'] = a * 2
   ...     tick['B'] = b
   >>> t = t.script(fun)
   >>> otp.run(t)
                        Time     A  B
   0 2003-12-01 00:00:00.000  2470  1
   1 2003-12-01 00:00:00.001  2470  2


Python function calls
---------------------

Simple python function calls results are also inserted into resulting OneTick code.

For example python built-in function ``sum`` will be replaced with its returned value:

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> def fun(tick):
   ...     tick['A'] = sum([1, 2, 3, 4, 5])
   >>> t = t.script(fun)
   >>> otp.run(t)
           Time   A
   0 2003-12-01  15


Python functions can also be translated to OneTick per-tick script syntax,
but only if they have certain signature and defined parameters and return value types:

.. doctest::

   >>> t = otp.Tick(A=7)

   >>> def multiply_a(tick, a: int) -> int:
   ...     return tick['A'] * a

   >>> def fun(tick):
   ...     tick['B'] = multiply_a(tick, 21)
   >>> t = t.script(fun)
   >>> otp.run(t)
           Time  A    B
   0 2003-12-01  7  147


Python ``for`` statements
-------------------------

Simple ``for`` statements can also be translated to per-tick script.
Only iterating over simple sequences (lists, tuples) with simple values (string and numbers) is supported.

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> def fun(tick):
   ...     for i in [1, 2, 3, 4, 5]:
   ...         tick['A'] += i
   >>> t = t.script(fun)
   >>> otp.run(t)
           Time   A
   0 2003-12-01  16


Looping with ``while`` statement
--------------------------------

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> def fun(tick):
   ...     tick['X'] = 0
   ...     while tick['X'] < 5:
   ...         tick['X'] += 1
   >>> t = t.script(fun)
   >>> otp.run(t)
           Time  A  X
   0 2003-12-01  1  5


Python ``with`` statement
-------------------------

``otp.once`` context manager can be used to execute code only once for the first tick, not on each arriving tick:


.. doctest::

   >>> t = otp.Ticks(A=[1, 2], B=[3, 4])
   >>> def fun(tick):
   ...     with otp.once():
   ...         tick['A'] = tick['B']
   >>> t = t.script(fun)
   >>> otp.run(t)
                        Time  A  B
   0 2003-12-01 00:00:00.000  3  3
   1 2003-12-01 00:00:00.001  2  4


Tick sequences
--------------

You can iterate over tick sequences inside per-tick script.
These sequences should be created outside of the per-tick script.

- :func:`otp.state.tick_sequence_tick`
- :func:`otp.state.tick_list`
- :func:`otp.state.tick_set`
- :func:`otp.state.tick_set_unordered`
- :func:`otp.state.tick_deque`

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> t.state_vars['list'] = otp.state.tick_list(otp.eval(otp.Ticks(X=[1, 2, 3, 4, 5])))
   >>> def fun(tick):
   ...     for t in tick.state_vars['list']:
   ...         tick['A'] += t['X']
   >>> t = t.script(fun)
   >>> otp.run(t)
           Time   A
   0 2003-12-01  16



Tick descriptor fields
----------------------

It's possible to iterate over tick descriptor fields in per-tick script,
get their names and types.

.. doctest::

   >>> t = otp.Tick(A=1, B=2)
   >>> def fun(tick):
   ...     tick['NAMES'] = ''
   ...     for field in otp.tick_descriptor_fields():
   ...         tick['NAMES'] += field.get_name() + ','
   >>> t = t.script(fun)
   >>> otp.run(t)
           Time  A  B  NAMES
   0 2003-12-01  1  2   A,B,

.. autoclass:: onetick.py.core.per_tick_script.TickDescriptorFields

.. autoclass:: onetick.py.core.per_tick_script.TickDescriptorField
   :members:


Tick objects
------------

Tick objects can be created inside per-tick script.
They can be copied or modified, also some methods are available.

- :func:`otp.state.tick_list_tick <onetick.py.core.per_tick_script.tick_list_tick>`
- :func:`otp.state.tick_set_tick <onetick.py.core.per_tick_script.tick_set_tick>`
- :func:`otp.state.tick_deque_tick <onetick.py.core.per_tick_script.tick_deque_tick>`
- :func:`otp.state.dynamic_tick <onetick.py.core.per_tick_script.dynamic_tick>`

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> def fun(tick):
   ...     t = otp.dynamic_tick()
   ...     t['A'] = 12345
   ...     tick.copy_tick(t)
   >>> t = t.script(fun)
   >>> otp.run(t)
           Time      A
   0 2003-12-01  12345


Input tick object also has method :py:meth:`~onetick.py.core.lambda_object._EmulateInputObject.copy_tick`
that can be used to copy data from input tick to the new tick object.

.. automethod:: onetick.py.core.lambda_object._EmulateInputObject.copy_tick


Get or set ticks' field via Operations
--------------------------------------

Ticks' field can be accessed via Operations with return field's name.

.. autofunction:: onetick.py.core.lambda_object._EmulateObject.get_long_value
.. autofunction:: onetick.py.core.lambda_object._EmulateObject.get_double_value
.. autofunction:: onetick.py.core.lambda_object._EmulateObject.get_string_value
.. autofunction:: onetick.py.core.lambda_object._EmulateObject.get_datetime_value
.. autofunction:: onetick.py.core.lambda_object._EmulateObject.set_long_value
.. autofunction:: onetick.py.core.lambda_object._EmulateObject.set_double_value
.. autofunction:: onetick.py.core.lambda_object._EmulateObject.set_string_value
.. autofunction:: onetick.py.core.lambda_object._EmulateObject.set_datetime_value


Using input ticks
-----------------

In some cases you might want to get tick the way it was before being updated in script.
You can do it with using `input` attribute.

.. doctest::

   >>> t = otp.Tick(A=1)
   >>> def fun(tick):
   ...     tick['A'] = 2
   ...     tick['B'] = tick.input['A']
   >>> t = t.script(fun)
   >>> otp.run(t)
           Time  A  B
   0 2003-12-01  2  1


Logging
-------

.. autofunction:: onetick.py.core.per_tick_script.logf


Error handling
--------------

.. autofunction:: onetick.py.core.per_tick_script.throw_exception

