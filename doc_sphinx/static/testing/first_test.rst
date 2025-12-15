Your first test
===============


Let's assume that we have some project in the `project-folder` folder.

::

    project-folder/


The first test
--------------

Let's add a simple test to the project. A few words about naming first.

``pytest`` has an `auto discovery <https://docs.pytest.org/en/latest/goodpractices.html#conventions-for-python-test-discovery>`_ mechanism,
and ``test_*.py`` or ``*_test.py`` files are included as files containing tests.
Every test case is either a Python function or method starting with the ``test_`` prefix or ending with the ``_test`` suffix.
They can be combined into classes with the ``Test`` prefix.

The best practices say to keep tests in the ``tests`` folder.
Lets add a first test into the new file ``test_simple.py``:

.. code-block:: bash

    project-folder/
    └── tests/
        └── test_simple.py


with the following logic:

.. code-block:: python

    def test_first():
        print('Hey-ho!')


You can easily run it inside the ``project-folder`` folder:

.. code-block:: bash

    $ pytest -vs

pytest automatically finds the test and runs it.
The output should look like this:

.. code-block:: bash

    =========================== test session starts =====================
    platform linux -- Python 3.9.6, pytest-7.1.2, pluggy-1.3.0 -- python3
    cachedir: .pytest_cache
    rootdir: /project-folder
    plugins: timeout-1.3.3, mock-1.11.0, cov-2.7.1
    collected 1 item

    tests/test_simple.py::test_first Hey-ho!
    PASSED


Plugins import
--------------

We need to import our ``onetick-py-test`` pytest plugin into the project.

``pytest`` recommends creating a ``conftest.py`` file in the root of the project
and keeping plugin imports and common helpers there.

.. code-block:: bash

    project-folder/
    ├── conftest.py
    └── tests/
        └── test_simple.py


Let's add the following line to ``conftest.py`` to import our plugin:

::

    pytest_plugins = ['onetick.test']


Everything imported in ``conftest.py`` automatically becomes available in all tests starting from the folder
where it's placed and down to all sub-folders recursively.


.. note::

    What does it import and add?

    Our plugin exposes different onetick-py configurations as common pytest fixtures
    (see `api <https://docs.pytest.org/en/latest/reference.html#fixtures-api>`_
    and `fixtures <https://docs.pytest.org/en/latest/fixture.html>`_) and helpers for debugging.

    More details can be found in the :ref:`list of onetick-py-test fixtures <onetick py test features>`.


Test onetick.py code
---------------------

Let's consider ``onetick.py`` code that calculates directional volume imbalance in a window of a given length.

The code also sets a flag to 1 if the buy size exceeds the sell size by at least the given threshold and to -1 if the opposite is true.


::

    import onetick.py as otp

    def trades_imbalance(orders: otp.Source,
                         threshold: int,
                         window_in_sec: int):
        '''
        Build `window_in_sec`-second buckets of buy and sell orders,
        join them and compare whether the volume was more than the `threshold`
        on one of the sides
        '''
        buy, sell = orders[(orders['SIDE'] == 'BUY')]

        buy = buy.agg({'BUY_SIZE': otp.agg.sum('SIZE'),
                       'BUY_COUNT': otp.agg.count()},
                       bucket_interval=window_in_sec)
        sell = sell.agg({'SELL_SIZE': otp.agg.sum('SIZE'),
                         'SELL_COUNT': otp.agg.count()},
                        bucket_interval=window_in_sec)

        result = otp.join(buy, sell, on='same_size')

        result = result.where((result['BUY_COUNT'] > 0) | (result['SELL_COUNT'] > 0))

        result['FLAG'] = result.apply(
            lambda tick:
                otp.math.sign(tick['BUY_SIZE'] - tick['SELL_SIZE'])
                if abs(tick['BUY_SIZE'] - tick['SELL_SIZE']) > threshold
                else 0
        )

        return result


The ``trades_imbalance`` interface allows passing a data source.

We will use :class:`otp.Ticks <onetick.py.Ticks>` to generate ticks with the goal of
checking that the code is at least runnable:

::

    def test_simple(m_session):
        orders = otp.Ticks([
            ['SIZE',  'SIDE', 'offset'],
            [     5,   'BUY',        0],
            [     7,  'SELL',      150],
            [    20,  'SELL',      700],
            [   100,   'BUY',     1100],
            [    70,  'SELL',     1900],
            [    55,   'BUY',     2300],
            [    59,  'SELL',     2430]
        ])

        res = trades_imbalance(orders,
                               threshold=5,
                               window_in_sec=1)

        df = otp.run(res)

        print()
        print(df)

        assert all(df['FLAG'] == [-1, 1, 0])
        assert all(df['BUY_SIZE'] == [5, 100, 55])
        assert all(df['SELL_SIZE'] == [27, 70, 59])


In this test we create a data source, pass it to the function we'd like to test, and check
the result. The ``m_session`` object is a ``onetick-py-test`` session *fixture*.

.. note::

    You may notice that there is no specified symbols and start / end times. Our framework
    has predefined default values to make it easier to write tests.
    We allow developers to change the defaults as we describe later.

Let's run it:


.. code-block:: bash

    $ pytest -vs

    =========================== test session starts =====================
    platform linux -- Python 3.9.6, pytest-7.1.2, pluggy-1.3.0 -- python3

    OneTick build: 20230831120000, onetick-py: 1.82.0, onetick-py-test: 1.1.34
    cachedir: .pytest_cache
    rootdir: /project-folder
    plugins: timeout-1.3.3, mock-1.11.0, pyfakefs-5.2.4, cov-2.7.1
    collected 1 item

    test_simple.py::test_simple
                     Time  BUY_SIZE  BUY_COUNT  SELL_SIZE  SELL_COUNT  FLAG
    0 2023-12-01 00:00:01         5          1         27           2    -1
    1 2023-12-01 00:00:02       100          1         70           1     1
    2 2023-12-01 00:00:03        55          1         59           1     0
    PASSED


Same test -- different parameters
---------------------------------

``pytest`` allows to run the same test with different sets of parameters.
Let's give it a try:

::

    @pytest.mark.parametrize(
        'threshold,expected_res', [
            ( 1, [-1, 1, -1]),
            (20, [-1, 1,  0]),
            (25, [ 0, 1,  0]),
            (35, [ 0, 0,  0])
        ]
    )
    def test_threshold(m_session, threshold, expected_res):
        orders = otp.Ticks([
            ['SIZE',  'SIDE', 'offset'],
            [     5,   'BUY',        0],
            [     7,  'SELL',      150],
            [    20,  'SELL',      700],
            [   100,   'BUY',     1100],
            [    70,  'SELL',     1900],
            [    55,   'BUY',     2300],
            [    59,  'SELL',     2430]
        ])

        res = trades_imbalance(orders,
                               threshold=threshold,
                               window_in_sec=1)

        df = otp.run(res)

        assert all(df['FLAG'] == expected_res)


This is a standard ``pytest`` technique. More about it could be found on the official site.

Add databases
--------------

In some cases a developer may want to use ticks from a OneTick database.

We suggest using :class:`otp.DB <onetick.py.db.db.DB>` for this goal. A developer can
create a new database, add ticks there under the specified tick type, symbol and date, and then
use it the code.

Let's change our test example to use ticks from a database:

::

    def test_db(f_session):
        orders = otp.Ticks([
            ['SIZE',  'SIDE', 'offset'],
            [     5,   'BUY',        0],
            [     7,  'SELL',      150],
            [    20,  'SELL',      700],
            [   100,   'BUY',     1100],
            [    70,  'SELL',     1900],
            [    55,   'BUY',     2300],
            [    59,  'SELL',     2430]
        ])

        # define database
        db = otp.DB('SOME_DB')

        # add ticks into the database
        db.add(orders,
               symbol='MSFT',
               tick_type='ORDER',
               date=otp.dt(2023, 1, 1))

        # include the database in the session
        f_session.use(db)

        # read ticks from our database
        src = otp.DataSource(db='SOME_DB',
                             tick_type='ORDER',
                             symbol='MSFT',
                             date=otp.dt(2023, 1, 1))

        # use ticks from the database instead of Ticks
        res = trades_imbalance(src,
                               threshold=5,
                               window_in_sec=1)

        df = otp.run(res)

        assert all(df['FLAG'] == [-1, 1, 0])
        assert all(df['BUY_SIZE'] == [5, 100, 55])
        assert all(df['SELL_SIZE'] == [27, 70, 59])

Note that we use the ``f_session`` fixture here. If we added a database into the ``m_session``
then it would be available for every test in a module that uses that fixture;
for the ``f_session`` it available only for this test.

We recommend to re-use databases as much as possible because the database creation mechanism
works with the filesystem objects that could slow down a test.

The following example shows how to re-use databases:

::

    @pytest.fixture(scope='module')
    def session_with_dbs(m_session):
         orders = otp.Ticks([
            ['SIZE',  'SIDE', 'offset'],
            [     5,   'BUY',        0],
            [     7,  'SELL',      150],
            [    20,  'SELL',      700],
            [   100,   'BUY',     1100],
            [    70,  'SELL',     1900],
            [    55,   'BUY',     2300],
            [    59,  'SELL',     2430]
        ])

        # define database
        db = otp.DB('SOME_DB')

        # add ticks into the database
        db.add(orders,
               symbol='MSFT',
               tick_type='ORDER',
               date=otp.dt(2023, 1, 1))

        # include database into the session
        m_session.use(db)

        yield m_session


    def test_1(session_with_dbs):
        ...

    def test_2(session_with_dbs):
        ...


Here we create a fixture based on the default *module* scope session, add databases there,
re-use it as a fixture in tests; the added databases are available for all tests where the
common fixture is used.

OTQ query
---------

``pytest`` can be used to test queries written in OneTick Query Designer (OTQs).

A developer can point to a query from some OTQ file on the local filesystem using
:class:`otp.query <onetick.py.query>`:

.. code-block:: python

    import onetick.py as otp

    query = otp.query("my.otq::Query")


It also allows to pass parameters to the query as key-value arguments.

Let's consider an example of how to test a
:download:`Bollinger Bands query <../../../doctest_resources/test_existed.otq>`:

.. testcode::

    query = otp.query("test_existed.otq::bollinger_bands",
                      # query parameters
                      INTERVAL_UNITS="SECONDS",
                      INTERVAL=3)
    data = otp.Ticks(PRICE=[1.45, 1.55, 1.45, 1.30, 1.40],
                     offset=[0, 1000, 2000, 4000, 10_000])
    data = data.apply(query)
    df = otp.run(data)
    print(df)

The result is:

.. testoutput::

                     Time  PRICE   AVERAGE   STDDEV  LOWER_BAND  UPPER_BAND
    0 2003-12-01 00:00:00   1.45  1.450000  0.00000    1.450000    1.450000
    1 2003-12-01 00:00:01   1.55  1.500000  0.05000    1.450000    1.550000
    2 2003-12-01 00:00:02   1.45  1.483333  0.04714    1.436193    1.530474
    3 2003-12-01 00:00:04   1.30  1.375000  0.07500    1.300000    1.450000
    4 2003-12-01 00:00:10   1.40  1.400000  0.00000    1.400000    1.400000


.. note::

   OneTick resolves the relative path to an OTQ file for the paths specified in the ``OTQ_PATH`` config
   variable of the OneTick config.

   The testing framework adds the current path (the path where a default session like ``f_session`` is used / initialized)
   to the ``OTQ_PATH`` of the OneTick config by default.

Default timezone, symbol, etc
-----------------------------

We mentioned before that the testing framework uses predefined default values for the timezone,
the symbol, the start and end for the query interval, etc. It simplifies testing in most
cases however sometimes a developer wants to change a default value to something else.

Here are two ways we could recommend:

- use fixtures like ``default_tz`` to override default values as described in this :ref:`onetick py test features`.

- patch the :class:`otp.config <onetick.py.configuration.Config>` using the ``monkeypatch`` default pytest fixture,
  or simply just change the default values directly in the ``conftest.py`` file.


Sessions customization
----------------------

The default sessions like the ``f_session`` could be customized a bit.

For example a developer can extend the default ``OTQ_PATH`` and ``CSV_PATH`` OneTick config variables
using the ``otq_path`` and ``csv_path`` fixtures correspondingly.

More about it in the :ref:`onetick py test features`.

Also a developer can create a fully custom session using :class:`otp.Session <onetick.py.Session>`
and use it instead of the default fixtures, for example:

::

    @pytest.fixture
    def my_session():
        with otp.Session() as s:
            yield s

    def test_something(my_session):
        ...



This approach provides full flexibility.
