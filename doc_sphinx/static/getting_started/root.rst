.. _getting started:

Getting Started
===============

The guides in this section take you through the typical uses of ``onetick-py`` and are structured
in the order that facilitates learning ``onetick-py`` from scratch.

``onetick-py`` requires some configuration to be set up before running the queries.
Note that for readability reasons the code for configurating ``onetick-py`` was omitted in the examples.

Basic configuration
:::::::::::::::::::

This code can be used to set some basic configuration on Linux:

::

    export OTP_DEFAULT_DB="DEMO_L1"
    export OTP_DEFAULT_SYMBOL="AAPL"
    export OTP_DEFAULT_START_TIME="2003/12/01 00:00:00"
    export OTP_DEFAULT_END_TIME="2003/12/04 00:00:00"
    export OTP_DEFAULT_TZ="EST5EDT"

On Windows:

::

    set OTP_DEFAULT_DB=DEMO_L1
    set OTP_DEFAULT_SYMBOL=AAPL
    set OTP_DEFAULT_START_TIME=2003/12/01 00:00:00
    set OTP_DEFAULT_END_TIME=2003/12/04 00:00:00
    set OTP_DEFAULT_TZ=EST5EDT


In the python code on any system (`before` importing ``onetick-py``):

::

   import os
   os.environ['OTP_DEFAULT_DB'] = 'DEMO_L1'
   os.environ['OTP_DEFAULT_SYMBOL'] = 'AAPL'
   os.environ['OTP_DEFAULT_START_TIME'] = '2003/12/01 00:00:00'
   os.environ['OTP_DEFAULT_END_TIME'] = '2003/12/04 00:00:00'
   os.environ['OTP_DEFAULT_TZ'] = 'EST5EDT'


See details about ``onetick-py`` configuration in :ref:`static/configuration/root:Configuration`.


Authentication with OneTick Cloud
:::::::::::::::::::::::::::::::::

If you are using ``onetick-py`` to connect to OneTick cloud, you will need to authenticate.

There are different ways to authenticate with OneTick cloud.

The easiest way is to create or log-in to account on `<https://www.onetick.com/cloud-services/>`_
and get ``CLIENT_ID`` and ``CLIENT_SECRET`` there.

Run this code to set up authentication for ``onetick-py``:

::

    import os
    os.environ['OTP_WEBAPI'] = '1'
    os.environ['OTP_HTTP_ADDRESS'] = 'https://rest.cloud.onetick.com'
    os.environ['OTP_ACCESS_TOKEN_URL'] = 'https://cloud-auth.parent.onetick.com/realms/OMD/protocol/openid-connect/token'
    os.environ['OTP_CLIENT_ID'] = '__FILL_IN__'
    os.environ['OTP_CLIENT_SECRET'] = '__FILL_IN__'


The other way is getting direct access to OneTick cloud with username and password.
You can ask your OneMarketData rep for them.
Replace the placeholders ``__FILL_IN__`` with the provided credentials.

::

    import os
    os.environ['OTP_WEBAPI'] = '1'
    os.environ['OTP_HTTP_ADDRESS'] = 'https://data.onetick.com:443'
    os.environ['OTP_HTTP_USERNAME'] = '__FILL_IN__'
    os.environ['OTP_HTTP_PASSWORD'] = '__FILL_IN__'


Quick examples
::::::::::::::

Runnable code for US equities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

    import onetick.py as otp

    sym = 'AAPL'
    start = otp.dt(2024, 2, 1, 10)
    еnd = start + otp.Minute(1)

    # Get trades
    trades = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
    print(otp.run(trades, start=start, end=еnd, symbols=sym))

    # Join trades with quotes
    quotes = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO')

    joined = otp.join_by_time([trades, quotes])

    res = otp.run(joined,
                  symbols=[sym],
                  start=start,
                  end=еnd)

    print(res)

Runnable code for futures
^^^^^^^^^^^^^^^^^^^^^^^^^

::

    import onetick.py as otp

    sym = r'NG\N24'
    start = otp.dt(2024, 2, 1, 10)
    еnd = start + otp.Minute(100)

    # Get trades
    trades = otp.DataSource('CME_SAMPLE', tick_type='TRD')
    print(otp.run(trades, start=start, end=еnd, symbols=sym))

    # Join trades with quotes
    quotes = otp.DataSource(db='CME_SAMPLE', tick_type='QTE')

    joined = otp.join_by_time([trades, quotes])

    res = otp.run(joined,
                  symbols=[sym],
                  start=start,
                  end=еnd)

    print(res)
