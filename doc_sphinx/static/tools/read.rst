.. _onetick read:

onetick read
============

``onetick read`` is a CLI tool distributed together with ``onetick-py``.

It can be used to read OneTick databases and print them in the terminal.

Installation
------------

``onetick read`` should be available right after installing ``onetick-py``:

.. code-block:: bash

    pip install onetick-py
    onetick read ...

Configuration
-------------

``onetick read`` follows the same configuration as ``onetick-py``.

There are several ways of configuration:

Several ways of configuration are possible:

1. Use ONE_TICK_CONFIG environment variable:

.. code-block:: bash

    export ONE_TICK_CONFIG=/path/to/one_tick_config.txt
    onetick read ...

2. Use onetick-py environment variables, e.g. using WebAPI mode and OneTick Cloud:

.. code-block:: bash

    export OTP_WEBAPI='1'
    export OTP_HTTP_ADDRESS='https://rest.cloud.onetick.com'
    export OTP_ACCESS_TOKEN_URL='https://cloud-auth.parent.onetick.com/realms/OMD/protocol/openid-connect/token'
    export OTP_CLIENT_ID='???????????'
    export OTP_CLIENT_SECRET='???????'
    onetick read ...

3. Using --remote-ts parameter to connect to remote OneTick server:

.. code-block:: bash

    onetick read ... --remote-ts path.to.remote.onetick.com:50015

4. Reading local OneTick database directory:

.. code-block:: bash

    onetick read ./path/to/local/DB ...


Examples
--------

Basics
^^^^^^

In ``onetick read`` required parameters are database name, tick type, symbol name and date.

Let's read data from database ``US_COMP_SAMPLE``, tick type ``QTE`` and symbol ``AAPL`` for 2024-02-01
(the local time zone is used by default):

.. warning::

    ``US_COMP_SAMPLE`` is a big database, so it may take some time to return results.
    Go to the next example to get results faster.

.. code-block:: bash

    onetick read US_COMP_SAMPLE QTE AAPL 2024-02-01

::

                                     Time EXCHANGE CORR        DELETED_TIME  TICK_STATUS COND NBBO_IND  ... ASK_PRICE ASK_SIZE    SEQ_NUM              PARTICIPANT_TIME      FINRA_ADF_TIME SECURITY_STATUS_IND OMDSEQ
    0       2024-02-01 03:59:00.048766251        K      1969-12-31 19:00:00            0    L        1  ...      0.00        0       1041 2024-02-01 03:59:00.047225000 1969-12-31 19:00:00                          0
    1       2024-02-01 03:59:00.076961302        Z      1969-12-31 19:00:00            0    L        1  ...      0.00        0       1879 2024-02-01 03:59:00.076273000 1969-12-31 19:00:00                          0
    2       2024-02-01 04:00:00.004396668        K      1969-12-31 19:00:00            0    R        4  ...    187.02        2       3828 2024-02-01 04:00:00.000225000 1969-12-31 19:00:00                          0
    3       2024-02-01 04:00:00.004557945        K      1969-12-31 19:00:00            0    R        0  ...    187.02        2       3836 2024-02-01 04:00:00.000225000 1969-12-31 19:00:00                          1
    4       2024-02-01 04:00:00.004697438        K      1969-12-31 19:00:00            0    R        0  ...    187.02        2       3851 2024-02-01 04:00:00.000225000 1969-12-31 19:00:00                          2
    ...                               ...      ...  ...                 ...          ...  ...      ...  ...       ...      ...        ...                           ...                 ...                 ...    ...
    8437010 2024-02-01 19:59:58.929486574        K      1969-12-31 19:00:00            0    R        0  ...    181.45        4  106224535 2024-02-01 19:59:58.929305000 1969-12-31 19:00:00                          0
    8437011 2024-02-01 19:59:58.939526360        Q      1969-12-31 19:00:00            0    R        2  ...    181.60        1  106224536 2024-02-01 19:59:58.939511759 1969-12-31 19:00:00                          0
    8437012 2024-02-01 19:59:59.169320534        P      1969-12-31 19:00:00            0    R        0  ...    181.45       25  106224546 2024-02-01 19:59:59.168978591 1969-12-31 19:00:00                          0
    8437013 2024-02-01 19:59:59.284979686        Q      1969-12-31 19:00:00            0    R        2  ...    181.60        1  106224563 2024-02-01 19:59:59.284965371 1969-12-31 19:00:00                          0
    8437014 2024-02-01 19:59:59.547784467        P      1969-12-31 19:00:00            0    R        2  ...    181.45       24  106224575 2024-02-01 19:59:59.547439434 1969-12-31 19:00:00                          0

    [8437015 rows x 26 columns]

Limit the data
^^^^^^^^^^^^^^

It is better to limit the amount of data to get results faster.

* use start time and end time parameters to set the smaller time range
* use ``--limit`` parameter to set the maximum number of rows to return
* use ``--columns`` or ``--columns-regex`` parameters to return only the specified columns
* use ``--tz`` parameter to set the time zone

.. code-block:: bash

    onetick read US_COMP_SAMPLE QTE AAPL '2024-02-01 09:30:00' '2024-02-01 09:30:01' --tz America/New_York --limit 100 --columns-regex PRICE

::

                                Time  BID_PRICE  ASK_PRICE
    0  2024-02-01 09:30:00.000860953     184.00     184.14
    1  2024-02-01 09:30:00.000969529     183.90     184.14
    2  2024-02-01 09:30:00.000998923     183.70     184.35
    3  2024-02-01 09:30:00.001655957       0.00       0.00
    4  2024-02-01 09:30:00.002127369     183.80     184.62
    ..                           ...        ...        ...
    95 2024-02-01 09:30:00.095290946     183.71     184.07
    96 2024-02-01 09:30:00.096162392     183.00     186.73
    97 2024-02-01 09:30:00.096761702     176.79     191.67
    98 2024-02-01 09:30:00.096787483     183.71     184.22
    99 2024-02-01 09:30:00.096955152     183.81     184.22

    [100 rows x 3 columns]


Output
^^^^^^

Use ``-o`` or ``--output`` parameter to return result in other formats.

Default format is ``compact``, it returns results in *pandas* text format,
but truncates the results to fit the screen, like in the previous examples.

``pandas`` format returns result in the same format, but without truncation:

.. code-block:: bash

    onetick read US_COMP_SAMPLE QTE AAPL 2024-02-01 --tz America/New_York --limit 10 --columns-regex 'PRICE|SIZE' -o pandas

::

                             Time  BID_PRICE  BID_SIZE  ASK_PRICE  ASK_SIZE
    2024-02-01 03:59:00.048766251       0.00         0       0.00         0
    2024-02-01 03:59:00.076961302       0.00         0       0.00         0
    2024-02-01 04:00:00.004396668     184.87         2     187.02         2
    2024-02-01 04:00:00.004557945     184.87         2     187.02         2
    2024-02-01 04:00:00.004697438     184.87         2     187.02         2
    2024-02-01 04:00:00.004780307     184.87         2     187.02         2
    2024-02-01 04:00:00.005816302     184.87         2     187.02         2
    2024-02-01 04:00:00.006738266     185.45         1     187.02         2
    2024-02-01 04:00:00.007607483     185.49         2     187.02         2
    2024-02-01 04:00:00.008367759     185.49         2     187.02         2
    2024-02-01 04:00:00.008377555     185.45         1     187.02         2

``csv`` format returns result in CSV format:

.. code-block:: bash

    onetick read US_COMP_SAMPLE QTE AAPL 2024-02-01 --tz America/New_York --limit 10 --columns-regex 'PRICE|SIZE' -o csv

::

    Time,BID_PRICE,BID_SIZE,ASK_PRICE,ASK_SIZE
    2024-02-01 03:59:00.048766251,0.0,0,0.0,0
    2024-02-01 03:59:00.076961302,0.0,0,0.0,0
    2024-02-01 04:00:00.004396668,184.87,2,187.02,2
    2024-02-01 04:00:00.004557945,184.87,2,187.02,2
    2024-02-01 04:00:00.004697438,184.87,2,187.02,2
    2024-02-01 04:00:00.004780307,184.87,2,187.02,2
    2024-02-01 04:00:00.005816302,184.87,2,187.02,2
    2024-02-01 04:00:00.006738266,185.45,1,187.02,2
    2024-02-01 04:00:00.007607483,185.49,2,187.02,2
    2024-02-01 04:00:00.008367759,185.49,2,187.02,2


``markdown`` format returns result as Markdown table:

.. code-block:: bash

    onetick read US_COMP_SAMPLE QTE AAPL 2024-02-01 --tz America/New_York --limit 10 --columns-regex 'PRICE|SIZE' -o markdown

::

    | Time                          |   BID_PRICE |   BID_SIZE |   ASK_PRICE |   ASK_SIZE |
    |-------------------------------|-------------|------------|-------------|------------|
    | 2024-02-01 03:59:00.048766251 |        0    |          0 |        0    |          0 |
    | 2024-02-01 03:59:00.076961302 |        0    |          0 |        0    |          0 |
    | 2024-02-01 04:00:00.004396668 |      184.87 |          2 |      187.02 |          2 |
    | 2024-02-01 04:00:00.004557945 |      184.87 |          2 |      187.02 |          2 |
    | 2024-02-01 04:00:00.004697438 |      184.87 |          2 |      187.02 |          2 |
    | 2024-02-01 04:00:00.004780307 |      184.87 |          2 |      187.02 |          2 |
    | 2024-02-01 04:00:00.005816302 |      184.87 |          2 |      187.02 |          2 |
    | 2024-02-01 04:00:00.006738266 |      185.45 |          1 |      187.02 |          2 |
    | 2024-02-01 04:00:00.007607483 |      185.49 |          2 |      187.02 |          2 |
    | 2024-02-01 04:00:00.008367759 |      185.49 |          2 |      187.02 |          2 |


``json`` format returns result as Json object:

.. code-block:: bash

    onetick read US_COMP_SAMPLE QTE AAPL 2024-02-01 --tz America/New_York --limit 5 --columns-regex 'PRICE|SIZE' -o json

::

    {"columns":["Time","BID_PRICE","BID_SIZE","ASK_PRICE","ASK_SIZE"],
     "data":[[1706759940048,0.0,0,0.0,0],[1706759940076,0.0,0,0.0,0],[1706760000004,184.87,2,187.02,2],[1706760000004,184.87,2,187.02,2],[1706760000004,184.87,2,187.02,2]]}
