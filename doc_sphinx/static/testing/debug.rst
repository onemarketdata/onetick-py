.. _debugging:

Debugging
=========

Here we would like to describe possible techniques that could help you to find and understand issues in your code.

Show stack trace
----------------

We provide ability to show line of code where the issue happens in case if code is written in `onetick.py`.

In code your set or :class:`otp.config['show_stack_info'] <onetick.py.configuration.Config>` or the ``OTP_SHOW_STACK_INFO`` env variable to enable this option (more about it is :ref:`here <static/configuration/root:Configuration>`)

In tests your could use the ``--show-stack-trace`` :ref:`option <show stack trace flag>`.

Dumping ticks
-------------

The :meth:`otp.Source.dump() <onetick.py.Source.dump>` method allows to print out ticks
in different parts of the complex onetick-py queries.

That might help to find ticks that leads to incorrect behaviour and reproduce it then in tests.

Simple example:

::

    t = otp.Ticks(A=[1, 2, 3])
    t.dump()
    t = t.first()
    df = otp.run(t)

Standard output (the result of :meth:`dump() <onetick.py.Source.dump>` method):

::

    #TIMESTAMP,A
    01-12-2003 00:00:00.000000000,1
    01-12-2003 00:00:00.001000000,2
    01-12-2003 00:00:00.002000000,3


The result of the query:

::

    print(df)
            Time  A
    0 2003-12-01  1


onetick view
------------

``onetick view`` is a terminal UI for browsing OneTick databases and their content.

This tool is built on top of the ``onetick.py`` and distributed
as a separate python package.


.. only:: Internal


    ``onetick view`` is located in the
    `onetick-ext-view <https://gitlab.sol.onetick.com/solutions/py-onetick/onetick-ext-view>`_ project
    on our Gitlab server.
    You can find all usage instructions in the
    `README.md <https://gitlab.sol.onetick.com/solutions/py-onetick/onetick-ext-view/-/blob/master/README.md>`_ file.


The ``onetick view`` python package currently is only available on the internal OneTick pip-servers.

First option (available with OneTick VPN):

::

    pip install -U --index-url https://pip.sol.onetick.com onetick-ext-view


Second option (public server, ask your OneMarketData rep for a USERNAME and PASSWORD):

::

    pip install -U --index-url https://USERNAME:PASSWORD@pip.distribution.sol.onetick.com/simple/ onetick-ext-view


After installation a developer just needs to go to a folder with OneTick databases
and execute the ``onetick view`` command in terminal -- it automatically discovers
databases and shows them.

You can also connect to remote Tick Server by specifying it's address via `--remote-ts` argument.

Usually it's convenient to use this tool along with the ``--keep-generated`` :ref:`flag in tests <keep generated flag>`,
because this flag provides all databases are used in tests with their content.

.. image:: images/onetick-view.png

Save .otq file
--------------

.otq file is a OneTick text file that represents OneTick query graph and its parameters.

:class:`otp.Source <onetick.py.Source>` object has :meth:`.to_otq() <onetick.py.Source.to_otq>` method
that allows to save a file on the disk with goal to check the OTQ graph.


.. note::

    This method has symbols and start/end query interval parameters
    that can be passed there the same as for :func:`otp.run <onetick.py.run>` method.
    But in this case these parameters will be written to the .otq file itself.

It's often useful when you think the ``onetick.py`` constructs the wrong OneTick graph
or if you want to check some implementation details.


Simple example that creates an .otq file and returns a path to it (with the name of the main query inside the file):

::

    t = otp.Tick(A=1)
    t.to_otq()
    # '/tmp/test_user/run_20251202_181018_11054/impetuous-bullfrog.to_otq.otq::query'

In this file you will see the OneTick graph query representation and its parameters:

::

    [query]
    ...
    NODE_2 = TICK_GENERATOR(BUCKET_INTERVAL=0,BUCKET_TIME=BUCKET_START,FIELDS="long A=1")
    ...

    [_meta]
    ...
    end_expression = PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J", "2003-12-04 00:00:00.000000000", _TIMEZONE)
    start_expression = PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J", "2003-12-01 00:00:00.000000000", _TIMEZONE)
    TZ = EST5EDT
    USER_NAME = user
    ...

Setting name of the query
-------------------------

Method :meth:`otp.Source.set_name <onetick.py.Source.set_name>` can be used to set the name of the source.
This name will be used as a part of the resulting .otq file name and as the name of the query inside this file.

Graph rendering
---------------

Alternatively we could propose to use the :meth:`otp.Source.render_otq() <onetick.py.Source.render_otq>` method
or :func:`render_otq <onetick.py.utils.render_otq>` function on an .otq file,
which draws a OneTick query graph in different formats using :graphviz:`graphviz <>` library.

It could help with quick investigations, because the OneTick .otq file format is not readable for big queries.

Simple example that draws a graph in the default *.svg* format and returns a path to it:

::

    t = otp.Tick(A=1)
    t.render_otq()
    # '/tmp/test_user/run_20251202_181018_11054/flat-lemur.svg'


In this file you will see the OneTick graph query visual representation:

.. image:: images/flat-lemur.svg

.. note::

    You can also try to use the older :meth:`.render() <onetick.py.Source.render>` method
    of the :class:`otp.Source <onetick.py.Source>`, however it produces less useful output by different methods.

Logging symbols
---------------

Configuration parameter :py:attr:`otp.config.log_symbol<onetick.py.configuration.Config.log_symbol>`
and parameter ``log_symbol`` of function :py:func:`otp.run <onetick.py.run>` can be used to enable
printing of each processed symbol to the standard output.

Note that this method has some limitations:

- In this case function :py:func:`otp.run <onetick.py.run>` is called in callback mode,
  thus it doesn't return pandas.DataFrame or other object.
- This only works with unbound symbols, so it will not work
  if :py:func:`otp.merge <onetick.py.merge>` is used to merge symbols into one flow.
