.. _session guide:

otp.Session: set up OneTick configuration files
***********************************************

``onetick-py`` provides tools for managing OneTick configuration files and databases
required to run OneTick locally (without WebAPI).

.. note::

    In WebAPI mode creating OneTick configuration files is not needed, because it's managed by WebAPI OneTick server.


Existing OneTick configuration
==============================

OneTick uses **ONE_TICK_CONFIG** environment variable to get the path to the configuration file.

If this variable is already set, then ``onetick-py`` can be used right away,
even without creating :class:`otp.Session <onetick.py.Session>` object:

.. doctest::

    >>> import os                                                                                      # doctest: +SKIP
    >>> from pathlib import Path                                                                       # doctest: +SKIP
    >>> os.environ['ONE_TICK_CONFIG'] = 'one_tick_config.txt'                                          # doctest: +SKIP
    >>> Path('one_tick_config.txt').write_text('DB_LOCATOR.DEFAULT=one_tick_locator.txt')              # doctest: +SKIP
    >>> Path('one_tick_locator.txt').write_text('<VERSION_INFO VERSION="2"/><DATABASES></DATABASES>')  # doctest: +SKIP

    >>> import onetick.py as otp                                # doctest: +SKIP
    >>> t = otp.Tick(A=1)                                       # doctest: +SKIP
    >>> otp.run(t, symbols='LOCAL::', date=otp.dt(2022, 1, 1))  # doctest: +SKIP
            Time  A
    0 2022-01-01  1

Creating session
================

Session can be created with :class:`otp.Session <onetick.py.Session>` class:

.. doctest::

    >>> import onetick.py as otp                                # doctest: +SKIP
    >>> session = otp.Session()                                 # doctest: +SKIP
    >>> # make required queries
    >>> t = otp.Tick(A=1)                                       # doctest: +SKIP
    >>> otp.run(t, symbols='LOCAL::', date=otp.dt(2022, 1, 1))  # doctest: +SKIP
            Time  A
    0 2022-01-01  1
    >>> session.close()

To avoid manually closing session, you can create it as a python context manager:

.. doctest::

    >>> import onetick.py as otp                                         # doctest: +SKIP
    >>> with otp.Session() as session:                                   # doctest: +SKIP
    ...     # make required queries
    ...     t = otp.Tick(A=1)                                            # doctest: +SKIP
    ...     df = otp.run(t, symbols='LOCAL::', date=otp.dt(2022, 1, 1))  # doctest: +SKIP
    ...     print(df)                                                    # doctest: +SKIP
            Time  A
    0 2022-01-01  1

Setting up custom OneTick configuration
=======================================

If you want override default temporary config, you can either pass path to config file or
:class:`otp.Config <onetick.py.session.Config>` object as :class:`otp.Session <onetick.py.Session>` ``config``
constructor parameter.

::

    config = otp.Config('/path/to/config')
    session = otp.Session(config)

Setting up custom database locator
==================================

If you want to create default configuration files, but override the locator file,
you can use :class:`otp.Locator <onetick.py.session.Locator>` object:

::

    config=otp.Config(
        locator=otp.Locator('/path/to/locator')
    )
    session = otp.Session(config)

The object :class:`otp.RemoteTS <onetick.py.servers.RemoteTS>` can also be used
to automatically create locator file pointing to the remote server:

::

    config=otp.Config(
        locator=otp.RemoteTS('path.to.the.server.com:50015')
    )
    session = otp.Session(config)


Setting up custom ACL
=====================

By default, a temporary generated :class:`otp.ACL <onetick.py.session.ACL>` object is created for every
:class:`otp.Config <onetick.py.session.Config>` and respectively for each session.

However you could pass path to ACL configuration file if you need to load custom ACL.

::

    acl = otp.ACL('/path/to/acl')
    config = otp.Config(acl=acl)
    session = otp.Session(config)

You can also add entities to the ACL by using :meth:`otp.ACL.add <onetick.py.session.ACL.add>` method or
remove entities using :meth:`otp.ACL.remove <onetick.py.session.ACL.remove>`.

::

    session.acl.add(otp.ACL.User('new_user'))
    session.acl.remove(otp.ACL.User('old_user'))


Creating temporary database
===========================

To create and add a temporary database to the locator, just create an :class:`otp.DB <onetick.py.DB>` object and
pass it to the :meth:`otp.Session.use <onetick.py.Session.use>` method.

.. doctest::

   >>> db = otp.DB('DB_NAME')
   >>> session.use(db)  # doctest: +SKIP

To add data to temporary database use :meth:`otp.DB.add <onetick.py.DB.add>` method:

.. doctest::

   >>> db.add(otp.Ticks(A=[1, 2, 3]), date=otp.dt(2003, 1, 1), symbol='SYM', tick_type='TT')

Alternatively, if you already have the data you want to add to the database, you could pass
:class:`otp.Source <onetick.py.Source>` object as :class:`otp.DB <onetick.py.DB>` constructor second parameter:

.. doctest::

   >>> data = otp.Ticks(A=[1, 2, 3])
   >>> db = otp.DB('DB_NAME', data)
   >>> session.use(db)  # doctest: +SKIP

Working with existing databases
===============================

Adding an existing database to the locator almost the same, as for temporary database.
However, you need to specify locations to load database from via ``db_locations`` parameter.

.. doctest::

   >>> db = otp.DB('NEW_DB', db_locations=[{'location': '/home/user/data/NEW_DB'}])
   >>> session.use(db)  # doctest: +SKIP

Additional locator configuration variables could be set via ``db_locations`` and ``db_properties`` parameters,
for ``location`` and ``db`` sections of database description in a locator configuration file correspondingly.

.. doctest::

   >>> db = otp.DB(
   ...     'TEST_DB',
   ...     db_properties={
   ...         'symbology': 'SYM',
   ...         'tick_timestamp_type': 'NANOS',
   ...     },
   ...     db_locations=[{
   ...         'access_method': otp.core.db_constants.access_method.FILE,
   ...         'location': '/path/to/test_db/',
   ...         'start_time': datetime(year=2003, month=1, day=1),
   ...         'end_time': datetime(year=2023, month=1, day=1),
   ...     }],
   ... )

See ``OneTick Locator Variables`` OneTick documentation for available locator configuration variables.

Remote databases
================

Remote servers can be added to OneTick database locator too
by passing :class:`otp.RemoteTS <onetick.py.servers.RemoteTS>` object
to the :meth:`otp.Session.use <onetick.py.Session.use>` method:


::

    session.use(otp.RemoteTS('path.to.the.server.com:50015'))


Or they can be added when creating :class:`otp.Session <onetick.py.Session>`:

.. doctest::

    >>> import onetick.py as otp                                      # doctest: +SKIP
    >>> with otp.Session(                                             # doctest: +SKIP
    ...     config=otp.Config(                                        # doctest: +SKIP
    ...         locator=otp.RemoteTS('path.to.the.server.com:50015')  # doctest: +SKIP
    ...     )                                                         # doctest: +SKIP
    ... ):                                                            # doctest: +SKIP
    ...     # get available databases
    ...     print(otp.databases(as_table=True)['DB_NAME'])            # doctest: +SKIP
    0                ABAXX
    1          ABAXX_DAILY
    2            ABU_DHABI
    3       ABU_DHABI_BARS
    4      ABU_DHABI_DAILY
                ...
    793        XETRA_DAILY
    794          ZHENGZHOU
    795     ZHENGZHOU_BARS
    796    ZHENGZHOU_DAILY
    797            __OQD__




Derived databases
=================

Derived databases could be added to the locator like a regular database.
Of course, a parent database must be added to create a derived database.

.. doctest::

   >>> db = otp.DB('DB_NAME')
   >>> session.use(db)  # doctest: +SKIP
   >>> derived_db = otp.DB('DB_NAME//DERIVED_LABEL')
   >>> session.use(derived_db)  # doctest: +SKIP

You can also add data to derived database.

.. doctest::

   >>> data = otp.Ticks(A=[1, 2, 3])
   >>> derived_db = otp.DB('DB_NAME//DERIVED_LABEL')
   >>> session.use(derived_db)  # doctest: +SKIP
   >>> derived_db.add(data)  # doctest: +SKIP

See ``Derived Databases`` OneTick documentation for more info about derived databases.


Useful types of sessions
========================

There are some other types of session classes,
that are inherited from base :class:`otp.Session <onetick.py.Session>` class,
but provide some additional functionality.

otp.TestSession
---------------

:class:`otp.TestSession <onetick.py.TestSession>` sets up some default onetick.py configuration values
and is useful for the purposes of quickly setting up environment to test some simple queries.

.. only:: Internal

    onetick.hosted.Session
    ----------------------

    ``onetick.hosted.Session`` automatically scans directory structure on the local machine
    finding all OneTick databases, and creating OneTick locator that allows to access them
    without the need of additional configuration.

    ``onetick.hosted`` is a separate module located in the
    `onetick-hosted <https://gitlab.sol.onetick.com/solutions/py-onetick/onetick-hosted>`_ project
    on our Gitlab server.
    You can find all usage instructions and some examples in the
    `README.md <https://gitlab.sol.onetick.com/solutions/py-onetick/onetick-hosted/-/blob/master/README.md>`_ file.

    ``onetick.hosted`` can be installed with ``pip``:

    ::

        pip install onetick-hosted


Creating session with different contexts
========================================

.. _switching contexts:

In OneTick context is a namespace for the databases.

Different contexts allow having sets of databases from different places, local or remote,
and easily switching context with parameter ``context`` supported by many onetick-py functions.

Default context is named **DEFAULT** and is created automatically by :class:`otp.Session <onetick.py.Session>`.
You can see it by reading the configuration file and seeing **DB_LOCATOR.DEFAULT** variable:

.. doctest::

   >>> session = otp.Session()   # doctest: +SKIP
   >>> with open(session.config.path) as r:   # doctest: +SKIP
   ...     print(r.read())   # doctest: +SKIP
   ONE_TICK_CONFIG.ALLOW_ENV_VARS=Yes
   ...
   ACCESS_CONTROL_FILE="/tmp/test_onetick/run_20250127_160920_16360/beige-malkoha.acl"
   DB_LOCATOR.DEFAULT="/tmp/test_onetick/run_20250127_160920_16360/lurking-frigatebird.locator"
   ...


Default context can be modified with parameter ``locator`` of :class:`otp.Config <onetick.py.session.Config>`.
Additional contexts can be created by adding other *DB_LOCATOR.* variables to OneTick configuration file.
Let's create context **OTHER**, and create databases in both contexts:

.. doctest::

   >>> default_locator = otp.Locator()  # doctest: +SKIP
   >>> default_locator.add(otp.DB('A', otp.Tick(A=1), tick_type='TT', symbol='S'))  # doctest: +SKIP
   >>> other_locator = otp.Locator(empty=True)  # doctest: +SKIP
   >>> other_locator.add(otp.DB('B', otp.Tick(B=2), tick_type='TT', symbol='S'))  # doctest: +SKIP
   >>> config = otp.Config(locator=default_locator,  # doctest: +SKIP
   ...                     variables={'DB_LOCATOR.OTHER': other_locator.path})
   >>> session = otp.Session(config)  # doctest: +SKIP
   >>> with open(session.config.path) as r:  # doctest: +SKIP
   ...     print(r.read())  # doctest: +SKIP
   ONE_TICK_CONFIG.ALLOW_ENV_VARS=Yes
   ...
   ACCESS_CONTROL_FILE="/tmp/test_onetick/run_20250127_160920_16360/ultra-inchworm.acl"
   DB_LOCATOR.DEFAULT="/tmp/test_onetick/run_20250127_160920_16360/infrared-crane.locator"
   DB_LOCATOR.OTHER="/tmp/test_onetick/run_20250127_160920_16360/tangerine-earthworm.locator"
   ...


After that both contexts can be used when running queries, thus making databases from different locators available:

.. doctest::

   >>> data = otp.DataSource('A', tick_type='TT', symbols='S', schema_policy='manual')  # doctest: +SKIP
   >>> # running query without parameter *context* will run the query in **DEFAULT** context
   >>> print(otp.run(data))  # doctest: +SKIP
           Time  A
   0 2003-12-01  1
   >>> data = otp.DataSource('B', tick_type='TT', symbols='S', schema_policy='manual')  # doctest: +SKIP
   >>> print(otp.run(data, context='OTHER'))  # doctest: +SKIP
           Time  B
   0 2003-12-01  2


Some other functions also have parameter ``context``, e.g. :func:`otp.databases <onetick.py.databases>`:


.. doctest::

   >>> otp.databases()  # doctest: +SKIP
   {'A': <onetick.py.db._inspection.DB at 0x7f520daa4160>,
    'COMMON': <onetick.py.db._inspection.DB at 0x7f520daa4280>,
    'DEMO_L1': <onetick.py.db._inspection.DB at 0x7f520daa4400>}
   >>> otp.databases(context='OTHER')  # doctest: +SKIP
   {'B': <onetick.py.db._inspection.DB at 0x7f52811c07f0>}
