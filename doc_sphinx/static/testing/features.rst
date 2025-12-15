.. _onetick py test features:


onetick-py-test plugin features
===============================

``pytest`` introduces fixtures to simplify testing and share common resources between tests.

For more information about fixtures see
`api <https://docs.pytest.org/en/latest/reference.html#fixtures-api>`_
and `fixtures <https://docs.pytest.org/en/latest/fixture.html>`_
in the official pytest documentation.

Also see the `list <https://docs.pytest.org/en/latest/reference/fixtures.html#fixture>`_ of fixtures
provided by pytest package itself.

Below are listed the fixtures provided by ``onetick-py-test`` package.

Location fixtures
-----------------

Fixtures that help to get directories.

======================  ==============================================================
Name                    Description
======================  ==============================================================
``cur_dir``             gets the absolute path to the folder with the test.
----------------------  --------------------------------------------------------------
``par_dir``             gets the absolute path to the parent folder of the test.
----------------------  --------------------------------------------------------------
``keep_generated_dir``  returns the absolute path to the folder were the test will be
                        saved if the ``keep-generated`` flag is specified.
----------------------  --------------------------------------------------------------
``otq_path``            Allows to specify `OTQ_PATH` in :class:`onetick-py session config <onetick.py.session.Config>`
                        with the location of OTQ files.
                        By default `OTQ_PATH` is not specified for session fixtures.

                        You need to override this fixture to specify your own value.

                        ::

                          import pytest

                          @pytest.fixture
                          def otq_path(cur_dir):
                              return cur_dir / 'otqs'

                          def test_1(f_session):
                              # an instance of that `f_session` will search for otqs in
                              # specified path
                              ...

                          def test_2(f_session):
                              # the same with this instance of the `f_session`
                              ...

                        .. note::
                           The ``otq_path`` fixture will be automatically applied for all
                           tests with the same scope. Scope visibility could be adjusted
                           using modules and classes.
                           For example

                           ::

                              class TestCustomOTQS:
                                  @pytest.fixture
                                  def otq_path(self, cur_dir):
                                      # this fixture will be used by any session fixtrue
                                      # in this class
                                      ...


                              def test_1(f_session):
                                  # ... but makes not effect on this session
======================  ==============================================================


Session fixtures
----------------

Session fixtures provide an instance of :class:`otp.Session <onetick.py.Session>`
to a test and take care of gracefully destroying it after.


================  ==============================================================
Name              Description
================  ==============================================================
``f_session``     The ``function`` scope session. A session instance is created
                  before the test and destroyed after.

                  Example:

                  ::

                    def test_1(f_session):
                        # `f_session` points to a new instance of the otp.Session
                        ...

                    def test_2(f_session):
                        # here `f_session` is a new instance
                        ...

----------------  --------------------------------------------------------------
``c_session``     The ``class`` scope session.

                  Example:

                  ::

                    class TestSomething:

                        def test_1(self, c_session):
                            # `c_session` is created for this class on the first occurance
                            ...

                        def test_2(self, c_session):
                            # the `c_session` instance is the same as in test `test_1`


                    class TestSomethingElse:

                        def test_1(self, c_session):
                            # here the `c_session` is a new instance because it is a test
                            # of another test class
                            ...

                  The ``f_session`` fixture in classes creates a new instance for every
                  test.

----------------  --------------------------------------------------------------
``m_session``     The ``module`` scope session; it is created on the first
                  usage and destroyed only when all tests in the test file are executed.
================  ==============================================================


Default values fixtures
-----------------------

======================  =======================================  =============  ===================
Name                    Description                              Expected type  Default
======================  =======================================  =============  ===================
``default_tz``          Allows to override the default           str            EST5EDT
                        timezone.
----------------------  ---------------------------------------  -------------  -------------------
``default_start_time``  Start time for any query interval        str            2003/12/01 00:00:00
                        of the :func:`otp.run <onetick.py.run>`
----------------------  ---------------------------------------  -------------  -------------------
``default_end_time``    End time for any query interval          str            2003/12/04 00:00:00
                        of the :func:`otp.run <onetick.py.run>`
----------------------  ---------------------------------------  -------------  -------------------
``default_symbol``      Default symbol name that is used         str            AAPL
                        everywhere where OneTick requires it,
                        for example any tick source like
                        the
                        :class:`otp.Source <onetick.py.Source>`
----------------------  ---------------------------------------  -------------  -------------------
``default_database``    Default database that is used            str            DEMO_L1
                        everywhere where OneTick expects it
======================  =======================================  =============  ===================

These fixtures are automatically picked up by the provided session fixtures such as ``f_session``.
You just need to override a fixture with your value and it will be automatically picked up for all
fixtures with the same scope.

For example:

::


    @pytest.fixture
    def default_tz():
       return 'GMT'

    def test_something(f_session):
        # f_session picks up the `default_tz` value on initialization
        ...


.. note::

   Default values come from the default OneTick installation that distributes a sample of trades
   in the **DEMO_L1** database. Using this default values helps share issues
   with the OneTick support team.


The ``--keep-generated`` flag
-----------------------------

.. _keep generated flag:

The plugin adds a custom ``--keep-generated`` flag to `pytest` that allows to control
the lifetime of generated resources during tests: config files for
:class:`otp.Session <onetick.py.Session>`, databases, OTQ queries, etc.

It's helpful in case something goes wrong and a developer wants to take a closer look
into the resources generated during testing.

Description from the ``pytest -h``:

::

    custom options:
      --keep-generated=KEEP_GENERATED

        Policy to keep temporary generated files, that has several options to run:
        * 'never' - do not keep any temporary generated files during the test run (default)
        * 'fail' - keep temporary generated files only when a test fails
        * 'always' - keep temporary generated files for every test
        Example: pytest --keep-generated=fail


This flag handles the folders and files that are created using the :class:`otp.TmpFile <onetick.py.utils.temp.TmpFile>`
and :class:`otp.TmpDir <onetick.py.utils.temp.TmpDir>` correspondingly.

These classes are used in onetick-py internally to create databases,
log files and any configuration files related to the :class:`otp.Session <onetick.py.Session>`.

Developers could also use these classes in code and tests to handle them in case of testing
and debugging.


Example:

.. code-block:: bash

    $ pytest -vs --keep-generated=always


This command will print out the path to a folder with the saved resources:

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
    [[ Generated resources: /tmp/test_user/run_20231129_101141_23129/test_my/test_simple ]]


This ``[[ Generated resources: /tmp/test_user/run_20231129_101141_23129/test_my/test_simple ]]`` line
points us where we could find the resources.

Let's go there and list the folder:

.. code-block:: bash

    $ cd /tmp/test_user/run_20231129_101141_23129/test_my/test_simple
    $ ls

    boisterous-ant.locator  dancing-wombat.cfg  green-buffalo.run.otq
    run.sh  tunneling-hippo.acl

* ``dancing-wombat.cfg`` is a OneTick config file that the test's session creates and uses
* ``tunneling-hippo.acl`` is the ACL that the ``dancing-wombat.cfg`` config points to
* ``boisterous-ant.locator`` is a locator file that the ``dancing-wombat.cfg`` config points to;
  it consists of databases that have been added into the test's session
* ``green-buffalo.run.otq`` is a query that has been passed into ``otp.run`` during tests;
  every call of that function dumps a query that can then be viewed as an OTQ
* the ``run.sh`` script allows to spin up a tick server using the saved configs and to play with the saved OTQ queries
  (On Windows it is the ``run.bat`` script)

The ``--show-stack-trace`` flag
-------------------------------

.. _show stack trace flag:

Show stack trace with a line of ``onetick.py`` code where the issues has happened
in case of failure.

.. note :

   It slows down a test run and could be sensitive if you have a lot of tests.


This flag enables the same mechanism like
the :class:`otp.config['show_stack_info'] <onetick.py.configuration.Config>` flag does.

Other
`````

Our plugin adds OneTick version into the pytest output. You might find it in the
header of the output where ``pytest`` lists the plugins and their versions.

In the following example:

.. code-block:: bash

    $ pytest -vs

    =========================== test session starts =====================
    platform linux -- Python 3.9.6, pytest-7.1.2, pluggy-1.3.0 -- python3

    OneTick build: 20230831120000, onetick-py: 1.82.0, onetick-py-test: 1.1.34
    cachedir: .pytest_cache
    ...

The following line shows OneTick related dependencies:

.. code-block:: bash

    OneTick build: 20230831120000, onetick-py: 1.82.0, onetick-py-test: 1.1.34
