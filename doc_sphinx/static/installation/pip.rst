.. _pip installation:

Other installation options
==========================

Prerequisites
:::::::::::::

- You installed `python 3.10 or newer <https://www.python.org/downloads/>`_.
- You installed `pip <https://pip.pypa.io/en/stable/installing/>`_.
- Optional, but it is **highly recommended** to use virtual environment for Python packages.

    Create and activate it with following commands:

    - Linux / MacOS: ``python3 -m venv venv && source venv/bin/activate``
    - Windows (cmd): ``python -m venv venv && venv\Scripts\activate``

Installation from OneTick pip server
::::::::::::::::::::::::::::::::::::

All recent ``onetick-py`` versions are now available on PyPI: `<https://pypi.org/project/onetick-py/>`_.

But in some cases, if you need some older or development versions, they can be installed from OneTick pip server.

Ask your OneMarketData rep for a USERNAME and PASSWORD and run the command below:

::

    pip install --index-url https://USERNAME:PASSWORD@pip.distribution.sol.onetick.com/simple/ "onetick-py[webapi]"

If you need to use proxy to reach external server, you need to specify it separately
(replace https://user_name:password@proxyname:port in the command with your proxy credentials):

::

    pip install --index-url https://USERNAME:PASSWORD@pip.distribution.sol.onetick.com/simple/ --proxy https://user_name:password@proxyname:port "onetick-py[webapi]"

For MacOS, additionally run: ``pip install "pyarrow<16"``

Installation with strict dependencies
:::::::::::::::::::::::::::::::::::::

The ``onetick.py`` depends on `pandas`, `numpy` and some other publicly available packages.

Full list of requirements can be displayed like this:

::

    pip show onetick-py

``onetick-py`` doesn't depend on fixed versions of `numpy` and `pandas`, different versions are supported.
But there is a set of versions that are used for testing, which can be installed with ``strict`` extra:
::

    pip install "onetick-py[strict]"


Installation without internet connection
::::::::::::::::::::::::::::::::::::::::

If your machine will not be connected to the internet, you can download the wheel files from `pip` repository,
and then distribute them to your machines:
::

    pip download -d /path/to/wheel/files "onetick-py[webapi]"

Then you can install `onetick-py` with the following command:
::

    pip install --no-index --find-links=/path/to/wheel/files "onetick-py[webapi]"


.. only:: Internal

    Installation with local pip
    :::::::::::::::::::::::::::

    1. You should add `pip.sol.onetick.com` to your pip config file:

    - for Linux
        put into `~/.pip/pip.conf`

    ::

        [global]
        extra-index-url = https://pip.sol.onetick.com

    - for Windows
        create `pip.ini` file

    ::

        cd %APPDATA%
        mkdir pip
        type nul > pip\pip.ini # it creates new file
        notepad pip\pip.ini

    * Content for Windows is the same as for Linux. For more information `look here <https://pip.pypa.io/en/stable/user_guide/#configuration>`_.

    1. install onetick-py

    ::

        python[3] -m pip install onetick-py [--user]

    - You should use ``python3`` command if the default python on your system is 2.7
    - You should use ``--user`` option to install onetick-py for your user only.

    3. Check everything is ok.

    - Run python interpreter:

    ::

        python[3]
        >>> import onetick.py as otp
