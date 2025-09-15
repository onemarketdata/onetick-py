.. _pip installation:

Other installation options
==========================

Prerequisites
:::::::::::::

- You installed `python 3.9 or newer <https://www.python.org/downloads/>`_.
- You installed `pip <https://pip.pypa.io/en/stable/installing/>`_.
- Optional, but it is **highly recommended** to use virtual environment for Python packages.

    Create and activate it with following commands:

    - Linux / MacOS: ``python3 -m venv venv && source venv/bin/activate``
    - Windows (cmd): ``python -m venv venv && venv\Scripts\activate``

Installation from OneTick pip server
::::::::::::::::::::::::::::::::::::

Ask your OneMarketData rep for a USERNAME and PASSWORD and run the command below

::

    pip install -U --index-url https://USERNAME:PASSWORD@pip.distribution.sol.onetick.com/simple/ "onetick-py[webapi]"

If you need to use proxy to reach external server, you need to specify it separately (replace https://user_name:password@proxyname:port in the command with your proxy credentials):

::

    pip install --index-url https://USERNAME:PASSWORD@pip.distribution.sol.onetick.com/simple/ --proxy https://user_name:password@proxyname:port "onetick-py[webapi]"

For MacOS, additionally run: ``pip install "pyarrow<16"``

If you need to install strictly defined versions of the packages `numpy` and `pandas`,
which is the most tested combination, then you can use the following command to install `onetick-py`:
::

    pip install -U --index-url https://USERNAME:PASSWORD@pip.distribution.sol.onetick.com/simple/ "onetick-py[strict,webapi]"

**Note**: if you have a local OneTick installation and ``PYTHONPATH`` pointing to it (e.g., to
``“C:\omd\one_market_data\one_tick\bin;C:\omd\one_market_data\one_tick\bin\python;C:\omd\one_market_data\one_tick\bin\numpy\python39;”``),
you need to **unset PYTHONPATH** in order to avoid conflicts:

- on Windows in PowerShell (``echo $env:PYTHONPATH`` - too see it; ``$env:PYTHONPATH=''`` - to unset it)
- on Linux/MacOS: ``unset PYTHONPATH``

Installation without internet connection
::::::::::::::::::::::::::::::::::::::::

If your machine is not connected to the internet, you can download the wheel files from our `pip` repository,
and distribute them to your machines:
::

    pip download -d /path/to/wheel/files "onetick-py[webapi]" --index-url https://USERNAME:PASSWORD@pip.distribution.sol.onetick.com/simple/

Then you can install `onetick-py` with the following command:
::

    pip install -U --no-index --find-links=/path/to/wheel/files "onetick-py[webapi]"
