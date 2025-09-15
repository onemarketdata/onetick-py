.. _onprem installation:

onetick-py with local OneTick binaries
======================================
For most customers pursuing tick analytics use cases, we recommend using the default installation method (WebAPI) described :ref:`here <default installation>`.

This installation method is specifically  for customers with an on-prem server installation of OneTick who:

1. Have not yet upgraded to a OneTick version that supports WebAPI, or
2. Rely on data processing pipelines that require running OneTick "locally" without specifying a CONTEXT for the tick server—for example, executing `tickdb_query.exe` without a CONTEXT.

If your setup matches these conditions, proceed with the instructions below to ensure optimal configuration.

Prerequisites
:::::::::::::

- You installed OneTick and have an active license or you are using hosted OneTick.
- You installed `python>=3.9 <https://www.python.org/downloads/>`_ (with dynamic libraries such as `libpython3.9.so.1.0`).
- You installed `pip <https://pip.pypa.io/en/stable/installing//>`_.

Installation with pip
:::::::::::::::::::::

The latest version of `onetick-py` is available on PyPI: `<https://pypi.org/project/onetick-py/>`_.

::

    pip install onetick-py

Environment variables
:::::::::::::::::::::

Please extend your environment variables to include the OneTick installation path.
``onetick-py`` depends on the ``onetick.query`` python package distributed as a part of OneTick build.

If you already use ``onetick.query`` or older version of ``onetick-py``
then you probably have **PYTHONPATH** variable updated with paths to OneTick python directories.

Starting from OneTick build **20230711-0** ``onetick-py`` is also distributed as a part of OneTick build,
but its version is probably older than the one you want to install from pip.

Because of the way **PYTHONPATH** works in python, having two directories of ``onetick-py``,
one specified in **PYTHONPATH** and one installed from pip, will result in the OneTick
distributed ``onetick-py`` overriding the one installed from pip.

So in this case, if you want to use the latest version of ``onetick-py`` installed from pip,
you will need to remove OneTick directories from **PYTHONPATH** and to create **MAIN_ONE_TICK_DIR** instead.
You will still be able to use ``onetick.query`` too with this method.

.. warning::
    The values below are just an example, you should update them according to your
    existing OneTick installation path

If you are using OneTick build more or equal to **20230711-0**
and you want to use the version of ``onetick-py`` installed with pip,
then please create **MAIN_ONE_TICK_DIR** variable:

- for Linux add the following to your ``.bashrc`` or other initialization file:

::

    export MAIN_ONE_TICK_DIR="/opt/one_market_data/one_tick"

- for Windows set your **MAIN_ONE_TICK_DIR** variable to this value:

    | ``C:\omd\one_market_data\one_tick``


In all other cases, if you are okay with using ``onetick-py`` version distributed with OneTick
or if you have older OneTick build version, please modify the **PYTHONPATH** variable like this:

- for Linux add the following to your ``.bashrc`` or other initialization file:

::

    export PYTHONPATH="/opt/one_market_data/one_tick/bin:$PYTHONPATH"
    export PYTHONPATH="/opt/one_market_data/one_tick/bin/python:$PYTHONPATH"
    export PYTHONPATH="/opt/one_market_data/one_tick/bin/numpy/python39:$PYTHONPATH"

- for Windows add the following values to your **PYTHONPATH** variable:

    | ``C:\omd\one_market_data\one_tick\bin``
    | ``C:\omd\one_market_data\one_tick\bin\python``
    | ``C:\omd\one_market_data\one_tick\bin\numpy\python39``

Requirements
:::::::::::::

The ``onetick.py`` package has the following requirements

.. include:: ../../../requirements.txt
    :literal:

These requirements are taken from the `requirements.txt` file and could be used as is for ``pip``.

::

    pip install -r requirements.txt

If you need to install strictly defined versions of the packages `numpy` and `pandas`,
which is the most tested combination, then you can use the following command to install `onetick-py`:
::

    pip install onetick-py[strict]

If your machine is not connected to the internet, you can download the wheel files from our `pip` repository,
and distribute them to your machines:
::

    pip download -d /path/to/wheel/files onetick-py

Then you can install `onetick-py` with the following command:
::

    pip install -U --no-index --find-links=/path/to/wheel/files onetick-py


.. only:: Internal

    Installation with local pip
    ::::::::::::::::::::::::::::::::::::::::::::

    1. You should add pip.sol.onetick.com to your pip config file:

    - for Linux
        put into `~/.pip/pip.conf`

    ::

        [global]
        extra-index-url = https://pip.sol.onetick.com

    - for Windows
        create pip.ini file

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
