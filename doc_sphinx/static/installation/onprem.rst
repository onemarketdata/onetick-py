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
- You installed `python>=3.10 <https://www.python.org/downloads/>`_ (with dynamic libraries such as `libpython3.10.so.1.0`).
- You installed `pip <https://pip.pypa.io/en/stable/installing//>`_.

Installation from PyPI
::::::::::::::::::::::

The latest version of `onetick-py` is available on PyPI: `<https://pypi.org/project/onetick-py/>`_.

::

    pip install onetick-py


Other installation options are also available: :ref:`Other installation options <pip installation>`.

Environment variables
:::::::::::::::::::::

In "local installation" mode ``onetick-py`` depends on the ``onetick.query`` python package distributed as a part of OneTick build.

The path to this OneTick directory must be set in the environment variable, so ``onetick-py`` can find it.

These environment variables can be used:

* **MAIN_ONE_TICK_DIR** (recommended)
* *PYTHONPATH* (deprecated)

**MAIN_ONE_TICK_DIR** is available on all recent onetick-py versions (since 1.69.0 version)
and recommended to be used in almost all types of local setups.

.. warning::
    The values below are just an example, you should update them according to your
    existing OneTick installation path

**MAIN_ONE_TICK_DIR** variable should be set like this:

- for Linux add the following to your ``.bashrc`` or other initialization file:

::

    export MAIN_ONE_TICK_DIR="/opt/one_market_data/one_tick"

- for Windows set your **MAIN_ONE_TICK_DIR** variable to this value:

    | ``C:\omd\one_market_data\one_tick``


Using **PYTHONPATH** is *deprecated* and is not recommended to be used unless you use a very old ``onetick-py`` version.
If you have used ``onetick.query`` library separately
then it's likely you already have **PYTHONPATH** variable updated with paths to OneTick python directories.
In this case ``onetick-py`` will also work, but it's recommended to switch to using **MAIN_ONE_TICK_DIR**.

If both **MAIN_ONE_TICK_DIR** and **PYTHONPATH** are set and ``onetick-py`` is found in both directories,
then ``onetick-py`` library located in **PYTHONPATH** will be used, due to how python interpreter works.

.. note::
    In some OneTick builds (from *20230711-0* to *20241018-0*)
    ``onetick-py`` was also distributed as a part of OneTick distribution.

    Because of the way **PYTHONPATH** works in python, having two directories of ``onetick-py``,
    one specified in **PYTHONPATH** and one installed from pip, will result in the OneTick
    distributed ``onetick-py`` overriding the one installed from pip.

    So in this case, if you want to use the latest version of ``onetick-py`` installed from pip,
    you will need to remove OneTick directories from **PYTHONPATH** and to create **MAIN_ONE_TICK_DIR** instead.
    You will still be able to use ``onetick.query`` too with this method.


**PYTHONPATH** variable can be set like this:

- for Linux add the following to your ``.bashrc`` or other initialization file:

::

    export PYTHONPATH="/opt/one_market_data/one_tick/bin:$PYTHONPATH"
    export PYTHONPATH="/opt/one_market_data/one_tick/bin/python:$PYTHONPATH"

- for Windows add the following values to your **PYTHONPATH** variable:

    | ``C:\omd\one_market_data\one_tick\bin``
    | ``C:\omd\one_market_data\one_tick\bin\python``


Note that on older OneTick versions you may also need to add ``numpy`` directory too.
On newer versions this directory doesn't exist and is not needed.
The directory must be for the same python version as will be used by ``onetick.py`` (in the example for python 3.10).

    | ``/opt/one_market_data/one_tick/bin/numpy/python310``   (Linux)
    | ``C:\omd\one_market_data\one_tick\bin\numpy\python310`` (Windows)
