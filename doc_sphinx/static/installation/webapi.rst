.. _default installation:

Installation
============

This section describes how to install the `onetick-py` package to use remotely with OneTick Cloud Server.
In this case, the package does not require OneTick binaries to be installed on the client side.

- If you have OneTick binaries installed on your machine and want to use `onetick-py` locally with them,
  please refer to the :ref:`On-Premises Installation <onprem installation>` section.
- If you want to configure your on-premise OneTick server with WebAPI support,
  see :ref:`WebAPI with on-prem OneTick server <webapi onprem installation>`.
- If you need to install `onetick-py` from OneTick's pip-servers or install `onetick-py` without internet connection,
  see :ref:`Other installation options <pip installation>`.

Prerequisites
:::::::::::::

- You installed `python 3.9 or newer <https://www.python.org/downloads/>`_.
- You installed `pip <https://pip.pypa.io/en/stable/installing/>`_.
- Optional, but it is **highly recommended** to use virtual environment for Python packages.

    Create and activate it with following commands:

    - Linux / MacOS: ``python3 -m venv venv && source venv/bin/activate``
    - Windows (cmd): ``python -m venv venv && venv\Scripts\activate``

Installation from PyPI
::::::::::::::::::::::

The latest version of `onetick-py` is available on PyPI: `<https://pypi.org/project/onetick-py/>`_.

::

    pip install onetick-py[webapi]


``webapi`` extra is needed to install additional dependencies required for making web-connection to the OneTick server.

Getting started
:::::::::::::::

Go to the :ref:`Getting Started <getting started>` page.
