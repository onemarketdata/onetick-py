.. _webapi onprem installation:

WebAPI with on-prem OneTick server
==================================

Prerequisites
:::::::::::::

- You installed OneTick and have an active license. The OneTick server is already configured and running.
- OneTick server version no older than Release 1.24 (patch 20240708171408) or weekly build ``20240821``.
- Users have installed `python 3.9 or newer <https://www.python.org/downloads/>`_ and `pip <https://pip.pypa.io/en/stable/installing/>`_

OneTick server-side configuration
:::::::::::::::::::::::::::::::::

OneTick server is configured with `ONE_TICK_CONFIG` variable, which points to the configuration file (e.g., `onetick.cfg`).
The configuration file should contain the following lines:

::

    HTTP_SERVER_PORT=48028
    TICK_SERVER_OTQ_CACHE_DIR=/tmp
    TICK_SERVER_CSV_CACHE_DIR=/tmp
    TICK_SERVER_DATA_CACHE_DIR=/tmp

The ``HTTP_SERVER_PORT`` is the port number where the OneTick server will listen for incoming WebAPI requests.
This port should be accessible from the client side.

The ``TICK_SERVER_OTQ_CACHE_DIR``, ``TICK_SERVER_CSV_CACHE_DIR``, and ``TICK_SERVER_DATA_CACHE_DIR``
are the cache directories for the OneTick server.
Change ``/tmp`` to the desired cache directory, but keep in mind that the directory should be accessible by the OneTick server.

After the configuration is set, the OneTick server should be restarted.
Now the server is ready to accept WebAPI requests on the specified port.

onetick-py client-side installation
:::::::::::::::::::::::::::::::::::

Follow the steps described in the :ref:`Installation <default installation>` section to install the ``onetick-py`` package with the ``[webapi]`` extra.
This page also contains useful examples to get started with the ``onetick-py`` package.
