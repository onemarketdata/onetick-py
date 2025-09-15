
.. _ray-server-guide:

Ray server installation
=======================

This guide describes how to install Ray server on machine with your OneTick installation and how to configure Ray client on other machines.
It is useful for deployed clients having their own infrastructure, but you can also use it for local development.

Prerequisites for OTP and Ray
:::::::::::::::::::::::::::::

- Installed and configured OneTick, as well as OneTick.Py and all its dependencies. See :ref:`Ray client installation <static/ray/ray_installation:Ray client installation>` for details.
- IP address of the machine where you want to install Ray server (<your_server_ip> below).
- You need to have open 10001 port on the machine where you want to install Ray server. If you are using a firewall, you need to open this port.
- If you want to access Ray dashboard, you need to have open 8265 port on the machine where you want to install Ray server. If you are using a firewall, you need to open this port.

Ray server configuration
::::::::::::::::::::::::

To install Ray, run the following command (inside your OneTick virtual environment, if you are using it):

.. code-block:: bash

    pip install "ray[default]==2.3.1" "redis>4.0" protobuf==3.20.1

Alternatively, you can install Ray 2.9 (preferred version now):

.. code-block:: bash

    pip install "ray[default]==2.9.0"

To run Ray server, execute the following command (substitute <your_server_ip> with the IP address of the machine where you want to install Ray server):

.. code-block:: bash

    ray start --head --dashboard-host=<your_server_ip> --node-ip-address=<your_server_ip>

This command starts Ray server on the machine. It will print the address of the server, which you will need to configure you Ray connection from client machines.
Argument `--dashboard-host` will expose Ray dashboard on port 8265. You can access it from your browser to monitor Ray server dashboard: ``http://<your_server_ip>:8265``.

6379, 8265, 10001 ports are used by Ray server. If you are using a firewall, you need to open these port. 
If you want to customize these ports, you can use ``--port``, ``--dashboard-port`` and ``--ray-client-server-port`` arguments respectively to specify desired ports.
or see `Ray documentation <https://docs.ray.io/en/latest/cluster/cli.html#ray-start>`_ for details.


Later, when you want to stop Ray server, execute the following command:

.. code-block:: bash

    ray stop

Ray client configuration
::::::::::::::::::::::::

First, you need to install ``onetick-query-stubs`` package on your client machines. See :ref:`Ray client installation <static/ray/ray_installation:Ray client installation>` for details (but skip ``Connection to Ray from outer network`` chapter).

Use following code to initialize connection to the Ray server:

::

    import ray
    ray.init("ray://<your_server_ip>:10001")

Now you can execute your functions with OneTick-Py code on the Ray server by adding ``@ray.remote()`` decorator on top of your functions.

.. code-block:: python

    @ray.remote
    def my_function():
        ... OTP code goes here ...

    ref = my_function.remote()
    result = ray.get(ref)

See :ref:`ray-example-function` for details on how to use OneTick-Py with Ray.
