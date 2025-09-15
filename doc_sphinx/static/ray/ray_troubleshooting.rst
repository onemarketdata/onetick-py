Troubleshooting
:::::::::::::::

Installation issues
-------------------

1. If you run Python script with  you facing the error `ImportError: We can't import onetick.query, because we can't find it and related libraries in the PYTHONPATH. Please, make sure that '<path-to-OneTick-dist>/bin','<path-to-OneTick-dist>/bin/python' and '<path-to-OneTick-dist>/bin/numpy/python39' directories from OneTick distribution are in PYTHONPATH.`
That means you haven't set environment variable `OTP_SKIP_OTQ_VALIDATION=1`. Run this Python code to be sure this variable is set inside the python process (IDE, terminal, Jupyter, etc.):
::

    import os
    print("OTP_SKIP_OTQ_VALIDATION =", os.environ["OTP_SKIP_OTQ_VALIDATION"])


Connection issues
-----------------

If you facing the problem, when after running Python script you have a exception:
::

    ConnectionError: ray client connection timeout

then do next steps to recognize the point of failure.

1. Re-check your IP in whitelist.
First, you need to know your IP address, most simple is to go google.com and type in search bar “what is my IP”.
4 digits separated by dots are your IP address. Ask your contact manager is this IP is whitelisted. If your IP is already whitelisted, go to next step.

2. Check your connection, is it allowed to have outcoming connection on 10001 port.
Type in your terminal command:
::

    telnet YOUR-RAY-HOST 10001

- If terminal freeze on ``Trying *.*.*.*...`` then something wrong with your network. Ask your network administrator about outcoming connection issues to port 10001 (firewall settings, network rules, ...)
- If your terminal freeze later on message `Escape character is '^]'.` going just after `Connected to YOUR-RAY-HOST.`, then your outcoming connection is good, proceed to next step.

3. Check your connection certificates files are downloaded and corresponding environment variables are set.
Add to your original Python code with remote function following lines:
::

    import os, sys
    print(os.environ.get("RAY_TLS_SERVER_CERT"))
    print(os.environ.get("RAY_TLS_SERVER_KEY"))
    print(os.environ.get("RAY_TLS_CA_CERT"))
    print(os.environ.get("RAY_USE_TLS"))
    sys.exit()

This will print your environment variables, seen as from Python interpreter, and you must see something like:
::

    /path/to/your/client-cert.pem
    /path/to/your/client-key.pem
    /path/to/your/ca-cert.pem
    1

If you don't see your paths (empty lines) or there is no ``1`` at the last line of output, then set your environment variables as described in :ref:`Ray client installation <static/ray/ray_installation:Ray client installation>` guide.
Check your paths to certificates by running following command in terminal:
::

    openssl verify -CAfile /path/to/your/ca-cert.pem /path/to/your/client-cert.pem

If output is ``client-cert.pem: OK``, then your paths and certificates are correct.
If you see other output, then act according to error message. Possible cases are:

- ``Error opening certificate file /path/to/your/client-cert.pem`` - find real path where your certificates are located and change environment variables.
- ``unable to load certificate`` - re-download your certificate files, as it seems like it is broken or corrupted.
- ``error ... lookup:certificate has expired`` - ask your contact manager to provide updated certificate files.

4. Test your gRPC-connection by Python script.
Put this code into new Python file (ex: ``test-grpc.py``), replace ``YOUR-RAY-HOST`` to your host domain, and run it:
code-block::

    import grpc, os
    host = 'YOUR-RAY-HOST:10001'
    _conn_state = None

    def _on_channel_state_change(conn_state: grpc.ChannelConnectivity):
        global _conn_state
        _conn_state = conn_state

    with open(os.environ["RAY_TLS_SERVER_CERT"], "rb") as f:
        server_cert_chain = f.read()
    with open(os.environ["RAY_TLS_SERVER_KEY"], "rb") as f:
        private_key = f.read()
    with open(os.environ["RAY_TLS_CA_CERT"], "rb") as f:
        ca_cert = f.read()

    credentials = grpc.ssl_channel_credentials(
        certificate_chain=server_cert_chain,
        private_key=private_key,
        root_certificates=ca_cert,
    )
    channel = grpc.secure_channel(host, credentials)
    channel.subscribe(_on_channel_state_change)

    print("Opening channel", channel)
    grpc.channel_ready_future(channel).result(timeout=10)
    print("Your connection state", _conn_state)

The positive output result is when your last line is: ``Your connection state ChannelConnectivity.READY``
All other results are most probably have a description of error:
- ``routines:OPENSSL_internal:CERTIFICATE_VERIFY_FAILED`` multiple times - your certificate is broken or outdated, ask your contact manager to provide you proper certificates.
- ``grpc.FutureTimeoutError`` - this is common error for connection issues.


5. In case you having ``grpc.FutureTimeoutError`` in step 4, please, run the same script with debugging attributes and dump log into file ``log.txt``:
code-block::

    GRPC_TRACE=all GRPC_VERBOSITY=debug python test-grpc.py > log.txt

Share resulted ``log.txt`` file with your contact manager in order to provide our technical team more information about issue.
