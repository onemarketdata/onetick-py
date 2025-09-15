Logging
=======

The main way to set up logging in ``onetick-py`` is
:py:attr:`otp.config.logging<onetick.py.configuration.Config.logging>` parameter.

This parameter can be set to severity level string or path to the file with configuration.
Default value for this parameter is ``WARNING``.

In case logging level is specified, the logging system is set up to print log to the standard error stream (stderr).

In case the path to the configuration file is specified, the logging system is initialized with
configuration provided in this file.

There are two formats supported:

* JSON file (file with *.json* suffix) with the data in
  `this format <https://docs.python.org/3/library/logging.config.html#logging-config-dictschema>`_
  that will be parsed by `logging.config.dictConfig <https://docs.python.org/3/library/logging.config.html#logging.config.dictConfig>`_
* File with any other suffix with the data in
  python `configparser <https://docs.python.org/3/library/logging.config.html#logging-config-fileformat>`_ format
  that will be parsed by `logging.config.fileConfig <https://docs.python.org/3/library/logging.config.html#logging.config.fileConfig>`_
