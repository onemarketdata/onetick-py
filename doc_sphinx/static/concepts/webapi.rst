Remote access with WebAPI
=========================

WebAPI is an interface for ``onetick-py`` to use remote OneTick server.
It could be OneMarketData Cloud Server or on-premises OneTick server.
Main advantage of this approach is that it does not require OneTick binaries to be installed on the client side.

Installation instructions for WebAPI are available in the :ref:`Installation <default installation>` section.
Also, you can find example code snippets there.

Instructions to configure your on-premise OneTick server for WebAPI are available on the :ref:`corresponding page <webapi onprem installation>`.


Difference of WebAPI mode
=========================

When using WebAPI, it is not required to use ``otp.Session()`` object, as it makes no sense in this context.

All functions, that rely on using binaries on the client side, are not supported in WebAPI mode:

* ``RefDB.put()``
* ``otp.perf``

The following features are not supported when using WebAPI mode:

* ``otp.Session()`` object (not required)
* Ignored ``otp.run()`` parameters:
    * ``start_time_expression``
    * ``end_time_expression``
    * ``alternative_username``
    * ``batch_size``
    * ``treat_byte_arrays_as_strings``
    * ``output_mode``
    * ``output_matrix_per_field``
    * ``return_utc_times``
    * ``connection``
    * ``svg_path``
    * ``use_connection_pool``
    * ``time_as_nsec``
    * ``max_expected_ticks_per_symbol``
    * ``manual_dataframe_callback``

OQD usage requires to have corresponding extension installed on the server side.

Callbacks in ``otp.run()`` are partially supported, meaning it receives only the final result of the query,
not tick-by-tick data arriving from the server. Callback class in WebAPI mode could have ``process_ticks()`` callback
for processing the data after the query is finished.

ODBC is not supported.

WebAPI differs ``otp.string[N]`` field type on empty results: ``<U4`` turns to ``<U64``

``otp.RemoteTS`` is not applicable, as all configuration is done on server side.

``Source.dump()`` is not supported.

``otp.logf()`` is not supported.

``Session._log_file`` is not supported.

Some error messages could differs.
