from typing import Optional, List

import onetick.py as otp
from onetick.py.otq import otq

import onetick.py.db._inspection
from onetick.py.core.source import Source

from .. import utils, configuration
from ..core.column_operations.base import _Operation
from ..compatibility import is_odbc_query_supported

from .common import update_node_tick_type


class ODBC(Source):

    _PROPERTIES = Source._PROPERTIES + [
        '_dsn',
        '_connection_string',
        '_authentication_type',
        '_user',
        '_password',
        '_sql',
        '_symbology',
        '_allow_unordered_ticks',
        '_tz',
        '_start_expr',
        '_end_expr',
        '_apply_symbol_name_history',
        '_numeric_scale',
        '_preserve_unicode_fields',
        '_db',
        '_tick_type',
        '_symbols',
        '_presort',
    ]

    def __init__(
        self,
        dsn: Optional[str] = None,
        connection_string: Optional[str] = None,
        authentication_type: str = 'credentials',
        user: Optional[str] = None,
        password: Optional[str] = None,
        sql: Optional[str] = None,
        start_expr: Optional[_Operation] = None,
        end_expr: Optional[_Operation] = None,
        tz=None,
        allow_unordered_ticks=False,
        numeric_scale=8,
        preserve_unicode_fields: Optional[List[str]] = None,
        symbology='',
        apply_symbol_name_history=False,
        start=utils.adaptive,
        end=utils.adaptive,
        db=utils.adaptive,
        tick_type=utils.adaptive,
        symbols=utils.adaptive_to_default,
        presort=False,
        schema=None,
        **kwargs,
    ):
        """
        Read from ODBC-compatible database and propagate the resulting data as a time series of ticks.

        A time series is generated for every symbol of the query and the symbol name can be passed down to SQL query.

        If an attribute with the name *TIMESTAMP* is present in the database schema,
        it is assumed to be of either SQL DATE or SQL TIMESTAMP type, in which case output tick timestamps
        will carry values of that field and the field itself will not be propagated.
        If such a field is absent, output tick timestamps will be equal to query end time.

        ODBC source supported only on OneTick versions starting from release 1.24
        (or from development build 20231108-0)

        Note
        ----

        To be able to use this class, you need to have a ODBC driver manager available on your machine
        (comes with Windows OS; **unixodbc** package may need to be installed on UNIX OS).

        Also, the following entry needs to be added to the main configuration file::

            LOAD_ODBC_UDF=true

        Parameters
        ----------
        dsn: str
            Target database's source name registered in ODBC configuration.
        connection_string: str
            ODBC connection string. The format depends on the database you are trying to connect.
            Example for SQLite database::

                DRIVER={SQLite3};Database=/path/to/the/database.db

            This parameter is mutually exclusive
            with parameters ``dsn``, ``user``, ``password`` and ``authentication_type``.
        authentication_type: str ('system' or 'credentials')

            - **system**: in this case a trusted connection to the database will be used.
              If authentication is enabled in the server configuration file
              (see the OneTick Installation and Administration Guide for details),
              the client will be authenticated and the client's credentials will be used for connection;
              otherwise, server credentials will be used.
            - **credentials**: username and password specified with parameter ``connection_string``
              or with parameters ``user`` and ``password`` will be used to connect to the database.

        user: str, optional
            The connection user name.

            Ignored if ``authentication_type`` is **system**.
        password: str, optional
            The connection password.

            Ignored if ``authentication_type`` is **system**.
        sql: str
            A query in SQL language, which may optionally contain parameter placeholders.

            There are 3 types of placeholders: **<_SYMBOL_NAME>**, **<_START_TIME>**, and **<_END_TIME>**.

            **<_SYMBOL_NAME>** will be replaced by the pure symbol name part of the query.

            **<_START_TIME>** and **<_END_TIME>** will be replaced by values of parameters
            ``start_expr`` and ``end_expr`` or, if they are not set, with values taken from query start and end times.

            These timestamps will be inserted in the format ``YYYY-MM-DD HH:MM:SS.sss TIMEZONE``.

        start_expr: str or :py:class:`~onetick.py.Operation`
            A constant string expression used to replace the **<_START_TIME>** placeholder in an ``sql`` query.
            Tick-dependent columns can't be used in this expression, only meta fields such as
            *TIMESTAMP*, *_START_TIME*, and *_END_TIME* are allowed.
        end_expr: str or :py:class:`~onetick.py.Operation`
            A constant string expression used to replace the **<_END_TIME>** placeholder in an ``sql`` query.
            Tick-dependent columns can't be used in this expression, only meta fields such as
            *TIMESTAMP*, *_START_TIME*, and *_END_TIME* are allowed.
        tz: str
            The timezone used to interpret *TIMESTAMP* and other datetime columns in the database.
            By default :py:attr:`tz<onetick.py.configuration.Config.tz>` is used.

        allow_unordered_ticks: bool
            If set to False, this class will raise an exception when unordered ticks will be encountered.

            If set to True, processing ticks unordered by timestamp will be allowed
            (exception may still be raised by other EPs of the graph that require ticks to be ordered).

        numeric_scale: int
            Number of digits after the decimal point for SQL_NUMERIC and SQL_DECIMAL data types.
            Precision (maximum number of digits) is always set to 34.
        preserve_unicode_fields: list of str
            By default ODBC queries all data as ANSI,
            ODBC Driver Manager then tries to convert Unicode characters to ANSI.

            This parameter is a list of fields, which have Unicode types in the data source
            and will be propagated without conversion.

            UNICODE_CHAR_TYPE field property will be set to UCS-2 for these fields.

        symbology: str
            If specified, **<_SYMBOL_NAME>** placeholder in ``sql`` query will be replaced with
            the mapping of the symbol name of the query for provided symbology.

        apply_symbol_name_history: bool
            If set to False, ODBC will not take into account the symbol name history
            when substituting the SQL query with actual symbol names.

            If set to True, ODBC will resolve symbol name changes according to the reference data
            by substituting the SQL query accordingly,
            provided the symbol date is specified in :py:func:`otp.run <onetick.py.run>` when running the query and
            the SQL query contains all three placeholders **<_SYMBOL_NAME>**, **<_START_TIME>**, **<_END_TIME>**.

            Note that ODBC data source needs to support "UNION ALL" syntax for this functionality to work.

        start:
            Custom start time of the query.
            By default the start time used by :py:func:`otp.run <onetick.py.run>` will be inherited.
        end:
            Custom end time of the query.
            By default the start time used by :py:func:`otp.run <onetick.py.run>` will be inherited.
        db: str
            Custom database name for the node of the graph.
            By default the database used by :py:func:`otp.run <onetick.py.run>` will be inherited.
        tick_type: str
            Custom tick type for the node of the graph.
            By default "ANY" tick type will be set.
        symbols: str or list of str
            Custom symbol name for the node of the graph or list of symbols.
            If list of symbols is specified, ticks from different symbols will be merged into one source.

            Separate ODBC connection will be created for each processed symbol.
            To avoid this and have a single connection per thread, ``presort`` parameter can be specified.

            By default the symbol name used by :py:func:`otp.run <onetick.py.run>` will be inherited.
        presort: bool
            Adds **PRESORT** EP before merging bound symbols specified in ``symbols``.
            That make ODBC use single connection for all symbols.
        schema: dict
            Set the schema of the python :py:class:`~onetick.py.Source` object of this class.

            Schema can't be taken automatically from the database, so it should be set manually
            for python-level type checking to work.
        kwargs:
            Deprecated. Use ``schema`` instead.
            Set the schema of the python :py:class:`~onetick.py.Source` object of this class.

        See also
        --------
        **ODBC_QUERY** OneTick event processor

        Examples
        --------

        Connect with database's ``dsn``, manually set schema, get all data from TEST_TABLE:

        >>> data = otp.ODBC(dsn='testdb_dsn', sql='select * from TEST_TABLE',
        ...                 schema={'A': str, 'B': int, 'C': float, 'D': otp.nsectime})  # doctest: +SKIP
        >>> otp.run(data)  # doctest: +SKIP
                Time   A     B        C                       D
        0 2003-12-04  A1  1975  8.12345 2022-01-01 12:13:14.111
        1 2003-12-04  A2  1971  7.98765 2022-01-02 22:23:24.222

        Connect using ``connection_string`` parameter:

        >>> data = otp.ODBC(connection_string='DRIVER={SQLite3};Database=/path/to/the/database',
        ...                 sql='select * from TEST_TABLE')  # doctest: +SKIP
        >>> otp.run(data)  # doctest: +SKIP
                Time   A     B        C                       D
        0 2003-12-04  A1  1975  8.12345 2022-01-01 12:13:14.111
        1 2003-12-04  A2  1971  7.98765 2022-01-02 22:23:24.222

        Substitute start time placeholder with ``start_expr`` parameter:

        >>> data = otp.ODBC(
        ...     dsn='testdb_dsn',
        ...     sql='select * from TEST_TIMESTAMP where TIMESTAMP >= "<_START_TIME>"',
        ...     start_expr=(otp.meta_fields['START_TIME'] + otp.Day(1)).dt.strftime('%Y-%m-%d %H:%M:%S.%q')
        ... )  # doctest: +SKIP
        >>> otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 3))  # doctest: +SKIP
                             Time  A
        0 2022-01-02 22:23:24.222  2

        Use parameter ``allow_unordered_ticks`` if needed:

        >>> data = otp.ODBC(dsn='testdb_dsn',
        ...                 sql='select * from TEST_UNORDERED',
        ...                 allow_unordered_ticks=True)  # doctest: +SKIP
        >>> otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 3))  # doctest: +SKIP
                             Time  A
        0 2022-01-02 22:23:24.222  2
        1 2022-01-01 12:13:14.111  1
        """
        if self._try_default_constructor(schema=schema, **kwargs):
            return

        if not is_odbc_query_supported():
            raise RuntimeError("ODBC source is not supported in this version of OneTick, "
                               "it is available starting from release 1.24 (or development build 20231108)")

        if not dsn and not connection_string:
            raise ValueError("One of the parameters 'dsn' or 'connection_string' must be specified")

        if authentication_type not in {'system', 'credentials'}:
            raise ValueError(f"Unknown value for parameter 'authentication_type': {authentication_type}")

        if connection_string and any([dsn, user, password, authentication_type == 'system']):
            raise ValueError("Parameter 'connection_string' is used instead of parameters"
                             " 'dsn', 'user', 'password' and 'authentication_type'")

        if not sql:
            raise ValueError("Parameter 'sql' must be set")

        if not isinstance(numeric_scale, int) or not 0 <= numeric_scale <= 34:
            raise ValueError("Unsupported value for parameter 'numeric_scale'")

        self._dsn = dsn
        self._connection_string = connection_string
        self._authentication_type = authentication_type
        self._user = user
        self._password = password
        self._sql = sql
        self._symbology = symbology
        self._allow_unordered_ticks = allow_unordered_ticks
        self._apply_symbol_name_history = apply_symbol_name_history
        self._numeric_scale = numeric_scale
        self._preserve_unicode_fields = preserve_unicode_fields
        self._tz = tz
        self._start_expr = start_expr
        self._end_expr = end_expr
        self._db = db
        self._tick_type = tick_type
        self._symbols = symbols
        self._presort = presort

        # if we have a list of symbols, they will be merged in self.base_ep()
        if isinstance(symbols, list):
            symbols = None

        super().__init__(
            _symbols=symbols, _start=start, _end=end, _base_ep_func=lambda: self.base_ep(schema=schema, **kwargs),
            schema=schema, **kwargs,
        )

    def base_ep(self, schema=None, **kwargs):

        src = Source(
            otq.Omd_odbcQuery(
                dsn=self._dsn or '',
                connection_string=self._connection_string or '',
                authentication_type='System' if self._authentication_type == 'system' else 'Username/Password',
                user=self._user or '',
                password=self._password or '',
                sql=self._sql,
                symbology=self._symbology or '',
                allow_unordered_ticks=self._allow_unordered_ticks,
                apply_symbol_name_history=self._apply_symbol_name_history,
                numeric_scale=self._numeric_scale,
                preserve_unicode_fields=','.join(self._preserve_unicode_fields or []),
                tz=self._tz or configuration.config.get('tz') or '',
                start_time_expr=str(self._start_expr) if self._start_expr is not None else '',
                end_time_expr=str(self._end_expr) if self._end_expr is not None else '',
            ),
            schema=schema,
            **kwargs,
        )

        db = self._db

        if isinstance(self._symbols, list):
            if self._presort:
                src.sink(otq.Presort().symbols(self._symbols))
                src.sink(otq.Merge(identify_input_ts=False))
            else:
                src.sink(otq.Merge(identify_input_ts=False).symbols(self._symbols))

        update_node_tick_type(src, self._tick_type, db)

        return src
