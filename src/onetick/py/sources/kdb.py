import re
from typing import Dict, Optional, Tuple

import onetick.py.types as ott
from onetick.py.otq import otq

from onetick.py.core.source import Source

from .. import utils

from .common import update_node_tick_type


class ReadFromKdb(Source):
    def __init__(
        self,
        server_address: Optional[str] = None,
        query: Optional[str] = None,
        symbol_column: Optional[str] = None,
        timestamp_column: Optional[str] = None,
        fields: Optional[str | Dict[str, Optional[Tuple]]] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        symbol=utils.adaptive,
        db=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        **kwargs,
    ):
        """
        Retrieves historical or real-time data from KDB databases.

        The EP connects to the KDB server, reads data from the specified table or run q queries and
        propagates each row as a tick in a time series. When executing running queries with ``NOW`` as the start time,
        it assumes that the data is stored in the real-time database of KDB and EP runs CEP query by subscribing
        to the real-time part of data.

        Parameters
        ----------
        server_address: str
            Address of the KDB server. Should have ``server_host:port`` format.
        query: str
            If name of the table is provided then EP will generate **q** query to read data from this table
            for given start/end time of query and for symbols listed in the otq query.
            if **qSQL** query is provided then EP will run this query. EP will replace ``$_SYMBOL_NAME`` by symbol name
            and ``$_START_TIME`` and ``$_END_TIME`` by start and end time of query respectively.

            .. note::
                If **qSQL** query is provided, then parameters ``symbol_column``, ``timestamp_column`` and ``fields``
                will have no effect.

        symbol_column: str, optional
            Name of column or expression which return symbol name.

            Default value: ``sym``.
        timestamp_column: str, optional
            Name of column or expression which return timestamp. Can be expression like `date+time`
            where ``time`` is time passed after given date.

            Special timestamp expressions include:

              * ``_AUTO`` lets the EP choose the timestamp column automatically when the table has a well-known schema.

            Default value: `_AUTO`
        fields: list, str, optional
            A optional parameter in one the following form:

            * Dictionary with output column name a key and either ``None`` / empty tuple or column type or tuple with
              column type (one of :ref:`supported types <api/types/root:types>`) and column name from KDB database.
              If you want to omit one of them, pass ``None`` to corresponding tuple part.
            * String in format ``FIELD_1 [TYPE_1]=[COLUMN_NAME_1], FIELD_2 [TYPE_2]=[COLUMN_NAME_2], … ,
              FIELD_N [TYPE_N]=[COLUMN_NAME_N]`` where ``FIELD_K`` is name of output field
              which have ``TYPE_K`` is type and value equal to of value of ``COLUMN_NAME_K`` column.
              Type specifier and expression are optional. Expressions can depend from columns of KDB table.

            Column types, passed with this parameter will be used to construct schema for source object.

            .. note::
                Can't be set with ``query`` containing **qSQL query**

        username: str, optional
            Username for authentication on the KDB server. If specified, `password` parameter must be also specified.
        password: str, optional
            Password for authentication on the KDB server. If specified, `username` parameter must be also specified.
        symbol: str, list of str, :class:`Source`, :class:`query`, :py:func:`eval query <onetick.py.eval>`
            Symbol(s) from which data should be taken.
        tick_type: str
            Tick type.
            Default: ANY.
        start: :py:class:`otp.datetime <onetick.py.datetime>`
            Start time for tick generation. By default the start time of the query will be used.
        end: :py:class:`otp.datetime <onetick.py.datetime>`
            End time for tick generation. By default the end time of the query will be used.

        See also
        --------
        **READ_FROM_KDB** OneTick event processor

        Examples
        --------

        Simple query from ``some_table``

        >>> src = otp.ReadFromKdb('kdb-server:5000', 'some_table')  # doctest: +SKIP
        >>> otp.run(src, symbol='US_COMP_SAMPLE::AAPL')  # doctest: +SKIP
                                   Time                date            time      price  size   sym
        0 2003-12-01 04:42:42.156933546 2003-11-30 19:00:00  34962156933546  68.668341   300  AAPL
        1 2003-12-01 08:59:33.463062196 2003-11-30 19:00:00  50373463062196  64.309823    60  AAPL
        2 2003-12-01 10:10:40.343438833 2003-11-30 19:00:00  54640343438833  67.087377   530  AAPL
        ...

        Rename fields from previous query and keep only them

        >>> src = otp.ReadFromKdb(
        ...     'kdb-server:5000', 'some_table',
        ...     fields={
        ...         'PRICE': (float, 'price'),
        ...         'SIZE': (int, 'size')
        ...     },
        ... )  # doctest: +SKIP
        >>> otp.run(src, symbol='US_COMP_SAMPLE::AAPL')  # doctest: +SKIP
                                   Time      PRICE  SIZE
        0 2003-12-01 04:42:42.156933546  68.668341   300
        1 2003-12-01 08:59:33.463062196  64.309823    60
        2 2003-12-01 10:10:40.343438833  67.087377   530
        ...

        Pass query as ``query`` parameter instead of table name. In this case filtering by symbol wouldn't work.

        >>> src = otp.ReadFromKdb('kdb-server:5000', 'select from some_table')  # doctest: +SKIP
        >>> otp.run(src, symbol='US_COMP_SAMPLE::AAPL')  # doctest: +SKIP
                         Time                date            time      price  size   sym
        0 2199-12-31 19:00:00 2003-11-30 19:00:00  12311440987139  65.868242   170  AAPL
        1 2199-12-31 19:00:00 2003-11-30 19:00:00  33435228341817  95.989642   248  MSFT
        2 2199-12-31 19:00:00 2003-11-30 19:00:00  34962156933546  68.668341   342  AAPL
        3 2199-12-31 19:00:00 2003-11-30 19:00:00  50373463062196  64.309823    60  AAPL
        ...

        """
        if self._try_default_constructor(**kwargs):
            return

        schema = {}

        if not server_address:
            raise ValueError("Missing required parameter `server_address`")

        if not re.match(r"^.*:\d+$", server_address):
            raise ValueError(
                "Incorrect `server_address` value passed: expected string in `server_host:port` format, "
                f"got `{server_address}`"
            )

        if not query:
            raise ValueError("Missing required parameter `query`")

        if len([arg for arg in [username, password] if arg is not None]) not in (0, 2):
            raise ValueError(
                "Only one of parameters `username` or `password` was set. "
                "Setting one of them requires another to be set too."
            )

        if isinstance(fields, dict):
            fields_str_list = []
            for field, value in fields.items():
                if not isinstance(value, tuple):
                    field_type = value
                    kdb_column_name = None
                else:
                    field_type = value[0]
                    if len(value) == 1:
                        kdb_column_name = None
                    else:
                        kdb_column_name = value[1]

                if not value or field_type is None and kdb_column_name is None:
                    fields_str_list.append(field)
                    continue

                if field_type is not None:
                    schema[field] = field_type

                if len(value) == 1 or kdb_column_name is None:
                    fields_str_list.append(f"{field} {ott.type2str(field_type)}")
                    continue

                if field_type is None:
                    fields_str_list.append(f"{field} = {kdb_column_name}")
                    continue

                fields_str_list.append(f"{field} {ott.type2str(field_type)} = {kdb_column_name}")

            fields = ",".join(fields_str_list)

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=lambda: self.base_ep(
                db=db,
                tick_type=tick_type,
                server_address=server_address,
                query=query,
                symbol_column=symbol_column,
                timestamp_column=timestamp_column,
                fields=fields,
                username=username,
                password=password,
            ),
            schema=schema if schema else None,
        )

    def base_ep(
        self,
        server_address,
        query,
        symbol_column=None,
        timestamp_column=None,
        fields=None,
        username=None,
        password=None,
        db=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
    ):
        if not hasattr(otq, "ReadFromKdb"):
            raise RuntimeError("Current version of OneTick don't support READ_FROM_KDB EP")

        if fields is None:
            fields = ""

        node_kwargs = {}
        if symbol_column:
            node_kwargs["symbol_column"] = symbol_column
        if timestamp_column:
            node_kwargs["timestamp_column"] = timestamp_column
        if username:
            node_kwargs["username"] = username
        if password:
            node_kwargs["password"] = password

        src = Source(
            otq.ReadFromKdb(
                server_address=server_address,
                query=query,
                fields=fields,
                **node_kwargs,
            )
        )

        if db and tick_type:
            update_node_tick_type(src, tick_type, db)

        return src
