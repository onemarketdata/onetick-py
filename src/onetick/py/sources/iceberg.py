import datetime
from typing import Optional

import onetick.py as otp
import onetick.py.types as ott
from onetick.py.otq import otq

from onetick.py.core.source import Source
from onetick.py.core._source.query_parameters import QueryParameters

from .. import utils

from .common import update_node_tick_type


def _as_of_time_to_epoch_msec(as_of_time):
    """
    Convert ``as_of_time`` parameter to the number of milliseconds since epoch expected by the EP.
    """
    if as_of_time is None:
        # the default value of the EP parameter
        return ""

    if isinstance(as_of_time, bool) or not isinstance(as_of_time, (int, ott.datetime, datetime.datetime)):
        raise ValueError(
            "Parameter `as_of_time` must be of type otp.datetime, datetime.datetime or int, "
            f"got {type(as_of_time)}"
        )

    if isinstance(as_of_time, int):
        # integers are treated as the number of milliseconds since epoch already
        return as_of_time

    # time2nsectime() returns nanoseconds, while READ_FROM_ICEBERG expects milliseconds
    return ott.time2nsectime(as_of_time, timezone=otp.config.tz) // 1_000_000


class ReadFromIceberg(Source):
    def __init__(
        self,
        catalog_config: Optional[str] = None,
        table_identifier: Optional[str] = None,
        fields: str | list[str] = "",
        drop_fields: bool = False,
        as_of_time: Optional[ott.datetime | datetime.datetime | int] = None,
        time_assignment: str = "end",
        where: str = "",
        symbology: str = "",
        symbol_name_field: str = "SYMBOL_NAME",
        batch_size: int = 1000,
        symbol=utils.adaptive,
        db=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        schema: Optional[dict] = None,
        query_parameters: Optional[QueryParameters] = None,
        **kwargs,
    ):
        """
        Read ticks from an Iceberg table.

        The table is specified by ``table_identifier`` and is accessed
        with the catalog settings from the ``catalog_config`` file.

        Parameters
        ----------
        catalog_config: str
            Path to a configuration file containing the Iceberg catalog parameters.
            For details on the file structure and how it is processed, see Examples below.
        table_identifier: str
            The Iceberg table identifier, including the namespace and table name
            (for example, ``namespace.tablename``).
        fields: list, str
            A list of fields (``list`` or comma-separated string) to be included in the output ticks.
            If not set, all available fields from the table are returned.
            See ``drop_fields`` parameter to propagate all fields except the listed ones.
        drop_fields: bool
            If set to ``True``, the fields listed in the ``fields`` parameter will not be propagated,
            and the fields that are not listed there will be propagated instead.
        as_of_time: :py:class:`otp.datetime <onetick.py.datetime>`, :py:class:`datetime.datetime`, int
            Timestamp for the Iceberg time travel query.
            The table snapshot that was current as of this timestamp will be read,
            following the standard Iceberg time travel semantics.
            Integers are treated as the number of milliseconds since epoch.
            Datetime values without timezone are treated as values
            in :py:attr:`otp.config.tz <onetick.py.configuration.Config.tz>`.
        time_assignment: str
            Timestamps of the ticks created by ``ReadFromIceberg`` are set to the start or to the end of the query
            depending on the ``time_assignment`` parameter.
            Possible values are ``start`` and ``end`` (for ``_START_TIME`` and ``_END_TIME``).
        where: str
            Specifies a criterion for selecting the rows to propagate.
            Wherever possible, Iceberg database predicate push-down is performed
            for certain sub-clauses of the specified expression.
        symbology: str
            Symbology of the symbol names found in the ``symbol_name_field`` field.
            If specified, the reference database is used
            to find synonyms for the query symbols within this symbology.
        symbol_name_field: str
            Field that is expected to contain the symbol name.
            When this parameter is set and specific symbols are bound to the query,
            only rows matching those symbols are propagated.
            If multiple symbols are present,
            rows are routed to the appropriate sub-queries based on this field's value.
        batch_size: int
            The maximum number of rows to be collected in the Java layer
            before they are transferred to the C++ layer.
        symbol: str, list of str, :class:`Source`, :class:`query`, :py:func:`eval query <onetick.py.eval>`
            Symbol(s) from which data should be taken.
        tick_type: str
            Tick type.
            Default: ANY.
        start: :py:class:`otp.datetime <onetick.py.datetime>`
            Start time for tick generation. By default the start time of the query will be used.
        end: :py:class:`otp.datetime <onetick.py.datetime>`
            End time for tick generation. By default the end time of the query will be used.
        schema: dict
            Set the schema of the python :py:class:`~onetick.py.Source` object of this class.

            Schema can't be automatically derived from the Iceberg table, so it should be set
            manually for Python-level type checking to work.
        query_parameters: :py:class:`otp.QueryParameters <onetick.py.QueryParameters>`
            Additional query properties to be set in the resulting .otq file.
            They will be used if they are not overridden by other parameters or in :py:func:`otp.run <onetick.py.run>`.
        kwargs:
            Deprecated. Use ``schema`` instead.
            Dictionary of columns names with their types.

        See also
        --------
        | **READ_FROM_ICEBERG** OneTick event processor
        | :py:meth:`onetick.py.Source.write_iceberg`

        Note
        ----

        This EP is supported only on OneTick for 64-bit Windows/Linux platforms.

        This source requires Java 17 or higher to be installed.
        Also Java library path should be added to **PATH** (Windows) or **LD_LIBRARY_PATH** (Linux)
        environment variables and to **OMD_JAVALIBPATH** OneTick config variable.

        Examples
        --------

        Write Iceberg catalog configuration first:

        >>> with open('catalog.cfg', 'w') as f:                           # doctest: +SKIP
        ...     f.write('''
        ...         catalog.name=rest_catalog
        ...         type=rest
        ...         uri=http://localhost:8181
        ...         warehouse=s3://warehouse/
        ...         s3.region=us-east-1
        ...         client.region=us-east-1
        ...         s3.path-style-access=true
        ...         s3.access-key-id=admin
        ...         s3.secret-access-key=password
        ...         io-impl=org.apache.iceberg.aws.s3.S3FileIO
        ...         s3.delete.enabled=true
        ...         s3.acceleration-enabled=false
        ...     ''')  # doctest: +SKIP

        Then read all fields from the table:

        >>> data = otp.ReadFromIceberg(catalog_config='catalog.cfg',
        ...                            table_identifier='exchange.trades')  # doctest: +SKIP
        >>> otp.run(data)  # doctest: +SKIP

        Read only the fields you need:

        >>> data = otp.ReadFromIceberg(catalog_config='catalog.cfg',
        ...                            table_identifier='exchange.trades',
        ...                            fields=['PRICE', 'SIZE'])  # doctest: +SKIP
        >>> otp.run(data)  # doctest: +SKIP

        Propagate all fields except the listed ones:

        >>> data = otp.ReadFromIceberg(catalog_config='catalog.cfg',
        ...                            table_identifier='exchange.trades',
        ...                            fields=['SIZE'],
        ...                            drop_fields=True)  # doctest: +SKIP
        >>> otp.run(data)  # doctest: +SKIP

        Filter rows and set tick timestamps from the ``trade_time`` field:

        >>> data = otp.ReadFromIceberg(catalog_config='catalog.cfg',
        ...                            table_identifier='exchange.trades',
        ...                            fields=['PRICE', 'SIZE', 'trade_time'],
        ...                            time_assignment='trade_time',
        ...                            where='PRICE > 20')  # doctest: +SKIP
        >>> otp.run(data)  # doctest: +SKIP

        Read the table snapshot that was current at the specified point in time:

        >>> data = otp.ReadFromIceberg(catalog_config='catalog.cfg',
        ...                            table_identifier='exchange.trades',
        ...                            fields=['PRICE', 'SIZE'],
        ...                            as_of_time=otp.datetime(2023, 1, 1, 12))  # doctest: +SKIP
        >>> otp.run(data)  # doctest: +SKIP
        """
        if self._try_default_constructor(schema=schema, **kwargs):
            return

        if not otp.compatibility._is_read_from_iceberg_supported():
            raise RuntimeError("Current version of OneTick doesn't support READ_FROM_ICEBERG EP")

        if drop_fields and not otp.compatibility._is_read_from_iceberg_drop_fields_supported():
            raise RuntimeError(
                "Parameter `drop_fields` of READ_FROM_ICEBERG EP is not supported by this version of OneTick"
            )

        if not catalog_config:
            raise ValueError("Missing required parameter `catalog_config`")

        if not table_identifier:
            raise ValueError("Missing required parameter `table_identifier`")

        if time_assignment not in {"start", "end"}:
            raise ValueError("Incorrect value for parameter `time_assignment`: `start` or `end` expected")
        time_assignment = f"_{time_assignment}_time".upper()

        if isinstance(fields, (list, tuple)):
            fields = ",".join(fields)

        if not isinstance(where, str):
            raise ValueError(f"Parameter `where` must be of type str, got {type(where)}")

        as_of_time = _as_of_time_to_epoch_msec(as_of_time)

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=lambda: self.base_ep(
                db=db,
                tick_type=tick_type,
                catalog_config=catalog_config,
                table_identifier=table_identifier,
                fields=fields,
                drop_fields=drop_fields,
                as_of_time=as_of_time,
                time_assignment=time_assignment,
                where=where,
                symbology=symbology,
                symbol_name_field=symbol_name_field,
                batch_size=batch_size,
            ),
            schema=schema,
            query_parameters=query_parameters,
            **kwargs,
        )

    def base_ep(
        self,
        catalog_config,
        table_identifier,
        fields="",
        drop_fields=False,
        as_of_time="",
        time_assignment="_END_TIME",
        where="",
        symbology="",
        symbol_name_field="SYMBOL_NAME",
        batch_size=1000,
        db=utils.adaptive_to_default,
        tick_type=utils.adaptive,
    ):
        node_kwargs = {}
        if drop_fields:
            node_kwargs["drop_fields"] = drop_fields

        src = Source(
            otq.ReadFromIceberg(
                catalog_config=catalog_config,
                table_identifier=table_identifier,
                fields=fields,
                as_of_time=as_of_time,
                time_assignment=time_assignment,
                where=where,
                symbology=symbology,
                symbol_name_field=symbol_name_field,
                batch_size=batch_size,
                **node_kwargs,
            )
        )

        if db or tick_type:
            update_node_tick_type(src, tick_type, db)

        return src
