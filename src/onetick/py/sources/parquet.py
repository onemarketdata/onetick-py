import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.core.source import Source

from .. import utils

from .common import update_node_tick_type


class ReadParquet(Source):
    def __init__(
        self,
        parquet_file_path=None,
        where=None,
        time_assignment="end",
        discard_fields=None,
        fields=None,
        symbol_name_field=None,
        symbol=utils.adaptive,
        db=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        schema=None,
        **kwargs,
    ):
        """
        Read ticks from Parquet file

        Parameters
        ----------
        parquet_file_path: str
            Specifies the path or URL of the Parquet file to read.
        where: str, None
            Specifies a criterion for selecting the ticks to propagate.
        time_assignment: str
            Timestamps of the ticks created by `ReadParquet` are set to the start/end of the query or to the given
            tick field depending on the `time_assignment` parameter.
            Possible values are `start` and `end` (for `_START_TIME` and `_END_TIME`) or a field name.
            Default: `end`
        discard_fields: list, str, None
            A list of fields (`list` or comma-separated string) to be discarded from the output ticks.
        fields: list, str, None
            A list of fields (`list` or comma-separated string) to be picked from the output ticks.
            The opposite to `discard_fields`.
        symbol_name_field: str, None
            Field that is expected to contain the symbol name.
            When this parameter is set and one or more symbols containing time series
            (i.e. `[dbname]::[time series name]` and not just `[dbname]::`) are bound to the query or to this EP,
            only rows belonging to those symbols will be propagated.
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
            Dictionary of columns names with their types.
            You should set schema manually, if you want to use fields in `onetick-py` query description
            before its execution.
        kwargs:
            Deprecated. Use ``schema`` instead.
            Dictionary of columns names with their types.

        See also
        --------
        | **READ_FROM_PARQUET** OneTick event processor
        | :py:meth:`onetick.py.Source.write_parquet`

        Examples
        --------
        Simple Parquet file read:

        >>> data = otp.ReadParquet("/path/to/parquet/file")
        >>> otp.run(data)  # doctest: +SKIP

        Read Parquet file and filter fields:

        >>> data = otp.ReadParquet("/path/to/parquet/file", fields=["some_field", "another_field"])
        >>> otp.run(data)  # doctest: +SKIP

        Read Parquet file and filter rows:

        >>> data = otp.ReadParquet("/path/to/parquet/file", where="PRICE > 20")
        >>> otp.run(data)  # doctest: +SKIP
        """
        if self._try_default_constructor(schema=schema, **kwargs):
            return

        if parquet_file_path is None:
            raise ValueError("Missing required parameter `parquet_file_path`")

        if discard_fields and fields:
            raise ValueError("`discard_fields` and `fields` cannot be specified at the same time")

        if where is None:
            where = ""

        if discard_fields is None:
            discard_fields = ""
        elif isinstance(discard_fields, list):
            discard_fields = ",".join(discard_fields)

        if fields is None:
            fields = ""
        elif isinstance(fields, list):
            fields = ",".join(fields)

        if time_assignment in {"start", "end"}:
            time_assignment = f"_{time_assignment}_time".upper()

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=lambda: self.base_ep(
                db=db,
                tick_type=tick_type,
                parquet_file_path=parquet_file_path,
                where=where,
                time_assignment=time_assignment,
                discard_fields=discard_fields,
                fields=fields,
                symbol_name_field=symbol_name_field,
            ),
            schema=schema,
            **kwargs,
        )

    def base_ep(
        self,
        parquet_file_path,
        where=None,
        time_assignment="end",
        discard_fields=None,
        fields=None,
        symbol_name_field=None,
        db=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
    ):
        if not hasattr(otq, "ReadFromParquet"):
            raise RuntimeError("Current version of OneTick don't support READ_FROM_PARQUET EP")

        node_kwargs = {}
        if symbol_name_field:
            node_kwargs["symbol_name_field"] = symbol_name_field

        src = Source(
            otq.ReadFromParquet(
                parquet_file_path=parquet_file_path,
                time_assignment=time_assignment,
                where=where,
                discard_fields=discard_fields,
                fields=fields,
                **node_kwargs,
            )
        )

        if db and tick_type:
            update_node_tick_type(src, tick_type, db)

        return src
