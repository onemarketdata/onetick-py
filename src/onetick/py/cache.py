from typing import Any, Callable, List, Optional, Tuple, Union
from types import FunctionType
from datetime import datetime

import onetick.py as otp
from onetick.py.otq import otq
from onetick.py.core.source import Source
from onetick.py.sources.common import update_node_tick_type
from onetick.py.sources.cache import process_otq_params
from onetick.py.types import datetime as otp_datetime

from . import configuration


def _check_status(df):
    if "STATUS" not in df:
        raise RuntimeError("Empty response returned")

    status = df["STATUS"][0]
    if status != "SUCCESS":
        raise RuntimeError(f"Error status returned: {status}")


def _convert_dt_to_str(dt: Union[str, datetime, otp_datetime]):
    if isinstance(dt, str):
        return dt
    elif isinstance(dt, (datetime, otp_datetime)):
        return dt.strftime('%Y%m%d%H%M%S.%f')
    raise ValueError(f"Unsupported value for 'dt' parameter: {type(dt)}")


def _convert_time_intervals(
    time_intervals_to_cache: List[Tuple[Union[str, datetime, otp_datetime], Union[str, datetime, otp_datetime]]],
):
    return "\n".join(
        [
            f"{_convert_dt_to_str(start_time)},{_convert_dt_to_str(end_time)}"
            for start_time, end_time in time_intervals_to_cache
        ]
    )


def create_cache(
    cache_name: str,
    query: Union['Source', Callable, str, None] = None,
    inheritability: bool = True,
    otq_params: Union[dict, None] = None,
    time_granularity: int = 0,
    time_granularity_units: Optional[str] = None,
    timezone: str = "",
    time_intervals_to_cache: Optional[List[tuple]] = None,
    allow_delete_to_everyone: bool = False,
    allow_update_to_everyone: bool = False,
    allow_search_to_everyone: bool = True,
    cache_expiration_interval: int = 0,
    tick_type: str = "ANY",
    symbol: Optional[str] = None,
    db: Optional[str] = None,
):
    """
    Create cache via CREATE_CACHE EP

    If :py:class:`onetick.py.Source` or callable passed as ``query`` parameter,
    cache will be created only for current session.

    Also, this case or passing absolute path to .otq as ``query`` parameter are supported
    only for caching on local OneTick server.

    Parameters ``symbol``, ``db`` and ``tick_type`` could be omitted if you want to use
    default symbol, default database and ``ANY`` tick type.

    Parameters
    ----------
    cache_name: str
        Name of the cache to be created.
    query: :py:class:`onetick.py.Source`, callable, str
        Query to be cached or path to .otq file (in ``filename.otq::QueryName`` format).
        Only queries residing on the server that runs the caching event processors are currently supported.
        For local OneTick server you can pass absolute path to local .otq file.
    inheritability: bool
        Indicates whether results can be obtained by combining time intervals that were cached with intervals
        freshly computed to obtain results for larger intervals.
    otq_params: dict
        OTQ params of the query to be cached.
        Setting `otq_params` in :py:class:`onetick.py.ReadCache` may not override this `otq_params`
        if cache not invalidated.
    time_granularity: int
        Value N for seconds/days/months granularity means that start and end time of the query have to be on N
        second/day/month boundaries relative to start of the day/month/year.
        This doesn't affect the frequency of data within the cache, just the start and end dates.
    time_granularity_units: str, None
        Units used in ``time_granularity`` parameter. Possible values: 'none', 'days', 'months', 'seconds' or None.
    timezone: str
        Timezone of the query to be cached.
    time_intervals_to_cache: List[tuple]
        List of tuples with start and end times in ``[(<start_time_1>, <end_time_1>), ...]`` format,
        where ``<start_time>`` and ``<end_time>`` should be one of these:

        * string in ``YYYYMMDDhhmmss[.msec]`` format.
        * :py:class:`datetime.datetime`
        * :py:class:`onetick.py.types.datetime`

        If specified only these time intervals can be cached. Ignored if ``inheritability=True``.
        If you try to make a query outside defined interval, error will be raised.
    allow_delete_to_everyone: bool
        When set to ``True`` everyone is allowed to delete the cache.
    allow_update_to_everyone: bool
        When set to ``True`` everyone is allowed to update the cache.
    allow_search_to_everyone: bool
        When set to ``True`` everyone is allowed to read the cached data.
    cache_expiration_interval: int
        If set to a non-zero value determines the periodicity of cache clearing, in seconds.
        The cache will be cleared every X seconds, triggering new query executions when data is requested.
    tick_type: str
        Tick type.
    symbol: str, list of str, list of otq.Symbol, :py:class:`onetick.py.Source`, :pandas:`pandas.DataFrame`, optional
        ``symbols`` parameter of ``otp.run()``.
    db: str
        Database.

    Note
    ----
    This function does NOT populate the cache, it's only reserves the cache name in the OneTick memory.
    Cache is only populated when an attempt is made to read the data from it via :py:class:`onetick.py.ReadCache`.

    See also
    --------
    | **CREATE_CACHE** OneTick event processor
    | :py:class:`onetick.py.ReadCache`
    | :py:func:`onetick.py.delete_cache`

    Examples
    --------
    Simple cache creation from .otq file on OneTick server under ``OTQ_FILE_PATH``

    >>> otp.create_cache(  # doctest: +SKIP
    ...    cache_name="some_cache", query="CACHE_EXAMPLE.otq::slowquery",
    ...    tick_type="TRD", db="LOCAL",
    ... )

    Cache creation from function

    >>> def query_func():
    ...    return otp.DataSource("COMMON", tick_type="TRD", symbols="AAPL")
    >>> otp.create_cache(
    ...    cache_name="some_cache", query=query_func, tick_type="TRD", db="LOCAL",
    ... )

    Create cache for time intervals with different datetime types:

    >>> otp.create_cache(  # doctest: +SKIP
    ...    cache_name="some_cache",
    ...    query="query_example.otq::query"),
    ...    inheritability=False,
    ...    time_intervals_to_cache=[
    ...        ("20220601123000.000000", "20220601183000.000000"),
    ...        (datetime(2022, 6, 2, 12, 30), datetime(2003, 1, 2, 18, 30)),
    ...        (otp.datetime(2022, 6, 3, 12, 30), otp.datetime(2022, 6, 3, 18, 30)),
    ...    ],
    ...    timezone="GMT",
    ...    tick_type="TRD",
    ...    db="LOCAL",
    ... )

    Create cache with OTQ params:

    >>> otp.create_cache(  # doctest: +SKIP
    ...    cache_name="some_cache",
    ...    query="query_example.otq::query"),
    ...    otq_params={"some_param": "some_value"},
    ...    timezone="GMT",
    ...    tick_type="TRD",
    ...    db="LOCAL",
    ... )
    """
    if query is None:
        raise ValueError("Parameter `query` should be set")

    if time_granularity_units is None:
        time_granularity_units = "none"

    if time_granularity_units not in {'none', 'days', 'months', 'seconds'}:
        raise ValueError(f"Incorrect `time_granularity_units` param value passed: {time_granularity_units}")

    time_granularity_units = time_granularity_units.upper()

    if symbol is None:
        symbol = configuration.config.default_symbol

    if db is None:
        db = configuration.config.default_db

    time_intervals_str = ""
    if time_intervals_to_cache:
        time_intervals_str = _convert_time_intervals(time_intervals_to_cache)

    otq_params_str = process_otq_params(otq_params)

    otq_file_path = None
    if query:
        if isinstance(query, FunctionType):
            query = query()

        if isinstance(query, Source):
            # will create sub-query after source is initialized below
            otq_file_path = 'THIS::create_cache_query'
        elif isinstance(query, str):
            otq_file_path = query
        else:
            raise ValueError(f"Passed `query` parameter value with incorrect type: {type(query)}")

    source = Source(
        otq.CreateCache(
            cache_name=cache_name,
            otq_file_path=otq_file_path,
            inheritability=inheritability,
            otq_params=otq_params_str,
            time_granularity=time_granularity,
            time_granularity_units=time_granularity_units,
            timezone=timezone,
            time_intervals_to_cache=time_intervals_str,
            allow_delete_to_everyone=allow_delete_to_everyone,
            allow_update_to_everyone=allow_update_to_everyone,
            allow_search_to_everyone=allow_search_to_everyone,
            cache_expiration_interval=cache_expiration_interval,
        ),
    )

    if isinstance(query, Source):
        # create temp file with query
        query._store_in_tmp_otq(source._tmp_otq, name='create_cache_query')

    update_node_tick_type(source, tick_type, db)

    res = otp.run(source, symbols=symbol)
    _check_status(res)


def delete_cache(
    cache_name: str,
    apply_to_entire_cache: bool = True,
    per_cache_otq_params: Union[dict, None] = None,
    tick_type: str = "ANY",
    symbol: Optional[str] = None,
    db: Optional[str] = None,
):
    """
    Delete cache via DELETE_CACHE EP

    Parameters
    ----------
    cache_name: str
        Name of the cache to be deleted.
    apply_to_entire_cache: bool
        When set to ``True`` deletes the cache for all symbols and time intervals.
    per_cache_otq_params: dict
        Deletes cache that have been associated with this OTQ parameters during its creation.
        Value of this parameter should be equal to the value of ``otq_params`` of
        :func:`<onetick.py.create_cache>`.
    tick_type: str
        Tick type.
    symbol: str, list of str, list of otq.Symbol, :py:class:`onetick.py.Source`, :pandas:`pandas.DataFrame`, optional
        ``symbols`` parameter of ``otp.run()``.
    db: str
        Database.

    See also
    --------
    | **DELETE_CACHE** OneTick event processor
    | :py:class:`onetick.py.ReadCache`
    | :py:func:`onetick.py.create_cache`

    Examples
    --------
    Simple cache deletion

    >>> otp.delete_cache(  # doctest: +SKIP
    ...    cache_name="some_cache", tick_type="TRD", symbol="SYM", db="LOCAL",
    ... )
    """
    if symbol is None:
        symbol = configuration.config.default_symbol

    if db is None:
        db = configuration.config.default_db

    source = Source(
        otq.DeleteCache(
            cache_name=cache_name,
            apply_to_entire_cache=apply_to_entire_cache,
            per_cache_otq_params=process_otq_params(per_cache_otq_params),
        ),
    )

    update_node_tick_type(source, tick_type, db)

    res = otp.run(source, symbols=symbol)
    _check_status(res)


def modify_cache_config(
    cache_name: str,
    param_name: str,
    param_value: Any,
    tick_type: str = "ANY",
    symbol: Optional[str] = None,
    db: Optional[str] = None,
):
    """
    Modify cache configuration via MODIFY_CACHE_CONFIG EP

    Parameters
    ----------
    cache_name: str
        Name of the cache to be deleted.
    param_name: str
        The name of the configuration parameter to be changed.
        Supported parameters:

        * ``inheritability``
        * ``time_granularity``
        * ``time_granularity_units``
        * ``timezone``
        * ``allow_search_to_everyone``
        * ``allow_delete_to_everyone``
        * ``allow_update_to_everyone``

    param_value: Any
        New value of configuration parameter. Will be converted to string.
    tick_type: str
        Tick type.
    symbol: str, list of str, list of otq.Symbol, :py:class:`onetick.py.Source`, :pandas:`pandas.DataFrame`, optional
        ``symbols`` parameter of ``otp.run()``.
    db: str
        Database.

    See also
    --------
    | **MODIFY_CACHE_CONFIG** OneTick event processor
    | :py:class:`onetick.py.ReadCache`
    | :py:func:`onetick.py.create_cache`

    Examples
    --------
    Simple cache config modification

    >>> otp.modify_cache_config(  # doctest: +SKIP
    ...    cache_name="some_cache",
    ...    param_name="time_granularity",
    ...    param_value=3,
    ...    tick_type="TRD", symbol="SYM", db="LOCAL",
    ... )
    """
    if symbol is None:
        symbol = configuration.config.default_symbol

    if db is None:
        db = configuration.config.default_db

    param_name = param_name.upper()
    param_value = str(param_value)

    source = Source(
        otq.ModifyCacheConfig(
            cache_name=cache_name,
            config_parameter_name=param_name,
            config_parameter_value=param_value,
        ),
    )

    update_node_tick_type(source, tick_type, db)

    res = otp.run(source, symbols=symbol)
    _check_status(res)
