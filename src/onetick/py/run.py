import asyncio
import inspect
import datetime
import warnings
from typing import Union, List, Optional, Dict, Any, Callable, Type
from collections import defaultdict

import numpy as np
import pandas as pd
from onetick.py.otq import otq, pyomd, otli

from onetick import py as otp
from onetick.py import utils, configuration
from onetick.py.core.column_operations.base import _Operation
from onetick.py.types import datetime2timeval, datetime2expr
from onetick.py.core.source import _is_dict_required
from onetick.py.compatibility import (
    has_max_expected_ticks_per_symbol,
    has_password_param,
    has_query_encoding_parameter,
    _add_version_info_to_exception,
)
from onetick.py._stack_info import _add_stack_info_to_exception
from onetick.py.callback import LogCallback, ManualDataframeCallback


def run(query: Union[Callable, Dict, otp.Source, otp.MultiOutputSource,  # NOSONAR
                     otp.query, str, otq.EpBase, otq.GraphQuery,
                     otq.ChainQuery, otq.Chainlet, otq.SqlQuery, otp.SqlQuery],
        *,
        symbols: Union[List[Union[str, otq.Symbol]], otp.Source, str, None] = None,
        start: Union[datetime.datetime, otp.datetime, pyomd.timeval_t, None] = utils.adaptive,  # type: ignore
        end: Union[datetime.datetime, otp.datetime, pyomd.timeval_t, None] = utils.adaptive,  # type: ignore
        date: Union[datetime.date, otp.date, None] = None,
        start_time_expression: Optional[str] = None,
        end_time_expression: Optional[str] = None,
        timezone=utils.default,  # type: ignore
        context=utils.default,  # type: ignore
        username: Optional[str] = None,
        alternative_username: Optional[str] = None,
        password: Optional[str] = None,
        batch_size: Union[int, Type[utils.default], None] = utils.default,
        running: Optional[bool] = False,
        query_properties: Optional[pyomd.QueryProperties] = None,  # type: ignore
        concurrency: Union[int, Type[utils.default], None] = utils.default,
        apply_times_daily: Optional[int] = None,
        symbol_date: Union[datetime.datetime, int, str, None] = None,
        query_params: Optional[Dict[str, Any]] = None,
        time_as_nsec: bool = True,
        treat_byte_arrays_as_strings: bool = True,
        output_matrix_per_field: bool = False,
        output_structure: Optional[str] = None,
        return_utc_times: Optional[bool] = None,
        connection=None,
        callback=None,
        svg_path=None,
        use_connection_pool: bool = False,
        node_name: Union[str, List[str], None] = None,
        require_dict: bool = False,
        max_expected_ticks_per_symbol: Optional[int] = None,
        log_symbol: Union[bool, Type[utils.default]] = utils.default,
        encoding: Optional[str] = None,
        manual_dataframe_callback: bool = False):
    """
    Executes a query and returns its result.

    Parameters
    ----------
    query: :py:class:`onetick.py.Source`, otq.Ep, otq.GraphQuery, otq.ChainQuery, str, otq.Chainlet,\
            Callable, otq.SqlQuery, :py:class:`onetick.py.SqlQuery`
        Query to execute can be source, path of the query on a disk or onetick.query graph or event processor.
        For running OTQ files, it represents the path (including filename) to the OTQ file to run a single query within
        the file. If more than one query is present, then the query to be run must be specified
        (that is, ``'path_to_file/otq_file.otq::query_to_run'``).

        ``query`` can also be a function that has a symbol object as the first parameter.
        This object can be used to get symbol name and symbol parameters.
        Function must return a :py:class:`Source <onetick.py.Source>`.
    symbols: str, list of str, list of otq.Symbol, :py:class:`onetick.py.Source`, :pandas:`pandas.DataFrame`, optional
        Symbol(s) to run the query for passed as a string, a list of strings,
        a :pandas:`pandas.DataFrame` with the ``SYMBOL_NAME`` column,
        or as a "symbols" query which results include the ``SYMBOL_NAME`` column.
        The start/end times for the symbols query will taken from the params below.
        See :ref:`symbols <static/concepts/symbols:Symbols: bound and unbound>` for more details.
    start: :py:class:`datetime.datetime`, :py:class:`otp.datetime <onetick.py.datetime>`,\
            :py:class:`pyomd.timeval_t`, optional
        The start time of the query. Can be timezone-naive or timezone-aware. See also ``timezone`` argument.
        onetick.py uses :py:attr:`otp.config.default_start_time<onetick.py.configuration.Config.default_start_time>`
        as default value, if you don't want to specify start time, e.g. to use saved time of the query,
        then you should specify None value.
    end: :py:class:`datetime.datetime`, :py:class:`otp.datetime <onetick.py.datetime>`,\
          :py:class:`pyomd.timeval_t`, optional
        The end time of the query (note that it's non-inclusive).
        Can be timezone-naive or timezone-aware. See also ``timezone`` argument.
        onetick.py uses :py:attr:`otp.config.default_end_time<onetick.py.configuration.Config.default_end_time>`
        as default value, if you don't want to specify end time, e.g. to use saved time of the query,
        then you should specify None value.
    date: :py:class:`datetime.date`, :py:class:`otp.date <onetick.py.date>`, optional
        The date to run the query for. Can be set instead of ``start`` and ``end`` parameters.
        If set then the interval to run the query will be from 0:00 to 24:00 of the specified date.
    start_time_expression: str, :py:class:`~onetick.py.Operation`, optional
        Start time onetick expression of the query. If specified, it will take precedence over ``start``.
        Supported only if query is Source, Graph or Event Processor.
        Not supported for WebAPI mode.
    end_time_expression: str, :py:class:`~onetick.py.Operation`, optional
        End time onetick expression of the query. If specified, it will take precedence over ``end``.
        Supported only if query is Source, Graph or Event Processor.
        Not supported for WebAPI mode.
    timezone: str, optional
         The timezone of output timestamps.
         Also, when start and/or end arguments are timezone-naive, it will define their timezone.
         If parameter is omitted timestamps of ticks will be formatted
         with the default :py:attr:`otp.config.tz<onetick.py.configuration.Config.tz>`.
    context: str, optional
        Allows specification of different contexts from OneTick configuration to connect to.
        If not set then default :py:attr:`otp.config.context<onetick.py.configuration.Config.context>` is used.
        See :ref:`guide about switching contexts <switching contexts>` for examples.
    username
        The username to make the connection.
        By default the user which executed the process is used or the value specified in
        :py:attr:`otp.config.default_username<onetick.py.configuration.Config.default_username>`.
    alternative_username: str
        The username used for authentication.
        Needs to be set only when the tick server is configured to use password-based authentication.
        By default,
        :py:attr:`otp.config.default_auth_username<onetick.py.configuration.Config.default_auth_username>` is used.
        Not supported for WebAPI mode.
    password: str, optional
        The password used for authentication.
        Needs to be set only when the tick server is configured to use password-based authentication.
        Note: not supported and ignored on older OneTick versions.
        By default, :py:attr:`otp.config.default_password<onetick.py.configuration.Config.default_password>` is used.
    batch_size: int
        number of symbols to run in one batch.
        By default, the value from
        :py:attr:`otp.config.default_batch_size<onetick.py.configuration.Config.default_batch_size>` is used.
        Not supported for WebAPI mode.
    running: bool, optional
        Indicates whether a query is CEP or not. Default is `False`.
    query_properties: :py:class:`pyomd.QueryProperties` or dict, optional
       Query properties, such as ONE_TO_MANY_POLICY, ALLOW_GRAPH_REUSE, etc
    concurrency: int, optional
        The maximum number of CPU cores to use to process the query.
        By default, the value from
        :py:attr:`otp.config.default_concurrency<onetick.py.configuration.Config.default_concurrency>` is used.
    apply_times_daily: bool
        Runs the query for every day in the ``start``-``end`` time range,
        using the time components of ``start`` and ``end`` datetimes.

        Note that those daily intervals are executed separately, so you don't have access
        to the data from previous or next days (see example in the next section).
    symbol_date:
        The symbol date used to look up symbology mapping information in the reference database,
        expressed as datetime object or integer of YYYYMMDD format
    query_params: dict
        Parameters of the query.
    time_as_nsec: bool
        Outputs timestamps up to nanoseconds granularity
        (defaults to False: by default we output timestamps in microseconds granularity)
    treat_byte_arrays_as_strings: bool
        Outputs byte arrays as strings (defaults to True)
        Not supported for WebAPI mode.
    output_matrix_per_field: bool
        Changes output format to list of matrices per field.
        Not supported for WebAPI mode.
    output_structure: otp.Source.OutputStructure, optional

        Structure (type) of the result. Supported values are:
          - `df` (default) - the result is returned as :pandas:`pandas.DataFrame` object
            or dictionary of symbol names and :pandas:`pandas.DataFrame` objects
            in case of using multiple symbols or first stage query.
          - `map` - the result is returned as SymbolNumpyResultMap.
          - `list` - the result is returned as list.
          - `polars` - the result is returned as
            `polars.DataFrame <https://docs.pola.rs/api/python/stable/reference/dataframe/index.html>`_ object
            or dictionary of symbol names and dataframe objects
            (**Only supported in WebAPI mode**).
    return_utc_times: bool
        If True Return times in UTC timezone and in local timezone otherwise
        Not supported for WebAPI mode.
    connection: :py:class:`pyomd.Connection`
        The connection to be used for discovering nested .otq files
        Not supported for WebAPI mode.
    callback: :py:class:`onetick.py.CallbackBase`
         Class with callback methods.
         If set, the output of the query should be controlled with callbacks
         and this function returns nothing.
    svg_path: str, optional
        Not supported for WebAPI mode.
    use_connection_pool: bool
        Default is False. If set to True, the connection pool is used.
        Not supported for WebAPI mode.
    node_name: str, List[str], optional
        Name of the output node to select result from. If query graph has several output nodes, you can specify the name
        of the node to choose result from. If node_name was specified, query should be presented by path on the disk
        and output_structure should be `df`
    require_dict: bool
        If set to True, result will be forced to be a dictionary even if it's returned for a single symbol
    max_expected_ticks_per_symbol: int
        Expected maximum number of ticks per symbol (used for performance optimizations).
        By default, :py:attr:`otp.config.max_expected_ticks_per_symbol \
            <onetick.py.configuration.Config.max_expected_ticks_per_symbol>` is used.
        Not supported for WebAPI mode.
    log_symbol: bool
        Log currently executed symbol.
        Note that this only works with unbound symbols.
        Also in this case :py:func:`otp.run<onetick.py.run>` is executed in ``callback`` mode
        and no value is returned from the function, so it should be used only for debugging purposes.
        This logging will not work if some other value specified in parameter ``callback``.
        By default, :py:attr:`otp.config.log_symbol<onetick.py.configuration.Config.log_symbol>` is used.
    encoding: str, optional
        The encoding of string fields.
    manual_dataframe_callback: bool
        Create dataframe manually with ``callback`` mode.
        Only works if ``output_structure='df'`` is specified and parameter ``callback`` is not.
        May improve performance in some cases.

    Returns
    -------
    result, list, dict, :pandas:`pandas.DataFrame`, None
        result of the query

    Examples
    --------

    Running :py:class:`onetick.py.Source` and setting start and end times:

    >>> data = otp.Tick(A=1)
    >>> otp.run(data, start=otp.dt(2003, 12, 2), end=otp.dt(2003, 12, 4))
            Time  A
    0 2003-12-02  1

    Setting query interval with ``date`` parameter:

    >>> data = otp.Tick(A=1)
    >>> data['START'] = data['_START_TIME']
    >>> data['END'] = data['_END_TIME']
    >>> otp.run(data, date=otp.dt(2003, 12, 1))
            Time  A      START        END
    0 2003-12-01  1 2003-12-01 2003-12-02

    Running otq.Ep and passing query parameters:

    >>> ep = otq.TickGenerator(bucket_interval=0, fields='long A = $X').tick_type('TT')
    >>> otp.run(ep, symbols='LOCAL::', query_params={'X': 1})
            Time  A
    0 2003-12-04  1

    Running in callback mode:

    >>> class Callback(otp.CallbackBase):
    ...     def __init__(self):
    ...         self.result = None
    ...     def process_tick(self, tick, time):
    ...         self.result = tick
    >>> data = otp.Tick(A=1)
    >>> callback = Callback()
    >>> otp.run(data, callback=callback)
    >>> callback.result
    {'A': 1}

    Running with ``apply_times_daily``.
    Note that daily intervals are processed separately so, for example,
    we can't access column **COUNT** from previous day.

    >>> trd = otp.DataSource('US_COMP', symbols='AAPL', tick_type='TRD')  # doctest: +SKIP
    >>> trd = trd.agg({'COUNT': otp.agg.count()},
    ...               bucket_interval=12 * 3600, bucket_time='start')  # doctest: +SKIP
    >>> trd['PREV_COUNT'] = trd['COUNT'][-1]  # doctest: +SKIP
    >>> otp.run(trd, apply_times_daily=True,
    ...         start=otp.dt(2023, 4, 3), end=otp.dt(2023, 4, 5), timezone='EST5EDT')  # doctest: +SKIP
                     Time   COUNT  PREV_COUNT
    0 2023-04-03 00:00:00  328447           0
    1 2023-04-03 12:00:00  240244      328447
    2 2023-04-04 00:00:00  263293           0
    3 2023-04-04 12:00:00  193018      263293

    Using a function as a ``query``, accessing symbol name and parameters:

    >>> def query(symbol):
    ...     t = otp.Tick(X='x')
    ...     t['SYMBOL_NAME'] = symbol.name
    ...     t['SYMBOL_PARAM'] = symbol.PARAM
    ...     return t
    >>> symbols = otp.Ticks({'SYMBOL_NAME': ['A', 'B'], 'PARAM': [1, 2]})
    >>> result = otp.run(query, symbols=symbols)
    >>> result['A']
            Time  X SYMBOL_NAME  SYMBOL_PARAM
    0 2003-12-01  x           A             1
    >>> result['B']
            Time  X SYMBOL_NAME  SYMBOL_PARAM
    0 2003-12-01  x           B             2

    Debugging unbound symbols with ``log_symbol`` parameter:

    >>> data = otp.Tick(X=1)
    >>> symbols = otp.Ticks({'SYMBOL_NAME': ['A', 'B'], 'PARAM': [1, 2]})
    >>> otp.run(query, symbols=symbols, log_symbol=True)  # doctest: +ELLIPSIS
    Running query <onetick.py.sources.ticks.Tick object at ...>
    Processing symbol A
    Processing symbol B

    By default, some non-standard characters in data strings could be processed incorrectly:

    >>> data = ['AA測試AA']
    >>> source = otp.Ticks({'A': data})
    >>> otp.run(source)
            Time           A
    0 2003-12-01  AAæ¸¬è©¦AA

    To fix this you can pass `encoding` parameter to `otp.run`:

    .. testcode::
       :skipif: not has_query_encoding_parameter()

       data = ['AA測試AA']
       source = otp.Ticks({'A': data})
       df = otp.run(source, encoding="utf-8")
       print(df)

    .. testoutput::

               Time        A
       0 2003-12-01  AA測試AA

    Note that query ``start`` time is inclusive, but query ``end`` time is not,
    meaning that ticks with timestamps equal to the query end time will not be included:

    >>> data = otp.Tick(A=1, bucket_interval=24*60*60)
    >>> data['A'] = data['TIMESTAMP'].dt.day_of_month()
    >>> otp.run(data, start=otp.dt(2003, 12, 1), end=otp.dt(2003, 12, 4))
            Time  A
    0 2003-12-01  1
    1 2003-12-02  2
    2 2003-12-03  3
    >>> otp.run(data, start=otp.dt(2003, 12, 1), end=otp.dt(2003, 12, 2))
            Time  A
    0 2003-12-01  1

    If you want to include such ticks, you can add one nanosecond to the query end time:

    >>> otp.run(data, start=otp.dt(2003, 12, 1), end=otp.dt(2003, 12, 2) + otp.Nano(1))
            Time  A
    0 2003-12-01  1
    1 2003-12-02  2
    """
    _ = otli.OneTickLib()

    query_schema = None
    if isinstance(query, otp.Source):
        query_schema = query.schema

    if timezone is utils.default:
        timezone = configuration.config.tz
    if context is utils.default or context is None:
        context = configuration.config.context
    if concurrency is utils.default:
        concurrency = configuration.default_query_concurrency()

    if batch_size is utils.default:
        batch_size = configuration.config.default_batch_size
    if query_properties is None:
        query_properties = pyomd.QueryProperties()

    if isinstance(query_properties, dict):
        qp_dict = query_properties
        query_properties = utils.query_properties_from_dict(qp_dict)
    else:
        qp_dict = utils.query_properties_to_dict(query_properties)

    if 'USE_FT' not in qp_dict:
        query_properties.set_property_value('USE_FT', otp.config.default_fault_tolerance)  # type: ignore[union-attr]

    if 'IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE' not in qp_dict:
        query_properties.set_property_value('IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE',  # type: ignore[union-attr]
                                            str(otp.config.ignore_ticks_in_unentitled_time_range).upper())

    if date is not None:
        for v in (start, end, start_time_expression, end_time_expression):
            if v is not None and v is not utils.adaptive:
                raise ValueError("Can't use 'date' parameter when other time interval parameters are specified")
        start = otp.date(date)
        end = start + otp.Day(1)

    has_source_start, has_source_end = False, False
    if isinstance(query, otp.Source):
        has_source_start, has_source_end = query.has_start_end_time()

    if (start is None or start is utils.adaptive) and otp.config.get('default_start_time') is None and \
            not has_source_start:
        warnings.warn('Start time is None and default start time is not set, '
                      'onetick.query will use 19700101 as start time, '
                      'which can cause unexpected results. '
                      'Please set start time explicitly.')
    if (end is None or end is utils.adaptive) and otp.config.get('default_end_time') is None and \
            not has_source_end:
        warnings.warn('End time is None and default end time is not set, '
                      'onetick.query will use 19700101 as end time, '
                      'which can cause unexpected results. '
                      'Please set end time explicitly.')

    if isinstance(start, _Operation) and start_time_expression is None:
        start_time_expression = str(start)
    if isinstance(end, _Operation) and end_time_expression is None:
        end_time_expression = str(end)

    if isinstance(start_time_expression, _Operation):
        start_time_expression = str(start_time_expression)
    if isinstance(end_time_expression, _Operation):
        end_time_expression = str(end_time_expression)

    # PY-1321: CEP-query seems to be using start and end values for some reason, so setting them to None
    if start_time_expression is not None:
        start = None
    if end_time_expression is not None:
        end = None

    if inspect.ismethod(query) or inspect.isfunction(query):
        t_s = None
        if isinstance(symbols, otp.Source):
            t_s = symbols
        if isinstance(symbols, otp.query):
            t_s = otp.Query(symbols)
        if isinstance(symbols, str):
            t_s = otp.Tick(SYMBOL_NAME=symbols)
        if isinstance(symbols, list):
            t_s = otp.Ticks(SYMBOL_NAME=symbols)

        if isinstance(t_s, otp.Source):
            query = query(t_s.to_symbol_param())  # type: ignore

    query, query_params = _preprocess_otp_query(query, query_params)
    # If query is an otp.Source object, then it can deal with otp.datetime and pd.Timestamp types

    if log_symbol is utils.default:
        log_symbol = otp.config.log_symbol
    if callback is None and log_symbol:
        callback = LogCallback(query)

    if manual_dataframe_callback:
        if output_structure and output_structure != 'df':
            raise ValueError("Parameter 'output_structure' must be set to 'df'"
                             " if parameter 'manual_dataframe_callback' is set")
        if log_symbol:
            raise ValueError("Parameters 'manual_dataframe_callback' and 'log_symbol' can't be set together")
        if callback is not None:
            raise ValueError("Parameters 'manual_dataframe_callback' and 'callback' can't be set together")
        callback = ManualDataframeCallback(timezone)

    output_mode = otq.QueryOutputMode.numpy
    if callback is not None:
        output_mode = otq.QueryOutputMode.callback
    if output_structure == 'polars':
        if not otq.webapi:
            raise ValueError("Parameter output_structure='polars' is only supported in WebAPI mode.")
        try:
            import polars as _  # type: ignore
        except ImportError:
            raise ValueError("Parameter output_structure='polars' is specified, but module polars can't be imported. "
                             "Use 'pip install onetick-py[polars]' command to install onetick-py with polars support.")
        try:
            output_mode = otq.QueryOutputMode.polars
        except AttributeError:
            raise ValueError("Parameter output_structure='polars' is specified, but it's not supported "
                             "by installed onetick.query_webapi library.")

    output_structure, output_structure_for_otq = _process_output_structure(output_structure)

    require_dict = require_dict or _is_dict_required(symbols)

    # converting symbols properly
    if isinstance(symbols, otp.Source):
        # check if SYMBOL_NAME is in schema, or if schema contains only one field
        if ('SYMBOL_NAME' not in symbols.columns(skip_meta_fields=True).keys()) and \
                len(symbols.columns(skip_meta_fields=True)) != 1:
            warnings.warn('Using as a symbol list a source without "SYMBOL_NAME" field '
                          'and with more than one field! This won\'t work unless the schema is incomplete')

        symbols = otp.Source._convert_symbol_to_string(
            symbol=symbols,
            tmp_otq=query._tmp_otq if isinstance(query, otp.Source) else None,
            start=start,
            end=end,
            timezone=timezone
        )
    if isinstance(symbols, str):
        symbols = [symbols]
    if isinstance(symbols, pd.DataFrame):
        symbols = utils.get_symbol_list_from_df(symbols)

    if isinstance(query, dict):
        # we assume it's a dictionary of sources for the MultiOutputSource object
        query = otp.MultiOutputSource(query)

    if symbol_date:
        # otq.run supports only strings and datetime.date
        symbol_date = utils.symbol_date_to_str(symbol_date)

    params_saved_to_otq = {}
    if isinstance(query, (otp.Source, otp.MultiOutputSource)):
        start = None if start is utils.adaptive else start
        end = None if end is utils.adaptive else end
        params_saved_to_otq = dict(
            symbols=symbols,
            start=start,
            end=end,
            start_time_expression=start_time_expression,
            end_time_expression=end_time_expression,
        )
        param_upd = query._prepare_for_execution(symbols=symbols, start=start, end=end,
                                                 timezone=timezone,
                                                 start_time_expression=start_time_expression,
                                                 end_time_expression=end_time_expression,
                                                 require_dict=require_dict,
                                                 running_query_flag=running,
                                                 node_name=node_name, has_output=None,
                                                 symbol_date=symbol_date)
        query, require_dict, node_name, symbol_date_to_run = param_upd
        # symbols and start/end times should be already stored in the query and should not be passed again
        symbols = None
        start = None
        end = None
        start_time_expression = None
        end_time_expression = None
        time_as_nsec = True
        # PY-1423: except for symbol date
        symbol_date = symbol_date_to_run

    elif isinstance(query, (otq.graph_components.EpBase, otq.Chainlet)):
        query = otq.GraphQuery(query)

    if isinstance(query, otq.SqlQuery):
        # This has no impact on query result, just placeholder values
        start = end = None

    if start is utils.adaptive:
        start = configuration.config.default_start_time

    if end is utils.adaptive:
        end = configuration.config.default_end_time

    if not otq.webapi:
        # converting to expressions, because in datetime objects nanoseconds are not supported on some OneTick versions
        if start is not None and not start_time_expression:
            start_time_expression = datetime2expr(start)
        if end is not None and not end_time_expression:
            end_time_expression = datetime2expr(end)

    # start and end parameters could be set to None,
    # because we use start and end time expressions,
    # but because of the bug it sometimes doesn't work
    # https://onemarketdata.atlassian.net/browse/BDS-454
    start, end = _get_start_end(start, end, timezone)

    # authentication
    username = username or otp.config.default_username
    alternative_username = alternative_username or otp.config.default_auth_username
    password = password or otp.config.default_password
    kwargs = {}
    if password is not None and has_password_param(throw_warning=True):
        kwargs['password'] = password

    max_expected_ticks_per_symbol = max_expected_ticks_per_symbol or otp.config.max_expected_ticks_per_symbol
    if max_expected_ticks_per_symbol is not None and has_max_expected_ticks_per_symbol(throw_warning=True):
        kwargs['max_expected_ticks_per_symbol'] = max_expected_ticks_per_symbol
    elif max_expected_ticks_per_symbol is None and has_max_expected_ticks_per_symbol(throw_warning=False):
        kwargs['max_expected_ticks_per_symbol'] = 2000

    if encoding is not None and has_query_encoding_parameter(throw_warning=True):
        kwargs['encoding'] = encoding

    run_params = dict(
        query=query,
        symbols=symbols, start=start, end=end, context=context, username=username,
        timezone=timezone,
        start_time_expression=start_time_expression,
        end_time_expression=end_time_expression,
        alternative_username=alternative_username, batch_size=batch_size,
        running_query_flag=running, query_properties=query_properties,
        max_concurrency=concurrency, apply_times_daily=apply_times_daily, symbol_date=symbol_date,
        query_params=query_params, time_as_nsec=time_as_nsec,
        treat_byte_arrays_as_strings=treat_byte_arrays_as_strings,
        output_mode=output_mode,
        output_matrix_per_field=output_matrix_per_field, output_structure=output_structure_for_otq,
        return_utc_times=return_utc_times, connection=connection,
        callback=callback, svg_path=svg_path, use_connection_pool=use_connection_pool, **kwargs
    )

    # some parameters were saved in .otq file, we need to debug them too
    debug_params = dict(run_params, **params_saved_to_otq) if params_saved_to_otq else run_params
    otp.get_logger(__name__).info(otp.utils.json_dumps(debug_params))

    try:
        result = otq.run(**run_params)
    except Exception as e:
        e = _add_stack_info_to_exception(e)
        e = _add_version_info_to_exception(e)
        raise e  # noqa: W0707

    if output_mode == otq.QueryOutputMode.callback:
        if manual_dataframe_callback:
            result = callback.result
        return result

    # node_names should be either a list of node names or None
    node_names: Optional[List[str]]
    if isinstance(node_name, str):
        node_names = [node_name]
    else:
        node_names = node_name

    if query_schema:
        # check if we have empty result for any symbol to add schema to empty dataframes
        _process_empty_results(result, query_schema, output_structure)

    return _format_call_output(result, output_structure=output_structure,
                               require_dict=require_dict, node_names=node_names)


async def run_async(*args, **kwargs):
    """
    Asynchronous alternative to :func:`otp.run <onetick.py.run>`.

    All parameters are the same.

    This function can be used via built-in python ``await`` syntax
    and standard `asyncio <https://docs.python.org/3/library/asyncio.html>`_ library.

    Note
    ----
    Internally this function is implemented as :func:`otp.run <onetick.py.run>` running in a separate thread.

    Threads in python are generally not interruptable,
    so some `asyncio` functionality may not work as expected.

    For example, canceling :func:`otp.run_async <onetick.py.run_async>` task by
    `timeout <https://docs.python.org/3/library/asyncio-task.html#timeouts>`_
    may block the waiting function
    or exiting the python process will block until the task is finished,
    depending on python and `asyncio` back-end implementation.

    Examples
    --------

    >>> data = otp.Ticks(A=[1, 2, 3])

    Calling :func:`otp.run_async <onetick.py.run_async>` will create a "coroutine" object:

    >>> otp.run_async(data) # doctest: +SKIP
    <coroutine object run_async at ...>

    Use `asyncio.run <https://docs.python.org/3/library/asyncio-runner.html#asyncio.run>`_
    to run this coroutine and wait for it to finish:

    >>> asyncio.run(otp.run_async(data))
                         Time  A
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2
    2 2003-12-01 00:00:00.002  3

    You can use standard `asyncio <https://docs.python.org/3/library/asyncio.html>`_
    library functions to create and schedule tasks.

    In the example below two tasks are executed in parallel,
    so total execution time will be around 3 seconds instead of 6:

    >>> import asyncio
    >>> import time
    >>> async def parallel_tasks():
    ...     # pause 1 second on each tick (thus 3 seconds for 3 ticks)
    ...     task_otp = asyncio.create_task(otp.run_async(data.pause(1000)))
    ...     # just sleep for 3 seconds
    ...     task_other = asyncio.create_task(asyncio.sleep(3))
    ...     otp_result = await task_otp
    ...     await task_other
    ...     print(otp_result)
    >>> start_time = time.time()
    >>> asyncio.run(parallel_tasks())
                         Time  A
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2
    2 2003-12-01 00:00:00.002  3
    >>> print('Finished in', time.time() - start_time, 'seconds') # doctest: +SKIP
    Finished in 3.0108885765075684 seconds
    """
    return await asyncio.to_thread(run, *args, **kwargs)


def _filter_returned_map_by_node(result, _node_names):
    """
    Here, result has the following format: {symbol: {node_name: data}}
    We need to filter by correct node_name
    """
    # TODO: implement filtering by node_name in a way
    # that no information from SymbolNumpyResultMap object is lost
    return result


def _filter_returned_list_by_node(result, node_names):
    """
    Here, result has the following format: [(symbol, data_1, data_2, node_name)]
    We need to filter by correct node_names
    """
    if not node_names:
        return result

    node_found = False

    res = []
    empty_result = True
    for symbol, data_1, data_2, node, *_ in result:
        if len(data_1) > 0:
            empty_result = False
        if node in node_names:
            node_found = True
            res.append((symbol, data_1, data_2, node))

    if not empty_result and not node_found:
        # TODO: Do we even want to raise it?
        raise ValueError(f'No passed node name(s) were found in the results. Passed node names were: {node_names}')
    return res


def _form_dict_from_list(data_list, output_structure):
    """
    Here, data_list has the following format: [(symbol, data_1, data_2, node_name), ...]
    We need to create the following result:
    either {symbol: DataFrame(data_1)} if there is only one result per symbol
    or {symbol: [DataFrame(data_1)]} if there are multiple results for symbol for a single node_name
    or {symbol: {node_name: DataFrame(data_1)}} if there are single results for multiple node names for a symbol
    or {symbol: {node_name: [DataFrame(data_1)]}} if there are multiple results for multiple node names for a symbol
    """

    def form_node_name_dict(lst):
        """
        lst is a lit of (node, dataframe)
        """
        d = defaultdict(list)
        for node, df in lst:
            d[node].append(df)
        for node, node_list in d.items():
            if len(node_list) == 1:
                d[node] = node_list[0]
        if len(d) == 1:
            d = list(d.values())[0]
        else:  # converting defaultdict to regular dict
            d = dict(d)
        return d

    def get_dataframe(data):
        if output_structure == 'df':
            return pd.DataFrame(dict(data))
        else:
            import polars
            if isinstance(data, polars.DataFrame):
                # polars only works in webapi mode,
                # and it's already returned as polars.DataFrame by onetick.query_webapi
                return data
            # but if there is no data, then we want to return empty polars.DataFrame
            return polars.DataFrame()

    symbols_dict = defaultdict(list)
    for symbol, data, _, node, *_ in data_list:
        df = get_dataframe(data)

        list_item = (node, df)
        symbols_dict[symbol].append(list_item)

    for symbol, lst in symbols_dict.items():
        symbols_dict[symbol] = form_node_name_dict(lst)

    return dict(symbols_dict)


def _format_call_output(result, output_structure, node_names, require_dict):
    """Formats output of otq.run() according to passed parameters.
    See parameters' description for more information

    Parameters
    ----------
    output_structure: ['df', 'list', 'map']
        If 'df': forms pandas.DataFrame from the result.

        Returns a dictionary with symbols as keys if there's more than one symbol
        in returned data of if require_dict = True.

        Values of the returned dictionary, or returned value itself if no dictionary is formed,
        is either a list of tuples: (node_name, dataframe) if there's output for more than one node
        or a dataframe

        If 'list' or 'map': returns data as returned by otq.run(), possibly filtered by node_name (see below)
    node_names: str, None
        If not None, then selects only output returned by nodes in node_names list
        for all output structures
    require_dict: bool
        If True, forces output for output_structure='df' to always be a dictionary, even if only one symbol is returned
        Has no effect for other values of output_structure

    Returns
    ----------
        Formatted output: pandas DataFrame, dictionary or list

    """
    if output_structure == 'list':
        return _filter_returned_list_by_node(result, node_names)
    elif output_structure == 'map':
        return _filter_returned_map_by_node(result, node_names)

    assert output_structure in ('df', 'polars'), (f'Output structure should be one of: "df", "map", "list", "polars" '
                                                  f'instead "{output_structure}" was passed')

    # "df" output structure implies that raw results came as a list
    result_list = _filter_returned_list_by_node(result, node_names)
    result_dict = _form_dict_from_list(result_list, output_structure)

    if len(result_dict) == 1 and not require_dict:
        return list(result_dict.values())[0]
    else:
        return result_dict


def _process_empty_results(result, query_schema, output_structure):
    """
    Process query results and add columns to empty responses based on query schema.
    """
    schema = [
        (field, np.array([], dtype=otp.types.type2np(dtype)))
        for field, dtype in {**query_schema, 'Time': otp.nsectime}.items()
    ]
    if isinstance(result, otq.SymbolNumpyResultMap):
        empty_data = dict(schema)
    else:
        empty_data = schema

    if output_structure == 'polars':
        import polars
        empty_data = polars.DataFrame(dict(schema))

    if isinstance(result, otq.SymbolNumpyResultMap):
        for result_item in result.get_dict().values():
            for node_name, symbol_result in result_item.items():
                if len(symbol_result[0]) == 0:
                    result_item[node_name] = (empty_data, symbol_result[1])
    else:
        for idx, result_item in enumerate(result):
            if len(result_item[1]) == 0:
                result[idx] = (
                    result_item[0], empty_data, result_item[2], result_item[3], *result_item[4:]
                )


def _preprocess_otp_query(query, query_params):

    if isinstance(query, otp.query._outputs):
        query = query['OUT']

    if isinstance(query, otp.query):
        if query.params:
            if query_params:
                raise ValueError("please specify parameters in query or in otp.run only")
            query_params = query.params
        query = query.path
    return query, query_params


def _get_start_end(start, end, timezone):
    """
    Convert datetime objects supported by onetick-py
    to datetime objects supported by onetick-query.
    """
    def support_nanoseconds(time):
        if isinstance(time, (pd.Timestamp, otp.datetime)):
            if otq.webapi:
                # onetick-query_webapi supports pandas.Timestamp and strings in %Y%m%s%H%M%S.%J format
                if isinstance(time, pd.Timestamp):
                    return time
                elif isinstance(time, otp.datetime):
                    return time.ts
            else:
                if otp.compatibility.is_correct_timezone_used_in_otq_run():
                    time = datetime2timeval(time, timezone)
                else:
                    # there is a bug in older onetick versions using wrong timezone
                    time = datetime2timeval(time, 'GMT')
        return time

    if start is utils.adaptive:
        start = configuration.config.default_start_time

    if end is utils.adaptive:
        end = configuration.config.default_end_time

    # `isinstance(obj, datetime.date)` is not correct because
    # isinstance(<datetime.datetime object>, datetime.date) = True
    # pylint: disable=unidiomatic-typecheck
    if type(start) is datetime.date:
        start = datetime.datetime(start.year, start.month, start.day)
    if type(end) is datetime.date:
        end = datetime.datetime(end.year, end.month, end.day)

    start = support_nanoseconds(start)
    end = support_nanoseconds(end)

    return start, end


def _process_output_structure(output_structure):
    if not output_structure or output_structure == "df":  # otq doesn't support df
        output_structure = "df"
        output_structure_for_otq = "symbol_result_list"
    elif output_structure == "list":
        output_structure_for_otq = "symbol_result_list"
    elif output_structure == "map":
        output_structure_for_otq = "symbol_result_map"
    elif output_structure == "polars":
        output_structure = "polars"
        output_structure_for_otq = "symbol_result_list"
    else:
        raise ValueError("output_structure support only the following values: df, list, map and polars")
    return output_structure, output_structure_for_otq
