import inspect
import warnings
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Union
from onetick.py.backports import Literal

from onetick import py as otp
from onetick.py import types as ott
from onetick.py.core._internal._state_objects import _TickSequence
from onetick.py.core._source._symbol_param import _SymbolParamSource
from onetick.py.core.column_operations._methods.op_types import are_strings
from onetick.py.core.column_operations.base import _Operation
from onetick.py.core.eval_query import prepare_params
from onetick.py.otq import otq
from onetick.py.compatibility import (
    is_supported_point_in_time,
    is_join_with_query_symbol_time_otq_supported,
    is_join_with_snapshot_snapshot_fields_parameter_supported,
)

if TYPE_CHECKING:
    from onetick.py.core.source import Source


def _process_keep_time_param(self: 'Source', keep_time, sub_source):
    if keep_time == "TIMESTAMP":
        raise ValueError("TIMESTAMP is reserved OneTick name, please, specify another one.")
    if keep_time in self.columns():
        raise ValueError(f"{keep_time} column is already presented.")
    sub_source = sub_source.copy()
    if keep_time:
        sub_source[keep_time] = sub_source["Time"]
    return sub_source


def _process_start_or_end_of_jwq(join_params, time, param_name):
    if time is not None:
        if isinstance(time, (datetime, otp.dt)):
            join_params[f"{param_name}"] = ott.datetime2expr(time)
        elif isinstance(time, _Operation):
            join_params[f"{param_name}"] = str(time)
        else:
            raise ValueError(f"{param_name} should be datetime.datetime instance or OneTick expression")


def _columns_to_params_for_joins(columns, query_params=False):
    """
    Converts a dictionary of columns into a parameters string.
    This is mainly used for join_with_query and join_with_collection.

    query_params control whether resulting string should be considered query params or symbol params
    (as it impacts some of the conversion rules)
    """
    params_list = []

    def get_msecs_expression(value):
        return f"tostring(GET_MSECS({str(value)}))"

    for key, value in columns.items():
        dtype = ott.get_object_type(value)
        convert_rule = "'" + key + "=' + "

        if key == '_PARAM_SYMBOL_TIME' and not query_params:
            # this symbol parameter has to be formatted differently because Onetick treats this parameter
            # in a special way
            if dtype is otp.nsectime:
                convert_rule += f'NSECTIME_FORMAT("%Y%m%d%H%M%S.%J",{ott.value2str(value)},_TIMEZONE)'
            elif dtype is str:
                if are_strings(getattr(value, "dtype", None)):
                    convert_rule += str(value)
                else:
                    convert_rule += '"' + value + '"'
            else:
                raise ValueError('Parameter symbol_time has to be a datetime value!')

        elif key == '_SYMBOL_TIME' and query_params:
            # hack to support passing _SYMBOL_TIME to called query as a parameter
            if dtype is otp.nsectime:
                convert_rule += get_msecs_expression(ott.value2str(value))
            elif dtype is str:
                ns = f'PARSE_NSECTIME("%Y%m%d%H%M%S", {ott.value2str(value)}, _TIMEZONE)'
                convert_rule += get_msecs_expression(ns)
            elif dtype is otp.msectime:
                # for backward compatibility
                convert_rule += get_msecs_expression(value)
            else:
                raise ValueError('Parameter symbol_time has to be a datetime value!')

        elif dtype is str:
            if are_strings(getattr(value, "dtype", None)):
                convert_rule += str(value)
            else:
                convert_rule += '"' + value + '"'
        elif dtype is otp.msectime:
            convert_rule += get_msecs_expression(value)
        elif dtype is otp.nsectime:
            if query_params:
                # this can be used for query params but cannot be used for symbol params
                # overall it's better
                convert_rule += "'NSECTIME('+tostring(NSECTIME_TO_LONG(" + str(value) + "))+')'"
            else:
                # this matches the common way onetick converts nanoseconds to symbol parameters
                convert_rule += (
                    get_msecs_expression(value) + "+'.'+SUBSTR(NSECTIME_FORMAT('%J'," + str(value) + ",_TIMEZONE),3,6)"
                )
        else:
            if issubclass(dtype, float) or dtype is otp.decimal:
                warnings.warn(f"Parameter '{key}' is of {dtype} type.\n"
                              "Parameters passed to query will have to be converted to string"
                              " so the precision may be lost (default precision of 8 will be used).\n"
                              "Use other types like integers or strings to pass parameters with higher precision.")
            convert_rule += "tostring(" + ott.value2str(value) + ")"
        params_list.append(convert_rule)
    return "+','+".join(params_list)


def _check_and_convert_symbol(symbol):
    """
    Convert the value of 'symbol' function parameter to symbol name
    OneTick string representation and dictionary of symbol parameters.
    """
    # "symbol" parameter can contain a symbol name (string, field, operation etc),
    # a symbol parameter list (dict, Source, _SymbolParamSource),
    # or both together as a tuple

    symbol_name = None
    symbol_param = {}

    # if "symbol" is tuple, we unpack it
    if isinstance(symbol, tuple) and len(symbol) == 2:
        symbol, symbol_param = symbol

    if isinstance(symbol, _Operation):  # TODO: PY-35
        symbol_name = f"tostring({str(symbol)})"
    elif isinstance(symbol, str):
        symbol_name = f"'{symbol}'"
    elif type(symbol) in {int, float}:  # constant
        symbol_name = f"tostring({symbol})"
    elif symbol is None:
        # this is necessary to distinguish None (which is valid value for symbol) from invalid values
        symbol_name = None
    else:
        if not symbol_param:
            symbol_param = symbol

    return symbol_name, symbol_param


def _convert_symbol_param_and_columns(symbol_param):
    """
    We need to create two objects from a symbol param (a dict, a Source or a _SymbolParamSource):

    1. Dictionary of columns to generate list of symbol parameters for the JOIN_WITH_QUERY EP
    2. _SymbolParamSource object to pass to the source function if necessary
    """

    if isinstance(symbol_param, dict):
        converted_symbol_param_columns = symbol_param
        converted_symbol_param = _SymbolParamSource(
            **{key: ott.get_object_type(column) for key, column in symbol_param.items()}
        )
    elif isinstance(symbol_param, otp.Source):
        converted_symbol_param_columns = {
            field_name: symbol_param[field_name] for field_name in symbol_param.columns(skip_meta_fields=True).keys()
        }
        converted_symbol_param = symbol_param.to_symbol_param()
    elif isinstance(symbol_param, _SymbolParamSource):
        converted_symbol_param_columns = {
            field_name: symbol_param[field_name] for field_name in symbol_param.schema.keys()
        }
        converted_symbol_param = symbol_param
    else:
        return None, None

    # we want to pass all the fields to the joined query as symbol parameters,
    # except for some special fields that would override explicitly set parameters
    ignore_symbol_fields = [
        '_PARAM_START_TIME_NANOS',
        '_PARAM_END_TIME_NANOS',
    ]
    filtered_converted_symbol_param_columns = {}
    for field_name, field_value in converted_symbol_param_columns.items():
        if field_name in ignore_symbol_fields:
            warnings.warn(
                f'Special symbol parameter "{field_name}" was passed to the joined query! '
                'This parameter would be ignored. Please, use parameters of the `join_with_query` '
                'function itself to set it.',
                FutureWarning,
                stacklevel=2,
            )
        else:
            filtered_converted_symbol_param_columns[field_name] = field_value
    filtered_converted_symbol_param = _SymbolParamSource(
        **{
            field_name: field_value
            for field_name, field_value in converted_symbol_param.schema.items()
            if field_name not in ignore_symbol_fields
        }
    )
    return filtered_converted_symbol_param_columns, filtered_converted_symbol_param


def _fill_time_param_for_jwq(join_params, start_time, end_time, timezone):
    _process_start_or_end_of_jwq(join_params, start_time, "start_timestamp")
    _process_start_or_end_of_jwq(join_params, end_time, "end_timestamp")
    if timezone:
        join_params["timezone"] = f"'{timezone}'"
    else:
        join_params["timezone"] = "_TIMEZONE"  # this may break something, need to test


def _fill_aux_params_for_joins(
    join_params, caching, end_time, prefix, start_time, symbol_name, timezone, for_join_with_collection=False
):
    if symbol_name and not for_join_with_collection:
        join_params["symbol_name"] = symbol_name
    if prefix is not None:
        join_params["prefix_for_output_ticks"] = str(prefix)
    if caching:
        if for_join_with_collection:
            supported = ("per_symbol",)
        else:
            supported = ("cross_symbol", "per_symbol")
        if caching in supported:
            join_params["caching_scope"] = caching
        else:
            raise ValueError(f"Unknown value for caching param, please use None or any of {supported}.")
    _fill_time_param_for_jwq(join_params, start_time, end_time, timezone)
    if for_join_with_collection:
        del join_params['timezone']


def _get_default_fields_for_outer_join_str(default_fields_for_outer_join, how, sub_source_schema):
    """
    Default fields for outer join definition.
    Used by join_with_query() and join_with_collection()
    """
    default_fields_for_outer_join_str = ''
    if default_fields_for_outer_join:
        if how != 'outer':
            raise ValueError('The `default_fields_for_outer_join` parameter can be used only for outer join')
        for field, expr in default_fields_for_outer_join.items():
            if field not in sub_source_schema.keys():
                raise KeyError(
                    f'Field {field} is specified in `default_fields_for_outer_join` parameter, '
                    'but is not present in the joined source schema!'
                )
            if default_fields_for_outer_join_str != '':
                default_fields_for_outer_join_str += ','
            default_fields_for_outer_join_str += f'{field}={ott.value2str(expr)}'
    return default_fields_for_outer_join_str


def _get_columns_with_prefix(self: 'Source', sub_source, prefix) -> dict:
    sub_source_columns = sub_source.schema
    if prefix is None:
        prefix = ""
    if not isinstance(prefix, str):
        raise ValueError("Only string constants are supported for now.")
    new_columns = {prefix + name: dtype for name, dtype in sub_source_columns.items()}
    same_names = set(new_columns) & set(self.schema)
    if same_names:
        raise ValueError(f"After applying prefix some columns aren't unique: {', '.join(same_names)}.")
    return new_columns


def join_with_collection(
    self: 'Source',
    collection_name,
    query_func=None,
    how="outer",
    params=None,
    start=None,
    end=None,
    prefix=None,
    caching=None,
    keep_time=None,
    default_fields_for_outer_join=None,
) -> 'Source':
    """
    For each tick uses ``query_func`` to join ticks from ``collection_name`` tick collection
    (tick set, unordered tick set, tick list, or tick deque).

    Parameters
    ----------
    collection_name: str
        Name of the collection state variable from which to join ticks. Collections are the following types:
        :py:class:`TickSet <onetick.py.core._internal._state_objects.TickSet>`,
        :py:class:`TickSetUnordered <onetick.py.core._internal._state_objects.TickSetUnordered>`,
        :py:class:`TickList <onetick.py.core._internal._state_objects.TickList>` and
        :py:class:`TickDeque <onetick.py.core._internal._state_objects.TickDeque>`.

    query_func: callable
        Callable ``query_func`` should return :class:`Source`. If passed, this query will be used on ticks
        from collection before joining them.
        In this case, ``query_func`` object will be evaluated by OneTick (not python)
        for every input tick. Note that python code will be executed only once,
        so all python's conditional expressions will be evaluated only once too.

        Callable should have ``source`` parameter. When callable is called, this parameter
        will have value of a :class:`Source` object representing ticks loaded directly from the collection.
        Any operation applied to this source will be applied to ticks from the collection
        before joining them.

        Also, callable should have the parameters with names
        from ``params`` if they are specified in this method.

        If ``query_func`` is not passed, then all ticks from the collection will be joined.
    how: 'inner', 'outer'
        Type of join. If **inner**, then output tick is propagated
        only if some ticks from the collection were joined to the input tick.
    params: dict
        Mapping of the parameters' names and their values for the ``query_func``.
        :py:class:`Columns <onetick.py.Column>` can be used as a value.
    start: :py:class:`otp.datetime <onetick.py.datetime>`, :py:class:`otp.Operation <onetick.py.Operation>`
        Start time to select ticks from collection.
        If specified, only ticks in collection that have higher or equal timestamp will be processed.
        If not passed, then there will be no lower time bound for the collection ticks.
        This means that even ticks with TIMESTAMP lower than _START_TIME of the main query will be joined.
    end: :py:class:`otp.datetime <onetick.py.datetime>`, :py:class:`otp.Operation <onetick.py.Operation>`
        End time to select ticks from collection.
        If specified, only ticks in collection that have lower timestamp will be processed.
        If not passed, then there will be no upper time bound for the collection ticks.
        This means that even ticks with TIMESTAMP higher than _END_TIME of the main query will be joined.
    prefix : str
        Prefix for the names of joined tick fields.
    caching : str
        If `None` caching is disabled. You can specify caching by using values:

            * 'per_symbol': cache is different for each symbol.
    keep_time : str
        Name for the joined timestamp column. `None` means no timestamp column will be joined.
    default_fields_for_outer_join : dict
        When you use outer join, all output ticks will have fields from the schema of the joined source.
        If nothing was joined to a particular output tick, these fields will have default values for their type.
        This parameter allows to override the values that would be added to ticks for which nothing was joined.
        Dictionary keys should be field names, and dictionary values should be constants
        or :class:`Operation` expressions

    Returns
    -------
    :class:`Source`
            Source with joined ticks from ``collection_name``

    See also
    --------
    **JOIN_WITH_COLLECTION_SUMMARY** OneTick event processor

    Examples
    --------
    >>> # OTdirective: snippet-name: Special functions.join with collection.without query;
    >>> src = otp.Tick(A=1)
    >>> src.state_vars['TICK_SET'] = otp.state.tick_set('LATEST_TICK', 'B', otp.eval(otp.Tick(B=1, C='STR')))
    >>> src = src.join_with_collection('TICK_SET')
    >>> otp.run(src)[["A", "B", "C"]]
        A  B    C
    0  1  1  STR

    >>> # OTdirective: snippet-name: Special functions.join with collection.with query and params;
    >>> src = otp.Ticks(A=[1, 2, 3, 4, 5],
    ...                 B=[2, 2, 3, 3, 3])
    >>> src.state_vars['TICK_LIST'] = otp.state.tick_list()
    >>> def fun(tick): tick.state_vars['TICK_LIST'].push_back(tick)
    >>> src = src.script(fun)
    >>>
    >>> def join_fun(source, param_b):
    ...     source = source.agg(dict(VALUE=otp.agg.sum(source['A'])))
    ...     source['VALUE'] = source['VALUE'] + param_b
    ...     return source
    >>>
    >>> src = src.join_with_collection('TICK_LIST', join_fun, params=dict(param_b=src['B']))
    >>> otp.run(src)[["A", "B", "VALUE"]]
        A  B  VALUE
    0  1  2      3
    1  2  2      5
    2  3  3      9
    3  4  3     13
    4  5  3     18

    Join last standing quote from each exchange to trades:

    >>> # OTdirective: snippet-name: Special functions.join with collection.standing quotes per exchange;
    >>> trd = otp.Ticks(offset=[1000, 2000, 3000, 4000, 5000],
    ...                 PRICE=[10.1, 10.2, 10.15, 10.23, 10.4],
    ...                 SIZE=[100, 50, 100, 60, 200])
    >>>
    >>> qte = otp.Ticks(offset=[500, 600, 1200, 2500, 3500, 3600, 4800],
    ...                 EXCHANGE=['N', 'C', 'Q', 'Q', 'C', 'N', 'C'],
    ...                 ASK_PRICE=[10.2, 10.18, 10.18, 10.15, 10.31, 10.32, 10.44],
    ...                 BID_PRICE=[10.1, 10.17, 10.17, 10.1, 10.23, 10.31, 10.4])
    >>>
    >>> trd['TICK_TYPE'] = 'TRD'
    >>> qte['TICK_TYPE'] = 'QTE'
    >>>
    >>> trd_qte = trd + qte
    >>> trd_qte.state_vars['LAST_QUOTE_PER_EXCHANGE'] = otp.state.tick_set(
    ...     'LATEST', 'EXCHANGE',
    ...     schema=['EXCHANGE', 'ASK_PRICE', 'BID_PRICE'])
    >>>
    >>> trd_qte = trd_qte.state_vars['LAST_QUOTE_PER_EXCHANGE'].update(where=trd_qte['TICK_TYPE'] == 'QTE',
    ...                                                                value_fields=['ASK_PRICE', 'BID_PRICE'])
    >>> trd = trd_qte.where(trd_qte['TICK_TYPE'] == 'TRD')
    >>> trd.drop(['ASK_PRICE', 'BID_PRICE', 'EXCHANGE'], inplace=True)
    >>> trd = trd.join_with_collection('LAST_QUOTE_PER_EXCHANGE')
    >>> otp.run(trd)[['PRICE', 'SIZE', 'EXCHANGE', 'ASK_PRICE', 'BID_PRICE']]
        PRICE  SIZE EXCHANGE  ASK_PRICE  BID_PRICE
    0   10.10   100        N      10.20      10.10
    1   10.10   100        C      10.18      10.17
    2   10.20    50        N      10.20      10.10
    3   10.20    50        C      10.18      10.17
    4   10.20    50        Q      10.18      10.17
    5   10.15   100        N      10.20      10.10
    6   10.15   100        C      10.18      10.17
    7   10.15   100        Q      10.15      10.10
    8   10.23    60        N      10.32      10.31
    9   10.23    60        C      10.31      10.23
    10  10.23    60        Q      10.15      10.10
    11  10.40   200        N      10.32      10.31
    12  10.40   200        C      10.44      10.40
    13  10.40   200        Q      10.15      10.10
    """

    # check that passed collection is good
    if collection_name not in self.state_vars.names:
        raise KeyError(f'Collection with name {collection_name} is not in the list of available state variables')

    if not isinstance(self.state_vars[collection_name], _TickSequence):
        raise ValueError(
            f'State variable {collection_name} is not a tick collection! '
            'Only TickSet, TickSetUnordered, TickList and TickDeque objects are supported '
            'as data sources for join_with_collection'
        )

    if params is None:
        params = {}

    special_params = ('source', '__fixed_start_time', '__fixed_end_time')
    for sp_param in special_params:
        if sp_param in params.keys():
            raise ValueError(
                f'Parameter name "{sp_param}" is special and cannot be used for params '
                'of join_with_collection function. Please, select a different name.'
            )

    # JOIN_WITH_COLLECTION_SUMMARY has START_TIME and END_TIME parameters with the precision of millisecond.
    # So, here we add a workaround on onetick.py side to support nsectime precision
    # "start" and "end" parameters of the EP are kept as they may be necessary
    # for performance reasons

    if start is not None:
        params['__fixed_start_time'] = start
        start = start - otp.Milli(1)

    if end is not None:
        params['__fixed_end_time'] = end
        end = end + otp.Milli(1)

    # prepare temporary file
    # ------------------------------------ #

    # TODO: this should be a common code somewhere
    collection_schema = {
        key: value
        for key, value in self.state_vars[collection_name].schema.items()
        if not self._check_key_is_reserved(key)
    }

    join_source_root = otp.DataSource(
        db=otp.config.default_db, tick_type="ANY", schema_policy="manual", schema=collection_schema,
    )
    if query_func is None:
        query_func = lambda source: source  # noqa

    converted_params = prepare_params(**params)

    fixed_start_time = None
    fixed_end_time = None
    if '__fixed_start_time' in converted_params.keys():
        fixed_start_time = converted_params['__fixed_start_time']
        del converted_params['__fixed_start_time']
    if '__fixed_end_time' in converted_params.keys():
        fixed_end_time = converted_params['__fixed_end_time']
        del converted_params['__fixed_end_time']

    sub_source = query_func(source=join_source_root, **converted_params)

    if fixed_start_time is not None:
        sub_source = sub_source[sub_source['TIMESTAMP'] >= fixed_start_time][0]
    if fixed_end_time is not None:
        sub_source = sub_source[sub_source['TIMESTAMP'] < fixed_end_time][0]

    sub_source = self._process_keep_time_param(keep_time, sub_source)

    params_str = _columns_to_params_for_joins(params, query_params=True)

    sub_source_schema = sub_source.schema.copy()

    columns = {}
    columns.update(self._get_columns_with_prefix(sub_source, prefix))
    columns.update(self.columns(skip_meta_fields=True))

    res = self.copy(columns=columns)

    res._merge_tmp_otq(sub_source)
    query_name = sub_source._store_in_tmp_otq(
        res._tmp_otq, symbols='_NON_EXISTING_SYMBOL_', operation_suffix="join_with_collection"
    )
    # ------------------------------------ #
    default_fields_for_outer_join_str = _get_default_fields_for_outer_join_str(
        default_fields_for_outer_join, how, sub_source_schema
    )

    join_params = dict(
        collection_name=str(self.state_vars[collection_name]),
        otq_query=f'"THIS::{query_name}"',
        join_type=how.upper(),
        otq_query_params=params_str,
        default_fields_for_outer_join=default_fields_for_outer_join_str,
    )

    _fill_aux_params_for_joins(
        join_params, caching, end, prefix, start, symbol_name=None, timezone=None, for_join_with_collection=True
    )
    res.sink(otq.JoinWithCollectionSummary(**join_params))
    res._add_table()
    res.sink(otq.Passthrough(fields="TIMESTAMP", drop_fields=True))

    return res


def join_with_query(
    self: 'Source',
    query,
    how="outer",
    symbol=None,
    params=None,
    start=None,
    end=None,
    timezone=None,
    prefix=None,
    caching=None,
    keep_time=None,
    where=None,
    default_fields_for_outer_join=None,
    symbol_time=None,
    concurrency=None,
    batch_size=None,
    shared_thread_count=None,
    process_query_async: bool = True,
    **kwargs,
) -> 'Source':
    """
    For each tick executes ``query``.

    Parameters
    ----------
    query: callable, Source
        Callable ``query`` should return :class:`Source`. This object will be evaluated by OneTick (not python)
        for every tick. Note python code will be executed only once, so all python's conditional expressions
        will be evaluated only once too.
        Callable should have ``symbol`` parameter and the parameters with names
        from ``params`` if they are specified in this method.

        If ``query`` is a :class:`Source` object then it will be propagated as a query to OneTick.
    how: 'inner', 'outer'
        Type of join. If **inner**, then each tick is propagated
        only if its ``query`` execution has a non-empty result.
    params: dict
        Mapping of the parameters' names and their values for the ``query``.
        :py:class:`Columns <onetick.py.Column>` can be used as a value.
    symbol: str, Operation, dict, Source, or Tuple[Union[str, Operation], Union[dict, Source]]
        Symbol name to use in ``query``. In addition, symbol params can be passed along with symbol name.

        Symbol name can be passed as a string or as an :class:`Operation`.

        Symbol parameters can be passed as a dictionary. Also, the main :class:`Source` object,
        or the object containing a symbol parameter list, can be used as a list of symbol parameter.
        Special symbol parameters (`_PARAM_START_TIME_NANOS` and `_PARAM_END_TIME_NANOS`)
        will be ignored and will not be propagated to ``query``.

        ``symbol`` will be interpreted as a symbol name or as symbol parameters, depending on its type.
        You can pass both as a tuple.

        If symbol name is not passed, then symbol name from the main source is used.
    start: :py:class:`otp.datetime <onetick.py.datetime>`, :py:class:`otp.Operation <onetick.py.Operation>`
        Start time of ``query``.
        By default, start time of the main source is used.
    end: :py:class:`otp.datetime <onetick.py.datetime>`, :py:class:`otp.Operation <onetick.py.Operation>`
        End time of ``query`` (note that it's non-inclusive).
        By default, end time of the main source is used.
    start_time:
        .. deprecated:: 1.48.4
            The same as ``start``.
    end_time:
        .. deprecated:: 1.48.4
            The same as ``end``.
    timezone : Optional, str
        Timezone of ``query``.
        By default, timezone of the main source is used.
    prefix : str
        Prefix for the names of joined tick fields.
    caching : str
        If `None` caching is disabled (default). You can specify caching by using values:

            * 'cross_symbol': cache is the same for all symbols

            * 'per_symbol': cache is different for each symbol.

        .. note::
            When parameter ``process_query_async`` is set to ``True`` (default), caching may work
            unexpectedly, because ticks will be accumulated in batches and ``query`` will be processed
            in different threads.
    keep_time : str
        Name for the joined timestamp column. `None` means no timestamp column will be joined.
    where : Operation
        Condition to filter ticks for which the result of the ``query`` will be joined.
    default_fields_for_outer_join : dict
        When you use outer join, all output ticks will have fields from the schema of the joined source.
        If nothing was joined to a particular output tick, these fields will have default values for their type.
        This parameter allows to override the values that would be added to ticks for which nothing was joined.
        Dictionary keys should be field names, and dictionary values should be constants
        or :class:`Operation` expressions
    symbol_time : :py:class:`otp.datetime <onetick.py.datetime>`, :py:class:`otp.Operation <onetick.py.Operation>`
        Time that will be used by Onetick to map the symbol with which ``query`` is executed to the reference data.
        This parameter is only necessary if the query is expected to perform symbology conversions.
    concurrency: int
        Specifies concurrency for the joined ``query`` execution.
        Default is 1 (no concurrency).
    batch_size: int
        Specifies batch size for the joined ``query`` execution. Default is 0.
    shared_thread_count: int
        Specifies number of threads for asynchronous processing of ``query`` per unbound symbol list.
        By default, the number of threads is 1.
    process_query_async: bool
        Switches between synchronous and asynchronous execution of queries.

        While asynchronous execution is generally much more effective,
        in certain cases synchronous execution may still be preferred
        (e.g., when there are a few input ticks, each initiating a memory-consuming query).

        In asynchronous mode typically while parallel thread is processing the query,
        EP accumulates some input ticks.


    Returns
    -------
    :class:`Source`
            Source with joined ticks from ``query``

    See also
    --------
    **JOIN_WITH_QUERY** OneTick event processor

    Examples
    --------
    >>> # OTdirective: snippet-name: Special functions.join with query.with an otp data source;
    >>> d = otp.Ticks(Y=[-1])
    >>> d = d.update(dict(Y=1), where=(d.Symbol.name == "a"))
    >>> data = otp.Ticks(X=[1, 2],
    ...                  S=["a", "b"])
    >>> res = data.join_with_query(d, how='inner', symbol=data['S'])
    >>> otp.run(res)[["X", "Y", "S"]]
       X  Y  S
    0  1  1  a
    1  2 -1  b

    >>> d = otp.Ticks(ADDED=[-1])
    >>> d = d.update(dict(ADDED=1), where=(d.Symbol.name == "3"))  # symbol name is always string
    >>> data = otp.Ticks(A=[1, 2], B=[2, 4])
    >>> res = data.join_with_query(d, how='inner', symbol=(data['A'] + data['B']))  # OTdirective: skip-snippet:;
    >>> df = otp.run(res)
    >>> df[["A", "B", "ADDED"]]
       A  B  ADDED
    0  1  2      1
    1  2  4     -1

    Constants as symbols are also supported:

    >>> d = otp.Ticks(ADDED=[d.Symbol.name])
    >>> data = otp.Ticks(A=[1, 2], B=[2, 4])
    >>> res = data.join_with_query(d, how='inner', symbol=1)    # OTdirective: skip-snippet:;
    >>> df = otp.run(res)
    >>> df[["A", "B", "ADDED"]]
       A  B ADDED
    0  1  2     1
    1  2  4     1

    Function object as query is also supported (Note it will be executed only once in python's code):

    >>> def func(symbol):
    ...     d = otp.Ticks(TYPE=["six"])
    ...     d = d.update(dict(TYPE="three"), where=(symbol.name == "3"))  # symbol is always converted to string
    ...     d["TYPE"] = symbol['PREF'] + d["TYPE"] + symbol['POST']
    ...     return d
    >>> # OTdirective: snippet-name: Special functions.join with query.with a function
    >>> data = otp.Ticks(A=[1, 2], B=[2, 4])
    >>> res = data.join_with_query(func, how='inner', symbol=(data['A'] + data['B'], dict(PREF="_", POST="$")))
    >>> df = otp.run(res)
    >>> df[["A", "B", "TYPE"]]
       A  B     TYPE
    0  1  2  _three$
    1  2  4    _six$

    It's possible to pass the source itself as a list of symbol parameters, which will make all of its fields
    accessible through the "symbol" object:

    >>> def func(symbol):
    ...     d = otp.Ticks(TYPE=["six"])
    ...     d["TYPE"] = symbol['PREF'] + d["TYPE"] + symbol['POST']
    ...     return d
    >>> # OTdirective: snippet-name: 'Source' operations.join with query.source as symbol;
    >>> data = otp.Ticks(A=[1, 2], B=[2, 4], PREF=["_", "$"], POST=["$", "_"])
    >>> res = data.join_with_query(func, how='inner', symbol=data)
    >>> df = otp.run(res)
    >>> df[["A", "B", "TYPE"]]
       A  B   TYPE
    0  1  2  _six$
    1  2  4  $six_

    The examples above can be rewritten by using onetick query parameters instead of symbol parameters.
    OTQ parameters are global for query, while symbol parameters can be redefined by bound symbols:

    >>> def func(symbol, pref, post):
    ...     d = otp.Ticks(TYPE=["six"])
    ...     d = d.update(dict(TYPE="three"), where=(symbol.name == "3"))  # symbol is always converted to string
    ...     d["TYPE"] = pref + d["TYPE"] + post
    ...     return d
    >>> # OTdirective: snippet-name: Special functions.join with query.with a function that takes params;
    >>> data = otp.Ticks(A=[1, 2], B=[2, 4])
    >>> res = data.join_with_query(func, how='inner', symbol=(data['A'] + data['B']),
    ...                            params=dict(pref="_", post="$"))
    >>> df = otp.run(res)
    >>> df[["A", "B", "TYPE"]]
       A  B     TYPE
    0  1  2  _three$
    1  2  4    _six$

    Some or all onetick query parameters can be column or expression also:

    >>> def func(symbol, pref, post):
    ...     d = otp.Ticks(TYPE=["six"])
    ...     d = d.update(dict(TYPE="three"), where=(symbol.name == "3"))  # symbol is always converted to string
    ...     d["TYPE"] = pref + d["TYPE"] + post
    ...     return d
    >>> # OTdirective: snippet-name: Special functions.join with query.with a function that takes params from fields;   # noqa
    >>> data = otp.Ticks(A=[1, 2], B=[2, 4], PREF=["^", "_"], POST=["!", "$"])
    >>> res = data.join_with_query(func, how='inner', symbol=(data['A'] + data['B']),
    ...                            params=dict(pref=data["PREF"] + ".", post=data["POST"]))
    >>> df = otp.run(res)
    >>> df[["A", "B", "TYPE"]]
       A  B      TYPE
    0  1  2  ^.three!
    1  2  4    _.six$

    You can specify ``start`` and ``end`` time of the query, otherwise time interval of the main query will be used:

    >>> # OTdirective: snippet-name: Special functions.join with query.passing start/end times;
    >>> d = otp.Ticks(Y=[1, 2])
    >>> data = otp.Ticks(X=[1, 2])
    >>> start = otp.datetime(2003, 12, 1, 0, 0, 0, 1000)
    >>> end = otp.datetime(2003, 12, 1, 0, 0, 0, 3000)
    >>> res = data.join_with_query(d, how='inner', start=start, end=end)
    >>> otp.run(res)
                         Time  Y  X
    0 2003-12-01 00:00:00.000  1  1
    1 2003-12-01 00:00:00.000  2  1
    2 2003-12-01 00:00:00.001  1  2
    3 2003-12-01 00:00:00.001  2  2

    By default joined query inherits start and end time from the main query:

    >>> joined_query = otp.Tick(JOINED_START_TIME=otp.meta_fields.start_time,
    ...                         JOINED_END_TIME=otp.meta_fields.end_time)
    >>> main_query = otp.Tick(A=1)
    >>> data = main_query.join_with_query(joined_query)
    >>> otp.run(data, start=otp.dt(2003, 12, 1), end=otp.dt(2003, 12, 4))
            Time JOINED_START_TIME JOINED_END_TIME  A
    0 2003-12-01        2003-12-01      2003-12-04  1

    Parameters ``start`` and ``end`` can be used to change time interval for the joined query:

    >>> data = main_query.join_with_query(joined_query, start=otp.dt(2024, 1, 1), end=otp.dt(2024, 1, 3))
    >>> otp.run(data, start=otp.dt(2003, 12, 1), end=otp.dt(2003, 12, 4))
            Time JOINED_START_TIME JOINED_END_TIME  A
    0 2003-12-01        2024-01-01      2024-01-03  1

    Note that query ``start`` time is inclusive, but query ``end`` time is not,
    meaning that ticks with timestamps equal to the query end time will not be included:

    >>> main_query = otp.Tick(A=1)
    >>> joined_query = otp.Tick(DAY=0, bucket_interval=24*60*60)
    >>> joined_query['DAY'] = joined_query['TIMESTAMP'].dt.day_of_month()
    >>> otp.run(joined_query, start=otp.dt(2003, 12, 1), end=otp.dt(2003, 12, 5))
            Time  DAY
    0 2003-12-01    1
    1 2003-12-02    2
    2 2003-12-03    3
    3 2003-12-04    4

    >>> joined_query = joined_query.last()
    >>> data = main_query.join_with_query(joined_query,
    ...                                   start=otp.dt(2003, 12, 1), end=otp.dt(2003, 12, 4))
    >>> otp.run(data)
            Time  DAY  A
    0 2003-12-01    3  1

    If you want to include such ticks, you can add one nanosecond to the query end time:

    >>> data = main_query.join_with_query(joined_query,
    ...                                   start=otp.dt(2003, 12, 1), end=otp.dt(2003, 12, 4) + otp.Nano(1))
    >>> otp.run(data)
            Time  DAY  A
    0 2003-12-01    4  1

    Use ``keep_time`` parameter to keep or rename original timestamp column:

    >>> # OTdirective: snippet-name: Special functions.join with query.keep the timestamps of the joined ticks;
    >>> d = otp.Ticks(Y=[1, 2])
    >>> data = otp.Ticks(X=[1, 2])
    >>> res = data.join_with_query(d, how='inner', keep_time="ORIG_TIME")
    >>> otp.run(res)
                         Time  Y               ORIG_TIME  X
    0 2003-12-01 00:00:00.000  1 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.000  2 2003-12-01 00:00:00.001  1
    2 2003-12-01 00:00:00.001  1 2003-12-01 00:00:00.000  2
    3 2003-12-01 00:00:00.001  2 2003-12-01 00:00:00.001  2
    """

    # TODO: check if join_with_query checks schema of joined source against primary source,
    # by itself or with process_by_group

    if params is None:
        params = {}

    converted_symbol_name, symbol_param = _check_and_convert_symbol(symbol)

    # default symbol name should be this: _SYMBOL_NAME if it is not empty else _NON_EXISTING_SYMBOL_
    # this way we will force JWQ to substitute symbol with any symbol parameters we may have passed
    # otherwise (if an empty symbol name is passed to JWQ), it will not substitute either symbol name
    # or symbol parameters, and so symbol parameters may get lost
    # see BDS-263
    if converted_symbol_name is None:
        converted_symbol_name = "CASE(_SYMBOL_NAME,'','_NON_EXISTING_SYMBOL',_SYMBOL_NAME)"

    converted_symbol_param_columns, converted_symbol_param = _convert_symbol_param_and_columns(symbol_param)
    if converted_symbol_param is None:
        # we couldn't interpret "symbols" as either symbol name or symbol parameters
        raise ValueError(
            '"symbol" parameter has a wrong format! It should be a symbol name, a symbol parameter '
            'object (dict or Source), or a tuple containing both'
        )

    if '_PARAM_SYMBOL_TIME' in converted_symbol_param_columns.keys():
        warnings.warn(
            '"_PARAM_SYMBOL_TIME" explicitly passed among join_with_query symbol parameters! '
            'This is deprecated - please use symbol_time parameter instead.',
            FutureWarning,
            stacklevel=2,
        )
    if '_SYMBOL_TIME' in params.keys():
        warnings.warn(
            'Query parameter "_SYMBOL_TIME" passed to join_with_query! '
            'This is deprecated. Please use a dedicated `symbol_time` parameter.',
            FutureWarning,
            stacklevel=2,
        )

    # prepare temporary file
    # ------------------------------------ #
    converted_params = prepare_params(**params)

    if isinstance(query, otp.Source):
        sub_source = query
    else:
        # inspect function
        # -------
        sig = inspect.signature(query)
        if "symbol" in sig.parameters:
            if "symbol" in converted_params.keys():
                raise AttributeError(
                    '"params" contains key "symbol", which is reserved for symbol parameters. '
                    'Please, rename this parameter to another name'
                )
            converted_params["symbol"] = converted_symbol_param  # type: ignore
        sub_source = query(**converted_params)

    sub_source = self._process_keep_time_param(keep_time, sub_source)

    if not sub_source._is_unbound_required():
        sub_source += otp.Empty()

    # adding symbol time
    if symbol_time is not None:
        if ott.get_object_type(symbol_time) is not otp.nsectime and ott.get_object_type(symbol_time) is not str:
            raise ValueError(
                f'Parameter of type {ott.get_object_type(symbol_time)} passed as symbol_time! '
                'This parameter only supports datetime values or strings'
            )
        if is_join_with_query_symbol_time_otq_supported():
            params = params.copy()
            params['_SYMBOL_TIME'] = symbol_time
        else:
            converted_symbol_param_columns['_PARAM_SYMBOL_TIME'] = symbol_time

    params_str = _columns_to_params_for_joins(params, query_params=True)
    symbol_params_str = _columns_to_params_for_joins(converted_symbol_param_columns)

    sub_source_schema = sub_source.schema.copy()

    columns = {}
    columns.update(self._get_columns_with_prefix(sub_source, prefix))
    columns.update(self.columns(skip_meta_fields=True))

    otq_properties = {}
    if concurrency is not None:
        if not isinstance(concurrency, int) or concurrency <= 0:
            raise ValueError(f"Parameter 'concurrency' should be a positive integer, got {concurrency}")
        otq_properties['concurrency'] = concurrency

    if batch_size is not None:
        if not isinstance(batch_size, int) or batch_size < 0:
            raise ValueError(f"Parameter 'batch_size' should be a non-negative integer, got {batch_size}")
        otq_properties['batch_size'] = batch_size

    res = self.copy(columns=columns)

    res._merge_tmp_otq(sub_source)
    query_name = sub_source._store_in_tmp_otq(
        res._tmp_otq, symbols='_NON_EXISTING_SYMBOL_', operation_suffix="join_with_query",
        **otq_properties,
    )  # TODO: combine with _convert_symbol_to_string
    # ------------------------------------ #

    if where is not None and how != 'outer':
        raise ValueError('The `where` parameter can be used only for outer join')

    default_fields_for_outer_join_str = _get_default_fields_for_outer_join_str(
        default_fields_for_outer_join, how, sub_source_schema
    )

    join_params = dict(
        otq_query=f'"THIS::{query_name}"',
        join_type=how.upper(),
        otq_query_params=params_str,
        symbol_params=symbol_params_str,
        where=str(where._make_python_way_bool_expression()) if where is not None else '',
        default_fields_for_outer_join=default_fields_for_outer_join_str,
        process_query_asynchronously=process_query_async,
    )
    if shared_thread_count is not None:
        if not isinstance(shared_thread_count, int) or shared_thread_count <= 0:
            raise ValueError("Parameter 'shared_thread_count' should be a positive integer")
        join_params['shared_thread_count'] = shared_thread_count

    start_time = kwargs.get('start_time', start)
    end_time = kwargs.get('end_time', end)
    _fill_aux_params_for_joins(join_params, caching, end_time, prefix, start_time, converted_symbol_name, timezone)
    res.sink(otq.JoinWithQuery(**join_params))
    res._add_table()
    res.sink(otq.Passthrough(fields="TIMESTAMP", drop_fields=True))

    return res


def point_in_time(
    self: 'Source',
    source: Union['Source', str],
    offsets: List[int],
    offset_type: Literal['time_msec', 'num_ticks'] = 'time_msec',
    input_ts_fields_to_propagate: Optional[List[str]] = None,
    symbol_date=None,
) -> 'Source':
    """
    This method joins ticks from current source with the ticks from another ``source``.

    Joined ticks are those that are offset by
    the specified number of milliseconds or by the specified number of ticks
    relative to the current source's tick timestamp.

    Output tick may be generated for each specified offset, so this method may output several ticks for each input tick.

    If another ``source`` doesn't have a tick with specified offset, then output tick is not generated.

    Fields **TICK_TIME** and **OFFSET** are also added to the output ticks,
    specifying original timestamp of the joined tick and the offset that it was specified to join by.

    Note
    ----
    In order for this method to have reasonable performance,
    the set of input ticks' timestamps has to be relatively small.

    In other words, the points in time, which the user is interested in,
    have to be quite few in order usage of this method to be justified.

    Parameters
    ----------
    source: :class:`Source` or str
        The source from which the data will be joined or the string with the path to the .otq file
        (note that in the latter case schema can not be updated automatically with the fields from the joined query).
    offsets:
        List of integers specifying offsets for each timestamp.
    offset_type: 'time_msec' or 'num_ticks'
        The type of offset: number of milliseconds or the number of ticks.
    input_ts_fields_to_propagate:
        The list of fields to propagate from the current source.
        By default no fields (except **TIMESTAMP**) are propagated.
    symbol_date: :py:class:`otp.datetime <onetick.py.datetime>`
        Symbol date that will be set for the ``source`` inner query.

    See also
    --------
    | **POINT_IN_TIME** OneTick event processor
    | :func:`onetick.py.PointInTime`
    | :func:`onetick.py.join_by_time`

    Examples
    --------

    Quotes and trades for testing:

    .. testcode::

       qte = otp.Ticks(ASK_PRICE=[20, 21, 22, 23, 24, 25], BID_PRICE=[20, 21, 22, 23, 24, 25])
       print(otp.run(qte))

    .. testoutput::

                            Time  ASK_PRICE  BID_PRICE
       0 2003-12-01 00:00:00.000         20         20
       1 2003-12-01 00:00:00.001         21         21
       2 2003-12-01 00:00:00.002         22         22
       3 2003-12-01 00:00:00.003         23         23
       4 2003-12-01 00:00:00.004         24         24
       5 2003-12-01 00:00:00.005         25         25

    .. testcode::

       trd = otp.Ticks(PRICE=[1, 3, 5], SIZE=[100, 300, 500], offset=[1, 3, 5])
       print(otp.run(trd))

    .. testoutput::

                            Time  PRICE  SIZE
       0 2003-12-01 00:00:00.001      1   100
       1 2003-12-01 00:00:00.003      3   300
       2 2003-12-01 00:00:00.005      5   500

    Joining each quote with first trade with equal or less timestamp:

    .. testcode::
       :skipif: not is_supported_point_in_time()

       data = qte.point_in_time(trd, offsets=[0])
       print(otp.run(data))

    .. testoutput::

                            Time  PRICE  SIZE               TICK_TIME  OFFSET
       0 2003-12-01 00:00:00.001      1   100 2003-12-01 00:00:00.001       0
       1 2003-12-01 00:00:00.002      1   100 2003-12-01 00:00:00.001       0
       2 2003-12-01 00:00:00.003      3   300 2003-12-01 00:00:00.003       0
       3 2003-12-01 00:00:00.004      3   300 2003-12-01 00:00:00.003       0
       4 2003-12-01 00:00:00.005      5   500 2003-12-01 00:00:00.005       0

    By default fields from the current source are not propagated,
    use parameter ``input_ts_fields_to_propagate`` to add them to the output:

    .. testcode::
       :skipif: not is_supported_point_in_time()

       data = qte.point_in_time(trd, offsets=[0], input_ts_fields_to_propagate=['ASK_PRICE', 'BID_PRICE'])
       print(otp.run(data))

    .. testoutput::

                            Time  ASK_PRICE  BID_PRICE  PRICE  SIZE               TICK_TIME  OFFSET
       0 2003-12-01 00:00:00.001         21         21      1   100 2003-12-01 00:00:00.001       0
       1 2003-12-01 00:00:00.002         22         22      1   100 2003-12-01 00:00:00.001       0
       2 2003-12-01 00:00:00.003         23         23      3   300 2003-12-01 00:00:00.003       0
       3 2003-12-01 00:00:00.004         24         24      3   300 2003-12-01 00:00:00.003       0
       4 2003-12-01 00:00:00.005         25         25      5   500 2003-12-01 00:00:00.005       0

    Note that first quote was not propagated, because it doesn't have corresponding trade.

    Offset may be positive or negative.
    If several offsets are specified, several output ticks may be generated for a single input tick:

    .. testcode::
       :skipif: not is_supported_point_in_time()

       data = qte.point_in_time(trd, offsets=[0, 1], input_ts_fields_to_propagate=['ASK_PRICE', 'BID_PRICE'])
       print(otp.run(data))

    .. testoutput::

                             Time  ASK_PRICE  BID_PRICE  PRICE  SIZE               TICK_TIME  OFFSET
       0  2003-12-01 00:00:00.000         20         20      1   100 2003-12-01 00:00:00.001       1
       1  2003-12-01 00:00:00.001         21         21      1   100 2003-12-01 00:00:00.001       0
       2  2003-12-01 00:00:00.001         21         21      1   100 2003-12-01 00:00:00.001       1
       3  2003-12-01 00:00:00.002         22         22      1   100 2003-12-01 00:00:00.001       0
       4  2003-12-01 00:00:00.002         22         22      3   300 2003-12-01 00:00:00.003       1
       5  2003-12-01 00:00:00.003         23         23      3   300 2003-12-01 00:00:00.003       0
       6  2003-12-01 00:00:00.003         23         23      3   300 2003-12-01 00:00:00.003       1
       7  2003-12-01 00:00:00.004         24         24      3   300 2003-12-01 00:00:00.003       0
       8  2003-12-01 00:00:00.004         24         24      5   500 2003-12-01 00:00:00.005       1
       9  2003-12-01 00:00:00.005         25         25      5   500 2003-12-01 00:00:00.005       0
       10 2003-12-01 00:00:00.005         25         25      5   500 2003-12-01 00:00:00.005       1

    By default the number of milliseconds is used as an offset.
    You can also specify the number of ticks as an offset:

    .. testcode::
       :skipif: not is_supported_point_in_time()

       data = qte.point_in_time(trd, offset_type='num_ticks', offsets=[-1, 1],
                                input_ts_fields_to_propagate=['ASK_PRICE', 'BID_PRICE'])
       print(otp.run(data))

    .. testoutput::

                            Time  ASK_PRICE  BID_PRICE  PRICE  SIZE               TICK_TIME  OFFSET
       0 2003-12-01 00:00:00.000         20         20      1   100 2003-12-01 00:00:00.001       1
       1 2003-12-01 00:00:00.001         21         21      3   300 2003-12-01 00:00:00.003       1
       2 2003-12-01 00:00:00.002         22         22      3   300 2003-12-01 00:00:00.003       1
       3 2003-12-01 00:00:00.003         23         23      1   100 2003-12-01 00:00:00.001      -1
       4 2003-12-01 00:00:00.003         23         23      5   500 2003-12-01 00:00:00.005       1
       5 2003-12-01 00:00:00.004         24         24      1   100 2003-12-01 00:00:00.001      -1
       6 2003-12-01 00:00:00.004         24         24      5   500 2003-12-01 00:00:00.005       1
       7 2003-12-01 00:00:00.005         25         25      3   300 2003-12-01 00:00:00.003      -1
    """
    if not is_supported_point_in_time():
        raise RuntimeError('PointInTime event processor is not supported on this OneTick version')

    res = self.copy()

    if offset_type not in ('time_msec', 'num_ticks'):
        raise ValueError(f"Wrong value for parameter 'offset_type': {offset_type}")

    if isinstance(source, str):
        otq_query = source
    else:
        query_name = source._store_in_tmp_otq(
            res._tmp_otq,
            operation_suffix='point_in_time',
            # set default symbol, even if it's not set by user, symbol's value doesn't matter in this case
            symbols=otp.config.get('default_symbol', 'ANY'),
            symbol_date=symbol_date,
        )
        otq_query = f'THIS::{query_name}'

    input_ts_fields_to_propagate = input_ts_fields_to_propagate or []

    pit_params = dict(
        otq_query=otq_query,
        offset_type=offset_type.upper(),
        offsets=','.join(map(str, offsets)),
        input_ts_fields_to_propagate=','.join(map(str, input_ts_fields_to_propagate)),
    )
    res.sink(otq.PointInTime(**pit_params))

    schema = {}
    if input_ts_fields_to_propagate:
        schema = {
            k: v for k, v in res.schema.items()
            if k in input_ts_fields_to_propagate
        }
    res.schema.set(**schema)
    if not isinstance(source, str):
        res.schema.update(**source.schema)
    res.schema.update(**{
        'TICK_TIME': otp.nsectime,
        'OFFSET': int,
    })
    return res


def join_with_snapshot(
    self: 'Source',
    snapshot_name='VALUE',
    snapshot_storage='memory',
    allow_snapshot_absence=False,
    join_keys=None,
    symbol_name_in_snapshot=None,
    database='',
    default_fields_for_outer_join=None,
    prefix_for_output_ticks='',
    snapshot_fields=None,
):
    """
    Saves last (at most) `n` ticks of each group of ticks from the input time series in global storage or
    in a memory mapped file under a specified snapshot name.
    Tick descriptor should be the same for all ticks saved into the snapshot.
    These ticks can then be read via :py:class:`ReadSnapshot <onetick.py.ReadSnapshot>` by using the name
    of the snapshot and the same symbol name (``<db_name>::<symbol>``) that were used by this method.

    .. warning::
        You should update schema manually, if you want to use fields from snapshot in `onetick-py` query description
        before its execution.

        That's due to the fact, that `onetick-py` can't identify a schema of data in a snapshot before making a query.

        If you set ``default_fields_for_outer_join`` parameter, schema will be guessed from default fields values.

    Parameters
    ----------
    snapshot_name: str
        The name that was specified in :py:meth:`onetick.py.Source.save_snapshot` as a ``snapshot_name`` during saving.

        Default: `VALUE`
    snapshot_storage: str
        This parameter specifies the place of storage of the snapshot. Possible options are:

        * `memory` - the snapshot is stored in the dynamic (heap) memory of the process
          that ran (or is still running) the :py:meth:`onetick.py.Source.save_snapshot` for the snapshot.
        * `memory_mapped_file` - the snapshot is stored in a memory mapped file.
          For each symbol to get the location of the snapshot in the file system, ``join_with_snapshot`` looks at
          the **SAVE_SNAPSHOT_DIR** parameter value in the locator section for the database of the symbol.
          In a specified directory it creates a new directory with the name of the snapshot and keeps
          the memory mapped file and some other helper files there.

        Default: `memory`
    allow_snapshot_absence: bool
        If specified, the EP does not display an error about missing snapshot
        if the snapshot has not been saved or is still being saved.

        Default: `False`
    join_keys: list, optional
        A list of names of attributes. A non-empty list causes input ticks to be joined only if all of them
        have matching values for all specified attributes.
        Currently, these fields need to match with ``group_by`` fields of the corresponding snapshot.
    symbol_name_in_snapshot: str, :class:`~onetick.py.Column` or :class:`~onetick.py.Operation`, optional
        Expression that evaluates to a string containing symbol name.
        Specified expression is reevaluated upon the arrival of each tick.
        If this parameter is empty, the input symbol name is used.
    database: str, optional
        The database to read the snapshot. If not specified database from the symbol is used.
    default_fields_for_outer_join: dict, optional
        A `dict` with field name as key and value, :class:`~onetick.py.Column` or :class:`~onetick.py.Operation`,
        which specifies the names and the values of the fields (also, optionally, the field type),
        used to form ticks to be joined with unmatched input ticks.

        If you want to specify field type, pass tuple of field dtype and expression or value as dict item value.

        This parameter is reevaluated upon the arrival of each tick.

        It's also used for auto detecting snapshot schema for using fields from snapshot
        while building query via ``ontick-py``.
    prefix_for_output_ticks: str
        The prefix for the names of joined tick fields.

        Default: `empty string`
    snapshot_fields: List[str], None
        Specifies list of fields from the snapshot to join with input ticks. When empty, all fields are included.

    See also
    --------
    | **JOIN_WITH_SNAPSHOT** OneTick event processor
    | :py:class:`onetick.py.ReadSnapshot`
    | :py:class:`onetick.py.ShowSnapshotList`
    | :py:class:`onetick.py.FindSnapshotSymbols`
    | :py:meth:`onetick.py.Source.save_snapshot`

    Examples
    --------
    Simple ticks join with snapshot:

    >>> src = otp.Ticks(A=[1, 2])
    >>> src = src.join_with_snapshot(snapshot_name='some_snapshot')  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP
                         Time  A  X  Y               TICK_TIME
    0 2003-12-01 00:00:00.000  1  1  4 2003-12-01 00:00:00.000
    1 2003-12-01 00:00:00.000  1  2  5 2003-12-01 00:00:00.001
    2 2003-12-01 00:00:00.001  2  1  4 2003-12-01 00:00:00.000
    3 2003-12-01 00:00:00.001  2  2  5 2003-12-01 00:00:00.001

    Add prefix ``T.`` for fields from snapshot:

    >>> src = otp.Ticks(A=[1, 2])
    >>> src = src.join_with_snapshot(
    ...     snapshot_name='some_snapshot', prefix_for_output_ticks='T.',
    ... )  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP
                         Time  A  T.X  T.Y             T.TICK_TIME
    0 2003-12-01 00:00:00.000  1    1    4 2003-12-01 00:00:00.000
    1 2003-12-01 00:00:00.000  1    2    5 2003-12-01 00:00:00.001
    2 2003-12-01 00:00:00.001  2    1    4 2003-12-01 00:00:00.000
    3 2003-12-01 00:00:00.001  2    2    5 2003-12-01 00:00:00.001

    To get only specific fields from snapshot use parameter ``snapshot_fields``:

    >>> src = otp.Ticks(A=[1, 2])
    >>> src = src.join_with_snapshot(
    ...     snapshot_name='some_snapshot', snapshot_fields=['Y'],
    ... )  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP
                         Time  A  Y
    0 2003-12-01 00:00:00.000  1  4
    1 2003-12-01 00:00:00.000  1  5
    2 2003-12-01 00:00:00.001  2  4
    3 2003-12-01 00:00:00.001  2  5

    Setting default values for snapshot fields for outer join via ``default_fields_for_outer_join_with_types``
    parameter with example of joining ticks with absent snapshot:

    >>> src = otp.Ticks(A=[1, 2])
    >>> src = src.join_with_snapshot(
    ...     snapshot_name='some_snapshot', allow_snapshot_absence=True,
    ...     default_fields_for_outer_join={
    ...         'B': 'Some string',
    ...         'C': (float, src['A'] * 2),
    ...         'D': 50,
    ...     },
    ... )  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP
                         Time  A            B    C     D
    0 2003-12-01 00:00:00.000  1  Some string  2.0  50.0
    1 2003-12-01 00:00:00.001  2  Some string  2.0  50.0

    In this case, schema for ``src`` object will be automatically detected from values for this parameter:

    >>> src.schema  # doctest: +SKIP
    {'A': <class 'int'>, 'B': <class 'str'>, 'C': <class 'float'>, 'D': <class 'int'>}


    You can join ticks from snapshot for each input tick for specified symbol name from string value or this tick
    via ``symbol_name_in_snapshot`` parameter.

    Let's create snapshot with different symbol names inside:

    >>> src = otp.Ticks(X=[1, 2, 3, 4], Y=['AAA', 'BBB', 'CCC', 'AAA'])
    >>> src = src.save_snapshot(
    ...     snapshot_name='some_snapshot', num_ticks=5, keep_snapshot_after_query=True, symbol_name_field='Y',
    ... )
    >>> otp.run(src)  # doctest: +SKIP

    Now we can join input only with ticks from snapshot with specified symbol name:

    >>> src = otp.Ticks(A=[1, 2])
    >>> src = src.join_with_snapshot(
    ...     snapshot_name='some_snapshot', symbol_name_in_snapshot='AAA',
    ... )  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP
                         Time  A  X               TICK_TIME
    0 2003-12-01 00:00:00.000  1  1 2003-12-01 00:00:00.000
    1 2003-12-01 00:00:00.000  1  4 2003-12-01 00:00:00.003
    2 2003-12-01 00:00:00.001  2  1 2003-12-01 00:00:00.000
    3 2003-12-01 00:00:00.001  2  4 2003-12-01 00:00:00.003

    Or we can join each tick with ticks from snapshot with symbol name from input ticks field:

    >>> src = otp.Ticks(A=[1, 2], SYM=['AAA', 'CCC'])
    >>> src = src.join_with_snapshot(
    ...     snapshot_name='some_snapshot', symbol_name_in_snapshot=src['SYM'],
    ... )  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP
                         Time  A  SYM  X               TICK_TIME
    0 2003-12-01 00:00:00.000  1  AAA  1 2003-12-01 00:00:00.000
    1 2003-12-01 00:00:00.000  1  AAA  4 2003-12-01 00:00:00.003
    2 2003-12-01 00:00:00.001  2  CCC  3 2003-12-01 00:00:00.002
    """
    kwargs = {}

    if not hasattr(otq, "JoinWithSnapshot"):
        raise RuntimeError("Current version of OneTick doesn't support JOIN_WITH_SNAPSHOT EP")

    if snapshot_storage not in ['memory', 'memory_mapped_file']:
        raise ValueError('`snapshot_storage` must be one of "memory", "memory_mapped_file"')

    is_snapshot_fields_param_supported = is_join_with_snapshot_snapshot_fields_parameter_supported()

    if snapshot_fields and not is_snapshot_fields_param_supported:
        raise RuntimeError(
            "Current version of OneTick doesn't support `snapshot_fields` parameter on JOIN_WITH_SNAPSHOT EP"
        )

    snapshot_storage = snapshot_storage.upper()

    if join_keys is None:
        join_keys_str = ''
    else:
        join_keys_str = ','.join(join_keys)

    if symbol_name_in_snapshot is None:
        symbol_name_in_snapshot = ''
    elif isinstance(symbol_name_in_snapshot, _Operation):
        symbol_name_in_snapshot = str(symbol_name_in_snapshot)

    if default_fields_for_outer_join is None:
        default_fields_for_outer_join = {}

    default_fields_list = []
    snapshot_schema = {}

    for field_name, field_value in default_fields_for_outer_join.items():
        if isinstance(field_value, tuple):
            field_type = field_value[0]

            default_fields_list.append(
                f'{field_name} {ott.type2str(field_type)} = {ott.value2str(field_value[1])}',
            )
        else:
            if isinstance(field_value, _Operation):
                field_type = field_value.dtype
            else:
                field_type = type(field_value)

            default_fields_list.append(f'{field_name} = {ott.value2str(field_value)}')

        snapshot_schema[f'{prefix_for_output_ticks}{field_name}'] = field_type

    default_fields_str = ','.join(default_fields_list)

    if snapshot_fields is not None:
        kwargs['snapshot_fields'] = ','.join(snapshot_fields)

    self.sink(
        otq.JoinWithSnapshot(
            snapshot_name=snapshot_name,
            snapshot_storage=snapshot_storage,
            allow_snapshot_absence=allow_snapshot_absence,
            join_keys=join_keys_str,
            symbol_name_in_snapshot=symbol_name_in_snapshot,
            database=database,
            default_fields_for_outer_join=default_fields_str,
            prefix_for_output_ticks=prefix_for_output_ticks,
            **kwargs,
        )
    )

    self.schema.update(**snapshot_schema)

    return self
