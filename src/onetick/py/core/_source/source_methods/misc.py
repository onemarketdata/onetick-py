import functools
import re
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import onetick.py as otp
from onetick.py.backports import Literal
from onetick.py import types as ott
from onetick.py import utils
from onetick.py.core.column import _Column
from onetick.py.otq import otq
from onetick.py.compatibility import (
    is_ob_virtual_prl_and_show_full_detail_supported,
    is_per_tick_script_boolean_problem,
)
from onetick.py.aggregations._docs import copy_signature

if TYPE_CHECKING:
    from onetick.py.core.source import Source


def inplace_operation(method):
    """Decorator that adds the `inplace` parameter and logic according to this
    flag. inplace=True means that method modifies an object, otherwise it copies
    the object firstly, modifies copy and returns the copy.
    """

    @functools.wraps(method)
    def _inner(self, *args, inplace=False, **kwargs):
        kwargs['inplace'] = inplace
        if inplace:
            method(self, *args, **kwargs)
            return None
        else:
            obj = self.copy()
            return method(obj, *args, **kwargs)

    return _inner


def _columns_names_regex(
    self: 'Source', objs: Tuple[Union[_Column, str]], drop: bool = False
) -> Tuple[List[str], bool]:
    """
    We can't be sure python Source has consistent columns cache, because sinking complex event processors
    can change columns unpredictable, so if user will specify regex as a param, we will pass regex
    as an onetick's param, but pass or delete all matched columns from python Source cache.

    Parameters
    ----------
    objs:
        Tuple of _Columns or string to pass or drop. String can be regex.
    drop: bool
        To drop columns from python schema or not.

    Returns
    -------
    items_to_passthrough:
        Items to pass to Passthrough as `field` parameter.
    regex:
        Value to pass to Passthrough as `use_regex` parameter.
    """
    def is_regex_string(obj):
        return isinstance(obj, str) and any(c in r"*+?\:[]{}()^$" for c in obj)

    # if any object from the list is a regexp, then all other objects will be treated like regexps too
    regex = any(is_regex_string(obj) for obj in objs)

    items_to_passthrough = []
    names_of_columns = []
    for obj in objs:
        if not isinstance(obj, (str, _Column)):
            raise TypeError(f"It is not supported to select or delete item '{obj}' of type '{type(obj)}'")
        if regex:
            # if column object or non-regex string is specified in regex mode,
            # then we assume that the user requested that exact column to be dropped
            if isinstance(obj, _Column):
                names_of_columns.append(obj.name)
                obj = f'^{obj.name}$'
            elif not is_regex_string(obj):
                names_of_columns.append(obj)
                obj = f'^{obj}$'
            else:
                names_of_columns.extend(col for col in self.columns() if re.search(obj, col))
            items_to_passthrough.append(obj)
        else:
            name = obj.name if isinstance(obj, _Column) else obj
            items_to_passthrough.append(name)
            names_of_columns.append(name)

    # remove duplications and meta_fields
    names_of_columns: set[str] = set(names_of_columns) - set(self.__class__.meta_fields)  # type: ignore[no-redef]
    # TODO: we definitely have the same logic of checking columns somewhere else too, need to refactor
    for item in names_of_columns:
        if item not in self.__dict__ or not isinstance(self.__dict__[item], _Column):
            raise AttributeError(f"There is no '{item}' column")
    if drop:
        for item in names_of_columns:
            del self.__dict__[item]
    return items_to_passthrough, regex


@inplace_operation
def pause(self: 'Source', delay, busy_waiting=False, where=None, inplace=False) -> Optional['Source']:
    """
    Pauses processing of each tick for number of milliseconds specified via ``delay`` expression.

    Parameters
    ----------
    delay: int or :py:class:`onetick.py.Operation`
        Integer number or OneTick expression used to calculate the delay.
        Delay is in milliseconds.
        Note that number can't be negative.
    busy_waiting: bool
        If True then delay is done via busy loop (consuming CPU time).
    where: :py:class:`onetick.py.Operation`
        Expression to select ticks for which processing will be paused.
        By default, all ticks are selected.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise method returns a new modified object.

    See also
    --------
    **PAUSE** OneTick event processor

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Pause every tick for 100 milliseconds (0.1 second).

    >>> data = otp.Tick(A=1)
    >>> data = data.pause(100)

    Set the ``delay`` with expression and use ``where`` parameter to pause conditionally:

    >>> data = otp.Ticks(A=[-1, 1, 2])
    >>> data = data.pause(data['A'] * 100, where=data['A'] > 0)
    """
    if not isinstance(delay, int):
        delay = str(delay)
    elif delay < 0:
        raise ValueError("Parameter 'delay' can't be negative")
    where = '' if where is None else str(where)
    self.sink(
        otq.Pause(
            delay=delay,
            busy_waiting=busy_waiting,
            where=where,
        )
    )
    return self


@inplace_operation
def insert_tick(
    self: 'Source',
    fields=None,
    where=None,
    preserve_input_ticks=True,
    num_ticks_to_insert=1,
    insert_before=True,
    inplace=False,
) -> Optional['Source']:
    """
    Insert tick.

    Parameters
    ----------
    fields: dict of str to :py:class:`onetick.py.Operation`
        Mapping of field names to some expressions or values.
        These fields in inserted ticks will be set to corresponding values or results of expressions.
        If field is presented in input tick, but not set in ``fields`` dict,
        then the value of the field will be copied from input tick to inserted tick.
        If parameter ``fields`` is not set at all, then values for inserted ticks' fields
        will be default values for fields' types from input ticks (0 for integers etc.).
    where: :py:class:`onetick.py.Operation`
        Expression to select ticks near which the new ticks will be inserted.
        By default, all ticks are selected.
    preserve_input_ticks: bool
        A switch controlling whether input ticks have to be preserved in output time series or not.
        While the former case results in fields of input ticks to be present in the output time series
        together with those defined by the ``fields`` parameter,
        the latter case results in only defined fields to be present.
        If a field of the input time series is defined in the ``fields`` parameter,
        the defined value takes precedence.
    num_ticks_to_insert: int
        Number of ticks to insert.
    insert_before: bool
        Insert tick before each input tick or after.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise method returns a new modified object.

    See also
    --------
    **INSERT_TICK** OneTick event processor

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Insert tick before each tick with default type values.

    >>> data = otp.Tick(A=1)
    >>> data = data.insert_tick()
    >>> otp.run(data)
            Time  A
    0 2003-12-01  0
    1 2003-12-01  1

    Insert tick before each tick with field `A` copied from input tick
    and field `B` set to specified value.

    >>> data = otp.Tick(A=1)
    >>> data = data.insert_tick(fields={'B': 'b'})
    >>> otp.run(data)
            Time  B  A
    0 2003-12-01  b  1
    1 2003-12-01     1

    Insert two ticks only after first tick.

    >>> data = otp.Ticks(A=[1, 2, 3])
    >>> data = data.insert_tick(where=data['A'] == 1,
    ...                         insert_before=False,
    ...                         num_ticks_to_insert=2)
    >>> otp.run(data)
                         Time  A
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.000  0
    2 2003-12-01 00:00:00.000  0
    3 2003-12-01 00:00:00.001  2
    4 2003-12-01 00:00:00.002  3
    """
    if not isinstance(num_ticks_to_insert, int) or num_ticks_to_insert <= 0:
        raise ValueError("Parameter 'num_ticks_to_insert' must be a positive integer")
    if not preserve_input_ticks and not fields:
        raise ValueError("Parameter 'fields' must be set if 'preserve_input_ticks' is False")
    where = '' if where is None else str(where)

    fields = fields or {}
    update_schema = {}
    for field, value in fields.items():
        dtype = ott.get_object_type(value)
        if field not in self.schema:
            update_schema[field] = dtype
        elif dtype is not self.schema[field]:
            raise ValueError(f"Incompatible types for field '{field}': {self.schema[field]} --> {dtype}")
        dtype = ott.type2str(dtype)
        if isinstance(value, type):
            value = ott.default_by_type(value)
        value = ott.value2str(value)
        fields[field] = (dtype, value)
    fields = ','.join(
        f'{field} {dtype}={value}' if value else f'{field} {dtype}' for field, (dtype, value) in fields.items()
    )

    self.sink(
        otq.InsertTick(
            fields=fields,
            where=where,
            preserve_input_ticks=preserve_input_ticks,
            num_ticks_to_insert=num_ticks_to_insert,
            insert_before=insert_before,
        )
    )
    if preserve_input_ticks:
        self.table(inplace=True, strict=False, **update_schema)
    else:
        self.table(inplace=True, strict=True, **update_schema)
    return self


@inplace_operation
def insert_at_end(
    self: 'Source',
    *,
    propagate_ticks: bool = True,
    delimiter_name: str = 'AT_END',
    inplace: bool = False,
) -> Optional['Source']:
    """
    This function adds a field ``delimiter_name``,
    which is set to zero for all inbound ticks
    and set to 1 for an additional tick that is generated when the data ends.

    The timestamp of the additional tick is set to the query end time.
    The values of all fields from the input schema of additional tick are set to default values for each type.

    Parameters
    ----------
    propagate_ticks: bool
        If True (default) this function returns all input ticks and an additionally generated tick,
        otherwise it returns only the last generated tick.
    delimiter_name: str
        The name of the delimiter field to add.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise method returns a new modified object.

    See also
    --------
    **INSERT_AT_END** OneTick event processor

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------
    Insert tick at the end of the stream:

    >>> data = otp.Ticks(A=[1, 2, 3])
    >>> data = data.insert_at_end()
    >>> otp.run(data)
                         Time  A  AT_END
    0 2003-12-01 00:00:00.000  1       0
    1 2003-12-01 00:00:00.001  2       0
    2 2003-12-01 00:00:00.002  3       0
    3 2003-12-04 00:00:00.000  0       1

    The name of the added field can be changed:

    >>> data = otp.Ticks(A=[1, 2, 3])
    >>> data = data.insert_at_end(delimiter_name='LAST_TICK')
    >>> otp.run(data)
                         Time  A  LAST_TICK
    0 2003-12-01 00:00:00.000  1          0
    1 2003-12-01 00:00:00.001  2          0
    2 2003-12-01 00:00:00.002  3          0
    3 2003-12-04 00:00:00.000  0          1

    If parameter ``propagate_ticks`` is set to False, then only the last tick is returned:

    >>> data = otp.Ticks(A=[1, 2, 3])
    >>> data = data.insert_at_end(propagate_ticks=False)
    >>> otp.run(data)
            Time  A  AT_END
    0 2003-12-04  0       1
    """
    if not hasattr(otq, 'InsertAtEnd'):
        raise RuntimeError("Function insert_at_end() is not available on this OneTick API version")
    if delimiter_name in self.schema:
        raise ValueError(f"Field '{delimiter_name}' is already in schema")

    self.sink(
        otq.InsertAtEnd(
            propagate_ticks=propagate_ticks,
            delimiter_name=delimiter_name,
        )
    )
    self.schema.update(**{delimiter_name: int})
    return self


@inplace_operation
def transpose(
    self: 'Source',
    inplace: bool = False,
    direction: Literal['rows', 'columns'] = 'rows',
    n: Optional[int] = None,
) -> Optional['Source']:
    """
    Data transposing.
    The main idea is joining many ticks into one or splitting one tick to many.

    Parameters
    ----------
    inplace: bool, default=False
        if `True` method will modify current object,
        otherwise it will return modified copy of the object.
    direction: 'rows', 'columns', default='rows'
        - `rows`: join certain input ticks (depending on other parameters) with preceding ones.
            Fields of each tick will be added to the output tick and their names will be suffixed
            with **_K** where **K** is the positional number of tick (starting from 1) in reverse order.
            So fields of current tick will be suffixed with **_1**, fields of previous tick will be
            suffixed with **_2** and so on.
        - `columns`: the operation is opposite to `rows`. It splits each input tick to several
            output ticks. Each input tick must have fields with names suffixed with **_K**
            where **K** is the positional number of tick (starting from 1) in reverse order.
    n: Optional[int], default=None
        must be specified only if ``direction`` is 'rows'.
        Joins every **n** number of ticks with **n-1** preceding ticks.

    Returns
    -------
    If ``inplace`` parameter is `True` method will return `None`,
    otherwise it will return modified copy of the object.

    See also
    --------
    **TRANSPOSE** OneTick event processor

    Examples
    --------
    Merging two ticks into one.

    >>> data = otp.Ticks(dict(A=[1, 2],
    ...                       B=[3, 4]))
    >>> data = data.transpose(direction='rows', n=2) # OTdirective: skip-snippet:;
    >>> otp.run(data)
                         Time             TIMESTAMP_1  A_1  B_1 TIMESTAMP_2  A_2  B_2
    0 2003-12-01 00:00:00.001 2003-12-01 00:00:00.001    2    4  2003-12-01    1    3

    And splitting them back into two.

    >>> data = data.transpose(direction='columns') # OTdirective: skip-snippet:;
    >>> otp.run(data)
                         Time  A  B
    0 2003-12-01 00:00:00.000  1  3
    1 2003-12-01 00:00:00.001  2  4
    """

    direction_map = {'rows': 'ROWS_TO_COLUMNS', 'columns': 'COLUMNS_TO_ROWS'}
    n: Union[str, int] = '' if n is None else n  # type: ignore[no-redef]

    self.sink(otq.Transpose(direction=direction_map[direction], key_constraint_values=n))
    # TODO: we should change source's schema after transposing
    return self


def cache(
    self: 'Source',
    cache_name: str,
    delete_if_exists: bool = True,
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
    start: Optional[ott.datetime] = None,
    end: Optional[ott.datetime] = None,
    read_mode: str = "automatic",
    update_cache: bool = True,
    tick_type: str = "ANY",
    symbol: Optional[str] = None,
    db: Optional[str] = None,
) -> 'Source':
    """
    Create cache from query and :py:class:`onetick.py.ReadCache` for created cache.

    Cache will be created only for current session.

    By default, if cache with specified name exists, it will be deleted and recreated.
    You can change this behaviour via ``delete_if_exists`` parameter.

    Parameters
    ----------
    cache_name: str
        Name of the cache to be deleted.
    delete_if_exists: bool
        If set to ``True`` (default), a check will be made to detect the existence of a cache
        with the specified name. Cache will be deleted and recreated only if it exists.
        If set to ``False``, if cache exists it won't be deleted and recreated.
    inheritability: bool
        Indicates whether results can be obtained by combining time intervals that were cached with intervals
        freshly computed to obtain results for larger intervals.
    otq_params: dict
        OTQ params of the query to be cached.
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
    start: :py:class:`otp.datetime <onetick.py.datetime>`
        Start time for cache query. By default, the start time of the query will be used.
    end: :py:class:`otp.datetime <onetick.py.datetime>`
        End time for cache query. By default, the end time of the query will be used.
    read_mode: str
        Mode of querying cache. One of these:

        * ``cache_only`` - only cached results are returned and queries are not performed.
        * ``query_only`` - the query is run irrespective of whether the result is already available in the cache.
        * ``automatic`` (default) - perform the query if the data is not found in the cache.
    update_cache: bool
        If set to ``True``, updates the cached data if ``read_mode=query_only`` or if ``read_mode=automatic`` and
        the result data not found in the cache. Otherwise, the cache remains unchanged.
    tick_type: str
        Tick type.
    symbol: str, list of str, list of otq.Symbol, :py:class:`onetick.py.Source`, :pandas:`pandas.DataFrame`, optional
        ``symbols`` parameter of ``otp.run()``.
    db: str
        Database.

    See also
    --------
    | :py:func:`onetick.py.create_cache`
    | :py:class:`onetick.py.ReadCache`

    Examples
    --------
    Simple usage

    >>> src = otp.DataSource("COMMON", tick_type="TRD", symbols="AAPL")
    >>> data = src.cache(
    ...    cache_name="some_cache",
    ...    tick_type="TRD", symbol="SYM", db="LOCAL",
    ... )
    >>> df = otp.run(data)  # doctest: +SKIP
    """
    from onetick.py.cache import create_cache, delete_cache, modify_cache_config
    from onetick.py.sources import ReadCache

    cache_exists = True

    if delete_if_exists:
        try:
            modify_cache_config(cache_name, "TEST_PARAM", "TEST_VALUE")
        except Exception as exc:
            if "There is no cache" in str(exc):
                cache_exists = False

    if cache_exists and delete_if_exists:
        delete_cache(cache_name)
        cache_exists = False

    if not cache_exists:
        create_cache(
            cache_name=cache_name,
            query=self,
            inheritability=inheritability,
            otq_params=otq_params,
            time_granularity=time_granularity,
            time_granularity_units=time_granularity_units,
            timezone=timezone,
            time_intervals_to_cache=time_intervals_to_cache,
            allow_delete_to_everyone=allow_delete_to_everyone,
            allow_update_to_everyone=allow_update_to_everyone,
            allow_search_to_everyone=allow_search_to_everyone,
            cache_expiration_interval=cache_expiration_interval,
            tick_type=tick_type,
            symbol=symbol,
            db=db,
        )

    return ReadCache(
        cache_name=cache_name,
        start=start if start is not None else utils.adaptive,
        end=end if end is not None else utils.adaptive,
        read_mode=read_mode,
        update_cache=update_cache,
        tick_type=tick_type,
        symbol=symbol if symbol is not None else utils.adaptive,
        db=db if db is not None else utils.adaptive_to_default,
    )


@inplace_operation
def pnl_realized(
    self: 'Source',
    computation_method: str = 'fifo',
    output_field_name: str = 'PNL_REALIZED',
    size_field: Union[str, _Column] = 'SIZE',
    price_field: Union[str, _Column] = 'PRICE',
    buy_sell_flag_field: Union[str, _Column] = 'BUY_SELL_FLAG',
    inplace=False,
) -> Optional['Source']:
    """
    Computes the realized Profit and Loss (**PNL**) on each tick and is applicable to both scenarios,
    whether selling after buying or buying after selling.

    Parameters
    ----------
    computation_method: str
        This parameter determines the approach used to calculate the realized Profit and Loss (**PnL**).

        Possible options are:

        * ``fifo`` - Stands for 'First-In-First-Out,' is used to calculate Profit and Loss (**PnL**)
          based on the principle that the first trading positions bought are the first ones to be sold,
          or conversely, the first trading positions sold are the first ones to be bought.

    output_field_name: str
        This parameter defines the name of the output field.

        Default: **PNL_REALIZED**.

    size_field: str, :py:class:`otp.Column <onetick.py.Column>`
        The name of the field with size, default is **SIZE**.
    price_field: str, :py:class:`otp.Column <onetick.py.Column>`
        The name of the field with price, default is **PRICE**.
    buy_sell_flag_field: str, :py:class:`otp.Column <onetick.py.Column>`
        The name of the field with buy/sell flag, default is **BUY_SELL_FLAG**.
        If the type of this field is string, then possible values are 'B' or 'b' for buy and 'S' or 's' for sell.
        If the type of this field is integer, then possible values are 0 for buy and 1 for sell.


    See also
    --------
    **PNL_REALIZED** OneTick event processor

    Examples
    --------
    Let's generate some data:

    >>> trades = otp.Ticks(
    ...     PRICE=[1.0, 2.0, 3.0, 2.5, 4.0, 5.0, 6.0, 7.0, 3.0, 4.0, 1.0],
    ...     SIZE=[700, 20, 570, 600, 100, 100, 100, 100, 150, 10, 100],
    ...     SELL_FLAG=[0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 1],
    ...     SIDE=['B', 'B', 'B', 'S', 'S', 'S', 'S', 'S', 'B', 'B', 'S'],
    ... )
    >>> otp.run(trades)
                          Time  PRICE  SIZE  SELL_FLAG SIDE
    0  2003-12-01 00:00:00.000    1.0   700          0    B
    1  2003-12-01 00:00:00.001    2.0    20          0    B
    2  2003-12-01 00:00:00.002    3.0   570          0    B
    3  2003-12-01 00:00:00.003    2.5   600          1    S
    4  2003-12-01 00:00:00.004    4.0   100          1    S
    5  2003-12-01 00:00:00.005    5.0   100          1    S
    6  2003-12-01 00:00:00.006    6.0   100          1    S
    7  2003-12-01 00:00:00.007    7.0   100          1    S
    8  2003-12-01 00:00:00.008    3.0   150          0    B
    9  2003-12-01 00:00:00.009    4.0    10          0    B
    10 2003-12-01 00:00:00.010    1.0   100          1    S

    And then calculate profit and loss metric for it.

    First let's use string ``buy_sell_flag_field`` field:

    >>> data = trades.pnl_realized(buy_sell_flag_field='SIDE')  # doctest: +SKIP
    >>> otp.run(data)[['Time', 'PRICE', 'SIZE', 'SIDE', 'PNL_REALIZED']]  # doctest: +SKIP
                          Time  PRICE  SIZE  SIDE  PNL_REALIZED
    0  2003-12-01 00:00:00.000    1.0   700     B           0.0
    1  2003-12-01 00:00:00.001    2.0    20     B           0.0
    2  2003-12-01 00:00:00.002    3.0   570     B           0.0
    3  2003-12-01 00:00:00.003    2.5   600     S         900.0
    4  2003-12-01 00:00:00.004    4.0   100     S         300.0
    5  2003-12-01 00:00:00.005    5.0   100     S         220.0
    6  2003-12-01 00:00:00.006    6.0   100     S         300.0
    7  2003-12-01 00:00:00.007    7.0   100     S         400.0
    8  2003-12-01 00:00:00.008    3.0   150     B           0.0
    9  2003-12-01 00:00:00.009    4.0    10     B           0.0
    10 2003-12-01 00:00:00.010    1.0   100     S        -200.0

    We can get the same result using integer ``buy_sell_flag_field`` field:

    >>> data = trades.pnl_realized(buy_sell_flag_field='SELL_FLAG')  # doctest: +SKIP
    >>> otp.run(data)[['Time', 'PRICE', 'SIZE', 'SELL_FLAG', 'PNL_REALIZED']]  # doctest: +SKIP
                          Time  PRICE  SIZE  SELL_FLAG  PNL_REALIZED
    0  2003-12-01 00:00:00.000    1.0   700          0           0.0
    1  2003-12-01 00:00:00.001    2.0    20          0           0.0
    2  2003-12-01 00:00:00.002    3.0   570          0           0.0
    3  2003-12-01 00:00:00.003    2.5   600          1         900.0
    4  2003-12-01 00:00:00.004    4.0   100          1         300.0
    5  2003-12-01 00:00:00.005    5.0   100          1         220.0
    6  2003-12-01 00:00:00.006    6.0   100          1         300.0
    7  2003-12-01 00:00:00.007    7.0   100          1         400.0
    8  2003-12-01 00:00:00.008    3.0   150          0           0.0
    9  2003-12-01 00:00:00.009    4.0    10          0           0.0
    10 2003-12-01 00:00:00.010    1.0   100          1        -200.0
    """
    if computation_method not in ['fifo']:
        raise ValueError(
            f"Parameter 'computation_method' has incorrect value: '{computation_method}', "
            f"should be one of these: 'fifo'."
        )

    for field in (size_field, price_field, buy_sell_flag_field):
        if str(field) not in self.schema:
            raise ValueError(f'Field {field} is not in schema')

    # PNL_REALIZED doesn't support choosing input fields, so we workaround by renaming fields
    restore_fields = {}
    added_fields = []
    for default_field, field in (('SIZE', size_field), ('PRICE', price_field), ('BUY_SELL_FLAG', buy_sell_flag_field)):
        if field != default_field:
            if default_field in self.schema:
                tmp_field_name = f'__TMP__{field}__TMP__'
                self[tmp_field_name] = self[default_field]
                restore_fields[default_field] = self[tmp_field_name]
            self[default_field] = self[field]
            added_fields.append(default_field)

    if output_field_name in self.schema:
        raise ValueError(f'Field {output_field_name} is already in schema')

    computation_method = computation_method.upper()
    self.sink(otq.PnlRealized(computation_method=computation_method, output_field_name=output_field_name))
    self.schema[output_field_name] = float

    if added_fields:
        self.drop(added_fields, inplace=True)
    if restore_fields:
        # restore fields from temporary fields and delete temporary fields
        self.add_fields(restore_fields)
        self.drop(list(restore_fields.values()), inplace=True)

    return self


@inplace_operation
def execute(self: 'Source', *operations, inplace=False) -> Optional['Source']:
    """
    Execute operations without returning their values.
    Some operations in onetick.py can be used to modify the state of some object
    (tick sequences mostly) and in that case user may not want to save the result of the
    operation to column.

    Parameters
    ----------
    operations : list of :py:class:`~onetick.py.Operation`
        operations to execute.
    inplace : bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object.

    Returns
    -------
    :class:`Source` or ``None``

    See Also
    --------
    **EXECUTE_EXPRESSIONS** OneTick event processor

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A')
    >>> data = data.execute(data.state_vars['SET'].erase(A=1))
    """
    if not operations:
        raise ValueError('At least one operation must be specified in execute() method')
    op_str = ';'.join(map(str, operations))
    self.sink(otq.ExecuteExpressions(op_str))
    return self


@inplace_operation
def fillna(self: 'Source', value=None, columns=None, inplace=False) -> Optional['Source']:
    """
    Replace NaN values in floating point type fields with the ``value``.

    Parameters
    ----------
    value : float, :py:class:`~onetick.py.Operation`
        The value to replace NaN.
        If not specified then the value from the previous tick will be used.
    columns: list
        List of strings with column names or :py:class:`~onetick.py.Column` objects.
        Only the values in specified columns will be replaced.
        By default the values in all floating point type fields will be replaced.
    inplace : bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object.

    Returns
    -------
    :class:`Source` or ``None``

    See Also
    --------
    :py:meth:`onetick.py.Operation.fillna`

    Examples
    --------

    By default, the value of the previous tick is used as a value to replace NaN
    (for the first tick the previous value do not exist, so it will still be NaN):

    >>> data = otp.Ticks({'A': [0, 1, 2, 3], 'B': [otp.nan, 2.2, otp.nan, 3.3]})
    >>> data = data.fillna()
    >>> otp.run(data)
                         Time  A    B
    0 2003-12-01 00:00:00.000  0  NaN
    1 2003-12-01 00:00:00.001  1  2.2
    2 2003-12-01 00:00:00.002  2  2.2
    3 2003-12-01 00:00:00.003  3  3.3

    The value can also be a constant:

    >>> data = otp.Ticks({'A': [0, 1, 2, 3], 'B': [otp.nan, 2.2, otp.nan, 3.3]})
    >>> data = data.fillna(777)
    >>> otp.run(data)
                         Time  A     B
    0 2003-12-01 00:00:00.000  0  777.0
    1 2003-12-01 00:00:00.001  1    2.2
    2 2003-12-01 00:00:00.002  2  777.0
    3 2003-12-01 00:00:00.003  3    3.3

    :py:class:`~onetick.py.Operation` objects can also be used as a ``value``:

    >>> data = otp.Ticks({'A': [0, 1, 2, 3], 'B': [otp.nan, 2.2, otp.nan, 3.3]})
    >>> data = data.fillna(data['A'])
    >>> otp.run(data)
                         Time  A     B
    0 2003-12-01 00:00:00.000  0    0.0
    1 2003-12-01 00:00:00.001  1    2.2
    2 2003-12-01 00:00:00.002  2    2.0
    3 2003-12-01 00:00:00.003  3    3.3

    Parameter ``columns`` can be used to specify the columns where values will be replaced:

    .. testcode::
       :skipif: is_per_tick_script_boolean_problem()

       data = otp.Ticks({'A': [0, 1, 2, 3], 'B': [otp.nan, 2.2, otp.nan, 3.3], 'C': [otp.nan, 2.2, otp.nan, 3.3]})
       data = data.fillna(columns=['B'])
       df = otp.run(data)
       print(df)

    .. testoutput::

                            Time  A    B    C
       0 2003-12-01 00:00:00.000  0  NaN  NaN
       1 2003-12-01 00:00:00.001  1  2.2  2.2
       2 2003-12-01 00:00:00.002  2  2.2  NaN
       3 2003-12-01 00:00:00.003  3  3.3  3.3
    """
    if columns:
        for column in columns:
            if column not in self.schema:
                raise ValueError(f"Column '{column}' is not in schema")
            if self.schema[column] is not float:
                raise TypeError(f"Column '{column}' doesn't have float type")
        columns = list(map(str, columns))
    no_columns = columns is None

    if value is not None:
        if isinstance(value, int):
            value = float(value)
        if isinstance(value, otp.Operation) and value.dtype is int:
            value = value.astype(float)
        dtype = ott.get_object_type(value)
        if dtype is not float:
            raise ValueError(f"The type of parameter 'value' must be float, not {dtype}")

        def fun(tick):
            for field in otp.tick_descriptor_fields():
                if (field.get_type() == 'double'
                        and tick.get_double_value(field.get_name()) == otp.nan
                        and (no_columns or field.get_name().isin(*columns))):
                    tick.set_double_value(field.get_name(), value)
    else:
        # deque may have already been created by previous call of fillna() method
        if '__OTP_FILLNA_DEQUE__' not in self.state_vars:
            self.state_vars['__OTP_FILLNA_DEQUE__'] = otp.state.tick_deque(scope='branch')

        def fun(tick):
            i = otp.static(0)
            prev_tick = otp.tick_deque_tick()
            # first tick doesn't have the previous tick, so skipping it
            if i > 0:
                # get the previous tick from the deque
                tick.state_vars['__OTP_FILLNA_DEQUE__'].get_tick(0, prev_tick)
                # replace value in all double fields with the value of the previous tick
                for field in otp.tick_descriptor_fields():
                    if (field.get_type() == 'double'
                            and tick.get_double_value(field.get_name()) == otp.nan
                            and (no_columns or field.get_name().isin(*columns))):
                        tick.set_double_value(field.get_name(),
                                              prev_tick.get_double_value(field.get_name()))
            # clear the deque and add the current tick to it
            tick.state_vars['__OTP_FILLNA_DEQUE__'].clear()
            tick.state_vars['__OTP_FILLNA_DEQUE__'].push_back(tick)
            i = i + 1

    return self.script(fun, inplace=inplace)


@inplace_operation
def mkt_activity(self: 'Source', calendar_name=None, inplace=False) -> Optional['Source']:
    """
    Adds a string field named **MKT_ACTIVITY** to each tick in the input tick stream.

    The value of this field is set to the union of session flags that apply for the security at the time of the tick,
    as specified in the calendar sections of the reference database (see Reference Database Guide).

    Session flags may differ between databases,
    but the following letters have reserved meaning: L - half day, H - holiday, W - weekend, R - regular.

    The calendar can either be specified explicitly by name
    (the *CALENDAR* sections of the reference database are assumed to contain a calendar with such a name),
    or default to the security- or exchange-level calendars for the queried symbol
    (the *SYMBOL_CALENDAR* or *EXCH_CALENDAR* sections of the reference database).

    The latter case requires a non-zero symbol date to be specified for queried symbols
    (see parameter ``symbol_date`` in :py:func:`otp.run <onetick.py.run>`).

    Parameters
    ----------
    calendar_name : str, :py:class:`~onetick.py.Column`
        The calendar name to choose for the respective calendar from the *CALENDAR* sections of the reference database.
        It can be a string constant or the name of the field with per-tick calendar name.

        When this parameter is not specified,
        default security- or exchange-level calendars configured for the queried database and symbol are used
        (but symbol date must be specified in this case).
    inplace : bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object.

    Note
    ----
    When applying :py:meth:`mkt_activity` to aggregated data,
    please take into account that session flags may change during the aggregation bucket.
    The field **MKT_ACTIVITY**, in this case, will represent the session flags at the time assigned to the bucket,
    which may be different from the session flags at some times during the bucket.

    Returns
    -------
    :class:`Source` or ``None``

    See Also
    --------
    **MKT_ACTIVITY** OneTick event processor

    Examples
    --------

    By default, security- or exchange-level calendars configured for the queried database and symbol are used
    (but symbol date must be specified in this case):

    >>> data = otp.DataSource(...)  # doctest: +SKIP
    >>> data = data.mkt_activity()  # doctest: +SKIP
    >>> otp.run(data, date=otp.date(2022, 1, 1), symbol_date=otp.date(2022, 1, 1))  # doctest: +SKIP

    Otherwise, parameter ``calendar_name`` must be specified:

    >>> data = otp.DataSource(...)  # doctest: +SKIP
    >>> data = data.mkt_activity(calendar_name='WNY')  # doctest: +SKIP
    >>> otp.run(data, date=otp.date(2022, 1, 1))  # doctest: +SKIP

    Parameter ``calendar_name`` can also be specified as a column.
    In this case calendar name can be different for each tick:

    >>> data = otp.DataSource(...)  # doctest: +SKIP
    >>> data = data.mkt_activity(calendar_name=data['CALENDAR_NAME'])  # doctest: +SKIP
    >>> otp.run(data, date=otp.date(2022, 1, 1))  # doctest: +SKIP

    In this example you can see how market activity status is changing during the days.
    We are getting first and last tick of the group each time the type of market activity is changed.
    You can see regular trades (R) from 9:30 to 16:00, and a holiday (H) on 2018-02-07.

    >>> data = otp.DataSource('TRAIN_A_PRL_TRD', tick_type='TRD', symbols='MSFT')  # doctest: +SKIP
    >>> data = data.mkt_activity('FRED')  # doctest: +SKIP
    >>> data = data[['PRICE', 'SIZE', 'MKT_ACTIVITY']]  # doctest: +SKIP
    >>> first = data.first(1, bucket_interval=(data['MKT_ACTIVITY'] != data['MKT_ACTIVITY'][-1]))  # doctest: +SKIP
    >>> last = data.last(1, bucket_interval=(data['MKT_ACTIVITY'] != data['MKT_ACTIVITY'][-1]))  # doctest: +SKIP
    >>> data = otp.merge([first, last])  # doctest: +SKIP
    >>> df = otp.run(data,  # doctest: +SKIP
    ...              start=otp.dt(2018, 2, 1), end=otp.dt(2018, 2, 9),
    ...              symbol_date=otp.dt(2018, 2, 1), timezone='EST5EDT')
    >>> df[['Time', 'MKT_ACTIVITY']]  # doctest: +SKIP
                          Time MKT_ACTIVITY
    0  2018-02-01 01:31:44.466
    1  2018-02-01 09:29:59.996
    2  2018-02-01 09:30:00.225            R
    3  2018-02-01 15:59:58.857            R
    4  2018-02-01 16:00:01.858
    5  2018-02-02 09:29:50.366
    6  2018-02-02 09:30:01.847            R
    7  2018-02-02 15:59:59.829            R
    8  2018-02-02 16:00:01.782
    9  2018-02-05 09:29:43.084
    10 2018-02-05 09:30:00.301            R
    11 2018-02-05 15:59:59.974            R
    12 2018-02-05 16:00:02.438
    13 2018-02-06 09:29:27.279
    14 2018-02-06 09:30:00.045            R
    15 2018-02-06 15:59:59.903            R
    16 2018-02-06 16:01:03.524
    17 2018-02-07 09:29:56.739
    18 2018-02-07 09:30:00.365            H
    19 2018-02-07 15:59:59.940            H
    20 2018-02-07 16:00:00.187
    21 2018-02-08 09:29:28.446
    22 2018-02-08 09:30:00.658            F
    23 2018-02-08 15:59:59.564            F
    24 2018-02-08 16:00:02.355
    25 2018-02-08 19:59:57.061
    """
    if calendar_name is None:
        calendar_name = ''
    if isinstance(calendar_name, str):
        calendar_field_name = ''
    elif isinstance(calendar_name, _Column):
        calendar_field_name = str(calendar_name)
        calendar_name = ''
    else:
        raise ValueError(f"Unsupported type for parameter 'calendar_name': {type(calendar_name)}")
    self.sink(
        otq.MktActivity(calendar_name=calendar_name, calendar_field_name=calendar_field_name)
    )
    self.schema['MKT_ACTIVITY'] = str
    return self


@inplace_operation
def book_diff(self: 'Source', include_initial_book: bool = False, inplace=False) -> Optional['Source']:
    """
    Performs book diffing for every pair of consecutive ticks, each representing a flat book of a fixed depth.

    This method can be thought to be an operation, opposite to :py:meth:`~onetick.py.Source.ob_snapshot_flat`.
    Every input tick, different from the previous one, results in a series of output PRL ticks to be propagated,
    each carrying information about a level deletion, addition or update.

    The input of this method is a time series of flat book ticks,
    just like the result of :py:meth:`~onetick.py.Source.ob_snapshot_flat`.
    More precisely, for a fixed depth N of the flat book,
    input ticks should carry the fields BID_<FIELD_NAME>K and ASK_<FIELD_NAME>K,
    where 1 <= K <= N and <FIELD_NAME> ranges over a specific set of fields F,
    among which PRICE and SIZE are mandatory.

    The output of this method is a time series of PRL ticks,
    carrying information about a level deletion, addition, or update.
    Each tick carries the fields from the above-mentioned set F,
    plus BUY_SELL_FLAG, RECORD_TYPE, TICK_STATUS, and DELETED_TIME.
    BUY_SELL_FLAG carries the side (0 - bid, 1 - ask), the rest are for internal use.

    Parameters
    ----------
    include_initial_book : bool
        This method treats the first tick as an "initial state" of the book.
        If this parameter is set to True, then this initial tick will also be in the output.
    inplace : bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object.

    Note
    ----
    Tick descriptor changes are not allowed in this event processor.

    Returns
    -------
    :class:`Source` or ``None``

    See Also
    --------
    **BOOK_DIFF** OneTick event processor

    Examples
    --------

    First let's get flat order book snapshot from the database (top level each 12 hours):

    >>> data = otp.DataSource('TRAIN_A_PRL_TRD', tick_type='PRL', symbols='MSFT')  # doctest: +SKIP
    >>> flat = data.ob_snapshot_flat(max_levels=1, bucket_interval=otp.Hour(12))  # doctest: +SKIP
    >>> otp.run(flat, date=otp.dt(2018, 2, 1))  # doctest: +SKIP
                     Time  BID_PRICE1  BID_SIZE1        BID_UPDATE_TIME1  ASK_PRICE1  ASK_SIZE1        ASK_UPDATE_TIME1
    0 2018-02-01 12:00:00       95.53       1400 2018-02-01 11:59:59.797       95.54        200 2018-02-01 11:59:59.978
    1 2018-02-02 00:00:00       94.52        100 2018-02-01 19:57:02.502       94.90        250 2018-02-01 18:35:38.543

    Then we can apply ``book_diff`` method to the result to get the PRL ticks again:

    >>> diff = flat.book_diff()  # doctest: +SKIP
    >>> otp.run(diff, date=otp.dt(2018, 2, 1))  # doctest: +SKIP
            Time  PRICE  SIZE             UPDATE_TIME  BUY_SELL_FLAG RECORD_TYPE  TICK_STATUS         DELETED_TIME
    0 2018-02-02  95.53     0 2018-02-01 11:59:59.797              0           R            0  1969-12-31 19:00:00
    1 2018-02-02  94.52   100 2018-02-01 19:57:02.502              0           R            0  1969-12-31 19:00:00
    2 2018-02-02  94.90   250 2018-02-01 18:35:38.543              1           R            0  1969-12-31 19:00:00
    3 2018-02-02  95.54     0 2018-02-01 11:59:59.978              1           R            0  1969-12-31 19:00:00

    By default the first tick in the query range is not included,
    use parameter ``include_initial_book`` to include it:

    >>> diff = flat.book_diff(include_initial_book=True)  # doctest: +SKIP
    >>> otp.run(diff, date=otp.dt(2018, 2, 1))  # doctest: +SKIP
                     Time  PRICE  SIZE             UPDATE_TIME BUY_SELL_FLAG RECORD_TYPE TICK_STATUS        DELETED_TIME
    0 2018-02-01 12:00:00  95.53  1400 2018-02-01 11:59:59.797             0           R           0 1969-12-31 19:00:00
    1 2018-02-01 12:00:00  95.54   200 2018-02-01 11:59:59.978             1           R           0 1969-12-31 19:00:00
    2 2018-02-02 00:00:00  95.53     0 2018-02-01 11:59:59.797             0           R           0 1969-12-31 19:00:00
    3 2018-02-02 00:00:00  94.52   100 2018-02-01 19:57:02.502             0           R           0 1969-12-31 19:00:00
    4 2018-02-02 00:00:00  94.90   250 2018-02-01 18:35:38.543             1           R           0 1969-12-31 19:00:00
    5 2018-02-02 00:00:00  95.54     0 2018-02-01 11:59:59.978             1           R           0 1969-12-31 19:00:00
    """
    self.sink(otq.BookDiff(include_initial_book=include_initial_book))
    return self


@inplace_operation
def limit(self: 'Source',
          tick_limit: int,
          tick_offset: Optional[int] = None,
          inplace=False) -> Optional['Source']:
    """
    Propagates ticks until the count limit is reached.

    Once the limit is reached,
    hidden ticks will still continue to propagate until the next regular tick appears.

    Parameters
    ----------
    tick_limit: int
        The number of regular ticks to propagate.
        Must be a non-negative integer or -1, which means no limit.
    tick_offset: int
        The number of regular ticks to skip before starting to propagate.
        Must be a non-negative integer.
        By default no ticks are skipped.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object.

    Returns
    -------
    :class:`Source` or ``None``

    See Also
    --------
    **LIMIT** OneTick event processor

    Examples
    --------

    Simple example, get first 3 ticks:

    .. testcode::
       :skipif: not otp.compatibility.is_limit_ep_supported()

       data = otp.Ticks(X=[1, 2, 3, 4, 5, 6])
       data = data.limit(3)
       df = otp.run(data)
       print(df)

    .. testoutput::

                            Time  X
       0 2003-12-01 00:00:00.000  1
       1 2003-12-01 00:00:00.001  2
       2 2003-12-01 00:00:00.002  3

    Disable limit by setting it to -1:

    .. testcode::
       :skipif: not otp.compatibility.is_limit_ep_supported()

       data = otp.Ticks(X=[1, 2, 3, 4, 5, 6])
       data = data.limit(-1)
       df = otp.run(data)
       print(df)

    .. testoutput::

                            Time  X
       0 2003-12-01 00:00:00.000  1
       1 2003-12-01 00:00:00.001  2
       2 2003-12-01 00:00:00.002  3
       3 2003-12-01 00:00:00.003  4
       4 2003-12-01 00:00:00.004  5
       5 2003-12-01 00:00:00.005  6

    Setting parameter ``tick_offset`` can be used to skip first ticks before propagating them.

    For example, we can skip first 2 ticks and propagate all other:

    .. testcode::
       :skipif: not otp.compatibility.is_limit_tick_offset_supported()

       data = otp.Ticks(X=[1, 2, 3, 4, 5, 6])
       data = data.limit(-1, tick_offset=2)
       df = otp.run(data)
       print(df)

    .. testoutput::

                            Time  X
       0 2003-12-01 00:00:00.002  3
       1 2003-12-01 00:00:00.003  4
       2 2003-12-01 00:00:00.004  5
       3 2003-12-01 00:00:00.005  6

    Or we can return ticks from the middle of the stream
    by skipping first 2 ticks and then returning next 2 ticks like this:

    .. testcode::
       :skipif: not otp.compatibility.is_limit_tick_offset_supported()

       data = otp.Ticks(X=[1, 2, 3, 4, 5, 6])
       data = data.limit(2, tick_offset=2)
       df = otp.run(data)
       print(df)

    .. testoutput::

                            Time  X
       0 2003-12-01 00:00:00.002  3
       1 2003-12-01 00:00:00.003  4
    """
    if not otp.compatibility.is_limit_ep_supported():
        raise RuntimeError('LIMIT EP isn\'t supported by the current OneTick version.')

    if tick_limit < 0 and tick_limit != -1:
        raise ValueError('Negative values, except -1, not allowed as `tick_limit` in `limit` method.')

    ep_kwargs = {}
    if tick_offset is not None:
        if not isinstance(tick_offset, int) or tick_offset < 0:
            raise ValueError("Parameter 'tick_offset' must be non-negative.")

        if not otp.compatibility.is_limit_tick_offset_supported():
            warnings.warn("Parameter 'tick_offset' is set, but is not supported on this OneTick version")
        else:
            ep_kwargs = {'tick_offset': tick_offset}

    self.sink(otq.Limit(tick_limit=tick_limit, **ep_kwargs))
    return self


def _merge_fields_by_regex(schema: dict, columns_regex: str, match_index: int):
    new_columns: Dict[str, type] = {}
    for column, column_type in list(schema.items()):
        match = re.match(columns_regex, column)
        if match:
            new_column = match.group(match_index)

            if new_column in schema:
                raise KeyError(
                    f'Can\'t apply `show_full_detail` for column `{column}`: '
                    f'column `{new_column}` is already in schema.'
                )

            if new_column in new_columns:
                if column_type != new_columns[new_column]:
                    raise TypeError(
                        f'Can\'t apply `show_full_detail`: '
                        f'type mismatch for columns with suffix `{new_column}`.'
                    )
            else:
                new_columns[new_column] = column_type

            del schema[column]

    schema.update(**new_columns)


@inplace_operation
def virtual_ob(
    self: 'Source',
    quote_source_fields: Optional[List[Union[str, _Column]]] = None,
    quote_timeout: Optional[float] = None,
    show_full_detail: bool = False,
    output_book_format: Literal['ob', 'prl'] = 'ob',
    inplace=False,
) -> Optional['Source']:
    """
    Creates a series of fake orders from a time series of best bids and asks. The algorithm used is as follows:
    For a tick with the best bid or ask, create an order tick adding the new best bid or ask and an order
    withdrawing the old one. Virtual order books can be created for multiple subgroups at once
    by using the ``quote_source_fields`` parameter to specify a list of string fields to be used for grouping.
    A separate book will be created for each combination.

    Parameters
    ----------
    quote_source_fields: Optional[List[Union[str, Column]]]
        Specifies a list of string fields for grouping quotes.
        The virtual order book is then constructed for each subgroup separately and
        the ``SOURCE`` field is constructed to contain the description of the group.
    quote_timeout: Optional[float]
        Specifies the maximum age of a quote that is not stale.
        A quote that is not replaced after more than ``quote_timeout`` seconds is considered stale,
        and delete orders will be generated for it.

        A value of ``quote_timeout`` can be fractional (for example, 3.51)
    show_full_detail: bool
        If set to ``True``, **virtual_ob** will attempt to include
        all fields in the input tick when forming the output tick.

        ``ASK_X/BID_X`` fields will combine under paired field ``X``
        ``ASK_X`` and ``BID_X`` must have the same type.

        If only ``ASK_X`` or ``BID_X`` exist, output will have ``X`` and the missing field
        will be assumed to have its default value.

        Paired and non-paired fields must not interfere with each other and the fields originally added by this EP
    output_book_format: 'prl' or 'ob'
        Supported values are ``prl`` and ``ob``. When set to ``prl``, field ``SIZE`` of output ticks represents
        current size for the tick's source, price, and side, and the EP propagates ``PRICE`` and ``SOURCE``
        as the state keys of its output time series.

        When set to ``ob``, field ``SIZE`` of output ticks represents the delta of size for
        the tick's source, price, and side, and the state key of the output ticks is empty.
    inplace : bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise, method returns a new modified object.

    Returns
    -------
    :class:`Source` or ``None``

    See Also
    --------
    **VIRTUAL_OB** OneTick event processor

    Examples
    --------

    Basic example

    >>> data = otp.DataSource(
    ...     db='US_COMP', symbols='AAPL', tick_type='QTE', date=otp.date(2003, 12, 1)
    ... )  # doctest: +SKIP
    >>> data = data[['ASK_PRICE', 'ASK_SIZE', 'BID_PRICE', 'BID_SIZE']]  # doctest: +SKIP
    >>> data = data.virtual_ob()  # doctest: +SKIP
    >>> otp.run(data)  # doctest: +SKIP
                          Time  PRICE        DELETED_TIME  SIZE  BUY_SELL_FLAG  TICK_STATUS SOURCE
    0  2003-12-01 00:00:00.000  22.28 1969-12-31 19:00:00   500              1            0   AAPL
    1  2003-12-01 00:00:00.000  21.66 1969-12-31 19:00:00   100              0            0   AAPL
    2  2003-12-01 00:00:00.001   21.8 1969-12-31 19:00:00   500              1            0   AAPL
    ...

    Specify columns to group quotes

    >>> data = otp.DataSource(
    ...     db='US_COMP', symbols='AAPL', tick_type='QTE', date=otp.date(2003, 12, 1)
    ... )  # doctest: +SKIP
    >>> data = data[['ASK_PRICE', 'ASK_SIZE', 'BID_PRICE', 'BID_SIZE', 'EXCHANGE']]  # doctest: +SKIP
    >>> data = data.virtual_ob(['EXCHANGE'])  # doctest: +SKIP
    >>> otp.run(data)  # doctest: +SKIP
                          Time  PRICE        DELETED_TIME  SIZE  BUY_SELL_FLAG  TICK_STATUS SOURCE
    0  2003-12-01 00:00:00.000  22.28 1969-12-31 19:00:00   500              1            0      D
    1  2003-12-01 00:00:00.000  21.66 1969-12-31 19:00:00   100              0            0      D
    2  2003-12-01 00:00:00.001   21.8 1969-12-31 19:00:00   500              1            0      P
    ...
    """
    kwargs: Dict[str, Any] = {}

    required_fields = {'BID_PRICE', 'BID_SIZE', 'ASK_PRICE', 'ASK_SIZE'}
    missing_fields = required_fields - set(self.schema.keys())

    if missing_fields:
        raise ValueError('Missing fields required by `virtual_ob`: ' + ', '.join(missing_fields))

    if output_book_format not in {'ob', 'prl'}:
        raise ValueError(
            f'Incorrect value for `output_book_format` parameter: {output_book_format}. '
            f'Supported values: \'ob\' or \'prl\''
        )

    if show_full_detail and output_book_format != 'prl':
        raise ValueError('`output_book_format` should be set to \'prl\' when `show_full_detail` set to `True`')

    is_pnl_and_show_full_detail_supported = is_ob_virtual_prl_and_show_full_detail_supported()

    if show_full_detail and not is_pnl_and_show_full_detail_supported:
        raise RuntimeError('`virtual_ob` not supports `show_full_detail` parameter on this OneTick version')

    if output_book_format != 'ob' and not is_pnl_and_show_full_detail_supported:
        raise RuntimeError('`virtual_ob` not supports `output_book_format` parameter on this OneTick version')

    if is_pnl_and_show_full_detail_supported:
        kwargs['show_full_detail'] = show_full_detail
        kwargs['output_book_format'] = output_book_format

    if quote_source_fields is None:
        quote_source_fields = []

    quote_source_fields_list = list(map(str, quote_source_fields))
    for column in quote_source_fields_list:
        if column not in self.schema:
            raise ValueError(f'Column \'{column}\' passed in `quote_source_fields` parameter is missing in the schema')

    self.sink(
        otq.VirtualOb(
            quote_source_fields=','.join(quote_source_fields_list),
            quote_timeout='' if quote_timeout is None else str(quote_timeout),
            **kwargs,
        )
    )

    schema = {
        'PRICE': float,
        'DELETED_TIME': otp.nsectime,
        'SIZE': float,
        'BUY_SELL_FLAG': int,
        'TICK_STATUS': int,
        'SOURCE': str,
    }

    if show_full_detail:
        exclude_fields = list(required_fields) + quote_source_fields_list
        schema.update({
            k: v for k, v in self.schema.copy().items()
            if k not in exclude_fields
        })

        _merge_fields_by_regex(schema, r'^(ASK|BID)_(.*)$', 2)

    self.schema.set(**schema)

    return self


@copy_signature(otp.functions.corp_actions, add_self=True, drop_parameters=['source'])
def corp_actions(self: 'Source', *args, **kwargs) -> 'Source':
    """
    Adjusts values using corporate actions information loaded into OneTick
    from the reference data file. To use it, location of reference database must
    be specified via OneTick configuration.

    Parameters
    ----------
    adjustment_date : :py:class:`onetick.py.date`, :py:class:`onetick.py.datetime`, int, str, None, optional
        The date as of which the values are adjusted.
        All corporate actions of the types specified in the parameters
        that lie between the tick timestamp and the adjustment date will be applied to each tick.

        This parameter can be either date or datetime .
        `int` and `str` format can be *YYYYMMDD* or *YYYYMMDDhhmmss*.
        When parameter is a date, the time is assumed to be 17:00:00 GMT
        and parameter ``adjustment_date_tz`` is ignored.

        If it is not set, the values are adjusted as of the end date in the query.

        Notice that the ``adjustment date`` is not affected neither by *_SYMBOL_PARAM._PARAM_END_TIME_NANOS*
        nor by the *apply_times_daily* setting in :py:func:`onetick.py.run`.

    adjustment_date_tz : str, optional
        Timezone for ``adjustment date``.

        By default global :py:attr:`tz<onetick.py.configuration.Config.tz>` value is used.
        Local timezone can't be used so in this case parameter is set to GMT.
        When ``adjustment_date`` is in YYYYMMDD format, this parameter is set to GMT.
    fields : str, optional
        A comma-separated list of fields to be adjusted. If this parameter is not set,
        some default adjustments will take place if appropriately named fields exist in the tick:

        - If the ``adjust_rule`` parameter is set to PRICE, and the PRICE field is present,
          it will get adjusted. If the fields ASK_PRICE or BID_PRICE are present, they will get adjusted.
          If fields ASK_VALUE or BID_VALUE are present, they will get adjusted

        - If the ``adjust_rule`` parameter is set to SIZE, and the SIZE field is present,
          it will get adjusted. If the fields ASK_SIZE or BID_SIZE are present, they will get adjusted.
          If fields ASK_VALUE or BID_VALUE are present, they will get adjusted.

    adjust_rule : str, optional
        When set to PRICE, adjustments are applied under the assumption that fields to be adjusted contain prices
        (adjustment direction is determined appropriately).

        When set to SIZE, adjustments are applied under the assumption that fields contain sizes
        (adjustment direction is opposite to that when the parameter's value is PRICE).

        By default the value is PRICE.
    apply_split : bool, optional
        If True, adjustments for splits are applied.
    apply_spinoff : bool, optional
        If True, adjustments for spin-offs are applied.
    apply_cash_dividend : bool, optional
        If True, adjustments for cash dividends are applied.
    apply_stock_dividend : bool, optional
        If True, adjustments for stock dividends are applied.
    apply_security_splice : bool, optional
        If True, adjustments for security splices are applied.
    apply_others : str, optional
        A comma-separated list of names of custom adjustment types to apply.
    apply_all : bool, optional
        If True, applies all types of adjustments, both built-in and custom.

    Returns
    -------
    :py:class:`onetick.py.Source`
        A new source object with applied adjustments.

    See also
    --------
    **CORP_ACTIONS** OneTick event processor

    Examples
    --------
    >>> src = otp.DataSource('US_COMP',
    ...                      tick_type='TRD',
    ...                      start=otp.dt(2022, 5, 20, 9, 30),
    ...                      end=otp.dt(2022, 5, 26, 16))
    >>> df = otp.run(src, symbols='MKD', symbol_date=otp.date(2022, 5, 22))
    >>> df["PRICE"][0]
    0.0911
    >>> src = src.corp_actions(adjustment_date=otp.date(2022, 5, 22),
    ...                        fields="PRICE")
    >>> df = otp.run(src, symbols='MKD', symbol_date=otp.date(2022, 5, 22))
    >>> df["PRICE"][0]
    1.36649931675
    """
    return otp.functions.corp_actions(self, *args, **kwargs)
