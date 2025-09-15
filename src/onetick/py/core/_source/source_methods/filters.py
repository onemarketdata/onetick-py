import re
import warnings
from contextlib import suppress
from datetime import time
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Union
from onetick.py.backports import Literal

from onetick import py as otp
from onetick.py import types as ott
from onetick.py import utils
from onetick.py.core.column import _Column
from onetick.py.core.column_operations.base import _Operation
from onetick.py.core.eval_query import _QueryEvalWrapper
from onetick.py.otq import otq

from .misc import inplace_operation

if TYPE_CHECKING:
    from onetick.py.core.source import Source


def if_else(self: 'Source', condition: _Operation, if_expr, else_expr) -> 'otp.Column':
    """
    Shortcut for :meth:`~onetick.py.Source.apply` with lambda if-else expression

    Parameters
    ----------
    condition: :class:`Operation`
        - condition for matching ticks

    if_expr: :class:`Operation`, value
        - value or `Operation` to set if `condition` is true

    else_expr: :class:`Operation`, value
        - value or `Operation` to set if `condition` is false

    Returns
    -------
    Column

    Examples
    --------
    Basic example of apply if-else to a tick flow:

    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> data['Y'] = data.if_else(data['X'] > 2, 1, 0)
    >>> otp.run(data)
                         Time  X  Y
    0 2003-12-01 00:00:00.000  1  0
    1 2003-12-01 00:00:00.001  2  0
    2 2003-12-01 00:00:00.002  3  1

    You can also set column value via :class:`Operation`:

    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> data['Y'] = data.if_else(data['X'] > 2, data['X'] * 2, 0)
    >>> otp.run(data)
                         Time  X  Y
    0 2003-12-01 00:00:00.000  1  0
    1 2003-12-01 00:00:00.001  2  0
    2 2003-12-01 00:00:00.002  3  6

    See Also
    --------
    :py:meth:`onetick.py.Source.apply`
    """
    return self.apply(lambda tick: if_expr if condition else else_expr)


def where_clause(
    self: 'Source', condition, discard_on_match: bool = False, stop_on_first_mismatch: bool = False
) -> Tuple['Source', 'Source']:
    """
    Split source in two branches depending on ``condition``:
    one branch with ticks that meet the condition and
    the other branch with ticks that don't meet the condition.

    Original source object is not modified.

    Parameters
    ----------
    condition: :class:`Operation`, :func:`eval`
        Condition expression to filter ticks or object evaluating another query.
        In the latter case another query should have only one tick as a result with only one field.
    discard_on_match: bool
        Inverts the ``condition``.

        Ticks that don't meet the condition will be returned in the first branch,
        and ticks that meet the condition will be returned in the second branch.
    stop_on_first_mismatch: bool
        If set, no ticks will be propagated in the first branch
        starting with the first tick that does not meet the ``condition``.

        Other branch will contain all ticks starting with the first mismatch, even if they don't meet the condition.

    See Also
    --------
    | :meth:`Source.where`
    | :meth:`Source.__getitem__`
    | **WHERE_CLAUSE** OneTick event processor

    Examples
    --------

    Filtering based on expression:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> odd, even = data.where_clause(data['X'] % 2 == 1)
    >>> otp.run(odd)
                         Time  X
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.002  3
    >>> otp.run(even)
                         Time  X
    0 2003-12-01 00:00:00.001  2
    1 2003-12-01 00:00:00.003  4

    Filtering based on the result of another query:

    >>> another_query = otp.Tick(WHERE='mod(X, 2) = 1')
    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data, _ = data.where_clause(otp.eval(another_query))
    >>> otp.run(data)
                         Time  X
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.002  3

    Using ``discard_on_match`` parameter to invert the condition:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> even, odd = data.where_clause(data['X'] % 2 == 1, discard_on_match=True)
    >>> otp.run(even)
                         Time  X
    0 2003-12-01 00:00:00.001  2
    1 2003-12-01 00:00:00.003  4
    >>> otp.run(odd)
                         Time  X
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.002  3

    Using ``stop_on_first_mismatch`` parameter to not propagate ticks after first mismatch:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data, other = data.where_clause(data['X'] % 2 == 1, stop_on_first_mismatch=True)
    >>> otp.run(data)
            Time  X
    0 2003-12-01  1

    But other branch will contain all ticks after the mismatch, even if they don't meet the condition:

    >>> otp.run(other)
                         Time  X
    0 2003-12-01 00:00:00.001  2
    1 2003-12-01 00:00:00.002  3
    2 2003-12-01 00:00:00.003  4
    """
    if not isinstance(condition, (_Operation, _QueryEvalWrapper)):
        raise TypeError(f"Unsupported type of value for 'condition' parameter: {type(condition)}")

    if isinstance(condition, _Operation):
        condition = condition._make_python_way_bool_expression()
    if isinstance(condition, _QueryEvalWrapper):
        condition = condition.to_eval_string(self._tmp_otq)
    where_branch = self.copy(
        ep=otq.WhereClause(
            where=str(condition), discard_on_match=discard_on_match, stop_on_first_mismatch=stop_on_first_mismatch
        )
    )

    if_source = where_branch.copy()
    if_source.node().out_pin("IF")

    else_source = where_branch.copy()
    else_source.node().out_pin("ELSE")
    # TODO: add ability to remove then this ep, because it is required only for right output
    else_source.sink(otq.Passthrough())

    return if_source, else_source


def where(self: 'Source', condition, discard_on_match: bool = False, stop_on_first_mismatch: bool = False) -> 'Source':
    """
    Filter ticks that meet the ``condition``.

    Returns new object, original source object is not modified.

    Parameters
    ----------
    condition: :class:`Operation`, :func:`eval`
        Condition expression to filter ticks or object evaluating another query.
        In the latter case another query should have only one tick as a result with only one field.

    discard_on_match: bool
        Inverts the ``condition``.

        Ticks that don't meet the condition will be returned.

    stop_on_first_mismatch: bool
        If set, no ticks will be propagated starting with the first tick that does not meet the ``condition``.

    See Also
    --------
    | :meth:`Source.where_clause`
    | :meth:`Source.__getitem__`
    | **WHERE_CLAUSE** OneTick event processor

    Examples
    --------

    Filtering based on expression:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.where(data['X'] % 2 == 1)
    >>> otp.run(data)
                         Time  X
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.002  3

    Filtering based on the result of another query:

    >>> another_query = otp.Tick(WHERE='mod(X, 2) = 1')
    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.where(otp.eval(another_query))
    >>> otp.run(data)
                         Time  X
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.002  3

    Using ``discard_on_match`` parameter to invert the condition:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.where(data['X'] % 2 == 1, discard_on_match=True)
    >>> otp.run(data)
                         Time  X
    0 2003-12-01 00:00:00.001  2
    1 2003-12-01 00:00:00.003  4

    Using ``stop_on_first_mismatch`` parameter to not propagate ticks after first mismatch:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.where(data['X'] % 2 == 1, stop_on_first_mismatch=True)
    >>> otp.run(data)
            Time  X
    0 2003-12-01  1
    """
    return self.where_clause(
        condition, discard_on_match=discard_on_match, stop_on_first_mismatch=stop_on_first_mismatch
    )[0]


def _get_integer_slice(self: 'Source', item: slice) -> Optional['Source']:
    """
    Treat otp.Source object as a sequence of ticks
    and apply common python integer slicing logic to it.
    """
    start, stop, step = item.start, item.stop, item.step
    for v in (start, stop, step):
        if v is not None and not isinstance(v, int):
            return None

    # let's filter out cases that we don't want to support
    if step is not None and step <= 0:
        raise ValueError("step value can't be negative or zero")
    if stop is not None and stop == 0:
        raise ValueError("stop value can't be zero")
    # pylint: disable=chained-comparison
    if start and stop and start > 0 and stop > 0 and start >= stop:
        raise ValueError("stop value can't be less than start")
    if start and start < 0 and stop and stop > 0:
        raise ValueError("start value can't be negative when start value is positive")

    def add_counter(src, force=False):
        if '__NUM__' not in src.schema or force:
            if '__NUM__' in src.schema:
                src = src.drop('__NUM__')
            src = src.agg({'__NUM__': otp.agg.count()}, running=True, all_fields=True)
        return src

    result = self.copy()
    if start:
        if start > 0:
            result = add_counter(result)
            result, _ = result[result['__NUM__'] > start]
        if start < 0:
            result = result.last(-start)
    if stop:
        if stop > 0:
            result = add_counter(result)
            result, _ = result[result['__NUM__'] <= stop]
        if stop < 0:
            result = add_counter(result)
            last_ticks = result.last(-stop)
            last_ticks['__FLAG__'] = 1
            last_ticks = last_ticks[['__FLAG__', '__NUM__']]
            result = otp.join(
                result, last_ticks, on=result['__NUM__'] == last_ticks['__NUM__'], how='left_outer', rprefix='RIGHT'
            )
            result, _ = result[result['__FLAG__'] == 0]
            result = result.drop(['__FLAG__', 'RIGHT___NUM__'])
    if step:
        if step > 0:  # NOSONAR
            # resetting counter
            result = add_counter(result, force=True)
            result, _ = result[(result['__NUM__'] - 1) % step == 0]
    if '__NUM__' in result.schema:
        result = result.drop('__NUM__')
    return result


def __getitem__(self: 'Source', item):
    """
    Allows to express multiple things:

    - access a field by name

    - filter ticks by condition

    - select subset of fields

    - set order of fields

    Parameters
    ----------
    item: str, :class:`Operation`, :func:`eval`, list of str

        - ``str`` is to access column by name or columns specified by regex.

        - ``Operation`` to express filter condition.

        - ``otp.eval`` to express filter condition based on external query

        - ``List[str]`` select subset of specified columns or columns specified in regexes.

        - ``slice[List[str]::]`` set order of columns

        - ``slice[Tuple[str, Type]::]`` type defaulting

        - ``slice[:]`` alias to :meth:`Source.copy()`

        - ``slice[int:int:int]`` select ticks the same way as elements in python lists

    Returns
    -------
    Column, Source or tuple of Sources
        - Column if column name was specified.

        - Two sources if filtering expression or eval was provided: the first one is for ticks that pass condition
            and the second one that do not.

    Examples
    --------

    Access to the `X` column: add `Y` based on `X`

    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> data['Y'] = data['X'] * 2
    >>> otp.run(data)
                         Time  X  Y
    0 2003-12-01 00:00:00.000  1  2
    1 2003-12-01 00:00:00.001  2  4
    2 2003-12-01 00:00:00.002  3  6

    Filtering based on expression:

    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> data_more, data_less = data[(data['X'] > 2)]
    >>> otp.run(data_more)
                         Time  X
    0 2003-12-01 00:00:00.002  3
    >>> otp.run(data_less)
                         Time  X
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2

    Filtering based on the result of another query. Another query should
    have only one tick as a result with only one field (whatever it names).

    >>> exp_to_select = otp.Ticks(WHERE=['X > 2'])
    >>> data = otp.Ticks(X=[1, 2, 3], Y=['a', 'b', 'c'], Z=[.4, .3, .1])
    >>> data, _ = data[otp.eval(exp_to_select)]
    >>> otp.run(data)
                         Time  X  Y    Z
    0 2003-12-01 00:00:00.002  3  c  0.1

    Select subset of specified columns:

    >>> data = otp.Ticks(X=[1, 2, 3], Y=['a', 'b', 'c'], Z=[.4, .3, .1])
    >>> data = data[['X', 'Z']]
    >>> otp.run(data)
                         Time  X    Z
    0 2003-12-01 00:00:00.000  1  0.4
    1 2003-12-01 00:00:00.001  2  0.3
    2 2003-12-01 00:00:00.002  3  0.1

    Slice with list will keep all columns, but change order:

    >>> data=otp.Tick(Y=1, X=2, Z=3)
    >>> otp.run(data)
            Time  Y  X  Z
    0 2003-12-01  1  2  3
    >>> data = data[['X', 'Y']:]
    >>> otp.run(data)
            Time  X  Y  Z
    0 2003-12-01  2  1  3

    Slice can be used as short-cut for :meth:`Source.copy`:

    >>> data[:] # doctest: +ELLIPSIS
    <onetick.py.sources.ticks.Tick object at ...>

    Slices can use integers.
    In this case ticks are selected the same way as elements in python lists.

    >>> data = otp.Ticks({'A': [1, 2, 3, 4, 5]})

    Select first 3 ticks:

    >>> otp.run(data[:3])
                         Time  A
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2
    2 2003-12-01 00:00:00.002  3

    Skip first 3 ticks:

    >>> otp.run(data[3:])
                         Time  A
    0 2003-12-01 00:00:00.003  4
    1 2003-12-01 00:00:00.004  5

    Select last 3 ticks:

    >>> otp.run(data[-3:])
                         Time  A
    0 2003-12-01 00:00:00.002  3
    1 2003-12-01 00:00:00.003  4
    2 2003-12-01 00:00:00.004  5

    Skip last 3 ticks:

    >>> otp.run(data[:-3])
                         Time  A
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2

    Skip first and last tick:

    >>> otp.run(data[1:-1])
                         Time  A
    0 2003-12-01 00:00:00.001  2
    1 2003-12-01 00:00:00.002  3
    2 2003-12-01 00:00:00.003  4

    Select every second tick:

    >>> otp.run(data[::2])
                         Time  A
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.002  3
    2 2003-12-01 00:00:00.004  5

    Select every second tick, not including first and last tick:

    >>> otp.run(data[1:-1:2])
                         Time  A
    0 2003-12-01 00:00:00.001  2
    1 2003-12-01 00:00:00.003  4

    Regular expressions can be used to select fields too:

    >>> data = otp.Tick(A=1, AA=2, AB=3, B=4, BB=5, BA=6)
    >>> otp.run(data['A.*'])
            Time  A  AA  AB  BA
    0 2003-12-01  1   2   3   6

    Note that by default pattern is matched in any position of the string.
    Use characters ^ and $ to specify start and end of the string:

    >>> otp.run(data['^A'])
            Time  A  AA  AB
    0 2003-12-01  1   2   3

    Several regular expressions can be specified too:

    >>> otp.run(data[['^A+$', '^B+$']])
            Time  A  AA  B  BB
    0 2003-12-01  1   2  4   5

    See Also
    --------
    | :meth:`Source.table`: another and more generic way to select subset of specified columns
    | **PASSTHROUGH** OneTick event processor
    | **WHERE_CLAUSE** OneTick event processor

    """

    strict = True

    with suppress(TypeError):
        return self.where_clause(item)

    if isinstance(item, slice):

        result = self._get_integer_slice(item)
        if result:
            return result

        if item.step:
            raise AttributeError("Source columns slice with step set makes no sense")
        if item.start and item.stop:
            raise AttributeError("Source columns slice with both start and stop set is not available now")
        if not item.start and item.stop:
            raise AttributeError("Source columns slice with only stop set is not implemented yet")
        if item.start is None and item.stop is None:
            return self.copy()

        item = item.start
        strict = False

        if isinstance(item, tuple):
            item = dict([item])

        elif isinstance(item, list):
            if not item:
                return self.copy()
            item_type = list(set([type(x) for x in item]))

            if len(item_type) > 1:
                raise AttributeError(f"Different types {item_type} in slice list is not supported")
            if item_type[0] == tuple:
                item = dict(item)

    if isinstance(item, (list, str)):
        # check if item has regex characters
        item_list = [item] if isinstance(item, str) else item
        try:
            items_to_passthrough, use_regex = self._columns_names_regex(item_list)
        except TypeError:
            use_regex = False
        if use_regex:
            src = self.copy()
            src.sink(otq.Passthrough(fields=','.join(items_to_passthrough), use_regex=True))
            return src

    if isinstance(item, list):
        # ---------
        # TABLE
        # ---------
        items = []

        for it in item:
            if isinstance(it, _Column):
                items.append(it.name)
            elif isinstance(it, str):
                items.append(it)
            else:
                raise ValueError(f"It is not supported to filter '{it}' object of '{type(it)}' type")

        # validation
        for item in items:
            if item not in self.schema:
                existing_columns = ", ".join(self.schema.keys())
                raise AttributeError(f"There is no '{item}' column. There are existing columns: {existing_columns}")

        columns = {
            column_name: self.schema[column_name] for column_name in items if not self._check_key_is_meta(column_name)
        }

        return self.table(strict=strict, **columns)

    if isinstance(item, dict):
        return self.table(strict=strict, **item)

    # way to set type
    if isinstance(item, tuple):
        name, dtype = item
        warnings.warn(
            'Using tuple with name and type in otp.Source.__getitem__() is not supported anymore,'
            ' change your code to use otp.Source.schema object instead.',
            FutureWarning,
        )
        return self._set_field_by_tuple(name, dtype)

    name = item
    if name not in self.__dict__:
        raise KeyError(
            f'Column name {name} is not in the schema. Please, check that this column '
            'is in the schema or add it using the .schema property'
        )
    if not isinstance(self.__dict__[name], _Column):
        raise AttributeError(f"There is no '{name}' column")
    return self.__dict__[name]


@inplace_operation
def dropna(
    self: 'Source', how: Literal["any", "all"] = "any", subset: Optional[List[Any]] = None, inplace=False
) -> Optional['Source']:
    """
    Drops ticks that contain NaN values according to the policy in the ``how`` parameter

    Parameters
    ----------
    how: "any" or "all"

        ``any`` - filters out ticks if at least one field has NaN value

        ``all`` - filters out ticks if all fields have NaN values.
    subset: list of str
        list of columns to check for NaN values. If ``None`` then all columns are checked.
    inplace: bool
        the flag controls whether operation should be applied inplace.

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Drop ticks where **at least one** field has ``nan`` value.

    >>> data = otp.Ticks([[     'X',     'Y'],
    ...                   [     0.0,     1.0],
    ...                   [ otp.nan,     2.0],
    ...                   [     4.0, otp.nan],
    ...                   [ otp.nan, otp.nan],
    ...                   [     6.0,    7.0]])
    >>> data = data.dropna()
    >>> otp.run(data)[['X', 'Y']]
        X   Y
    0 0.0 1.0
    1 6.0 7.0

    Drop ticks where **all** fields have ``nan`` values.

    >>> data = otp.Ticks([[     'X',     'Y'],
    ...                   [     0.0,     1.0],
    ...                   [ otp.nan,     2.0],
    ...                   [     4.0, otp.nan],
    ...                   [ otp.nan, otp.nan],
    ...                   [     6.0,    7.0]])
    >>> data = data.dropna(how='all')
    >>> otp.run(data)[['X', 'Y']]
        X   Y
    0 0.0 1.0
    1 NaN 2.0
    2 4.0 NaN
    3 6.0 7.0

    Drop ticks where **all** fields in **subset** of columns have ``nan`` values.

    >>> data = otp.Ticks([[     'X',     'Y',    'Z'],
    ...                   [     0.0,     1.0, otp.nan],
    ...                   [ otp.nan,     2.0, otp.nan],
    ...                   [     4.0, otp.nan, otp.nan],
    ...                   [ otp.nan, otp.nan, otp.nan],
    ...                   [     6.0,     7.0, otp.nan]])
    >>> data = data.dropna(how='all', subset=['X', 'Y'])
    >>> otp.run(data)[['X', 'Y', 'Z']]
        X   Y   Z
    0 0.0 1.0 NaN
    1 NaN 2.0 NaN
    2 4.0 NaN NaN
    3 6.0 7.0 NaN

    """
    if how not in ["any", "all"]:
        raise ValueError(f"It is expected to see 'any' or 'all' values for 'how' parameter, but got '{how}'")

    condition = None
    columns = self.columns(skip_meta_fields=True)
    if subset is not None:
        for column_name in subset:
            if column_name not in columns:
                raise ValueError(f"There is no '{column_name}' column in the source")
            if columns[column_name] is not float:
                raise ValueError(f"Column '{column_name}' is not float type")

    for column_name, dtype in columns.items():
        if subset is not None and column_name not in subset:
            continue
        if dtype is float:
            if condition is None:
                condition = self[column_name] != ott.nan
            else:
                if how == "any":
                    condition &= self[column_name] != ott.nan
                elif how == "all":
                    condition |= self[column_name] != ott.nan

    self.sink(otq.WhereClause(where=str(condition)))
    return self


@inplace_operation
def time_filter(
    self: 'Source',
    discard_on_match: bool = False,
    start_time: Union[str, int, time] = 0,
    end_time: Union[str, int, time] = 0,
    day_patterns: Union[str, List[str]] = "",
    timezone=utils.default,  # type: ignore
    end_time_tick_matches: bool = False,
    inplace=False,
) -> Optional['Source']:
    """
    Filters ticks by time.

    Parameters
    ----------
    discard_on_match : bool, optional
        If ``True``, then ticks that match the filter will be discarded.
        Otherwise, only ticks that match the filter will be passed.
    start_time : str or int or :py:class:`datetime.time`, optional
        Start time of the filter, string must be in the format ``HHMMSSmmm``.
        Default value is 0.
    end_time : str or int or :py:class:`datetime.time`, optional
        End time of the filter, string must be in the format ``HHMMSSmmm``.
        To filter ticks for an entire day, this parameter should be set to 240000000.
        Default value is 0.
    day_patterns : list or str
        Pattern or list of patterns that determines days for which the ticks can be propagated.
        A tick can be propagated if its date matches one or more of the patterns.
        Three supported pattern formats are:

        1. ``month.week.weekdays``, 0 month means any month, 0 week means any week,
            6 week means the last week of the month for a given weekday(s),
            weekdays are digits for each day, 0 being Sunday.

        2. ``month/day``, 0 month means any month.

        3. ``year/month/day``, 0 year means any year, 0 month means any month.

    timezone : str, optional
        Timezone of the filter.
        Default value is ``configuration.config.tz``
        or timezone set in the parameter of :py:func:`onetick.py.run`.
    end_time_tick_matches : bool, optional
        If ``True``, then the end time is inclusive.
        Otherwise, the end time is exclusive.
    inplace : bool, optional
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object. Default value is ``False``.

    Returns
    -------
    :class:`Source` or ``None``
        Returns ``None`` if ``inplace=True``.

    See also
    --------
    **TIME_FILTER** OneTick event processor

    Examples
    --------
    >>> data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL')
    >>> data = data.time_filter(start_time='000000001', end_time='000000003')
    >>> otp.run(data, start=otp.dt(2022, 3, 1), end=otp.dt(2022, 3, 2))
                         Time  PRICE  SIZE
    0 2022-03-01 00:00:00.001    1.4    10
    1 2022-03-01 00:00:00.002    1.4    50

    """
    if timezone is utils.default:
        # doesn't work without expr for some reason
        timezone = 'expr(_TIMEZONE)'

    if day_patterns:
        if isinstance(day_patterns, str):
            day_patterns = [day_patterns]
        for day_pattern in day_patterns:
            if not re.match(r"(^\d\d?\.[0-6].\d\d?$)|(^\d\d?\/\d\d?$)|(^\d{1,4}\/\d\d?\/\d\d?$)", day_pattern):
                raise ValueError(f"Invalid day pattern: {day_pattern}")

    if isinstance(start_time, time):
        start_time = start_time.strftime('%H%M%S%f')[:-3]

    if isinstance(end_time, time):
        end_time = end_time.strftime('%H%M%S%f')[:-3]

    day_patterns = ",".join(day_patterns)
    self.sink(
        otq.TimeFilter(
            discard_on_match=discard_on_match,
            start_time=start_time,
            end_time=end_time,
            timezone=timezone,
            day_patterns=day_patterns,
            end_time_tick_matches=end_time_tick_matches,
        )
    )
    return self


@inplace_operation
def skip_bad_tick(
    self: 'Source',
    field: Union[str, _Column],
    discard_on_match: bool = False,
    jump_threshold: float = 2.0,
    num_neighbor_ticks: int = 5,
    use_absolute_values: bool = False,
    inplace=False,
) -> Optional['Source']:
    """
    Discards ticks based on whether the value of the attribute specified by ``field`` differs from the value
    of the same attribute in the surrounding ticks more times than a given threshold.
    Uses SKIP_BAD_TICK EP.

    Parameters
    ----------
    field: str, :py:class:`~onetick.py.Column`
        Name of the field (must be present in the input tick descriptor).
    discard_on_match: bool
        When set to ``True`` only ticks that did not match the filter are propagated,
        otherwise ticks that satisfy the filter condition are propagated.
    jump_threshold: float
        A threshold to determine if a tick is "good" or "bad."

        Good ticks are the ticks whose ``field`` value differs less than ``jump_threshold`` times
        from the ``field``'s value of less than or half of the surrounding ``num_neighbor_ticks`` ticks.
    num_neighbor_ticks: int
        The number of ticks before this tick and after this tick to compare a tick against.
    use_absolute_values: bool
        When set to ``True``, use absolute values of numbers when checking whether they are within the jump threshold.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    **SKIP_BAD_TICK** OneTick event processor

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------
    Keep ticks whose price did not jump by more than 20% relative to the surrounding ticks:

    >>> data = otp.Ticks(X=[10, 11, 15, 11, 9, 10])
    >>> data = data.skip_bad_tick(field="X", jump_threshold=1.2, num_neighbor_ticks=1)
    >>> otp.run(data)
                         Time   X
    0 2003-12-01 00:00:00.000  10
    1 2003-12-01 00:00:00.001  11
    2 2003-12-01 00:00:00.003  11
    3 2003-12-01 00:00:00.005  10

    Same example, but with passing column as ``field`` parameter:

    >>> data = otp.Ticks(X=[10, 11, 15, 11, 9, 10])
    >>> data = data.skip_bad_tick(field=data["X"], jump_threshold=1.2, num_neighbor_ticks=1)
    >>> otp.run(data)
                         Time   X
    0 2003-12-01 00:00:00.000  10
    1 2003-12-01 00:00:00.001  11
    2 2003-12-01 00:00:00.003  11
    3 2003-12-01 00:00:00.005  10

    If you want to keep only "bad ticks", which don't match the filter,
    set ``discard_on_match`` parameter to ``True``:

    >>> data = otp.Ticks(X=[10, 11, 15, 11, 9, 10])
    >>> data = data.skip_bad_tick(field=data["X"], jump_threshold=1.2, num_neighbor_ticks=1, discard_on_match=True)
    >>> otp.run(data)
                         Time   X
    0 2003-12-01 00:00:00.002  15
    1 2003-12-01 00:00:00.004   9

    In case, if you need to compare values on an absolute basis, set ``use_absolute_values`` parameter to ``True``:

    >>> data = otp.Ticks(X=[10, -11, -15, 11, 9, 10])
    >>> data = data.skip_bad_tick(field=data["X"], jump_threshold=1.2, num_neighbor_ticks=1, use_absolute_values=True)
    >>> otp.run(data)
                         Time   X
    0 2003-12-01 00:00:00.000  10
    1 2003-12-01 00:00:00.001  -11
    2 2003-12-01 00:00:00.003  11
    3 2003-12-01 00:00:00.005  10
    """
    if isinstance(field, _Column):
        field = field.name

    if field not in self.schema:
        raise ValueError(f'Field {field} not in the schema.')

    self.sink(otq.SkipBadTick(
        discard_on_match=discard_on_match,
        jump_threshold=jump_threshold,
        field=field,
        num_neighbor_ticks=num_neighbor_ticks,
        use_absolute_values=use_absolute_values,
    ))

    return self


@inplace_operation
def character_present(
    self: 'Source',
    field: Union[str, _Column],
    characters: Union[str, List[str]],
    characters_field: Union[str, _Column] = "",
    discard_on_match: bool = False,
    inplace: bool = False,
):
    """
    Propagates ticks based on whether the value of the field specified by `field` contains a character
    in the set of characters specified by `characters`.
    Uses **CHARACTER_PRESENT** EP.

    Parameters
    ----------
    field: str, :py:class:`~onetick.py.Column`
        Name of the field (must be present in the input tick descriptor).
    characters: str, List[str]
        A set of characters that are searched for in the value of the `field`.
        If set as string, works as list of characters.
    characters_field: str, :py:class:`~onetick.py.Column`
        If specified, will take a current value of that field and append it to `characters`, if any.
    discard_on_match: bool
        When set to ``True`` only ticks that did not match the filter are propagated,
        otherwise ticks that satisfy the filter condition are propagated.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    **CHARACTER_PRESENT** OneTick event processor

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Select ticks that have the N or T in EXCHANGE field

    >>> data = otp.DataSource('TEST_DATABASE', tick_type='TRD', symbols='A')  # doctest: +SKIP
    >>> data = data[['PRICE', 'SIZE', 'EXCHANGE']]  # doctest: +SKIP
    >>> data = data.character_present(field=data['EXCHANGE'], characters='NT')  # doctest: +SKIP
    >>> otp.run(data)  # doctest: +SKIP
                         Time  PRICE   SIZE EXCHANGE
    0 2003-12-01 00:00:00.000  28.44  55100        N
    1 2003-12-01 00:00:00.001  28.44    100        T
    2 2003-12-01 00:00:00.002  28.44    200        T
    3 2003-12-01 00:00:00.003  28.45    100        T
    4 2003-12-01 00:00:00.004  28.44    500        T

    Select ticks that have the N or T in EXCHANGE field and character set in OLD_EXCHANGE field

    >>> data = otp.DataSource('TEST_DATABASE', tick_type='TRD', symbols='A')  # doctest: +SKIP
    >>> data = data.character_present(  # doctest: +SKIP
    ...     field=data['EXCHANGE'], characters='NT', characters_field=data['OLD_EXCHANGE'],
    ... )
    >>> data = data[['PRICE', 'SIZE', 'EXCHANGE']]  # doctest: +SKIP
    >>> otp.run(data)  # doctest: +SKIP
                         Time  PRICE   SIZE EXCHANGE
    0 2003-12-01 00:00:00.000  28.44  55100        N
    1 2003-12-01 00:00:00.001  28.44    100        B
    2 2003-12-01 00:00:00.002  28.44    200        B
    3 2003-12-01 00:00:00.003  28.45    100        T
    4 2003-12-01 00:00:00.004  28.44    200        T
    """
    if isinstance(field, _Column):
        field = field.name

    if isinstance(characters_field, _Column):
        characters_field = characters_field.name

    if isinstance(characters, list):
        characters = ''.join(characters)

    for name, value in zip(['field', 'characters_field'], [field, characters_field]):
        if not value:
            continue

        if value not in self.schema:
            raise ValueError(f'Field {value}, passed as parameter `{name}`, not in the schema.')

        if not (self.schema[value] == str or issubclass(self.schema[value], otp.string)):
            raise TypeError(
                f'Field {value}, passed as parameter `{name}`, has incompatible type: {self.schema[value]}, '
                f'expected: str',
            )

    self.sink(otq.CharacterPresent(
        field=field,
        characters=characters,
        characters_field=characters_field,
        discard_on_match=discard_on_match,
    ))

    return self
