import re
import warnings
from contextlib import suppress
from datetime import time
from typing import TYPE_CHECKING, Any, Optional, Union, Literal
from collections.abc import Sequence

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
) -> tuple['Source', 'Source']:
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

        - ``list[str]`` select subset of specified columns or columns specified in regexes.

        - ``slice[list[str]::]`` set order of columns

        - ``slice[tuple[str, type]::]`` type defaulting

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
            item_type = list(set(type(x) for x in item))

            if len(item_type) > 1:
                raise AttributeError(f"Different types {item_type} in slice list is not supported")
            if item_type[0] is tuple:
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
    self: 'Source', how: Literal["any", "all"] = "any", subset: Optional[list[Any]] = None, inplace=False
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

    Drop ticks where **at least one** field has NaN value.

    >>> data = otp.Ticks([[     'X',     'Y'],
    ...                   [     0.0,     1.0],
    ...                   [ otp.nan,     2.0],
    ...                   [     4.0, otp.nan],
    ...                   [ otp.nan, otp.nan],
    ...                   [     6.0,    7.0]])
    >>> data = data.dropna()
    >>> otp.run(data)
                         Time    X    Y
    0 2003-12-01 00:00:00.000  0.0  1.0
    1 2003-12-01 00:00:00.004  6.0  7.0

    Drop ticks where **all** fields have NaN values.

    >>> data = otp.Ticks([[     'X',     'Y'],
    ...                   [     0.0,     1.0],
    ...                   [ otp.nan,     2.0],
    ...                   [     4.0, otp.nan],
    ...                   [ otp.nan, otp.nan],
    ...                   [     6.0,    7.0]])
    >>> data = data.dropna(how='all')
    >>> otp.run(data)
                         Time    X    Y
    0 2003-12-01 00:00:00.000  0.0  1.0
    1 2003-12-01 00:00:00.001  NaN  2.0
    2 2003-12-01 00:00:00.002  4.0  NaN
    3 2003-12-01 00:00:00.004  6.0  7.0

    Drop ticks where **all** fields in ``subset`` of columns have NaN values.

    >>> data = otp.Ticks([[     'X',     'Y',    'Z'],
    ...                   [     0.0,     1.0, otp.nan],
    ...                   [ otp.nan,     2.0, otp.nan],
    ...                   [     4.0, otp.nan, otp.nan],
    ...                   [ otp.nan, otp.nan, otp.nan],
    ...                   [     6.0,     7.0, otp.nan]])
    >>> data = data.dropna(how='all', subset=['X', 'Y'])
    >>> otp.run(data)
                         Time    X    Y   Z
    0 2003-12-01 00:00:00.000  0.0  1.0 NaN
    1 2003-12-01 00:00:00.001  NaN  2.0 NaN
    2 2003-12-01 00:00:00.002  4.0  NaN NaN
    3 2003-12-01 00:00:00.004  6.0  7.0 NaN
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
    day_patterns: Union[str, list[str]] = "",
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
        Otherwise, only ticks that match the filter will be propagated.
    start_time : str or int or :py:class:`datetime.time`, optional
        Start time of the filter, string must be in the format ``HHMMSSmmm`` or ``HH:MM:SS[.mmm]``.
        Default value is 0.
    end_time : str or int or :py:class:`datetime.time`, optional
        End time of the filter, string must be in the format ``HHMMSSmmm`` or ``HH:MM:SS[.mmm]``.
        To filter ticks for an entire day, this parameter should be set to 24:00:00.
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
        Default value is :attr:`otp.config.tz <onetick.py.configuration.Config.tz>`
        or timezone set in the parameter of :func:`otp.run <onetick.py.run>`.
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

    Filter ticks from 09:30:00 to 09:30:00.001:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data[['PRICE', 'SIZE']]
    >>> data = data.time_filter(start_time='09:30:00', end_time='09:30:00.001')
    >>> otp.run(data, date=otp.dt(2024, 2, 1))
                                Time   PRICE  SIZE
    0  2024-02-01 09:30:00.000961260  184.01   302
    1  2024-02-01 09:30:00.000961491  184.00   100
    2  2024-02-01 09:30:00.000961701  184.00     1
    3  2024-02-01 09:30:00.000973163  184.00     1
    4  2024-02-01 09:30:00.000973355  184.00     5
    5  2024-02-01 09:30:00.000973517  184.00     5
    6  2024-02-01 09:30:00.000973674  184.00     5
    7  2024-02-01 09:30:00.000980967  184.00     1
    8  2024-02-01 09:30:00.000984514  184.00    10
    9  2024-02-01 09:30:00.000984626  184.00     1
    10 2024-02-01 09:30:00.000984730  184.00     1
    11 2024-02-01 09:30:00.000989008  184.00     1
    12 2024-02-01 09:30:00.000992129  184.00     2
    13 2024-02-01 09:30:00.000994257  184.00     2
    14 2024-02-01 09:30:00.000994541  184.00     3
    15 2024-02-01 09:30:00.000996695  184.00    10
    16 2024-02-01 09:30:00.000999752  184.00   100

    Filter ticks from 09:30:00 to 09:30:00.005 every Monday:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data[['PRICE', 'SIZE']]
    >>> data = data.time_filter(start_time='09:30:00', end_time='09:30:00.005', day_patterns='0.0.1')
    >>> otp.run(data, start=otp.dt(2024, 2, 1), end=otp.dt(2024, 3, 1))
                               Time   PRICE  SIZE
    0 2024-02-05 09:30:00.002470305  188.13    26
    1 2024-02-05 09:30:00.003490655  188.15    26
    2 2024-02-05 09:30:00.004413985  188.13     1
    3 2024-02-12 09:30:00.004972167  188.41    50
    4 2024-02-26 09:30:00.003312559  182.33   100
    5 2024-02-26 09:30:00.003316234  182.33   100
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
                         Time    X
    0 2003-12-01 00:00:00.000   10
    1 2003-12-01 00:00:00.001  -11
    2 2003-12-01 00:00:00.003   11
    3 2003-12-01 00:00:00.005   10
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
    characters: Union[str, list[str]],
    characters_field: Union[str, _Column] = "",
    discard_on_match: bool = False,
    inplace: bool = False,
):
    """
    Propagates ticks based on whether the value of the field specified by ``field`` contains a character
    in the set of characters specified by ``characters``.
    Uses **CHARACTER_PRESENT** EP.

    Parameters
    ----------
    field: str, :py:class:`~onetick.py.Column`
        Name of the field (must be present in the input tick descriptor).
    characters: str, list[str]
        A set of characters that are searched for in the value of the ``field``.
        If set as string, works as list of characters.
    characters_field: str, :py:class:`~onetick.py.Column`
        If specified, will take a current value of that field and append it to ``characters``, if any.
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

    Select ticks that have the N or T in EXCHANGE field:

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

    Select ticks that have the N or T in EXCHANGE field and character set in OLD_EXCHANGE field:

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

        if not (self.schema[value] is str or issubclass(self.schema[value], otp.string)):
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


@inplace_operation
def value_present(
    self: 'Source',
    field: Union[str, _Column],
    values: list[Any],
    discard_on_match: bool = False,
    inplace: bool = False,
):
    """
    Propagates ticks based on whether the value of the ``field`` contains one of the list of ``values``.

    Parameters
    ----------
    field: str, :py:class:`~onetick.py.Column`
        Name of the field (must be present in the input tick descriptor).
    values: str, list[str]
        A set of values that are searched for in the value of the ``field``.
    discard_on_match: bool
        When set to ``True`` only ticks that did not match the filter are propagated,
        otherwise ticks that satisfy the filter condition are propagated.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    **VALUE_PRESENT** OneTick event processor
    :meth:`onetick.py.Operation.isin`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Select ticks that have specified values in field A:

    >>> data = otp.Ticks(A=[1, 2, 3, 4, 5])
    >>> data = data.value_present(field='A', values=[2, 3])
    >>> otp.run(data)
                         Time  A
    0 2003-12-01 00:00:00.001  2
    1 2003-12-01 00:00:00.002  3

    Use parameter ``discard_on_match`` to filter values *out*:

    >>> data = otp.Ticks(A=[1, 2, 3, 4, 5])
    >>> data = data.value_present(field='A', values=[2, 3], discard_on_match=True)
    >>> otp.run(data)
                         Time  A
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.003  4
    2 2003-12-01 00:00:00.004  5
    """
    if isinstance(field, _Column):
        field = field.name

    if field not in self.schema:
        raise ValueError(f"Field '{field}' is not in schema")

    if not isinstance(values, Sequence) or isinstance(values, str):
        raise ValueError(f"Parameter 'values' must be a list or other sequence, got {type(values)}")

    if not values:
        raise ValueError("Parameter 'values' is empty")

    try:
        values_type = ott.get_base_type(ott.get_type_by_objects(values))
    except TypeError as e:
        raise ValueError("All values must be of the same type in 'values' parameter") from e

    field_type = ott.get_base_type(self.schema[field])
    if values_type != field_type:
        raise ValueError(f"Field '{field}' and parameter 'values' have different types: {field_type} and {values_type}")

    values_str = ','.join(ott.value2str(v) for v in values)

    self.sink(otq.ValuePresent(
        field=field,
        values=values_str,
        discard_on_match=discard_on_match,
    ))

    return self


def primary_exch(self: 'Source', discard_on_match: bool = False) -> tuple['Source', 'Source']:
    """
    Propagates the tick if its exchange is the PRIMARY exchange of the security.

    The primary exchange information is supplied through the Reference Database.

    It expects the security level symbol (IBM, not IBM.N) and works
    by looking for a field called ``EXCHANGE`` and filtering out ticks where the field does not match
    the primary exchange for the security.

    .. note::
        This EP may not work correctly with OneTick Cloud databases, due to differences
        in format of exchange names in RefDB and in tick data.

    Parameters
    ----------
    discard_on_match: bool
        When set to ``True`` only ticks from non-primary exchange are propagated,
        otherwise ticks from primary exchange are propagated.

    See also
    --------
    **PRIMARY_EXCH** OneTick event processor

    Returns
    -------
    Two :class:`Source` for each of if-else branches

    Examples
    --------

    Get ticks from primary exchange:

    >>> src = otp.DataSource('SOME_DB', tick_type='TRD', symbols='AAA')              # doctest: +SKIP
    >>> src, _ = src.primary_exch()                                                  # doctest: +SKIP
    >>> otp.run(src, date=otp.date(2003, 12, 1), symbol_date=otp.date(2003, 12, 1))  # doctest: +SKIP
                         Time  PRICE  SIZE EXCHANGE
    0 2003-12-01 00:00:00.001   26.5   150        B
    1 2003-12-01 00:00:00.002   25.7    20        B

    Get all ticks, but mark ticks from primary exchange in column ``T``:

    >>> src = otp.DataSource('SOME_DB', tick_type='TRD', symbols='AAA', date=otp.date(2003, 12, 1))  # doctest: +SKIP
    >>> primary, other = src.primary_exch()                                                          # doctest: +SKIP
    >>> primary['T'] = 1                                                                             # doctest: +SKIP
    >>> other['T'] = 0                                                                               # doctest: +SKIP
    >>> data = otp.merge([primary, other])                                                           # doctest: +SKIP
    >>> otp.run(src, symbol_date=otp.date(2003, 12, 1))                                              # doctest: +SKIP
                         Time  PRICE  SIZE EXCHANGE  T
    0 2003-12-01 00:00:00.000   25.2   100        A  0
    1 2003-12-01 00:00:00.001   26.5   150        B  1
    2 2003-12-01 00:00:00.002   25.7    20        B  1
    3 2003-12-01 00:00:00.003   24.8    40        A  0
    """
    source = self.copy(ep=otq.PrimaryExch(discard_on_match=discard_on_match))

    if_source = source.copy()
    if_source.node().out_pin("IF")

    else_source = source.copy()
    else_source.node().out_pin("ELSE")

    return if_source, else_source


@inplace_operation
def show_hidden_ticks(self: 'Source', inplace: bool = False) -> 'Source':
    """
    Propagates all of a tick's fields without changing their values. Propagates all ticks,
    even those with a status not equal to 0, which are normally hidden.

    Use this method to display all original ticks and correction ticks in the same time series in the same
    time sequence in which they were originally sent with a filter to remove unneeded **TICK_STATUS** values.

    Parameters
    ----------
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    **SHOW_HIDDEN_TICKS** OneTick event processor
    :meth:`~onetick.py.Source.show_corrected_ticks`
    :meth:`~onetick.py.Source.correct_tick_filter`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Show hidden ticks and filter them by choosing non-zero *TICK_STATUS* values:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data.show_hidden_ticks()
    >>> data = data.where(data['TICK_STATUS'] != 0)
    >>> data = data[['PRICE', 'SIZE', 'TICK_STATUS']]
    >>> otp.run(data, date=otp.dt(2024, 2, 1))
                                Time     PRICE  SIZE  TICK_STATUS
    0  2024-02-01 11:50:15.621123654  185.6950     1            1
    1  2024-02-01 14:43:17.690903111  185.6950     1            4
    2  2024-02-01 14:43:17.690903111  185.6950     1            7
    3  2024-02-01 15:36:14.597279750  186.6100    50            1
    4  2024-02-01 15:42:02.658055953  186.8700     1            1
    5  2024-02-01 15:42:02.661911428  186.8700     1            1
    6  2024-02-01 15:46:38.890972031  186.6100    50            4
    7  2024-02-01 15:46:38.890972031  186.6100    50            7
    8  2024-02-01 15:46:38.891369862  186.6100    45            1
    9  2024-02-01 15:49:02.043717846  186.6100    45            4
    10 2024-02-01 15:49:02.043717846  186.6100    45            7
    11 2024-02-01 15:57:29.621873687  186.7078   306            1
    12 2024-02-01 16:06:23.115605520  186.7078   306            4
    13 2024-02-01 16:06:23.115605520  186.7078   306            7
    14 2024-02-01 16:15:02.784032763  186.8700     1            4
    15 2024-02-01 16:15:02.784032763  186.8700     1            7
    16 2024-02-01 16:15:02.801559379  186.8700     1            4
    17 2024-02-01 16:15:02.801559379  186.8700     1            7
    """
    self.sink(otq.ShowHiddenTicks())
    return self


@inplace_operation
def hide_ticks_with_status(self: 'Source', inplace: bool = False) -> 'Source':
    """
    Makes visible ticks with **TICK_STATUS** not equal to 0 or 8 (which stands for INSERT) invisible,
    so they are treated the same way all invisible ticks are treated.

    This method has effect that is opposite to the effect of :meth:`~onetick.py.Source.show_hidden_ticks`.
    It can be used to construct or fix order books, as well as to inject or modify correction/cancellation ticks.

    Parameters
    ----------
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    | **HIDE_TICKS_WITH_STATUS** OneTick event processor
    | :meth:`~onetick.py.Source.show_hidden_ticks`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Hidden ticks with non-zero **TICK_STATUS** can be seen with :meth:`~onetick.py.Source.show_hidden_ticks`:

    >>> data = otp.DataSource(db='CME_SAMPLE', tick_type='PRL_FULL', symbols=r'NQ\\H24')
    >>> data = data.show_hidden_ticks()
    >>> data = data[['UPDATE_TYPE', 'PRICE', 'SIZE', 'RECORD_TYPE', 'TICK_STATUS']]
    >>> data = data.limit(100)
    >>> otp.run(data, date=otp.dt(2024, 2, 1))  # doctest: +ELLIPSIS
                                Time UPDATE_TYPE     PRICE  SIZE RECORD_TYPE  TICK_STATUS
    0  2024-02-01 00:00:00.211493687                   NaN     0           B           32
    1  2024-02-01 00:00:00.211493687           A  17317.75     1           R            0
    2  2024-02-01 00:00:00.211493687                   NaN     0           E           33
    3  2024-02-01 00:00:00.211541475                   NaN     0           B           32
    4  2024-02-01 00:00:00.211541475           D  17317.00     0           R            0
    ...

    Use this method to make them invisible again:

    >>> data = otp.DataSource(db='CME_SAMPLE', tick_type='PRL_FULL', symbols=r'NQ\\H24')
    >>> data = data.show_hidden_ticks()
    >>> data = data.hide_ticks_with_status()
    >>> data = data[['UPDATE_TYPE', 'PRICE', 'SIZE', 'RECORD_TYPE', 'TICK_STATUS']]
    >>> data = data.limit(100)
    >>> otp.run(data, date=otp.dt(2024, 2, 1))  # doctest: +ELLIPSIS
                                Time UPDATE_TYPE     PRICE  SIZE RECORD_TYPE  TICK_STATUS
    0  2024-02-01 00:00:00.211493687           A  17317.75     1           R            0
    1  2024-02-01 00:00:00.211541475           D  17317.00     0           R            0
    2  2024-02-01 00:00:00.211630059           A  17317.25     1           R            0
    3  2024-02-01 00:00:00.211642827           A  17317.75     1           R            0
    4  2024-02-01 00:00:00.211654537           D  17317.25     0           R            0
    ...
    """
    self.sink(otq.HideTicksWithStatus())
    return self


@inplace_operation
def show_initialization_ticks(self: 'Source', inplace: bool = False) -> 'Source':
    """
    Propagates only the ticks that arrived before the start time of the query (initialization ticks)
    and makes them visible. Normally such ticks are not visible.

    The ticks are propagated in the order opposite to the order of their original timestamps.
    The timestamp of the ticks is set to the start time of the query.
    To see the original timestamp, add another field to the tick and assign original timestamp to it
    before calling this method.

    To avoid affecting results of the event processors which require initialization ticks,
    while observing the initialization ticks they are receiving,
    this method should be applied on a separate branch of the query.

    Parameters
    ----------
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    | **SHOW_INITIALIZATION_TICKS** OneTick event processor
    | :meth:`~onetick.py.Source.intercept_initialization_ticks`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Ticks taken from before the start time of the query
    with ``back_to_first_tick`` parameter are initialization ticks.

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL', date=otp.dt(2024, 2, 1),
    ...                       back_to_first_tick=otp.Day(1), max_back_ticks_to_prepend=100)
    >>> data = data[['PRICE', 'SIZE']]
    >>> data['TICK_TIME'] = data['TIMESTAMP']
    >>> data = data.show_initialization_ticks()
    >>> otp.run(data)  # doctest: +ELLIPSIS
             Time     PRICE  SIZE                     TICK_TIME
    0  2024-02-01  185.6800    10 2024-01-31 19:59:57.877747563
    1  2024-02-01  185.6999     5 2024-01-31 19:59:53.314804162
    2  2024-02-01  185.7000   100 2024-01-31 19:59:53.302285347
    3  2024-02-01  185.6900     1 2024-01-31 19:59:53.019288051
    4  2024-02-01  185.7000   440 2024-01-31 19:59:51.062739337
    ...
    """
    self.sink(otq.ShowInitializationTicks())
    return self


@inplace_operation
def intercept_initialization_ticks(self: 'Source', inplace: bool = False) -> 'Source':
    """
    Removes initialization ticks (the ticks that arrived before the start time of the query),
    thus preventing them from delivery to the client application.

    Parameters
    ----------
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    | **INTERCEPT_INITIALIZATION_TICKS** OneTick event processor
    | :meth:`~onetick.py.Source.show_initialization_ticks`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Ticks taken from before the start time of the query
    with ``back_to_first_tick`` parameter are initialization ticks
    and can be seen with :meth:`~onetick.py.Source.show_initialization_ticks`:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL', date=otp.dt(2024, 2, 1),
    ...                       back_to_first_tick=otp.Day(1), max_back_ticks_to_prepend=100)
    >>> data = data[['PRICE', 'SIZE']]
    >>> data['TICK_TIME'] = data['TIMESTAMP']
    >>> data = data.show_initialization_ticks()
    >>> otp.run(data)  # doctest: +ELLIPSIS
             Time     PRICE  SIZE                     TICK_TIME
    0  2024-02-01  185.6800    10 2024-01-31 19:59:57.877747563
    1  2024-02-01  185.6999     5 2024-01-31 19:59:53.314804162
    2  2024-02-01  185.7000   100 2024-01-31 19:59:53.302285347
    3  2024-02-01  185.6900     1 2024-01-31 19:59:53.019288051
    4  2024-02-01  185.7000   440 2024-01-31 19:59:51.062739337
    ...

    Intercepting initialization ticks will remove them from the data flow:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL', date=otp.dt(2024, 2, 1),
    ...                       back_to_first_tick=otp.Day(1), max_back_ticks_to_prepend=100)
    >>> data = data[['PRICE', 'SIZE']]
    >>> data['TICK_TIME'] = data['TIMESTAMP']
    >>> data = data.intercept_initialization_ticks()
    >>> data = data.show_initialization_ticks()
    >>> otp.run(data)  # doctest: +ELLIPSIS
    Empty DataFrame
    ...
    """
    self.sink(otq.InterceptInitializationTicks())
    return self


@inplace_operation
def show_corrected_ticks(self: 'Source', inplace: bool = False) -> 'Source':
    """
    Shows ticks which were corrected or canceled within the interval of the query.
    Only corrected and correction ticks will be propagated.

    A *TICK_STATUS* field is added to each tick.

    For Corrections:

    * Old tick TICK_STATUS = 5
    * New tick TICK_STATUS = 6

    For Cancellations:

    * Old tick TICK_STATUS = 4
    * New tick TICK_STATUS = 7

    Parameters
    ----------
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    **SHOW_CORRECTED_TICKS** OneTick event processor
    :meth:`~onetick.py.Source.show_hidden_ticks`
    :meth:`~onetick.py.Source.correct_tick_filter`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Show corrected or canceled ticks:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data.show_corrected_ticks()
    >>> data = data[['PRICE', 'SIZE', 'TICK_STATUS']]
    >>> otp.run(data, date=otp.dt(2024, 2, 1))
                                Time     PRICE  SIZE  TICK_STATUS
    0  2024-02-01 14:43:17.690903111  185.6950     1            4
    1  2024-02-01 14:43:17.690903111  185.6950     1            7
    2  2024-02-01 15:46:38.890972031  186.6100    50            4
    3  2024-02-01 15:46:38.890972031  186.6100    50            7
    4  2024-02-01 15:49:02.043717846  186.6100    45            4
    5  2024-02-01 15:49:02.043717846  186.6100    45            7
    6  2024-02-01 16:06:23.115605520  186.7078   306            4
    7  2024-02-01 16:06:23.115605520  186.7078   306            7
    8  2024-02-01 16:15:02.784032763  186.8700     1            4
    9  2024-02-01 16:15:02.784032763  186.8700     1            7
    10 2024-02-01 16:15:02.801559379  186.8700     1            4
    11 2024-02-01 16:15:02.801559379  186.8700     1            7
    """
    self.sink(otq.ShowCorrectedTicks())
    if 'TICK_STATUS' not in self.schema:
        self.schema['TICK_STATUS'] = int
    return self


@inplace_operation
def correct_tick_filter(
    self: 'Source',
    discard_on_match: Optional[bool] = None,
    as_of_time: otp.datetime | int | str | Literal['NOW'] | None = None,
    inplace: bool = False,
) -> 'Source':
    """
    This allows one to view data as it was as of any point in time.

    Correction or cancellation ticks will be applied if they arrived before the time
    specified by ``as_of_time``,
    and if they were applied to the ticks that arrived after that ``as_of_time``.

    Parameters
    ----------
    discard_on_match: bool
        If this parameter is False (default), the output is a time series of ticks,
        with correction and cancellations that arrived after the ``as_of_time`` not applied to the ticks
        that arrived before or at the the ``as_of_time``.

        If this parameter is True, the correct ticks, incorrect copies of which arrived before or at the ``as_of_time``,
        which were corrected after ``as_of_time`` will be propagated.

    as_of_time: :class:`otp.datetime <onetick.py.datetime>` or int or str

        Datetime object or integer/string in the YYYYMMDDhhmmss format.

        The cut-off time, in the timezone of the query, for the correction tick or cancellation to be applied
        (the correction or cancellation must happen before or at this cut-off time,
        or the original (corrected or canceled) tick must arrive after this cutoff time,
        for this correction or cancellation to affect the output of this filter.

        The special value NOW (stands for "as of now") ensures that only the corrected ticks
        are being sent to the output stream.

        Special value 0 (stands for "beginning of time") results in the propagation
        of all uncorrected ticks and correction ticks as part of the stream.

        Default value: NOW.

    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    **CORRECT_TICK_FILTER** OneTick event processor
    :meth:`~onetick.py.Source.show_hidden_ticks`
    :meth:`~onetick.py.Source.show_corrected_ticks`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Let's find some canceled ticks first and see the time they were deleted:

    >>> start = otp.dt(2024, 2, 1, 11, 50, 15)
    >>> end   = otp.dt(2024, 2, 1, 11, 50, 15, 900_000)
    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data.show_hidden_ticks()
    >>> data = data.where(data['TICK_STATUS'] == 1)
    >>> data = data[['PRICE', 'SIZE', 'TICK_STATUS', 'DELETED_TIME']]
    >>> otp.run(data, start=start, end=end)
                               Time    PRICE  SIZE  TICK_STATUS            DELETED_TIME
    0 2024-02-01 11:50:15.621123654  185.695     1            1 2024-02-01 14:43:17.690

    Now let's use it as ``as_of_time`` parameter.
    First let's set it to the time *before* the tick was deleted.
    We will see the deleted tick along with regular ticks:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data.correct_tick_filter(as_of_time=otp.dt('2024-02-01 14:43:17'))
    >>> data = data[['PRICE', 'SIZE', 'TICK_STATUS', 'DELETED_TIME']]
    >>> otp.run(data, start=start, end=end)
                               Time    PRICE  SIZE  TICK_STATUS            DELETED_TIME
    0 2024-02-01 11:50:15.232051023  185.695     1            0 1969-12-31 19:00:00.000
    1 2024-02-01 11:50:15.621123654  185.695     1            1 2024-02-01 14:43:17.690
    2 2024-02-01 11:50:15.835322561  185.695    14            0 1969-12-31 19:00:00.000

    Now let's set ``as_of_time`` parameter to the time *after* the tick was deleted.
    The deleted tick has disappeared and we see the corrected data now.

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data.correct_tick_filter(as_of_time=otp.dt('2024-02-01 14:43:18'))
    >>> data = data[['PRICE', 'SIZE', 'TICK_STATUS', 'DELETED_TIME']]
    >>> otp.run(data, start=start, end=end)
                               Time    PRICE  SIZE  TICK_STATUS        DELETED_TIME
    0 2024-02-01 11:50:15.232051023  185.695     1            0 1969-12-31 19:00:00
    1 2024-02-01 11:50:15.835322561  185.695    14            0 1969-12-31 19:00:00
    """
    ep_kwargs: dict[str, Any] = {}
    if discard_on_match is not None:
        ep_kwargs['discard_on_match'] = discard_on_match
    if as_of_time is not None:
        if hasattr(as_of_time, 'strftime'):
            as_of_time = as_of_time.strftime('%Y%m%d%H%M%S')

        ep_kwargs['as_of_time'] = as_of_time

    self.sink(otq.CorrectTickFilter(**ep_kwargs))
    return self


@inplace_operation
def byte_present(
    self: 'Source',
    field: str | _Column,
    /,
    bytes: list[int | str] | str,
    *,
    discard_on_match: bool = False,
    inplace: bool = False,
) -> 'Source':
    """
    Propagates ticks based on whether the value of the ``field``
    contains a byte in the set of ``bytes``.

    Parameters
    ----------
    field: str, :class:`~onetick.py.Column`
        The name of the field to apply filter to.

    bytes: list
        List of bytes values expressed as integers or strings or a comma-separated string.
        Both hexadecimal and decimal formats are supported.
        Note that invalid byte values are converted to zero.

    discard_on_match: bool
        If this parameter is False (default), only ticks that match the filter will be propagated.
        Otherwise ticks that match the filter will be discarded.

    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    | **BYTE_PRESENT** OneTick event processor
    | :meth:`~onetick.py.Source.character_present`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Propagate only the ticks where field EXCHANGE contains P or Q characters
    (expressed as ASCII decimal or hex values):

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data[['PRICE', 'SIZE', 'EXCHANGE']]
    >>> data = data.byte_present('EXCHANGE', [0x51, 80])
    >>> data = data.limit(100)
    >>> otp.run(data, date=otp.dt(2024, 2, 1), timezone='America/New_York')  # doctest: +ELLIPSIS
                                Time   PRICE  SIZE EXCHANGE
    0  2024-02-01 04:00:00.033759205  185.90    16        P
    1  2024-02-01 04:00:00.169091123  185.90    87        P
    2  2024-02-01 04:00:00.413581861  185.98     1        P
    3  2024-02-01 04:00:00.721593975  185.77     6        P
    4  2024-02-01 04:00:00.721594183  185.76     1        P
    ..                           ...     ...   ...      ...
    95 2024-02-01 04:03:25.351272711  185.97     2        Q
    96 2024-02-01 04:03:25.351544878  185.90   100        Q
    97 2024-02-01 04:03:25.351546823  185.88   648        Q
    98 2024-02-01 04:03:25.351553003  185.88   117        Q
    99 2024-02-01 04:03:25.351703930  185.87   128        P
    ...
    """
    if str(field) not in self.schema:
        raise ValueError(f"There is no '{field}' column in the schema")

    if isinstance(bytes, str):
        bytes_str = bytes
    else:
        bytes_str = ','.join(str(v) for v in bytes)

    ep = otq.BytePresent(field=str(field), bytes=bytes_str, discard_on_match=discard_on_match)
    self.sink(ep)
    return self


@inplace_operation
def regex_matches(
    self: 'Source',
    field: str | _Column,
    /,
    pattern: str,
    *,
    discard_on_match: bool = False,
    inplace: bool = False,
) -> 'Source':
    """
    Propagates ticks based on whether the value in the ``field`` matches the regular expression ``pattern``.

    Parameters
    ----------
    field: str, :class:`~onetick.py.Column`
        The name of the field to apply filter to.

    pattern: str
        Pattern to match, expected to be a Perl 5 regular expression.

    discard_on_match: bool
        If this parameter is False (default), only ticks that match the filter will be propagated.
        Otherwise ticks that match the filter will be discarded.

    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    **REGEX_MATCHES** OneTick event processor

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Propagate only the ticks where field EXCHANGE contains P or Q characters:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data[['PRICE', 'SIZE', 'EXCHANGE']]
    >>> data = data.regex_matches('EXCHANGE', 'P|Q')
    >>> data = data.limit(100)
    >>> otp.run(data, date=otp.dt(2024, 2, 1), timezone='America/New_York')  # doctest: +ELLIPSIS
                                Time   PRICE  SIZE EXCHANGE
    0  2024-02-01 04:00:00.033759205  185.90    16        P
    1  2024-02-01 04:00:00.169091123  185.90    87        P
    2  2024-02-01 04:00:00.413581861  185.98     1        P
    3  2024-02-01 04:00:00.721593975  185.77     6        P
    4  2024-02-01 04:00:00.721594183  185.76     1        P
    ..                           ...     ...   ...      ...
    95 2024-02-01 04:03:25.351272711  185.97     2        Q
    96 2024-02-01 04:03:25.351544878  185.90   100        Q
    97 2024-02-01 04:03:25.351546823  185.88   648        Q
    98 2024-02-01 04:03:25.351553003  185.88   117        Q
    99 2024-02-01 04:03:25.351703930  185.87   128        P
    ...
    """
    if str(field) not in self.schema:
        raise ValueError(f"There is no '{field}' column in the schema")

    ep = otq.RegexMatches(field=str(field), pattern=pattern, discard_on_match=discard_on_match)
    self.sink(ep)
    return self


@inplace_operation
def uptick(
    self: 'Source',
    *,
    discard_on_match: bool = False,
    inplace: bool = False,
) -> 'Source':
    """
    Propagates ticks based on its PRICE being strictly greater than that of the previous tick.

    Parameters
    ----------
    discard_on_match: bool
        If this parameter is False (default), only ticks that match the filter will be propagated.
        Otherwise ticks that match the filter will be discarded.

    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    **UPTICK** OneTick event processor

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Propagate only the ticks where field PRICE is greater than the PRICE of the previous tick:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data[['PRICE', 'SIZE', 'EXCHANGE']]
    >>> data = data.uptick()
    >>> data = data.limit(100)
    >>> otp.run(data, date=otp.dt(2024, 2, 1), timezone='America/New_York')  # doctest: +ELLIPSIS
                                Time   PRICE  SIZE EXCHANGE
    0  2024-02-01 04:00:00.011224206  185.50     2        K
    1  2024-02-01 04:00:00.012928231  185.68    11        K
    2  2024-02-01 04:00:00.012987210  185.72     5        K
    3  2024-02-01 04:00:00.033759205  185.90    16        P
    4  2024-02-01 04:00:00.169091123  185.90    87        P
    ..                           ...     ...   ...      ...
    95 2024-02-01 04:12:37.454294671  185.87     1        P
    96 2024-02-01 04:12:48.042468499  185.86     1        K
    97 2024-02-01 04:13:15.728879547  185.86     3        Q
    98 2024-02-01 04:13:26.821715886  185.84     2        K
    99 2024-02-01 04:13:46.906139654  185.85    30        K
    ...
    """
    if 'PRICE' not in self.schema:
        raise ValueError("There is no 'PRICE' column in the schema")

    ep = otq.Uptick(discard_on_match=discard_on_match)
    self.sink(ep)
    return self


@inplace_operation
def volume_limit(
    self: 'Source',
    /,
    max_volume: int | float,
    *,
    propagate_tick_at_crossed_boundary: bool = False,
    discard_on_match: bool = False,
    inplace: bool = False,
) -> 'Source':
    """
    Propagates ticks based on the comparison of their total trading volume (the sum of field SIZE)
    to the value specified by the parameter ``max_volume``.

    Parameters
    ----------
    max_volume: int or float
        The number with maximum trading volume.

    propagate_tick_at_crossed_boundary: bool
        If True, the last tick that crossed the ``max_volume`` will be propagated, otherwise not.
        Default is False.

    discard_on_match: bool
        If this parameter is False (default), only ticks that match the filter will be propagated.
        Otherwise ticks that match the filter will be discarded.

    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    **VOLUME_LIMIT** OneTick event processor

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Filter out ticks after the cumulative volume exceeds 10 000 shares:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data[['PRICE', 'SIZE', 'EXCHANGE']]
    >>> data = data.volume_limit(10_000)
    >>> otp.run(data, date=otp.dt(2024, 2, 1), timezone='America/New_York')  # doctest: +ELLIPSIS
                                 Time   PRICE  SIZE EXCHANGE
    0   2024-02-01 04:00:00.008283417  186.50     6        K
    1   2024-02-01 04:00:00.008290927  185.59     1        K
    2   2024-02-01 04:00:00.008291153  185.49   107        K
    3   2024-02-01 04:00:00.010381671  185.49     1        K
    4   2024-02-01 04:00:00.011224206  185.50     2        K
    ..                            ...     ...   ...      ...
    414 2024-02-01 04:20:20.380578855  185.80    50        K
    415 2024-02-01 04:20:20.380599543  185.82    10        Q
    416 2024-02-01 04:20:20.380601983  185.81     1        Q
    417 2024-02-01 04:20:20.380604453  185.80    10        Q
    418 2024-02-01 04:20:20.380609048  185.79    10        Q
    ...

    Setting parameter ``propagate_tick_at_crossed_boundary`` will also propagate the tick that crossed the volume limit:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data[['PRICE', 'SIZE', 'EXCHANGE']]
    >>> data = data.volume_limit(10_000, propagate_tick_at_crossed_boundary=True)
    >>> otp.run(data, date=otp.dt(2024, 2, 1), timezone='America/New_York')  # doctest: +ELLIPSIS
                                 Time   PRICE  SIZE EXCHANGE
    0   2024-02-01 04:00:00.008283417  186.50     6        K
    1   2024-02-01 04:00:00.008290927  185.59     1        K
    2   2024-02-01 04:00:00.008291153  185.49   107        K
    3   2024-02-01 04:00:00.010381671  185.49     1        K
    4   2024-02-01 04:00:00.011224206  185.50     2        K
    ..                            ...     ...   ...      ...
    415 2024-02-01 04:20:20.380599543  185.82    10        Q
    416 2024-02-01 04:20:20.380601983  185.81     1        Q
    417 2024-02-01 04:20:20.380604453  185.80    10        Q
    418 2024-02-01 04:20:20.380609048  185.79    10        Q
    419 2024-02-01 04:20:20.380611535  185.79    70        Q
    ...
    """
    if 'SIZE' not in self.schema:
        raise ValueError("There is no 'SIZE' column in the schema")

    tick_handling_at_crossed_boundary = 'PROPAGATE' if propagate_tick_at_crossed_boundary else 'DISCARD'

    ep = otq.VolumeLimit(
        max_volume=max_volume,
        tick_handling_at_crossed_boundary=tick_handling_at_crossed_boundary,
        discard_on_match=discard_on_match,
    )
    self.sink(ep)
    return self


@inplace_operation
def value_compare(
    self: 'Source',
    field: str | _Column,
    relationship_to_const: str,
    value: int | str,
    /,
    *,
    discard_on_match: bool = False,
    inplace: bool = False,
) -> 'Source':
    """
    Propagates ticks based on whether the value in the ``field``
    is in the specified ``relationship_to_const`` to the ``value``.

    Parameters
    ----------
    field: str, :class:`~onetick.py.Column`
        The name of the field to apply filter to.

    relationship_to_const: str
        Relationship to check between the value of the ``field`` and constant ``value``.
        Supported values:

        * ``lt`` (less),
        * ``le`` (less or equal),
        * ``eq`` (equal),
        * ``ne`` (not equal),
        * ``ge`` (greater or equal),
        * ``gt`` (greater).

    value: str, int
        The value to compare the ``field`` to.
        Only string and numeric values are supported.

    discard_on_match: bool
        If this parameter is False (default), only ticks that match the filter will be propagated.
        Otherwise ticks that match the filter will be discarded.

    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    **VALUE_COMPARE** OneTick event processor

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Propagate only the ticks where field EXCHANGE equals to P character:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data[['PRICE', 'SIZE', 'EXCHANGE']]
    >>> data = data.value_compare('EXCHANGE', 'eq',  'P')
    >>> data = data.limit(100)
    >>> otp.run(data, date=otp.dt(2024, 2, 1), timezone='America/New_York')  # doctest: +ELLIPSIS
                                Time   PRICE  SIZE EXCHANGE
    0  2024-02-01 04:00:00.033759205  185.90    16        P
    1  2024-02-01 04:00:00.169091123  185.90    87        P
    2  2024-02-01 04:00:00.413581861  185.98     1        P
    3  2024-02-01 04:00:00.721593975  185.77     6        P
    4  2024-02-01 04:00:00.721594183  185.76     1        P
    ..                           ...     ...   ...      ...
    95 2024-02-01 04:07:38.594705586  185.81    10        P
    96 2024-02-01 04:07:47.740645808  185.81     7        P
    97 2024-02-01 04:07:47.740649834  185.81     1        P
    98 2024-02-01 04:07:58.714205622  185.86     5        P
    99 2024-02-01 04:07:59.902144628  185.81     7        P
    ...
    """
    if str(field) not in self.schema:
        raise ValueError(f"There is no '{field}' column in the schema")

    if relationship_to_const not in ('lt', 'le', 'eq', 'ne', 'ge', 'gt'):
        raise ValueError(f"Not supported value for parameter 'relationship_to_const': {relationship_to_const!r}")

    ep = otq.ValueCompare(
        field=str(field),
        relationship_to_const=relationship_to_const.upper(),
        value=value,
        discard_on_match=discard_on_match,
    )
    self.sink(ep)
    return self


@inplace_operation
def intercept_heartbeats(self: 'Source', inplace: bool = False) -> 'Source':
    """
    Intercepts heartbeats (timer events), leaving all other events of the time series unchanged.

    Parameters
    ----------
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    | **INTERCEPT_HEARTBEATS** OneTick event processor
    | :meth:`~onetick.py.Source.insert_heartbeat`
    | :meth:`~onetick.py.Source.show_heartbeats`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Assume we have heartbeats and data quality events in the data flow:

    >>> data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data[['PRICE', 'SIZE']]
    >>> data = data.insert_heartbeat(where=data['SIZE'] > 1000)
    >>> data = data.insert_data_quality_event('OK', where=data['SIZE'] > 5000)

    Intercepting heartbeats removes only heartbeats, data quality events are left unchanged:

    >>> data = data.intercept_heartbeats()
    >>> otp.run(data.show_heartbeats(), date=otp.dt(2024, 2, 1))  # doctest: +ELLIPSIS
    Empty DataFrame
    ...

    >>> otp.run(data.show_data_quality(), date=otp.dt(2024, 2, 1))  # doctest: +ELLIPSIS
                                 Time  DATA_QUALITY_TYPE DATA_QUALITY_NAME
    0   2024-02-01 08:00:00.074574483                  0                OK
    1   2024-02-01 08:00:00.074937867                  0                OK
    2   2024-02-01 08:00:00.075738202                  0                OK
    3   2024-02-01 09:00:06.565175834                  0                OK
    4   2024-02-01 09:24:20.329799843                  0                OK
    ...
    """
    self.sink(otq.InterceptHeartbeats())
    return self
