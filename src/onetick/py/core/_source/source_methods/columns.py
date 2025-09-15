import functools
from typing import TYPE_CHECKING, Any

from onetick import py as otp
from onetick.py import types as ott
from onetick.py.core.column_operations._methods.op_types import are_numerics, are_time
from onetick.py.core.column_operations._methods.op_types import are_strings

if TYPE_CHECKING:
    import onetick.py as otp


def mean(self: 'otp.Source', *columns) -> 'otp.Operation':
    """
    Get average value of the specified columns.

    Do not confuse it with :py:func:`otp.agg.average <onetick.py.agg.average>`,
    this method gets average of the columns' values from single row, not doing
    aggregation of all rows.

    Parameters
    ----------
    columns: str, :class:`Column`
        Columns names or columns objects.
        All columns must have compatible types.

    Returns
    -------
    :class:`~onetick.py.Operation`

    Examples
    --------

    Integers and floating point values can be mixed:

    >>> t = otp.Tick(A=1, B=3.3)
    >>> t['AVG'] = t.mean('A', 'B')
    >>> otp.run(t)
            Time  A    B   AVG
    0 2003-12-01  1  3.3  2.15

    You can get the average for datetime values too:

    >>> t = otp.Tick(START_TIME=otp.dt(2022, 1, 1), END_TIME=otp.dt(2023, 1, 1))
    >>> t['MID_TIME'] = t.mean('START_TIME', 'END_TIME')
    >>> otp.run(t, timezone='GMT')
            Time  START_TIME    END_TIME             MID_TIME
    0 2003-12-01  2022-01-01  2023-01-01  2022-07-02 12:00:00

    Note that things may get confusing for datetimes when
    the timezone with daylight saving time is used:

    >>> t = otp.Tick(START_TIME=otp.dt(2022, 1, 1), END_TIME=otp.dt(2023, 1, 1))
    >>> t['MID_TIME'] = t.mean('START_TIME', 'END_TIME')
    >>> otp.run(t, timezone='EST5EDT')
            Time  START_TIME    END_TIME             MID_TIME
    0 2003-12-01  2022-01-01  2023-01-01  2022-07-02 13:00:00
    """
    if not columns:
        raise ValueError('No columns were specified')

    dtypes = set()
    for column_name in map(str, columns):
        if column_name not in self.schema:
            raise ValueError(f"There is no '{column_name}' column in the schema")
        dtypes.add(self.schema[column_name])

    if not are_numerics(*dtypes) and not are_time(*dtypes):
        raise ValueError(
            'Only int, float and datetime columns are supported. Numeric and datetime columns should not be mixed.'
        )

    op: Any = None
    for column_name in map(str, columns):
        column = self[column_name]
        dtype = self.schema[column_name]
        if dtype is otp.msectime and otp.nsectime in dtypes:
            column = column.astype(otp.nsectime)
        if are_time(dtype):
            column = column.astype(int)
        if op is None:
            op = column
        else:
            op += column

    op = op / len(columns)

    dtype = ott.get_type_by_objects(dtypes)
    if are_time(dtype):
        # can't convert float to datetime, converting to int first
        op = op.astype(int)
    op = op.astype(dtype)

    return op


def unite_columns(self: 'otp.Source', sep="", *, apply_str=False) -> 'otp.Operation':
    """
    Join values of all columns into one string

    The method unite all fields to one string, just like python ``join`` method. All fields should be strings,
    otherwise the error will be generated. To change this behavior, ``apply_str=True`` argument should be specified,
    in this case all fields will be converted to string type before joining.

    Parameters
    ----------
    sep: str
        Separator between values, empty string be dafault.
    apply_str: bool
        If set every column will be converted to string during operation. False be default.

    Returns
    -------
    result: column
        Column with str type

    Examples
    --------

    >>> # OTdirective: snippet-name: Arrange.join columns as strings;
    >>> data = otp.Ticks(X=[1, 2, 3], A=["A", "A", "A"], B=["A", "B", "C"])
    >>> data["S_ALL"] = data.unite_columns(sep=",", apply_str=True)
    >>> data["S"] = data[["A", "B"]].unite_columns()
    >>> otp.run(data)[["S", "S_ALL"]]
        S  S_ALL
    0  AA  1,A,A
    1  AB  2,A,B
    2  AC  3,A,C
    """
    if apply_str:
        cols = (self[col].apply(str) for col in self.schema)
    else:
        not_str = [name for name, t in self.schema.items() if not are_strings(t)]
        if not_str:
            raise ValueError(
                f"All joining columns should be strings, while {', '.join(not_str)} "
                f"are not. Specify `apply_str=True` for automatic type conversion"
            )
        else:
            cols = (self[col] for col in self.schema)
    return functools.reduce(lambda x, y: x + sep + y, cols)
