from typing import TYPE_CHECKING, Collection, Optional, Union

from onetick.py.core.column import _Column
from onetick.py.otq import otq

from .misc import inplace_operation

if TYPE_CHECKING:
    from onetick.py.core.source import Source


@inplace_operation
def sort(
    self: 'Source', by: Union[str, Collection[Union[str, '_Column']]], ascending=True, inplace=False
) -> Optional['Source']:
    """Sort ticks by columns.

    Parameters
    ----------
    by: str, Column or list of them
        Column(s) to sort by. It is possible to pass a list of column, where is the order is important:
        from the left to the right.
    ascending: bool or list
        Order to sort by. If list of columns is specified, then list of ascending values per column is expected.
        (the :class:`nan` is the smallest for ``float`` type fields)
    inplace: bool
        A flag controls whether operation should be applied inplace.
        If ``inplace=True``, then it returns nothing. Otherwise method
        returns a new modified object

    Returns
    -------
    :class:`Source`

    See also
    --------
    **ORDER_BY** OneTick event processor

    Examples
    --------

    Single column examples

    >>> data = otp.Ticks({'X':[     94,   5,   34],
    ...                   'Y':[otp.nan, 3.1, -0.3]})
    >>> data = data.sort(data['X'])
    >>> otp.run(data)
                         Time   X    Y
    0 2003-12-01 00:00:00.001   5  3.1
    1 2003-12-01 00:00:00.002  34 -0.3
    2 2003-12-01 00:00:00.000  94  NaN

    >>> data = otp.Ticks({'X':[     94,   5,   34],
    ...                   'Y':[otp.nan, 3.1, -0.3]})
    >>> data = data.sort(data['Y'])
    >>> otp.run(data)
                         Time   X    Y
    0 2003-12-01 00:00:00.000  94  NaN
    1 2003-12-01 00:00:00.002  34 -0.3
    2 2003-12-01 00:00:00.001   5  3.1

    Inplace

    >>> data = otp.Ticks({'X':[     94,   5,   34],
    ...                   'Y':[otp.nan, 3.1, -0.3]})
    >>> data.sort(data['Y'], inplace=True)  # OTdirective: snippet-name: Arrange.sort.inplace;
    >>> otp.run(data)
                         Time   X    Y
    0 2003-12-01 00:00:00.000  94 NaN
    1 2003-12-01 00:00:00.002  34 -0.3
    2 2003-12-01 00:00:00.001  5   3.1

    Multiple columns

    >>> data = otp.Ticks({'X':[  5,   6,   3,   6],
    ...                   'Y':[1.4, 3.1, 9.1, 5.5]})
    >>> data = data.sort([data['X'], data['Y']])
    >>> otp.run(data)
                         Time  X    Y
    0 2003-12-01 00:00:00.002  3  9.1
    1 2003-12-01 00:00:00.000  5  1.4
    2 2003-12-01 00:00:00.001  6  3.1
    3 2003-12-01 00:00:00.003  6  5.5

    Ascending/descending control

    >>> data = otp.Ticks({'X':[     94,   5,   34],
    ...                   'Y':[otp.nan, 3.1, -0.3]})
    >>> data = data.sort(data['X'], ascending=False)
    >>> otp.run(data)
                         Time   X    Y
    0 2003-12-01 00:00:00.000  94  NaN
    1 2003-12-01 00:00:00.002  34 -0.3
    2 2003-12-01 00:00:00.001   5  3.1

    >>> # OTdirective: snippet-name: Arrange.sort.sort;
    >>> data = otp.Ticks({'X':[  5,   6,   3,   6],
    ...                   'Y':[1.4, 3.1, 9.1, 5.5]})
    >>> data = data.sort([data['X'], data['Y']], ascending=[True, False])
    >>> otp.run(data)
                         Time  X    Y
    0 2003-12-01 00:00:00.002  3  9.1
    1 2003-12-01 00:00:00.000  5  1.4
    2 2003-12-01 00:00:00.003  6  5.5
    3 2003-12-01 00:00:00.001  6  3.1
    """
    columns = by

    if isinstance(columns, list):
        objs = columns
    else:
        objs = [columns]

    if isinstance(ascending, list):
        asc_objs = ascending
    else:
        asc_objs = [ascending]

    items = []

    # -------------------------------
    # Columns processing
    # -------------------------------
    # convert to strings
    # TODO: it seems as a common code, need to move to a separate function
    for obj in objs:
        if isinstance(obj, _Column):
            items.append(obj.name)
        elif isinstance(obj, str):
            items.append(obj)
        else:
            # TODO: cover with tests
            raise TypeError(f"It is not supported to order by '{obj}' of type '{type(obj)}'")

    # validate
    for item in items:
        if item in self.__dict__:
            if not isinstance(self.__dict__[item], _Column):
                # TODO: cover with tests
                raise AttributeError(f"There is no '{item}' column")
            #  if
        else:
            # TODO: covert with tests
            raise AttributeError(f"There is no '{item}' column")

    # -------------------------------
    # Asc processing
    # -------------------------------
    asc_items = [True] * len(items)

    def asc_convert(v):
        return "ASC" if v else "DESC"

    for inx in range(len(items)):
        if inx >= len(asc_objs):
            asc_obj = asc_items[inx]
        else:
            asc_obj = asc_objs[inx]

        if isinstance(asc_obj, bool):
            asc_items[inx] = asc_convert(asc_obj)
        elif isinstance(asc_obj, int):
            asc_items[inx] = asc_convert(asc_obj)
        else:
            raise TypeError(f"asc can not be '{asc_obj}' of type '{type(asc_obj)}'")

    # ---------------
    # combine together
    order_by = [column_name + " " + asc for column_name, asc in zip(items, asc_items)]

    self.sink(otq.OrderByEp(order_by=",".join(order_by)))
    return self


def sort_values(self: 'Source', *args, **kwargs):
    """
    alias of sort

    See Also
    --------
    :meth:`Source.sort`
    """
    return self.sort(*args, **kwargs)
