from typing import TYPE_CHECKING, Any, List, Optional

from onetick.py.otq import otq

from .misc import inplace_operation

if TYPE_CHECKING:
    from onetick.py.core.source import Source


@inplace_operation
def drop(self: 'Source', columns: List[Any], inplace=False) -> Optional['Source']:
    r"""
    Remove a list of columns specified by names or regular expressions from the Source.

    If a column with such name wasn't found the error will be raised,
    if the regex was specified and there aren't any matched columns, do nothing.

    Regex is any string containing any of characters ***+?\:[]{}()**,
    dot is a valid symbol for OneTick identifier,
    so **a.b** will be passed to OneTick as an identifier,
    if you want specify such regex use parenthesis - **(a.b)**

    If a string without special characters or :py:class:`~onetick.py.Column` object is specified in the list,
    then it is assumed that this is a literal name of the column, not regular expression.

    Parameters
    ----------
    columns : str, Column or list of them
        Column(s) to remove. You could specify a regex or collection of regexes, in such case columns with
        match names will be deleted.
    inplace: bool
        A flag controls whether operation should be applied inplace.
        If inplace=True, then it returns nothing. Otherwise method
        returns a new modified object.

    Returns
    ----------
    :class:`Source` or ``None``

    See Also
    --------
    **PASSTHROUGH** OneTick event processor

    Examples
    --------

    Drop a single field:

    >>> data = otp.Ticks(X1=[1, 2, 3],
    ...                  X2=[3, 2, 1],
    ...                  A1=["A", "A", "A"])
    >>> data = data.drop("X1")  # OTdirective: snippet-name: Arrange.drop.one field;
    >>> otp.run(data)
                         Time  X2 A1
    0 2003-12-01 00:00:00.000   3  A
    1 2003-12-01 00:00:00.001   2  A
    2 2003-12-01 00:00:00.002   1  A

    Regexes also could be specified in such case all matched columns will be deleted:

    >>> data = otp.Ticks(X1=[1, 2, 3],
    ...                  X2=[3, 2, 1],
    ...                  A1=["A", "A", "A"])
    >>> data = data.drop(r"X\d+")   # OTdirective: snippet-name: Arrange.drop.regex;
    >>> otp.run(data)
                         Time A1
    0 2003-12-01 00:00:00.000  A
    1 2003-12-01 00:00:00.001  A
    2 2003-12-01 00:00:00.002  A

    Both column names and regular expressions can be specified at the same time:

    >>> # OTdirective: snippet-name: Arrange.drop.multiple;
    >>> data = otp.Ticks(X1=[1, 2, 3],
    ...                  X2=[3, 2, 1],
    ...                  Y1=[1, 2, 3],
    ...                  Y2=[3, 2, 1],
    ...                  Y11=["a", "b", "c"],
    ...                  Y22=["A", "A", "A"])
    >>> data = data.drop([r"X\d+", "Y1", data["Y2"]])
    >>> otp.run(data)
                         Time Y11 Y22
    0 2003-12-01 00:00:00.000   a   A
    1 2003-12-01 00:00:00.001   b   A
    2 2003-12-01 00:00:00.002   c   A

    **a.b** will be passed to OneTick as an identifier,
    if you want specify such regex use parenthesis - **(a.b)**:

    >>> data = otp.Ticks({"COLUMN.A": [1, 2, 3], "COLUMN1A": [3, 2, 1],
    ...                   "COLUMN1B": ["a", "b", "c"], "COLUMN2A": ["c", "b", "a"]})
    >>> data = data.drop("COLUMN.A")    # OTdirective: skip-snippet:;
    >>> otp.run(data)
                         Time  COLUMN1A COLUMN1B COLUMN2A
    0 2003-12-01 00:00:00.000         3        a        c
    1 2003-12-01 00:00:00.001         2        b        b
    2 2003-12-01 00:00:00.002         1        c        a

    >>> data = otp.Ticks({"COLUMN.A": [1, 2, 3], "COLUMN1A": [3, 2, 1],
    ...                   "COLUMN1B": ["a", "b", "c"], "COLUMN2A": ["c", "b", "a"]})
    >>> data = data.drop("(COLUMN.A)")  # OTdirective: skip-snippet:;
    >>> otp.run(data)
                         Time COLUMN1B
    0 2003-12-01 00:00:00.000        a
    1 2003-12-01 00:00:00.001        b
    2 2003-12-01 00:00:00.002        c
    """
    self.__delitem__(columns)
    return self


def __delitem__(self: 'Source', obj):
    if isinstance(obj, (list, tuple)):
        objs = obj
    else:
        objs = (obj,)

    items_to_passthrough, regex = self._columns_names_regex(objs, drop=True)
    self.sink(otq.Passthrough(drop_fields=True, fields=",".join(items_to_passthrough), use_regex=regex))
