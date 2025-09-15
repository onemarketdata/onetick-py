from typing import TYPE_CHECKING

from onetick import py as otp

if TYPE_CHECKING:
    import pandas
    from onetick.py.core.source import Source


def plot(self: 'Source', y, x='Time', kind='line', **kwargs):
    """
    Executes the query with known properties and builds a plot resulting dataframe.

    Uses the :pandas:`pandas.DataFrame.plot` method to plot data.
    Other parameters could be specified through the ``kwargs``.

    Parameters
    ----------
    x: str
        Column name for the X axis
    y: str
        Column name for the Y axis
    kind: str
        The kind of plot

    Examples
    --------
    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> data.plot(y='X', kind='bar')  # doctest: +SKIP
    """
    result = self.copy()
    return result[[y, x]]().plot(x=x, y=y, kind=kind, **kwargs)


def count(self: 'Source', **kwargs) -> int:
    """
    Returns the number of ticks in the query.

    Adds an aggregation that calculate total ticks count, and *executes a query*.
    Result is a single value -- number of ticks. Possible application is the Jupyter when
    a developer wants to check data presences for example.

    Parameters
    ----------
    kwargs
        parameters that will be passed to :py:func:`otp.run <onetick.py.run>`

    Returns
    -------
    int

    See Also
    --------
    | :py:func:`onetick.py.agg.count`
    | :py:func:`otp.run <onetick.py.run>`
    | :py:meth:`onetick.py.Source.head`
    | :py:meth:`onetick.py.Source.tail`

    Examples
    --------

    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> data.count()
    3

    >>> data = otp.Empty()
    >>> data.count()
    0
    """
    result = self.copy()
    df = otp.run(result.agg({'__num_rows': otp.agg.count()}), **kwargs)
    if df.empty:
        return 0
    return int(df['__num_rows'][0])


def head(self: 'Source', n=5, **kwargs) -> 'pandas.DataFrame':
    """
    *Executes the query* and returns first ``n`` ticks as a pandas dataframe.

    It is useful in the Jupyter case when you want to observe first ``n`` values.

    Parameters
    ----------
    n: int, default=5
        number of ticks to return
    kwargs:
        parameters will be passed to :py:func:`otp.run <onetick.py.run>`

    Returns
    -------
    :pandas:`DataFrame <pandas.DataFrame>`

    See Also
    --------
    | :py:func:`onetick.py.agg.first`
    | :py:func:`otp.run <onetick.py.run>`
    | :py:meth:`onetick.py.Source.tail`
    | :py:meth:`onetick.py.Source.count`

    Examples
    --------

    >>> data = otp.Ticks(X=list('abcdefgik'))
    >>> data.head()[['X']]
        X
    0 a
    1 b
    2 c
    3 d
    4 e
    """
    result = self.copy()
    result = result.first(n=n)  # pylint: disable=E1123
    return otp.run(result, **kwargs)


def tail(self: 'Source', n=5, **kwargs) -> 'pandas.DataFrame':
    """
    *Executes the query* and returns last ``n`` ticks as a pandas dataframe.

    It is useful in the Jupyter case when you want to observe last ``n`` values.

    Parameters
    ----------
    n: int
        number of ticks to return
    kwargs:
        parameters will be passed to :py:func:`otp.run <onetick.py.run>`

    Returns
    -------
    :pandas:`DataFrame <pandas.DataFrame>`

    See Also
    --------
    | :py:func:`onetick.py.agg.last`
    | :py:func:`otp.run <onetick.py.run>`
    | :py:meth:`onetick.py.Source.head`
    | :py:meth:`onetick.py.Source.count`

    Examples
    --------
    >>> data = otp.Ticks(X=list('abcdefgik'))
    >>> data.tail()[['X']]
        X
    0 e
    1 f
    2 g
    3 i
    4 k
    """
    result = self.copy()
    result = result.last(n=n)  # pylint: disable=E1123
    return otp.run(result, **kwargs)
