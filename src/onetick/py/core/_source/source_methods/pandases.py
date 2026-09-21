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
    data = self.copy()
    data = data[[y, x]]
    df = otp.run(data)
    return df.plot(x=x, y=y, kind=kind, **kwargs)


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
    data = self.copy()
    df = otp.run(data.agg({'__num_rows': otp.agg.count()}), **kwargs)
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
    >>> data.head()
                         Time  X
    0 2003-12-01 00:00:00.000  a
    1 2003-12-01 00:00:00.001  b
    2 2003-12-01 00:00:00.002  c
    3 2003-12-01 00:00:00.003  d
    4 2003-12-01 00:00:00.004  e
    """
    data = self.copy()
    data = data.first(n=n)  # pylint: disable=E1123
    return otp.run(data, **kwargs)


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
    >>> data.tail()
                         Time  X
    0 2003-12-01 00:00:00.004  e
    1 2003-12-01 00:00:00.005  f
    2 2003-12-01 00:00:00.006  g
    3 2003-12-01 00:00:00.007  i
    4 2003-12-01 00:00:00.008  k
    """
    data = self.copy()
    data = data.last(n=n)  # pylint: disable=E1123
    return otp.run(data, **kwargs)
