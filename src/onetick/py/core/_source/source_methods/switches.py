from typing import TYPE_CHECKING, Tuple

from onetick import py as otp
from onetick.py.otq import otq

if TYPE_CHECKING:
    from onetick.py.core.source import Source


def split(self: 'Source', expr, cases, default=False) -> Tuple['Source', ...]:
    """
    The method splits data using passed expression ``expr`` for several
    outputs by passed ``cases``. The method is the alias for the :meth:`Source.switch`

    Parameters
    ----------
    expr : Operation
        column or column based expression
    cases : list
        list of values or :py:class:`onetick.py.range` objects to split by
    default : bool
        ``True`` adds the default output

    Returns
    -------
    Outputs according to passed cases, number of outputs is number of cases plus one if ``default=True``

    See also
    --------
    | :meth:`Source.switch`
    | **SWITCH** OneTick event processor

    Examples
    --------
    >>> # OTdirective: snippet-name: Source operations.split;
    >>> data = otp.Ticks(X=[0.33, -5.1, otp.nan, 9.4])
    >>> r1, r2, r3 = data.split(data['X'], [otp.nan, otp.range(0, 100)], default=True)
    >>> otp.run(r1)
                         Time   X
    0 2003-12-01 00:00:00.002 NaN
    >>> otp.run(r2)
                         Time     X
    0 2003-12-01 00:00:00.000  0.33
    1 2003-12-01 00:00:00.003  9.40
    >>> otp.run(r3)
                         Time    X
    0 2003-12-01 00:00:00.001 -5.1

    See Also
    --------
    Source.switch
    :py:class:`onetick.py.range`
    """

    output_num = len(cases)

    # format cases
    def to_str(v):
        if isinstance(v, otp.utils.range):
            return "[" + str(v.start) + "," + str(v.stop) + "]"
        elif isinstance(v, str):
            return '"' + v + '"'
        elif isinstance(v, tuple):
            return ",".join(map(to_str, list(v)))

        return str(v)

    cases = [f"{to_str(cases[inx])}:OUT{inx}" for inx in range(output_num)]

    # create ep
    params = dict(switch=str(expr), cases=";".join(cases))
    if default:
        params["default_output"] = "DEF_OUT"

    switch_branch = self.copy(ep=otq.SwitchEp(**params))

    # construct results
    result = []

    for inx in range(output_num):
        res = switch_branch.copy()
        res.node().out_pin(f"OUT{inx}")
        res.sink(otq.Passthrough())

        result.append(res)

    if default:
        res = switch_branch.copy()
        res.node().out_pin("DEF_OUT")
        res.sink(otq.Passthrough())

        result.append(res)

    return tuple(result)


def switch(self: 'Source', expr, cases, default=False) -> Tuple['Source', ...]:
    """
    The method splits data using passed expression for several
    outputs by passed cases. This method is an alias for
    :meth:`Source.split` method.

    Parameters
    ----------
    expr : Operation
        column or column based expression
    cases : list
        list of values or :py:class:`onetick.py.range` objects to split by
    default : bool
        ``True`` adds the default output

    Returns
    -------
    Outputs according to passed cases, number of outputs is number of cases plus one if ``default=True``

    See also
    --------
    | :meth:`Source.split`
    | **SWITCH** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(X=[0.33, -5.1, otp.nan, 9.4])
    >>> r1, r2, r3 = data.switch(data['X'], [otp.nan, otp.range(0, 100)], default=True)
    >>> otp.run(r1)
                         Time   X
    0 2003-12-01 00:00:00.002 NaN
    >>> otp.run(r2)
                         Time     X
    0 2003-12-01 00:00:00.000  0.33
    1 2003-12-01 00:00:00.003  9.40
    >>> otp.run(r3)
                         Time    X
    0 2003-12-01 00:00:00.001 -5.1

    See Also
    --------
    Source.split
    :py:class:`onetick.py.range`
    """

    return self.split(expr, cases, default)
