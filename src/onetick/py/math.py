from typing import Optional
from onetick.py.core.column_operations.base import _Operation


class _MaxOperator(_Operation):

    def __init__(self, objs):
        from onetick.py.types import get_type_by_objects

        super().__init__(dtype=get_type_by_objects(objs))

        def _str_max(l_val, r_val):
            if isinstance(r_val, list):
                if len(r_val) > 1:
                    r_val = _str_max(r_val[0], r_val[1:])
                else:
                    r_val = r_val[0]
            # CASE should be uppercased because it can be used in per-tick script
            return 'CASE({0} > {1}, 1, {0}, {1})'.format(str(l_val), str(r_val))

        self._repr = _str_max(objs[0], objs[1:])

    def __str__(self):
        return self._repr


def max(*objs):
    """
    Returns maximum value from list of ``objs``.

    Parameters
    ----------
    objs: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['MAX'] = otp.math.max(5, data['A'])
    >>> otp.run(data)
            Time  A  MAX
    0 2003-12-01  1    5
    """
    return _MaxOperator(list(objs))


class _MinOperator(_Operation):

    def __init__(self, objs):
        from onetick.py.types import get_type_by_objects

        super().__init__(dtype=get_type_by_objects(objs))

        def _str_min(l_val, r_val):
            if isinstance(r_val, list):
                if len(r_val) > 1:
                    r_val = _str_min(r_val[0], r_val[1:])
                else:
                    r_val = r_val[0]
            # CASE should be uppercased because it can be used in per-tick script
            return 'CASE({0} < {1}, 1, {0}, {1})'.format(str(l_val), str(r_val))

        self._repr = _str_min(objs[0], objs[1:])

    def __str__(self):
        return self._repr


def min(*objs):
    """
    Returns minimum value from list of ``objs``.

    Parameters
    ----------
    objs: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['MIN'] = otp.math.min(-5, data['A'])
    >>> otp.run(data)
            Time  A  MIN
    0 2003-12-01  1   -5
    """
    return _MinOperator(list(objs))


class _RandomFunc(_Operation):
    """
    It implements the `rand` built-in function.
    """

    def __init__(self, min_value: int, max_value: int, seed: Optional[int] = None):
        super().__init__(dtype=int)

        def _repr(min_value, max_value, seed):
            result = f'rand({str(min_value)}, {str(max_value)}'
            if seed is not None:
                result += f',{str(seed)})'
            else:
                result += ')'
            return result

        self._repr = _repr(min_value, max_value, seed)

    def __str__(self):
        return self._repr


def rand(min_value: int, max_value: int, seed: Optional[int] = None):
    """
    Returns a pseudo-random value in the range between ``min_value`` and ``max_value`` (both inclusive).
    If ``seed`` is not specified, the function produces different values each time a query is invoked.
    If ``seed`` is specified, for this seed the function produces the same sequence of values
    each time a query is invoked.

    Parameters
    ----------
    min_value: int, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`
    max_value: int, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`
    seed: int, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['RAND'] = otp.math.rand(1, 1000)
    """

    if isinstance(min_value, int) and min_value < 0:
        raise ValueError("It is not possible to use negative values for the `min_value`")
    if isinstance(min_value, int) and isinstance(max_value, int) and min_value >= max_value:
        raise ValueError("The `max_value` parameter should be more than `min_value`")

    return _RandomFunc(min_value, max_value, seed)


class _Now(_Operation):

    def __init__(self):
        from onetick.py.types import nsectime

        super().__init__(dtype=nsectime)

        def _repr():
            return 'now()'

        self._repr = _repr()

    def __str__(self):
        return self._repr


# TODO: this is not math, let's move it somewhere else
def now():
    """
    Returns the current time expressed as the number of milliseconds since the UNIX epoch.

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['NOW'] = otp.now()
    """
    return _Now()


class _Ln(_Operation):
    """
    Compute the natural logarithm.
    """

    def __init__(self, value):
        super().__init__(dtype=float)

        def _repr(value):
            return f'LOG({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def ln(value):
    """
    Compute the natural logarithm of the ``value``.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['LN'] = otp.math.ln(2.718282)
    >>> otp.run(data)
            Time  A   LN
    0 2003-12-01  1  1.0

    See Also
    --------
    onetick.py.math.exp
    """
    return _Ln(value)


class _Log10(_Operation):
    """
    Compute the base-10 logarithm.
    """

    def __init__(self, value):
        super().__init__(dtype=float)

        def _repr(value):
            return f'LOG10({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def log10(value):
    """
    Compute the base-10 logarithm of the ``value``.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['LOG10'] = otp.math.log10(100)
    >>> otp.run(data)
            Time  A  LOG10
    0 2003-12-01  1    2.0
    """
    return _Log10(value)


class _Exp(_Operation):
    """
    Compute the natural exponent.
    """

    def __init__(self, value):
        super().__init__(dtype=float)

        def _repr(value):
            return f'EXP({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def exp(value):
    """
    Compute the natural exponent of the ``value``.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['E'] = otp.math.exp(1)
    >>> otp.run(data)
            Time  A         E
    0 2003-12-01  1  2.718282

    See Also
    --------
    onetick.py.math.ln
    """
    return _Exp(value)


class _Sqrt(_Operation):
    """
    Compute the square root.
    """

    def __init__(self, value):
        super().__init__(dtype=float)

        def _repr(value):
            return f'SQRT({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def sqrt(value):
    """
    Compute the square root of the ``value``.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['SQRT'] = otp.math.sqrt(4)
    >>> otp.run(data)
            Time  A  SQRT
    0 2003-12-01  1   2.0
    """
    return _Sqrt(value)


class _Sign(_Operation):
    """
    Get the sign of value.
    """

    def __init__(self, value):
        super().__init__(dtype=int)

        def _repr(value):
            return f'SIGN({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def sign(value):
    """
    Compute the sign of the ``value``.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['SIGN_POS'] = otp.math.sign(123)
    >>> data['SIGN_ZERO'] = otp.math.sign(0)
    >>> data['SIGN_NEG'] = otp.math.sign(-123)
    >>> otp.run(data)
            Time  A  SIGN_POS  SIGN_ZERO  SIGN_NEG
    0 2003-12-01  1         1          0        -1
    """
    return _Sign(value)


class _Power(_Operation):
    """
    Compute the ``base`` to the power of the ``exponent``.
    """

    def __init__(self, base, exponent):
        super().__init__(dtype=float)

        def _repr(base, exponent):
            return f'POWER({str(base)}, {str(exponent)})'

        self._repr = _repr(base, exponent)

    def __str__(self):
        return self._repr


def pow(base, exponent):
    """
    Compute the ``base`` to the power of the ``exponent``.

    Parameters
    ----------
    base: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`
    exponent: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=2)
    >>> data['RES'] = otp.math.pow(data['A'], 10)
    >>> otp.run(data)
            Time  A     RES
    0 2003-12-01  2  1024.0
    """
    return _Power(base, exponent)


class _Pi(_Operation):

    def __init__(self):
        super().__init__(dtype=float)

        def _repr():
            return 'PI()'

        self._repr = _repr()

    def __str__(self):
        return self._repr


def pi():
    """
    Returns the value of Pi number.

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['PI'] = otp.math.pi()
    >>> otp.run(data)
            Time  A        PI
    0 2003-12-01  1  3.141593
    """
    return _Pi()


class _Sin(_Operation):
    """
    Returns the value of trigonometric function `sin` for the given angle number expressed in radians.
    """

    def __init__(self, value):
        super().__init__(dtype=float)

        def _repr(value):
            return f'SIN({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def sin(value):
    """
    Returns the value of trigonometric function `sin` for the given ``value`` number expressed in radians.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['SIN'] = otp.math.sin(otp.math.pi() / 6)
    >>> otp.run(data)
            Time  A  SIN
    0 2003-12-01  1  0.5

    See Also
    --------
    :py:data:`onetick.py.math.pi`
    :py:func:`onetick.py.math.asin`
    """
    return _Sin(value)


class _Cos(_Operation):
    """
    Returns the value of trigonometric function `cos` for the given angle number expressed in radians.
    """

    def __init__(self, value):
        super().__init__(dtype=float)

        def _repr(value):
            return f'COS({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def cos(value):
    """
    Returns the value of trigonometric function `cos` for the given ``value`` number expressed in radians.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['COS'] = otp.math.cos(otp.math.pi() / 3)
    >>> otp.run(data)
            Time  A  COS
    0 2003-12-01  1  0.5

    See Also
    --------
    :py:data:`onetick.py.math.pi`
    :py:func:`onetick.py.math.acos`
    """
    return _Cos(value)


class _Tan(_Operation):
    """
    Returns the value of trigonometric function `tan` for the given angle number expressed in radians.
    """

    def __init__(self, value):
        super().__init__(dtype=float)

        def _repr(value):
            return f'TAN({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def tan(value):
    """
    Returns the value of trigonometric function `tan` for the given ``value`` number expressed in radians.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['TAN'] = otp.math.tan(otp.math.pi() / 4)
    >>> otp.run(data)
            Time  A  TAN
    0 2003-12-01  1  1.0

    See Also
    --------
    :py:data:`onetick.py.math.pi`
    :py:func:`onetick.py.math.atan`
    """
    return _Tan(value)


class _Cot(_Operation):
    """
    Returns the value of trigonometric function `cot` for the given angle number expressed in radians.
    """

    def __init__(self, value):
        super().__init__(dtype=float)

        def _repr(value):
            return f'COT({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def cot(value):
    """
    Returns the value of trigonometric function `cot` for the given ``value`` number expressed in radians.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['COT'] = otp.math.cot(otp.math.pi() / 4)
    >>> otp.run(data)
            Time  A  COT
    0 2003-12-01  1  1.0

    See Also
    --------
    :py:data:`onetick.py.math.pi`
    :py:func:`onetick.py.math.acot`
    """
    return _Cot(value)


class _Asin(_Operation):
    """
    Returns the value of inverse trigonometric function `arcsin`.
    """

    def __init__(self, value):
        super().__init__(dtype=float)

        def _repr(value):
            return f'ASIN({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def asin(value):
    """
    Returns the value of inverse trigonometric function `arcsin`.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['ASIN'] = otp.math.asin(1).round(4)  # should return pi/2 ~ 1.5708
    >>> otp.run(data)
            Time  A    ASIN
    0 2003-12-01  1  1.5708

    `otp.math.arcsin()` is the alias for `otp.math.asin()`:

    >>> data = otp.Tick(A=1)
    >>> data['ASIN'] = otp.math.arcsin(1).round(4)
    >>> otp.run(data)
            Time  A    ASIN
    0 2003-12-01  1  1.5708

    See Also
    --------
    :py:data:`onetick.py.math.pi`
    :py:func:`onetick.py.math.sin`
    """
    return _Asin(value)


arcsin = asin


class _Acos(_Operation):
    """
    Returns the value of inverse trigonometric function `arccos`.
    """

    def __init__(self, value):
        super().__init__(dtype=float)

        def _repr(value):
            return f'ACOS({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def acos(value):
    """
    Returns the value of inverse trigonometric function `arccos`.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['ACOS'] = otp.math.acos(-1).round(4)  # should return pi ~ 3.1416
    >>> otp.run(data)
            Time  A    ACOS
    0 2003-12-01  1  3.1416

    `otp.math.arccos()` is the alias for `otp.math.acos()`:

    >>> data = otp.Tick(A=1)
    >>> data['ACOS'] = otp.math.arccos(-1).round(4)
    >>> otp.run(data)
            Time  A    ACOS
    0 2003-12-01  1  3.1416

    See Also
    --------
    :py:data:`onetick.py.math.pi`
    :py:func:`onetick.py.math.cos`
    """
    return _Acos(value)


arccos = acos


class _Atan(_Operation):
    """
    Returns the value of inverse trigonometric function `arctan`.
    """

    def __init__(self, value):
        super().__init__(dtype=float)

        def _repr(value):
            return f'ATAN({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def atan(value):
    """
    Returns the value of inverse trigonometric function `arctan`.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['ATAN'] = otp.math.atan(1).round(4)  # should return pi/4 ~ 0.7854
    >>> otp.run(data)
            Time  A    ATAN
    0 2003-12-01  1  0.7854

    `otp.math.arctan()` is the alias for `otp.math.atan()`:

    >>> data = otp.Tick(A=1)
    >>> data['ATAN'] = otp.math.arctan(1).round(4)
    >>> otp.run(data)
            Time  A    ATAN
    0 2003-12-01  1  0.7854

    See Also
    --------
    :py:data:`onetick.py.math.pi`
    :py:func:`onetick.py.math.tan`
    """
    return _Atan(value)


arctan = atan


class _Acot(_Operation):
    """
    Returns the value of inverse trigonometric function `arccot`.
    """

    def __init__(self, value):
        super().__init__(dtype=float)

        def _repr(value):
            return f'ACOT({str(value)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def acot(value):
    """
    Returns the value of inverse trigonometric function `arccot`.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['ACOT'] = otp.math.acot(1).round(4)  # should return pi/4 ~ 0.7854
    >>> otp.run(data)
            Time  A    ACOT
    0 2003-12-01  1  0.7854

    `otp.math.arccot()` is the alias for `otp.math.acot()`:

    >>> data = otp.Tick(A=1)
    >>> data['ACOT'] = otp.math.arccot(1).round(4)
    >>> otp.run(data)
            Time  A    ACOT
    0 2003-12-01  1  0.7854

    See Also
    --------
    :py:data:`onetick.py.math.pi`
    :py:func:`onetick.py.math.cot`
    """
    return _Acot(value)


arccot = acot


class _Mod(_Operation):
    """
    Implements the remainder from dividing ``value1`` by ``value2``
    """

    def __init__(self, value1, value2):
        super().__init__(dtype=int)

        def _repr(value1, value2):
            return f'MOD({str(value1)}, {str(value2)})'

        self._repr = _repr(value1, value2)

    def __str__(self):
        return self._repr


def mod(value1, value2):
    """
    Computes the remainder from dividing ``value1`` by ``value2``

    Parameters
    ----------
    value1: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`
    value2: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=100)
    >>> data['MOD'] = otp.math.mod(data['A'], 72)
    >>> otp.run(data)
            Time    A  MOD
    0 2003-12-01  100   28
    """
    return _Mod(value1, value2)


class _Floor(_Operation):
    """
    Returns a long integer value representing the largest integer that is less than or equal to the `value`.
    """

    def __init__(self, value):
        super().__init__(dtype=int)

        def _repr(val):
            return f'FLOOR({str(val)})'

        self._repr = _repr(value)

    def __str__(self):
        return self._repr


def floor(value):
    """
    Returns a long integer value representing the largest integer that is less than or equal to the `value`.

    Parameters
    ----------
    value: int, float, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1.2)
    >>> data['FLOOR'] = otp.math.floor(data['A'])
    >>> otp.run(data)
            Time    A  FLOOR
    0 2003-12-01  1.2      1
    """
    return _Floor(value)
