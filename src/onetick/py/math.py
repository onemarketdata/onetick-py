from onetick.py.core.column_operations.base import _Operation
from onetick.py.types import value2str, nsectime, get_type_by_objects, decimal


def _decimal_or_float(*args):
    dtype = get_type_by_objects(args)
    if dtype is decimal:
        return decimal
    else:
        return float


def max(*objs):
    """
    Returns maximum value from list of ``objs``.
    The objects must be of the same type.

    Parameters
    ----------
    objs: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **MAX** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['MAX'] = otp.math.max(5, data['A'])
    >>> otp.run(data)
            Time  A  MAX
    0 2003-12-01  1    5
    """
    if len(objs) < 2:
        raise ValueError("otp.math.max expects at least 2 values to compare")

    def _max_func(*objs):
        dtype = get_type_by_objects(objs)
        onetick_params = map(value2str, objs)
        if dtype is nsectime:
            onetick_params = [f'NSECTIME_TO_LONG({param})' for param in onetick_params]
            onetick_params_str = ', '.join(onetick_params)
            return f"NSECTIME(MAX({onetick_params_str}))", dtype
        else:
            onetick_params_str = ', '.join(onetick_params)
            return f"MAX({onetick_params_str})", dtype

    return _Operation(
        op_func=_max_func,
        op_params=list(objs),
    )


def min(*objs):
    """
    Returns minimum value from list of ``objs``.

    The objects must be of the same type.

    Parameters
    ----------
    objs: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **MIN** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['MIN'] = otp.math.min(-5, data['A'])
    >>> otp.run(data)
            Time  A  MIN
    0 2003-12-01  1   -5
    """
    if len(objs) < 2:
        raise ValueError("otp.math.min expects at least 2 values to compare")

    def _min_func(*objs):
        dtype = get_type_by_objects(objs)
        onetick_params = map(value2str, objs)
        if dtype is nsectime:
            onetick_params = [f'NSECTIME_TO_LONG({param})' for param in onetick_params]
            onetick_params_str = ', '.join(onetick_params)
            return f"NSECTIME(MIN({onetick_params_str}))", dtype
        else:
            onetick_params_str = ', '.join(onetick_params)
            return f"MIN({onetick_params_str})", dtype

    return _Operation(
        op_func=_min_func,
        op_params=list(objs),
    )


def rand(min_value, max_value, seed=None):
    """
    Returns a pseudo-random value in the range between ``min_value`` and ``max_value`` (both inclusive).

    If ``seed`` is not specified, the function produces different values each time a query is invoked.
    If ``seed`` is specified, for this seed the function produces the same sequence of values
    each time a query is invoked.

    Parameters
    ----------
    min_value: int, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`
    max_value: int, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`
    seed: int, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **RAND** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['RAND'] = otp.math.rand(1, 1000)
    >>> otp.run(data)  # doctest: +SKIP
            Time  A  RAND
    0 2003-12-01  1   155
    """

    if isinstance(min_value, int) and min_value < 0:
        raise ValueError("It is not possible to use negative values for the `min_value`")
    if isinstance(min_value, int) and isinstance(max_value, int) and min_value >= max_value:
        raise ValueError("The `max_value` parameter should be more than `min_value`")

    def _random_func(min_value, max_value, seed=None):
        result = f'RAND({value2str(min_value)}, {value2str(max_value)}'
        if seed is not None:
            result += f', {value2str(seed)})'
        else:
            result += ')'
        return result, int

    return _Operation(
        op_func=_random_func,
        op_params=[min_value, max_value, seed],
    )


def frand(min_value=0, max_value=1, *, seed=None):
    """
    Returns a pseudo-random value in the range between ``min_value`` and ``max_value``.

    Parameters
    ----------
    min_value: float, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`
    max_value: float, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`
    seed: int, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`
        If not specified, the function produces different values each time a query is invoked.
        If specified, for this seed the function produces the same sequence of values
        each time a query is invoked.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **FRAND** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['FRAND'] = otp.math.frand()
    >>> otp.run(data)  # doctest: +SKIP
            Time  A     FRAND
    0 2003-12-01  1  0.667519
    """

    if isinstance(min_value, (int, float)) and min_value < 0:
        raise ValueError("It is not possible to use negative values for the `min_value`")
    if isinstance(min_value, (int, float)) and isinstance(max_value, (int, float)) and min_value >= max_value:
        raise ValueError("The `max_value` parameter should be more than `min_value`")

    def _frand_func(min_value, max_value, seed=None):
        func_args = [min_value, max_value]
        if seed is not None:
            func_args.append(seed)
        onetick_args_str = ', '.join(value2str(arg) for arg in func_args)
        return f'FRAND({onetick_args_str})', float

    return _Operation(
        op_func=_frand_func,
        op_params=[min_value, max_value, seed],
    )


# TODO: this is not math, let's move it somewhere else
def now(precision: int | _Operation | None = None):
    """
    Returns the current datetime in the timezone of the query.

    Parameters
    ----------
    precision: int or :class:`~onetick.py.Operation`
        Determines the milliseconds precision of the returned timestamp value.
        Valid values are 0-3.
        Default value is 3.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **NOW** OneTick function
    | **CURRENT_TIME**/**CURRENT_TIMESTAMP** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['NOW'] = otp.now()
    >>> otp.run(data)  # doctest: +SKIP
            Time  A                      NOW
    0 2003-12-01  1  2025-09-29 09:09:00.158
    """
    return _Operation(
        op_func=lambda v: (f'CURRENT_TIME({value2str(v)})' if v is not None else 'NOW()', nsectime),
        op_params=[precision],
    )


def ln(value):
    """
    Compute the natural logarithm of the ``value``.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['LN'] = otp.math.ln(2.718282)
    >>> otp.run(data)
            Time  A   LN
    0 2003-12-01  1  1.0

    See Also
    --------
    | :py:func:`onetick.py.math.exp`
    | **LOG** OneTick function
    """
    return _Operation(
        op_func=lambda v: (f'LOG({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


log = ln


def log10(value):
    """
    Compute the base-10 logarithm of the ``value``.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **LOG10** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['LOG10'] = otp.math.log10(100)
    >>> otp.run(data)
            Time  A  LOG10
    0 2003-12-01  1    2.0
    """
    return _Operation(
        op_func=lambda v: (f'LOG10({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


def exp(value):
    """
    Compute the natural exponent of the ``value``.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['E'] = otp.math.exp(1)
    >>> otp.run(data)
            Time  A         E
    0 2003-12-01  1  2.718282

    See Also
    --------
    | **EXP** OneTick function
    | :py:func:`onetick.py.math.ln`
    """
    return _Operation(
        op_func=lambda v: (f'EXP({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


def sqrt(value):
    """
    Compute the square root of the ``value``.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **SQRT** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['SQRT'] = otp.math.sqrt(4)
    >>> otp.run(data)
            Time  A  SQRT
    0 2003-12-01  1   2.0
    """
    return _Operation(
        op_func=lambda v: (f'SQRT({value2str(v)})', float),
        op_params=[value],
    )


def sign(value):
    """
    Compute the sign of the ``value``.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **SIGN** OneTick function

    Examples
    --------
    >>> data = otp.Ticks(A=[123, 0, -123])
    >>> data['SIGN'] = otp.math.sign(data['A'])
    >>> otp.run(data)
                         Time    A  SIGN
    0 2003-12-01 00:00:00.000  123     1
    1 2003-12-01 00:00:00.001    0     0
    2 2003-12-01 00:00:00.002 -123    -1
    """
    return _Operation(
        op_func=lambda v: (f'SIGN({value2str(v)})', int),
        op_params=[value],
    )


def pow(base, exponent):
    """
    Compute the ``base`` to the power of the ``exponent``.

    Parameters
    ----------
    base: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`
    exponent: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **POWER** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=2)
    >>> data['RES'] = otp.math.pow(data['A'], 10)
    >>> otp.run(data)
            Time  A     RES
    0 2003-12-01  2  1024.0
    """
    return _Operation(
        op_func=lambda b, e: (f'POWER({value2str(b)}, {value2str(e)})', _decimal_or_float(b)),
        op_params=[base, exponent],
    )


def pi():
    """
    Returns the value of Pi number.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **PI** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['PI'] = otp.math.pi()
    >>> otp.run(data)
            Time  A        PI
    0 2003-12-01  1  3.141593
    """
    return _Operation(
        op_func=lambda: ('PI()', float),
    )


def sin(value):
    """
    Returns the value of trigonometric function `sin` for the given ``value`` number expressed in radians.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['SIN'] = otp.math.sin(otp.math.pi() / 6)
    >>> otp.run(data)
            Time  A  SIN
    0 2003-12-01  1  0.5

    See Also
    --------
    | **SIN** OneTick function
    | :py:func:`onetick.py.math.pi`
    | :py:func:`onetick.py.math.asin`
    """
    return _Operation(
        op_func=lambda v: (f'SIN({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


def cos(value):
    """
    Returns the value of trigonometric function `cos` for the given ``value`` number expressed in radians.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['COS'] = otp.math.cos(otp.math.pi() / 3)
    >>> otp.run(data)
            Time  A  COS
    0 2003-12-01  1  0.5

    See Also
    --------
    | **COS** OneTick function
    | :py:func:`onetick.py.math.pi`
    | :py:func:`onetick.py.math.acos`
    """
    return _Operation(
        op_func=lambda v: (f'COS({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


def tan(value):
    """
    Returns the value of trigonometric function `tan` for the given ``value`` number expressed in radians.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['TAN'] = otp.math.tan(otp.math.pi() / 4)
    >>> otp.run(data)
            Time  A  TAN
    0 2003-12-01  1  1.0

    See Also
    --------
    | **TAN** OneTick function
    | :py:func:`onetick.py.math.pi`
    | :py:func:`onetick.py.math.atan`
    """
    return _Operation(
        op_func=lambda v: (f'TAN({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


def cot(value):
    """
    Returns the value of trigonometric function `cot` for the given ``value`` number expressed in radians.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['COT'] = otp.math.cot(otp.math.pi() / 4)
    >>> otp.run(data)
            Time  A  COT
    0 2003-12-01  1  1.0

    See Also
    --------
    | **COT** OneTick function
    | :py:func:`onetick.py.math.pi`
    | :py:func:`onetick.py.math.acot`
    """
    return _Operation(
        op_func=lambda v: (f'COT({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


def asin(value):
    """
    Returns the value of inverse trigonometric function `arcsin`.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['ASIN'] = otp.math.asin(1).round(4)  # should return pi/2 ~ 1.5708
    >>> otp.run(data)
            Time  A    ASIN
    0 2003-12-01  1  1.5708

    ``otp.math.arcsin()`` is the alias for :func:`~onetick.py.math.asin`:

    >>> data = otp.Tick(A=1)
    >>> data['ASIN'] = otp.math.arcsin(1).round(4)
    >>> otp.run(data)
            Time  A    ASIN
    0 2003-12-01  1  1.5708

    See Also
    --------
    | **ASIN** OneTick function
    | :py:func:`onetick.py.math.pi`
    | :py:func:`onetick.py.math.sin`
    """
    return _Operation(
        op_func=lambda v: (f'ASIN({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


arcsin = asin


def acos(value):
    """
    Returns the value of inverse trigonometric function `arccos`.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['ACOS'] = otp.math.acos(-1).round(4)  # should return pi ~ 3.1416
    >>> otp.run(data)
            Time  A    ACOS
    0 2003-12-01  1  3.1416

    ``otp.math.arccos()`` is the alias for :func:`~onetick.py.math.acos`:

    >>> data = otp.Tick(A=1)
    >>> data['ACOS'] = otp.math.arccos(-1).round(4)
    >>> otp.run(data)
            Time  A    ACOS
    0 2003-12-01  1  3.1416

    See Also
    --------
    | **ACOS** OneTick function
    | :py:func:`onetick.py.math.pi`
    | :py:func:`onetick.py.math.cos`
    """
    return _Operation(
        op_func=lambda v: (f'ACOS({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


arccos = acos


def atan(value):
    """
    Returns the value of inverse trigonometric function `arctan`.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['ATAN'] = otp.math.atan(1).round(4)  # should return pi/4 ~ 0.7854
    >>> otp.run(data)
            Time  A    ATAN
    0 2003-12-01  1  0.7854

    ``otp.math.arctan()`` is the alias for :func:`~onetick.py.math.atan`:

    >>> data = otp.Tick(A=1)
    >>> data['ATAN'] = otp.math.arctan(1).round(4)
    >>> otp.run(data)
            Time  A    ATAN
    0 2003-12-01  1  0.7854

    See Also
    --------
    | **ATAN** OneTick function
    | :py:func:`onetick.py.math.pi`
    | :py:func:`onetick.py.math.tan`
    """
    return _Operation(
        op_func=lambda v: (f'ATAN({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


arctan = atan


def acot(value):
    """
    Returns the value of inverse trigonometric function `arccot`.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['ACOT'] = otp.math.acot(1).round(4)  # should return pi/4 ~ 0.7854
    >>> otp.run(data)
            Time  A    ACOT
    0 2003-12-01  1  0.7854

    ``otp.math.arccot()`` is the alias for :func:`~onetick.py.math.acot`:

    >>> data = otp.Tick(A=1)
    >>> data['ACOT'] = otp.math.arccot(1).round(4)
    >>> otp.run(data)
            Time  A    ACOT
    0 2003-12-01  1  0.7854

    See Also
    --------
    | **ACOT** OneTick function
    | :py:func:`onetick.py.math.pi`
    | :py:func:`onetick.py.math.cot`
    """
    return _Operation(
        op_func=lambda v: (f'ACOT({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


arccot = acot


def atn2(y, x):
    """
    Returns the principal value of inverse trigonometric function ``arctan`` of ``y/x``.

    Parameters
    ----------
    y, x: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **ATN2** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['ATN2'] = otp.math.atn2(1, 1).round(4)  # should return pi/4 ~ 0.7854
    >>> otp.run(data)
            Time  A    ATN2
    0 2003-12-01  1  0.7854
    """
    return _Operation(
        op_func=lambda y, x: (f'ATN2({value2str(y)}, {value2str(x)})', _decimal_or_float(y, x)),
        op_params=[y, x],
    )


def mod(value1, value2):
    """
    Computes the remainder from dividing ``value1`` by ``value2``.

    Parameters
    ----------
    value1: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`
    value2: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **MOD** OneTick function
    | div

    Examples
    --------
    >>> data = otp.Tick(A=100, B=72)
    >>> data['MOD'] = otp.math.mod(data['A'], data['B'])
    >>> otp.run(data)
            Time    A   B  MOD
    0 2003-12-01  100  72   28
    """
    return _Operation(
        op_func=lambda v1, v2: (f'MOD({value2str(v1)}, {value2str(v2)})', int),
        op_params=[value1, value2],
    )


def div(value1, value2):
    """
    Computes the quotient by dividing ``value1`` by ``value2``.

    Parameters
    ----------
    value1: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`
    value2: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **DIV** OneTick function
    | mod

    Examples
    --------
    >>> data = otp.Tick(A=100, B=72)
    >>> data['DIV'] = otp.math.div(data['A'], data['B'])
    >>> otp.run(data)
            Time    A   B  DIV
    0 2003-12-01  100  72    1
    """
    return _Operation(
        op_func=lambda v1, v2: (f'DIV({value2str(v1)}, {value2str(v2)})', int),
        op_params=[value1, value2],
    )


def gcd(value1, value2):
    """
    Computes the greatest common divisor between ``value1`` and ``value2``.

    Parameters
    ----------
    value1: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`
    value2: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GCD** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=99, B=72)
    >>> data['GCD'] = otp.math.gcd(data['A'], data['B'])
    >>> otp.run(data)
            Time   A   B  GCD
    0 2003-12-01  99  72    9
    """
    return _Operation(
        op_func=lambda v1, v2: (f'GCD({value2str(v1)}, {value2str(v2)})', int),
        op_params=[value1, value2],
    )


def floor(value):
    """
    Returns a float value representing the largest number that is less than or equal to the ``value``.

    Note
    ----
    Rounding :class:`otp.nan <onetick.py.nan>` returns NaN
    and rounding :class:`otp.inf <onetick.py.inf>` returns Infinity.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **FLOOR** OneTick function
    | ceil

    Examples
    --------
    >>> data = otp.Ticks(A=[-1.7, -1.5, -1.2, -1, 0 , 1, 1.2, 1.5, 1.7, -otp.inf, otp.inf, otp.nan])
    >>> data['FLOOR'] = otp.math.floor(data['A'])
    >>> otp.run(data)
                          Time     A  FLOOR
    0  2003-12-01 00:00:00.000  -1.7   -2.0
    1  2003-12-01 00:00:00.001  -1.5   -2.0
    2  2003-12-01 00:00:00.002  -1.2   -2.0
    3  2003-12-01 00:00:00.003  -1.0   -1.0
    4  2003-12-01 00:00:00.004   0.0    0.0
    5  2003-12-01 00:00:00.005   1.0    1.0
    6  2003-12-01 00:00:00.006   1.2    1.0
    7  2003-12-01 00:00:00.007   1.5    1.0
    8  2003-12-01 00:00:00.008   1.7    1.0
    9  2003-12-01 00:00:00.009  -inf   -inf
    10 2003-12-01 00:00:00.010   inf    inf
    11 2003-12-01 00:00:00.011   NaN    NaN
    """
    def fun(v):
        v = value2str(v)
        return f'CASE({v}, NAN(), NAN(), INFINITY(), INFINITY(), -INFINITY(), -INFINITY(), FLOOR({v}) * 1.0)', float

    return _Operation(
        op_func=fun,
        op_params=[value],
    )


def ceil(value):
    """
    Returns a float value representing the smallest number that is greater than or equal to the ``value``.

    Note
    ----
    Rounding :class:`otp.nan <onetick.py.nan>` returns NaN
    and rounding :class:`otp.inf <onetick.py.inf>` returns Infinity.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **CEIL** OneTick function
    | floor

    Examples
    --------
    >>> data = otp.Ticks(A=[-1.7, -1.5, -1.2, -1, 0 , 1, 1.2, 1.5, 1.7, -otp.inf, otp.inf, otp.nan])
    >>> data['CEIL'] = otp.math.ceil(data['A'])
    >>> otp.run(data)
                          Time     A  CEIL
    0  2003-12-01 00:00:00.000  -1.7  -1.0
    1  2003-12-01 00:00:00.001  -1.5  -1.0
    2  2003-12-01 00:00:00.002  -1.2  -1.0
    3  2003-12-01 00:00:00.003  -1.0  -1.0
    4  2003-12-01 00:00:00.004   0.0   0.0
    5  2003-12-01 00:00:00.005   1.0   1.0
    6  2003-12-01 00:00:00.006   1.2   2.0
    7  2003-12-01 00:00:00.007   1.5   2.0
    8  2003-12-01 00:00:00.008   1.7   2.0
    9  2003-12-01 00:00:00.009  -inf  -inf
    10 2003-12-01 00:00:00.010   inf   inf
    11 2003-12-01 00:00:00.011   NaN   NaN
    """
    def fun(v):
        v = value2str(v)
        return f'CASE({v}, NAN(), NAN(), INFINITY(), INFINITY(), -INFINITY(), -INFINITY(), CEIL({v}) * 1.0)', float

    return _Operation(
        op_func=fun,
        op_params=[value],
    )


def round(value, precision=0, rounding_method='upward'):
    """
    Rounds value with specified ``precision`` and ``rounding_method``.

    Rounding :class:`otp.nan <onetick.py.nan>` returns NaN
    and rounding :class:`otp.inf <onetick.py.inf>` returns Infinity.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`
        The value to round.
    precision: int
        Number from -12 to 12.
        Positive precision is precision after the floating point.
        Negative precision is precision before the floating point (and the fraction part is dropped in this case).
    rounding_method: str
        Used for values that are exactly half-way between two integers (when the fraction part of value is exactly 0.5).
        Available values are **upward**, **downward**, **towards_zero**, **away_from_zero**.
        Default is **upward**.

    See Also
    --------
    | **ROUND_DOUBLE** OneTick function
    | **ROUND_DECIMAL** OneTick function

    Examples
    --------

    Different rounding methods produce different results for values that are exactly half-way between two integers:

    >>> t = otp.Ticks(A=[-123.45, 123.45, -123.4, 123.6])
    >>> t['UPWARD'] = otp.math.round(t['A'], precision=1, rounding_method='upward')
    >>> t['DOWNWARD'] = otp.math.round(t['A'], precision=1, rounding_method='downward')
    >>> t['TOWARDS_ZERO'] = otp.math.round(t['A'], precision=1, rounding_method='towards_zero')
    >>> t['AWAY_FROM_ZERO'] = otp.math.round(t['A'], precision=1, rounding_method='away_from_zero')
    >>> otp.run(t).head(2)
                         Time       A  UPWARD  DOWNWARD  TOWARDS_ZERO  AWAY_FROM_ZERO
    0 2003-12-01 00:00:00.000 -123.45  -123.4    -123.5        -123.4          -123.5
    1 2003-12-01 00:00:00.001  123.45   123.5     123.4         123.4           123.5

    Note that for other cases all methods produce the same results:

    >>> otp.run(t).tail(2)
                         Time      A  UPWARD  DOWNWARD  TOWARDS_ZERO  AWAY_FROM_ZERO
    2 2003-12-01 00:00:00.002 -123.4  -123.4    -123.4        -123.4          -123.4
    3 2003-12-01 00:00:00.003  123.6   123.6     123.6         123.6           123.6

    Positive precision truncates to the number of digits *after* floating point:

    >>> t = otp.Ticks(A=[-123.45, 123.45])
    >>> t['ROUND1'] = otp.math.round(t['A'], 1)
    >>> t['ROUND2'] = otp.math.round(t['A'], 2)
    >>> otp.run(t)
                         Time       A  ROUND1   ROUND2
    0 2003-12-01 00:00:00.000 -123.45  -123.4  -123.45
    1 2003-12-01 00:00:00.001  123.45   123.5   123.45

    Negative precision truncates to the number of digits *before* floating point
    (and the fraction part is dropped in this case):

    >>> t = otp.Ticks(A=[-123.45, 123.45])
    >>> t['ROUND_M1'] = otp.math.round(t['A'], -1)
    >>> t['ROUND_M2'] = otp.math.round(t['A'], -2)
    >>> otp.run(t)
                         Time       A  ROUND_M1  ROUND_M2
    0 2003-12-01 00:00:00.000 -123.45    -120.0    -100.0
    1 2003-12-01 00:00:00.001  123.45     120.0     100.0

    Rounding :class:`otp.nan <onetick.py.nan>` returns NaN
    and rounding :class:`otp.inf <onetick.py.inf>` returns Infinity in all cases:

    >>> t = otp.Ticks(A=[otp.inf, -otp.inf, otp.nan])
    >>> t['ROUND_0'] = otp.math.round(t['A'])
    >>> t['ROUND_P2'] = otp.math.round(t['A'], 2)
    >>> t['ROUND_M2'] = otp.math.round(t['A'], -2)
    >>> otp.run(t)
                         Time    A  ROUND_0  ROUND_P2  ROUND_M2
    0 2003-12-01 00:00:00.000  inf      inf       inf       inf
    1 2003-12-01 00:00:00.001 -inf     -inf      -inf      -inf
    2 2003-12-01 00:00:00.002  NaN      NaN       NaN       NaN
    """
    if not -12 <= precision <= 12:
        raise ValueError("Parameter 'precision' must be an integer in range [-12, 12]")
    supported_rounding_methods = {'upward', 'downward', 'towards_zero', 'away_from_zero'}
    if rounding_method not in supported_rounding_methods:
        raise ValueError(f"Parameter 'rounding_method' must be one of {supported_rounding_methods}")

    def fun(value, precision, rounding_method):
        dtype = _decimal_or_float(value)
        args_str = f'{value2str(value)},{value2str(precision)},{value2str(rounding_method)}'
        if dtype is decimal:
            return f'ROUND_DECIMAL({args_str})', dtype
        else:
            return f'ROUND_DOUBLE({args_str})', dtype

    return _Operation(
        op_func=fun,
        op_params=[value, precision, rounding_method],
    )


def radians(value):
    """
    Returns the corresponding angle, in radians, for ``value`` specified in degrees.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **RADIANS** OneTick function
    | :func:`~onetick.py.math.degrees`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['RADIANS'] = otp.math.radians(45).round(4)  # should return pi/4 ~ 0.7854
    >>> otp.run(data)
            Time  A  RADIANS
    0 2003-12-01  1   0.7854
    """
    return _Operation(
        op_func=lambda v: (f'RADIANS({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


def degrees(value):
    """
    Returns the corresponding angle, in degrees, for ``value`` specified in radians.

    Parameters
    ----------
    value: int, float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **DEGREES** OneTick function
    | :func:`~onetick.py.math.radians`

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['DEGREES'] = otp.math.degrees(otp.math.pi() / 4)  # should return 45
    >>> otp.run(data)
            Time  A  DEGREES
    0 2003-12-01  1     45.0
    """
    return _Operation(
        op_func=lambda v: (f'DEGREES({value2str(v)})', _decimal_or_float(v)),
        op_params=[value],
    )


def truncate(number, precision=0):
    """
    Returns the ``number`` truncated to ``precision`` places right of the decimal point.

    If ``precision`` is negative, the number is truncated to the left of the decimal point.

    Note
    ----
    Truncating :class:`otp.nan <onetick.py.nan>` returns NaN
    and truncating :class:`otp.inf <onetick.py.inf>` returns Infinity.

    Parameters
    ----------
    number: float, :class:`~onetick.py.decimal`, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`
    precision: int, :class:`~onetick.py.Operation`, :class:`~onetick.py.Column`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **TRUNCATE** OneTick function

    Examples
    --------
    >>> data = otp.Ticks(A=[1.0, 1.23456, 123.456, 12345.6, otp.nan, otp.inf, -otp.inf])
    >>> data['TRUNCATED'] = otp.math.truncate(data['A'])
    >>> data['TRUNCATED_P2'] = otp.math.truncate(data['A'], 2)
    >>> data['TRUNCATED_M2'] = otp.math.truncate(data['A'], -2)
    >>> otp.run(data)
                         Time            A  TRUNCATED  TRUNCATED_P2  TRUNCATED_M2
    0 2003-12-01 00:00:00.000      1.00000        1.0          1.00           0.0
    1 2003-12-01 00:00:00.001      1.23456        1.0          1.23           0.0
    2 2003-12-01 00:00:00.002    123.45600      123.0        123.45         100.0
    3 2003-12-01 00:00:00.003  12345.60000    12345.0      12345.60       12300.0
    4 2003-12-01 00:00:00.004          NaN        NaN           NaN           NaN
    5 2003-12-01 00:00:00.005          inf        inf           inf           inf
    6 2003-12-01 00:00:00.006         -inf       -inf          -inf          -inf
    """
    return _Operation(
        op_func=lambda number, precision: (f'TRUNCATE({value2str(number)},{value2str(precision)})',
                                           _decimal_or_float(number)),
        op_params=[number, precision],
    )
