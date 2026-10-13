from onetick.py import types as ott
from onetick.py.core.column_operations.accessors._accessor import _Accessor


class _FloatAccessor(_Accessor):
    """
    Accessor for float (double in Onetick terminology) functions

    >>> data = otp.Ticks(X=[1.1, 1.2])
    >>> data["Y"] = data["X"].float.<function_name>()  # doctest: +SKIP
    """

    def str(self, length=10, precision=6):
        """
        Converts float to str.

        Converts number to string with given ``length`` and ``precision``.
        The specified ``length`` should be greater than or equal
        to the part of the number before the decimal point plus the number's sign (if any).

        If ``length`` is specified as an int, the method will return strings with ``length`` characters,
        if ``length`` is specified as a column, the method will return string default (64 characters) length.

        Parameters
        ----------
        length: Operation or int
            Length of the string.
        precision: Operation or int
            Number of symbols after dot.

        Returns
        -------
        result: Operation
            String representation of float value.

        See Also
        --------
        | **STR** OneTick function

        Examples
        --------

        >>> data = otp.Ticks(X=[1, 2.17, 10.31861, 3.141593, otp.nan, otp.inf, -otp.inf])
        >>> # OTdirective: snippet-name: float operations.to string.constant precision;
        >>> data['Y'] = data['X'].float.str(15, 3)
        >>> otp.run(data)
                             Time          X       Y
        0 2003-12-01 00:00:00.000   1.000000   1.000
        1 2003-12-01 00:00:00.001   2.170000   2.170
        2 2003-12-01 00:00:00.002  10.318610  10.319
        3 2003-12-01 00:00:00.003   3.141593   3.142
        4 2003-12-01 00:00:00.004        NaN     nan
        5 2003-12-01 00:00:00.005        inf     inf
        6 2003-12-01 00:00:00.006       -inf    -inf

        Parameters ``length`` and ``precision`` can also be taken from the columns:

        >>> data = otp.Ticks(X=[1, 2.17, 10.31841, 3.141593],
        ...                  LENGTH=[2, 3, 4, 5],
        ...                  PRECISION=[5, 5, 3, 3])
        >>> # OTdirective: snippet-name: float operations.to string.precision from fields;
        >>> data['Y'] = data['X'].float.str(data['LENGTH'], data['PRECISION'])
        >>> otp.run(data)
                             Time          X  LENGTH  PRECISION      Y
        0 2003-12-01 00:00:00.000   1.000000       2          5      1
        1 2003-12-01 00:00:00.001   2.170000       3          5    2.2
        2 2003-12-01 00:00:00.002  10.318410       4          3   10.3
        3 2003-12-01 00:00:00.003   3.141593       5          3  3.142
        """
        dtype = ott.string[length] if isinstance(length, int) else str

        def formatter(column, _length, _precision):
            column = ott.value2str(column)
            _length = ott.value2str(_length)
            _precision = ott.value2str(_precision)
            str_expr = f"str({column}, {_length}, {_precision})"
            # BDS-478: str() function raises exception for nan and inf values, so converting them manually
            str_expr = f'CASE({column}, NAN(), "nan", INFINITY(), "inf", -INFINITY(), "-inf", {str_expr})'
            return str_expr

        return _FloatAccessor.Formatter(
            op_params=[self._base_column, length, precision],
            dtype=dtype,
            formatter=formatter,
        )

    def cmp(self, other, eps):
        """
        Compare two double values between themselves according to ``eps`` relative difference.

        This function returns 0 if column == other, 1 if column > other, and -1 if column < other.

        If both values are NaN, the result is 0.
        If only one value is NaN, NaN is treated as less than any non-NaN value.

        Two numbers are considered to be equal if:

        * ``abs(column - other) <= 1e-12`` (absolute tolerance; useful near zero)
        * or ``abs(column - other) / max(1, max(abs(column), abs(other))) <= eps`` (relative tolerance).

        ``eps`` is a relative epsilon (scale-dependent), not an absolute difference.

        Parameters
        ----------
        other: Operation or float
            column or value to compare with
        eps: Operation or float
            column or value with relative difference

        Returns
        -------
        result: Operation
            0 if column == other, 1 if column > other, and -1 if column < other.

        See Also
        --------
        | eq
        | **DOUBLE_COMPARE** OneTick function

        Examples
        --------

        >>> data = otp.Ticks(X=[2.17, 2.17, 10.31841, 3.141593, 5],
        ...                  OTHER=[2.1, 2.1, 10.32841, 3.14, 6],
        ...                  EPS=[0.01, 0.1, 0.1, 0.0001, 0.01])
        >>> # OTdirective: snippet-name: float operations.approximate comparison.lt|eq|gt;
        >>> data['Y'] = data['X'].float.cmp(data['OTHER'], data['EPS'])
        >>> otp.run(data)  # doctest: +SKIP
                             Time          X     OTHER     EPS    Y
        0 2003-12-01 00:00:00.000   2.170000   2.10000  0.0100  1.0
        1 2003-12-01 00:00:00.001   2.170000   2.10000  0.1000  0.0
        2 2003-12-01 00:00:00.002  10.318410  10.32841  0.1000  0.0
        3 2003-12-01 00:00:00.003   3.141593   3.14000  0.0001  1.0
        4 2003-12-01 00:00:00.004   5.000000   6.00000  0.0100 -1.0
        """
        def formatter(column, _other, _eps):
            column = ott.value2str(column)
            _other = ott.value2str(_other)
            _eps = ott.value2str(_eps)
            return f"double_compare({column}, {_other}, {_eps})"

        return _FloatAccessor.Formatter(
            op_params=[self._base_column, other, eps],
            dtype=float,
            formatter=formatter,
        )

    def eq(self, other, delta):
        """
        Compare two double values between themselves according to ``delta`` relative difference.
        Calculated as ``abs(column - other) <= delta``.

        Parameters
        ----------
        other: Operation, float
            column or value to compare with
        delta: Operation, float
            column or value with relative difference

        See Also
        --------
        | cmp

        Returns
        -------
            Operation

        Examples
        --------

        >>> data = otp.Ticks(X=[1, 2.17, 10.31841, 3.141593, 6],
        ...                  OTHER=[1.01, 2.1, 10.32841, 3.14, 5],
        ...                  DELTA=[0, 1, 0.1, 0.01, 0.001])
        >>> # OTdirective: snippet-name: float operations.approximate comparison.eq;
        >>> data['Y'] = (1 + data['X'] - 1).float.eq(data['OTHER'], data['DELTA'])
        >>> otp.run(data)
                             Time          X     OTHER  DELTA    Y
        0 2003-12-01 00:00:00.000   1.000000   1.01000  0.000  0.0
        1 2003-12-01 00:00:00.001   2.170000   2.10000  1.000  1.0
        2 2003-12-01 00:00:00.002  10.318410  10.32841  0.100  1.0
        3 2003-12-01 00:00:00.003   3.141593   3.14000  0.010  1.0
        4 2003-12-01 00:00:00.004   6.000000   5.00000  0.001  0.0
        """
        def formatter(column, _other, _delta):
            column = ott.value2str(column)
            _other = ott.value2str(_other)
            _delta = ott.value2str(_delta)
            return f"abs({column} - {_other}) <= {_delta}"

        return _FloatAccessor.Formatter(
            op_params=[self._base_column, other, delta],
            dtype=bool,
            formatter=formatter,
        )

    def to_32nds(self, *, throw_if_not_convertible: bool = True):
        """
        Converts double number to *32nds* format string.

        If ``throw_if_not_convertible`` flag is set to True (default),
        function will throw an exception if it can not convert double to a *32nds* string,
        otherwise it will return a string whose double implementation is close to the given number.

        Parameters
        ----------
        throw_if_not_convertible: bool or :class:`~onetick.py.Operation`

        Returns
        -------
        :class:`~onetick.py.Operation`

        See Also
        --------
        | **CONVERT_DOUBLE_TO_32NDS** OneTick function
        | :meth:`~onetick.py.core.column_operations.accessors.str_accessor._StrAccessor.from_32nds`

        Examples
        --------

        Convert double number to *32nds* format:

        >>> data = otp.Ticks(PRICE=[100.0, 101 + 1/32, 102 + 16/32, 103 + 16.5/32, 104 + 7/256])
        >>> data['PRICE_32NDS'] = data['PRICE'].float.to_32nds()
        >>> otp.run(data)
                             Time       PRICE  PRICE_32NDS
        0 2003-12-01 00:00:00.000  100.000000       100-00
        1 2003-12-01 00:00:00.001  101.031250       101-01
        2 2003-12-01 00:00:00.002  102.500000       102-16
        3 2003-12-01 00:00:00.003  103.515625      103-16+
        4 2003-12-01 00:00:00.004  104.027344      104-007

        If the number can't be converted, exception will be raised:

        >>> data = otp.Ticks(PRICE=[100.0 + 1/33])
        >>> data['PRICE_32NDS'] = data['PRICE'].float.to_32nds()
        >>> otp.run(data)  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        Exception...Can't convert double to 32nds format string...

        In this case parameter ``throw_if_not_convertible=False`` can be set,
        and unconvertible values will be converted to the closest *32nds* value:

        >>> data = otp.Ticks(PRICE=[100.0 + 1/33])
        >>> data['PRICE_32NDS'] = data['PRICE'].float.to_32nds(throw_if_not_convertible=False)
        >>> otp.run(data)
                Time       PRICE PRICE_32NDS
        0 2003-12-01  100.030303     100-007
        """
        def formatter(column, throw_if_not_convertible):
            if isinstance(throw_if_not_convertible, bool):
                throw_if_not_convertible = f"'{str(throw_if_not_convertible).upper()}'"
            else:
                throw_if_not_convertible = ott.value2str(throw_if_not_convertible)
            return f'CONVERT_DOUBLE_TO_32NDS({ott.value2str(column)},{throw_if_not_convertible})'

        return _FloatAccessor.Formatter(
            op_params=[self._base_column, throw_if_not_convertible],
            dtype=str,
            formatter=formatter,
        )
