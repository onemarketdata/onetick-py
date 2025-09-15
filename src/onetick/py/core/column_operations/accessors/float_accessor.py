from onetick.py import types as ott
from onetick.py.core.column_operations.accessors._accessor import _Accessor


class _FloatAccessor(_Accessor):
    """
    Accessor for float (double in Onetick terminology) functions

    >>> data = otp.Ticks(X=[1.1, 1.2])
    >>> data["Y"] = data["X"].float.<function_name>()   # doctest: +SKIP
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

        Examples
        --------

        >>> data = otp.Ticks(X=[1, 2.17, 10.31861, 3.141593])
        >>> # OTdirective: snippet-name: float operations.to string.constant precision;
        >>> data["X"] = data["X"].float.str(15, 3)
        >>> data = otp.run(data)
        >>> data["X"]
        0    1.000
        1    2.170
        2    10.319
        3    3.142
        Name: X, dtype: object

        >>> data = otp.Ticks(X=[1, 2.17, 10.31841, 3.141593],
        ...                  LENGTH=[2, 3, 4, 5],
        ...                  PRECISION=[5, 5, 3, 3])
        >>> # OTdirective: snippet-name: float operations.to string.precision from fields;
        >>> data["X"] = data["X"].float.str(data["LENGTH"], data["PRECISION"])
        >>> data = otp.run(data)
        >>> data["X"]
        0    1
        1    2.2
        2    10.3
        3    3.142
        Name: X, dtype: object
        """
        dtype = ott.string[length] if isinstance(length, int) else str
        length = ott.value2str(length)
        precision = ott.value2str(precision)
        return _FloatAccessor.Formatter(self._base_column,
                                        dtype,
                                        formatter=lambda x: f"str({x}, {length}, {precision})")

    def cmp(self, other, eps):
        """
        Compare two double values between themselves according to ``eps`` relative difference.

        This function returns 0 if column = other, 1 if column > other, and -1 if column < other.
        Two numbers are considered to be equal if both of them are NaN or
        ``abs(column - other) / (abs(column) + abs(other)) < eps``.
        In other words, ``eps`` represents a relative difference (percentage) between the two numbers,
        not an absolute difference.

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
        eq

        Examples
        --------

        >>> data = otp.Ticks(X=[1, 2.17, 10.31841, 3.141593, 6],
        ...                  OTHER=[1.01, 2.1, 10.32841, 3.14, 5],
        ...                  EPS=[0, 1, 0.1, 0.001, 0.001])
        >>> # OTdirective: snippet-name: float operations.approximate comparison.lt|eq|gt;
        >>> data["X"] = data["X"].float.cmp(data["OTHER"], data["EPS"])
        >>> data = otp.run(data)
        >>> data["X"]
        0   -1.0
        1    0.0
        2    0.0
        3    0.0
        4    1.0
        Name: X, dtype: float64
        """
        other = ott.value2str(other)
        eps = ott.value2str(eps)
        return _FloatAccessor.Formatter(self._base_column,
                                        float,
                                        formatter=lambda x: f"double_compare({x}, {other}, {eps})")

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
        cmp

        Returns
        -------
            Operation

        Examples
        --------

        >>> data = otp.Ticks(X=[1, 2.17, 10.31841, 3.141593, 6],
        ...                  OTHER=[1.01, 2.1, 10.32841, 3.14, 5],
        ...                  DELTA=[0, 1, 0.1, 0.01, 0.001])
        >>> # OTdirective: snippet-name: float operations.approximate comparison.eq;
        >>> data["X"] = (1 + data["X"] - 1).float.eq(data["OTHER"], data["DELTA"])
        >>> data = otp.run(data)
        >>> data["X"]
        0    0.0
        1    1.0
        2    1.0
        3    1.0
        4    0.0
        Name: X, dtype: float64
        """
        other = ott.value2str(other)
        delta = ott.value2str(delta)
        return _FloatAccessor.Formatter(self._base_column,
                                        bool,
                                        formatter=lambda x: f"abs({x} - {other}) <= {delta}")
