from onetick.py import types as ott
from onetick.py.core.column_operations.accessors._accessor import _Accessor
from onetick.py.core.column_operations.base import _Operation


class _DecimalAccessor(_Accessor):
    """
    Accessor for decimal functions

    >>> data = otp.Ticks(X=[otp.decimal(1.1), otp.decimal(1.2)])
    >>> data["Y"] = data["X"].decimal.<function_name>()   # doctest: +SKIP
    """

    def str(self, precision=8):
        """
        Converts decimal to str.

        Parameters
        ----------
        precision: Operation or int
            Number of digits after floating point.

        Returns
        -------
        result: Operation
            String representation of decimal value.

        Examples
        --------

        >>> data = otp.Ticks(X=[otp.decimal(1), otp.decimal(2.17), otp.decimal(10.31861), otp.decimal(3.141593)])
        >>> data['X'] = data['X'].decimal.str(precision=3)
        >>> data = otp.run(data)
        >>> data['X']
        0    1.000
        1    2.170
        2    10.319
        3    3.142
        Name: X, dtype: object
        """
        def formatter(column, _precision):
            column = ott.value2str(column)
            _precision = ott.value2str(_precision)

            return f'decimal_to_string({column}, {_precision})'

        return _DecimalAccessor.Formatter(
            op_params=[self._base_column, precision],
            dtype=str,
            formatter=formatter,
        )

    def cmp(self, other, eps):
        """
        Compare two decimal values according to ``eps`` relative difference.

        This function returns 0 if column = other, 1 if column > other, and -1 if column < other.
        Two numbers are considered to be equal if both of them are NaN or
        ``abs(column - other) / (abs(column) + abs(other)) < eps``.
        In other words, ``eps`` represents a relative difference (percentage) between the two numbers,
        not an absolute difference.

        Parameters
        ----------
        other: Operation or decimal
            column or value to compare with
        eps: Operation or decimal
            column or value with relative difference

        Returns
        -------
        result: Operation
            0 if column == other, 1 if column > other, and -1 if column < other.

        Examples
        --------

        >>> data = otp.Ticks(
        ...     X=[otp.decimal(1), otp.decimal(2.17), otp.decimal(10.31841), otp.decimal(3.141593), otp.decimal(6)],
        ...     OTHER=[otp.decimal(1.01), otp.decimal(2.1), otp.decimal(10.32841), otp.decimal(3.14), otp.decimal(5)],
        ...     EPS=[0, 1, 0.1, 0.001, 0.001]
        ... )
        >>> data['X'] = data['X'].decimal.cmp(data['OTHER'], data['EPS'])
        >>> data = otp.run(data)
        >>> data['X']
        0   -1.0
        1    0.0
        2    0.0
        3    0.0
        4    1.0
        Name: X, dtype: float64
        """

        def formatter(column, _other, _eps):
            column = ott.value2str(column)
            _other = ott.value2str(_other)
            _eps = ott.value2str(_eps)
            return f'decimal_compare({column}, {_other}, {_eps})'

        return _DecimalAccessor.Formatter(
            op_params=[self._base_column, other, eps],
            dtype=ott.decimal,
            formatter=formatter,
        )
