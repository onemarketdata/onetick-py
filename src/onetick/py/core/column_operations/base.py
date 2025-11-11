import copy
import warnings
from typing import Type, Sequence

from onetick.py import types as ott
from onetick.py.core.column_operations import _methods

from onetick.py.core.column_operations._methods.op_types import (
    are_ints_not_time,
    are_time,
    are_floats,
    are_strings
)
from onetick.py.core.column_operations._methods.methods import DatetimeSubtractionWarning


class Expr:
    """
    EP parameter's value can be set to an expression.
    Expressions are evaluated before parameters are actually passed to event processors.

    See also
    --------
    :py:attr:`onetick.py.Operation.expr`
    """
    def __init__(self, operation):
        self.operation = operation

    def __str__(self):
        return f'expr({str(self.operation)})'

    def __repr__(self):
        """onetick.query_webapi calls repr() on the object to get the string representation of the object"""
        # TODO
        from onetick.py.otq import otq
        return otq.onetick_repr(self.__str__())


class Operation:
    """
    :py:class:`~onetick.py.Source` column operation container.

    This is the object you get when applying most operations on :py:class:`~onetick.py.Column`
    or on other operations.
    Eventually you can add a new column using the operation you got or pass it as a parameter
    to some functions.

    Examples
    --------
    >>> t = otp.Tick(A=1)
    >>> t['A']
    Column(A, <class 'int'>)
    >>> t['A'] / 2
    Operation((A) / (2))
    >>> t['B'] = t['A'] / 2
    >>> t['B']
    Column(B, <class 'float'>)
    """
    emulation_enabled = False

    def __init__(self, op_func=None, op_params=None, dtype=None, obj_ref=None, op_str=None):
        self._op_func = op_func
        self._op_params = op_params
        self.obj_ref = obj_ref
        self.__warnings = []
        if op_func:
            if op_str:
                raise ValueError("You should specify either op_func or op_str")
            with warnings.catch_warnings(record=True) as warning_list:
                # we want to raise this warning only in some cases
                # that's why we're catching it and saving for later use
                warnings.simplefilter('always', category=DatetimeSubtractionWarning)
                op_str, dtype = self._evaluate_func()

            for w in warning_list:
                if w.category is DatetimeSubtractionWarning:
                    self.__warnings.append(w)
                else:
                    warnings.warn_explicit(w.message, w.category, w.filename, w.lineno)

        self._op_str = op_str
        self._dtype = dtype

    def __bool__(self):
        if Operation.emulation_enabled:
            # True is default for classes without overriden __bool__
            return True
        raise TypeError('It is not allowed to use compare in if-else and while clauses')

    def __str__(self):
        return self.op_str

    def __repr__(self):
        return f"Operation({str(self)})"

    @property
    def dtype(self):
        """
        Returns the type of the column or operation.

        See also
        --------
        :py:meth:`Source.schema <onetick.py.Source.schema>`

        Examples
        --------
        >>> t = otp.Tick(A=1, B=2.3, C='3')
        >>> t['TIMESTAMP'].dtype
        <class 'onetick.py.types.nsectime'>
        >>> t['A'].dtype
        <class 'int'>
        >>> t['B'].dtype
        <class 'float'>
        >>> t['C'].dtype
        <class 'str'>
        """
        dtype = self._dtype
        if not dtype:
            _, dtype = self._evaluate_func(set_fields=True)
        return dtype

    @property
    def op_str(self):
        for w in self.__warnings:
            warnings.showwarning(w.message, w.category, w.filename, w.lineno)
        op_str = self._op_str
        if not op_str:
            op_str, _ = self._evaluate_func(set_fields=True)
        return op_str

    @property
    def expr(self):
        """
        Get expression to use in EP parameters.

        See also
        --------
        :py:class:`~onetick.py.core.column_operations.base.Expr`
        """
        return Expr(self)

    def round(self, precision=0):
        """
        Rounds input column with specified ``precision``.

        Rounding :class:`otp.nan <onetick.py.nan>` returns NaN
        and rounding :class:`otp.inf <onetick.py.inf>` returns Infinity.

        For values that are exactly half-way between two integers (when the fraction part of value is exactly 0.5),
        the rounding method used here is *upwards*, which returns the bigger number.
        For other rounding methods see :func:`otp.math.round <onetick.py.math.round>` function.

        Parameters
        ----------
        precision: int
            Number from -12 to 12.
            Positive precision is precision after the floating point.
            Negative precision is precision before the floating point.

        See also
        --------
        __round__

        Examples
        --------
        >>> t = otp.Tick(A=1234.5678)
        >>> t['B'] = t['A'].round()
        >>> t['C'] = t['A'].round(2)
        >>> t['D'] = t['A'].round(-2)
        >>> otp.run(t)
                Time          A       B        C       D
        0 2003-12-01  1234.5678  1235.0  1234.57  1200.0

        Returns
        -------
        Operation
        """
        return round(self, precision)

    def map(self, arg, default=None):
        """
        Map values of the column to new values according to the mapping in ``arg``.
        If the value is not in the mapping, it is set to the ``default`` value.
        If ``default`` value is not set, it is set to default value for the column type.

        Parameters
        ----------
        arg: dict
            Mapping from old values to new values.
            All values must have the same type, compatible with the column type.
        default: simple value or Column or Operation
            Default value if no mapping is found in ``arg``.
            By default, it is set to default value for the column type.
            (0 for numbers, empty string for strings, etc.)

        Examples
        --------
        >>> t = otp.Ticks(A=[1, 2, 3, 4, 5])
        >>> t['B'] = t['A'].map({1: 10, 2: 20, 3: 30})
        >>> otp.run(t)
                             Time  A   B
        0 2003-12-01 00:00:00.000  1  10
        1 2003-12-01 00:00:00.001  2  20
        2 2003-12-01 00:00:00.002  3  30
        3 2003-12-01 00:00:00.003  4   0
        4 2003-12-01 00:00:00.004  5   0

        Example with ``default`` parameter set:

        >>> t = otp.Ticks(A=[1, 2, 3, 4, 5])
        >>> t['B'] = t['A'].map({1: 10, 2: 20, 3: 30}, default=-1)
        >>> otp.run(t)
                             Time  A   B
        0 2003-12-01 00:00:00.000  1  10
        1 2003-12-01 00:00:00.001  2  20
        2 2003-12-01 00:00:00.002  3  30
        3 2003-12-01 00:00:00.003  4  -1
        4 2003-12-01 00:00:00.004  5  -1

        ``default`` parameter can also be an :py:class:`~onetick.py.Operation` or :py:class:`~onetick.py.Column`.
        For example, to keep unmapped values:

        >>> t = otp.Ticks(A=[1, 2, 3, 4, 5])
        >>> t['B'] = t['A'].map({1: 10, 2: 20, 3: 30}, default=t['A'])
        >>> otp.run(t)
                             Time  A   B
        0 2003-12-01 00:00:00.000  1  10
        1 2003-12-01 00:00:00.001  2  20
        2 2003-12-01 00:00:00.002  3  30
        3 2003-12-01 00:00:00.003  4   4
        4 2003-12-01 00:00:00.004  5   5

        Returns
        -------
        Operation
        """
        if not isinstance(arg, dict) or not arg:
            raise TypeError("map() argument must be a dict with keys and values to map")

        try:
            values_type = ott.get_type_by_objects(arg.values())
        except TypeError as e:
            raise TypeError("map() argument must be a dict with same types for all values") from e

        if default is not None:
            try:
                default_type = ott.get_type_by_objects([default])
                ott.get_type_by_objects([default_type, values_type])
            except TypeError as e:
                raise TypeError(
                    f"map() default value type {default_type} must be compatible with values type {values_type}"
                ) from e

        try:
            keys_type = ott.get_type_by_objects(arg.keys())
        except TypeError as e:
            raise TypeError("map() argument must be a dict with same types for all keys") from e

        try:
            ott.get_type_by_objects([keys_type, self.dtype])
        except TypeError as e:
            raise TypeError(f"map() keys type {keys_type} must be compatible with column type {self.dtype}") from e

        return _Operation(_methods._map, [self, arg, values_type, default])

    def apply(self, lambda_f):
        """
        Apply function or type to column

        Parameters
        ----------
        lambda_f: type or callable
            if type - will convert column to requested type

            if callable - will translate python code to similar OneTick's CASE expression.
            There are some limitations to which python operators can be used in this callable.
            See :ref:`Python callables parsing guide <python callable parser>` article for details.
            In :ref:`Remote OTP with Ray<ray-remote>` any `Callable` must be decorated with `@otp.remote` decorator,
            see :ref:`Ray usage examples<apply-remote-context>` for details.

        Examples
        --------
        Converting type of the column, e.g. string column to integer:

        >>> data = otp.Ticks({'A': ['1', '2', '3']})
        >>> data['B'] = data['A'].apply(int) + 10   # OTdirective: snippet-name: column operations.type convertation;
        >>> otp.run(data)
                             Time  A   B
        0 2003-12-01 00:00:00.000  1  11
        1 2003-12-01 00:00:00.001  2  12
        2 2003-12-01 00:00:00.002  3  13

        More complicated logic:

        >>> data = otp.Ticks({'A': [-321, 0, 123]})
        >>> data['SIGN'] = data['A'].apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
        >>> otp.run(data)
                             Time    A  SIGN
        0 2003-12-01 00:00:00.000 -321    -1
        1 2003-12-01 00:00:00.001    0     0
        2 2003-12-01 00:00:00.002  123     1

        See also
        --------
        :py:meth:`onetick.py.Source.apply`
        :ref:`Python callables parsing guide <python callable parser>`
        """
        if isinstance(lambda_f, Type) and ott.is_type_basic(lambda_f):
            return self._convert_to(lambda_f)

        from onetick.py.core.lambda_object import apply_lambda

        return apply_lambda(lambda_f, self)

    def astype(self, to_type):
        """
        Alias for the :meth:`apply` method with type.

        See also
        --------
        :meth:`apply`

        Examples
        --------
        >>> data = otp.Tick(A=1, B=2.2, C='3.3')
        >>> data['A'] = data['A'].astype(str) + 'A'
        >>> data['B'] = data['B'].astype(int) + 1
        >>> data['C'] = data['C'].astype(float) + 0.1
        >>> otp.run(data)
                Time  B   A    C
        0 2003-12-01  3  1A  3.4
        """
        return self.apply(to_type)

    def isin(self, *items):
        """
        Check if column's value is in ``items``.

        Parameters
        ----------
        items
            Possible string or numeric values to be checked against column's value.
            Such values can be passed as the function arguments
            or as the list of such values in the first function argument.

        Returns
        -------
        Operation
            Returns an :py:class:`onetick.py.Operation` object with the value of 1.0 if
            the column's value was found among ``items`` or 0.0 otherwise.

        See also
        --------
        :py:meth:`Source.__getitem__`

        Examples
        --------

        Passing values as function arguments:

        >>> data = otp.Ticks(A=['a', 'b', 'c'])
        >>> data['B'] = data['A'].isin('a', 'c')
        >>> otp.run(data)
                             Time  A    B
        0 2003-12-01 00:00:00.000  a  1.0
        1 2003-12-01 00:00:00.001  b  0.0
        2 2003-12-01 00:00:00.002  c  1.0

        Passing values as a list:

        >>> data = otp.Ticks(A=['a', 'b', 'c'])
        >>> data['B'] = data['A'].isin(['a', 'c'])
        >>> otp.run(data)
                             Time  A    B
        0 2003-12-01 00:00:00.000  a  1.0
        1 2003-12-01 00:00:00.001  b  0.0
        2 2003-12-01 00:00:00.002  c  1.0

        This function's result can be used as filter expression:

        >>> data = otp.Ticks(A=[1, 2, 3, 0])
        >>> yes, no = data[data["A"].isin(0, 1)]    # OTdirective: snippet-name: column operations.is in.constant;
        >>> otp.run(yes)[["A"]]
           A
        0  1
        1  0

        :py:class:`Columns <onetick.py.Column>` and :py:class:`operations <onetick.py.Operation>` are also supported:

        >>> # OTdirective: snippet-name: column operations.is in.from fields;
        >>> data = otp.Ticks(A=["ab", "cv", "bc", "a", "d"], B=["a", "c", "b", "a", "a"])
        >>> yes, no = data[data["A"].isin(data["B"], data["B"] + "b")]
        >>> otp.run(yes)[["A", "B"]]
            A  B
        0  ab  a
        1   a  a
        """
        if items and isinstance(items[0], Sequence) and not isinstance(items[0], str):
            if len(items) > 1:
                raise ValueError("If the first argument of isin() function is a list,"
                                 " it must be the only argument passed to a function.")
            items = items[0]
        return _Operation(_methods.isin, [self, items])

    def fillna(self, value):
        """
        Fill :py:class:`~onetick.py.nan` values with ``value``.

        Parameters
        ----------
        value: float, int, :py:class:`~onetick.py.Operation`
            value to use instead :py:class:`~onetick.py.nan`

        Examples
        --------

        Replace NaN values with a constant:

        >>> data = otp.Ticks({'A': [1, otp.nan, 2]})
        >>> data['A'] = data['A'].fillna(100)   # OTdirective: snippet-name: column operations.fillna;
        >>> otp.run(data)
                             Time      A
        0 2003-12-01 00:00:00.000    1.0
        1 2003-12-01 00:00:00.001  100.0
        2 2003-12-01 00:00:00.002    2.0

        Replace NaN values with a value from the previous tick:

        >>> data = otp.Ticks({'A': [1, otp.nan, 2]})
        >>> data['A'] = data['A'].fillna(data['A'][-1])
        >>> otp.run(data)
                             Time    A
        0 2003-12-01 00:00:00.000  1.0
        1 2003-12-01 00:00:00.001  1.0
        2 2003-12-01 00:00:00.002  2.0
        """
        return _Operation(_methods.fillna, [self, value])

    @property
    def str(self):
        """
        Property that provides access to methods specific to string types.

        See also
        --------
        :py:class:`otp.string <onetick.py.types.string>`
        """

        if issubclass(self.dtype, str):
            from onetick.py.core.column_operations.accessors.str_accessor import _StrAccessor
            return _StrAccessor(self)
        else:
            raise TypeError(".str accessor is available only for string type columns")

    @property
    def dt(self):
        """
        Property that provides access to methods specific to datetime types.

        See also
        --------
        :py:class:`otp.nsectime <onetick.py.types.nsectime>`
        :py:class:`otp.msectime <onetick.py.types.msectime>`
        """

        if issubclass(self.dtype, ott.nsectime) \
                or issubclass(self.dtype, ott.msectime):
            from onetick.py.core.column_operations.accessors.dt_accessor import _DtAccessor
            return _DtAccessor(self)
        else:
            raise TypeError(".dt accessor is available only for datetime type columns")

    @property
    def float(self):
        """
        Property that provides access to
        methods specific to float type.
        """
        if issubclass(self.dtype, float):
            from onetick.py.core.column_operations.accessors.float_accessor import _FloatAccessor
            return _FloatAccessor(self)
        else:
            raise TypeError(".float accessor is available only for float type columns")

    @property
    def decimal(self):
        """
        Property that provides access to methods specific to decimal type.

        See also
        --------
        :py:class:`otp.decimal <onetick.py.types.decimal>`
        """
        if self.dtype is ott.decimal:
            from onetick.py.core.column_operations.accessors.decimal_accessor import _DecimalAccessor
            return _DecimalAccessor(self)
        else:
            raise TypeError(".decimal accessor is available only for decimal type columns")

    def __abs__(self):
        """
        Return the absolute value of float or int column.

        Examples
        --------
        >>> t = otp.Tick(A=-1, B=-2.3)
        >>> t['A'] = abs(t['A'])
        >>> t['B'] = abs(t['B'])
        >>> otp.run(t)[['A', 'B']]
           A    B
        0  1  2.3
        """
        return _Operation(_methods.abs, [self])

    def __round__(self, precision=0):
        """
        Rounds value with specified ``precision``.

        Rounding :class:`otp.nan <onetick.py.nan>` returns NaN
        and rounding :class:`otp.inf <onetick.py.inf>` returns Infinity.

        For values that are exactly half-way between two integers (when the fraction part of value is exactly 0.5),
        the rounding method used here is *upwards*, which returns the bigger number.
        For other rounding methods see :func:`otp.math.round <onetick.py.math.round>` function.

        See also
        --------
        :func:`otp.math.round <onetick.py.math.round>`
        :func:`otp.math.floor <onetick.py.math.floor>`
        :func:`otp.math.ceil <onetick.py.math.ceil>`

        Parameters
        ----------
        precision: int
            Number from -12 to 12.
            Positive precision is precision after the floating point.
            Negative precision is precision before the floating point (and the fraction part is dropped in this case).

        Examples
        --------

        By default the ``precision`` is zero and the number is rounded to the closest integer number
        (and to the bigger number when the fraction part of value is exactly 0.5):

        >>> t = otp.Ticks(A=[-123.45, 123.45, 123.5])
        >>> t['ROUND'] = round(t['A'])
        >>> otp.run(t)
                             Time       A  ROUND
        0 2003-12-01 00:00:00.000 -123.45 -123.0
        1 2003-12-01 00:00:00.001  123.45  123.0
        2 2003-12-01 00:00:00.002  123.50  124.0

        Positive precision truncates to the number of digits *after* floating point:

        >>> t = otp.Ticks(A=[-123.45, 123.45])
        >>> t['ROUND1'] = round(t['A'], 1)
        >>> t['ROUND2'] = round(t['A'], 2)
        >>> otp.run(t)
                             Time       A  ROUND1  ROUND2
        0 2003-12-01 00:00:00.000 -123.45  -123.4 -123.45
        1 2003-12-01 00:00:00.001  123.45   123.5  123.45

        Negative precision truncates to the number of digits *before* floating point
        (and the fraction part is dropped in this case):

        >>> t = otp.Ticks(A=[-123.45, 123.45])
        >>> t['ROUND_M1'] = round(t['A'], -1)
        >>> t['ROUND_M2'] = round(t['A'], -2)
        >>> otp.run(t)
                             Time       A  ROUND_M1  ROUND_M2
        0 2003-12-01 00:00:00.000 -123.45    -120.0    -100.0
        1 2003-12-01 00:00:00.001  123.45     120.0     100.0

        Rounding :class:`otp.nan <onetick.py.nan>` returns NaN
        and rounding :class:`otp.inf <onetick.py.inf>` returns Infinity in all cases:

        >>> t = otp.Ticks(A=[otp.inf, -otp.inf, otp.nan])
        >>> t['ROUND_0'] = round(t['A'])
        >>> t['ROUND_P2'] = round(t['A'], 2)
        >>> t['ROUND_M2'] = round(t['A'], -2)
        >>> otp.run(t)
                             Time    A  ROUND_0  ROUND_P2  ROUND_M2
        0 2003-12-01 00:00:00.000  inf      inf       inf       inf
        1 2003-12-01 00:00:00.001 -inf     -inf      -inf      -inf
        2 2003-12-01 00:00:00.002  NaN      NaN       NaN       NaN

        Returns
        -------
        Operation
        """
        if precision is None:
            precision = 0
        return _Operation(_methods.round, [self, precision])

    def __neg__(self):
        """
        Return the negative value of float or int column.

        Examples
        --------
        >>> t = otp.Tick(A=1, B=2.3)
        >>> t['A'] = -t['A']
        >>> t['B'] = -t['B']
        >>> otp.run(t)[['A', 'B']]
           A    B
        0 -1 -2.3
        """
        return _Operation(_methods.neg, [self])

    def __add__(self, other):
        """
        Return the sum of column and ``other`` value.

        Parameters
        ----------
        other: int, float, str, :ref:`offset <datetime_offsets>`, :py:class:`onetick.py.Column`

        Examples
        --------
        >>> t = otp.Tick(A=1, B=2.3, C='c', D=otp.datetime(2022, 5, 12))
        >>> t['A'] = t['A'] + t['B']
        >>> t['B'] = t['B'] + 1
        >>> t['C'] = t['C'] + '_suffix'
        >>> t['D'] = t['D'] + otp.Day(1)
        >>> otp.run(t)[['A', 'B', 'C', 'D']]
             A    B         C          D
        0  3.3  3.3  c_suffix 2022-05-13
        """
        return _Operation(_methods.add, [self, other])

    def __radd__(self, other):
        """
        See also
        --------
        __add__

        Examples
        --------
        >>> t = otp.Tick(A=1, B=2.3, C='c', D=otp.datetime(2022, 5, 12))
        >>> t['A'] += t['B']
        >>> t['B'] += 1
        >>> t['C'] += '_suffix'
        >>> t['D'] += otp.Day(1)
        >>> otp.run(t)[['A', 'B', 'C', 'D']]
             A    B         C          D
        0  3.3  3.3  c_suffix 2022-05-13
        """
        return _Operation(_methods.add, [other, self])

    def __sub__(self, other):
        """
        Subtract ``other`` value from column.

        Parameters
        ----------
        other: int, float, :ref:`offset <datetime_offsets>`, :py:class:`onetick.py.Column`

        Examples
        --------
        >>> t = otp.Tick(A=1, B=2.3, D=otp.datetime(2022, 5, 12))
        >>> t['A'] = t['A'] - t['B']
        >>> t['B'] = t['B'] - 1
        >>> t['D'] = t['D'] - otp.Day(1)
        >>> otp.run(t)[['A', 'B', 'D']]
             A    B          D
        0 -1.3  1.3 2022-05-11
        """
        return _Operation(_methods.sub, [self, other])

    def __rsub__(self, other):
        """
        See also
        --------
        __sub__

        Examples
        --------
        >>> t = otp.Tick(A=1, B=2.3, D=otp.datetime(2022, 5, 12))
        >>> t['A'] -= t['B']
        >>> t['B'] -= 1
        >>> t['D'] -= otp.Day(1)
        >>> otp.run(t)[['A', 'B', 'D']]
             A    B          D
        0 -1.3  1.3 2022-05-11
        """
        return _Operation(_methods.sub, [other, self])

    def __mul__(self, other):
        """
        Multiply column by ``other`` value.

        Parameters
        ----------
        other: int, float, str, :py:class:`onetick.py.Column`

        Examples
        --------
        >>> t = otp.Tick(A=1, B=2.3, C='c')
        >>> t['A'] = t['A'] * t['B']
        >>> t['B'] = t['B'] * 2
        >>> t['C'] = t['C'] * 3
        >>> otp.run(t)[['A', 'B', 'C']]
             A    B    C
        0  2.3  4.6  ccc
        """
        return _Operation(_methods.mul, [self, other])

    def __rmul__(self, other):
        """
        See also
        --------
        __mul__

        Examples
        --------
        >>> t = otp.Tick(A=1, B=2.3, C='c')
        >>> t['A'] *= t['B']
        >>> t['B'] *= 2
        >>> t['C'] *= 3
        >>> otp.run(t)[['A', 'B', 'C']]
             A    B    C
        0  2.3  4.6  ccc
        """
        return _Operation(_methods.mul, [other, self])

    def __truediv__(self, other):
        """
        Divide column by ``other`` value.

        Parameters
        ----------
        other: int, float, :py:class:`onetick.py.Column`

        Examples
        --------
        >>> t = otp.Tick(A=1, B=2.3)
        >>> t['A'] = t['A'] / t['B']
        >>> t['B'] = t['B'] / 2
        >>> otp.run(t)[['A', 'B']]
                  A     B
        0  0.434783  1.15
        """
        return _Operation(_methods.div, [self, other])

    def __rtruediv__(self, other):
        """
        See also
        --------
        __truediv__

        Examples
        --------
        >>> t = otp.Tick(A=1, B=2.3)
        >>> t['A'] /= t['B']
        >>> t['B'] /= 2
        >>> otp.run(t)[['A', 'B']]
                  A     B
        0  0.434783  1.15
        """
        return _Operation(_methods.div, [other, self])

    def __mod__(self, other):
        """
        Return modulo of division of int column by ``other`` value.

        Parameters
        ----------
        other: int, :py:class:`onetick.py.Column`

        Examples
        --------
        >>> t = otp.Tick(A=3, B=3)
        >>> t['A'] = t['A'] % t['B']
        >>> t['B'] = t['B'] % 2
        >>> otp.run(t)[['A', 'B']]
           A  B
        0  0  1
        """
        return _Operation(_methods.mod, [self, other])

    def __invert__(self):
        """
        Return inversion of filter operation.

        Examples
        --------
        >>> t = otp.Ticks(A=range(4))
        >>> t = t.where(~(t['A'] > 1))
        >>> otp.run(t)[['A']]
           A
        0  0
        1  1
        """
        result = _Operation(_methods.invert, [self])
        return result

    def __eq__(self, other):
        """
        Return equality in filter operation.

        Examples
        --------
        >>> t = otp.Ticks(A=range(4))
        >>> t = t.where((t['A'] == 1))
        >>> otp.run(t)[['A']]
           A
        0  1
        """
        result = _Operation(_methods.eq, [self, other])
        return result

    def __ne__(self, other):
        """
        Return inequality in filter operation.

        Examples
        --------
        >>> t = otp.Ticks(A=range(4))
        >>> t = t.where((t['A'] != 1))
        >>> otp.run(t)[['A']]
           A
        0  0
        1  2
        2  3
        """
        result = _Operation(_methods.ne, [self, other])
        return result

    def __or__(self, other):
        """
        Return logical ``or`` in filter operation.

        Examples
        --------
        >>> t = otp.Ticks(A=range(4))
        >>> t = t.where((t['A'] == 1) | (t['A'] == 2))
        >>> otp.run(t)[['A']]
           A
        0  1
        1  2
        """
        result = _Operation(_methods.or_, [self, other])
        return result

    def __and__(self, other):
        """
        Return logical ``and`` in filter operation.

        Examples
        --------
        >>> t = otp.Ticks(A=[1, 1], B=[1, 2])
        >>> t = t.where((t['A'] == 1) & (t['B'] == 1))
        >>> otp.run(t)[['A', 'B']]
           A  B
        0  1  1
        """
        result = _Operation(_methods.and_, [self, other])
        return result

    def __le__(self, other):
        """
        Return <= in filter operation.

        Examples
        --------
        >>> t = otp.Ticks(A=range(4))
        >>> t = t.where(t['A'] <= 2)
        >>> otp.run(t)[['A']]
           A
        0  0
        1  1
        2  2
        """
        result = _Operation(_methods.le, [self, other])
        return result

    def __lt__(self, other):
        """
        Return < in filter operation.

        Examples
        --------
        >>> t = otp.Ticks(A=range(4))
        >>> t = t.where(t['A'] < 2)
        >>> otp.run(t)[['A']]
           A
        0  0
        1  1
        """
        result = _Operation(_methods.lt, [self, other])
        return result

    def __ge__(self, other):
        """
        Return >= in filter operation.

        Examples
        --------
        >>> t = otp.Ticks(A=range(4))
        >>> t = t.where(t['A'] >= 2)
        >>> otp.run(t)[['A']]
           A
        0  2
        1  3
        """
        result = _Operation(_methods.ge, [self, other])
        return result

    def __gt__(self, other):
        """
        Return > in filter operation.

        Examples
        --------
        >>> t = otp.Ticks(A=range(4))
        >>> t = t.where(t['A'] > 2)
        >>> otp.run(t)[['A']]
           A
        0  3
        """
        result = _Operation(_methods.gt, [self, other])
        return result

    def __format__(self, format_spec):
        # pylint: disable=E0311
        class_name = self.__class__.__name__
        warnings.warn(
            f"Using `{class_name}` in formatted string literals and `str.format` method is disallowed. "
            f"You can use `otp.format` function for formatting `{class_name}` objects.",
            FutureWarning,
            stacklevel=2,
        )

        return super().__format__(format_spec)

    def _evaluate_func(self, *, set_fields=False):
        if self._op_func:
            op_str, dtype = self._op_func(*self._op_params) if self._op_params else self._op_func()
            if set_fields:
                self._op_str = op_str
                self._dtype = dtype
            return op_str, dtype
        return None

    def _convert_to(self, to_type):
        return _Operation(_methods.CONVERSIONS[self.dtype, to_type], [self])

    def _make_python_way_bool_expression(self):
        dtype = ott.get_object_type(self)
        if dtype is bool:
            return self
        if are_ints_not_time(dtype):
            return _Operation(_methods.ne, (self, 0))
        elif are_time(dtype):
            return _Operation(_methods.ne, (self._convert_to(int), 0))
        elif are_floats(dtype):
            return _Operation(_methods.ne, (self, 0.0))
        elif are_strings(dtype):
            return _Operation(_methods.ne, (self, ""))
        else:
            raise TypeError("Filter expression should return bool, int, float or string")

    def _replace_parameters(self, operation_cb, return_replace_tuples=False):
        """
        Replaces operation parameters (on any nesting level)
        by logic defined by ``operation_cb`` and returns new operation object.

        Parameters
        ----------
        operation_cb: callable
            Callable, with one parameter: current parameter.
            If function returns anything current parameter will be replaced with the result of call
        return_replace_tuples:
            If ``True`` then also returns the list of tuples with old and new parameters.

        Returns
        -------
        result: _Operation or Tuple[_Operation, List[Tuple[_Operation, _Operation]]]
            Returns new operation object with parameters replaced by callback.
            Also may return the list of tuples with old and new parameters.
        """
        self_copy = copy.copy(self)

        replace_tuples = []

        class Node:
            def __init__(self, operation, parent):
                self.operation = operation
                self.parent = parent

        nodes_to_reevaluate = []
        nodes_to_process = [Node(self_copy, None)]

        while nodes_to_process:
            current_node = nodes_to_process.pop()
            current_obj = current_node.operation
            if not getattr(current_obj, '_op_params', None):
                continue

            op_params = current_obj._op_params.copy()

            for i, op in enumerate(current_obj._op_params):
                new_op = operation_cb(op)
                if new_op is not None:
                    replace_tuples.append((op, new_op))
                    op_params[i] = new_op
                    nodes_to_reevaluate.append(Node(new_op, current_node))
                else:
                    if isinstance(op, _Operation):
                        op = copy.copy(op)
                        op_params[i] = op
                        nodes_to_process.append(Node(op, current_node))

            current_obj._op_params = op_params

        for node in reversed(nodes_to_reevaluate):
            parent = node.parent
            while parent is not None:
                parent.operation._evaluate_func(set_fields=True)
                parent = parent.parent

        if return_replace_tuples:
            return self_copy, replace_tuples
        return self_copy


_Operation = Operation  # alias to support backward compatibility


class Raw(Operation):
    """
    Data type representing raw OneTick expression.

    Examples
    --------
    >>> t = otp.Tick(A=1)
    >>> t['A'] = '_TIMEZONE'
    >>> t['B'] = otp.raw('_TIMEZONE', dtype=otp.string[64])
    >>> otp.run(t, timezone='Asia/Yerevan')
            Time          A             B
    0 2003-12-01  _TIMEZONE  Asia/Yerevan
    """
    def __init__(self, raw, dtype):
        if dtype is str:
            warnings.warn(
                f'Be careful, default string length in OneTick is {ott.string.DEFAULT_LENGTH}. '
                "Length of the result raw expression can't be calculated automatically, "
                "so you'd better use onetick.py.string type.",
                stacklevel=2,
            )
        super().__init__(op_str=raw, dtype=dtype)


class OnetickParameter(Operation):
    """
    Data type representing OneTick parameter.

    This object can be used in all places where :class:`~onetick.py.Operation` can be used
    and also in some additional places, e.g. some functions' parameters.

    Parameters
    ----------
    name: str
        The name of the parameter.
    dtype:
        The type of the parameter.
    string_literal: bool
        By default, OneTick inserts string parameters as is in the graph,
        and they are interpreted as an expression.

        In case this parameter is ``True``
        quotes are added when evaluating string parameters, making them the string literals.
        We assume that in most cases it's desirable for the user's parameters to be used as a string literal,
        so the default value for this is ``True``.

        If you want your string parameters to be interpreted as OneTick expressions, set this to ``False``.

    Examples
    --------

    Generate tick with parametrized field value:

    >>> t = otp.Tick(A=otp.param('PARAM', dtype=int))
    >>> otp.run(t, query_params={'PARAM': 1})
            Time  A
    0 2003-12-01  1

    Set bucket interval in aggregation as a parameter:

    >>> t = otp.Ticks(A=[1, 2, 3, 4, 5, 6])
    >>> t = t.agg({'A': otp.agg.sum('A')},
    ...           bucket_units='ticks', bucket_interval=otp.param('PARAM', dtype=int))
    >>> otp.run(t, query_params={'PARAM': 3})
                         Time   A
    0 2003-12-01 00:00:00.002   6
    1 2003-12-01 00:00:00.005  15

    Parameter ``string_literal=False`` can be used if you need string to be interpreted as an expression:

    >>> t = otp.Tick(A=otp.param('PARAM', dtype=str),
    ...              B=otp.param('PARAM', dtype=str, string_literal=False))
    >>> otp.run(t, query_params={'PARAM': 'TOSTRING(NAN())'})
            Time                A    B
    0 2003-12-01  TOSTRING(NAN())  nan
    """
    def __init__(self, name, dtype, string_literal=True):
        self.parameter_expression = f'${name}'

        parameter_expression = self.parameter_expression
        if string_literal and issubclass(dtype, str):
            # OneTick parameters are inserted as is in the graph,
            # so we need to add quotes around string to make it a literal
            # (otherwise it would be interpreted as a column name)
            parameter_expression = ott.value2str(parameter_expression)

        if dtype is float:
            # BDS-93
            # need atof to be able to convert string 'nan' to float
            parameter_expression = f'atof({ott.value2str(parameter_expression)})'

        super().__init__(op_str=parameter_expression, dtype=dtype)
