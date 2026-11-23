import inspect
import string

from .column_operations.base import _Operation
from .. import types as ott


_allowed_field_name_chars = set(string.ascii_letters) | set(string.digits) | {'_', '.'}


def validate_onetick_field_name(field_name):
    if len(field_name) < 1 or len(field_name) > 127:
        return False
    chars = set(char for char in field_name)

    return chars.issubset(_allowed_field_name_chars)


def field_name_contains_lowercase(field_name):
    for char in field_name:
        if char.islower():
            return True
    return False


class Column(_Operation):
    """
    :py:class:`~onetick.py.Source` column container.

    This is the object you get when using :py:meth:`~onetick.py.Source.__getitem__`.
    You can use this object everywhere where :py:class:`~onetick.py.Operation` object can be used.

    Examples
    --------
    >>> t = otp.Tick(A=1)
    >>> t['A']
    Column(A, <class 'int'>)
    """

    @staticmethod
    def _check_name(name):
        if not validate_onetick_field_name(name):
            raise ValueError(f"Field name '{name}' is not a valid field name. "
                             "Onetick field names can contain upper and lower English letters, digits, "
                             "symbols '.' and '_'. Also, Onetick field names cannot be longer than 127 characters.")

    def __init__(self, name, dtype=float, obj_ref=None, precision=None):
        if not dtype or not inspect.isclass(dtype) or not ott.is_type_supported(dtype):
            raise TypeError(f'Column does not support "{dtype}" type')

        # validating column name
        if name != 'Time':  # this is a special value
            self._check_name(name)

        self.name = name
        super().__init__(dtype=dtype, obj_ref=obj_ref, op_str=name)

        # optional properties
        if precision is not None:
            if issubclass(dtype, float):
                self._precision = precision
            else:
                raise ValueError("precision is supported only for columns with float or decimal dtypes")

    def rename(self, new_name, update_parent_object=True):
        self._check_name(new_name)
        if self.obj_ref and update_parent_object:
            self.obj_ref.rename({self.name: new_name}, inplace=True)

        self.name = new_name

    def __len__(self):
        if issubclass(self.dtype, str):
            if issubclass(self.dtype, ott.string):
                return self.dtype.length
            else:
                return ott.string.DEFAULT_LENGTH
        else:
            raise TypeError(f'It is not applicable for the column with type {self.dtype}')  # TODO: test

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"Column({str(self)}, {self.dtype})"

    def copy(self, obj_ref=None):
        return _Column(self.name, self.dtype, obj_ref)

    def __bool__(self):
        if _Column.emulation_enabled:
            if issubclass(self.dtype, int):
                return (self != 0).__bool__()
            if issubclass(self.dtype, float):
                return (self != 0).__bool__()
            if issubclass(self.dtype, str):
                return (self != "").__bool__()

        raise TypeError("It is not allowed to use columns in if-else and while clauses")

    def __getitem__(self, item):
        """
        Provides an ability to get values from future or past ticks.

        - Negative values refer to past ticks

        - Zero to current tick

        - Positive - future ticks

        Boundary values will be defaulted. For instance for ``item=-1`` first tick value will be defaulted
        (there is no tick before first tick).

        Parameters
        ----------
        item: int
            number of ticks to look back/forward

        Returns
        -------
        Operation

        See Also
        --------
        | :meth:`tick_shift`

        Examples
        --------
        >>> data = otp.Ticks({'A': [1, 2, 3]})
        >>> data['PAST1'] = data['A'][-1]
        >>> data['PAST2'] = data['A'][-2]
        >>> data['FUTURE1'] = data['A'][1]
        >>> data['FUTURE2'] = data['A'][2]
        >>> otp.run(data)
                             Time  A  PAST1  PAST2  FUTURE1  FUTURE2
        0 2003-12-01 00:00:00.000  1      0      0        2        3
        1 2003-12-01 00:00:00.001  2      1      0        3        0
        2 2003-12-01 00:00:00.002  3      2      1        0        0
        """

        if not isinstance(item, int):
            raise TypeError(
                f"Lag operation supports only integer const values, but passed value of type '{type(item)}'"
            )
        if item == 0:
            return self

        return _LagOperator(self, item)

    def tick_shift(
        self,
        offset: int,
        /,
        *,
        group_by: str | list[str] | None = None,
    ):
        """
        Returns the value of this column from a tick preceding the current one by ``offset``.

        Boundary values will be set to the default value by type: 0 for integers, empty string for strings, etc.

        Parameters
        ----------
        offset: int
            Non negative offset looking back from the current tick.
        group_by: str | list[str]
            Specifies a field name or a list of field names,
            in which case ``offset`` is counted among the ticks with same values of the specified fields.

        Returns
        -------
        :class:`~onetick.py.Operation`

        See Also
        --------
        | :meth:`__getitem__`
        | **TICK_SHIFT** OneTick function

        Examples
        --------
        >>> data = otp.Ticks(A=[1, 2, 3, 4, 5, 6, 7, 8, 9], B=[1, 1, 1, 2, 2, 3, 3, 2, 3])
        >>> data['A1'] = data['A'].tick_shift(1)
        >>> data['A1_GROUP_BY_B'] = data['A'].tick_shift(1, group_by='B')
        >>> otp.run(data)
                             Time  A  B  A1  A1_GROUP_BY_B
        0 2003-12-01 00:00:00.000  1  1   0              0
        1 2003-12-01 00:00:00.001  2  1   1              1
        2 2003-12-01 00:00:00.002  3  1   2              2
        3 2003-12-01 00:00:00.003  4  2   3              0
        4 2003-12-01 00:00:00.004  5  2   4              4
        5 2003-12-01 00:00:00.005  6  3   5              0
        6 2003-12-01 00:00:00.006  7  3   6              6
        7 2003-12-01 00:00:00.007  8  2   7              5
        8 2003-12-01 00:00:00.008  9  3   8              7
        """
        if offset < 0:
            raise ValueError("Parameter 'offset' must be non-negative")

        def fun(offset, group_by):
            params_str = f'{ott.value2str(str(self))},{ott.value2str(offset)}'
            if group_by is not None:
                if not isinstance(group_by, list):
                    group_by = [group_by]
                group_by_str = ','.join(str(f) for f in group_by)
                if group_by_str:
                    params_str += f',{ott.value2str(group_by_str)}'

            return f'TICK_SHIFT({params_str})', self.dtype

        return _Operation(
            op_func=fun,
            op_params=[offset, group_by],
        )

    def time_shift(
        self,
        shift_time: int | float,
        /,
        *,
        selection_criteria: str | None = None,
        timestamp_field: str = 'TIMESTAMP',
        group_by: str | list[str] | None = None,
    ):
        """
        Returns the value of the specified field, from a tick,
        whose timestamp differs from current tick's timestamp by ``shift_time``.

        If ``shift_time`` is negative, it returns the value for the preceding tick,
        and if ``shift_time`` is positive, succeeding ticks (those that have not arrived yet) are considered.

        For shifts crossing the boundaries of query time range default values by type will be returned:
        0 for numbers, empty strings for strings etc.

        Parameters
        ----------
        shift_time: int or float
            Integer values are treated as milliseconds
            and float values are treated as ``milliseconds[.nanoseconds]`` format.

            .. note::

                Positive shift times are allowed only when adding fields and in per-tick script.

        selection_criteria: str
            Possible values for the criteria parameter are:

            * *last_before*
            * *last_at_or_before* (default for negative shift times)
            * *first_after*
            * *first_at_or_after* (default for positive shift times)

        timestamp_field: str
            Specifies an alternative timestamp field name
            (values in this field are assumed to be in non-decreasing order).

            By default the tick's TIMESTAMP field is used.

        group_by: str | list[str]
            Specifies a field name or a list of field names,
            in which case selection is performed among the ticks with same values of the specified fields.

            .. note::

                This parameter can be set only with negative time shifts.

        Returns
        -------
        :class:`~onetick.py.Operation`

        See Also
        --------
        | :meth:`tick_shift`
        | **TIME_SHIFT** OneTick function

        Examples
        --------
        >>> data = otp.Ticks(A=[1, 2, 3, 4, 5, 6, 7, 8, 9], B=[1, 1, 1, 2, 2, 3, 3, 2, 3])
        >>> data['A1_AT'] = data['A'].time_shift(1, selection_criteria='first_at_or_after')
        >>> data['A1_AFTER'] = data['A'].time_shift(1, selection_criteria='first_after')
        >>> data['AM1_AT_GROUP_BY_B'] = data['A'].time_shift(-1, group_by='B', timestamp_field='TIMESTAMP')
        >>> otp.run(data)
                             Time  A  B  A1_AT  A1_AFTER  AM1_AT_GROUP_BY_B
        0 2003-12-01 00:00:00.000  1  1      2         3                  0
        1 2003-12-01 00:00:00.001  2  1      3         4                  1
        2 2003-12-01 00:00:00.002  3  1      4         5                  2
        3 2003-12-01 00:00:00.003  4  2      5         6                  0
        4 2003-12-01 00:00:00.004  5  2      6         7                  4
        5 2003-12-01 00:00:00.005  6  3      7         8                  0
        6 2003-12-01 00:00:00.006  7  3      8         9                  6
        7 2003-12-01 00:00:00.007  8  2      9         0                  5
        8 2003-12-01 00:00:00.008  9  3      0         0                  7
        """
        if selection_criteria:
            if selection_criteria not in ('last_before', 'last_at_or_before', 'first_after', 'first_at_or_after'):
                raise ValueError(f"Wrong value for parameter 'selection_criteria': {selection_criteria}")

        if timestamp_field is not None and self.obj_ref is not None and str(timestamp_field) not in self.obj_ref.schema:
            raise ValueError(f"Field set in parameter 'timestamp_field' is not in schema: {timestamp_field}")

        def fun(shift_time, selection_criteria, timestamp_field, group_by):
            params_str = f'{ott.value2str(str(self))},{ott.value2str(shift_time)}'
            if selection_criteria is None:
                selection_criteria = ''
            params_str += f',{ott.value2str(selection_criteria.upper())}'
            if not timestamp_field:
                timestamp_field = 'TIMESTAMP'
            params_str += f',{ott.value2str(str(timestamp_field))}'
            if group_by is not None:
                if not isinstance(group_by, list):
                    group_by = [group_by]
                group_by_str = ','.join(str(f) for f in group_by)
                if group_by_str:
                    params_str += f',{ott.value2str(group_by_str)}'

            return f'TIME_SHIFT({params_str})', self.dtype

        return _Operation(
            op_func=fun,
            op_params=[shift_time, selection_criteria, timestamp_field, group_by],
        )

    def cumsum(self):
        """
        Cumulative sum of the column.

        Can only be used when creating or updating column.

        Examples
        --------
        >>> t = otp.Ticks({'A': [1, 2, 3]})
        >>> t['X'] = t['A'].cumsum()
        >>> otp.run(t)
                             Time  A  X
        0 2003-12-01 00:00:00.000  1  1
        1 2003-12-01 00:00:00.001  2  3
        2 2003-12-01 00:00:00.002  3  6
        """
        import onetick.py as otp

        return _ColumnAggregation(
            otp.agg.sum(self.name, running=True, all_fields=True, overwrite_output_field=True)
        )

    def __iter__(self):
        raise TypeError("It is not allowed to use columns in for-clauses")


class _LagOperator(_Operation):
    """
    Implements referencing to the prior tick
    """

    def __init__(self, base_column, inx):
        self._inx = inx
        op_str = f"{str(base_column)}[{self.index}]"
        super().__init__(op_params=[base_column], dtype=base_column.dtype,
                         op_str=op_str, obj_ref=base_column.obj_ref)

    @property
    def index(self):
        return self._inx


class _ColumnAggregation:
    """
    Object to specify how column will be aggregated.
    """
    def __init__(self, aggregation):
        from ..aggregations._base import _Aggregation
        if not isinstance(aggregation, _Aggregation):
            raise ValueError(f'Expected aggregation object, got {type(aggregation)}')
        if not aggregation.running or not aggregation.all_fields or not aggregation.overwrite_output_field:
            raise ValueError("Column aggregations only support 'running' aggregations"
                             " with 'all_fields' and 'overwrite_output_field' parameters set")
        self.aggregation = aggregation

    def apply(self, src, name):
        return self.aggregation.apply(src, name=name, inplace=True)


_Column = Column  # alias for backward compatibility
