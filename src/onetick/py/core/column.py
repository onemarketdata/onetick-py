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
        (there is no tick before first tick)

        Parameters
        ----------
        item: int
            number of ticks to look back/forward

        Returns
        -------
        Operation

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
