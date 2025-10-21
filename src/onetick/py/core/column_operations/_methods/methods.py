import warnings

from onetick.py import types as ott
from onetick.py.types import value2str
from ._internal import MethodResult, _wrap_object, _type_error_for_op, _init_binary_op
from .op_types import (
    are_numerics, are_strings, are_bools, are_time, are_ints_not_time, _get_widest_type,
    are_floats, are_ints_or_time
)


class DatetimeSubtractionWarning(FutureWarning):
    pass


def round(prev_op, precision):
    return MethodResult(f'round_double({str(prev_op)},{str(precision)})', float)


def isin(prev_op, items):
    if not items:
        raise ValueError("Method isin() can't be used without values")
    op_str = f'IN({str(prev_op)}, {",".join(value2str(i) for i in items)})'
    return MethodResult(op_str, bool)


def _map(prev_op, items, values_type, default=None):
    default_str = '' if default is None else f', {value2str(default)}'
    op_str = (f'CASE({str(prev_op)}, {",".join(value2str(k) + "," + value2str(v) for k, v in items.items())}'
              f'{default_str})')
    return MethodResult(op_str, values_type)


def fillna(prev_op, value=0):
    result = str(prev_op)

    if not issubclass(prev_op.dtype, float):
        raise TypeError(f"It is allowed to apply .fillna() is only columns that have 'float' type, "
                        f"but it is applied on a column with the {prev_op.dtype.__name__} type")
    dtype = ott.get_object_type(value)
    if not are_numerics(dtype):
        raise TypeError(f"fillna expects numeric type, but got value of type '{type(value)}'")
    op_str = f'replace_nan({result},{value2str(value)})'
    return MethodResult(op_str, float)


def add(prev_op, other):
    left, right, left_t, right_t, _, _ = _init_binary_op(prev_op, other)
    result = _return_dateadd_command(prev_op, other, left, right, left_t, right_t, "")
    # if it is DATEADD command return it, else try to form + operation
    if result:
        return result
    return _plus(prev_op, other, left, right, left_t, right_t)


def sub(prev_op, other):
    left, right, left_t, right_t, _, _ = _init_binary_op(prev_op, other)
    result = _return_dateadd_command(prev_op, other, left, right, left_t, right_t, "-")
    # if it is DATEADD command return it, else try to form - operation
    if result:
        return result
    return _minus(prev_op, other, left, right, left_t, right_t)


def _return_dateadd_command(prev_op, other, left, right, left_t, right_t, op_sign):
    if issubclass(left_t, ott.OTPBaseTimeOffset) and are_time(right_t):
        return _form_method_result_for_dateadd(right, right_t, op_sign, prev_op)
    elif issubclass(right_t, ott.OTPBaseTimeOffset) and are_time(left_t):
        return _form_method_result_for_dateadd(left, left_t, op_sign, other)
    return None


def _form_method_result_for_dateadd(op_str, dtype, op_sign, datepart):
    op_str = f"DATEADD({datepart.datepart}, {op_sign}({str(datepart.n)}), {op_str}, _TIMEZONE)"
    return MethodResult(op_str, dtype)


def _plus(prev_op, other, left, right, left_t, right_t):
    def get_str_len(op_or_str_const, dtype):
        if not isinstance(op_or_str_const, str):
            if dtype is str:
                return ott.string.DEFAULT_LENGTH
            else:
                return dtype.length
        elif isinstance(op_or_str_const, ott.varstring):
            return Ellipsis
        else:
            return len(op_or_str_const)  # ott.string as well as builtin strings have len operand

    op_str = f"{left} + {right}"
    if are_strings(left_t, right_t):  # strings support concatenation
        left_length, right_length = get_str_len(prev_op, left_t), get_str_len(other, right_t)
        if left_length is Ellipsis or right_length is Ellipsis:
            dtype = ott.varstring
        else:
            length = max(left_length, right_length)
            dtype = str if length <= ott.string.DEFAULT_LENGTH else ott.string[length]
    else:
        dtype = _get_dtype_for_plus_or_minus(left_t, right_t)
    if dtype:
        return MethodResult(op_str, dtype)
    else:
        raise _type_error_for_op("+", f"'{left_t}' and '{right_t}'")


def _minus(prev_op, other, left, right, left_t, right_t):  # noqa # NOSONAR
    op_str = f"{left} - {right}"
    # Between datetime (msectime and nsectime) types, only the - operator is allowed.
    if are_time(left_t, right_t):
        warnings.warn("Subtracting datetimes without specifying resulted time unit is deprecated. "
                      "Specify resulted time unit explicitly, ex: otp.Hour(a - b) or otp.Nano(a - b). "
                      "Difference in milliseconds will be returned now, but in future it will throw error. ",
                      DatetimeSubtractionWarning)
        dtype = int
        # TODO uncomment if we will decide to use nanosecond as default time unit
        # op_str = f"DATEDIFF('nanosecond', {right}, {left}, _TIMEZONE)"
        # return MethodResult(op_str, dtype)
    else:
        dtype = _get_dtype_for_plus_or_minus(left_t, right_t)
    if dtype:
        return MethodResult(op_str, dtype)
    else:
        raise _type_error_for_op("-", f"'{left_t}' and '{right_t}'")


def _get_dtype_for_plus_or_minus(left_t, right_t):
    dtype = None
    if are_numerics(left_t, right_t):
        dtype = _get_widest_type(left_t, right_t)
    # It is possible to add an integral value to datetime or subtract an integer from it,
    # in this case the type is not changed
    elif (are_ints_not_time(left_t) and issubclass(right_t, ott.nsectime)
          or are_ints_not_time(right_t) and issubclass(left_t, ott.nsectime)):
        dtype = ott.nsectime
    elif (are_ints_not_time(left_t) and issubclass(right_t, ott.msectime)
          or are_ints_not_time(right_t) and issubclass(left_t, ott.msectime)):
        dtype = ott.msectime
    # Any operation between floating-point and datetime types are not allowed in tick script,
    # but supported in EP, so we also allow such operation, but generate warning.
    elif (are_floats(left_t) and issubclass(right_t, ott.nsectime)
          or are_floats(right_t) and issubclass(left_t, ott.nsectime)):
        dtype = ott.nsectime
        warnings.warn("Onetick will shrink the fractional part")
    elif (are_floats(left_t) and issubclass(right_t, ott.msectime)
          or are_floats(right_t) and issubclass(left_t, ott.msectime)):
        dtype = ott.msectime
        warnings.warn("Onetick will shrink the fractional part")
    return dtype


def mul(prev_op, other):
    left, right, left_t, right_t, op_str, dtype = _init_binary_op(prev_op, other)

    if are_numerics(left_t, right_t):
        dtype = _get_widest_type(left_t, right_t)
        op_str = f"{left} * {right}"
    elif issubclass(left_t, str) and are_ints_not_time(right_t):
        op_str = f"repeat({left}, {right})"
        dtype = left_t
    elif issubclass(right_t, str) and are_ints_not_time(left_t):
        op_str = f"repeat({right}, {left})"
        dtype = right_t
    if dtype and op_str:
        return MethodResult(op_str, dtype)
    else:
        raise _type_error_for_op("*", f"'{left_t}' and '{right_t}'")


def div(prev_op, other):
    left, right, left_t, right_t, op_str, dtype = _init_binary_op(prev_op, other)
    if not are_numerics(left_t, right_t):
        raise _type_error_for_op("/", f"'{left_t}' and '{right_t}'")
    dtype = _get_widest_type(_get_widest_type(left_t, right_t), float)
    op_str = f"{left} / {right}"
    return MethodResult(op_str, dtype)


def mod(prev_op, other):
    left, right, left_t, right_t, op_str, dtype = _init_binary_op(prev_op, other)
    if not are_ints_not_time(left_t, right_t):
        raise _type_error_for_op("mod", f"'{left_t}' and '{right_t}'")
    dtype = int
    op_str = f"mod({left}, {right})"
    return MethodResult(op_str, dtype)


def abs(prev_op):
    dtype = ott.get_object_type(prev_op)
    if not are_numerics(dtype):
        raise TypeError(f"Operation is not supported for type '{dtype}'")
    op_str = f"abs{_wrap_object(prev_op)}"
    return MethodResult(op_str, dtype)


def pos(prev_op):
    dtype = ott.get_object_type(prev_op)
    if not are_numerics(dtype):
        raise TypeError(f"Operation is not supported for type '{dtype}'")
    op_str = f"(+{_wrap_object(prev_op)})"
    return MethodResult(op_str, dtype)


def neg(prev_op):
    dtype = ott.get_object_type(prev_op)
    if not are_numerics(dtype):
        raise TypeError(f"Operation is not supported for type '{dtype}'")
    op_str = f"(-{_wrap_object(prev_op)})"
    return MethodResult(op_str, dtype)


def invert(prev_op):
    dtype = ott.get_object_type(prev_op)
    if not issubclass(dtype, bool):
        raise TypeError(f"Operation is not supported for type '{dtype}'")
    op_str = f"not({_wrap_object(prev_op)})"
    return MethodResult(op_str, dtype)


def eq(prev_op, other):
    return _compare(prev_op, other, "=", "==")


def ne(prev_op, other):
    return _compare(prev_op, other, "!=")


def lt(prev_op, other):
    return _compare(prev_op, other, "<")


def le(prev_op, other):
    return _compare(prev_op, other, "<=")


def gt(prev_op, other):
    return _compare(prev_op, other, ">")


def ge(prev_op, other):
    return _compare(prev_op, other, ">=")


def _compare(prev_op, other, op_sign, op_sign_python_level=None):
    def one_boolean_operation_is_allowed(prev_op, other, left_t, right_t):
        return (isinstance(prev_op, bool) and issubclass(left_t, bool) or
                isinstance(other, bool) and issubclass(right_t, bool))

    def the_same_type(left_t, right_t):
        # Operands of compare operator are expected to be both numeric, both booleans, both matrices or both strings
        return (
            not are_bools(left_t) and not are_bools(right_t)
            and (
                are_numerics(left_t, right_t) or are_strings(left_t, right_t)
                or are_ints_or_time(left_t, right_t)
            )
            or are_bools(left_t, right_t)
        )

    left, right, left_t, right_t, op_str, _ = _init_binary_op(prev_op, other)
    if one_boolean_operation_is_allowed(prev_op, other, left_t, right_t) or the_same_type(left_t, right_t):
        op_str = f"{left} {op_sign} {right}"
        return MethodResult(op_str, bool)
    else:
        # replace = with == for comparisions
        raise _type_error_for_op(f"{op_sign_python_level or op_sign}", f"'{left_t}' and '{right_t}'")


def and_(prev_op, other):
    return _and_or(prev_op, other, "AND")


def or_(prev_op, other):
    return _and_or(prev_op, other, "OR")


def _and_or(prev_op, other, op_sign):
    left, right, left_t, right_t, op_str, _ = _init_binary_op(prev_op, other)
    if are_bools(left_t, right_t):
        op_str = f"{left} {op_sign} {right}"
        return MethodResult(op_str, prev_op.dtype)
    else:
        raise _type_error_for_op(f"{op_sign}", f"'{left_t}' and '{right_t}'")


def is_arithmetical(op):
    op = getattr(op, "_op_func", None)
    return op in {neg, pos, abs, add, sub, mul, div, mod}


def is_compare(op):
    op = getattr(op, "_op_func", None)
    return op in {invert, or_, and_, eq, ne, lt, le, gt, ge}
