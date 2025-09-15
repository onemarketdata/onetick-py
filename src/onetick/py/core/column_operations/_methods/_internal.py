from collections import namedtuple

from onetick.py import types as ott
from onetick.py.core.column_operations._methods.op_types import are_strings
from onetick.py.types import value2str

MethodResult = namedtuple("MethodResult", ("op_str", "dtype"))


def _wrap_object(o):
    if isinstance(o, str) or are_strings(getattr(o, "dtype", None)):
        # In PER_TICK_SCRIPT: parenthesis are not allowed in string expressions.
        return value2str(o)
    return f"({value2str(o)})"


def _type_error_for_op(op, types):
    return TypeError(f"Unsupported operand type(s) for {op} operation: {types}")


def _init_binary_op(prev_op, other):
    left = _wrap_object(prev_op)
    right = _wrap_object(other)
    left_t = ott.get_object_type(prev_op)
    right_t = ott.get_object_type(other)
    dtype = None
    op_str = None
    return left, right, left_t, right_t, op_str, dtype
