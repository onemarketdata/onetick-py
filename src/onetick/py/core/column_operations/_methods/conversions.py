from collections import UserDict
from functools import partial

from onetick.py import types as ott
from onetick.py.core.column_operations._methods._internal import MethodResult
from onetick.py.core.column_operations._methods.op_types import are_ints_not_time

NSECTIME_TO_STR_FORMAT = "%Y-%m-%d %H:%M:%S.%J"
NSECTIME_TO_STR_FORMAT_2 = "%Y/%m/%d %H:%M:%S.%J"
MSECTIME_TO_STR_FORMAT = "%Y-%m-%d %H:%M:%S.%q"


def float_to_int(prev_op, dtype=int):
    # CASE should be uppercased because it can be used in per-tick script
    op_str = f"CASE({str(prev_op)} > 0, 1, floor({str(prev_op)}), ceil({str(prev_op)}))"
    return MethodResult(op_str, dtype)


def float_to_str(prev_op, dtype=str):
    precision = getattr(prev_op, "_precision", 7)

    op_str = f"tostring({str(prev_op)}, 10, {precision})"

    if precision == 0:
        # otherwise 1.7 would be 1, but should be 1.0, because it is still float TODO: forbid it?
        op_str = f"({op_str} + '.0')"
    # if

    # we need it to remove trailing zeros
    op_str = fr'regex_replace({op_str}, "(\.\d+?)0+\b", "\1")'
    return MethodResult(op_str, dtype)


def num_to_decimal(prev_op):
    return MethodResult(f'decimal({str(prev_op)})', ott.decimal)


def decimal_to_str(prev_op, dtype=str):
    precision = getattr(prev_op, "_precision", 8)
    op_str = f"decimal_to_string({str(prev_op)}, {precision})"
    return MethodResult(op_str, dtype)


def decimal_to_float(prev_op):
    return MethodResult(f'{str(prev_op)}', float)


def nsectime_to_str(prev_op, dtype=str):
    op_str = f"nsectime_format('{NSECTIME_TO_STR_FORMAT}', {str(prev_op)}, _TIMEZONE)"
    return MethodResult(op_str, dtype)


def msectime_to_str(prev_op, dtype=str):
    op_str = f"time_format('{MSECTIME_TO_STR_FORMAT}', {str(prev_op)}, _TIMEZONE)"
    return MethodResult(op_str, dtype)


def nsectime_to_int(prev_op, dtype=int):
    op_str = f"nsectime_to_long({str(prev_op)})"
    return MethodResult(op_str, dtype)


def msectime_to_int(prev_op, dtype=int):
    op_str = str(prev_op)
    return MethodResult(op_str, dtype)


def nsectime_to_msectime(prev_op):
    op_str = f"nsectime({str(prev_op)})"
    return MethodResult(op_str, ott.msectime)


def msectime_to_nsectime(prev_op):
    op_str = f"nsectime({str(prev_op)})"
    return MethodResult(op_str, ott.nsectime)


def int_to_str(prev_op, dtype=str):
    op_str = f"tostring({str(prev_op)})"
    return MethodResult(op_str, dtype)


def int_to_float(prev_op):
    op_str = f"{str(prev_op)} * 1.0"
    return MethodResult(op_str, float)


def int_to_nsectime(prev_op):
    op_str = f"nsectime({str(prev_op)})"
    return MethodResult(op_str, ott.nsectime)


def int_to_msectime(prev_op):
    op_str = str(prev_op)
    return MethodResult(op_str, ott.msectime)


def str_to_float(prev_op):
    op_str = f"atof({str(prev_op)})"
    return MethodResult(op_str, float)


def str_to_decimal(prev_op):
    op_str = f"string_to_decimal({str(prev_op)})"
    return MethodResult(op_str, ott.decimal)


def str_to_int(prev_op, dtype=int):
    op_str = f"atol({str(prev_op)})"
    return MethodResult(op_str, dtype)


def str_to_nsectime(prev_op):
    op_str = _str_to_time(prev_op)
    return MethodResult(op_str, ott.nsectime)


def str_to_msectime(prev_op):
    op_str = _str_to_time(prev_op)
    return MethodResult(op_str, ott.msectime)


def _str_to_time(prev_op):
    result = str(prev_op)

    first_option = f"parse_nsectime('{NSECTIME_TO_STR_FORMAT}', {result}, _TIMEZONE)"
    second_option = f"parse_nsectime('{NSECTIME_TO_STR_FORMAT_2}', {result}, _TIMEZONE)"

    # CASE should be uppercased because it can be used in per-tick script
    return f"CASE(position('-', {result}) > 0, 1, {first_option}, {second_option})"


def bool_to_int(prev_op, dtype=int):
    op_str = f"CASE({str(prev_op)}, true, 1, 0)"
    return MethodResult(op_str, dtype)


def bool_to_float(prev_op):
    op_str = f"CASE({str(prev_op)}, true, 1.0, 0.0)"
    return MethodResult(op_str, float)


def _same_to_same(prev_op, dtype=None):
    if dtype is None:
        dtype = prev_op.dtype
    return MethodResult(str(prev_op), dtype)


class _ConversionsDict(UserDict):
    def __init__(self, conversions):
        self.data = conversions

    def __getitem__(self, key):
        if not (isinstance(key, tuple) and len(key) == 2):
            raise ValueError("wrong usage of _ConversionsDict")

        from_, to = key
        if from_ == to:
            return _same_to_same

        try:
            return super().__getitem__(key)
        except KeyError:
            pass

        new_from, new_to = None, None

        # int subclasses are converted by the same method
        if are_ints_not_time(from_):
            new_from = int
        if are_ints_not_time(to):
            new_to = int

        # otp.string and str are converted by the same methods
        if from_ is not str and issubclass(from_, str):
            new_from = str
        if to is not str and issubclass(to, str):
            new_to = str

        if new_from is not None or new_to is not None:
            new_key = (new_from or from_, new_to or to)
            if new_key[0] == new_key[1]:
                method = _same_to_same
            else:
                method = super().__getitem__(new_key)
            if issubclass(to, str) or are_ints_not_time(to):
                method = partial(method, dtype=to)
            return method

        raise TypeError(f"can not convert {from_} to {to}")


CONVERSIONS = _ConversionsDict({(float, int): float_to_int,
                                (float, str): float_to_str,
                                (float, ott.decimal): num_to_decimal,
                                (ott.decimal, int): float_to_int,
                                (ott.decimal, str): decimal_to_str,
                                (ott.decimal, float): decimal_to_float,
                                (ott.nsectime, str): nsectime_to_str,
                                (ott.msectime, str): msectime_to_str,
                                (ott.nsectime, int): nsectime_to_int,
                                (ott.msectime, int): msectime_to_int,
                                (ott.nsectime, ott.msectime): nsectime_to_msectime,
                                (ott.msectime, ott.nsectime): msectime_to_nsectime,
                                (int, str): int_to_str,
                                (int, float): int_to_float,
                                (int, ott.nsectime): int_to_nsectime,
                                (int, ott.msectime): int_to_msectime,
                                (int, ott.decimal): num_to_decimal,
                                (str, float): str_to_float,
                                (str, ott.decimal): str_to_decimal,
                                (str, int): str_to_int,
                                (str, ott.nsectime): str_to_nsectime,
                                (str, ott.msectime): str_to_msectime,
                                (bool, int): bool_to_int,
                                (bool, float): bool_to_float})
