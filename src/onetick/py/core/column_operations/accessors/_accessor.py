import typing

from onetick.py import Operation
from onetick.py.core.column_operations.base import _Operation
from onetick.py.types import value2str


class _Accessor:

    def __init__(self, base_column):
        self._base_column = base_column

    class Formatter(_Operation):
        def __init__(self, dtype, formatter, op_params):
            def op_func(*args, **kwargs):
                return formatter(*args, **kwargs), dtype

            super().__init__(op_func=op_func, op_params=op_params, dtype=dtype)

    def _preprocess_tz_and_format(self,
                                  timezone: typing.Union[Operation, str, None],
                                  format_str):  # it is common for str and dt accessors
        if timezone is None or isinstance(timezone, str) and timezone == "_TIMEZONE":
            timezone = "_TIMEZONE"
        else:
            timezone = value2str(timezone)
        format_str = value2str(format_str)
        return timezone, format_str
