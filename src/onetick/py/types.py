import ctypes
import functools
import inspect
import warnings
import decimal as _decimal
from typing import Optional, Type, Union
from datetime import date as _date
from datetime import datetime as _datetime
from datetime import timedelta as _timedelta

import pandas as pd
import numpy as np
from pandas.tseries import offsets
from packaging.version import parse as parse_version

import onetick.py as otp
from onetick.py.otq import otq, pyomd
from onetick.py.compatibility import has_timezone_parameter
from onetick.py.core._internal._op_utils.every_operand import every_operand
from onetick.py.utils import get_tzfile_by_name, get_timezone_from_datetime
from onetick.py.docs.utils import is_windows

# --------------------------------------------------------------- #
# TYPES IMPLEMENTATION
# --------------------------------------------------------------- #


class OTPBaseTimeStamp(type):
    pass


class _nsectime(OTPBaseTimeStamp):
    def __str__(cls):
        return "nsectime"


class nsectime(int, metaclass=_nsectime):
    """
    OneTick data type representing datetime with nanoseconds precision.
    Can be used to specify otp.Source column type when converting columns or creating new ones.
    Note that this constructor creates datetime value in GMT timezone
    and doesn't take into account the timezone with which the query is executed.

    Examples
    --------
    >>> t = otp.Tick(A=0)
    >>> t['A'] = t['A'].apply(otp.nsectime)
    >>> t['B'] = otp.nsectime(24 * 60 * 60 * 1000 * 1000 * 1000 + 2)
    >>> t.schema
    {'A': <class 'onetick.py.types.nsectime'>, 'B': <class 'onetick.py.types.nsectime'>}
    >>> otp.run(t)
            Time                   A                             B
    0 2003-12-01 1969-12-31 19:00:00 1970-01-01 19:00:00.000000002
    """
    def __str__(self):
        return super().__repr__()

    def __repr__(self):
        return f'{self.__class__.__name__}({self})'


class _msectime(OTPBaseTimeStamp):
    def __str__(cls):
        return "msectime"


class msectime(int, metaclass=_msectime):
    """
    OneTick data type representing datetime with milliseconds precision.
    Can be used to specify otp.Source column type when converting columns or creating new ones.
    Note that this constructor creates datetime value in GMT timezone
    and doesn't take into account the timezone with which the query is executed.

    Examples
    --------
    >>> t = otp.Tick(A=1)
    >>> t = t.table(A=otp.msectime)
    >>> t['B'] = otp.msectime(2)
    >>> t.schema
    {'A': <class 'onetick.py.types.msectime'>, 'B': <class 'onetick.py.types.msectime'>}
    >>> otp.run(t)
            Time                       A                       B
    0 2003-12-01 1969-12-31 19:00:00.001 1969-12-31 19:00:00.002
    """
    def __str__(self):
        return super().__repr__()

    def __repr__(self):
        return f'{self.__class__.__name__}({self})'


class OTPBaseTimeOffset:
    datepart = "'invalid'"  # that is just base class for other dateparts
    n = 0
    delta = pd.Timedelta(seconds=0)

    def get_offset(self):
        return self.n, self.datepart[1:-1]


class ExpressionDefinedTimeOffset(OTPBaseTimeOffset):
    def __init__(self, datepart, n):
        self.datepart = datepart
        self.n = n

        from onetick.py.core.column_operations.base import _Operation

        def proxy_wrap(attr):
            def f(self, *args, **kwargs):
                return getattr(self.n, attr)(*args, **kwargs)
            return f

        for attr, value in inspect.getmembers(_Operation, callable):
            if attr in {'__class__', '__init__', '__new__', '__init_subclass__', '__dir__',
                        '__getattribute__', '__getattr__', '__delattr__', '__setattr__',
                        '__subclasshook__', '__sizeof__', '__str__', '__repr__'}:
                continue
            setattr(ExpressionDefinedTimeOffset, attr, proxy_wrap(attr))


# ---------------------------- #
# Implement datepart units

def _construct_dpf(dp_class, str_repr=None, **dp_class_params):
    """ construct a datepart factory """

    if str_repr is None:
        str_repr = dp_class.__name__.lower()

    class _DatePartCls(dp_class, OTPBaseTimeOffset):
        datepart = f"'{str_repr}'"

    def _factory(n):
        from onetick.py.core.column_operations._methods.methods import is_arithmetical
        from onetick.py.core.column import _Column

        if isinstance(n, int):
            if dp_class_params:
                return _DatePartCls(**dp_class_params) * n
            return _DatePartCls(n)
        if is_arithmetical(n):
            n = _process_datediff(n)
            return ExpressionDefinedTimeOffset(_DatePartCls.datepart, n)
        if isinstance(n, _Column):
            return ExpressionDefinedTimeOffset(_DatePartCls.datepart, n)
        raise ValueError("Unknown type was passed as arg, integer constant or column or expression is expected here")

    def _process_datediff(n):

        n_time_operand = _get_n_time_operand(n)
        if n_time_operand:
            # check if otp.Hour(date1 - date2) is called, return a number of hours between two days in such ways
            from onetick.py.core.column_operations._methods.methods import sub, _wrap_object
            from onetick.py.core.column_operations.base import _Operation
            from onetick.py.core.column import _LagOperator

            available_types = (_Operation, _LagOperator)
            if (getattr(n, "_op_func", sub) and len(n._op_params) == 2
                    and isinstance(n._op_params[0], available_types) and isinstance(n._op_params[1], available_types)):
                def _datediff(*args):
                    args = ', '.join(map(_wrap_object, args))
                    return f'DATEDIFF({_DatePartCls.datepart}, {args}, _TIMEZONE)', int
                return _Operation(_datediff, [n._op_params[1], n._op_params[0]])
            else:
                raise ValueError(
                    "Date arithmetic operations (except date2-date1, which calculate an amount of "
                    "periods between two dates) are not accepted in TimeOffset constructor"
                )
        return n

    def _get_n_time_operand(n):
        from onetick.py.core.column_operations._methods.op_types import are_time

        result = 0
        for op in every_operand(n):
            if are_time(get_object_type(op)):
                result += 1
        return result

    return _factory


def _construct_float_dpf(dp_class, dp_float_class, power, str_repr=None, float_str_repr=None, **dp_class_params):
    def _factory(n):
        if isinstance(n, float):
            return _construct_dpf(dp_float_class, float_str_repr, **dp_class_params)(int(n * (10 ** power)))
        else:
            return _construct_dpf(dp_class, str_repr, **dp_class_params)(n)

    return _factory


_add_examples_to_docs = functools.partial(
    """
    Object representing {0}'s datetime offset.

    Can be added to or subtracted from:

    * :py:class:`otp.datetime <onetick.py.datetime>` objects
    * :py:class:`Source <onetick.py.Source>` columns of datetime type

    Parameters
    ----------
    n: int, :class:`~onetick.py.Column`, :class:`~onetick.py.Operation`{additional_types}
        Offset integer value or column of :class:`~onetick.py.Source`.
        The only :class:`~onetick.py.Operation` supported is
        subtracting one datetime column from another. See example below.{additional_notes}

    Examples
    --------
    {1}
    """.format, additional_types="", additional_notes="",
)

_float_dpf_types = ", float"
_float_dpf_notes = """
        Offset could be ``float`` to pass a fractional time unit value."""


Year = _construct_dpf(offsets.DateOffset, "year", years=1)
Year.__doc__ = _add_examples_to_docs('year', """
    Add to or subtract from :py:class:`otp.datetime <onetick.py.datetime>` object:

    >>> otp.datetime(2012, 12, 12, 12) + otp.Year(1)
    2013-12-12 12:00:00
    >>> otp.datetime(2012, 12, 12, 12) - otp.Year(1)
    2011-12-12 12:00:00

    Use offset in columns:

    >>> t = otp.Tick(A=1)
    >>> t['T'] = otp.datetime(2012, 12, 12, 12)
    >>> t['T'] += otp.Year(t['A'])
    >>> otp.run(t)
            Time                   T  A
    0 2003-12-01 2013-12-12 12:00:00  1

    Use it to calculate difference between two dates:

    >>> t = otp.Tick(A=otp.dt(2022, 1, 1), B=otp.dt(2023, 1, 1))
    >>> t['DIFF'] = otp.Year(t['B'] - t['A'])
    >>> otp.run(t)
            Time           A           B  DIFF
    0 2003-12-01  2022-01-01  2023-01-01     1
""")

Quarter = _construct_dpf(offsets.DateOffset, "quarter", months=3)
Quarter.__doc__ = _add_examples_to_docs('quarter', """
    Add to or subtract from :py:class:`otp.datetime <onetick.py.datetime>` object:

    >>> otp.datetime(2012, 12, 12, 12) + otp.Quarter(1)
    2013-03-12 12:00:00
    >>> otp.datetime(2012, 12, 12, 12) - otp.Quarter(1)
    2012-09-12 12:00:00

    Use offset in columns:

    >>> t = otp.Tick(A=1)
    >>> t['T'] = otp.datetime(2012, 12, 12, 12, tz='GMT')
    >>> t['T'] += otp.Quarter(t['A'])
    >>> otp.run(t, start=otp.datetime(2003, 12, 2), end=otp.datetime(2003, 12, 3), timezone='GMT')
            Time                   T  A
    0 2003-12-02 2013-03-12 12:00:00  1

    Use it to calculate difference between two dates:

    >>> t = otp.Tick(A=otp.dt(2022, 1, 1), B=otp.dt(2023, 1, 1))
    >>> t['DIFF'] = otp.Quarter(t['B'] - t['A'])
    >>> otp.run(t)
            Time           A           B  DIFF
    0 2003-12-01  2022-01-01  2023-01-01     4
""")

Month = _construct_dpf(offsets.DateOffset, "month", months=1)
Month.__doc__ = _add_examples_to_docs('month', """
    Add to or subtract from :py:class:`otp.datetime <onetick.py.datetime>` object:

    >>> otp.datetime(2012, 12, 12, 12) + otp.Month(1)
    2013-01-12 12:00:00
    >>> otp.datetime(2012, 12, 12, 12) - otp.Month(1)
    2012-11-12 12:00:00

    Use offset in columns:

    >>> t = otp.Tick(A=1)
    >>> t['T'] = otp.datetime(2012, 12, 12, 12)
    >>> t['T'] += otp.Month(t['A'])
    >>> otp.run(t)
            Time                   T  A
    0 2003-12-01 2013-01-12 12:00:00  1

    Use it to calculate difference between two dates:

    >>> t = otp.Tick(A=otp.dt(2022, 1, 1), B=otp.dt(2023, 1, 1))
    >>> t['DIFF'] = otp.Month(t['B'] - t['A'])
    >>> otp.run(t)
            Time           A           B  DIFF
    0 2003-12-01  2022-01-01  2023-01-01    12
""")

Week = _construct_dpf(offsets.Week)
Week.__doc__ = _add_examples_to_docs('week', """
    Add to or subtract from :py:class:`otp.datetime <onetick.py.datetime>` object:

    >>> otp.datetime(2012, 12, 12, 12) + otp.Week(1)
    2012-12-19 12:00:00
    >>> otp.datetime(2012, 12, 12, 12) - otp.Week(1)
    2012-12-05 12:00:00

    Use offset in columns:

    >>> t = otp.Tick(A=1)
    >>> t['T'] = otp.datetime(2012, 12, 12, 12)
    >>> t['T'] += otp.Week(t['A'])
    >>> otp.run(t)
            Time                   T  A
    0 2003-12-01 2012-12-19 12:00:00  1

    Use it to calculate difference between two dates:

    >>> t = otp.Tick(A=otp.dt(2022, 1, 1), B=otp.dt(2023, 1, 1))
    >>> t['DIFF'] = otp.Week(t['B'] - t['A'])
    >>> otp.run(t)
            Time           A           B  DIFF
    0 2003-12-01  2022-01-01  2023-01-01    53
""")

Day = _construct_dpf(offsets.Day)
Day.__doc__ = _add_examples_to_docs('day', """
    Add to or subtract from :py:class:`otp.datetime <onetick.py.datetime>` object:

    >>> otp.datetime(2012, 12, 12, 12) + otp.Day(1)
    2012-12-13 12:00:00
    >>> otp.datetime(2012, 12, 12, 12) - otp.Day(1)
    2012-12-11 12:00:00

    Use offset in columns:

    >>> t = otp.Tick(A=1)
    >>> t['T'] = otp.datetime(2012, 12, 12, 12)
    >>> t['T'] += otp.Day(t['A'])
    >>> otp.run(t)
            Time                   T  A
    0 2003-12-01 2012-12-13 12:00:00  1

    Use it to calculate difference between two dates:

    >>> t = otp.Tick(A=otp.dt(2022, 1, 1), B=otp.dt(2023, 1, 1))
    >>> t['DIFF'] = otp.Day(t['B'] - t['A'])
    >>> otp.run(t)
            Time           A           B  DIFF
    0 2003-12-01  2022-01-01  2023-01-01   365
""")

Hour = _construct_dpf(offsets.Hour)
Hour.__doc__ = _add_examples_to_docs('hour', """
    Add to or subtract from :py:class:`otp.datetime <onetick.py.datetime>` object:

    >>> otp.datetime(2012, 12, 12, 12) + otp.Hour(1)
    2012-12-12 13:00:00
    >>> otp.datetime(2012, 12, 12, 12) - otp.Hour(1)
    2012-12-12 11:00:00

    Use offset in columns:

    >>> t = otp.Tick(A=1)
    >>> t['T'] = otp.datetime(2012, 12, 12, 12)
    >>> t['T'] += otp.Hour(t['A'])
    >>> otp.run(t)
            Time                   T  A
    0 2003-12-01 2012-12-12 13:00:00  1

    Use it to calculate difference between two dates:

    >>> t = otp.Tick(A=otp.dt(2022, 1, 1), B=otp.dt(2022, 1, 2))
    >>> t['DIFF'] = otp.Hour(t['B'] - t['A'])
    >>> otp.run(t)
            Time           A           B  DIFF
    0 2003-12-01  2022-01-01  2022-01-02    24
""")

Minute = _construct_dpf(offsets.Minute)
Minute.__doc__ = _add_examples_to_docs('minute', """
    Add to or subtract from :py:class:`otp.datetime <onetick.py.datetime>` object:

    >>> otp.datetime(2012, 12, 12, 12) + otp.Minute(1)
    2012-12-12 12:01:00
    >>> otp.datetime(2012, 12, 12, 12) - otp.Minute(1)
    2012-12-12 11:59:00

    Use offset in columns:

    >>> t = otp.Tick(A=1)
    >>> t['T'] = otp.datetime(2012, 12, 12, 12)
    >>> t['T'] += otp.Minute(t['A'])
    >>> otp.run(t)
            Time                   T  A
    0 2003-12-01 2012-12-12 12:01:00  1

    Use it to calculate difference between two dates:

    >>> t = otp.Tick(A=otp.dt(2022, 1, 1), B=otp.dt(2022, 1, 1, 1))
    >>> t['DIFF'] = otp.Minute(t['B'] - t['A'])
    >>> otp.run(t)
            Time           A                    B  DIFF
    0 2003-12-01  2022-01-01  2022-01-01 01:00:00    60
""")

Second = _construct_float_dpf(offsets.Second, offsets.Nano, 9, None, "nanosecond")
Second.__doc__ = _add_examples_to_docs('second', """
    Add to or subtract from :py:class:`otp.datetime <onetick.py.datetime>` object:

    >>> otp.datetime(2012, 12, 12, 12) + otp.Second(1)
    2012-12-12 12:00:01
    >>> otp.datetime(2012, 12, 12, 12) - otp.Second(1)
    2012-12-12 11:59:59

    Using ``float`` value to pass nanoseconds:

    >>> otp.datetime(2012, 12, 12, 12) + otp.Second(1.000000123)
    2012-12-12 12:00:01.000000123

    Use offset in columns:

    >>> t = otp.Tick(A=1)
    >>> t['T'] = otp.datetime(2012, 12, 12, 12)
    >>> t['T'] += otp.Second(t['A'])
    >>> otp.run(t)
            Time                   T  A
    0 2003-12-01 2012-12-12 12:00:01  1

    Use it to calculate difference between two dates:

    >>> t = otp.Tick(A=otp.dt(2022, 1, 1), B=otp.dt(2022, 1, 1, 0, 1))
    >>> t['DIFF'] = otp.Second(t['B'] - t['A'])
    >>> otp.run(t)
            Time           A                    B  DIFF
    0 2003-12-01  2022-01-01  2022-01-01 00:01:00    60
""", additional_types=_float_dpf_types, additional_notes=_float_dpf_notes)

Milli = _construct_float_dpf(offsets.Milli, offsets.Nano, 6, "millisecond", "nanosecond")
Milli.__doc__ = _add_examples_to_docs('millisecond', """
    Add to or subtract from :py:class:`otp.datetime <onetick.py.datetime>` object:

    >>> otp.datetime(2012, 12, 12, 12) + otp.Milli(1)
    2012-12-12 12:00:00.001000
    >>> otp.datetime(2012, 12, 12, 12) - otp.Milli(1)
    2012-12-12 11:59:59.999000

    Using ``float`` value to pass nanoseconds:

    >>> otp.datetime(2012, 12, 12, 12) + otp.Milli(1.000123)
    2012-12-12 12:00:00.001000123

    Use offset in columns:

    >>> t = otp.Tick(A=1)
    >>> t['T'] = otp.datetime(2012, 12, 12, 12)
    >>> t['T'] += otp.Milli(t['A'])
    >>> otp.run(t)
            Time                       T  A
    0 2003-12-01 2012-12-12 12:00:00.001  1

    Use it to calculate difference between two dates:

    >>> t = otp.Tick(A=otp.dt(2022, 1, 1), B=otp.dt(2022, 1, 1, 0, 0, 1))
    >>> t['DIFF'] = otp.Milli(t['B'] - t['A'])
    >>> otp.run(t)
            Time           A                    B  DIFF
    0 2003-12-01  2022-01-01  2022-01-01 00:00:01  1000
""", additional_types=_float_dpf_types, additional_notes=_float_dpf_notes)

# microseconds are not supported yet

Nano = _construct_dpf(offsets.Nano, "nanosecond")
Nano.__doc__ = _add_examples_to_docs('nanosecond', """
    Add to or subtract from :py:class:`otp.datetime <onetick.py.datetime>` object:

    >>> otp.datetime(2012, 12, 12, 12) + otp.Nano(1)
    2012-12-12 12:00:00.000000001
    >>> otp.datetime(2012, 12, 12, 12) - otp.Nano(1)
    2012-12-12 11:59:59.999999999

    Use offset in columns:

    >>> t = otp.Tick(A=1)
    >>> t['T'] = otp.datetime(2012, 12, 12, 12)
    >>> t['T'] += otp.Nano(t['A'])
    >>> otp.run(t)
            Time                             T  A
    0 2003-12-01 2012-12-12 12:00:00.000000001  1

    Use it to calculate difference between two dates:

    >>> t = otp.Tick(A=otp.dt(2022, 1, 1), B=otp.dt(2022, 1, 1, 0, 0, 1))
    >>> t['DIFF'] = otp.Nano(t['B'] - t['A'])
    >>> otp.run(t)
            Time           A                    B        DIFF
    0 2003-12-01  2022-01-01  2022-01-01 00:00:01  1000000000
""")

# ---------------------------- #


class _inner_string(type):
    def __str__(cls):
        if cls.length is Ellipsis:
            return "varstring"
        if cls.length:
            return f"string[{cls.length}]"
        else:
            return "string"

    def __repr__(cls):
        return str(cls)

    # We have ot use functools.cache, because 'class' in python is an object,
    # and _inner_str for the same item is different for every call,
    # but we want to make str[1024] be equal to another str[1024]
    @functools.lru_cache(maxsize=None)  # noqa: W1518
    def __getitem__(cls, item):

        if (not isinstance(item, int) or item < 1) and item is not Ellipsis:
            raise TypeError("It is not allowed to have non numeric index")

        class _inner_str(string):  # type: ignore[misc]
            length = item

            def __len__(self):
                return self.__class__.length

        return _inner_str


class string(str, metaclass=_inner_string):  # type: ignore[misc]
    """
    OneTick data type representing string with length and varstring.
    To set string length use ``__getitem__``.
    If the length is not set then the :py:attr:`~DEFAULT_LENGTH` length is used by default.
    In this case using ``otp.string`` is the same as using ``str``.
    If the length is set to Ellipse it represents varstring. Varstring is used for returning variably sized strings.

    Note
    ----
    If you try to set value with length x to string[y] and x > y, value will be truncated to y length.

    Attributes
    ----------
    DEFAULT_LENGTH: int
        default length of the string when the length is not specified

    Examples
    --------

    Adding new fields with :class:`otp.string <string>` type:

    >>> t = otp.Tick(A=otp.string[32]('HELLO'))
    >>> t['B'] = otp.string[128]('WORLD')
    >>> t.schema
    {'A': string[32], 'B': string[128]}
    >>> otp.run(t)
            Time      A      B
    0 2003-12-01  HELLO  WORLD

    Note that class :class:`otp.string <string>` is a child of python's ``str`` class,
    so every object passed to constructor is converted to string.
    So it may not work as expected in some cases, e.g. when passing :class:`~onetick.py.Operation` objects
    (because their string representation is the name of the column or OneTick expression).
    In this case it's better to use direct type conversion to get :class:`otp.string <string>` object:

    >>> t = otp.Tick(X=otp.string[256](t['_SYMBOL_NAME']))
    >>> t['Y'] = t['_SYMBOL_NAME'].astype(otp.string[256])
    >>> t.schema
    {'X': string[256], 'Y': string[256]}
    >>> otp.run(t, symbols='AAAA')
            Time             X     Y
    0 2003-12-01  _SYMBOL_NAME  AAAA

    Setting the type of the existing field:

    >>> # OTdirective: skip-snippet:;
    >>> t = otp.Tick(A='a')
    >>> t = t.table(A=otp.string[10])
    >>> t.schema
    {'A': string[10]}

    Example of truncation column value to set string length.

    >>> # OTdirective: skip-snippet:;
    >>> t['A'] *= 100
    >>> t['B'] = t['A'].str.len()
    >>> otp.run(t)
            Time           A   B
    0 2003-12-01  aaaaaaaaaa  10

    Example of string with default length.

    >>> t = otp.Tick(A='a')
    >>> t['A'] *= 100
    >>> t['B'] = t['A'].str.len()
    >>> otp.run(t)
            Time                                                                 A   B
    0 2003-12-01  aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  64

    Setting Ellipsis as length represents varstring.

    >>> t = otp.Tick(A='a')
    >>> t = t.table(A=otp.string[...])
    >>> t.schema
    {'A': varstring}

    Varstring length is multiplied.

    >>> t['A'] *= 65
    >>> t['B'] = t['A'].str.len()
    >>> otp.run(t)
            Time                                                                  A   B
    0 2003-12-01  aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  65

    `otp.varstring` is a shortcut:

    >>> t = otp.Tick(A='a')
    >>> t = t.table(A=otp.varstring)
    >>> t.schema
    {'A': varstring}
    """

    DEFAULT_LENGTH = 64
    length = None

    def __repr__(self):
        return f'{self.__class__}({super().__repr__()})'


varstring = string[...]  # type: ignore[type-arg,misc]


class _nan_base(type):
    def __str__(cls):
        return "double"


class _nan(float, metaclass=_nan_base):
    """
    Object that represents NaN (not a number) float value.
    Can be used anywhere where float value is expected.

    Examples
    --------
    >>> t = otp.Ticks({'A': [1.1, 2.2, otp.nan]})
    >>> t['B'] = otp.nan
    >>> t['C'] = t['A'] / 0
    >>> t['D'] = t['A'] + otp.nan
    >>> otp.run(t)
                         Time    A   B    C   D
    0 2003-12-01 00:00:00.000  1.1 NaN  inf NaN
    1 2003-12-01 00:00:00.001  2.2 NaN  inf NaN
    2 2003-12-01 00:00:00.002  NaN NaN  NaN NaN
    """

    __name__ = 'nan'

    def __str__(self):
        return "NAN()"

    def __repr__(self):
        return 'nan'


nan = _nan()


class _inf(float, metaclass=_nan_base):
    """
    Object that represents infinity value.
    Can be used anywhere where float value is expected.

    Examples
    --------
    >>> t = otp.Ticks({'A': [1.1, 2.2, otp.inf]})
    >>> t['B'] = otp.inf
    >>> t['C'] = t['A'] / 0
    >>> t['D'] = t['A'] - otp.inf
    >>> otp.run(t)
                         Time    A    B    C    D
    0 2003-12-01 00:00:00.000  1.1  inf  inf -inf
    1 2003-12-01 00:00:00.001  2.2  inf  inf -inf
    2 2003-12-01 00:00:00.002  inf  inf  inf  NaN
    """

    __name__ = 'inf'

    def __init__(self):
        self._sign = ""  # empty string or '-' for negative infinity

    def __str__(self):
        return f"{self._sign}INFINITY()"

    def __repr__(self):
        return f'{self._sign}inf'

    def __neg__(self):
        result = _inf()
        result._sign = "" if self._sign else "-"
        return result


inf = _inf()


class decimal:
    """
    Object that represents decimal OneTick value.
    Decimal is 128 bit base 10 floating point number.

    Parameters
    ----------
    value: int, float, str
        The value to initialize decimal from.
        Note that float values may be converted with precision lost.

    Examples
    --------

    :py:class:`~onetick.py.types.decimal` objects can be used in tick generators
    and column operations as any other onetick-py type:

    >>> t = otp.Ticks({'A': [otp.decimal(1), otp.decimal(2)]})
    >>> t['B'] = otp.decimal(1.23456789)
    >>> t['C'] = t['A'] / 0
    >>> t['D'] = t['A'] + otp.nan
    >>> otp.run(t)
                         Time    A         B    C    D
    0 2003-12-01 00:00:00.000  1.0  1.234568  inf  NaN
    1 2003-12-01 00:00:00.001  2.0  1.234568  inf  NaN

    Additionally, any arithmetic operation with :py:class:`~onetick.py.types.decimal` object will return
    an :py:class:`~onetick.py.Operation` object:

    >>> t = otp.Tick(A=1)
    >>> t['X'] = otp.decimal(1) / 0
    >>> otp.run(t)
            Time    A    X
    0 2003-12-01    1  inf

    Note that converting from float (first row) may result in losing precision.
    :py:class:`~onetick.py.types.decimal` objects are created from strings or integers, so they don't lose precision:

    >>> t0 = otp.Tick(A=0.1)
    >>> t1 = otp.Tick(A=otp.decimal(0.01))
    >>> t2 = otp.Tick(A=otp.decimal('0.001'))
    >>> t3 = otp.Tick(A=otp.decimal(1) / otp.decimal(10_000))
    >>> t = otp.merge([t0, t1, t2, t3], enforce_order=True)
    >>> t['STR_A'] = t['A'].decimal.str(34)
    >>> otp.run(t)
            Time       A                                 STR_A
    0 2003-12-01  0.1000  0.1000000000000000055511151231257827
    1 2003-12-01  0.0100  0.0100000000000000000000000000000000
    2 2003-12-01  0.0010  0.0010000000000000000000000000000000
    3 2003-12-01  0.0001  0.0001000000000000000000000000000000

    Note that :py:class:`otp.Ticks <onetick.py.Ticks>` will convert everything from string under the hood,
    so even the float values will not lose precision:

    >>> t = otp.Ticks({'A': [0.1, otp.decimal(0.01), otp.decimal('0.001'), otp.decimal(1e-4)]})
    >>> t['STR_A'] = t['A'].decimal.str(34)
    >>> otp.run(t)
                         Time       A                                 STR_A
    0 2003-12-01 00:00:00.000  0.1000  0.1000000000000000000000000000000000
    1 2003-12-01 00:00:00.001  0.0100  0.0100000000000000000000000000000000
    2 2003-12-01 00:00:00.002  0.0010  0.0010000000000000000000000000000000
    3 2003-12-01 00:00:00.003  0.0001  0.0001000000000000000000000000000000
    """
    def __new__(cls, *args, **kwargs):
        # this method dynamically adds properties and methods
        # from otp.Operation class to this one

        # otp.decimal class doesn't fit well in onetick-py type system,
        # so this class is a mix of both type and Operation logic

        # Basically it works like this:
        #   otp.decimal is a OneTick type
        #   otp.decimal(1) is a decimal type object
        # Doing anything with this object returns an otp.Operation:
        #   otp.decimal(1) / 2

        def proxy_wrap(attr, value):
            if callable(value):
                @functools.wraps(value)
                def f(self, *args, **kwargs):
                    op = self.to_operation()
                    return getattr(op, attr)(*args, **kwargs)
                return f
            else:
                @functools.wraps(value)
                def f(self):
                    op = self.to_operation()
                    return getattr(op, attr)
                return property(f)

        for attr, value in inspect.getmembers(otp.Operation):
            # comparison methods are defined by default for some reason,
            # but we want to get them from otp.Operation
            if not hasattr(cls, attr) or attr in ('__lt__', '__le__', '__eq__', '__ne__', '__gt__', '__ge__'):
                setattr(cls, attr, proxy_wrap(attr, value))

        return super().__new__(cls)

    def __init__(self, value):
        from onetick.py.core.column_operations.base import OnetickParameter
        supported_types = (str, int, float, OnetickParameter)
        if not isinstance(value, supported_types):
            raise TypeError(f"Parameter 'value' must be one of these types: {supported_types}, got {type(value)}")
        self.__value = value

    @classmethod
    def _to_onetick_type_string(cls):
        # called by ott.type2str
        return 'decimal'

    def _to_onetick_string(self):
        # called by ott.value2str
        from onetick.py.core.column_operations.base import OnetickParameter
        if isinstance(self.__value, OnetickParameter):
            value = self.__value
        else:
            value = str(self.__value)
        return f'STRING_TO_DECIMAL({value2str(value)})'

    def to_operation(self):
        return otp.Operation(op_str=self._to_onetick_string(), dtype=decimal)

    def __str__(self):
        # called by otp.CSV, we don't need to convert the value with OneTick functions in this case
        return str(self.__value)

    def __repr__(self):
        return f"{self.__class__.__name__}({value2str(self.__value)})"

    def __format__(self, __format_spec: str) -> str:
        return _decimal.Decimal(self.__value).__format__(__format_spec)

# --------------------------------------------------------------- #
# AUXILIARY FUNCTIONS
# --------------------------------------------------------------- #


def is_type_basic(dtype):
    return dtype in (
        int,
        float,
        str,
        byte,
        short,
        uint,
        ulong,
        _int,
        long,
        nsectime,
        msectime,
        decimal,
    ) or issubclass(dtype, string)


# TODO: PY-632: unify these functions with others
def get_source_base_type(value):
    if inspect.isclass(value):
        value_type = value
        if not is_type_basic(value_type):
            warnings.warn('Setting schema with complex types is deprecated,'
                          ' use basic type instead', FutureWarning, stacklevel=2)
    else:
        warnings.warn('Setting schema with instance of the class is deprecated,'
                      ' use type instead', FutureWarning, stacklevel=2)
        value_type = type(value)
        # convert string to custom string if necessary
        if value_type is str and len(value) > string.DEFAULT_LENGTH:
            value_type = string[len(value)]

    if issubclass(value_type, bool):
        value_type = float

    if is_time_type(value_type):
        value_type = nsectime

    # check valid value type
    if get_base_type(value_type) not in [int, float, str, bool, decimal]:
        raise TypeError(f'Type "{repr(value_type)}" is not supported.')

    if not is_type_basic(value_type):
        raise TypeError(f"Type {repr(value_type)} can't be set in schema.")
    return value_type


def is_type_supported(dtype):
    return get_base_type(dtype) in [int, float, str, bool, decimal] or issubclass(dtype, (datetime, date))


def get_base_type(obj):
    if issubclass(obj, str):
        return str
    elif issubclass(obj, bool):
        return bool
    elif issubclass(obj, int):
        return int
    elif issubclass(obj, float):
        return float
    elif issubclass(obj, decimal):
        return decimal

    return type(None)


def get_object_type(obj):
    if isinstance(obj, (_nan, _inf)):
        return float
    if isinstance(obj, Type):
        return obj
    if hasattr(obj, 'dtype'):
        dtype = obj.dtype
        if isinstance(dtype, np.dtype):
            return dtype.type
        return dtype
    if is_time_type(obj):
        return nsectime
    # pylint: disable-next=unidiomatic-typecheck
    if type(obj) is int and obj > long.MAX:
        # by default we use python's int (onetick's long) for numbers
        # in case the number is too big, let's use onetick's ulong
        return ulong
    return type(obj)


def get_type_by_objects(objs):
    """
    Helper that calculates the widest type of the list passed objects.
    Used to determine type by returned values.
    """

    # collect types
    types = set()
    for v in objs:
        t = get_object_type(v)
        if issubclass(t, str):
            t = str
        types.add(t)

    # does not allow to mix string and numeric types
    dtype = None
    if str in types and (float in types or int in types or bool in types or nsectime in types or msectime in types):
        raise TypeError("It is not allowed to return values of string type and numeric type in one function.")

    # if there is only one value there, then
    # use it as is
    if len(types) == 1:
        dtype = next(iter(types))
        if dtype is bool:
            return dtype

    # process numeric types: the most generic is float
    if int in types:
        dtype = int
    if bool in types:
        dtype = float
    # None is equal to otp.nan
    if float in types or type(None) in types:
        dtype = float

    # process string types, taking into account OneTick long strings
    if str in types:
        max_len = string.DEFAULT_LENGTH
        for v in objs:
            t = get_object_type(v)
            if issubclass(t, string):
                if t.length is Ellipsis or max_len is Ellipsis:
                    max_len = Ellipsis
                else:
                    max_len = max(t.length, max_len)
            elif isinstance(v, str):
                max_len = max(len(v), max_len)

        if max_len == string.DEFAULT_LENGTH:
            dtype = str
        else:
            dtype = string[max_len]  # pylint: disable=E1136

    # process msectime and nsectime
    if dtype is float and (msectime in types or nsectime in types):
        raise TypeError("It is not allowed to return value of time type and float type in one function.")

    if msectime in types:
        dtype = msectime
    if nsectime in types:
        dtype = nsectime

    # we assume the None value has float default value, ie NaN
    # pylint: disable-next=unidiomatic-typecheck
    if type(None) is dtype:
        dtype = float

    return dtype


# ------------------- #
# extend datetime


class AbstractTime:
    def __init__(self):
        self.ts: pd.Timestamp

    @property
    def year(self):
        return self.ts.year

    @property
    def month(self):
        return self.ts.month

    @property
    def day(self):
        return self.ts.day

    def date(self):
        return _date(self.year, self.month, self.day)

    @property
    def start(self):
        return pd.Timestamp(self.year, self.month, self.day)

    @property
    def end(self):
        return pd.Timestamp(next_day(self.start))

    def strftime(self, fmt):
        return self.ts.strftime(fmt)

    @property
    def value(self):
        return self.ts.value

    def timestamp(self):
        return self.ts.timestamp()

    def __eq__(self, other):
        other = getattr(other, "ts", other)
        return self.ts == other

    def __hash__(self):
        return hash(self.ts)

    def __gt__(self, other):
        other = getattr(other, "ts", other)
        return self.ts > other

    def __ge__(self, other):
        other = getattr(other, "ts", other)
        return self.ts >= other

    def __lt__(self, other):
        other = getattr(other, "ts", other)
        return self.ts < other

    def __le__(self, other):
        other = getattr(other, "ts", other)
        return self.ts <= other


class datetime(AbstractTime):
    """
    Class :py:class:`otp.datetime <onetick.py.datetime>` is used for representing date with time in onetick-py.
    It can be used both when specifying start and end time for queries and
    in column operations with :py:class:`onetick.py.Source`.
    :ref:`Datetime offset objects <datetime_offsets>` (e.g. `otp.Nano`, `otp.Day`)
    can be added to or subtracted from `otp.datetime` object.

    Note
    ----
    Class :py:class:`otp.datetime <onetick.py.datetime>` share many methods
    that classes :pandas:`pandas.Timestamp` and :py:class:`datetime.datetime` have,
    but these objects are not fully interchangeable.
    Class :py:class:`otp.datetime <onetick.py.datetime>` should work in all onetick-py methods and classes,
    other classes should work too if documented,
    and may even work when not documented, but the users should not count on it.

    Parameters
    ----------
    first_arg: int, str, :py:class:`otp.datetime <onetick.py.datetime>`, :py:class:`otp.date <onetick.py.date>`\
                :pandas:`pandas.Timestamp`, :py:class:`datetime.datetime`
        If `month`, `day` and other parts of date are specified,
        first argument will be considered as year.
        Otherwise, first argument will be converted to :py:class:`otp.datetime <onetick.py.datetime>`.
    month: int
        Number between 1 and 12.
    day: int
        Number between 1 and 31.
    hour: int, default=0
        Number between 0 and 23.
    minute: int, default=0
        Number between 0 and 59.
    second: int, default=0
        Number between 0 and 59.
    microsecond: int, default=0
        Number between 0 and 999999.
    nanosecond: int, default=0
        Number between 0 and 999.
    tzinfo: :py:class:`datetime.tzinfo`
        Timezone object.
    tz: str
        Timezone name.

    Examples
    --------

    Initialization by :py:class:`datetime.datetime` class from standard library:

    >>> otp.datetime(datetime.datetime(2019, 1, 1, 1))
    2019-01-01 01:00:00

    Initialization by :pandas:`pandas.Timestamp` class:

    >>> otp.datetime(pd.Timestamp(2019, 1, 1, 1))
    2019-01-01 01:00:00

    Initialization by int timestamp:

    >>> otp.datetime(1234567890)
    1970-01-01 00:00:01.234567890

    Initialization by params with nanoseconds:

    >>> otp.datetime(2019, 1, 1, 1, 2, 3, 4, 5)
    2019-01-01 01:02:03.000004005

    Initialization by string:

    >>> otp.datetime('2019/01/01 1:02')
    2019-01-01 01:02:00

    `otp.dt` is the alias for `otp.datetime`:

    >>> otp.dt(2019, 1, 1)
    2019-01-01 00:00:00

    See also
    --------
    :ref:`Datetime offset objects <datetime_guide>`.
    """
    def __init__(
        self,
        first_arg,
        month=None,
        day=None,
        hour=None,
        minute=None,
        second=None,
        microsecond=None,
        nanosecond=None,
        *,
        tzinfo=None,
        tz=None,
    ):  # TODO: python 3.8 change first_arg to positional only arg
        tz, tzinfo = self._process_timezones_args(tz, tzinfo)

        if not any([month, day, hour, minute, second, microsecond, nanosecond]):
            result = self._create_from_one_arg(first_arg, tz, tzinfo)
        else:
            result = self._create_from_several_arg(first_arg, month, day, hour, minute, second, microsecond, nanosecond,
                                                   tzinfo)
        self.ts = result

    def _process_timezones_args(self, tz, tzinfo):
        if tz is not None:
            if tzinfo is None:
                # parameter tz in pandas.Timestamp is broken https://github.com/pandas-dev/pandas/issues/31929
                # it is fixed in pandas>=2.0.0, but we need to support older versions
                tzinfo = get_tzfile_by_name(tz)
                tz = None
            else:
                raise ValueError(
                    "tzinfo and tz params are mutually exclusive parameters, "
                    "they can't be specified both at the same time"
                )
        return tz, tzinfo

    def _create_from_several_arg(self, first_arg, month, day, hour, minute, second, microsecond, nanosecond, tzinfo):
        if nanosecond is not None and not (0 <= nanosecond <= 999):
            raise ValueError(
                "Nanosecond parameter should be between 0 and 999. "
                "Please use microsecond parameter or otp.Nano object."
            )
        if parse_version(pd.__version__) >= parse_version("2.0.0"):
            result = pd.Timestamp(
                first_arg, month, day, hour or 0, minute or 0, second or 0, microsecond or 0,
                nanosecond=nanosecond or 0,
            ).replace(tzinfo=tzinfo)
        else:
            result = pd.Timestamp(
                first_arg, month, day, hour or 0, minute or 0, second or 0, microsecond or 0, nanosecond or 0,
            ).replace(tzinfo=tzinfo)
        return result

    def _create_from_one_arg(self, first_arg, tz, tzinfo):
        arg_tz = getattr(first_arg, "tz", None)
        arg_tzinfo = getattr(first_arg, "tzinfo", None)
        if tz and arg_tz and arg_tz != tz or tzinfo and arg_tzinfo and arg_tzinfo != tzinfo:
            raise ValueError(
                "You've specified the timezone for the object, which already has it. "
                "It is recommended to swap the current timezone to desired by method of this object "
                "and then create otp.datetime object."
            )
        if isinstance(first_arg, (datetime, date)):
            first_arg = first_arg.ts
        result = pd.Timestamp(first_arg, tzinfo=tzinfo, tz=tz)
        return result

    @property
    def start(self):
        return super().start.replace(tzinfo=self.tzinfo)

    @property
    def end(self):
        return super().end.replace(tzinfo=self.tzinfo)

    def replace(self, **kwargs):
        """
        Replace parts of `otp.datetime` object.

        Parameters
        ----------
        year: int, optional
        month: int, optional
        day: int, optional
        hour: int, optional
        minute: int, optional
        second: int, optional
        microsecond: int, optional
        nanosecond: int, optional
        tzinfo: tz-convertible, optional

        Returns
        -------
        result: :py:class:`otp.datetime <onetick.py.datetime>`
            Timestamp with fields replaced.

        Examples
        --------
        >>> ts = otp.datetime(2022, 2, 24, 3, 15, 54, 999, 1)
        >>> ts
        2022-02-24 03:15:54.000999001
        >>> ts.replace(year=2000, month=2, day=2, hour=2, minute=2, second=2, microsecond=2, nanosecond=2)
        2000-02-02 02:02:02.000002002
        """
        return datetime(self.ts.replace(**kwargs))

    @property
    def tz(self):
        return self.ts.tz

    @property
    def tzinfo(self):
        return self.ts.tzinfo

    @property
    def hour(self):
        return self.ts.hour

    @property
    def minute(self):
        return self.ts.minute

    @property
    def second(self):
        return self.ts.second

    @property
    def microsecond(self):
        return self.ts.microsecond

    @property
    def nanosecond(self):
        return self.ts.nanosecond

    @staticmethod
    def now(tz=None):
        """
        Will return :py:class:`otp.datetime <onetick.py.datetime>` object
        with timestamp at the moment of calling this function.
        Not to be confused with function :func:`otp.now <onetick.py.now>` which can only add column
        with current timestamp to the :py:class:`otp.Source <onetick.py.Source>` when running the query.

        Parameters
        ----------
        tz : str or timezone object, default None
            Timezone to localize to.
        """
        return datetime(pd.Timestamp.now(tz))

    def __add__(self, other):
        """
        Add :ref:`datetime offset <datetime_offsets>` to otp.datetime.

        Parameters
        ----------
        other: :ref:`datetime offset <datetime_offsets>`, :py:class:`otp.datetime <onetick.py.datetime>`
            object to add

        Returns
        -------
        result: :py:class:`otp.datetime <onetick.py.datetime>`, :pandas:`pandas.Timedelta`
            return :py:class:`otp.datetime <onetick.py.datetime>`
            if otp.Nano or another :ref:`datetime offset <datetime_offsets>` object was passed as an argument,
            or :pandas:`pandas.Timedelta` object if :py:class:`otp.datetime <onetick.py.datetime>`
            was passed as an argument.

        Examples
        --------
        >>> otp.datetime(2022, 2, 24) + otp.Nano(1)
        2022-02-24 00:00:00.000000001
        """
        self._error_on_int_param(other, "+")
        return datetime(self.ts + other)

    def __sub__(self, other):
        """
        Subtract :ref:`datetime offset <datetime_offsets>` from otp.datetime.

        Parameters
        ----------
        other: :ref:`datetime offset <datetime_offsets>`, :py:class:`otp.datetime <onetick.py.datetime>`
            object to subtract

        Returns
        -------
        result: :py:class:`otp.datetime <onetick.py.datetime>`, :pandas:`pandas.Timedelta`
            return datetime if otp.Nano or another :ref:`datetime offset <datetime_offsets>`
            object was passed as an argument,
            or :pandas:`pandas.Timedelta` object if :py:class:`otp.datetime <onetick.py.datetime>`
            was passed as an argument.

        Examples
        --------
        >>> otp.datetime(2022, 2, 24) - otp.Nano(1)
        2022-02-23 23:59:59.999999999
        """
        self._error_on_int_param(other, "-")
        other = getattr(other, "ts", other)
        result = self.ts - other
        # do not convert to datetime in case timedelta is returned (arg is date)
        result = datetime(result) if isinstance(result, pd.Timestamp) else result
        return result

    def _error_on_int_param(self, other, op):
        if isinstance(other, int):
            raise TypeError(f"unsupported operand type(s) for {op}: 'otp.datetime' and 'int'")

    def __str__(self):
        return str(self.ts)

    def __repr__(self):
        return str(self.ts)

    def tz_localize(self, tz):
        """
        Localize tz-naive datetime object to a given timezone

        Parameters
        ----------
        tz: str or tzinfo
            timezone to localize datetime object into

        Returns
        -------
        result: :py:class:`otp.datetime <onetick.py.datetime>`
            localized datetime object

        Examples
        --------
        >>> d = otp.datetime(2021, 6, 3)
        >>> d.tz_localize("EST5EDT")
        2021-06-03 00:00:00-04:00
        """
        return datetime(self.ts.tz_localize(tz))

    def tz_convert(self, tz):
        """
        Convert tz-aware datetime object to another timezone

        Parameters
        ----------
        tz: str or tzinfo
            timezone to convert datetime object into

        Returns
        -------
        result: :py:class:`otp.datetime <onetick.py.datetime>`
            converted datetime object

        Examples
        --------
        >>> d = otp.datetime(2021, 6, 3, tz="EST5EDT")
        >>> d.tz_convert("Europe/Moscow")
        2021-06-03 07:00:00+03:00
        """
        return datetime(self.ts.tz_convert(tz))

    def to_operation(self, timezone=None):
        """
        Convert :py:class:`otp.datetime <onetick.py.datetime>` object to
        :py:class:`otp.Operation <onetick.py.Operation>`

        Parameters
        ----------
        timezone: Operation
            Can be used to specify timezone as an Operation.

        Examples
        --------
        >>> t = otp.Ticks(TZ=['EST5EDT', 'GMT'])
        >>> t['DT'] = otp.dt(2022, 1, 1).to_operation(timezone=t['TZ'])
        >>> otp.run(t, timezone='GMT')[['TZ', 'DT']]
                TZ                  DT
        0  EST5EDT 2022-01-01 05:00:00
        1      GMT 2022-01-01 00:00:00
        """
        return otp.Operation(op_str=otp.types.datetime2expr(self, timezone=timezone), dtype=otp.nsectime)


dt = datetime


class date(datetime):
    """
    Class ``date`` is used for representing date in onetick-py.
    It can be used both when specifying start and end time for queries and
    in column operations with :py:class:`onetick.py.Source`.

    Note
    ----
    Class :py:class:`otp.date <onetick.py.date>` share many methods
    that classes :pandas:`pandas.Timestamp` and :py:class:`datetime.date` have,
    but these objects are not fully interchangeable.
    Class :py:class:`otp.date <onetick.py.date>` should work in all onetick-py methods and classes,
    other classes should work too if documented,
    and may even work when not documented, but the users should not count on it.

    Parameters
    ----------
    first_arg: int, str, :py:class:`otp.datetime <onetick.py.datetime>`, :py:class:`otp.date <onetick.py.date>`\
                :pandas:`pandas.Timestamp`, :py:class:`datetime.datetime`, :py:class:`datetime.date`
        If `month` and `day` arguments are specified, first argument will be considered as year.
        Otherwise, first argument will be converted to otp.date.
    month: int
        Number between 1 and 12.
    day: int
        Number between 1 and 31.

    Examples
    --------
    :ref:`Datetime guide <datetime_guide>`.
    """

    def __init__(self, first_arg: Union[int, str, _date, _datetime, pd.Timestamp, AbstractTime],
                 month=None, day=None):
        if month is None and day is None:
            if isinstance(first_arg, AbstractTime):
                first_arg = first_arg.ts
            elif isinstance(first_arg, (int, str)):
                first_arg = pd.Timestamp(first_arg)
            if isinstance(first_arg, (_datetime, pd.Timestamp, datetime)):
                first_arg = first_arg.date()
            self.ts = pd.Timestamp(first_arg)  # remove hour, minutes and so on
        elif all((month, day)):
            self.ts = pd.Timestamp(first_arg, month, day)
        else:
            raise ValueError("Please specify three integers (year, month, day) "
                             "or object or create date from (string, int timestamp, "
                             "pandas.Timestamp, otp.datetime, otp.date, "
                             "datetime.datetime, datetime.date)")

    def __str__(self):
        return self.ts.strftime("%Y-%m-%d")

    def __repr__(self):
        return self.ts.strftime("%Y-%m-%d")

    def to_str(self, format="%Y%m%d"):
        """
        Convert date to string, by default it will be in YYYYMMDD format.

        Parameters
        ----------
        format: str
            strftime format of string to convert to.
        Returns
        -------
        result: str
        """
        return self.ts.strftime(format)


class _integer_meta(type):
    def __str__(cls):
        return getattr(cls, '_NAME', cls.__name__)

    @property
    def TYPE_SIZE(cls):
        return 8 * ctypes.sizeof(cls._CTYPE)

    @property
    def MIN(cls):
        if cls._UNSIGNED:
            return 0
        else:
            return -(2 ** (cls.TYPE_SIZE - 1))

    @property
    def MAX(cls):
        if cls._UNSIGNED:
            return (2 ** cls.TYPE_SIZE) - 1
        else:
            return (2 ** (cls.TYPE_SIZE - 1)) - 1


class _integer(int, metaclass=_integer_meta):
    def __new__(cls, value, *args, **kwargs):
        if not cls.MIN <= value <= cls.MAX:
            raise ValueError(f"{cls.__name__} values must be between {cls.MIN} and {cls.MAX}")
        return super().__new__(cls, value, *args, **kwargs)

    def __get_result(self, value):
        if isinstance(value, int):
            return self.__class__(self._CTYPE(value).value)
        return value

    def __add__(self, other):
        return self.__get_result(
            super().__add__(other)
        )

    def __radd__(self, other):
        return self.__get_result(
            super().__radd__(other)
        )

    def __sub__(self, other):
        return self.__get_result(
            super().__sub__(other)
        )

    def __rsub__(self, other):
        return self.__get_result(
            super().__rsub__(other)
        )

    def __mul__(self, other):
        return self.__get_result(
            super().__mul__(other)
        )

    def __rmul__(self, other):
        return self.__get_result(
            super().__rmul__(other)
        )

    def __truediv__(self, other):
        return self.__get_result(
            super().__truediv__(other)
        )

    def __rtruediv__(self, other):
        return self.__get_result(
            super().__rtruediv__(other)
        )

    def __str__(self):
        return super().__repr__()

    def __repr__(self):
        return f"{self.__class__.__name__}({self})"


class long(_integer):
    """
    OneTick data type representing signed long integer.

    The size of the type is not specified and may vary across different systems.
    Most commonly it's a 8-byte type with allowed values from -2**63 to 2**63 - 1.

    Note that the value is checked to be valid in constructor,
    but no overflow checking is done when arithmetic operations are performed.

    Examples
    --------
    >>> t = otp.Tick(A=otp.long(1))
    >>> t['B'] = otp.long(1) + 1
    >>> t.schema
    {'A': <class 'onetick.py.types.long'>, 'B': <class 'onetick.py.types.long'>}
    """
    _CTYPE = ctypes.c_long
    _UNSIGNED = False


class ulong(_integer):
    """
    OneTick data type representing unsigned long integer.

    The size of the type is not specified and may vary across different systems.
    Most commonly it's a 8-byte type with allowed values from 0 to 2**64 - 1.

    Note that the value is checked to be valid in constructor,
    but no overflow checking is done when arithmetic operations are performed.

    Examples
    --------
    >>> t = otp.Tick(A=otp.ulong(1))
    >>> t['B'] = otp.ulong(1) + 1
    >>> t.schema
    {'A': <class 'onetick.py.types.ulong'>, 'B': <class 'onetick.py.types.ulong'>}

    Note that arithmetic operations may result in overflow.
    Here we get 2**64 - 1 instead of -1.

    .. testcode::
       :skipif: is_windows()

       t = otp.Tick(A=otp.ulong(0) - 1)
       df = otp.run(t)
       print(df)

    .. testoutput::

               Time                     A
       0 2003-12-01  18446744073709551615
    """
    _CTYPE = ctypes.c_ulong
    _UNSIGNED = True


class _int(_integer):
    """
    OneTick data type representing signed integer.

    The size of the type is not specified and may vary across different systems.
    Most commonly it's a 4-byte type with allowed values from -2**31 to 2**31 - 1.

    Note that the value is checked to be valid in constructor,
    but no overflow checking is done when arithmetic operations are performed.

    Examples
    --------
    >>> t = otp.Tick(A=otp.int(1))
    >>> t['B'] = otp.int(1) + 1
    >>> t.schema
    {'A': <class 'onetick.py.types._int'>, 'B': <class 'onetick.py.types._int'>}
    """
    _CTYPE = ctypes.c_int
    _UNSIGNED = False
    _NAME = 'int'


class uint(_integer):
    """
    OneTick data type representing unsigned integer.

    The size of the type is not specified and may vary across different systems.
    Most commonly it's a 4-byte type with allowed values from 0 to 2**32 - 1.

    Note that the value is checked to be valid in constructor,
    but no overflow checking is done when arithmetic operations are performed.

    Examples
    --------
    >>> t = otp.Tick(A=otp.uint(1))
    >>> t['B'] = otp.uint(1) + 1
    >>> t.schema
    {'A': <class 'onetick.py.types.uint'>, 'B': <class 'onetick.py.types.uint'>}

    Note that arithmetic operations may result in overflow.
    Here we get 2**32 - 1 instead of -1.

    .. testcode::
       :skipif: is_windows()

       t = otp.Tick(A=otp.uint(0) - 1)
       df = otp.run(t)
       print(df)

    .. testoutput::

               Time           A
       0 2003-12-01  4294967295
    """
    _CTYPE = ctypes.c_uint
    _UNSIGNED = True


class byte(_integer):
    """
    OneTick data type representing byte integer.

    The size of the type is not specified and may vary across different systems.
    Most commonly it's a 1-byte type with allowed values from -128 to 127.

    Note that the value is checked to be valid in constructor,
    but no overflow checking is done when arithmetic operations are performed.

    Examples
    --------
    >>> t = otp.Tick(A=otp.byte(1))
    >>> t['B'] = otp.byte(1) + 1
    >>> t.schema
    {'A': <class 'onetick.py.types.byte'>, 'B': <class 'onetick.py.types.byte'>}

    Note that arithmetic operations may result in overflow.
    Here we get 127 instead of -129.

    >>> t = otp.Tick(A=otp.byte(-128) - 1)
    >>> otp.run(t)
            Time    A
    0 2003-12-01  127
    """
    _CTYPE = ctypes.c_byte
    _UNSIGNED = False


class short(_integer):
    """
    OneTick data type representing short integer.

    The size of the type is not specified and may vary across different systems.
    Most commonly it's a 2-byte type with allowed values from -32768 to 32767.

    Note that the value is checked to be valid in constructor,
    but no overflow checking is done when arithmetic operations are performed.

    Examples
    --------
    >>> t = otp.Tick(A=otp.short(1))
    >>> t['B'] = otp.short(1) + 1
    >>> t.schema
    {'A': <class 'onetick.py.types.short'>, 'B': <class 'onetick.py.types.short'>}

    Note that arithmetic operations may result in overflow.
    Here we get 32767 instead of -32769.

    >>> t = otp.Tick(A=otp.short(-32768) - 1)
    >>> otp.run(t)
            Time      A
    0 2003-12-01  32767
    """
    _CTYPE = ctypes.c_short
    _UNSIGNED = False


# ------------------- #
def type2str(t):
    if t is int:
        return "long"
    if t is str:
        return "string"
    if t is float:
        return "double"
    if t is None:
        return ''
    if t is decimal:
        return t._to_onetick_type_string()
    return str(t)


def str2type(type_name: str):
    """
    Converts OneTick type by its name into Python/OTP domain type.

    Parameters
    ----------
    type_name: str
        name of type from CSV or OneTick DB

    Returns
    -------
    class:
        Python/OTP type representing OneTick type
    """
    if type_name == "time32":
        return int
    if type_name == "long":
        # otp.long can be used too, but we use int for backward compatibility
        return int
    if type_name == "int":
        return _int
    if type_name == "byte":
        return byte
    if type_name == "short":
        return short
    if type_name == "uint":
        return uint
    if type_name == "ulong":
        return ulong
    elif type_name in ["double", "float"]:
        return float
    elif type_name == "decimal":
        return decimal
    elif type_name == "msectime":
        return msectime
    elif type_name == "nsectime":
        return nsectime
    elif type_name in ["string", "matrix", f"string[{string.DEFAULT_LENGTH}]"]:
        return str
    elif type_name == "varstring":
        return varstring
    elif type_name.find("string") != -1:
        length = int(type_name[type_name.find("[") + 1:type_name.find("]")])
        return string[length]
    return None


def type2np(t):
    if issubclass(t, str):
        if issubclass(t, otp.string) and isinstance(t.length, int):
            return '<U' + str(t.length)
        return '<U64'
    elif issubclass(t, bool):
        return 'boolean'
    elif issubclass(t, nsectime):
        return 'datetime64[ns]'
    elif issubclass(t, msectime):
        return 'datetime64[ms]'
    elif issubclass(t, datetime):
        return 'datetime64[ns]'
    elif issubclass(t, byte):
        if otq.webapi:
            return 'int8'
        return 'int32'
    elif issubclass(t, short):
        if otq.webapi:
            return 'int16'
        return 'int32'
    elif issubclass(t, uint):
        return 'uint32'
    elif issubclass(t, ulong):
        return 'uint64'
    elif issubclass(t, _int):
        return 'int32'
    elif issubclass(t, long):
        return 'int64'
    elif issubclass(t, int):
        return 'int64'
    elif issubclass(t, float):
        return 'float64'
    elif issubclass(t, decimal):
        return 'float64'
    else:
        return np.dtype(t)


def np2type(t):
    if not isinstance(t, (np.dtype, pd.api.extensions.ExtensionDtype)):
        raise ValueError(f'Unsupported value passed to `np2type`: `{t}` not numpy dtype')

    if t.name == 'int64':
        return int
    elif t.name == 'int8':
        return byte
    elif t.name == 'int16':
        return short
    elif t.name == 'int32':
        return _int
    elif t.name == 'uint64':
        return ulong
    elif t.name == 'uint32':
        return uint
    elif t.name == 'float64':
        return float
    elif t.name == 'boolean':
        return bool
    elif t.name.startswith('datetime64[ns'):
        return nsectime
    elif t.name.startswith('datetime64[ms'):
        return msectime
    elif t.name == 'object':
        return str
    elif t.str.startswith('<U'):
        length = t.name[2:]
        if length:
            return string[int(length)]
        else:
            return str

    raise ValueError(f'Unknown numpy dtype passed to `np2type`: `{t}`')


# TODO: move this union of types to some common place
def datetime2expr(
    dt_obj: Union[_datetime, _date, pd.Timestamp, date, datetime],
    timezone: Optional[str] = None,
    timezone_naive: Optional[str] = None,
) -> str:
    """
    Convert python datetime values to OneTick string representation.
    If ``dt_obj`` is timezone-aware then timezone will be taken from ``dt_obj`` value.
    If ``dt_obj`` is timezone-naive then timezone specified with otp.config['tz'] or otp.run() will be used.

    Parameters
    ----------
    dt_obj
        date or datetime value
    timezone: str or Operation
        This timezone will be used unconditionally.
    timezone_naive: str or Operation
        This timezone will be used if ``dt_obj`` is timezone-naive.
    """
    dt_str = _format_datetime(dt_obj, '%Y-%m-%d %H:%M:%S.%f', add_nano_suffix=True)
    if timezone is None:
        timezone = get_timezone_from_datetime(dt_obj)
    if timezone is None:
        timezone = timezone_naive
    if not isinstance(timezone, otp.Operation):
        timezone = f'"{timezone}"' if timezone else '_TIMEZONE'
    return f'PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J", "{dt_str}", {str(timezone)})'


def datetime2timeval(dt_obj: Union[_datetime, _date, pd.Timestamp, date, datetime], timezone: str = 'GMT'):
    """
    Get nanosecond-precision pyomd.timeval_t
    from different datetime types supported by onetick-py.

    If ``dt_obj`` is timezone-aware, then its timezone will be used.
    If ``dt_obj`` is timezone-naive , then it will be localized to ``timezone`` parameter (GMT by default).
    """
    dt_str = _format_datetime(dt_obj, '%Y-%m-%d %H:%M:%S.%f', add_nano_suffix=True)
    tz_str = get_timezone_from_datetime(dt_obj)
    if tz_str:
        # use timezone from the object
        return pyomd.TimeParser('%Y-%m-%d %H:%M:%S.%J', tz_str).parse_time(dt_str)
    else:
        # localize to the timezone specified in fucntion parameter
        return pyomd.TimeParser('%Y-%m-%d %H:%M:%S.%J', timezone).parse_time(dt_str)


def _format_datetime(dt_obj, fmt, add_nano_suffix=True):
    dt_str = dt_obj.strftime(fmt)
    if add_nano_suffix:
        if isinstance(dt_obj, (pd.Timestamp, datetime)):
            dt_str += f'{dt_obj.nanosecond:03}'[-3:]
        else:
            dt_str += '000'
    return dt_str


def value2str(v):
    """
    Converts a python value from the `v` parameter into OneTick format.
    """
    if issubclass(type(v), str):
        # there is no escape, so replacing double quotes with concatenation with it
        return '"' + str(v).replace('"', '''"+'"'+"''') + '"'

    if isinstance(v, decimal):
        return v._to_onetick_string()

    if isinstance(v, float) and not (isinstance(v, (_inf, _nan))):
        # PY-286: support science notation
        s = str(v)
        if "e" in s:
            return f'atof({value2str(s)})'
        if s == "nan":
            return str(nan)
        return s

    if is_time_type(v):
        return datetime2expr(v)

    if isinstance(v, nsectime):
        # we do not need the same for msectime because it works as is
        if int(v) == 0 or int(v) > 15e12:  # it is 2445/5/1
            return f'NSECTIME({v})'
        # This branch is for backward compatibility. Originally here was a bug that
        # allowed to pass only milliseconds as a value into the otp.nsectime constructor.
        # Obviously we expect there only nanoseconds, and the built-in NSECTIME works only
        # with nanoseconds.
        warnings.warn('It seems that you are using number of milliseconds as nanoseconds. ', stacklevel=2)

    return str(v)


# TODO: maybe can be removed, it is used only in tests now
def time2nsectime(time, timezone=None):
    """
    Converts complex time types to nsectime timestamp.

    Parameters
    ----------
    time: :py:class:`datetime.datetime`, :py:class:`datetime.date`,\
            :py:class:`otp.datetime <onetick.py.datetime>`, :py:class:`otp.date <onetick.py.date>`, pandas.Timestamp
        time to convert
    timezone:
        convert timezone before nsectime calculation

    Returns
    -------
    result: int
        number of nanoseconds since epoch
    """
    if isinstance(time, (_datetime, _date)):
        time = pd.Timestamp(time)
    elif isinstance(time, date):
        time = datetime(time)
    if timezone:
        if not has_timezone_parameter():  # accommodating legacy behavior prior to 20220327-3 weekly build
            time = time.replace(tzinfo=None)
        else:
            if time.tzinfo is None:
                time = time.tz_localize(timezone)
            else:
                # timezone conversion doesn't change the offset from epoch, only string representation,
                # so time.value (the number of nanoseconds) will stay the same
                # this line can be deleted
                time = time.tz_convert(timezone)
    return time.value


def is_time_type(time):
    """ Returns true if argument is subclass of any time type

    Checks if pass type is time type, currently checks for otp.date, otp.datetime,
    pd.Timestamp, datetime.date, datetime.datetime

    Parameters
    ----------
    time:
        object or type of the object

    Returns
    -------
    result: bool
        Return true if argument is time type

    Examples
    --------

    >>> is_time_type(datetime.datetime)  # OTdirective: skip-example: ;
    True
    >>> is_time_type(type(5))   # OTdirective: skip-example: ;
    False
    >>> is_time_type(datetime.datetime(2019, 1, 1))  # OTdirective: snippet-name: types.is time;
    True
    """
    time = time if inspect.isclass(time) else type(time)
    # do not check for datetime.datetime and pd.Timestamp, because they are in the same hierarchy
    # datetime.date -> datetime.datetime -> pd.Timestamp, where `->` means base class
    return issubclass(time, (_date, datetime, date))


def next_day(dt_obj: Union[_date, _datetime, date, datetime, pd.Timestamp]) -> _datetime:
    """
    Return the start of the next day of ``dt_obj`` as timezone-naive :py:class:`datetime.datetime`.

    Next day in this case means simply incrementing day/month/year number,
    not adding 24 hours (which may return the same date on DST days).
    """
    return _datetime(dt_obj.year, dt_obj.month, dt_obj.day) + _timedelta(days=1)


def default_by_type(dtype):
    """
    Get default value by OneTick type.

    Parameters
    ----------
    dtype:
        one of onetick-py base types

    Examples
    --------
    >>> otp.default_by_type(float)
    nan
    >>> otp.default_by_type(otp.decimal)
    decimal(0)
    >>> otp.default_by_type(int)
    0
    >>> otp.default_by_type(otp.ulong)
    ulong(0)
    >>> otp.default_by_type(otp.uint)
    uint(0)
    >>> otp.default_by_type(otp.short)
    short(0)
    >>> otp.default_by_type(otp.byte)
    byte(0)
    >>> otp.default_by_type(otp.nsectime)
    nsectime(0)
    >>> otp.default_by_type(otp.msectime)
    msectime(0)
    >>> otp.default_by_type(str)
    ''
    >>> otp.default_by_type(otp.string)
    string('')
    >>> otp.default_by_type(otp.string[123])
    string[123]('')
    >>> otp.default_by_type(otp.varstring)
    varstring('')
    """
    # TODO: think if we want to treat bool as basic onetick type
    if dtype is bool:
        return 0
    if not is_type_basic(dtype):
        raise TypeError(f"Can't get default value for type: {dtype}")
    if issubclass(dtype, int):
        return dtype(0)
    if dtype is otp.decimal:
        return otp.decimal(0)
    if issubclass(dtype, float):
        return nan
    if issubclass(dtype, str):
        return dtype('')
    if issubclass(dtype, nsectime) or issubclass(dtype, msectime):
        return dtype(0)
    raise TypeError(f"Can't get default value for type: {dtype}")


class timedelta(pd.Timedelta):
    """
    The object representing the delta between timestamps.

    Parameters
    ----------
    value: :py:class:`otp.timedelta <onetick.py.timedelta>`, :pandas:`pandas.Timedelta`,\
           :py:class:`datetime.timedelta`, str, or int
        Initialize this object from other types of objects.
    kwargs:
        Dictionary of offset names and their values.
        Available offset names:
        *weeks*, *days*, *hours*, *minutes*, *seconds*,
        *milliseconds*, *microseconds*, *nanoseconds*.

    Examples
    --------

    Create :py:class:`otp.timedelta <onetick.py.timedelta>` from key-value arguments:

    >>> otp.timedelta(weeks=1, days=1, hours=1, minutes=1, seconds=1, milliseconds=1, microseconds=1, nanoseconds=1)
    timedelta('8 days 01:01:01.001001001')

    Create :py:class:`otp.timedelta <onetick.py.timedelta>` from different types of objects:

    >>> otp.timedelta(datetime.timedelta(days=2, hours=3))
    timedelta('2 days 03:00:00')

    >>> otp.timedelta('20 days 13:02:01.999777666')
    timedelta('20 days 13:02:01.999777666')

    Adding :py:class:`otp.timedelta <onetick.py.timedelta>` object to :py:class:`otp.datetime <onetick.py.datetime>`:

    >>> otp.datetime(2022, 1, 1, 1, 2, 3) + otp.timedelta(days=1, hours=1, minutes=1, seconds=1)
    2022-01-02 02:03:04

    Adding :py:class:`otp.timedelta <onetick.py.timedelta>` object to :py:class:`otp.date <onetick.py.date>`:

    >>> otp.date(2022, 1, 1) + otp.timedelta(weeks=1, nanoseconds=1)
    2022-01-08 00:00:00.000000001
    """

    def __repr__(self):
        return super().__repr__().lower()

    def _get_offset(self):
        return self.value, 'nanosecond'
