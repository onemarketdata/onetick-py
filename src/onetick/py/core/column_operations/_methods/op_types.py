import datetime
import inspect

import pandas as pd
import numpy as np

from onetick.py import types as ott


def are_numerics(*dtypes):
    return all(
        inspect.isclass(dtype)
        and (issubclass(dtype, (float, int)) or np.issubdtype(dtype, np.integer) or issubclass(dtype, ott.decimal))
        and not issubclass(dtype, (ott.nsectime, ott.msectime))
        for dtype in dtypes
    )


def are_ints_not_time(*dtypes):
    return all(inspect.isclass(dtype)
               and (issubclass(dtype, int) or np.issubdtype(dtype, np.integer))
               and not issubclass(dtype, (ott.nsectime, ott.msectime)) for dtype in dtypes)


def are_time(*dtypes):
    return all(inspect.isclass(dtype) and issubclass(dtype, (ott.nsectime, ott.msectime)) for dtype in dtypes)


def are_ints_or_time(*dtypes):
    return all(inspect.isclass(dtype)
               and (issubclass(dtype, (int, datetime.datetime, ott.datetime, pd.Timestamp))
                    or np.issubdtype(dtype, np.integer))
               for dtype in dtypes)


def are_floats(*dtypes):
    return all(inspect.isclass(dtype) and issubclass(dtype, (float, np.float64, np.double)) for dtype in dtypes)


def are_strings(*dtypes):
    return all(inspect.isclass(dtype) and issubclass(dtype, str) for dtype in dtypes)


def are_bools(*dtypes):
    return all(inspect.isclass(dtype) and issubclass(dtype, bool) for dtype in dtypes)


def _get_widest_type(left, right):
    """ return the widest type

    If the left is subclass of right return left, if right is subclass of left return right,
        returns None in all other cases. ott.nsectime, ott.msectime and bool won't be considered as int, only
        custom classes.

    Parameters
    ----------
    left: type
        type of the first argument
    right: type
        type of the second argument

    Returns
    -------
    result: type or None
        widest type if it exist, or None in another case

    Examples
    --------

    >>> class MyInt(int):   # OTdirective: skip-snippet:;
    ...     pass
    >>> class MyFloat(float):
    ...     pass
    >>> def test():
    ...     print(_get_widest_type(int, MyInt))
    ...     print(_get_widest_type(float, MyFloat))
    ...     print(_get_widest_type(int, float))
    ...     print(_get_widest_type(MyInt, MyFloat))
    ...     print(_get_widest_type(bool, float))
    ...     print(_get_widest_type(int, bool))
    ...     print(_get_widest_type(MyInt, bool))
    ...     print(_get_widest_type(bool, MyFloat))
    >>> test()  # doctest: +ELLIPSIS
    <class '...MyInt'>
    <class '...MyFloat'>
    <class 'float'>
    <class '...MyFloat'>
    <class 'float'>
    <class 'int'>
    <class '...MyInt'>
    <class '...MyFloat'>

    >>> from onetick.py import types as ott     # OTdirective: skip-snippet:;
    >>> class MyString(str):
    ...     pass
    >>> (_get_widest_type(str, MyString),
    ...  _get_widest_type(str, ott.string[15]),
    ...  _get_widest_type(MyString, ott.string[15]))  # doctest: +ELLIPSIS
    (<class 'onetick.py.core.column_operations._methods.op_types.MyString'>, string[15], None)

    >>> from onetick.py import types as ott     # OTdirective: skip-snippet:;
    >>> _get_widest_type(int, ott.msectime), _get_widest_type(int, ott.nsectime)
    (None, None)

    >>> from onetick.py import types as ott     # OTdirective: skip-snippet:;
    >>> class MyTime(ott.OTPBaseTimeStamp):
    ...     pass
    >>> class MyNSec(ott.nsectime):
    ...     pass
    >>> (_get_widest_type(MyTime, ott.OTPBaseTimeStamp), _get_widest_type(MyTime, ott.nsectime),
    ... _get_widest_type(MyNSec, ott.nsectime))  # doctest: +ELLIPSIS
    (<class '...MyTime'>, None, <class '...MyNSec'>)
    """

    # decimal takes precedence before integer and floating point types
    if issubclass(left, ott.decimal) and are_numerics(right):
        return left
    if are_numerics(left) and issubclass(right, ott.decimal):
        return right

    if issubclass(left, float) and issubclass(right, float):
        # between np.float and float we choose base float
        if left is not float and np.issubdtype(left, np.floating):
            left = float
        if right is not float and np.issubdtype(right, np.floating):
            right = float

    if issubclass(left, float):
        if are_ints_not_time(right):
            return left
        else:
            return _get_wideclass(left, right) if issubclass(right, float) else None
    elif issubclass(right, float):
        if are_ints_not_time(left):
            return right
        else:
            return _get_wideclass(left, right) if issubclass(left, float) else None

    if are_time(left) and are_ints_not_time(right) or are_time(right) and are_ints_not_time(left):
        return None

    if issubclass(right, int) and issubclass(left, bool):
        return right
    if issubclass(left, int) and issubclass(right, bool):
        return left

    if issubclass(right, int) and np.issubdtype(left, np.integer):
        return right
    if issubclass(left, int) and np.issubdtype(right, np.integer):
        return left

    return _get_wideclass(left, right)


def _get_wideclass(left, right):
    if issubclass(left, right):
        return left
    if issubclass(right, left):
        return right
    return None
