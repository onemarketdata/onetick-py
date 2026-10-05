import onetick.py.types as ott
from onetick.py.core.column_operations.base import _Operation


def today(timezone: str | _Operation | None = None):
    """
    Returns the current date in the specified ``timezone``.

    Parameters
    ----------
    timezone: str | :py:class:`~onetick.py.Operation`
        The timezone name to use.
        Special value ``local`` can be used to use local timezone name.

        By default the timezone of the query is used.

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    See Also
    --------
    | **TODAY** OneTick function
    | **CURRENT_DATE**/**CURDATE** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data['TODAY'] = otp.today()
    >>> otp.run(data)  # doctest: +SKIP
            Time  A       TODAY
    0 2003-12-01  1  2026-09-25
    """
    def fun(timezone):
        if timezone == 'local':
            return 'CURRENT_DATE()', ott.nsectime
        if timezone is None:
            timezone = '_TIMEZONE'
        else:
            timezone = ott.value2str(timezone)
        return f'TODAY({timezone})', ott.nsectime

    return _Operation(
        op_func=fun,
        op_params=[timezone],
    )


def matches_time_filter(start_time: int | str | _Operation,
                        end_time: int | str | _Operation,
                        timezone: str | _Operation | None = None,
                        day_patterns: str | _Operation | None = None,
                        end_time_tick_matches: bool | _Operation | None = None):
    """
    Returns True if a tick matches the specified time filter criteria,
    including a time range and optional day patterns.

    Parameters
    ----------
    start_time, end_time: int | str | :py:class:`~onetick.py.Operation`
        The start and end times of the filter, specified as either an integer or a string.

        Accepted formats:

        * HHMMSS: For example, 093000 for 9:30 AM
        * HHMMSSmmm: For example, 093000000 for 9:30:00.000 AM.
        * HH:MM:SS[.mmm]: For example, "09:30:00.000" for 9:30 AM.

    timezone: str | :py:class:`~onetick.py.Operation`
        The timezone to use.

        By default the timezone of the query is used.

    day_patterns: str or :py:class:`~onetick.py.Operation`
        A string representing day patterns that define when ticks should be propagated.
        For more details and examples of supported formats, refer to the :meth:`onetick.py.Source.time_filter`.

    end_time_tick_matches: bool or :py:class:`~onetick.py.Operation`
        A flag indicating whether ticks at the ``end_time`` boundary
        should be considered within the filter range.

        Defaults to False.

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    See Also
    --------
    | :meth:`onetick.py.Source.time_filter`
    | **MATCHES_TIME_FILTER** OneTick function
    | **TIME_FILTER** OneTick event processor

    Examples
    --------

    Set flag for each timestamp from 02:30 till 06:00:

    >>> data = otp.Ticks(A=range(8), offset=[otp.Hour(i) for i in range(8)])
    >>> data['X'] = otp.matches_time_filter('023000', '060000')
    >>> otp.run(data)
                     Time  A    X
    0 2003-12-01 00:00:00  0  0.0
    1 2003-12-01 01:00:00  1  0.0
    2 2003-12-01 02:00:00  2  0.0
    3 2003-12-01 03:00:00  3  1.0
    4 2003-12-01 04:00:00  4  1.0
    5 2003-12-01 05:00:00  5  1.0
    6 2003-12-01 06:00:00  6  0.0
    7 2003-12-01 07:00:00  7  0.0

    Set parameter ``end_time_tick_matches`` to include ``end_time`` in the range:

    >>> data = otp.Ticks(A=range(8), offset=[otp.Hour(i) for i in range(8)])
    >>> data['X'] = otp.matches_time_filter('023000', '060000', end_time_tick_matches=True)
    >>> otp.run(data)
                     Time  A    X
    0 2003-12-01 00:00:00  0  0.0
    1 2003-12-01 01:00:00  1  0.0
    2 2003-12-01 02:00:00  2  0.0
    3 2003-12-01 03:00:00  3  1.0
    4 2003-12-01 04:00:00  4  1.0
    5 2003-12-01 05:00:00  5  1.0
    6 2003-12-01 06:00:00  6  1.0
    7 2003-12-01 07:00:00  7  0.0
    """

    def fun(start_time, end_time, timezone=None, day_patterns=None, end_time_tick_matches=None):
        params = [ott.value2str(start_time), ott.value2str(end_time)]
        if timezone is not None:
            params.append(ott.value2str(timezone))
        else:
            params.append('_TIMEZONE')
        if day_patterns is not None:
            params.append(ott.value2str(day_patterns))
        else:
            params.append(ott.value2str('0/0'))
        if end_time_tick_matches is not None:
            params.append(ott.value2str(end_time_tick_matches))
        params_str = ','.join(params)
        return f'MATCHES_TIME_FILTER({params_str})', bool

    return _Operation(
        op_func=fun,
        op_params=[start_time, end_time, timezone, day_patterns, end_time_tick_matches],
    )
