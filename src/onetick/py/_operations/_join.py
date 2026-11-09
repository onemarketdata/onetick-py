import onetick.py.types as ott
from onetick.py.core.column import _Column
from onetick.py.core.column_operations.base import _Operation

__all__ = ['sametime', 'sametime_as_existing']


def _check_time_column(column, parameter_name):
    if not isinstance(column, _Column) or column.name != 'TIMESTAMP':
        raise ValueError(f"Parameter '{parameter_name}' must be the timestamp column of the source, "
                         "e.g. data['Time'] or data['TIMESTAMP']")


def sametime(left_time: _Column, right_time: _Column,
             keep_zero_duration_ticks: bool | None = None) -> _Operation:
    """
    Returns **True** for a tick from the left source and the tick from the right source
    which arrived immediately before it, and vice versa
    (for a tick from the right source and the tick from the left source which arrived immediately before it).

    Can be used only in the ``on`` parameter of :func:`onetick.py.join`.

    Parameters
    ----------
    left_time: :class:`~onetick.py.Column`
        Timestamp column of the left source (``left['TIMESTAMP']``).
    right_time: :class:`~onetick.py.Column`
        Timestamp column of the right source (``right['TIMESTAMP']``).
    keep_zero_duration_ticks: bool
        If **False**, output ticks that last zero time are discarded from the output.

        Such ticks can result when some ticks in the first time series have the same timestamps
        as the ticks in the other time series. When a tick from the first time series arrives,
        the corresponding tick with the same timestamp from the other time series might not have arrived yet,
        so that the output tick would contain the new tick from the first time series and the oldest tick
        from the second time series. However, when a new tick from the second time series (with the same timestamp)
        arrives, the computed output tick contains new ticks from the first and second time series.
        Timestamps of the output ticks would be the same, and the first output tick would be undesirable;
        for example when joining two same-sized vectors.

        Default is **True**.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | :func:`onetick.py.join`
    | :func:`onetick.py.ops.sametime_as_existing`
    | **SAMETIME** OneTick function
    | **JOIN** OneTick event processor

    Examples
    --------

    Join each tick with the tick from the other source that arrived immediately before it:

    >>> left = otp.Ticks(A=[1, 2, 3], offset=[0, 2, 4])
    >>> right = otp.Ticks(B=[10, 20, 30], offset=[1, 3, 5])
    >>> data = otp.join(left, right, on=otp.ops.sametime(left['TIMESTAMP'], right['TIMESTAMP']), how='inner')
    >>> otp.run(data)
                         Time  A   B
    0 2003-12-01 00:00:00.001  1  10
    1 2003-12-01 00:00:00.002  2  10
    2 2003-12-01 00:00:00.003  2  20
    3 2003-12-01 00:00:00.004  3  20
    4 2003-12-01 00:00:00.005  3  30

    Zero-duration ticks can be discarded with ``keep_zero_duration_ticks=False``:

    >>> left = otp.Ticks(A=[1, 2, 3])
    >>> right = otp.Ticks(B=[10, 20, 30])
    >>> data = otp.join(left, right, on=otp.ops.sametime(left['TIMESTAMP'], right['TIMESTAMP']), how='inner')
    >>> otp.run(data)
                         Time  A   B
    0 2003-12-01 00:00:00.000  1  10
    1 2003-12-01 00:00:00.001  2  10
    2 2003-12-01 00:00:00.001  2  20
    3 2003-12-01 00:00:00.002  3  20
    4 2003-12-01 00:00:00.002  3  30

    >>> data = otp.join(left, right,
    ...                 on=otp.ops.sametime(left['TIMESTAMP'], right['TIMESTAMP'], keep_zero_duration_ticks=False),
    ...                 how='inner')
    >>> otp.run(data)
                         Time  A   B
    0 2003-12-01 00:00:00.000  1  10
    1 2003-12-01 00:00:00.001  2  20
    2 2003-12-01 00:00:00.002  3  30
    """
    _check_time_column(left_time, 'left_time')
    _check_time_column(right_time, 'right_time')

    def fun(_left_time, _right_time, _keep_zero_duration_ticks):
        params = [str(_left_time), str(_right_time)]
        if _keep_zero_duration_ticks is not None:
            params.append(ott.value2str('TRUE' if _keep_zero_duration_ticks else 'FALSE'))
        return f'SAMETIME({",".join(params)})', bool

    return _Operation(
        op_func=fun,
        op_params=[left_time, right_time, keep_zero_duration_ticks],
    )


def sametime_as_existing(left_time: _Column, right_time: _Column, delay_msec: int) -> _Operation:
    """
    If ``delay_msec`` is positive, it returns **True** for a tick from the passed left time series and the latest tick
    from the passed right time series that arrived at least by ``delay_msec`` before the tick from the left time series.

    If ``delay_msec`` is negative, it returns **True** for a tick from the passed left time series and the latest tick
    from the passed right time series that arrived at most ``delay_msec`` after the tick from the left time series;
    that is, the latest tick from the interval ``(-Inf, t1 + |delay_msec|]``, where ``t1`` is the timestamp of the tick
    from the left time series.

    Can be used only in the ``on`` parameter of :func:`onetick.py.join`.

    Parameters
    ----------
    left_time: :class:`~onetick.py.Column`
        Timestamp column of the left source (``left['TIMESTAMP']``).
    right_time: :class:`~onetick.py.Column`
        Timestamp column of the right source (``right['TIMESTAMP']``).
    delay_msec: int
        Delay in milliseconds.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | :func:`onetick.py.join`
    | :func:`onetick.py.ops.sametime`
    | **SAMETIME_AS_EXISTING** OneTick function
    | **JOIN** OneTick event processor

    Examples
    --------

    Join each tick from the left source with the latest tick from the right source
    that arrived at least 2 milliseconds before it:

    >>> left = otp.Ticks(A=[1, 2, 3], offset=[3, 5, 7])
    >>> right = otp.Ticks(B=[10, 20, 30, 40], offset=[0, 2, 4, 6])
    >>> data = otp.join(left, right, on=otp.ops.sametime_as_existing(left['TIMESTAMP'], right['TIMESTAMP'], 2))
    >>> otp.run(data)
                         Time  A   B
    0 2003-12-01 00:00:00.003  1  10
    1 2003-12-01 00:00:00.005  2  20
    2 2003-12-01 00:00:00.007  3  30

    With negative delay ticks from the right source that arrived less than 2 milliseconds after
    the tick from the left source are also considered:

    >>> data = otp.join(left, right, on=otp.ops.sametime_as_existing(left['TIMESTAMP'], right['TIMESTAMP'], -2))
    >>> otp.run(data)
                         Time  A   B
    0 2003-12-01 00:00:00.003  1  30
    1 2003-12-01 00:00:00.005  2  40
    2 2003-12-01 00:00:00.007  3  40
    """
    _check_time_column(left_time, 'left_time')
    _check_time_column(right_time, 'right_time')
    if not isinstance(delay_msec, int) or isinstance(delay_msec, bool):
        raise TypeError(f"Parameter 'delay_msec' must be an integer, got {type(delay_msec).__name__}")

    def fun(_left_time, _right_time, _delay_msec):
        params = [str(_left_time), str(_right_time), str(_delay_msec)]
        return f'SAMETIME_AS_EXISTING({",".join(params)})', bool

    return _Operation(
        op_func=fun,
        op_params=[left_time, right_time, delay_msec],
    )
