from typing import TYPE_CHECKING, Optional

from onetick import py as otp
from onetick.py import types as ott
from onetick.py.otq import otq

from .misc import inplace_operation

if TYPE_CHECKING:
    from onetick.py.core.source import Source


@inplace_operation
def update_timestamp(
    self: 'Source',
    timestamp_field: str,
    timestamp_psec_field: Optional[str] = None,
    max_delay_of_original_timestamp=0,
    max_delay_of_new_timestamp=0,
    max_out_of_order_interval=0,
    max_delay_handling: str = 'complain',
    out_of_order_timestamp_handling: str = 'complain',
    zero_timestamp_handling: Optional[str] = None,
    log_sequence_violations: bool = False,
    inplace=False,
) -> Optional['Source']:
    """
    Assigns alternative timestamps to input ticks.

    In case resulting timestamps are out of order, time series is sorted in ascending order of those timestamps.

    Ticks with equal alternative timestamps are sorted in ascending order of the respective original timestamps,
    and equal ones among those are further sorted by ascending values of the **OMDSEQ** field,
    if such a field is present.

    Note
    ----

    In case parameters ``max_delay_of_original_timestamp`` or ``max_delay_of_new_timestamp`` are specified
    this method automatically uses :py:meth:`~onetick.py.Source.modify_query_times` method, affecting
    the start or end time of the query thus possibly changing some logic of the nodes placed higher in the graph.

    Also there are some limitations on using this method in the graph, e.g. this method can't be used in the
    "diamond" pattern and can't be used twice in the same graph.

    Parameters
    ----------
    timestamp_field: str
        Specifies the name of the input field, which is assumed to carry alternative timestamps.
    timestamp_psec_field: str
        Fractional (< 1 millisecond) parts of alternative timestamps will be taken from this field.
        They are assumed to be in picoseconds (0.001 of nanosecond).

        If this field is specified, then even if alternative timestamps have nanosecond granularity,
        only millisecond parts of them will be taken, nanosecond part will be rewritten.

        Useful in case ``timestamp_field`` has lower than millisecond granularity.

    max_delay_of_original_timestamp: int or :ref:`datetime offset<api/datetime/offsets/root:datetime offsets>`
        Changes the start time of the query to ``original_start_time - max_delay_of_original_timestamp``
        to make sure it processes all possible ticks that might have a new timestamp in the query time range.

        If integer value is specified, it is assumed to be milliseconds.

    max_delay_of_new_timestamp: int or :ref:`datetime offset<api/datetime/offsets/root:datetime offsets>`
        Changes the end time of the query to ``original_end_time + max_delay_of_new_timestamp``
        to make sure it processes all possible ticks that might have a new timestamp in the query time range.

        If integer value is specified, it is assumed to be milliseconds.

    max_out_of_order_interval: int or :ref:`datetime offset<api/datetime/offsets/root:datetime offsets>`
        Specifies the maximum out-of-order interval for alternative timestamps.
        Ticks with new timestamps out of order will be sorted in ascending order.

        This is the only parameter that leads to accumulation of ticks.

        If integer value is specified, it is assumed to be milliseconds.

    max_delay_handling: str ('complain' or 'discard' or 'use_original_timestamp' or 'use_new_timestamp')

        This parameter how to process ticks that are delayed even more than specified in
        ``max_delay_of_original_timestamp`` or ``max_delay_of_new_timestamp`` parameters.

        - **complain**: raise an exception

        - **discard**: do not add tick to the output time series

        - **use_original_timestamp**: assign original timestamp to the tick

        - **use_new_timestamp**: try to assign new timestamp to the tick. If the new timestamp of previous tick
            is greater than new timestamp of this tick then previous one is used.
            **NOTE: A previously propagated timestamp could be from a heartbeat.**
            When there is no previous propagated tick and the new timestamp falls behind the query start time,
            the latter is preferred, while query end time is preferred if the new timestamp exceeds it.

        This parameter is processed *before* any action from ``out_of_order_timestamp_handling`` parameter.

    out_of_order_timestamp_handling: str ('complain' or 'use_previous_value' or 'use_original_timestamp')

        This parameter how to process ticks that are out of order even more than specified in
        ``max_out_of_order_interval`` parameter.

        - **complain**: raise an exception

        - **use_previous_value**: assign new timestamp from a previous propagated tick

        - **use_original_timestamp**: try to use original timestamp. If maximum out-of-order interval is still
            exceeded then exception will be thrown.

        This parameter is processed *after* any action from ``max_delay_handling`` parameter.

    zero_timestamp_handling: str ('preserve_sequence'), optional

        This parameter specifies how to process ticks with zero alternative timestamps.

        If value is None, actions from ``max_delay_handling``
        and ``out_of_order_timestamp_handling`` are performed on it.

        If value is **preserve_sequence** then new timestamp will be set to the maximum between
        query start time and the new timestamp of the previous propagated tick.

    log_sequence_violations: bool
        If set to True, then warnings about actions from parameters ``out_of_order_timestamp_handling``,
        ``max_delay_handling`` and ``zero_timestamp_handling`` are logged into the log file.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise method returns a new modified object.

    Returns
    -------
    :class:`Source` or ``None``

    See also
    --------
    **UPDATE_TIMESTAMP** OneTick event processor

    Examples
    --------

    Data and timestamps from the database:

    >>> start = otp.dt(2022, 3, 2)
    >>> end = otp.dt(2022, 3, 3)
    >>> data = otp.DataSource('US_COMP', symbols='AAPL', tick_type='TRD')
    >>> otp.run(data, start=start, end=end)
                         Time  PRICE  SIZE
    0 2022-03-02 00:00:00.000    1.0   100
    1 2022-03-02 00:00:00.001    1.1   101
    2 2022-03-02 00:00:00.002    1.2   102

    Adding one hour to all ticks.
    Parameter ``max_delay_of_original_timestamp`` must be specified in this case:

    >>> data = otp.DataSource('US_COMP', symbols='AAPL', tick_type='TRD')
    >>> data['ORIG_TS'] = data['TIMESTAMP']
    >>> data['NEW_TS'] = data['TIMESTAMP'] + otp.Hour(1)
    >>> data = data.update_timestamp('NEW_TS', max_delay_of_original_timestamp=otp.Hour(1))
    >>> otp.run(data, start=start, end=end)[['Time', 'PRICE', 'SIZE', 'ORIG_TS']]
                         Time  PRICE  SIZE                  ORIG_TS
    0 2022-03-02 01:00:00.000    1.0   100  2022-03-02 00:00:00.000
    1 2022-03-02 01:00:00.001    1.1   101  2022-03-02 00:00:00.001
    2 2022-03-02 01:00:00.002    1.2   102  2022-03-02 00:00:00.002

    Subtracting one day from all ticks.
    Parameter ``max_delay_of_new_timestamp`` must be specified in this case.

    >>> data = otp.DataSource('US_COMP', symbols='AAPL', tick_type='TRD')
    >>> data['ORIG_TS'] = data['TIMESTAMP']
    >>> data['NEW_TS'] = data['TIMESTAMP'] - otp.Day(1)
    >>> data = data.update_timestamp('NEW_TS', max_delay_of_new_timestamp=otp.Day(1))
    >>> otp.run(data, start=start - otp.Day(1), end=end)[['Time', 'PRICE', 'SIZE', 'ORIG_TS']]
                         Time  PRICE  SIZE                 ORIG_TS
    0 2022-03-01 00:00:00.000    1.0   100 2022-03-02 00:00:00.000
    1 2022-03-01 00:00:00.001    1.1   101 2022-03-02 00:00:00.001
    2 2022-03-01 00:00:00.002    1.2   102 2022-03-02 00:00:00.002

    Parameter ``max_delay_handling`` can be used to specify how to handle ticks exceeding the maximum:

    >>> data = otp.DataSource('US_COMP', symbols='AAPL', tick_type='TRD')
    >>> data['ORIG_TS'] = data['TIMESTAMP']
    >>> data['NEW_TS'] = data.apply(
    ...     lambda row: row['TIMESTAMP'] + otp.Hour(24)
    ...     if row['PRICE'] == 1.1
    ...     else row['TIMESTAMP'] + otp.Hour(1)
    ... )
    >>> data = data.update_timestamp('NEW_TS',
    ...                              max_delay_of_original_timestamp=otp.Hour(1),
    ...                              max_delay_handling='discard')
    >>> otp.run(data, start=start, end=end)[['Time', 'PRICE', 'SIZE', 'ORIG_TS']]
                         Time  PRICE  SIZE                  ORIG_TS
    0 2022-03-02 01:00:00.000    1.0   100  2022-03-02 00:00:00.000
    1 2022-03-02 01:00:00.002    1.2   102  2022-03-02 00:00:00.002

    Parameter ``max_out_of_order_interval`` can be used in case new timestamp are out of order:

    >>> data = otp.DataSource('US_COMP', symbols='AAPL', tick_type='TRD')
    >>> data['ORIG_TS'] = data['TIMESTAMP']
    >>> data = data.agg({'COUNT': otp.agg.count()}, running=True, all_fields=True)
    >>> data['NEW_TS'] = data['TIMESTAMP'] - otp.Minute(data['COUNT'])
    >>> data = data.update_timestamp('NEW_TS',
    ...                              max_delay_of_new_timestamp=otp.Hour(10),
    ...                              max_out_of_order_interval=otp.Minute(100))
    >>> otp.run(data, start=start - otp.Hour(2), end=end)[['Time', 'PRICE', 'SIZE', 'ORIG_TS', 'COUNT']]
                         Time  PRICE  SIZE                 ORIG_TS  COUNT
    0 2022-03-01 23:57:00.002    1.2   102 2022-03-02 00:00:00.002      3
    1 2022-03-01 23:58:00.001    1.1   101 2022-03-02 00:00:00.001      2
    2 2022-03-01 23:59:00.000    1.0   100 2022-03-02 00:00:00.000      1
    """
    if timestamp_field not in self.schema:
        raise ValueError(f"Field '{timestamp_field}' is not in schema")
    if self.schema[timestamp_field] not in {otp.msectime, otp.nsectime}:
        raise ValueError(f"Unsupported type for 'timestamp_field': {self.schema[timestamp_field]}")

    if timestamp_psec_field is not None:
        if timestamp_psec_field not in self.schema:
            raise ValueError(f"Field '{timestamp_psec_field}' is not in schema")
        if self.schema[timestamp_psec_field] is not int:
            raise ValueError(f"Unsupported type for 'timestamp_psec_field': {self.schema[timestamp_psec_field]}")

    if max_delay_handling not in {'complain', 'discard', 'use_original_timestamp', 'use_new_timestamp'}:
        raise ValueError(f"Unsupported value for parameter 'max_delay_handling': {max_delay_handling}")

    if out_of_order_timestamp_handling not in {'complain', 'use_previous_value', 'use_original_timestamp'}:
        raise ValueError(
            f"Unsupported value for parameter 'out_of_order_timestamp_handling': {out_of_order_timestamp_handling}"
        )

    if zero_timestamp_handling not in {None, 'preserve_sequence'}:
        raise ValueError(f"Unsupported value for parameter 'zero_timestamp_handling': {zero_timestamp_handling}")

    def get_ms(value):
        if isinstance(value, int):
            return value
        try:
            nanos = value.nanos
        except Exception as e:
            raise ValueError(f'Unsupported parameter value: {type(value)}') from e
        if nanos < 1_000_000:
            raise ValueError('Values less than one millisecond are not supported in this parameter')
        return nanos // 1_000_000

    self.sink(
        otq.UpdateTimestamp(
            timestamp_field_name=timestamp_field,
            timestamp_psec_field_name=timestamp_psec_field or '',
            max_delay_of_original_timestamp=get_ms(max_delay_of_original_timestamp),
            max_delay_of_new_timestamp=get_ms(max_delay_of_new_timestamp),
            max_out_of_order_interval_msec=get_ms(max_out_of_order_interval),
            max_delay_handling=max_delay_handling,
            out_of_order_timestamp_handling=out_of_order_timestamp_handling,
            zero_timestamp_handling=zero_timestamp_handling,
            log_sequence_violations=log_sequence_violations,
        )
    )
    return self


@inplace_operation
def modify_query_times(
    self: 'Source', start=None, end=None, output_timestamp=None, propagate_heartbeats=True, inplace=False
) -> Optional['Source']:
    """
    Modify ``start`` and ``end`` time of the query.

    * query times are changed for all operations
        only **before** this method up to the source of the graph.
    * all ticks' timestamps produced by this method
        **must** fall into original time range of the query.

    It is possible to change ticks' timestamps with parameter ``output_timestamp``,
    so they will stay inside the original time range.

    Note
    ----
    Due to how OneTick works internally, tick generators
    :py:class:`otp.Tick <onetick.py.Tick>` and :py:func:`otp.Ticks <onetick.py.Ticks>`
    are not affected by this method.

    Parameters
    ----------
    start: :py:class:`otp.datetime <onetick.py.datetime>` or \
            :py:class:`~onetick.py.core.source.MetaFields` or :py:class:`~onetick.py.Operation`
        Expression to replace query start time.
        By default, start time is not changed.
        Note that expression in this parameter can't depend on ticks, thus only
        :py:class:`~onetick.py.core.source.MetaFields` and constants can be used.
    end: :py:class:`otp.datetime <onetick.py.datetime>` or \
            :py:class:`~onetick.py.core.source.MetaFields` or :py:class:`~onetick.py.Operation`
        Expression to replace query end time.
        By default, end time is not changed.
        Note that expression in this parameter can't depend on ticks, thus only
        :py:class:`~onetick.py.core.source.MetaFields` and constants can be used.
    output_timestamp: :py:class:`onetick.py.Operation`
        Expression that produces timestamp for each tick.
        By default, the following expression is used: ``orig_start + orig_timestamp - start``
        This expression covers cases when start time of the query is changed and keeps
        timestamp inside original time range.
        Note that it doesn't cover cases, for example, if end time was increased,
        you have to handle such cases yourself.
    propagate_heartbeats: bool
        Controls heartbeat propagation.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise method returns a new modified object.

    See also
    --------
    | **MODIFY_QUERY_TIMES** OneTick event processor
    | :py:meth:`onetick.py.Source.time_interval_shift`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    >>> start = otp.dt(2022, 3, 2)
    >>> end = otp.dt(2022, 3, 2) + otp.Milli(3)
    >>> data = otp.DataSource('US_COMP', symbols='AAPL', tick_type='TRD')

    By default, method does nothing:

    >>> t = data.modify_query_times()
    >>> otp.run(t, start=start, end=end)
                         Time  PRICE  SIZE
    0 2022-03-02 00:00:00.000    1.0   100
    1 2022-03-02 00:00:00.001    1.1   101
    2 2022-03-02 00:00:00.002    1.2   102

    See how ``_START_TIME`` and ``_END_TIME`` meta fields are changed.
    They are changed *before* ``modify_query_times``:

    >>> t = data.copy()
    >>> t['S_BEFORE'] = t['_START_TIME']
    >>> t['E_BEFORE'] = t['_END_TIME']
    >>> t = t.modify_query_times(start=t['_START_TIME'] + otp.Milli(1),
    ...                          end=t['_END_TIME'] - otp.Milli(1))
    >>> t['S_AFTER'] = t['_START_TIME']
    >>> t['E_AFTER'] = t['_END_TIME']
    >>> otp.run(t, start=start, end=end)
            Time  PRICE  SIZE                S_BEFORE                E_BEFORE    S_AFTER                 E_AFTER
    0 2022-03-02    1.1   101 2022-03-02 00:00:00.001 2022-03-02 00:00:00.002 2022-03-02 2022-03-02 00:00:00.003

    You can decrease time interval without problems:

    >>> t = data.modify_query_times(start=data['_START_TIME'] + otp.Milli(1),
    ...                             end=data['_END_TIME'] - otp.Milli(1))
    >>> otp.run(t, start=start, end=end)
            Time  PRICE  SIZE
    0 2022-03-02    1.1   101

    Note that the timestamp of the tick was changed with default expression.
    In this case we can output original timestamps,
    because they fall into original time range:

    >>> t = data.modify_query_times(start=data['_START_TIME'] + otp.Milli(1),
    ...                             end=data['_END_TIME'] - otp.Milli(1),
    ...                             output_timestamp=data['TIMESTAMP'])
    >>> otp.run(t, start=start, end=end)
                         Time  PRICE  SIZE
    0 2022-03-02 00:00:00.001    1.1   101

    But it will not work if new time range is wider than original:

    >>> t = data.modify_query_times(start=data['_START_TIME'] - otp.Milli(1),
    ...                             output_timestamp=data['TIMESTAMP'])
    >>> otp.run(t, start=start + otp.Milli(1), end=end + otp.Milli(1)) # doctest: +ELLIPSIS
    Traceback (most recent call last):
    Exception...timestamp is falling out of initial start/end time range...

    In this case default ``output_timestamp`` expression would work just fine:

    >>> t = data.modify_query_times(start=data['_START_TIME'] - otp.Milli(1))
    >>> otp.run(t, start=start + otp.Milli(1), end=end + otp.Milli(1))
                         Time  PRICE  SIZE
    0 2022-03-02 00:00:00.001    1.0   100
    1 2022-03-02 00:00:00.002    1.1   101
    2 2022-03-02 00:00:00.003    1.2   102

    But it doesn't work, for example, if end time has crossed the borders of original time range.
    In this case other ``output_timestamp`` expression must be specified:

    >>> t = data.modify_query_times(
    ...     start=data['_START_TIME'] - otp.Milli(2),
    ...     output_timestamp=otp.math.min(data['TIMESTAMP'] + otp.Milli(2), data['_END_TIME'])
    ... )
    >>> otp.run(t, start=start + otp.Milli(2), end=end)
                         Time  PRICE  SIZE
    0 2022-03-02 00:00:00.002    1.0   100
    1 2022-03-02 00:00:00.003    1.1   101
    2 2022-03-02 00:00:00.003    1.2   102

    Remember that ``start`` and ``end`` parameters can't depend on ticks:

    >>> t = data.copy()
    >>> t['X'] = 12345
    >>> t = t.modify_query_times(start=t['_START_TIME'] + t['X'] - t['X'],
    ...                          end=t['_END_TIME'] - otp.Milli(1))
    >>> otp.run(t, start=start, end=end) # doctest: +ELLIPSIS
    Traceback (most recent call last):
    Exception...parameter must not depend on ticks...

    Constant datetime values can be used as parameters too:

    >>> t = data.modify_query_times(start=start + otp.Milli(1),
    ...                             end=end - otp.Milli(1))
    >>> otp.run(t, start=start, end=end)
            Time  PRICE  SIZE
    0 2022-03-02    1.1   101

    Note that some graph patterns are not allowed when using this method.
    For example, modifying query times for a branch that will be merged later:

    >>> t1, t2 = data[data['PRICE'] > 1.3]
    >>> t2 = t2.modify_query_times(start=start + otp.Milli(1))
    >>> t = otp.merge([t1, t2])
    >>> otp.run(t, start=start, end=end) # doctest: +ELLIPSIS
    Traceback (most recent call last):
    Exception...Invalid graph...time bound to a node...an intermediate node in one of the cycles in graph...
    """
    start = ott.value2str(start) if start is not None else ''
    end = ott.value2str(end) if end is not None else ''
    output_timestamp = ott.value2str(output_timestamp) if output_timestamp is not None else ''
    self.sink(
        otq.ModifyQueryTimes(
            start_time=start,
            end_time=end,
            output_timestamp=output_timestamp,
            propagate_heartbeats=propagate_heartbeats,
        )
    )
    return self


def time_interval_shift(self: 'Source', shift, inplace=False) -> Optional['Source']:
    """
    Shifting time interval for a source.

    The whole data flow is shifted all the way up to the source of the graph.

    The start and end times of the query will be changed for all operations before this method,
    and will stay the same after this method.

    WARNING: The ticks' timestamps *are changed* automatically so they fit into original time range.

    You will get different set of ticks from the database, but the timestamps of the ticks
    from that database will not be the same as in the database.

    They need to be changed so they fit into the original query time range.
    See details in :py:meth:`onetick.py.Source.modify_query_times`.

    Parameters
    ----------
    shift: int or :ref:`datetime offset<api/datetime/offsets/root:datetime offsets>`
        Offset to shift the whole time interval.
        Can be positive or negative.
        Positive value moves time interval into the future, negative -- to the past.
        int values are interpreted as milliseconds.

        Timestamps of the ticks will be changed so they fit into the original query time range
        by subtracting ``shift`` from each timestamp.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise method returns a new modified object.

    Returns
    -------
    :class:`Source` or ``None``

    See also
    --------
    | :py:meth:`onetick.py.Source.modify_query_times`
    | :py:meth:`onetick.py.Source.time_interval_change`

    Examples
    --------

    --> Also see use-case using :py:meth:`time_interval_shift` for calculating
    `Markouts <../../static/getting_started/use_cases.html#point-in-time-benchmarks-bbo-at-different-markouts>`_


    >>> start = otp.dt(2022, 3, 2)
    >>> end = otp.dt(2022, 3, 2) + otp.Milli(3)
    >>> data = otp.DataSource('US_COMP', symbols='AAPL', tick_type='TRD')

    Default data:

    >>> otp.run(data, start=start, end=end)
                         Time  PRICE  SIZE
    0 2022-03-02 00:00:00.000    1.0   100
    1 2022-03-02 00:00:00.001    1.1   101
    2 2022-03-02 00:00:00.002    1.2   102

    Get window for a third tick:

    >>> otp.run(data, start=start + otp.Milli(2), end=start + otp.Milli(3))
                         Time  PRICE  SIZE
    0 2022-03-02 00:00:00.002    1.2   102

    Shifting time window will result in different set of ticks,
    but the ticks will have their timestamps changed to fit into original time range.
    Let's shift time 2 milliseconds back and thus get the first tick:

    >>> t = data.time_interval_shift(shift=-otp.Milli(2))
    >>> otp.run(t, start=start + otp.Milli(2), end=start + otp.Milli(3))
                         Time  PRICE  SIZE
    0 2022-03-02 00:00:00.002    1.0   100

    Here we are querying empty time interval, but shifting one second back to get ticks.

    >>> t = data.time_interval_shift(shift=-otp.Second(1))
    >>> otp.run(t, start=start + otp.Second(1), end=end + otp.Second(1))
                         Time  PRICE  SIZE
    0 2022-03-02 00:00:01.000    1.0   100
    1 2022-03-02 00:00:01.001    1.1   101
    2 2022-03-02 00:00:01.002    1.2   102

    Note that tick generators
    :py:class:`otp.Tick <onetick.py.Tick>` and :py:func:`otp.Ticks <onetick.py.Ticks>`
    are not really affected by this method, they will have the same timestamps:

    >>> t = otp.Tick(A=1)
    >>> otp.run(t)
            Time  A
    0 2003-12-01  1

    >>> t = t.time_interval_shift(shift=otp.Second(1))
    >>> otp.run(t)
            Time  A
    0 2003-12-01  1
    """
    start = self['_START_TIME'] + shift
    end = self['_END_TIME'] + shift
    # change timestamps so they fit into original time range
    output_timestamp = self['TIMESTAMP'] - shift
    return self.modify_query_times(start=start, end=end, output_timestamp=output_timestamp, inplace=inplace)


def time_interval_change(self: 'Source', start_change=0, end_change=0, inplace=False) -> Optional['Source']:
    """
    Changing time interval by making it bigger or smaller.

    All timestamps of ticks that are crossing the border of original time range
    will be set to original start time or end time depending on their original time.

    Parameters
    ----------
    start_change: int or :ref:`datetime offset <api/datetime/offsets/root:datetime offsets>`
        Offset to shift start time.
        Can be positive or negative.
        Positive value moves start time into the future, negative -- to the past.
        int values are interpreted as milliseconds.
    end_change: int or :ref:`datetime offset <api/datetime/offsets/root:datetime offsets>`
        Offset to shift end time.
        Can be positive or negative.
        Positive value moves end time into the future, negative -- to the past.
        int values are interpreted as milliseconds.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise method returns a new modified object.

    Returns
    -------
    :class:`Source` or ``None``

    See also
    --------
    | :py:meth:`onetick.py.Source.modify_query_times`
    | :py:meth:`onetick.py.Source.time_interval_shift`

    Examples
    --------

    >>> start = otp.dt(2022, 3, 2)
    >>> end = otp.dt(2022, 3, 2) + otp.Milli(3)
    >>> data = otp.DataSource('US_COMP', symbols='AAPL', tick_type='TRD')

    By default, ``time_interval_change()`` does nothing:

    >>> t = data.time_interval_change()
    >>> otp.run(t, start=start, end=end)
                         Time  PRICE  SIZE
    0 2022-03-02 00:00:00.000    1.0   100
    1 2022-03-02 00:00:00.001    1.1   101
    2 2022-03-02 00:00:00.002    1.2   102

    Decreasing time range will not change ticks' timestamps:

    >>> t = data.time_interval_change(start_change=otp.Milli(1), end_change=-otp.Milli(1))
    >>> otp.run(t, start=start, end=end)
                         Time  PRICE  SIZE
    0 2022-03-02 00:00:00.001    1.1   101

    Increasing time range will change timestamps of the ticks that crossed the border.
    In this case first tick's timestamp will be set to original start time,
    and third tick's to original end time.

    >>> t = data.time_interval_change(start_change=-otp.Milli(1), end_change=otp.Milli(1))
    >>> otp.run(t, start=start + otp.Milli(1), end=start + otp.Milli(2))
                         Time  PRICE  SIZE
    0 2022-03-02 00:00:00.001    1.0   100
    1 2022-03-02 00:00:00.001    1.1   101
    2 2022-03-02 00:00:00.002    1.2   102

    Here we are querying empty time interval, but changing start time one second back to get ticks.

    >>> t = data.time_interval_change(start_change=-otp.Second(1))
    >>> otp.run(t, start=start + otp.Second(1), end=end + otp.Second(1))
                     Time  PRICE  SIZE
    0 2022-03-02 00:00:01    1.0   100
    1 2022-03-02 00:00:01    1.1   101
    2 2022-03-02 00:00:01    1.2   102
    """
    start = self['_START_TIME'] + start_change
    end = self['_END_TIME'] + end_change

    # change ticks' timestamps only if they are out of bounds
    output_timestamp = self['TIMESTAMP']
    output_timestamp = otp.math.min(output_timestamp, self['_END_TIME'])
    output_timestamp = otp.math.max(output_timestamp, self['_START_TIME'])

    return self.modify_query_times(start=start, end=end, output_timestamp=output_timestamp, inplace=inplace)
