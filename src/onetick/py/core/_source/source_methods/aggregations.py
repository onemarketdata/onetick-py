import warnings
from typing import TYPE_CHECKING, Tuple, Union

from onetick import py as otp
from onetick.py import aggregations
from onetick.py.aggregations._docs import (
    _all_fields_with_policy_doc,
    _boundary_tick_bucket_doc,
    _bucket_end_condition_doc,
    _bucket_interval_doc,
    _bucket_time_doc,
    _bucket_units_doc,
    _end_condition_per_group_doc,
    _group_by_doc,
    _groups_to_display_doc,
    _running_doc,
    copy_method,
)
from onetick.py.docs.utils import docstring, param_doc
from onetick.py.otq import otq

from .misc import inplace_operation

if TYPE_CHECKING:
    from onetick.py.core.source import Source


_agg_doc = param_doc(
    name='aggs',
    annotation=dict,
    str_annotation='dict of aggregations',
    desc="""
    aggregation dict:

    * key - output column name for regular aggregations, prefix for column names for tick and multi-column aggregations;
    * value - aggregation
    """,
)


@docstring(
    parameters=[
        _agg_doc,
        _running_doc,
        _all_fields_with_policy_doc,
        _bucket_interval_doc,
        _bucket_time_doc,
        _bucket_units_doc,
        _bucket_end_condition_doc,
        _end_condition_per_group_doc,
        _boundary_tick_bucket_doc,
        _group_by_doc,
        _groups_to_display_doc,
    ],
    add_self=True,
)
def agg(self: 'Source', aggs, *args, **kwargs) -> 'Source':
    """
    Applies composition of :ref:`otp.agg <aggregations_funcs>` aggregations

    See Also
    --------
    | :ref:`Aggregations <aggregations_funcs>`
    | **COMPUTE** OneTick event processor

    Returns
    -------
        :py:class:`~onetick.py.Source`

    Examples
    --------

    By default the whole data is aggregated:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.agg({'X_SUM': otp.agg.sum('X')})
    >>> otp.run(data)
            Time  X_SUM
    0 2003-12-04     10

    Multiple aggregations can be applied at the same time:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.agg({'X_SUM': otp.agg.sum('X'),
    ...                  'X_MEAN': otp.agg.average('X')})
    >>> otp.run(data)
            Time  X_SUM  X_MEAN
    0 2003-12-04     10     2.5

    Aggregation can be used in running mode:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.agg({'CUM_SUM': otp.agg.sum('X')}, running=True)
    >>> otp.run(data)
                         Time  CUM_SUM
    0 2003-12-01 00:00:00.000        1
    1 2003-12-01 00:00:00.001        3
    2 2003-12-01 00:00:00.002        6
    3 2003-12-01 00:00:00.003       10

    Aggregation can be split in buckets:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.agg({'X_SUM': otp.agg.sum('X')}, bucket_interval=2, bucket_units='ticks')
    >>> otp.run(data)
                         Time  X_SUM
    0 2003-12-01 00:00:00.001      3
    1 2003-12-01 00:00:00.003      7

    Running aggregation can be used with buckets too. In this case (all_fields=False and running=True) output ticks
    are created when a tick enters or leaves the sliding window (that's why for this example there are 8 output
    ticks for 4 input ticks):

    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[0, 1000, 1500, 3600])
    >>> data = data.agg(dict(X_MEAN=otp.agg.average("X"),
    ...                      X_STD=otp.agg.stddev("X")),
    ...                 running=True, bucket_interval=2)
    >>> otp.run(data)
                         Time  X_MEAN     X_STD
    0 2003-12-01 00:00:00.000     1.0  0.000000
    1 2003-12-01 00:00:01.000     1.5  0.500000
    2 2003-12-01 00:00:01.500     2.0  0.816497
    3 2003-12-01 00:00:02.000     2.5  0.500000
    4 2003-12-01 00:00:03.000     3.0  0.000000
    5 2003-12-01 00:00:03.500     NaN       NaN
    6 2003-12-01 00:00:03.600     4.0  0.000000
    7 2003-12-01 00:00:05.600     NaN       NaN

    By default, if you run aggregation with buckets and group_by, then a bucket will be taken first, and after that
    grouping and aggregation will be performed:

    >>> ticks = otp.Ticks(
    ...     {
    ...         'QTY': [10, 2, 30, 4, 50],
    ...         'TRADER': ['A', 'B', 'A', 'B', 'A']
    ...     }
    ... )
    >>>
    >>> ticks = ticks.agg(
    ...     {'SUM_QTY': otp.agg.sum('QTY')}, group_by='TRADER',
    ...     bucket_interval=3, bucket_units='ticks',
    ...     running=True, all_fields=True,
    ... )
    >>>
    >>> otp.run(ticks)
                         Time  TRADER  QTY  SUM_QTY
    0 2003-12-01 00:00:00.000       A   10       10
    1 2003-12-01 00:00:00.001       B    2        2
    2 2003-12-01 00:00:00.002       A   30       40
    3 2003-12-01 00:00:00.003       B    4        6
    4 2003-12-01 00:00:00.004       A   50       80

    In the row with index 4, the result of summing up the trades for trader "A" turned out to be 80, instead of 90.
    We first took a bucket of 3 ticks, then within it took the group with trader "A" (2 ticks remained) and
    added up the volumes.
    To prevent this behaviour, and group ticks first, set parameter ``end_condition_per_group`` to True:

    >>> ticks = otp.Ticks(
    ...     {
    ...         'QTY': [10, 2, 30, 4, 50],
    ...         'TRADER': ['A', 'B', 'A', 'B', 'A']
    ...     }
    ... )
    >>>
    >>> ticks = ticks.agg(
    ...     {'SUM_QTY': otp.agg.sum('QTY')}, group_by='TRADER',
    ...     bucket_interval=3, bucket_units='ticks',
    ...     running=True, all_fields=True,
    ...     end_condition_per_group=True,
    ... )
    >>>
    >>> otp.run(ticks)
                         Time  TRADER  QTY  SUM_QTY
    0 2003-12-01 00:00:00.000       A   10       10
    1 2003-12-01 00:00:00.001       B    2        2
    2 2003-12-01 00:00:00.002       A   30       40
    3 2003-12-01 00:00:00.003       B    4        6
    4 2003-12-01 00:00:00.004       A   50       90

    Tick aggregations and aggregations, which return more than one output column, could be also used.
    Dict key set for an aggregation in ``aggs`` parameter will be used as prefix
    for each output column of this aggregation.

    >>> data = otp.Ticks(X=[1, 2, 3, 4], Y=[10, 20, 30, 40])
    >>> data = data.agg({'X_SUM': otp.agg.sum('X'), 'X_FIRST': otp.agg.first_tick()})
    >>> otp.run(data)
            Time  X_FIRST.X  X_FIRST.Y  X_SUM
    0 2003-12-04          1         10     10

    These aggregations can be split in buckets too:

    >>> data = otp.Ticks(X=[1, 2, 3, 4], Y=[10, 20, 30, 40])
    >>> data = data.agg(
    ...     {'X_SUM': otp.agg.sum('X'), 'X_FIRST': otp.agg.first_tick()},
    ...     bucket_interval=2, bucket_units='ticks',
    ... )
    >>> otp.run(data)
                         Time  X_FIRST.X  X_FIRST.Y  X_SUM
    0 2003-12-01 00:00:00.001          1         10      3
    1 2003-12-01 00:00:00.003          3         30      7

    If all_fields=True an output tick is generated only for arrival events, but all attributes from the input tick
    causing an arrival event are copied over to the output tick and the aggregation is added as another attribute:

    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[0, 1000, 1500, 3600])
    >>> data = data.agg(dict(X_MEAN=otp.agg.average("X"),
    ...                      X_STD=otp.agg.stddev("X")),
    ...                 all_fields=True, running=True)
    >>> otp.run(data)
                         Time  X  X_MEAN     X_STD
    0 2003-12-01 00:00:00.000  1     1.0  0.000000
    1 2003-12-01 00:00:01.000  2     1.5  0.500000
    2 2003-12-01 00:00:01.500  3     2.0  0.816497
    3 2003-12-01 00:00:03.600  4     2.5  1.118034

    ``all_fields`` parameter can be used when there is need to have all original fields in the output:

    >>> ticks = otp.Ticks(X=[3, 4, 1, 2])
    >>> data = ticks.agg(dict(X_MEAN=otp.agg.average("X"),
    ...                       X_STD=otp.agg.stddev("X")),
    ...                  all_fields=True)
    >>> otp.run(data)
            Time  X  X_MEAN     X_STD
    0 2003-12-04  3     2.5  1.118034

    There are different politics for ``all_fields`` parameter:

    >>> data = ticks.agg(dict(X_MEAN=otp.agg.average("X"),
    ...                       X_STD=otp.agg.stddev("X")),
    ...                  all_fields="last")
    >>> otp.run(data)
            Time  X  X_MEAN     X_STD
    0 2003-12-04  2     2.5  1.118034

    For low/high policies the field selected as input is set this way:

    >>> data = ticks.agg(dict(X_MEAN=otp.agg.average("X"),
    ...                       X_STD=otp.agg.stddev("X")),
    ...                  all_fields=otp.agg.low_tick(data["X"]))
    >>> otp.run(data)
            Time  X  X_MEAN     X_STD
    0 2003-12-04  1     2.5  1.118034

    Example of using 'flexible' buckets. Here every bucket consists of consecutive upticks.

    >>> trades = otp.Ticks(PRICE=[194.65, 194.65, 194.65, 194.75, 194.75, 194.51, 194.70, 194.71, 194.75, 194.71])
    >>> trades = trades.agg({'COUNT': otp.agg.count(),
    ...                     'FIRST_TIME': otp.agg.first('Time'),
    ...                     'LAST_TIME': otp.agg.last('Time')},
    ...                     bucket_units='flexible',
    ...                     bucket_end_condition=trades['PRICE'] < trades['PRICE'][-1])
    >>> otp.run(trades)
                         Time  COUNT              FIRST_TIME               LAST_TIME
    0 2003-12-01 00:00:00.005      5 2003-12-01 00:00:00.000 2003-12-01 00:00:00.004
    1 2003-12-01 00:00:00.009      4 2003-12-01 00:00:00.005 2003-12-01 00:00:00.008
    2 2003-12-04 00:00:00.000      1 2003-12-01 00:00:00.009 2003-12-01 00:00:00.009
    """

    aggs = aggs.copy()
    result = self.copy()

    what_to_aggregate = aggregations.compute(
        *args,
        **kwargs,
    )

    for name, ag in aggs.items():
        what_to_aggregate.add(name, ag)

    result = what_to_aggregate.apply(result)
    result._add_table()

    return result


# Aggregations copy
# we need this functions to store and collect documentation
# copy_method decorator will
#       set docstring (will compare docstring of donor function and method docstring)
#       apply same signature from donor function + self
#       for mimic=True will apply agg function as is
@copy_method(aggregations.functions.high_tick)
def high(self, *args, **kwargs):
    """
    Examples
    --------
    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[0, 1000, 1500, 3000])
    >>> data = data.high(['X'], 2)     # OTdirective: snippet-name: Aggregations.high tick;
    >>> otp.run(data)
                         Time  X
    0 2003-12-01 00:00:01.500  3
    1 2003-12-01 00:00:03.000  4
    """
    pass


@copy_method(aggregations.functions.low_tick)
def low(self, *args, **kwargs):
    """
    Examples
    --------
    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[0, 1000, 1500, 3000])
    >>> data = data.low(['X'],2)      # OTdirective: snippet-name: Aggregations.low tick;
    >>> otp.run(data)
                     Time  X
    0 2003-12-01 00:00:00  1
    1 2003-12-01 00:00:01  2
    """
    pass


@copy_method(aggregations.functions.first_tick)
def first(self, *args, **kwargs):
    """
    Examples
    --------

    Get first tick:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.first()    # OTdirective: snippet-name: Aggregations.first;
    >>> otp.run(data)
            Time  X
    0 2003-12-01  1

    Get first tick each day:

    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[otp.Day(0), otp.Day(0), otp.Day(2), otp.Day(2)])
    >>> data = data.first(bucket_interval=1, bucket_units='days', bucket_time='start')
    >>> otp.run(data)
            Time  X
    0 2003-12-01  1
    1 2003-12-03  3

    Get first tick each day and set tick value for empty buckets:

    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[otp.Day(0), otp.Day(0), otp.Day(2), otp.Day(2)])
    >>> data = data.first(bucket_interval=1, bucket_units='days', bucket_time='start', default_tick={'X': -1})
    >>> otp.run(data)
            Time   X
    0 2003-12-01   1
    1 2003-12-02  -1
    2 2003-12-03   3
    """
    pass


@copy_method(aggregations.functions.last_tick)
def last(self, *args, **kwargs):
    """
    Examples
    --------

    Get last tick:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.last()     # OTdirective: snippet-name: Aggregations.last;
    >>> otp.run(data)
                         Time  X
    0 2003-12-01 00:00:00.003  4

    Get last tick each day:

    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[otp.Day(0), otp.Day(0), otp.Day(2), otp.Day(2)])
    >>> data = data.last(bucket_interval=1, bucket_units='days', bucket_time='start')
    >>> otp.run(data)
            Time  X
    0 2003-12-01  2
    1 2003-12-03  4

    Get last tick each day and set tick value for empty buckets:

    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[otp.Day(0), otp.Day(0), otp.Day(2), otp.Day(2)])
    >>> data = data.last(bucket_interval=1, bucket_units='days', bucket_time='start', default_tick={'X': -1})
    >>> otp.run(data)
            Time   X
    0 2003-12-01   2
    1 2003-12-02  -1
    2 2003-12-03   4
    """
    pass


@copy_method(aggregations.functions.distinct, mimic=False)
def distinct(self: 'Source', *args, **kwargs):
    """
    Examples
    --------
    >>> data = otp.Ticks(dict(x=[1, 3, 1, 5, 3]))
    >>> data = data.distinct('x')   # OTdirective: snippet-name: Aggregations.distinct;
    >>> otp.run(data)
            Time  x
    0 2003-12-04  1
    1 2003-12-04  3
    2 2003-12-04  5
    """
    if 'bucket_interval_units' in kwargs:
        kwargs['bucket_units'] = kwargs.pop('bucket_interval_units')
    aggr = aggregations.functions.distinct(*args, **kwargs)
    return aggr.apply(self)


# mimic=False for backward compatibility
@copy_method(aggregations.functions.high_time, mimic=False, drop_examples=True)
def high_time(self: 'Source', *args, **kwargs):
    """
    Returns timestamp of tick with the highest value of input field

        .. deprecated:: 1.14.5

        Use :py:func:`.high_time` instead

    See Also
    --------
    :py:func:`.high_time`

    """
    warnings.warn(
        f"{self.__class__.__name__}.high_time deprecated. Use otp.agg.high_time instead",
        FutureWarning,
        stacklevel=2,
    )
    aggr = aggregations.functions.high_time(*args, **kwargs)
    return aggr.apply(self, 'VALUE')


# mimic=False for backward compatibility
@copy_method(aggregations.functions.low_time, mimic=False, drop_examples=True)
def low_time(self: 'Source', *args, **kwargs):
    """
    Returns timestamp of tick with the lowest value of input field

        .. deprecated:: 1.14.5

        Use :py:func:`.low_time` instead

    See Also
    --------
    :py:func:`.low_time`

    """
    warnings.warn(
        f"{self.__class__.__name__}.low_time deprecated. Use otp.agg.low_time instead",
        FutureWarning,
        stacklevel=2,
    )
    aggr = aggregations.functions.low_time(*args, **kwargs)
    return aggr.apply(self, 'VALUE')


@copy_method(aggregations.functions.ob_snapshot)
def ob_snapshot(self, *args, **kwargs):
    """
    Examples
    --------
    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = data.ob_snapshot(max_levels=1)  # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  PRICE             UPDATE_TIME  SIZE  LEVEL  BUY_SELL_FLAG
    0 2003-12-04    2.0 2003-12-01 00:00:00.003     6      1              1
    1 2003-12-04    5.0 2003-12-01 00:00:00.004     7      1              0
    """
    pass


@copy_method(aggregations.functions.ob_snapshot_wide)
def ob_snapshot_wide(self, *args, **kwargs):
    """
    Examples
    --------
    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = data.ob_snapshot_wide(max_levels=1)  # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  BID_PRICE         BID_UPDATE_TIME  BID_SIZE  ASK_PRICE         ASK_UPDATE_TIME  ASK_SIZE  LEVEL
    0 2003-12-03        5.0 2003-12-01 00:00:00.004         7        2.0 2003-12-01 00:00:00.003         6      1
    """
    pass


@copy_method(aggregations.functions.ob_snapshot_flat)
def ob_snapshot_flat(self, *args, **kwargs):
    """
    Examples
    --------
    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = data.ob_snapshot_flat(max_levels=1)  # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  BID_PRICE1        BID_UPDATE_TIME1  BID_SIZE1  ASK_PRICE1        ASK_UPDATE_TIME1  ASK_SIZE1
    0 2003-12-03         5.0 2003-12-01 00:00:00.004          7         2.0 2003-12-01 00:00:00.003          6
    """
    pass


@copy_method(aggregations.functions.ob_summary)
def ob_summary(self, *args, **kwargs):
    """
    Examples
    --------
    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = data.ob_summary(max_levels=1) # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  BID_PRICE  BID_SIZE  BID_VWAP  BEST_BID_PRICE  WORST_BID_SIZE  NUM_BID_LEVELS  ASK_SIZE\
                ASK_VWAP  BEST_ASK_PRICE  WORST_ASK_PRICE  NUM_ASK_LEVELS
    0 2003-12-04        NaN         7       5.0             5.0             NaN               1         6\
            2.0             2.0              2.0               1
    """
    pass


@copy_method(aggregations.functions.ob_size)
def ob_size(self, *args, **kwargs):
    """
    Examples
    --------
    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = data.ob_size(max_levels=10) # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  ASK_VALUE  BID_VALUE
    0 2003-12-01      84800      64500
    """
    pass


@copy_method(aggregations.functions.ob_vwap)
def ob_vwap(self, *args, **kwargs):
    """
    Examples
    --------
    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = data.ob_vwap(max_levels=10) # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  ASK_VALUE  BID_VALUE
    0 2003-12-01     23.313   23.20848
    """
    pass


@copy_method(aggregations.functions.ob_num_levels)
def ob_num_levels(self, *args, **kwargs):
    """
    Examples
    --------
    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = data.ob_num_levels() # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  ASK_VALUE  BID_VALUE
    0 2003-12-01        248         67
    """
    pass


@copy_method(aggregations.functions.ranking)
def ranking(self: 'Source', *args, **kwargs):
    # method implementation is copied by decorator
    pass


@copy_method(aggregations.functions.percentile)
def percentile(self: 'Source', *args, **kwargs):
    # method implementation is copied by decorator
    pass


@copy_method(aggregations.functions.find_value_for_percentile)
def find_value_for_percentile(self: 'Source', *args, **kwargs):
    # method implementation is copied by decorator
    pass


@copy_method(aggregations.functions.exp_w_average)
def exp_w_average(self: 'Source', *args, **kwargs):
    # method implementation is copied by decorator
    pass


@copy_method(aggregations.functions.exp_tw_average)
def exp_tw_average(self: 'Source', *args, **kwargs):
    # method implementation is copied by decorator
    pass


@copy_method(aggregations.functions.standardized_moment)
def standardized_moment(self: 'Source', *args, **kwargs):
    # method implementation is copied by decorator
    pass


@copy_method(aggregations.functions.portfolio_price)
def portfolio_price(self: 'Source', *args, **kwargs):
    # method implementation is copied by decorator
    pass


@copy_method(aggregations.functions.multi_portfolio_price)
def multi_portfolio_price(self: 'Source', *args, **kwargs):
    # method implementation is copied by decorator
    pass


@copy_method(aggregations.functions.return_ep)
def return_ep(self: 'Source', *args, **kwargs):
    # method implementation is copied by decorator
    pass


@copy_method(aggregations.functions.implied_vol)
def implied_vol(self: 'Source', *args, **kwargs):
    # method implementation is copied by decorator
    pass


@copy_method(aggregations.functions.linear_regression)
def linear_regression(self: 'Source', *args, **kwargs):
    # method implementation is copied by decorator
    pass


@inplace_operation
def process_by_group(
    self: 'Source', process_source_func, group_by=None, source_name=None, num_threads=None, inplace=False
) -> Union['Source', Tuple['Source', ...], None]:
    """
    Groups data by ``group_by`` and run ``process_source_func`` for each group and merge outputs for every group.
    Note ``process_source_func`` will be converted to Onetick object and passed to query,
    that means that python callable will be called only once.

    Parameters
    ----------
    process_source_func: callable
        ``process_source_func`` should take :class:`Source` apply necessary logic and return it
        or tuple of :class:`Source` in this case all of them should have a common root that is the
        input :class:`Source`.
        The number of sources returned by this method is the same as the number of sources
        returned by ``process_source_func``.
    group_by: list
        A list of field names to group input ticks by.

        If group_by is None then no group_by fields are defined
        and logic of ``process_source_func`` is applied to all input ticks
        at once
    source_name: str
        A name for the source that represents all of group_by sources. Can be passed here or as a name
        of the inner sources; if passed by both ways, should be consistent
    num_threads: int
        If specified and not zero, turns on asynchronous processing mode
        and specifies number of threads to be used for processing input ticks.
        If this parameter is not specified or zero, then input ticks are processed synchronously.
    inplace: bool
        If True - nothing will be returned and changes will be applied to current query
        otherwise changes query will be returned.
        Error is raised if ``inplace`` is set to True
        and multiple sources returned by ``process_source_func``.

    Returns
    -------
    :class:`Source`, Tuple[:class:`Source`] or None:

    See also
    --------
    **GROUP_BY** OneTick event processor

    Examples
    --------

    >>> # OTdirective: snippet-name: Arrange.group.single output;
    >>> d = otp.Ticks(X=[1, 1, 2, 2],
    ...               Y=[1, 2, 3, 4])
    >>>
    >>> def func(source):
    ...     return source.first()
    >>>
    >>> res = d.process_by_group(func, group_by=['X'])
    >>> otp.run(res)[["X", "Y"]]
       X  Y
    0  1  1
    1  2  3

    Set asynchronous processing:

    >>> res = d.process_by_group(func, group_by=['X'], num_threads=2)
    >>> otp.run(res)[['X', 'Y']]
       X  Y
    0  1  1
    1  2  3

    Return multiple outputs, each with unique grouping logic:

    >>> d = otp.Ticks(X=[1, 1, 2, 2],
    ...               Y=[1, 2, 1, 3])
    >>>
    >>> def func(source):
    ...     source['Z'] = source['X']
    ...     source2 = source.copy()
    ...     source = source.first()
    ...     source2 = source2.last()
    ...     return source, source2
    >>> # OTdirective: snippet-name: Arrange.group.multiple output;
    >>> res1, res2 = d.process_by_group(func, group_by=['Y'])
    >>> df1 = otp.run(res1)
    >>> df2 = otp.run(res2)
    >>> df1[['X', 'Y', 'Z']]
       X  Y  Z
    0  1  1  1
    1  1  2  1
    2  2  3  2
    >>> df2[['X', 'Y', 'Z']]    # OTdirective: skip-snippet:;
       X  Y  Z
    0  1  2  1
    1  2  1  2
    2  2  3  2
    """

    if group_by is None:
        group_by = []

    if inplace:
        main_source = self
    else:
        main_source = self.copy()

    input_schema = main_source.columns(skip_meta_fields=True)
    for field in group_by:
        if field not in input_schema:
            raise ValueError(f"Group by field name {field} not present in input source schema")

    process_source_root = otp.DataSource(tick_type="ANY", schema_policy="manual", schema=input_schema)
    if source_name:
        process_source_root.set_name(source_name)
    process_sources = process_source_func(process_source_root)

    if isinstance(process_sources, otp.Source):
        # returned one source
        process_sources = [process_sources]
    elif len(process_sources) == 1:
        # returned one source as an iterable
        pass
    else:
        # returned multiple sources
        if inplace:
            raise ValueError("Cannot use inplace=True with multi-source processing function!")

    num_source = 0
    for process_source in process_sources:
        output_schema = process_source.columns(skip_meta_fields=True)

        if process_source.get_name():
            if not process_source_root.get_name():
                process_source_root.set_name(process_source.get_name())
            if process_source_root.get_name() != process_source.get_name():
                warnings.warn(
                    "Different strings passed as names for the root source used in "
                    f"process_by_group: '{process_source.get_name()}' "
                    f"and '{process_source_root.get_name()}'"
                )

        # removing key fields from output schema since they will be
        # added by the GROUP_BY EP
        process_source.drop([field for field in group_by if field in output_schema], inplace=True)
        process_source.sink(otq.Passthrough().node_name(f"OUT_{num_source}"))
        process_source_root.node().add_rules(process_source.node().copy_rules())
        main_source._merge_tmp_otq(process_source)
        num_source += 1

    query_name = process_source_root._store_in_tmp_otq(
        main_source._tmp_otq, operation_suffix="group_by", add_passthrough=False,
        # set default symbol, even if it's not set by user, symbol's value doesn't matter in this case
        symbols=otp.config.get('default_symbol', 'ANY')
    )
    process_path = f'THIS::{query_name}'
    num_outputs = len(process_sources)

    # we shouldn't set named outputs if GROUP_BY EP has only one output due to onetick behaviour
    if num_outputs == 1:
        outputs = ""
    else:
        outputs = ",".join([f"OUT_{i}" for i in range(0, num_outputs)])

    kwargs = {}
    if num_threads is not None:
        if num_threads < 0:
            raise ValueError("Parameter 'num_threads' can't be negative.")
        kwargs['num_threads'] = num_threads

    main_source.sink(otq.GroupBy(key_fields=",".join(group_by), query_name=process_path, outputs=outputs, **kwargs))

    output_sources = []
    for num_output in range(0, num_outputs):
        if num_outputs == 1 and inplace:
            output_source = main_source
        else:
            output_source = main_source.copy()

        if num_outputs > 1:
            output_source.node().out_pin(f"OUT_{num_output}")

        # setting schema after processing
        output_schema = process_sources[num_output].columns(skip_meta_fields=True)
        for field in group_by:
            output_schema[field] = input_schema[field]
        for field, field_type in output_schema.items():
            output_source.schema[field] = field_type
        output_source = output_source[list(output_schema)]
        output_source._merge_tmp_otq(main_source)
        output_sources.append(output_source)

    if num_outputs == 1:
        return output_sources[0]
    else:
        return tuple(output_sources)
