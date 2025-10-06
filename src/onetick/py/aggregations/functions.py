from typing import Union

import onetick.py as otp
from onetick.py.core.column import _Column
from .compute import Compute
from .high_low import Max, Min, HighTick, LowTick, HighTime, LowTime
from .other import (First, Last, FirstTime, LastTime, Count, Vwap, FirstTick,
                    LastTick, Distinct, Sum, Average, StdDev, TimeWeightedAvg,
                    Median, Correlation, OptionPrice, Ranking, Variance,
                    Percentile, FindValueForPercentile, ExpWAverage, ExpTwAverage,
                    StandardizedMoment, PortfolioPrice, MultiPortfolioPrice, Return, ImpliedVol,
                    LinearRegression)
from .order_book import (ObSnapshot, OB_SNAPSHOT_DOC_PARAMS,
                         ObSnapshotWide, OB_SNAPSHOT_WIDE_DOC_PARAMS,
                         ObSnapshotFlat, OB_SNAPSHOT_FLAT_DOC_PARAMS,
                         ObSummary, OB_SUMMARY_DOC_PARAMS,
                         ObSize, OB_SIZE_DOC_PARAMS,
                         ObVwap, OB_VWAP_DOC_PARAMS,
                         ObNumLevels, OB_NUM_LEVELS_DOC_PARAMS)
from .generic import Generic
from ._docs import (_column_doc,
                    _running_doc,
                    _all_fields_doc,
                    _bucket_interval_doc,
                    _bucket_time_doc,
                    _bucket_units_doc,
                    _bucket_end_condition_doc,
                    _end_condition_per_group_doc,
                    _boundary_tick_bucket_doc,
                    _group_by_doc,
                    _groups_to_display_doc,
                    _n_doc,
                    _keep_timestamp_doc,
                    _time_series_type_doc,
                    _time_series_type_w_doc,
                    _selection_doc,
                    _query_fun_doc,
                    _bucket_delimiter_doc,
                    _biased_doc,
                    _large_ints_doc,
                    _null_int_val_doc,
                    _skip_tick_if_doc,
                    _default_tick_doc,
                    _decay_doc,
                    _decay_value_type_doc,
                    _decay_value_type_hl_doc,
                    _degree_doc,
                    _weight_field_name_doc,
                    _weight_multiplier_field_name_doc,
                    _portfolio_side_doc,
                    _weight_type_doc,
                    _symbols_doc,
                    _portfolios_query_doc,
                    _portfolios_query_params_doc,
                    _portfolio_value_field_name_doc,
                    _columns_portfolio_doc,
                    _interest_rate_doc,
                    _price_field_doc,
                    _option_price_field_doc,
                    _method_doc,
                    _precision_doc,
                    _value_for_non_converge_doc,
                    _option_type_field_doc,
                    _strike_price_field_doc,
                    _days_in_year_doc,
                    _days_till_expiration_field_doc,
                    _expiration_date_field_doc)
from onetick.py.docs.utils import docstring, param_doc

# This module mostly focused on providing annotations and documentation for end user
# If you are looking for implementation check return object of each function


@docstring(parameters=[_running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc, _bucket_units_doc,
                       _bucket_end_condition_doc, _end_condition_per_group_doc, _boundary_tick_bucket_doc,
                       _group_by_doc, _groups_to_display_doc])
def compute(*args, **kwargs):
    """
    Generate object that collects aggregations

    See also
    --------
    **COMPUTE** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[0, 1000, 1500, 3000])
    >>> c = otp.agg.compute(running=True, bucket_interval=2)
    >>> c.add('X_MEAN', otp.agg.average("X"))
    >>> c.add('X_STD', otp.agg.stddev("X"))
    >>> data = c.apply(data)
    >>> otp.run(data)
                         Time  X_MEAN     X_STD
    0 2003-12-01 00:00:00.000     1.0  0.000000
    1 2003-12-01 00:00:01.000     1.5  0.500000
    2 2003-12-01 00:00:01.500     2.0  0.816497
    3 2003-12-01 00:00:02.000     2.5  0.500000
    4 2003-12-01 00:00:03.000     3.5  0.500000
    5 2003-12-01 00:00:03.500     4.0  0.000000
    6 2003-12-01 00:00:05.000     NaN       NaN

    """
    return Compute(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _time_series_type_doc, _large_ints_doc,
                       _null_int_val_doc])
def max(*args, **kwargs):
    """
    Return maximum value of input ``column``

    See also
    --------
    **HIGH** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.agg({'RESULT': otp.agg.max('X')})  # OTdirective: snippet-name: Aggregations.max;
    >>> otp.run(data)
            Time  RESULT
    0 2003-12-04       4
    """
    return Max(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _time_series_type_doc, _large_ints_doc,
                       _null_int_val_doc])
def min(*args, **kwargs):
    """
    Return minimum value of input ``column``

    See also
    --------
    **LOW** OneTick event processor

    Examples
    --------

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.agg({'RESULT': otp.agg.min('X')})     # OTdirective: snippet-name: Aggregations.min;
    >>> otp.run(data)
            Time  RESULT
    0 2003-12-04       1
    """
    return Min(*args, **kwargs)


@docstring(parameters=[_column_doc, _n_doc, _running_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _keep_timestamp_doc, _selection_doc,
                       _time_series_type_doc])
def high_tick(*args, **kwargs):
    """
    Select ``n`` ticks with the highest values in the ``column`` field

    See also
    --------
    **HIGH_TICK** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[0, 1000, 1500, 3000])
    >>> agg = otp.agg.high_tick('X', 2)
    >>> data = agg.apply(data)
    >>> otp.run(data)
                         Time  X
    0 2003-12-01 00:00:01.500  3
    1 2003-12-01 00:00:03.000  4
    """
    return HighTick(*args, **kwargs)


@docstring(parameters=[_column_doc, _n_doc, _running_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _keep_timestamp_doc, _selection_doc,
                       _time_series_type_doc])
def low_tick(*args, **kwargs):
    """
    Select ``n`` ticks with the lowest values in the ``column`` field

    See also
    --------
    **LOW_TICK** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[0, 1000, 1500, 3000])
    >>> agg = otp.agg.low_tick('X', 2)
    >>> data = agg.apply(data)
    >>> otp.run(data)
                     Time  X
    0 2003-12-01 00:00:00  1
    1 2003-12-01 00:00:01  2
    """
    return LowTick(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _selection_doc, _time_series_type_doc])
def high_time(*args, **kwargs):
    """
    Returns timestamp of tick with highest value of input field

    See also
    --------
    **HIGH_TIME** OneTick event processor

    Examples
    --------

    >>> data = otp.Ticks(X=[1, 2, 4, 3], offset=[0, 1000, 1500, 3000])
    >>> data = data.agg({'RESULT': otp.agg.high_time(['X'])})    # OTdirective: snippet-name: Aggregations.high time;
    >>> otp.run(data)
            Time                  RESULT
    0 2003-12-04 2003-12-01 00:00:01.500
    """
    return HighTime(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _selection_doc, _time_series_type_doc])
def low_time(*args, **kwargs):
    """
    Returns timestamp of tick with lowest value of input field

    See also
    --------
    **LOW_TIME** OneTick event processor

    Examples
    --------

    >>> data = otp.Ticks(X=[2, 1, 3, 4], offset=[0, 1000, 1500, 3000])
    >>> data = data.agg({'RESULT': otp.agg.low_time(['X'])})     # OTdirective: snippet-name: Aggregations.low time;
    >>> otp.run(data)
            Time              RESULT
    0 2003-12-04 2003-12-01 00:00:01
    """
    return LowTime(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _large_ints_doc, _null_int_val_doc,
                       _skip_tick_if_doc, _time_series_type_doc])
def first(*args, **kwargs):
    """
    Return first value of input field

    See also
    --------
    **FIRST** OneTick event processor

    Examples
    --------

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> agg = otp.agg.first('X')
    >>> data = agg.apply(data)
    >>> otp.run(data)
            Time  X
    0 2003-12-04  1
    """
    return First(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _large_ints_doc, _null_int_val_doc,
                       _skip_tick_if_doc, _time_series_type_doc])
def last(*args, **kwargs):
    """
    Return last value of input field

    See also
    --------
    **LAST** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> agg = otp.agg.last('X')
    >>> data = agg.apply(data)
    >>> otp.run(data)
            Time  X
    0 2003-12-04  4
    """
    return Last(*args, **kwargs)


@docstring(parameters=[_running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _time_series_type_doc])
def first_time(*args, **kwargs):
    """
    Return timestamp of first tick

    See also
    --------
    **FIRST_TIME** OneTick event processor

    Examples
    --------

    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[0, 1000, 1500, 3000])
    >>> data = data.agg({'RESULT': otp.agg.first_time()})   # OTdirective: snippet-name: Aggregations.first time;
    >>> otp.run(data)
            Time     RESULT
    0 2003-12-04 2003-12-01
    """
    return FirstTime(*args, **kwargs)


@docstring(parameters=[_running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _time_series_type_doc])
def last_time(*args, **kwargs):
    """
    Return timestamp of last tick

    See also
    --------
    **LAST_TIME** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[0, 1000, 1500, 3000])
    >>> data = data.agg({'RESULT': otp.agg.last_time()})   # OTdirective: snippet-name: Aggregations.last time;
    >>> otp.run(data)
            Time              RESULT
    0 2003-12-04 2003-12-01 00:00:03
    """
    return LastTime(*args, **kwargs)


@docstring(parameters=[_running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc, _bucket_units_doc,
                       _bucket_end_condition_doc, _end_condition_per_group_doc, _boundary_tick_bucket_doc,
                       _group_by_doc, _groups_to_display_doc])
def count(*args, **kwargs):
    """
    Returns number of ticks

    See also
    --------
    **NUM_TICKS** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.agg({'RESULT': otp.agg.count()})   # OTdirective: snippet-name: Aggregations.count;
    >>> otp.run(data)
            Time  RESULT
    0 2003-12-04       4
    """
    return Count(*args, **kwargs)


_price_doc = param_doc(name='price_column',
                       str_annotation='str or Column',
                       desc='price column for vwap',
                       annotation=Union[str, _Column])

_size_doc = param_doc(name='size_column',
                      str_annotation='str or Column',
                      desc='size column for vwap',
                      annotation=Union[str, _Column])


@docstring(parameters=[_price_doc, _size_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc])
def vwap(*args, **kwargs):
    """
    Returns volume weighted average price

    See also
    --------
    **VWAP** OneTick event processor

    Examples
    --------
    >>> # OTdirective: snippet-name: Aggregations.vwap;
    >>> data = otp.Ticks(P=[1, 2, 3, 4], S=[10, 20, 30, 40])
    >>> data = data.agg({'RESULT': otp.agg.vwap('P','S')})
    >>> otp.run(data)
            Time  RESULT
    0 2003-12-04     3.0
    """
    return Vwap(*args, **kwargs)


@docstring(parameters=[_running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc])
def correlation(*args, **kwargs):
    """
    Returns Pearson correlation coefficient value between two numeric fields.

    Parameters
    ----------
    column_name_1: str
        The name of the field in the input stream whose value is to be used by the aggregation
    column_name_2: str
        The name of the field in the input stream whose value is to be used by the aggregation

    See also
    --------
    **CORRELATION** OneTick event processor

    Examples
    --------
    >>> # OTdirective: snippet-name: Aggregations.correlation;
    >>> data = otp.Ticks(P=[1, 2, 3, 4], S=[10, 20, 30, 40])
    >>> data = data.agg({'RESULT': otp.agg.correlation('P','S')})
    >>> otp.run(data)
            Time  RESULT
    0 2003-12-04     1.0

    >>> data = otp.Ticks(P=[1, 2, 3, 4], S=[40, 30, 20, 10])
    >>> data = otp.agg.correlation('P', 'S').apply(data)
    >>> otp.run(data)
            Time  CORRELATION
    0 2003-12-04         -1.0

    >>> data = otp.Ticks(P=[1, 2, 3, 4], S=[40, 30, 10, 20])
    >>> data = otp.agg.correlation('P', 'S', bucket_units='ticks', bucket_interval=2).apply(data)
    >>> otp.run(data)
                         Time  CORRELATION
    0 2003-12-01 00:00:00.001         -1.0
    1 2003-12-01 00:00:00.003          1.0
    """
    return Correlation(*args, **kwargs)


@docstring(parameters=[_n_doc, _running_doc, _bucket_interval_doc, _bucket_time_doc, _bucket_units_doc,
                       _bucket_end_condition_doc, _end_condition_per_group_doc, _boundary_tick_bucket_doc,
                       _group_by_doc, _groups_to_display_doc, _keep_timestamp_doc, _default_tick_doc])
def first_tick(*args, **kwargs):
    """
    Select the first **n** ticks

    See also
    --------
    **FIRST_TICK** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(P=[1, 2, 3, 4], S=[10, 20, 30, 40])
    >>> agg = otp.agg.first_tick()
    >>> data = agg.apply(data)
    >>> otp.run(data)
            Time  P   S
    0 2003-12-01  1  10
    """
    return FirstTick(*args, **kwargs)


@docstring(parameters=[_n_doc, _running_doc, _bucket_interval_doc, _bucket_time_doc, _bucket_units_doc,
                       _bucket_end_condition_doc, _end_condition_per_group_doc, _boundary_tick_bucket_doc,
                       _group_by_doc, _groups_to_display_doc, _keep_timestamp_doc, _time_series_type_doc])
def last_tick(*args, **kwargs):
    """
    Select the last ``n`` ticks

    See also
    --------
    **LAST_TICK** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(P=[1, 2, 3, 4], S=[10, 20, 30, 40], offset=[0, 1000, 1500, 3000])
    >>> agg = otp.agg.last_tick()
    >>> data = agg.apply(data)
    >>> otp.run(data)
                     Time  P   S
    0 2003-12-01 00:00:03  4  40
    """
    return LastTick(*args, **kwargs)


@docstring(parameters=[_running_doc, _bucket_interval_doc, _bucket_time_doc, _bucket_units_doc,
                       _bucket_end_condition_doc, _boundary_tick_bucket_doc, _selection_doc])
def distinct(*args, **kwargs):
    """
    Outputs all distinct values for a specified set of key fields.

    Parameters
    ----------
    keys: str or list
        Specifies a list of tick attributes for which unique values are found. The ticks in the input time series
        must contain those attributes.
    key_attrs_only: bool
        If set to true, output ticks will contain only key fields. Otherwise, output ticks will contain all fields
        of an input tick in which a given distinct combination of key values was first encountered.

    See also
    --------
    **DISTINCT** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(dict(x=[1, 3, 1, 5, 3]))
    >>> d = otp.agg.distinct('x')
    >>> data = d.apply(data)
    >>> otp.run(data)
            Time  x
    0 2003-12-04  1
    1 2003-12-04  3
    2 2003-12-04  5
    """
    return Distinct(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc])
def sum(*args, **kwargs):
    r"""
    Implement sum aggregation

    See also
    --------
    **SUM** OneTick event processor

    Examples
    --------

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.agg({'RESULT': otp.agg.sum('X')})  # OTdirective: snippet-name: Aggregations.sum;
    >>> otp.run(data)
            Time  RESULT
    0 2003-12-04      10

    Note that **seconds** bucket unit doesn't take into account daylight-saving time of the timezone,
    so you may not get expected results when using, for example, 24 * 60 * 60 seconds as bucket interval.

    >>> data = otp.DataSource('CME', symbols=r'NQ\H23', tick_type='TRD')  # doctest: +SKIP
    >>> data = data.agg({'VOLUME': otp.agg.sum('SIZE')},  # doctest: +SKIP
    ...                 bucket_interval=24*60*60, bucket_units='seconds', bucket_time='start')
    >>> otp.run(data, start=otp.dt(2023, 3, 11), end=otp.dt(2023, 3, 15), timezone='EST5EDT')  # doctest: +SKIP
                     Time  VOLUME
    0 2023-03-11 00:00:00       0
    1 2023-03-12 00:00:00   66190
    2 2023-03-13 01:00:00  631750
    3 2023-03-14 01:00:00  345952

    In such case use **days** bucket unit instead:

    >>> data = otp.DataSource('CME', symbols=r'NQ\H23', tick_type='TRD')  # doctest: +SKIP
    >>> data = data.agg({'VOLUME': otp.agg.sum('SIZE')},  # doctest: +SKIP
    ...                 bucket_interval=1, bucket_units='days', bucket_time='start')
    >>> otp.run(data, start=otp.dt(2023, 3, 11), end=otp.dt(2023, 3, 15), timezone='EST5EDT')  # doctest: +SKIP
            Time  VOLUME
    0 2023-03-11       0
    1 2023-03-12   62940
    2 2023-03-13  634172
    3 2023-03-14  346780
    """
    return Sum(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc])
def average(*args, **kwargs):
    """
    Implement average aggregation

    See also
    --------
    **AVERAGE** OneTick event processor

    Examples
    --------

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.agg({'RESULT': otp.agg.average('X')})   # OTdirective: snippet-name: Aggregations.average/mean;
    >>> otp.run(data)
            Time  RESULT
    0 2003-12-04     2.5
    """
    return Average(*args, **kwargs)


def mean(*args, **kwargs):
    """
    Implement average aggregation.

    See also
    --------
    | :func:`average`
    | **AVERAGE** OneTick event processor

    Examples
    --------

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.agg({'RESULT': otp.agg.mean('X')})
    >>> otp.run(data)
            Time  RESULT
    0 2003-12-04     2.5
    """
    return average(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc, _biased_doc])
def stddev(*args, **kwargs):
    """
    Implement standard deviation aggregation

    See also
    --------
    **STDDEV** OneTick event processor

    Examples
    --------

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = data.agg({'RESULT': otp.agg.stddev('X')})    # OTdirective: snippet-name: Aggregations.stddev;
    >>> otp.run(data)
            Time    RESULT
    0 2003-12-04  1.118034
    """
    return StdDev(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc, _time_series_type_w_doc])
def tw_average(*args, **kwargs):
    """
    Returns time weighted average of input field

    See also
    --------
    **TW_AVERAGE** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[0, 1000, 1500, 3000])
    >>> otp.run(data, start=otp.dt(2023, 4, 25), end=otp.dt(2023, 4, 25)+otp.Second(4))
                        Time	X
    0	2023-04-25 00:00:00.000	1
    1	2023-04-25 00:00:01.000	2
    2	2023-04-25 00:00:01.500	3
    3	2023-04-25 00:00:03.000	4

    >>> # OTdirective: snippet-name: Aggregations.time weighted average;
    >>> data = otp.Ticks(X=[1, 2, 3, 4], offset=[0, 1000, 1500, 3000])
    >>> data = data.agg({'RESULT': otp.agg.tw_average('X')})
    >>> otp.run(data, start=otp.dt(2023, 4, 25), end=otp.dt(2023, 4, 25)+otp.Second(4))
            Time  RESULT
    0 2023-04-25 00:00:04  2.625
    """
    return TimeWeightedAvg(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc])
def median(*args, **kwargs):
    """
    Implement median aggregation

    See also
    --------
    **MEDIAN** OneTick event processor

    Examples
    --------

    >>> data = otp.Ticks(X=[1, 2, 3, 3, 4])
    >>> data = data.agg({'RESULT': otp.agg.median('X')})    # OTdirective: snippet-name: Aggregations.median;
    >>> otp.run(data)
            Time  RESULT
    0 2003-12-04     3.0
    """
    return Median(*args, **kwargs)


@docstring(parameters=OB_SNAPSHOT_DOC_PARAMS)
def ob_snapshot(*args, **kwargs):
    """
    Returns the order book state at the end of each bucket interval:
    the price, the size, the side, and the time of the last update for a specified number of order book levels.

    See also
    --------
    | :func:`onetick.py.ObSnapshot`
    | **OB_SNAPSHOT** OneTick event processor


    Examples
    --------
    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = otp.agg.ob_snapshot(max_levels=1).apply(data)  # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  PRICE             UPDATE_TIME  SIZE  LEVEL  BUY_SELL_FLAG
    0 2003-12-04    2.0 2003-12-01 00:00:00.003     6      1              1
    1 2003-12-04    5.0 2003-12-01 00:00:00.004     7      1              0
    """
    return ObSnapshot(*args, **kwargs)


@docstring(parameters=OB_SNAPSHOT_WIDE_DOC_PARAMS)
def ob_snapshot_wide(*args, **kwargs):
    """
    Returns a side by side order book state at the end of each interval:
    the price, the size, and the time of the last update for a specified number of order book levels.

    See also
    --------
    | :func:`onetick.py.ObSnapshotWide`
    | **OB_SNAPSHOT_WIDE** OneTick event processor

    Examples
    --------
    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = otp.agg.ob_snapshot_wide(max_levels=1).apply(data)  # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  BID_PRICE         BID_UPDATE_TIME  BID_SIZE  ASK_PRICE         ASK_UPDATE_TIME  ASK_SIZE  LEVEL
    0 2003-12-03        5.0 2003-12-01 00:00:00.004         7        2.0 2003-12-01 00:00:00.003         6      1
    """
    return ObSnapshotWide(*args, **kwargs)


@docstring(parameters=OB_SNAPSHOT_FLAT_DOC_PARAMS)
def ob_snapshot_flat(*args, **kwargs):
    """
    Returns the snapshot for a specified number of book levels as a single tick
    with multiple field groups corresponding to book levels.

    See also
    --------
    | :func:`onetick.py.ObSnapshotFlat`
    | **OB_SNAPSHOT_FLAT** OneTick event processor

    Examples
    --------
    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = otp.agg.ob_snapshot_flat(max_levels=1).apply(data)  # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  BID_PRICE1        BID_UPDATE_TIME1  BID_SIZE1  ASK_PRICE1        ASK_UPDATE_TIME1  ASK_SIZE1
    0 2003-12-03         5.0 2003-12-01 00:00:00.004          7         2.0 2003-12-01 00:00:00.003          6
    """
    return ObSnapshotFlat(*args, **kwargs)


@docstring(parameters=OB_SUMMARY_DOC_PARAMS)
def ob_summary(*args, **kwargs):
    """
    Computes summary statistics, such as:
    VWAP, best and worst price, total size, and number of levels, for one or both sides of an order book.

    See also
    --------
    | :func:`onetick.py.ObSummary`
    | **OB_SUMMARY** OneTick event processor


    Examples
    --------
    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = otp.agg.ob_summary(max_levels=1).apply(data)  # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  BID_PRICE  BID_SIZE  BID_VWAP  BEST_BID_PRICE  WORST_BID_SIZE  NUM_BID_LEVELS  ASK_SIZE\
              ASK_VWAP  BEST_ASK_PRICE  WORST_ASK_PRICE  NUM_ASK_LEVELS
    0 2003-12-04        NaN         7       5.0             5.0             NaN               1         6\
           2.0             2.0              2.0               1
    """
    return ObSummary(*args, **kwargs)


@docstring(parameters=OB_SIZE_DOC_PARAMS)
def ob_size(*args, **kwargs):
    """
    Returns the total size for a specified number of order book levels at the end of each bucket interval.

    See also
    --------
    | :func:`onetick.py.ObSize`
    | **OB_SIZE** OneTick event processor

    Examples
    --------

    Basic example

    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = otp.agg.ob_size(bucket_interval=otp.Second(300), max_levels=10).apply(data)  # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
                         Time  ASK_VALUE  BID_VALUE
    0 2003-12-01 00:05:00.000     194800      47200
    1 2003-12-01 00:10:00.000     194800      47200
    2 2003-12-01 00:15:00.000     313400       7700
    ...

    Selecting side via ``side`` parameter

    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = otp.agg.ob_size(bucket_interval=otp.Second(300), max_levels=10, size='ASK').apply(data)  # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
                         Time   VALUE
    0 2003-12-01 00:05:00.000  194800
    1 2003-12-01 00:10:00.000  194800
    2 2003-12-01 00:15:00.000  313400
    ...
    """
    return ObSize(*args, **kwargs)


@docstring(parameters=OB_VWAP_DOC_PARAMS)
def ob_vwap(*args, **kwargs):
    """
    Returns the size-weighted price computed over a specified number of order book levels at the end of each interval.

    See also
    --------
    | :func:`onetick.py.ObVwap`
    | **OB_VWAP** OneTick event processor

    Examples
    --------

    Basic example

    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = otp.agg.ob_vwap(bucket_interval=otp.Second(300)).apply(data)  # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
                         Time  ASK_VALUE  BID_VALUE
    0 2003-12-01 00:05:00.000   23.93496   23.80502
    1 2003-12-01 00:10:00.000   23.93496   23.80502
    2 2003-12-01 00:15:00.000   24.35387   24.35387
    ...

    Selecting side via ``side`` parameter

    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = otp.agg.ob_vwap(bucket_interval=otp.Second(300), size='BID').apply(data)  # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
                         Time     VALUE
    0 2003-12-01 00:05:00.000  23.80502
    1 2003-12-01 00:10:00.000  23.80502
    2 2003-12-01 00:15:00.000  24.35387
    ...
    """
    return ObVwap(*args, **kwargs)


@docstring(parameters=OB_NUM_LEVELS_DOC_PARAMS)
def ob_num_levels(*args, **kwargs):
    """
    Returns the number of levels in the order book at the end of each bucket.

    Note
    ----
    This EP supports only seconds as ``bucket_interval``.

    See also
    --------
    | :func:`onetick.py.ObNumLevels`
    | **OB_NUM_LEVELS** OneTick event processor

    Examples
    --------

    Basic example

    >>> data = otp.DataSource(db='SOME_DB', tick_type='PRL', symbols='AA')  # doctest: +SKIP
    >>> data = otp.agg.ob_num_levels(bucket_interval=otp.Second(300)).apply(data)  # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
                         Time  ASK_VALUE  BID_VALUE
    0 2003-12-01 00:05:00.000        243         74
    1 2003-12-01 00:10:00.000        243         74
    2 2003-12-01 00:15:00.000        208         45
    ...
    """
    return ObNumLevels(*args, **kwargs)


@docstring(parameters=[
    _query_fun_doc,
    _bucket_delimiter_doc,
    _bucket_interval_doc,
    _bucket_units_doc,
    _bucket_time_doc,
    _bucket_end_condition_doc,
    _running_doc,
    _group_by_doc,
    _groups_to_display_doc,
    _end_condition_per_group_doc,
    _boundary_tick_bucket_doc,
])
def generic(*args, **kwargs):
    """
    Generic aggregation.
    Aggregation logic is provided in ``query_fun`` parameter
    and this logic is applied for ticks in each bucket.
    Currently, this aggregation can be used only with ``.apply()`` method.

    Note, that ``query_fun`` should return a :py:class:`~onetick.py.Source` object,
    assuming that resulted query have only one tick per bucket.

    Also, ``query_fun`` could have additional parameters, which will be passed to ``query_fun``
    during aggregation. Those parameters should be specified in ``.apply()`` as keyword arguments,
    ex: ``.apply(src, additional_param=1)``.

    Generic aggregations could be also used in :py:meth:`onetick.py.Source.agg` method.
    Dict key set for an aggregation in ``aggs`` parameter will be used as prefix
    for each output column of this aggregation.
    Also, all tick fields not contained in the Python-level schema in a passed to generic aggregation
    :class:`Source` object will be removed.

    Note
    ----
    Some functions may be not supported in ``query_fun``.
    For example, :py:func:`~onetick.py.join` and :py:meth:`~onetick.py.Source.rename`.

    See also
    --------
    **GENERIC_AGGREGATION** OneTick event processor

    Examples
    --------

    The simplest case, just copying some other aggregation logic:

    >>> data = otp.Ticks({'A': [1, 2, 3]})
    >>> def agg_fun(source):
    ...     return source.agg({'X': otp.agg.count()})
    >>> data = otp.agg.generic(agg_fun).apply(data)
    >>> otp.run(data)
            Time  X
    0 2003-12-04  3

    Passing parameters to aggregation function:

    >>> data = otp.Ticks({'A': [1, 2, 1]})
    >>> def count_values(source, value):
    ...     values = source.where(source['A'] == value)
    ...     return values.agg({'count': otp.agg.count()})
    >>> data = otp.agg.generic(count_values).apply(data, value=1)
    >>> otp.run(data)
            Time  count
    0 2003-12-04  2

    Getting first 3 ticks from 5 milliseconds buckets:

    >>> data = otp.Ticks({'A': list(range(10))})
    >>> def agg_fun(source, n):
    ...     return source.first(n)
    >>> data = otp.agg.generic(agg_fun, bucket_interval=0.005).apply(data, n=3)
    >>> otp.run(data, start=otp.config.default_start_time, end=otp.config.default_start_time + otp.Milli(10))
                         Time  A
    0 2003-12-01 00:00:00.005  0
    1 2003-12-01 00:00:00.005  1
    2 2003-12-01 00:00:00.005  2
    3 2003-12-01 00:00:00.010  5
    4 2003-12-01 00:00:00.010  6
    5 2003-12-01 00:00:00.010  7

    Using generic aggregation inside :py:meth:`onetick.py.Source.agg` method:

    >>> def agg_fun(source):
    ...     return source.agg({'SUM': otp.agg.sum('X'), 'AVG': otp.agg.average('X')})
    ...
    >>> data = otp.Ticks(X=[1, 2, 3, 4, 5])
    >>> data = data.agg({'X': otp.agg.generic(agg_fun)})
    >>> otp.run(data)
            Time  X.SUM  X.AVG
    0 2003-12-04     15    3.0
    """

    return Generic(*args, **kwargs)


@docstring(parameters=[_running_doc, _bucket_interval_doc, _bucket_time_doc, _bucket_units_doc,
                       _bucket_end_condition_doc, _boundary_tick_bucket_doc])
def option_price(*args, **kwargs):
    """
    This aggregation requires several parameters to compute the option price.
    Those are, OPTION_TYPE, STRIKE_PRICE, EXPIRATION_DATE or DAYS_TILL_EXPIRATION, VOLATILITY, and INTEREST_RATE.
    Each parameter can be specified, either via a symbol parameter with the same name or via a tick field,
    by specifying the name of that field as an EP parameter, as follows.
    Besides, VOLATILITY and INTEREST_RATE can also be specified as parameters. If they are also specified as fields,
    the parameters value are ignored.
    In either case, the OPTION_TYPE value must be set to either CALL or PUT (case insensitive).
    EXPIRATION_DATE is in YYYYMMDD format, a string in case of a symbol parameter and
    an integer in case of a tick attribute.
    Additionally, NUMBER_OF_STEPS should be specified in case of Cox-Ross-Rubinstein method.

    Note
    ----
    This aggregation is used with ``.apply()``, but latest OneTick builds support also the ``.agg()`` method.

    Parameters
    ----------
    volatility: float
        The historical volatility of the asset’s returns.
    interest_rate: float
        The risk-free interest rate.
    compute_model: str
        Allowed values are `BS` and `CRR`.
        Choose between Black–Scholes (`BS`) and Cox-Ross-Rubinstein (`CRR`) models for computing call/put option price.
        Default: `BS`
    number_of_steps: int
        Specifies the number of time steps between the valuation and expiration dates.
        This is a mandatory parameter for `CRR` model.
    compute_delta: bool
        Specifies whether Delta is to be computed or not. This parameter is used only in case of `BS` model.
        Default: False
    compute_gamma: bool
        Specifies whether Gamma is to be computed or not. This parameter is used only in case of `BS` model.
        Default: False
    compute_theta: bool
        Specifies whether Theta is to be computed or not. This parameter is used only in case of `BS` model.
        Default: False
    compute_vega: bool
        Specifies whether Vega is to be computed or not. This parameter is used only in case of `BS` model.
        Default: False
    compute_rho: bool
        Specifies whether Rho is to be computed or not. This parameter is used only in case of `BS` model.
        Default: False
    volatility_field_name: str
        Specifies name of the field, which carries the historical volatility of the asset’s returns.
        Default: empty
    interest_rate_field_name: str
        Specifies name of the field, which carries the risk-free interest rate.
        Default: empty
    option_type_field_name: str
        Specifies name of the field, which carries the option type (either CALL or PUT).
        Default: empty
    strike_price_field_name: str
        Specifies name of the field, which carries the strike price of the option.
        Default: empty
    days_in_year: int
        Specifies number of days in a year (say, 365 or 252 (business days, etc.).
        Used with DAYS_TILL_EXPIRATION parameter to compute the fractional years till expiration.
        Default: 365
    days_till_expiration_field_name: str
        Specifies name of the field, which carries number of days till expiration of the option.
        Default: empty
    expiration_date_field_name: str
        Specifies name of the field, which carries the expiration date of the option, in YYYYMMDD format.
        Default: empty

    See also
    --------
    **OPTION_PRICE** OneTick event processor

    Examples
    ---------
    Black–Scholes with parameters passed through symbol params and calculated delta:

    >>> symbol = otp.Tick(SYMBOL_NAME='SYMB')
    >>> symbol['OPTION_TYPE'] = 'CALL'
    >>> symbol['STRIKE_PRICE'] = 100.0
    >>> symbol['DAYS_TILL_EXPIRATION'] = 30
    >>> symbol['VOLATILITY'] = 0.25
    >>> symbol['INTEREST_RATE'] = 0.05
    >>> data = otp.Ticks(PRICE=[100.7, 101.1, 99.5], symbol=symbol)
    >>> data = otp.agg.option_price(compute_delta=True).apply(data)
    >>> otp.run(data)['SYMB']
            Time     VALUE    DELTA
    0 2003-12-04  2.800999  0.50927
    >>> data.schema
    {'VALUE': <class 'float'>, 'DELTA': <class 'float'>}

    Cox-Ross-Rubinstein with parameters passed through fields:

    >>> data = otp.Ticks(
    ...     PRICE=[100.7, 101.1, 99.5],
    ...     OPTION_TYPE=['CALL']*3,
    ...     STRIKE_PRICE=[100.0]*3,
    ...     DAYS_TILL_EXPIRATION=[30]*3,
    ...     VOLATILITY=[0.25]*3,
    ...     INTEREST_RATE=[0.05]*3,
    ... )
    >>> data = otp.agg.option_price(
    ...     compute_model='CRR',
    ...     number_of_steps=5,
    ...     option_type_field_name='OPTION_TYPE',
    ...     strike_price_field_name='STRIKE_PRICE',
    ...     days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
    ...     volatility_field_name='VOLATILITY',
    ...     interest_rate_field_name='INTEREST_RATE',
    ... ).apply(data)
    >>> otp.run(data)
            Time     VALUE
    0 2003-12-04  2.937537

    Black–Scholes with some parameters passed through parameters:

    >>> data = otp.Ticks(
    ...     PRICE=[100.7, 101.1, 99.5],
    ...     OPTION_TYPE=['CALL']*3,
    ...     STRIKE_PRICE=[100.0]*3,
    ...     DAYS_TILL_EXPIRATION=[30]*3,
    ... )
    >>> data = otp.agg.option_price(
    ...     option_type_field_name='OPTION_TYPE',
    ...     strike_price_field_name='STRIKE_PRICE',
    ...     days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
    ...     volatility=0.25,
    ...     interest_rate=0.05,
    ... ).apply(data)
    >>> otp.run(data)
            Time     VALUE
    0 2003-12-04  2.800999

    To compute values for each tick in a series, set ``bucket_interval=1`` and ``bucket_units='ticks'``

    >>> data = otp.Ticks(
    ...    PRICE=[110.0, 101.0, 112.0],
    ...    OPTION_TYPE=["CALL"]*3,
    ...    STRIKE_PRICE=[110.0]*3,
    ...    DAYS_TILL_EXPIRATION=[30]*3,
    ...    VOLATILITY=[0.2]*3,
    ...    INTEREST_RATE=[0.05]*3
    ... )
    >>> data = otp.agg.option_price(
    ...    option_type_field_name='OPTION_TYPE',
    ...    strike_price_field_name='STRIKE_PRICE',
    ...    days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
    ...    volatility_field_name='VOLATILITY',
    ...    interest_rate_field_name='INTEREST_RATE',
    ...    bucket_interval=1,
    ...    bucket_units='ticks',
    ... ).apply(data)
    >>> otp.run(data)
                           Time 	VALUE
    0 	2003-12-01 00:00:00.000 	2.742714
    1 	2003-12-01 00:00:00.001 	0.212927
    2 	2003-12-01 00:00:00.002 	3.945447

    Usage with the ``.agg()`` method (on the latest OneTick builds).

    .. testcode::
       :skipif: not otp.compatibility.is_supported_agg_option_price()

       data = otp.Ticks(
           PRICE=[100.7, 101.1, 99.5],
           OPTION_TYPE=['CALL']*3,
           STRIKE_PRICE=[100.0]*3,
           DAYS_TILL_EXPIRATION=[30]*3,
       )
       data = data.agg({
           'RESULT': otp.agg.option_price(
               option_type_field_name='OPTION_TYPE',
               strike_price_field_name='STRIKE_PRICE',
               days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
               volatility=0.25,
               interest_rate=0.05,
           )
       })
       df = otp.run(data)
       print(df)

    .. testoutput::

               Time    RESULT
       0 2003-12-04  2.800999

    The following examples show results for different cases of option price calculation.
    Results are compared with two online calculators:
    `Drexel University <https://www.math.drexel.edu/~pg/fin/VanillaCalculator.html>`_ (DU) and
    `Wolfram Alpha <https://www.wolframalpha.com/input?i=black+scholes>`_ (WA).

    Call option, strike price 110.0, underlying price 120.0, volatility 20.0%, interest 5.0%, expiring in 15 days.

    >>> data = {
    ...     "PRICE": 120.,
    ...     "OPTION_TYPE": "call",
    ...     "STRIKE_PRICE": 110.,
    ...     "DAYS_TILL_EXPIRATION": 15,
    ...     "VOLATILITY": 0.2,
    ...     "INTEREST_RATE": 0.05,
    ... }
    >>> data = otp.Tick(**data)
    >>> data = otp.agg.option_price(
    ...     option_type_field_name='OPTION_TYPE',
    ...     strike_price_field_name='STRIKE_PRICE',
    ...     days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
    ...     volatility_field_name='VOLATILITY',
    ...     interest_rate_field_name='INTEREST_RATE',
    ...     compute_delta=True,
    ...     compute_gamma=True,
    ...     compute_theta=True,
    ...     compute_vega=True,
    ...     compute_rho=True
    ... ).apply(data)
    >>> res = otp.run(data).drop("Time", axis=1)
    >>> for key, val in res.to_dict(orient='list').items():
    ...     print(f"{key}={val[0]}")
    VALUE=10.248742578738629
    DELTA=0.9866897658932824
    GAMMA=0.007022082258701294
    THETA=-7.430061156928735
    VEGA=0.8311067221257422
    RHO=4.444686136785831

    .. csv-table:: Benchmark comparison
        :widths: 10, 25, 25, 10
        :header: Field, OneTick, DU benchmark, WA benchmark

        VALUE, 10.248742578738629, 10.248742577611323400, 10.249
        DELTA, 0.9866897658932824, 0.986689766547165200, 0.987
        GAMMA, 0.007022082258701294, 0.007022082258701300, 0.007
        THETA, -7.430061156928735, -7.430061160908399600, -7.430
        VEGA, 0.8311067221257422, 0.831106722125743000, 0.831
        RHO, 4.444686136785831, 4.444686140056785400, 4.445

    Put option, strike price 110.0, underlying price 120.0, volatility 20.0%, interest 5.0%, expiring in 15 days.

    .. testcode::
       :skipif: not otp.compatibility.is_option_price_theta_value_changed()

       data = {
           "PRICE": 120.,
           "OPTION_TYPE": "put",
           "STRIKE_PRICE": 110.,
           "DAYS_TILL_EXPIRATION": 15,
           "VOLATILITY": 0.2,
           "INTEREST_RATE": 0.05,
       }
       data = otp.Tick(**data)
       data = otp.agg.option_price(
           option_type_field_name='OPTION_TYPE',
           strike_price_field_name='STRIKE_PRICE',
           days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
           volatility_field_name='VOLATILITY',
           interest_rate_field_name='INTEREST_RATE',
           compute_delta=True,
           compute_gamma=True,
           compute_theta=True,
           compute_vega=True,
           compute_rho=True
       ).apply(data)
       res = otp.run(data).drop("Time", axis=1)
       for key, val in res.to_dict(orient='list').items():
           print(f"{key}={val[0]:.14f}")

    .. testoutput::

       VALUE=0.02294724243400
       DELTA=-0.01331023410672
       GAMMA=0.00702208225870
       THETA=-1.94135092374397
       VEGA=0.83110672212574
       RHO=-0.06658254802357

    .. csv-table:: Benchmark comparison
        :widths: 10, 25, 25, 10
        :header: Field, OneTick, DU benchmark, WA benchmark

        VALUE, 0.022947242433995818, 0.022947241306682900, 0.023
        DELTA, -0.013310234106717611, -0.013310233452834800, -0.013
        GAMMA, 0.007022082258701294, 0.007022082258701300, 0.007
        THETA, -1.94135092374397, -1.941350927723632000, -1.941
        VEGA, 0.8311067221257422, 0.831106722125743000, 0.831
        RHO, -0.06658254802356636, -0.066582544752611000, -0.067


    Put option, strike price 90.0, underlying price 80.0, volatility 30.0%, interest 8.0%, expiring in 20 days.

    .. testcode::
       :skipif: not otp.compatibility.is_option_price_theta_value_changed()

       data = {
           "PRICE": 80.,
           "OPTION_TYPE": "put",
           "STRIKE_PRICE": 90.,
           "DAYS_TILL_EXPIRATION": 20,
           "VOLATILITY": 0.3,
           "INTEREST_RATE": 0.08,
       }
       data = otp.Tick(**data)
       data = otp.agg.option_price(
           option_type_field_name='OPTION_TYPE',
           strike_price_field_name='STRIKE_PRICE',
           days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
           volatility_field_name='VOLATILITY',
           interest_rate_field_name='INTEREST_RATE',
           compute_delta=True,
           compute_gamma=True,
           compute_theta=True,
           compute_vega=True,
           compute_rho=True
       ).apply(data)
       res = otp.run(data).drop("Time", axis=1)
       for key, val in res.to_dict(orient='list').items():
           print(f"{key}={val[0]}")

    .. testoutput::

       VALUE=9.739720671039635
       DELTA=-0.9429118423759162
       GAMMA=0.020391626464263516
       THETA=0.9410250231811439
       VEGA=2.1453108389800515
       RHO=-4.666995510197969

    .. csv-table:: Benchmark comparison
        :widths: 10, 25, 25, 10
        :header: Field, OneTick, DU benchmark, WA benchmark

        VALUE, 9.739720671039635, 9.739720664487278600, 9.740
        DELTA, -0.9429118423759162, -0.942911845180447200, -0.943
        GAMMA, 0.020391626464263516, 0.020391626464263600, 0.020
        THETA, 0.9410250231811439, 0.941025040605956600, 0.941
        VEGA, 2.1453108389800515, 2.145310838980050700, 2.145
        RHO, -4.666995510197969, -4.666995522132770800, -4.667

    Call option, strike price 90.0, underlying price 80.0, volatility 30.0%, interest 8.0%, expiring in 20 days.

    >>> data = {
    ...     "PRICE": 80.,
    ...     "OPTION_TYPE": "call",
    ...     "STRIKE_PRICE": 90.,
    ...     "DAYS_TILL_EXPIRATION": 20,
    ...     "VOLATILITY": 0.3,
    ...     "INTEREST_RATE": 0.08,
    ... }
    >>> data = otp.Tick(**data)
    >>> data = otp.agg.option_price(
    ...     option_type_field_name='OPTION_TYPE',
    ...     strike_price_field_name='STRIKE_PRICE',
    ...     days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
    ...     volatility_field_name='VOLATILITY',
    ...     interest_rate_field_name='INTEREST_RATE',
    ...     compute_delta=True,
    ...     compute_gamma=True,
    ...     compute_theta=True,
    ...     compute_vega=True,
    ...     compute_rho=True
    ... ).apply(data)
    >>> res = otp.run(data).drop("Time", axis=1)
    >>> for key, val in res.to_dict(orient='list').items():
    ...     print(f"{key}={val[0]:.13f}")
    VALUE=0.1333777785229
    DELTA=0.0570881576241
    GAMMA=0.0203916264643
    THETA=-6.2274824082202
    VEGA=2.1453108389801
    RHO=0.2429410866523

    .. csv-table:: Benchmark comparison
        :widths: 10, 25, 25, 10
        :header: Field, OneTick, DU benchmark, WA benchmark

        VALUE, 0.13337777852292199, 0.133377771970562400, 0.133
        DELTA, 0.05708815762408384, 0.057088154819552600, 0.057
        GAMMA, 0.020391626464263516, 0.020391626464263600, 0.020
        THETA, -6.227482408220195, -6.227482390795381800, -6.227
        VEGA, 2.1453108389800515, 2.145310838980050700, 2.145
        RHO, 0.2429410866522622, 0.242941074717460500, 0.243


    Call option, strike price 140.0, underlying price 150.0, volatility 60.0%, interest 7.0%, expiring in 10 days.

    >>> data = {
    ...     "PRICE": 150.,
    ...     "OPTION_TYPE": "call",
    ...     "STRIKE_PRICE": 140.,
    ...     "DAYS_TILL_EXPIRATION": 10,
    ...     "VOLATILITY": 0.6,
    ...     "INTEREST_RATE": 0.07,
    ... }
    >>> data = otp.Tick(**data)
    >>> data = otp.agg.option_price(
    ...     option_type_field_name='OPTION_TYPE',
    ...     strike_price_field_name='STRIKE_PRICE',
    ...     days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
    ...     volatility_field_name='VOLATILITY',
    ...     interest_rate_field_name='INTEREST_RATE',
    ...     compute_delta=True,
    ...     compute_gamma=True,
    ...     compute_theta=True,
    ...     compute_vega=True,
    ...     compute_rho=True
    ... ).apply(data)
    >>> res = otp.run(data).drop("Time", axis=1)
    >>> for key, val in res.to_dict(orient='list').items():
    ...     print(f"{key}={val[0]:.13f}")
    VALUE=12.2728332229748
    DELTA=0.7774682547236
    GAMMA=0.0200066930955
    THETA=-88.3314253858302
    VEGA=7.3997358024512
    RHO=2.8588330133030

    .. csv-table:: Benchmark comparison
        :widths: 10, 25, 25, 10
        :header: Field, OneTick, DU benchmark, WA benchmark

        VALUE, 12.272833222974782, 12.272833124496244700, 12.27
        DELTA, 0.7774682547235652, 0.777468265655730700, 0.777
        GAMMA, 0.020006693095516285, 0.020006693095516300, 0.020
        THETA, -88.33142538583016, -88.331425507511386000, -88.331
        VEGA, 7.399735802451228, 7.399735802451227600, 7.400
        RHO, 2.858833013303013, 2.858833060927763500, 2.859


    Put option, strike price 140.0, underlying price 150.0, volatility 60.0%, interest 7.0%, expiring in 10 days.

    .. testcode::
       :skipif: not otp.compatibility.is_option_price_theta_value_changed()

       data = {
           "PRICE": 150.,
           "OPTION_TYPE": "put",
           "STRIKE_PRICE": 140.,
           "DAYS_TILL_EXPIRATION": 10,
           "VOLATILITY": 0.6,
           "INTEREST_RATE": 0.07,
       }
       data = otp.Tick(**data)
       data = otp.agg.option_price(
           option_type_field_name='OPTION_TYPE',
           strike_price_field_name='STRIKE_PRICE',
           days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
           volatility_field_name='VOLATILITY',
           interest_rate_field_name='INTEREST_RATE',
           compute_delta=True,
           compute_gamma=True,
           compute_theta=True,
           compute_vega=True,
           compute_rho=True).apply(data)
       res = otp.run(data).drop("Time", axis=1)
       for key, val in res.to_dict(orient='list').items():
           print(f"{key}={val[0]:.13f}")

    .. testoutput::

       VALUE=2.0045973669685
       DELTA=-0.2225317452764
       GAMMA=0.0200066930955
       THETA=-78.5502018957506
       VEGA=7.3997358024512
       RHO=-0.9694344974913

    .. csv-table:: Benchmark comparison
        :widths: 10, 25, 25, 10
        :header: Field, OneTick, DU benchmark, WA benchmark

        VALUE, 2.0045973669685395, 2.004597268490016500, 2.00
        DELTA, -0.22253174527643482, -0.222531734344269000, -0.223
        GAMMA, 0.020006693095516285, 0.020006693095516300, 0.020
        THETA, -78.5502018957506, -78.550202017431814100, -78.550
        VEGA, 7.399735802451228, 7.399735802451227600, 7.400
        RHO, -0.9694344974913363, -0.969434449866586000, -0.969

    """

    return OptionPrice(*args, **kwargs)


@docstring(parameters=[
    _bucket_interval_doc,
    _bucket_units_doc,
    _bucket_time_doc,
    _bucket_end_condition_doc,
    _boundary_tick_bucket_doc,
    _group_by_doc,
    _groups_to_display_doc,
    _end_condition_per_group_doc,
])
def ranking(*args, **kwargs):
    """
    Ranking **running** aggregation.

    Sorts a series of ticks over a bucket interval
    using a specified set of tick fields specified in ``rank_by``
    and adds a new field ``RANKING``
    with the position of the tick in the sort order
    or the percentage of ticks with values less than (or equal) to the value of the tick.

    Does not change the order of the ticks.

    See also
    --------
    **RANKING** OneTick event processor

    Parameters
    ----------
    rank_by: str or list or dict
        Set of fields to sort by.
        Can be one field specified by string, list of fields or dictionary with field names
        as keys and ``asc`` or ``desc`` string literals as values. Latter allows to specify
        sorting direction. Default direction is ``desc``.
    show_rank_as: str

        - ``order``: calculate number that represents the position of the tick in the sort order
        - ``percent_le_values``: calculate the percentage of ticks that have higher or equal value\
           of the position in the sort order, relative to the tick
        - ``percent_lt_values``: calculate the percentage of ticks that have higher value\
           of the position in the sort order, relative to the tick
        - ``percentile_standard``: calculate Percentile Rank of the tick in the sort order.
    include_tick: bool, default=False
        Specifies whether the current tick should be included in calculations
        if ``show_rank_as`` is ``percent_lt_values`` or ``percentile_standard``.

    Examples
    --------
    >>> t = otp.Ticks({'A': [1, 2, 3]})
    >>> t = t.ranking('A')
    >>> otp.run(t)
                         Time  A  RANKING
    0 2003-12-01 00:00:00.000  1        3
    1 2003-12-01 00:00:00.001  2        2
    2 2003-12-01 00:00:00.002  3        1

    >>> t = otp.Ticks({'A': [1, 2, 3]})
    >>> t = t.ranking({'A': 'asc'})
    >>> otp.run(t)
                         Time  A  RANKING
    0 2003-12-01 00:00:00.000  1        1
    1 2003-12-01 00:00:00.001  2        2
    2 2003-12-01 00:00:00.002  3        3

    >>> t = otp.Ticks({'A': [1, 2, 2, 3, 2, 1]})
    >>> otp.run(t.ranking({'A': 'asc'}, show_rank_as='percent_lt_values', include_tick=True))
                         Time  A    RANKING
    0 2003-12-01 00:00:00.000  1  66.666667
    1 2003-12-01 00:00:00.001  2  16.666667
    2 2003-12-01 00:00:00.002  2  16.666667
    3 2003-12-01 00:00:00.003  3   0.000000
    4 2003-12-01 00:00:00.004  2  16.666667
    5 2003-12-01 00:00:00.005  1  66.666667

    >>> otp.run(t.ranking({'A': 'asc'}, show_rank_as='percent_lt_values', include_tick=False))
                         Time  A  RANKING
    0 2003-12-01 00:00:00.000  1     80.0
    1 2003-12-01 00:00:00.001  2     20.0
    2 2003-12-01 00:00:00.002  2     20.0
    3 2003-12-01 00:00:00.003  3      0.0
    4 2003-12-01 00:00:00.004  2     20.0
    5 2003-12-01 00:00:00.005  1     80.0
    """
    return Ranking(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc])
def variance(*args, **kwargs):
    """
    Implement variance aggregation

    Parameters
    ----------
    biased: bool
        Switches between biased and unbiased variance calculation.

    See also
    --------
    **VARIANCE** OneTick event processor

    Examples
    --------

    >>> data = otp.Ticks(X=[1, 2, 3, 3, 4])
    >>> data = data.agg({'RESULT': otp.agg.variance('X', biased=False)})
    >>> otp.run(data)
            Time  RESULT
    0 2003-12-04     1.3

    >>> data = otp.Ticks(X=[1, 2, 3, 3, 4])
    >>> data = data.agg({'RESULT': otp.agg.variance('X', biased=True)})
    >>> otp.run(data)
            Time  RESULT
    0 2003-12-04     1.04
    """
    return Variance(*args, **kwargs)


@docstring(parameters=[
    _running_doc,
    _bucket_interval_doc,
    _bucket_units_doc,
    _bucket_time_doc,
    _bucket_end_condition_doc,
    _boundary_tick_bucket_doc,
    _group_by_doc,
    _groups_to_display_doc,
    _end_condition_per_group_doc,
])
def percentile(*args, **kwargs):
    """
    Percentile **running** aggregation.

    For each bucket, propagates its ``n-1`` ``n-quantiles`` where a comparison between ticks is done
    using a specified set of tick fields. A new field (``QUANTILE``) with the quantile number is added.

    See also
    --------
    **PERCENTILE** OneTick event processor

    Parameters
    ----------
    number_of_quantiles: int
        Specifies the number ``n`` of quantiles. Setting it to 2 will propagate only one tick - the median.

        Default: 2
    input_field_names: List[Union[Union[str, onetick.py.Column], Tuple[Union[str, onetick.py.Column], str]]]
        List of numeric field names to run aggregation on.
        You can use as list elements either string column name, either :py:class:`Columns <onetick.py.Column>`.

        You can change default comparison order (``desc``) for the field,
        by passing a tuple of column name and comparison order (``desc`` or ``asc``) instead column name.
    output_field_names: Optional[List[str]]
        Output columns name in the same order as columns from ``input_field_names``

        ``output_field_names`` and ``input_field_names`` must have the same number of fields.

        If not set, ``output_field_names`` will be equal to ``input_field_names``.

    Examples
    --------
    >>> t = otp.Ticks({'A': [1, 2, 3, 4]})
    >>> t = t.percentile(['A'])
    >>> otp.run(t)
            Time  A  QUANTILE
    0 2003-12-04  2         1

    You can also pass column:

    >>> t = otp.Ticks({'A': [1, 2, 3, 4]})
    >>> t = t.percentile([t['A']])
    >>> otp.run(t)
            Time  A  QUANTILE
    0 2003-12-04  2         1

    Change number of quantiles:

    >>> t = otp.Ticks({'A': [1, 2, 3, 4]})
    >>> t = t.percentile(['A'], number_of_quantiles=3)
    >>> otp.run(t)
            Time  A  QUANTILE
    0 2003-12-04  3         1
    1 2003-12-04  2         2

    Or change default comparison order:

    >>> t = otp.Ticks({'A': [1, 2, 3, 4]})
    >>> t = t.percentile([('A', 'asc')], number_of_quantiles=3)
    >>> otp.run(t)
            Time  A  QUANTILE
    0 2003-12-04  2         1
    1 2003-12-04  3         2

    You can also change output column name via ``output_field_names`` parameter:

    >>> t = otp.Ticks({'A': [1, 2, 3, 4]})
    >>> t = t.percentile(['A'], output_field_names=['B'])
    >>> otp.run(t)
            Time  B  QUANTILE
    0 2003-12-04  2         1
    """
    return Percentile(*args, **kwargs)


@docstring(parameters=[
    _running_doc,
    _bucket_interval_doc,
    _bucket_units_doc,
    _bucket_time_doc,
    _bucket_end_condition_doc,
    _boundary_tick_bucket_doc,
    _group_by_doc,
    _groups_to_display_doc,
    _end_condition_per_group_doc,
])
def find_value_for_percentile(*args, **kwargs):
    """
    For each bucket, finds the value which has percentile rank closest to ``percentile`` parameter.

    For more details, see `Percentile rank <https://en.wikipedia.org/wiki/Percentile_rank>`_.

    See also
    --------
    **FIND_VALUE_FOR_PERCENTILE** OneTick event processor

    Parameters
    ----------
    percentile: int
        Specifies the percentile number in 0-100% range
        for which find the value of input field that has percentile rank close to it.
    show_percentile_as: str
        * By default (when parameter is not set),
          the output field contains the value which has percentile rank closest to ``percentile`` parameter.
        * When set to *interpolated_value*, the output field contains the interpolated value of:
          The value above and below specified ``percentile``, or one of them if it's biggest or smallest value.
        * When set to *first_value_with_ge_percentile*, the output field contains
          the value which has greater or equal than specified ``percentile``.

    Examples
    --------

    Find interpolated value of 50% percentile rank:

    .. testcode::
       :skipif: not otp.compatibility.is_find_value_for_percentile_supported()

       t = otp.Ticks({'A': [1, 2, 3, 4]})
       data = t.find_value_for_percentile('A', 50, 'interpolated_value')
       df = otp.run(data)
       print(df)

    .. testoutput::

               Time    A
       0 2003-12-04  2.5

    Find first value greater or equal to 50% percentile rank:

    .. testcode::
       :skipif: not otp.compatibility.is_find_value_for_percentile_supported()

       data = t.find_value_for_percentile('A', 50, 'first_value_with_ge_percentile')
       df = otp.run(data)
       print(df)

    .. testoutput::

               Time    A
       0 2003-12-04  3.0

    Use :py:meth:`~onetick.py.Source.agg` method to apply several aggregations at the same time:

    .. testcode::
       :skipif: not otp.compatibility.is_find_value_for_percentile_supported()

       data = t.agg({
           'X': otp.agg.find_value_for_percentile('A', 50, 'interpolated_value'),
           'COUNT': otp.agg.count(),
       })
       df = otp.run(data)
       print(df)

    .. testoutput::

               Time    X  COUNT
       0 2003-12-04  2.5      4
    """
    return FindValueForPercentile(*args, **kwargs)


@docstring(parameters=[
    _decay_doc,
    _decay_value_type_doc,
    _running_doc,
    _bucket_interval_doc,
    _bucket_units_doc,
    _bucket_time_doc,
    _bucket_end_condition_doc,
    _boundary_tick_bucket_doc,
    _all_fields_doc,
    _group_by_doc,
    _groups_to_display_doc,
    _end_condition_per_group_doc,
    _time_series_type_w_doc,
])
def exp_w_average(*args, **kwargs):
    """
    ``EXP_W_AVERAGE`` aggregation.

    For each bucket, computes the **exponentially weighted average** value of the specified numeric attribute.
    Weights of data points in a bucket decrease exponentially in the direction from the most recent tick
    to the most aged one, being equal to ``exp(-Lambda * N)`` for a fixed weight decay value **Lambda**,
    where **N** ranges over **0, 1, 2, …** as ticks in reverse order of their arrival are treated.
    Once the weights are known, the average is found using the formula ``sum(weight*value)/sum(weight)``,
    where the sum is computed across all data points.

    See also
    --------
    **EXP_W_AVERAGE** OneTick event processor

    Examples
    --------

    Basic example

    >>> data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
    >>> data = data.exp_w_average('A', decay=2, bucket_interval=2, bucket_units='ticks')
    >>> otp.run(data)
                         Time         A
    0 2003-12-01 00:00:00.001  1.880797
    1 2003-12-01 00:00:00.003  2.984124
    2 2003-12-04 00:00:00.000  3.880797

    You can switch to ``half_life_index`` as ``decay_value_type``

    >>> data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
    >>> data = data.exp_w_average(
    ...     'A', decay=2, decay_value_type='half_life_index', bucket_interval=2, bucket_units='ticks',
    ... )
    >>> otp.run(data)
                         Time         A
    0 2003-12-01 00:00:00.001  1.585786
    1 2003-12-01 00:00:00.003  2.773459
    2 2003-12-04 00:00:00.000  3.585786
    """
    return ExpWAverage(*args, **kwargs)


@docstring(parameters=[
    _decay_doc,
    _decay_value_type_hl_doc,
    _running_doc,
    _bucket_interval_doc,
    _bucket_units_doc,
    _bucket_time_doc,
    _bucket_end_condition_doc,
    _boundary_tick_bucket_doc,
    _all_fields_doc,
    _group_by_doc,
    _groups_to_display_doc,
    _end_condition_per_group_doc,
])
def exp_tw_average(*args, **kwargs):
    """
    ``EXP_TW_AVERAGE`` aggregation.

    For each bucket, computes the **exponentially time-weighted average** value of a specified numeric field.
    The weight of each point in the time series is computed relative to the end time of the bucket,
    so that the value which is in effect during some infinitely small time interval `delta t`
    has weight **(delta t)*exp(-Lambda*(end_time - t))**,
    where `Lambda` is a constant, `end_time` represents end time of the bucket,
    and `t` represents the timestamp of that infinitely small time interval.

    See also
    --------
    **EXP_TW_AVERAGE** OneTick event processor

    Examples
    --------

    Basic example

    >>> data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
    >>> data = data.exp_tw_average('A', decay=2, bucket_interval=2, bucket_units='ticks')
    >>> otp.run(data)
                         Time         A
    0 2003-12-01 00:00:00.001  1.000000
    1 2003-12-01 00:00:00.003  2.500087
    2 2003-12-04 00:00:00.000  4.000000

    You can switch to ``lambda`` as ``decay_value_type``

    >>> data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
    >>> data = data.exp_tw_average(
    ...     'A', decay=2, decay_value_type='lambda', bucket_interval=2, bucket_units='ticks',
    ... )
    >>> otp.run(data)
                         Time       A
    0 2003-12-01 00:00:00.001  1.0000
    1 2003-12-01 00:00:00.003  2.5005
    2 2003-12-04 00:00:00.000  4.0000
    """
    return ExpTwAverage(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc, _degree_doc])
def standardized_moment(*args, **kwargs):
    """
    ``STANDARDIZED_MOMENT`` aggregation.

    Computes the standardized statistical moment of degree **k** of the input ``column``
    over the specified bucket interval.
    The standardized moment of degree **k** is defined
    as the expected value of the expression ``((X - mean) / stddev)^k``.

    See also
    --------
    **STANDARDIZED_MOMENT** OneTick event processor

    Examples
    --------

    Basic example

    .. testcode::
       :skipif: not otp.compatibility.is_standardized_moment_supported()

       data = otp.Ticks({'A': [1, 2, 4, 4, 4, 6]})
       data = data.standardized_moment('A', degree=3, bucket_interval=3, bucket_units='ticks')
       df = otp.run(data)
       print(df)

    .. testoutput::
                            Time         A
       0 2003-12-01 00:00:00.002  0.381802
       1 2003-12-01 00:00:00.005  0.707107
    """
    return StandardizedMoment(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _weight_field_name_doc, _portfolio_side_doc,
                       _weight_type_doc, _symbols_doc])
def portfolio_price(*args, **kwargs):
    """
    ``PORTFOLIO_PRICE`` aggregation.

    For each bucket, computes weighted portfolio price.

    See also
    --------
    **PORTFOLIO_PRICE** OneTick event processor

    Examples
    --------

    Basic example, by default this EP takes ``PRICE`` column as input

    >>> data = otp.DataSource(
    ...     'US_COMP', symbol='AAPL', tick_type='TRD',
    ...     start=otp.dt(2022, 3, 1), end=otp.dt(2022, 3, 2),
    ... )
    >>> data = data.portfolio_price()
    >>> otp.run(data)
            Time  VALUE  NUM_SYMBOLS
    0 2022-03-02    1.4            1

    Getting portfolio price for multiple symbols:

    >>> data = otp.DataSource('US_COMP', tick_type='TRD', start=otp.dt(2022, 3, 1), end=otp.dt(2022, 3, 2))
    >>> data = data.portfolio_price(symbols=['AAPL', 'AAP'])
    >>> otp.run(data)
            Time  VALUE  NUM_SYMBOLS
    0 2022-03-02  46.81            2

    Applying **PORTFOLIO_PRICE** on custom column

    >>> data = otp.Ticks(X=[10.0, 12.5, 11.0, 10.2, 15])
    >>> data = data.portfolio_price('X')
    >>> otp.run(data)
            Time  VALUE  NUM_SYMBOLS
    0 2003-12-04   15.0            1

    Specifying weights via column 'WEIGHS'

    >>> data = otp.Ticks(PRICE=[10.0, 12.5, 11.0, 10.2, 15], WEIGHTS=[1, 2, -1, 2, 2])
    >>> data = data.portfolio_price(weight_field_name=data['WEIGHTS'])
    >>> otp.run(data)
            Time  VALUE  NUM_SYMBOLS
    0 2003-12-04   30.0            1
    """
    return PortfolioPrice(*args, **kwargs)


@docstring(parameters=[_portfolios_query_doc, _columns_portfolio_doc, _running_doc, _bucket_interval_doc,
                       _bucket_time_doc, _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _weight_field_name_doc,
                       _weight_multiplier_field_name_doc, _portfolio_side_doc, _weight_type_doc,
                       _portfolios_query_params_doc, _portfolio_value_field_name_doc, _symbols_doc])
def multi_portfolio_price(*args, **kwargs):
    """
    ``MULTI_PORTFOLIO_PRICE`` aggregation.

    For each bucket, computes weighted portfolio price for multiple portfolios.

    See also
    --------
    **MULTI_PORTFOLIO_PRICE** OneTick event processor

    Examples
    --------

    Basic example, by default this EP takes ``PRICE`` column as input

    >>> data = otp.DataSource(
    ...     'US_COMP', tick_type='TRD', date=otp.dt(2022, 3, 1)
    ... )
    >>> data = data.multi_portfolio_price(
    ...     portfolios_query='some_query.otq::portfolios_query',
    ...     symbols=['US_COMP::AAPL', 'US_COMP::MSFT', 'US_COMP::ORCL'],
    ... )
    >>> otp.run(data)  # doctest: +SKIP
            Time  VALUE  NUM_SYMBOLS PORTFOLIO_NAME
    0 2003-12-01   95.0            3    PORTFOLIO_1
    1 2003-12-01   47.5            1    PORTFOLIO_2
    2 2003-12-01   32.5            2    PORTFOLIO_3

    Override ``weight`` returned by ``portfolios_query`` with ``weight_field_name``

    >>> data = otp.DataSource(
    ...     'US_COMP', tick_type='TRD', date=otp.dt(2022, 3, 1)
    ... )
    >>> data['WEIGHT'] = 2
    >>> data = data.multi_portfolio_price(
    ...     portfolios_query='some_query.otq::portfolios_query',
    ...     weight_field_name='WEIGHT',
    ...     symbols=['US_COMP::AAPL', 'US_COMP::MSFT', 'US_COMP::ORCL'],
    ... )
    >>> otp.run(data)  # doctest: +SKIP
            Time  VALUE  NUM_SYMBOLS PORTFOLIO_NAME
    0 2003-12-01   38.0            3    PORTFOLIO_1
    1 2003-12-01   19.0            1    PORTFOLIO_2
    2 2003-12-01   13.0            2    PORTFOLIO_3

    Pass parameters to the query from ``portfolios_query`` via ``portfolios_query_params``

    >>> data = otp.DataSource(
    ...     'US_COMP', tick_type='TRD', date=otp.dt(2022, 3, 1)
    ... )
    >>> data = data.multi_portfolio_price(
    ...     portfolios_query='some_query.otq::portfolios_query_with_param',
    ...     symbols=['US_COMP::AAPL', 'US_COMP::MSFT', 'US_COMP::ORCL'],
    ...     portfolios_query_params={'PORTFOLIO_1_NAME': 'CUSTOM_NAME'}
    ... )
    >>> otp.run(data)  # doctest: +SKIP
            Time  VALUE  NUM_SYMBOLS PORTFOLIO_NAME
    0 2003-12-01   95.0            3    CUSTOM_NAME
    1 2003-12-01   47.5            1    PORTFOLIO_2
    2 2003-12-01   32.5            2    PORTFOLIO_3

    Use ``otp.Source`` object as ``portfolios_query`` (only for local queries)

    >>> portfolios = otp.Ticks(
    ...     SYMBOL_NAME=['US_COMP::AAPL', 'US_COMP::MSFT', 'US_COMP::AAPL'],
    ...     PORTFOLIO_NAME=['PORTFOLIO_1', 'PORTFOLIO_1', 'PORTFOLIO_2'],
    ...     WEIGHT=[1, 1, 2],
    ... )
    >>> data = otp.DataSource(
    ...     'US_COMP', tick_type='TRD', date=otp.dt(2022, 3, 1)
    ... )
    >>> data = data.multi_portfolio_price(
    ...     portfolios_query=portfolios,
    ...     symbols=['US_COMP::AAPL', 'US_COMP::MSFT'],
    ... )
    >>> otp.run(data)  # doctest: +SKIP
            Time  VALUE  NUM_SYMBOLS PORTFOLIO_NAME
    0 2003-12-01   47.5            2    PORTFOLIO_1
    1 2003-12-01   46.0            1    PORTFOLIO_2
    """
    return MultiPortfolioPrice(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc])
def return_ep(*args, **kwargs):
    """
    ``RETURN`` aggregation.

    Computes the ratio between the value of the input field at the end of a bucket interval and the value
    of this field at the start of a bucket interval.
    If ``running`` is set to ``True``, then ``RETURN`` is calculated as the ratio between latest tick
    and the first tick from the start time. Below, the input field is referred to as 'price'.

    ``Return for an interval = ending price for the interval / starting price for the interval``

    where

    * ``ending price for the interval`` is taken from the last tick observed in the stream
      up to the end of the interval.
    * ``starting price for the interval`` is taken from the first tick in the interval for the first bucket interval
      that has any ticks. For all other intervals, it is the price of the first tick with timestamp of the start
      of interval or, if no such tick is present, the ending price of the previous interval. The ending price
      for the interval is taken from the last tick observed in the stream up to the end of the interval.

    See also
    --------
    **RETURN** OneTick event processor

    Examples
    --------

    Basic example as :py:class:`~onetick.py.Source` method

    >>> data = otp.DataSource('US_COMP', symbol='AAPL', tick_type='TRD')
    >>> data = data.return_ep(data['PRICE'], bucket_interval=otp.Minute(10))
    >>> otp.run(data, date=otp.dt(2022, 3, 1))  # doctest: +SKIP
                         Time    PRICE
    0 2022-03-01 00:00:00.000  0.99953
    1 2022-03-01 00:10:00.000   1.0043
    2 2022-03-01 00:20:00.000   0.9986
    3 2022-03-01 00:30:00.000  0.99643
    4 2022-03-01 00:40:00.000    1.042
    ...

    Basic example as aggregation

    >>> data = otp.DataSource('US_COMP', symbol='AAPL', tick_type='TRD')
    >>> data = otp.agg.return_ep(data['PRICE'], bucket_interval=otp.Minute(10)).apply(data)
    >>> otp.run(data, date=otp.dt(2022, 3, 1))  # doctest: +SKIP
                         Time    PRICE
    0 2022-03-01 00:00:00.000  0.99953
    1 2022-03-01 00:10:00.000   1.0043
    2 2022-03-01 00:20:00.000   0.9986
    3 2022-03-01 00:30:00.000  0.99643
    4 2022-03-01 00:40:00.000    1.042
    ...
    """
    return Return(*args, **kwargs)


@docstring(parameters=[_column_doc, _running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc,
                       _interest_rate_doc, _price_field_doc, _option_price_field_doc,
                       _method_doc, _precision_doc, _value_for_non_converge_doc, _option_type_field_doc,
                       _strike_price_field_doc, _days_in_year_doc, _days_till_expiration_field_doc,
                       _expiration_date_field_doc,
                       ])
def implied_vol(*args, **kwargs):
    """
    ``IMPLIED_VOL`` aggregation.

    For each bucket, computes implied volatility value for the last tick in the bucket,
    based on the Black-Scholes option pricing model.

    This EP requires a time series of ticks, having the ``PRICE`` and ``OPTION_PRICE`` attributes.

    It also requires several parameters to compute the implied volatility.
    Those are, ``OPTION_TYPE``, ``STRIKE_PRICE``, ``EXPIRATION_DATE`` or ``DAYS_TILL_EXPIRATION`` and ``INTEREST_RATE``.
    Each parameter can be specified either via a symbol parameter with the same name, or via a tick field,
    by specifying name of that field as an EP parameter.
    Besides, ``interest_rate`` can also be specified as aggregation parameter.
    In either case ``OPTION_TYPE`` must have either ``CALL`` value, or ``PUT``.
    ``EXPIRATION_DATE`` is in ``YYYYMMDD`` format, a string in case of a symbol parameter and an integer
    in case of a tick attribute.

    See also
    --------
    **IMPLIED_VOL** OneTick event processor

    Examples
    --------

    Basic example:

    >>> data = otp.DataSource('SOME_DB', symbol='AAA', tick_type='TT')  # doctest: +SKIP
    >>> data = data.implied_vol(
    ...     interest_rate=0.05, option_type_field=data['OPTION_TYPE'],
    ...     strike_price_field=data['STRIKE_PRICE'], days_till_expiration_field=data['DAYS_TILL_EXPIRATION'],
    ... )  # doctest: +SKIP
    >>> otp.run(data)  # doctest: +SKIP
            Time     VALUE
    0 2003-12-04  0.889491

    Specifying ``interest_rate`` and ``strike_price`` as symbol parameters:

    >>> sym = otp.Ticks({
    ...     'SYMBOL_NAME': ['TEST'],
    ...     'INTEREST_RATE': [0.05],
    ...     'STRIKE_PRICE': [100.0],
    ... })  # doctest: +SKIP
    >>> data = otp.DataSource('SOME_DB', symbol='AAA', tick_type='TT')  # doctest: +SKIP
    >>> data = data.implied_vol(
    ...     option_type_field=data['OPTION_TYPE'], days_till_expiration_field=data['DAYS_TILL_EXPIRATION'],
    ... )  # doctest: +SKIP
    >>> otp.run(data)  # doctest: +SKIP
            Time     VALUE
    0 2003-12-04  0.889491
    """
    return ImpliedVol(*args, **kwargs)


@docstring(parameters=[_running_doc, _all_fields_doc, _bucket_interval_doc, _bucket_time_doc,
                       _bucket_units_doc, _bucket_end_condition_doc, _end_condition_per_group_doc,
                       _boundary_tick_bucket_doc,
                       ])
def linear_regression(*args, **kwargs):
    """
    ``LINEAR_REGRESSION`` aggregation.

    For each bucket, computes the linear regression parameters slope and intercept of specified input fields
    ``dependent_variable_field_name`` and ``independent_variable_field_name``.
    Adds computed parameters as `SLOPE` and `INTERCEPT` fields in output time series.
    The relationship between the dependent variable (``Y``) and the independent variable (``X``) is defined
    by the formula: Y = SLOPE * X + INTERCEPT, where `SLOPE` and `INTERCEPT` are the calculated output parameters.

    See also
    --------
    **LINEAR_REGRESSION** OneTick event processor

    Examples
    --------

    >>> data = otp.Ticks({'X': [10.0, 9.5, 8.0, 8.5], 'Y': [3.0, 5.0, 4.5, 3.5]})
    >>> data = data.linear_regression(
    ...     dependent_variable_field_name=data['Y'],
    ...     independent_variable_field_name=data['X'],
    ... )  # doctest: +SKIP
    >>> otp.run(data)  # doctest: +SKIP
            Time  SLOPE  INTERCEPT
    0 2003-12-04   -0.3        6.7
    """
    return LinearRegression(*args, **kwargs)
