import warnings
from typing import TYPE_CHECKING, List, Optional, Union

from onetick import py as otp
from onetick.py.otq import otq
from onetick.py.functions import __copy_sources_on_merge_or_join
from onetick.py.aggregations._docs import (
    _boundary_tick_bucket_doc,
    _bucket_end_condition_doc,
    _bucket_interval_doc,
    _bucket_time_doc,
    _bucket_units_doc,
    _end_condition_per_group_doc,
    _group_by_doc,
    _groups_to_display_doc,
)
from onetick.py.docs.utils import docstring, param_doc
from onetick.py.aggregations._base import _Aggregation
from onetick.py.compatibility import is_diff_show_all_ticks_supported


if TYPE_CHECKING:
    from onetick.py.core.source import Source


def __add__(self: 'Source', other: 'Source') -> 'Source':
    return otp.merge([self, other])


def append(self: 'Source', other) -> 'Source':
    """
    Merge data source with `other`

    Parameters
    ----------
    other: List, Source
        data source to merge

    Returns
    -------
    Source
    """
    if isinstance(other, list):
        return otp.merge(other + [self])
    else:
        return otp.merge([self, other])


def diff(self: 'Source', other: 'Source',
         fields: Optional[Union[str, List[str]]] = None,
         ignore: bool = False,
         output_ignored_fields: Optional[bool] = None,
         show_only_fields_that_differ: Optional[bool] = None,
         show_matching_ticks: Optional[bool] = None,
         show_all_ticks: bool = False,
         non_decreasing_value_fields: Optional[Union[str, List[str]]] = None,
         threshold: Optional[int] = None,
         left_prefix: str = 'L',
         right_prefix: str = 'R',
         drop_index: bool = True) -> 'Source':
    """
    Compare two time-series.

    A tick from the first time series is considered to match a tick from the second time series
    if both ticks have identical ``non_decreasing_value_fields`` values
    and matching values of all ``fields`` that are present in both ticks and are not listed as the fields to be ignored.

    A field is considered to match another field
    if both fields have identical names, comparable types, and identical values.

    Field names in an output tick are represented as <source_name>.<field_name>.

    Note
    ----
    The first source is considered to be the base for starting the matching process.
    If a match is found, all ticks in the second source that occur before the first matched tick
    are considered different from those in the first source.
    The subsequent searches for the matches in the second source will begin after this latest matched tick.
    The ticks from the second source that are between the found matches
    are considered different from the ticks in the first source.

    Parameters
    ----------
    fields:
        List of fields to be used or ignored while comparing input time series.
        By default, if this value is not set, all fields are used in comparison.
    ignore:
        If True, fields specified in the ``fields`` parameter are ignored while comparing the input time series.
        Otherwise, by default, only the fields that are specified in the ``fields`` parameter are used in comparison.
        If the ``fields`` parameter is empty, the value of this parameter is ignored.
    output_ignored_fields:
        If False, the fields which are not used in comparison are excluded from the output.
        Otherwise, by default, all fields are propagated.
    show_only_fields_that_differ:
        If True (default), the method outputs only the tick fields the values of which were different.
        But if ``output_ignored_fields`` is set to True, then ignored fields are still propagated.
    show_matching_ticks:
        If True, the output of this method consists of matched ticks from both input time series
        instead of unmatched ticks.
        The output tick timestamp is equal to the earliest timestamp of its corresponding input ticks.
        Default value is False.
    show_all_ticks: bool
        If specified, the output of this EP consists of both matched and unmatched ticks from both input time series.
        ``MATCH_STATUS`` field will be added to the output tick with the possible values of:

        * ``0`` - different ticks
        * ``1`` - matching ticks
        * ``2`` - tick from one source only

        Default: ``False``
    non_decreasing_value_fields:
        List of *non-decreasing* value fields to be used for matching.
        If value of this parameter is **TIMESTAMP** (default), it compares two time series based on tick timestamp.
        If other field is specified, a field named <source_name>.<TIMESTAMP>
        will be added to the tick whose value equals the tick's primary timestamp.
    threshold:
        Specifies the number of first diff ticks to propagate.
        By default all such ticks are propagated.
    left_prefix:
        The prefix used in the output for fields from the left source.
    right_prefix:
        The prefix used in the output for fields from the right source.
    drop_index: bool
        If False, the output tick will also carry the position(s) of input tick(s)
        in input time series in field <source_name>.INDEX for each source.
        The lowest position number is 1.
        If True then <source_name>.INDEX fields will not be included in the output.

    Returns
    -------
    :class:`Source`

    See Also
    --------
    **DIFF** OneTick event processor

    Examples
    --------

    Print all ticks that have any unmatched fields:

    >>> t = otp.Ticks(A=[1, 2], B=[0, 0])
    >>> q = otp.Ticks(A=[1, 3], B=[0, 0])
    >>> data = t.diff(q)
    >>> otp.run(data)
                         Time  L.A  R.A
    0 2003-12-01 00:00:00.001    2    3

    Also show fields that were not different:

    >>> t = otp.Ticks(A=[1, 2], B=[0, 0])
    >>> q = otp.Ticks(A=[1, 3], B=[0, 0])
    >>> data = t.diff(q, show_only_fields_that_differ=False)
    >>> otp.run(data)
                         Time  L.A  L.B  R.A  R.B
    0 2003-12-01 00:00:00.001    2    0    3    0

    Change prefixes for output fields:

    >>> t = otp.Ticks(A=[1, 2], B=[0, 0])
    >>> q = otp.Ticks(A=[1, 3], B=[0, 0])
    >>> data = t.diff(q, left_prefix='LEFT', right_prefix='RIGHT')
    >>> otp.run(data)
                         Time  LEFT.A  RIGHT.A
    0 2003-12-01 00:00:00.001       2        3

    If there are several matching ticks then only the first will be matched:

    .. testcode::
       :skipif: not otp.compatibility.is_diff_show_matching_ticks_supported()

       t = otp.Ticks(A=[1, 1, 1, 1, 1], B=[1, 2, 3, 4, 5], offset=[0, 0, 1000, 2000, 2000])
       q = otp.Ticks(A=[1, 1, 1, 1, 1], B=[3, 4, 5, 6, 7], offset=[0, 1000, 1000, 2000, 2000])
       data = t.diff(q, fields=['A'], show_matching_ticks=True, output_ignored_fields=True)
       print(otp.run(data))

    .. testoutput::

                        Time  L.A  L.B  R.A  R.B
       0 2003-12-01 00:00:00    1    1    1    3
       1 2003-12-01 00:00:01    1    3    1    4
       2 2003-12-01 00:00:02    1    4    1    6
       3 2003-12-01 00:00:02    1    5    1    7

    Showing diff for every tick with ``show_all_ticks`` parameter:

    .. testcode::
       :skipif: not otp.compatibility.is_diff_show_all_ticks_supported()

       t = otp.Ticks(A=[1, 2, 3], B=[0, 0, 1])
       q = otp.Ticks(A=[1, 3], B=[0, 0])
       data = t.diff(q, show_all_ticks=True)
       print(otp.run(data))

    .. testoutput::

                            Time  MATCH_STATUS  L.A  R.A  L.B
       0 2003-12-01 00:00:00.000             1    0    0    0
       1 2003-12-01 00:00:00.001             0    2    3    0
       2 2003-12-01 00:00:00.002             2    3    0    1

    """

    if not fields:
        fields = []
    if isinstance(fields, str):
        fields = [fields]
    fields = list(map(str, fields))
    for field in fields:
        if field not in self.schema or field not in other.schema:
            raise ValueError(f"Field {field} is not in schema")

    if threshold is None:
        threshold = ''  # type: ignore
    elif threshold < 0:
        raise ValueError("Parameter 'threshold' must be non-negative")

    ep_params = dict(
        fields=','.join(map(str, fields)),
        ignore=ignore,
        threshold=threshold,
    )

    if non_decreasing_value_fields is not None:
        if not non_decreasing_value_fields:
            raise ValueError("Parameter 'non_decreasing_value_fields' can't be empty")
        if isinstance(non_decreasing_value_fields, str):
            non_decreasing_value_fields = [non_decreasing_value_fields]
        non_decreasing_value_fields = list(map(str, non_decreasing_value_fields))
        for field in non_decreasing_value_fields:
            if field not in self.schema or field not in other.schema:
                raise ValueError(f"Field {field} is not in schema")
        if otp.compatibility.is_diff_non_decreasing_value_fields_supported():
            ep_params['non_decreasing_value_fields'] = ','.join(map(str, non_decreasing_value_fields))
        else:
            warnings.warn("Parameter 'non_decreasing_value_fields' is not supported on this version of OneTick")

    if show_only_fields_that_differ and output_ignored_fields:
        raise ValueError(
            "Parameters 'output_ignored_fields' and 'show_only_fields_that_differ' can't be set at the same time"
        )

    if show_all_ticks:
        if not is_diff_show_all_ticks_supported():
            raise RuntimeError('`show_all_ticks` parameter not supported on current OneTick version')

        ep_params['show_all_ticks'] = show_all_ticks

    if show_only_fields_that_differ is None and output_ignored_fields is None:
        if ignore:
            ep_params['output_ignored_fields'] = True
        else:
            ep_params['output_ignored_fields'] = False
        ep_params['show_only_fields_that_differ'] = not ep_params['output_ignored_fields']
    elif show_only_fields_that_differ is None:
        ep_params['show_only_fields_that_differ'] = not output_ignored_fields
    elif output_ignored_fields is None:
        ep_params['output_ignored_fields'] = False
        ep_params['show_only_fields_that_differ'] = show_only_fields_that_differ

    if show_matching_ticks is not None:
        if otp.compatibility.is_diff_show_matching_ticks_supported():
            ep_params['show_matching_ticks'] = show_matching_ticks
        else:
            warnings.warn("Parameter 'show_matching_ticks' is not supported on this version of OneTick")

        if ep_params.get('show_matching_ticks') and show_only_fields_that_differ is None:
            ep_params['show_only_fields_that_differ'] = False

    schema = {
        f'{left_prefix}.INDEX': int,
        f'{right_prefix}.INDEX': int,
    }
    for src_prefix, src_schema in [(left_prefix, self.schema), (right_prefix, other.schema)]:
        for field, dtype in src_schema.items():
            if ignore and field in fields and not output_ignored_fields:
                continue
            schema[f'{src_prefix}.{field}'] = dtype

    if show_all_ticks:
        schema['MATCH_STATUS'] = int

    result = otp.Source(
        node=otq.Diff(**ep_params),
        schema=schema,
    )
    __copy_sources_on_merge_or_join(result, (self, other),
                                    names=(left_prefix, right_prefix))
    if drop_index:
        result = result.drop([f'{left_prefix}.INDEX', f'{right_prefix}.INDEX'])

    return result


def lee_and_ready(self: 'Source', qte: 'Source',
                  quote_delay: float = 0.0,
                  show_quote_fields: bool = False) -> 'Source':
    """
    Adds a numeric attribute to each tick in the stream of trade ticks,
    the value of which classifies the trade as a buy, a sell, or undefined.

    This is an implementation of the Lee and Ready algorithm:
    Match up a trade with the most recent good quote that is at least X seconds older than the trade —

    * if the trade's price is closer to the ask price, label trade a buy (1);
    * else, if it is closer to the bid price, label it a sell (-1);
    * else, if trade's price is at the mid-quote, then if it is higher than the last trade's price,
      classify it as a buy (1);
    * else, if it is less, classify it as a sell (-1);
    * else, if it is the same, classify it the same way as the previous trade was classified.
    * If all of these fail, classify the trade as unknown (0).

    This method expects two sources as its input: source of trades (``self``) and source of quotes (``qte``).
    While ticks propagated by trades source should have the PRICE,SIZE fields,
    ticks propagated by ``qte`` source should have the ASK_PRICE,ASK_SIZE,BID_PRICE,BID_SIZE fields.

    Output of this method is a time series of trades ticks
    with the Lee and Ready indicator field (**BuySellFlag**) added.

    Parameters
    ----------
    qte:
        The source of quotes.
    quote_delay:
        The minimum number of seconds that needs to elapse between the trade and the quote
        before the quote can be considered for a join with the trade.

        The value is a float number.
        Only the first three digits of the fraction are currently used,
        thus the highest supported granularity of quote delay is milliseconds.
        Sub-millisecond parts of the trade's and the quote's timestamps are ignored when computing delay between them.
    show_quote_fields:
        If set to True, the quote fields that classified trade will also be shown for each trade.
        Note that if there were no quotes before trade, then quote fields will be set to 0.

    Returns
    -------
    :class:`Source`

    See Also
    --------
    **LEE_AND_READY** OneTick event processor

    Examples
    --------

    Add field **BuySellFlag** to the ``trd`` source:

    >>> import os
    >>> trd = otp.CSV(os.path.join(csv_path, 'trd.csv'))
    >>> qte = otp.CSV(os.path.join(csv_path, 'qte.csv'))
    >>> data = trd.lee_and_ready(qte)
    >>> otp.run(data).head(5)
                            Time   PRICE  SIZE  BuySellFlag
    0 2003-12-01 09:00:00.086545  178.26   246         -1.0
    1 2003-12-01 09:00:00.245208  178.26     1          1.0
    2 2003-12-01 09:00:00.245503  178.26     1          1.0
    3 2003-12-01 09:00:00.387100  178.21     9          1.0
    4 2003-12-01 09:00:00.387105  178.21    12          1.0

    Fields from ``qte`` can be added with ``show_quote_fields`` parameter:

    >>> data = trd.lee_and_ready(qte, show_quote_fields=True)
    >>> data = data.drop(['ASK_SIZE', 'BID_SIZE'])
    >>> otp.run(data).head(5)
                            Time   PRICE  SIZE  BuySellFlag               QTE_TIMESTAMP  ASK_PRICE  BID_PRICE
    0 2003-12-01 09:00:00.086545  178.26   246         -1.0  2003-12-01 09:00:00.028307     178.80     177.92
    1 2003-12-01 09:00:00.245208  178.26     1          1.0  2003-12-01 09:00:00.244626     178.57     177.75
    2 2003-12-01 09:00:00.245503  178.26     1          1.0  2003-12-01 09:00:00.244626     178.57     177.75
    3 2003-12-01 09:00:00.387100  178.21     9          1.0  2003-12-01 09:00:00.387096     178.57     177.75
    4 2003-12-01 09:00:00.387105  178.21    12          1.0  2003-12-01 09:00:00.387096     178.57     177.75

    Set ``quote_delay`` parameter to 300 milliseconds:

    >>> data = trd.lee_and_ready(qte, show_quote_fields=True, quote_delay=0.3)
    >>> data = data.drop(['ASK_SIZE', 'BID_SIZE'])
    >>> otp.run(data).head(5)
                            Time   PRICE  SIZE  BuySellFlag               QTE_TIMESTAMP  ASK_PRICE  BID_PRICE
    0 2003-12-01 09:00:00.086545  178.26   246          0.0  1969-12-31 19:00:00.000000        0.0       0.00
    1 2003-12-01 09:00:00.245208  178.26     1          0.0  1969-12-31 19:00:00.000000        0.0       0.00
    2 2003-12-01 09:00:00.245503  178.26     1          0.0  1969-12-31 19:00:00.000000        0.0       0.00
    3 2003-12-01 09:00:00.387100  178.21     9         -1.0  2003-12-01 09:00:00.087540      180.0     177.62
    4 2003-12-01 09:00:00.387105  178.21    12         -1.0  2003-12-01 09:00:00.087540      180.0     177.62
    """

    schema = self.schema.copy()
    if show_quote_fields:
        schema.update(**qte.schema, **{'QTE_TIMESTAMP': otp.nsectime})
    schema.update(**{'BuySellFlag': float})

    result = otp.Source(
        node=otq.LeeAndReady(
            quote_delay=quote_delay,
            show_quote_fields=show_quote_fields
        ),
        schema=schema,
    )
    __copy_sources_on_merge_or_join(result, (self, qte), names=('TRD', 'QTE'))

    return result


_smallest_time_granularity_msec_name_doc = param_doc(
    name='smallest_time_granularity_msec',
    annotation=int,
    desc="""
    This method works by first sampling the source tick series with a constant rate.
    This is the sampling interval (1 / rate).
    As a consequence, any computed delay will be divisible by this value.
    It is important to carefully choose this parameter, as this method has a computational cost of O(N * log(N))
    per bucket, where N = (*duration_of_bucket_in_msec* + ``max_ts_delay_msec``) / ``smallest_time_granularity_msec``.
    Default: 1.
    """,
    default=1,
)


_max_ts_delay_msec_doc = param_doc(
    name='max_ts_delay_msec',
    annotation=int,
    desc="""
     The known upper bound on the delay's magnitude.
    The computed delay will never be greater than this value.
    Default: 1000.
    """,
    default=1000,
)


@docstring(
    parameters=[
        _smallest_time_granularity_msec_name_doc,
        _max_ts_delay_msec_doc,
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
def estimate_ts_delay(self: 'Source', other: 'Source',
                      input_field1_name: str, input_field2_name: str,
                      **kwargs) -> 'Source':
    """
    Given two time series of ticks, computes how much delay the second series has in relation to the first.
    A negative delay should be interpreted as the first series being delayed instead.

    The two series do not necessarily have to be identical with respect to the delay,
    nor do they have to represent the same quantity (i.e. be of the same magnitude).
    The only requirement to get meaningful results is that the two series be (linearly) correlated.

    Output ticks always have 2 fields:

        * *DELAY_MS*, which is the computed delay in milliseconds, and
        * *CORRELATION*, which is the Zero-Normalized Cross-Correlation of the two time series
          after that delay is applied.

    Parameters
    ----------
    other: Source
        The other source.
    input_field1_name: str
        The name of the compared field from the first source.
    input_field2_name: str
        The name of the compared field from the second source.

    Returns
    -------
    :class:`Source`

    See Also
    --------
    **ESTIMATE_TS_DELAY** OneTick event processor

    Examples
    --------

    Calculating delay between the same sources will result in DELAY_MSEC equal to 0.0 and CORRELATION equal to 1.0:
    (Note that correlation method may return NaN values for smaller buckets):

    .. testcode::
       :skipif: not otp.compatibility.is_supported_estimate_ts_delay()

       import os
       trd = otp.CSV(os.path.join(csv_path, 'trd.csv'))
       other = trd.deepcopy()
       data = trd.estimate_ts_delay(other, 'PRICE', 'PRICE', bucket_interval=10, bucket_time='start')
       df = otp.run(data, start=otp.dt(2003, 12, 1, 9), end=otp.dt(2003, 12, 1, 10))
       print(df)

    .. testoutput::

                          Time  DELAY_MSEC  CORRELATION
       0   2003-12-01 09:00:00         0.0          1.0
       1   2003-12-01 09:00:10         0.0          1.0
       2   2003-12-01 09:00:20         0.0          1.0
       3   2003-12-01 09:00:30         0.0          1.0
       4   2003-12-01 09:00:40         0.0          1.0
       ..                  ...         ...          ...
       355 2003-12-01 09:59:10         0.0          1.0
       356 2003-12-01 09:59:20         0.0          1.0
       357 2003-12-01 09:59:30         NaN          NaN
       358 2003-12-01 09:59:40         0.0          1.0
       359 2003-12-01 09:59:50         0.0          1.0

       [360 rows x 3 columns]

    Try changing timestamps of other time-series to see how delay values are changed:

    .. testcode::
       :skipif: not otp.compatibility.is_supported_estimate_ts_delay()

       import os
       trd = otp.CSV(os.path.join(csv_path, 'trd.csv'))
       other = trd.deepcopy()
       other['TIMESTAMP'] += otp.Milli(5)
       data = trd.estimate_ts_delay(other, 'PRICE', 'PRICE', bucket_interval=10, bucket_time='start')
       df = otp.run(data, start=otp.dt(2003, 12, 1, 9), end=otp.dt(2003, 12, 1, 10))
       print(df)

    .. testoutput::

                          Time  DELAY_MSEC  CORRELATION
       0   2003-12-01 09:00:00        -5.0          1.0
       1   2003-12-01 09:00:10        -5.0          1.0
       2   2003-12-01 09:00:20        -5.0          1.0
       3   2003-12-01 09:00:30        -5.0          1.0
       4   2003-12-01 09:00:40        -5.0          1.0
       ..                  ...         ...          ...
       355 2003-12-01 09:59:10        -5.0          1.0
       356 2003-12-01 09:59:20        -5.0          1.0
       357 2003-12-01 09:59:30         NaN          NaN
       358 2003-12-01 09:59:40        -5.0          1.0
       359 2003-12-01 09:59:50        -5.0          1.0

       [360 rows x 3 columns]

    Try filtering out some ticks from other time-series to see how delay and correlation values are changed:

    .. testcode::
       :skipif: not otp.compatibility.is_supported_estimate_ts_delay()

       import os
       trd = otp.CSV(os.path.join(csv_path, 'trd.csv'))
       other = trd.deepcopy()
       other = other[::2]
       data = trd.estimate_ts_delay(other, 'PRICE', 'PRICE', bucket_interval=10, bucket_time='start')
       df = otp.run(data, start=otp.dt(2003, 12, 1, 9), end=otp.dt(2003, 12, 1, 10))
       print(df)

    .. testoutput::

                          Time  DELAY_MSEC  CORRELATION
       0   2003-12-01 09:00:00         0.0     1.000000
       1   2003-12-01 09:00:10         0.0     1.000000
       2   2003-12-01 09:00:20         0.0     1.000000
       3   2003-12-01 09:00:30         0.0     0.999115
       4   2003-12-01 09:00:40     -1000.0     0.706111
       ..                  ...         ...          ...
       355 2003-12-01 09:59:10         0.0     0.983786
       356 2003-12-01 09:59:20         0.0     1.000000
       357 2003-12-01 09:59:30         NaN          NaN
       358 2003-12-01 09:59:40      -306.0     0.680049
       359 2003-12-01 09:59:50         0.0     0.752731

       [360 rows x 3 columns]
    """
    if not otp.compatibility.is_supported_estimate_ts_delay():
        raise RuntimeError('estimate_ts_delay() is not supported on this OneTick version')

    if input_field1_name not in self.schema:
        raise ValueError(f"Field '{input_field1_name}' is not in the schema of the first source.")

    if input_field2_name not in other.schema:
        raise ValueError(f"Field '{input_field2_name}' is not in the schema of the second source.")

    schema = {'DELAY_MSEC': float, 'CORRELATION': float}

    smallest_time_granularity_msec = kwargs.pop('smallest_time_granularity_msec', 1)
    max_ts_delay_msec = kwargs.pop('max_ts_delay_msec', 1000)

    # we only use this class for validation of common parameters
    class EstimateTsDelay(_Aggregation):
        EP = otq.EstimateTsDelay
        NAME = 'ESTIMATE_TS_DELAY'
        FIELDS_TO_SKIP = ['column_name', 'running', 'all_fields', 'output_field_name']

    common_ep_params = {k.lower(): v for k, v in EstimateTsDelay('TIMESTAMP', **kwargs).ep_params.items()}

    result = otp.Source(
        node=otq.EstimateTsDelay(
            input_field1_name=input_field1_name,
            input_field2_name=input_field2_name,
            smallest_time_granularity_msec=smallest_time_granularity_msec,
            max_ts_delay_msec=max_ts_delay_msec,
            **common_ep_params,
        ),
        schema=schema,
    )
    __copy_sources_on_merge_or_join(result, (self, other))
    return result
