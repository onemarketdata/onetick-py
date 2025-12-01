from typing import List, Dict, Union, Optional, Callable, TYPE_CHECKING
from onetick.py.backports import Literal

from functools import wraps
from inspect import Signature, Parameter
from onetick.py.core.column import Column
from onetick.py.core.column_operations.base import Operation, OnetickParameter
from onetick.py.core._source._symbol_param import _SymbolParamColumn
from onetick.py.types import OTPBaseTimeOffset

if TYPE_CHECKING:
    from onetick.py.core.source import Source  # hack for annotations

from onetick.py.docs.docstring_parser import Docstring
from onetick.py.docs.utils import param_doc
from onetick.py.aggregations.high_low import HighTick, LowTick


_column_doc = param_doc(
    name='column',
    str_annotation='str or Column or Operation',
    desc='''
    String with the name of the column to be aggregated or :py:class:`~onetick.py.Column` object.
    :py:class:`~onetick.py.Operation` object can also be used -- in this case
    the results of this operation for each tick are aggregated
    (see example in :ref:`common aggregation examples <aggregations_funcs>`).
    ''',
    annotation=Union[str, Column, Operation]
)

_running_doc = param_doc(
    name='running',
    desc='''
    See :ref:`Aggregation buckets guide <buckets_guide>` to see examples of how this parameter works.

    Specifies if the aggregation will be calculated as a sliding window.
    ``running`` and ``bucket_interval`` parameters determines when new buckets are created.

    * ``running`` = True

      aggregation will be calculated in a sliding window.

      * ``bucket_interval`` = N (N > 0)

        Window size will be N. Output tick will be generated when tick "enter" window (**arrival event**) and
        when "exit" window (**exit event**)

      * ``bucket_interval`` = 0

        Left boundary of window will be set to query start time. For each tick aggregation will be calculated in
        the interval [start_time; tick_t] from query start time to the tick's timestamp (inclusive).

    * ``running`` = False (default)

      buckets partition the [query start time, query end time) interval into non-overlapping intervals
      of size ``bucket_interval`` (with the last interval possibly of a smaller size).
      If ``bucket_interval`` is set to **0** a single bucket for the entire interval is created.

      Note that in non-running mode OneTick unconditionally divides the whole time interval
      into specified number of buckets.
      It means that you will always get this specified number of ticks in the result,
      even if you have less ticks in the input data.

    Default: False
    ''',
    default=False,
    annotation=bool
)

_all_fields_doc = param_doc(
    name='all_fields',
    desc="""
    See :ref:`Aggregation buckets guide <buckets_guide>` to see examples of how this parameter works.

    * ``all_fields`` = True

      output ticks include all fields from the input ticks

      * ``running`` = True

      an output tick is created only when a tick enters the sliding window

      * ``running`` = False

      fields of first tick in bucket will be used

    * ``all_fields`` = False and ``running`` = True

      output ticks are created when a tick enters or leaves the sliding window.

    * ``all_fields`` = "when_ticks_exit_window" and ``running`` = True

      output ticks are generated only for exit events, but all attributes from the exiting tick are copied over
      to the output tick and the aggregation is added as another attribute.
    """,
    default=False,
    annotation=Union[bool, str]
)
_all_fields_with_policy_doc = param_doc(
    name='all_fields',
    desc="""
    See :ref:`Aggregation buckets guide <buckets_guide>` to see examples of how this parameter works.

    - If ``all_fields`` False - output tick will have only aggregation fields.

    - If ``all_fields`` is True and ``running`` is False - additional fields will be copied from the first
      tick in the bucket to the output tick.

    - If ``all_fields`` False and ``running`` True - output ticks are created when a tick enters or leaves the
      sliding window.

    - If ``all_fields`` True and ``running`` True - an output tick is generated only for arrival events,
      but all attributes from the input tick causing an arrival event are copied over to the output tick
      and the aggregation is added as another attribute.

    - If ``all_fields`` True and ``running`` "when_ticks_exit_window" -
      an output tick is generated only for exit events,
      but all attributes from the exiting tick are copied over to the output tick
      and the aggregation is added as another attribute.

    - If ``all_fields`` set to "first", "last", "high", or "low" - explicitly set tick selection policy for all
      fields values. For "high" and "low" "PRICE" field will be selected as an input.
      Otherwise, you will get the runtime error.
      If ``all_fields`` is set to one of these values, ``running`` can't be `True`.

    - If ``all_fields`` is aggregation ``HighTick`` or ``LowTick`` - set tick selection policy for all fields values to
      "high" or "low" accordingly. But instead of "PRICE" the field selected as input will be set as aggregation's
      first parameter.
    """,
    default=False,
    str_annotation=("True, False, 'when_ticks_exit_window', 'first', 'last', 'high', 'low', "
                    ":py:func:`~onetick.py.agg.high_tick`, :py:func:`~onetick.py.agg.low_tick`"),
)
_bucket_interval_doc = param_doc(
    name='bucket_interval',
    desc="""
    Determines the length of each bucket (units depends on ``bucket_units``).

    If :py:class:`~onetick.py.Operation` of bool type is passed, acts as ``bucket_end_condition``.

    Bucket interval can also be set as a *float* value
    if ``bucket_units`` is set to *seconds*.
    Note that values less than 0.001 (1 millisecond) are not supported.

    Bucket interval can be set via some of the :ref:`datetime offset objects <datetime_offsets>`:
    :py:func:`otp.Milli <onetick.py.Milli>`, :py:func:`otp.Second <onetick.py.Second>`,
    :py:func:`otp.Minute <onetick.py.Minute>`, :py:func:`otp.Hour <onetick.py.Hour>`,
    :py:func:`otp.Day <onetick.py.Day>`, :py:func:`otp.Month <onetick.py.Month>`.
    In this case you could omit setting ``bucket_units`` parameter.

    Bucket interval can also be set with integer :py:class:`~onetick.py.core.column_operations.base.OnetickParameter`
    or :py:meth:`symbol parameter <onetick.py.core._source.symbol.SymbolType.__getitem__>`.
    """,
    default=0,
    annotation=Union[int, float, Operation, OnetickParameter, _SymbolParamColumn, OTPBaseTimeOffset],
    str_annotation=('int or float or :py:class:`~onetick.py.Operation`'
                    ' or :py:class:`~onetick.py.core.column_operations.base.OnetickParameter`'
                    ' or :py:meth:`symbol parameter <onetick.py.core._source.symbol.SymbolType.__getitem__>`'
                    ' or :ref:`datetime offset object <datetime_offsets>`'),
)
_bucket_time_doc = param_doc(
    name='bucket_time',
    desc="""
    Control output timestamp.

    * **start**

      the timestamp  assigned to the bucket is the start time of the bucket.

    * **end**

      the timestamp assigned to the bucket is the end time of the bucket.
    """,
    default="end",
    annotation=Literal["start", "end"]
)
_bucket_units_doc_kwargs = dict(
    name='bucket_units',
    desc="""
    Set bucket interval units.

    By default, if ``bucket_units`` and ``bucket_end_condition`` not specified, set to **seconds**.
    If ``bucket_end_condition`` specified, then ``bucket_units`` set to **flexible**.

    If set to **flexible** then ``bucket_end_condition`` must be set.

    Note that **seconds** bucket unit doesn't take into account daylight-saving time of the timezone,
    so you may not get expected results when using, for example, 24 * 60 * 60 seconds as bucket interval.
    In such case use **days** bucket unit instead.
    See example in :py:func:`onetick.py.agg.sum`.
    """,
    default=None,
    annotation=Optional[Literal["seconds", "ticks", "days", "months", "flexible"]]
)
_bucket_units_doc = param_doc(**_bucket_units_doc_kwargs)
_bucket_units_ob_doc = param_doc(**{
    **_bucket_units_doc_kwargs,
    # OB eps do not support 'ticks' bucket units
    'annotation': Optional[Literal["seconds", "days", "months", "flexible"]]
})

_bucket_end_condition_doc = param_doc(
    name='bucket_end_condition',
    str_annotation='condition',
    desc='''
    An expression that is evaluated on every tick. If it evaluates to "True", then a new bucket is created.
    This parameter is only used if ``bucket_units`` is set to "flexible".

    Also can be set via ``bucket_interval`` parameter by passing :py:class:`~onetick.py.Operation` object.
    ''',
    annotation=Optional[Operation],
    default=None
)
_end_condition_per_group_doc = param_doc(
    name='end_condition_per_group',
    desc='''
    Controls application of ``bucket_end_condition`` in groups.

    * ``end_condition_per_group`` = True

      ``bucket_end_condition`` is applied only to the group defined by ``group_by``

    * ``end_condition_per_group`` = False

      ``bucket_end_condition`` applied across all groups

    This parameter is only used if ``bucket_units`` is set to "flexible".

    When set to True, applies to all bucketing conditions. Useful, for example, if you need to specify ``group_by``,
    and you want to group items first, and create buckets after that.
    ''',
    annotation=bool,
    default=False
)
_boundary_tick_bucket_doc = param_doc(
    name='boundary_tick_bucket',
    desc='''
    Controls boundary tick ownership.

    * **previous**

      A tick on which ``bucket_end_condition`` evaluates to "true" belongs to the bucket being closed.

    * **new**

      tick belongs to the new bucket.

    This parameter is only used if ``bucket_units`` is set to "flexible"
    ''',
    annotation=Literal["new", "previous"],
    default="new"
)
_group_by_doc = param_doc(
    name='group_by',
    str_annotation='list, str or expression',
    desc='''
    When specified, each bucket is broken further into additional sub-buckets based on specified field values.
    If :py:class:`~onetick.py.Operation` is used then GROUP_{i} column is added. Where i is index in group_by list.
    For example, if Operation is the only element in ``group_by`` list then GROUP_0 field will be added.
    ''',
    annotation=Optional[Union[List, str, Operation]],
    default=None
)
_groups_to_display_doc = param_doc(
    name='groups_to_display',
    desc='''
    Specifies for which sub-buckets (groups) ticks should be shown for each bucket interval.
    By default **all** groups are shown at the end of each bucket interval.
    If this parameter is set to **event_in_last_bucket**, only the groups that received at least one tick
    within a given bucket interval are shown.
    ''',
    annotation=Literal["all", "previous"],
    default="all",
)
_biased_doc = param_doc(
    name='biased',
    annotation=bool,
    desc='''
    Switches between biased and unbiased standard deviation calculation.
    ''',
    default=True
)
_n_doc = param_doc(
    name='n',
    default=1,
    desc='''
    Number of ticks to output''',
    annotation=int
)
_keep_timestamp_doc = param_doc(
    name='keep_timestamp',
    desc="""
    If True, timestamps of the output ticks are the same as timestamps of the original ticks.
    Otherwise, timestamps of the output ticks are determined by bucket_time, and original timestamps
    are put in the TICK_TIME field.
    """,
    annotation=bool,
    default=True
)
_time_series_type_common = dict(
    name='time_series_type',
    desc='''
    Controls initial value for each bucket

    * **event_ts**

      only ticks from current bucket used for calculations

    * **state_ts**

      * if there is a tick in bucket with timestamp = bucket start

        only ticks in bucket used for calculation max value
      * else

        latest tick from previous bucket included in current bucket
    ''',
    annotation=Literal["event_ts", "state_ts"],
)
_time_series_type_doc = param_doc(
    default="event_ts",
    **_time_series_type_common,
)
_time_series_type_w_doc = param_doc(
    default="state_ts",
    **_time_series_type_common,
)
_selection_doc = param_doc(
    name='selection',
    desc='''
    Controls the selection of the respective beginning or trailing part of ticks.
    ''',
    annotation=Literal["first", "last"],
    default="first"
)
_side_doc = param_doc(
    name='side',
    desc="""
    Specifies whether the function is to be applied to sell orders (ASK), buy orders (BID), or both (empty).
    """,
    annotation=Literal['ASK', 'BID'],
    default=None,
)
_max_levels_doc = param_doc(
    name='max_levels',
    desc="""
    Number of order book levels (between 1 and 100_000) that need to be computed.
    If empty, all levels will be computed.
    """,
    annotation=int,
    default=None,
)
_min_levels_doc = param_doc(
    name='min_levels',
    desc="""
    Minimum number of order book levels, if so many levels are available in the book,
    that need to be included into the computation when either ``max_depth_shares`` or/and ``max_depth_for_price``
    are specified
    """,
    annotation=int,
    default=None,
)
_max_depth_shares_doc = param_doc(
    name='max_depth_shares',
    desc="""
    The total number of shares (i.e., the combined SIZE across top several levels of the book)
    that determines the number of order book levels that need to be part of the order book computation.
    If that number of levels exceeds `max_levels`, only `max_levels` levels of the book will be computed.
    The shares in excess of `max_depth_shares`, from the last included level, are not taken into account.
    """,
    annotation=int,
    default=None,
)
_max_depth_for_price_doc = param_doc(
    name='max_depth_for_price',
    desc="""
    The multiplier, product of which with the price at the top level of the book determines maximum price distance
    from the top of the book for the levels that are to be included into the book.
    In other words, only bids at <top_price>*(1-`max_depth_for_price`) and above
    and only asks of <top_price>*(1+`max_depth_for_price`) and less will be returned.
    If the number of the levels that are to be included into the book, according to this criteria,
    exceeds `max_levels`, only `max_levels` levels of the book will be returned.
    """,
    annotation=float,
    default=None,
)
_max_spread_doc = param_doc(
    name='max_spread',
    desc="""
    An absolute value, price levels with price that satisfies ``abs(<MID price> - <order price>) <= max_spread/2``
    contribute to computed book. If ``max_spread`` is specified, ``side`` should not be specified.
    Empty book is returned when one side is empty.
    """,
    annotation=float,
    default=None,
)
_min_levels_doc = param_doc(
    name='min_levels',
    desc="""
    Minimum number of order book levels, if so many levels are available in the book, that need to be included
    into the computation when either `max_depth_shares` or/and `max_depth_for_price` are specified.
    """,
    annotation=int,
    default=None,
)
_max_initialization_days_doc = param_doc(
    name='max_initialization_days',
    desc="""
    This parameter specifies how many days back book event processors should go in order to find
    the latest full state of the book.
    The query will not go back resulting number of days if it finds initial book state earlier.
    When book event processors are used after VIRTUAL_OB EP, this parameter should be set to 0.
    When set, this parameter takes precedence over the configuration parameter BOOKS.MAX_INITIALIZATION_DAYS.
    """,
    annotation=int,
    default=1,
)
_book_uncross_method_doc = param_doc(
    name='book_uncross_method',
    desc="""
    When set to "REMOVE_OLDER_CROSSED_LEVELS", all ask levels that have price lower or equal to
    the price of a new bid tick get removed from the book, and all bid levels that have price higher or equal
    to the price of a new ask tick get removed from the book.
    """,
    annotation=Literal['REMOVE_OLDER_CROSSED_LEVELS'],
    default=None,
)
_dq_events_that_clear_book_doc = param_doc(
    name='dq_events_that_clear_book',
    desc="""
    A list of names of data quality events arrival of which should clear the order book.
    """,
    annotation=List[str],
    default=None,
)
_best_ask_price_field_doc = param_doc(
    name='best_ask_price_field',
    desc="""
    If specified, this parameter represents the name of the field value of which represents the lowest ask price
    starting from which the book ask size is to be computed.
    This value would also be used as the top price, relative to which ``max_depth_for_price`` would be computed.
    """,
    annotation=Union[str, Column],
    default=None,
)
_best_bid_price_field_doc = param_doc(
    name='best_bid_price_field',
    desc="""
    If specified, this parameter represents the name of the field value of which represents the highest bid price
    starting from which the book bid size is to be computed.
    This value would also be used as the top price, relative to which ``max_depth_for_price`` would be computed.
    """,
    annotation=Union[str, Column],
    default=None,
)
_bucket_interval_ob_num_levels_doc = param_doc(
    name='bucket_interval',
    desc="""
    Determines the length of each bucket in seconds.

    Bucket interval can be set via :ref:`datetime offset objects <datetime_offsets>`
    like :py:func:`otp.Second <onetick.py.Second>`, :py:func:`otp.Minute <onetick.py.Minute>`,
    :py:func:`otp.Hour <onetick.py.Hour>`, :py:func:`otp.Day <onetick.py.Day>`.
    In this case it will be converted to seconds.
    """,
    default=0,
    annotation=Union[int, OTPBaseTimeOffset],
    str_annotation='int or :ref:`datetime offset object <datetime_offsets>`',
)
_identify_source_doc = param_doc(
    name='identify_source',
    desc="""
    When this parameter is set to "true" and the input stream is fed through the VIRTUAL_OB event processor
    (with the QUOTE_SOURCE_FIELDS parameter specified) and `group_by` is not set to be "SOURCE"
    it will separate a tick with the same price from different sources into multiple ticks.
    The parameter can also be used when merging ticks from multiple feeds.
    Each feed going into the merge would need an ADD_FIELD EP source value set for the VALUE parameter,
    where the value would be different for each leg.
    """,
    annotation=bool,
    default=False,
)
_show_full_detail_doc = param_doc(
    name='show_full_detail',
    desc="""
    When set to "true" and if the state key of the input ticks consists of some fields besides PRICE,
    output ticks will contain all fields from the input ticks for each price level.
    When set to "false" only PRICE, UPDATE_TIME, SIZE, LEVEL, and BUY_SELL_FLAG fields will be populated.
    Note: setting this flag to "true" has no effect on a time series that does not have a state key.
    """,
    annotation=bool,
    default=False,
)
_show_only_changes_doc = param_doc(
    name='show_only_changes',
    desc="""
    When set to true, the output stream carries only changes to the book. The representation is as follows:
      * Changed and added levels are represented by themselves.
      * Deleted levels are shown with a size and level of zero.

    As with other modes, correct detection of update boundaries may require setting the `book_delimiters` option.
    """,
    annotation=bool,
    default=False,
)
_book_delimiters_doc = param_doc(
    name='book_delimiters',
    desc="""
    When set to "D" an extra tick is created after each book.
    Also, an additional column, called DELIMITER, is added to output ticks.
    The extra tick has values of all fields set to the defaults (0,NaN,""),
    except the delimiter field, which is set to "D."
    All other ticks have the DELIMITER set to zero (0).
    """,
    annotation=Literal['D'],
    default=None,
)
_state_key_max_inactivity_sec_doc = param_doc(
    name='state_key_max_inactivity_sec',
    desc="""
    If set, specifies in how many seconds after it was added
    a given state key should be automatically removed from the book.
    """,
    annotation=int,
    default=None,
)
_size_max_fractional_digits_doc = param_doc(
    name='size_max_fractional_digits',
    desc="""
    Specifies maximum number of digits after dot in SIZE, if SIZE can be fractional.
    """,
    annotation=int,
    default=0,
)
_include_market_order_ticks_doc = param_doc(
    name='include_market_order_ticks',
    desc="""
    If set, market order ticks (they have price NaN) are included into the order book,
    and are at the order book's top level.

        Default is False.
    """,
    annotation=bool,
    default=None,
)
_query_fun_doc = param_doc(
    name='query_fun',
    desc="""
    Function that takes :class:`~onetick.py.Source` as a parameter,
    applies some aggregation logic to it
    and returns :class:`~onetick.py.Source` as a result.
    Note that currently only methods that support dynamic symbol change
    could be used in the provided function.
    For example, :meth:`~onetick.py.Source.rename` can't be used.
    If you try to use such methods here, you will get an error during runtime.
    """,
    annotation=Callable,
)
_bucket_delimiter_doc = param_doc(
    name='bucket_delimiter',
    desc="""
    When set to ``True`` an extra tick is created after each bucket.
    Also, an additional column, called DELIMITER, is added to output ticks.
    The extra tick has values of all fields set to the defaults (0,NaN,""),
    except the delimiter field, which is set to "D"
    All other ticks have the DELIMITER set to string zero "0".
    """,
    annotation=bool,
    default=False,
)
_large_ints_doc = param_doc(
    name='large_ints',
    desc="""
    This parameter should be set
    if the input field of this aggregation may contain integer values that consist of 15 digits or more.

    Such large integer values cannot be represented by the double type without precision errors
    and thus require special handling.

    If set to **True** , the input field is expected to be a 64-bit integer number.
    The output field will also have 64-bit integer type.
    When no tick belongs to a given time bucket, the output value is set to a minimum of 64-bit integer.

    When this parameter set to :py:class:`onetick.py.adaptive`, the aggregation behaves the same way
    as when this parameter is set to True when the input field is a 64-bit integer type,
    and the same way as when this parameter is set to **False** when the input field is not a 64-bit integer type.
    """,
    annotation=bool,
    str_annotation="bool or :py:class:`onetick.py.adaptive`",
    default=False,
)
_null_int_val_doc = param_doc(
    name='null_int_val',
    desc="""
    The value of this parameter is considered to be the equivalent of ``NaN``
    when ``large_ints`` is set to ``True`` or
    when ``large_ints`` is set to :py:class:`onetick.py.adaptive` and the input field is a 64-bit integer type.
    """,
    annotation=int,
    default=0,
)
_skip_tick_if_doc = param_doc(
    name='skip_tick_if',
    desc="""
    If value of the input field is equal to the value in this parameter,
    this tick is ignored in the aggregation computation.

    This parameter is currently only supported for numeric fields.
    """,
    annotation=Optional[int],
    default=None,
)
_default_tick_doc = param_doc(
    name='default_tick',
    desc="""
    Mapping of input stream field names to some values.
    When set, exactly one tick with specified default values will be created for each empty bucket.
    If default value is specified as ``None``, then fields are initialized to the corresponding "zero" values:
    0 for integer types, NaN for doubles, empty string for string types, etc.
    """,
    annotation=Optional[dict],
    default=None,
)
_decay_doc = param_doc(
    name='decay',
    desc="""
    Weight decay. If **decay_value_type** is set to ``lambda``,
    **decay** provides the value of the **Lambda** variable in the aforementioned formula.
    Otherwise, if **decay_value_type** is set to ``half_life_index``, **decay** specifies the necessary number
    of consecutive ticks, the first one of which would have twice less the weight of the last one.
    The **Lambda** value is then calculated using this number.
    """,
    annotation=float,
)
_decay_value_type_common = dict(
    name='decay_value_type',
    desc="""
    The decay value can specified either directly or indirectly, controlled respectively by
    **lambda** and **half_life_index** values of this parameter.
    """,
    annotation=Literal['lambda', 'half_life_index'],
)
_decay_value_type_doc = param_doc(
    default='lambda',
    **_decay_value_type_common,
)
_decay_value_type_hl_doc = param_doc(
    default='half_life_index',
    **_decay_value_type_common,
)
_degree_doc = param_doc(
    name='degree',
    annotation=int,
    desc='''
    The order (degree) of the standardized moment to compute, denoted as k in the description above.
    ''',
    default=3,
)
_weight_field_name_doc = param_doc(
    name='weight_field_name',
    str_annotation='Optional, str or Column',
    annotation=Optional[Union[str, Column]],
    desc='''
    The name of the field that contains the current value of weight for a member of the portfolio
    that contributed the tick.

    You can also specify weight through the value of symbol parameter ``WEIGHT``.

    If ``weight_field_name`` is specified, all ticks should have the field pointed by this parameter and the value
    of this field is used as the weight.

    If weights are not specified in any of these ways, and you are running a single-stage query,
    the weights take the default value ``1``.
    ''',
    default='',
)
_weight_multiplier_field_name_doc = param_doc(
    name='weight_multiplier_field_name',
    desc="""
    Name of the field, value from which is used for multiplying portfolio value result.
    """,
    annotation=str,
    default='',
)
_portfolio_side_doc = param_doc(
    name='side',
    desc="""
    When set to **long**, the price of the portfolio is computed only for the input time series with ``weight > 0``.

    When set to **short**, the price of the portfolio is computed only for the input time series with ``weight < 0``.

    When set to **both**, the price of the portfolio is computed for all input time series.
    """,
    annotation=Literal['long', 'short', 'both'],
    default='both',
)
_weight_type_doc = param_doc(
    name='weight_type',
    desc="""
    When set to ``absolute``, the portfolio price is computed as the sum of ``input_field_value*weight``
    across all members of the portfolio.

    When set to ``relative``, the portfolio price is computed as the sum of
    ``input_field_value*weight/sum_of_all_weights`` across all members of the portfolio.
    """,
    annotation=Literal['absolute', 'relative'],
    default='absolute',
)
_portfolios_query_doc = param_doc(
    name='portfolios_query',
    desc="""
    A mandatory parameter that the specifies server-side `.otq` file that is expected to return
    mandatory columns ``PORTFOLIO_NAME`` and ``SYMBOL_NAME``,
    as well as an optional columns ``WEIGHT``, ``FX_SYMBOL_NAME`` and ``FX_MULTIPLY``.

    For a local OneTick server you can pass `otp.Source` objects.
    """,
    str_annotation='str, :class:`Source`',
)
_portfolios_query_params_doc = param_doc(
    name='portfolios_query_params',
    desc="""
    An optional parameter that specifies parameters of the query specified in `portfolios_query`.
    """,
    annotation=Union[str, Dict[str, str]],
    default='',
)
_portfolio_value_field_name_doc = param_doc(
    name='portfolio_value_field_name',
    desc="""
    List of the names (string with comma-separated list or ``list`` of strings/``Columns``) of the output fields
    which contain computed values of the portfolio.

    The number of the field names must match the number of the field names listed in the `columns` parameter.
    """,
    annotation=Union[str, List[Union[str, Column]]],
    default='VALUE',
)
_columns_portfolio_doc = param_doc(
    name='columns',
    str_annotation='str or list of Column or str',
    desc='''
    A list of the names of the input fields for which portfolio value is computed.

    Could be set as a comma-separated list of the names or ``list`` of name strings/``Columns`` objects.
    ''',
    annotation=Union[str, List[Union[str, Column]]],
    default='PRICE',
)
_symbols_doc = param_doc(
    name='symbols',
    desc="""
    Symbol(s) from which data should be taken.
    """,
    str_annotation=('str, list of str, :class:`Source`, :class:`query`, :py:func:`eval query <onetick.py.eval>`, '
                    ':py:class:`onetick.query.GraphQuery`.'),
    default=None,
)
_interest_rate_doc = param_doc(
    name='interest_rate',
    desc='''
    The risk-free interest rate.

    Could be set via a tick field, by specifying name of that field as string or
    passing :py:class:`~onetick.py.Column` object.
    ''',
    annotation=Optional[Union[int, float, str, Column]],
    default=None,
)
_price_field_doc = param_doc(
    name='price_field',
    desc='''
    The name of the field carrying the price value.
    ''',
    annotation=Union[str, Column],
    default='PRICE',
)
_option_price_field_doc = param_doc(
    name='option_price_field',
    desc='''
    The name of the field carrying the option price value.
    ''',
    annotation=Union[str, Column],
    default='OPTION_PRICE',
)
_method_doc = param_doc(
    name='method',
    desc='''
    Allowed values are ``newton``, ``newton_with_fallback`` and ``bisections``.

    Choose between ``newton`` and ``bisections`` for finding successively better approximations
    to the implied volatility value.

    Choose ``newton_with_fallback`` to automatically fall back to ``bisections`` method
    when ``newton`` fails to converge.
    ''',
    annotation=str,
    default='newton',
)
_precision_doc = param_doc(
    name='precision',
    desc='''
    Precision of the implied volatility value.
    ''',
    annotation=float,
    default='1.0e-5',
)
_value_for_non_converge_doc = param_doc(
    name='value_for_non_converge',
    desc='''
    Allowed values are ``nan_val`` and ``closest_found_val``, where ``closest_found_val`` stands
    for the volatility value for which the difference between calculated option price and input option price is minimal.

    Choose between ``nan_val`` and ``closest_found_val`` as implied volatility value,
    when the root-finding method does not converge within the specified precision.
    ''',
    annotation=str,
    default='nan_val',
)
_option_type_field_doc = param_doc(
    name='option_type_field',
    desc='''
    Specifies name of the field, which carries the option type (either **CALL** or **PUT**).
    ''',
    annotation=Union[str, Column],
    default='',
)
_strike_price_field_doc = param_doc(
    name='strike_price_field',
    desc='''
    Specifies name of the field, which carries the strike price of the option.
    ''',
    annotation=Union[str, Column],
    default='',
)
_days_in_year_doc = param_doc(
    name='days_in_year',
    desc='''
    Specifies number of days in a year (say, 365 or 252 (business days, etc.).
    Used with ``days_till_expiration`` parameter to compute the fractional years till expiration.
    ''',
    annotation=int,
    default=365,
)
_days_till_expiration_field_doc = param_doc(
    name='days_till_expiration_field',
    desc='''
    Specifies name of the field, which carries number of days till expiration of the option.
    ''',
    annotation=Union[str, Column],
    default='',
)
_expiration_date_field_doc = param_doc(
    name='expiration_date_field',
    desc='''
    Specifies name of the field, which carries the expiration date of the option, in **YYYYMMDD** format.
    ''',
    annotation=Union[str, Column],
    default='',
)


class DocMetaclass(type):
    def __new__(mcs, name, bases, attrs, parameters: Optional[list] = None):
        cls = super().__new__(mcs, name, bases, attrs)
        doc = None
        if '__init__' in attrs and cls.__init__.__doc__:    # type: ignore
            doc = cls.__init__.__doc__  # type: ignore
        elif '__doc__' in attrs:
            doc = attrs['__doc__']
        if doc and parameters:
            doc = Docstring(doc)
            for param in parameters:
                doc['Parameters'] = param
            cls.__init__.__doc__ = doc.build()  # type: ignore

        return cls


def copy_method(obj, mimic=True, drop_examples=False):

    """
    Decorator to copy aggregation function as method

    We assume that this decorator will be used only with aggregations.

    Updates (same way as for dict) `obj` docstring with decorated function docstring
    Updates decorated function signature: [self] + `obj` signature
    if mimic is True - won't execute decorated function, will execute `obj` and apply self instead
    if mimic is False - will execute decorated function as is

    Parameters
    ----------
    obj:
        donor aggregation function
    mimic: bool, default=True
        flag to execute decorated function or not
    drop_examples: bool
        Can be used to drop examples from ``obj`` docstring.
    """

    doc = obj.__doc__ or ''
    params = [Parameter(name='self',
                        kind=Parameter.POSITIONAL_OR_KEYWORD)] + list(Signature.from_callable(obj).parameters.values())
    doc = Docstring(doc)
    if drop_examples and 'Examples' in doc.docstring:
        doc.docstring.pop('Examples')

    def _decorator(fun):
        @wraps(fun)
        def _inner(self, *args, **kwargs):
            if mimic:
                agg = obj(*args, **kwargs)
                return agg.apply(self)
            else:
                return fun(self, *args, **kwargs)
        fun_doc = fun.__doc__ or ''
        fun_doc = Docstring(fun_doc)
        doc.update(fun_doc)
        _inner.__signature__ = Signature(parameters=params, return_annotation='Source')
        _inner.__doc__ = doc.build()
        return _inner
    return _decorator


def copy_signature(obj, add_self=False, drop_parameters=None, return_annotation='Source'):
    """
    Decorator that copies signature of the callable ``obj`` to the decorated function.
    """
    drop_parameters = drop_parameters or []
    obj_parameters = list(
        param for param_name, param in Signature.from_callable(obj).parameters.items()
        if param_name not in drop_parameters
    )
    params = []
    if add_self:
        params.append(
            Parameter(name='self', kind=Parameter.POSITIONAL_ONLY)
        )
    params.extend(obj_parameters)

    def _decorator(fun):
        @wraps(fun)
        def _inner(*args, **kwargs):
            return fun(*args, **kwargs)
        _inner.__signature__ = Signature(parameters=params, return_annotation=return_annotation)
        return _inner
    return _decorator
