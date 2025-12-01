from typing import TYPE_CHECKING, List, Optional, Union
from onetick.py.backports import Literal

from abc import ABC

import onetick.py as otp
from onetick.py.otq import otq
from onetick.py import types as ott

if TYPE_CHECKING:
    from onetick.py.core.source import Source   # hack for annotations
from onetick.py.core.column import _Column
from onetick.py.compatibility import is_supported_otq_ob_summary, is_max_spread_supported
from ._base import _Aggregation, get_seconds_from_time_offset
from ._docs import (_running_doc,
                    _bucket_interval_doc,
                    _bucket_time_doc,
                    _bucket_units_ob_doc,
                    _bucket_end_condition_doc,
                    _end_condition_per_group_doc,
                    _group_by_doc,
                    _groups_to_display_doc,
                    _side_doc,
                    _max_levels_doc,
                    _min_levels_doc,
                    _max_depth_shares_doc,
                    _max_depth_for_price_doc,
                    _max_spread_doc,
                    _book_uncross_method_doc,
                    _dq_events_that_clear_book_doc,
                    _best_ask_price_field_doc,
                    _best_bid_price_field_doc,
                    _bucket_interval_ob_num_levels_doc,
                    _identify_source_doc,
                    _show_full_detail_doc,
                    _show_only_changes_doc,
                    _book_delimiters_doc,
                    _max_initialization_days_doc,
                    _state_key_max_inactivity_sec_doc,
                    _size_max_fractional_digits_doc,
                    _include_market_order_ticks_doc)


OB_SNAPSHOT_DOC_PARAMS = [
    _running_doc,
    _bucket_interval_doc, _bucket_time_doc, _bucket_units_ob_doc,
    _bucket_end_condition_doc, _end_condition_per_group_doc, _group_by_doc, _groups_to_display_doc,
    _side_doc, _max_levels_doc, _max_depth_shares_doc, _max_depth_for_price_doc, _max_spread_doc,
    _book_uncross_method_doc, _dq_events_that_clear_book_doc, _identify_source_doc,
    _show_full_detail_doc, _show_only_changes_doc, _book_delimiters_doc,
    _max_initialization_days_doc, _state_key_max_inactivity_sec_doc,
    _size_max_fractional_digits_doc,
    _include_market_order_ticks_doc,
]
OB_SNAPSHOT_WIDE_DOC_PARAMS = [
    _running_doc,
    _bucket_interval_doc, _bucket_time_doc, _bucket_units_ob_doc, _bucket_end_condition_doc,
    _end_condition_per_group_doc, _group_by_doc, _groups_to_display_doc,
    _max_levels_doc, _max_depth_shares_doc, _max_depth_for_price_doc, _max_spread_doc,
    _book_uncross_method_doc, _dq_events_that_clear_book_doc,
    _book_delimiters_doc,
    _max_initialization_days_doc, _state_key_max_inactivity_sec_doc,
    _size_max_fractional_digits_doc,
    _include_market_order_ticks_doc,
]
OB_SNAPSHOT_FLAT_DOC_PARAMS = [
    _running_doc,
    _bucket_interval_doc, _bucket_time_doc, _bucket_units_ob_doc, _bucket_end_condition_doc,
    _end_condition_per_group_doc, _group_by_doc, _groups_to_display_doc,
    _max_levels_doc,
    _book_uncross_method_doc, _dq_events_that_clear_book_doc,
    _show_full_detail_doc,
    _max_initialization_days_doc, _state_key_max_inactivity_sec_doc,
    _size_max_fractional_digits_doc,
    _include_market_order_ticks_doc,
]
OB_SUMMARY_DOC_PARAMS = [
    _running_doc,
    _bucket_interval_doc, _bucket_time_doc, _bucket_units_ob_doc,
    _bucket_end_condition_doc, _end_condition_per_group_doc, _group_by_doc, _groups_to_display_doc,
    _side_doc, _max_levels_doc, _min_levels_doc, _max_depth_shares_doc, _max_depth_for_price_doc, _max_spread_doc,
    _book_uncross_method_doc, _dq_events_that_clear_book_doc, _max_initialization_days_doc,
    _state_key_max_inactivity_sec_doc, _size_max_fractional_digits_doc,
    _include_market_order_ticks_doc,
]

OB_SIZE_DOC_PARAMS = [
    _running_doc,
    _bucket_interval_doc, _bucket_time_doc, _bucket_units_ob_doc,
    _bucket_end_condition_doc, _end_condition_per_group_doc, _group_by_doc, _groups_to_display_doc,
    _side_doc, _max_levels_doc, _max_depth_for_price_doc, _max_spread_doc,
    _book_uncross_method_doc, _dq_events_that_clear_book_doc, _max_initialization_days_doc,
    _best_ask_price_field_doc, _best_bid_price_field_doc,
]

OB_VWAP_DOC_PARAMS = [
    _running_doc,
    _bucket_interval_doc, _bucket_time_doc, _bucket_units_ob_doc,
    _bucket_end_condition_doc, _end_condition_per_group_doc, _group_by_doc, _groups_to_display_doc,
    _side_doc, _max_levels_doc, _max_depth_shares_doc,
    _book_uncross_method_doc, _dq_events_that_clear_book_doc, _max_initialization_days_doc,
]

OB_NUM_LEVELS_DOC_PARAMS = [
    _running_doc,
    _bucket_interval_ob_num_levels_doc, _side_doc,
    _book_uncross_method_doc, _dq_events_that_clear_book_doc, _max_initialization_days_doc,
]


class _OrderBookAggregation(_Aggregation, ABC):
    FIELDS_TO_SKIP = ['column_name', 'all_fields', 'boundary_tick_bucket', 'output_field_name']
    FIELDS_MAPPING = dict(_Aggregation.FIELDS_MAPPING, **{
        'side': 'SIDE',
        'max_levels': 'MAX_LEVELS',
        'max_depth_shares': 'MAX_DEPTH_SHARES',
        'max_depth_for_price': 'MAX_DEPTH_FOR_PRICE',
        'max_spread': 'MAX_SPREAD',
        'max_initialization_days': 'MAX_INITIALIZATION_DAYS',
        'book_uncross_method': 'BOOK_UNCROSS_METHOD',
        'dq_events_that_clear_book': 'DQ_EVENTS_THAT_CLEAR_BOOK',
    })
    FIELDS_DEFAULT = dict(_Aggregation.FIELDS_DEFAULT, **{
        'side': None,
        'max_levels': None,
        'max_depth_shares': None,
        'max_depth_for_price': None,
        'max_spread': None,
        'max_initialization_days': 1,
        'book_uncross_method': None,
        'dq_events_that_clear_book': None,
    })
    _validations_to_skip = ['running_all_fields']

    def __init__(self,
                 *args,
                 side: Optional[Literal['ASK', 'BID']] = None,
                 max_levels: Optional[int] = None,
                 max_depth_shares: Optional[int] = None,
                 max_depth_for_price: Optional[float] = None,
                 max_spread: Optional[float] = None,
                 max_initialization_days: int = 1,
                 book_uncross_method: Optional[Literal['REMOVE_OLDER_CROSSED_LEVELS']] = None,
                 dq_events_that_clear_book: Optional[List[str]] = None,
                 **kwargs):
        self.side = side
        self.max_levels = max_levels
        self.max_depth_shares = max_depth_shares
        self.max_depth_for_price = max_depth_for_price
        self.max_spread = max_spread
        self.max_initialization_days = max_initialization_days
        self.book_uncross_method = book_uncross_method
        self.dq_events_that_clear_book = ','.join(dq_events_that_clear_book) if dq_events_that_clear_book else None
        self.bound_symbols = None
        self._validate_ob_input_columns = True

        super().__init__(_Column('TIMESTAMP'), *args, **kwargs)

    def _param_validation(self):
        super()._param_validation()
        book_uncross_methods = (None, 'REMOVE_OLDER_CROSSED_LEVELS')
        if self.book_uncross_method not in book_uncross_methods:
            raise ValueError(
                f"Wrong value for parameter 'book_uncross_method': '{self.book_uncross_method}'. "
                f"Possible values: {book_uncross_methods}."
            )
        valid_units = ("seconds", "days", "months", "flexible")
        if self.bucket_units not in valid_units:
            raise ValueError("'bucket_units' can be one of the following: "
                             f"'{', '.join(valid_units)}'; however, '{self.bucket_units}' was passed")

        if self.max_spread is not None:
            if self.side:
                raise ValueError('Parameters `max_spread` and `side` shouldn\'t be specified both at the same time')

            if not is_max_spread_supported():
                raise RuntimeError('Parameter `max_spread` is not supported on this OneTick version')

    def disable_ob_input_columns_validation(self):
        self._validate_ob_input_columns = False

    def validate_input_columns(self, src: 'Source'):
        super().validate_input_columns(src)

        if not self._validate_ob_input_columns:
            return

        if any([
            not {'BUY_SELL_FLAG', 'PRICE', 'SIZE'}.issubset(src.schema),
            'UPDATE_TIME' not in src.schema and 'DELETED_TIME' not in src.schema
        ]):
            raise TypeError(f"Aggregation `{self.NAME}` need these columns: "
                            f"BUY_SELL_FLAG, PRICE, SIZE and (UPDATE_TIME or DELETED_TIME)")

    def to_ep(self, *args, **kwargs):
        ob_ep = super().to_ep(*args, **kwargs)
        if self.bound_symbols:
            ob_ep = ob_ep.symbols(self.bound_symbols)

        return ob_ep

    def set_bound_symbols(self, bound_symbols=None):
        if isinstance(bound_symbols, str):
            bound_symbols = [bound_symbols]

        self.bound_symbols = bound_symbols


class ObSnapshot(_OrderBookAggregation):
    NAME = 'OB_SNAPSHOT'
    EP = otq.ObSnapshot

    FIELDS_MAPPING = dict(_OrderBookAggregation.FIELDS_MAPPING, **{
        'identify_source': 'IDENTIFY_SOURCE',
        'show_full_detail': 'SHOW_FULL_DETAIL',
        'show_only_changes': 'SHOW_ONLY_CHANGES',
        'book_delimiters': 'BOOK_DELIMITERS',
        'state_key_max_inactivity_sec': 'STATE_KEY_MAX_INACTIVITY_SEC',
        'size_max_fractional_digits': 'SIZE_MAX_FRACTIONAL_DIGITS',
        'include_market_order_ticks': 'INCLUDE_MARKET_ORDER_TICKS',
    })
    FIELDS_DEFAULT = dict(_OrderBookAggregation.FIELDS_DEFAULT, **{
        'identify_source': False,
        'show_full_detail': False,
        'show_only_changes': False,
        'book_delimiters': None,
        'state_key_max_inactivity_sec': None,
        'size_max_fractional_digits': 0,
        'include_market_order_ticks': None,
    })

    def __init__(self,
                 *args,
                 identify_source: bool = False,
                 show_full_detail: bool = False,
                 show_only_changes: bool = False,
                 book_delimiters: Optional[Literal['D']] = None,
                 state_key_max_inactivity_sec: Optional[int] = None,
                 size_max_fractional_digits: int = 0,
                 include_market_order_ticks: Optional[bool] = None,
                 **kwargs):
        self.identify_source = identify_source
        self.show_full_detail = show_full_detail
        self.show_only_changes = show_only_changes
        self.book_delimiters = book_delimiters
        self.state_key_max_inactivity_sec = state_key_max_inactivity_sec
        self.size_max_fractional_digits = size_max_fractional_digits
        self.include_market_order_ticks = include_market_order_ticks
        # we don't want to set hard limit on the output of order book aggregations
        if self.show_full_detail:
            kwargs['all_fields'] = True
        self._size_type = int
        if self.size_max_fractional_digits > 0:
            self._size_type = float  # type: ignore[assignment]
        super().__init__(*args, **kwargs)

    def _param_validation(self):
        super()._param_validation()
        if self.include_market_order_ticks is not None:
            if 'include_market_order_ticks' not in self.EP.Parameters.list_parameters():
                raise ValueError("Parameter 'include_market_order_ticks' is not supported on this OneTick API version")
            otp.compatibility.is_include_market_order_ticks_supported(
                throw_warning=True,
                feature_name="parameter 'include_market_order_ticks'",
            )

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None) -> dict:
        schema = {
            'PRICE': float,
            'SIZE': self._size_type,
            'LEVEL': int,
            'UPDATE_TIME': otp.nsectime,
            'BUY_SELL_FLAG': int,
        }
        if self.book_delimiters:
            schema['DELIMITER'] = str
        return schema


class ObSnapshotWide(ObSnapshot):
    NAME = 'OB_SNAPSHOT_WIDE'
    EP = otq.ObSnapshotWide

    FIELDS_TO_SKIP = [
        *ObSnapshot.FIELDS_TO_SKIP, 'side', 'identify_source', 'show_full_detail', 'show_only_changes'
    ]

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None) -> dict:
        schema = {
            'BID_PRICE': float,
            'BID_SIZE': self._size_type,
            'BID_UPDATE_TIME': otp.nsectime,
            'ASK_PRICE': float,
            'ASK_SIZE': self._size_type,
            'ASK_UPDATE_TIME': otp.nsectime,
            'LEVEL': int,
        }
        if self.book_delimiters:
            schema['DELIMITER'] = str
        return schema


class ObSnapshotFlat(ObSnapshot):
    NAME = 'OB_SNAPSHOT_FLAT'
    EP = otq.ObSnapshotFlat

    FIELDS_TO_SKIP = [
        *ObSnapshot.FIELDS_TO_SKIP,
        'side', 'identify_source', 'show_only_changes',
        'book_delimiters', 'max_depth_shares', 'max_depth_for_price', 'max_spread',
    ]

    def validate_input_columns(self, src: 'Source'):
        super().validate_input_columns(src)
        if self.max_levels is None or self.max_levels < 1 or self.max_levels > 100_000:
            raise ValueError(f"Parameter 'max_levels' must be set in aggregation `{self.NAME}`"
                             f" and must be between 1 and 100000.")

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None) -> dict:
        schema = {}
        assert self.max_levels is not None
        for level in range(1, self.max_levels + 1):
            schema.update({
                f'BID_PRICE{level}': float,
                f'BID_SIZE{level}': self._size_type,
                f'BID_UPDATE_TIME{level}': otp.nsectime,
                f'ASK_PRICE{level}': float,
                f'ASK_SIZE{level}': self._size_type,
                f'ASK_UPDATE_TIME{level}': otp.nsectime,
            })
        return schema


class ObSummary(_OrderBookAggregation):
    NAME = 'OB_SUMMARY'

    # Will be set later, to prevent error while importing this module with outdated onetick
    EP = None

    FIELDS_MAPPING = dict(_OrderBookAggregation.FIELDS_MAPPING, **{
        'min_levels': 'MIN_LEVELS',
        'state_key_max_inactivity_sec': 'STATE_KEY_MAX_INACTIVITY_SEC',
        'size_max_fractional_digits': 'SIZE_MAX_FRACTIONAL_DIGITS',
        'include_market_order_ticks': 'INCLUDE_MARKET_ORDER_TICKS',
    })
    FIELDS_DEFAULT = dict(_OrderBookAggregation.FIELDS_DEFAULT, **{
        'min_levels': None,
        'state_key_max_inactivity_sec': None,
        'size_max_fractional_digits': 0,
        'include_market_order_ticks': None,
    })

    def __init__(self,
                 *args,
                 min_levels: Optional[int] = None,
                 state_key_max_inactivity_sec: Optional[int] = None,
                 size_max_fractional_digits: int = 0,
                 include_market_order_ticks: Optional[bool] = None,
                 **kwargs):
        if is_supported_otq_ob_summary():
            self.EP = otq.ObSummary
        else:
            raise RuntimeError("Used onetick installation not support onetick.query.ObSummary")

        self.min_levels = min_levels
        self.state_key_max_inactivity_sec = state_key_max_inactivity_sec
        self.size_max_fractional_digits = size_max_fractional_digits
        self.include_market_order_ticks = include_market_order_ticks
        self._size_type = int
        if self.size_max_fractional_digits > 0:
            self._size_type = float   # type: ignore[assignment]
        super().__init__(*args, **kwargs)

    def _param_validation(self):
        super()._param_validation()
        if self.include_market_order_ticks is not None:
            if 'include_market_order_ticks' not in self.EP.Parameters.list_parameters():
                raise ValueError("Parameter 'include_market_order_ticks' is not supported on this OneTick API version")
            otp.compatibility.is_include_market_order_ticks_supported(
                throw_warning=True,
                feature_name="parameter 'include_market_order_ticks'",
            )

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None) -> dict:
        schema = {
            'BID_SIZE': self._size_type,
            'BID_VWAP': float,
            'BEST_BID_PRICE': float,
            'WORST_BID_PRICE': float,
            'NUM_BID_LEVELS': int,
            'ASK_SIZE': self._size_type,
            'ASK_VWAP': float,
            'BEST_ASK_PRICE': float,
            'WORST_ASK_PRICE': float,
            'NUM_ASK_LEVELS': int,
        }
        return schema


class ObSize(_OrderBookAggregation):
    NAME = 'OB_SIZE'
    EP = otq.ObSize

    FIELDS_MAPPING = dict(_OrderBookAggregation.FIELDS_MAPPING, **{
        'min_levels': 'MIN_LEVELS',
        'best_ask_price_field': 'BEST_ASK_PRICE_FIELD',
        'best_bid_price_field': 'BEST_BID_PRICE_FIELD',
    })
    FIELDS_DEFAULT = dict(_OrderBookAggregation.FIELDS_DEFAULT, **{
        'min_levels': None,
        'best_ask_price_field': '',
        'best_bid_price_field': '',
    })

    def __init__(
        self, *args, min_levels: Optional[int] = None,
        best_ask_price_field: Optional[Union[str, _Column]] = None,
        best_bid_price_field: Optional[Union[str, _Column]] = None,
        **kwargs,
    ):
        if min_levels and not kwargs.get('max_depth_for_price'):
            raise ValueError('`min_levels` parameter must not be set when `max_depth_for_price` not set')

        self.min_levels = min_levels

        if isinstance(best_ask_price_field, _Column):
            best_ask_price_field = str(best_ask_price_field)
        elif best_ask_price_field is None:
            best_ask_price_field = ''

        if isinstance(best_bid_price_field, _Column):
            best_bid_price_field = str(best_bid_price_field)
        elif best_bid_price_field is None:
            best_bid_price_field = ''

        self.best_ask_price_field = best_ask_price_field
        self.best_bid_price_field = best_bid_price_field
        super().__init__(*args, **kwargs)

    def validate_input_columns(self, src: 'Source'):
        super().validate_input_columns(src)

        if self.best_ask_price_field and self.best_ask_price_field not in src.schema:
            raise ValueError(
                f'Column \'{self.best_ask_price_field}\' from `best_ask_price_field` parameter not in the schema.'
            )

        if self.best_bid_price_field and self.best_bid_price_field not in src.schema:
            raise ValueError(
                f'Column \'{self.best_bid_price_field}\' from `best_bid_price_field` parameter not in the schema.'
            )

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None) -> dict:
        if self.side:
            return {'VALUE': float}

        return {
            'ASK_VALUE': float,
            'BID_VALUE': float,
        }


class ObVwap(_OrderBookAggregation):
    NAME = 'OB_VWAP'
    EP = otq.ObVwap

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None) -> dict:
        if self.side:
            return {'VALUE': float}

        return {
            'ASK_VALUE': float,
            'BID_VALUE': float,
        }


class ObNumLevels(_OrderBookAggregation):
    NAME = 'OB_NUM_LEVELS'
    EP = otq.ObNumLevels

    def __init__(self, *args, bucket_interval: Union[int, ott.OTPBaseTimeOffset] = 0, **kwargs):
        if not isinstance(bucket_interval, (int, ott.OTPBaseTimeOffset)):
            raise ValueError('Unsupported value type for `bucket_interval` parameter')

        if isinstance(bucket_interval, ott.OTPBaseTimeOffset):
            _, datepart = bucket_interval.get_offset()
            if datepart not in {'second', 'minute', 'hour', 'day'}:
                raise ValueError(f"Unsupported DatePart passed to bucket_interval: {datepart}")

            bucket_interval = get_seconds_from_time_offset(bucket_interval)

        super().__init__(*args, bucket_interval=bucket_interval, **kwargs)

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None) -> dict:
        if self.side:
            return {'VALUE': float}

        return {
            'ASK_VALUE': float,
            'BID_VALUE': float,
        }
