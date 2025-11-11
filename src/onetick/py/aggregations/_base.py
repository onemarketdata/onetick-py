from abc import ABC, abstractmethod
from typing import List, Dict, Union, TYPE_CHECKING, Tuple, Optional, Any
from copy import deepcopy
from functools import wraps
from collections import namedtuple
import pandas as pd

if TYPE_CHECKING:
    from onetick.py.core.source import Source   # hack for annotations

from onetick.py.core.column import _Column
from onetick.py.core.column_operations.base import _Operation, OnetickParameter
from onetick.py.core._source._symbol_param import _SymbolParamColumn
from onetick.py import types as ott
from onetick.py import utils
from onetick.py.otq import otq


def validate(method):
    """wraps schema getter with validations of input columns + src and resulting schema + output column"""

    @wraps(method)
    def inner(obj: '_Aggregation', src: 'Source', name):
        obj.validate_input_columns(src)
        for column in obj.group_by:
            if str(column) not in src.schema or not isinstance(src[str(column)], _Column):
                raise KeyError(f"There is no '{column}' column to group by")
        schema: Dict = method(obj, src=src, name=name)
        if not obj.overwrite_output_field:
            obj.validate_output_name(schema, name)
        return schema

    return inner


def operation_gb(method):
    """wraps aggregation to apply _Operation and remove it after aggregation"""

    @wraps(method)
    def inner(obj: '_Aggregation', src: 'Source', *args, **kwargs) -> 'Source':
        inplace = kwargs.get('inplace')
        res = src if inplace else src.copy()
        src_schema = src.schema

        gb_copy = obj.group_by.copy()
        obj.group_by = []
        for i, gb in enumerate(gb_copy):
            if isinstance(gb, _Operation) and not isinstance(gb, _Column):
                name = f'GROUP_{i}'
                if name in src_schema:
                    raise AttributeError(f"'{name}' column name is reserved for group by Operation "
                                         f"but it exists in current schema")
                res[name] = gb
                obj.group_by.append(res[name])
            else:
                obj.group_by.append(gb)
        res = method(obj, res, *args, **kwargs)

        obj.group_by = gb_copy
        return res
    return inner


def operation_replacer(method):
    """
    PY-378
    Decorator allows working with aggregation's columns specified as operations.
    """

    @wraps(method)
    def inner(obj: '_Aggregation', src: 'Source', *args, **kwargs) -> 'Source':
        inplace = kwargs.get('inplace')
        res = src if inplace else src.copy()
        tmp_columns = {}

        aggrs = getattr(obj, 'aggrs', None)
        if aggrs:
            aggs = aggrs.values()
        else:
            name = args[0] if args else kwargs.get('name')
            # pylint: disable-next=unidiomatic-typecheck
            if type(obj.column_name) is _Operation and name is None:
                raise ValueError('Output field name must be specified when aggregating operation')
            aggs = [obj]

        # Add operation from each aggregation object to source `res` as column
        # and replace *column_name* property in each aggregation object with this column's name.
        for i, agg in enumerate(aggs):
            # pylint: disable-next=unidiomatic-typecheck
            if type(agg.column_name) is _Operation:
                tmp_name = f'__TMP_AGG_COLUMN_{i}__'
                res[tmp_name] = agg.column_name
                tmp_columns[tmp_name] = (agg, agg.column_name)
                agg.column_name = tmp_name

        res = method(obj, res, *args, **kwargs)

        if tmp_columns:
            # Rollback all aggregation objects and source `res`.
            # Delete all temporary columns and change property *column_name* back in aggregations.
            to_drop = list(set(tmp_columns).intersection(res.schema))
            if to_drop:
                res.drop(to_drop, inplace=True)
            for agg, column_name in tmp_columns.values():
                agg.column_name = column_name

        return res
    return inner


def output_column_overwriter(method):
    """
    Allows outputting aggregation to existing field.
    In this case temporary renaming existing field.
    """
    @wraps(method)
    def inner(obj: '_Aggregation', src: 'Source', *args, **kwargs) -> 'Source':
        column_name = obj.column_name
        name = args[0] if args else kwargs.get('name')
        name = name or column_name

        if not obj.overwrite_output_field or not name or name not in src.schema:
            return method(obj, src, *args, **kwargs)

        inplace = kwargs.get('inplace')
        res = src if inplace else src.copy()

        # rename existing field to the temporary name
        tmp_name = f'__TMP_AGG_COLUMN_{name}__'
        res[tmp_name] = res[name]
        res.drop(name, inplace=True)
        # aggregating renamed field
        kwargs['name'] = name
        obj.column_name = tmp_name

        res = method(obj, res, *args, **kwargs)

        # removing temporary field
        if tmp_name in res.schema:
            res.drop(tmp_name, inplace=True)
        obj.column_name = column_name

        return res
    return inner


def get_seconds_from_time_offset(time_offset):
    if not isinstance(time_offset, ott.OTPBaseTimeOffset):
        raise ValueError('Only DatePart objects can be passed in this function')

    return int(pd.Timedelta(time_offset).total_seconds())


def get_bucket_interval_from_datepart(bucket_interval):
    if not isinstance(bucket_interval, ott.OTPBaseTimeOffset):
        raise ValueError('Only DatePart objects can be passed in this function')

    if isinstance(bucket_interval, ott.ExpressionDefinedTimeOffset):
        raise ValueError(f"Operation as DatePart isn't allowed: {str(bucket_interval.n)}")

    # bucket_interval also could be one of these:
    # otp.Milli, otp.Second, otp.Minute, otp.Hour, otp.Day, otp.Month
    # bucket_interval will be converted and corresponding bucket_units value will be set

    offset, datepart = bucket_interval.get_offset()
    if datepart not in {'millisecond', 'second', 'minute', 'hour', 'day', 'month'}:
        raise ValueError(f"Unsupported DatePart passed to bucket_interval: {datepart}")

    if offset < 0:
        raise ValueError(
            f"Negative DateParts aren't allowed for bucket_interval: {offset} ({datepart})"
        )

    if datepart in {'millisecond', 'minute', 'hour'}:
        # bucket_units could be only seconds, days, months or ticks
        # so other DateParts are converted to seconds
        if datepart == 'millisecond':
            offset, datepart = offset / 1000, 'second'
        else:
            offset, datepart = ott.Second(get_seconds_from_time_offset(bucket_interval)).get_offset()

    return offset, f"{datepart}s"  # type: ignore[union-attr]


class _Aggregation(ABC):

    @property
    @abstractmethod
    def NAME(self) -> str:
        pass

    @property
    @abstractmethod
    def EP(self) -> otq.EpBase:
        pass

    DEFAULT_OUTPUT_NAME = 'VALUE'

    FIELDS_MAPPING = {
        "column_name": "INPUT_FIELD_NAME",
        "running": "IS_RUNNING_AGGR",
        "all_fields": "ALL_FIELDS_FOR_SLIDING",
        "bucket_interval": "BUCKET_INTERVAL",
        "bucket_time": "BUCKET_TIME",
        "bucket_units": "BUCKET_INTERVAL_UNITS",
        "bucket_end_condition": "BUCKET_END_CRITERIA",
        "end_condition_per_group": "BUCKET_END_PER_GROUP",
        "boundary_tick_bucket": "BOUNDARY_TICK_BUCKET",
        "group_by": "GROUP_BY",
        "groups_to_display": "GROUPS_TO_DISPLAY",
    }
    FIELDS_DEFAULT = {
        "running": False,
        "all_fields": False,
        "bucket_interval": 0,
        "bucket_time": "BUCKET_END",
        "bucket_units": "seconds",
        "bucket_end_condition": None,
        "end_condition_per_group": False,
        "boundary_tick_bucket": "new",
        "group_by": [],
        "groups_to_display": "all",
    }

    FIELDS_TO_SKIP: List = []   # attr listed here won't be used in self.__str__

    output_field_type: Optional[type] = None  # None will force to use type of input column
    require_type: Optional[Tuple[type, ...]] = None
    _validations_to_skip: List = []

    def __init__(self,
                 column: Union[str, _Column, _Operation],
                 running: bool = False,
                 all_fields: Union[bool, str] = False,
                 bucket_interval: Union[int, ott.OTPBaseTimeOffset] = 0,
                 bucket_time: str = "end",
                 bucket_units: Union[str, None] = None,
                 bucket_end_condition: Optional[_Operation] = None,
                 end_condition_per_group: bool = False,
                 boundary_tick_bucket: str = "new",
                 group_by: Optional[Union[List, str, _Operation]] = None,
                 groups_to_display: str = "all",
                 overwrite_output_field: bool = False):
        """
        Abstract method that implements common logic for aggregations
        """
        if isinstance(column, list):
            column = ','.join(map(str, column))

        column_name: Union[str, _Operation] = str(column)

        if column_name == "Time":
            # TODO: need to understand how to better work with alias
            column_name = "TIMESTAMP"

        # pylint: disable-next=unidiomatic-typecheck
        if type(column) is _Operation:
            column_name = column

        if isinstance(bucket_interval, float):
            if bucket_units is not None and bucket_units != 'seconds':
                raise ValueError('Float values for bucket_interval are only supported for seconds.')
            if bucket_interval < 0.001:
                raise ValueError('Float values for bucket_interval less than 0.001 are not supported.')

        if isinstance(bucket_interval, ott.OTPBaseTimeOffset):
            bucket_interval, bucket_units = get_bucket_interval_from_datepart(bucket_interval)

        if isinstance(all_fields, str) and all_fields == "when_ticks_exit_window":
            if not running:
                raise ValueError("`all_fields` can't be set to 'when_ticks_exit_window' when `running=False`")

            if not bucket_interval:
                raise ValueError(
                    "`all_fields` can't be set to 'when_ticks_exit_window' when `bucket_interval` is zero`"
                )

            all_fields = all_fields.upper()

        self.column_name = column_name
        self.running = running
        self.all_fields = all_fields
        self.bucket_time = bucket_time

        if isinstance(bucket_interval, _Operation):
            if bucket_interval.dtype is bool:
                if bucket_end_condition is not None:
                    raise ValueError(
                        "Bucket end condition passed on both `bucket_interval` and `bucket_end_condition` parameters"
                    )

                bucket_end_condition = bucket_interval
                bucket_interval = 0
            elif isinstance(bucket_interval, OnetickParameter) and bucket_interval.dtype is int:
                bucket_interval = str(bucket_interval)
            elif isinstance(bucket_interval, _SymbolParamColumn) and bucket_interval.dtype is int:
                bucket_interval = str(bucket_interval.expr)
            else:
                raise ValueError("Bucket interval can only be boolean otp.Operation or integer otp.param")

        self.bucket_interval = bucket_interval

        if bucket_end_condition is None:
            self.bucket_end_condition = None  # type: ignore
        else:
            self.bucket_end_condition = str(bucket_end_condition)

        self.bucket_units = bucket_units
        if self.bucket_units is None:
            if self.bucket_end_condition:
                # allow omitting bucket_units if bucket_end_condition is set
                self.bucket_units = 'flexible'
            else:
                # default value
                self.bucket_units = 'seconds'

        self.end_condition_per_group = end_condition_per_group
        self.boundary_tick_bucket = boundary_tick_bucket
        self.large_ints = False
        if isinstance(group_by, (_Operation, str)):
            group_by = [group_by]
        self.group_by = group_by or []
        self.groups_to_display = groups_to_display
        self.overwrite_output_field = overwrite_output_field

        self._param_validation()
        self.bucket_time = f'BUCKET_{self.bucket_time.upper()}'

    @staticmethod
    def _attr2str(value) -> str:
        if isinstance(value, bool):
            return 'true' if value else 'false'
        if isinstance(value, list):
            return ','.join(value)
        return str(value)

    @property
    def ep_params(self) -> Dict:
        """prepare params for self.__str__ and otq.EpBase"""
        params = {}

        for field, ep_param in self.FIELDS_MAPPING.items():
            if field in self.FIELDS_TO_SKIP:
                continue

            default_value = self.FIELDS_DEFAULT.get(field)
            if getattr(self, field) != default_value:
                if field == 'group_by':
                    params[ep_param] = ",".join(list(map(str, self.group_by)))
                else:
                    params[ep_param] = getattr(self, field)
        return params

    def __str__(self):
        params = [f'{k}={self._attr2str(v)}' for k, v in self.ep_params.items()]
        return self.NAME + "(" + ",".join(params) + ")"

    def to_ep(self, name: Optional[str]) -> otq.EpBase:
        params = dict((k.lower(), v) for k, v in self.ep_params.items())
        if 'output_field_name' not in self.FIELDS_TO_SKIP:
            params['output_field_name'] = name
        return self.EP(**params)

    @validate
    def _get_common_schema(self, src: 'Source', name: str) -> Dict:
        """return data schema without output fields (this fields should be added further)"""
        schema = {}
        for column in self.group_by:
            schema[str(column)] = src.schema[str(column)]
        if self.all_fields:
            schema.update(src.schema)
        return schema

    def _modify_source(self, res: 'Source', **kwargs):
        """
        Modify resulting source inplace before sinking to aggregation.
        Can be overriden if needed.
        """
        pass

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None) -> Dict:
        if not name or name in src.__class__.meta_fields:
            return {}
        return {
            name: self.output_field_type or src.schema[self.column_name]
        }

    @operation_gb
    @operation_replacer
    @output_column_overwriter
    def apply(self, src: 'Source', name: Optional[str] = None, inplace: bool = False) -> 'Source':
        """
        Applies aggregation to Source and sets proper schema

        Parameters
        ----------
        src: Source
            Source to apply aggregation
        name: str, optional
            Name of output column. If not specified, will be used self.column_name
        inplace: bool
            Modify passed ``src`` object or return modified copy.
        """
        if inplace:
            res = src
            src = src.copy()
        else:
            res = src.copy()
        out_name = name or self.column_name
        schema = self._get_common_schema(src, out_name)
        # it's important to validate input schema before sinking
        self._modify_source(res)
        res.sink(self.to_ep(name=str(out_name)))
        schema.update(self._get_output_schema(src, str(out_name)))
        res.schema.set(**schema)

        if not self.all_fields:
            # in this case we propagate only resulting fields, that stored in res.schema (flexible schema case)
            res._add_table(strict=True)
        else:
            # adding table to convert types in schema, e.g. float to int
            res._add_table(strict=False)
        return res

    def validate_input_columns(self, src: 'Source'):
        """checks that columns used in aggregation presented in Source"""
        if self.column_name not in src.schema:
            raise TypeError(f"Aggregation `{self.NAME}` uses column `{self.column_name}` as input, which doesn't exist")
        if not self.require_type:
            return
        dtype = src.schema[self.column_name]
        base_dtype = ott.get_base_type(dtype)
        for t in self.require_type:
            # more generic types can be specified in self.require_type too
            if dtype is t or base_dtype is t:
                return
        raise TypeError(f"Aggregation `{self.NAME}` require {self.require_type} types, got {dtype}")

    @staticmethod
    def validate_output_name(schema: Dict, name: Union[List, str]):
        """checks that aggregation won't output columns with same names"""
        if not isinstance(name, list):
            name = [name]

        same_fields = []
        for n in name:
            if n in schema:
                if '__long_nsec_' in n:
                    same_fields.append(n.replace('__long_nsec_', ''))   # hack for large ints
                else:
                    same_fields.append(n)
        if same_fields:
            raise ValueError("You try to propagate all fields and put result into already existing fields: "
                             f"'{', '.join(same_fields)}' ")

    def _param_validation(self):
        """validate __init__ parameters"""
        if self.running and self.bucket_time == "start":
            raise ValueError("It is not allowed to set up running=True and bucket_time='start'")
        if self.bucket_units == "flexible" and self.bucket_end_condition is None:
            raise ValueError("bucket_units is set to 'flexible' but bucket_end_condition is not specified. "
                             "Please specify bucket_end_condition.")
        if self.bucket_units != "flexible" and self.bucket_end_condition is not None:
            raise ValueError("bucket_end_condition can be used only with 'flexible' bucket_units. "
                             "Please set bucket_units to 'flexible'.")

        if self.bucket_time not in ['start', 'end']:
            raise ValueError(f"'bucket_time' might be either 'start' or 'end', but passed '{self.bucket_time}'")

        valid_units = ("seconds", "ticks", "days", "months", "flexible")
        if self.bucket_units not in valid_units:
            raise ValueError("'bucket_units' can be one of the following: "
                             f"'{', '.join(valid_units)}'; however, '{self.bucket_units}' was passed")

        valid_boundary = {"new", "previous"}
        if self.boundary_tick_bucket not in valid_boundary:
            message = "'boundary_tick_bucket' can be one of the following: {}; however, {} was passed"
            raise ValueError(message.format(', '.join(list(valid_boundary)), self.boundary_tick_bucket))

        for column in self.group_by:
            if not isinstance(column, _Operation) and not isinstance(column, str):
                raise TypeError(f"Unsupported type '{column}' of a column to group by")

        if self.groups_to_display not in ('all', 'event_in_last_bucket'):
            raise ValueError("Parameter 'groups_to_display' can only be set to 'all' or 'event_in_last_bucket':"
                             f" got '{self.groups_to_display}'")

        if self.all_fields and not self.running and 'running_all_fields' not in self._validations_to_skip:
            raise ValueError("It is not allowed set all_fields to True for not running aggregation")

        if not self.running and self.overwrite_output_field:
            raise ValueError("Parameter 'overwrite_output_field' can only be used with running aggregations")

    @property
    def is_multi_column_aggregation(self):
        return isinstance(self, _MultiColumnAggregation)

    @property
    def is_all_columns_aggregation(self):
        return isinstance(self, _AllColumnsAggregation)


class _AggregationTSType(_Aggregation):

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['time_series_type'] = 'TIME_SERIES_TYPE'
    FIELDS_DEFAULT = deepcopy(_Aggregation.FIELDS_DEFAULT)
    FIELDS_DEFAULT['time_series_type'] = 'event_ts'

    def __init__(self, column, time_series_type: str = "event_ts", *args, **kwargs):
        """
        Abstract class that implements common logic for aggregations with ability to select time series type
        inherited from _Aggregation

        Parameters
        ----------
        column: see _Aggregation
        time_series_type: "event_ts" or "state_ts", default="event_ts"
            "state_ts":
                if there is a tick in bucket with timestamp = bucket start:
                    only ticks in bucket used for calculation max value
                else:
                    latest tick from previous bucket included in current bucket
            "event_ts": only ticks from current bucket used for calculations
        args: see _Aggregation
        kwargs: see _Aggregation
        """
        if time_series_type not in ["event_ts", "state_ts"]:
            raise ValueError('time_series_type argument must be "event_ts" or "state_ts"')
        self.time_series_type = time_series_type
        super().__init__(column, *args, **kwargs)


class _AggregationTSSelection(_Aggregation):

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['selection'] = 'SELECTION'
    FIELDS_DEFAULT = deepcopy(_Aggregation.FIELDS_DEFAULT)
    FIELDS_DEFAULT['selection'] = 'first'

    def __init__(self, column, selection: str = "first", *args, **kwargs):
        if selection not in ["first", "last"]:
            raise ValueError(f'{self.__class__.__name__} selection argument must be "first" or "last"')
        self.selection = selection
        super().__init__(column, *args, **kwargs)


class _FloatAggregation(_Aggregation):

    require_type = (int, float, ott._inf, ott.decimal)

    """
    Aggregation that expect int or float as input
    """


class _KeepTs(_Aggregation):

    def __init__(self, *args, keep_timestamp=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.keep_timestamp = keep_timestamp

    @validate     # type: ignore
    def _get_common_schema(self, src: 'Source', *args, **kwargs) -> Dict:
        schema = src.schema.copy()
        schema['TICK_TIME'] = ott.nsectime
        return schema

    def apply(self, src: 'Source', *args, **kwargs) -> 'Source':
        res = super().apply(src=src, *args, **kwargs)
        if self.keep_timestamp:
            # TICK_TIME can be empty if it's a tick from default_tick aggregation parameter
            res['TICK_TIME'] = res.if_else(res['TICK_TIME'], res['TICK_TIME'], res['TIMESTAMP'])
            res['TIMESTAMP'] = res['TICK_TIME']
            res.drop('TICK_TIME', inplace=True)
        return res


class _ExpectLargeInts(_Aggregation):
    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['large_ints'] = 'EXPECT_LARGE_INTS'
    FIELDS_MAPPING['null_int_val'] = 'NULL_INT_VAL'
    FIELDS_DEFAULT = deepcopy(_Aggregation.FIELDS_DEFAULT)
    FIELDS_DEFAULT['large_ints'] = False
    FIELDS_DEFAULT['null_int_val'] = 0

    def __init__(self, *args, large_ints=False, null_int_val=0, **kwargs):
        super().__init__(*args, **kwargs)
        if large_ints not in {True, False, utils.adaptive}:
            raise ValueError(f"Wrong value for {self.__class__.__name__} aggregation"
                             f" 'large_ints' parameter: {large_ints}")
        if large_ints is utils.adaptive:
            large_ints = 'IF_INPUT_VAL_IS_LONG_INTEGER'

        if null_int_val and not large_ints:
            raise ValueError(
                f"Wrong value for {self.__class__.__name__} aggregation:"
                f" 'null_int_val' parameter is set, however 'large_ints' is `False`"
            )

        self.large_ints = large_ints
        self.null_int_val = null_int_val

    def apply(self, src: 'Source', name: Optional[str] = None) -> 'Source':
        out_name = name or self.column_name
        res, col, convert_back = self._ts_to_long(src, str(out_name))
        res = super().apply(res, col.tmp_out_column)
        if not convert_back:
            return res
        return self._long_to_ts(res, col)

    def _ts_to_long(self, src: 'Source', name: str) -> Tuple['Source', Any, bool]:
        agg_columns = namedtuple('agg_columns', ('in_column', 'tmp_in_column', 'tmp_out_column', 'out_column'))
        if src.schema[self.column_name] != ott.nsectime:
            return src, agg_columns(self.column_name, self.column_name, name, name), False
        self.large_ints = True
        res = src.copy()
        col = agg_columns(self.column_name, f'__long_nsec_{self.column_name}',
                          f'__long_nsec_{name}', name)
        res[col.tmp_in_column] = res[col.in_column].apply(int)
        self.column_name = col.tmp_in_column
        return res, col, True

    def _long_to_ts(self, src: 'Source', col) -> 'Source':
        res = src.copy()
        res[col.out_column] = res[col.tmp_out_column].astype(ott.nsectime)
        to_drop = []
        for c in [col.tmp_out_column, col.tmp_in_column]:
            if c in res.schema:
                to_drop.append(c)
        if to_drop:
            res.drop(to_drop, inplace=True)
        self.column_name = col.in_column
        return res


class _MultiColumnAggregation:
    """
    Helper class for identifying multi-column aggregations.
    """
    pass


class _AllColumnsAggregation(_MultiColumnAggregation):
    """
    Helper class for identifying aggregations, which returns all fields from original ticks.
    """
    pass
