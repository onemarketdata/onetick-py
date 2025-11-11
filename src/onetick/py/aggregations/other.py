from collections import defaultdict
from typing import List, Dict, Union, Optional, Tuple, TYPE_CHECKING
from copy import deepcopy

if TYPE_CHECKING:
    from onetick.py.core.source import Source   # hack for annotations

from onetick.py.core.column import _Column, _Operation
from onetick.py.core.column_operations.base import OnetickParameter
from onetick.py.core._source.tmp_otq import TmpOtq
from onetick.py import types as ott
from onetick.py.otq import otq

from ._base import (
    _Aggregation,
    _AggregationTSType,
    _AggregationTSSelection,
    _KeepTs,
    _FloatAggregation,
    _ExpectLargeInts,
    _MultiColumnAggregation,
    _AllColumnsAggregation,
    validate,
)


class First(_AggregationTSType, _ExpectLargeInts):
    NAME = "FIRST"
    EP = otq.First

    FIELDS_MAPPING = deepcopy(_AggregationTSType.FIELDS_MAPPING)
    FIELDS_MAPPING.update(_ExpectLargeInts.FIELDS_MAPPING)
    FIELDS_MAPPING['skip_tick_if'] = 'SKIP_TICK_IF'
    FIELDS_DEFAULT = deepcopy(_AggregationTSType.FIELDS_DEFAULT)
    FIELDS_DEFAULT.update(_ExpectLargeInts.FIELDS_DEFAULT)
    FIELDS_DEFAULT['skip_tick_if'] = ''

    def __init__(self, *args, skip_tick_if=None, **kwargs):
        super().__init__(*args, **kwargs)

        if skip_tick_if is ott.nan:
            skip_tick_if = 'NAN'

        self.skip_tick_if = '' if skip_tick_if is None else skip_tick_if


class FirstTime(_AggregationTSType):
    NAME = "FIRST_TIME"
    EP = otq.FirstTime
    FIELDS_TO_SKIP = ["column_name"]

    def __init__(self, *args, **kwargs):
        super().__init__(column=_Column("TIMESTAMP"), *args, **kwargs)


class LastTime(_AggregationTSType):
    NAME = "LAST_TIME"
    EP = otq.LastTime
    FIELDS_TO_SKIP = ["column_name"]

    def __init__(self, *args, **kwargs):
        super().__init__(column=_Column("TIMESTAMP"), *args, **kwargs)


class Last(First):
    NAME = "LAST"
    EP = otq.Last

    FIELDS_MAPPING = deepcopy(First.FIELDS_MAPPING)
    FIELDS_MAPPING['skip_tick_if'] = 'FWD_FILL_IF'
    FIELDS_DEFAULT = deepcopy(First.FIELDS_DEFAULT)
    FIELDS_DEFAULT['skip_tick_if'] = ''


class Count(_Aggregation):
    NAME = "NUM_TICKS"
    EP = otq.NumTicks
    FIELDS_TO_SKIP = ["column_name"]
    output_field_type = int

    def __init__(self, *args, **kwargs):
        super().__init__(column=_Column("TIMESTAMP"), *args, **kwargs)

    def apply(self, src, name: str = 'VALUE', *args, **kwargs):
        return super().apply(src=src, name=name, *args, **kwargs)


class Vwap(_Aggregation):

    NAME = "VWAP"
    EP = otq.Vwap

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['price_column'] = 'PRICE_FIELD_NAME'
    FIELDS_MAPPING['size_column'] = 'SIZE_FIELD_NAME'
    FIELDS_DEFAULT = deepcopy(_Aggregation.FIELDS_DEFAULT)
    FIELDS_DEFAULT['price_column'] = 'PRICE'
    FIELDS_DEFAULT['size_column'] = 'SIZE'

    FIELDS_TO_SKIP: List = ['column_name']

    output_field_type = float
    require_type = (int, float, ott.nsectime, ott.decimal)

    def __init__(self,
                 price_column: str,
                 size_column: str,
                 *args, **kwargs):
        super().__init__(column=_Column('TIMESTAMP'), *args, **kwargs)  # type: ignore
        self.price_column = str(price_column)
        self.size_column = str(size_column)

    def validate_input_columns(self, src: 'Source'):
        for column in [self.price_column, self.size_column]:
            if column not in src.schema:
                raise TypeError(
                    f"Aggregation `{self.NAME}` uses column `{column}` as input, which doesn't exist")
            if not issubclass(src.schema[column], self.require_type):
                raise TypeError(f"Aggregation `{self.NAME}` require {self.require_type} types, "
                                f"got {src.schema[column]}")

    def apply(self, src: 'Source', name: str = 'VWAP', *args, **kwargs) -> 'Source':
        return super().apply(src=src, name=name, *args, **kwargs)


class Correlation(_Aggregation):

    NAME = 'CORRELATION'
    EP = otq.Correlation

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['column_name_1'] = 'INPUT_FIELD1_NAME'
    FIELDS_MAPPING['column_name_2'] = 'INPUT_FIELD2_NAME'

    FIELDS_TO_SKIP: List = ['column_name']

    output_field_type = float
    require_type = (int, float)

    def __init__(self,
                 column_name_1: str,
                 column_name_2: str,
                 *args, **kwargs):
        super().__init__(column=_Column('TIMESTAMP'), *args, **kwargs)  # type: ignore
        self.column_name_1 = str(column_name_1)
        self.column_name_2 = str(column_name_2)

    def validate_input_columns(self, src: 'Source'):
        for column in [self.column_name_1, self.column_name_2]:
            if column not in src.schema:
                raise TypeError(
                    f"Aggregation `{self.NAME}` uses column `{column}` as input, which doesn't exist")
            if src.schema[column] not in self.require_type:
                raise TypeError(f"Aggregation `{self.NAME}` require {self.require_type} types, "
                                f"got {src.schema[column]}")

    def apply(self, src: 'Source', name: str = 'CORRELATION', *args, **kwargs) -> 'Source':
        return super().apply(src=src, name=name, *args, **kwargs)


class FirstTick(_AggregationTSType, _KeepTs, _AllColumnsAggregation):
    EP = otq.FirstTick
    NAME = 'FIRST_TICK'
    DEFAULT_OUTPUT_NAME = 'FIRST_TICK'

    FIELDS_MAPPING = deepcopy(_AggregationTSType.FIELDS_MAPPING)
    FIELDS_MAPPING['n'] = 'NUM_TICKS'
    FIELDS_MAPPING['default_tick'] = 'DEFAULT_TICK'
    FIELDS_DEFAULT = deepcopy(_AggregationTSType.FIELDS_DEFAULT)
    FIELDS_DEFAULT['n'] = 1
    FIELDS_DEFAULT['default_tick'] = ''

    FIELDS_TO_SKIP = ['column_name', 'output_field_name', 'all_fields']

    _validations_to_skip = ['running_all_fields']

    def __init__(self, n: int = 1, *args, default_tick=None, **kwargs):
        kwargs['all_fields'] = True
        super().__init__(_Column("TIMESTAMP"), *args, **kwargs)
        self.n = n
        self._default_tick = default_tick
        self.default_tick = ''

    def validate_input_columns(self, src: 'Source'):
        super().validate_input_columns(src)
        if not self._default_tick:
            self.default_tick = ''
            return

        fields = {}
        for field, value in self._default_tick.items():
            if field not in src.schema:
                raise ValueError(f"Field '{field}' is not in schema")
            dtype = src.schema[field]
            if value is not None:
                dtype = ott.get_object_type(value)
                if dtype is not src.schema[field]:
                    raise ValueError(f"Incompatible types for field '{field}': {src.schema[field]} --> {dtype}")
                value = ott.value2str(value)
            dtype_str = ott.type2str(dtype)
            fields[field] = (dtype_str, value)
        self.default_tick = ','.join(
            f'{field} {dtype} ({value})' if value is not None else f'{field} {dtype}'
            for field, (dtype, value) in fields.items()
        )


class LastTick(FirstTick):
    EP = otq.LastTick
    NAME = 'LAST_TICK'
    DEFAULT_OUTPUT_NAME = 'LAST_TICK'


class Distinct(_AggregationTSSelection):
    EP = otq.Distinct
    NAME = 'DISTINCT'

    FIELDS_MAPPING = deepcopy(_AggregationTSSelection.FIELDS_MAPPING)
    FIELDS_MAPPING['column_name'] = 'KEYS'
    FIELDS_MAPPING['key_attrs_only'] = 'KEY_ATTRS_ONLY'
    FIELDS_DEFAULT = deepcopy(_AggregationTSSelection.FIELDS_DEFAULT)
    FIELDS_DEFAULT['key_attrs_only'] = True

    FIELDS_TO_SKIP = ['end_condition_per_group', 'group_by', 'output_field_name', 'all_fields']

    def __init__(self,
                 keys: Union[str, List[str], _Column, List[_Column]],
                 key_attrs_only: bool = True,
                 *args, **kwargs):
        keys = keys if isinstance(keys, list) else [keys]   # type: ignore
        super().__init__(column=keys, *args, **kwargs)  # type: ignore
        self.key_attrs_only = key_attrs_only

    @validate
    def _get_common_schema(self, src: 'Source', *args, **kwargs) -> Dict:
        if self.key_attrs_only:
            return super()._get_common_schema(src=src, *args, **kwargs)
        return src.schema.copy()

    @staticmethod
    def validate_output_name(*args, **kwargs):
        # Distinct aggregation doesn't have output fields
        pass

    def validate_input_columns(self, src: 'Source'):
        for col in str(self.column_name).split(','):
            if col.strip() not in src.schema:
                raise TypeError(f"Aggregation `{self.NAME}` uses column `{col.strip()}` as input, which doesn't exist")

    def apply(self, src: 'Source', *args, **kwargs) -> 'Source':
        res = src.copy()
        res.sink(self.to_ep(name=None))
        schema = self._get_common_schema(src, None)
        for col in str(self.column_name).split(','):
            schema[col.strip()] = src.schema[col.strip()]
        res.schema.set(**schema)
        return res


class Sum(_FloatAggregation):
    NAME = "SUM"
    EP = otq.Sum


class Average(_FloatAggregation):
    NAME = "AVERAGE"
    EP = otq.Average
    output_field_type = float


class StdDev(_Aggregation):     # Stddev does not support inf, so no need to use _FloatAggregation
    NAME = "STDDEV"
    EP = otq.Stddev
    require_type = (int, float, ott.decimal)
    output_field_type = float
    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['biased'] = 'BIASED'

    def __init__(self, *args, biased: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        self.biased = biased


class TimeWeightedAvg(_AggregationTSType, _FloatAggregation):
    NAME = "TW_AVERAGE"
    EP = otq.TwAverage
    output_field_type = float
    FIELDS_DEFAULT = deepcopy(_AggregationTSType.FIELDS_DEFAULT)
    FIELDS_DEFAULT['time_series_type'] = 'state_ts'

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('time_series_type', 'state_ts')
        super().__init__(*args, **kwargs)


class Median(_FloatAggregation):
    NAME = "MEDIAN"
    EP = otq.Median
    output_field_type = float


class OptionPrice(_Aggregation):

    NAME = 'OPTION_PRICE'
    EP = otq.OptionPrice

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['volatility'] = 'VOLATILITY'
    FIELDS_MAPPING['interest_rate'] = 'INTEREST_RATE'
    FIELDS_MAPPING['compute_model'] = 'COMPUTE_MODEL'
    FIELDS_MAPPING['number_of_steps'] = 'NUMBER_OF_STEPS'
    FIELDS_MAPPING['compute_delta'] = 'COMPUTE_DELTA'
    FIELDS_MAPPING['compute_gamma'] = 'COMPUTE_GAMMA'
    FIELDS_MAPPING['compute_theta'] = 'COMPUTE_THETA'
    FIELDS_MAPPING['compute_vega'] = 'COMPUTE_VEGA'
    FIELDS_MAPPING['compute_rho'] = 'COMPUTE_RHO'
    FIELDS_MAPPING['volatility_field_name'] = 'VOLATILITY_FIELD_NAME'
    FIELDS_MAPPING['interest_rate_field_name'] = 'INTEREST_RATE_FIELD_NAME'
    FIELDS_MAPPING['option_type_field_name'] = 'OPTION_TYPE_FIELD_NAME'
    FIELDS_MAPPING['strike_price_field_name'] = 'STRIKE_PRICE_FIELD_NAME'
    FIELDS_MAPPING['days_in_year'] = 'DAYS_IN_YEAR'
    FIELDS_MAPPING['days_till_expiration_field_name'] = 'DAYS_TILL_EXPIRATION_FIELD_NAME'
    FIELDS_MAPPING['expiration_date_field_name'] = 'EXPIRATION_DATE_FIELD_NAME'

    FIELDS_TO_SKIP: List = ['column_name', 'all_fields', 'end_condition_per_group', 'group_by', 'output_field_name']

    output_field_type = float
    require_type = (float,)

    def __init__(self,  # NOSONAR
                 volatility: Optional[float] = None,
                 interest_rate: Optional[float] = None,
                 compute_model: str = 'BS',
                 number_of_steps: Optional[int] = None,
                 compute_delta: bool = False,
                 compute_gamma: bool = False,
                 compute_theta: bool = False,
                 compute_vega: bool = False,
                 compute_rho: bool = False,
                 volatility_field_name: str = '',
                 interest_rate_field_name: str = '',
                 option_type_field_name: str = '',
                 strike_price_field_name: str = '',
                 days_in_year: int = 365,
                 days_till_expiration_field_name: str = '',
                 expiration_date_field_name: str = '',
                 *args, **kwargs):
        super().__init__(column=_Column('PRICE'), *args, **kwargs)  # type: ignore
        self.volatility = volatility
        self.interest_rate = interest_rate
        self.compute_model = compute_model
        self.number_of_steps = number_of_steps
        self.compute_delta = compute_delta
        self.compute_gamma = compute_gamma
        self.compute_theta = compute_theta
        self.compute_vega = compute_vega
        self.compute_rho = compute_rho
        self.volatility_field_name = volatility_field_name
        self.interest_rate_field_name = interest_rate_field_name
        self.option_type_field_name = option_type_field_name
        self.strike_price_field_name = strike_price_field_name
        self.days_in_year = days_in_year
        self.days_till_expiration_field_name = days_till_expiration_field_name
        self.expiration_date_field_name = expiration_date_field_name

    def apply(self, src: 'Source', name: str = 'VALUE', *args, **kwargs) -> 'Source':
        return super().apply(src=src, name=name, *args, **kwargs)

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None) -> Dict:
        output_schema = super()._get_output_schema(src, name=name)
        compute = {
            k.upper(): float for k in ['delta', 'gamma', 'theta', 'vega', 'rho'] if getattr(self, f'compute_{k}')
        }
        output_schema.update(compute)
        return output_schema


class Ranking(_KeepTs):
    NAME = 'RANKING'
    EP = otq.Ranking
    FIELDS_TO_SKIP = ['column_name', 'output_field_name']
    FIELDS_MAPPING = deepcopy(_KeepTs.FIELDS_MAPPING)
    FIELDS_MAPPING.update({
        'rank_by': 'RANK_BY',
        'show_rank_as': 'SHOW_RANK_AS',
        'include_tick_in_percentage': 'INCLUDE_TICK_IN_PERCENTAGE',
    })
    FIELDS_DEFAULT = deepcopy(_KeepTs.FIELDS_DEFAULT)
    FIELDS_DEFAULT.update({
        'show_rank_as': 'ORDER',
        'include_tick_in_percentage': False,
    })
    _ALLOWED_RANK_BY = {'asc', 'desc'}
    _ALLOWED_SHOW_RANK_AS = {'order', 'percentile_standard', 'percent_le_values', 'percent_lt_values'}

    def __init__(self, rank_by, *args, show_rank_as='ORDER', include_tick=False, **kwargs):
        super().__init__(column=_Column('TIMESTAMP'), *args, **kwargs)

        if isinstance(rank_by, str):
            rank_by = [rank_by]
        if not isinstance(rank_by, dict):
            rank_by = {column: 'desc' for column in rank_by}
        for k in rank_by:
            rank_by[k] = rank_by[k].lower()
        rank_by_values = set(rank_by.values())
        diff = rank_by_values.difference(self._ALLOWED_RANK_BY)
        if diff:
            raise ValueError(f"Only {self._ALLOWED_RANK_BY} values of parameter 'rank_by' are allowed, got {diff}.")
        self._rank_by = rank_by

        show_rank_as = show_rank_as.lower()
        if show_rank_as not in self._ALLOWED_SHOW_RANK_AS:
            raise ValueError(f"Only {self._ALLOWED_SHOW_RANK_AS} values of parameter 'show_rank_as' are allowed.")
        self.show_rank_as = show_rank_as
        self.include_tick_in_percentage = include_tick

    @property
    def rank_by(self):
        return ','.join(f'{k} {w}' for k, w in self._rank_by.items())

    def apply(self, src):
        return super().apply(src=src, name='RANKING')

    def validate_input_columns(self, src: 'Source'):
        for col in self._rank_by:
            if col not in src.schema:
                raise TypeError(f"Ranking aggregation uses column '{col}' as input, which doesn't exist")

    @staticmethod
    def validate_output_name(schema, name):
        if 'RANKING' in schema:
            raise ValueError("Output field of ranking aggregation will be named 'RANKING',"
                             " but this field exists in the input schema.")
        _KeepTs.validate_output_name(schema, name)

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None):
        return {
            'RANKING': int if self.show_rank_as == 'order' else float
        }


class Variance(_Aggregation):
    NAME = 'VARIANCE'
    EP = otq.Variance
    output_field_type = float
    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['biased'] = 'BIASED'
    require_type = (int, float)

    def __init__(self,
                 column: Union[str, _Column, _Operation],
                 biased: bool,
                 *args, **kwargs):
        super().__init__(column, *args, **kwargs)
        self.biased = biased

    def apply(self, src: 'Source', name: str = 'VARIANCE', *args, **kwargs) -> 'Source':
        return super().apply(src=src, name=name, *args, **kwargs)


class Percentile(_Aggregation):
    NAME = 'PERCENTILE'
    EP = otq.Percentile

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['number_of_quantiles'] = 'NUMBER_OF_QUANTILES'
    FIELDS_MAPPING['column_name'] = 'INPUT_FIELD_NAMES'
    FIELDS_MAPPING['output_field_names'] = 'OUTPUT_FIELD_NAMES'

    FIELDS_TO_SKIP = ['output_field_name', 'all_fields']

    def __init__(
        self,
        input_field_names: List[Union[Union[str, _Column], Tuple[Union[str, _Column], str]]],
        output_field_names: Optional[List[str]] = None,
        number_of_quantiles: int = 2,
        *args, **kwargs,
    ):
        self.number_of_quantiles = number_of_quantiles

        if not input_field_names:
            raise ValueError('`input_field_names` should be passed')

        if not isinstance(input_field_names, list):
            raise ValueError('`input_field_names` should be the `list` of columns')

        if output_field_names is not None and not isinstance(output_field_names, list):
            raise ValueError('`output_field_names` should be the `list` of columns')

        if output_field_names and len(input_field_names) != len(output_field_names):
            raise ValueError('`output_field_names` should have the same number of elements as `input_field_names`')

        columns = ', '.join(
            str(field) if isinstance(field, (str, _Column)) else f'{str(field[0])} {field[1].upper()}'
            for field in input_field_names
        )

        self._columns = [
            str(field) if not isinstance(field, tuple) else str(field[0])
            for field in input_field_names
        ]

        self.output_field_names = '' if output_field_names is None else ', '.join(output_field_names)

        super().__init__(columns, *args, **kwargs)

    def validate_input_columns(self, src: 'Source'):
        for col in self._columns:
            if col.strip() not in src.schema:
                raise TypeError(f"Aggregation `{self.NAME}` uses column `{col.strip()}` as input, which doesn't exist")

    def apply(self, src: 'Source', *args, **kwargs) -> 'Source':
        res = src.copy()
        res.sink(self.to_ep(name=None))
        schema = self._get_common_schema(src, None)
        for col in self._columns:
            schema[col.strip()] = src.schema[col.strip()]

        schema['QUANTILE'] = int
        res.schema.set(**schema)
        return res


class FindValueForPercentile(_FloatAggregation):
    NAME = 'FIND_VALUE_FOR_PERCENTILE'
    EP = None

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['percentile'] = 'PERCENTILE'
    FIELDS_MAPPING['show_percentile_as'] = 'SHOW_PERCENTILE_AS'

    FIELDS_TO_SKIP = ['all_fields']

    output_field_type = float

    def __init__(
        self, column,
        percentile: int,
        show_percentile_as: str = '',
        *args, **kwargs
    ):
        try:
            self.EP = otq.FindValueForPercentile
        except AttributeError as exc:
            raise RuntimeError("This OneTick build doesn't support FindValueForPercentile aggregation") from exc

        if not isinstance(percentile, int) or percentile < 0 or percentile > 100:
            raise ValueError("Parameter 'percentile' must be a number between 0 and 100.")
        if show_percentile_as and show_percentile_as not in {'interpolated_value', 'first_value_with_ge_percentile'}:
            raise ValueError(f"Unsupported value for parameter 'show_percentile_as': {show_percentile_as}")
        self.percentile = percentile
        self.show_percentile_as = show_percentile_as.upper()

        FindValueForPercentile.FIELDS_MAPPING['show_percentile_as'] = \
            'SHOW_PERCENTILE_AS' if 'show_percentile_as' in self.EP.Parameters.__dict__ else 'COMPUTE_VALUE_AS'

        super().__init__(column, *args, **kwargs)


class ExpWAverage(_FloatAggregation, _AggregationTSType):
    NAME = 'EXP_W_AVERAGE'
    EP = otq.ExpWAverage

    FIELDS_MAPPING = deepcopy(_AggregationTSType.FIELDS_MAPPING)
    FIELDS_MAPPING['decay'] = 'DECAY'
    FIELDS_MAPPING['decay_value_type'] = 'DECAY_VALUE_TYPE'
    FIELDS_DEFAULT = deepcopy(_AggregationTSType.FIELDS_DEFAULT)
    FIELDS_DEFAULT['time_series_type'] = 'state_ts'
    FIELDS_DEFAULT['decay_value_type'] = 'lambda'

    output_field_type = float

    def __init__(self, column, decay, decay_value_type='lambda', time_series_type='state_ts', *args, **kwargs):
        super().__init__(column, time_series_type=time_series_type, *args, **kwargs)

        if decay_value_type not in ['lambda', 'half_life_index']:
            raise ValueError(f"Parameter 'decay_value_type' has incorrect value: {decay_value_type}, "
                             f"should be one of next: 'lambda', 'half_life_index'")

        self.decay = decay
        self.decay_value_type = decay_value_type.upper()


class ExpTwAverage(_FloatAggregation):
    NAME = 'EXP_TW_AVERAGE'
    EP = otq.ExpTwAverage

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['decay'] = 'DECAY'
    FIELDS_MAPPING['decay_value_type'] = 'DECAY_VALUE_TYPE'
    FIELDS_DEFAULT = deepcopy(_Aggregation.FIELDS_DEFAULT)
    FIELDS_DEFAULT['decay_value_type'] = 'half_life_index'

    output_field_type = float

    def __init__(self, column, decay, decay_value_type='half_life_index', *args, **kwargs):
        if decay_value_type not in ['lambda', 'half_life_index']:
            raise ValueError(f"Parameter 'decay_value_type' has incorrect value: {decay_value_type}, "
                             f"should be one of next: 'lambda', 'half_life_index'")

        self.decay = decay
        self.decay_value_type = decay_value_type.upper()

        super().__init__(column, *args, **kwargs)


class StandardizedMoment(_Aggregation):
    NAME = 'STANDARDIZED_MOMENT'
    EP = None

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['degree'] = 'DEGREE'
    FIELDS_DEFAULT = deepcopy(_Aggregation.FIELDS_DEFAULT)
    FIELDS_DEFAULT['degree'] = 3

    output_field_type = float

    def __init__(self, *args, degree=3, **kwargs):
        try:
            self.EP = otq.StandardizedMoment
        except AttributeError as exc:
            raise RuntimeError("Used onetick installation not support onetick.query.StandardizedMoment") from exc

        self.degree = degree

        super().__init__(*args, **kwargs)


class PortfolioPrice(_Aggregation, _MultiColumnAggregation):
    NAME = 'PORTFOLIO_PRICE'
    EP = otq.PortfolioPrice
    DEFAULT_OUTPUT_NAME = 'VALUE'

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['column_name'] = 'INPUT_FIELD_NAME'
    FIELDS_MAPPING['weight_field_name'] = 'WEIGHT_FIELD_NAME'
    FIELDS_MAPPING['side'] = 'SIDE'
    FIELDS_MAPPING['weight_type'] = 'WEIGHT_TYPE'

    FIELDS_DEFAULT = deepcopy(_Aggregation.FIELDS_DEFAULT)
    FIELDS_DEFAULT['column_name'] = ''
    FIELDS_DEFAULT['weight_field_name'] = ''
    FIELDS_DEFAULT['side'] = 'BOTH'
    FIELDS_DEFAULT['weight_type'] = 'ABSOLUTE'

    FIELDS_TO_SKIP = ['output_field_name']

    output_field_type = float

    def __init__(
        self, column='', weight_field_name='', side='both', weight_type='absolute', symbols=None, *args, **kwargs,
    ):
        if side not in ['long', 'short', 'both']:
            raise ValueError(f"Parameter `side` has incorrect value: {side}, "
                             f"should be one of next: 'long', 'short', 'both")

        if weight_type not in ['absolute', 'relative']:
            raise ValueError(f"Parameter `weight_type` has incorrect value: {weight_type}, "
                             f"should be one of next: 'absolute', 'relative'")

        if isinstance(weight_field_name, _Column):
            weight_field_name = str(weight_field_name)

        self.weight_field_name = weight_field_name
        self.side = side.upper()
        self.weight_type = weight_type.upper()
        self.symbols = symbols

        super().__init__(column=column, *args, **kwargs)

    def apply(self, src, name='VALUE', inplace=False):
        if inplace:
            res = src
            src = src.copy()
        else:
            res = src.copy()

        out_name = name or self.column_name
        schema = self._get_common_schema(src, out_name)
        # it's important to validate input schema before sinking
        self._modify_source(res)

        ep = self.to_ep(name=str(out_name))

        if self.symbols:
            from onetick.py.core.source import Source
            from onetick.py.sources import query as otp_query
            from onetick.py.core.eval_query import _QueryEvalWrapper

            tmp_otq = TmpOtq()

            if isinstance(self.symbols, (otp_query, _QueryEvalWrapper)):
                self.symbols = self.symbols.to_eval_string(tmp_otq=tmp_otq)
            elif isinstance(self.symbols, (Source, otq.GraphQuery)):
                self.symbols = Source._convert_symbol_to_string(self.symbols, tmp_otq=tmp_otq)

            ep = ep.symbols(self.symbols)

            new_res = Source(schema=schema)

            new_res._copy_state_vars_from(res)
            new_res._clean_sources_dates()
            new_res._merge_tmp_otq(res)

            if isinstance(self.symbols, Source):
                new_res._merge_tmp_otq(self.symbols)

            new_res._tmp_otq.merge(tmp_otq)

            eps = defaultdict()
            new_res.source(res.node().copy_graph(eps))
            new_res.node().add_rules(res.node().copy_rules())
            new_res._set_sources_dates(res, copy_symbols=not bool(self.symbols))

            res = new_res

            if inplace:
                src = res

        res.sink(ep)

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
        if self.weight_field_name and self.weight_field_name not in src.schema:
            raise TypeError(
                f"Aggregation `{self.NAME}` uses column `{self.weight_field_name}` as input, which doesn't exist",
            )

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None):
        if not name:
            name = self.DEFAULT_OUTPUT_NAME

        return {
            name: float,
            'NUM_SYMBOLS': int,
        }


class MultiPortfolioPrice(PortfolioPrice):
    NAME = 'MULTI_PORTFOLIO_PRICE'
    EP = otq.MultiPortfolioPrice
    DEFAULT_OUTPUT_NAME = 'VALUE'

    FIELDS_MAPPING = deepcopy(PortfolioPrice.FIELDS_MAPPING)
    FIELDS_MAPPING['weight_multiplier_field_name'] = 'WEIGHT_MULTIPLIER_FIELD_NAME'
    FIELDS_MAPPING['portfolios_query'] = 'PORTFOLIOS_QUERY'
    FIELDS_MAPPING['portfolios_query_params'] = 'PORTFOLIOS_QUERY_PARAMS'
    FIELDS_MAPPING['portfolio_value_field_name'] = 'PORTFOLIO_VALUE_FIELD_NAME'

    FIELDS_DEFAULT = deepcopy(PortfolioPrice.FIELDS_DEFAULT)
    FIELDS_DEFAULT['weight_multiplier_field_name'] = ''
    FIELDS_DEFAULT['portfolios_query'] = ''
    FIELDS_DEFAULT['portfolios_query_params'] = ''
    FIELDS_DEFAULT['portfolio_value_field_name'] = ''

    FIELDS_TO_SKIP = ['output_field_name']

    output_field_type = float

    def __init__(
        self, columns='', weight_multiplier_field_name='', portfolios_query='', portfolios_query_params='',
        portfolio_value_field_name='', *args, **kwargs,
    ):
        if not portfolios_query:
            raise ValueError('Required parameter `portfolio_query` not set')

        self.weight_multiplier_field_name = weight_multiplier_field_name

        from onetick.py.core.source import Source
        if isinstance(portfolios_query, Source):
            self.portfolios_query = portfolios_query.to_otq()
        else:
            self.portfolios_query = portfolios_query

        if isinstance(portfolios_query_params, dict):
            portfolios_query_params = ','.join([f'{k}={ott.value2str(v)}' for k, v in portfolios_query_params.items()])

        self.portfolios_query_params = portfolios_query_params

        if not portfolio_value_field_name:
            portfolio_value_field_name = self.DEFAULT_OUTPUT_NAME

        if isinstance(portfolio_value_field_name, str):
            portfolio_value_field_name_str = portfolio_value_field_name
            portfolio_value_field_name_list = portfolio_value_field_name.split(',')
        else:
            portfolio_value_field_name_str = ','.join(map(str, portfolio_value_field_name))
            portfolio_value_field_name_list = list(map(str, portfolio_value_field_name))

        self._portfolio_value_field_name = portfolio_value_field_name_list
        self.portfolio_value_field_name = portfolio_value_field_name_str

        len_columns = max(len(columns) if isinstance(columns, list) else len(columns.split(',')), 1)
        if len_columns != len(portfolio_value_field_name_list):
            raise RuntimeError('The number of the field names must match the number of the field names '
                               'listed in the `columns` parameter')

        super().__init__(column=columns, *args, **kwargs)

    def validate_input_columns(self, src: 'Source'):
        super().validate_input_columns(src)

        if self.weight_multiplier_field_name and self.weight_multiplier_field_name not in src.schema:
            raise TypeError(
                f"Aggregation `{self.NAME}` uses column `{self.weight_multiplier_field_name}` as input, "
                f"which doesn't exist",
            )

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None):
        if not name:
            return super()._get_output_schema(src, name)

        return {
            **{column_name: float for column_name in self._portfolio_value_field_name},
            'NUM_SYMBOLS': int,
            'PORTFOLIO_NAME': str,
        }


class Return(_Aggregation):
    NAME = 'RETURN'
    EP = otq.Return
    DEFAULT_OUTPUT_NAME = 'RETURN'

    output_field_type = float


class ImpliedVol(_Aggregation):
    NAME = 'IMPLIED_VOL'
    EP = otq.ImpliedVol

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['interest_rate'] = 'INTEREST_RATE'
    FIELDS_MAPPING['price_field_name'] = 'PRICE_FIELD_NAME'
    FIELDS_MAPPING['option_price_field_name'] = 'OPTION_PRICE_FIELD_NAME'
    FIELDS_MAPPING['method'] = 'METHOD'
    FIELDS_MAPPING['precision'] = 'PRECISION'
    FIELDS_MAPPING['value_for_non_converge'] = 'VALUE_FOR_NON_CONVERGE'
    FIELDS_MAPPING['interest_rate_field_name'] = 'INTEREST_RATE_FIELD_NAME'
    FIELDS_MAPPING['option_type_field_name'] = 'OPTION_TYPE_FIELD_NAME'
    FIELDS_MAPPING['strike_price_field_name'] = 'STRIKE_PRICE_FIELD_NAME'
    FIELDS_MAPPING['days_in_year'] = 'DAYS_IN_YEAR'
    FIELDS_MAPPING['days_till_expiration_field_name'] = 'DAYS_TILL_EXPIRATION_FIELD_NAME'
    FIELDS_MAPPING['expiration_date_field_name'] = 'EXPIRATION_DATE_FIELD_NAME'

    FIELDS_DEFAULT = deepcopy(_Aggregation.FIELDS_DEFAULT)
    FIELDS_DEFAULT['interest_rate'] = None
    FIELDS_DEFAULT['price_field_name'] = 'PRICE'
    FIELDS_DEFAULT['option_price_field_name'] = 'OPTION_PRICE'
    FIELDS_DEFAULT['method'] = 'NEWTON'
    FIELDS_DEFAULT['precision'] = 0.00001
    FIELDS_DEFAULT['value_for_non_converge'] = 'NAN_VAL'
    FIELDS_DEFAULT['interest_rate_field_name'] = ''
    FIELDS_DEFAULT['option_type_field_name'] = ''
    FIELDS_DEFAULT['strike_price_field_name'] = ''
    FIELDS_DEFAULT['days_in_year'] = 365
    FIELDS_DEFAULT['days_till_expiration_field_name'] = ''
    FIELDS_DEFAULT['expiration_date_field_name'] = ''

    FIELDS_TO_SKIP = ['column_name', 'output_field_name']

    output_field_type = float

    def __init__(
        self,
        interest_rate: Optional[Union[float, int, _Column, str]] = None,
        price_field: Union[_Column, str] = 'PRICE',
        option_price_field: Union[_Column, str] = 'OPTION_PRICE',
        method: str = 'newton',
        precision: float = 0.00001,
        value_for_non_converge: str = 'nan_val',
        option_type_field: Union[_Column, str] = '',
        strike_price_field: Union[_Column, str] = '',
        days_in_year: int = 365,
        days_till_expiration_field: Union[_Column, str] = '',
        expiration_date_field: Union[_Column, str] = '',
        *args, **kwargs,
    ):
        self.interest_rate_field_name = ''
        self.interest_rate = None

        if interest_rate is not None:
            if isinstance(interest_rate, (str, _Column)):
                self.interest_rate_field_name = str(interest_rate)
            elif isinstance(interest_rate, (int, float)):
                self.interest_rate = interest_rate
            else:
                raise ValueError(f'Unsupported value passed as `interest_rate`: {type(interest_rate)}')

        if value_for_non_converge not in {'nan_val', 'closest_found_val'}:
            raise ValueError(f'Unsupported value for `value_for_non_converge` was passed: {value_for_non_converge}')

        if method not in {'newton', 'newton_with_fallback', 'bisections'}:
            raise ValueError(f'Unsupported value for `method` was passed: {method}')

        if isinstance(price_field, _Column):
            price_field = str(price_field)
        if isinstance(option_price_field, _Column):
            option_price_field = str(option_price_field)

        if isinstance(option_type_field, _Column):
            option_type_field = str(option_type_field)
        if isinstance(strike_price_field, _Column):
            strike_price_field = str(strike_price_field)
        if isinstance(days_till_expiration_field, _Column):
            days_till_expiration_field = str(days_till_expiration_field)
        if isinstance(expiration_date_field, _Column):
            expiration_date_field = str(expiration_date_field)

        self.price_field_name = price_field
        self.option_price_field_name = option_price_field
        self.method = method.upper()
        self.precision = precision
        self.value_for_non_converge = value_for_non_converge.upper()
        self.option_type_field_name = option_type_field
        self.strike_price_field_name = strike_price_field
        self.days_in_year = days_in_year
        self.days_till_expiration_field_name = days_till_expiration_field
        self.expiration_date_field_name = expiration_date_field

        super().__init__(_Column(price_field), *args, **kwargs)

    def validate_input_columns(self, src: 'Source'):
        columns_to_check: Dict[str, Tuple[Union[str, _Column], Union[Tuple, type]]] = {
            'price_field': (self.price_field_name, (int, float)),
            'option_price_field': (self.option_price_field_name, (int, float)),
            'interest_rate': (self.interest_rate_field_name, (int, float)),
            'option_type_field': (self.option_type_field_name, str),
            'strike_price_field': (self.strike_price_field_name, (int, float)),
            'days_till_expiration_field': (self.days_till_expiration_field_name, int),
            'expiration_date_field': (self.expiration_date_field_name, int),
        }

        for parameter, (column, required_type) in columns_to_check.items():
            if not column:
                # probably parameter will be set as symbol parameter, we couldn't check this right now
                continue

            if column not in src.schema:
                raise TypeError(
                    f"Aggregation `{self.NAME}` uses column `{column}` from parameter `{parameter}` as input, "
                    f"which doesn't exist."
                )

            if not issubclass(src.schema[column], required_type):
                raise TypeError(
                    f"Aggregation `{self.NAME}` require column `{column}` from parameter `{parameter}` "
                    f"to be {required_type}, got {src.schema[column]}"
                )

    def apply(self, src: 'Source', name: str = 'VALUE', *args, **kwargs) -> 'Source':
        return super().apply(src=src, name=name, *args, **kwargs)


class LinearRegression(_Aggregation, _MultiColumnAggregation):
    NAME = 'LINEAR_REGRESSION'
    EP = None

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['dependent_variable_field_name'] = 'DEPENDENT_VARIABLE_FIELD_NAME'
    FIELDS_MAPPING['independent_variable_field_name'] = 'INDEPENDENT_VARIABLE_FIELD_NAME'

    FIELDS_TO_SKIP = ['column_name', 'output_field_name']

    def __init__(
        self,
        dependent_variable_field_name: Union[_Column, str, OnetickParameter],
        independent_variable_field_name: Union[_Column, str, OnetickParameter],
        *args, **kwargs,
    ):
        try:
            self.EP = otq.LinearRegression
        except AttributeError as exc:
            raise RuntimeError("Used onetick installation not support onetick.query.LinearRegression") from exc

        if isinstance(dependent_variable_field_name, _Column):
            dependent_variable_field_name = str(dependent_variable_field_name)
        if isinstance(independent_variable_field_name, _Column):
            independent_variable_field_name = str(independent_variable_field_name)

        self.dependent_variable_field_name = dependent_variable_field_name
        self.independent_variable_field_name = independent_variable_field_name

        super().__init__(_Column('TIMESTAMP'), *args, **kwargs)

    def validate_input_columns(self, src: 'Source'):
        super().validate_input_columns(src)

        for column in [self.dependent_variable_field_name, self.independent_variable_field_name]:
            if isinstance(column, OnetickParameter):
                continue

            if column not in src.schema:
                raise TypeError(f"Aggregation `{self.NAME}` uses column `{column}` as input, which doesn't exist")

        for out_field in ['SLOPE', 'INTERCEPT']:
            if out_field in src.schema:
                raise TypeError(f"Field `{out_field}`, which is `{self.NAME}` aggregation output column, is in schema.")

    def _get_output_schema(self, src: 'Source', name: Optional[str] = None) -> dict:
        return {
            'SLOPE': float,
            'INTERCEPT': float,
        }
