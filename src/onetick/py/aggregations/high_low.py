from typing import Union, TYPE_CHECKING
from copy import deepcopy

if TYPE_CHECKING:
    from onetick.py.core.source import Source   # hack for annotations

from onetick.py.core.column import _Column
from onetick.py import types as ott
from onetick.py.otq import otq

from ._base import (
    _AggregationTSType, _AggregationTSSelection, _KeepTs, _FloatAggregation, _ExpectLargeInts, _AllColumnsAggregation,
)


class Max(_AggregationTSType, _ExpectLargeInts):

    NAME = "HIGH"
    EP = otq.High
    require_type = (int, float, ott.nsectime, ott._inf)

    FIELDS_MAPPING = deepcopy(_AggregationTSType.FIELDS_MAPPING)
    FIELDS_MAPPING.update(_ExpectLargeInts.FIELDS_MAPPING)
    FIELDS_DEFAULT = deepcopy(_AggregationTSType.FIELDS_DEFAULT)
    FIELDS_DEFAULT.update(_ExpectLargeInts.FIELDS_DEFAULT)


class Min(Max):
    NAME = "LOW"
    EP = otq.Low


class HighTick(_AggregationTSType, _AggregationTSSelection, _KeepTs, _FloatAggregation, _AllColumnsAggregation):
    EP = otq.HighTick
    NAME = 'HIGH_TICK'
    DEFAULT_OUTPUT_NAME = 'HIGH_TICK'

    FIELDS_MAPPING = deepcopy(_AggregationTSType.FIELDS_MAPPING)
    FIELDS_MAPPING.update(_AggregationTSSelection.FIELDS_MAPPING)
    FIELDS_MAPPING['n'] = 'NUM_TICKS'
    FIELDS_DEFAULT = deepcopy(_AggregationTSType.FIELDS_DEFAULT)
    FIELDS_DEFAULT.update(_AggregationTSSelection.FIELDS_DEFAULT)
    FIELDS_DEFAULT['n'] = 1

    FIELDS_TO_SKIP = ['output_field_name', 'all_fields']

    def __init__(self, column: Union[str, _Column], n: int = 1, *args, **kwargs):
        """
        Select `n` ticks with the highest values in the `column` field
        """
        super().__init__(column, *args, **kwargs)
        self.n = n

    @staticmethod
    def validate_output_name(*args, **kwargs):
        # HighTick and LowTick aggregations don't have output fields
        pass


class LowTick(HighTick):
    EP = otq.LowTick
    NAME = 'LOW_TICK'
    DEFAULT_OUTPUT_NAME = 'LOW_TICK'


class HighTime(_AggregationTSType, _AggregationTSSelection, _FloatAggregation):
    NAME = "HIGH_TIME"
    EP = otq.HighTime

    FIELDS_MAPPING = deepcopy(_AggregationTSType.FIELDS_MAPPING)
    FIELDS_MAPPING.update(_AggregationTSSelection.FIELDS_MAPPING)
    FIELDS_DEFAULT = deepcopy(_AggregationTSType.FIELDS_DEFAULT)
    FIELDS_DEFAULT.update(_AggregationTSSelection.FIELDS_DEFAULT)
    output_field_type = ott.nsectime


class LowTime(HighTime):
    """Returns timestamp of tick with lowest value of input field"""
    NAME = "LOW_TIME"
    EP = otq.LowTime
