from typing import List, Dict, TYPE_CHECKING, Tuple, Optional, Union
from copy import deepcopy
from collections import namedtuple
from onetick.py.compatibility import is_supported_agg_option_price

if TYPE_CHECKING:
    from onetick.py.core.source import Source  # hack for annotations

from onetick.py.core.column import Column
from onetick.py import types as ott
from onetick.py.otq import otq
import onetick.py as otp

from ._base import _Aggregation, operation_gb, operation_replacer, _MultiColumnAggregation
from onetick.py.aggregations.other import (
    FirstTick, LastTick, OptionPrice, Ranking, Percentile, PortfolioPrice, MultiPortfolioPrice,
)
from onetick.py.aggregations.high_low import HighTick, LowTick
from onetick.py.aggregations.generic import Generic


class Compute(_Aggregation):
    @property
    def NAME(self):
        return "COMPUTE"

    @property
    def EP(self):
        try:
            return otq.compute
        except AttributeError:
            return otq.ComputeEp

    FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
    FIELDS_MAPPING['all_fields'] = 'SHOW_ALL_FIELDS'
    FIELDS_TO_SKIP = ['column_name']

    DISALLOWED_FIELDS = ["running", "all_fields", "bucket_interval", "bucket_time",
                         "bucket_units", "end_condition_per_group", "boundary_tick_bucket", "group_by"]
    # [bucket_end_condition, all_fields] can not be used without bucket_units=flexible -> already disallowed

    _validations_to_skip = ['running_all_fields']

    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(column=Column('Time'), *args, **kwargs)

        self.aggrs = {}
        self.has_multi_column_aggregations = False

        # Aggregation from `all_fields` parameter is saved as regular aggregation in `self.aggrs` with empty field name
        if isinstance(self.all_fields, str):
            if self.all_fields.lower() not in ["last", "first", "high", "low", "when_ticks_exit_window"]:
                raise ValueError(f"Unknown all_fields '{self.all_fields}' policy provided.")

            if isinstance(self.all_fields, str) and self.all_fields.lower() != "when_ticks_exit_window":
                if self.running:
                    raise ValueError(
                        "`all_fields` can't be one of 'last', 'first', 'high' or 'low' when `running=True`"
                    )

                aggr_class = globals()[self.all_fields.capitalize() + "Tick"]
                aggr_args = {}
                if self.all_fields in {"high", "low"}:
                    aggr_args['column'] = "PRICE"

                all_fields_aggr = aggr_class(**aggr_args)

                self.add('', all_fields_aggr)

        if isinstance(self.all_fields, (HighTick, LowTick)):
            self.add('', self.all_fields)

        if self.all_fields is True and not self.running:
            self.add('', FirstTick())

    def add(self, name: str, aggr: _Aggregation):
        """Adds aggregation for multiple aggregation"""
        self._validate_aggregation(aggr)
        if name in self.aggrs:
            raise KeyError(f'You are trying to output two aggregations to one field: "{name}"')

        if name == '' and not isinstance(aggr, (FirstTick, LastTick, HighTick, LowTick)):
            raise ValueError(
                'Only one instance of FirstTick, LastTick, HighTick or LowTick aggregation '
                'is allowed to have empty name in order to save compatibility with `all_fields` parameter'
            )

        if aggr.is_multi_column_aggregation:
            self.has_multi_column_aggregations = True

        self.aggrs[name] = aggr

    def _validate_aggregation(self, aggr: _Aggregation):
        if not isinstance(aggr, _Aggregation):
            raise TypeError(f"It is allowed to pass only aggregations, but got '{type(aggr)}'")

        if isinstance(aggr, OptionPrice) and not is_supported_agg_option_price():
            raise NotImplementedError(f".agg() method does not support {type(aggr)} on OneTick {otp.__build__} build")

        if isinstance(aggr, Ranking):
            raise ValueError('Using ranking aggregation in otp.Source.agg method is not supported. '
                             'Use otp.agg.ranking.apply() or otp.Source.ranking() methods.')

        if isinstance(aggr, Percentile):
            raise ValueError('Using percentile aggregation in otp.Source.agg method is not supported. '
                             'Use otp.agg.percentile.apply() or otp.Source.percentile() methods.')

        if isinstance(aggr, PortfolioPrice) and getattr(aggr, 'symbols'):
            raise ValueError('It\'s not allowed to pass `symbols` parameter while using `portfolio_price` '
                             'in compute or `Source.agg`')

        if isinstance(aggr, MultiPortfolioPrice):
            raise ValueError('Using multi_portfolio_price aggregation in otp.Source.agg method is not supported, '
                             'because it outputs multiple ticks at once. '
                             'Use otp.agg.percentile.apply() or otp.Source.percentile() methods.')

        for field in self.DISALLOWED_FIELDS:
            if field not in aggr.FIELDS_TO_SKIP and getattr(aggr, field) != aggr.FIELDS_DEFAULT[field]:
                raise ValueError(f'"{field}" parameter can not be specified in multiple aggregation')

    def validate_input_columns(self, src: 'Source'):
        self._validate_aggregation_with_source(src)

    def _validate_aggregation_with_source(self, src: 'Source'):
        for name, aggr in self.aggrs.items():
            aggr.validate_input_columns(src)

    @property
    def ep_params(self) -> Dict:
        all_fields_orig = self.all_fields
        if self.has_multi_column_aggregations:
            self.all_fields = False
        params = super().ep_params
        if self.has_multi_column_aggregations:
            self.all_fields = all_fields_orig

        params['append_output_field_name'] = self.has_multi_column_aggregations

        what_to_compute = []
        for name, aggr in self.aggrs.items():
            if name:
                str_aggr = str(aggr) + " " + name
            else:
                agg_params = [f'{k}={aggr._attr2str(v)}' for k, v in aggr.ep_params.items()]
                agg_params.append("KEEP_INITIAL_SCHEMA='true'")
                str_aggr = aggr.NAME + "(" + ",".join(agg_params) + ")"

            what_to_compute.append(str_aggr)

        if what_to_compute:
            params['COMPUTE'] = ','.join(what_to_compute)

        return params

    def to_ep(self, *args, **kwargs) -> otq.EpBase:
        params = dict((k.lower(), v) for k, v in self.ep_params.items())
        return self.EP(**params)

    @operation_gb
    @operation_replacer
    def apply(self, src: 'Source') -> 'Source':
        if not self.aggrs:
            raise TypeError('Nothing to aggregate')

        res = src.copy()
        res, what_to_wrap = self._ts_to_long(res)
        schema = self._get_common_schema(res, [key for key in self.aggrs.keys() if key])

        # run _modify_source like it is in apply
        for aggr in self.aggrs.values():
            if isinstance(aggr, Generic):
                aggr_schema = aggr._detect_query_fun_schema(res)

                if aggr._query is None:
                    raise RuntimeError('Generic aggregation was not initialized correctly.')

                # keep only fields from schema
                query = aggr._query.table(**aggr_schema, strict=True)

                # add LAST_TICK EP
                default_tick = ','.join(f'{field} {ott.type2str(dtype)}' for field, dtype in aggr_schema.items())
                aggr._query = query.sink(otq.LastTick(default_tick=default_tick), inplace=False)

                # set empty query params for Generic aggregations
                aggr._set_query_params()

            aggr._modify_source(res)

        # it's important to validate input schema before sinking
        res.sink(self.to_ep())

        for name, aggr in self.aggrs.items():
            if not name:
                # No need to check `all_fields` aggregation: it will have the same schema, as input source
                continue

            if not aggr.is_multi_column_aggregation:
                agg_out_schema = aggr._get_output_schema(res, name=name)
            else:
                aggr_schema_base = aggr._get_output_schema(res)
                if aggr.is_all_columns_aggregation:
                    aggr_schema_base = src.schema

                agg_out_schema = {f'{name}.{key}': value for key, value in aggr_schema_base.items()}

            schema.update(agg_out_schema)

        res.schema.set(**schema)

        if self.has_multi_column_aggregations:
            res = self._remove_all_fields_tick_suffix(res)

        res = self._long_to_ts(res, what_to_wrap)
        return res

    def _ts_to_long(self, src: 'Source') -> Tuple['Source', List]:
        """Convert nsectime columns to long, so aggregation can work in a proper way"""
        res = src.copy()
        forward_result = namedtuple('forward_result', ('aggr', 'columns'))
        what_to_wrap = []
        for name, aggr in self.aggrs.items():
            if '_ts_to_long' in dir(aggr) and callable(aggr._ts_to_long):
                res, col, convert_back = aggr._ts_to_long(res, name)
                if convert_back:
                    what_to_wrap.append(forward_result(aggr, col))
        for r in what_to_wrap:
            self.aggrs[r.columns.tmp_out_column] = self.aggrs.pop(r.columns.out_column)
        return res, what_to_wrap

    def _long_to_ts(self, src: 'Source', what_to_wrap: List) -> 'Source':
        """Convert long columns to nsectime if needed (used along with _ts_to_long)"""
        res = src.copy()
        for r in what_to_wrap:
            res = r.aggr._long_to_ts(res, r.columns)
            self.aggrs[r.columns.out_column] = self.aggrs.pop(r.columns.tmp_out_column)
        return res

    def _remove_all_fields_tick_suffix(self, src: 'Source') -> 'Source':
        """
        renames columns (in query and schema) after compute with multi-column aggregations
        """
        schema = src.schema.copy()
        rename_rule = {}
        fields_to_drop = []

        for name, aggr in self.aggrs.items():
            if aggr.is_all_columns_aggregation or isinstance(aggr, Generic):
                # In case of Generic: remove TICK_TIME field returned by LAST_TICK from generic aggregation
                field_name = name if name else aggr.DEFAULT_OUTPUT_NAME
                tick_time_field = f'{field_name}.TICK_TIME'
                schema.update(**{tick_time_field: ott.nsectime})
                fields_to_drop.append(tick_time_field)

            if aggr.is_multi_column_aggregation:
                continue

            new_name = f'{name}.{aggr.DEFAULT_OUTPUT_NAME}'
            schema[new_name] = schema.pop(name)
            rename_rule[new_name] = name

        res = src.copy()
        res.schema.set(**schema)

        if fields_to_drop:
            res.drop(fields_to_drop, inplace=True)

        if rename_rule:
            res.rename(rename_rule, inplace=True)

        all_fields_aggr = self.aggrs.get('')
        if all_fields_aggr:
            # drop first_tick columns duplicating group_by columns
            all_fields_tick_name = all_fields_aggr.DEFAULT_OUTPUT_NAME

            for group_by in self.group_by:
                group_by_name = group_by
                if isinstance(group_by, Column):
                    group_by_name = group_by.name
                first_tick_field = f'{all_fields_tick_name}.{group_by_name}'
                res.sink(otq.Passthrough(fields=first_tick_field, drop_fields=True))

            res.sink(otq.RenameFieldsEp(rename_fields=rf"{all_fields_tick_name}\.(.*)=\1", use_regex=True))

        return res
