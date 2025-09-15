from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from onetick.py.core.source import Source   # hack for annotations

import onetick.py as otp
from onetick.py.otq import otq
from onetick.py.core.column import _Column
from ._base import _Aggregation, _MultiColumnAggregation


class Generic(_Aggregation, _MultiColumnAggregation):
    NAME = 'GENERIC_AGGREGATION'
    EP = otq.GenericAggregation

    FIELDS_TO_SKIP = ['column_name', 'all_fields', 'output_field_name']
    FIELDS_MAPPING = dict(_Aggregation.FIELDS_MAPPING, **{
        'query_name': 'QUERY_NAME',
        'bucket_delimiter': 'BUCKET_DELIMITERS',
    })
    FIELDS_DEFAULT = dict(_Aggregation.FIELDS_DEFAULT, **{
        'bucket_delimiter': None,
    })
    _validations_to_skip = ['running_all_fields']

    def __init__(self,
                 query_fun,
                 bucket_delimiter: bool = False,
                 **kwargs):
        self._query: Optional['Source'] = None
        self._query_fun = query_fun
        self._query_params: Optional[dict] = None
        self.bucket_delimiter = 'D' if bucket_delimiter else None

        # init variables which will be set later
        self.query_name: Optional[str] = None
        self._query_schema: Optional[dict] = None

        if 'all_fields' in kwargs:
            raise ValueError("Parameter 'all_fields' for generic aggregation is meaningless. "
                             "Aggregated source will have all fields returned by 'query_fun'.")
        # we don't want to set hard limit on the output of order book aggregations
        kwargs['all_fields'] = True
        super().__init__(column=_Column('TIMESTAMP'), **kwargs)

    def _set_query_params(self, **kwargs):
        self._query_params = kwargs

    def apply(self, src: 'Source', name: Optional[str] = None, **kwargs) -> 'Source':
        """Applies generic aggregation to Source and sets proper schema

        Parameters
        ----------
        src: Source
            Source to apply aggregation
        name: str, optional
            Name of output column. If not specified, will be used self.column_name
        kwargs: dict
            Parameters to be passed to `query_fun()` when creating aggregation query
        """
        self._set_query_params(**kwargs)
        return super().apply(src, name=name)

    def _make_query_object(self, schema):
        query_params = self._query_params if self._query_params else {}
        query = otp.DataSource(symbols='LOCAL::', tick_type='ANY', schema_policy='manual', schema=schema)
        query = self._query_fun(query, **query_params)
        return query

    def _detect_query_fun_schema(self, res):
        # this will be translated to passthrough with symbol and tick type set
        if self._query is None:
            self._query = self._make_query_object(res.schema)

        return self._query.schema.copy()

    def _modify_source(self, res: 'Source', **kwargs):
        query_schema = self._detect_query_fun_schema(res)

        # schema will be used when validating output
        if self._query_schema is None:
            self._query_schema = query_schema

        if self._query is None:
            raise RuntimeError('Attempted to use `self._query` before initialization')

        # query_name will be used to create ep
        query_name = self._query._store_in_tmp_otq(
            res._tmp_otq, operation_suffix='generic_aggregation', add_passthrough=False,
        )
        self.query_name = f'THIS::{query_name}'

    def _get_common_schema(self, src, name):
        super()._get_common_schema(src, name)
        return {
            column: src.schema[column]
            for column in map(str, self.group_by)
        }

    def _get_output_schema(self, src, name=None):
        schema = self._query_schema.copy()
        if self.bucket_delimiter:
            schema['DELIMITER'] = str
        return schema
