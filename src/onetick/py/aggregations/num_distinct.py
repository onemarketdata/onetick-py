from typing import TYPE_CHECKING
from copy import deepcopy

if TYPE_CHECKING:
    from onetick.py.core.source import Source   # hack for annotations

from onetick.py.otq import otq

from onetick.py.core.column import _Column
from onetick.py import types as ott

from ._base import _Aggregation
from ._docs import (_running_doc,
                    _all_fields_doc,
                    _bucket_interval_doc,
                    _bucket_time_doc,
                    _bucket_units_doc,
                    _bucket_end_condition_doc,
                    _boundary_tick_bucket_doc,
                    _group_by_doc,
                    _groups_to_display_doc)
from onetick.py.docs.utils import docstring


# OneTick build >= 20220913120000
if hasattr(otq, 'NumDistinct'):

    class NumDistinct(_Aggregation):
        NAME = 'NUM_DISTINCT'
        EP = otq.NumDistinct

        FIELDS_MAPPING = deepcopy(_Aggregation.FIELDS_MAPPING)
        FIELDS_MAPPING['keys'] = 'KEYS'

        FIELDS_TO_SKIP = ['column_name', 'end_condition_per_group']

        output_field_type = int

        def __init__(self, keys, *args, **kwargs):
            super().__init__(column=_Column('TIMESTAMP'), *args, **kwargs)
            if isinstance(keys, str):
                keys = [keys]
            self._keys = keys

        @property
        def keys(self):
            return ott.value2str(','.join(self._keys))

        def apply(self, src, name='VALUE'):
            return super().apply(src, name)

        def validate_input_columns(self, src: 'Source'):
            for column in self._keys:
                if column not in src.schema:
                    raise TypeError(f"Aggregation {self.__class__.__name__} uses"
                                    f" column '{column}' as input, which doesn't exist")

    @docstring(parameters=[_running_doc, _all_fields_doc,
                           _bucket_interval_doc, _bucket_units_doc, _bucket_time_doc,
                           _bucket_end_condition_doc, _boundary_tick_bucket_doc, _group_by_doc, _groups_to_display_doc])
    def num_distinct(*args, **kwargs):
        """
        Outputs number of distinct values for a specified set of key fields.

        Parameters
        ----------
        keys: str or list of str or list of :py:class:`~onetick.py.Column`
            Specifies a list of tick attributes for which unique values are found.
            The ticks in the input time series must contain those attributes.

        Examples
        --------
        >>> data = otp.Ticks(dict(X=[1, 3, 2, 1, 3]))
        >>> data = data.agg({'X': otp.agg.num_distinct('X')})
        >>> otp.run(data)
                Time  X
        0 2003-12-04  3

        See also
        --------
        **NUM_DISTINCT** OneTick event processor
        """
        return NumDistinct(*args, **kwargs)
