from typing import Iterable, Callable

import onetick.py as otp
from onetick.py.docs.utils import docstring

from ..aggregations.order_book import (
    OB_SNAPSHOT_DOC_PARAMS,
    OB_SNAPSHOT_WIDE_DOC_PARAMS,
    OB_SNAPSHOT_FLAT_DOC_PARAMS,
    OB_SUMMARY_DOC_PARAMS,
    OB_SIZE_DOC_PARAMS,
    OB_VWAP_DOC_PARAMS,
    OB_NUM_LEVELS_DOC_PARAMS,
)
from ..aggregations.functions import (
    ob_snapshot, ob_snapshot_wide, ob_snapshot_flat, ob_summary, ob_size, ob_vwap, ob_num_levels,
)
from .. import utils

from .data_source import DataSource, DATA_SOURCE_DOC_PARAMS


class _ObSource(DataSource):
    OB_AGG_FUNC: Callable
    OB_AGG_PARAMS: Iterable
    _PROPERTIES = DataSource._PROPERTIES + ['_ob_agg']

    def __init__(self, db=None, schema=None, **kwargs):
        if self._try_default_constructor(schema=schema, **kwargs):
            return

        ob_agg_params = {
            param.name: kwargs.pop(param.name, param.default)
            for _, param in self.OB_AGG_PARAMS
        }

        symbol_param = kwargs.get('symbol')
        symbols_param = kwargs.get('symbols')

        if symbol_param and symbols_param:
            raise ValueError(
                'You have set the `symbol` and `symbols` parameters together, it is not allowed. '
                'Please, clarify parameters'
            )

        symbols = symbol_param if symbol_param else symbols_param
        tmp_otq = None

        # Use bound symbols only in case, if db not passed
        use_bound_symbols = not db and symbols and symbols is not utils.adaptive
        if use_bound_symbols:
            symbols, tmp_otq = self._cross_symbol_convert(symbols, kwargs.get('symbol_date'))

            if symbols_param:
                del kwargs['symbols']

            kwargs['symbol'] = None

        self._ob_agg = self.__class__.OB_AGG_FUNC(**ob_agg_params)

        if kwargs.get('schema_policy') in [DataSource.POLICY_MANUAL, DataSource.POLICY_MANUAL_STRICT]:
            self._ob_agg.disable_ob_input_columns_validation()

        if use_bound_symbols:
            self._ob_agg.set_bound_symbols(symbols)

        super().__init__(db=db, schema=schema, **kwargs)

        ob_agg_output_schema = self._ob_agg._get_output_schema(otp.Empty())

        if getattr(self._ob_agg, 'show_full_detail', None):
            self.schema.update(**ob_agg_output_schema)
        else:
            self.schema.set(**ob_agg_output_schema)

        if tmp_otq:
            self._tmp_otq.merge(tmp_otq)

    def base_ep(self, *args, **kwargs):
        src = super().base_ep(*args, **kwargs)
        return self._ob_agg.apply(src)

    def _base_ep_for_cross_symbol(self, *args, **kwargs):
        src = super()._base_ep_for_cross_symbol(*args, **kwargs)
        return self._ob_agg.apply(src)


@docstring(parameters=OB_SNAPSHOT_DOC_PARAMS + DATA_SOURCE_DOC_PARAMS)
class ObSnapshot(_ObSource):
    """
    Construct a source providing order book snapshot for a given ``db``.
    This is just a shortcut for
    :class:`~onetick.py.DataSource` + :func:`~onetick.py.agg.ob_snapshot`.

    See also
    --------
    | :class:`onetick.py.DataSource`
    | :meth:`onetick.py.Source.ob_snapshot`
    | :func:`onetick.py.agg.ob_snapshot`
    | **OB_SNAPSHOT** OneTick event processor

    Examples
    ---------

    >>> data = otp.ObSnapshot(db='SOME_DB', tick_type='PRL', symbols='AA', max_levels=1) # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  PRICE             UPDATE_TIME  SIZE  LEVEL  BUY_SELL_FLAG
    0 2003-12-04    2.0 2003-12-01 00:00:00.003     6      1              1
    1 2003-12-04    5.0 2003-12-01 00:00:00.004     7      1              0
    """
    OB_AGG_FUNC = ob_snapshot
    OB_AGG_PARAMS = OB_SNAPSHOT_DOC_PARAMS


@docstring(parameters=OB_SNAPSHOT_WIDE_DOC_PARAMS + DATA_SOURCE_DOC_PARAMS)
class ObSnapshotWide(_ObSource):
    """
    Construct a source providing order book wide snapshot for a given ``db``.
    This is just a shortcut for
    :class:`~onetick.py.DataSource` + :func:`~onetick.py.agg.ob_snapshot_wide`.

    See also
    --------
    | :class:`onetick.py.DataSource`
    | :meth:`onetick.py.Source.ob_snapshot_wide`
    | :func:`onetick.py.agg.ob_snapshot_wide`
    | **OB_SNAPSHOT_WIDE** OneTick event processor

    Examples
    ---------

    >>> data = otp.ObSnapshotWide(db='SOME_DB', tick_type='PRL', symbols='AA', max_levels=1) # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  BID_PRICE         BID_UPDATE_TIME  BID_SIZE  ASK_PRICE         ASK_UPDATE_TIME  ASK_SIZE  LEVEL
    0 2003-12-03        5.0 2003-12-01 00:00:00.004         7        2.0 2003-12-01 00:00:00.003         6      1
    """
    OB_AGG_FUNC = ob_snapshot_wide
    OB_AGG_PARAMS = OB_SNAPSHOT_WIDE_DOC_PARAMS


@docstring(parameters=OB_SNAPSHOT_FLAT_DOC_PARAMS + DATA_SOURCE_DOC_PARAMS)
class ObSnapshotFlat(_ObSource):
    """
    Construct a source providing order book flat snapshot for a given ``db``.
    This is just a shortcut for
    :class:`~onetick.py.DataSource` + :func:`~onetick.py.agg.ob_snapshot_flat`.

    See also
    --------
    | :class:`onetick.py.DataSource`
    | :meth:`onetick.py.Source.ob_snapshot_flat`
    | :func:`onetick.py.agg.ob_snapshot_flat`
    | **OB_SNAPSHOT_FLAT** OneTick event processor

    Examples
    ---------

    >>> data = otp.ObSnapshotFlat(db='SOME_DB', tick_type='PRL', symbols='AA', max_levels=1) # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  BID_PRICE1        BID_UPDATE_TIME1  BID_SIZE1  ASK_PRICE1        ASK_UPDATE_TIME1  ASK_SIZE1
    0 2003-12-03         5.0 2003-12-01 00:00:00.004          7         2.0 2003-12-01 00:00:00.003          6
    """
    OB_AGG_FUNC = ob_snapshot_flat
    OB_AGG_PARAMS = OB_SNAPSHOT_FLAT_DOC_PARAMS


@docstring(parameters=OB_SUMMARY_DOC_PARAMS + DATA_SOURCE_DOC_PARAMS)
class ObSummary(_ObSource):
    """
    Construct a source providing order book summary for a given ``db``.
    This is just a shortcut for
    :class:`~onetick.py.DataSource` + :func:`~onetick.py.agg.ob_summary`.

    See also
    --------
    | :class:`onetick.py.DataSource`
    | :meth:`onetick.py.Source.ob_summary`
    | :func:`onetick.py.agg.ob_summary`
    | **OB_SUMMARY** OneTick event processor

    Examples
    ---------

    >>> data = otp.ObSummary(db='SOME_DB', tick_type='PRL', symbols='AA', max_levels=1) # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  BID_PRICE  BID_SIZE  BID_VWAP  BEST_BID_PRICE  WORST_BID_SIZE  NUM_BID_LEVELS  ASK_SIZE\
              ASK_VWAP  BEST_ASK_PRICE  WORST_ASK_PRICE  NUM_ASK_LEVELS
    0 2003-12-04        NaN         7       5.0             5.0             NaN               1         6\
           2.0             2.0              2.0               1
    """
    OB_AGG_FUNC = ob_summary
    OB_AGG_PARAMS = OB_SUMMARY_DOC_PARAMS


@docstring(parameters=OB_SIZE_DOC_PARAMS + DATA_SOURCE_DOC_PARAMS)
class ObSize(_ObSource):
    """
    Construct a source providing number of order book levels for a given ``db``.
    This is just a shortcut for
    :class:`~onetick.py.DataSource` + :func:`~onetick.py.agg.ob_size`.

    See also
    --------
    | :class:`onetick.py.DataSource`
    | :meth:`onetick.py.Source.ob_size`
    | :func:`onetick.py.agg.ob_size`
    | **OB_SIZE** OneTick event processor

    Examples
    ---------

    >>> data = otp.ObSize(db='SOME_DB', tick_type='PRL', symbols='AA', max_levels=10) # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  ASK_VALUE  BID_VALUE
    0 2003-12-01      84800      64500
    """
    OB_AGG_FUNC = ob_size
    OB_AGG_PARAMS = OB_SIZE_DOC_PARAMS


@docstring(parameters=OB_VWAP_DOC_PARAMS + DATA_SOURCE_DOC_PARAMS)
class ObVwap(_ObSource):
    """
    Construct a source providing the size-weighted price
    computed over a specified number of order book levels for a given ``db``.
    This is just a shortcut for
    :class:`~onetick.py.DataSource` + :func:`~onetick.py.agg.ob_vwap`.

    See also
    --------
    | :class:`onetick.py.DataSource`
    | :meth:`onetick.py.Source.ob_vwap`
    | :func:`onetick.py.agg.ob_vwap`
    | **OB_VWAP** OneTick event processor

    Examples
    ---------

    >>> data = otp.ObVwap(db='SOME_DB', tick_type='PRL', symbols='AA', max_levels=10) # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  ASK_VALUE  BID_VALUE
    0 2003-12-01     23.313   23.20848
    """
    OB_AGG_FUNC = ob_vwap
    OB_AGG_PARAMS = OB_VWAP_DOC_PARAMS


@docstring(parameters=OB_NUM_LEVELS_DOC_PARAMS + DATA_SOURCE_DOC_PARAMS)
class ObNumLevels(_ObSource):
    """
    Construct a source providing the number of levels in the order book for a given ``db``.
    This is just a shortcut for
    :class:`~onetick.py.DataSource` + :func:`~onetick.py.agg.ob_num_levels`.

    See also
    --------
    | :class:`onetick.py.DataSource`
    | :meth:`onetick.py.Source.ob_num_levels`
    | :func:`onetick.py.agg.ob_num_levels`
    | **OB_NUM_LEVELS** OneTick event processor

    Examples
    ---------

    >>> data = otp.ObNumLevels(db='SOME_DB', tick_type='PRL', symbols='AA') # doctest: +SKIP
    >>> otp.run(data) # doctest: +SKIP
            Time  ASK_VALUE  BID_VALUE
    0 2003-12-01        248         67
    """
    OB_AGG_FUNC = ob_num_levels
    OB_AGG_PARAMS = OB_NUM_LEVELS_DOC_PARAMS
