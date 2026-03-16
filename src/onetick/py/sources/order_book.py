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
    r"""
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

    >>> data = otp.ObSnapshot(db='CME_SAMPLE', tick_type='PRL_FULL', symbols=r'NQ\H24', max_levels=3)  # doctest: +SKIP
    >>> otp.run(data, start=otp.dt(2024, 2, 1, 10), end=otp.dt(2024, 2, 1, 10)) # doctest: +SKIP
                     Time     PRICE  SIZE  LEVEL                   UPDATE_TIME  BUY_SELL_FLAG
    0 2024-02-01 10:00:00  17351.75     1      1 2024-02-01 09:59:59.701711193              1
    1 2024-02-01 10:00:00  17352.00     3      2 2024-02-01 09:59:59.582195881              1
    2 2024-02-01 10:00:00  17352.25     3      3 2024-02-01 09:59:59.580457957              1
    3 2024-02-01 10:00:00  17351.25     1      1 2024-02-01 09:59:59.867609851              0
    4 2024-02-01 10:00:00  17351.00     6      2 2024-02-01 09:59:59.867226023              0
    5 2024-02-01 10:00:00  17350.75     2      3 2024-02-01 09:59:59.867226023              0
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

    >>> data = otp.ObSnapshotWide(db='CME_SAMPLE', tick_type='PRL_FULL', symbols=r'NQ\\H24',
    ...                           max_levels=3)  # doctest: +SKIP
    >>> otp.run(data, start=otp.dt(2024, 2, 1, 10), end=otp.dt(2024, 2, 1, 10)) # doctest: +SKIP
                     Time  BID_PRICE  BID_SIZE               BID_UPDATE_TIME  ASK_PRICE  ASK_SIZE \
                                      ASK_UPDATE_TIME  LEVEL
    0 2024-02-01 10:00:00   17351.25         1 2024-02-01 09:59:59.867609851   17351.75         1 \
                        2024-02-01 09:59:59.701711193      1
    1 2024-02-01 10:00:00   17351.00         6 2024-02-01 09:59:59.867226023   17352.00         3 \
                        2024-02-01 09:59:59.582195881      2
    2 2024-02-01 10:00:00   17350.75         2 2024-02-01 09:59:59.867226023   17352.25         3 \
                        2024-02-01 09:59:59.580457957      3
    """
    OB_AGG_FUNC = ob_snapshot_wide
    OB_AGG_PARAMS = OB_SNAPSHOT_WIDE_DOC_PARAMS


@docstring(parameters=OB_SNAPSHOT_FLAT_DOC_PARAMS + DATA_SOURCE_DOC_PARAMS)
class ObSnapshotFlat(_ObSource):
    r"""
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

    >>> data = otp.ObSnapshotFlat(db='CME_SAMPLE', tick_type='PRL_FULL', symbols=r'NQ\H24',
    ...                           max_levels=3)  # doctest: +SKIP
    >>> otp.run(data, start=otp.dt(2024, 2, 1, 10), end=otp.dt(2024, 2, 1, 10)) # doctest: +SKIP
                     Time  BID_PRICE1  BID_SIZE1              BID_UPDATE_TIME1  ASK_PRICE1  ASK_SIZE1 ...
    0 2024-02-01 10:00:00    17351.25          1 2024-02-01 09:59:59.867609851    17351.75          1 ...
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

    >>> data = otp.ObSummary(db='CME_SAMPLE', tick_type='PRL_FULL', symbols=r'NQ\\H24', max_levels=3)  # doctest: +SKIP
    >>> otp.run(data, start=otp.dt(2024, 2, 1, 10), end=otp.dt(2024, 2, 1, 10)) # doctest: +SKIP
                     Time  BID_SIZE      BID_VWAP  BEST_BID_PRICE  WORST_BID_PRICE  NUM_BID_LEVELS  ASK_SIZE \
                             ASK_VWAP  BEST_ASK_PRICE  WORST_ASK_PRICE  NUM_ASK_LEVELS
    0 2024-02-01 10:00:00         9  17350.972222        17351.25         17350.75               3         7 \
                         17352.071429        17351.75         17352.25               3
    """
    OB_AGG_FUNC = ob_summary
    OB_AGG_PARAMS = OB_SUMMARY_DOC_PARAMS


@docstring(parameters=OB_SIZE_DOC_PARAMS + DATA_SOURCE_DOC_PARAMS)
class ObSize(_ObSource):
    r"""
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

    >>> data = otp.ObSize(db='CME_SAMPLE', tick_type='PRL_FULL', symbols=r'NQ\H24',
    ...                   bucket_interval=otp.Minute(5), max_levels=3)  # doctest: +SKIP
    >>> otp.run(data, start=otp.dt(2024, 2, 1, 10), end=otp.dt(2024, 2, 1, 11)) # doctest: +SKIP
                      Time  ASK_VALUE  BID_VALUE
    0  2024-02-01 10:05:00       12.0       10.0
    1  2024-02-01 10:10:00       12.0        5.0
    2  2024-02-01 10:15:00       11.0       13.0
    ...
    """
    OB_AGG_FUNC = ob_size
    OB_AGG_PARAMS = OB_SIZE_DOC_PARAMS


@docstring(parameters=OB_VWAP_DOC_PARAMS + DATA_SOURCE_DOC_PARAMS)
class ObVwap(_ObSource):
    r"""
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

    >>> data = otp.ObVwap(db='CME_SAMPLE', tick_type='PRL_FULL', symbols=r'NQ\H24',
    ...                   bucket_interval=otp.Minute(5))  # doctest: +SKIP
    >>> otp.run(data, start=otp.dt(2024, 2, 1, 10), end=otp.dt(2024, 2, 1, 11)) # doctest: +SKIP
                      Time     ASK_VALUE     BID_VALUE
    0  2024-02-01 10:05:00  17493.087642  17013.839286
    1  2024-02-01 10:10:00  17486.863024  17006.515027
    2  2024-02-01 10:15:00  17494.471485  17014.829879
    ...
    """
    OB_AGG_FUNC = ob_vwap
    OB_AGG_PARAMS = OB_VWAP_DOC_PARAMS


@docstring(parameters=OB_NUM_LEVELS_DOC_PARAMS + DATA_SOURCE_DOC_PARAMS)
class ObNumLevels(_ObSource):
    r"""
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

    >>> data = otp.ObNumLevels(db='CME_SAMPLE', tick_type='PRL_FULL', symbols=r'NQ\H24',
    ...                        bucket_interval=otp.Second(300))  # doctest: +SKIP
    >>> otp.run(data, start=otp.dt(2024, 2, 1, 10), end=otp.dt(2024, 2, 1, 11))  # doctest: +SKIP
                      Time  ASK_VALUE  BID_VALUE
    0  2024-02-01 10:05:00      743.0      830.0
    1  2024-02-01 10:10:00      753.0      820.0
    2  2024-02-01 10:15:00      741.0      831.0
    ...
    """
    OB_AGG_FUNC = ob_num_levels
    OB_AGG_PARAMS = OB_NUM_LEVELS_DOC_PARAMS
