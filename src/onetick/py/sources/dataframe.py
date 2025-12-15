from typing import Optional, Tuple

import onetick.py as otp
from onetick.py.otq import otq
import pandas as pd

from onetick.py.core.source import Source

from .. import types as ott
from .. import utils

from .common import update_node_tick_type


class _ReadFromDataFrameSource(Source):
    def __init__(
        self,
        dataframe=None,
        timestamp_column=None,
        symbol_name_field=None,
        symbol_value=None,
        symbol=utils.adaptive,
        db=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        schema=None,
        **kwargs,
    ):
        if self._try_default_constructor(**kwargs):
            return

        if schema is None:
            schema = {}

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=lambda: self.base_ep(
                dataframe=dataframe,
                timestamp_column=timestamp_column,
                symbol_name_field=symbol_name_field,
                symbol_value=symbol_value,
                db=db,
                tick_type=tick_type,
                columns=schema,
            ),
            schema=schema,
        )

    def base_ep(
        self,
        dataframe,
        timestamp_column=None,
        symbol_name_field=None,
        symbol_value=None,
        db=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        columns=None,
    ):
        if columns is None:
            columns = {}

        if symbol_value is None:
            symbol_value = ''

        if symbol_name_field is None:
            symbol_name_field = ''

        if timestamp_column:
            temp_column_name = f'__TMP_TS_COLUMN__{timestamp_column}__'
            dataframe = dataframe.rename(columns={timestamp_column: temp_column_name})
            timestamp_column = temp_column_name

        src = Source(
            otq.ReadFromDataFrame(
                dataframe=dataframe,
                symbol_name_field=symbol_name_field,
                symbol_value=symbol_value,
            ).get_data_file_ep(),
            schema=columns,
        )

        update_node_tick_type(src, tick_type, db)

        if timestamp_column:
            # DATA_FILE_QUERY process timestamps as GMT timezone
            # In order to process timestamps with query TZ we store timestamps as strings
            # and process them after this EP
            src.sink(
                otq.UpdateField(
                    field="TIMESTAMP",
                    value=f'parse_nsectime("%Y-%m-%d %H:%M:%S.%J", {timestamp_column}, _TIMEZONE)',
                    allow_unordered_output_times=True,
                )
            )
            src.sink(otq.Passthrough(fields=timestamp_column, drop_fields=True))

        return src


def _get_offsets(dataframe, timestamp_column) -> Tuple[otp.datetime, list]:
    offsets = [ott.timedelta(0)]
    base_ts = pd.Timestamp(dataframe[timestamp_column][0])

    for i in range(1, len(dataframe)):
        diff: pd.Timedelta = pd.Timestamp(dataframe[timestamp_column][i]) - base_ts
        offsets.append(ott.timedelta(**diff.components._asdict()))

    return ott.datetime(base_ts), offsets


def _fallback_source(
    dataframe,
    timestamp_column=None,
    symbol_value=None,
    symbol_name_field=None,
    db=utils.adaptive_to_default,
    tick_type=utils.adaptive,
):
    rows_num = len(dataframe)
    if symbol_value:
        dataframe['SYMBOL_NAME'] = symbol_value

    if symbol_name_field:
        dataframe['SYMBOL_NAME'] = dataframe[symbol_name_field]

    data = dataframe.to_dict(orient='list')

    ticks_kwargs = {}
    if timestamp_column:
        if rows_num > 0:
            base_ts, offsets = _get_offsets(dataframe, timestamp_column)
            ticks_kwargs['offset'] = offsets
            ticks_kwargs['start'] = base_ts

        # remove original timestamp
        del data[timestamp_column]

    ts_column_mapping = []
    save_ts_column_list = [
        col for col in data.keys()
        if col and col.lower() == 'timestamp'
    ]
    if save_ts_column_list:
        # For some reason OneTick CSV_FILE_LISTING doesn't like timestamp columns with any case

        for idx, col in enumerate(save_ts_column_list):
            _mapping = (f'__TMP_TIMESTAMP_COLUMN_{idx}__', col)
            ts_column_mapping.append(_mapping)
            data[_mapping[0]] = data[_mapping[1]]
            del data[_mapping[1]]

    src = otp.Ticks(data=data, db=db, tick_type=tick_type, **ticks_kwargs)

    if symbol_name_field:
        src.drop(['SYMBOL_NAME'], inplace=True)

    if not timestamp_column:
        src['Time'] = otp.meta_fields.end_time

    if ts_column_mapping:
        for _mapping in ts_column_mapping:
            src.rename({_mapping[0]: _mapping[1]}, inplace=True)

    return src


def _autodetect_timestamp_column(dataframe: pd.DataFrame) -> Optional[str]:
    timestamp_columns = [col for col in dataframe.columns if col.lower() in ['time', 'timestamp']]
    if len(timestamp_columns) > 1:
        raise ValueError(
            'Could not determine timestamp column from multiple available choices: ' + ', '.join(timestamp_columns)
        )
    elif len(timestamp_columns) == 1:
        return timestamp_columns[0]

    return None


def ReadFromDataFrame(
    dataframe=None,
    timestamp_column=utils.adaptive,
    symbol_name_field=None,
    symbol=utils.adaptive,
    db=utils.adaptive_to_default,
    tick_type=utils.adaptive,
    start=utils.adaptive,
    end=utils.adaptive,
    force_compatibility_mode=False,
    **kwargs,
):
    """
    Load :pandas:`pandas.DataFrame` as data source

    Parameters
    ----------
    dataframe: :pandas:`pandas.DataFrame`
        Pandas DataFrame to load.
    timestamp_column: str, optional
        Column containing time info.

        If parameter not set and DataFrame has one of columns ``TIME`` or ``Timestamp`` (case-insensitive),
        it will be automatically used as ``timestamp_column``. To disable this, set ``timestamp_column=None``.

        Timestamp column dtype should be either datetime related or string.
    symbol_name_field: str, optional
        Column containing symbol name.
    symbol: str
        Symbol(s) from which data should be taken.

        If both `symbol_name_field` and `symbol` are omitted
        :py:attr:`otp.config.default_symbol<onetick.py.configuration.Config.default_symbol>` value will be used.
    db: str
        Custom database name for the node of the graph.
    tick_type: str
        Tick type.
        Default: ANY.
    start: :py:class:`otp.datetime <onetick.py.datetime>`
        Custom start time of the query.
    end: :py:class:`otp.datetime <onetick.py.datetime>`
        Custom end time of the query.
    force_compatibility_mode: bool
        Force use of old dataframe load method

    Examples
    --------

    Let's assume that we have the following pandas dataframe:

    >>> print(dataframe)  # doctest: +SKIP
                     Timestamp  SIDE  PRICE  SIZE
    0  2024-01-01 12:00:00.001   BUY  50.05   100
    1  2024-01-01 12:00:02.000  SELL  50.05   150
    2  2024-01-01 12:00:02.500   BUY  49.95   200
    3  2024-01-01 12:00:03.100  SELL  49.98    80
    4  2024-01-01 12:00:03.250   BUY  50.02   250

    Simple dataframe loading, timestamp column will be automatically detected and converted to datetime:

    >>> src = otp.ReadFromDataFrame(dataframe, symbol='AAPL')  # doctest: +SKIP
    >>> otp.run(src, date=otp.date(2024, 1, 1))  # doctest: +SKIP
                         Time  SIDE  PRICE  SIZE SYMBOL_NAME
    0 2024-01-01 12:00:00.001   BUY  50.05   100        AAPL
    1 2024-01-01 12:00:02.000  SELL  50.05   150        AAPL
    2 2024-01-01 12:00:02.500   BUY  49.95   200        AAPL
    3 2024-01-01 12:00:03.100  SELL  49.98    80        AAPL
    4 2024-01-01 12:00:03.250   BUY  50.02   250        AAPL

    Setting custom `timestamp_column`. For example, if we have ``DATA_TIMES`` column, instead of ``Timestamps``

    >>> src = otp.ReadFromDataFrame(dataframe, symbol='AAPL', timestamp_column='DATA_TIMES')  # doctest: +SKIP
    >>> otp.run(src, date=otp.date(2024, 1, 1))  # doctest: +SKIP
                         Time  SIDE  PRICE  SIZE SYMBOL_NAME
    0 2024-01-01 12:00:00.001   BUY  50.05   100        AAPL
    1 2024-01-01 12:00:02.000  SELL  50.05   150        AAPL
    2 2024-01-01 12:00:02.500   BUY  49.95   200        AAPL
    3 2024-01-01 12:00:03.100  SELL  49.98    80        AAPL
    4 2024-01-01 12:00:03.250   BUY  50.02   250        AAPL

    You can load data even without time data. ``Time`` column will be set as query end time.

    >>> src = otp.ReadFromDataFrame(dataframe, symbol='AAPL')  # doctest: +SKIP
    >>> otp.run(src, date=otp.date(2024, 1, 1))  # doctest: +SKIP
            Time  SIDE  PRICE  SIZE SYMBOL_NAME
    0 2024-01-02   BUY  50.05   100        AAPL
    1 2024-01-02  SELL  50.05   150        AAPL
    2 2024-01-02   BUY  49.95   200        AAPL
    3 2024-01-02  SELL  49.98    80        AAPL
    4 2024-01-02   BUY  50.02   250        AAPL

    Same effect will be if you don't set ``timestamp_column`` and disable automatic timestamp detection:

    >>> src = otp.ReadFromDataFrame(dataframe, symbol='AAPL', timestamp_column=None)  # doctest: +SKIP
    >>> otp.run(src, date=otp.date(2024, 1, 1))  # doctest: +SKIP
            Time                Timestamp  SIDE  PRICE  SIZE SYMBOL_NAME
    0 2024-01-02  2024-01-01 12:00:00.001   BUY  50.05   100        AAPL
    1 2024-01-02  2024-01-01 12:00:02.000  SELL  50.05   150        AAPL
    2 2024-01-02  2024-01-01 12:00:02.500   BUY  49.95   200        AAPL
    3 2024-01-02  2024-01-01 12:00:03.100  SELL  49.98    80        AAPL
    4 2024-01-02  2024-01-01 12:00:03.250   BUY  50.02   250        AAPL

    Setting ``symbol_name_field`` for setting symbol name from dataframe.
    In this example, let's say, that we have column ``SYMBOL`` with symbol names.

    >>> src = otp.ReadFromDataFrame(dataframe, symbol_name_field='SYMBOL')  # doctest: +SKIP
    >>> otp.run(src, date=otp.date(2024, 1, 1))  # doctest: +SKIP
                         Time  SIDE  PRICE  SIZE SYMBOL
    0 2024-01-01 12:00:00.001   BUY  50.05   100   AAPL
    1 2024-01-01 12:00:02.000  SELL  50.05   150   AAPL
    2 2024-01-01 12:00:02.500   BUY  49.95   200   AAPL
    3 2024-01-01 12:00:03.100  SELL  49.98    80   AAPL
    4 2024-01-01 12:00:03.250   BUY  50.02   250   AAPL
    """
    if dataframe is None:
        raise ValueError('DataFrame should be passed to `ReadFromDataFrame` constructor')

    if not isinstance(dataframe, pd.DataFrame):
        raise ValueError(f'`dataframe` parameter expected to be pandas DataFrame, got `{type(dataframe)}`')

    dataframe = dataframe.copy(deep=True)

    if ('TIMESTAMP' in dataframe.columns or 'Time' in dataframe.columns) and not (
        timestamp_column in ['TIMESTAMP', 'Time'] or timestamp_column is utils.adaptive
    ):
        # Can't set meta fields
        raise ValueError(
            'It\'s not allowed to both have `TIMESTAMP` or `Time` column in DataFrame '
            'and pass `timestamp_column` parameter with different column '
            'or disable timestamp column autodetection'
        )

    if timestamp_column is utils.adaptive:
        timestamp_column = _autodetect_timestamp_column(dataframe)

    if timestamp_column:
        if timestamp_column not in dataframe.columns:
            raise ValueError(f'Column `{timestamp_column}` passed as `timestamp_column` parameter not in dataframe')

        if ott.np2type(dataframe[timestamp_column].dtype) in [ott.nsectime, ott.msectime]:
            # convert back to string
            dataframe[timestamp_column] = (dataframe[timestamp_column].dt.strftime('%Y-%m-%d %H:%M:%S.%f') +
                                           dataframe[timestamp_column].dt.nanosecond.astype(str).str.zfill(3))

    columns = {}
    for column, dtype in dataframe.dtypes.to_dict().items():
        if timestamp_column == column:
            continue
        else:
            dtype = ott.np2type(dtype)

        columns[column] = dtype

    if not symbol_name_field and symbol is utils.adaptive:
        symbol = otp.config.get('default_symbol')

    symbol_value = None
    if symbol is not utils.adaptive:
        if symbol_name_field:
            raise ValueError('`symbol_name_field` parameter is passed while `symbol` parameter is defined')

        symbol_value = symbol

        # otq.ReadFromDataFrame adds this column in this case
        columns['SYMBOL_NAME'] = str
    elif symbol_name_field and symbol_name_field not in dataframe.columns:
        raise ValueError(f'Column `{symbol_name_field}` passed as `symbol_name_field` parameter not in dataframe')

    if hasattr(otq, 'ReadFromDataFrame') and not force_compatibility_mode:
        return _ReadFromDataFrameSource(
            dataframe=dataframe,
            timestamp_column=timestamp_column,
            symbol_name_field=symbol_name_field,
            symbol_value=symbol_value,
            symbol=symbol,
            db=db,
            tick_type=tick_type,
            start=start,
            end=end,
            schema=columns,
            **kwargs,
        )
    else:
        return _fallback_source(
            dataframe=dataframe,
            timestamp_column=timestamp_column,
            symbol_name_field=symbol_name_field,
            symbol_value=symbol_value,
        )
