import datetime as dt
import inspect
import sys
import warnings
import math

from typing import Optional, Union, Type, Sequence

import onetick.py as otp
from onetick.py.otq import otq
import pandas as pd

import onetick.py.core._source
import onetick.py.functions
import onetick.py.db._inspection
from onetick.py.core.column import _Column
from onetick.py.core.source import Source

from .. import types as ott
from .. import utils, configuration
from ..core.column_operations._methods.methods import is_arithmetical
from ..core.column_operations.base import _Operation
from ..compatibility import is_supported_bucket_units_for_tick_generator
from onetick.py.aggregations._base import get_bucket_interval_from_datepart

from ..aggregations._docs import _bucket_time_doc
from onetick.py.docs.utils import docstring

from .common import get_start_end_by_date, update_node_tick_type, AdaptiveTickType
from .empty import Empty


def get_case_expr_with_datetime_limited_by_end_time(dt_expr: str) -> str:
    return f'CASE({dt_expr} > _END_TIME, 1, _END_TIME, {dt_expr})'


class Tick(Source):

    @docstring(parameters=[_bucket_time_doc], add_self=True)
    def __init__(
        self,
        data: Optional[dict] = None,
        offset=0,
        offset_part='millisecond',
        time: Optional[ott.datetime] = None,
        timezone_for_time=None,
        symbol=utils.adaptive_to_default,
        db=utils.adaptive_to_default,
        start=utils.adaptive,
        end=utils.adaptive,
        date=None,
        tick_type: Optional[AdaptiveTickType] = utils.adaptive,
        bucket_time: str = "start",
        bucket_interval: int = 0,
        bucket_units: Union[str, Type[utils.adaptive]] = utils.adaptive,
        num_ticks_per_timestamp: int = 1,
        **kwargs,
    ):
        """
        Generates a single tick for each bucket.
        By default a single tick for the whole query time interval is generated.

        Parameters
        ----------
        data: dict
            dictionary of columns names with their values.
            If specified, then parameter ``kwargs`` can't be used.
        offset: int, :ref:`datetime offset <datetime_offsets>`,\
                :py:class:`otp.timedelta <onetick.py.timedelta>`, default=0
            tick timestamp offset from query start time in `offset_part`
        offset_part: one of [nanosecond, millisecond, second, minute, hour, day, dayofyear, weekday, week, month, quarter, year], default=millisecond   #noqa
            unit of time to calculate ``offset`` from.
            Could be omitted if :ref:`datetime offset <datetime_offsets>` or
            :py:class:`otp.timedelta <onetick.py.timedelta>` objects are set as ``offset``.
        time: :py:class:`otp.datetime <onetick.py.datetime>`
            fixed time to set to all ticks.
            Note that this time should be inside time interval set by ``start`` and ``end`` parameters
            or by query time range.
        timezone_for_time: str
            timezone of the ``time``
        symbol: str, list of str, :class:`Source`, :class:`query`, :py:func:`eval query <onetick.py.eval>`
            Symbol(s) from which data should be taken.
        db: str
            Database to use for tick generation
        start: :py:class:`otp.datetime <onetick.py.datetime>`
            start time for tick generation. By default the start time of the query will be used.
        end: :py:class:`otp.datetime <onetick.py.datetime>`
            end time for tick generation. By default the end time of the query will be used.
        date: :py:class:`otp.datetime <inetick.py.datetime>` – allows to specify a whole day
            instead of passing explicitly start and end parameters. If it is set along with
            the start and end parameters then last two are ignored.
        tick_type: str
            By default, the tick type value is not significant, and a placeholder string constant will be utilized.
            If you prefer to use the sink node's tick type instead of specifying your own,
            you can set the value to None.
        bucket_interval: int or :ref:`datetime offset objects <datetime_offsets>`
            Determines the length of each bucket (units depends on ``bucket_units``)
            for which the tick will be generated.

            Bucket interval can also be set via :ref:`datetime offset objects <datetime_offsets>`
            like :py:func:`otp.Second <onetick.py.Second>`, :py:func:`otp.Minute <onetick.py.Minute>`,
            :py:func:`otp.Hour <onetick.py.Hour>`, :py:func:`otp.Day <onetick.py.Day>`,
            :py:func:`otp.Month <onetick.py.Month>`.
            In this case you could omit setting ``bucket_units`` parameter.
        bucket_units: 'seconds', 'days' or 'months'
            Unit for value in ``bucket_interval``.
            Default is 'seconds'.
        num_ticks_per_timestamp: int
            The number of ticks to generate for every value of timestamp.
        kwargs:
            dictionary of columns names with their values.
            If specified, then parameter ``data`` can't be used.

        See also
        --------
        | **TICK_GENERATOR** OneTick event processor
        | :py:class:`otp.Ticks <onetick.py.Ticks>`

        Examples
        --------

        Simple usage, generate single tick:

        >>> t = otp.Tick(A=1, B='string', C=3.14, D=otp.dt(2000, 1, 1, 1, 1, 1, 1))
        >>> otp.run(t)
                Time  A       B     C                          D
        0 2003-12-01  1  string  3.14 2000-01-01 01:01:01.000001

        Generate single tick with offset:

        >>> t = otp.Tick(A=1, offset=otp.Minute(10))
        >>> otp.run(t)
                Time           A
        0 2003-12-01 00:10:00  1

        Generate one tick for each day in a week:

        >>> t = otp.Tick(A=1, start=otp.dt(2023, 1, 1), end=otp.dt(2023, 1, 8), bucket_interval=24 * 60 * 60)
        >>> otp.run(t)
                Time  A
        0 2023-01-01  1
        1 2023-01-02  1
        2 2023-01-03  1
        3 2023-01-04  1
        4 2023-01-05  1
        5 2023-01-06  1
        6 2023-01-07  1

        Generate tick every hour and add 1 minute offset to ticks' timestamps:

        >>> t = otp.Tick(A=1, offset=1, offset_part='minute', bucket_interval=60 * 60)
        >>> t.head(5)
                          Time  A
        0  2003-12-01 00:01:00  1
        1  2003-12-01 01:01:00  1
        2  2003-12-01 02:01:00  1
        3  2003-12-01 03:01:00  1
        4  2003-12-01 04:01:00  1

        Generate tick every hour and set fixed time:

        >>> t = otp.Tick(A=1, time=otp.dt(2023, 1, 2, 3, 4, 5, 6), bucket_interval=60 * 60,
        ...              start=otp.dt(2023, 1, 1), end=otp.dt(2023, 1, 8))
        >>> t.head(5)
                                Time  A
        0 2023-01-02 03:04:05.000006  1
        1 2023-01-02 03:04:05.000006  1
        2 2023-01-02 03:04:05.000006  1
        3 2023-01-02 03:04:05.000006  1
        4 2023-01-02 03:04:05.000006  1

        Use :ref:`datetime offset object <datetime_offsets>` as a ``bucket_interval``:

        .. testcode::
           :skipif: not is_supported_bucket_units_for_tick_generator()

           t = otp.Tick(A=1, bucket_interval=otp.Day(1))
           df = otp.run(t, start=otp.dt(2023, 1, 1), end=otp.dt(2023, 1, 5))
           print(df)

        .. testoutput::

                   Time  A
           0 2023-01-01  1
           1 2023-01-02  1
           2 2023-01-03  1
           3 2023-01-04  1
        """

        if self._try_default_constructor(**kwargs):
            return

        if data and kwargs:
            raise ValueError("Parameters 'data' and **kwargs can't be used at the same time")

        if data:
            kwargs = data

        if len(kwargs) == 0:
            raise ValueError("It is not allowed to have a tick without fields")

        if isinstance(offset, ott.OTPBaseTimeOffset):
            offset, offset_part = offset.get_offset()

            if offset < 0:
                raise ValueError("Negative offset not allowed")

            if offset_part not in {
                "nanosecond", "millisecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year"
            }:
                raise ValueError(f"Unsupported DatePart passed to offset: {offset_part}")
        elif isinstance(offset, ott.timedelta):
            offset, offset_part = offset._get_offset()

        if time is not None and offset != 0:
            raise ValueError("It's not allowed to set parameter 'time' and set non-zero offset at the same time")

        bucket_time = self._get_bucket_time(bucket_time)
        if bucket_time == "BUCKET_END" and offset != 0:
            raise ValueError(
                "It's not allowed to set parameter 'bucket_time' to 'end' and set non-zero offset at the same time"
            )

        if isinstance(bucket_interval, ott.OTPBaseTimeOffset):
            bucket_interval, bucket_units = get_bucket_interval_from_datepart(bucket_interval)

        if date:
            # TODO: write a warning in that case
            start, end = get_start_end_by_date(date)

        columns = {}
        for key, value in kwargs.items():
            # the way to skip a field
            if value is None:
                continue

            if inspect.isclass(value):
                raise TypeError(f"Tick constructor expects values but not types, {value}")
            else:
                value_type = ott.get_object_type(value)

            if value_type is str:
                if isinstance(value, _Operation) or is_arithmetical(value):
                    if value.dtype is not str:
                        value_type = value.dtype
                elif len(value) > ott.string.DEFAULT_LENGTH:
                    value_type = ott.string[len(value)]

            if value_type is bool:
                value_type = float

            if issubclass(value_type, (ott.datetime, ott.date, dt.datetime, dt.date, pd.Timestamp)):
                value_type = ott.nsectime

            columns[key] = value_type

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=lambda: self.base_ep(db=db,
                                               tick_type=tick_type,
                                               offset=offset,
                                               offset_part=offset_part,
                                               time=time,
                                               timezone_for_time=timezone_for_time,
                                               columns=columns,
                                               bucket_time=bucket_time,
                                               bucket_interval=bucket_interval,
                                               bucket_units=bucket_units,
                                               num_ticks_per_timestamp=num_ticks_per_timestamp,
                                               **kwargs),
            schema=columns,
        )

    def base_ep(self,
                db=utils.adaptive_to_default,
                tick_type=utils.adaptive,
                offset=0,
                offset_part='millisecond',
                time=None,
                timezone_for_time=None,
                columns=None,
                bucket_time="start",
                bucket_interval=0,
                bucket_units=utils.adaptive,
                num_ticks_per_timestamp=1,
                **kwargs):
        if columns is None:
            columns = {}

        params = ",".join(
            ott.type2str(columns[key]) + " " + str(key) + "=" + ott.value2str(value)
            for key, value in kwargs.items()
            if value is not None
        )

        tick_generator_kwargs = {}
        if bucket_units is not utils.adaptive:
            if is_supported_bucket_units_for_tick_generator(throw_warning=True):
                tick_generator_kwargs['bucket_interval_units'] = bucket_units.upper()
            elif bucket_units != 'seconds':
                raise ValueError("Parameter 'bucket_units' in otp.Tick is not supported on this OneTick version")

        src = Source(
            otq.TickGenerator(
                bucket_interval=bucket_interval,
                bucket_time=bucket_time,
                fields=params,
                num_ticks_per_timestamp=num_ticks_per_timestamp,
                **tick_generator_kwargs,
            ),
            schema=columns,
        )

        update_node_tick_type(src, tick_type, db)

        # TIMESTAMP += offset will add redundant nodes to sort the timestamps.
        # No sorting needed for a single tick.

        if offset:
            src.sink(
                otq.UpdateField(
                    field="TIMESTAMP",
                    value=get_case_expr_with_datetime_limited_by_end_time(
                        f"dateadd('{offset_part}', {offset}, TIMESTAMP, _TIMEZONE)"
                    )
                )
            )
        elif time:
            src.sink(otq.UpdateField(field="TIMESTAMP",
                                     value=ott.datetime2expr(time, timezone_naive=timezone_for_time)))
        return src

    @staticmethod
    def _get_bucket_time(bucket_time):
        if bucket_time == "BUCKET_START":
            warnings.warn("BUCKET_START value is deprecated. Please, use 'start' instead", FutureWarning)
        elif bucket_time == "BUCKET_END":
            warnings.warn("BUCKET_END value is deprecated. Please, use 'end' instead", FutureWarning)
        elif bucket_time == "start":
            bucket_time = "BUCKET_START"
        elif bucket_time == "end":
            bucket_time = "BUCKET_END"
        else:
            raise ValueError(f"Only 'start' and 'end' values supported as bucket time, but you've passed {bucket_time}")
        return bucket_time


def Ticks(data=None,  # NOSONAR
          symbol=utils.adaptive_to_default,
          db=utils.adaptive_to_default,
          start=utils.adaptive,
          end=utils.adaptive,
          tick_type: Optional[AdaptiveTickType] = utils.adaptive,
          timezone_for_time=None,
          offset=utils.adaptive,
          **inplace_data):
    """
    Data source that generates ticks.

    By default ticks are placed with the 1 millisecond offset from
    each other starting from the start of the query interval.

    The offset for each tick can be changed using the
    special reserved field name ``offset``, that specifies the time offset from the query start time.
    ``offset`` can be an integer, :ref:`datetime offset <datetime_offsets>` object
    or :py:class:`otp.timedelta <onetick.py.timedelta>`.

    Parameters
    ----------
    data: dict, list or :pandas:`pandas.DataFrame`, optional
        Ticks values

        * ``dict`` -- <field_name>: <values>

        * ``list`` -- [[<field_names>], [<first_tick_values>], ..., [<n_tick_values>]]

        * :pandas:`DataFrame <pandas.DataFrame>`

        .. deprecated:: 1.178.0

        * ``None`` -- ``inplace_data`` will be used

    symbol: str, list of str, :class:`Source`, :class:`query`, :py:func:`eval query <onetick.py.eval>`
        Symbol(s) from which data should be taken.
    db: str
        Database to use for tick generation
    start, end: :py:class:`datetime.datetime`, :py:class:`otp.datetime <onetick.py.datetime>`, \
                    :py:class:`onetick.py.adaptive`
        Timestamp for data generation
    tick_type: str
        tick type for data generation
    timezone_for_time: str
        timezone for data generation
    offset: int, :ref:`datetime offset <datetime_offsets>` or :py:class:`otp.timedelta <onetick.py.timedelta>` \
            or list of such values or None
        Specifies the time offset for each tick from the query start time.
        Should be specified as the list of values, one for each tick,
        or as a single value that will be the same for all ticks.
        Special value None will disable changing timestamps for each tick,
        so all timestamps will be set to the query start time.
        Can't be used at the same time with the column `offset`.
    **inplace_data: list
        <field_name>: list(<field_values>)

    See also
    --------
    | **TICK_GENERATOR** OneTick event processor
    | **CSV_FILE_LISTING** OneTick event processor
    | :py:class:`otp.Tick <onetick.py.Tick>`

    Examples
    --------

    Pass the data as a dictionary:

    >>> d = otp.Ticks({'A': [1, 2, 3], 'B': [4, 5, 6]})
    >>> otp.run(d)
                         Time  A  B
    0 2003-12-01 00:00:00.000  1  4
    1 2003-12-01 00:00:00.001  2  5
    2 2003-12-01 00:00:00.002  3  6

    Pass the data using key-value arguments:

    >>> d = otp.Ticks(A=[1, 2, 3], B=[4, 5, 6])
    >>> otp.run(d)
                         Time  A  B
    0 2003-12-01 00:00:00.000  1  4
    1 2003-12-01 00:00:00.001  2  5
    2 2003-12-01 00:00:00.002  3  6

    Pass the data using list:

    >>> d = otp.Ticks([['A', 'B'],
    ...                [1, 4],
    ...                [2, 5],
    ...                [3, 6]])
    >>> otp.run(d)
                         Time  A  B
    0 2003-12-01 00:00:00.000  1  4
    1 2003-12-01 00:00:00.001  2  5
    2 2003-12-01 00:00:00.002  3  6

    Pass the data using :pandas:`pandas.DataFrame`.
    DataFrame should have a ``Time`` column containing datetime objects.

    >>> start_datetime = datetime.datetime(2023, 1, 1, 12)
    >>> time_array = [start_datetime + otp.Hour(1) + otp.Nano(1)]
    >>> a_array = [start_datetime - otp.Day(15) - otp.Nano(7)]
    >>> df = pd.DataFrame({'Time': time_array,'A': a_array})
    >>> data = otp.Ticks(df)  # doctest: +SKIP
    >>> otp.run(data, start=start_datetime, end=start_datetime + otp.Day(1))  # doctest: +SKIP
                               Time                             A
    0 2023-01-01 13:00:00.000000001 2022-12-17 11:59:59.999999993

    Example with setting ``offset`` for each tick:

    >>> data = otp.Ticks(X=[1, 2, 3], offset=[0, otp.Nano(1), 1])
    >>> otp.run(data)
                               Time  X
    0 2003-12-01 00:00:00.000000000  1
    1 2003-12-01 00:00:00.000000001  2
    2 2003-12-01 00:00:00.001000000  3

    Remove the ``offset`` for all ticks, in this case the timestamp of each tick is set to the start time of the query:

    >>> data = otp.Ticks(X=[1, 2, 3], offset=None)
    >>> otp.run(data)
            Time  X
    0 2003-12-01  1
    1 2003-12-01  2
    2 2003-12-01  3

    Parameter ``offset`` allows to set the same value for all ticks:

    >>> data = otp.Ticks(X=[1, 2, 3], offset=otp.Nano(13))
    >>> otp.run(data)
                               Time  X
    0 2003-12-01 00:00:00.000000013  1
    1 2003-12-01 00:00:00.000000013  2
    2 2003-12-01 00:00:00.000000013  3
    """

    if db is utils.adaptive_to_default:
        db = configuration.config.get('default_db')

    if isinstance(data, pd.DataFrame):
        warnings.warn(
            "Using pandas DataFrame as `data` parameter is deprecated, "
            "use `otp.ReadFromDataFrame` source instead.",
            FutureWarning,
        )

        if offset is not utils.adaptive:
            raise ValueError("Parameter 'offset' can't be set when passing pandas.DataFrame.")
        if data.empty:
            warnings.warn('otp.Ticks got empty DataFrame as input, returning otp.Empty', stacklevel=2)
            return Empty(db=db, symbol=symbol, tick_type=tick_type, start=start, end=end)
        if 'Time' not in data.columns:
            raise ValueError('Field `Time` is required for constructing an `otp.Source` from `pandas.DataFrame`')
        data = data.rename(columns={"Time": "time"})
        # to_dict('list') doesn't work correctly with pandas timestamps on some versions
        data = data.to_dict('series')
        data = {column_name: series.to_list() for column_name, series in data.items()}

    if data and len(inplace_data) != 0:
        raise ValueError("Data can be passed only using either the `data` parameter "
                         "or inplace through the key-value args")

    if isinstance(data, list):
        reform = {}
        for inx, key in enumerate(data[0]):
            reform[key] = [sub_list[inx] for sub_list in data[1:]]

        data = reform

    if data is None:
        if inplace_data:
            data = inplace_data
        else:
            raise ValueError("You don't specify any date to create ticks from. "
                             "Please, use otp.Empty for creating empty data source")
    else:
        data = data.copy()

    def check_value_len(_data):
        # check all columns to have the same number of rows
        value_len = None
        for key, value in _data.items():
            if value_len is None:
                value_len = len(value)
            elif value_len != len(value):
                raise ValueError(
                    f"It is not allowed to have different columns of different lengths, "
                    f"some of columns have {value_len} length, but column '{key}', as instance, has {len(value)}"
                )
        return value_len

    value_len = check_value_len(data)

    use_absolute_time = False

    if 'offset' in data:
        if offset is not utils.adaptive:
            raise ValueError("Parameter 'offset' and column 'offset' can't be set at the same time.")
        else:
            offset = data['offset']

    if offset is not utils.adaptive:
        if "time" in data:
            raise ValueError("You cannot specify 'offset' column/parameter and 'time' column at the same time.")
    elif "time" in data:
        use_absolute_time = True
    else:
        # by default the difference between each offset is 1 (1 millisecond)
        offset = list(range(value_len))

    disable_offsets = False
    if offset is None:
        disable_offsets = True
    elif offset is not utils.adaptive and not isinstance(offset, Sequence):
        # if offset is set as a single value then just copy-paste it for all rows
        offset = [offset] * value_len

    if offset is not utils.adaptive and offset is not None:
        data['offset'] = offset
        check_value_len(data)

    if not use_absolute_time and not disable_offsets:
        offset_values = []
        offset_parts = []
        for ofv in data['offset']:
            if isinstance(ofv, ott.offsets.Tick):
                offset_values.append(ofv.n)
                try:
                    str_repr = str(ofv.datepart)[1:-1]
                except Exception:
                    str_repr = str(ofv.base)[1:-1].lower()
                offset_parts.append(str_repr)
            elif isinstance(ofv, ott.timedelta):
                value, str_repr = ofv._get_offset()
                offset_values.append(value)
                offset_parts.append(str_repr)
            else:
                offset_values.append(ofv)
                offset_parts.append('millisecond')
        data['offset'] = offset_values
        data['offset_part'] = offset_parts

    def split_data_for_tick(columns):
        tick_parameters = {}
        tick_columns = {}
        for key, value in columns.items():
            if key in {'offset', 'offset_part', 'time'}:
                tick_parameters[key] = value
            else:
                tick_columns[key] = value
        return tick_columns, tick_parameters

    if value_len == 1:
        columns = {key: value[0] for key, value in data.items()}
        tick_columns, tick_parameters = split_data_for_tick(columns)
        return Tick(tick_columns, db=db, symbol=symbol, tick_type=tick_type, start=start, end=end,
                    timezone_for_time=timezone_for_time, **tick_parameters)
    else:
        # select only columns that do not contain None there to support
        # heterogeneous data
        not_none_columns = []
        for key in data.keys():
            data[key] = [float(elem) if isinstance(elem, bool) else elem for elem in data[key]]
        for key, value in data.items():
            add = True
            for v in value:
                # we need it, because can't use _Column instances in if-clauses
                if isinstance(v, _Column):
                    continue
                if v is None:
                    add = False
                    break

            if add:
                not_none_columns.append(key)

        # if a field is a onetick operation, it cannot be csv'd (it's dynamic)
        is_outside_data_dependent = False
        for key, value in data.items():
            for v in value:
                if isinstance(v, _Operation):
                    is_outside_data_dependent = True
                    break

        # infinity() and (on windows) nan() cannot be natively read from a csv
        has_special_values = False
        for key, value in data.items():
            for v in value:
                if isinstance(v, ott._inf) or \
                    (isinstance(v, ott._nan) or isinstance(v, float) and math.isnan(v)) \
                        and sys.platform.startswith("win"):
                    has_special_values = True
                    break

        if (len(not_none_columns) == len(data)) and (not is_outside_data_dependent) and (not has_special_values):
            # Data is homogenous; CSV backing can be used
            return _DataCSV(data, value_len, db=db, symbol=symbol, tick_type=tick_type, start=start, end=end,
                            timezone_for_time=timezone_for_time, use_absolute_time=use_absolute_time,
                            disable_offsets=disable_offsets)
        else:
            # Fallback is a merge of individual ticks
            ticks = []

            for inx in range(value_len):
                columns = {key: value[inx] for key, value in data.items()}
                tick_columns, tick_parameters = split_data_for_tick(columns)
                ticks.append(Tick(tick_columns, db=db, symbol=symbol, tick_type=tick_type, start=start, end=end,
                                  timezone_for_time=timezone_for_time, **tick_parameters))

            return onetick.py.functions.merge(ticks, align_schema=not_none_columns)


class _DataCSV(Source):
    def __init__(
        self,
        data=None,
        length=None,
        db=utils.adaptive_to_default,
        symbol=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        use_absolute_time=False,
        timezone_for_time=None,
        disable_offsets=False,
        **kwargs,
    ):
        if self._try_default_constructor(**kwargs):
            return

        if data is None or length is None:
            raise ValueError("'data' and 'length' parameters can't be None")

        def datetime_to_expr(v):
            if ott.is_time_type(v):
                return ott.datetime2expr(v, timezone_naive=timezone_for_time)
            if isinstance(v, ott.nsectime):
                # TODO: change to ott.value2str after PY-441
                return f'NSECTIME({v})'
            if isinstance(v, ott.msectime):
                return ott.value2str(v)
            raise ValueError(f"Can't convert value {v} to datetime expression")

        if use_absolute_time:
            # converting values of "time" column to onetick expressions
            converted_times = []
            for d in data["time"]:
                converted_times.append(datetime_to_expr(d))
            data["time"] = converted_times

        def csv_rep(value):
            if issubclass(type(value), str):
                return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
            else:
                return str(value)

        def get_type_of_column(key):
            def get_type_of_value(value):
                t = ott.get_object_type(value)

                if ott.is_time_type(t):
                    return ott.nsectime
                elif t is str:
                    if len(value) <= ott.string.DEFAULT_LENGTH:
                        return str
                    else:
                        return ott.string[len(value)]
                else:
                    return t

            types = [get_type_of_value(v) for v in data[key]]
            res, _ = utils.get_type_that_includes(types)
            return res

        columns = {key: get_type_of_column(key) for key in data}

        expression_columns = []
        header_columns = {}
        for key in list(columns):
            header_columns[key] = columns[key]
            # converting values of datetime columns to onetick expressions
            if columns[key] is ott.nsectime:
                data[key] = [datetime_to_expr(v) for v in data[key]]
                header_columns[key] = get_type_of_column(key)
                expression_columns.append(key)

        transposed_data = [[csv_rep(value[i]) for key, value in data.items()] for i in range(length)]

        text_header = ",".join(f"{ott.type2str(v)} {k}" for k, v in header_columns.items())
        text_data = "\n".join([",".join(data_row) for data_row in transposed_data])

        if use_absolute_time:
            del columns["time"]
        elif not disable_offsets:
            del columns["offset"]
            del columns["offset_part"]

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=lambda: self.base_ep(columns=columns,
                                               db=db,
                                               tick_type=tick_type,
                                               use_absolute_time=use_absolute_time,
                                               text_header=text_header,
                                               text_data=text_data,
                                               expression_columns=expression_columns,
                                               disable_offsets=disable_offsets),
            schema=columns,
        )

    def base_ep(self, columns, db, tick_type, use_absolute_time, text_header, text_data, expression_columns=None,
                disable_offsets=False):

        node = Source(
            otq.CsvFileListing(
                discard_timestamp_column=True,
                time_assignment="_START_TIME",
                field_delimiters="','",
                quote_chars='"""',
                handle_escaped_chars=True,
                file_contents=text_data,
                first_line_is_title=False,
                fields=text_header,
            ),
            schema=columns,
        )

        update_node_tick_type(node, tick_type, db)

        if use_absolute_time:
            # don't trust UpdateField
            node.sink(otq.AddField(field='____TMP____', value="EVAL_EXPRESSION(time, 'datetime')"))
            node.sink(otq.UpdateField(field="TIMESTAMP", value="____TMP____"))
            node.sink(otq.Passthrough(fields="time,____TMP____", drop_fields="True"))
            node.sink(otq.OrderBy(order_by="TIMESTAMP ASC"))
        elif not disable_offsets:
            node.sink(
                otq.AddField(
                    field='nsectime ____TMP____',
                    value=get_case_expr_with_datetime_limited_by_end_time(
                        "dateadd(offset_part, offset, TIMESTAMP, _TIMEZONE)"
                    )
                )
            )
            node.sink(otq.OrderBy(order_by="____TMP____ ASC"))
            node.sink(otq.UpdateField(field="TIMESTAMP", value="____TMP____"))
            node.sink(otq.Passthrough(fields="offset,offset_part,____TMP____", drop_fields="True"))
            node.sink(otq.OrderBy(order_by="TIMESTAMP ASC"))

        for column in expression_columns or []:
            # don't trust UpdateField
            node.sink(otq.RenameFields(f'{column}=____TMP____'))
            node.sink(otq.AddField(field=column, value="EVAL_EXPRESSION(____TMP____, 'datetime')"))
            node.sink(otq.Passthrough(fields='____TMP____', drop_fields=True))
        node.sink(otq.Table(keep_input_fields=True,
                            fields=', '.join(f'nsectime {column}' for column in expression_columns)))

        return node


def TTicks(data):  # NOSONAR
    """
    .. deprecated:: 1.3.101

    Transposed Ticks format.

    Parameters
    ----------
    data: list
        list of list, where the first sublist is the header, and other are values
    """

    warnings.warn("The nice and helpful function `TTicks` is going to be deprecated. "
                  "You could use the `Ticks` to pass data in the same format there",
                  FutureWarning)

    data_dict = {}

    for inx, key in enumerate(data[0]):
        data_dict[key] = [sub_list[inx] for sub_list in data[1:]]

    return Ticks(data_dict)
