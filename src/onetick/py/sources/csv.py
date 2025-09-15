import datetime as dt
import os
import io
import string

from functools import partial
from typing import Optional, Union, Dict

import onetick.py as otp
from onetick.py.otq import otq
import pandas as pd

from onetick.py.core._source._symbol_param import _SymbolParamSource
from onetick.py.core.source import Source

from .. import types as ott
from .. import utils, configuration
from ..core import _csv_inspector

from .common import default_date_converter, to_timestamp_nanos, update_node_tick_type
from .ticks import Ticks


def CSV(  # NOSONAR
    filepath_or_buffer=None,
    timestamp_name: Optional[str] = "Time",
    first_line_is_title: bool = True,
    names: Optional[list] = None,
    dtype: Optional[dict] = None,
    converters: Optional[dict] = None,
    order_ticks=False,
    drop_index=True,
    change_date_to=None,
    auto_increase_timestamps=True,
    db='LOCAL',
    field_delimiter=',',
    handle_escaped_chars=False,
    quote_char='"',
    timestamp_format: Optional[Union[str, Dict[str, str]]] = None,
    file_contents: Optional[str] = None,
    **kwargs,
):
    """
    Construct source based on CSV file.

    There are several steps determining column types.

    1. Initially, all column treated as ``str``.
    2. If column name in CSV title have format ``type COLUMNNAME``,
       it will change type from ``str`` to specified type.
    3. All column type are determined automatically from its data.
    4. You could override determined types in ``dtype`` argument explicitly.
    5. ``converters`` argument is applied after ``dtype`` and could also change column type.

    NOTE: Double quotes are not supported in CSV files for escaping quotes in strings,
    you should use escape character ``\\`` before the quote instead,
    for example: ``"I'm a string with a \\"quotes\\" inside"``. And then set `handle_escaped_chars=True`.

    Parameters
    ----------
    filepath_or_buffer: str, os.PathLike, FileBuffer, optional
        Path to CSV file or :class:`file buffer <FileBuffer>`. If None value is taken through symbol.
        When taken from symbol, symbol must have ``LOCAL::`` prefix.
        In that case you should set the columns otherwise schema will be empty.
    timestamp_name: str, default "Time"
        Name of TIMESTAMP column used for ticks. Used only if it is exists in CSV columns, otherwise ignored.
        Output data will be sorted by this column.
    first_line_is_title: bool
        Use first line of CSV file as a source for column names and types.
        If CSV file is started with # symbol, this parameter **must** be ``True``.

        - If ``True``, column names are inferred from the first line of the file,
          it is not allowed to have empty name for any column.

        - If ``False``, first line is processed as data, column names will be COLUMN_1, ..., COLUMN_N.
          You could specify column names in ``names`` argument.

    names: list, optional
        List of column names to use, or None.
        Length must be equal to columns number in file.
        Duplicates in this list are not allowed.
    dtype: dict, optional
        Data type for columns, as dict of pairs {column_name: type}.
        Will convert column type from ``str`` to specified type, before applying converters.
    converters: dict, optional
        Dict of functions for converting values in certain columns. Keys are column names.
        Function must be valid callable with ``onetick.py`` syntax, example::

            converters={
                "time_number": lambda c: c.apply(otp.nsectime),
                "stock": lambda c: c.str.lower(),
            }

        Converters applied *after* ``dtype`` conversion.
    order_ticks: bool, optional
        If ``True`` and ``timestamp_name`` column are used, then source will order tick by time.
        Note, that if ``False`` and ticks are not ordered in sequence, then OneTick will raise Exception in runtime.
    drop_index: bool, optional
        if ``True`` and 'Index' column is in the csv file then this column will be removed.
    change_date_to: datetime, date, optional
        change date from a timestamp column to a specific date. Default is None, means not changing timestamp column.
    auto_increase_timestamps: bool, optional
        Only used if provided CSV file does not have a TIMESTAMP column. If ``True``, timestamps of loaded ticks
        would start at ``start_time`` and on each next tick, would increase by 1 millisecond.
        If ``False``, timestamps of all loaded ticks would be equal to ``start_time``
    db: str, optional
        Name of a database to define a destination where the csv file will be transported for processing.
        ``LOCAL`` is default value that means OneTick will process it on the site where a query runs.
    field_delimiter: str, optional
        A character that is used to tokenize each line of the CSV file.
        For a tab character \t (back-slash followed by t) should be specified.
    handle_escaped_chars: bool, optional
        If set, the backslash char ``\\`` gets a special meaning and everywhere in the input text
        the combinations ``\\'``, ``\\"`` and ``\\\\`` are changed correspondingly by ``'``, ``"`` and ``\\``,
        which are processed then as regular chars.
        Besides, combinations like ``\\x??``, where ?-s are hexadecimal digits (0-9, a-f or A-F),
        are changed by the chars with the specified ASCII code.
        For example, ``\\x0A`` will be replaced by a newline character, ``\\x09`` will be replaced by tab, and so on.
        Default: False
    quote_char: str
        Character used to denote the start and end of a quoted item. Quoted items can include the delimiter,
        and it will be ignored. The same character cannot be marked both as the quote character and as the
        field delimiter. Besides, space characters cannot be used as quote.
        Default: " (double quotes)
    timestamp_format: str or dict
        Expected format for ``timestamp_name`` and all other datetime columns.
        If dictionary is passed, then different format can be specified for each column.
        This format is expected when converting strings from csv file to ``dtype``.
        Default format is ``%Y/%m/%d %H:%M:%S.%J`` for :py:class:`~onetick.py.nsectime` columns and
        ``%Y/%m/%d %H:%M:%S.%q`` for :py:class:`~onetick.py.msectime` columns.
    file_contents: str
        Specify the contents of the csv file as string.
        Can be used instead of ``filepath_or_buffer`` parameter.

    See also
    --------
    **CSV_FILE_LISTING** OneTick event processor

    Examples
    --------
    Simple CSV file reading

    >>> data = otp.CSV(os.path.join(csv_path, "data.csv"))
    >>> otp.run(data)
                         Time          time_number      px side
    0 2003-12-01 00:00:00.000  1656690986953602371   30.89  Buy
    1 2003-12-01 00:00:00.001  1656667706281508365  682.88  Buy

    Read CSV file and get timestamp for ticks from specific field.
    You need to specify query start/end interval including all ticks.

    >>> data = otp.CSV(os.path.join(csv_path, "data.csv"),
    ...                timestamp_name="time_number",
    ...                converters={"time_number": lambda c: c.apply(otp.nsectime)},
    ...                start=otp.dt(2010, 8, 1),
    ...                end=otp.dt(2022, 9, 2))
    >>> otp.run(data)
                               Time      px side
    0 2022-07-01 05:28:26.281508365  682.88  Buy
    1 2022-07-01 11:56:26.953602371   30.89  Buy

    Path to csv can be passed via symbol with `LOCAL::` prefix:

    >>> data = otp.CSV()
    >>> otp.run(data, symbols=f"LOCAL::{os.path.join(csv_path, 'data.csv')}")
                         Time          time_number      px side
    0 2003-12-01 00:00:00.000  1656690986953602371   30.89  Buy
    1 2003-12-01 00:00:00.001  1656667706281508365  682.88  Buy

    Field delimiters can be set via ``field_delimiters`` parameter:

    >>> data = otp.CSV(os.path.join(csv_path, 'data_diff_delimiters.csv'),
    ...                field_delimiter=' ',
    ...                first_line_is_title=False)
    >>> otp.run(data)
                         Time COLUMN_0 COLUMN_1
    0 2003-12-01 00:00:00.000      1,2        3
    1 2003-12-01 00:00:00.001        4      5,6

    Quote char can be set via ``quote_char`` parameter:

    >>> data = otp.CSV(os.path.join(csv_path, 'data_diff_quote_chars.csv'),
    ...                quote_char="'",
    ...                first_line_is_title=False)
    >>> otp.run(data)
                         Time COLUMN_0 COLUMN_1
    0 2003-12-01 00:00:00.000     1,"2       3"
    1 2003-12-01 00:00:00.001       "1     2",3

    Use parameter ``file_contents`` to read the data from string:

    >>> data = otp.CSV(file_contents=os.linesep.join([
    ...     'A,B,C',
    ...     '1,f,3.3',
    ...     '2,g,4.4',
    ... ]))
    >>> otp.run(data)
                         Time  A  B    C
    0 2003-12-01 00:00:00.000  1  f  3.3
    1 2003-12-01 00:00:00.001  2  g  4.4
    """
    csv_source = _CSV(
        filepath_or_buffer=filepath_or_buffer,
        timestamp_name=timestamp_name,
        first_line_is_title=first_line_is_title,
        names=names,
        dtype=dtype,
        converters=converters,
        order_ticks=order_ticks,
        drop_index=drop_index,
        change_date_to=change_date_to,
        auto_increase_timestamps=auto_increase_timestamps,
        db=db,
        field_delimiter=field_delimiter,
        handle_escaped_chars=handle_escaped_chars,
        quote_char=quote_char,
        timestamp_format=timestamp_format,
        file_contents=file_contents,
        **kwargs,
    )
    csv_source = csv_source.sort(csv_source['Time'])
    return otp.merge([csv_source, otp.Empty(db=db)])


class _CSV(Source):
    _PROPERTIES = Source._PROPERTIES + [
        "_dtype",
        "_names",
        "_columns",
        "_columns_with_bool_replaced",
        "_forced_title",
        "_default_types",
        "_has_time",
        "_to_drop",
        "_start",
        "_end",
        "_ep_fields",
        "_symbols",
        "_field_delimiter",
        "_converters",
        "_order_ticks",
        "_auto_increase_timestamps",
        "_db",
        "_drop_index",
        "_change_date_to",
        "_timestamp_name",
        "_filepath_or_buffer",
        "_first_line_is_title",
        "_handle_escaped_chars",
        "_quote_char",
        "_timestamp_format",
        "_file_contents",
    ]

    def __init__(self,
                 filepath_or_buffer=None,
                 timestamp_name: Optional[str] = "Time",
                 first_line_is_title: bool = True,
                 names: Optional[list] = None,
                 dtype: Optional[dict] = None,
                 converters: Optional[dict] = None,
                 order_ticks=False,
                 drop_index=True,
                 change_date_to=None,
                 auto_increase_timestamps=True,
                 db='LOCAL',
                 field_delimiter=',',
                 handle_escaped_chars=False,
                 quote_char='"',
                 timestamp_format: Optional[Union[str, Dict[str, str]]] = None,
                 file_contents: Optional[str] = None,
                 **kwargs):

        self._dtype = dtype or {}
        self._names = names
        self._converters = converters or {}
        if (len(field_delimiter) != 1 and field_delimiter != '\t') or field_delimiter == '"' or field_delimiter == "'":
            raise ValueError(f'`field_delimiter` can be single character (except quotes) '
                             f'or "\t" but "{field_delimiter}" was passed')
        self._field_delimiter = field_delimiter
        if len(quote_char) > 1:
            raise ValueError(f'quote_char should be single char but `{quote_char}` was passed')
        if self._field_delimiter == quote_char:
            raise ValueError(f'`{self._field_delimiter}` is both field_delimiter and quote_char')
        if quote_char in string.whitespace:
            raise ValueError('Whitespace can not be a quote_char')
        self._quote_char = quote_char
        self._order_ticks = order_ticks
        self._auto_increase_timestamps = auto_increase_timestamps
        self._db = db
        self._drop_index = drop_index
        self._change_date_to = change_date_to
        self._timestamp_name = timestamp_name
        self._filepath_or_buffer = filepath_or_buffer
        self._first_line_is_title = first_line_is_title
        self._handle_escaped_chars = handle_escaped_chars
        self._timestamp_format = timestamp_format
        self._file_contents = file_contents

        if self._try_default_constructor(**kwargs):
            return

        if self._filepath_or_buffer is not None and self._file_contents is not None:
            raise ValueError("Parameters 'filepath_or_buffer' and 'file_contents' can't be set at the same time.")

        need_to_parse_file = (
            self._file_contents is not None or
            self._filepath_or_buffer is not None and not isinstance(self._filepath_or_buffer, _SymbolParamSource)
        )
        if need_to_parse_file:
            self._columns, self._default_types, self._forced_title, self._symbols = self._parse_file()
        else:
            self._filepath_or_buffer = None
            names = self._names or []
            self._columns = {name: str for name in names}
            self._default_types = {}
            # we don't know it is actually forced, but otherwise we would ignore the first not commented-out line
            self._forced_title = self._first_line_is_title
            self._symbols = None

        self._check_time_column()

        for t in self._dtype:
            if t not in self._columns:
                raise ValueError(f"dtype '{t}' not found in columns list")
            self._columns[t] = self._dtype[t]

        self._ep_fields = ",".join(
            f'{ott.type2str(dtype)} {column}' if issubclass(dtype, otp.string) else column
            for column, dtype in self._columns.items()
        )

        self._to_drop = self._get_to_drop()
        self._has_time, self._start, self._end = self._get_start_end(**kwargs)

        self._columns_with_bool_replaced = dict((n, c if c != bool else float) for n, c in self._columns.items())

        super().__init__(
            _symbols=self._symbols,
            _start=self._start,
            _end=self._end,
            _base_ep_func=self.base_ep,
            schema=self._columns_with_bool_replaced,
        )

        # fake run converters to set proper schema
        if self._converters:
            for column, converter in self._converters.items():
                self.schema[column] = converter(self[column]).dtype

        if self._has_time and self._timestamp_name in self.schema:
            if self.schema[self._timestamp_name] not in [ott.nsectime, ott.msectime]:
                raise ValueError(f"CSV converter for {self._timestamp_name} is converting to "
                                 f"{self.schema[timestamp_name]} type, but expected resulted type is "
                                 f"ott.msectime or ott.nsectime")

        # remove timestamp_name column, if we use it as TIMESTAMP source
        if self._has_time and self._timestamp_name != "Time":
            del self[self._timestamp_name]

    def _check_time_column(self):
        if "TIMESTAMP" in self._columns:
            raise ValueError(
                "It is not allowed to have 'TIMESTAMP' columns, because it is reserved name in OneTick"
            )

        if "Time" in self._columns and self._timestamp_name != "Time":
            raise ValueError(
                "It is not allowed to have 'Time' column not used as timestamp field."
            )

    def _get_to_drop(self):
        to_drop = []
        if "TICK_STATUS" in self._columns:
            del self._columns["TICK_STATUS"]
            to_drop.append("TICK_STATUS")

        if "Index" in self._columns and self._drop_index:
            del self._columns["Index"]
            to_drop.append("Index")
        return to_drop

    def _get_start_end(self, **kwargs):
        start = kwargs.get("start", utils.adaptive)
        end = kwargs.get("end", utils.adaptive)

        has_time = False
        if self._timestamp_name in self._columns:
            has_time = True

            # remove to resolve exception in Source.__init__
            if self._timestamp_name == "Time":
                del self._columns["Time"]

        # redefine start/end time for change_date_to
        if self._change_date_to:
            start = dt.datetime(self._change_date_to.year, self._change_date_to.month, self._change_date_to.day)
            end = ott.next_day(start)
        return has_time, start, end

    def _parse_file(self):
        """
        This function finds the file and get columns names, default types and checks if first line is title via pandas.
        Is also sets the correct value for symbols.
        """
        obj_to_inspect = self._filepath_or_buffer
        if isinstance(obj_to_inspect, utils.FileBuffer):
            obj_to_inspect = io.StringIO(obj_to_inspect.get())
        if self._file_contents is not None:
            obj_to_inspect = io.StringIO(self._file_contents)

        if isinstance(obj_to_inspect, str) and not os.path.exists(obj_to_inspect):
            # if not found, probably, CSV file is located in OneTick CSV_FILE_PATH, check it for inspect_by_pandas()
            csv_paths = otp.utils.get_config_param(os.environ["ONE_TICK_CONFIG"], "CSV_FILE_PATH", default="")
            if csv_paths:
                for csv_path in csv_paths.split(","):
                    csv_path = os.path.join(csv_path, obj_to_inspect)
                    if os.path.exists(csv_path):
                        obj_to_inspect = csv_path
                        break

        columns, default_types, forced_title = _csv_inspector.inspect_by_pandas(
            obj_to_inspect,
            self._first_line_is_title,
            self._names,
            self._field_delimiter,
            self._quote_char,
        )
        if isinstance(self._filepath_or_buffer, utils.FileBuffer) or self._file_contents is not None:
            symbols = 'DUMMY'
        else:
            # str, because there might passed an os.PathLike object
            symbols = str(obj_to_inspect)
        return columns, default_types, forced_title, symbols

    def _get_timestamp_format(self, column_name, dtype):
        if dtype not in (ott.nsectime, ott.msectime):
            raise ValueError(f"Wrong value for parameter 'dtype': {dtype}")
        if self._timestamp_format is None:
            # by default we parse timestamp_name into TIMESTAMP field
            # from typical/default Time format from OneTick dump
            if dtype is ott.nsectime:
                return '%Y/%m/%d %H:%M:%S.%J'
            else:
                return '%Y/%m/%d %H:%M:%S.%q'
        if isinstance(self._timestamp_format, dict):
            return self._timestamp_format[column_name]
        return self._timestamp_format

    def base_ep(self):
        # initialize Source and set schema to columns.
        file_contents = ''
        columns_to_drop = self._to_drop.copy()

        if isinstance(self._filepath_or_buffer, utils.FileBuffer):
            file_contents = self._filepath_or_buffer.get()
        if self._file_contents is not None:
            file_contents = self._file_contents

        csv = Source(
            otq.CsvFileListing(
                field_delimiters=f"'{self._field_delimiter}'",
                time_assignment="_START_TIME",
                # we use EP's first_line_is_title only when file path is passed through symbol
                # otherwise we don't use EP's first_line_is_title, because EP raise error on empty column name,
                # and we explicitly define name for such columns in FIELDS arg.
                # but if first line started with # (forced_title=True), then this param ignored :(
                first_line_is_title=(self._filepath_or_buffer is None and
                                     self._file_contents is None and
                                     self._first_line_is_title),
                fields=self._ep_fields,
                file_contents=file_contents,
                handle_escaped_chars=self._handle_escaped_chars,
                quote_chars=f"'{self._quote_char}'",
            ),
            schema=self._columns_with_bool_replaced,
        )

        if self._first_line_is_title and not self._forced_title:
            # remove first line with titles for columns.
            csv.sink(otq.DeclareStateVariables(variables="long __TICK_INDEX=0"))
            csv.sink(otq.PerTickScript("STATE::__TICK_INDEX = STATE::__TICK_INDEX + 1;"))
            csv.sink(otq.WhereClause(discard_on_match=False, where="STATE::__TICK_INDEX > 1"))

        # set tick type to ANY
        update_node_tick_type(csv, "ANY", self._db)

        # check whether need to update types, because if column type is not specified in header
        # then by default column has string type in OneTick
        update_columns = {}
        for name, dtype in self._columns.items():
            if not issubclass(dtype, str) and name not in self._default_types:
                update_columns[name] = dtype

        for name, dtype in update_columns.items():
            if dtype is int:
                # BE-142 - workaround for converting string to int
                # OneTick first convert string to float, and then to int, which leeds to losing precision
                csv.sink(otq.AddField(field=f"_TMP_{name}", value="atol(" + name + ")"))
                csv.sink(otq.Passthrough(fields=name, drop_fields=True))
                csv.sink(otq.AddField(field=f"{name}", value=f"_TMP_{name}"))
                csv.sink(otq.Passthrough(fields=f"_TMP_{name}", drop_fields=True))
            elif dtype is float:
                csv.sink(otq.UpdateField(field=name, value="atof(" + name + ")"))
            elif dtype is ott.msectime:
                timestamp_format = self._get_timestamp_format(name, dtype)
                csv.sink(otq.UpdateField(field=name,
                                         value=f'time_format("{timestamp_format}",0,_TIMEZONE)',
                                         where=name + '=""'))
                csv.sink(otq.UpdateField(field=name, value=f'parse_time("{timestamp_format}",{name},_TIMEZONE)'))
            elif dtype is ott.nsectime:
                timestamp_format = self._get_timestamp_format(name, dtype)
                csv.sink(otq.UpdateField(field=name,
                                         value=f'time_format("{timestamp_format}",0,_TIMEZONE)',
                                         where=name + '=""'))
                # TODO: this is the logic from otp.Source._update_field,
                # we should use _Source methods here or refactor
                csv.sink(otq.AddField(field=f"_TMP_{name}",
                                      value=f'parse_nsectime("{timestamp_format}",{name},_TIMEZONE)'))
                csv.sink(otq.Passthrough(fields=name, drop_fields=True))
                csv.sink(otq.AddField(field=name, value=f"_TMP_{name}"))
                csv.sink(otq.Passthrough(fields=f"_TMP_{name}", drop_fields=True))
            elif dtype is bool:
                csv.sink(otq.UpdateField(field=name, value="CASE(" + name + ", 'true', 1.0, 0.0)"))
            else:
                raise TypeError(f"Unsupported type '{dtype}'")

        # run converters
        if self._converters:
            for column, converter in self._converters.items():
                if csv[column].dtype is not otp.nsectime and converter(csv[column]).dtype is otp.nsectime:
                    # workaround for resolve bug on column type changing:
                    # https://onemarketdata.atlassian.net/browse/PY-416
                    csv[f'_T_{name}'] = converter(csv[column])
                    del csv[column]
                    csv[column] = csv[f'_T_{name}']
                    del csv[f'_T_{name}']
                else:
                    csv[column] = converter(csv[column])

        if self._has_time:
            # if timestamp_name column is defined in the csv, then apply tick time adjustment

            if self._timestamp_name in self._converters:
                # we assume that if timestamp_name field in converters,
                # then it is already converted to otp.dt
                csv.sink(
                    otq.UpdateField(
                        field="TIMESTAMP",
                        value=self._timestamp_name,
                        allow_unordered_output_times=True,
                    )
                )
            else:
                if self._change_date_to:
                    self._change_date_to = self._change_date_to.strftime("%Y/%m/%d")
                    csv.sink(otq.UpdateField(field="Time",
                                             value=f'"{self._change_date_to}" + substr({self._timestamp_name}, 10)'))

                timestamp_format = self._get_timestamp_format(self._timestamp_name, otp.nsectime)
                csv.sink(
                    otq.UpdateField(
                        field="TIMESTAMP",
                        value=f'parse_nsectime("{timestamp_format}", {self._timestamp_name}, _TIMEZONE)',
                        allow_unordered_output_times=True,
                    )
                )

            # drop source timestamp_name field in favor of new TIMESTAMP field
            columns_to_drop.append(self._timestamp_name)
        elif self._auto_increase_timestamps:
            # default time for ticks are increasing from 0
            csv.sink(otq.DeclareStateVariables(variables="long __TIMESTAMP_INC__ = 0"))
            csv.sink(otq.UpdateField(
                field="TIMESTAMP",
                value='DATEADD("millisecond",STATE::__TIMESTAMP_INC__,TIMESTAMP,_TIMEZONE)'))
            csv.sink(otq.UpdateField(field="STATE::__TIMESTAMP_INC__", value="STATE::__TIMESTAMP_INC__ + 1"))

        if self._order_ticks:
            csv.sort('TIMESTAMP', inplace=True)

        if columns_to_drop:
            csv.sink(otq.Passthrough(fields=",".join(columns_to_drop), drop_fields="True"))

        return csv


def LocalCSVTicks(path,  # NOSONAR
                  start=utils.adaptive,
                  end=utils.adaptive,
                  date_converter=default_date_converter,
                  additional_date_columns=None,
                  converters=None,
                  tz=None,
                  ):
    """
    Loads ticks from csv file, and creating otp.Ticks object from them

    Parameters
    ----------
    path: str
        Absolute path to csv file
    start: datetime object
        Start of the query interval
    end: datetime object
        End of the query interval
    date_converter:
        A converter from string to datetime format, by default used only to TIMESTAMP column
    additional_date_columns:
        Other columns to convert to datetime format
    converters:
        Non default converters to columns from strings
    tz:
        timezone

    Returns
    -------
    otp.Ticks
    """
    if tz is None:
        tz = configuration.config.tz

    c = {'TIMESTAMP': partial(to_timestamp_nanos, date_converter=date_converter, tz=tz)}
    if converters is not None:
        c.update(converters)
    if additional_date_columns is not None:
        c.update({column: partial(to_timestamp_nanos,
                                  date_converter=date_converter,
                                  tz=tz,
                                  ) for column in additional_date_columns})
    df = pd.read_csv(path, converters=c)
    df['TS_'] = df['TIMESTAMP']
    df['SYMBOL_NAME'] = df['#SYMBOL_NAME']
    d = df.to_dict(orient='list')
    del d['TIMESTAMP']
    del d['#SYMBOL_NAME']

    ticks = Ticks(d, start=start, end=end)
    ticks['TIMESTAMP'] = ticks['TS_']
    ticks = ticks.drop('TS_')

    return ticks
