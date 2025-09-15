import os
import shutil
import pytest
import datetime
import pandas as pd
import numpy as np
from pathlib import Path

try:
    from pandas.core.dtypes.common import get_dtype
except ImportError:
    from pandas.core.dtypes.common import _get_dtype as get_dtype

import onetick.py as otp
from onetick.py.utils import TmpDir
from onetick.py.core._csv_inspector import inspect_by_pandas
from onetick.py.compatibility import is_supported_uint_numpy_interface


@pytest.fixture(scope="module")
def data_dir():
    """ reference to the data dir"""
    return os.path.join(os.path.dirname(__file__), "data")


def test_csv_file_path_relative(data_dir):
    cfg = otp.Config(csv_path=[TmpDir().path])

    with otp.Session(cfg, copy=False) as session:
        csv_paths = otp.utils.get_config_param(session.config.path, "CSV_FILE_PATH")
        csv_path = os.path.join(csv_paths.split(",")[0], "tmp.csv")
        shutil.copyfile(os.path.join(data_dir, "data1.csv"), csv_path)
        csv = otp.CSV("tmp.csv")
        df = otp.run(csv)
        assert len(df) == 1


def test_csv_file_path_relative_with_empty_csv_file_path(data_dir):
    cfg = otp.Config(csv_path=None)

    with otp.Session(cfg, copy=False):
        with pytest.raises(FileNotFoundError):
            otp.CSV("tmp.csv")


def test_read_csv(data_dir, m_session):
    # test simple CSV without any param set
    data = otp.CSV(os.path.join(data_dir, "data1.csv"))
    df = otp.run(data)
    assert len(df) == 1


def test_read_csv_using_buffer(data_dir, m_session):
    # test simple CSV without any param set
    data = otp.CSV(otp.utils.file(os.path.join(data_dir, "data1.csv")))
    df = otp.run(data)
    assert len(df) == 1


def test_read_csv_using_file_contents(data_dir, m_session):
    file_path = os.path.join(data_dir, "data1.csv")
    file_contents = Path(file_path).read_text()
    with pytest.raises(ValueError, match="Parameters 'filepath_or_buffer' and 'file_contents' can't be set"):
        otp.CSV(file_path, file_contents='A,B')
    data = otp.CSV(file_contents=file_contents)
    assert data.schema['Symbol'] is str
    assert data.schema['PRICE'] is float
    assert data.schema['SIZE'] is int
    df = otp.run(data)
    assert len(df) == 1
    assert df['Time'][0] == pd.Timestamp('2003/12/01 16:02:17.000')
    assert df['Symbol'][0] == 'DEMO_L1::AAPL'
    assert df['PRICE'][0] == 20.99
    assert df['SIZE'][0] == 200
    assert df['COND'][0] == 'T   '


@pytest.mark.parametrize('first_line_is_title,count', [[True, 1], [False, 2]])
def test_empty_column_name(data_dir, m_session, first_line_is_title, count):
    csv = otp.CSV(
        os.path.join(data_dir, "test_empty_column_name.csv"),
        first_line_is_title=first_line_is_title,
    )
    df = otp.run(csv)
    assert len(df) == count


def test_csv_names(data_dir, m_session):
    data = otp.CSV(
        os.path.join(data_dir, "example_events.csv"),
        names=['idx2', 'stock2', 'time_number2', 'px2', 'side2', 'clOrdId2'])
    df = otp.run(data)
    assert len(df) == 84
    assert df.dtypes['idx2'] == np.dtype('int64')
    assert df.dtypes['stock2'] == np.dtype('O')  # dtype('O') is for string in pandas
    assert df.dtypes['time_number2'] == np.dtype('int64')
    assert df.dtypes['px2'] == float
    assert df.dtypes['side2'] == np.dtype('O')  # dtype('O') is for string in pandas
    assert df.dtypes['clOrdId2'] == np.dtype('int64')


class TestCSVSchema:
    def test_pandas_types_priority(self, data_dir, m_session):
        # for the future clarity test shows up pandas read_csv() typing workflow
        # https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
        # If converters are specified, they will be applied INSTEAD of dtype conversion.
        # NOTE: but still dtype are used for columns, not defined in converters.
        with pytest.warns(pd.errors.ParserWarning):
            df = pd.read_csv(
                os.path.join(data_dir, "example_events.csv"),
                converters={"px": str},
                dtype={"px": int},  # this int() must raise exception on "30.89" value, but it doesn't!
            )
        assert df.dtypes['px'] == np.dtype("O")

    def test_read_csv_auto_types_schema(self, data_dir, m_session):
        data = otp.CSV(
            os.path.join(data_dir, "example_events.csv"))
        df = otp.run(data)
        assert len(df) == 84
        assert df.dtypes['idx'] == np.dtype('int64')
        assert df.dtypes['stock'] == np.dtype('O')  # dtype('O') is for string in pandas
        assert df.dtypes['time_number'] == np.dtype('int64')
        assert df.dtypes['px'] == float
        assert df.dtypes['side'] == np.dtype('O')  # dtype('O') is for string in pandas
        assert df.dtypes['clOrdId'] == np.dtype('int64')

    def test__convert_pandas_types(self, data_dir, m_session):
        from onetick.py.core._csv_inspector import _convert_pandas_types
        assert _convert_pandas_types(get_dtype(np.int64)) == int
        assert _convert_pandas_types(get_dtype(int)) == int
        assert _convert_pandas_types(get_dtype(float)) == float
        assert _convert_pandas_types(get_dtype(str)) == str

    def test_inspect_by_pandas_default_types(self, data_dir, m_session):
        columns, _, _ = inspect_by_pandas(os.path.join(data_dir, "test_default_types.csv"))
        assert columns['PRICE'] == float
        assert len(columns.keys()) == 2

    def test_read_csv_dtype(self, data_dir, m_session):
        data = otp.CSV(
            os.path.join(data_dir, "example_events.csv"),
            dtype={
                "clOrdId": str,
                "px": float,
            })
        assert data.schema['clOrdId'] == str
        assert data.schema['px'] == float

        df = otp.run(data)
        assert df.dtypes['clOrdId'] == np.dtype('O')  # dtype('O') is for string in pandas
        assert df.dtypes['px'] == float

    def test_bad_dtype(self, data_dir, m_session):
        with pytest.raises(ValueError, match="not found in columns list"):
            otp.CSV(
                os.path.join(data_dir, "example_events.csv"),
                dtype={"not_exists": int}
            )

    def test_read_csv_with_converter(self, data_dir, m_session):
        csv = otp.CSV(
            os.path.join(data_dir, "example_events.csv"),
            converters={
                "time_number": lambda c: c.apply(otp.nsectime),
                "stock": lambda c: c.str.lower(),
            },
        )

        df = otp.run(csv)
        assert len(df) == 84

        assert csv.schema['idx'] == int
        assert csv.schema['stock'] == str
        assert csv.schema['px'] == float
        assert csv.schema['side'] == str
        assert csv.schema['clOrdId'] == int
        assert csv.schema['time_number'] == otp.nsectime

        assert df.dtypes['idx'] == np.dtype('int64')
        assert df.dtypes['stock'] == np.dtype('O')
        assert df.dtypes['px'] == float
        assert df.dtypes['side'] == np.dtype('O')
        assert df.dtypes['clOrdId'] == np.dtype('int64')
        assert df.dtypes['time_number'].type == np.datetime64

        assert df['stock'][0] == "top"

    def test_csv_default_types(self, data_dir, m_session):
        csv = otp.CSV(
            os.path.join(data_dir, "test_default_types.csv"),
            first_line_is_title=True,
        )
        df = otp.run(csv)
        assert df.dtypes['PRICE'] == float
        assert df['PRICE'][0] == 1.0
        assert csv.schema['PRICE'] == float

    def test_bad_dtype_for_timestamp_field(self, data_dir, m_session):
        with pytest.raises(ValueError, match="expected resulted type is ott.msectime"):
            otp.CSV(
                os.path.join(data_dir, "example_events.csv"),
                timestamp_name="time_number",
                converters={"time_number": lambda c: c.astype(str)}
            )


def test_csv_hash_in_title(data_dir, m_session):
    csv = otp.CSV(
        os.path.join(data_dir, "test_hash_title.csv"),
        start=otp.dt(2022, 5, 26, 0, 0, 0),
        end=otp.dt(2022, 5, 28, 0, 0, 0),
        first_line_is_title=True,
    )
    df = otp.run(csv)
    assert len(df) == 2
    assert df.dtypes['BID_PRICE'] == float


def test_csv_hash_in_title_with_first_line_is_title(data_dir, m_session):
    # CSV_FILE_LISTING will ignore first_line_is_title=False, if first line in CSV starts with #
    # it leads to inconsistency of pandas inspection and OneTick
    with pytest.raises(ValueError, match="If first line of CSV starts with #"):
        otp.CSV(
            os.path.join(data_dir, "test_hash_title.csv"),
            start=otp.dt(2022, 5, 26, 0, 0, 0),
            end=otp.dt(2022, 5, 28, 0, 0, 0),
            first_line_is_title=False,
        )


class TestDefaultTimeInterval:
    """ Check that Time column has the same value as in the original file
        when Time has values between DEFAULT_START_TIME and DEFAULT_END_TIME
    """

    def test_middle_range(self, data_dir, m_session):
        data = otp.CSV(os.path.join(data_dir, "data1.csv"))
        df = otp.run(data)

        assert "Time" in df.columns
        assert df["Time"][0] == otp.datetime(2003, 12, 1, 16, 2, 17)

    def test_corner_range(self, data_dir, m_session):
        data = otp.CSV(os.path.join(data_dir, "data2.csv"))
        df = otp.run(data)

        assert "Time" in df.columns
        assert df["Time"][0] == otp.datetime(2003, 12, 1, 1, 1, 10)


class TestDropIndex:
    """ Test the drop_index parameter """

    def test_default(self, data_dir, m_session):
        """ default value is True, that means drop Index column"""
        data = otp.CSV(os.path.join(data_dir, "data1.csv"))
        df = otp.run(data)

        assert "Index" not in data.columns()
        assert "Index" not in df.columns

    def test_drop_index(self, data_dir, m_session):
        data = otp.CSV(os.path.join(data_dir, "data1.csv"), drop_index=True)
        df = otp.run(data)

        assert "Index" not in data.columns()
        assert "Index" not in df.columns

    def test_do_not_drop_index(self, data_dir, m_session):
        data = otp.CSV(os.path.join(data_dir, "data1.csv"), drop_index=False)
        df = otp.run(data)

        assert "Index" in data.columns()
        assert "Index" in df.columns


class TestChangeDateTo:
    """ Test the change_date_to parameter that allows to adjust
    resulting date for the timestamp
    """

    def test_change_date_to_specific(self, data_dir, m_session):
        """ it is possible to specify a date to change date from Time column in csv file"""
        data = otp.CSV(os.path.join(data_dir, "data3.csv"),
                       change_date_to=otp.config['default_start_time'] + otp.Day(1))
        df = otp.run(data)

        assert df["Time"][0] == otp.datetime(2003, 12, 2, 13, 5, 17)

    def test_change_date_to_specific_out_1(self, data_dir, m_session):
        """ check case when change_date_to > DEFAULT_END_TIME """
        data = otp.CSV(os.path.join(data_dir, "data3.csv"), change_date_to=otp.datetime(2018, 9, 13))
        df = otp.run(data)

        assert df["Time"][0] == otp.datetime(2018, 9, 13, 13, 5, 17)

    def test_change_date_to_specific_out_2(self, data_dir, m_session):
        """ check case when change_date_to < DEFAULT_START_TIME """
        data = otp.CSV(os.path.join(data_dir, "data3.csv"), change_date_to=otp.datetime(2001, 2, 16))
        df = otp.run(data)

        assert df["Time"][0] == otp.datetime(2001, 2, 16, 13, 5, 17)

    def test_change_date_to_and_ticks(self, data_dir, m_session):
        """ check case change_date to modifies query interval and
        the query uses Ticks"""
        data1 = otp.CSV(os.path.join(data_dir, "data3.csv"), change_date_to=otp.datetime(2006, 2, 16))
        data2 = otp.Ticks(dict(x=[1]))

        merged_data = data1 + data2

        df = otp.run(merged_data)
        assert all(df["Time"] == [pd.Timestamp("2006-02-16 00:00:00"), pd.Timestamp("2006-02-16 13:05:17")])


class TestSolRegressionCases:
    def test_db_loading(self, m_session, data_dir):
        start_dt = datetime.datetime.combine(
            datetime.date.today() - datetime.timedelta(days=4),
            datetime.datetime.min.time())
        dbinfo_db = otp.DB("DB_INFO")
        symbol = 'MS21'
        src = otp.CSV(os.path.join(data_dir, 'dbinfo_MS21.csv'))
        dbinfo_db.add(src=src, date=start_dt, symbol=symbol, tick_type="COMPLETENESS")
        m_session.use(dbinfo_db)

    def test_hash_first_line_with_time_from_test_layering_and_spoofing(self, data_dir, m_session):
        # Time format: 06/01/2016 10:18:00.184
        csv = otp.CSV(
            os.path.join(data_dir, "test_hash_with_time.csv"),
            first_line_is_title=True,
            change_date_to=otp.config['default_start_time'] + otp.Day(1)
        )
        df = otp.run(csv)
        assert len(df) == 1


class TestCSVTimestamp:
    def test_timestamp_column_exception(self, data_dir, m_session):
        with pytest.raises(ValueError, match="reserved name"):
            otp.CSV(
                os.path.join(data_dir, "test_timestamp_column.csv"),
                timestamp_name="TIMESTAMP",
            )

    def test_time_and_timestamp_together_exception(self, data_dir, m_session):
        with pytest.raises(ValueError, match="not used as timestamp field"):
            otp.CSV(
                os.path.join(data_dir, "test_time_column.csv"),
                timestamp_name="Time2",
            )

    def test_strange_timestamp_format(self, data_dir, m_session):
        # set TIMESTAMP in format like 123123435.456 (msecs.nano)
        csv = otp.CSV(
            os.path.join(data_dir, "test_timestamp_format.csv"),
            converters={
                "time_formatted": lambda c: (c.apply(float) * 1000).apply(int).apply(otp.nsectime)
            },
        )
        df = otp.run(csv)
        assert csv.schema['time_formatted'] == otp.nsectime
        assert df.dtypes['time_formatted'].type == np.datetime64
        assert df['time_formatted'][0] == otp.dt(2022, 7, 1, 11, 56, 26, 953602, 304)

    def test_converters_date_formatters(self, data_dir, m_session):
        csv = otp.CSV(
            os.path.join(data_dir, "test_date_formatters.csv"),
            converters={
                "time_formatted": lambda c: c.str.to_datetime("%m/%d/%y-%H:%M:%S.%J", 'Europe/London')
            },
        )
        df = otp.run(csv)
        assert csv.schema['time_formatted'] == otp.nsectime
        assert df.dtypes['time_formatted'].type == np.datetime64
        assert df['time_formatted'][0] == otp.dt(2022, 5, 17, 6, 10, 56, 123000)

    def test_converter_time_column(self, data_dir, m_session):
        csv = otp.CSV(
            os.path.join(data_dir, "test_Time_formatter.csv"),
            timestamp_format="%m/%d/%y-%H:%M:%S.%J",
        )
        df = otp.run(csv, date=otp.dt(2022, 5, 17), timezone='GMT')
        assert csv.schema['Time'] == otp.nsectime
        assert csv.schema['time_formatted'] == str
        assert csv.schema['time_formatted_2'] == str
        assert df.dtypes['Time'].type == np.datetime64
        assert df.dtypes['time_formatted'].type == np.dtype('O')
        assert df.dtypes['time_formatted_2'].type == np.dtype('O')
        assert df['Time'][0] == otp.dt(2022, 5, 17, 11, 10, 56, 123123, 123)
        assert df['time_formatted'][0] == '6/18/23-12:11:57.124124124'
        assert df['time_formatted_2'][0] == '2023-06-18 12:11:57.654321'
        assert df['Time'][1] == otp.dt(2022, 5, 17, 11, 10, 56, 123123, 124)
        assert df['time_formatted'][1] == ''
        assert df['time_formatted_2'][1] == ''

        csv = otp.CSV(
            os.path.join(data_dir, "test_Time_formatter.csv"),
            timestamp_format="%m/%d/%y-%H:%M:%S.%J",
            dtype={'time_formatted': otp.nsectime},
        )
        df = otp.run(csv, date=otp.dt(2022, 5, 17), timezone='GMT')
        assert csv.schema['Time'] == otp.nsectime
        assert csv.schema['time_formatted'] == otp.nsectime
        assert csv.schema['time_formatted_2'] == str
        assert df.dtypes['Time'].type == np.datetime64
        assert df.dtypes['time_formatted'].type == np.datetime64
        assert df.dtypes['time_formatted_2'].type == np.dtype('O')
        assert df['Time'][0] == otp.dt(2022, 5, 17, 11, 10, 56, 123123, 123)
        assert df['time_formatted'][0] == otp.dt(2023, 6, 18, 12, 11, 57, 124124, 124)
        assert df['time_formatted_2'][0] == '2023-06-18 12:11:57.654321'
        assert df['Time'][1] == otp.dt(2022, 5, 17, 11, 10, 56, 123123, 124)
        assert df['time_formatted'][1] == otp.dt(1970, 1, 1)
        assert df['time_formatted_2'][1] == ''

        csv = otp.CSV(
            os.path.join(data_dir, "test_Time_formatter.csv"),
            timestamp_format={
                'Time': "%m/%d/%y-%H:%M:%S.%J",
                'time_formatted': "%m/%d/%y-%H:%M:%S.%J",
                'time_formatted_2': "%Y-%m-%d %H:%M:%S.%q",
            },
            dtype={
                'time_formatted': otp.nsectime,
                'time_formatted_2': otp.msectime,
            },
        )
        df = otp.run(csv, date=otp.dt(2022, 5, 17), timezone='GMT')
        assert csv.schema['Time'] == otp.nsectime
        assert csv.schema['time_formatted'] == otp.nsectime
        assert csv.schema['time_formatted_2'] == otp.msectime
        assert df.dtypes['Time'].type == np.datetime64
        assert df.dtypes['time_formatted'].type == np.datetime64
        assert df.dtypes['time_formatted_2'].type == np.datetime64
        assert df['Time'][0] == otp.dt(2022, 5, 17, 11, 10, 56, 123123, 123)
        assert df['time_formatted'][0] == otp.dt(2023, 6, 18, 12, 11, 57, 124124, 124)
        assert df['time_formatted_2'][0] == otp.dt(2023, 6, 18, 12, 11, 57, 654000)
        assert df['Time'][1] == otp.dt(2022, 5, 17, 11, 10, 56, 123123, 124)
        assert df['time_formatted'][1] == otp.dt(1970, 1, 1)
        assert df['time_formatted_2'][1] == otp.dt(1970, 1, 1)

    def test_read_csv_with_timestamp_name(self, data_dir, m_session):
        data = otp.CSV(
            os.path.join(data_dir, "example_events.csv"),
            converters={
                "time_number": lambda c: c.apply(otp.nsectime),
                "stock": lambda c: c.str.lower(),
            },
            timestamp_name="time_number",
            start=otp.dt(2010, 8, 1),
            end=otp.dt(2022, 9, 2),
            order_ticks=True,
        )
        df = otp.run(data)
        assert len(df) == 84
        assert "time_number" not in data.schema
        assert df['Time'][0] == otp.dt(2022, 7, 1, 4, 11, 12, 634071, 31)

    def test_read_csv_timezone_changed(self, data_dir, monkeypatch, m_session):
        monkeypatch.setattr(otp.config, 'tz', 'EST4EDT')

        data = otp.CSV(
            os.path.join(data_dir, "example_events.csv"),
            converters={
                "time_number": lambda c: c.apply(otp.nsectime),
            },
            timestamp_name="time_number",
            start=otp.dt(2022, 6, 1),
            end=otp.dt(2022, 7, 2),
            order_ticks=True,
        )
        df = otp.run(data)
        assert df['Time'][0] == otp.dt(2022, 7, 1, 5, 11, 12, 634071, 31)


def test_long_string(m_session, data_dir):
    data = otp.CSV(os.path.join(data_dir, "test_long_string.csv"))
    assert data.schema['SEC_ID'] is otp.string[128]
    assert data.schema['VAR'] is otp.varstring
    df = otp.run(data)
    assert df['SEC_ID'][0] == 'here_is_my_super_long_string_' * 4
    assert df['VAR'][0] == 'here_is_my_super_long_string_' * 4


@pytest.mark.parametrize("dtype", (otp.string[128], otp.varstring))
def test_long_string_dtype(m_session, data_dir, dtype):
    data = otp.CSV(os.path.join(data_dir, "test_long_string_dtype.csv"),
                   dtype={'SEC_ID': dtype})
    assert data.schema['SEC_ID'] is dtype
    df = otp.run(data)
    assert df['SEC_ID'][0] == 'here_is_my_super_long_string_' * 4


@pytest.mark.skipif(os.name == 'nt', reason='may be different sizes on windows')
def test_integers(m_session, data_dir):
    data = otp.CSV(os.path.join(data_dir, 'test_integers.csv'))
    assert data.schema['BYTE_VAR'] is otp.byte
    assert data.schema['SHORT_VAR'] is otp.short
    assert data.schema['INT_VAR'] is otp.int
    assert data.schema['UINT_VAR'] is otp.uint
    assert data.schema['ULONG_VAR'] is otp.ulong
    df = otp.run(data)
    assert df['BYTE_VAR'][0] == 127
    assert df['SHORT_VAR'][0] == 32767
    assert df['ULONG_VAR'][0] == 18446744073709551615
    if is_supported_uint_numpy_interface():
        assert df['UINT_VAR'][0] == 4294967295
    else:
        assert df['UINT_VAR'][0] == -1


def test_start_time_nanos(m_session, data_dir):
    data = otp.CSV(os.path.join(data_dir, 'test_integers.csv'))
    df = otp.run(data, start=otp.dt(2003, 12, 1, 0, 0, 0, 1001))
    assert df['Time'][0] == pd.Timestamp(2003, 12, 1, 0, 0, 0, 1001)


def test_decimal(m_session, data_dir):
    data = otp.CSV(os.path.join(data_dir, 'test_decimal.csv'))
    assert data.schema['DECIMAL_VAR'] is otp.decimal
    assert data.schema['FLOAT_VAR'] is float
    data['DEC'] = data['DECIMAL_VAR'].decimal.str(precision=34)
    data['FLO'] = data['FLOAT_VAR'].float.str(precision=34)
    df = otp.run(data)
    assert df['DEC'][0] == '0.2999999999999999888977697537484346'
    assert df['FLO'][0] == '0.30000000'


def test_long_number(m_session):
    # BE-142
    # 1688964028797322000 is too big and ATOL() trims it to 1688964028797321984
    tmpdir = otp.utils.TmpDir()
    tmp_file = os.path.join(tmpdir, "test_long_number.csv")
    pd.DataFrame([[1688964028797322000]], columns=["te"]).to_csv(tmp_file, index=False)
    query = otp.CSV(tmp_file, dtype={"te": int})
    query["te1"] = query["te"] + 1
    data = otp.run(query)
    assert data['te'][0] == 1688964028797322000
    assert data['te1'][0] == 1688964028797322001


@pytest.mark.parametrize(
    'auto_increase_timestamps', [True, False, None]
)
def test_timestamp_increment(m_session, data_dir, auto_increase_timestamps):
    if auto_increase_timestamps is not None:
        data = otp.CSV(os.path.join(data_dir, 'test_timestamp_increment.csv'),
                       auto_increase_timestamps=auto_increase_timestamps)
    else:
        data = otp.CSV(os.path.join(data_dir, 'test_timestamp_increment.csv'))

    start_time = otp.dt(2020, 1, 2, 3, 44, 55, 129)
    res = otp.run(data, start=start_time, end=start_time + otp.Day(1))
    assert len(res) == 4
    for i in range(0, len(res)):
        if auto_increase_timestamps is True or auto_increase_timestamps is None:
            assert res['Time'][i] == start_time + otp.Milli(i)
        else:
            assert res['Time'][i] == start_time


def test_path_in_symbol_name(m_session, data_dir):
    path = os.path.join(data_dir, 'test_path_in_symbol_name.csv')
    csv = otp.CSV()
    df = otp.run(csv, symbols=f"LOCAL::{path}")
    assert all(df['te'] == ['1688964028797322'])


def test_path_in_symbol_name_without_header(m_session, data_dir):
    path = os.path.join(data_dir, 'test_path_in_symbol_name_without_header.csv')
    csv = otp.CSV(names=['te'], first_line_is_title=False)
    df = otp.run(csv, symbols=f"LOCAL::{path}")
    assert all(df['te'] == ['1688964028797322'])


def test_path_in_symbol_name_jwq(m_session, data_dir):
    path = os.path.join(data_dir, 'test_path_in_symbol_name.csv')

    def fun_jwq(symbol, pv):
        query = otp.CSV(symbol, dtype={"te": int}, names=["te"])
        query = query.add_prefix("22")
        query["pvv"] = pv
        return query

    query2 = otp.CSV(path, dtype={"te": int})
    query2["start"] = otp.meta_fields["_START_TIME"]
    query2["end"] = otp.meta_fields["_END_TIME"]

    query3 = query2.join_with_query(fun_jwq, params={"pv": query2["te"]}, symbol=path)

    df = otp.run(
        query3,
        start=pd.to_datetime("2023-07-09 00:51:23.797"),
        end=pd.to_datetime("2023-07-18 23:00:00"),
    )
    assert all(df['te'] == [1688964028797322])
    assert all(df['22te'] == [1688964028797322])
    assert all(df['pvv'] == [1688964028797322])


def test_not_title_set_names(m_session, data_dir):
    data = otp.CSV(
        os.path.join(data_dir, 'test_not_title_set_names.csv'),
        first_line_is_title=False,
        names=['A', 'B']
    )
    df = otp.run(data)
    assert len(df) == 2
    assert all(df['A'] == [1, 3])
    assert all(df['B'] == [2, 4])


def test_escape_chars(m_session, data_dir):
    data = otp.CSV(
        os.path.join(data_dir, 'test_escape_chars.csv'),
        handle_escaped_chars=True,
    )
    df = otp.run(data)
    assert len(df) == 2
    assert df['A'][1] == '"1,1"'


def test_file_and_unbound_symbol(m_session, data_dir):
    data = otp.CSV(os.path.join(data_dir, "data1.csv"))
    df = otp.run(data, symbols='AAPL')
    assert len(df) == 1


@pytest.mark.parametrize('field_delimiter', [' ', ',', None])
def test_field_delimiters(m_session, data_dir, field_delimiter):
    if field_delimiter is None:
        data = otp.CSV(os.path.join(data_dir, 'data_diff_delimiters.csv'),
                       first_line_is_title=False)
    else:
        data = otp.CSV(os.path.join(data_dir, 'data_diff_delimiters.csv'),
                       field_delimiter=field_delimiter,
                       first_line_is_title=False)
    df = otp.run(data)
    if field_delimiter == ' ':
        assert all(df['COLUMN_0'] == ['1,2', '4'])
        assert all(df['COLUMN_1'] == ['3', '5,6'])
    else:
        assert all(df['COLUMN_0'] == ['1', '4 5'])
        assert all(df['COLUMN_1'] == ['2 3', '6'])


@pytest.mark.parametrize('quote_char', ['34', ' ', '\t', ','])
def test_incorrect_quote_chars(m_session, data_dir, quote_char):
    with pytest.raises(ValueError):
        otp.CSV(os.path.join(data_dir, 'data1.csv'), quote_char=quote_char)


@pytest.mark.parametrize('quote_char', ["'", '"', None])
def test_quote_char(m_session, data_dir, quote_char):
    if quote_char is None:
        data = otp.CSV(os.path.join(data_dir, 'data_diff_quote_chars.csv'),
                       first_line_is_title=False)
    else:
        data = otp.CSV(os.path.join(data_dir, 'data_diff_quote_chars.csv'),
                       quote_char=quote_char,
                       first_line_is_title=False)
    df = otp.run(data)
    if quote_char == "'":
        assert all(df['COLUMN_0'] == ['1,"2', '"1'])
        assert all(df['COLUMN_1'] == ['3"', '2",3'])
    else:
        assert all(df['COLUMN_0'] == ["'1", "1,'2"])
        assert all(df['COLUMN_1'] == ["2',3", "3'"])


def test_bool_column_in_csv(m_session, data_dir):
    csv = os.path.join(data_dir, 'test_bool_column_in_csv.csv')
    query = otp.CSV(csv, first_line_is_title=True, field_delimiter=',')
    assert query.schema['BOOL_COLUMN'] == float
    df = otp.run(query)
    true_only = df[df['BOOL_COLUMN'] == 1.0]
    assert not true_only.empty
    assert df.dtypes['BOOL_COLUMN'] == np.dtype('float64')
    assert list(df['BOOL_COLUMN']) == [0.0, 0.0, 1.0]
