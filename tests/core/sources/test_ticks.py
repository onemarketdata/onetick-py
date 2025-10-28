import math
import os
import zoneinfo
from pathlib import Path

import pytest
import datetime
import pandas as pd
import numpy as np

import onetick.py as otp

from .test_tick import tick_and_copies


@pytest.mark.parametrize("t", tick_and_copies(otp.Ticks(A=[1, 2, 3])))
def test_copy(session, t):
    assert isinstance(t, otp.sources.ticks._DataCSV)


def test_heterogeneous_ticks(session, par_dir):
    """
    udefined.otq::set_undefined query does the following:
        - adds X_UNDEFINED and Y_UNDEFINED fields
        - if the 'X' field is not defined in a tick, then
          it sets X_UNDEFINED to 'TRUE'; otherwise sets to 'FALSE'
        - if the 'Y' field is not defined in a tick, then
          it sets Y_UNDEFINED to 'TRUE'; otherwise sets to 'FALSE'
    """
    data = otp.Ticks([["X", "Y"], [3, None], [None, -7]])

    q = otp.query(os.path.join(par_dir, "otqs", "undefined.otq") + "::set_undefined")

    df = otp.run(data.apply(q))

    assert "X" in df.columns
    assert "Y" in df.columns

    assert df.X_UNDEFINED[0] == "FALSE" and df.Y_UNDEFINED[0] == "TRUE"
    assert df.X_UNDEFINED[1] == "TRUE" and df.Y_UNDEFINED[1] == "FALSE"


def test_hetero_ticks_common_columns(session, par_dir):
    data = otp.Ticks([["X", "Y", "Z"], [3, 4, None], [None, 5.9, -7]])

    db = otp.DB("SOME_DB")
    db.add(data)

    q = otp.query(os.path.join(par_dir, "otqs", "undefined.otq") + "::set_undefined")

    df = otp.run(data.apply(q))

    assert "X" in df.columns
    assert "Y" in df.columns

    assert df.X_UNDEFINED[0] == "FALSE" and df.Y_UNDEFINED[0] == "FALSE"
    assert df.X_UNDEFINED[1] == "TRUE" and df.Y_UNDEFINED[1] == "FALSE"


def test_date(session):
    original_dates = [datetime.datetime(2010, 10, 12, 20, 4, 3), datetime.datetime(2020, 1, 1)]
    tz = zoneinfo.ZoneInfo("GMT")
    dates = list(map((lambda x: x.replace(tzinfo=tz)), original_dates))

    t = otp.Ticks({"x": dates})

    column = otp.run(t, timezone="GMT").x

    for i in range(2):
        assert str(column[i]) == dates[i].strftime("%Y-%m-%d %H:%M:%S")


def test_time(session):
    data = otp.Ticks(X=[1], Y=[otp.nsectime(0)])

    res = otp.run(data, timezone='GMT')

    assert res['X'][0] == 1
    assert res['Y'][0] == otp.dt(1970, 1, 1)


def test_nsectime(session):
    src = otp.Ticks({
        'X': [otp.msectime(1), otp.msectime(2)],
        'Y': [otp.nsectime(1), otp.nsectime(2)],
    })
    df = otp.run(src, timezone='GMT')
    assert df['X'][0] == otp.dt(1970, 1, 1, 0, 0, 0, 1000)
    assert df['X'][1] == otp.dt(1970, 1, 1, 0, 0, 0, 2000)
    assert df['Y'][0] == otp.dt(1970, 1, 1, 0, 0, 0, 0, 1)
    assert df['Y'][1] == otp.dt(1970, 1, 1, 0, 0, 0, 0, 2)


def test_special_characters(session):
    str_list = ["A", 'A"', "A,", '"A"', "A\\", "A\\\\", "\\A", 'A\\"']
    data = otp.Ticks({"x": str_list})

    df = otp.run(data)

    assert "x" in df.columns

    for i in range(len(str_list)):
        assert df.x[i] == str_list[i]


def test_data_as_kwargs(session):
    """ Check that it is possible to specify data directly
    as key-value pairs"""
    data1 = otp.Ticks(X=[1, 2, 3], Y=['a', 'b', 'c'])
    data2 = otp.Ticks(dict(X=[1, 2, 3], Y=['a', 'b', 'c']))

    assert otp.run(data1).equals(otp.run(data2))


def test_data_as_kwargs_and_as_first_parameter(session):
    """ Check that it is not allowed to specify data as a
    first parameter and as key-value pairs simultaneously """
    with pytest.raises(ValueError, match='Data can be passed only'):
        otp.Ticks(dict(X=[1, 2, 3]), X=[1, 2, 3])


class TestSymbol:
    @pytest.fixture(scope="class")
    def graph(self):
        data = otp.Ticks(X=[1, 2, 3], symbol="SYM")
        data["S"] = data.Symbol.name
        return data

    def test_ticks_arg(self, graph, session):
        # PY-291
        df = otp.run(graph)
        assert all(df["S"] == 3 * ["SYM"])

    def test_ticks_and_symbol_arg(self, graph, session):
        with pytest.raises(Exception, match="Query graph binds all sources to specific symbol names"):
            otp.run(graph, symbols="VALUE")


def test_with_no_data(session):
    with pytest.raises(ValueError, match="You don't specify any date to create ticks from"):
        otp.Ticks()


class TestAbsoluteTickTimes:

    START_TIME = otp.datetime(2020, 1, 1)
    END_TIME = otp.datetime(2020, 2, 1)

    def test_dt_parts(self, session):
        src = otp.Ticks(
            time=[
                otp.datetime(2020, 1, 2, hour=1, minute=2, second=3,
                             microsecond=4005, nanosecond=6, tz='GMT'),
                otp.datetime(2020, 1, 3, hour=7, minute=8, second=9,
                             microsecond=9010, nanosecond=11, tz='EST5EDT'),
            ],
            A=[1, 2])
        df = otp.run(src, start=self.START_TIME, end=self.END_TIME, timezone='GMT')
        assert df['Time'][0] == otp.datetime(2020, 1, 2, hour=1, minute=2, second=3,
                                             microsecond=4005, nanosecond=6)
        assert df['Time'][1] == otp.datetime(2020, 1, 3, hour=7, minute=8, second=9,
                                             microsecond=9010, nanosecond=11) + otp.Hour(5)

    def test_absolute_time_is_date(self, session):
        src = otp.Ticks({
            'time': [otp.date(2020, 1, 2), otp.date(2020, 1, 3)],
            'A': [1, 2],
        })
        df = otp.run(src, start=self.START_TIME, end=self.END_TIME)
        assert df['Time'][0] == otp.datetime(2020, 1, 2)
        assert df['Time'][1] == otp.datetime(2020, 1, 3)

    @pytest.mark.parametrize(
        'set_timezone_in_datetime',
        [True, False]
    )
    @pytest.mark.parametrize(
        'dt_timezone,target_timezone,expected_time_shift',
        [
            ('GMT', 'GMT', otp.Hour(0)),
            ('EST5EDT', 'EST5EDT', otp.Hour(0)),
            ('GMT', 'EST5EDT', otp.Hour(-5)),
            ('EST5EDT', 'GMT', otp.Hour(5)),
            ('America/Chicago', 'Europe/Moscow', otp.Hour(9)),
            ('Europe/Moscow', 'America/Chicago', otp.Hour(-9)),
        ]
    )
    def test_timezones_single_tick(self, session, set_timezone_in_datetime,
                                   dt_timezone, target_timezone, expected_time_shift):
        if set_timezone_in_datetime:
            src = otp.Ticks(time=[otp.datetime(2020, 1, 2, nanosecond=1, tz=dt_timezone)], A=[1])
        else:
            src = otp.Ticks(time=[otp.datetime(2020, 1, 2, nanosecond=1)], timezone_for_time=dt_timezone, A=[1])
        df = otp.run(src, start=self.START_TIME, end=self.END_TIME, timezone=target_timezone)
        assert df['Time'][0] == otp.datetime(2020, 1, 2, nanosecond=1) + expected_time_shift

    @pytest.mark.parametrize(
        'set_timezone_in_datetime',
        [True, False]
    )
    @pytest.mark.parametrize(
        'dt_timezone,target_timezone,expected_time_shift',
        [
            ('GMT', 'GMT', otp.Hour(0)),
            ('EST5EDT', 'EST5EDT', otp.Hour(0)),
            ('GMT', 'EST5EDT', otp.Hour(-5)),
            ('EST5EDT', 'GMT', otp.Hour(5)),
            ('America/Chicago', 'Europe/Moscow', otp.Hour(9)),
            ('Europe/Moscow', 'America/Chicago', otp.Hour(-9)),
        ]
    )
    def test_timezones_multiple_ticks(self, session, set_timezone_in_datetime,
                                      dt_timezone, target_timezone, expected_time_shift):
        if set_timezone_in_datetime:
            src = otp.Ticks(time=[otp.datetime(2020, 1, 2, nanosecond=1, tz=dt_timezone)] * 5, A=[1] * 5)
        else:
            src = otp.Ticks(time=[otp.datetime(2020, 1, 2, nanosecond=1)] * 5, timezone_for_time=dt_timezone, A=[1] * 5)
        df = otp.run(src, start=self.START_TIME, end=self.END_TIME, timezone=target_timezone)
        assert all(df['Time'] == [otp.datetime(2020, 1, 2, nanosecond=1) + expected_time_shift] * 5)

    def test_no_tz(self, session):
        # if no TZ, we expect otp to use default tz
        src = otp.Ticks(time=[otp.datetime(2020, 1, 2, nanosecond=1)] * 2, A=[1] * 2)
        df = otp.run(src, start=self.START_TIME, end=self.END_TIME)
        assert df['Time'][0] == otp.datetime(2020, 1, 2, nanosecond=1)
        assert df['Time'][1] == otp.datetime(2020, 1, 2, nanosecond=1)

    def test_time_and_offset(self, session):
        with pytest.raises(ValueError,
                           match=("It is not allowed to have different columns of different lengths,"
                                  " some of columns have 2 length, but column 'offset', as instance, has 1")):
            otp.Ticks(offset=[1000], datetime=[otp.datetime(2020, 1, 2)] * 2, A=[1] * 2)()

    def test_default_and_non_default_tz(self, session):
        """
        If we only set timezone in some of the ticks' datetimes, it should fall back to the general timezone
        for those datetimes where timezone is not set
        """
        src = otp.Ticks(
            timezone_for_time='America/New_York',
            time=[
                otp.datetime(2020, 1, 2, nanosecond=1, tz='GMT'),
                otp.datetime(2020, 1, 2, nanosecond=2),
                otp.datetime(2020, 1, 2, nanosecond=3, tz='America/New_York'),
                otp.datetime(2020, 1, 2, nanosecond=4, tz='America/Chicago'),
            ],
            A=[1, 2, 3, 4]
        )
        df = otp.run(src, start=self.START_TIME, end=self.END_TIME, timezone='GMT')
        assert df['Time'][0] == otp.datetime(2020, 1, 2, nanosecond=1)
        assert df['Time'][1] == otp.datetime(2020, 1, 2, nanosecond=2) + otp.Hour(5)
        assert df['Time'][2] == otp.datetime(2020, 1, 2, nanosecond=3) + otp.Hour(5)
        assert df['Time'][3] == otp.datetime(2020, 1, 2, nanosecond=4) + otp.Hour(6)


class TestSourcesFromPandasDataFrame:
    def test_from_ticks(self, session):
        a_array = [1, 2, 3]
        t = otp.Ticks(A=a_array)
        df = otp.run(t)
        t1 = otp.Ticks(df)
        df1 = otp.run(t1)
        assert all(df1['A'] == a_array)

    @pytest.mark.parametrize("a_array", [
        [otp.nan],
        [math.nan]
    ])
    def test_nan(self, session, a_array):
        t = otp.Ticks(A=a_array)
        df = otp.run(t)
        t1 = otp.Ticks(df)
        df1 = otp.run(t1)
        assert math.isnan(df1['A'][0])

    def test_not_modified(self, session):
        df = pd.DataFrame({'Time': [pd.Timestamp(1)], 'A': [1]})
        df2 = pd.DataFrame({'Time': [pd.Timestamp(1)], 'A': [1]})
        otp.Ticks(df)
        assert df.equals(df2)

    def test_empty(self, session):
        df = pd.DataFrame()
        with pytest.warns(UserWarning, match='otp.Ticks got empty DataFrame as input'):
            t = otp.Ticks(df)
        df1 = otp.run(t)
        assert df1.empty

    def test_ulong(self, session):
        df = pd.DataFrame({
            'Time': [otp.config.default_start_time],
            'A': [10],
            'B': [2**64 - 10],
        })

        t = otp.Ticks(data=df)
        res = otp.run(t)
        assert res['A'][0] == 10
        assert res['B'][0] == 2**64 - 10


def test_different_offsets(session):
    data = otp.Ticks(
        X=[1, 2],
        offset=[otp.Nano(1), otp.Nano(2)]
    )
    df = otp.run(data)
    assert list(df['X']) == [1, 2]
    assert list(df['Time']) == [otp.config.default_start_time + otp.Nano(1),
                                otp.config.default_start_time + otp.Nano(2)]

    data = otp.Ticks(
        X=[1, 2, 3],
        offset=[otp.Nano(1), otp.Nano(2), otp.Hour(1)]
    )
    df = otp.run(data)
    assert list(df['X']) == [1, 2, 3]
    assert list(df['Time']) == [otp.config.default_start_time + otp.Nano(1),
                                otp.config.default_start_time + otp.Nano(2),
                                otp.config.default_start_time + otp.Hour(1)]


def test_different_offsets_sum(session):
    data = otp.Ticks(dict(X=[1, 2],
                          offset=[otp.Hour(10), otp.Hour(10) + otp.Second(1)]))
    df = otp.run(data, date=otp.dt(2022, 1, 1))
    assert df['Time'][0] == otp.dt(2022, 1, 1, 10)
    assert df['Time'][1] == otp.dt(2022, 1, 1, 10, 0, 1)


def test_single_tick_interface(session):
    # BE-209
    data = {'A': [1, 2], 'symbol': ['a', 'b']}
    t = otp.Ticks(data)
    df = otp.run(t)
    assert list(df['A']) == [1, 2]
    assert list(df['symbol']) == ['a', 'b']

    data = {'A': [1], 'symbol': ['a']}
    t = otp.Ticks(data)
    df = otp.run(t)
    assert list(df['A']) == [1]
    assert list(df['symbol']) == ['a']

    data = {'A': [otp.inf, otp.inf], 'symbol': ['a', 'b']}
    t = otp.Ticks(data)
    df = otp.run(t)
    assert all(np.isinf(df['A']))
    assert list(df['symbol']) == ['a', 'b']


def test_query_params(session):
    t = otp.Ticks(A=['$A1', '$A2'])
    df = otp.run(t, query_params={'A1': 1, 'A2': 2})
    assert df['A'][0] == '1'
    assert df['A'][1] == '2'

    df = otp.run(t)
    assert df['A'][0] == '$A1'
    assert df['A'][1] == '$A2'

    df = otp.run(t, query_params={'A1': 'a', 'A2': 'b'})
    assert df['A'][0] == 'a'
    assert df['A'][1] == 'b'


def test_timedelta_offset(session):
    t = otp.Ticks({'A': [1, 2], 'offset': [otp.timedelta(seconds=1), otp.timedelta(hours=1)]})
    df = otp.run(t)
    assert df['Time'][0] == otp.config.default_start_time + otp.Second(1)
    assert df['Time'][1] == otp.config.default_start_time + otp.Hour(1)


def test_empty_time_interval(session):
    t = otp.Ticks({'A': [1, 2]})
    # start and end time are the same
    df = otp.run(t, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 1))
    assert list(df['Time']) == [otp.dt(2022, 1, 1), otp.dt(2022, 1, 1)]

    t = otp.Ticks({'A': [1, 2, 3]})
    df = otp.run(t, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 1, 0, 0, 0, 1000))
    assert list(df['Time']) == [otp.dt(2022, 1, 1),
                                otp.dt(2022, 1, 1, 0, 0, 0, 1000),
                                otp.dt(2022, 1, 1, 0, 0, 0, 1000)]


class TestOffsetParameter:
    def test_errors(self, session):
        err_msg = "Parameter 'offset' and column 'offset' can't be set at the same time."
        with pytest.raises(ValueError, match=err_msg):
            otp.Ticks({'A': [1, 2, 3], 'offset': [1, 2, 3]}, offset=0)
        with pytest.raises(ValueError, match=err_msg):
            otp.Ticks([['A', 'offset'], [1, 1], [2, 2], [3, 3]], offset=0)
        with pytest.raises(ValueError, match="Parameter 'offset' can't be set when passing pandas.DataFrame."):
            otp.Ticks(pd.DataFrame({'A': [1], 'Time': [pd.Timestamp(2000, 1, 1)]}), offset=0)

    def test_single_offset(self, session):
        t = otp.Ticks(A=[1, 2, 3], offset=5)
        df = otp.run(t)
        assert list(df['Time']) == [otp.config.default_start_time + otp.Milli(5)] * 3
        t = otp.Ticks(A=[1, 2, 3], offset=otp.Nano(11))
        df = otp.run(t)
        assert list(df['Time']) == [otp.config.default_start_time + otp.Nano(11)] * 3
        t = otp.Ticks(A=[1, 2, 3], offset=otp.timedelta(weeks=1))
        df = otp.run(t, start=otp.config.default_start_time, end=otp.config.default_start_time + otp.Day(100))
        assert list(df['Time']) == [otp.config.default_start_time + otp.timedelta(weeks=1)] * 3

    def test_remove_offset(self, session):
        t = otp.Ticks(A=[1, 2, 3], offset=None)
        df = otp.run(t)
        assert list(df['Time']) == [otp.config.default_start_time] * 3
        assert 'offset' not in Path(t.to_otq().split('::')[0]).read_text()
        t = otp.Ticks(A=[1, 2, 3], offset=0)
        df = otp.run(t)
        assert list(df['Time']) == [otp.config.default_start_time] * 3
        assert 'offset' in Path(t.to_otq().split('::')[0]).read_text()
