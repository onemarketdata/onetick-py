import zoneinfo

import pytest
import pandas as pd

import onetick.py as otp


class TestCommon:
    def test_access(self):
        """ only datetime type columns have dt accessor"""
        data = otp.Ticks(dict(x=[otp.datetime(2015, 12, 1, 1)]))
        assert data.x.dtype is otp.nsectime
        data.x.dt

        data = otp.Ticks(dict(x=[otp.msectime(0), 1]))
        assert data.x.dtype is otp.msectime
        data.x.dt

        data = otp.Ticks(dict(x=[1, 2]))
        assert data.x.dtype is int
        with pytest.raises(TypeError, match="dt accessor is available only for datetime type columns"):
            data.x.dt

        data = otp.Ticks(dict(x=["a", "b"]))
        assert data.x.dtype is str
        with pytest.raises(TypeError, match="dt accessor is available only for datetime type columns"):
            data.x.dt

        data = otp.Ticks(dict(x=[1.2, 3.4]))
        assert data.x.dtype is float
        with pytest.raises(TypeError, match="dt accessor is available only for datetime type columns"):
            data.x.dt


class TestToStr:
    def test_default(self, m_session):
        dates = [otp.datetime(2015, 12, 1, 1, 45, 34)]

        data = otp.Ticks(dict(x=dates))

        data["y"] = data["x"].dt.strftime()
        assert data["y"].dtype is str

        df = otp.run(data)

        assert df["y"][0] == "2015/12/01 01:45:34.000000000"

    def test_nanoseconds(self, m_session):
        data = otp.Ticks(dict(x=["2005/12/01 01:45:34.123456789"]))

        data["dt_x"] = data["x"].str.to_datetime("%Y/%m/%d %H:%M:%S.%f")
        data["str_x"] = data["dt_x"].dt.strftime()

        df = otp.run(data)

        assert df["str_x"][0] == df["x"][0]

    # @pytest.mark.skip("PY-416, this bug is not resolved yet.")
    def test_str_to_datetime_bug(self, m_session):
        data = otp.Ticks(X=["5/17/22-11:10:56.123456789"])
        data['Y'] = data['X'].str.to_datetime("%m/%d/%y-%H:%M:%S.%J", 'Europe/London')
        data['X'] = data['X'].str.to_datetime("%m/%d/%y-%H:%M:%S.%J", 'Europe/London')
        df = otp.run(data)
        assert df['Y'][0] == df['X'][0]

    def test_str_year_month_day(self, m_session):
        dates = [otp.datetime(2015, 12, 1, 1, 45, 34)]

        data = otp.Ticks(dict(x=dates))

        data["year"] = data["x"].dt.strftime("%Y")
        assert data["year"].dtype is str
        data["month"] = data["x"].dt.strftime("%m")
        assert data["month"].dtype is str
        data["day"] = data["x"].dt.strftime("%d")
        assert data["day"].dtype is str

        df = otp.run(data)

        assert df["year"][0] == "2015"
        assert df["month"][0] == "12"
        assert df["day"][0] == "01"

    def test_separator(self, m_session):
        dates = [otp.datetime(2015, 12, 1, 1, 45, 34)]

        data = otp.Ticks(dict(x=dates))

        data["str_date"] = data["x"].dt.strftime("%Y-%m-%d")

        df = otp.run(data)

        assert df["str_date"][0] == "2015-12-01"

    def test_timezone(self, m_session):
        dates = [otp.datetime(2015, 12, 1, 1, 45, 34, tzinfo=zoneinfo.ZoneInfo('GMT'))]

        data = otp.Ticks(dict(x=dates))

        data["str_date"] = data["x"].dt.strftime("%d %H:%M:%S", "EST5EDT")

        df = otp.run(data, timezone='GMT')

        assert df["str_date"][0] == "30 20:45:34"

    def test_zero(self, m_session):
        data = otp.Ticks(dict(x=[otp.nsectime(0)]))
        assert data.x.dtype is otp.nsectime

        data["str_date"] = data["x"].dt.strftime("%Y-%m-%d %H:%M:%S.%J", 'GMT')

        df = otp.run(data, timezone='GMT')

        assert df["str_date"][0] == "1970-01-01 00:00:00.000000000"


class TestDate:
    def test_timezone(self, m_session):
        data = otp.Ticks(T=[otp.nsectime(0), otp.dt(2011, 1, 2, 1, 1, 1),
                            otp.dt(2011, 2, 2, 2, 2, 2), otp.date(2011, 3, 3)])
        data["T"] = data["T"].dt.date()
        df = otp.run(data, timezone="America/Lima")
        # otp.nsectime is timezone-naive, others are not
        assert all(df["T"] == [otp.date(1969, 12, 31), otp.date(2011, 1, 2),
                               otp.date(2011, 2, 2), otp.datetime(2011, 3, 3, 0, 0, 0)])

    def test_update_column_with_datetime(self, m_session):
        # test for https://onemarketdata.atlassian.net/browse/BDS-267
        data = otp.Ticks({
            'UPDATED_LONG_FIELD': [123456789],
            'UPDATED_DOUBLE_FIELD': [1234.56789],
            'UPDATED_STRING_FIELD': ['123456789'],
        })
        # creating new field works as expected
        data['NEW_FIELD'] = data['TIMESTAMP'].dt.date()
        # in OneTick after updating fields with functions that return datetime values
        # the type of column will not change for long and double columns
        # and will change to long (or double in older versions) when updating string column
        # (see BDS-267)
        # This behavior should be fixed in onetick-py
        data['UPDATED_LONG_FIELD'] = data['TIMESTAMP'].dt.date()
        data['UPDATED_DOUBLE_FIELD'] = data['TIMESTAMP'].dt.date()
        data['UPDATED_STRING_FIELD'] = data['TIMESTAMP'].dt.date()
        df = otp.run(data)
        assert df['NEW_FIELD'][0] == pd.Timestamp(otp.config['default_start_time'])
        assert df['UPDATED_LONG_FIELD'][0] == pd.Timestamp(otp.config['default_start_time'])
        assert df['UPDATED_DOUBLE_FIELD'][0] == pd.Timestamp(otp.config['default_start_time'])
        assert df['UPDATED_STRING_FIELD'][0] == pd.Timestamp(otp.config['default_start_time'])

    def test_update_column_with_datetime_and_convert_to_param(self, m_session):
        # test for https://onemarketdata.atlassian.net/browse/RCMBESTEX-233
        data = otp.Ticks({
            'UPDATED_LONG_FIELD': [123456789],
        })
        data['UPDATED_LONG_FIELD'] = data['TIMESTAMP'].dt.date()
        data = data.join_with_query(query=lambda param: otp.Tick(A=1),
                                    start_time=data['UPDATED_LONG_FIELD'],
                                    end_time=data['UPDATED_LONG_FIELD'],
                                    # converting column to param
                                    params={'param': data['UPDATED_LONG_FIELD']})
        df = otp.run(data)
        assert df['UPDATED_LONG_FIELD'][0] == pd.Timestamp(otp.config['default_start_time'])
        assert df['A'][0] == 1


class TestDayOfWeek:

    def test_default(self, m_session):
        days = [otp.dt(2021, 11, i, 1, 1, 1, tz='Europe/Moscow') for i in range(8, 22)]
        data = otp.Ticks(T=days)
        data['DAY'] = data['T'].dt.day_of_week()
        df = otp.run(data, timezone='Europe/Moscow')
        for i in range(len(df)):
            assert df['DAY'][i] == i % 7 + 1

    def test_start_index(self, m_session):
        days = [otp.dt(2021, 11, i, 1, 1, 1, tz='Europe/Moscow') for i in range(8, 22)]
        data = otp.Ticks(T=days)
        data['DAY'] = data['T'].dt.day_of_week(start_index=10)
        df = otp.run(data, timezone='Europe/Moscow')
        for i in range(len(df)):
            assert df['DAY'][i] == i % 7 + 10

    def test_start_day(self, m_session):
        days = [otp.dt(2021, 11, i, 1, 1, 1, tz='Europe/Moscow') for i in range(7, 21)]
        data = otp.Ticks(T=days)
        data['DAY'] = data['T'].dt.day_of_week(start_day='sunday')
        df = otp.run(data, timezone='Europe/Moscow')
        for i in range(len(df)):
            assert df['DAY'][i] == i % 7 + 1

    def test_wrong_start_day(self):
        data = otp.Ticks(T=[otp.dt(2021, 11, 1, 1, 1, 1)])
        with pytest.raises(ValueError, match='not in'):
            data['T'].dt.day_of_week(start_day='tuesday')

    def test_timezone(self, m_session):
        data = otp.Ticks(T=[otp.dt(2021, 11, 9, 1, 1, 1, tz='Europe/Moscow')])
        data['DAY'] = data['T'].dt.day_of_week(timezone='GMT')
        df = otp.run(data, timezone='Europe/Moscow')
        assert df['DAY'][0] == 1


class TestDateTrunc:
    def test_all(self, m_session):
        data = otp.Ticks(X=[otp.dt(2020, 11, 11, 5, 4, 13, 101737, 879)] * 10,
                         DATE_PART=['year', 'quarter', 'month', 'week', 'day', 'hour',
                                    'minute', 'second', 'millisecond', 'nanosecond'])
        data['TRUNCATED_X'] = data['X'].dt.date_trunc(data['DATE_PART'])
        df = otp.run(data)
        assert all(df['X'] == otp.datetime(2020, 11, 11, 5, 4, 13, 101737, 879).ts)
        df = df[['TRUNCATED_X', 'DATE_PART']]
        assert dict(df.iloc[0]) == {'TRUNCATED_X': pd.Timestamp(2020, 1, 1), 'DATE_PART': 'year'}
        if otp.compatibility.is_date_trunc_fixed():
            assert dict(df.iloc[1]) == {'TRUNCATED_X': pd.Timestamp(2020, 10, 1), 'DATE_PART': 'quarter'}
            assert dict(df.iloc[2]) == {'TRUNCATED_X': pd.Timestamp(2020, 11, 1), 'DATE_PART': 'month'}
            assert dict(df.iloc[3]) == {'TRUNCATED_X': pd.Timestamp(2020, 11, 9), 'DATE_PART': 'week'}
        else:
            assert dict(df.iloc[1]) == {'TRUNCATED_X': pd.Timestamp(2020, 10, 1, 1), 'DATE_PART': 'quarter'}
            assert dict(df.iloc[2]) == {'TRUNCATED_X': pd.Timestamp(2020, 11, 1, 1), 'DATE_PART': 'month'}
            assert dict(df.iloc[3]) == {'TRUNCATED_X': pd.Timestamp(2020, 11, 8), 'DATE_PART': 'week'}
        assert dict(df.iloc[4]) == {'TRUNCATED_X': pd.Timestamp(2020, 11, 11), 'DATE_PART': 'day'}
        assert dict(df.iloc[5]) == {'TRUNCATED_X': pd.Timestamp(2020, 11, 11, 5), 'DATE_PART': 'hour'}
        assert dict(df.iloc[6]) == {'TRUNCATED_X': pd.Timestamp(2020, 11, 11, 5, 4), 'DATE_PART': 'minute'}
        assert dict(df.iloc[7]) == {'TRUNCATED_X': pd.Timestamp(2020, 11, 11, 5, 4, 13), 'DATE_PART': 'second'}
        assert dict(df.iloc[8]) == {'TRUNCATED_X': pd.Timestamp(2020, 11, 11, 5, 4, 13, 101000),
                                    'DATE_PART': 'millisecond'}
        assert dict(df.iloc[9]) == {'TRUNCATED_X': otp.datetime(2020, 11, 11, 5, 4, 13, 101737, 879).ts,
                                    'DATE_PART': 'nanosecond'}

    def test_dst_year(self, m_session):
        # Notice that there is bug when 1 hour before new year is returned for case ``date_part='year'``
        # if there was daylight saving time:
        data = otp.Tick(X=otp.dt(2020, 5, 11, 5, 4, 13, 101737, 879))
        data['TRUNCATED_X'] = data['X'].dt.date_trunc('year')
        df = otp.run(data)
        if otp.compatibility.is_date_trunc_fixed():
            assert df['TRUNCATED_X'][0] == pd.Timestamp(2020, 1, 1)
        else:
            assert df['TRUNCATED_X'][0] == pd.Timestamp(2019, 12, 31, 23)


def test_replace_parameters(m_session):
    from onetick.py.functions import _add_node_name_prefix_to_columns_in_operation
    t = otp.Tick(AA=otp.datetime(2025, 12, 1, 1), BB='%d.%m.%Y', CC='UTC', DD=1, EE='hour')
    t.node_name('PREFIX')

    ops = [
        t['AA'].dt.strftime(t['BB'], t['CC']),
        t['AA'].dt.date(),
        t['AA'].dt.day_of_week(t['DD'], 'monday', t['CC']),
        t['AA'].dt.day_name(t['CC']),
        t['AA'].dt.day_of_month(t['CC']),
        t['AA'].dt.day_of_year(t['CC']),
        t['AA'].dt.hour(t['CC']),
        t['AA'].dt.minute(t['CC']),
        t['AA'].dt.second(t['CC']),
        t['AA'].dt.month(t['CC']),
        t['AA'].dt.month_name(t['CC']),
        t['AA'].dt.quarter(t['CC']),
        t['AA'].dt.year(t['CC']),
        t['AA'].dt.date_trunc(t['EE'], t['CC']),
        t['AA'].dt.year(t['CC']),
    ]

    for op in ops:
        str_op = str(op)
        str_replaced_op = str_op[:]
        for column in t.schema:
            str_replaced_op = str_replaced_op.replace(column, f'PREFIX.{column}')
        str_node_name_prefix_op = str(_add_node_name_prefix_to_columns_in_operation(op, t))
        assert str_replaced_op == str_node_name_prefix_op
