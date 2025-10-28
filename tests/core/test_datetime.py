import os
import datetime
import random
import zoneinfo
from functools import partial

import dateutil
import pandas as pd
import pytest

import onetick.py as otp
import onetick.py.types as ott


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    yield m_session


class TestTimestamp:
    def test_dst(self):
        d0 = pd.Timestamp("2019/04/12")
        d1 = pd.Timestamp("2019/04/12", tz="EST5EDT")
        d2 = pd.Timestamp("2019/04/12", tz="GMT")

        data = otp.Ticks(dict(x=[d0, d1, d2]))

        df = otp.run(data)

        # EST5EDT in left side, because it converts back by OneTick, ie
        # it refers to 2019/03/12 20:00:00, ie GMT in EST5EDT
        # Right side keeps as is (default is GMT) and needs only to compare
        # because pandas does not allow compare tz-aware timestamp and timestamp
        # without timezone
        assert df["x"][0] == d0
        assert df["x"][1].tz_localize("EST5EDT") == d1
        assert df["x"][2].tz_localize("EST5EDT") == d2

    def test_nanos(self):
        d = pd.Timestamp("2019/01/02 03:04:05.123456789")
        data = otp.Tick(x=d)

        df = otp.run(data, timezone="GMT")

        assert df["x"][0] == d

    # checking both boundaries of the random interval to reduce the probability of random test failures
    @pytest.mark.parametrize('timestamp',
                             [
                                 otp.datetime(1970, 1, 1, tzinfo=dateutil.tz.gettz("EST5EDT")).timestamp(),
                                 otp.datetime(2032, 1, 1, tzinfo=dateutil.tz.gettz("EST5EDT")).timestamp(),
                                 'random'
                             ])
    def test_random_timestamp(self, timestamp):
        if timestamp == 'random':
            timestamp = random.randrange(
                int(otp.datetime(1970, 1, 1, tzinfo=dateutil.tz.gettz("EST5EDT")).timestamp()),
                int(otp.datetime(2032, 1, 1, tzinfo=dateutil.tz.gettz("EST5EDT")).timestamp()),
            )
        # otp generates timestamp with seconds
        timestamp = pd.Timestamp(timestamp, tz="EST5EDT", unit="s")
        data = otp.Ticks(dict(x=[timestamp]))
        df = otp.run(data)
        assert df["x"][0].tz_localize("EST5EDT") == timestamp

    def test_from_source_default(self):
        d = otp.datetime(otp.config['default_start_time'] + otp.Milli(1), tz="GMT")
        assert str(d) == "2003-12-01 00:00:00.001000+00:00"

    @pytest.mark.parametrize("timezone", ["GMT", "EST5EDT"])
    def test_datetime_timezone_aware_plus(self, timezone):
        d = datetime.datetime(2022, 1, 1, 0, 0, 0, tzinfo=dateutil.tz.gettz(timezone))
        d2 = d + otp.Day(1)
        assert d2.tzinfo == dateutil.tz.gettz(timezone)
        assert d2 == datetime.datetime(2022, 1, 2, 0, 0, 0, tzinfo=dateutil.tz.gettz(timezone))


class TestDatetime:
    def test_dst_with_zoneinfo(self):
        d0 = otp.dt(2019, 4, 12, tzinfo=zoneinfo.ZoneInfo("EST5EDT"))
        d1 = otp.dt(2019, 12, 4, tzinfo=zoneinfo.ZoneInfo("EST5EDT"))

        data = otp.Ticks(dict(x=[d0, d1]))

        df = otp.run(data)

        assert otp.datetime(2019, 4, 12).timestamp() == df["x"][0].timestamp()
        assert otp.datetime(2019, 12, 4).timestamp() == df["x"][1].timestamp()

    def test_dst_with_dateutil(self):
        d0 = otp.dt(2019, 4, 12, tzinfo=dateutil.tz.gettz("EST5EDT"))
        d1 = otp.dt(2019, 12, 4, tzinfo=dateutil.tz.gettz("EST5EDT"))

        data = otp.Ticks(dict(x=[d0, d1]))

        df = otp.run(data)

        assert otp.datetime(2019, 4, 12).timestamp() == df["x"][0].timestamp()
        assert otp.datetime(2019, 12, 4).timestamp() == df["x"][1].timestamp()

    def test_high_precision(self):
        d = otp.dt(2019, 4, 12, 1, 2, 3, 456789, tzinfo=dateutil.tz.gettz("GMT"))

        data = otp.Ticks(dict(x=[d]))

        df = otp.run(data, timezone="GMT")

        assert d.timestamp() == df["x"][0].timestamp()

    @pytest.mark.parametrize(
        "timestamp",
        [
            datetime.datetime(2019, 4, 12, tzinfo=zoneinfo.ZoneInfo("EST5EDT")),
            pd.Timestamp("2019/01/02 03:04:05.123456789"),
            otp.datetime(2019, 4, 12, tzinfo=zoneinfo.ZoneInfo("EST5EDT")),
            "2019/01/02 03:04:05.123456789",
            otp.date(2019, 4, 12),
        ],
    )
    def test_create_with_one_arg(self, timestamp):
        otp_datetime = otp.datetime(timestamp)
        timestamp = pd.Timestamp(timestamp) if isinstance(timestamp, str) else timestamp
        assert otp_datetime == timestamp

    @pytest.mark.parametrize("timezone_param", [dict(tz="Europe/Moscow"), dict(tzinfo=zoneinfo.ZoneInfo("GMT"))])
    def test_create_with_wrong_timezone(self, timezone_param):
        d = datetime.datetime(2019, 4, 12, tzinfo=zoneinfo.ZoneInfo("EST5EDT"))
        with pytest.raises(ValueError, match="You've specified the timezone for the object, which already has it"):
            otp.datetime(d, **timezone_param)

    def test_create_with_int_timestamp(self):
        timestamp = pd.Timestamp("2019/01/02 03:04:05.123456789")
        otp_datetime = otp.datetime(timestamp.value)
        assert otp_datetime == timestamp

    def test_create_with_int_timestamp_with_timezones(self):
        timestamp = pd.Timestamp("2019/01/02 03:04:05.123456789", tz="Europe/Paris")
        otp_datetime = otp.datetime(timestamp.value, tz="Europe/Paris")
        assert otp_datetime == timestamp
        timestamp = pd.Timestamp("2019/01/02 03:04:05.123456789", tzinfo=zoneinfo.ZoneInfo("EST5EDT"))
        otp_datetime = otp.datetime(timestamp.value, tzinfo=zoneinfo.ZoneInfo("EST5EDT"))
        assert otp_datetime == timestamp

    def test_create_with_kwargs(self):
        expected = datetime.datetime(1999, 1, 4, tzinfo=zoneinfo.ZoneInfo("GMT"))
        actual = otp.datetime(first_arg=1999, month=1, day=4, tzinfo=zoneinfo.ZoneInfo("GMT"))
        assert actual == expected

    def test_user_specified_both_tz_and_tzinfo(self):
        with pytest.raises(ValueError, match="tzinfo and tz params are mutually exclusive parameters"):
            otp.datetime(first_arg=1999, month=1, day=4, tzinfo=zoneinfo.ZoneInfo("GMT"), tz="tz")

    def test_correct_nanos(self):
        timestamps = []
        for nanos in (0, 100, 999):
            t = otp.dt(1999, 1, 4, 1, 2, 3, 999, nanos, tzinfo=zoneinfo.ZoneInfo("GMT"))
            assert t.nanosecond == nanos
            timestamps.append(t)
        data = otp.Ticks(dict(TIME_VALUE=timestamps))
        df = otp.run(data)
        expected = ("1999-01-03 20:02:03.000999", "1999-01-03 20:02:03.000999100", "1999-01-03 20:02:03.000999999")
        assert all(str(actual) == e for actual, e in zip(df["TIME_VALUE"], expected))

    @pytest.mark.parametrize("nanos", [-1, 1000])
    def test_wrong_nanos(self, nanos):
        with pytest.raises(ValueError, match="Nanosecond parameter should be between 0 and 999"):
            otp.dt(1999, 1, 4, 1, 2, 3, 999, nanos, tzinfo=zoneinfo.ZoneInfo("GMT"))

    @pytest.mark.parametrize("method", ["__add__", "__sub__"])
    def test_operations_with_int_is_not_supported(self, method):
        with pytest.raises(TypeError, match=r"unsupported operand type\(s\)"):
            d0 = otp.dt(2019, 4, 12, 4, 5, 7, tzinfo=dateutil.tz.gettz("GMT"))
            method = getattr(d0, method)
            method(1)

    @pytest.mark.parametrize("tz, expected", [("GMT+0", "+00:00"), ("ETC/GMT-3", "+03:00"), ("GMT+3", "+03:00")])
    def test_with_gmt_timezone(self, tz, expected):
        d = otp.dt(1999, 1, 4, 1, 2, 3, tz=tz)
        d = str(d)
        assert d.startswith("1999-01-04 01:02:03")
        assert d.endswith(expected)

    def test_now(self):
        assert abs(otp.dt.now().timestamp() - pd.Timestamp.now().timestamp()) < 0.5


class TestAssignment:
    def test(self):
        data = otp.Ticks(dict(X=[1]))
        data["T"] = otp.dt(2019, 4, 12, 1, 2, 3, 456789, tzinfo=dateutil.tz.gettz('GMT'))
        df = otp.run(data, timezone='GMT')
        expected = ("2019-04-12 01:02:03.456789", )
        assert all(str(a) == e for a, e in zip(df["T"], expected))


class TestDateAdd:
    def test_add_random_datepart(self):
        date_parts = {otp.Year: 2019, otp.Month: 4, otp.Day: 12, otp.Hour: 8, otp.Minute: 1, otp.Second: 3}
        d0 = otp.dt(
            date_parts[otp.Year],
            date_parts[otp.Month],
            date_parts[otp.Day],
            date_parts[otp.Hour],
            date_parts[otp.Minute],
            date_parts[otp.Second],
            tzinfo=dateutil.tz.gettz("GMT"),
        )

        datepart1, datepart2 = random.sample(tuple(date_parts.keys()), k=2)
        n1 = random.randint(1, 5)
        n2 = random.randint(1, 5)

        data = otp.Ticks(dict(x=[d0]))
        data["y"] = data["x"] + datepart1(n1)
        data["x"] += datepart2(n2)
        df = otp.run(data, timezone="GMT")

        date_parts1 = date_parts.copy()
        date_parts1[datepart1] += n1
        assert (
            otp.datetime(
                date_parts1[otp.Year],
                date_parts1[otp.Month],
                date_parts1[otp.Day],
                date_parts1[otp.Hour],
                date_parts1[otp.Minute],
                date_parts1[otp.Second],
                tzinfo=dateutil.tz.gettz("GMT"),
            ).timestamp()
            == df["y"][0].timestamp()
        )
        assert df["y"][0].timestamp() != d0.timestamp()
        date_parts2 = date_parts.copy()
        date_parts2[datepart2] += n2
        assert (
            otp.datetime(
                date_parts2[otp.Year],
                date_parts2[otp.Month],
                date_parts2[otp.Day],
                date_parts2[otp.Hour],
                date_parts2[otp.Minute],
                date_parts2[otp.Second],
                tzinfo=dateutil.tz.gettz("GMT"),
            ).timestamp()
            == df["x"][0].timestamp()
        )
        assert df["x"][0].timestamp() != d0.timestamp()

    def test_quarter_and_week(self):
        d0 = otp.dt(2019, 4, 12, tzinfo=dateutil.tz.gettz("GMT"))

        data = otp.Ticks(dict(x=[d0]))
        data["y"] = data["x"] + otp.Quarter(1)
        data["z"] = data["x"] + otp.Week(4)
        data["x"] += otp.Quarter(4)
        df = otp.run(data, timezone="GMT")
        assert otp.datetime(2019, 7, 12, tzinfo=dateutil.tz.gettz("GMT")).timestamp() == df["y"][0].timestamp()
        assert otp.datetime(2019, 5, 10, tzinfo=dateutil.tz.gettz("GMT")).timestamp() == df["z"][0].timestamp()
        assert otp.datetime(2020, 4, 12, tzinfo=dateutil.tz.gettz("GMT")).timestamp() == df["x"][0].timestamp()

    def test_synonymous_datepart(self):
        d0 = otp.dt(2019, 4, 12, 1, 15, 18, tzinfo=dateutil.tz.gettz("GMT"))
        d1 = otp.dt(2003, 5, 10, 14, 8, 1, tzinfo=dateutil.tz.gettz("GMT"))
        d2 = otp.dt(2015, 8, 11, 8, 1, 6, tzinfo=dateutil.tz.gettz("GMT"))

        data = otp.Ticks(dict(x=[d0, d1, d2]))
        data["y"] = data["x"] + otp.Year(1)
        data["q"] = data["x"] + otp.Quarter(4)
        data["M"] = data["x"] + otp.Month(12)
        data["w"] = data["x"] + otp.Week(4)
        data["d"] = data["x"] + otp.Day(28)
        data["h"] = data["x"] + otp.Hour(2)
        data["m"] = data["x"] + otp.Minute(120)
        data["s1"] = data["x"] + otp.Second(120 * 60)
        data["s2"] = data["x"] + otp.Second(2)
        data["ms"] = data["x"] + otp.Milli(2 * 1000)
        data["ns"] = data["x"] + otp.Nano(2 * 1000 ** 3)
        data = otp.run(data, timezone="GMT")
        assert all(data["y"] == data["q"])
        assert all(data["y"] == data["M"])
        assert all(data["w"] == data["d"])
        assert all(data["h"] == data["s1"])
        assert all(data["h"] == data["m"])
        assert all(data["ms"] == data["ns"])
        assert all(data["ms"] == data["s2"])

    def test_add_int(self):
        d0 = otp.dt(2019, 4, 12, 4, 5, 7, tzinfo=dateutil.tz.gettz("GMT"))
        data = otp.Ticks(dict(x=[d0]))
        data["x"] += 10000
        df = otp.run(data, timezone="GMT")
        assert (
            otp.datetime(2019, 4, 12, 4, 5, 17, tzinfo=dateutil.tz.gettz("GMT")).timestamp() == df["x"][0].timestamp()
        )

    def test_add_str(self):
        d0 = otp.dt(2019, 4, 12, 4, 5, 7, tzinfo=dateutil.tz.gettz("GMT"))
        data = otp.Ticks(dict(x=[d0]))
        with pytest.raises(TypeError):
            data["x"] += "dst"

    def test_time_nanos(self):
        d = otp.dt(2019, 4, 12, 4, 5, 7, tzinfo=dateutil.tz.gettz("GMT"))
        data = otp.Ticks(dict(x=[1]))

        assert data.Time.dtype is otp.nsectime
        assert data.TIMESTAMP.dtype is otp.nsectime

        data.Time += otp.Nano(1)
        df = otp.run(data, start=d.start, end=d.end, timezone="GMT")

        assert (df["Time"][0]).timestamp() == (d.start + otp.Nano(1)).timestamp()

    def test_negative_value(self):
        d0 = otp.dt(2019, 4, 12, 4, 5, 7, tzinfo=dateutil.tz.gettz("GMT"))
        data = otp.Ticks(dict(x=[d0]))
        data["y"] = data["x"] + otp.Quarter(-1)
        data["z"] = data["x"] + otp.Milli(-1)
        data["x"] += otp.Year(-1)
        df = otp.run(data, timezone="GMT")
        assert otp.datetime(2018, 4, 12, 4, 5, 7, tzinfo=dateutil.tz.gettz("GMT")).timestamp() == df["x"][0].timestamp()
        assert otp.datetime(2019, 1, 12, 4, 5, 7, tzinfo=dateutil.tz.gettz("GMT")).timestamp() == df["y"][0].timestamp()
        assert (
            otp.datetime(2019, 4, 12, 4, 5, 6, 999 * 1000, tzinfo=dateutil.tz.gettz("GMT")).timestamp()
            == df["z"][0].timestamp()
        )

    def test_arithmetic_ops_return_otp_datetime(self):
        d0 = otp.dt(2019, 4, 12, 4, 5, 7, tz="Europe/Minsk")
        d1 = d0 + otp.Hour(2)
        assert d1.start == otp.dt(2019, 4, 12, tz="Europe/Minsk")
        assert d1.end == otp.dt(2019, 4, 13, tz="Europe/Minsk")
        assert isinstance(d1, otp.datetime)

    def test_columns_as_param(self):
        d0 = otp.dt(2014, 2, 1, 3, 55, 14, tzinfo=dateutil.tz.gettz("GMT"))
        data = otp.Ticks(dict(DATE=[d0] * 3, VAL=[1, 0, -1]))
        data["DIFF"] = data["DATE"] + otp.Minute(data["VAL"])
        df = otp.run(data, timezone="GMT")
        expected = ("2014-02-01 03:56:14", "2014-02-01 03:55:14", "2014-02-01 03:54:14")
        assert all(str(actual) == e for actual, e in zip(df["DIFF"], expected))

    def test_expression_as_param(self):
        d0 = otp.dt(2014, 2, 1, 3, 55, 14, tzinfo=dateutil.tz.gettz("GMT"))
        data = otp.Ticks(dict(DATE=[d0] * 3, VAL=[1, 0, -1]))
        data["DIFF"] = data["DATE"] + otp.Minute(data["VAL"] + 1)
        df = otp.run(data, timezone="GMT")
        expected = ("2014-02-01 03:57:14", "2014-02-01 03:56:14", "2014-02-01 03:55:14")
        assert all(str(actual) == e for actual, e in zip(df["DIFF"], expected))

    def test_add_nsectime_after_update(self):
        # BDS-267 related
        offsets = [1, 3, 7]
        delta = otp.Nano(123456789)
        data = otp.Ticks({
            'A': offsets,
            'offset': offsets,
        })
        data['UPDATED_DATETIME'] = data['TIMESTAMP']
        data['UPDATED_DATETIME'] += delta
        df = otp.run(data)
        for i, offset in enumerate(offsets):
            assert df['UPDATED_DATETIME'][i] == pd.Timestamp(
                otp.config['default_start_time'] + otp.Milli(offset) + delta
            )


class TestDateDiff:
    def test(self):
        d0 = otp.dt(2019, 4, 12, 4, 5, 7, tzinfo=dateutil.tz.gettz("GMT"))
        data = otp.Ticks(dict(x=[d0]))
        data["y"] = data["x"] - otp.Quarter(1)
        data["z"] = data["x"] - otp.Milli(1)
        data["t"] = d0 - otp.Nano(1)
        data["x"] -= otp.Year(1)
        df = otp.run(data, timezone="GMT")
        assert otp.datetime(2018, 4, 12, 4, 5, 7, tzinfo=dateutil.tz.gettz("GMT")).timestamp() == df["x"][0].timestamp()
        assert otp.datetime(2019, 1, 12, 4, 5, 7, tzinfo=dateutil.tz.gettz("GMT")).timestamp() == df["y"][0].timestamp()
        assert (
            otp.datetime(2019, 4, 12, 4, 5, 6, 999 * 1000, tzinfo=dateutil.tz.gettz("GMT")).timestamp()
            == df["z"][0].timestamp()
        )
        assert (d0 - otp.Nano(1)).timestamp() == df["t"][0].timestamp()

    def test_double_negative(self):
        d0 = otp.dt(2014, 2, 1, 3, 55, 14, tzinfo=dateutil.tz.gettz("GMT"))
        data = otp.Ticks(dict(x=[d0]))
        data["y"] = data["x"] - otp.Minute(-1)
        data["x"] -= otp.Month(-1)
        df = otp.run(data, timezone="GMT")
        assert (
            otp.datetime(2014, 2, 1, 3, 56, 14, tzinfo=dateutil.tz.gettz("GMT")).timestamp() == df["y"][0].timestamp()
        )
        assert (
            otp.datetime(2014, 3, 1, 3, 55, 14, tzinfo=dateutil.tz.gettz("GMT")).timestamp() == df["x"][0].timestamp()
        )

    def test_zero_n(self):
        d0 = otp.dt(2014, 2, 1, 3, 55, 14, tzinfo=dateutil.tz.gettz("GMT"))
        data = otp.Ticks(dict(x=[d0]))
        data["y"] = data["x"] - otp.Minute(0)
        data["x"] += otp.Month(0)
        df = otp.run(data, timezone="GMT")
        assert (
            otp.datetime(2014, 2, 1, 3, 55, 14, tzinfo=dateutil.tz.gettz("GMT")).timestamp() == df["y"][0].timestamp()
        )
        assert (
            otp.datetime(2014, 2, 1, 3, 55, 14, tzinfo=dateutil.tz.gettz("GMT")).timestamp() == df["x"][0].timestamp()
        )

    def test_arithmetic_ops_return_otp_datetime(self):
        d0 = otp.dt(2019, 4, 12, 4, 5, 7)
        d1 = d0 - otp.Minute(2)
        assert d1.start == otp.dt(2019, 4, 12)
        assert d1.end == otp.dt(2019, 4, 13)
        assert isinstance(d1, otp.datetime)

    @pytest.mark.parametrize(
        "other",
        [otp.dt(2019, 4, 12, 4, 5, 7), datetime.datetime(2019, 4, 12, 4, 5, 7), pd.Timestamp(2019, 4, 12, 4, 5, 7)],
    )
    def test_minus_date(self, other):
        d0 = otp.dt(2019, 4, 12, 4, 5, 7)
        d1 = d0 - other
        assert str(d1) == "0 days 00:00:00"

    def test_columns_as_param(self):
        d0 = otp.dt(2014, 2, 1, 3, 55, 14, tzinfo=dateutil.tz.gettz("GMT"))
        data = otp.Ticks(dict(DATE=[d0] * 3, VAL=[1, 0, -1]))
        data["DIFF"] = data["DATE"] - otp.Hour(data["VAL"])
        df = otp.run(data, timezone="GMT")
        expected = ("2014-02-01 02:55:14", "2014-02-01 03:55:14", "2014-02-01 04:55:14")
        assert all(str(actual) == e for actual, e in zip(df["DIFF"], expected))

    def test_columns_int_expression(self):
        d0 = otp.dt(2014, 2, 1, 3, 55, 14, tzinfo=dateutil.tz.gettz("GMT"))
        data = otp.Ticks(dict(DATE=[d0] * 3, VAL=[1, 0, -1], VAL2=[1, 2, -2]))
        data["VAL3"] = data["VAL"] - data["VAL2"]
        data["DIFF1"] = data["DATE"] - otp.Hour(data["VAL"] - data["VAL2"])
        data["DIFF2"] = data["DATE"] - otp.Hour(data["VAL3"])
        df = otp.run(data, timezone="GMT")
        expected = ("2014-02-01 03:55:14", "2014-02-01 05:55:14", "2014-02-01 02:55:14")
        assert all(str(actual) == e for actual, e in zip(df["DIFF1"], expected))
        assert all(df["DIFF1"] == df["DIFF2"])

    def test_columns_expression_with_dates(self):
        dt = partial(otp.dt, 2014, 2, 1, tzinfo=dateutil.tz.gettz("GMT"))
        d = dt(3)
        data = otp.Ticks(dict(DATE=[d] * 2, DATE1=[dt(4), dt(5)], DATE2=[dt(5), dt(4)]))
        data["DATE"] += otp.Hour(data["DATE2"] - data["DATE1"])
        df = otp.run(data, timezone="GMT")
        expected = ("2014-02-01 04:00:00", "2014-02-01 02:00:00")
        assert all(str(actual) == e for actual, e in zip(df["DATE"], expected))

    def test_complex_columns_expression_with_dates_and_ints(self):
        dt = partial(otp.dt, 2014, 2, 1, tzinfo=dateutil.tz.gettz("GMT"))
        d = dt(3)
        data = otp.Ticks(dict(DATE=[d] * 2, DATE1=[dt(4), dt(5)], DATE2=[dt(5), dt(4)], VAL=[1, 2]))
        with pytest.raises(ValueError, match="Date arithmetic operations (.*) are not accepted"):
            with pytest.warns(FutureWarning):
                data["DATE"] += otp.Hour(data["DATE2"] - data["DATE1"] + 1)

    def test_ops(self):
        t = otp.Tick(A=otp.datetime(2022, 1, 1), B=otp.datetime(2023, 1, 1))
        t['DY'] = otp.Year(t['B'] - t['A']) + 0 - 0
        t['DM'] = otp.Month(t['B'] - t['A']).map({12: 12})
        t['DD'] = otp.Day(t['B'] - t['A']).apply(lambda x: x + 0)
        t['DH'] = otp.Hour(t['B'] - t['A']).astype(str)
        t['DMIN'] = otp.Minute(t['B'] - t['A'])
        t['DS'] = otp.Second(t['B'] - t['A'])
        df = otp.run(t)
        assert df['DY'][0] == 1
        assert df['DM'][0] == 12
        assert df['DD'][0] == 365
        assert df['DH'][0] == str(365 * 24)
        assert df['DMIN'][0] == 365 * 24 * 60
        assert df['DS'][0] == 365 * 24 * 60 * 60


class TestDatePartDiff:
    def test_hour(self):
        d0 = otp.dt(2019, 4, 12, 16, 15, 18, tzinfo=dateutil.tz.gettz("GMT"))
        d1 = otp.dt(2019, 4, 12, 1, 15, 18, tzinfo=dateutil.tz.gettz("GMT"))

        data = otp.Ticks(dict(x=[d0], y=[d1]))
        data["d"] = otp.Hour(data["x"] - data["y"])
        df = otp.run(data)
        assert all(df["d"] == [15])

    def test_year(self):
        d0 = otp.dt(2019, 4, 12, 16, 15, 18, tzinfo=dateutil.tz.gettz("GMT"))
        d1 = otp.dt(2014, 5, 6, 10, 17, 11, tzinfo=dateutil.tz.gettz("GMT"))

        data = otp.Ticks(dict(x=[d0], y=[d1]))
        data["d"] = otp.Year(data["x"] - data["y"])
        df = otp.run(data)
        assert all(df["d"] == [4])

    def test_nano_and_zero(self):
        d0 = otp.dt(2019, 4, 12, 16, 15, 18, tzinfo=dateutil.tz.gettz("GMT"))
        d1 = otp.dt(2019, 4, 12, 16, 15, 18, tzinfo=dateutil.tz.gettz("GMT"))

        data = otp.Ticks(dict(x=[d0], y=[d1]))
        data["d"] = otp.Nano(data["x"] - data["y"])
        df = otp.run(data)
        assert all(df["d"] == [0])


class TestOffset:
    def test_ticks(self):
        data = otp.Ticks(X=[1, 2], Y=['A', 'B'], offset=[otp.Nano(1), otp.Hour(2)])
        otp.run(data)

    def test_tick(self):
        """ Implementation for a single tick or ticks with only one row differs from multiple """
        data = otp.Ticks(X=[1], Y=['A'], offset=[otp.Nano(1)])
        otp.run(data)


class TestNextDay:
    @pytest.mark.parametrize('dt', [
        otp.datetime(2022, 1, 1, 1, 2, 3),
        otp.date(2022, 1, 1),
        datetime.datetime(2022, 1, 1, 1, 2, 3),
        datetime.date(2022, 1, 1),
        pd.Timestamp(2022, 1, 1, 1, 2, 3),
        otp.datetime(2022, 1, 1, 1, 2, 3, tz='America/New_York'),
        datetime.datetime(2022, 1, 1, 1, 2, 3, tzinfo=zoneinfo.ZoneInfo('America/New_York')),
        pd.Timestamp(2022, 1, 1, 1, 2, 3).replace(tzinfo=zoneinfo.ZoneInfo('America/New_York')),
    ])
    def test_next_day(self, dt):
        assert ott.next_day(dt) == datetime.datetime(2022, 1, 2)

    def test_next_day_timezone_transition(self):
        # summer to winter
        dt = otp.datetime("2023-11-05 10:00:00", tz='America/New_York')
        assert dt.end == otp.datetime(2023, 11, 6, tz='America/New_York')

        # winter to summer
        dt = otp.datetime("2023-03-12 10:00:00", tz='America/New_York')
        assert dt.end == otp.datetime(2023, 3, 13, tz='America/New_York')


class TestDatetimeTimezoneAwareness:
    @pytest.fixture
    def otp_config_tz(self, request, monkeypatch):
        tz = getattr(request, 'param', None)
        if tz:
            monkeypatch.setattr(otp.config, 'tz', tz)
        return tz

    @pytest.fixture
    def dates(self):
        return [
            otp.datetime(2011, 1, 1, 1, 1, 1),
            otp.date(2011, 3, 3),
            datetime.datetime(2011, 1, 1, 1, 1, 1),
            datetime.date(2011, 3, 3),
            pd.Timestamp(2011, 1, 1, 1, 1, 1),
        ]

    @pytest.fixture
    def date_columns(self, dates):
        return {
            f'T{i}': dt for i, dt in enumerate(dates)
        }

    @pytest.mark.parametrize('otp_config_tz', (None, 'Europe/Moscow'))
    def test_ticks_columns(self, otp_config_tz, dates):
        dates = list(dates)
        data = otp.Ticks(T=dates)
        if otp_config_tz:
            df = otp.run(data)
        else:
            df = otp.run(data, timezone='Europe/Moscow')
        for i, value in enumerate(dates):
            assert df['T'][i] == (pd.Timestamp(value) if isinstance(value, datetime.date) else value)

    @pytest.mark.parametrize('otp_config_tz', (None, 'Europe/Moscow'))
    def test_tick_columns(self, otp_config_tz, date_columns):
        data = otp.Tick(**date_columns)
        if otp_config_tz:
            df = otp.run(data)
        else:
            df = otp.run(data, timezone='Europe/Moscow')
        for column, value in date_columns.items():
            assert df[column][0] == (pd.Timestamp(value) if isinstance(value, datetime.date) else value)

    @pytest.mark.parametrize('otp_config_tz', (None, 'Europe/Moscow'))
    def test_add_column(self, otp_config_tz, date_columns):
        data = otp.Tick(A=1)
        for column, value in date_columns.items():
            data[column] = value
        if otp_config_tz:
            df = otp.run(data)
        else:
            df = otp.run(data, timezone='Europe/Moscow')
        for column, value in date_columns.items():
            assert df[column][0] == (pd.Timestamp(value) if isinstance(value, datetime.date) else value)

    def test_timezone_aware_datetime(self):
        date_columns = {
            'T1': otp.datetime(2011, 1, 1, 1, 1, 1, tz='Europe/Moscow'),
            'T2': datetime.datetime(2011, 1, 1, 1, 1, 1, tzinfo=zoneinfo.ZoneInfo('Europe/Moscow')),
            'T3': pd.Timestamp(2011, 1, 1, 1, 1, 1).tz_localize('Europe/Moscow'),
        }
        data = otp.Tick(**date_columns)
        df = otp.run(data, timezone='GMT')
        for column, value in date_columns.items():
            assert df[column][0].tz_localize('GMT') == \
                   pd.Timestamp(2011, 1, 1, 1, 1, 1).tz_localize('Europe/Moscow').tz_convert('GMT')


class TestLocalTimezone:
    @pytest.fixture
    def change_local_timezone(self, tz):
        old_tz = os.environ.get('TZ')
        os.environ['TZ'] = tz
        yield tz
        if old_tz:
            os.environ['TZ'] = old_tz
        else:
            del os.environ['TZ']

    @pytest.mark.skipif(os.name == 'nt', reason='Changing timezone via TZ is not supported on windows')
    @pytest.mark.parametrize('tz', ['US/Eastern', 'UTC'])
    def test_dst(self, change_local_timezone, tz):
        t = otp.Tick(A=1)
        t['A'] = otp.datetime(2022, 11, 5)
        # DST change day for New York is November 6th
        t['B'] = t['A'] + otp.Day(2)
        df = otp.run(t, timezone='EST5EDT')
        assert df['B'][0] == otp.datetime(2022, 11, 7)

    def test_ops(self):
        t = otp.Tick(X=1)
        t['A'] = otp.datetime(2022, 11, 5)
        t['B'] = '2022/11/05 00:00:00.000000000'
        dateadd = t['A'] + otp.Day(2)
        datediff = t['A'] + otp.Day(t['A'] - t['A'])  # NOSONAR
        day_of_week = t['A'].dt.day_of_week()
        nsectime_format = t['A'].dt.strftime()
        parse_nsectime = t['B'].str.to_datetime()
        assert '_TIMEZONE' in str(dateadd)
        assert str(datediff).count('_TIMEZONE') == 2
        assert '_TIMEZONE' in str(day_of_week)
        assert '_TIMEZONE' in str(nsectime_format)
        assert '_TIMEZONE' in str(parse_nsectime)

    def test_with_join(self):
        ta = otp.Tick(A=1, D=otp.datetime(2022, 11, 5))
        tb = otp.Tick(B=2, S='2022/11/05 00:00:00.000000000')
        t_timezone = otp.join(ta, tb, on=ta['_TIMEZONE'] == tb['_TIMEZONE'])
        t_dateadd = otp.join(ta, tb, on=ta['TIMESTAMP'] + otp.Milli(1) > tb['TIMESTAMP'])
        t_datediff = otp.join(ta, tb, on=ta['D'] + otp.Day(ta['D'] - ta['D']) != tb['TIMESTAMP'])  # NOSONAR
        t_day_of_week = otp.join(ta, tb, on=ta['D'].dt.day_of_week() != tb['B'] + 100)
        t_nsectime_format = otp.join(ta, tb, on=ta['D'].dt.strftime() == tb['S'])
        t_parse_nsectime = otp.join(ta, tb, on=tb['S'].str.to_datetime() == ta['D'])
        for src in (
            t_timezone,
            t_dateadd,
            t_datediff,
            t_day_of_week,
            t_nsectime_format,
            t_parse_nsectime,
        ):
            df = otp.run(src)
            assert set(src.schema) == {'A', 'B', 'D', 'S'}
            assert set(df.columns) == {'Time', 'A', 'B', 'D', 'S'}
            assert len(df) == 1
            assert df['A'][0] == 1
            assert df['B'][0] == 2
