import datetime

import pytest
import pandas as pd
import onetick.py as otp


@pytest.mark.parametrize("arg", [datetime.datetime(2019, 1, 1), datetime.date(2019, 1, 1),
                                 "20190101", "2019-01-01T17:30", "2019-01-01 17:30",
                                 pd.Timestamp(2019, 1, 1), pd.Timestamp(2019, 1, 1, 10, 10, 10),
                                 pd.Timestamp(2019, 1, 1, 10, 10, 10).value,
                                 otp.datetime(2019, 1, 1), otp.date(2019, 1, 1)])
def test_init(arg):
    date = otp.date(arg)
    assert date == pd.Timestamp("2019-01-01 00:00:00")


def test_start_end():
    date = otp.date("20190303")
    assert date.start == pd.Timestamp("2019-03-03 00:00:00")
    assert date.end == pd.Timestamp("2019-03-04 00:00:00")


def test_wrong_arg():
    with pytest.raises(ValueError, match=r"Please specify three integers \(year, month, day\) "
                                         "or object or create date from"):
        otp.date(pd.Timestamp(2019, 1, 1), month=3)


def test_cmp_ne():
    d1 = otp.date(2019, 1, 1)
    d2 = otp.date(2019, 1, 2)
    assert d1 < d2
    assert d1 <= d2
    assert not (d1 > d2)  # NOSONAR
    assert not (d1 >= d2)  # NOSONAR
    assert not (d1 == d2)  # NOSONAR
    assert d1 != d2


def test_cmp_eq():
    d1 = otp.date(2019, 1, 1)
    d2 = otp.date(otp.datetime(2019, 1, 1, 17, tz="Europe/Berlin"))
    assert not (d1 < d2)  # NOSONAR
    assert d1 <= d2
    assert not (d1 > d2)  # NOSONAR
    assert d1 >= d2
    assert d1 == d2
    assert not (d1 != d2)  # NOSONAR


@pytest.mark.parametrize("other", [datetime.date(2019, 1, 1), otp.date(2019, 1, 1), otp.datetime(2019, 1, 1),
                                   pd.Timestamp(2019, 1, 1)])
def test_hash_and_eq(other):
    d1 = otp.date(2019, 1, 1)
    assert d1 == (pd.Timestamp(other) if isinstance(other, datetime.date) else other)
    if isinstance(other, (otp.datetime, otp.date, pd.Timestamp)):
        assert hash(d1) == hash(other)


def test_str_and_repr():
    d = otp.date(otp.datetime(2019, 1, 1, 17))
    assert str(d) == "2019-01-01"
    assert repr(d) == "2019-01-01"


def test_add_sub():
    assert otp.date(2022, 1, 1) + otp.Day(3) == otp.datetime(2022, 1, 4)
    assert otp.date(2022, 1, 1) + otp.Second(3) == otp.datetime(2022, 1, 1, 0, 0, 3)
    assert otp.date(2022, 1, 1) - otp.Year(2) == otp.datetime(2020, 1, 1)
    assert otp.date(2022, 1, 1) - otp.Month(1) == otp.datetime(2021, 12, 1)
    assert otp.date(2022, 1, 2) - otp.date(2022, 1, 1) == pd.Timedelta(days=1)


def test_add_column(session):
    dt = otp.date(2022, 1, 1)
    t = otp.Tick(A=1)
    t['T'] = dt
    df = otp.run(t, timezone='GMT')
    assert df['T'][0] == dt
