import sys
from datetime import datetime
import pytest
import os
import onetick.py as otp
import pandas as pd

from onetick.py.sources import LocalCSVTicks


def test_default(m_session, cur_dir):
    path = os.path.join(cur_dir, 'data', 'test.csv')
    ticks = LocalCSVTicks(path)
    df = otp.run(ticks, start=datetime(2021, 4, 13), end=datetime(2021, 4, 15))
    assert all(df['SIZE'] == 2)
    assert all(df['PRICE'] == [
        pytest.approx(1.195620),
        pytest.approx(1.195640),
        pytest.approx(1.195630)
    ])
    assert all(df['SYMBOL_NAME'] == "NY.CNX.EURUSD")
    assert all(df['Time'] == [
        otp.dt(2021, 4, 14, 0, 3, 12, 52924, 123),
        otp.dt(2021, 4, 14, 0, 3, 43, 61975, 432),
        otp.dt(2021, 4, 14, 0, 6, 15, 252152, 333),
    ])


def test_start_end(m_session, cur_dir):
    path = os.path.join(cur_dir, 'data', 'test.csv')
    ticks = LocalCSVTicks(path, start=datetime(2021, 4, 13), end=datetime(2021, 4, 15))
    df = otp.run(ticks)
    assert all(df['SIZE'] == 2)
    assert all(df['PRICE'] == [
        pytest.approx(1.195620),
        pytest.approx(1.195640),
        pytest.approx(1.195630)
    ])
    assert all(df['SYMBOL_NAME'] == "NY.CNX.EURUSD")
    assert all(df['Time'] == [
        otp.dt(2021, 4, 14, 0, 3, 12, 52924, 123),
        otp.dt(2021, 4, 14, 0, 3, 43, 61975, 432),
        otp.dt(2021, 4, 14, 0, 6, 15, 252152, 333),
    ])


def test_tz(m_session, cur_dir):
    path = os.path.join(cur_dir, 'data', 'test.csv')
    ticks = LocalCSVTicks(path, tz="GMT")
    df = otp.run(ticks, start=datetime(2021, 4, 13), end=datetime(2021, 4, 15))
    assert all(df['Time'] == [
        otp.dt(2021, 4, 13, 20, 3, 12, 52924, 123),
        otp.dt(2021, 4, 13, 20, 3, 43, 61975, 432),
        otp.dt(2021, 4, 13, 20, 6, 15, 252152, 333),
    ])


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Nan values doesn't work correct on windows")
def test_nan(m_session, cur_dir):
    path = os.path.join(cur_dir, 'data', 'test_nan.csv')
    ticks = LocalCSVTicks(path)
    ticks['IS_NAN'] = ticks['PRICE'] == otp.nan
    df = otp.run(ticks, start=datetime(2021, 4, 13), end=datetime(2021, 4, 15))
    assert all(df['IS_NAN'] == [1., 1., 0.])


@pytest.mark.parametrize('date_converter', [
    lambda date: datetime.strptime(date, '%Y-%m-%d-%H%M%S.%f'),
    lambda date: pd.to_datetime(date, format='%Y-%m-%d-%H%M%S.%f'),
    lambda date: otp.dt(pd.to_datetime(date, format='%Y-%m-%d-%H%M%S.%f'))
])
def test_timestamp_converter(m_session, cur_dir, date_converter):
    path = os.path.join(cur_dir, 'data', 'test_converter.csv')
    ticks = LocalCSVTicks(path, date_converter=date_converter)
    df = otp.run(ticks, start=datetime(2021, 4, 13), end=datetime(2021, 4, 15))
    assert all(df['Time'] == [otp.dt(2021, 4, 14, 0, 3, 12, 52924)])


def test_converters(m_session, cur_dir):
    path = os.path.join(cur_dir, 'data', 'test.csv')
    ticks = LocalCSVTicks(path, converters={
        'SIZE': lambda x: int(x) + 2,
        'PRICE': lambda x: float(x) + 3,
    })
    df = otp.run(ticks, start=datetime(2021, 4, 13), end=datetime(2021, 4, 15))
    assert all(df['SIZE'] == 4)
    assert all(df['PRICE'] == [
        pytest.approx(4.195620),
        pytest.approx(4.195640),
        pytest.approx(4.195630),
    ])


def test_additional_columns(m_session, cur_dir):
    path = os.path.join(cur_dir, 'data', 'test_additional_columns.csv')
    ticks = LocalCSVTicks(path, additional_date_columns=['TS'])
    df = otp.run(ticks, start=datetime(2021, 4, 13), end=datetime(2021, 4, 15))
    assert df['Time'][0] == otp.dt(2021, 4, 14, 0, 3, 12, 52924, 123)
    assert df['TS'][0] == otp.dt(2021, 4, 14, 0, 3, 43, 61975, 432)
