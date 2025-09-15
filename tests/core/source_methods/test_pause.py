import pytest
import pandas as pd
import onetick.py as otp


def test_int(session):
    t = otp.Tick(A=1)
    t['B'] = otp.now()
    with pytest.raises(ValueError):
        t = t.pause(-1)
    t = t.pause(1000)
    t['C'] = otp.now()  # noqa: E1137 (false positive for some reason)
    df = otp.run(t)
    diff = df['C'][0] - df['B'][0]
    assert pd.Timedelta(seconds=1) <= diff < pd.Timedelta(seconds=2)


def test_busy_waiting(session):
    t = otp.Tick(A=1)
    t['B'] = otp.now()
    t = t.pause(1000, busy_waiting=True)
    t['C'] = otp.now()  # noqa: E1137 (false positive for some reason)
    df = otp.run(t)
    diff = df['C'][0] - df['B'][0]
    assert pd.Timedelta(seconds=1) <= diff < pd.Timedelta(seconds=2)


def test_operation(session):
    t = otp.Tick(A=1)
    t['B'] = otp.now()
    t = t.pause(t['A'] * 1000)
    t['C'] = otp.now()
    df = otp.run(t)
    diff = df['C'][0] - df['B'][0]
    assert pd.Timedelta(seconds=1) <= diff < pd.Timedelta(seconds=2)


def test_where(session):
    t = otp.Ticks(A=[-1, 1])
    t['B'] = otp.now()
    t = t.pause(t['A'] * 1000, where=t['A'] > 0)
    t['C'] = otp.now()
    df = otp.run(t)
    diff = df['C'][0] - df['B'][0]
    assert pd.Timedelta(seconds=0) <= diff <= pd.Timedelta(seconds=1)
    diff = df['C'][1] - df['B'][1]
    assert pd.Timedelta(seconds=1) <= diff < pd.Timedelta(seconds=2)
