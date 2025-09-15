import pytest

import onetick.py as otp
from onetick.py.otq import otq


def test_default(session):
    t = otp.Ticks(A=[1, 2], B=[0, 0])
    q = otp.Ticks(A=[1, 3], B=[0, 0])
    data = t.diff(q)
    assert 'L.A' in data.schema
    assert 'R.A' in data.schema
    assert 'A' not in data.schema
    assert 'B' not in data.schema
    assert 'L.INDEX' not in data.schema
    assert 'R.INDEX' not in data.schema
    df = otp.run(data)
    assert list(df) == ['Time', 'L.A', 'R.A']
    assert list(df['L.A']) == [2]
    assert list(df['R.A']) == [3]


def test_prefix(session):
    t = otp.Ticks(A=[1, 2], B=[0, 0])
    q = otp.Ticks(A=[1, 3], B=[0, 0])
    data = t.diff(q, left_prefix='LEFT', right_prefix='RIGHT')
    assert 'LEFT.A' in data.schema
    assert 'RIGHT.A' in data.schema
    df = otp.run(data)
    assert list(df) == ['Time', 'LEFT.A', 'RIGHT.A']
    assert list(df['LEFT.A']) == [2]
    assert list(df['RIGHT.A']) == [3]


def test_drop_index(session):
    t = otp.Ticks(A=[1, 2], B=[0, 0])
    q = otp.Ticks(A=[1, 3], B=[0, 0])
    data = t.diff(q, drop_index=False)
    assert 'L.INDEX' in data.schema
    assert 'R.INDEX' in data.schema
    df = otp.run(data)
    assert list(df) == ['Time', 'L.INDEX', 'L.A', 'R.INDEX', 'R.A']
    assert list(df['L.A']) == [2]
    assert list(df['R.A']) == [3]
    assert list(df['L.INDEX']) == [2]
    assert list(df['R.INDEX']) == [2]


def test_fields(session):
    t = otp.Ticks(A=[1, 2], B=[4, 0])
    q = otp.Ticks(A=[1, 3], B=[5, 0])
    with pytest.raises(ValueError):
        t.diff(q, fields='NON_EXISTENT')
    data = t.diff(q, fields='B')
    df = otp.run(data)
    assert list(df) == ['Time', 'L.B', 'R.B']
    assert list(df['L.B']) == [4]
    assert list(df['R.B']) == [5]


def test_ignore(session):
    t = otp.Ticks(A=[1, 2], B=[4, 0])
    q = otp.Ticks(A=[1, 3], B=[5, 0])
    data = t.diff(q, fields='B', ignore=True)
    df = otp.run(data)
    assert list(df) == ['Time', 'L.A', 'L.B', 'R.A', 'R.B']
    assert list(df['L.A']) == [2]
    assert list(df['R.A']) == [3]
    assert list(df['L.B']) == [0]
    assert list(df['R.B']) == [0]


def test_output_ignored_fields(session):
    t = otp.Ticks(A=[1, 2], B=[4, 0])
    q = otp.Ticks(A=[1, 3], B=[5, 0])
    data = t.diff(q, fields='B', ignore=True, output_ignored_fields=False)
    df = otp.run(data)
    assert list(df) == ['Time', 'L.A', 'R.A']
    assert list(df['L.A']) == [2]
    assert list(df['R.A']) == [3]


def test_show_matching_ticks(session):
    t = otp.Ticks(A=[1, 2, 3, 4, 5])
    q = otp.Ticks(A=[5, 4, 3, 2, 1])
    if not otp.compatibility.is_diff_show_matching_ticks_supported():
        with pytest.warns(Warning,
                          match="Parameter 'show_matching_ticks' is not supported on this version of OneTick"):
            data = t.diff(q, show_matching_ticks=True)
        df = otp.run(data)
        assert list(df) == ['Time', 'L.A', 'R.A']
        assert list(df['L.A']) == [1, 2, 4, 5]
        assert list(df['R.A']) == [5, 4, 2, 1]
    else:
        data = t.diff(q, show_matching_ticks=True)
        df = otp.run(data)
        assert list(df) == ['Time', 'L.A', 'R.A']
        assert list(df['L.A']) == [3]
        assert list(df['R.A']) == [3]


def test_show_only_fields_that_differ(session):
    t = otp.Ticks(A=[1, 2], B=[0, 0])
    q = otp.Ticks(A=[1, 3], B=[0, 0])
    data = t.diff(q, show_only_fields_that_differ=False)
    assert data.schema == {
        'L.A': int,
        'L.B': int,
        'R.A': int,
        'R.B': int,
    }
    df = otp.run(data)
    assert list(df) == ['Time', 'L.A', 'L.B', 'R.A', 'R.B']
    assert list(df['L.A']) == [2]
    assert list(df['R.A']) == [3]
    assert list(df['L.B']) == [0]
    assert list(df['R.B']) == [0]

    data = t.diff(q, show_only_fields_that_differ=True)
    assert data.schema == {
        'L.A': int,
        'L.B': int,
        'R.A': int,
        'R.B': int,
    }
    df = otp.run(data)
    assert list(df) == ['Time', 'L.A', 'R.A']
    assert list(df['L.A']) == [2]
    assert list(df['R.A']) == [3]
