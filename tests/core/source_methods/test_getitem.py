import pytest

import onetick.py as otp


class TestSlice:

    def test_errors(self, session):
        t = otp.Ticks({'A': [1, 2, 3, 4, 5]})
        with pytest.raises(ValueError):
            t[::-1]
        with pytest.raises(ValueError):
            t[3:1:]
        with pytest.raises(ValueError):
            t[1:0:]
        with pytest.raises(ValueError):
            t[-1:1:]

    @pytest.mark.parametrize('slice_', [
        slice(3, None),
        slice(-3, None),
        slice(None, 3),
        slice(None, -3),
        slice(1, -1),
        slice(1, 3),
        slice(-3, -1),
        slice(3, None, 2),
        slice(-3, None, 2),
        slice(None, 3, 2),
        slice(None, -3, 2),
        slice(1, -1, 2),
        slice(1, 3, 2),
        slice(-3, -1, 2),
        slice(0, 1, None),
    ])
    def test_slice(self, session, slice_):
        t = otp.Ticks({'A': [1, 2, 3, 4, 5]})
        t = t[slice_]
        df = otp.run(t)
        assert list(df['A']) == [1, 2, 3, 4, 5][slice_]


def test_time_alias(session):
    t = otp.Tick(A=1)
    t = t[['TIMESTAMP', 'A']]
    assert 'TIMESTAMP' in t.schema
    assert set(t.schema) == {'A'}
    df = otp.run(t)
    assert set(df) == {'Time', 'A'}
    assert df['A'][0] == 1

    t = otp.Tick(A=1)
    t = t[['Time', 'A']]
    assert 'Time' in t.schema
    assert set(t.schema) == {'A'}
    df = otp.run(t)
    assert set(df) == {'Time', 'A'}
    assert df['A'][0] == 1
