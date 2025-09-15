import pytest
import pandas as pd

import onetick.py as otp


def compare_float_lists(a_list, b_list):
    for a, b in zip(a_list, b_list):
        if not any([
            pd.isna(a) and pd.isna(b),
            a == b,
        ]):
            return False
    return True


def test_no_floats(session):
    t = otp.Tick(A=1)
    t = t.fillna()
    df = otp.run(t)
    assert list(df['A']) == [1]


def test_errors(session):
    t = otp.Tick(S='s')
    with pytest.raises(ValueError, match="The type of parameter 'value' must be float"):
        t.fillna('x')
    with pytest.raises(ValueError, match="The type of parameter 'value' must be float"):
        t.fillna(t['S'])


def test_double(session):
    data = otp.Ticks({'A': [0, 1, 2, 3], 'B': [otp.nan, 2.2, otp.nan, 3.3]})
    data = data.fillna()
    data = data.fillna()
    df = otp.run(data)
    assert compare_float_lists(df['B'], [float('nan'), 2.2, 2.2, 3.3])


@pytest.mark.skipif(otp.compatibility.is_per_tick_script_boolean_problem(),
                    reason='Fails on a specific OneTick version')
def test_columns(session):
    data = otp.Ticks({'A': [0, 1, 2, 3], 'B': [otp.nan, 2.2, otp.nan, 3.3], 'C': [otp.nan, 2.2, otp.nan, 3.3]})
    with pytest.raises(ValueError, match="Column 'XXX' is not in schema"):
        _ = data.fillna(columns=['XXX'])
    with pytest.raises(TypeError, match="Column 'A' doesn't have float type"):
        _ = data.fillna(columns=['A'])
    data1 = data.fillna(columns=['B'])
    df = otp.run(data1)
    assert compare_float_lists(df['B'], [float('nan'), 2.2, 2.2, 3.3])
    assert compare_float_lists(df['C'], [float('nan'), 2.2, float('nan'), 3.3])
    data2 = data.fillna(columns=['B'], value=0.0)
    df = otp.run(data2)
    assert compare_float_lists(df['B'], [0.0, 2.2, 0.0, 3.3])
    assert compare_float_lists(df['C'], [float('nan'), 2.2, float('nan'), 3.3])
