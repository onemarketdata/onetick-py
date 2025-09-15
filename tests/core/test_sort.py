import pytest
import numpy as np
import pandas as pd

import onetick.py as otp


def test_sort_1(session):
    # default -> default is asc
    data = otp.Ticks({"x": [94, 5, 34], "y": [otp.nan, 3.1, 0.3]})

    data = data.sort(data.x)
    data.ST = data._START_TIME
    data.ET = data._END_TIME

    assert hasattr(data, "x") and data.x.dtype is int
    assert hasattr(data, "y") and data.y.dtype is float

    res = otp.run(data)
    assert len(res) == 3
    assert isinstance(res.x[0], np.integer)
    assert isinstance(res.y[0], np.float64)

    assert res.x[0] == 5 and res.y[0] == 3.1
    assert res.x[1] == 34 and res.y[1] == 0.3
    assert res.x[2] == 94 and np.isnan(res.y[2])


def test_sort_2(session):
    # asc
    data = otp.Ticks({"x": [94, 5, 34], "y": [otp.nan, 3.1, 0.3]})

    data = data.sort(data.x, True)
    data.ST = data._START_TIME
    data.ET = data._END_TIME

    assert hasattr(data, "x") and data.x.dtype is int
    assert hasattr(data, "y") and data.y.dtype is float

    res = otp.run(data)
    assert len(res) == 3
    assert isinstance(res.x[0], np.integer)
    assert isinstance(res.y[0], np.float64)

    assert res.x[0] == 5 and res.y[0] == 3.1
    assert res.x[1] == 34 and res.y[1] == 0.3
    assert res.x[2] == 94 and np.isnan(res.y[2])


def test_sort_3(session):
    # desc
    data = otp.Ticks({"x": [94, 5, 34], "y": [otp.nan, 3.1, 0.3]})

    data = data.sort(data.x, False)
    data.ST = data._START_TIME
    data.ET = data._END_TIME

    assert hasattr(data, "x") and data.x.dtype is int
    assert hasattr(data, "y") and data.y.dtype is float

    res = otp.run(data)
    assert len(res) == 3
    assert isinstance(res.x[0], np.integer)
    assert isinstance(res.y[0], np.float64)

    assert res.x[0] == 94 and np.isnan(res.y[0])
    assert res.x[1] == 34 and res.y[1] == 0.3
    assert res.x[2] == 5 and res.y[2] == 3.1


def test_sort_4(session):
    # asc
    data = otp.Ticks({"x": [94, 5, 34], "y": [otp.nan, 3.1, 0.3]})

    data = data.sort(data.x, ascending=True)
    data.ST = data._START_TIME
    data.ET = data._END_TIME

    res = otp.run(data)
    assert res.x[0] == 5 and res.y[0] == 3.1
    assert res.x[1] == 34 and res.y[1] == 0.3
    assert res.x[2] == 94 and np.isnan(res.y[2])

    data = data.sort(data.Time)

    res = otp.run(data)
    assert res.x[0] == 94 and np.isnan(res.y[0])
    assert res.x[1] == 5 and res.y[1] == 3.1
    assert res.x[2] == 34 and res.y[2] == 0.3


def test_sort_5(session):
    # multiple columns
    d = {"x": [5, 6, 3, 6], "y": [1.4, 3.1, 9.1, 5.5]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data = data.sort_values(by=["x", "y"], ascending=[True, False])
    df = df.sort_values(by=["x", "y"], ascending=[True, False])

    res = otp.run(data)
    assert len(res) == 4

    assert res.x[0] == 3 and res.y[0] == 9.1
    assert res.x[1] == 5 and res.y[1] == 1.4
    assert res.x[2] == 6 and res.y[2] == 5.5
    assert res.x[3] == 6 and res.y[3] == 3.1

    for inx in range(4):
        assert res.x[inx] == df.x.iloc[inx] and res.y[inx] == df.y.iloc[inx]


def test_sort_6(session):
    # miltiple columns
    d = {"x": [5, 6, 3, 6], "y": [1.4, 3.1, 9.1, 5.5]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data = data.sort([data.x, data.y], [True, True])
    df = df.sort_values(["x", "y"], ascending=[True, True])

    res = otp.run(data)
    assert len(res) == 4

    assert res.x[0] == 3 and res.y[0] == 9.1
    assert res.x[1] == 5 and res.y[1] == 1.4
    assert res.x[2] == 6 and res.y[2] == 3.1
    assert res.x[3] == 6 and res.y[3] == 5.5

    for inx in range(4):
        assert res.x[inx] == df.x.iloc[inx] and res.y[inx] == df.y.iloc[inx]


def test_sort_7(session):
    # multile columns, default value for asc is True
    d = {"x": [5, 6, 3, 6], "y": [1.4, 3.1, 9.1, 5.5]}

    data = otp.Ticks(d)

    assert data.x.dtype is int
    assert data.y.dtype is float

    data = data.sort([data.x, data.y], [True])

    assert data.x.dtype is int
    assert data.y.dtype is float

    res = otp.run(data)
    assert len(res) == 4

    assert res.x[0] == 3 and res.y[0] == 9.1
    assert res.x[1] == 5 and res.y[1] == 1.4
    assert res.x[2] == 6 and res.y[2] == 3.1
    assert res.x[3] == 6 and res.y[3] == 5.5


def test_sort_8(session):
    # multile columns, default value for asc is True
    d = {"x": [5, 6, 3, 6], "y": [1.4, 3.1, 9.1, 5.5]}

    data = otp.Ticks(d)

    data = data.sort([data.y, data.x], [True, False])

    res = otp.run(data)
    assert len(res) == 4

    assert res.x[0] == 5 and res.y[0] == 1.4
    assert res.x[1] == 6 and res.y[1] == 3.1
    assert res.x[2] == 6 and res.y[2] == 5.5
    assert res.x[3] == 3 and res.y[3] == 9.1


def test_sort_9(session):
    # multile columns, default value for asc is True
    d = {"x": [5, 6, 3, 6], "y": [1.4, 3.1, 9.1, 5.5]}

    data = otp.Ticks(d)

    data = data.sort([data.y, data.x], [True, True])

    res = otp.run(data)
    assert len(res) == 4

    assert res.x[0] == 5 and res.y[0] == 1.4
    assert res.x[1] == 6 and res.y[1] == 3.1
    assert res.x[2] == 6 and res.y[2] == 5.5
    assert res.x[3] == 3 and res.y[3] == 9.1


def test_sort_10(session):
    # larger asc list is truncated
    d = {"x": [5, 6, 3, 6], "y": [1.4, 3.1, 9.1, 5.5]}

    data = otp.Ticks(d)

    data = data.sort([data.y, data.x], [True, True, False, True])

    res = otp.run(data)
    assert len(res) == 4

    assert res.x[0] == 5 and res.y[0] == 1.4
    assert res.x[1] == 6 and res.y[1] == 3.1
    assert res.x[2] == 6 and res.y[2] == 5.5
    assert res.x[3] == 3 and res.y[3] == 9.1


def test_sort_12(session):
    data = otp.Ticks({"x": [1, 0], "new_offset": [10, 1], "offset": [0, 1]})

    data.new_time = data.Time + data.new_offset
    data.sort(data.new_time)
    data.Time = data.new_time
    res = otp.run(data)
    assert res.x[0] == 0 and res.x[1] == 1


@pytest.mark.parametrize('inplace', [True, False])
def test_inplace(session, inplace):
    data = otp.Ticks(X=[5, 3, 9, 2])

    res = data.sort(data['X'], inplace=inplace)

    if inplace:
        assert res is None
        assert all(otp.run(data)['X'] == [2, 3, 5, 9])
    else:
        assert isinstance(res, otp.Source)
        assert all(otp.run(data)['X'] == [5, 3, 9, 2])
        assert all(otp.run(res)['X'] == [2, 3, 5, 9])
