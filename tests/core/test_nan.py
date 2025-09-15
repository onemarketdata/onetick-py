import sys
import pytest
import numpy as np
import pandas as pd

import onetick.py as otp


def test_nan_1(session):
    d = {"x": [0.1, otp.nan, 0.2]}

    data = otp.Ticks(d)

    assert data.x.dtype is float

    df = otp.run(data)
    assert df.x[0] == 0.1
    assert np.isnan(df.x[1])
    assert df.x[2] == 0.2


def test_nan_2(session):
    data = otp.Ticks({"x": [1, 2, 3, 4, otp.nan]})

    assert data.x.dtype is float

    df = otp.run(data)
    assert df.x[0] == 1.0
    assert df.x[1] == 2.0
    assert df.x[2] == 3.0
    assert df.x[3] == 4.0
    assert np.isnan(df.x[4])


def test_nan_3(session):
    data = otp.Ticks({"x": [1, 2, 3, 4]})

    assert data.x.dtype is int

    data.x = otp.nan

    assert data.x.dtype is float

    df = otp.run(data)
    for inx in range(4):
        assert np.isnan(df.x[inx])


def test_nan_4(session):
    data = otp.Ticks({"x": [1, 2, 3, 4]})

    data.y = otp.nan

    assert data.y.dtype is float

    df = otp.run(data)
    for inx in range(4):
        assert np.isnan(df.y[inx])


def test_nan_5(session):
    data = otp.Ticks({"x": [1, 2, 3, 4]})

    data.x = data.x * otp.nan

    assert data.x.dtype is float

    df = otp.run(data)
    for inx in range(4):
        assert np.isnan(df.x[inx])


def test_nan_6(session):
    data = otp.Ticks({"x": [1, otp.nan, 3, otp.nan]})

    left, right = data[data.x != otp.nan]

    assert left.x.dtype is float
    assert right.x.dtype is float

    left = otp.run(left)
    right = otp.run(right)
    assert len(left) == 2
    assert len(right) == 2

    assert left.x[0] == 1
    assert left.x[1] == 3
    assert np.isnan(right.x[0])
    assert np.isnan(right.x[1])


def test_nan_7(session):
    data = otp.Ticks({"x": [1, otp.nan, 3, otp.nan]})

    data.x = data.x.apply(lambda v: v * 2 if v != otp.nan else 0)

    assert data.x.dtype is float

    df = otp.run(data)
    assert len(df) == 4
    assert df.x[0] == 2
    assert df.x[1] == 0
    assert df.x[2] == 6
    assert df.x[3] == 0


@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="https://gitlab.sol.onetick.com/solutions/py-onetick/onetick-py/issues/2"
)
def test_nan_8(session):
    data = otp.Ticks({"x": [1, otp.nan, 2, otp.nan]})
    df = pd.DataFrame({"x": [1, np.nan, 2, np.nan]})

    data.x = data.x.apply(str)
    df.x = df.x.apply(str)

    res = otp.run(data)
    assert df.x[0] == res.x[0]
    assert df.x[1] == res.x[1]
    assert df.x[2] == res.x[2]
    assert df.x[3] == res.x[3]


def test_fillna_1(session):
    d = {"x": [1, otp.nan, 2, otp.nan]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.x = data.x.fillna(0)
    df.x = df.x.fillna(0)

    assert data.x.dtype is float

    res = otp.run(data)
    for inx in range(4):
        assert res.x[inx] == df.x.iloc[inx]


def test_fillna_2(session):
    d = {"x": [1, otp.nan, 2, otp.nan]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data["y"] = data.x.fillna(0).astype(int)
    df["y"] = df.x.fillna(0).astype(int)

    assert data.y.dtype is int

    res = otp.run(data)
    for inx in range(4):
        assert res.y[inx] == df.y.iloc[inx]


def test_fillna_3(session):
    d = {"x": [1, otp.nan, 2, otp.nan]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data["y"] = -data.x.fillna(0).astype(int) * 3 + 9
    df["y"] = -df.x.fillna(0).astype(int) * 3 + 9

    res = otp.run(data)
    for inx in range(4):
        assert res.y[inx] == df.y.iloc[inx]


def test_fillna_4(session):
    d = {"x": ["a", "b", "c"]}

    data = otp.Ticks(d)

    # .fillna() is applicable only for float type columns
    with pytest.raises(TypeError):
        data.x = data.x.fillna(0)


def test_fillna_5(session):
    d = {"x": [1, 2, 3]}

    data = otp.Ticks(d)

    assert data.x.dtype is int

    # .fillna() is applicable only for float type columns
    with pytest.raises(TypeError):
        data.x = data.x.fillna(0)


def test_fillna_6(session):
    d = {"x": [1, 2, 3]}

    data = otp.Ticks(d)

    data.x = data.x.astype(float).fillna(0)

    assert data.x.dtype is float
    assert isinstance(otp.run(data).x[0], np.float64)


def test_fillna_7(session):
    d = {"x": [1.3, otp.nan, 5.1, otp.nan, otp.nan, -0.1]}

    data = otp.Ticks(d)

    # support only numeric types
    with pytest.raises(TypeError):
        data.x = data.x.fillna("missing")


def test_dropna_1(session):
    d = {"x": [1.3, otp.nan, 5.1, otp.nan, otp.nan, -0.1], "y": [1, 2, 3, 4, otp.nan, 6]}

    data = otp.Ticks(d)

    clean_data = data.dropna()

    assert len(otp.run(data)) == 6

    df = otp.run(clean_data)
    assert len(df) == 3

    assert df.x[0] == 1.3 and df.y[0] == 1
    assert df.x[1] == 5.1 and df.y[1] == 3
    assert df.x[2] == -0.1 and df.y[2] == 6


def test_dropna_2(session):
    d = {"x": [1.3, otp.nan, 5.1, otp.nan, otp.nan, -0.1], "y": [1, 2, 3, 4, otp.nan, 6]}

    data = otp.Ticks(d)

    clean_data = data.dropna(how="any")

    assert len(otp.run(data)) == 6
    assert len(otp.run(clean_data)) == 3


def test_dropna_3(session):
    d = {"x": [1.3, otp.nan, 5.1, otp.nan, otp.nan, -0.1], "y": [1, 2, 3, 4, otp.nan, 6]}

    data = otp.Ticks(d)

    clean_data = data.dropna(how="all")

    assert len(otp.run(data)) == 6

    df = otp.run(clean_data)
    assert len(df) == 5

    assert df.x[0] == 1.3 and df.y[0] == 1
    assert np.isnan(df.x[1]) and df.y[1] == 2
    assert df.x[2] == 5.1 and df.y[2] == 3
    assert np.isnan(df.x[3]) and df.y[3] == 4
    assert df.x[4] == -0.1 and df.y[4] == 6


def test_dropna_4(session):
    d = {"x": [1.3, otp.nan, 5.1, otp.nan, otp.nan, -0.1], "y": [1, 2, 3, 4, otp.nan, 6]}

    data = otp.Ticks(d)

    assert len(otp.run(data)) == 6

    data.dropna(how="any", inplace=True)

    assert len(otp.run(data)) == 3


def test_dropna_5(session):
    d = {"x": [1.3, otp.nan, 5.1, otp.nan, otp.nan, -0.1], "y": [1, 2, 3, 4, otp.nan, 6]}

    data = otp.Ticks(d)

    assert len(otp.run(data)) == 6

    data.dropna(how="all", inplace=True)

    assert len(otp.run(data)) == 5


def test_dropna_6(session):
    d = {"x": [1.3, otp.nan, 5.1, otp.nan, otp.nan, -0.1], "y": [1, 2, 3, 4, otp.nan, 6]}

    data = otp.Ticks(d)

    assert len(otp.run(data)) == 6

    data.dropna(how="any", inplace=False)

    assert len(otp.run(data)) == 6


def test_dropna_7(session):
    d = {"x": [1.3, otp.nan, 5.1, otp.nan, otp.nan, -0.1], "y": [1, 2, 3, 4, otp.nan, 6]}

    data = otp.Ticks(d)

    assert len(otp.run(data)) == 6

    data.dropna(how="all", inplace=False)

    assert len(otp.run(data)) == 6


def test_dropna_8(session):
    d = {"x": [1.3, 5.1, -0.1], "y": [1, 2, 3]}

    data = otp.Ticks(d)

    assert len(otp.run(data)) == 3

    data.dropna(inplace=True)

    assert len(otp.run(data)) == 3


def test_dropna_9(session):
    d = {"x": [1.3, 5.1, -0.1], "y": [1, 2, 3]}

    data = otp.Ticks(d)

    assert len(otp.run(data)) == 3

    res_data = data.dropna(inplace=False)

    assert len(otp.run(res_data)) == 3


def test_dropna_10(session):
    d = {"x": [1.3, otp.nan, 5.1, otp.nan, otp.nan, -0.1], "y": [1, 2, 3, 4, otp.nan, 6]}

    data = otp.Ticks(d)

    # only 'all' or 'any' allowed
    with pytest.raises(ValueError):
        data.dropna(how="ololo")


@pytest.mark.parametrize("how,result", [("all", 2), ("any", 1)])
def test_dropna_subset(session, how, result):
    d = {"x": [1, 2, otp.nan],
         "y": [1, otp.nan, otp.nan],
         "z": [otp.nan] * 3}

    data = otp.Ticks(d)
    data.dropna(how=how, subset=["x", "y"], inplace=True)
    assert len(otp.run(data)) == result


def test_dropna_subset_raise(session):
    d = {"x": [1, 2, otp.nan]}
    data = otp.Ticks(d)
    with pytest.raises(ValueError):
        data.dropna(subset=["y"], inplace=True)


def test_dropna_subset_raise_not_float(session):
    d = {"x": ["a"]}
    data = otp.Ticks(d)
    with pytest.raises(ValueError):
        data.dropna(subset=["x"], inplace=True)
