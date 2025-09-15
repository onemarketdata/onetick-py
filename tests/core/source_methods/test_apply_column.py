import pytest
import numpy as np

import onetick.py as otp


def test_column_apply_1(session):
    t = otp.Ticks({"x": [3, -4, 9]})

    assert t.x.dtype is int
    df = otp.run(t)
    assert df.x[0] == 3
    assert df.x[1] == -4
    assert df.x[2] == 9

    t.x = t.x.apply(lambda x: x * 3)

    assert t.x.dtype is int
    df = otp.run(t)
    assert df.x[0] == 9
    assert df.x[1] == -12
    assert df.x[2] == 27


def test_column_apply_2(session):
    t = otp.Ticks({"x": [3, -4, 9], "y": [0.3, 0.1, -0.5]})

    assert t.x.dtype is int
    assert t.y.dtype is float
    df = otp.run(t)
    assert df.x[0] == 3 and df.y[0] == 0.3
    assert df.x[1] == -4 and df.y[1] == 0.1
    assert df.x[2] == 9 and df.y[2] == -0.5

    t.x = t.x.apply(lambda x: x)
    t.y = t.y.apply(lambda x: x - x)  # NOSONAR

    assert t.x.dtype is int
    assert t.y.dtype is float
    df = otp.run(t)
    assert df.x[0] == 3 and df.y[0] == 0
    assert df.x[1] == -4 and df.y[1] == 0
    assert df.x[2] == 9 and df.y[2] == -0


def test_column_apply_3(session):
    t = otp.Ticks({"x": [3, -4, 9]})

    t.x = t.x.apply(lambda _: 77)

    df = otp.run(t)
    assert df.x[0] == 77
    assert df.x[1] == 77
    assert df.x[2] == 77
    assert t.x.dtype is int and isinstance(df.x[0], np.integer)


def test_column_apply_4(session):
    t = otp.Ticks({"x": [3, -4, 9]})

    def my_func(session):
        pass

    t.y = t.x.apply(my_func)
    assert t.y.dtype is float

    df = otp.run(t)

    assert np.isnan(df["y"][0])


def test_column_apply_4_5(session):
    t = otp.Ticks({"x": [3, -4, 9]})

    assert not hasattr(t, "z")
    df = otp.run(t)
    assert not hasattr(df, "z")

    t.z = t.x.apply(lambda _: _ * -1)

    assert hasattr(t, "z") and t.z.dtype is int
    df = otp.run(t)
    assert hasattr(df, "z") and isinstance(df.z[0], np.integer)

    assert df.z[0] == -3 and df.x[0] == 3
    assert df.z[1] == 4 and df.x[1] == -4
    assert df.z[2] == -9 and df.x[2] == 9


def test_column_apply_5(session):
    t = otp.Ticks({"x": [3, -4, 9]})

    t.x = t.x.apply(lambda value: 7 if value < 0 else value if value < 5 else value * value)  # NOSONAR

    df = otp.run(t)
    assert df.x[0] == 3
    assert df.x[1] == 7
    assert df.x[2] == 81


def test_column_apply_6(session):
    t = otp.Ticks({"x": [3, -4, 9]})

    t.x = t.x.apply(lambda value: 7 if value < 0 else -1)

    df = otp.run(t)
    assert df.x[0] == -1
    assert df.x[1] == 7
    assert df.x[2] == -1


def test_column_apply_7(session):
    t = otp.Ticks({"x": [3, -4, 9], "y": [-0.35, 0.1, 0.2]})

    t.y = t.x.apply(lambda value: -value if value > 0 else value)

    df = otp.run(t)
    assert df.x[0] == -df.y[0]
    assert df.x[1] == df.y[1]
    assert df.x[2] == -df.y[2]


def test_column_apply_8(session):
    t = otp.Ticks({"x": [3, -4, 9], "y": [-0.35, 0.1, 0.2]})

    assert not hasattr(t, "z")
    df = otp.run(t)
    assert not hasattr(df, "z")

    t.z = t.x.apply(lambda value: -value if value > 0 else value)

    assert hasattr(t, "z") and t.z.dtype is int
    df = otp.run(t)
    assert hasattr(df, "z") and isinstance(df.z[0], np.integer)

    assert df.x[0] == -df.z[0] and df.y[0] == -0.35 and df.z[0] == -3
    assert df.x[1] == df.z[1] and df.y[1] == 0.1 and df.z[1] == -4
    assert df.x[2] == -df.z[2] and df.y[2] == 0.2 and df.z[2] == -9


def test_column_apply_9(session):
    t = otp.Ticks({"x": [3, -4, 9], "y": [0.3, 0.1, -0.5]})

    my_var = 34

    t.z = t.x.apply(lambda v: v if my_var < 30 else -v)
    t.z2 = t.y.apply(lambda v: v * v if my_var > 30 else -v)

    assert t.z.dtype is int
    assert t.z2.dtype is float

    df = otp.run(t)
    assert isinstance(df.z[0], np.integer)
    assert isinstance(df.z2[0], np.float64)

    assert df.z[0] == -3
    assert df.z[1] == 4
    assert df.z[2] == -9

    assert df.z2[0] == 0.3 * 0.3
    assert df.z2[1] == 0.1 * 0.1
    assert df.z2[2] == (-0.5) * (-0.5)


def test_column_apply_10(session):
    t = otp.Ticks({"x": [3, -4, 9], "y": [0.3, 0.1, -0.5]})

    with pytest.raises(ValueError, match=r".* expected to get a function, method or lambda.*"):
        t.x = t.x.apply(lambda v: x for x in range(10))  # NOSONAR


def test_column_apply_11(session):
    # set strings
    t = otp.Ticks({"x": [3, 4, 5]})

    assert t.x.dtype is int

    t.x = t.x.apply(lambda v: "abc" if v >= 4 else "xxx")

    assert t.x.dtype is str
    df = otp.run(t)
    assert df.x[0] == "xxx"
    assert df.x[1] == "abc"
    assert df.x[2] == "abc"


def test_column_apply_12(session):
    # add strings
    t = otp.Ticks({"x": [3, 4, 5]})

    assert t.x.dtype is int
    assert not hasattr(t, "z")

    t.z = t.x.apply(lambda v: "abc" if v >= 4 else "xxx")

    assert t.x.dtype is int
    assert t.z.dtype is str
    df = otp.run(t)
    assert df.z[0] == "xxx"
    assert df.z[1] == "abc"
    assert df.z[2] == "abc"


def test_column_apply_13(session):
    # change type to float
    t = otp.Ticks({"x": [3, -4, 5]})

    assert t.x.dtype is int

    t.x = t.x.apply(lambda v: 17 if v < -4 else 16.3 if v < 0 else 15 if v < 4 else 14)  # NOSONAR
    assert t.x.dtype is float
    df = otp.run(t)
    assert isinstance(df.x[0], np.float64)

    assert df.x[0] == 15
    assert df.x[1] == 16.3
    assert df.x[2] == 14


def test_column_apply_14(session):
    # add column of float type
    t = otp.Ticks({"x": [3, -4, 5]})

    assert t.x.dtype is int

    t.z = t.x.apply(lambda v: 17 if v < -4 else 16.3 if v < 0 else 15 if v < 4 else 14)  # NOSONAR
    assert t.z.dtype is float
    df = otp.run(t)
    assert isinstance(df.z[0], np.float64)

    assert df.z[0] == 15
    assert df.z[1] == 16.3
    assert df.z[2] == 14


def test_column_apply_14_1(session):
    # add column of float type
    t = otp.Ticks({"x": [3, 4, 5]})

    assert t.x.dtype is int
    df = otp.run(t)
    assert isinstance(df.x[0], np.integer)

    t.z = t.x.apply(lambda v: 17 if v < -4 else 16.3 if v < 0 else 15 if v < 4 else 14)  # NOSONAR

    assert t.z.dtype is float
    df = otp.run(t)
    assert isinstance(df.z[0], np.float64)


def test_column_apply_15(session):
    t = otp.Ticks({"x": [3, -4, 9], "y": [0.3, 0.1, -0.5]})

    assert t.y.dtype is float
    df = otp.run(t)
    assert isinstance(df.y[0], np.float64)

    t.y = t.x.apply(lambda v: 1 if v < 0 else 0)
    # PY-574: now y is int after column conversion
    assert t.y.dtype is int
    df = otp.run(t)
    assert isinstance(df.y[0], np.int64)


def test_column_apply_16(session):
    t = otp.Ticks({"x": [3, -4, 9], "y": [0.3, 0.1, -0.5]})

    assert t.y.dtype is float
    df = otp.run(t)
    assert isinstance(df.y[0], np.float64)

    t.z = t.x.apply(lambda v: 1 if v < 0 else 0)

    assert t.z.dtype is int
    df = otp.run(t)
    assert isinstance(df.z[0], np.integer)


def test_column_apply_16_1(session):
    t = otp.Ticks({"x": [3, 4, 5]})

    with pytest.raises(TypeError):
        t.x.apply(lambda v: "bc" if v > 0 else 2.3)
    # with


def test_column_apply_17(session):
    # add with right sum
    t = otp.Ticks({"x": [3, -4, 9], "y": [0.3, 0.1, -0.5], "offset": [0, 1, 2]})

    assert t.y.dtype is float
    df = otp.run(t)
    assert isinstance(df.y[0], np.float64)

    t.t = t.x.apply(lambda v: 1 if v < 0 else 0) + 5

    assert t.t.dtype is int
    df = otp.run(t)
    assert isinstance(df.t[0], np.integer)
    assert df.t[0] == 5
    assert df.t[1] == 6
    assert df.t[2] == 5


def test_column_apply_18(session):
    # add with left sum
    t = otp.Ticks({"x": [3, -4, 9], "y": [0.3, 0.1, -0.5], "offset": [0, 1, 2]})

    assert t.y.dtype is float
    df = otp.run(t)
    assert isinstance(df.y[0], np.float64)

    t.t = 5 + t.x.apply(lambda v: 1 if v < 0 else 0)

    assert t.t.dtype is int
    df = otp.run(t)
    assert isinstance(df.t[0], np.integer)
    assert df.t[0] == 5
    assert df.t[1] == 6
    assert df.t[2] == 5


def test_column_apply_19(session):
    # add with left sum
    t = otp.Ticks({"x": [3, -4, 9], "y": [0.3, 0.1, -0.5], "offset": [0, 1, 2]})

    assert t.y.dtype is float
    df = otp.run(t)
    assert isinstance(df.y[0], np.float64)

    t.t = 5 - t.x.apply(lambda v: 1 if v < 0 else 0)

    assert t.t.dtype is int
    df = otp.run(t)
    assert isinstance(df.t[0], np.integer)
    assert df.t[0] == 5
    assert df.t[1] == 4
    assert df.t[2] == 5


def test_apply_20(session):
    t = otp.Ticks({"x": [1, 2, 3]})

    t.y = t.x.apply(lambda: 1)

    df = otp.run(t)
    assert df.y[0] == 1 and df.y[1] == 1 and df.y[2] == 1


def test_apply_21(session):
    t = otp.Ticks({"x": [1, 2, 3]})

    t.y = t.x.apply(lambda v: 1)

    df = otp.run(t)
    assert df.y[0] == 1 and df.y[1] == 1 and df.y[2] == 1


def test_apply_22(session):
    t = otp.Ticks({"x": [1, 2, 3]})

    with pytest.raises(ValueError, match=r"take either one or zero parameters"):
        t.y = t.x.apply(lambda a, b: 1)
    # with
