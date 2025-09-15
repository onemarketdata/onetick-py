import pytest

import onetick.py as otp


@pytest.fixture(scope="module")
def data():
    return otp.Ticks(dict(x=[1, 2, 3, 2, 5, 1]))


@pytest.fixture(scope="module")
def data2():
    return otp.Ticks(dict(x=[1, 2, 3, 4, 5], y=[-1, 1, -1, 1, -1]))


def test_without_default(f_session, data):
    """
    The simples use case
    """
    res1, res2, res3 = data.split(data.x, cases=[1, 3, 5])

    df = otp.run(res1)
    assert len(df) == 2
    assert df.x[0] == 1

    df = otp.run(res2)
    assert len(df) == 1
    assert df.x[0] == 3

    df = otp.run(res3)
    assert len(df) == 1
    assert df.x[0] == 5


def test_with_default(f_session, data):
    """
    Check split with non-empty results
    """
    res1, res2, res3, default = data.split(data.x, cases=[1, 3, 5], default=True)

    df = otp.run(res1)
    assert len(df) == 2
    assert df.x[0] == 1

    df = otp.run(res2)
    assert len(df) == 1
    assert df.x[0] == 3

    df = otp.run(res3)
    assert len(df) == 1
    assert df.x[0] == 5

    df = otp.run(default)
    assert len(df) == 2
    assert df.x[0] == 2


def test_empty_output(f_session, data):
    """
    Check split with empty results
    """
    res1, res2, res3 = data.split(data.x, cases=[0, 9], default=True)

    assert len(otp.run(res1)) == 0
    assert len(otp.run(res2)) == 0

    assert len(otp.run(res3)) == len(otp.run(data))


def test_range(f_session, data):
    """
    Check the simple range use case
    """
    res1, res2 = data.split(data.x, cases=[otp.range(0, 2), otp.range(2.5, 9)])

    df = otp.run(res1)
    assert len(df) == 4
    assert df.x[0] == 1
    assert df.x[1] == 2

    df = otp.run(res2)
    assert len(df) == 2
    assert df.x[0] == 3
    assert df.x[1] == 5


def test_range_default(f_session, data):
    """
    Cases are range + empty default output
    """
    res1, res2, res3 = data.split(data.x, cases=[otp.range(-1, 2.5), otp.range(3, 9)], default=True)

    assert len(otp.run(res1)) == 4
    assert len(otp.run(res2)) == 2
    assert len(otp.run(res3)) == 0


def test_range_empty_default(f_session, data):
    """
    Ranges where one is empty + empty default output
    """
    r1, r2, r3, r4 = data.split(data.x, cases=[otp.range(-4, -2), otp.range(-1, 4), otp.range(4.5, 9)], default=True)

    assert len(otp.run(r1)) == 0
    assert len(otp.run(r2)) == 5
    assert len(otp.run(r3)) == 1
    assert len(otp.run(r4)) == 0


def test_select_range_1(f_session, data):
    """
    Case when one output contains range and selected value + default output
    """
    r1, r2 = data.split(data.x, cases=[(otp.range(0, 2), 5)], default=True)

    assert len(otp.run(r1)) == 5

    df = otp.run(r2)
    assert len(df) == 1
    assert df.x[0] == 3


def test_select_range_2(f_session, data):
    """
    One more test with selected value and ra range + default output
    """
    r1, r2 = data.split(data.x, cases=[(1, otp.range(2, 3))], default=True)

    assert len(otp.run(r1)) == 5

    df = otp.run(r2)
    assert len(df) == 1
    assert df.x[0] == 5


def test_select_two_ranges(f_session, data):
    """
    Select two ranges
    """

    res1, res2 = data.split(data.x, cases=[(otp.range(-4, 2), otp.range(4, 15))], default=True)

    assert len(otp.run(res1)) == 5
    assert len(otp.run(res2)) == 1


def test_expression(f_session, data2):
    """
    Check expression as a target value
    """
    r1, r2 = data2.split(data2.x * data2.y, cases=[otp.range(-100, 0), otp.range(1, 100)])

    assert len(otp.run(r1)) == 3
    assert len(otp.run(r2)) == 2


def test_strings(f_session):
    """
    Check string variables
    """
    data = otp.Ticks(dict(x=["a", "b", "c", "d", "e"]))

    r1, r2, r3 = data.split(data.x, cases=[("a", "d"), "b"], default=True)

    assert len(otp.run(r1)) == 2
    assert len(otp.run(r2)) == 1
    assert len(otp.run(r3)) == 2


def test_doubles(f_session):
    """
    Check double values
    """
    data = otp.Ticks(dict(x=[0.33, -5.1, otp.nan, 9.4]))

    r1, r2, r3 = data.split(data.x, cases=[otp.nan, otp.range(0, 100)], default=True)

    assert len(otp.run(r1)) == 1
    assert len(otp.run(r2)) == 2
    assert len(otp.run(r3)) == 1


def test_bool_expression(f_session):
    """
    Check boolean expressions
    """
    data = otp.Ticks(dict(x=[0.33, -5.1, 9.4, -3, 0.0]))

    r1, r2 = data.split(data.x < 0, cases=[1, 0])

    assert len(otp.run(r1)) == 2
    assert len(otp.run(r2)) == 3


def test_switch(f_session):
    """
    Check switch
    """
    data = otp.Ticks(dict(x=[0.33, -5.1, 9.4, -3, 0.0]))

    r1, r2 = data.switch(data.x < 0, cases=[1, 0])

    assert len(otp.run(r1)) == 2
    assert len(otp.run(r2)) == 3
