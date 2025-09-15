import numpy as np

import onetick.py as otp

Tick = otp.Tick


def test_add_bool_column_1(session):
    t = Tick(x=3)

    t.y = True
    t.y = False

    value = otp.run(t).y[0]
    assert bool(value) is False and value == 0.0 and isinstance(value, np.float64)


def test_add_bool_column_2(session):
    t = Tick(x=3)

    t.y = False
    t.y = True

    value = otp.run(t).y[0]
    assert bool(value) is True and value == 1.0 and isinstance(value, np.float64)


def test_add_bool_column_3(session):
    t = Tick(x=3, y=True)

    value = otp.run(t).y[0]
    assert bool(value) is True and value == 1.0 and isinstance(value, np.float64)


def test_add_bool_column_4(session):
    t = Tick(x=3, y=False)

    value = otp.run(t).y[0]
    assert bool(value) is False and value == 0.0 and isinstance(value, np.float64)


def test_add_bool_column_5(session):
    t = Tick(x=3, y=False)

    value = otp.run(t).x[0]
    assert value == 3 and isinstance(value, np.integer)

    t.x = t.x.apply(lambda: True)

    value = otp.run(t).x[0]
    assert bool(value) is True and value == 1.0 and isinstance(value, np.float64)

    t.x = t.x.apply(lambda v: False)

    value = otp.run(t).x[0]
    assert bool(value) is False and value == 0.0 and isinstance(value, np.float64)


def test_multiple_ticks(session):
    a = otp.run(otp.Ticks({'A': [True, True]}))
    assert all(a['A'] == 1.0)

    a = otp.run(otp.Ticks({'A': [False, False]}))
    assert all(a['A'] == 0.0)
