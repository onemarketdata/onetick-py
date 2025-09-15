import pandas as pd
import onetick.py as otp
from onetick.test.fixtures import m_session as session

from onetick.py import nsectime, msectime


def test_int(session):
    data = otp.Ticks(dict(X=[1, 0, -1, 0], N=list(range(4))))
    yes, no = data[data["X"]]
    yes = otp.run(yes)
    assert all(yes["X"] == [1, -1])
    assert all(yes["N"] == [0, 2])
    no = otp.run(no)
    assert all(no["X"] == [0, 0])
    assert all(no["N"] == [1, 3])


def test_float(session):
    data = otp.Ticks(dict(X=[1.0, -0.5, -1.0, 0.0], N=list(range(4))))
    yes, no = data[2 * data["X"] + 1]
    yes = otp.run(yes)
    no = otp.run(no)
    assert all(yes["X"] == [1.0, -1.0, 0.0])
    assert all(yes["N"] == [0, 2, 3])
    assert all(no["X"] == [-0.5])
    assert all(no["N"] == [1])


def test_str(session):
    data = otp.Ticks(dict(X=["1", "", "A"], N=list(range(3))))
    yes, no = data[data["X"]]
    yes = otp.run(yes)
    assert all(yes["X"] == ["1", "A"])
    assert all(yes["N"] == [0, 2])
    no = otp.run(no)
    assert all(no["X"] == [""])
    assert all(no["N"] == [1])


def test_nsectime(session):
    data = otp.Ticks(dict(X=[nsectime(0), nsectime(1), msectime(0)], N=list(range(3))))
    yes, no = data[data["X"]]
    yes = otp.run(yes, timezone="GMT")
    assert all(yes["X"] == [pd.Timestamp(0) + pd.offsets.Nano(1)])
    assert all(yes["N"] == [1])
    no = otp.run(no, timezone="GMT")
    assert all(no["X"] == [pd.Timestamp(0), pd.Timestamp(0)])
    assert all(no["N"] == [0, 2])
