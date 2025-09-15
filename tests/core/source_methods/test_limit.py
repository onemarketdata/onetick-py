import pytest

import onetick.py as otp
from onetick.py.compatibility import is_limit_ep_supported

pytestmark = pytest.mark.skipif(
    not is_limit_ep_supported(), reason='LIMIT EP not supported on the current OneTick version',
)


def test_base(session):
    data = otp.Ticks(X=[1, 2, 3, 4, 5, 6])
    data = data.limit(tick_limit=3)
    df = otp.run(data)
    assert list(df['X']) == [1, 2, 3]


def test_no_limit(session):
    data = otp.Ticks(X=[1, 2, 3, 4, 5, 6])
    data = data.limit(tick_limit=-1)
    df = otp.run(data)
    assert list(df['X']) == [1, 2, 3, 4, 5, 6]


def test_empty(session):
    data = otp.Ticks(X=[1, 2, 3, 4, 5, 6])
    data = data.limit(tick_limit=0)
    df = otp.run(data)
    assert list(df['X']) == []


def test_exceptions():
    data = otp.Ticks(X=[1, 2, 3, 4, 5, 6])

    with pytest.raises(ValueError):
        _ = data.limit(tick_limit=-6)
