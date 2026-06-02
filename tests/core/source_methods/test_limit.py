import pytest

import onetick.py as otp
from onetick.py.otq import otq
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


@pytest.mark.skipif(
    'apply_across_symbols' not in otq.Limit.Parameters.list_parameters(),
    reason='Not supported on this OneTick version',
)
def test_apply_across_symbols(session):
    db = otp.DB('TEST_LIMIT_ACS')
    session.use(db)
    db.add(otp.Ticks(X=[1, 3, 5, 7], offset=[0, 2, 4, 6]), tick_type='TT', symbol='A', date=otp.config.default_start_time)
    db.add(otp.Ticks(X=[2, 4, 6, 8], offset=[1, 3, 5, 7]), tick_type='TT', symbol='B', date=otp.config.default_start_time)

    data = otp.DataSource(db='TEST_LIMIT_ACS', tick_type='TT')
    data1 = data.limit(tick_limit=2, apply_across_symbols=True)
    data2 = data.limit(tick_limit=2, apply_across_symbols=False)
    data3 = data.limit(tick_limit=6, apply_across_symbols=True)
    res1 = otp.run(data1, symbols=['A', 'B'])
    res2 = otp.run(data2, symbols=['A', 'B'])
    res3 = otp.run(data3, symbols=['A', 'B'])

    assert len(res2['A']) == len(res2['B']) == 2
    assert res1['A']['X'].to_list() == [1, 3] and res1['B'].empty
    assert res3['A']['X'].to_list() == [1, 3, 5, 7] and res3['B']['X'].to_list() == [2, 4]
