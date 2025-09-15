import pytest

import onetick.py as otp
from onetick.py.otq import otq


@pytest.fixture
def real_session(f_session, monkeypatch):
    db = otp.DB('DB')
    db.add(otp.Tick(A=12345), symbol='S', tick_type='TT')
    f_session.use(db)

    for option in otp.config.get_changeable_config_options():
        monkeypatch.setattr(otp.config, option, otp.config.default)
    yield f_session


def test_data_source(real_session):
    data = otp.DataSource('DB', symbol='S', tick_type='TT')
    df = otp.run(data, start=otp.datetime(2003, 12, 1), end=otp.datetime(2003, 12, 2), timezone='EST5EDT')
    assert df['A'][0] == 12345


def test_tick(real_session):
    t = otp.Tick(A=1)
    df = otp.run(t, start=otp.datetime(2003, 12, 1), end=otp.datetime(2003, 12, 2), symbols='DB::S')
    assert df['A'][0] == 1


def test_ticks(real_session):
    t = otp.Ticks(A=[1, 2, 3])
    df = otp.run(t, start=otp.datetime(2003, 12, 1), end=otp.datetime(2003, 12, 2), symbols='DB::S')
    assert list(df['A']) == [1, 2, 3]


def test_empty(real_session):
    t = otp.Empty()
    df = otp.run(t, start=otp.datetime(2003, 12, 1), end=otp.datetime(2003, 12, 2), symbols='DB::S')
    assert df.empty


def test_databases(real_session):
    assert 'DB' in otp.databases()
    assert 'DB' in real_session.databases


def test_reload_config(real_session):
    real_session.locator.reload()
    real_session.acl.reload()


def test_apply_query(real_session):
    t = otp.Tick(A=1)
    t = t.apply(otq.GraphQuery(otq.UpdateField('A', 'A + 1')))
    df = otp.run(t, start=otp.datetime(2003, 12, 1), end=otp.datetime(2003, 12, 2), symbols='DB::S')
    assert df['A'][0] == 2


def test_apply_pts(real_session):
    t = otp.Tick(A=1)
    t = t.apply(otq.GraphQuery(otq.PerTickScript('A = A + 1;\nA = A + 2;\nA = A + 3;\n')))
    df = otp.run(t, start=otp.datetime(2003, 12, 1), end=otp.datetime(2003, 12, 2), symbols='DB::S')
    assert df['A'][0] == 7


def test_tmp_locator(real_session):
    otp.utils.tmp_locator()


def test_split_query_output_symbol(real_session, monkeypatch):
    # unfortunately, we still need default symbol here
    monkeypatch.setenv('OTP_DEFAULT_SYMBOL', 'DB::S')

    data = otp.Ticks({'X': [1, 2, 3, 4], 'TICKER': ['A', 'B', 'B', 'A']})
    data = otp.SplitQueryOutputBySymbol(data, data['TICKER'])
    df = otp.run(data, start=otp.datetime(2003, 12, 1), end=otp.datetime(2003, 12, 2), symbols='DB::A')
    assert list(df['X']) == [1, 4]


def test_join_with_query(real_session):
    t = otp.Tick(A=1)
    t = t.join_with_query(otp.Tick(B=2))
    df = otp.run(t, start=otp.datetime(2003, 12, 1), end=otp.datetime(2003, 12, 2), symbols='DB::S')
    assert df['A'][0] == 1
    assert df['B'][0] == 2
