import os
import onetick.py as otp
import pytest
from datetime import datetime


class TestDefaultDB:

    @pytest.fixture
    def empty_session(self):
        locator = otp.Locator(empty=True)
        cfg = otp.Config(locator=locator)

        with otp.Session(cfg) as s:
            yield s

    @pytest.mark.parametrize("db", [None, "MY_DB", "OTHER_DB"])
    def test_error(self, monkeypatch, db, empty_session):
        if db:
            monkeypatch.setattr(otp.config, 'default_db', otp.config.default)
            monkeypatch.setenv('OTP_DEFAULT_DB', db)
        db = 'DEMO_L1' if db is None else db
        assert db not in empty_session.databases

    @pytest.mark.parametrize("db", ['MY_DB', None, 'SOME_DB'])
    def test_application(self, cur_dir, monkeypatch, db):
        """
        Validates that a database is applied in the all usages
        """
        if db:
            monkeypatch.setattr(otp.config, 'default_db', otp.config.default)
            monkeypatch.setenv('OTP_DEFAULT_DB', db)

        db = 'DEMO_L1' if db is None else db

        with otp.Session() as s:
            dbs = s.databases
            assert db in dbs
            assert 'COMMON' in dbs
            assert len(s.databases) == 2

            # check that Tick works fine
            assert len(otp.run(otp.Tick(X=1))) == 1

            # check that Ticks works fine
            assert len(otp.run(otp.Ticks(X=[1, 2]))) == 2

            # check that Empty works fine
            assert len(otp.run(otp.Empty())) == 0

            # check that otp.query works
            q = otp.query(os.path.join(cur_dir, 'otqs', 'update1.otq') + '::update')
            res = otp.run(q(otp.Ticks(x=[1, 2, 3])))
            assert len(res) == 3
            assert all(res['x'] == [2, 4, 6])

            # check adding database
            db = otp.DB(name="TESTDEFAULTDB")
            s.use(db)
            db.add(otp.Tick(X=1))


@pytest.mark.parametrize('start', [None, datetime(2003, 12, 1, 16, 32, 17), datetime(2003, 12, 2)])
def test_default_start(start, monkeypatch):
    if start:
        monkeypatch.setattr(otp.config, 'default_start_time', otp.config.default)
        monkeypatch.setenv('OTP_DEFAULT_START_TIME', start.strftime("%Y/%m/%d %H:%M:%S"))
    else:
        start = datetime(2003, 12, 1)

    with otp.Session() as s:

        # check that tick start datetime is changing
        data = otp.run(otp.Tick(X=1))

        assert data['Time'][0] == start

        # check default timestamp for writing into a db
        db = otp.DB("TEST_DEFAULT_START")
        s.use(db)
        res = db.add(otp.Tick(X=1), propagate=True)
        assert res['Time'][0] == start


@pytest.mark.parametrize('end', [None, datetime(2003, 12, 6, 16, 32, 17), datetime(2003, 12, 9)])
def test_default_end(end, monkeypatch):
    if end:
        monkeypatch.setattr(otp.config, 'default_end_time', otp.config.default)
        monkeypatch.setenv('OTP_DEFAULT_END_TIME', end.strftime("%Y/%m/%d %H:%M:%S.%f"))
    else:
        end = datetime(2003, 12, 4)

    with otp.Session():
        # check that tick start datetime is changing
        data = otp.Tick(X=1)
        data['ET'] = data._END_TIME
        res = otp.run(data)

        assert res['ET'][0] == end


@pytest.mark.parametrize('tz', ['EST5EDT', 'GMT'])
@pytest.mark.parametrize('method', ['env', 'attr'])
def test_default_tz(tz, monkeypatch, method):
    monkeypatch.setattr(otp.config, 'tz', otp.config.default)
    if method == 'env':
        monkeypatch.setenv('OTP_DEFAULT_TZ', tz)
    else:
        monkeypatch.setattr(otp.config, 'tz', tz)

    with otp.Session() as s:
        db = otp.DB('TESTDB')
        s.use(db)
        db.add(otp.Ticks(X=[1, 2, 3]))

        data = otp.DataSource(db=db)
        data['TZ'] = data['_TIMEZONE']

        result = otp.run(data)

        assert len(result) == 3
        assert all(result['X'] == [1, 2, 3])
        assert all(result['TZ'] == tz)


def test_real_defaults(monkeypatch):
    for option in otp.config.get_changeable_config_options():
        monkeypatch.setattr(otp.config, option, otp.config.default)
    assert otp.config['tz'] is None
    with pytest.raises(ValueError):
        assert otp.config['default_db'] is None
    with pytest.raises(ValueError):
        assert otp.config['default_symbol'] is None
    with pytest.raises(ValueError):
        assert otp.config['default_db_symbol'] is None
    with pytest.raises(ValueError):
        assert otp.config['default_start_time'] is None
    with pytest.raises(ValueError):
        assert otp.config['default_end_time'] is None
    with pytest.raises(ValueError):
        assert otp.config['default_date'] is None


def test_conftest_defaults():
    assert otp.config['tz'] == 'EST5EDT'
    assert otp.config['default_db'] == 'DEMO_L1'
    assert otp.config['default_symbol'] == 'AAPL'
    assert otp.config['default_db_symbol'] == 'DEMO_L1::AAPL'
    assert otp.config['default_start_time'] == datetime(2003, 12, 1, 0, 0, 0)
    assert otp.config['default_end_time'] == datetime(2003, 12, 4, 0, 0, 0)
    assert otp.config['default_date'] == datetime(2003, 12, 1)


def test_deprecated_vars():
    with pytest.warns(FutureWarning):
        assert otp.DEFAULT_TZ == 'EST5EDT'
    with pytest.warns(FutureWarning):
        assert otp.DEFAULT_DB == 'DEMO_L1'
    with pytest.warns(FutureWarning):
        assert otp.DEFAULT_SYMBOL == 'AAPL'
    with pytest.warns(FutureWarning):
        assert otp.DEFAULT_DB_SYMBOL == 'DEMO_L1::AAPL'
    with pytest.warns(FutureWarning):
        assert otp.DEFAULT_START_TIME == datetime(2003, 12, 1, 0, 0, 0)
    with pytest.warns(FutureWarning):
        assert otp.DEFAULT_END_TIME == datetime(2003, 12, 4, 0, 0, 0)
