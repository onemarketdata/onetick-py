import pytest

import onetick.py as otp

from datetime import date, datetime


if not otp.compatibility.is_start_time_as_minimum_start_date_supported():
    pytest.skip("Doesn't work on old OneTick versions", allow_module_level=True)


@pytest.fixture(scope='module')
def session(session):
    # first creating database where we can write data to all dates
    db = otp.DB('DB_LIMITED',
                db_properties={'day_boundary_tz': 'GMT'})
    db.add(
        otp.Ticks({'A': [1, 2, 3, 4, 5]}),
        tick_type='TT',
        symbol='S',
        date=otp.dt(2022, 1, 2),
        timezone='GMT',
    )
    # unaccessible date
    db.add(
        otp.Ticks({'A': [6, 7, 8, 9, 10]}),
        tick_type='TT2',
        symbol='S2',
        date=otp.dt(2022, 1, 3),
        timezone='GMT',
    )
    session.use(db)
    # deleting previous database and create new limited database with the same data
    session.locator.remove(db)
    session.acl.remove(db)
    db_with_limited_date = otp.DB('DB_LIMITED',
                                  minimum_start_date=otp.dt(2022, 1, 2, 1, 2, 3),
                                  maximum_end_date=otp.dt(2022, 1, 3),
                                  db_properties={'day_boundary_tz': 'GMT'},
                                  db_locations=db._db_locations)
    session.use(db_with_limited_date)
    yield session


class TestDbWithLimitedDate:

    def test_inspection(self, session):
        dbs = otp.databases()
        assert 'DB_LIMITED' in dbs
        db = dbs['DB_LIMITED']

        info = db.access_info()
        assert info['MIN_START_DATE_SET'] == 1
        assert info['MIN_START_DATE_MSEC'] == otp.dt(2022, 1, 2)
        assert info['MAX_END_DATE_SET'] == 1
        assert info['MAX_END_DATE_MSEC'] == otp.dt(2022, 1, 3)
        assert db.min_acl_start_date == date(2022, 1, 2)
        assert db.max_acl_end_date == date(2022, 1, 3)

        assert db.dates() == [date(2022, 1, 2), date(2022, 1, 3)]
        assert db.dates(respect_acl=True) == [date(2022, 1, 2)]
        assert db.last_date == date(2022, 1, 2)

        # unaccessible date
        with pytest.raises(ValueError, match='Date 2022-01-03 GMT violates ACL rules for the database DB_LIMITED'):
            db.tick_types(otp.dt(2022, 1, 3), timezone='GMT')
        with pytest.raises(ValueError, match='Date 2022-01-03 GMT violates ACL rules for the database DB_LIMITED'):
            db.schema(otp.dt(2022, 1, 3), timezone='GMT')
        # correct date, but incorrect default timezone, should still work
        assert db.tick_types(otp.dt(2022, 1, 2)) == ['TT']
        assert db.schema(otp.dt(2022, 1, 2)) == {'A': int}
        # last accessible date
        assert db.tick_types() == ['TT']
        assert db.schema() == {'A': int}
        # correct date and timezone
        assert db.tick_types(otp.dt(2022, 1, 2), timezone='GMT') == ['TT']
        assert db.schema(otp.dt(2022, 1, 2), timezone='GMT') == {'A': int}
        # for some reason FindDbSymbols doesn't check ACL violation
        assert db.symbols(otp.dt(2022, 1, 3), timezone='GMT') == ['S2']
        assert db.symbols(otp.dt(2022, 1, 2), timezone='GMT') == ['S']

    def test_run_manual(self, session):
        data = otp.DataSource('DB_LIMITED', tick_type='TT', schema_policy='manual')
        with pytest.raises(Exception, match='query start time violated minimum start date criteria'):
            otp.run(data, symbols='S', start=otp.dt(2022, 1, 2) - otp.Milli(1), end=otp.dt(2022, 1, 3), timezone='GMT')
        with pytest.raises(Exception, match='query end time violated minimum age criteria'):
            otp.run(data, symbols='S', start=otp.dt(2022, 1, 2), end=otp.dt(2022, 1, 3) + otp.Milli(1), timezone='GMT')

        df = otp.run(data, symbols='S', start=otp.dt(2022, 1, 2), end=otp.dt(2022, 1, 3), timezone='GMT')
        assert list(df['A']) == [1, 2, 3, 4, 5]

    def test_ignore_ticks_in_unentitled_time_range(self, session, monkeypatch):
        monkeypatch.setattr(otp.config, 'ignore_ticks_in_unentitled_time_range', True)
        data = otp.DataSource('DB_LIMITED', tick_type='TT', schema_policy='manual')
        with pytest.warns(UserWarning, match='IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE'):
            df = otp.run(data, symbols='S', start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 4), timezone='GMT')
        assert list(df['A']) == [1, 2, 3, 4, 5]
        with pytest.warns(UserWarning, match='IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE'):
            data = otp.DataSource('DB_LIMITED', tick_type='TT', start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 4))
            df = otp.run(data, symbols='S', timezone='GMT')
        assert list(df['A']) == [1, 2, 3, 4, 5]

    def test_data_source(self, session, monkeypatch):
        monkeypatch.setattr(otp.config, 'tz', 'GMT')

        # custom end time is not used to get schema, so no exception here
        data = otp.DataSource('DB_LIMITED', tick_type='TT',
                              start=otp.dt(2022, 1, 2), end=otp.dt(2022, 1, 3) + otp.Milli(1))
        with pytest.raises(Exception, match='query end time violated minimum age criteria'):
            otp.run(data, symbols='S')

        data = otp.DataSource('DB_LIMITED', tick_type='TT', start=otp.dt(2022, 1, 2), end=otp.dt(2022, 1, 3))
        df = otp.run(data, symbols='S', timezone='GMT')
        assert list(df['A']) == [1, 2, 3, 4, 5]

    def test_run_tolerant(self, session, monkeypatch):
        # PY-834
        data = otp.DataSource('DB_LIMITED', tick_type='TT')
        df = otp.run(data, symbols='S', start=otp.dt(2022, 1, 2), end=otp.dt(2022, 1, 3), timezone='GMT')
        assert list(df['A']) == [1, 2, 3, 4, 5]
