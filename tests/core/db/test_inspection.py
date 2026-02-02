import pytest
import os
from datetime import datetime, date, timedelta
import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.db._inspection import databases
from onetick.py.compatibility import is_show_db_list_show_description_supported


def cmp_lists(l1, l2):
    return all((x == y for x, y in zip(sorted(l1), sorted(l2))))


def test_get_databases(f_session):
    assert cmp_lists(databases().keys(), ['DEMO_L1', 'COMMON'])

    f_session.use(otp.DB('ANOTHER_DB'))

    assert cmp_lists(databases().keys(), ['ANOTHER_DB', 'DEMO_L1', 'COMMON'])


def test_dates(f_session):
    db = otp.DB('MY_DB',
                db_locations=[dict(location=otp.utils.TmpDir(),
                                   start_time=datetime(2003, 1, 2),
                                   end_time=datetime(2010, 3, 4)),
                              dict(location=otp.utils.TmpDir(),
                                   start_time=datetime(2010, 3, 4),
                                   end_time=datetime(2019, 7, 10)),
                              dict(location=otp.utils.TmpDir(),
                                   start_time=datetime(2019, 7, 11),
                                   end_time=datetime(2025, 11, 4))])
    db.add(otp.Ticks(X=[1, 2]), date=otp.dt(2009, 1, 7))
    db.add(otp.Ticks(Y=[3, 4]), date=otp.dt(2011, 1, 1))
    db.add(otp.Ticks(Z=[4, 5]), date=otp.dt(2011, 1, 2))
    db.add(otp.Ticks(X=[5, 6]), date=otp.dt(2011, 1, 4))

    f_session.use(db)

    dbs = databases()

    assert cmp_lists([date(2009, 1, 7), date(2011, 1, 1), date(2011, 1, 2), date(2011, 1, 4)],
                     dbs['MY_DB'].dates())
    assert dbs['MY_DB'].last_date == date(2011, 1, 4)


def test_empty_dates(f_session):
    f_session.use(otp.DB('MY_DB'))
    dbs = databases()

    assert dbs['MY_DB'].dates() == []
    assert dbs['MY_DB'].last_date is None
    assert dbs['MY_DB'].schema(tick_type='TRD') == {}

    data = otp.run(otp.DataSource('MY_DB'))
    assert data.empty

    data = otp.run(otp.DataSource('MY_DB', tick_type='QTE'))
    assert data.empty

    data = otp.run(otp.DataSource('MY_DB', symbol='ABC'))
    assert data.empty


def test_tick_types(f_session):
    db = otp.DB('MY_DB')
    f_session.use(db)

    db.add(otp.Tick(X=1), tick_type='A', date=otp.dt(2009, 1, 1))
    db.add(otp.Tick(X=2), tick_type='B', date=otp.dt(2009, 1, 2))
    db.add(otp.Tick(X=3), tick_type='C', date=otp.dt(2009, 1, 3))
    db.add(otp.Tick(X=4), tick_type='D', date=otp.dt(2009, 1, 3))

    dbs = databases()
    my_db = dbs['MY_DB']

    assert my_db.tick_types(date(2008, 1, 1)) == []
    assert my_db.tick_types(date(2009, 1, 1)) == ['A']
    assert my_db.tick_types(date(2009, 1, 2)) == ['B']
    assert cmp_lists(my_db.tick_types(), ['C', 'D'])  # takes last day


def test_symbols(f_session):
    db = otp.DB('MY_DB')
    f_session.use(db)

    db.add(otp.Ticks(X=[1]), symbol='A', tick_type='T1', date=otp.dt(2009, 1, 1))
    db.add(otp.Ticks(Y=[1]), symbol='B', tick_type='T2', date=otp.dt(2009, 1, 2))
    db.add(otp.Ticks(Z=[1]), symbol='C', tick_type='T3', date=otp.dt(2009, 1, 2))

    dbs = databases()
    db = dbs['MY_DB']

    symbols = db.symbols()
    assert symbols == ['B', 'C']

    symbols = db.symbols(tick_type='T1')
    assert symbols == []

    symbols = db.symbols(tick_type='T3')
    assert symbols == ['C']

    symbols = db.symbols(date=otp.dt(2009, 1, 1))
    assert symbols == ['A']

    symbols = db.symbols(date=otp.dt(2009, 1, 3))
    assert symbols == []


def test_symbols_long_symbol_name(f_session):
    db = otp.DB('MY_DB')
    f_session.use(db)

    long_symbol_name = 'S' * 100
    db.add(otp.Ticks(X=[1]), symbol=long_symbol_name, tick_type='TT', date=otp.dt(2009, 1, 1))

    dbs = databases()
    db = dbs['MY_DB']

    symbols = db.symbols()
    assert symbols == [long_symbol_name]


def test_schema(f_session):
    db = otp.DB('MY_DB')
    f_session.use(db)

    data = otp.Ticks(X=[1])
    fields = 'byte BF=0,' \
             'short SHF=0,' \
             'long LF=0,' \
             'int IF=0,' \
             'uint UIF=0,' \
             'ulong ULF=0,' \
             'decimal DF=0.0,' \
             'string SF="",' \
             'string[4096] CSF="",' \
             'msectime MSF=0,' \
             'nsectime NSF=0,' \
             'varstring VSF="xxxxxxxxxxxxxxxxxxxxxxxxx"'
    data.sink(otq.AddFields(fields=fields))
    db.add(data, tick_type='A', symbol='S1', date=otp.dt(2009, 1, 1))
    db.add(otp.Ticks(Y=[.3], Z=['X']), tick_type='A', symbol='S2', date=otp.dt(2009, 1, 1))

    dbs = databases()
    db = dbs['MY_DB']

    schema = db.schema()

    assert schema['X'] is int
    assert schema['BF'] is otp.byte
    assert schema['SHF'] is otp.short
    assert schema['LF'] is int
    assert schema['IF'] is otp.int
    assert schema['UIF'] is otp.uint
    assert schema['ULF'] is otp.ulong
    assert schema['SF'] is str
    assert schema['CSF'] is otp.string[4096]
    assert schema['MSF'] is otp.msectime
    assert schema['NSF'] is otp.nsectime
    assert schema['VSF'] is otp.varstring
    assert schema['Y'] is float
    assert schema['DF'] is otp.decimal


def test_different_days_schema(f_session):
    ''' There is logic that fulfill lookups for multiple days for
    cases when there is no data due the weekends and holidays. Check
    how it works in case of different schemas '''
    db = otp.DB('MY_DB')
    f_session.use(db)

    data = otp.Tick(X=1, Y=.1, Z='ss')
    db.add(data, tick_type='TT', symbol='S1', date=otp.dt(2009, 1, 5))
    data = otp.Tick(A='abc')
    db.add(data, tick_type='TT', symbol='S1', date=otp.dt(2009, 1, 3))
    data = otp.Tick(X='abc')
    db.add(data, tick_type='TT', symbol='S1', date=otp.dt(2009, 1, 4))

    dbs = databases()
    db = dbs['MY_DB']

    assert db.schema() == {'X': int, 'Y': float, 'Z': str}
    assert db.schema(date=otp.dt(2009, 1, 5)) == {'X': int, 'Y': float, 'Z': str}
    assert db.schema(date=otp.dt(2009, 1, 4)) == {'X': str}
    assert db.schema(date=otp.dt(2009, 1, 3)) == {'A': str}


@pytest.mark.parametrize('tz', ['GMT', 'EST5EDT', 'Etc/GMT+10', 'Etc/GMT+4',
                                'Etc/GMT-0', 'Etc/GMT-1', 'Etc/GMT-12',
                                'Etc/GMT-14', 'Etc/GMT-6', 'UTC', 'Etc/GMT+12'])
def test_timezone(tz, f_session):
    ''' Seems that all methods are timezone independent '''
    db = otp.DB('MY_DB',
                db_locations=[dict(day_boundary_tz=tz)])
    f_session.use(db)

    db.add(otp.Tick(X=1), timezone=tz, date=otp.dt(2005, 1, 1))

    db = databases()['MY_DB']

    schema = db.schema()
    assert len(schema) == 1 and 'X' in schema

    assert db.tick_types() == ['TRD']
    assert db.symbols() == ['AAPL']
    assert db.dates() == [date(2005, 1, 1)]


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='RemoteTS is not applicable for WebAPI')
@pytest.mark.integration
@pytest.mark.skipif(os.name == "nt", reason="Default windows user in CI doesn't have access")
def test_remote(cloud_server):
    with otp.Session(
        otp.Config(locator=cloud_server,
                   # needed for PY-1463
                   variables={'USE_GRANT_LICENSE_MODE': False},
                   license=otp.license.Default())
    ) as s:
        from pathlib import Path
        print(Path(s.config.file).read_text())
        dbs = databases()

        db = dbs['US_COMP']

        print('Last date :')
        last_date = db.last_date
        print(db.last_date)
        print('Number of days :')
        print(len(db.dates()))
        print('Tick types : ')
        print(db.tick_types())
        print('Symbols : ')
        symbols = db.symbols(tick_type='QTE')
        print(len(symbols), symbols[-1])
        print('Schema : ')
        print(db.schema(tick_type='QTE'))

        start = datetime(last_date.year, last_date.month, last_date.day, 9, 30)
        end = start + timedelta(hours=2)
        data = otp.DataSource(db='US_COMP', tick_type='QTE', symbol='ZZZ', start=start, end=end)
        print(data.head())

        # -----

        data = otp.DataSource(db='US_COMP',
                              tick_type='QTE',
                              symbol=['AAPL', 'GOOG'],
                              start=otp.dt(2021, 6, 21, 10),
                              end=otp.dt(2021, 6, 21, 11))

        data = data.agg({'MAX_BID': otp.agg.max(data['BID_PRICE']),
                         'MIN_BID': otp.agg.min(data['BID_PRICE']),
                         'AVG_BID': otp.agg.average(data['BID_PRICE']),
                         'FIRST_BID': otp.agg.first(data['BID_PRICE']),
                         'LAST_BID': otp.agg.last(data['BID_PRICE']),
                         'MAX_ASK': otp.agg.max(data['ASK_PRICE']),
                         'MIN_ASK': otp.agg.min(data['ASK_PRICE']),
                         'AVG_ASK': otp.agg.average(data['ASK_PRICE']),
                         'FIRST_ASK': otp.agg.first(data['ASK_PRICE']),
                         'LAST_ASK': otp.agg.last(data['ASK_PRICE']),
                         'NUM_QUOTES': otp.agg.count()}, bucket_interval=60)

        print(data.head(n=10))


@pytest.mark.integration
@pytest.mark.skipif(os.name == "nt", reason="Default windows user in CI doesn't have access")
def test_remote_service(cloud_server):
    with otp.Session(otp.Config(locator=cloud_server)):
        access_ep = otq.AccessInfo(info_type='ROLES',
                                   show_for_all_users=True)
        access_ep.set_tick_type('ANY')

        print(otp.run(access_ep, symbols='DEMO_L1::'))


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='otp.Sessions with config file makes no sense for WebAPI')
@pytest.mark.skipif(os.name == "nt", reason="License is fixed in config with Linux path")
@pytest.mark.parametrize('context,db_to_check', [
    ('DEFAULT', 'COMMON'),
    ('REMOTE', 'REMOTE_COMMON')
])
def test_context(cur_dir, monkeypatch, context, db_to_check):
    monkeypatch.setitem(otp.config, 'context', context)

    config_dir = os.path.join(cur_dir, 'cfg')
    monkeypatch.setenv('CONFIG_DIR', config_dir)
    cfg_path = os.path.join(config_dir, 'onetick.cfg')

    with otp.Session(config=cfg_path):
        assert db_to_check in otp.databases()


def test_symbols_with_colons(f_session):
    db = otp.DB('MY_DB', db_locations=[dict(location=otp.utils.TmpDir())])
    db.add(otp.Ticks(X=[1, 2]), date=otp.dt(2022, 1, 1), tick_type='TT', symbol='OK')
    db.add(otp.Ticks(X=[3, 4]), date=otp.dt(2022, 1, 1), tick_type='TT', symbol='OK:OK:OK')

    f_session.use(db)

    symbols = otp.databases()['MY_DB'].symbols(tick_type='TT', date=otp.dt(2022, 1, 1))
    assert symbols == ['OK', 'OK:OK:OK']


def test_big_dates(f_session):
    db = otp.DB('MY_DB',
                db_locations=[dict(location=otp.utils.TmpDir(),
                                   start_time=datetime(2003, 1, 2),
                                   end_time='99991231240000')])

    db.add(otp.Ticks(X=[1, 2]), date=otp.dt(2009, 1, 7))

    f_session.use(db)

    dbs = otp.databases()

    assert dbs['MY_DB'].schema() == {'X': int}
    assert dbs['MY_DB']._locator_date_ranges[0][0] == datetime(2003, 1, 2)
    assert dbs['MY_DB']._locator_date_ranges[0][1] == datetime.max


@pytest.mark.skipif(
    not is_show_db_list_show_description_supported(), reason="Not supported on this version of OneTick",
)
@pytest.mark.parametrize('readable_only', [True, False])
def test_description(f_session, readable_only):
    db = otp.DB('DESC_TEST', db_properties={'description': 'TEST'})
    f_session.use(db)

    result = otp.databases(readable_only=readable_only, fetch_description=True)
    assert result['DESC_TEST'].description == 'TEST'

    result = otp.databases(readable_only=readable_only, fetch_description=False)
    assert result['DESC_TEST'].description == ''
