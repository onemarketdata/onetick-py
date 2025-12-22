import pytest
import onetick.py as otp


@pytest.fixture
def session(f_session):
    db = otp.DB('DB')
    f_session.use(db)
    yield f_session


@pytest.mark.parametrize('propagate', [True, False])
def test_propagate_ticks(session, propagate):
    t = otp.Tick(A=1)
    t = t.write('DB', symbol='S', tick_type='TT', propagate=propagate)
    assert set(t.schema) == {'A'}
    df = otp.run(t)
    if propagate:
        assert len(df) == 1
    else:
        assert len(df) == 0
    df = otp.run(otp.DataSource('DB', symbol='S', tick_type='TT'))
    assert len(df) == 1
    assert set(df.columns) == {'Time', 'A'}


def test_deprecated(session):
    t = otp.Tick(A=1)
    with pytest.warns(FutureWarning):
        t.write('DB', symbol='S', tick_type='TT', append_mode=False)
    with pytest.warns(FutureWarning):
        t.write('DB', symbol='S', tick_type='TT', timestamp_field='A')
    with pytest.warns(FutureWarning):
        t.write('DB', symbol='S', tick_type='TT',
                start=otp.config['default_start_time'],
                end=otp.config['default_start_time'] + otp.Day(2))


def test_inplace(session):
    t = otp.Tick(A=1)
    kwargs = dict(db='DB', symbol='S', tick_type='TT',
                  propagate=True, append=True)
    t = t.write(**kwargs)
    t = t.write(**kwargs, inplace=False)
    t.write(**kwargs, inplace=True)
    otp.run(t)
    df = otp.run(otp.DataSource('DB', symbol='S', tick_type='TT'))
    assert len(df) == 3


def test_inplace_multidate_exception(session):
    t = otp.Tick(A=1)
    kwargs = dict(db='DB', symbol='S', tick_type='TT',
                  propagate=True, append=True)
    with pytest.raises(Exception):
        t.write(**kwargs,
                inplace=True,
                start=otp.config['default_start_time'],
                end=otp.config['default_start_time'] + otp.Day(2))


class TestSymbolNameAndTickType:

    def test_symbol_and_tick_type(self, session):
        t = otp.Tick(A=1)
        t = t.write('DB', symbol='S', tick_type='TT')
        assert set(t.schema) == {'A'}
        otp.run(t)
        df = otp.run(otp.DataSource('DB', symbol='S', tick_type='TT'))
        assert len(df) == 1
        assert set(df.columns) == {'Time', 'A'}

    def test_symbol_and_tick_type_as_column(self, session):
        t = otp.Tick(A=1, S='S', TT='TT')
        t = t.write('DB', symbol=t['S'], tick_type=t['TT'])
        assert set(t.schema) == {'A', 'S', 'TT'}
        otp.run(t)
        df = otp.run(otp.DataSource('DB', symbol='S', tick_type='TT'))
        assert len(df) == 1
        assert set(df.columns) == {'Time', 'A', 'S', 'TT'}

    def test_symbol_and_tick_type_mixed(self, session):
        t = otp.Tick(A=1, S='S')
        t = t.write('DB', symbol=t['S'], tick_type='TT')
        assert set(t.schema) == {'A', 'S'}
        otp.run(t)
        df = otp.run(otp.DataSource('DB', symbol='S', tick_type='TT'))
        assert len(df) == 1
        assert set(df.columns) == {'Time', 'A', 'S', '_TICK_TYPE_FIELD_'}

    @pytest.mark.parametrize('keep_symbol_and_tick_type', [True, False])
    def test_keep_symbol_and_tick_type(self, session, keep_symbol_and_tick_type):
        t = otp.Tick(A=1)
        t['X'] = 'S'
        t['Y'] = 'TT'
        t = t.write('DB',
                    symbol=t['X'], tick_type=t['Y'],
                    propagate=True,
                    keep_symbol_and_tick_type=keep_symbol_and_tick_type)
        if keep_symbol_and_tick_type:
            assert set(t.schema) == {'A', 'X', 'Y'}
        else:
            assert set(t.schema) == {'A'}
        otp.run(t)
        df = otp.run(otp.DataSource('DB', symbol='S', tick_type='TT'))
        assert len(df) == 1
        if keep_symbol_and_tick_type:
            assert set(df.columns) == {'Time', 'A', 'X', 'Y'}
        else:
            assert set(df.columns) == {'Time', 'A'}


class TestOutOfRangeAction:
    def test_exception(self, session):
        t = otp.Tick(A=1)
        t['TIMESTAMP'] = t['TIMESTAMP'] + otp.Day(2)
        t = t.write('DB', symbol='S', tick_type='TT')
        with pytest.raises(Exception):
            otp.run(t)

    def test_ignore(self, session):
        t = otp.Tick(A=1)
        t['TIMESTAMP'] = t['TIMESTAMP'] + otp.Day(2)
        t = t.write('DB', symbol='S', tick_type='TT',
                    out_of_range_tick_action='ignore')
        otp.run(t)
        with pytest.warns(match="Can't find not empty day"):
            df = otp.run(otp.DataSource('DB', symbol='S', tick_type='TT'))
        assert len(df) == 0


class TestTimestampField:
    @pytest.mark.parametrize('keep_timestamp', (True, False))
    def test_timestamp(self, session, keep_timestamp):
        t = otp.Tick(A=1)
        t['T2'] = t['TIMESTAMP'] + otp.Hour(1)
        t = t.write('DB', symbol='S', tick_type='TT',
                    timestamp=t['T2'], keep_timestamp=keep_timestamp)
        if keep_timestamp:
            assert 'T2' in t.schema
        else:
            assert 'T2' not in t.schema
        otp.run(t)
        df = otp.run(otp.DataSource('DB', symbol='S', tick_type='TT'))
        if keep_timestamp:
            assert 'T2' in df
        else:
            assert 'T2' not in df
        assert df['Time'][0] == otp.config['default_start_time'] + otp.Hour(1)


def test_write_start_end(session):
    days = 10
    start = otp.dt(2022, 1, 1)
    t = otp.Ticks(A=range(days),
                  offset=[otp.Day(i) for i in range(days)],
                  start=start,
                  end=start + otp.Day(days))

    src = t.write('DB', symbol='S', tick_type='TT',
                  start_date=start,
                  end_date=start + otp.Day(days))

    df = otp.run(src)
    assert len(df) == days
    assert all(df.columns == ["Time", "A"])
    assert df['Time'].min() == start
    assert df['Time'].max() == start + otp.Day(days - 1)


@pytest.mark.parametrize('day_boundary_tz,tz,out_of_range_tick_action,len_result', [
    # good cases, timezones are the same
    (None, None, None, 1),
    ('GMT', 'GMT', None, 1),
    ('GMT', 'GMT', 'EXCEPTION', 1),
    ('GMT', 'GMT', 'IGNORE', 1),
    # bad cases, timezones are not the same
    (None, 'GMT', 'EXCEPTION', None),
    (None, 'GMT', 'IGNORE', 0),
    # test default behavior, should raise exception
    (None, 'GMT', None, None),
])
def test_wrong_day_boundary_tz(f_session, day_boundary_tz, tz, out_of_range_tick_action, len_result):
    kwargs = {}
    if day_boundary_tz is not None:
        kwargs['db_locations'] = [{
            'day_boundary_tz': day_boundary_tz,
        }]
    t_db = otp.DB('DB', **kwargs)

    tick_type, symbol = 'TT', 'AA'
    kwargs = dict(
        symbol=symbol,
        tick_type=tick_type,
        start=otp.date(2022, 1, 1),
        end=otp.date(2022, 1, 2),
    )
    if tz is not None:
        kwargs['timezone'] = tz
    if out_of_range_tick_action is not None:
        kwargs['out_of_range_tick_action'] = out_of_range_tick_action

    if len_result is None:
        with pytest.raises(Exception, match='of a tick, visible or hidden, earlier than 2022-01-01'):
            t_db.add(otp.Tick(A=1), **kwargs)
        return

    t_db.add(otp.Tick(A=1), **kwargs)
    f_session.use(t_db)


def test_write_with_day_boundary_offset_hhmmss(f_session):
    """quite long test, need to compare original write with date
    and workaround implementation with start+end.
    comparing behaviour similarity and exception messages formatting."""

    # create a db with a day boundary offset of -01:30:00 (22:30 is the day boundary)
    kwargs = {}
    kwargs['db_locations'] = [{
        'day_boundary_tz': 'GMT',
        'day_boundary_offset_hhmmss': '-013000',
    }]
    t_db = otp.DB('DB', **kwargs)
    f_session.use(t_db)

    # ticks for a full day each hour (shifted by 111 milliseconds to verify exception formatting)
    hours = 24
    start = otp.dt(2022, 1, 1, tz="GMT")
    t = otp.Ticks(A=range(hours),
                  offset=[111 + i * 1000 * 60 * 60 for i in range(hours)],
                  start=start,
                  end=start + otp.Day(1))

    # write with `date` and get exception on 23:00:00 tick
    t2 = t.write('DB',
                 symbol='S',
                 tick_type='TT',
                 date=start,
                 propagate=True,)
    with pytest.raises(Exception, match='Timestamp 20220101230000.111000000000 of a tick, visible or hidden,  '
                                        'falls outside 20220101 in GMT timezone'):
        otp.run(t2, symbols="DB::S", timezone="GMT")

    # write with `start`/`end` and get similar exception on 23:00:00 tick
    t2 = t.write('DB',
                 symbol='S',
                 tick_type='TT',
                 start_date=start,
                 end_date=start,
                 propagate=True,)
    with pytest.raises(Exception, match='Timestamp 20220101230000.111000000 of a tick, visible or hidden, '
                                        'later than 2022-01-02 in timezone GMT.'):
        otp.run(t2, symbols="DB::S", timezone="GMT")

    # check that nothing is written yet
    df = otp.run(otp.DataSource('DB', symbols="S", tick_type="TT"),
                 timezone="GMT",
                 start=start,
                 end=start + otp.Day(2))
    assert len(df) == 0

    # now really write and check that everything is ok
    t2 = t.write('DB',
                 symbol='S',
                 tick_type='TT',
                 start_date=start,
                 end_date=start + otp.Day(1),
                 propagate=True,)
    df = otp.run(t2, symbols="DB::S", timezone="GMT")
    assert len(df) == hours

    # read DB and verify all 24 ticks are written
    df = otp.run(otp.DataSource('DB', symbols="S", tick_type="TT"),
                 timezone="GMT",
                 start=start,
                 end=start + otp.Day(2))
    assert len(df) == hours


def test_out_of_range_tick_action_load(f_session):
    t_db = otp.DB(
        'DB',
        db_locations=[{'day_boundary_tz': 'GMT'}],
        db_properties={
            'tick_search_max_day_boundary_offset_sec': 60 * 60 * 23
        })
    f_session.use(t_db)

    hours = 25
    start = otp.dt(2022, 1, 1, tz="GMT")
    t = otp.Ticks(A=range(hours),
                  offset=[i * 1000 * 60 * 60 for i in range(hours)],
                  start=start,
                  end=start + otp.Day(2)
                  )
    t2 = t.write('DB',
                 symbol='S',
                 tick_type='TT',
                 date=start,
                 out_of_range_tick_action='load',
                 propagate=True,)
    otp.run(t2, symbols="DB::S", timezone="GMT")

    # read DB and verify all 25 ticks are written, despite it is out of day range
    df = otp.run(otp.DataSource('DB', symbols="S", tick_type="TT"),
                 timezone="GMT",
                 start=start,
                 end=start + otp.Day(2))
    ticks = otp.run(t, timezone="GMT", start=start, end=start + otp.Day(2))
    assert len(df) == len(ticks)


@pytest.mark.parametrize('allow_lowercase_in_saved_fields', (True, False))
def test_lowercase_field_names(f_session, allow_lowercase_in_saved_fields):

    otp.config.allow_lowercase_in_saved_fields = allow_lowercase_in_saved_fields

    t_db = otp.DB('DB')
    f_session.use(t_db)

    src = otp.Tick(lowercase=1)

    df = otp.run(src)
    assert src.schema['lowercase'] is int
    assert df['lowercase'][0] == 1

    if not otp.config.allow_lowercase_in_saved_fields:
        with pytest.raises(ValueError, match='Field "lowercase" contains lowercase characters'):
            src.write(db='DB', symbol='SYMBOL_NAME', tick_type='TT', date=otp.dt(2024, 3, 20))
    else:
        with pytest.warns(UserWarning, match='Field "lowercase" contains lowercase characters'):
            src = src.write(db='DB', symbol='SYMBOL_NAME', tick_type='TT', date=otp.dt(2024, 3, 20))

        df = otp.run(src, date=otp.dt(2024, 3, 20))
        assert src.schema['lowercase'] is int
        assert df['lowercase'][0] == 1

        src = otp.DataSource('DB', symbol='SYMBOL_NAME', tick_type='TT', date=otp.dt(2024, 3, 20))
        df = otp.run(src)
        assert src.schema['LOWERCASE'] is int
        assert df['LOWERCASE'][0] == 1

    otp.config.allow_lowercase_in_saved_fields = otp.config.default


def test_db_add_without_symbol(session):
    src = otp.Tick(A=1)
    dt = otp.dt('2003-12-01')
    src = src.write(db='DB', tick_type='TEST_TICK_TYPE', date=dt)
    otp.run(src)

    df = otp.run(otp.DataSource('DB', tick_type='TEST_TICK_TYPE'), date=dt)
    assert len(df) == 1
    assert set(df.columns) == {'Time', 'A'}
    assert list(df['A']) == [1]


@pytest.mark.parametrize('set_date', (True, False))
def test_start_end_with_day_boundary_offset(session, set_date):
    # PY-1339
    day_boundary_offset_db = otp.DB(
        'DB_DAY_BOUNDARY_TZ',
        db_properties={
            'day_boundary_tz': 'UTC',
            'day_boundary_offset_hhmmss': '-120000',
            'tick_timestamp_type': 'NANOS',
            'symbology': 'BZX'
        },
        db_locations=[{
            'access_method': 'file',
            'start_time': otp.dt(2023, 1, 1),
            'end_time': otp.dt(2023, 12, 31),
        }]
    )
    session.use(day_boundary_offset_db)
    date = otp.dt(2023, 1, 1)
    # ticks for a full day each hour
    ticks = otp.Ticks({
        'A': list(range(24)),
        'offset': [i * 60 * 60 * 1000 for i in range(24)],
    })
    if set_date:
        kwargs = dict(date=date)
    else:
        kwargs = dict(
            start_date=date,
            end_date=date,
        )
    write_query = ticks.write(
        db='DB_DAY_BOUNDARY_TZ',
        symbol='SYM',
        tick_type='TT',
        out_of_range_tick_action='ignore',
        **kwargs,
    )
    df = otp.run(write_query, timezone="UTC", date=date)
    assert list(df['A']) == list(range(12))
    data = otp.DataSource('DB_DAY_BOUNDARY_TZ', symbol='SYM', tick_type='TT', schema_policy='manual')
    df = otp.run(data, timezone="UTC", date=date)
    assert list(df['A']) == list(range(12))


def test_multiple_locations_case(session):
    import datetime
    db_name = 'MULTI_LOC_DB'
    db = otp.DB(
        db_name,
        db_locations=[
            {
                'start_time': datetime.datetime(2023, 1, 1),
                'end_time': datetime.datetime(2025, 1, 1),
                'day_boundary_tz': 'UTC',
            },
            {
                'start_time': datetime.datetime(2025, 1, 1),
                'end_time': datetime.datetime(2030, 12, 31),
                'day_boundary_tz': 'UTC',
            }
        ]
    )
    session.use(db)
    date = otp.dt(2023, 1, 1)
    ticks = otp.Ticks({'A': list(range(5))}, start=date, end=date + otp.Hour(1))
    write_query = ticks.write(
        db=db_name,
        symbol='SYM',
        tick_type='TT',
        start_date=date,
        end_date=date,
        append=False,
        out_of_range_tick_action='exception'
    )

    df = otp.run(write_query, timezone="UTC")

    assert list(df['A']) == [0, 1, 2, 3, 4]
