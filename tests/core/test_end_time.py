import pytest
import onetick.py as otp


tz = 'GMT'
day_ns = 24 * 60 * 60 * 1_000_000_000


@pytest.fixture(scope='module', autouse=True)
def session(m_session):
    t_db = otp.DB('DB', db_locations=[{'day_boundary_tz': tz}])
    ticks = otp.Ticks({
        'A': [-1, -2],
        'offset': [otp.Nano(day_ns - 1), otp.Nano(day_ns - 2)],
    })
    t_db.add(ticks, symbol='AA', tick_type='TT', date=otp.date(2022, 1, 1), timezone=tz)

    ticks = otp.Ticks({
        'A': [0, 1, 2],
        'offset': [otp.Nano(0), otp.Nano(1), otp.Nano(2)],
    })
    t_db.add(ticks, symbol='AA', tick_type='TT', date=otp.date(2022, 1, 2), timezone=tz)

    m_session.use(t_db)
    yield m_session


def test_onetick_end_time_is_not_included_in_time_interval():
    data = otp.DataSource('DB', tick_type='TT', symbols='AA')
    df = otp.run(
        data,
        start=otp.datetime(2022, 1, 1, tz=tz),
        end=otp.datetime(2022, 1, 3, tz=tz),
        timezone=tz,
    )
    assert len(df) == 5
    df = otp.run(
        data,
        start=otp.datetime(2022, 1, 1, tz=tz),
        end=otp.datetime(2022, 1, 2, tz=tz),
        timezone=tz,
    )
    assert len(df) == 2
    df = otp.run(
        data,
        start=otp.datetime(2022, 1, 1, tz=tz),
        end=otp.datetime(2022, 1, 1, 23, 59, 59, 999999, 999, tz=tz),
        timezone=tz,
    )
    assert len(df) == 1
