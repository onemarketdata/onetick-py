import pytest
import pandas as pd

import onetick.py as otp


@pytest.fixture(scope="module", autouse=True)
def session(m_session, cloud_server):
    m_session.use(cloud_server)
    yield m_session


def get_data():
    data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL', date=otp.dt(2024, 2, 1),
                          back_to_first_tick=otp.Day(1), max_back_ticks_to_prepend=100)
    data = data[['PRICE', 'SIZE']]
    data['TICK_TIME'] = data['TIMESTAMP']
    return data


def test_show_initialization_ticks():
    data = get_data()
    data = data.show_initialization_ticks()
    df = otp.run(data)
    assert len(df) == 100
    # timestamps are set to the start time of the query
    assert (df['Time'] == pd.Timestamp(2024, 2, 1)).all()
    assert (df['TICK_TIME'] < pd.Timestamp(2024, 2, 1)).all()
    # ticks are propagated in reverse order
    assert df['TICK_TIME'].is_monotonic_decreasing


def test_intercept_initialization_ticks():
    data = get_data()
    data = data.intercept_initialization_ticks()
    data = data.show_initialization_ticks()
    df = otp.run(data)
    assert df.empty
