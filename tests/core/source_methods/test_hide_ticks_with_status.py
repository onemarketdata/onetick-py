import pytest

import onetick.py as otp


@pytest.fixture(scope="module", autouse=True)
def session(m_session, cloud_server):
    m_session.use(cloud_server)
    yield m_session


def get_data():
    data = otp.DataSource(db='CME_SAMPLE', tick_type='PRL_FULL', symbols=r'NQ\H24')
    data = data.show_hidden_ticks()
    data = data[['UPDATE_TYPE', 'PRICE', 'SIZE', 'RECORD_TYPE', 'TICK_STATUS']]
    return data


def test_hide_ticks_with_status():
    data = get_data()
    data = data.limit(100)
    df = otp.run(data, date=otp.dt(2024, 2, 1))
    assert len(df) == 100
    assert set(df['TICK_STATUS']) == {0, 32, 33}

    data = get_data()
    data = data.hide_ticks_with_status()
    data = data.limit(100)
    df = otp.run(data, date=otp.dt(2024, 2, 1))
    assert len(df) == 100
    assert set(df['TICK_STATUS']) == {0}
