import pytest

import onetick.py as otp


START = otp.dt(2024, 2, 1, 9, 30)
END = otp.dt(2024, 2, 1, 9, 35)


@pytest.fixture(scope="module", autouse=True)
def session(m_session, cloud_server):
    m_session.use(cloud_server)
    yield m_session


def get_data():
    data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL')
    data = data[['PRICE', 'SIZE']]
    return data


def get_expected_times(size):
    df = otp.run(get_data(), start=START, end=END)
    times = set(df[df['SIZE'] > size]['Time'])
    assert times
    return times


@pytest.mark.parametrize('insert_before', [True, False])
def test_insert_heartbeat(insert_before):
    expected = otp.run(get_data(), start=START, end=END)

    data = get_data()
    data = data.insert_heartbeat(where=data['SIZE'] > 100, insert_before=insert_before)
    df = otp.run(data, start=START, end=END)
    # heartbeats are not visible
    assert df.equals(expected)

    data = data.show_heartbeats()
    df = otp.run(data, start=START, end=END)
    assert set(df['CURRENT_TIME']) == get_expected_times(100)


def test_insert_heartbeat_no_where():
    data = get_data()
    data = data.insert_heartbeat()
    data = data.show_heartbeats()
    df = otp.run(data, start=START, end=END)
    assert set(df['CURRENT_TIME']) == get_expected_times(0)


@pytest.mark.parametrize('listen_mode', ['active', 'passive'])
def test_show_heartbeats(listen_mode):
    data = get_data()
    data = data.insert_heartbeat(where=data['SIZE'] > 100)
    data = data.show_heartbeats(listen_mode=listen_mode)
    assert dict(data.schema) == {'CURRENT_TIME': otp.nsectime}
    df = otp.run(data, start=START, end=END)
    assert list(df.columns) == ['Time', 'CURRENT_TIME']
    assert (df['Time'] == df['CURRENT_TIME']).all()
    assert set(df['CURRENT_TIME']) == get_expected_times(100)


def test_show_heartbeats_wrong_listen_mode():
    data = get_data()
    with pytest.raises(ValueError, match="listen_mode"):
        data.show_heartbeats(listen_mode='wrong')


def test_intercept_heartbeats():
    data = get_data()
    data = data.insert_heartbeat(where=data['SIZE'] > 100)
    data = data.insert_data_quality_event('OK', where=data['SIZE'] > 1000)
    data = data.intercept_heartbeats()

    df = otp.run(data.show_heartbeats(), start=START, end=END)
    assert df.empty

    df = otp.run(data.show_data_quality(), start=START, end=END)
    assert set(df['Time']) == get_expected_times(1000)
