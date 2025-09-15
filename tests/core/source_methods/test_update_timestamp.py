from pathlib import Path
import os
import pytest
import onetick.py as otp


@pytest.fixture(scope='module')
def session(session):
    db = otp.DB('DB')
    db.add(otp.Ticks(X=[1, 2, 3]), symbol='S1', tick_type='TT', date=otp.dt(2022, 1, 1))
    session.use(db)
    return session


def test_wrong_values(session):
    data = otp.DataSource('DB', symbols='S1', tick_type='TT')
    data['NEW_TS'] = data['TIMESTAMP'] + otp.Nano(1)
    data['S'] = data['X'].apply(str)
    with pytest.raises(ValueError, match="Field 'WRONG' is not in schema"):
        data = data.update_timestamp('WRONG')
    with pytest.raises(ValueError, match="Unsupported type for 'timestamp_field'"):
        data = data.update_timestamp('S')
    with pytest.raises(ValueError, match="Field 'WRONG' is not in schema"):
        data = data.update_timestamp('NEW_TS', timestamp_psec_field='WRONG')
    with pytest.raises(ValueError, match="Unsupported type for 'timestamp_psec_field'"):
        data = data.update_timestamp('NEW_TS', timestamp_psec_field='S')
    with pytest.raises(ValueError, match='less than one millisecond are not supported'):
        data = data.update_timestamp('NEW_TS', max_delay_of_original_timestamp=otp.Nano(1))
    with pytest.raises(ValueError, match='less than one millisecond are not supported'):
        data = data.update_timestamp('NEW_TS', max_delay_of_new_timestamp=otp.Nano(1))
    with pytest.raises(ValueError, match='less than one millisecond are not supported'):
        data = data.update_timestamp('NEW_TS', max_out_of_order_interval=otp.Nano(1))
    with pytest.raises(ValueError, match="Unsupported value for parameter 'max_delay_handling'"):
        data = data.update_timestamp('NEW_TS', max_delay_handling='WRONG')
    with pytest.raises(ValueError, match="Unsupported value for parameter 'zero_timestamp_handling'"):
        data = data.update_timestamp('NEW_TS', zero_timestamp_handling='WRONG')
    with pytest.raises(ValueError, match="Unsupported value for parameter 'out_of_order_timestamp_handling'"):
        data = data.update_timestamp('NEW_TS', out_of_order_timestamp_handling='WRONG')


def test_nanos(session):
    data = otp.DataSource('DB', symbols='S1', tick_type='TT')
    data['NEW_TS'] = data['TIMESTAMP'] + otp.Nano(1)
    data = data.update_timestamp('NEW_TS', max_delay_of_original_timestamp=otp.Milli(1))
    df = otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 2))
    assert list(df['Time']) == [otp.dt(2022, 1, 1, microsecond=1000 * i, nanosecond=1) for i in range(3)]
    assert all(df['Time'] == df['NEW_TS'])


def test_timestamp_psec_field(session):
    data = otp.DataSource('DB', symbols='S1', tick_type='TT')
    data['NEW_TS'] = data['TIMESTAMP'] + otp.Nano(1)
    data['TS_PSEC_FIELD'] = 3000
    data = data.update_timestamp('NEW_TS',
                                 timestamp_psec_field='TS_PSEC_FIELD',
                                 max_delay_of_original_timestamp=otp.Milli(1))
    df = otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 2))
    assert list(df['Time']) == [otp.dt(2022, 1, 1, microsecond=1000 * i, nanosecond=3) for i in range(3)]
    assert all(df['Time'] != df['NEW_TS'])


def test_max_delay_of_original_timestamp(session):
    data = otp.DataSource('DB', symbols='S1', tick_type='TT')
    data['NEW_TS'] = data['TIMESTAMP'] + otp.Hour(12)
    with pytest.raises(Exception, match='exceeds maximum allowed delay'):
        res = data.update_timestamp('NEW_TS', max_delay_of_original_timestamp=otp.Hour(1))
        otp.run(res, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 2))
    data['NEW_START'] = data['_START_TIME']
    data = data.update_timestamp('NEW_TS', max_delay_of_original_timestamp=otp.Hour(12))
    data['ORIG_START'] = data['_START_TIME']  # noqa: E1137 (false positive for some reason)
    df = otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 2))
    assert list(df['Time']) == [otp.dt(2022, 1, 1, hour=12, microsecond=1000 * i) for i in range(3)]
    assert all(df['Time'] == df['NEW_TS'])
    assert list(df['ORIG_START']) == [otp.dt(2022, 1, 1)] * 3
    assert list(df['NEW_START']) == [otp.dt(2021, 12, 31, hour=12)] * 3


def test_max_delay_of_new_timestamp(session):
    data = otp.DataSource('DB', symbols='S1', tick_type='TT')
    data['NEW_TS'] = data['TIMESTAMP'] - otp.Hour(12)
    with pytest.raises(Exception, match='exceeds maximum allowed delay'):
        res = data.update_timestamp('NEW_TS', max_delay_of_new_timestamp=otp.Hour(1))
        otp.run(res, start=otp.dt(2021, 12, 31), end=otp.dt(2022, 1, 2))
    data['NEW_END'] = data['_END_TIME']
    data = data.update_timestamp('NEW_TS', max_delay_of_new_timestamp=otp.Hour(12))
    data['ORIG_END'] = data['_END_TIME']  # noqa: E1137 (false positive for some reason)
    df = otp.run(data, start=otp.dt(2021, 12, 31), end=otp.dt(2022, 1, 2))
    assert list(df['Time']) == [otp.dt(2021, 12, 31, hour=12, microsecond=1000 * i) for i in range(3)]
    assert all(df['Time'] == df['NEW_TS'])
    assert list(df['ORIG_END']) == [otp.dt(2022, 1, 2)] * 3
    assert list(df['NEW_END']) == [otp.dt(2022, 1, 2, hour=12)] * 3


def test_max_out_of_order_interval(session):
    data = otp.DataSource('DB', symbols='S1', tick_type='TT')
    data['NEW_TS'] = data['TIMESTAMP'] + otp.Hour(12) - otp.Hour(data['X'])
    with pytest.raises(Exception, match='out-of-order interval is larger'):
        res = data.update_timestamp('NEW_TS',
                                    max_delay_of_original_timestamp=otp.Hour(12),
                                    max_out_of_order_interval=0)
        otp.run(res, start=otp.dt(2021, 12, 31), end=otp.dt(2022, 1, 2))
    data = data.update_timestamp('NEW_TS',
                                 max_delay_of_original_timestamp=otp.Hour(12),
                                 max_out_of_order_interval=otp.Hour(1))
    df = otp.run(data, start=otp.dt(2021, 12, 31), end=otp.dt(2022, 1, 2))
    assert list(df['Time']) == [otp.dt(2022, 1, 1, hour=12 - 3, microsecond=2000),
                                otp.dt(2022, 1, 1, hour=12 - 2, microsecond=1000),
                                otp.dt(2022, 1, 1, hour=12 - 1, microsecond=0)]
    assert list(df['X']) == [3, 2, 1]
    assert all(df['Time'] == df['NEW_TS'])


@pytest.mark.parametrize('max_delay_handling', ('complain', 'discard', 'use_original_timestamp', 'use_new_timestamp'))
def test_max_delay_handling(session, max_delay_handling):
    data = otp.DataSource('DB', symbols='S1', tick_type='TT')
    data['NEW_TS'] = data['TIMESTAMP'] + otp.Hour(10) + otp.Hour(data['X'])

    data = data.update_timestamp('NEW_TS',
                                 max_delay_of_original_timestamp=otp.Hour(12),
                                 max_delay_handling=max_delay_handling,
                                 max_out_of_order_interval=otp.Hour(12))

    if max_delay_handling == 'complain':
        with pytest.raises(Exception, match='exceeds maximum allowed delay'):
            otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 2))
        return

    df = otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 2))

    if max_delay_handling == 'discard':
        assert len(df) == 2
        assert list(df['Time']) == [otp.dt(2022, 1, 1, hour=11, microsecond=0),
                                    otp.dt(2022, 1, 1, hour=12, microsecond=1000)]
        assert all(df['Time'] == df['NEW_TS'])
        assert list(df['X']) == [1, 2]
    elif max_delay_handling == 'use_original_timestamp':
        assert list(df['Time']) == [otp.dt(2022, 1, 1, hour=0, microsecond=2000),
                                    otp.dt(2022, 1, 1, hour=11, microsecond=0),
                                    otp.dt(2022, 1, 1, hour=12, microsecond=1000)]
        assert list(df['X']) == [3, 1, 2]
    elif max_delay_handling == 'use_new_timestamp':
        assert list(df['Time']) == [otp.dt(2022, 1, 1, hour=11, microsecond=0),
                                    otp.dt(2022, 1, 1, hour=12, microsecond=1000),
                                    otp.dt(2022, 1, 1, hour=13, microsecond=2000)]
        assert all(df['Time'] == df['NEW_TS'])
        assert list(df['X']) == [1, 2, 3]


@pytest.mark.parametrize('out_of_order_timestamp_handling',
                         ('complain', 'use_previous_value', 'use_original_timestamp'))
def test_out_of_order_timestamp_handling(session, out_of_order_timestamp_handling):
    data = otp.DataSource('DB', symbols='S1', tick_type='TT')
    data['NEW_TS'] = data['TIMESTAMP'] - otp.Hour(data['X'])

    data = data.update_timestamp('NEW_TS',
                                 out_of_order_timestamp_handling=out_of_order_timestamp_handling,
                                 max_delay_of_new_timestamp=otp.Day(365) * 100)

    if out_of_order_timestamp_handling == 'complain':
        with pytest.raises(Exception, match='out-of-order interval is larger'):
            otp.run(data, start=otp.dt(2021, 12, 31), end=otp.dt(2022, 1, 2))
        return

    df = otp.run(data, start=otp.dt(2021, 12, 31), end=otp.dt(2022, 1, 2))
    if out_of_order_timestamp_handling == 'use_previous_value':
        assert list(df['Time']) == [otp.dt(2021, 12, 31, hour=23, microsecond=0),
                                    otp.dt(2021, 12, 31, hour=23, microsecond=0),
                                    otp.dt(2021, 12, 31, hour=23, microsecond=0)]
        assert list(df['X']) == [1, 2, 3]
    elif out_of_order_timestamp_handling == 'use_original_timestamp':
        assert list(df['Time']) == [otp.dt(2021, 12, 31, hour=23, microsecond=0),
                                    otp.dt(2022, 1, 1, hour=0, microsecond=1000),
                                    otp.dt(2022, 1, 1, hour=0, microsecond=2000)]
        assert list(df['X']) == [1, 2, 3]


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='session._log_file is not supported in WebAPI')
@pytest.mark.parametrize('log_sequence_violations', (False, True))
@pytest.mark.parametrize('max_out_of_order_interval', (0, otp.Day(365 * 100)))
def test_log_sequence_violations(session, log_sequence_violations, max_out_of_order_interval):
    data = otp.DataSource('DB', symbols='S1', tick_type='TT')
    data['NEW_TS'] = data['TIMESTAMP'] + otp.Hour(data['X'] * (2 * (data['X'] % 2) - 1))

    data = data.update_timestamp('NEW_TS',
                                 max_delay_handling='use_new_timestamp',
                                 max_out_of_order_interval=max_out_of_order_interval,
                                 log_sequence_violations=log_sequence_violations)
    df = otp.run(data, start=otp.dt(2021, 12, 31), end=otp.dt(2022, 1, 2))

    log_text = Path(session._log_file).read_text()

    if max_out_of_order_interval == 0:
        assert list(df['Time']) == [otp.dt(2022, 1, 1, hour=1, microsecond=0),
                                    otp.dt(2022, 1, 1, hour=1, microsecond=0),
                                    otp.dt(2022, 1, 1, hour=3, microsecond=2000)]
        assert list(df['X']) == [1, 2, 3]
        assert ('exceeds maximum allowed' in log_text) == log_sequence_violations
    else:
        assert list(df['Time']) == [otp.dt(2021, 12, 31, hour=22, microsecond=1000),
                                    otp.dt(2022, 1, 1, hour=1, microsecond=0),
                                    otp.dt(2022, 1, 1, hour=3, microsecond=2000)]
        assert list(df['X']) == [2, 1, 3]


@pytest.mark.parametrize('zero_timestamp_handling', (None, 'preserve_sequence'))
def test_zero_timestamp_handling(session, zero_timestamp_handling):
    data = otp.DataSource('DB', symbols='S1', tick_type='TT')
    data['NEW_TS'] = data.apply(lambda row: 0 if row['X'] == 2 else row['TIMESTAMP'] + otp.Hour(1))

    data = data.update_timestamp('NEW_TS',
                                 zero_timestamp_handling=zero_timestamp_handling,
                                 max_delay_of_original_timestamp=otp.Hour(1))

    if zero_timestamp_handling is None:
        with pytest.raises(Exception, match='exceeds maximum allowed delay'):
            otp.run(data, start=otp.dt(2021, 12, 31), end=otp.dt(2022, 1, 2))
    else:
        df = otp.run(data, start=otp.dt(2021, 12, 31), end=otp.dt(2022, 1, 2))
        assert list(df['Time']) == [otp.dt(2022, 1, 1, hour=1, microsecond=0),
                                    otp.dt(2022, 1, 1, hour=1, microsecond=0),
                                    otp.dt(2022, 1, 1, hour=1, microsecond=2000)]
        assert list(df['X']) == [1, 2, 3]


def test_twice(session):
    data = otp.DataSource('DB', symbols='S1', tick_type='TT')
    data['NEW_TS'] = data['TIMESTAMP'] + otp.Nano(1)
    data = data.update_timestamp('NEW_TS', max_delay_of_original_timestamp=otp.Milli(1))
    data['NEW_TS'] = data['TIMESTAMP'] + otp.Nano(1)  # noqa: E1137 (false positive for some reason)
    data = data.update_timestamp('NEW_TS', max_delay_of_original_timestamp=otp.Milli(1))
    with pytest.raises(
        Exception,
        match='It is not currently supported to place one UPDATE_TIMESTAMP event processor on top of another'
    ):
        otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 2))


def test_diamond_pattern(session):
    data = otp.DataSource('DB', symbols='S1', tick_type='TT')
    other = data.copy()
    data['NEW_TS'] = data['TIMESTAMP'] + otp.Nano(1)
    data = data.update_timestamp('NEW_TS', max_delay_of_original_timestamp=otp.Milli(1))
    data = otp.merge([data, other])
    with pytest.raises(
        Exception,
        match='It appears that different branches of query graph have different adjustments of query interval.'
    ):
        otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 2))
