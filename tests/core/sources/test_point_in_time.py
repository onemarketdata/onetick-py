import pytest

import onetick.py as otp

if not otp.compatibility.is_supported_point_in_time():
    pytest.skip(allow_module_level=True,
                reason='PointInTime event processor is not supported on this OneTick version')


def test_simple(f_session):
    qte = otp.Ticks(ASK_PRICE=[20, 21, 22, 23, 24, 25], BID_PRICE=[20, 21, 22, 23, 24, 25])
    data = otp.PointInTime(qte,
                           times=[otp.dt(2003, 12, 1, 0, 0, 0, 1000), '20031201000000.003'],
                           offsets=[0, 1])
    assert data.schema['ASK_PRICE'] is int
    assert data.schema['BID_PRICE'] is int
    assert data.schema['TICK_TIME'] is otp.nsectime
    assert data.schema['OFFSET'] is int

    df = otp.run(data)
    assert list(df['Time']) == [
        otp.dt(2003, 12, 1, 0, 0, 0, 1000),
        otp.dt(2003, 12, 1, 0, 0, 0, 1000),
        otp.dt(2003, 12, 1, 0, 0, 0, 3000),
        otp.dt(2003, 12, 1, 0, 0, 0, 3000),
    ]
    assert list(df['ASK_PRICE']) == [21, 22, 23, 24]
    assert list(df['BID_PRICE']) == [21, 22, 23, 24]
    assert list(df['TICK_TIME']) == [
        otp.dt(2003, 12, 1, 0, 0, 0, 1000),
        otp.dt(2003, 12, 1, 0, 0, 0, 2000),
        otp.dt(2003, 12, 1, 0, 0, 0, 3000),
        otp.dt(2003, 12, 1, 0, 0, 0, 4000),
    ]
    assert list(df['OFFSET']) == [0, 1, 0, 1]


def test_merge(f_session):
    qte = otp.Ticks(ASK_PRICE=[20, 21, 22, 23, 24, 25], BID_PRICE=[20, 21, 22, 23, 24, 25])
    data = otp.merge([
        otp.PointInTime(qte, times=['20031201000000.001'], offsets=[0]).drop(['TICK_TIME', 'OFFSET']),
        otp.PointInTime(qte, times=['20031201000000.003'], offsets=[0]).drop(['TICK_TIME', 'OFFSET']),
        otp.Tick(A=999),
    ])
    df = otp.run(data)
    assert list(df['A']) == [999, 0, 0]
    assert list(df['ASK_PRICE']) == [0, 21, 23]
    assert 'TICK_TIME' not in df
    assert 'OFFSET' not in df


class TestEmptySession:

    @pytest.fixture(scope="function", autouse=True)
    def session(self, monkeypatch, f_session):
        for option in otp.config.get_changeable_config_options():
            monkeypatch.setattr(otp.config, option, otp.config.default)
        yield f_session

    def test_no_default_symbol(self):
        # PY-1280

        qte = otp.Ticks(ASK_PRICE=[20, 21, 22, 23, 24, 25], BID_PRICE=[20, 21, 22, 23, 24, 25])
        q = otp.PointInTime(qte,
                            times=[otp.dt(2003, 12, 1, 0, 0, 0, 1000),
                                   otp.dt(2003, 12, 1, 0, 0, 0, 4000)],
                            offsets=[0])
        df = otp.run(q, date=otp.dt(2003, 12, 1), symbols='LOCAL::')
        assert not df.empty
