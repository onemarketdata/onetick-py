import pytest

import onetick.py as otp

if not otp.compatibility.is_supported_point_in_time():
    pytest.skip(allow_module_level=True,
                reason='PointInTime event processor is not supported on this OneTick version')


def test_simple(f_session):
    qte = otp.Ticks(ASK_PRICE=[20, 21, 22, 23, 24, 25], BID_PRICE=[20, 21, 22, 23, 24, 25])
    trd = otp.Ticks(PRICE=[1, 3, 5], SIZE=[100, 300, 500], offset=[1, 3, 5])
    data = qte.point_in_time(trd, offsets=[0], input_ts_fields_to_propagate=['ASK_PRICE', 'BID_PRICE'])

    assert data.schema['ASK_PRICE'] is int
    assert data.schema['BID_PRICE'] is int
    assert data.schema['PRICE'] is int
    assert data.schema['SIZE'] is int
    assert data.schema['TICK_TIME'] is otp.nsectime
    assert data.schema['OFFSET'] is int

    df = otp.run(data)

    assert list(df['Time']) == [
        otp.dt(2003, 12, 1, 0, 0, 0, 1000),
        otp.dt(2003, 12, 1, 0, 0, 0, 2000),
        otp.dt(2003, 12, 1, 0, 0, 0, 3000),
        otp.dt(2003, 12, 1, 0, 0, 0, 4000),
        otp.dt(2003, 12, 1, 0, 0, 0, 5000),
    ]
    assert list(df['ASK_PRICE']) == [21, 22, 23, 24, 25]
    assert list(df['BID_PRICE']) == [21, 22, 23, 24, 25]
    assert list(df['PRICE']) == [1, 1, 3, 3, 5]
    assert list(df['SIZE']) == [100, 100, 300, 300, 500]
    assert list(df['TICK_TIME']) == [
        otp.dt(2003, 12, 1, 0, 0, 0, 1000),
        otp.dt(2003, 12, 1, 0, 0, 0, 1000),
        otp.dt(2003, 12, 1, 0, 0, 0, 3000),
        otp.dt(2003, 12, 1, 0, 0, 0, 3000),
        otp.dt(2003, 12, 1, 0, 0, 0, 5000),
    ]
    assert list(df['OFFSET']) == [0, 0, 0, 0, 0]


def test_errors(f_session):
    qte = otp.Ticks(ASK_PRICE=[20, 21, 22, 23, 24, 25], BID_PRICE=[20, 21, 22, 23, 24, 25])
    trd = otp.Ticks(PRICE=[1, 3, 5], SIZE=[100, 300, 500], offset=[1, 3, 5])
    with pytest.raises(ValueError):
        qte.point_in_time(trd, offsets=[0], offset_type='WRONG')


def test_input_ts_fields_to_propagate(f_session):
    qte = otp.Ticks(ASK_PRICE=[20, 21, 22, 23, 24, 25], BID_PRICE=[20, 21, 22, 23, 24, 25])
    trd = otp.Ticks(PRICE=[1, 3, 5], SIZE=[100, 300, 500], offset=[1, 3, 5])
    data = qte.point_in_time(trd, offsets=[0])
    assert 'ASK_PRICE' not in data.schema
    assert 'BID_PRICE' not in data.schema


class TestEmptySession:

    @pytest.fixture(scope="function", autouse=True)
    def session(self, monkeypatch, f_session):
        for option in otp.config.get_changeable_config_options():
            monkeypatch.setattr(otp.config, option, otp.config.default)
        yield f_session

    def test_no_default_symbol(self):
        # PY-1280
        qte = otp.Ticks(ASK_PRICE=[20, 21, 22, 23, 24, 25], BID_PRICE=[20, 21, 22, 23, 24, 25])
        trd = otp.Ticks(PRICE=[1, 3, 5], SIZE=[100, 300, 500], offset=[1, 3, 5])
        data = qte.point_in_time(trd, offsets=[0], input_ts_fields_to_propagate=['ASK_PRICE', 'BID_PRICE'])
        df = otp.run(data, date=otp.dt(2003, 12, 1), symbols='LOCAL::')
        assert not df.empty
