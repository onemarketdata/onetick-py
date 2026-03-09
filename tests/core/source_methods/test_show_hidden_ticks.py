import pytest

import onetick.py as otp
from onetick.py.otq import otq


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    data = otp.Ticks(
        X=[1, 2, 3, 4, 5, 6],
        PRICE=[100, 80, 90, 110, 100, 75],
        SIZE=[10, 30, 25, 100, 20, 30],
        UPDATE_TIME=[otp.config['default_start_time']] * 6,
        BUY_SELL_FLAG=[0, 0, 1, 1, 1, 0],
        TICK_STATUS=[otp.byte(0), otp.byte(31), otp.byte(0), otp.byte(0), otp.byte(31), otp.byte(0)],
        RECORD_TYPE=['R'] * 6,
    )
    data.sink(otq.ModifyTsProperties('STATE_KEYS', 'PRICE,BUY_SELL_FLAG'))
    db = otp.DB('TEST_HIDDEN_TICKS_DB')
    db.add(src=data, symbol='A', tick_type='PRL')
    m_session.use(db)


def test_show_hidden_ticks():
    src = otp.DataSource('TEST_HIDDEN_TICKS_DB', tick_type='PRL', symbols='A')
    src.show_hidden_ticks(inplace=True)
    df = otp.run(src).to_dict(orient='list')
    assert df['X'] == [0, 1, 2, 3, 4, 5, 6]
    assert df['TICK_STATUS'] == [31, 0, 31, 0, 0, 31, 0]
