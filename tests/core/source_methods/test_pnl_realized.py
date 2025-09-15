import pytest

import onetick.py as otp

if not otp.compatibility.is_supported_pnl_realized():
    pytest.skip("PNL_REALIZE isn't supported by this OneTick version", allow_module_level=True)


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    db = otp.DB('SOME_DB')
    db.add(src=otp.Ticks({
        'BUY_SELL_FLAG': ['B', 'B', 'S', 'B', 'S', 'S', 'B'],
        'PRICE': [5, 4, 3, 10, 7, 9, 8],
        'SIZE': [20, 30, 15, 50, 18, 6, 40],
    }), symbol='AA', tick_type='TEST')

    db.add(src=otp.Ticks({
        'BUY_SELL_FLAG_OTHER': ['B', 'B', 'S', 'B', 'S', 'S', 'B'],
        'PRICE_OTHER': [5, 4, 3, 10, 7, 9, 8],
        'SIZE_OTHER': [20, 30, 15, 50, 18, 6, 40],
    }), symbol='AA', tick_type='TEST_NAMES')
    m_session.use(db)

    db = otp.DB('SOME_DB_BIN')
    db.add(src=otp.Ticks({
        'BUY_SELL_FLAG': [0, 0, 1, 0, 1, 1, 0],
        'PRICE': [5, 4, 3, 10, 7, 9, 8],
        'SIZE': [20, 30, 15, 50, 18, 6, 40],
    }), symbol='AA', tick_type='TEST')
    m_session.use(db)

    return m_session


def test_pnl_realized_fifo():
    data = otp.DataSource('SOME_DB', tick_type='TEST', symbols='AA')
    data = data.pnl_realized()
    assert data.schema['PNL_REALIZED'] is float
    df = otp.run(data)
    assert list(df['PNL_REALIZED']) == [0.0, 0.0, -30.0, 0.0, 49.0, 30.0, 0.0]


def test_pnl_realized_output_field_name():
    data = otp.DataSource('SOME_DB', tick_type='TEST', symbols='AA')
    with pytest.raises(ValueError, match='Field PRICE is already in schema'):
        data.pnl_realized(output_field_name='PRICE')
    data = data.pnl_realized(output_field_name='TEST_NAME')
    assert data.schema['TEST_NAME'] is float
    assert 'PNL_REALIZED' not in data.schema
    df = otp.run(data)
    assert list(df['TEST_NAME']) == [0.0, 0.0, -30.0, 0.0, 49.0, 30.0, 0.0]


@pytest.mark.skipif(
    not otp.compatibility.is_supported_pnl_realized_buy_sell_flag_bin(),
    reason="PNL_REALIZED on this version of OneTick doesn't support 0 and 1 as BUY_SELL_FLAG values",
)
def test_pnl_realized_buy_sell_flag_bin():
    data = otp.DataSource('SOME_DB_BIN', tick_type='TEST', symbols='AA')
    data = data.pnl_realized()
    assert data.schema['PNL_REALIZED'] is float
    df = otp.run(data)
    assert list(df['PNL_REALIZED']) == [0.0, 0.0, -30.0, 0.0, 49.0, 30.0, 0.0]


def test_inplace():
    data = otp.DataSource('SOME_DB', tick_type='TEST', symbols='AA')
    new_data = data.pnl_realized()
    assert new_data is not data
    df = otp.run(new_data)
    assert list(df['PNL_REALIZED']) == [0.0, 0.0, -30.0, 0.0, 49.0, 30.0, 0.0]

    data = otp.DataSource('SOME_DB', tick_type='TEST', symbols='AA')
    new_data = data.pnl_realized(inplace=False)
    assert new_data is not data
    df = otp.run(new_data)
    assert list(df['PNL_REALIZED']) == [0.0, 0.0, -30.0, 0.0, 49.0, 30.0, 0.0]

    data = otp.DataSource('SOME_DB', tick_type='TEST', symbols='AA')
    new_data = data.pnl_realized(inplace=True)
    assert new_data is None
    df = otp.run(data)
    assert list(df['PNL_REALIZED']) == [0.0, 0.0, -30.0, 0.0, 49.0, 30.0, 0.0]


def test_change_input_names():
    data = otp.DataSource('SOME_DB', tick_type='TEST_NAMES', symbols='AA')
    with pytest.raises(ValueError, match='Field PRICE is not in schema'):
        data.pnl_realized(size_field='SIZE_OTHER', price_field='PRICE', buy_sell_flag_field='BUY_SELL_FLAG_OTHER')
    with pytest.raises(ValueError, match='Field SIZE is not in schema'):
        data.pnl_realized(size_field='SIZE', price_field='PRICE_OTHER', buy_sell_flag_field='BUY_SELL_FLAG_OTHER')
    with pytest.raises(ValueError, match='Field BUY_SELL_FLAG is not in schema'):
        data.pnl_realized(size_field='SIZE_OTHER', price_field='PRICE_OTHER', buy_sell_flag_field='BUY_SELL_FLAG')

    data = data.pnl_realized(price_field='PRICE_OTHER',
                             size_field='SIZE_OTHER',
                             buy_sell_flag_field='BUY_SELL_FLAG_OTHER')
    assert data.schema == {
        'PRICE_OTHER': int,
        'SIZE_OTHER': int,
        'BUY_SELL_FLAG_OTHER': str,
        'PNL_REALIZED': float,
    }
    df = otp.run(data)
    assert list(df['PNL_REALIZED']) == [0.0, 0.0, -30.0, 0.0, 49.0, 30.0, 0.0]
    assert set(df) == {'Time', 'PRICE_OTHER', 'SIZE_OTHER', 'BUY_SELL_FLAG_OTHER', 'PNL_REALIZED'}
    assert list(df['PRICE_OTHER']) == [5, 4, 3, 10, 7, 9, 8]
    assert list(df['SIZE_OTHER']) == [20, 30, 15, 50, 18, 6, 40]
    assert list(df['BUY_SELL_FLAG_OTHER']) == ['B', 'B', 'S', 'B', 'S', 'S', 'B']
