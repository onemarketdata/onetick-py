"""
Special symbols ``_ALL_SYMBOLS_`` and ``_ALL_DATA_`` are processed by the OneTick server,
onetick-py just passes them to OneTick as regular symbols.
So these tests are checking that all the ways of setting symbols work with them on the server
that supports these special symbols.
"""
import pandas as pd
import pytest

import onetick.py as otp
import tests.compatibility

pytestmark = pytest.mark.skipif(not tests.compatibility.is_special_symbols_supported(),
                                reason='Special symbols are not supported on this OneTick version')

DB_NAME = 'SPECIAL_SYMBOLS_DB'
# ticks of the symbols interleave in time: X is the position of the tick in the merged time series
DATA = {
    'A': dict(X=[1, 4], offset=[0, 3]),
    'B': dict(X=[2, 5], offset=[1, 4]),
    'C': dict(X=[3, 6], offset=[2, 5]),
}


@pytest.fixture(scope='module')
def db():
    db = otp.DB(DB_NAME)
    for symbol, ticks in DATA.items():
        db.add(otp.Ticks(**ticks), symbol=symbol, tick_type='TRD')
    yield db


@pytest.fixture(scope='module', autouse=True)
def session(m_session, db):
    m_session.use(db)
    yield m_session


def check_all_symbols_unbound(result, prefix=''):
    # separate result for each symbol of the database
    assert set(result) == {f'{prefix}{symbol}' for symbol in DATA}
    for symbol, ticks in DATA.items():
        assert list(result[f'{prefix}{symbol}']['X']) == ticks['X']


def check_all_symbols_bound(df):
    # ticks of all symbols of the database are merged
    assert list(df['X']) == [1, 2, 3, 4, 5, 6]


def check_all_data(df):
    assert len(df) == 6
    # all ticks have the start time of the query as their timestamp,
    # real symbol and timestamp of the tick are in the added fields
    start = pd.Timestamp(otp.config.default_start_time)
    assert set(df['Time']) == {start}
    assert set(df['SYMBOL_NAME']) == set(DATA)
    # ticks are returned in the storage order, so check each symbol separately
    for symbol, ticks in DATA.items():
        symbol_df = df[df['SYMBOL_NAME'] == symbol]
        assert list(symbol_df['X']) == ticks['X']
        assert list(symbol_df['TICK_TIME']) == [start + pd.Timedelta(milliseconds=ms) for ms in ticks['offset']]


def test_merge_all_symbols():
    data = otp.DataSource(DB_NAME, tick_type='TRD')
    data = otp.merge([data], symbols='_ALL_SYMBOLS_')
    df = otp.run(data)
    check_all_symbols_bound(df)


def test_merge_all_symbols_with_db():
    data = otp.DataSource(tick_type='TRD')
    data = otp.merge([data], symbols=f'{DB_NAME}::_ALL_SYMBOLS_')
    df = otp.run(data)
    check_all_symbols_bound(df)


def test_data_source_all_symbols():
    data = otp.DataSource(DB_NAME, tick_type='TRD', symbols='_ALL_SYMBOLS_')
    result = otp.run(data)
    check_all_symbols_unbound(result)


def test_run_all_symbols():
    data = otp.DataSource(DB_NAME, tick_type='TRD')
    result = otp.run(data, symbols='_ALL_SYMBOLS_')
    check_all_symbols_unbound(result)


def test_run_all_symbols_with_db():
    data = otp.DataSource(tick_type='TRD')
    result = otp.run(data, symbols=f'{DB_NAME}::_ALL_SYMBOLS_')
    check_all_symbols_unbound(result, prefix=f'{DB_NAME}::')


def test_merge_all_data():
    data = otp.DataSource(DB_NAME, tick_type='TRD')
    data = otp.merge([data], symbols='_ALL_DATA_')
    df = otp.run(data)
    check_all_data(df)


def test_merge_all_data_with_db():
    data = otp.DataSource(tick_type='TRD')
    data = otp.merge([data], symbols=f'{DB_NAME}::_ALL_DATA_')
    df = otp.run(data)
    check_all_data(df)


def test_data_source_all_data():
    data = otp.DataSource(DB_NAME, tick_type='TRD', symbols='_ALL_DATA_')
    df = otp.run(data)
    check_all_data(df)


def test_run_all_data():
    data = otp.DataSource(DB_NAME, tick_type='TRD')
    df = otp.run(data, symbols='_ALL_DATA_')
    check_all_data(df)


def test_run_all_data_with_db():
    data = otp.DataSource(tick_type='TRD')
    df = otp.run(data, symbols=f'{DB_NAME}::_ALL_DATA_')
    check_all_data(df)
