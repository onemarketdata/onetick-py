import pytest

import os
import sqlite3
from pathlib import Path

import onetick.py as otp

if not otp.compatibility.is_odbc_query_supported():
    pytest.skip("Doesn't work on old OneTick versions", allow_module_level=True)

if os.getenv('OTP_WEBAPI_TEST_MODE'):
    pytest.skip("WebAPI and ODBC is not compatible", allow_module_level=True)

pytestmark = pytest.mark.skipif(os.name == 'nt', reason='We do not have ODBC and SQLite support on Windows')


@pytest.fixture(scope='module')
def database_path():
    path = Path.home() / 'test.db'
    yield path
    if path.exists():
        path.unlink()


@pytest.fixture(scope='module')
def odbc_config(database_path):
    odbc_config_path = database_path.parent / '.odbc.ini'
    odbc_config_path.write_text(f"""
[testdb_dsn]
Description=My SQLite sample database
Driver=SQLite3
Database={database_path}
""")
    yield odbc_config_path
    odbc_config_path.unlink()


@pytest.fixture(scope='module')
def sqlite_db(odbc_config, database_path):
    con = sqlite3.connect(str(database_path))
    cur = con.cursor()
    cur.execute("create table TEST_TABLE(A, B, C, D datetime)")
    cur.execute(
        """
        insert into TEST_TABLE values
            ('A1', 1975, 8.12345, '2022-01-01 12:13:14.111'),
            ('A2', 1971, 7.98765, '2022-01-02 22:23:24.222')
        """
    )
    cur.execute("create table TEST_TIMESTAMP(TIMESTAMP, A)")
    cur.execute(
        """
        insert into TEST_TIMESTAMP values
            ('2022-01-01 12:13:14.111', 1),
            ('2022-01-02 22:23:24.222', 2)
        """
    )
    cur.execute("create table TEST_UNORDERED(TIMESTAMP, A)")
    cur.execute(
        """
        insert into TEST_UNORDERED values
            ('2022-01-02 22:23:24.222', 2),
            ('2022-01-01 12:13:14.111', 1)
        """
    )
    cur.execute("create table TEST_TEXT(A, B)")
    cur.execute(
        """
        insert into TEST_TEXT values
            ('hello', 1),
            ('привет', 2)
        """
    )
    cur.execute("create table TEST_SYMBOL(A, B)")
    cur.execute(
        """
        insert into TEST_SYMBOL values
            ('one', 1),
            ('two', 2)
        """
    )
    cur.execute("create table TEST_SYMBOL2(A, B)")
    cur.execute(
        """
        insert into TEST_SYMBOL2 values
            ('three', 3),
            ('four', 4)
        """
    )
    con.commit()
    cur.close()
    yield con
    con.close()


def test_dsn_and_schema(session, sqlite_db):
    data = otp.ODBC(dsn='testdb_dsn', sql='select * from TEST_TABLE',
                    schema={'A': str, 'B': int, 'C': float, 'D': otp.nsectime})
    assert data.schema['A'] is str
    assert data.schema['B'] is int
    assert data.schema['C'] is float
    assert data.schema['D'] is otp.nsectime
    df = otp.run(data)
    assert list(df['A']) == ['A1', 'A2']
    assert list(df['B']) == [1975, 1971]
    assert list(df['C']) == [8.12345, 7.98765]
    assert list(df['D']) == [otp.dt(2022, 1, 1, 12, 13, 14, 111000), otp.dt(2022, 1, 2, 22, 23, 24, 222000)]
    assert list(df['Time']) == [otp.config.default_end_time, otp.config.default_end_time]


def test_connection_string(session, sqlite_db, database_path):
    data = otp.ODBC(connection_string='DRIVER={SQLite3};Database=' + str(database_path),
                    sql='select * from TEST_TABLE')
    df = otp.run(data)
    assert list(df['A']) == ['A1', 'A2']
    assert list(df['B']) == [1975, 1971]
    assert list(df['C']) == [8.12345, 7.98765]
    assert list(df['D']) == [otp.dt(2022, 1, 1, 12, 13, 14, 111000), otp.dt(2022, 1, 2, 22, 23, 24, 222000)]
    assert list(df['Time']) == [otp.config.default_end_time, otp.config.default_end_time]


def test_timestamp(session, sqlite_db):
    data = otp.ODBC(dsn='testdb_dsn', sql='select * from TEST_TIMESTAMP')
    df = otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 3))
    assert list(df['Time']) == [otp.dt(2022, 1, 1, 12, 13, 14, 111000), otp.dt(2022, 1, 2, 22, 23, 24, 222000)]
    assert list(df['A']) == [1, 2]


def test_start_expr(session, sqlite_db):
    data = otp.ODBC(
        dsn='testdb_dsn',
        sql='select * from TEST_TIMESTAMP where TIMESTAMP >= "<_START_TIME>"',
        start_expr=(otp.meta_fields['START_TIME'] + otp.Day(1)).dt.strftime('%Y-%m-%d %H:%M:%S.%q')
    )
    df = otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 3))
    assert list(df['Time']) == [otp.dt(2022, 1, 2, 22, 23, 24, 222000)]
    assert list(df['A']) == [2]


def test_end_expr(session, sqlite_db):
    data = otp.ODBC(
        dsn='testdb_dsn',
        sql='select * from TEST_TIMESTAMP where TIMESTAMP < "<_END_TIME>"',
        end_expr=(otp.meta_fields['END_TIME'] - otp.Day(1)).dt.strftime('%Y-%m-%d %H:%M:%S.%q')
    )
    df = otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 3))
    assert list(df['Time']) == [otp.dt(2022, 1, 1, 12, 13, 14, 111000)]
    assert list(df['A']) == [1]


def test_tz(session, sqlite_db):
    data = otp.ODBC(dsn='testdb_dsn', sql='select * from TEST_TIMESTAMP', tz='GMT')
    df = otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 3), timezone='GMT')
    assert list(df['Time']) == [otp.dt(2022, 1, 1, 12, 13, 14, 111000), otp.dt(2022, 1, 2, 22, 23, 24, 222000)]
    assert list(df['A']) == [1, 2]


@pytest.mark.parametrize('allow_unordered_ticks', (False, True))
def test_unordered(session, sqlite_db, allow_unordered_ticks):
    data = otp.ODBC(dsn='testdb_dsn',
                    sql='select * from TEST_UNORDERED',
                    allow_unordered_ticks=allow_unordered_ticks)
    if not allow_unordered_ticks:
        with pytest.raises(Exception, match='Ticks are not ordered by timestamp'):
            otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 3))
        return
    df = otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 3))
    assert list(df['Time']) == [otp.dt(2022, 1, 2, 22, 23, 24, 222000), otp.dt(2022, 1, 1, 12, 13, 14, 111000)]
    assert list(df['A']) == [2, 1]


@pytest.mark.xfail(reason='OneTick does not work well with SQLite strings?', strict=True)
def test_unicode(session, sqlite_db):
    data = otp.ODBC(dsn='testdb_dsn', sql='select A from TEST_TEXT', preserve_unicode_fields=['A'], schema={'A': str})
    assert 'A' in data.schema
    assert data.schema['A'] is str
    df = otp.run(data)
    assert 'B' not in df
    assert list(df['AU']) == ['hello', 'привет']


def test_symbols(session, sqlite_db):
    data = otp.ODBC(
        dsn='testdb_dsn',
        sql='select * from TEST_<_SYMBOL_NAME>',
    )
    df = otp.run(data, symbols='SYMBOL', start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 3))
    assert list(df['Time']) == [otp.dt(2022, 1, 3), otp.dt(2022, 1, 3)]
    assert list(df['A']) == ['one', 'two']
    assert list(df['B']) == [1, 2]

    data = otp.ODBC(
        dsn='testdb_dsn',
        sql='select * from TEST_<_SYMBOL_NAME>',
        symbols='SYMBOL',
    )
    df = otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 3))
    assert list(df['Time']) == [otp.dt(2022, 1, 3), otp.dt(2022, 1, 3)]
    assert list(df['A']) == ['one', 'two']
    assert list(df['B']) == [1, 2]

    data = otp.ODBC(
        dsn='testdb_dsn',
        sql='select * from TEST_<_SYMBOL_NAME>',
        symbols=['SYMBOL', 'SYMBOL2'],
    )
    df = otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 3))
    assert list(df['Time']) == [otp.dt(2022, 1, 3)] * 4
    assert list(df['A']) == ['one', 'two', 'three', 'four']
    assert list(df['B']) == [1, 2, 3, 4]

    data = otp.ODBC(
        dsn='testdb_dsn',
        sql='select * from TEST_<_SYMBOL_NAME>',
        symbols=['SYMBOL', 'SYMBOL2'],
        presort=True,
    )
    df = otp.run(data, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 3))
    assert list(df['Time']) == [otp.dt(2022, 1, 3)] * 4
    assert list(df['A']) == ['one', 'two', 'three', 'four']
    assert list(df['B']) == [1, 2, 3, 4]
