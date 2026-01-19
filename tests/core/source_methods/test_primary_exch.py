import os
import pytest
import onetick.py as otp


pytestmark = pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason='Not supported via WebAPI')


@pytest.fixture(scope='module')
def db_data(session):
    ref_db_name = 'REF_DATA_PRIMARY_EXCH'
    symbology = 'TICKER'

    ref_db = otp.RefDB(
        ref_db_name,
        db_properties={
            'symbology': symbology,
        },
    )

    session.use(ref_db)
    ref_db.put([
        otp.RefDB.SymbolNameHistory('TEST||19991118000000|99999999000000', symbology=symbology),
        otp.RefDB.PrimaryExchange('TEST||19991118000000|99999999000000|B|', symbology=symbology),
    ])

    db = otp.DB(
        'TEST_DB_EXCH',
        db_properties={
            'ref_data_db': ref_db_name,
            'symbology': symbology,
        }
    )
    db.add(otp.Ticks(
        X=[1, 2, 3, 4],
        EXCHANGE=['A', 'B', 'B', 'A'],
    ), symbol='TEST', tick_type='TT', date=otp.date(2003, 12, 1))
    session.use(db)


def test_primary_exch(db_data):
    data = otp.DataSource('TEST_DB_EXCH', tick_type='TT', symbols='TEST', date=otp.date(2003, 12, 1))
    left, right = data.primary_exch()
    left['T'] = 1
    right['T'] = 0
    data = otp.merge([left, right])
    df = otp.run(data, symbol_date=otp.date(2003, 12, 1))
    assert list(df['T']) == [0, 1, 1, 0]
    assert list(df['X']) == [1, 2, 3, 4]
    assert list(df['EXCHANGE']) == ['A', 'B', 'B', 'A']


def test_discard_on_match(db_data):
    data = otp.DataSource('TEST_DB_EXCH', tick_type='TT', symbols='TEST', date=otp.date(2003, 12, 1))
    data, _ = data.primary_exch(discard_on_match=True)
    df = otp.run(data, symbol_date=otp.date(2003, 12, 1))
    assert list(df['X']) == [1, 4]
    assert list(df['EXCHANGE']) == ['A', 'A']
