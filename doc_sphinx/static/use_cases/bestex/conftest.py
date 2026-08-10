import pytest
import onetick.py as otp


@pytest.fixture(scope='module')
def session(session):
    session.dbs['ORDERS_DB'].add(
        otp.Tick(
            PRICE=0.0,
            PRICE_FILLED=0.0,
            QTY=0,
            QTY_FILLED=0,
            BUY_FLAG=0,
            SIDE='',
            STATE='',
            ID='',
        ),
        tick_type='ORDER',
        symbol='TSLA',
        date=otp.dt(2024, 2, 1),
    )

    yield session
