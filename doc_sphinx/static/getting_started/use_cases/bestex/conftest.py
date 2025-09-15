import pytest
import onetick.py as otp


@pytest.fixture(scope='module')
def session(session):

    session.dbs['US_COMP'].add(
        otp.Tick(
            PRICE=0.0,
            SIZE=0.0,
        ).table(
            **session.real_db_schemas['us_comp_trd']
        ),
        tick_type='TRD',
        symbol='TSLA',
        date=otp.dt(2022, 3, 2),
    )
    session.dbs['US_COMP'].add(
        otp.Tick(
            ASK_PRICE=0.0,
            ASK_SIZE=0.0,
        ).table(
            **session.real_db_schemas['us_comp_qte']
        ),
        tick_type='QTE',
        symbol='TSLA',
        date=otp.dt(2022, 3, 2),
    )
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
        date=otp.dt(2022, 3, 2),
    )

    yield session
