import datetime

import os
import pytest
import onetick.py as otp
from onetick.py.otq import otq


@pytest.fixture(scope='function')
def session(pytestconfig):

    config = otp.Config(
        otq_path=[
            os.path.join(pytestconfig.rootdir, 'doctest_resources')
        ]
    )

    with otp.Session(config) as session_:
        db = otp.DB(name='SOME_DB')
        session_.use(db)
        db.add(otp.Ticks(X=[1, 2, 3]), symbol='S1', tick_type='TT')
        db.add(otp.Ticks(X=[-3, -2, -1]), symbol='S2', tick_type='TT')

        db_2 = otp.DB(name='SOME_DB_2')
        session_.use(db_2)
        db_2.add(otp.Ticks(X=[4, 5, 6]), symbol='S1', tick_type='TT')
        db_2.add(otp.Ticks(X=[-4, -5, -6]), symbol='S2', tick_type='TT')

        # refdb needed for otp.corp_actions example in docs
        ref_db = otp.RefDB("REF_DATA_US_COMP", db_properties={'symbology': "CORE"})
        ref_db.put([
            otp.RefDB.SymbolNameHistory('MKD||20220501000000|20220530000000', "CORE"),
            otp.RefDB.CorpActions('MKD||20220522120000|14.9999925|0.0|SPLIT', symbology="CORE"),
        ])
        session_.use(ref_db)

        db = otp.DB(
            name='US_COMP',
            db_properties={
                'ref_data_db': "REF_DATA_US_COMP",
                'symbology': "CORE",
            },
        )
        session_.use(db)
        db.add(otp.Ticks(PRICE=[1.3, 1.4, 1.4], SIZE=[100, 10, 50]),
               tick_type='TRD',
               symbol='AAPL',
               date=otp.dt(2022, 3, 1))
        db.add(otp.Ticks(PRICE=[45.37, 45.41]),
               tick_type='TRD',
               symbol='AAP',
               date=otp.dt(2022, 3, 1))
        db.add(otp.Ticks(ASK_PRICE=[1.5, 1.4], BID_PRICE=[1.2, 1.3]),
               tick_type='QTE',
               symbol='AAPL',
               date=otp.dt(2022, 3, 1))

        db.add(otp.Ticks(PRICE=[1.0, 1.1, 1.2], SIZE=[100, 101, 102]),
               tick_type='TRD',
               symbol='AAPL',
               date=otp.dt(2022, 3, 2))

        db.add(otp.Ticks(PRICE=[1.0, 1.1, 1.2, 2.0, 2.1, 2.2],
                         SIZE=[100, 101, 102, 200, 201, 202],
                         offset=[0, 1, 2, 60000, 60001, 60002]),
               tick_type='TRD',
               symbol='MSFT',
               date=otp.dt(2022, 3, 3))

        # otp.corp_actions examples in docs
        db.add(otp.Ticks(PRICE=[0.0911], start=otp.dt(2022, 5, 20, 9, 30, 0),),
               tick_type='TRD',
               symbol='MKD',
               date=otp.dt(2022, 5, 20))

        db2 = otp.DB(name='OQD')
        session_.use(db2)
        db2.add(otp.Ticks(X=[1, 2, 3]), symbol='BTKR::::GOOGL US', tick_type='TT')

        yield session_


# pylint: disable=redefined-outer-name
@pytest.fixture(autouse=True)
def add_session(pytestconfig, doctest_namespace, request, session):
    doctest_namespace['otq'] = otq
    doctest_namespace['otp'] = otp
    doctest_namespace['session'] = session
    doctest_namespace['datetime'] = datetime
    doctest_namespace['csv_path'] = os.path.join(pytestconfig.rootdir, 'doctest_resources')
