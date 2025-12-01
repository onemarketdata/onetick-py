import sys
import pytest
import datetime
import doctest
import numpy as np
import pandas as pd
import onetick.py as otp
from onetick.py.otq import otq
import backoff


# global marker for the whole module
pytestmark = pytest.mark.integration


def _give_up(e):
    return not ('unexpected end of socket' in str(e) or 'Connection refused' in str(e))


our_backoff = backoff.on_exception(backoff.expo, Exception, max_time=60, giveup=_give_up)


if sys.platform.startswith('win'):
    pytest.skip("Default windows user in CI doesn't have access to cloud", allow_module_level=True)


@pytest.fixture
def session(cloud_server):
    with otp.Session() as session:
        session.use(cloud_server)
        yield session


def nptype2otp(dtype):
    if dtype is np.float64:
        return float
    if dtype is np.int32 or dtype is np.int64:
        return int
    if dtype is pd.Timestamp:
        return otp.nsectime
    return dtype


class TestOHLCV:
    @our_backoff
    @pytest.mark.parametrize('tz', ['GMT', 'EST5EDT', 'Pacific/Chatham'])
    @pytest.mark.parametrize('exch', ['all', 'main', 'USPRIM'])
    def test_types_and_schema(self, session, exch, tz):
        src = otp.oqd.sources.OHLCV(exch=exch)
        src = src.first()

        schema_check = dict(OID=str,
                            EXCH=str,
                            CURRENCY=str,
                            OPEN=float,
                            HIGH=float,
                            LOW=float,
                            CLOSE=float,
                            VOLUME=float)

        assert src.schema == schema_check

        df = otp.run(src,
                     symbols='BTKR::::GOOGL US',
                     start=otp.dt(2018, 8, 1),
                     end=otp.dt(2018, 8, 2),
                     symbol_date=otp.dt(2018, 8, 1),
                     timezone=tz)

        assert len(df.columns) == len(schema_check) + 1  # +1 -- Time

        for name, dtype in schema_check.items():
            assert nptype2otp(type(df[name][0])) is dtype, f'field {name} has wrong type'

    @our_backoff
    @pytest.mark.parametrize('tz', ['GMT', 'EST5EDT', 'Pacific/Chatham'])
    @pytest.mark.parametrize('exch', ['all', 'main', 'USPRIM', 'USCOMP', 'NOT_EXISTED'])
    def test_exch_selection(self, session, exch, tz):
        src = otp.oqd.sources.OHLCV(exch=exch)

        df = otp.run(src,
                     symbols='BTKR::::GOOGL US',
                     start=otp.dt(2018, 8, 1),
                     end=otp.dt(2018, 8, 1, 23, 59),  # TODO: need to fix to 24:00:00
                     symbol_date=otp.dt(2018, 8, 1),
                     timezone=tz)

        if exch == 'all':
            assert all(df['EXCH'] == ['USCOMP', 'USPRIM', 'USXNMS'])
        elif exch == 'main':
            assert all(df['EXCH'] == ['USCOMP'])
        elif exch == 'NOT_EXISTED':
            assert len(df) == 0
        else:
            assert all(df['EXCH'] == [exch])

    @our_backoff
    def test_from_doc(self, session):
        src = otp.oqd.sources.OHLCV(exch="USPRIM")
        df = otp.run(src,
                     symbols='BTKR::::GOOGL US',
                     start=otp.dt(2018, 8, 1),
                     end=otp.dt(2018, 8, 2),
                     symbol_date=otp.dt(2018, 8, 1))
        assert doctest.OutputChecker().check_output("""
                         Time    OID    EXCH CURRENCY     OPEN     HIGH      LOW    CLOSE    VOLUME
        0 2018-08-01 00:00:00  74143  USPRIM      USD  1242.73  1245.72  1225.00  1232.99  605680.0
        1 2018-08-01 20:00:00  74143  USPRIM      USD  1219.69  1244.25  1218.06  1241.13  596960.0
        """, str(df), doctest.NORMALIZE_WHITESPACE)


class TestCorporateActions:

    @our_backoff
    def test_doc_test_example(self, session):
        # test example from docs
        src = otp.oqd.sources.CorporateActions()
        df = otp.run(src,
                     symbols='TDEQ::::MKD',
                     start=otp.dt(2021, 5, 20, 9, 30),
                     end=otp.dt(2022, 12, 24, 16),
                     symbol_date=otp.dt(2022, 2, 24),
                     timezone='GMT').head()
        print()
        print(df)

    @our_backoff
    def test_types_and_schemas(self, session):
        src = otp.oqd.sources.CorporateActions()
        src = src.first()

        schema_check = dict(OID=str,
                            ACTION_ID=int,
                            ACTION_TYPE=str,
                            ACTION_ADJUST=float,
                            ACTION_CURRENCY=str,
                            ANN_DATE=int,
                            EX_DATE=int,
                            PAY_DATE=int,
                            REC_DATE=int,
                            TERM_NOTE=str,
                            TERM_RECORD_TYPE=str,
                            ACTION_STATUS=str)

        assert schema_check == src.schema

        df = otp.run(src,
                     symbols='TDEQ::::AAPL',
                     start=otp.dt(2021, 1, 1),
                     end=otp.dt(2021, 8, 6),
                     symbol_date=otp.dt(2021, 2, 18),
                     timezone='GMT')

        assert len(df.columns) == len(schema_check) + 1  # +1 -- Time

        for name, dtype in schema_check.items():
            assert nptype2otp(type(df[name][0])) is dtype, f'field {name} has wrong type'

        print()
        print(df)

    @our_backoff
    def test_from_doc(self, session):
        src = otp.oqd.sources.CorporateActions()
        df = otp.run(src,
                     symbols='TDEQ::::AAPL',
                     start=otp.dt(2021, 1, 1),
                     end=otp.dt(2021, 8, 6),
                     symbol_date=otp.dt(2021, 2, 18),
                     timezone='GMT')
        assert doctest.OutputChecker().check_output("""
                Time   OID  ACTION_ID    ACTION_TYPE  ACTION_ADJUST ACTION_CURRENCY  ANN_DATE   EX_DATE  PAY_DATE\
                      REC_DATE       TERM_NOTE TERM_RECORD_TYPE ACTION_STATUS
        0 2021-02-05  9706   16799540  CASH_DIVIDEND          0.205             USD  20210127  20210205  20210211\
                      20210208  CASH:0.205@USD                         NORMAL
        1 2021-05-07  9706   17098817  CASH_DIVIDEND          0.220             USD  20210428  20210507  20210513\
                      20210510   CASH:0.22@USD                         NORMAL
        2 2021-08-06  9706   17331864  CASH_DIVIDEND          0.220             USD  20210727  20210806  20210812\
                      20210809   CASH:0.22@USD                         NORMAL
        """, str(df), doctest.NORMALIZE_WHITESPACE)


class TestDescriptiveFields:

    @our_backoff
    def test_types_and_schemas(self, session):
        src = otp.oqd.sources.DescriptiveFields()
        src = src.first()

        schema_check = dict(OID=str,
                            END_DATE=otp.nsectime,
                            COUNTRY=str,
                            EXCH=str,
                            NAME=str,
                            ISSUE_DESC=str,
                            ISSUE_CLASS=str,
                            ISSUE_TYPE=str,
                            ISSUE_STATUS=str,
                            SIC_CODE=str,
                            IDSYM=str,
                            TICKER=str,
                            CALENDAR=str,)

        assert schema_check == src.schema

        df = otp.run(src,
                     symbols='1000001589',
                     start=otp.dt(2020, 1, 1),
                     end=otp.dt(2023, 3, 2),
                     timezone='GMT')

        assert len(df.columns) == len(schema_check) + 1  # +1 -- Time

        for name, dtype in schema_check.items():
            assert nptype2otp(type(df[name][0])) is dtype, f'field {name} has wrong type'

        print()
        print(df)

    @our_backoff
    def test_year_9999_in_enddate_field(self, session):
        src = otp.oqd.sources.DescriptiveFields()
        df = otp.run(src,
                     symbols='1000001589',
                     start=otp.dt(2020, 3, 1),
                     end=otp.dt(2023, 3, 2),
                     timezone='GMT')
        print()
        print(df)

    @our_backoff
    def test_from_doc(self, session):
        src = otp.oqd.sources.DescriptiveFields()
        df = otp.run(src,
                     symbols='1000001589',
                     start=otp.dt(2020, 3, 1),
                     end=otp.dt(2023, 3, 2),
                     timezone='GMT').iloc[:6]
        assert doctest.OutputChecker().check_output("""
                Time         OID    END_DATE COUNTRY  EXCH                NAME                   ISSUE_DESC\
                      ISSUE_CLASS ISSUE_TYPE ISSUE_STATUS SIC_CODE    IDSYM TICKER CALENDAR
        0 2020-03-01  1000001589  2020-03-23     LUX  EL^X  INVESTEC GLOBAL ST   EUROPEAN HIGH YLD BD INC 2\
                             FUND                  NORMAL           B2PT4G9
        1 2020-03-23  1000001589  2020-04-01     LUX  EL^X  NINETY ONE LIMITED   EUROPEAN HIGH YLD BD INC 2\
                             FUND                  NORMAL           B2PT4G9
        2 2020-04-01  1000001589  2021-01-01     LUX  EL^X  NINETY ONE LUX S.A   EUROPEAN HIGH YLD BD INC 2\
                             FUND                  NORMAL           B2PT4G9
        3 2021-01-01  1000001589  2021-06-18     LUX  EL^X  NINETY ONE LUX S.A   EUROPEAN HIGH YLD BD INC 2\
                             FUND                  NORMAL           B2PT4G9
        4 2021-06-18  1000001589  2022-01-01     LUX  EL^X  NINETY ONE LUX S.A  GSF GBL HIGH YLD A2 EUR DIS\
                             FUND                  NORMAL           B2PT4G9
        5 2022-01-01  1000001589  2022-01-28     LUX  EL^X  NINETY ONE LUX S.A  GSF GBL HIGH YLD A2 EUR DIS\
                             FUND                  NORMAL           B2PT4G9
        """, str(df), doctest.NORMALIZE_WHITESPACE)


class TestCorporateActionsFunc:

    @our_backoff
    def test_corp_actions_ep(self, session):
        src = otp.DataSource('US_COMP', tick_type='TRD', start=otp.dt(2022, 5, 20, 9, 30), end=otp.dt(2022, 5, 24, 16))
        src.sink(otq.CorpActions(
            adjustment_date=20220524,
            fields="PRICE"))
        src = src.first()

        df = otp.run(src, symbols='MKD', symbol_date=otp.date(2022, 5, 24))
        assert len(df) == 1
        assert df["PRICE"][0] == 1.36649931675

    @our_backoff
    def test_corp_actions_change_price(self, session):
        src = otp.DataSource('US_COMP',
                             tick_type='TRD',
                             start=otp.dt(2022, 5, 20, 9, 30),
                             end=otp.dt(2022, 5, 24, 16))
        src = src.first()
        df1 = otp.run(src, symbols='MKD', symbol_date=otp.date(2022, 5, 24))

        src = otp.corp_actions(src,
                               adjustment_date=otp.date(2022, 5, 24),
                               fields="PRICE",)
        df2 = otp.run(src, symbols='MKD', symbol_date=otp.date(2022, 5, 24))

        print()
        print(df1)
        print(df2)

        assert df1["PRICE"][0] == 0.0911
        assert df2["PRICE"][0] == 1.36649931675  # adjusted price

    @our_backoff
    @pytest.mark.parametrize("adjustment_date", [
        otp.date(2022, 5, 24),
        otp.datetime(2022, 5, 24),
        20220524,
        datetime.date(2022, 5, 24),
        datetime.datetime(2022, 5, 24),
        20220524130000,
        '',
        None,
    ])
    def test_corp_actions_date_input(self, adjustment_date, session):
        src = otp.DataSource('US_COMP',
                             tick_type='TRD',
                             start=otp.dt(2022, 5, 20, 9, 30),
                             end=otp.dt(2022, 5, 24, 16))
        src = otp.corp_actions(src,
                               adjustment_date=adjustment_date,
                               fields="PRICE")
        src = src.first()
        df = otp.run(src, symbols='MKD', symbol_date=otp.date(2022, 5, 24))
        print()
        print(df)
        assert len(df) == 1
        assert df["PRICE"][0] == 1.36649931675

    @pytest.mark.parametrize("adjustment_date", [
        "2022-05-24",
        "2022-05-24 00:00:00",
        202211,
        9999,
        5.5,
    ])
    def test_corp_actions_date_input_exception(self, adjustment_date, session):
        src = otp.DataSource('US_COMP',
                             tick_type='TRD',
                             start=otp.dt(2022, 5, 20, 9, 30),
                             end=otp.dt(2022, 5, 24, 16),
                             schema_policy='manual')
        with pytest.raises(ValueError,
                           match="Parameter 'adjustment_date' must be in YYYYMMDDhhmmss or YYYYMMDD formats."):
            _ = otp.corp_actions(src,
                                 adjustment_date=adjustment_date,
                                 adjustment_date_tz='EST5EDT',
                                 fields="PRICE")

    @pytest.mark.parametrize("adjustment_date,warning", [
        (otp.date(2022, 5, 24), True),
        (otp.datetime(2022, 5, 24), False),
        (otp.datetime(2022, 5, 24, 13), False),
        (20220524, True),
        (datetime.date(2022, 5, 24), True),
        (datetime.datetime(2022, 5, 24), False),
        (datetime.datetime(2022, 5, 24, 13), False),
        (20220524130000, False),
        ('20220524130000', False),
        (20220524000000, False),
        ('20220524000000', False),
        ('', False),
        (None, False),
    ])
    def test_corp_actions_date_input_warning(self, adjustment_date, warning, session):
        src = otp.DataSource('US_COMP',
                             tick_type='TRD',
                             start=otp.dt(2022, 5, 20, 9, 30),
                             end=otp.dt(2022, 5, 24, 16),
                             schema_policy='manual')
        if warning:
            with pytest.warns(UserWarning,
                              match='it is the only valid value when `adjustment_date` is in YYYYMMDD format.'):
                _ = otp.corp_actions(src,
                                     adjustment_date=adjustment_date,
                                     adjustment_date_tz='EST5EDT',
                                     fields="PRICE")
        else:
            _ = otp.corp_actions(src,
                                 adjustment_date=adjustment_date,
                                 adjustment_date_tz='EST5EDT',
                                 fields="PRICE")

    @our_backoff
    def test_corp_actions_date_input_default(self, session):
        src = otp.DataSource('US_COMP', tick_type='TRD')
        src = otp.corp_actions(src, fields="PRICE")
        src = src.first()
        df = otp.run(src, symbols='MKD', symbol_date=otp.date(2022, 5, 24),
                     start=otp.dt(2022, 5, 20, 9, 30), end=otp.dt(2022, 5, 24, 16))
        assert len(df) == 1
        assert df["PRICE"][0] == 1.36649931675

    @our_backoff
    @pytest.mark.parametrize("adjustment_date", [
        otp.datetime(2022, 5, 24, 13),
        datetime.datetime(2022, 5, 24, 13),
        20220524130000,
    ])
    def test_corp_actions_datetime_tz(self, adjustment_date, session):
        src = otp.DataSource('US_COMP',
                             tick_type='TRD',
                             start=otp.dt(2022, 5, 20, 9, 30),
                             end=otp.dt(2022, 5, 24, 16))
        src = otp.corp_actions(src,
                               adjustment_date=adjustment_date,
                               adjustment_date_tz='EST5EDT',
                               fields="PRICE")
        src = src.first()
        df = otp.run(src, symbols='MKD', symbol_date=otp.date(2022, 5, 24), timezone='EST5EDT')
        print()
        print(df)
        assert len(df) == 1
        assert df["PRICE"][0] == 1.36649931675


class TestSharesOutstanding:
    @our_backoff
    def test_from_doc(self, session):
        src = otp.oqd.sources.SharesOutstanding()
        df = otp.run(src,
                     symbols='TDEQ::::AAPL',
                     start=otp.dt(2021, 1, 1),
                     end=otp.dt(2021, 8, 6),
                     symbol_date=otp.dt(2021, 2, 18),
                     timezone='GMT')
        assert doctest.OutputChecker().check_output("""
                Time   OID   END_DATE REPORT_MONTH        SHARES
        0 2021-01-01  9706 2021-01-06       202009  1.700180e+10
        1 2021-01-06  9706 2021-01-29       202009  1.682326e+10
        2 2021-01-29  9706 2021-05-03       202012  1.678810e+10
        3 2021-05-03  9706 2021-07-30       202103  1.668763e+10
        4 2021-07-30  9706 2021-10-29       202106  1.653017e+10
        """, str(df), doctest.NORMALIZE_WHITESPACE)


@our_backoff
def test_x(session):
    df = otp.run(otp.DataSource(db='US_COMP', tick_type='TRD').first(),
                 symbols='TDEQ::::AAPL',
                 start=otp.dt(2021, 1, 1),
                 end=otp.dt(2021, 8, 6),
                 symbol_date=otp.dt(2021, 2, 18),
                 timezone='GMT')
    print()
    print(df)


@our_backoff
def test_bbgsym(session):
    src = otp.oqd.eps.OqdSourceBbgbsym().tick_type('OQD::*')

    df = otp.run(src,
                 symbols='BTKR::::GOOGL US',
                 start=otp.dt(2018, 8, 1),
                 end=otp.dt(2018, 8, 2),
                 symbol_date=otp.dt(2018, 8, 1),
                 timezone='GMT')
    print(df)


@our_backoff
def test_cacts(session):
    # # *All, *Exch
    # # Time     OID    EXCH CURRENCY     OPEN      HIGH     LOW   CLOSE      VOLUME
    # # 0  2021-08-31 20:00:00  109037  USCOMP      USD  302.865  305.1900  301.49  301.83  18983830.0
    # ep = otp.oqd.eps.OqdSourceCacs().tick_type('OQD::*')
    # df = otp.run(otq.GraphQuery(ep),
    #              symbols='TDEQ::::AAPL',
    #              # symbols='TKR::::ALSPW.FRXPAR',
    #              start=otp.dt(2021, 1, 1),
    #              end=otp.dt(2022, 10, 4),
    #              symbol_date=otp.dt(2021, 2, 18))
    # print()
    # print(df)
    # print('------------------')

    ep = otp.oqd.OqdSourceCact().tick_type('__OQD__::*')
    df = otp.run(otq.GraphQuery(ep),
                 symbols='ANN_DATE',
                 start=otp.dt(2021, 1, 1),
                 end=otp.dt(2022, 10, 4),
                 symbol_date=otp.dt(2021, 2, 18))
    print(df)


@our_backoff
def test_oqs_source_des(session):
    # print(otp.dt(3055, 1, 1))

    ep = otp.oqd.OqdSourceDes().tick_type('__OQD__::*')
    df = otp.run(otq.GraphQuery(ep),
                 symbols='1000001589',
                 start=otp.dt(2020, 3, 1),
                 end=otp.dt(2021, 3, 2))
    print(df)


@our_backoff
def test_corp_actions_with_mocked_refdb():
    # Mock the refdb for corporate actions
    # This example is for the case when the corporate action is a stock split
    # It is repeating the test_corp_actions_change_price test, but with the refdb mocked
    # Based on the example in the documentation and real life split:
    # https://www.nasdaqtrader.com/TraderNews.aspx?id=ECA2022-111

    print()
    with otp.Session(otp.Config()) as session:
        # RefDB
        db_name = 'US_COMP'
        ref_db_name = f'REF_DATA_{db_name}'
        symbology = 'CORE'

        ref_db = otp.RefDB(
            ref_db_name,
            db_properties={
                'symbology': symbology,
            },
        )

        ref_db.put([
            otp.RefDB.SymbolNameHistory('MKD||20220501000000|20220530000000', symbology),
            otp.RefDB.CorpActions('MKD||20220522120000|14.9999925|0.0|SPLIT', symbology=symbology),
        ])
        session.use(ref_db)

        # trades DB
        db = otp.DB(
            db_name,
            db_properties={
                'ref_data_db': ref_db_name,
                'symbology': symbology,
            },
        )
        session.use(db)

        # ticks
        db.add(otp.Ticks(PRICE=[0.0911], start=otp.dt(2022, 5, 20, 9, 30, 0),),
               tick_type='TRD',
               symbol='MKD',
               date=otp.dt(2022, 5, 20))

        # fetch trades
        symbol_date = otp.date(2022, 5, 23)
        unadj = otp.DataSource('US_COMP', tick_type='TRD')
        unadj = unadj.first()
        df = otp.run(unadj, symbols='MKD', symbol_date=symbol_date, start=otp.dt(2022, 5, 20), end=otp.dt(2022, 5, 27))
        print(df.head())
        assert df["PRICE"][0] == 0.0911

        # adjust
        adj = otp.corp_actions(unadj,
                               adjustment_date=20220524,
                               fields="PRICE",)

        df = otp.run(adj, symbols='MKD', symbol_date=symbol_date, start=otp.dt(2022, 5, 20), end=otp.dt(2022, 5, 27))
        print(df.head())
        assert df["PRICE"][0] == 1.36649931675

        # check that unadj src is not changed
        df = otp.run(unadj, symbols='MKD', symbol_date=symbol_date, start=otp.dt(2022, 5, 20), end=otp.dt(2022, 5, 27))
        print(df.head())
        assert df["PRICE"][0] == 0.0911
