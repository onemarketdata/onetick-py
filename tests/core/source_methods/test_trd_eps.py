import numpy as np
import pandas as pd
import pytest

import onetick.py as otp


@pytest.fixture(scope="module", autouse=True)
def session(m_session, cloud_server):
    m_session.use(cloud_server)
    yield m_session


RUN_KWARGS = dict(start=otp.dt(2024, 2, 1, 9, 30), end=otp.dt(2024, 2, 1, 9, 30, 1))


def get_trd():
    data = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    return data[['PRICE', 'SIZE']]


def get_qte():
    data = otp.DataSource('US_COMP_SAMPLE', tick_type='QTE', symbols='AAPL')
    return data[['ASK_PRICE', 'BID_PRICE']]


def get_cme_trd():
    data = otp.DataSource(db='CME_SAMPLE', tick_type='TRD', symbols=r'NQ\H24')
    return data[['PRICE', 'SIZE']]


def get_cme_ob():
    return otp.DataSource(db='CME_SAMPLE', tick_type='PRL_FULL', symbols=r'NQ\H24')


class TestTrdQuoteJoin:
    def test_default(self):
        data = get_trd().trd_quote_join(get_qte())
        assert data.schema == {
            'TRD.PRICE': float,
            'TRD.SIZE': int,
            'QTE.TIMESTAMP': otp.nsectime,
            'QTE.ASK_PRICE': float,
            'QTE.BID_PRICE': float,
        }
        data = data.limit(5)
        df = otp.run(data, date=otp.dt(2024, 2, 1)).to_dict(orient='list')
        del df['Time']
        assert df == {
            'TRD.PRICE': [186.5, 185.59, 185.49, 185.49, 185.5],
            'TRD.SIZE': [6, 1, 107, 1, 2],
            'QTE.TIMESTAMP': [
                pd.Timestamp('2024-02-01 04:00:00.007607483'),
                pd.Timestamp('2024-02-01 04:00:00.007607483'),
                pd.Timestamp('2024-02-01 04:00:00.007607483'),
                pd.Timestamp('2024-02-01 04:00:00.010349268'),
                pd.Timestamp('2024-02-01 04:00:00.010686602'),
            ],
            'QTE.ASK_PRICE': [187.02, 187.02, 187.02, 185.8, 185.8],
            'QTE.BID_PRICE': [185.49, 185.49, 185.49, 185.45, 185.45],
        }

    def test_quote_delay(self):
        data = get_trd().trd_quote_join(get_qte(), quote_delay=0.001)
        df = otp.run(data, **RUN_KWARGS)
        assert len(df) > 0
        assert (df['Time'] - df['QTE.TIMESTAMP'] >= np.timedelta64(1, 'ms')).all()

    def test_go_back_to_first_quote(self):
        # the first trade in this interval is at 09:30:00.000961260,
        # the prevailing quote for it is at 09:30:00.000860953, before the start of the interval
        run_kwargs = dict(start=otp.dt(2024, 2, 1, 9, 30, 0, 900), end=otp.dt(2024, 2, 1, 9, 30, 0, 1000))
        data = get_trd().trd_quote_join(get_qte(), go_back_to_first_quote=1)
        df = otp.run(data, **run_kwargs)
        assert str(df['QTE.TIMESTAMP'][0]) == '2024-02-01 09:30:00.000860953'
        assert df['QTE.ASK_PRICE'][0] == 184.14
        assert df['QTE.BID_PRICE'][0] == 184.0

    def test_exceptions(self):
        with pytest.raises(ValueError, match='quote_delay'):
            get_trd().trd_quote_join(get_qte(), quote_delay=-1)
        with pytest.raises(ValueError, match='go_back_to_first_quote'):
            get_trd().trd_quote_join(get_qte(), go_back_to_first_quote=-1)


class TestTrdObJoin:
    def test_default(self):
        trd = get_cme_trd()
        data = trd.trd_ob_join(get_cme_ob())
        assert data.schema == {
            **trd.schema,
            'ASK_VWAP': float,
            'BID_VWAP': float,
            'ASK_SIZE': int,
            'BID_SIZE': int,
        }
        df = otp.run(data.limit(4), **RUN_KWARGS).to_dict(orient='list')
        del df['Time']
        assert df == {
            'PRICE': [17306.5, 17306.5, 17306.5, 17306.0],
            'SIZE': [1, 1, 1, 1],
            'ASK_VWAP': [17474.544311298392, 17474.544311298392, 17474.162637940095, 17474.124967165746],
            'BID_VWAP': [17020.59178811659, 17020.59178811659, 17020.670145699074, 17020.509952340904],
            'ASK_SIZE': [3797, 3797, 3806, 3807],
            'BID_SIZE': [3568, 3568, 3569, 3567],
        }

    def test_max_levels(self):
        data = get_cme_trd().trd_ob_join(get_cme_ob(), max_levels=1)
        df = otp.run(data.limit(4), **RUN_KWARGS).to_dict(orient='list')
        del df['Time']
        assert df == {
            'PRICE': [17306.5, 17306.5, 17306.5, 17306.0],
            'SIZE': [1, 1, 1, 1],
            'ASK_VWAP': [17307.0, 17307.0, 17306.75, 17306.75],
            'BID_VWAP': [17306.5, 17306.5, 17306.5, 17306.25],
            'ASK_SIZE': [1, 1, 1, 2],
            'BID_SIZE': [2, 2, 1, 1],
        }

    def test_exceptions(self):
        with pytest.raises(ValueError, match='order_book_delay'):
            get_cme_trd().trd_ob_join(get_cme_ob(), order_book_delay=-1)
        with pytest.raises(ValueError, match='max_levels'):
            get_cme_trd().trd_ob_join(get_cme_ob(), max_levels=0)
        with pytest.raises(ValueError, match='max_initialization_days'):
            get_cme_trd().trd_ob_join(get_cme_ob(), max_initialization_days=-1)


class TestTrdVsMid:
    def test_default(self):
        data = get_trd().trd_vs_mid(get_qte())
        assert data.schema == {'PRICE': float, 'SIZE': int}
        df = otp.run(data.limit(5), date=otp.dt(2024, 2, 1)).to_dict(orient='list')
        del df['Time']
        assert df == {
            'PRICE': [186.5, 185.68, 185.68, 185.68, 185.72],
            'SIZE': [6, 11, 10, 23, 5],
        }

    def test_relationship_to_mid(self):
        data = get_trd().trd_vs_mid(get_qte(), 'lt')
        df = otp.run(data.limit(5), date=otp.dt(2024, 2, 1)).to_dict(orient='list')
        del df['Time']
        assert df == {
            'PRICE': [185.59, 185.49, 185.49, 185.5, 185.5],
            'SIZE': [1, 107, 1, 2, 1],
        }

    def test_exceptions(self):
        with pytest.raises(ValueError, match='relationship_to_mid'):
            get_trd().trd_vs_mid(get_qte(), 'GE')
        with pytest.raises(ValueError, match="'PRICE'"):
            get_trd()[['SIZE']].trd_vs_mid(get_qte())
        with pytest.raises(ValueError, match="'ASK_PRICE'"):
            get_trd().trd_vs_mid(get_qte()[['BID_PRICE']])
        with pytest.raises(ValueError, match="'BID_PRICE'"):
            get_trd().trd_vs_mid(get_qte()[['ASK_PRICE']])


class TestTrdVsQuote:
    def test_default(self):
        data = get_trd().trd_vs_quote(get_qte())
        assert data.schema == {'PRICE': float, 'SIZE': int}
        df = otp.run(data.limit(5), date=otp.dt(2024, 2, 1)).to_dict(orient='list')
        del df['Time']
        assert df == {
            'PRICE': [185.9, 185.9, 185.98, 185.89, 185.89],
            'SIZE': [16, 87, 26, 122, 200],
        }

    def test_relationship_to_quote(self):
        data = get_trd().trd_vs_quote(get_qte(), 'eq')
        df = otp.run(data.limit(5), date=otp.dt(2024, 2, 1)).to_dict(orient='list')
        del df['Time']
        assert df == {
            'PRICE': [185.98, 185.89, 185.89, 185.9, 185.9],
            'SIZE': [26, 122, 200, 17, 89],
        }

    def test_side(self):
        data = get_trd().trd_vs_quote(get_qte(), 'eq', side='bid')
        df = otp.run(data.limit(5), date=otp.dt(2024, 2, 1)).to_dict(orient='list')
        del df['Time']
        assert df == {
            'PRICE': [185.49, 185.88, 185.88, 185.88, 185.88],
            'SIZE': [107, 100, 25, 648, 117],
        }

    def test_exceptions(self):
        with pytest.raises(ValueError, match='relationship_to_quote'):
            get_trd().trd_vs_quote(get_qte(), 'equal')
        with pytest.raises(ValueError, match='side'):
            get_trd().trd_vs_quote(get_qte(), side='ASK')
        with pytest.raises(ValueError, match="'PRICE'"):
            get_trd()[['SIZE']].trd_vs_quote(get_qte())
        with pytest.raises(ValueError, match="'ASK_PRICE'"):
            get_trd().trd_vs_quote(get_qte()[['BID_PRICE']])
        with pytest.raises(ValueError, match="'BID_PRICE'"):
            get_trd().trd_vs_quote(get_qte()[['ASK_PRICE']])
