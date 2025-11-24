import pytest

import onetick.py as otp
from onetick.py.otq import otq
from onetick.py.compatibility import is_supported_otq_ob_summary


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    data = otp.Ticks({
        'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
        # or DELETED_TIME
        'UPDATE_TIME': [otp.config['default_start_time']] * 5,
        'PRICE': [1, 2, 1, 2, 5],
        'SIZE': [1, 2, 3, 4, 7],
        'X': ['A', 'B', 'B', 'A', 'B'],
        'TICK_STATUS': [otp.byte(0)] * 5,
        'RECORD_TYPE': ['R'] * 5,
    })
    data.sink(otq.ModifyTsProperties('STATE_KEYS', 'PRICE,BUY_SELL_FLAG'))

    db = otp.DB('SOME_DB')
    db.add(src=data, symbol='AA', tick_type='PRL')
    m_session.use(db)

    db = otp.DB('BOUND_DB')
    db.add(src=data, symbol='AA', tick_type='PRL')
    db.add(src=data, symbol='AAA', tick_type='PRL')
    m_session.use(db)

    db = otp.DB('BOUND_DB_2')
    db.add(src=data, symbol='BB', tick_type='PRL')
    m_session.use(db)

    return m_session


class TestObSnapshot:

    def test_ob_snapshot(self):
        data = otp.ObSnapshot('SOME_DB', tick_type='PRL', symbols='AA', max_levels=1)
        assert data.schema == {
            'PRICE': float,
            'SIZE': int,
            'UPDATE_TIME': otp.nsectime,
            'BUY_SELL_FLAG': int,
            'LEVEL': int,
        }
        df = otp.run(data)
        assert len(df) == 2
        assert set(df['PRICE']) == {2, 5}
        assert set(df['SIZE']) == {4, 7}
        assert set(df['BUY_SELL_FLAG']) == {1, 0}
        assert len(set(df['LEVEL'])) == 1
        assert 'X' not in df

    def test_ob_snapshot_manual_schema(self):
        data = otp.ObSnapshot(tick_type='PRL', max_levels=1, schema_policy='manual')
        df = otp.run(data, symbols='SOME_DB::AA')
        assert set(df['PRICE']) == {2, 5}
        assert set(df['SIZE']) == {4, 7}
        assert set(df['BUY_SELL_FLAG']) == {1, 0}
        assert len(set(df['LEVEL'])) == 1


class TestObSnapshotWide:

    def test_ob_snapshot_wide(self):
        data = otp.ObSnapshotWide('SOME_DB', tick_type='PRL', symbols='AA', max_levels=1)
        assert data.schema == {
            'BID_PRICE': float,
            'BID_SIZE': int,
            'BID_UPDATE_TIME': otp.nsectime,
            'ASK_PRICE': float,
            'ASK_SIZE': int,
            'ASK_UPDATE_TIME': otp.nsectime,
            'LEVEL': int,
        }
        df = otp.run(data)
        assert len(df) == 1
        assert set(df['BID_PRICE']) == {5}
        assert set(df['BID_SIZE']) == {7}
        assert set(df['ASK_PRICE']) == {2}
        assert set(df['ASK_SIZE']) == {4}
        assert len(set(df['LEVEL'])) == 1
        assert 'X' not in df


class TestObSnapshotFlat:

    def test_ob_snapshot_flat(self):
        data = otp.ObSnapshotFlat('SOME_DB', tick_type='PRL', symbols='AA', max_levels=1)
        assert data.schema == {
            'BID_PRICE1': float,
            'BID_SIZE1': int,
            'BID_UPDATE_TIME1': otp.nsectime,
            'ASK_PRICE1': float,
            'ASK_SIZE1': int,
            'ASK_UPDATE_TIME1': otp.nsectime,
        }
        df = otp.run(data)
        assert len(df) == 1
        assert {
            'BID_PRICE1', 'BID_SIZE1', 'BID_UPDATE_TIME1',
            'ASK_PRICE1', 'ASK_SIZE1', 'ASK_UPDATE_TIME1',
        }.issubset(df)
        assert 'X' not in df


class TestObSummary:
    def test_ob_summary(self):
        if not is_supported_otq_ob_summary(throw_warning=True):
            with pytest.raises(RuntimeError):
                otp.run(
                    otp.ObSummary('SOME_DB', tick_type='PRL', symbols='AA', max_levels=1)
                )

            return

        data = otp.ObSummary('SOME_DB', tick_type='PRL', symbols='AA', max_levels=1)
        print(data.schema)
        assert data.schema == {
            'BID_SIZE': int,
            'BID_VWAP': float,
            'BEST_BID_PRICE': float,
            'WORST_BID_PRICE': float,
            'NUM_BID_LEVELS': int,
            'ASK_SIZE': int,
            'ASK_VWAP': float,
            'BEST_ASK_PRICE': float,
            'WORST_ASK_PRICE': float,
            'NUM_ASK_LEVELS': int,
        }
        df = otp.run(data)

        df_short = {}
        for key, value in df.items():
            value = value[0]
            if key == 'Time':
                continue

            df_short[key] = value

        assert df_short == {
            'BID_SIZE': 7,
            'BID_VWAP': 5.0,
            'BEST_BID_PRICE': 5.0,
            'WORST_BID_PRICE': 5.0,
            'NUM_BID_LEVELS': 1,
            'ASK_SIZE': 4,
            'ASK_VWAP': 2.0,
            'BEST_ASK_PRICE': 2.0,
            'WORST_ASK_PRICE': 2.0,
            'NUM_ASK_LEVELS': 1,
        }


class TestObSize:
    def test_ob_size(self):
        data = otp.ObSize('SOME_DB', tick_type='PRL', symbols='AA', max_levels=1)
        assert data.schema == {
            'ASK_VALUE': float,
            'BID_VALUE': float,
        }
        df = otp.run(data)

        assert list(df['ASK_VALUE']) == [4.0]
        assert list(df['BID_VALUE']) == [7.0]


class TestObVwap:
    def test_ob_vwap(self):
        data = otp.ObVwap('SOME_DB', tick_type='PRL', symbols='AA')
        assert data.schema == {
            'ASK_VALUE': float,
            'BID_VALUE': float,
        }
        df = otp.run(data)

        assert list(df['ASK_VALUE']) == [2.0]
        assert list(df['BID_VALUE']) == [pytest.approx(3.8)]


class TestObNumLevels:
    def test_ob_num_levels(self):
        data = otp.ObNumLevels('SOME_DB', tick_type='PRL', symbols='AA')
        assert data.schema == {
            'ASK_VALUE': float,
            'BID_VALUE': float,
        }
        df = otp.run(data)

        assert list(df['ASK_VALUE']) == [1.0]
        assert list(df['BID_VALUE']) == [2.0]


class TestObBoundSymbols:
    @pytest.mark.parametrize('symbols', [
        ['BOUND_DB::AA', 'BOUND_DB_2::BB'],
        otp.Symbols(db='BOUND_DB', for_tick_type='PRL', keep_db=True),
    ])
    def test_simple(self, symbols):
        data = otp.ObSnapshot(tick_type='PRL', symbols=symbols, schema_policy='manual', schema={
            'BUY_SELL_FLAG': int, 'PRICE': float, 'SIZE': float, 'UPDATE_TIME': otp.nsectime,
        })
        df = otp.run(data)
        df = df.to_dict(orient='list')
        assert [df['PRICE'], df['SIZE'], df['LEVEL']] == [[2.0, 5.0, 1.0], [8, 14, 6], [1, 1, 2]]
