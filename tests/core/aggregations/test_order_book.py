# pylama:ignore=E1123
import pytest

import onetick.py as otp
from onetick.py.compatibility import (
    is_supported_new_ob_snapshot_behavior, is_supported_otq_ob_summary, is_max_spread_supported,
)
from onetick.py.otq import otq


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    return m_session


class TestObSnapshot:

    @pytest.fixture
    def data(self):
        return otp.Ticks({
            'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
            # or DELETED_TIME
            'UPDATE_TIME': [otp.config['default_start_time']] * 5,
            'PRICE': [1, 2, 1, 2, 5],
            'SIZE': [1, 2, 3, 4, 7],
            'X': ['A', 'B', 'B', 'A', 'B'],
        })

    def test_ob_snapshot_check_input(self):
        with pytest.raises(TypeError):
            otp.agg.ob_snapshot().apply(
                otp.Tick(WRONG='INPUT')
            )

    def test_ob_snapshot(self, data):
        data = otp.agg.ob_snapshot().apply(data)
        assert 'X' not in data.schema
        df = otp.run(data)
        assert len(df) == 3
        assert set(df['PRICE']) == {1, 2, 5}
        assert set(df['SIZE']) == {4, 6, 7}
        assert set(df['BUY_SELL_FLAG']) == {0, 1}
        assert len(set(df['LEVEL'])) > 1
        assert 'X' not in df

    def test_ob_snapshot_running(self, data):
        data = otp.agg.ob_snapshot(running=True).apply(data)
        df = otp.run(data)
        assert len(df) > 3

    def test_ob_snapshot_buckets(self, data):
        data = otp.agg.ob_snapshot(bucket_interval=1,
                                   bucket_units='days',
                                   bucket_time='start').apply(data)
        df = otp.run(data)
        assert set(df['Time']) == {otp.config['default_start_time'] + otp.Day(i) for i in range(3)}

    def test_ob_snapshot_group_by(self, data):
        data = otp.agg.ob_snapshot(group_by=['X']).apply(data)
        df = otp.run(data)
        assert 'X' in df
        assert set(df['X']) == {'A', 'B'}
        assert len(df) == 5

    def test_ob_snapshot_side(self, data):
        data = otp.agg.ob_snapshot(side='ASK').apply(data)
        df = otp.run(data)
        assert all(df['BUY_SELL_FLAG'] == 1)

    def test_ob_snapshot_max_levels(self, data):
        data = otp.agg.ob_snapshot(max_levels=1).apply(data)
        df = otp.run(data)
        assert set(df['LEVEL']) == {1}

    def test_ob_snapshot_max_depth_shares(self, data):
        data = otp.agg.ob_snapshot(max_depth_shares=2).apply(data)
        df = otp.run(data)
        assert len(df) == 2

    def test_ob_snapshot_max_depth_for_price(self, data):
        data = otp.agg.ob_snapshot(max_depth_for_price=0.5).apply(data)
        df = otp.run(data)
        assert len(df) == 2

    @pytest.mark.skipif(not is_max_spread_supported(), reason='Not supported on this version of OneTick')
    def test_ob_snapshot_max_spread(self, data):
        data = otp.agg.ob_snapshot(max_spread=0.5).apply(data)
        df = otp.run(data)
        assert len(df) == 2

    @pytest.mark.skipif(not is_supported_new_ob_snapshot_behavior(), reason='new parameter was added')
    def test_ob_snapshot_book_uncross_method(self):
        data = otp.Ticks(
            BID_PRICE=[1, 1, 2, 3],
            BID_SIZE=[3, 5, 10, 15],
            ASK_PRICE=[2, 5, 4, 4],
            ASK_SIZE=[1, 10, 15, 17],
        )
        data.sink(otq.VirtualOb(output_book_format='PRL'))
        data.schema.set(
            PRICE=float,
            BUY_SELL_FLAG=int,
            SIZE=int,
            TICK_STATUS=int,
            DELETED_TIME=otp.nsectime,
        )
        with pytest.raises(ValueError):
            data = otp.agg.ob_snapshot(book_uncross_method='wrong').apply(data)
        data = otp.agg.ob_snapshot(book_uncross_method='REMOVE_OLDER_CROSSED_LEVELS').apply(data)
        otp.run(data)

    @pytest.mark.skipif(is_supported_new_ob_snapshot_behavior(), reason='ob_snapshot behavior was changed')
    def test_ob_snapshot_book_uncross_method_old(self, data):
        with pytest.raises(ValueError):
            data = otp.agg.ob_snapshot(book_uncross_method='wrong').apply(data)
        data = otp.agg.ob_snapshot(book_uncross_method='REMOVE_OLDER_CROSSED_LEVELS').apply(data)
        otp.run(data)

    def test_ob_snapshot_book_delimiters(self, data):
        data = otp.agg.ob_snapshot(book_delimiters='D').apply(data)
        df = otp.run(data)
        assert len(df) == 4
        assert df.iloc[-1]['DELIMITER'] == 'D'

    @pytest.mark.parametrize('param,value', [
        ('show_full_detail', True),
        ('show_only_changes', True),
        ('max_initialization_days', 2),
        # commented because we don't have state key in data
        # ('state_key_max_inactivity_sec', 2),
        ('size_max_fractional_digits', 7),
    ])
    def test_ob_snapshot_params(self, data, param, value):
        # not much to test here, because of the data, so just checking params exist
        data = otp.agg.ob_snapshot(**{param: value}).apply(data)
        otp.run(data)

    def test_ob_snapshot_fractional_sizes(self):
        data = otp.Ticks({
            'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
            'UPDATE_TIME': [otp.config['default_start_time']] * 5,
            'PRICE': [1, 2, 1, 2, 5],
            'SIZE': [1.1, 2.2, 3.3, 4.4, 7.5],
            'X': ['A', 'B', 'B', 'A', 'B'],
        })

        data = otp.agg.ob_snapshot(size_max_fractional_digits=8).apply(data)
        assert data.schema['SIZE'] == float

        df = otp.run(data)
        assert len(df) == 3
        assert set(df['PRICE']) == {1, 2, 5}
        assert set(df['SIZE']) == {4.4, 6.6, 7.5}


class TestObSnapshotWide:

    @pytest.fixture
    def data(self):
        return otp.Ticks({
            'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
            # or DELETED_TIME
            'UPDATE_TIME': [otp.config['default_start_time']] * 5,
            'PRICE': [1, 2, 1, 2, 5],
            'SIZE': [1, 2, 3, 4, 7],
            'X': ['A', 'B', 'B', 'A', 'B'],
        })

    def test_ob_snapshot_wide_check_input(self):
        with pytest.raises(TypeError):
            otp.agg.ob_snapshot_wide().apply(
                otp.Tick(WRONG='INPUT')
            )

    def test_ob_snapshot_wide(self, data):
        data = otp.agg.ob_snapshot_wide().apply(data)
        assert 'X' not in data.schema
        df = otp.run(data)
        assert len(df) == 2
        assert set(df['BID_PRICE']) == {1, 5}
        assert set(df['BID_SIZE']) == {4, 7}
        assert set(df['ASK_PRICE'].fillna('nan')) == {2, 'nan'}
        assert set(df['ASK_SIZE']) == {6, 0}
        assert len(set(df['LEVEL'])) > 1
        assert 'X' not in df

    def test_ob_snapshot_wide_fractional_sizes(self):
        data = otp.Ticks({
            'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
            'UPDATE_TIME': [otp.config['default_start_time']] * 5,
            'PRICE': [1, 2, 1, 2, 5],
            'SIZE': [1.1, 2.2, 3.3, 4.4, 7.5],
            'X': ['A', 'B', 'B', 'A', 'B'],
        })

        data = otp.agg.ob_snapshot_wide(size_max_fractional_digits=8).apply(data)
        assert data.schema['BID_SIZE'] == float
        assert data.schema['ASK_SIZE'] == float

        df = otp.run(data)
        assert set(df['BID_SIZE']) == {4.4, 7.5}
        assert set(df['ASK_SIZE']) == {6.6, 0}


class TestObSnapshotFlat:

    @pytest.fixture
    def data(self):
        return otp.Ticks({
            'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
            # or DELETED_TIME
            'UPDATE_TIME': [otp.config['default_start_time']] * 5,
            'PRICE': [1, 2, 1, 2, 5],
            'SIZE': [1, 2, 3, 4, 7],
            'X': ['A', 'B', 'B', 'A', 'B'],
        })

    def test_ob_snapshot_flat_check_max_levels(self, data):
        with pytest.raises(ValueError):
            otp.agg.ob_snapshot_flat().apply(data)
        with pytest.raises(ValueError):
            otp.agg.ob_snapshot_flat(max_levels=-1).apply(data)
        with pytest.raises(ValueError):
            otp.agg.ob_snapshot_flat(max_levels=999999).apply(data)

    def test_ob_snapshot_flat_check_input(self):
        with pytest.raises(TypeError):
            otp.agg.ob_snapshot_flat(max_levels=1).apply(
                otp.Tick(WRONG='INPUT')
            )

    def test_ob_snapshot_flat(self, data):
        data = otp.agg.ob_snapshot_flat(max_levels=2).apply(data)
        assert 'X' not in data.schema
        df = otp.run(data)
        assert len(df) == 1
        assert {
            'BID_PRICE1', 'BID_SIZE1', 'BID_UPDATE_TIME1',
            'ASK_PRICE1', 'ASK_SIZE1', 'ASK_UPDATE_TIME1',
            'BID_PRICE2', 'BID_SIZE2', 'BID_UPDATE_TIME2',
            'ASK_PRICE2', 'ASK_SIZE2', 'ASK_UPDATE_TIME2',
        }.issubset(df)
        assert 'X' not in df

    def test_ob_snapshot_flat_fractional_sizes(self):
        data = otp.Ticks({
            'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
            'UPDATE_TIME': [otp.config['default_start_time']] * 5,
            'PRICE': [1, 2, 1, 2, 5],
            'SIZE': [1.1, 2.2, 3.3, 4.4, 7.5],
            'X': ['A', 'B', 'B', 'A', 'B'],
        })

        data = otp.agg.ob_snapshot_flat(
            size_max_fractional_digits=8, max_levels=2
        ).apply(data)
        for i in (1, 2):
            assert data.schema[f'BID_SIZE{i}'] == float
            assert data.schema[f'ASK_SIZE{i}'] == float

        d = otp.run(data).iloc[0].to_dict()

        assert d['BID_SIZE1'] == 7.5
        assert d['BID_SIZE2'] == 4.4
        assert d['ASK_SIZE1'] == 6.6
        assert d['ASK_SIZE2'] == 0.


@pytest.mark.skipif(not is_supported_otq_ob_summary(), reason='not supported on older OneTick versions')
class TestObSummary:

    @pytest.fixture
    def data(self):
        return otp.Ticks({
            'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
            # or DELETED_TIME
            'UPDATE_TIME': [otp.config['default_start_time']] * 5,
            'PRICE': [1, 2, 1, 2, 3],
            'SIZE': [1, 2, 4, 3, 5],
            'X': ['A', 'B', 'B', 'A', 'B'],
        })

    def test_ob_summary_check_input(self):
        with pytest.raises(TypeError):
            otp.agg.ob_summary().apply(
                otp.Tick(WRONG='INPUT')
            )

    def test_ob_summary(self, data):
        data = otp.agg.ob_summary().apply(data)
        df = otp.run(data).to_dict()
        assert 'X' not in df

        df_short = {}
        for key, value in df.items():
            value = value[0]
            if key == 'Time':
                continue

            df_short[key] = value

        assert df_short == {
            'BID_SIZE': 10,
            'BID_VWAP': 2.0,
            'BEST_BID_PRICE': 3.0,
            'WORST_BID_PRICE': 1.0,
            'NUM_BID_LEVELS': 2,
            'ASK_SIZE': 5,
            'ASK_VWAP': 2.0,
            'BEST_ASK_PRICE': 2.0,
            'WORST_ASK_PRICE': 2.0,
            'NUM_ASK_LEVELS': 1,
        }

    def test_ob_summary_fractional_sizes(self):
        data = otp.Ticks({
            'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
            'UPDATE_TIME': [otp.config['default_start_time']] * 5,
            'PRICE': [1, 2, 1, 2, 3],
            'SIZE': [1.1, 2.2, 4.3, 3.4, 5.5],
            'X': ['A', 'B', 'B', 'A', 'B'],
        })

        data = otp.agg.ob_summary(size_max_fractional_digits=8).apply(data)
        assert data.schema['BID_SIZE'] == float
        assert data.schema['ASK_SIZE'] == float

        d = otp.run(data).iloc[0].to_dict()

        assert d['BID_SIZE'] == 10.9
        assert d['BID_VWAP'] == pytest.approx(2.0091, abs=1e-4)
        assert d['ASK_SIZE'] == 5.6
        assert d['ASK_VWAP'] == 2.0


class TestObSize:
    @pytest.fixture
    def data(self):
        return otp.Ticks({
            'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
            'UPDATE_TIME': [otp.config['default_start_time']] * 5,
            'PRICE': [1, 2, 1, 2, 3],
            'SIZE': [1, 2, 4, 3, 5],
            'ASK_X': [1] * 5,
            'BID_X': [1] * 5,
            'offset': [otp.Second(i) for i in range(5)],
        })

    @pytest.mark.parametrize('ask_field,bid_field,result', [
        (None, None, (5.0, 10.0)), ('ASK_X', None, (5.0, 10.0)),
        (None, 'BID_X', (5.0, 5.0)), ('ASK_X', 'BID_X', (5.0, 5.0)),
    ])
    def test_simple(self, data, ask_field, bid_field, result):
        data = otp.agg.ob_size(best_ask_price_field=ask_field, best_bid_price_field=bid_field).apply(data)
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'ASK_VALUE', 'BID_VALUE'}
        assert list(df['ASK_VALUE']) == [result[0]]
        assert list(df['BID_VALUE']) == [result[1]]

    @pytest.mark.parametrize('ask_field,bid_field,result', [
        (None, None, (5.0, 5.0)), ('ASK_X', None, (5.0, 5.0)),
        (None, 'BID_X', (5.0, 0.0)), ('ASK_X', 'BID_X', (5.0, 0.0)),
    ])
    def test_max_levels(self, data, ask_field, bid_field, result):
        data = otp.agg.ob_size(max_levels=1, best_ask_price_field=ask_field, best_bid_price_field=bid_field).apply(data)
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'ASK_VALUE', 'BID_VALUE'}
        assert list(df['ASK_VALUE']) == [result[0]]
        assert list(df['BID_VALUE']) == [result[1]]

    @pytest.mark.parametrize('ask_field,bid_field,result', [
        (None, None, ([2.0, 5.0], [5.0, 10.0])), ('ASK_X', None, ([2.0, 5.0], [5.0, 10.0])),
        (None, 'BID_X', ([2.0, 5.0], [5.0, 5.0])), ('ASK_X', 'BID_X', ([2.0, 5.0], [5.0, 5.0])),
    ])
    def test_interval(self, data, ask_field, bid_field, result):
        data = otp.agg.ob_size(
            best_ask_price_field=ask_field, best_bid_price_field=bid_field,
            bucket_interval=3, bucket_units='seconds',
        ).apply(data)
        df = otp.run(data, start=otp.config.default_start_time, end=otp.config.default_start_time + otp.Second(5))
        assert set(df.columns) == {'Time', 'ASK_VALUE', 'BID_VALUE'}
        assert list(df['ASK_VALUE']) == result[0]
        assert list(df['BID_VALUE']) == result[1]

    def test_interval_ticks(self):
        with pytest.raises(
            ValueError,
            match="'bucket_units' can be one of the following: 'seconds, days, months, flexible'"
        ):
            _ = otp.agg.ob_size(bucket_interval=3, bucket_units='ticks')

    def test_min_levels(self, data):
        data = otp.agg.ob_size(min_levels=2, max_depth_for_price=0.5).apply(data)
        df = otp.run(data)
        assert list(df['ASK_VALUE']) == [5.0]
        assert list(df['BID_VALUE']) == [10.0]

    @pytest.mark.parametrize('side,result', [('ASK', 5.0), ('BID', 10.0)])
    def test_side(self, data, side, result):
        data = otp.agg.ob_size(side=side).apply(data)
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'VALUE'}
        assert list(df['VALUE']) == [result]

    def test_exceptions(self, data):
        with pytest.raises(ValueError):
            _ = otp.agg.ob_size(best_ask_price_field='A').apply(data)

        with pytest.raises(ValueError):
            _ = otp.agg.ob_size(best_bid_price_field='A').apply(data)

        with pytest.raises(ValueError):
            _ = otp.agg.ob_size(min_levels=2).apply(data)

    @pytest.mark.skipif(not is_max_spread_supported(), reason='Not supported on this version of OneTick')
    def test_ob_snapshot_max_spread(self, data):
        data = otp.agg.ob_size(max_spread=0.2).apply(data)
        df = otp.run(data)
        assert len(df) == 1


class TestObVwap:
    @pytest.fixture
    def data(self):
        return otp.Ticks({
            'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
            'UPDATE_TIME': [otp.config['default_start_time']] * 5,
            'PRICE': [1, 2, 1, 2, 3],
            'SIZE': [1, 2, 4, 3, 5],
            'offset': [otp.Second(i) for i in range(5)],
        })

    def test_simple(self, data):
        data = otp.agg.ob_vwap().apply(data)
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'ASK_VALUE', 'BID_VALUE'}
        assert list(df['ASK_VALUE']) == [2.0]
        assert list(df['BID_VALUE']) == [2.0]

    def test_interval(self, data):
        with pytest.raises(
            ValueError,
            match="'bucket_units' can be one of the following: 'seconds, days, months, flexible'"
        ):
            data = otp.agg.ob_vwap(bucket_interval=3, bucket_units='ticks').apply(data)
        data = otp.agg.ob_vwap(bucket_interval=3, bucket_units='seconds').apply(data)
        df = otp.run(data, start=otp.config.default_start_time, end=otp.config.default_start_time + otp.Second(5))
        assert set(df.columns) == {'Time', 'ASK_VALUE', 'BID_VALUE'}
        assert list(df['ASK_VALUE']) == [2.0, 2.0]
        assert list(df['BID_VALUE']) == [1.0, 2.0]

    @pytest.mark.parametrize('side,result', [('ASK', 2.0), ('BID', 2.0)])
    def test_side(self, data, side, result):
        data = otp.agg.ob_vwap(side=side).apply(data)
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'VALUE'}
        assert list(df['VALUE']) == [result]


class TestObNumLevels:
    @pytest.fixture
    def data(self):
        return otp.Ticks({
            'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
            'UPDATE_TIME': [otp.config['default_start_time']] * 5,
            'PRICE': [1, 2, 1, 2, 3],
            'SIZE': [1, 2, 4, 3, 5],
            'offset': [0, 1000, 2000, 3000, 4000]
        })

    def test_simple(self, data):
        data = otp.agg.ob_num_levels().apply(data)
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'ASK_VALUE', 'BID_VALUE'}
        assert list(df['ASK_VALUE']) == [1.0]
        assert list(df['BID_VALUE']) == [2.0]

    @pytest.mark.parametrize('interval', [3, otp.Second(3)])
    def test_interval(self, data, interval):
        start_time = otp.datetime(2003, 12, 1)
        data = otp.agg.ob_num_levels(bucket_interval=interval, running=False).apply(data)
        df = otp.run(data, start=start_time, end=start_time + otp.Second(6))
        assert set(df.columns) == {'Time', 'ASK_VALUE', 'BID_VALUE'}
        assert list(df['ASK_VALUE']) == [1.0, 1.0]
        assert list(df['BID_VALUE']) == [1.0, 2.0]

    def test_time_offset_convert(self):
        start_time = otp.datetime(2003, 12, 1)
        data = otp.Ticks({
            'BUY_SELL_FLAG': [0, 1, 0, 1, 0],
            'UPDATE_TIME': [otp.config['default_start_time']] * 5,
            'PRICE': [1, 2, 1, 2, 3],
            'SIZE': [1, 2, 4, 3, 5],
            'offset': [0, otp.Minute(1), otp.Minute(2), otp.Minute(3), otp.Minute(4)]
        })
        data = otp.agg.ob_num_levels(bucket_interval=otp.Minute(3)).apply(data)
        df = otp.run(data, start=start_time, end=start_time + otp.Minute(6))

        assert list(df['ASK_VALUE']) == [1.0, 1.0]
        assert list(df['BID_VALUE']) == [1.0, 2.0]

        times = list(df['Time'])
        assert times[0] + otp.Minute(3) == times[1]

    @pytest.mark.parametrize('side,result', [('ASK', 1.0), ('BID', 2.0)])
    def test_side(self, data, side, result):
        data = otp.agg.ob_num_levels(side=side).apply(data)
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'VALUE'}
        assert list(df['VALUE']) == [result]
