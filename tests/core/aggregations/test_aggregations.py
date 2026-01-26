import os

import onetick.py as otp
from onetick.py.aggregations.other import (First, Last, FirstTime, LastTime, Count, Vwap, FirstTick, Distinct,
                                           Sum, Average, StdDev, TimeWeightedAvg, LastTick, Variance,
                                           Percentile, FindValueForPercentile, ExpWAverage, ExpTwAverage,
                                           StandardizedMoment, PortfolioPrice, MultiPortfolioPrice, Return,
                                           ImpliedVol, LinearRegression, PartitionEvenlyIntoGroups)
from onetick.py.aggregations.high_low import Max, Min, HighTime, LowTime, HighTick, LowTick
from onetick.py.compatibility import (is_percentile_bug_fixed,
                                      is_supported_agg_option_price,
                                      is_supported_num_distinct,
                                      is_supported_large_ints_empty_interval,
                                      is_standardized_moment_supported)
from onetick.py.otq import otq
import pytest
import numpy as np
import pandas as pd


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    db = otp.DB('DB')
    day_1 = otp.Tick(PRICE=15, X=1)
    day_2 = otp.Tick(PRICE=10)
    db.add(day_1, date=otp.datetime(2020, 1, 1))
    db.add(day_2, date=otp.datetime(2020, 1, 2))
    m_session.use(db)
    return m_session


@pytest.mark.parametrize('aggregation,expected_fun,expected_str', [(Sum, sum, 'SUM'),
                                                                   (Average, lambda x: sum(x) / len(x),
                                                                    'AVERAGE'),
                                                                   (StdDev, np.std, 'STDDEV'),
                                                                   (Min, min, 'LOW'),
                                                                   (Max, max, 'HIGH')])
class TestSumAvgStdMinMax:
    def test_simple(self, aggregation, expected_fun, expected_str):
        d = {'A': [1, 2, 3, 5]}
        data = otp.Ticks(d)
        agg = aggregation('A')
        assert agg.NAME == expected_str
        data = agg.apply(data)
        df = otp.run(data)
        assert df['A'][0] == expected_fun(d['A'])

    def test_use_str(self, aggregation, expected_fun, expected_str):
        d = {'A': ['a', 'b', 'c']}
        data = otp.Ticks(d)
        agg = aggregation('A')
        with pytest.raises(TypeError):
            agg.apply(data)

    @pytest.mark.parametrize('value', [otp.inf, otp.nan])
    def test_inf(self, aggregation, expected_fun, expected_str, value):
        data = otp.Ticks({'A': [value]})
        agg = aggregation('A')
        data = agg.apply(data)
        if aggregation is StdDev and value is otp.inf:
            with pytest.raises(Exception, match='Error computing variance: infinite input values are not supported'):
                otp.run(data)
        else:
            otp.run(data)      # checking that validation and otp.run won't fail


class TestMinMaxNsec:

    @pytest.mark.parametrize('name', [None, 'TT'])
    def test_large_int(self, name):
        data = otp.Ticks({'A': [1, 2, 3]})
        data['TS'] = data['TIMESTAMP']
        agg = Max(data['TS'])
        data = agg.apply(data, name)
        name = name or 'TS'
        assert data.schema == {name: otp.nsectime}
        df = otp.run(data)
        assert set(df.columns) == {'Time', name}
        assert df[name][0] == otp.config['default_start_time'] + otp.Milli(2)

    def test_large_int_gb_operation(self):
        data = otp.Ticks({'GB': [1, 1, 2, 2], 'GB1': 'aabc', 'GB2': 'aazx'})
        data['TS'] = data['TIMESTAMP']
        agg = Max(data['TS'], group_by=[data['GB'], data['GB1'] + data['GB2']])
        data = agg.apply(data)
        assert data.schema == {'TS': otp.nsectime, 'GB': int, 'GROUP_1': str}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'TS', 'GB', 'GROUP_1'}
        assert list(df['TS']) == [otp.config['default_start_time'] + otp.Milli(i) for i in [1, 2, 3]]

    def test_large_int_all_fields_error(self):
        data = otp.Ticks({'A': [1, 2, 3]})
        data['TS'] = data['TIMESTAMP']
        agg = Max(data['TS'], all_fields=True, running=True)
        with pytest.raises(ValueError, match="already existing fields: 'TS'"):
            agg.apply(data)

    def test_large_int_all_fields(self):
        data = otp.Ticks({'A': [1, 2, 3]})
        data['TS'] = data['TIMESTAMP']
        agg = Max(data['TS'], running=True, all_fields=True)
        data = agg.apply(data, 'TT')

        assert data.schema == {'TT': otp.nsectime, 'TS': otp.nsectime, 'A': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'TS', 'TT', 'A'}
        assert list(df['TT']) == [otp.config['default_start_time'] + otp.Milli(i) for i in range(3)]

    @pytest.mark.parametrize('aggr_func', [otp.agg.min, otp.agg.max])
    def test_aggregation_reuse(self, aggr_func):
        aggr = aggr_func('TIMESTAMP')
        assert aggr.column_name == 'TIMESTAMP'
        assert otp.run(otp.Tick(A=1).agg({'X': aggr}))['X'][0] == otp.config['default_start_time']
        assert aggr.column_name == 'TIMESTAMP'
        assert otp.run(otp.Tick(A=1).agg({'X': aggr}))['X'][0] == otp.config['default_start_time']


class TestFirstLast:

    @pytest.mark.parametrize('aggregation', [First, Last])
    @pytest.mark.parametrize('column', ['A', 'S'])
    def test_simple(self, column, aggregation):
        d = {'A': [1, 2, 3], 'S': ['a', 'b', 'c']}
        data = otp.Ticks(d)
        agg = aggregation(column)
        data = agg.apply(data)
        df = otp.run(data)
        assert len(df) == 1
        if aggregation.NAME == 'FIRST':
            assert df[column][0] == d[column][0]
        else:
            assert df[column][0] == d[column][-1]

    def test_datetime(self):
        ts = otp.Tick(A=0)
        ts['MS'] = ts['TIMESTAMP'] + otp.Milli(1)
        ts['NS'] = ts['TIMESTAMP'] + otp.Nano(1)
        ts = ts.agg({
            'FIRST_MS': otp.agg.first('MS'),
            'LAST_MS': otp.agg.last('MS'),
            'FIRST_NS': otp.agg.first('NS'),
            'LAST_NS': otp.agg.last('NS'),
        })
        df = otp.run(ts)
        assert df['FIRST_MS'][0] == otp.config.default_start_time + otp.Milli(1)
        assert df['LAST_MS'][0] == otp.config.default_start_time + otp.Milli(1)
        assert df['FIRST_NS'][0] == otp.config.default_start_time + otp.Nano(1)
        assert df['LAST_NS'][0] == otp.config.default_start_time + otp.Nano(1)


@pytest.mark.parametrize('aggregation', [FirstTime,
                                         LastTime])
class TestFirstLastTime:

    def test_simple(self, aggregation):
        data = otp.Ticks({'A': [1, 1, 2], 'offset': [1, 2, 3]})
        agg = aggregation()
        data = agg.apply(data, 'ASD')
        df = otp.run(data)
        if aggregation.NAME == 'FIRST_TIME':
            assert df['ASD'][0] == otp.config['default_start_time'] + otp.Milli(1)
        else:
            assert df['ASD'][0] == otp.config['default_start_time'] + otp.Milli(3)


@pytest.mark.parametrize('aggregation,exp_fun', [(HighTime, max),
                                                 (LowTime, min)])
class TestHighLowTime:

    @pytest.mark.parametrize('selection', ['first', 'last'])
    def test_simple(self, aggregation, exp_fun, selection):
        d = {'A': [3, 1, 4, 4, 1, 2], 'offset': [1, 2, 3, 4, 5, 6]}
        data = otp.Ticks(d)
        agg = aggregation('A', selection=selection)
        data = agg.apply(data)
        df = otp.run(data)
        values = d['A'].copy()
        if selection == 'last':
            values.reverse()
        exp_idx = values.index(exp_fun(values))
        if selection == 'last':
            d['offset'].reverse()
        assert df['A'][0] == otp.config['default_start_time'] + otp.Milli(d['offset'][exp_idx])

    def test_use_str(self, aggregation, exp_fun):
        data = otp.Ticks({'A': 'asd'})
        agg = aggregation('A')
        with pytest.raises(TypeError):
            agg.apply(data)


class TestCount:

    def test_simple(self):
        data = otp.Ticks({'A': [1, 2, 3], 'offset': [1, 2, 3]})
        agg = Count()
        data = agg.apply(data)
        df = otp.run(data)
        assert df['VALUE'][0] == 3


class TestVwap:

    def test_simple(self):
        data = otp.Ticks({'P': [1, 2, 4], 'S': [2, 4, 8]})
        agg = Vwap(price_column='P', size_column='S')
        data = agg.apply(data)
        df = otp.run(data)
        assert df['VWAP'][0] == 3

    def test_missing_column(self):
        data = otp.Ticks({'A': [1, 2, 3]})
        agg = Vwap(price_column='A', size_column='B')
        with pytest.raises(TypeError, match='uses column `B`'):
            agg.apply(data)
        agg = Vwap(price_column='B', size_column='A')
        with pytest.raises(TypeError, match='uses column `B`'):
            agg.apply(data)

    def test_use_str(self):
        data = otp.Ticks({'P': 'abc', 'S': 'zxc'})
        agg = Vwap(price_column='P', size_column='S')
        with pytest.raises(TypeError):
            agg.apply(data)

    def test_str(self):
        assert str(Vwap(price_column='P', size_column='S')) == 'VWAP(PRICE_FIELD_NAME=P,SIZE_FIELD_NAME=S)'

    def test_vwap_ts(self):
        data = otp.Ticks({'A': [1, 2, 3]})
        data['TS1'] = data['TIMESTAMP']
        data['TS2'] = data['TIMESTAMP']
        agg = Vwap(price_column='TS1', size_column='TS2')
        data = agg.apply(data)
        otp.run(data)

    def test_vwap_with_columns(self):
        data = otp.Ticks({'P': [1, 2, 4], 'S': [2, 4, 8]})
        agg = Vwap(price_column=data['P'], size_column=data['S'])
        data = agg.apply(data)
        df = otp.run(data)
        assert df['VWAP'][0] == 3


class TestTwAvg:

    def test_simple(self):
        data = otp.Ticks({'A': [1, 2, 3]})
        agg = TimeWeightedAvg('A')
        data = agg.apply(data)
        df = otp.run(data)
        assert df['A'][0] == pytest.approx(3)

    def test_use_str(self):
        data = otp.Ticks({'A': 'asd'})
        agg = TimeWeightedAvg('A')
        with pytest.raises(TypeError):
            agg.apply(data)


@pytest.mark.parametrize('aggregation,exp_fun', [(FirstTick, lambda x: x[0]),
                                                 (LastTick, lambda x: x[-1])])
class TestFirstTick:

    def test_simple(self, aggregation, exp_fun):
        data = otp.Ticks({'A': [1, 2, 3, 4], 'B': 'asdq'})
        agg = aggregation(keep_timestamp=False)
        data = agg.apply(data)
        assert data.schema == {'A': int, 'B': str, 'TICK_TIME': otp.nsectime}
        df = otp.run(data)
        assert len(df) == 1
        assert set(df.columns) == {'Time', 'A', 'B', 'TICK_TIME'}
        assert df['A'][0] == exp_fun([1, 2, 3, 4])

    def test_flexible_schema(self, aggregation, exp_fun):
        data = otp.Ticks({'A': [1, 1, 2, 4], 'B': 'asdq'})
        data.schema.set(**{'A': int})
        assert data.schema == {'A': int}
        agg = aggregation(group_by=data['A'])
        data = agg.apply(data)
        assert data.schema == {'A': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'A', 'B'}
        assert df.shape == (3, 3)
        df = df.set_index('A')
        assert df['B'][1] == exp_fun('as')
        assert df['B'][2] == exp_fun('d')
        assert df['B'][4] == exp_fun('q')

    def test_keep_timestamp_ambiguous(self, aggregation, exp_fun):
        # PY-947
        data = otp.Ticks({'A': [1, 2, 3, 4]})
        data.sink(otq.AddField('TIMESTAMP', 'nsectime(0)'))
        agg = aggregation(keep_timestamp=True)
        data = agg.apply(data)
        with pytest.raises(Exception, match='Ambiguous use of TIMESTAMP'):
            otp.run(data)


class TestDistinct:

    def test_simple(self):
        data = otp.Ticks({'A': [1, 2, 3, 3], 'B': 'asdq'})
        agg = Distinct(keys='A')
        data = agg.apply(data)
        assert data.schema == {'A': int}
        df = otp.run(data)
        assert df.shape == (3, 2)
        assert set(df.columns) == {'Time', 'A'}
        assert list(df['A']) == [1, 2, 3]

    def test_not_key_attrs_only(self):
        data = otp.Ticks({'A': [1, 1, 1, 2], 'B': 'aabc', 'D': [1, 2, 3, 4]})
        agg = Distinct(keys=[data['A'], data['B']], key_attrs_only=False)
        data = agg.apply(data)
        assert data.schema == {'A': int, 'B': str, 'D': int}
        df = otp.run(data)
        assert df.shape == (3, 4)
        assert set(df.columns) == {'Time', 'A', 'B', 'D'}
        assert set(df['A']) == {1, 2}
        assert set(df['B']) == {'a', 'b', 'c'}

    def test_flexible_schema(self):
        """Test checks that if field not in schema it won't be dropped"""
        t = otp.Ticks({'A': [1, 2, 3], 'B': [4, 5, 6]})
        t.sink(otq.AddField(field='NOT_IN_SCHEMA', value=1))
        schema_before_agg = t.schema.copy()
        agg = Distinct('A', key_attrs_only=False)
        t = agg.apply(t)
        assert t.schema == schema_before_agg
        assert 'NOT_IN_SCHEMA' not in schema_before_agg
        df = otp.run(t)
        assert 'NOT_IN_SCHEMA' in df


class TestGroupByCollision:
    def test_collision(self):
        r = otp.Ticks({
            'DENSITY':       [5, 4, 3, 6, 2],
            'CREATION_TIME': range(5),
            'BUY_FLAG':      ['0'] * 5,
            'offset':        range(5),
        })
        for group_by in [r.BUY_FLAG, "BUY_FLAG"]:
            r2 = r.agg(
                {
                    'ALERT_END_TS': otp.agg.last_time(),
                    'PINGS_N': otp.agg.count(),
                },
                bucket_units='flexible',
                bucket_end_condition=r['DENSITY'] > r['DENSITY'][-1],
                all_fields=True,

                bucket_time='start',
                group_by=[group_by],
            )
            df = otp.run(r2)
            assert len(df) == 2
            assert sorted(list(df.columns)) == [
                'ALERT_END_TS', 'BUY_FLAG', 'CREATION_TIME', 'DENSITY', 'PINGS_N', 'Time']


class TestTickAggregationPolicies:
    @pytest.mark.parametrize('all_fields,expected', [
        (True, 0),
        ('first', 0),
        ('last', 5),
        ('high', 3),
        ('low', 2),
    ])
    def test_last_tick_on_column(self, all_fields, expected):
        r = otp.Ticks({
            'X': list(range(6)),
            'PRICE': [3, 1, 0, 5, 4, 2],
            'offset': list(range(6)),
        })

        r2 = r.agg(
            {
                'COUNT': otp.agg.count(),
            },
            all_fields=all_fields,
        )
        df = otp.run(r2)
        assert len(df) == 1
        assert sorted(list(df.columns)) == ['COUNT', 'PRICE', 'Time', 'X']
        assert df['X'][0] == expected

    @pytest.mark.parametrize('all_fields,raised', [
        (True, False),
        (False, False),
        ('first', True),
        ('last', True),
        ('high', True),
        ('low', True),
    ])
    def test_fail_if_running(self, all_fields, raised):
        r = otp.Ticks({
            'X': list(range(6)),
            'PRICE': [3, 1, 0, 5, 4, 2],
            'offset': list(range(6)),
        })

        if raised:
            with pytest.raises(ValueError):
                r.agg({'COUNT': otp.agg.count()}, all_fields=all_fields, running=True)
        else:
            r.agg({'COUNT': otp.agg.count()}, all_fields=all_fields, running=True)

    @pytest.mark.parametrize('all_fields,expected', [
        (True, 1),
        ('first', 1),
        ('last', 0),
        ('high', 1),
        ('low', 0),
    ])
    def test_different_schemas(self, all_fields, expected):
        r = otp.DataSource('DB')

        r2 = r.agg(
            {
                'COUNT': otp.agg.count(),
            },
            all_fields=all_fields,
        )
        df = otp.run(r2, start=otp.datetime(2020, 1, 1), end=otp.datetime(2020, 1, 7))
        assert len(df) == 1
        assert sorted(list(df.columns)) == ['COUNT', 'PRICE', 'Time', 'X']
        assert df['X'][0] == expected

    @pytest.mark.parametrize('all_fields', ['low', 'high'])
    def test_no_price(self, all_fields):
        data = otp.Ticks(A=[3, 1, 4, 3])
        with pytest.raises(TypeError):
            data.agg({'SUM': otp.agg.sum(data['A'])}, all_fields=all_fields)

    @pytest.mark.parametrize('all_fields', [otp.agg.low_tick, otp.agg.high_tick])
    def test_no_field(self, all_fields):
        data = otp.Ticks(A=[3, 1, 4, 3])
        with pytest.raises(TypeError):
            data.agg({'SUM': otp.agg.sum(data['A'])}, all_fields=all_fields('B'))

    @pytest.mark.parametrize('all_fields', [otp.agg.low_tick, otp.agg.high_tick])
    def test_wrong_type(self, all_fields):
        data = otp.Ticks(A=[3, 1, 4, 3], B=['a', 'b', 'c', 'd'])
        with pytest.raises(TypeError):
            data.agg({'SUM': otp.agg.sum(data['A'])}, all_fields=all_fields('B'))

    @pytest.mark.parametrize('all_fields', [otp.agg.low_tick, otp.agg.high_tick])
    @pytest.mark.parametrize('column', [True, False])
    def test_okay(self, all_fields, column):
        data = otp.Ticks(A=[3, 1, 4, 3])
        all_fields_column = data['A'] if column else 'A'
        data = data.agg({'SUM': otp.agg.sum(data['A'])}, all_fields=all_fields(all_fields_column))
        df = otp.run(data)
        assert len(df) == 1
        assert df['SUM'][0] == 11
        if all_fields is otp.agg.low_tick:
            assert df['A'][0] == 1
        else:
            assert df['A'][0] == 4


class TestOperationInAggregation:
    def test_agg_operation(self):
        data = otp.Ticks(X=[0, 1, 2, 3, 4],
                         Y=[0, 0, 1, 0, 1])
        with pytest.raises(ValueError):
            data = Average(data['X'] + data['Y']).apply(data)
        data = Average(data['X'] + data['Y']).apply(data, name='Z')
        assert set(data.schema) == {'Z'}
        df = otp.run(data)
        assert set(df) == {'Time', 'Z'}
        assert df['Z'][0] == 2.4

    def test_agg_operation_running(self):
        data = otp.Ticks(X=[0, 1, 2, 3, 4],
                         Y=[0, 0, 1, 0, 1])
        data = Average(data['X'] + data['Y'], running=True, all_fields=True).apply(data, name='Z')
        df = otp.run(data)
        assert set(df) == {'Time', 'X', 'Y', 'Z'}

    def test_agg_operation_one_after_another(self):
        data = otp.Ticks(X=[0, 1, 2, 3, 4],
                         Y=[0, 0, 1, 0, 1])
        data = Average(data['X'] + data['Y'], running=True, all_fields=True).apply(data, name='Z')
        assert set(data.schema) == {'X', 'Y', 'Z'}
        data = Average(data['X'] + data['Y']).apply(data, name='Z')
        df = otp.run(data)
        assert set(df) == {'Time', 'Z'}
        assert df['Z'][0] == 2.4

    def test_agg_compute_operation(self):
        data = otp.Ticks(X=[0, 1, 2, 3, 4],
                         Y=[0, 0, 1, 0, 1])
        data = data.agg({'MEAN': otp.agg.average(data['X'] + data['Y'])})
        df = otp.run(data)
        assert set(df) == {'Time', 'MEAN'}
        assert df['MEAN'][0] == 2.4

    def test_agg_compute_operation_one_after_another(self):
        data = otp.Ticks(X=[0, 1, 2, 3, 4],
                         Y=[0, 0, 1, 0, 1])
        data = data.agg({'MEAN': otp.agg.average(data['X'] + data['Y'])}, running=True, all_fields=True)
        assert set(data.schema) == {'X', 'Y', 'MEAN'}
        data = data.agg({'MEAN': otp.agg.average(data['X'] + data['Y'])})
        df = otp.run(data)
        assert set(df) == {'Time', 'MEAN'}
        assert df['MEAN'][0] == 2.4

    def test_agg_compute_operation_2(self):
        data = otp.Ticks(X=[0, 1, 2, 3, 4],
                         Y=[0, 0, 1, 0, 1])
        data = data.agg({
            'MEAN': otp.agg.average(data['X'] + data['Y']),
            'MEAN_X': otp.agg.average(data['X'] * data['Y'])
        })
        df = otp.run(data)
        assert set(df) == {'Time', 'MEAN', 'MEAN_X'}
        assert df['MEAN'][0] == 2.4
        assert df['MEAN_X'][0] == 1.2


class TestInheritance:
    @pytest.mark.parametrize('cls', (HighTick, LowTick, HighTime, LowTime))
    def test_selection(self, cls):
        with pytest.raises(ValueError):
            cls('A', selection='kek')
        obj = cls('A', selection='last')
        assert obj.selection == 'last'
        assert 'selection' in cls.FIELDS_MAPPING
        assert 'selection' in cls.FIELDS_DEFAULT

    @pytest.mark.parametrize('cls', (
        HighTick, LowTick,
        HighTime, LowTime,
        Max, Min,
        First, Last,
        FirstTime, LastTime,
        FirstTick, LastTick
    ))
    def test_time_series_type(self, cls):
        with pytest.raises(ValueError):
            if cls in (FirstTime, LastTime, FirstTick, LastTick):
                cls(time_series_type='kek')
            else:
                cls('A', time_series_type='kek')
        if cls in (FirstTime, LastTime, FirstTick, LastTick):
            obj = cls(time_series_type='state_ts')
        else:
            obj = cls('A', time_series_type='state_ts')
        assert obj.time_series_type == 'state_ts'
        assert 'time_series_type' in cls.FIELDS_MAPPING
        assert 'time_series_type' in cls.FIELDS_DEFAULT

    @pytest.mark.parametrize('cls', (HighTick, LowTick, FirstTick, LastTick))
    def test_keep_timestamp(self, cls):
        obj = cls('A', keep_timestamp=False)
        assert obj.keep_timestamp is False


@pytest.mark.skipif(not is_supported_num_distinct(), reason='NumDistinct is not available on older builds')
class TestNumDistinct:

    def test_apply(self):
        data = otp.Ticks({'A': [1, 2, 3, 3]})
        agg = otp.agg.num_distinct('A')
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['VALUE']) == [3]

    def test_agg(self):
        data = otp.Ticks({'A': [1, 2, 3, 3]})
        data = data.agg({'X': otp.agg.num_distinct(keys='A')})
        df = otp.run(data)
        assert list(df['X']) == [3]

    def test_error(self):
        data = otp.Ticks({'A': [1, 1, 1]})
        with pytest.raises(TypeError):
            otp.agg.num_distinct(keys='F').apply(data)
        with pytest.raises(TypeError):
            data.agg({'X': otp.agg.num_distinct(keys='F')})

    def test_schema(self):
        data = otp.Ticks({'A': [1, 1, 1]})
        data = data.agg({'X': otp.agg.num_distinct(keys='A')})
        assert data.schema == {'X': int}
        df = otp.run(data)
        assert df.dtypes['X'] == np.int64
        assert list(df['X']) == [1]

    def test_keys(self):
        data = otp.Ticks({'A': [1, 1, 1], 'B': [1, 1, 2]})
        data = data.agg({'X': otp.agg.num_distinct(keys=['A', 'B'])})
        df = otp.run(data)
        assert list(df['X']) == [2]

    def test_running(self):
        data = otp.Ticks({'A': [1, 2, 1, 3, 1, 2, 4]})
        data = data.agg({'X': otp.agg.num_distinct(keys=['A'])}, running=True)
        df = otp.run(data)
        assert list(df['X']) == [1, 2, 2, 3, 3, 3, 4]

    def test_group_by_with_buckets(self):
        ticks = otp.Ticks(
            {
                'QTY': [10, 2, 30, 4, 50, 6, 70, 8],
                'TRADER': ['A', 'B', 'A', 'B', 'A', 'B', 'A', 'B']
            }
        )

        ticks = ticks.agg(
            {
                'SUM_QTY': otp.agg.sum('QTY'),
            },
            group_by='TRADER',
            bucket_interval=3,
            bucket_units='ticks',
            running=True,
            all_fields=True,
            end_condition_per_group=True,
        )

        row = otp.run(ticks).iloc[4].to_dict()
        expected_row = {'TRADER': 'A', 'QTY': 50, 'SUM_QTY': 90}

        assert all(row[key] == value for key, value in expected_row.items())


class TestOptionPrice:
    def test_agg(self):
        data = otp.Ticks(
            PRICE=[100.7, 101.1, 99.5],
            OPTION_TYPE=['CALL'] * 3,
            STRIKE_PRICE=[100.0] * 3,
            DAYS_TILL_EXPIRATION=[30] * 3,
        )
        if is_supported_agg_option_price():
            data = data.agg({
                'RESULT': otp.agg.option_price(
                    option_type_field_name='OPTION_TYPE',
                    strike_price_field_name='STRIKE_PRICE',
                    days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
                    volatility=0.25,
                    interest_rate=0.05,
                )
            })
            df = otp.run(data)
            assert df['RESULT'][0] == 2.8009992601230636
        else:
            with pytest.raises(NotImplementedError):
                data.agg({
                    'RESULT': otp.agg.option_price(
                        option_type_field_name='OPTION_TYPE',
                        strike_price_field_name='STRIKE_PRICE',
                        days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
                        volatility=0.25,
                        interest_rate=0.05,
                    )
                })

    def test_case1(self):
        data = {
            "PRICE": 120.,
            "OPTION_TYPE": "put",
            "STRIKE_PRICE": 110.,
            "DAYS_TILL_EXPIRATION": 15,
            "VOLATILITY": 0.2,
            "INTEREST_RATE": 0.05,
        }
        data = otp.Tick(**data)
        data = otp.agg.option_price(
            option_type_field_name='OPTION_TYPE',
            strike_price_field_name='STRIKE_PRICE',
            days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
            volatility_field_name='VOLATILITY',
            interest_rate_field_name='INTEREST_RATE',
            compute_delta=True,
            compute_gamma=True,
            compute_theta=True,
            compute_vega=True,
            compute_rho=True
        ).apply(data)
        df = otp.run(data)
        expected = dict(
            VALUE=0.02294724243400,
            DELTA=-0.01331023410672,
            GAMMA=0.00702208225870,
            THETA=-1.81737208518948,
            VEGA=0.83110672212574,
            RHO=-0.06658254802357,
        )
        if otp.compatibility.is_option_price_theta_value_changed():
            expected['THETA'] = -1.94135092374397
        for key, val in expected.items():
            assert val == pytest.approx(df[key][0])

    def test_case2(self):
        data = {
            "PRICE": 80.,
            "OPTION_TYPE": "put",
            "STRIKE_PRICE": 90.,
            "DAYS_TILL_EXPIRATION": 20,
            "VOLATILITY": 0.3,
            "INTEREST_RATE": 0.08,
        }
        data = otp.Tick(**data)
        data = otp.agg.option_price(
            option_type_field_name='OPTION_TYPE',
            strike_price_field_name='STRIKE_PRICE',
            days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
            volatility_field_name='VOLATILITY',
            interest_rate_field_name='INTEREST_RATE',
            compute_delta=True,
            compute_gamma=True,
            compute_theta=True,
            compute_vega=True,
            compute_rho=True
        ).apply(data)
        df = otp.run(data)
        expected = dict(
            VALUE=9.739720671039635,
            DELTA=-0.9429118423759162,
            GAMMA=0.020391626464263516,
            THETA=-5.139606667983257,
            VEGA=2.1453108389800515,
            RHO=-4.666995510197969,
        )
        if otp.compatibility.is_option_price_theta_value_changed():
            expected['THETA'] = 0.9410250231811439
        for key, val in expected.items():
            assert val == pytest.approx(df[key][0])

    def test_case3(self):
        data = {
            "PRICE": 150.,
            "OPTION_TYPE": "put",
            "STRIKE_PRICE": 140.,
            "DAYS_TILL_EXPIRATION": 10,
            "VOLATILITY": 0.6,
            "INTEREST_RATE": 0.07,
        }
        data = otp.Tick(**data)
        data = otp.agg.option_price(
            option_type_field_name='OPTION_TYPE',
            strike_price_field_name='STRIKE_PRICE',
            days_till_expiration_field_name='DAYS_TILL_EXPIRATION',
            volatility_field_name='VOLATILITY',
            interest_rate_field_name='INTEREST_RATE',
            compute_delta=True,
            compute_gamma=True,
            compute_theta=True,
            compute_vega=True,
            compute_rho=True).apply(data)
        df = otp.run(data)
        expected = dict(
            VALUE=2.0045973669685,
            DELTA=-0.2225317452764,
            GAMMA=0.0200066930955,
            THETA=-77.89770986056945,
            VEGA=7.3997358024512,
            RHO=-0.9694344974913,
        )
        if otp.compatibility.is_option_price_theta_value_changed():
            expected['THETA'] = -78.5502018957506
        for key, val in expected.items():
            assert val == pytest.approx(df[key][0])


class TestAllFieldsParameter:
    def test_agg_nsectime_all_fields_true(self):
        data = otp.Tick(A=1)
        data = data.agg({'X': otp.agg.max('TIMESTAMP')}, all_fields=True)
        df = otp.run(data)
        assert all(df['A'] == [1])
        assert all(df['X'] == [otp.config.default_start_time])

    def test_agg_nsectime_all_fields_false(self):
        data = otp.Tick(A=1)
        data = data.agg({'X': otp.agg.max('TIMESTAMP')}, all_fields=False)
        assert 'A' not in data.schema
        df = otp.run(data)
        assert all(df['X'] == [otp.config.default_start_time])

    def test_agg_int_all_fields_true(self):
        data = otp.Tick(A=1)
        data = data.agg({'X': otp.agg.max('A')}, all_fields=True)
        df = otp.run(data)
        assert all(df['A'] == [1])
        assert all(df['X'] == [1])

    def test_agg_int_all_fields_false(self):
        data = otp.Tick(A=1)
        data = data.agg({'X': otp.agg.max('A')}, all_fields=False)
        assert 'A' not in data.schema
        df = otp.run(data)
        assert all(df['X'] == [1])


class TestGroupBy:
    def test_group_by_field(self):
        data = otp.Ticks(X=['A', 'B', 'C'], Y=['C', 'Y', 'Y'])
        data = data.agg({'CNT': otp.agg.count()}, group_by=data['Y'])
        df = otp.run(data)
        assert all(df['Y'] == ['C', 'Y'])
        assert all(df['CNT'] == [1, 2])

    def test_group_by_two_fields(self):
        data = otp.Ticks(X=['A', 'B', 'C'], Y=['C', 'Y', 'Y'])
        data = data.agg({'CNT': otp.agg.count()}, group_by=[data['X'], data['Y']])
        df = otp.run(data)
        assert all(df['X'] == ['A', 'B', 'C'])
        assert all(df['Y'] == ['C', 'Y', 'Y'])
        assert all(df['CNT'] == [1, 1, 1])

    def test_group_by_some_logic(self):
        data = otp.Ticks(X=['A', 'B', 'C'], Y=['C', 'B', 'Y'])
        data = data.agg({'CNT': otp.agg.count()}, group_by=data['X'].str.replace('A', 'B'))
        df = otp.run(data)
        assert all(df['GROUP_0'] == ['B', 'C'])
        assert all(df['CNT'] == [2, 1])

    def test_group_by_some_logic_2(self):
        data = otp.Ticks(X=['A', 'B', 'C'], Y=['C', 'B', 'Y'])
        data = data.agg({'CNT': otp.agg.count()},
                        group_by=[data['X'].str.replace('A', 'B'), data['Y'].str.replace('A', 'B')])
        df = otp.run(data)
        assert all(df['GROUP_0'] == ['B', 'B', 'C'])
        assert all(df['GROUP_1'] == ['B', 'C', 'Y'])
        assert all(df['CNT'] == [1, 1, 1])

    def test_exception(self):
        data = otp.Ticks(X=['A', 'B', 'C'], GROUP_0=['C', 'B', 'Y'])
        with pytest.raises(AttributeError):
            data.agg({'CNT': otp.agg.count()}, group_by=data['X'].str.replace('A', 'B'))

    def test_groups_to_display(self):
        data = otp.Ticks(X=['A', 'B', 'A'], offset=[0, otp.Minute(20), otp.Minute(40)])
        with pytest.raises(ValueError, match="Parameter 'groups_to_display' can only be set to"):
            _ = data.agg({'CNT': otp.agg.count()}, group_by=data['X'], groups_to_display='WRONG')

        default = data.agg({'CNT': otp.agg.count()}, bucket_interval=otp.Minute(30), group_by=data['X'])
        df_default = otp.run(default,
                             start=otp.config.default_start_time,
                             end=otp.config.default_start_time + otp.Hour(1))
        assert list(df_default['Time']) == [otp.config.default_start_time + otp.Minute(30),
                                            otp.config.default_start_time + otp.Minute(30),
                                            otp.config.default_start_time + otp.Hour(1),
                                            otp.config.default_start_time + otp.Hour(1)]
        assert list(df_default['X']) == ['A', 'B', 'A', 'B']
        assert list(df_default['CNT']) == [1, 1, 1, 0]

        all_ = data.agg({'CNT': otp.agg.count()}, bucket_interval=otp.Minute(30),
                        group_by=data['X'], groups_to_display='all')
        df_all = otp.run(all_,
                         start=otp.config.default_start_time,
                         end=otp.config.default_start_time + otp.Hour(1))
        assert df_all.equals(df_default)

        event_in_last_bucket = data.agg({'CNT': otp.agg.count()},
                                        bucket_interval=otp.Minute(30),
                                        group_by=data['X'], groups_to_display='event_in_last_bucket')
        df_event_in_last_bucket = otp.run(event_in_last_bucket,
                                          start=otp.config.default_start_time,
                                          end=otp.config.default_start_time + otp.Hour(1))
        assert list(df_event_in_last_bucket['Time']) == [otp.config.default_start_time + otp.Minute(30),
                                                         otp.config.default_start_time + otp.Minute(30),
                                                         otp.config.default_start_time + otp.Hour(1)]
        assert list(df_event_in_last_bucket['X']) == ['A', 'B', 'A']
        assert list(df_event_in_last_bucket['CNT']) == [1, 1, 1]


class TestVariance:
    def test_not_biased(self):
        data = otp.Ticks({'A': [1, 2, 4]})
        agg = Variance(column='A', biased=False)
        data = agg.apply(data)
        df = otp.run(data)
        assert pytest.approx(df['VARIANCE'][0], 0.001) == 2.333

    def test_biased(self):
        data = otp.Ticks({'A': [1, 2, 4]})
        agg = Variance(column='A', biased=True)
        data = agg.apply(data)
        df = otp.run(data)
        assert pytest.approx(df['VARIANCE'][0], 0.001) == 1.556

    def test_float(self):
        data = otp.Ticks({'A': [1.3, 2.5, 4.2]})
        agg = Variance(column='A', biased=False)
        data = agg.apply(data)
        df = otp.run(data)
        assert pytest.approx(df['VARIANCE'][0], 0.001) == 2.123


class TestPercentile:
    def test_base(self):
        data = otp.Ticks({'A': [1, 2, 4], 'B': [1, 2, 4]})
        agg = Percentile(input_field_names=['A'], number_of_quantiles=3)
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['A']) == [2, 1]
        assert list(df['QUANTILE']) == [1, 2]
        assert 'B' not in df

    def test_base_source(self):
        data = otp.Ticks({'A': [1, 2, 4], 'B': [1, 2, 4]})
        data = data.percentile(input_field_names=['A'], number_of_quantiles=3)
        df = otp.run(data)
        assert list(df['A']) == [2, 1]
        assert list(df['QUANTILE']) == [1, 2]
        assert 'B' not in df

    def test_multiple_columns(self):
        data = otp.Ticks({'A': [1, 2, 4], 'B': [2, 3, 5]})
        agg = Percentile(input_field_names=['A', 'B'], number_of_quantiles=3)
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['A']) == [2, 1]
        assert list(df['B']) == [3, 2]
        assert list(df['QUANTILE']) == [1, 2]

    def test_output_fields_names(self):
        data = otp.Ticks({'A': [1, 2, 4], 'B': [2, 3, 5]})
        agg = Percentile(input_field_names=['A', 'B'], number_of_quantiles=3, output_field_names=['C', 'D'])
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['C']) == [2, 1]
        assert list(df['D']) == [3, 2]
        assert list(df['QUANTILE']) == [1, 2]

    def test_columns(self):
        data = otp.Ticks({'A': [1, 2, 4], 'B': [2, 3, 5]})
        agg = Percentile(input_field_names=['A', data['B']], number_of_quantiles=3)
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['A']) == [2, 1]
        assert list(df['B']) == [3, 2]
        assert list(df['QUANTILE']) == [1, 2]

    def test_comparison(self):
        data = otp.Ticks({'A': [1, 2, 4, 5], 'B': [2, 3, 5, 6]})
        agg = Percentile(input_field_names=[('A', 'asc'), (data['B'], 'desc')], number_of_quantiles=3)
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['A']) == [2, 4]
        assert list(df['B']) == [3, 5]
        assert list(df['QUANTILE']) == [1, 2]

    def test_partial_comparison(self):
        data = otp.Ticks({'A': [1, 2, 4, 5], 'B': [2, 3, 5, 6]})
        agg = Percentile(input_field_names=[('A', 'asc'), data['B']], number_of_quantiles=3)
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['A']) == [2, 4]
        assert list(df['B']) == [3, 5]
        assert list(df['QUANTILE']) == [1, 2]

    def test_errors(self):
        with pytest.raises(ValueError):
            _ = Percentile(input_field_names=None)

        with pytest.raises(ValueError):
            _ = Percentile(input_field_names=['A', 'B'], output_field_names=['A'], number_of_quantiles=3)

        with pytest.raises(ValueError):
            _ = Percentile(input_field_names='A', output_field_names=['A'])

        with pytest.raises(ValueError):
            _ = Percentile(input_field_names=['A'], output_field_names='A')


@pytest.mark.skipif(not otp.compatibility.is_find_value_for_percentile_supported(),
                    reason='not supported on older OneTick versions')
class TestFindValueForPercentile:

    @pytest.mark.parametrize('percentile,show_percentile_as,result', [
        (0, 'interpolated_value', 1.0),
        (50, 'interpolated_value', 2.5),
        (100, 'interpolated_value', 4.0),
        (0, 'first_value_with_ge_percentile', 1.0),
        (50, 'first_value_with_ge_percentile', 3.0),
        (100, 'first_value_with_ge_percentile', 4.0 if is_percentile_bug_fixed() else float('nan')),
    ])
    def test_all(self, percentile, show_percentile_as, result):
        t = otp.Ticks({'A': [1, 2, 3, 4]})
        t = t.find_value_for_percentile('A', percentile, show_percentile_as=show_percentile_as)
        assert t.schema == {'A': float}
        df = otp.run(t)
        if pd.isna(result):
            assert pd.isna(df['A'][0])
        else:
            assert df['A'][0] == result

    @pytest.mark.xfail(condition=not is_percentile_bug_fixed(),
                       reason='default show_percentile_as value is unstable on older builds')
    @pytest.mark.parametrize('percentile,result', [
        (0, 1.0),
        (50, 2.5),
        (100, 4.0 if is_percentile_bug_fixed() else float('nan')),
    ])
    def test_empty_show_percentile_as(self, percentile, result):
        t = otp.Ticks({'A': [1, 2, 3, 4]})
        t = t.find_value_for_percentile('A', percentile)
        assert t.schema == {'A': float}
        df = otp.run(t)
        if pd.isna(result):
            assert pd.isna(df['A'][0])
        else:
            assert df['A'][0] == result

    def test_errors(self):
        with pytest.raises(ValueError, match="Parameter 'percentile' must be a number between 0 and 100."):
            _ = FindValueForPercentile('A', -1)
        with pytest.raises(ValueError, match="Unsupported value for parameter 'show_percentile_as'"):
            _ = FindValueForPercentile('A', 50, 'WRONG')


class TestExpWAverage:
    def test_base(self):
        data = otp.Ticks({'A': [1, 2, 3, 3, 4]})
        agg = ExpWAverage('A', decay=2, bucket_interval=2, bucket_units='ticks')
        data = agg.apply(data)
        df = otp.run(data)
        exp = [1.880797, 2.984124, 3.880797]
        res = list(df['A'])
        assert all([pytest.approx(res[i]) == exp[i] for i in range(len(res))])

    def test_decay_value_type_hl(self):
        data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
        agg = ExpWAverage('A', decay=2, decay_value_type='half_life_index', bucket_interval=2, bucket_units='ticks')
        data = agg.apply(data)
        df = otp.run(data)
        exp = [1.585786, 2.773459, 3.585786]
        res = list(df['A'])
        assert all([pytest.approx(res[i]) == exp[i] for i in range(len(res))])

    def test_time_series_type_event_ts(self):
        data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
        agg = ExpWAverage('A', decay=2, bucket_interval=2, bucket_units='ticks', time_series_type='event_ts')
        data = agg.apply(data)
        df = otp.run(data)
        exp = [1.880797, 3.0, 4.0]
        res = list(df['A'])
        assert all([pytest.approx(res[i]) == exp[i] for i in range(len(res))])

    def test_exceptions(self):
        with pytest.raises(ValueError):
            _ = ExpWAverage('A', decay=2, decay_value_type='test')

        with pytest.raises(ValueError):
            _ = ExpWAverage('A', decay=2, time_series_type='test')

        with pytest.raises(TypeError):
            data = otp.Ticks({'A': ['a', 'b', 'c']})
            _ = ExpWAverage('A', decay=2).apply(data)


class TestExpTwAverage:
    def test_base(self):
        data = otp.Ticks({'A': [1, 2, 3, 3, 4]})
        agg = ExpTwAverage('A', decay=2, bucket_interval=2, bucket_units='ticks')
        data = agg.apply(data)
        df = otp.run(data)
        exp = [1.0, 2.500087, 4.0]
        res = list(df['A'])
        assert all([pytest.approx(res[i]) == exp[i] for i in range(len(res))])

    def test_decay_value_type_lambda(self):
        data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
        agg = ExpTwAverage('A', decay=2, decay_value_type='lambda', bucket_interval=2, bucket_units='ticks')
        data = agg.apply(data)
        df = otp.run(data)
        exp = [1.0, 2.5005, 4.0]
        res = list(df['A'])
        assert all([pytest.approx(res[i]) == exp[i] for i in range(len(res))])

    def test_exceptions(self):
        with pytest.raises(ValueError):
            _ = ExpTwAverage('A', decay=2, decay_value_type='test')

        with pytest.raises(TypeError):
            data = otp.Ticks({'A': ['a', 'b', 'c']})
            _ = ExpTwAverage('A', decay=2).apply(data)


@pytest.mark.skipif(
    not is_standardized_moment_supported(), reason='StandardizedMoment is not available on older builds',
)
class TestStandardizedMoment:
    def test_simple(self):
        data = otp.Ticks({'A': [1, 2, 4, 4, 4, 6]})
        agg = StandardizedMoment('A', bucket_interval=3, bucket_units='ticks')
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['A']) == [pytest.approx(0.381802), pytest.approx(0.707107)]

    def test_degree(self):
        data = otp.Ticks({'A': [1, 2, 4, 4, 4, 6]})
        agg = StandardizedMoment('A', degree=5, bucket_interval=3, bucket_units='ticks')
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['A']) == [pytest.approx(0.954504), pytest.approx(1.767767)]


class TestPortfolioPrice:
    @pytest.fixture(scope='class', autouse=True)
    def db_fixture(self, session):
        db_test = otp.DB(name='TEST_DB')
        db_test.add(
            otp.Ticks(PRICE=[12.5, 10, 6.5, 12, 14, 15], W=[1, 2, 2, 1, 1, 2]),
            symbol='S1', tick_type='TT',
            date=otp.dt(2003, 12, 1),
        )
        db_test.add(
            otp.Ticks(PRICE=[10, 8.5, 9.0, 11.5], W=[2, 2, 2, 1]),
            symbol='S2', tick_type='TT',
            date=otp.dt(2003, 12, 1),
        )
        session.use(db_test)

    def test_simple(self):
        data = otp.Ticks({'PRICE': [12.5, 10, 6.5, 12, 14, 15]})
        agg = PortfolioPrice(bucket_interval=1, bucket_units='ticks')
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['VALUE']) == [12.5, 10, 6.5, 12, 14, 15]
        assert list(df['NUM_SYMBOLS']) == [1] * 6

    def test_multi_symbols(self):
        data = otp.DataSource('TEST_DB', tick_type='TT', date=otp.datetime(2003, 12, 1))
        agg = PortfolioPrice(symbols=['S1', 'S2'])
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['VALUE']) == [26.5]
        assert list(df['NUM_SYMBOLS']) == [2]

    def test_multi_symbols_weights(self):
        data = otp.DataSource('TEST_DB', tick_type='TT', date=otp.datetime(2003, 12, 1))
        agg = PortfolioPrice(weight_field_name=data['W'], symbols=['S1', 'S2'])
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['VALUE']) == [41.5]
        assert list(df['NUM_SYMBOLS']) == [2]

    def test_multi_symbols_relative(self):
        data = otp.DataSource('TEST_DB', tick_type='TT', date=otp.datetime(2003, 12, 1))
        agg = PortfolioPrice(weight_field_name=data['W'], weight_type='relative', symbols=['S1', 'S2'])
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['VALUE']) == [pytest.approx(13.833333)]
        assert list(df['NUM_SYMBOLS']) == [2]

    def test_custom_input_column(self):
        data = otp.Ticks({'TEST': [12.5, 10, 6.5, 12, 14, 15]})
        agg = PortfolioPrice(data['TEST'], bucket_interval=1, bucket_units='ticks')
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['VALUE']) == [12.5, 10, 6.5, 12, 14, 15]

    def test_column_as_weight_field_name(self):
        data = otp.Ticks({'A': [1, 2, 1], 'PRICE': [12.5, 10, 6.5]})
        agg = PortfolioPrice(weight_field_name=data['A'], bucket_interval=1, bucket_units='ticks')
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['VALUE']) == [12.5, 20, 6.5]

    @pytest.mark.parametrize('weight_field_name,side,weight_type,interval,result', [
        ('A', None, None, 1, [75.0, 20.0, 13.0, 12.0, 70.0, 90.0]),
        ('A', None, None, 2, [20.0, 12.0, 90.0]),
        ('A', 'long', None, 2, [20.0, 12.0, 90.0]),
    ])
    def test_complex(self, weight_field_name, side, weight_type, interval, result):
        weights = [6, 2, 2, 1, 5, 6]

        data = otp.Ticks({'A': weights, 'PRICE': [12.5, 10, 6.5, 12, 14, 15]})
        kwargs = {}

        if weight_field_name is not None:
            kwargs['weight_field_name'] = weight_field_name

        if side is not None:
            kwargs['side'] = side

        if weight_type is not None:
            kwargs['weight_type'] = weight_type

        agg = PortfolioPrice(**kwargs, bucket_interval=interval, bucket_units='ticks')
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['VALUE']) == result

    def test_exceptions(self):
        with pytest.raises(ValueError):
            _ = PortfolioPrice(weight_type='test')

        with pytest.raises(ValueError):
            _ = PortfolioPrice(side='test')

        with pytest.raises(TypeError):
            data = otp.Ticks(B=[1, 2, 3], PRICE=[1, 1, 1])
            agg = PortfolioPrice(weight_field_name='A')
            _ = agg.apply(data)

    @pytest.mark.parametrize('weights,side,match', [
        ([1, 2, 3], 'long', 'SIDE must be set BOTH'),
        ([1, 2, 3], 'short', 'SIDE must be set BOTH'),
        ([-1, 2, 3], 'both', 'Detected negative value of weight'),
    ])
    def test_relative_exceptions(self, weights, side, match):
        data = otp.Ticks({'A': weights, 'PRICE': [1, 2, 3]})
        agg = PortfolioPrice(weight_field_name='A', side=side, weight_type='relative')
        data = agg.apply(data)

        with pytest.raises(Exception, match=match):
            _ = otp.run(data)


class TestMultiPortfolioPrice:
    @pytest.fixture(scope='class', autouse=True)
    def db_fixture(self, session):
        db_test = otp.DB(name='TEST_DB_MULTI')
        db_test.add(
            otp.Ticks(PRICE=[12.5, 10, 6.5, 12, 14, 15], W=[1, 2, 2, 1, 1, 2]),
            symbol='A', tick_type='TRD',
            date=otp.dt(2003, 12, 1),
        )
        db_test.add(
            otp.Ticks(PRICE=[10, 8.5, 9.0, 11.5], W=[2, 2, 2, 1]),
            symbol='AA', tick_type='TRD',
            date=otp.dt(2003, 12, 1),
        )
        db_test.add(
            otp.Ticks(PRICE=[20, 22, 19.5, 24, 21], W=[1, 2, 2, 2, 1]),
            symbol='AAA', tick_type='TRD',
            date=otp.dt(2003, 12, 1),
        )
        session.use(db_test)

    def test_base(self, cur_dir):
        data = otp.DataSource(db='TEST_DB_MULTI', tick_type='TRD', date=otp.datetime(2003, 12, 1))
        query_path = os.path.join(cur_dir, "..", "otqs", "multi_portfolio_price.otq::build_portfolios")
        agg = MultiPortfolioPrice(
            portfolios_query=query_path,
            symbols=['TEST_DB_MULTI::A', 'TEST_DB_MULTI::AA', 'TEST_DB_MULTI::AAA'],
        )
        data = agg.apply(data)
        df = otp.run(data)

        assert list(df['VALUE']) == [95.0, 47.5, 32.5, 21.0]
        assert list(df['NUM_SYMBOLS']) == [3, 3, 2, 1]
        assert list(df['PORTFOLIO_NAME']) == ['A2', 'A', 'AA', 'AAA']

    def test_source_as_portfolios_query(self):
        portfolios = otp.Ticks(
            SYMBOL_NAME=['TEST_DB_MULTI::A', 'TEST_DB_MULTI::AA', 'TEST_DB_MULTI::AAA', 'TEST_DB_MULTI::AA'],
            PORTFOLIO_NAME=['A1', 'A1', 'A1', 'A2'],
            WEIGHT=[1, 1, 1, 2],
        )
        data = otp.DataSource(db='TEST_DB_MULTI', tick_type='TRD', date=otp.datetime(2003, 12, 1))
        agg = MultiPortfolioPrice(
            portfolios_query=portfolios,
            symbols=['TEST_DB_MULTI::A', 'TEST_DB_MULTI::AA', 'TEST_DB_MULTI::AAA'],
        )
        data = agg.apply(data)
        df = otp.run(data)

        assert list(df['VALUE']) == [47.5, 23.0]
        assert list(df['NUM_SYMBOLS']) == [3, 1]
        assert list(df['PORTFOLIO_NAME']) == ['A1', 'A2']

    @pytest.mark.parametrize('params,value,num_symbols', [
        (('', 'W_1', 'both', 'absolute', '', '', None), {'VALUE': [142.5, 142.5, 97.5, 63.0]}, 1),
        (('', 'W_1', 'both', 'absolute', '', '', 5), {
            'VALUE': [115.5, 115.5, 85.5, 60.0, 121.5, 121.5, 85.5, 58.5, 142.5, 142.5, 97.5, 63.0],
        }, 3),
        (('PRICE,W', '', 'both', 'absolute', '', 'A,B', None), {
            'A': [95.0, 47.5, 32.5, 21.0], 'B': [8.0, 4.0, 2.0, 1.0],
        }, 1),
        (('', 'W_1', 'long', 'absolute', '', '', None), {'VALUE': [142.5, 142.5, 97.5, 63.0]}, 1),
        (('', 'W_1', 'both', 'relative', '', '', None), {
            'VALUE': [pytest.approx(15.83333), pytest.approx(15.83333), 16.25, 21.0],
        }, 1),
        (('', '', 'both', 'absolute', 'W_2', '', None), {'VALUE': [190.0, 95.0, 65.0, 42.0]}, 1),
    ])
    def test_complex(self, cur_dir, params, value, num_symbols):
        (column, weight_field_name, side, weight_type, weight_multiplier_field_name, portfolio_value_field_name,
         interval) = params

        kwargs = {}
        if interval:
            kwargs.update({'bucket_interval': interval, 'bucket_units': 'ticks'})

        data = otp.DataSource(db='TEST_DB_MULTI', tick_type='TRD', date=otp.datetime(2003, 12, 1))

        if weight_field_name:
            data[weight_field_name] = 3

        if weight_multiplier_field_name:
            data[weight_multiplier_field_name] = 2

        query_path = os.path.join(cur_dir, "..", "otqs", "multi_portfolio_price.otq::build_portfolios")
        agg = MultiPortfolioPrice(
            columns=column,
            portfolios_query=query_path,
            symbols=['TEST_DB_MULTI::A', 'TEST_DB_MULTI::AA', 'TEST_DB_MULTI::AAA'],
            weight_field_name=weight_field_name,
            side=side,
            weight_type=weight_type,
            weight_multiplier_field_name=weight_multiplier_field_name,
            portfolio_value_field_name=portfolio_value_field_name,
            **kwargs,
        )
        data = agg.apply(data)
        df = otp.run(data)

        for field, result in value.items():
            assert list(df[field]) == result

        assert list(df['NUM_SYMBOLS'] == [3, 3, 2, 1] * num_symbols)

    def test_multi_fields_by_column_and_lists(self, cur_dir):
        data = otp.DataSource(db='TEST_DB_MULTI', tick_type='TRD', date=otp.datetime(2003, 12, 1))
        query_path = os.path.join(cur_dir, "..", "otqs", "multi_portfolio_price.otq::build_portfolios")
        agg = MultiPortfolioPrice(
            columns=[data['PRICE'], 'W'],
            portfolios_query=query_path,
            symbols=['TEST_DB_MULTI::A', 'TEST_DB_MULTI::AA', 'TEST_DB_MULTI::AAA'],
            portfolio_value_field_name=['A', 'B'],
        )
        data = agg.apply(data)
        df = otp.run(data)

        assert list(df['A']) == [95.0, 47.5, 32.5, 21.0]
        assert list(df['B']) == [8.0, 4.0, 2.0, 1.0]

    def test_exceptions(self, cur_dir):
        query_path = os.path.join(cur_dir, "..", "otqs", "multi_portfolio_price.otq::build_portfolios")

        with pytest.raises(ValueError):
            _ = MultiPortfolioPrice(portfolios_query=query_path, weight_type='test')

        with pytest.raises(ValueError):
            _ = MultiPortfolioPrice(portfolios_query=query_path, side='test')

        with pytest.raises(TypeError):
            data = otp.DataSource(db='TEST_DB_MULTI', tick_type='TRD', date=otp.datetime(2003, 12, 1))
            agg = MultiPortfolioPrice(portfolios_query=query_path, weight_field_name='A')
            _ = agg.apply(data)

        with pytest.raises(TypeError):
            data = otp.DataSource(db='TEST_DB_MULTI', tick_type='TRD', date=otp.datetime(2003, 12, 1))
            agg = MultiPortfolioPrice(portfolios_query=query_path, weight_multiplier_field_name='A')
            _ = agg.apply(data)

        with pytest.raises(ValueError):
            _ = MultiPortfolioPrice(portfolios_query=None)

    def test_params(self, cur_dir):
        data = otp.DataSource(db='TEST_DB_MULTI', tick_type='TRD', date=otp.datetime(2003, 12, 1))
        query_path = os.path.join(cur_dir, "..", "otqs", "multi_portfolio_price.otq::build_portfolios_param")
        agg = MultiPortfolioPrice(
            portfolios_query=query_path,
            symbols=['TEST_DB_MULTI::A', 'TEST_DB_MULTI::AA', 'TEST_DB_MULTI::AAA'],
            portfolios_query_params={'PARAM1': 2, 'PARAM2': 3}
        )
        data = agg.apply(data)
        df = otp.run(data)

        assert list(df['VALUE']) == [95.0, 95.0, 97.5, 21.0]
        assert list(df['NUM_SYMBOLS']) == [3, 3, 2, 1]
        assert list(df['PORTFOLIO_NAME']) == ['A2', 'A', 'AA', 'AAA']

    @pytest.mark.parametrize('weights_multiplier,side,match', [
        (1, 'long', 'SIDE must be set BOTH'),
        (1, 'short', 'SIDE must be set BOTH'),
        (-1, 'both', 'Detected negative value of weight'),
    ])
    def test_relative_exceptions(self, cur_dir, weights_multiplier, side, match):
        data = otp.DataSource(db='TEST_DB_MULTI', tick_type='TRD', date=otp.datetime(2003, 12, 1))
        data['W'] = data['W'] * weights_multiplier

        query_path = os.path.join(cur_dir, "..", "otqs", "multi_portfolio_price.otq::build_portfolios")
        agg = MultiPortfolioPrice(
            portfolios_query=query_path, weight_field_name='W', side=side, weight_type='relative',
            symbols=['TEST_DB_MULTI::A', 'TEST_DB_MULTI::AA', 'TEST_DB_MULTI::AAA'],
        )
        data = agg.apply(data)

        with pytest.raises(Exception, match=match):
            _ = otp.run(data)


class TestReturnEP:
    def test_base(self):
        data = otp.Ticks(X=[2, 3, 4, 5, 6, 10])
        agg = Return(data['X'], bucket_interval=3, bucket_units='ticks')
        data = agg.apply(data)
        df = otp.run(data)
        assert list(df['X']) == [2, 2.5]

    def test_running(self):
        data = otp.Ticks(X=[2, 3, 4, 5, 6, 10], offset=[i * 1000 for i in range(6)])
        agg = Return(data['X'], bucket_interval=otp.Second(3), running=True)
        data = agg.apply(data)
        df = otp.run(data, start=otp.datetime(2023, 12, 1), end=otp.datetime(2023, 12, 1) + otp.Second(5))
        assert list(df['X']) == [1.0, 1.5, 2.0, 2.5, 2.0, 2.5]


class TestNewTypes:
    def test_sum(self):
        dtypes = (int, float, otp.decimal, otp.uint, otp.ulong, otp.short, otp.byte, otp.int, otp.long)
        data = {
            f'A_{i}': [cls(1), cls(2)]
            for i, cls in enumerate(dtypes)
        }
        t = otp.Ticks(data)
        t = t.agg({
            field: otp.agg.sum(field)
            for field in data
        })
        for field, dtype in zip(data, dtypes):
            assert t.schema[field] is dtype
        df = otp.run(t)
        for field in data:
            assert df[field][0] == 3

    def test_average(self):
        dtypes = (int, float, otp.decimal, otp.uint, otp.ulong, otp.short, otp.byte, otp.int, otp.long)
        data = {
            f'A_{i}': [cls(1), cls(2)]
            for i, cls in enumerate(dtypes)
        }
        t = otp.Ticks(data)
        t = t.agg({
            field: otp.agg.average(field)
            for field in data
        })
        for field in data:
            assert t.schema[field] is float
        df = otp.run(t)
        for field in data:
            assert df[field][0] == 1.5

    def test_stddev(self):
        dtypes = (int, float, otp.decimal, otp.uint, otp.ulong, otp.short, otp.byte, otp.int, otp.long)
        data = {
            f'A_{i}': [cls(1), cls(2)]
            for i, cls in enumerate(dtypes)
        }
        t = otp.Ticks(data)
        t = t.agg({
            field: otp.agg.stddev(field)
            for field in data
        })
        for field in data:
            assert t.schema[field] is float
        df = otp.run(t)
        for field in data:
            assert df[field][0] == 0.5

    def test_vwap(self):
        dtypes = (int, float, otp.decimal, otp.uint, otp.ulong, otp.short, otp.byte, otp.int, otp.long)
        data = {}
        for i, cls in enumerate(dtypes):
            data[f'A_{i}'] = [cls(1), cls(2)]
            data[f'B_{i}'] = [cls(1), cls(2)]
        t = otp.Ticks(data)
        t = t.agg({
            f'C_{i}': otp.agg.vwap(f'A_{i}', f'B_{i}')
            for i in range(len(dtypes))
        })
        for i in range(len(dtypes)):
            assert t.schema[f'C_{i}'] is float
        df = otp.run(t)
        for i in range(len(dtypes)):
            assert df[f'C_{i}'][0] == 5 / 3


class TestApply:
    def test_sum_int(self):
        t = otp.Ticks({'A': [0, 1, 2, 3, 4, 5]})
        from_agg = t.agg({'X': otp.agg.sum('A')}, running=True, all_fields=True)
        from_apply = otp.agg.sum('A', running=True, all_fields=True).apply(t, name='X')
        assert from_agg.schema['X'] is int
        assert from_apply.schema['X'] is int
        df_agg = otp.run(from_agg)
        df_apply = otp.run(from_apply)
        assert df_agg.equals(df_apply)
        assert df_agg['X'].dtype == np.int64
        assert df_apply['X'].dtype == np.int64


class TestLargeInts:

    def test_wrong(self):
        with pytest.raises(ValueError, match="Wrong value for First aggregation 'large_ints' parameter: WRONG"):
            otp.agg.first('A', large_ints='WRONG')
        with pytest.raises(Exception, match='value( which)? is too high to be accurately represented as double'):
            data = otp.Ticks(A=[9007199254740992, 9007199254740993, 9007199254740994])
            data = data.agg({'F': otp.agg.first('A')})
            otp.run(data)
        with pytest.raises(
            Exception,
            match=r'field A is not a .*field of one of integer types, while EXPECT_LARGE_INTS=true',
        ):
            data = otp.Ticks(A=[1.1, 1.2, 1.3])
            data = data.agg({'F': otp.agg.first('A', large_ints=True)})
            otp.run(data)

    @pytest.mark.parametrize('large_ints', (True, otp.adaptive))
    def test_int(self, large_ints):
        data = otp.Ticks(A=[9007199254740992, 9007199254740993, 9007199254740994])
        data = data.agg({'F': otp.agg.first('A', large_ints=large_ints)})
        df = otp.run(data)
        if 9007199254740992 <= otp.long.MAX:
            assert data.schema['F'] is int
        else:
            assert data.schema['F'] is otp.ulong
        assert list(df['F']) == [9007199254740992]

    @pytest.mark.parametrize('large_ints', (False, otp.adaptive))
    def test_float(self, large_ints):
        data = otp.Ticks(A=[1.1, 1.2, 1.3])
        data = data.agg({'F': otp.agg.first('A', large_ints=large_ints)})
        if large_ints is True:
            with pytest.raises(
                Exception,
                match='field A is not a 64bit field of one of integer types, while EXPECT_LARGE_INTS=true',
            ):
                otp.run(data)
        else:
            df = otp.run(data)
            assert df['F'][0] == 1.1

    def test_empty(self):
        data = otp.Empty(schema={'A': int, 'B': float})
        data = data.table(A=int, B=float)
        data = data.agg({'MAX_TRUE': otp.agg.max('A', large_ints=True),
                         'MIN_TRUE': otp.agg.min('A', large_ints=True),
                         'MAX_FALSE': otp.agg.max('A', large_ints=False),
                         'MIN_FALSE': otp.agg.min('A', large_ints=False),
                         'MAX_ADAPTIVE': otp.agg.max('A', large_ints=otp.adaptive),
                         'MIN_ADAPTIVE': otp.agg.min('A', large_ints=otp.adaptive),
                         'MAX_FLOAT_TRUE': otp.agg.max('B', large_ints=True),
                         'MIN_FLOAT_TRUE': otp.agg.min('B', large_ints=True),
                         'MAX_FLOAT_FALSE': otp.agg.max('B', large_ints=False),
                         'MIN_FLOAT_FALSE': otp.agg.min('B', large_ints=False),
                         'MAX_FLOAT_ADAPTIVE': otp.agg.max('B', large_ints=otp.adaptive),
                         'MIN_FLOAT_ADAPTIVE': otp.agg.min('B', large_ints=otp.adaptive)})
        df = otp.run(data)

        assert df['MAX_FALSE'][0] == 0
        assert df['MIN_FALSE'][0] == 0
        assert df['MAX_ADAPTIVE'][0] == 0
        if is_supported_large_ints_empty_interval():
            assert df['MIN_ADAPTIVE'][0] == 9223372036854775807
        else:
            assert df['MIN_ADAPTIVE'][0] == 0
        assert df['MAX_TRUE'][0] == 0
        if is_supported_large_ints_empty_interval():
            assert df['MIN_TRUE'][0] == 9223372036854775807
        else:
            assert df['MIN_TRUE'][0] == 0

        assert pd.isna(df['MAX_FLOAT_FALSE'][0])
        assert pd.isna(df['MIN_FLOAT_FALSE'][0])
        assert df['MAX_FLOAT_ADAPTIVE'][0] == 0.0
        if is_supported_large_ints_empty_interval():
            assert df['MIN_FLOAT_ADAPTIVE'][0] == pytest.approx(9.223372e+18)
        else:
            assert pd.isna(df['MIN_FLOAT_ADAPTIVE'][0])
        assert df['MAX_FLOAT_TRUE'][0] == 0.0
        if is_supported_large_ints_empty_interval():
            assert df['MIN_FLOAT_TRUE'][0] == pytest.approx(9.223372e+18)
        else:
            assert pd.isna(df['MIN_FLOAT_TRUE'][0])

    @pytest.mark.parametrize('large_ints', (True, otp.adaptive))
    def test_null_int_val(self, large_ints):
        data = otp.Ticks(A=[otp.long(1), otp.long(2), otp.long(3)], offset=[0, 2000, 4000])
        data = data.agg({'X': otp.agg.last('A', large_ints=large_ints, null_int_val=123)}, bucket_interval=1)
        df = otp.run(data, start=otp.dt(2003, 12, 1), end=otp.dt(2003, 12, 1, 0, 0, 5))
        assert list(df['X']) == [1, 123, 2, 123, 3]


class TestImpliedVol:
    @pytest.fixture(scope='class', autouse=True)
    def data(self, session):
        return otp.Ticks(
            PRICE=[100.7, 101.1, 99.5],
            PRICE_2=[100.6, 101.0, 99.4],
            OPTION_PRICE=[10.0] * 3,
            INTEREST_RATE=[0.05] * 3,
            OPTION_TYPE=['CALL'] * 3,
            OPTION_TYPE_2=['PUT'] * 3,
            STRIKE_PRICE=[100.0] * 3,
            DAYS_TILL_EXPIRATION=[30] * 3,
            SOME_FIELD=['a', 'b', 'c'],
            EXPIRATION=[20040131] * 3,
        )

    @pytest.mark.parametrize('method', [None, 'newton', 'bisections'])
    def test_simple(self, data, method):
        params = {}

        if method:
            params['method'] = method

        data = ImpliedVol(
            interest_rate=data['INTEREST_RATE'], option_type_field=data['OPTION_TYPE'],
            strike_price_field='STRIKE_PRICE', days_till_expiration_field='DAYS_TILL_EXPIRATION',
            **params,
        ).apply(data)
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'VALUE'}
        assert list(df['VALUE']) == [pytest.approx(0.884634)]

    def test_compute(self, data):
        data = data.agg({'X': otp.agg.implied_vol(
            interest_rate=data['INTEREST_RATE'], option_type_field=data['OPTION_TYPE'],
            strike_price_field='STRIKE_PRICE', days_till_expiration_field='DAYS_TILL_EXPIRATION',
        )})
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'X'}
        assert list(df['X']) == [pytest.approx(0.884634)]

    def test_source(self, data):
        data = data.implied_vol(
            price_field=data['PRICE_2'], interest_rate='INTEREST_RATE', option_type_field='OPTION_TYPE',
            strike_price_field=data['STRIKE_PRICE'], days_till_expiration_field=data['DAYS_TILL_EXPIRATION'],
        )
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'VALUE'}
        assert list(df['VALUE']) == [pytest.approx(0.8894909)]

    def test_value_for_non_converge(self, data):
        data = ImpliedVol(
            interest_rate=data['INTEREST_RATE'], option_type_field=data['OPTION_TYPE'],
            strike_price_field=data['STRIKE_PRICE'], expiration_date_field=data['EXPIRATION'],
            value_for_non_converge='closest_found_val',
        ).apply(data)
        df = otp.run(data)
        assert list(df['VALUE']) == [pytest.approx(0.6096502)]

    def test_interval(self, data):
        data = ImpliedVol(
            interest_rate=0.1, precision=0.0001, option_type_field='OPTION_TYPE_2',
            strike_price_field=data['STRIKE_PRICE'], expiration_date_field=data['EXPIRATION'],
            bucket_interval=1, bucket_units='ticks',
        ).apply(data)
        df = otp.run(data)
        assert list(df['VALUE']) == [pytest.approx(0.689574), pytest.approx(0.699786), pytest.approx(0.658000)]

    def test_symbol_params(self, data):
        sym = otp.Ticks({
            'SYMBOL_NAME': ['TEST'],
            'OPTION_TYPE': ['CALL'],
            'STRIKE_PRICE': [100.0],
            'INTEREST_RATE': [0.05],
        })

        data = ImpliedVol(days_till_expiration_field='DAYS_TILL_EXPIRATION').apply(data)
        df = otp.run(data, symbols=sym)
        assert list(df['TEST']['VALUE']) == [pytest.approx(0.884634)]

    def test_expiration_date_symbol_params(self, data):
        sym = otp.Ticks({
            'SYMBOL_NAME': ['TEST'],
            'EXPIRATION_DATE': [20031231],
        })

        data = ImpliedVol(
            interest_rate=data['INTEREST_RATE'], option_type_field=data['OPTION_TYPE'],
            strike_price_field=data['STRIKE_PRICE'],
        ).apply(data)
        df = otp.run(data, symbols=sym)
        assert list(df['TEST']['VALUE']) == [pytest.approx(0.887835)]

    def test_exceptions(self, data):
        with pytest.raises(ValueError, match='Unsupported value passed'):
            _ = ImpliedVol(interest_rate=[])

        with pytest.raises(ValueError, match='Unsupported value'):
            _ = ImpliedVol(value_for_non_converge='test')

        with pytest.raises(ValueError, match='Unsupported value'):
            _ = ImpliedVol(method='test')

        with pytest.raises(TypeError, match='column `MISSING_FIELD` from parameter `option_type_field`'):
            _ = ImpliedVol(
                interest_rate=0.1, option_type_field='MISSING_FIELD',
                strike_price_field=data['STRIKE_PRICE'], expiration_date_field=data['EXPIRATION'],
            ).apply(data)

        with pytest.raises(TypeError, match='column `SOME_FIELD` from parameter `interest_rate`'):
            _ = ImpliedVol(
                interest_rate='SOME_FIELD', option_type_field=data['OPTION_TYPE'],
                strike_price_field=data['STRIKE_PRICE'], expiration_date_field=data['EXPIRATION'],
            ).apply(data)


@pytest.mark.skipif(
    not getattr(otq, 'LinearRegression', False), reason="LinearRegression is not supported on cureent OneTick version",
)
class TestLinearRegression:
    def test_simple(self):
        data = otp.Ticks(X=[1, 2, 2, 1], Y=[5, 4, 3, 4])
        agg = LinearRegression(dependent_variable_field_name='Y', independent_variable_field_name=data['X'])
        data = agg.apply(data)
        assert data.schema == {'SLOPE': float, 'INTERCEPT': float}
        df = otp.run(data).to_dict(orient='list')
        assert set(df.keys()) == {'Time', 'SLOPE', 'INTERCEPT'}
        assert df['SLOPE'] == [-1.0]
        assert df['INTERCEPT'] == [5.5]


class TestPartitionEvenlyIntoGroups:
    def test_simple(self):
        data = otp.Ticks(
            X=['A', 'B', 'A', 'C', 'A', 'D'], SIZE=[10, 30, 15, 20, 15, 14], PRICE=[100, 110, 115, 95, 104, 100],
        )
        agg = PartitionEvenlyIntoGroups(field_to_partition=data['X'], weight_field='SIZE', number_of_groups=3)
        data = agg.apply(data)
        assert data.schema == {'FIELD_TO_PARTITION': str, 'GROUP_ID': int}
        df = otp.run(data).to_dict(orient='list')
        del df['Time']
        assert df == {
            'FIELD_TO_PARTITION': ['A', 'B', 'C', 'D'],
            'GROUP_ID': [0, 1, 2, 2],
        }

    def test_exceptions(self):
        with pytest.raises(ValueError, match='number_of_groups'):
            PartitionEvenlyIntoGroups(field_to_partition='TEST', weight_field='SIZE', number_of_groups=-10)
