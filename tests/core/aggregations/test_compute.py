import os
import re
import datetime
import math

import pytest
import numpy as np
import pandas as pd

import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.compatibility import (
    is_all_fields_when_ticks_exit_window_supported,
    is_first_ep_skip_tick_if_supported,
    is_last_ep_fwd_fill_if_supported,
    is_standardized_moment_supported,
    is_multi_column_generic_aggregations_supported,
)


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    return m_session


class TestCompute:

    def test_aggr_validation(self):
        c = otp.agg.compute()

        with pytest.raises(TypeError, match='It is allowed to pass only aggregations'):
            c.add('A', 'B')

        for field, value in [("running", True),
                             ("bucket_interval", 10),
                             ("bucket_time", 'start'),
                             ("bucket_units", 'days'),
                             ("end_condition_per_group", True),
                             ("boundary_tick_bucket", 'previous'),
                             ("group_by", ['C'])]:
            with pytest.raises(ValueError, match='parameter can not be specified'):
                c.add('A', otp.agg.sum('A', **{field: value}))

        c.add('B', otp.agg.sum('A'))
        with pytest.raises(KeyError, match='two aggregations to one field'):
            c.add('B', otp.agg.sum('C'))

    def test_aggr_validation_with_source(self):
        data = otp.Ticks({'A': [1]})
        c = otp.agg.compute()

        with pytest.raises(TypeError, match='Nothing to aggregate'):
            c.apply(data)

        c.add('A', otp.agg.sum('Z'))
        with pytest.raises(TypeError, match='Aggregation `SUM` uses column'):
            c.apply(data)

        data['Z'] = 'a'
        with pytest.raises(TypeError, match='Aggregation `SUM` require'):
            c.apply(data)

        c = otp.agg.compute(group_by=['ZZ'])
        c.add('A', otp.agg.sum('A'))
        with pytest.raises(KeyError, match='There is no'):
            c.apply(data)

        c = otp.agg.compute(all_fields=True, running=True)
        c.add('A', otp.agg.sum('A'))
        with pytest.raises(ValueError, match="already existing fields: 'A'"):
            c.apply(data)
        c.add('Z', otp.agg.sum('A'))
        with pytest.raises(ValueError, match="already existing fields: 'A, Z'"):
            c.apply(data)

    @pytest.mark.parametrize('output_name', ['A', 'ASD'])
    def test_simple(self, output_name):
        data = otp.Ticks({'A': [1, 2, 3]})
        c = otp.agg.compute()
        c.add(output_name, otp.agg.sum('A'))
        data = c.apply(data)
        assert data.schema == {output_name: int}
        df = otp.run(data)
        assert len(df) == 1
        assert set(df.columns) == {'Time', output_name}
        assert df[output_name][0] == 6

    def test_multiple_aggr(self):
        data = otp.Ticks({'P': [1, 2, 4], 'S': [2, 4, 8]})
        c = otp.agg.compute()
        c.add('V', otp.agg.vwap(price_column='P', size_column='S'))
        c.add('ASD', otp.agg.sum('S'))
        data = c.apply(data)
        assert data.schema == {'V': float, 'ASD': int}
        df = otp.run(data)
        assert len(df) == 1
        assert set(df.columns) == {'Time', 'V', 'ASD'}
        assert df['V'][0] == 3
        assert df['ASD'][0] == 14

    def test_group_by(self):
        data = otp.Ticks({'P': [1, 2, 4, 5], 'S': [2, 4, 8, 6], 'GB': [1, 1, 1, 2]})
        c = otp.agg.compute(group_by=['GB'])
        c.add('V', otp.agg.vwap(price_column='P', size_column='S'))
        c.add('ASD', otp.agg.sum('S'))
        data = c.apply(data)
        assert data.schema == {'V': float, 'ASD': int, 'GB': int}
        df = otp.run(data)
        assert len(df) == 2
        assert set(df.columns) == {'Time', 'V', 'ASD', 'GB'}
        assert df['V'][0] == 3
        assert df['V'][1] == 5
        assert df['ASD'][0] == 14
        assert df['ASD'][1] == 6

    def test_operation_gb(self):
        data = otp.Ticks({'P': [1, 2, 4, 5], 'S': [2, 4, 8, 6], 'GB': [1, 1, 1, 2], 'GB1': 'aabb', 'GB2': 'aabb'})
        c = otp.agg.compute(group_by=['GB', data['GB1'] + data['GB2']])
        c.add('V', otp.agg.vwap(price_column='P', size_column='S'))
        c.add('ASD', otp.agg.sum('S'))
        data = c.apply(data)
        assert data.schema == {'V': float, 'ASD': int, 'GB': int, 'GROUP_1': str}
        df = otp.run(data)
        assert len(df) == 3
        assert set(df.columns) == {'Time', 'V', 'ASD', 'GB', 'GROUP_1'}
        assert df['V'][0] == 5 / 3
        assert df['V'][1] == 4
        assert list(df['V']) == [5 / 3, 4, 5]
        assert list(df['ASD']) == [6, 8, 6]

    @pytest.mark.parametrize('all_fields', [True, False])
    def test_running(self, all_fields):
        data = otp.Ticks({'P': [1, 2, 4], 'S': [2, 4, 8], 'Z': 'asd'})
        c = otp.agg.compute(running=True, all_fields=all_fields)
        c.add('ASD1', otp.agg.sum('P'))
        c.add('ASD2', otp.agg.sum('S'))
        data = c.apply(data)
        expected_schema = {'ASD1': int, 'ASD2': int}
        expected_fields = {'Time', 'ASD1', 'ASD2'}
        if all_fields:
            expected_schema['Z'] = str
            expected_schema['P'] = int
            expected_schema['S'] = int
            expected_fields.update('Z', 'P', 'S')
        assert data.schema == expected_schema
        df = otp.run(data)
        assert len(df) == 3
        assert set(df.columns) == expected_fields
        assert list(df['ASD1']) == [1, 3, 7]
        assert list(df['ASD2']) == [2, 6, 14]

    @pytest.mark.parametrize('name', ['TS', 'TT'])
    def test_large_int(self, name):
        data = otp.Ticks({'A': [1, 2, 3]})
        data['TS'] = data['TIMESTAMP']
        c = otp.agg.compute()
        c.add(name, otp.agg.max(data['TS']))
        data = c.apply(data)
        assert data.schema == {name: otp.nsectime}
        df = otp.run(data)
        assert set(df.columns) == {'Time', name}
        assert df[name][0] == otp.config['default_start_time'] + otp.Milli(2)

    def test_large_int_gb_operation(self):
        data = otp.Ticks({'GB': [1, 1, 2, 2], 'GB1': 'aabc', 'GB2': 'aazx', 'Z': '1234'})
        data['TS'] = data['TIMESTAMP']
        agg = otp.agg.max(data['TS'])
        c = otp.agg.compute(group_by=[data['GB'], data['GB1'] + data['GB2']])
        c.add('TS', agg)
        data = c.apply(data)
        assert data.schema == {'TS': otp.nsectime, 'GB': int, 'GROUP_1': str}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'TS', 'GB', 'GROUP_1'}
        assert list(df['TS']) == [otp.config['default_start_time'] + otp.Milli(i) for i in [1, 2, 3]]

    def test_large_int_all_fields_error(self):
        data = otp.Ticks({'A': [1, 2, 3]})
        data['TS'] = data['TIMESTAMP']
        agg = otp.agg.max(data['TS'])
        c = otp.agg.compute(all_fields=True, running=True)
        c.add('TS', agg)
        with pytest.raises(ValueError, match="already existing fields: 'TS'"):
            c.apply(data)

    def test_large_int_all_fields(self):
        data = otp.Ticks({'A': [1, 2, 3]})
        data['TS'] = data['TIMESTAMP']
        agg = otp.agg.max(data['TS'])
        c = otp.agg.compute(all_fields=True, running=True)
        c.add('TT', agg)
        data = c.apply(data)

        assert data.schema == {'TT': otp.nsectime, 'TS': otp.nsectime, 'A': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'TS', 'TT', 'A'}
        assert list(df['TT']) == [otp.config['default_start_time'] + otp.Milli(i) for i in range(3)]

    def test_all_fields_only(self):
        data = otp.Ticks({'A': [1, 2, 3], 'B': [4, 5, 6]})
        c = otp.agg.compute(all_fields=True)
        c.add('X', otp.agg.sum('A'))
        data = c.apply(data)
        assert data.schema == {'A': int, 'B': int, 'X': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'A', 'B', 'X'}
        assert df['X'][0] == 6
        assert df['A'][0] == 1

    @pytest.mark.skipif(
        not is_all_fields_when_ticks_exit_window_supported(),
        reason="when_ticks_exit_window in `all_fields` not supported on this OneTick version",
    )
    def test_all_fields_when_ticks_exit_window(self):
        data = otp.Ticks({'A': [1, 2, 3], 'B': [4, 5, 6]})
        c = otp.agg.compute(all_fields='when_ticks_exit_window', running=True, bucket_interval=2, bucket_units='ticks')
        c.add('X', otp.agg.sum('A'))
        data = c.apply(data)
        assert data.schema == {'A': int, 'B': int, 'X': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'A', 'B', 'X'}
        assert list(df['X']) == [3.0, 5.0]

    @pytest.mark.skipif(
        not is_all_fields_when_ticks_exit_window_supported(),
        reason="when_ticks_exit_window in `all_fields` not supported on this OneTick version",
    )
    def test_all_fields_when_ticks_exit_window_errors(self):
        with pytest.raises(ValueError, match='running'):
            _ = otp.agg.compute(
                all_fields='when_ticks_exit_window', running=False, bucket_interval=2, bucket_units='ticks',
            )

        with pytest.raises(ValueError, match='bucket_interval'):
            _ = otp.agg.compute(
                all_fields='when_ticks_exit_window', running=True, bucket_interval=0,
            )

    @pytest.mark.parametrize('running', [True, False])
    def test_flexible_schema(self, running):
        """Test checks that if field not in schema it won't be dropped"""
        t = otp.Ticks({'A': [1, 2, 3], 'B': [4, 5, 6]})
        t.sink(otq.AddField(field='NOT_IN_SCHEMA', value=1))
        schema_before_agg = t.schema.copy()
        c = otp.agg.compute(all_fields=True, running=running)
        c.add('C', otp.agg.sum('A'))
        t = c.apply(t)
        schema_before_agg['C'] = int
        assert t.schema == schema_before_agg
        assert 'NOT_IN_SCHEMA' not in schema_before_agg
        df = otp.run(t)
        assert 'NOT_IN_SCHEMA' in df


class TestMultipleAggregation:

    def test_min_time_series_type(self):
        data = otp.Ticks({"x": [2, 4, 4, 6, 6, 8, 8, 10, 12, 14],
                          "offset": [0, 2, 3, 4, 5, 6, 7, 12, 22, 32]})

        data = data.agg({
            "x_min": otp.agg.min(data.x),
            "x_min_event_ts": otp.agg.min(data.x, time_series_type="event_ts"),
            "x_min_state_ts": otp.agg.min(data.x, time_series_type="state_ts"),
        }, running=True, all_fields=True, bucket_interval=0.003)
        df = otp.run(data)

        assert list(df.x_min) == [2, 2, 4, 4, 4, 6, 6, 10, 12, 14]
        assert list(df.x_min) == list(df.x_min_event_ts)
        assert list(df.x_min_state_ts) == [2, 2, 2, 2, 4, 4, 6, 8, 10, 12]

    def test_tw_average_time_series_type(self):
        data = otp.Ticks({'X': [64, 63, 96, 72, 11, 95, 65, 20, 33, 4],
                          "offset": [100, 200, 300, 400, 500, 600, 700, 1200, 2200, 3200]})

        data = data.agg({
            'X_DEFAULT': otp.agg.tw_average(data['X']),
            'X_STATE_TS': otp.agg.tw_average(data['X'], time_series_type='state_ts'),
            'X_EVENT_TS': otp.agg.tw_average(data['X'], time_series_type='event_ts'),
        }, bucket_interval=1)
        df = otp.run(data, end=otp.config.default_start_time + otp.Second(4))
        assert list(df['X_DEFAULT']) == list(df['X_STATE_TS'])
        assert list(df['X_DEFAULT']) != list(df['X_EVENT_TS'])
        assert list(df['X_STATE_TS']) == [pytest.approx(66.222222), 29.0, 30.4, 9.8]
        assert list(df['X_EVENT_TS']) == [pytest.approx(66.222222), 20.0, 33.0, 4.0]

    def test_aggregation_1(self):
        # check sum
        data = otp.Ticks({"x": [3, 9, 13]})

        data = data.agg({"x_sum": otp.agg.sum(data.x)})
        assert hasattr(data, "x_sum") and data.x_sum.dtype is int
        assert not hasattr(data, "x")

        df = otp.run(data)
        assert hasattr(df, "x_sum") and isinstance(df.x_sum[0], np.integer)
        assert not hasattr(df, "x")

        assert len(df) == 1

        assert df.x_sum[0] == (3 + 9 + 13)

    def test_aggregation_2(self):
        # check first
        data = otp.Ticks({"x": [3, 9, 13]})

        data = data.agg({"x_first": otp.agg.first(data.x)})

        assert hasattr(data, "x_first") and data.x_first.dtype is int
        assert not hasattr(data, "x")

        df = otp.run(data)
        assert hasattr(df, "x_first") and isinstance(df.x_first[0], np.integer)
        assert not hasattr(df, "x")

        assert len(df) == 1

        assert df.x_first[0] == 3

    def test_aggregation_3(self):
        # check last
        data = otp.Ticks({"x": [3, 9, 13]})

        data = data.agg({"x_last": otp.agg.last(data.x)})

        assert hasattr(data, "x_last") and data.x_last.dtype is int
        assert not hasattr(data, "x")

        df = otp.run(data)
        assert hasattr(df, "x_last") and isinstance(df.x_last[0], np.integer)
        assert not hasattr(df, "x")

        assert len(df) == 1

        assert df.x_last[0] == 13

    def test_aggregation_4(self):
        # check max
        data = otp.Ticks({"x": [3, 9, 15]})

        data = data.agg({"x_max": otp.agg.max(data.x)})

        assert hasattr(data, "x_max") and data.x_max.dtype is int
        assert not hasattr(data, "x")

        df = otp.run(data)
        assert hasattr(df, "x_max") and isinstance(df.x_max[0], np.integer)
        assert not hasattr(df, "x")

        assert len(df) == 1

        assert df.x_max[0] == 15

    def test_aggregation_5(self):
        # check min
        data = otp.Ticks({"x": [9, 2, 13]})
        data = data.agg({"x_min": otp.agg.min(data.x)})

        assert hasattr(data, "x_min") and data.x_min.dtype is int
        assert not hasattr(data, "x")

        df = otp.run(data)
        assert hasattr(df, "x_min") and isinstance(df.x_min[0], np.integer)
        assert not hasattr(df, "x")

        assert len(df) == 1

        assert df.x_min[0] == 2

    def test_aggregation_6(self):
        # test several sums
        data = otp.Ticks({"x": [15, 2, 9], "y": [-5, 3, 99]})

        data = data.agg({"x_sum": otp.agg.sum(data.x), "y_sum": otp.agg.sum(data.y)})

        assert hasattr(data, "x_sum") and data.x_sum.dtype is int
        assert hasattr(data, "y_sum") and data.y_sum.dtype is int
        assert not hasattr(data, "x")
        assert not hasattr(data, "y")

        df = otp.run(data)
        assert hasattr(df, "x_sum") and isinstance(df.x_sum[0], np.integer)
        assert hasattr(df, "y_sum") and isinstance(df.y_sum[0], np.integer)
        assert not hasattr(df, "x")
        assert not hasattr(df, "y")

        assert len(df) == 1

        assert df.x_sum[0] == (15 + 2 + 9)
        assert df.y_sum[0] == (-5 + 3 + 99)

    def test_aggregation_7(self):
        # test heterogenous aggregations
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5, 99]})

        data = data.agg(
            {
                "x_sum": otp.agg.sum(data.x),
                "x_min": otp.agg.min(data.x),
                "x_max": otp.agg.max(data.x),
                "x_first": otp.agg.first(data.x),
                "x_last": otp.agg.last(data.x),
                "y_sum": otp.agg.sum(data.y),
                "y_min": otp.agg.min(data.y),
                "y_max": otp.agg.max(data.y),
                "y_first": otp.agg.first(data.y),
                "y_last": otp.agg.last(data.y),
            }
        )

        assert hasattr(data, "x_sum") and data.x_sum.dtype is int
        assert hasattr(data, "x_min") and data.x_min.dtype is int
        assert hasattr(data, "x_max") and data.x_max.dtype is int
        assert hasattr(data, "x_first") and data.x_first.dtype is int
        assert hasattr(data, "x_last") and data.x_last.dtype is int
        assert hasattr(data, "y_sum") and data.y_sum.dtype is int
        assert hasattr(data, "y_min") and data.y_min.dtype is int
        assert hasattr(data, "y_max") and data.y_max.dtype is int
        assert hasattr(data, "y_first") and data.y_first.dtype is int
        assert hasattr(data, "y_last") and data.y_last.dtype is int
        assert hasattr(data, "_START_TIME") and data._START_TIME.dtype is otp.nsectime
        assert hasattr(data, "_END_TIME") and data._END_TIME.dtype is otp.nsectime

        df = otp.run(data)
        assert len(df) == 1

        assert df.x_sum[0] == (15 + 2 + 9)
        assert df.x_min[0] == 2
        assert df.x_max[0] == 15
        assert df.x_first[0] == 15
        assert df.x_last[0] == 9
        assert df.y_sum[0] == (-5 + 3 + 99)
        assert df.y_min[0] == -5
        assert df.y_max[0] == 99
        assert df.y_first[0] == 3
        assert df.y_last[0] == 99

    def test_aggregation_8(self):
        """ string aggregation """

        data = otp.Ticks({"x": ["abc", "bcd", "efg"]})

        assert data.x.dtype is str

        first = data.agg({"first_x": otp.agg.first(data.x)})
        last = data.agg({"last_x": otp.agg.last(data.x)})

        assert list(otp.run(first).first_x) == ["abc"]
        assert list(otp.run(last).last_x) == ["efg"]

        # long string
        data.z = "x" * 999

        assert data.z.dtype is otp.string[999]

        first = data.agg({"first_z": otp.agg.first(data.z)})
        last = data.agg({"last_z": otp.agg.last(data.z)})

        assert first.first_z.dtype is otp.string[999]
        assert last.last_z.dtype is otp.string[999]

        assert otp.run(first).first_z[0] == "x" * 999
        assert otp.run(last).last_z[0] == "x" * 999

    def test_aggregation_9(self):
        """ string aggregation """

        data = otp.Ticks({"x": ["abc", "bcd", "efg"]})

        # it is not allowed to sum strings
        with pytest.raises(TypeError):
            data.agg({"sum": otp.agg.sum(data.x)})

        with pytest.raises(TypeError):
            data.agg({"max": otp.agg.max(data.x)})

        with pytest.raises(TypeError):
            data.agg({"min": otp.agg.min(data.x)})

    def test_aggregation_10(self):
        # aggregate timestamps -> float type
        data = otp.Ticks({"x": [1, 3, 5]})

        data.TS = data.Time

        assert data.TS.dtype is otp.nsectime
        assert isinstance(otp.run(data).TS[0], pd.Timestamp)

        data = data.agg({"ts_min": otp.agg.min(data.TS)})

        assert data.ts_min.dtype is otp.nsectime
        assert isinstance(otp.run(data).ts_min[0], pd.Timestamp)

    def test_aggregation_11(self):
        # bucket time = end
        data = otp.Ticks({"x": [1, 3, 5]})

        data = data.agg({"x_sum": otp.agg.sum(data.x)}, bucket_time="end")
        data.ET = data._END_TIME

        df = otp.run(data)
        assert df.Time[0] == df.ET[0]

    def test_aggregation_12(self):
        # bucket time = start
        data = otp.Ticks({"x": [1, 3, 5]})

        data = data.agg({"x_sum": otp.agg.sum(data.x)}, bucket_time="start")
        data.ST = data._START_TIME

        df = otp.run(data)
        assert df.Time[0] == df.ST[0]

    def test_aggregation_13(self):
        # bucket time = default
        data = otp.Ticks({"x": [1, 3, 5]})

        data = data.agg({"x_sum": otp.agg.sum(data.x)})
        data.ET = data._END_TIME

        df = otp.run(data)
        assert df.Time[0] == df.ET[0]

    def test_aggregation_14(self):
        data = otp.Ticks({"x": [1, 3, 5]})

        data = data.agg({"x_sum": otp.agg.sum(data.x)}, running=True)
        assert hasattr(data, "x_sum") and data.x_sum.dtype is int
        assert not hasattr(data, "x")

        df = otp.run(data)
        assert len(df) == 3
        assert not hasattr(data, "x")
        assert isinstance(df.x_sum[0], np.integer)

        assert df.x_sum[0] == 1
        assert df.x_sum[1] == 1 + 3
        assert df.x_sum[2] == 1 + 3 + 5

    def test_aggregation_15(self):
        data = otp.Ticks({"x": [1, 3, 5]})

        data = data.agg({"x_sum": otp.agg.sum(data.x)}, running=False)

        assert hasattr(data, "x_sum") and data.x_sum.dtype is int
        assert not hasattr(data, "x")

        df = otp.run(data)
        assert len(df) == 1
        assert not hasattr(data, "x")

        assert df.x_sum[0] == 1 + 3 + 5

    def test_aggregation_16(self):
        data = otp.Ticks({"x": [1, 3, 5]})

        data = data.agg({"x_sum": otp.agg.sum(data.x)})

        assert hasattr(data, "x_sum") and data.x_sum.dtype is int
        assert not hasattr(data, "x")

        df = otp.run(data)
        assert len(df) == 1
        assert not hasattr(data, "x")

        assert df.x_sum[0] == 1 + 3 + 5

    def test_aggregation_17(self):
        # test heterogenous and running flag
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5, 99]})

        data = data.agg(
            {
                "x_sum": otp.agg.sum(data.x),
                "x_min": otp.agg.min(data.x),
                "x_max": otp.agg.max(data.x),
                "x_first": otp.agg.first(data.x),
                "x_last": otp.agg.last(data.x),
                "y_sum": otp.agg.sum(data.y),
                "y_min": otp.agg.min(data.y),
                "y_max": otp.agg.max(data.y),
                "y_first": otp.agg.first(data.y),
                "y_last": otp.agg.last(data.y),
            },
            running=True,
        )

        assert hasattr(data, "x_sum") and data.x_sum.dtype is int
        assert hasattr(data, "x_min") and data.x_min.dtype is int
        assert hasattr(data, "x_max") and data.x_max.dtype is int
        assert hasattr(data, "x_first") and data.x_first.dtype is int
        assert hasattr(data, "x_last") and data.x_last.dtype is int
        assert hasattr(data, "y_sum") and data.y_sum.dtype is int
        assert hasattr(data, "y_min") and data.y_min.dtype is int
        assert hasattr(data, "y_max") and data.y_max.dtype is int
        assert hasattr(data, "y_first") and data.y_first.dtype is int
        assert hasattr(data, "y_last") and data.y_last.dtype is int

        df = otp.run(data)
        assert len(df) == 3

        assert df.x_max[0] == 15 and df.x_max[1] == 15 and df.x_max[2] == 15
        assert df.x_first[0] == 15 and df.x_first[1] == 15 and df.x_first[2] == 15
        assert df.x_last[0] == 15 and df.x_last[1] == 2 and df.x_last[2] == 9

        assert df.y_sum[0] == 3 and df.y_sum[1] == -2 and df.y_sum[2] == 97
        assert df.y_min[0] == 3 and df.y_min[1] == -5 and df.y_min[2] == -5
        assert df.y_max[0] == 3 and df.y_max[1] == 3 and df.y_max[2] == 99
        assert df.y_first[0] == 3 and df.y_first[1] == 3 and df.y_first[2] == 3
        assert df.y_last[0] == 3 and df.y_last[1] == -5 and df.y_last[2] == 99

    def test_aggregation_18(self):
        # test heterogenous and running flag
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5, 99]})

        data = data.agg({"x_sum": otp.agg.sum(data.x)}, running=True, all_fields=True)

        assert hasattr(data, "x_sum")
        assert hasattr(data, "x")
        assert hasattr(data, "y")

        df = otp.run(data)
        assert hasattr(df, "x_sum")
        assert hasattr(df, "x")
        assert hasattr(df, "y")

    def test_aggregation_19(self):
        # test heterogenous and running flag
        data = otp.Ticks([["x", "y"], [15, 3], [2, -5], [9, 99]])

        data = data.agg({"x_sum": otp.agg.sum(data.x)}, running=True, all_fields=False)
        assert hasattr(data, "x_sum")
        assert not hasattr(data, "x")
        assert not hasattr(data, "y")

        df = otp.run(data)
        assert hasattr(df, "x_sum")
        assert not hasattr(df, "x")
        assert not hasattr(df, "y")

    def test_aggregation_20(self):
        # test heterogenous and running flag
        data = otp.Ticks([["x", "y"], [15, 3], [2, -5], [9, 99]])

        # it is not allowed to have an output name as the same as input when all_fields=True
        with pytest.raises(ValueError):
            data.agg({"x": otp.agg.sum(data.x)}, running=True, all_fields=True)

    def test_aggregation_23(self):
        # Default bucketing unit is seconds
        data = otp.Ticks({"x": [1, 2, 3, 4, 5], "offset": [0, 500, 1000, 1500, 2000]})
        data = data.agg({"s": otp.agg.sum(data.x)}, bucket_interval=1)
        df = otp.run(data)
        assert df.s[0] == 3
        assert df.s[1] == 7
        assert df.s[2] == 5
        assert df.s[3] == 0

    def test_aggregation_24(self):
        # bucket_units=ticks. If any other unit is used instead, result will have only one bucket.
        data = otp.Ticks({"x": [1, 2, 3, 4, 5], "offset": [0, 100, 200, 300, 400]})
        data = data.agg({"s": otp.agg.sum(data.x)}, bucket_units="ticks", bucket_interval=2)
        df = otp.run(data)
        assert df.s[0] == 3
        assert df.s[1] == 7
        assert df.s[2] == 5

    def test_aggregation_25(self):
        # bucket_units=days. If seconds or ticks are used, result ticks will have (many) 0s between them.
        # If months, there will be only one nonzero bucket.
        day = 24 * 60 * 60 * 1000
        start_time = datetime.datetime(2003, 12, 1)
        end_time = start_time + datetime.timedelta(days=7)

        data = otp.Ticks({"x": [1, 2, 3, 4, 5], "offset": [day * i for i in range(5)]})
        data = data.agg({"s": otp.agg.sum(data.x)}, bucket_units="days", bucket_interval=1)
        data = otp.run(data, start=start_time, end=end_time)

        assert len(data) == 7

        assert data.s[0] == 1
        assert data.s[1] == 2
        assert data.s[2] == 3
        assert data.s[3] == 4
        assert data.s[4] == 5
        assert data.s[5] == 0
        assert data.s[6] == 0

    def test_aggregation_26(self):
        # bucket_units=months. If anything else is used, there will be more than two buckets.
        day = 24 * 60 * 60 * 1000

        start_time = datetime.datetime(2003, 12, 1)
        end_time = start_time + datetime.timedelta(days=60)

        data = otp.Ticks({"x": [1, 2, 3, 4, 5], "offset": [day * i for i in range(5)]})
        data = data.agg({"s": otp.agg.sum(data.x)}, bucket_units="months", bucket_interval=1)
        data = otp.run(data, start=start_time, end=end_time)

        assert len(data) == 2

        assert data.s[0] == 15
        assert data.s[1] == 0

    def test_aggregation_27(self):
        data = otp.Ticks({"x": [1, 2, 3, 4, 5]})
        with pytest.raises(TypeError, match="Aggregation `SUM` uses column `y` as input, which doesn't exist"):
            data.agg({"s": otp.agg.sum("y")})

    def test_aggregation_28(self):
        # test bucket_units = 'flexible'
        data = otp.Ticks({"x": [1, 1, 1, 2, 2, 3, 3, 3, 3, 4],
                          "offset": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]})
        data = data.agg({"s": otp.agg.sum("x"),
                         "n": otp.agg.count()},
                        bucket_units='flexible',
                        boundary_tick_bucket='new',
                        bucket_end_condition=(data['x'] != data['x'][-1]) & (data['x'][-1] != 0))
        res = otp.run(data)
        assert len(res) == 4
        assert res['Time'][0] == otp.config['default_start_time'] + otp.Milli(40)
        assert res['s'][0] == 3
        assert res['n'][0] == 3
        assert res['Time'][1] == otp.config['default_start_time'] + otp.Milli(60)
        assert res['s'][1] == 4
        assert res['n'][1] == 2
        assert res['Time'][2] == otp.config['default_start_time'] + otp.Milli(100)
        assert res['s'][2] == 12
        assert res['n'][2] == 4
        assert res['Time'][3] == otp.config['default_end_time']
        assert res['s'][3] == 4
        assert res['n'][3] == 1

    def test_aggregation_29(self):
        # test bucket_units = 'flexible'
        data = otp.Ticks({"x": [1, 1, 1, 2, 2, 3, 3, 3, 3, 4],
                          "offset": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]})
        data = data.agg({"s": otp.agg.sum("x"),
                         "n": otp.agg.count()},
                        bucket_units='flexible',
                        boundary_tick_bucket='previous',
                        bucket_time='start',
                        bucket_end_condition=(data['x'] != data['x'][-1]) & (data['x'][-1] != 0))
        res = otp.run(data)
        assert len(res) == 4
        assert res['Time'][0] == otp.config['default_start_time']
        assert res['s'][0] == 5
        assert res['n'][0] == 4
        assert res['Time'][1] == otp.config['default_start_time'] + otp.Milli(40)
        assert res['s'][1] == 5
        assert res['n'][1] == 2
        assert res['Time'][2] == otp.config['default_start_time'] + otp.Milli(60)
        assert res['s'][2] == 13
        assert res['n'][2] == 4
        assert res['Time'][3] == otp.config['default_start_time'] + otp.Milli(100)
        assert res['s'][3] == 0
        assert res['n'][3] == 0

    def test_aggregation_30(self):
        # test end_condition_per_group='False'
        data = otp.Ticks({"x": [1, 1, 1, 2, 2, 3, 3, 3, 3, 4],
                          "y": [1, 2, 2, 2, 1, 1, 1, 2, 2, 1],
                          "offset": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]})
        data = data.agg({"s": otp.agg.sum("x"),
                         "n": otp.agg.count()},
                        bucket_units='flexible',
                        end_condition_per_group='True',
                        bucket_end_condition=(data['x'] != data['x'][-1]) & (data['x'][-1] != 0),
                        group_by=['y'])
        res = otp.run(data)
        assert len(res) == 7
        assert res['Time'][0] == otp.config['default_start_time'] + otp.Milli(40)
        assert res['y'][0] == 2
        assert res['Time'][1] == otp.config['default_start_time'] + otp.Milli(50)
        assert res['y'][1] == 1
        assert res['Time'][2] == otp.config['default_start_time'] + otp.Milli(60)
        assert res['y'][2] == 1
        assert res['Time'][3] == otp.config['default_start_time'] + otp.Milli(80)
        assert res['y'][3] == 2
        assert res['Time'][4] == otp.config['default_start_time'] + otp.Milli(100)
        assert res['y'][4] == 1
        assert res['Time'][5] == otp.config['default_end_time']
        assert res['y'][5] == 1
        assert res['Time'][6] == otp.config['default_end_time']
        assert res['y'][6] == 2

    def test_aggregation_31(self):
        # test end_condition_per_group='False'
        data = otp.Ticks({"x": [1, 1, 1, 2, 2, 3, 3, 3, 3, 4],
                          "y": [1, 2, 2, 2, 1, 1, 1, 2, 2, 1],
                          "offset": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]})
        data = data.agg({"s": otp.agg.sum("x"),
                         "n": otp.agg.count()},
                        bucket_units='flexible',
                        end_condition_per_group='False',
                        bucket_end_condition=(data['x'] != data['x'][-1]) & (data['x'][-1] != 0),
                        group_by=['y'])
        res = otp.run(data)
        assert len(res) == 8
        assert res['Time'][0] == otp.config['default_start_time'] + otp.Milli(40)
        assert res['y'][0] == 1
        assert res['n'][0] == 1
        assert res['s'][0] == 1
        assert res['Time'][1] == otp.config['default_start_time'] + otp.Milli(40)
        assert res['y'][1] == 2
        assert res['n'][1] == 2
        assert res['s'][1] == 2
        assert res['Time'][2] == otp.config['default_start_time'] + otp.Milli(60)
        assert res['y'][2] == 1
        assert res['n'][2] == 1
        assert res['s'][2] == 2
        assert res['Time'][3] == otp.config['default_start_time'] + otp.Milli(60)
        assert res['y'][3] == 2
        assert res['n'][3] == 1
        assert res['s'][3] == 2
        assert res['Time'][4] == otp.config['default_start_time'] + otp.Milli(100)
        assert res['y'][4] == 1
        assert res['n'][4] == 2
        assert res['s'][4] == 6
        assert res['Time'][5] == otp.config['default_start_time'] + otp.Milli(100)
        assert res['y'][5] == 2
        assert res['n'][5] == 2
        assert res['s'][5] == 6
        assert res['Time'][6] == otp.config['default_end_time']
        assert res['y'][6] == 1
        assert res['n'][6] == 1
        assert res['s'][6] == 4
        assert res['Time'][7] == otp.config['default_end_time']
        assert res['y'][7] == 2
        assert res['n'][7] == 0
        assert res['s'][7] == 0

    def test_count_1(self):
        # basic check
        data = otp.Ticks([["x", "y"], [15, 3], [2, -5], [9, 99]])

        data = data.agg({"num_ticks": otp.agg.count()})

        assert not hasattr(data, "x")
        assert not hasattr(data, "y")
        assert hasattr(data, "num_ticks") and data.num_ticks.dtype is int

        df = otp.run(data)
        assert len(df) == 1

        assert isinstance(df.num_ticks[0], np.integer) and df.num_ticks[0] == 3

    def test_count_2(self):
        # check running
        data = otp.Ticks([["x", "y", "offset"], [15, 3, 0], [2, -5, 5], [9, 99, 9]])

        data = data.agg({"num_ticks": otp.agg.count()}, running=True)
        data.ST = data._START_TIME

        assert not hasattr(data, "x")
        assert not hasattr(data, "y")
        assert hasattr(data, "num_ticks") and data.num_ticks.dtype is int

        df = otp.run(data)
        assert len(df) == 3

        assert df.num_ticks[0] == 1 and df.Time[0] == df.ST[0]
        assert df.num_ticks[1] == 2 and df.Time[1] == df.ST[1] + otp.Milli(5)
        assert df.num_ticks[2] == 3 and df.Time[2] == df.ST[2] + otp.Milli(9)

    def test_count_3(self):
        # check bucket time; default is 'end'
        data = otp.Ticks([["x", "y", "offset"], [15, 3, 0], [2, -5, 5], [9, 99, 9]])

        data = data.agg({"num_ticks": otp.agg.count()})
        data.ET = data._END_TIME

        df = otp.run(data)
        assert len(df) == 1
        assert df.num_ticks[0] == 3 and df.Time[0] == df.ET[0]

    def test_count_4(self):
        # check bucket time set to 'start'
        data = otp.Ticks([["x", "y", "offset"], [15, 3, 0], [2, -5, 5], [9, 99, 9]])

        data = data.agg({"num_ticks": otp.agg.count()}, bucket_time="start")
        data.ST = data._START_TIME

        df = otp.run(data)
        assert len(df) == 1
        assert df.num_ticks[0] == 3 and df.Time[0] == df.ST[0]

    def test_count_5(self):
        # check bucket time set to 'end'
        data = otp.Ticks([["x", "y", "offset"], [15, 3, 0], [2, -5, 5], [9, 99, 9]])

        data = data.agg({"num_ticks": otp.agg.count()}, bucket_time="end")
        data.ET = data._END_TIME

        df = otp.run(data)
        assert len(df) == 1
        assert df.num_ticks[0] == 3 and df.Time[0] == df.ET[0]

    def test_count_6(self):
        # heterogeneous aggragion with count
        data = otp.Ticks({"x": [15, 2, 9], "y": [3.0, -5, 99]})

        data = data.agg(
            {
                "x_sum": otp.agg.sum(data.x),
                "x_min": otp.agg.min(data.x),
                "x_max": otp.agg.max(data.x),
                "x_first": otp.agg.first(data.x),
                "x_last": otp.agg.last(data.x),
                "y_sum": otp.agg.sum(data.y),
                "y_min": otp.agg.min(data.y),
                "y_max": otp.agg.max(data.y),
                "y_first": otp.agg.first(data.y),
                "y_last": otp.agg.last(data.y),
                "num_ticks": otp.agg.count(),
            }
        )

        assert hasattr(data, "x_sum") and data.x_sum.dtype is int
        assert hasattr(data, "x_min") and data.x_min.dtype is int
        assert hasattr(data, "x_max") and data.x_max.dtype is int
        assert hasattr(data, "x_first") and data.x_first.dtype is int
        assert hasattr(data, "x_last") and data.x_last.dtype is int
        assert hasattr(data, "y_sum") and data.y_sum.dtype is float
        assert hasattr(data, "y_min") and data.y_min.dtype is float
        assert hasattr(data, "y_max") and data.y_max.dtype is float
        assert hasattr(data, "y_first") and data.y_first.dtype is float
        assert hasattr(data, "y_last") and data.y_last.dtype is float
        assert hasattr(data, "_START_TIME") and data._START_TIME.dtype is otp.nsectime
        assert hasattr(data, "_END_TIME") and data._END_TIME.dtype is otp.nsectime
        assert hasattr(data, "num_ticks") and data.num_ticks.dtype is int

        df = otp.run(data)
        assert len(df) == 1

        assert df.x_sum[0] == (15 + 2 + 9)
        assert df.x_min[0] == 2
        assert df.x_max[0] == 15
        assert df.x_first[0] == 15
        assert df.x_last[0] == 9
        assert df.y_sum[0] == (-5 + 3 + 99)
        assert df.y_min[0] == -5
        assert df.y_max[0] == 99
        assert df.y_first[0] == 3
        assert df.y_last[0] == 99
        assert df.num_ticks[0] == 3

    def test_count_7(self):
        # num ticks in 1 sec interval with bucket_time='start'
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5, 99], "offset": [0, 999, 1001]})

        data = data.agg({"num_ticks": otp.agg.count()}, bucket_interval=1, bucket_time="start")
        data.ST = data._START_TIME

        df = otp.run(data)
        assert df.num_ticks[0] == 2 and df.Time[0] == df.ST[0]
        assert df.num_ticks[1] == 1 and df.Time[1] == df.ST[1] + otp.Second(1)
        assert df.num_ticks[2] == 0 and df.Time[2] == df.ST[2] + otp.Second(2)
        assert df.num_ticks[3] == 0 and df.Time[3] == df.ST[3] + otp.Second(3)
        # ... etc for whole run query interval

    def test_count_8(self):
        # num ticks in 1 sec interval with bucket_time='end'
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5, 99], "offset": [0, 999, 1001]})

        data = data.agg({"num_ticks": otp.agg.count()}, bucket_interval=1, bucket_time="end")
        data.ST = data._START_TIME

        df = otp.run(data)
        assert df.num_ticks[0] == 2 and df.Time[0] == df.ST[0] + otp.Second(1)
        assert df.num_ticks[1] == 1 and df.Time[1] == df.ST[1] + otp.Second(2)
        assert df.num_ticks[2] == 0 and df.Time[2] == df.ST[2] + otp.Second(3)

    def test_count_9(self):
        # num ticks in 1 sec interval and running=True
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5, 99], "offset": [0, 999, 1001]})

        # it is not allowed to have running=True and bucket_time=start
        with pytest.raises(ValueError):
            otp.run(data.agg({"num_ticks": otp.agg.count()}, running=True, bucket_interval=1, bucket_time="start"))

        data = data.agg({"num_ticks": otp.agg.count()}, running=True, bucket_interval=1)
        data.ST = data._START_TIME

        df = otp.run(data)
        assert len(df) == 6

        assert df.num_ticks[0] == 1 and df.Time[0] == df.ST[0]
        assert df.num_ticks[1] == 2 and df.Time[1] == df.ST[1] + otp.Milli(999)
        assert df.num_ticks[2] == 1 and df.Time[2] == df.ST[2] + otp.Second(1)
        assert (df.num_ticks[3] == 2 and df.Time[3] == df.ST[3] + otp.Second(1) + otp.Milli(1))
        assert (df.num_ticks[4] == 1 and df.Time[4] == df.ST[4] + otp.Second(1) + otp.Milli(999))
        assert (df.num_ticks[5] == 0 and df.Time[5] == df.ST[5] + otp.Second(2) + otp.Milli(1))

    def test_group_by_1(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "z": [1, 0, 1]})

        data = data.agg({"x_sum": otp.agg.sum(data.x), "y_sum": otp.agg.sum(data.y)}, group_by=[data.z])

        assert hasattr(data, "z") and data.z.dtype is int
        assert hasattr(data, "y_sum") and data.y_sum.dtype is float
        assert hasattr(data, "x_sum") and data.x_sum.dtype is int
        assert not hasattr(data, "x")
        assert not hasattr(data, "y")

        data = data.sort([data.z])

        df = otp.run(data)
        assert len(df) == 2
        assert isinstance(df.z[0], np.integer)
        assert isinstance(df.y_sum[0], np.float64)
        assert isinstance(df.x_sum[0], np.integer)

        assert df.z[0] == 0 and df.x_sum[0] == 2 and df.y_sum[0] == -5.1
        assert df.z[1] == 1 and df.x_sum[1] == (15 + 9) and df.y_sum[1] == (3 + 99)

    def test_group_by_2(self):
        d = {"x": [15, 2, 9], "y": [3, -5.1, 99], "z": [1, 1, 1]}

        data = otp.Ticks(d)

        data = data.agg({"x_sum": otp.agg.sum(data.x), "y_sum": otp.agg.sum(data.y)}, group_by=[data.z])

        assert hasattr(data, "z") and data.z.dtype is int
        assert hasattr(data, "y_sum") and data.y_sum.dtype is float
        assert hasattr(data, "x_sum") and data.x_sum.dtype is int
        assert not hasattr(data, "x")
        assert not hasattr(data, "y")

        data = data.sort([data.z])

        df = otp.run(data)
        assert len(df) == 1
        assert isinstance(df.z[0], np.integer)
        assert isinstance(df.y_sum[0], np.float64)
        assert isinstance(df.x_sum[0], np.integer)

        assert (df.z[0] == 1 and df.x_sum[0] == sum(d["x"]) and df.y_sum[0] == sum(d["y"]))

    def test_group_by_3(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "z": ["a", "a", "b"]})

        data = data.agg(
            {"x_sum": otp.agg.sum(data.x), "y_max": otp.agg.max(data.y), "num": otp.agg.count()}, group_by=[data.z]
        )

        assert len(otp.run(data)) == 2
        assert hasattr(data, "z") and data.z.dtype is str
        assert hasattr(data, "x_sum") and data.x_sum.dtype is int
        assert hasattr(data, "y_max") and data.y_max.dtype is float

        data = data.sort([data.z])

        df = otp.run(data)
        assert df.z[0] == "a" and df.x_sum[0] == 17 and df.y_max[0] == 3
        assert df.z[1] == "b" and df.x_sum[1] == 9 and df.y_max[1] == 99

        assert df.z.equals(pd.Series(["a", "b"]))

    def test_group_by_4(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "z": ["a", "a", "b"]})

        data = data.agg(
            {"x_sum": otp.agg.sum(data.x), "y_max": otp.agg.max(data.y), "num": otp.agg.count()}, group_by=["z"]
        )

        data = data.sort([data.z])

        df = pd.DataFrame({"z": ["a", "b"], "x_sum": [17, 9], "y_max": [3.0, 99.0], "num": [2, 1]})

        df_res = otp.run(data)
        df_res = df_res.drop(["Time"], axis=1)
        pd.testing.assert_frame_equal(df_res.sort_index(axis=1), df.sort_index(axis=1))

    def test_group_by_5(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "z": ["a", "a", "b"]})

        # it is not allowed to pass 1 as object to group by
        with pytest.raises(TypeError):
            data.agg(
                {"x_sum": otp.agg.sum(data.x), "y_max": otp.agg.max(data.y), "num": otp.agg.count()}, group_by=[1]
            )

    def test_group_by_6(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "z": ["a", "a", "b"]})

        with pytest.raises(KeyError):
            data.agg(
                {"x_sum": otp.agg.sum(data.x), "y_max": otp.agg.max(data.y), "num": otp.agg.count()}, group_by=["agg"]
            )

    def test_vwap(self):
        prices = [3.5, 3.57, 3.49, 3.51]
        qty = [100, 150, 50, 200]
        data = otp.Ticks({"price": prices, "qty": qty})

        result = data.agg({"vwap": otp.agg.vwap(data.price, data.qty)})

        assert hasattr(result, "vwap") and result.vwap.dtype is float
        assert not hasattr(result, "price")
        assert not hasattr(result, "qty")

        df = otp.run(result)
        assert hasattr(df, "vwap") and isinstance(otp.run(result).vwap[0], np.float64)
        assert not hasattr(df, "price")
        assert not hasattr(df, "qty")

        assert len(df) == 1

        assert df.vwap[0] == sum(q * p for q, p in zip(prices, qty)) / sum(qty)

    def test_data_first_time(self):
        d = {"x": [5, 6, 3, 6], "y": [1.4, 3.1, 9.1, 5.5]}

        data = otp.Ticks(d)
        agg_data = data.agg({"first_t": otp.agg.first_time()}, group_by=["x"])

        df = otp.run(agg_data)
        res = otp.run(data)
        for inx in range(3):
            if df.x[inx] == 3:
                assert df.first_t[inx] == res.Time[2]
            elif df.x[inx] == 6:
                assert df.first_t[inx] == res.Time[1]
            elif df.x[inx] == 5:
                assert df.first_t[inx] == res.Time[0]

    def test_data_last_time(self):
        d = {"x": [5, 6, 3, 6], "y": [1.4, 3.1, 9.1, 5.5]}

        data = otp.Ticks(d)
        agg_data = data.agg({"last_t": otp.agg.last_time()}, group_by=["x"])

        df = otp.run(agg_data)
        res = otp.run(data)
        for inx in range(3):
            if df.x[inx] == 3:
                assert df.last_t[inx] == res.Time[2]
            elif df.x[inx] == 6:
                assert df.last_t[inx] == res.Time[3]
            elif df.x[inx] == 5:
                assert df.last_t[inx] == res.Time[0]

    def test_data_types(self):
        d = {"x": [5, 6, 3, 6], "y": [1.4, 3.1, 9.1, 5.5]}

        data = otp.Ticks(d)

        data.arrival_time = data.Time

        data = data.agg(
            {"x_sum": otp.agg.sum("x"), "y_sum": otp.agg.sum("y"), "first_t": otp.agg.first("arrival_time")})

        assert data.x_sum.dtype is int
        assert data.y_sum.dtype is float
        assert data.first_t.dtype is otp.nsectime

        df = otp.run(data)
        assert isinstance(df.x_sum[0], np.integer)
        assert isinstance(df.y_sum[0], np.float64)
        assert isinstance(df.first_t[0], pd.Timestamp)

    def test_average_1(self):
        # basic
        data = otp.Ticks({"x": [0, 1, 2, 3, 4], "y": [5, 6, 7, 8, 9]})

        data = data.agg({"average": otp.agg.average("x")})

        assert not hasattr(data, "x")
        assert not hasattr(data, "y")
        assert hasattr(data, "average") and data.average.dtype is float

        df = otp.run(data)
        assert len(df) == 1

        assert isinstance(df.average[0], float) and df.average[0] == 2

    def test_average_2(self):
        # running
        data = otp.Ticks({"x": [0, 1, 2, 3, 4], "y": [5, 6, 7, 8, 9]})

        data = data.agg({"average": otp.agg.average("x")}, running=True)
        data.ST = data._START_TIME

        assert not hasattr(data, "x")
        assert not hasattr(data, "y")
        assert hasattr(data, "average") and data.average.dtype is float

        df = otp.run(data)
        assert len(df) == 5

        for i in range(5):
            assert df.average[i] == i / 2
            assert df.Time[i] == df.ST[0] + otp.Milli(i)

    def test_average_3(self):
        # bucket_time 'end'
        data = otp.Ticks({"x": [0, 1, 2, 3, 4], "y": [5, 6, 7, 8, 9]})

        data = data.agg({"average": otp.agg.average("x")}, bucket_time="end")
        data.ET = data._END_TIME

        df = otp.run(data)
        assert len(df) == 1
        assert df.Time[0] == df.ET[0]

    def test_average_4(self):
        # bucket_time 'start'
        data = otp.Ticks({"x": [0, 1, 2, 3, 4], "y": [5, 6, 7, 8, 9]})

        data = data.agg({"average": otp.agg.average("x")}, bucket_time="start")
        data.ST = data._START_TIME

        df = otp.run(data)
        assert len(df) == 1
        assert df.Time[0] == df.ST[0]

    def test_average_5(self):
        # 1 sec interval with bucket_time='start'
        data = otp.Ticks({"x": [0, 1, 2, 3, 4], "y": [5, 6, 7, 8, 9], "offset": [0, 500, 1000, 1500, 2000]})

        data = data.agg({"average": otp.agg.average("x")}, bucket_interval=1, bucket_time="start")
        data.ST = data._START_TIME

        df = otp.run(data)
        assert df.average[0] == 0.5 and df.Time[0] == df.ST[0]
        assert df.average[1] == 2.5 and df.Time[1] == df.ST[1] + otp.Second(1)
        assert df.average[2] == 4.0 and df.Time[2] == df.ST[2] + otp.Second(2)
        assert np.isnan(df.average[3]) and df.Time[3] == df.ST[3] + otp.Second(3)

    def test_average_6(self):
        # 1 sec interval with bucket_time='end'
        data = otp.Ticks({"x": [0, 1, 2, 3, 4], "y": [5, 6, 7, 8, 9], "offset": [0, 500, 1000, 1500, 2000]})

        data = data.agg({"average": otp.agg.average("x")}, bucket_interval=1, bucket_time="end")
        data.ST = data._START_TIME

        df = otp.run(data)
        assert df.average[0] == 0.5 and df.Time[0] == df.ST[0] + otp.Second(1)
        assert df.average[1] == 2.5 and df.Time[1] == df.ST[1] + otp.Second(2)
        assert df.average[2] == 4.0 and df.Time[2] == df.ST[2] + otp.Second(3)
        assert np.isnan(df.average[3]) and df.Time[3] == df.ST[3] + otp.Second(4)

    def test_average_7(self):
        # 1 sec interval, bucket_time=start, running=True
        data = otp.Ticks({"x": [0, 1, 2, 3, 4], "y": [5, 6, 7, 8, 9], "offset": [0, 500, 1000, 1500, 2000]})

        with pytest.raises(ValueError):
            otp.run(data.agg({"average": otp.agg.average("x")}, running=True, bucket_interval=1, bucket_time="start"))
        # with

    # def

    def test_average_8(self):
        # 1 sec interval, bucket_time=default ('end'), running=True
        data = otp.Ticks({"x": [0, 1, 2, 3, 4], "y": [5, 6, 7, 8, 9], "offset": [0, 500, 1000, 1500, 2000]})

        data = data.agg({"average": otp.agg.average("x")}, running=True, bucket_interval=1)
        data.ST = data._START_TIME

        df = otp.run(data)
        assert len(df) == 7

        assert df.average[0] == 0.0 and df.Time[0] == df.ST[0]
        assert df.average[1] == 0.5 and df.Time[1] == df.ST[1] + otp.Milli(500)
        assert df.average[2] == 1.5 and df.Time[2] == df.ST[2] + otp.Milli(1000)
        assert df.average[3] == 2.5 and df.Time[3] == df.ST[3] + otp.Milli(1500)
        assert df.average[4] == 3.5 and df.Time[4] == df.ST[4] + otp.Milli(2000)
        assert df.average[5] == 4.0 and df.Time[5] == df.ST[5] + otp.Milli(2500)
        assert np.isnan(df.average[6]) and df.Time[6] == df.ST[6] + otp.Milli(3000)

    @pytest.mark.parametrize('running', [True, False])
    def test_flexible_schema(self, running):
        """Test checks that if field not in schema it won't be dropped"""
        t = otp.Ticks({'A': [1, 2, 3], 'B': [4, 5, 6]})
        t.sink(otq.AddField(field='NOT_IN_SCHEMA', value=1))
        schema_before_agg = t.schema.copy()
        t = t.agg({'C': otp.agg.sum('A')}, all_fields=True, running=running)
        schema_before_agg['C'] = int
        assert t.schema == schema_before_agg
        assert 'NOT_IN_SCHEMA' not in schema_before_agg
        df = otp.run(t)
        assert 'NOT_IN_SCHEMA' in df


class TestSourceFirstTick:

    def test_first_tick_1(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.first()
        data.ST = data._START_TIME

        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 1
        assert df.x[0] == 15 and df.y[0] == 3 and df.Time[0] == df.ST[0]

    def test_first_tick_2(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.first(n=2)
        data.ST = data._START_TIME

        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 2
        assert df.x[0] == 15 and df.y[0] == 3 and df.Time[0] == df.ST[0]
        assert (df.x[1] == 2 and df.y[1] == -5.1 and df.Time[1] == df.ST[1] + otp.Milli(5))

    def test_first_tick_3(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.first(n=2, running=True)
        data.ST = data._START_TIME

        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 5
        assert df.x[0] == 15 and df.y[0] == 3 and df.Time[0] == df.ST[0]
        assert df.x[1] == 15 and df.y[1] == 3 and df.Time[1] == df.ST[1]
        assert df.x[2] == 15 and df.y[2] == 3 and df.Time[2] == df.ST[2]
        assert (df.x[3] == 2 and df.y[3] == -5.1 and df.Time[3] == df.ST[3] + otp.Milli(5))
        assert (df.x[4] == 2 and df.y[4] == -5.1 and df.Time[4] == df.ST[4] + otp.Milli(5))

    def test_first_tick_4(self):
        """test keep_timestamp parameter"""
        data = otp.Ticks({"x": [15, 2, 9], "offset": [1, 5, 9]})
        res_1 = otp.run(data.first(bucket_time='start', keep_timestamp=False))
        assert len(res_1) == 1
        assert res_1['Time'][0] == otp.config['default_start_time']
        assert res_1['x'][0] == 15
        assert res_1['TICK_TIME'][0] == otp.config['default_start_time'] + otp.Milli(1)
        res_2 = otp.run(data.first(bucket_time='end', keep_timestamp=False))
        assert len(res_2) == 1
        assert res_2['Time'][0] == otp.config['default_end_time']
        assert res_2['x'][0] == 15
        assert res_2['TICK_TIME'][0] == otp.config['default_start_time'] + otp.Milli(1)

    def test_group_by_first(self):
        data = otp.Ticks(
            {
                "x": [15, 2, 9, 17, 1],
                "y": [3, -5.1, 99, -3.8, 21],
                "z": [1, 1, 0, 1, 0],
                "w": ["b", "a", "a", "b", "a"],
                "offset": list(range(5)),
            }
        )

        result = data.first(group_by=[data.z, data.w]).sort([data.z, data.w])

        df = pd.DataFrame({"z": [0, 1, 1], "w": ["a", "a", "b"], "x": [9, 2, 15], "y": [99, -5.1, 3]})
        df_res = otp.run(result).drop(["Time"], axis=1)

        assert df_res.x.equals(df.x)
        assert df_res.y.equals(df.y)
        assert df_res.z.equals(df.z)
        assert df_res.w.equals(df.w)


class TestSourceLastTick:

    def test_last_tick_1(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.last()
        data.ST = data._START_TIME

        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 1
        assert (df.x[0] == 9 and df.y[0] == 99 and df.Time[0] == df.ST[0] + otp.Milli(9))

    def test_last_tick_2(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.last(n=2)
        data.ST = data._START_TIME

        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 2
        assert (df.x[0] == 2 and df.y[0] == -5.1 and df.Time[0] == df.ST[0] + otp.Milli(5))
        assert (df.x[1] == 9 and df.y[1] == 99 and df.Time[1] == df.ST[1] + otp.Milli(9))

    def test_last_tick_3(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.last(n=2, running=True)
        data.ST = data._START_TIME

        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 5
        assert df.x[0] == 15 and df.y[0] == 3 and df.Time[0] == df.ST[0]
        assert df.x[1] == 15 and df.y[1] == 3 and df.Time[1] == df.ST[1]
        assert (df.x[2] == 2 and df.y[2] == -5.1 and df.Time[2] == df.ST[2] + otp.Milli(5))
        assert (df.x[3] == 2 and df.y[3] == -5.1 and df.Time[3] == df.ST[3] + otp.Milli(5))
        assert (df.x[4] == 9 and df.y[4] == 99 and df.Time[4] == df.ST[4] + otp.Milli(9))

    def test_last_tick_4(self):
        """test keep_timestamp parameter"""
        data = otp.Ticks({"x": [15, 2, 9], "offset": [1, 5, 9]})
        res_1 = otp.run(data.last(bucket_time='start', keep_timestamp=False))
        assert len(res_1) == 1
        assert res_1['Time'][0] == otp.config['default_start_time']
        assert res_1['x'][0] == 9
        assert res_1['TICK_TIME'][0] == otp.config['default_start_time'] + otp.Milli(9)
        res_2 = otp.run(data.last(bucket_time='end', keep_timestamp=False))
        assert len(res_2) == 1
        assert res_2['Time'][0] == otp.config['default_end_time']
        assert res_2['x'][0] == 9
        assert res_2['TICK_TIME'][0] == otp.config['default_start_time'] + otp.Milli(9)

    def test_group_by_last(self):
        data = otp.Ticks(
            {
                "x": [15, 2, 9, 17, 1],
                "y": [3, -5.1, 99, -3.8, 21],
                "z": [1, 1, 0, 1, 0],
                "w": ["b", "a", "a", "b", "a"],
                "offset": list(range(5)),
            }
        )

        result = data.last(group_by=[data.z, data.w]).sort([data.z, data.w])

        df = pd.DataFrame({"z": [0, 1, 1], "w": ["a", "a", "b"], "x": [1, 2, 17], "y": [21, -5.1, -3.8]})
        df_res = otp.run(result).drop(["Time"], axis=1)

        assert df_res.x.equals(df.x)
        assert df_res.y.equals(df.y)
        assert df_res.z.equals(df.z)
        assert df_res.w.equals(df.w)


class TestSourceHighTick:

    def test_high_tick_1(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.high(data.x)
        data.ST = data._START_TIME
        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 1
        assert df.x[0] == 15 and df.y[0] == 3 and df.Time[0] == df.ST[0]

    def test_high_tick_2(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.high("y")
        data.ST = data._START_TIME

        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 1
        assert (df.x[0] == 9 and df.y[0] == 99 and df.Time[0] == df.ST[0] + otp.Milli(9))

    def test_high_tick_3(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        # it's allowed to pass either column or column name
        with pytest.raises(TypeError):
            data.high(0)

    def test_high_tick_4(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        # there is no 'X' column
        with pytest.raises(TypeError):
            data.high("X")

    def test_high_tick_5(self):
        # check 'running' parameter
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.high(data.x, running=True)
        data.ST = data._START_TIME

        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 3
        assert df.x[0] == 15 and df.y[0] == 3 and df.Time[0] == df.ST[0]
        assert df.x[0] == 15 and df.y[0] == 3 and df.Time[1] == df.ST[0]
        assert df.x[0] == 15 and df.y[0] == 3 and df.Time[2] == df.ST[0]

    def test_high_tick_6(self):
        # check 'n' parameter
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.high("y", n=2)
        data.ST = data._START_TIME

        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 2
        assert df.x[0] == 15 and df.y[0] == 3 and df.Time[0] == df.ST[0]
        assert (df.x[1] == 9 and df.y[1] == 99 and df.Time[1] == df.ST[0] + otp.Milli(9))

    def test_high_tick_7(self):
        """test keep_timestamp parameter"""
        data = otp.Ticks({"x": [15, 2, 9], "offset": [1, 5, 9]})
        res_1 = otp.run(data.high(column='x', bucket_time='start', keep_timestamp=False))
        assert len(res_1) == 1
        assert res_1['Time'][0] == otp.config['default_start_time']
        assert res_1['x'][0] == 15
        assert res_1['TICK_TIME'][0] == otp.config['default_start_time'] + otp.Milli(1)
        res_2 = otp.run(data.high(column='x', bucket_time='end', keep_timestamp=False))
        assert len(res_2) == 1
        assert res_2['Time'][0] == otp.config['default_end_time']
        assert res_2['x'][0] == 15
        assert res_1['TICK_TIME'][0] == otp.config['default_start_time'] + otp.Milli(1)

    def test_group_by_high_1(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "z": [1, 0, 1]})

        result = data.high(data.x, group_by=[data.z]).sort([data.z])

        df = pd.DataFrame({"z": [0, 1], "x": [2, 15], "y": [-5.1, 3]})

        df_res = otp.run(result).drop(["Time"], axis=1)

        assert df_res.x.equals(df.x)
        assert df_res.y.equals(df.y)
        assert df_res.z.equals(df.z)

    def test_group_by_high_2(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "z": [1, 0, 1]})

        result = data.high(data.x, group_by=["z"]).sort([data.z])

        df = pd.DataFrame({"z": [0, 1], "x": [2, 15], "y": [-5.1, 3]})
        df_res = otp.run(result).drop(["Time"], axis=1)

        assert df_res.x.equals(df.x)
        assert df_res.y.equals(df.y)
        assert df_res.z.equals(df.z)

    def test_group_by_high_3(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "z": [1, 0, 1]})

        with pytest.raises(KeyError):
            data.high(data.x, group_by=["high"])


class TestSourceLowTick:

    def test_low_tick_1(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.low(data.x)
        data.ST = data._START_TIME

        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 1
        assert (df.x[0] == 2 and df.y[0] == -5.1 and df.Time[0] == df.ST[0] + otp.Milli(5))

    def test_low_tick_2(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.low("y")
        data.ST = data._START_TIME

        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 1
        assert (df.x[0] == 2 and df.y[0] == -5.1 and df.Time[0] == df.ST[0] + otp.Milli(5))

    def test_low_tick_3(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        # it's allowed to pass either column or column name
        with pytest.raises(TypeError):
            data.low(0)

    def test_low_tick_4(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        # there is no 'X' column
        with pytest.raises(TypeError):
            data.low("X")

    def test_low_tick_5(self):
        # check 'running' parameter
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "offset": [0, 5, 9]})

        data = data.low(data.x, running=True)
        data.ST = data._START_TIME

        assert hasattr(data, "x") and data.x.dtype is int
        assert hasattr(data, "y") and data.y.dtype is float

        df = otp.run(data)
        assert len(df) == 3
        assert df.x[0] == 15 and df.y[0] == 3 and df.Time[0] == df.ST[0]
        assert (df.x[1] == 2 and df.y[1] == -5.1 and df.Time[1] == df.ST[0] + otp.Milli(5))
        assert (df.x[2] == 2 and df.y[2] == -5.1 and df.Time[2] == df.ST[0] + otp.Milli(5))

    def test_low_tick_6(self):
        """test keep_timestamp parameter"""
        data = otp.Ticks({"x": [15, 2, 9], "offset": [1, 5, 9]})
        res_1 = otp.run(data.low(column='x', bucket_time='start', keep_timestamp=False))
        assert len(res_1) == 1
        assert res_1['Time'][0] == otp.config['default_start_time']
        assert res_1['x'][0] == 2
        assert res_1['TICK_TIME'][0] == otp.config['default_start_time'] + otp.Milli(5)
        res_2 = otp.run(data.low(column='x', bucket_time='end', keep_timestamp=False))
        assert len(res_2) == 1
        assert res_2['Time'][0] == otp.config['default_end_time']
        assert res_2['x'][0] == 2
        assert res_1['TICK_TIME'][0] == otp.config['default_start_time'] + otp.Milli(5)

    def test_group_by_low_1(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "z": [1, 0, 1]})

        result = data.low(data.x, group_by=[data.z]).sort([data.z])

        df = pd.DataFrame({"z": [0, 1], "x": [2, 9], "y": [-5.1, 99]})
        df_res = otp.run(result).drop(["Time"], axis=1)

        assert df_res.x.equals(df.x)
        assert df_res.y.equals(df.y)
        assert df_res.z.equals(df.z)

    def test_group_by_low_2(self):
        data = otp.Ticks({"x": [15, 2, 9], "y": [3, -5.1, 99], "z": [1, 1, 0]})

        result = data.low(data.y, group_by=["z"]).sort([data.z])

        df = pd.DataFrame({"z": [0, 1], "x": [9, 2], "y": [99, -5.1]})
        df_res = otp.run(result).drop(["Time"], axis=1)

        assert df_res.x.equals(df.x)
        assert df_res.y.equals(df.y)
        assert df_res.z.equals(df.z)

    def test_group_by_low_3(self):
        data = otp.Ticks(
            {"x": [15, 2, 9, 17, 1], "y": [3, -5.1, 99, -3.8, 21], "z": [1, 1, 0, 1, 0], "w": ["b", "a", "a", "b", "a"]}
        )

        result = data.low(data.y, group_by=[data.z, data.w]).sort([data.z, data.w])

        df = pd.DataFrame({"z": [0, 1, 1], "w": ["a", "a", "b"], "x": [1, 2, 17], "y": [21, -5.1, -3.8]})
        df_res = otp.run(result).drop(["Time"], axis=1)

        assert df_res.x.equals(df.x)
        assert df_res.y.equals(df.y)
        assert df_res.z.equals(df.z)
        assert df_res.w.equals(df.w)


class TestSourceHighTime:

    @pytest.mark.parametrize('use_agg', [True, False])
    def test_high_time_box_or_agg(self, use_agg):
        data = otp.Ticks(
            {"AAA": [2, 15, 9, 2], "BBB": [3, 20, -5.1, 20], "offset": [0, 5, 10, 15]}
        )

        if use_agg:
            data = data.agg({"CCC": otp.agg.high_time(column="BBB")})
            result_column = "CCC"
        else:
            data = otp.agg.high_time(column="BBB").apply(data, name="VALUE")
            result_column = "VALUE"

        data.ST = data._START_TIME
        data.ET = data._END_TIME

        assert hasattr(data, result_column)
        assert hasattr(data, "Time")

        df = otp.run(data)
        assert len(df) == 1
        assert df.Time[0] == df.ET[0]  # bucket end
        result_dt = df[result_column][0]
        assert result_dt == df.ST[0] + otp.Milli(5)  # timestamp of y=20

    @pytest.mark.parametrize('field_name', ['x', 'VALUE'])
    def test_high_time_box_value_column_exists(self, field_name):
        # Checks that existing column names does not affect aggregation result column names
        data = otp.Ticks(
            {"x": [2, 15, 9, 2], "VALUE": [3, 20, -5.1, 20], "offset": [0, 5, 10, 15]}
        )

        data = otp.agg.high_time(field_name).apply(data, name='VALUE')
        data.ST = data._START_TIME
        data.ET = data._END_TIME

        data_df = otp.run(data)

        assert hasattr(data_df, "VALUE")
        assert not hasattr(data_df, "x")
        assert hasattr(data_df, "Time")

        assert len(data_df) == 1
        assert data_df.Time[0] == data_df.ET[0]  # bucket end
        assert data_df["VALUE"][0] == data_df.ST[0] + otp.Milli(5)


class TestSourceLowTime:

    @pytest.mark.parametrize('use_agg', [False, True])
    def test_low_time_1(self, use_agg):
        data = otp.Ticks({"PRICE": [15, 2, 9, 2], "offset": [0, 5, 11, 17]})

        if use_agg:
            data = data.agg({"PRICE": otp.agg.low_time(column="PRICE")})
            result_column = "PRICE"
        else:
            with pytest.warns(FutureWarning):
                data = data.low_time("PRICE")
            result_column = "VALUE"

        data.ST = data._START_TIME
        data.ET = data._END_TIME

        assert hasattr(data, result_column)
        assert hasattr(data, "Time")

        df = otp.run(data)
        assert len(df) == 1
        assert df.Time[0] == df.ET[0]  # bucket end
        result_dt = df[result_column][0]
        assert result_dt == df.ST[0] + otp.Milli(5)  # timestamp of y=20

    @pytest.mark.parametrize(
        'selection,expected_offset',
        [['first', 0], ['last', 17]]
    )
    def test_low_time_2(self, selection, expected_offset):
        data = otp.Ticks({"x": [2, 15, 9, 2], "offset": [0, 5, 11, 17]})

        with pytest.warns(FutureWarning):
            data = data.low_time("x", selection=selection)
        data.ST = data._START_TIME
        data.ET = data._END_TIME

        assert not hasattr(data, "x")
        assert hasattr(data, "VALUE")
        assert hasattr(data, "Time")

        df = otp.run(data)
        assert len(df) == 1
        assert df.Time[0] == df.ET[0]  # bucket end
        assert df.VALUE[0] == df.ST[0] + otp.Milli(expected_offset)  # timestamp of y=20


def test_all_field_and_arg_is_presented():
    data = otp.Tick(X=1)
    with pytest.raises(ValueError, match="You try to propagate all fields and put result into "
                                         "already existing fields: 'X'"):
        data.agg({"X": otp.agg.average("X")}, all_fields=True, running=True)


class TestDistinct:
    def _get_data(self):
        return otp.Ticks(
            {
                "count": [0, 1, 1, 2, 2],
                "price": [5, 5, 5, 5, 5],
                "unique": [1, 2, 3, 4, 5],
                "exchange": ["N", "C", "N", "C", "C"],
            }
        )

    def test_int(self):
        data = self._get_data()
        distinct_count = otp.run(data.distinct("count"))
        assert all(distinct_count["count"] == [0, 1, 2])

    def test_chars(self):
        data = self._get_data()
        distinct_count = otp.run(data.distinct("exchange"))
        assert all(distinct_count["exchange"] == ["N", "C"])

    def test_all_the_same(self):
        data = self._get_data()
        distinct_count = otp.run(data.distinct("price"))
        assert all(distinct_count["price"] == [5])

    def test_all_unique(self):
        data = self._get_data()
        distinct_count = otp.run(data.distinct("unique"))
        assert all(distinct_count["unique"] == [1, 2, 3, 4, 5])

    def test_schema_for_key_attrs(self):
        data = self._get_data()
        exchanges = data.distinct('exchange')
        columns = exchanges.columns(skip_meta_fields=True)
        assert len(columns) == 1
        assert 'exchange' in columns

    def test_all_attr_on_unique_field(self):
        data = self._get_data()
        distinct_count = otp.run(data.distinct("unique", key_attrs_only=False))
        assert len(distinct_count.columns) == 5  # 4 user defined + TIMESTAMP
        assert all(distinct_count["unique"] == [1, 2, 3, 4, 5])
        data = otp.run(data)
        for column in data.columns[1:]:  # skip timestamp
            assert all(distinct_count[column] == data[column])

    def test_all_attr_on_non_unique_field(self):
        data = self._get_data()
        distinct_count = otp.run(data.distinct("count", key_attrs_only=False))
        assert len(distinct_count.columns) == 5  # 4 user defined + TIMESTAMP
        assert len(distinct_count) == 3

    def test_bucket_units(self):
        data = self._get_data()
        distinct_count = otp.run(data.distinct("exchange", bucket_interval_units="ticks", bucket_interval=3))
        assert all(distinct_count["exchange"] == ["N", "C", "C"])

    def test_list_keys(self):
        data = self._get_data()
        expected = otp.run(data).drop_duplicates(["exchange", "count"])
        distinct_count = otp.run(data.distinct(["exchange", data["count"]]))
        assert distinct_count[["exchange", "count"]].equals(expected[["exchange", "count"]])

    @pytest.mark.parametrize('keys', ["exchange, count",
                                      "exchange,count"])
    def test_backward_compatibility(self, keys):
        data = self._get_data()
        keys_list = [i.strip() for i in keys.split(',')]
        expected = otp.run(data).drop_duplicates(keys_list)
        distinct_count = otp.run(data.distinct(keys))
        assert distinct_count[keys_list].equals(expected[["exchange", "count"]])


class TestMaxMinTime:
    @pytest.mark.parametrize('func,exp', [(otp.agg.min, 0), (otp.agg.max, 4000)], ids=['min', 'max'])
    def test_simple(self, func, exp):
        data = otp.Ticks(X=[1, 2, 3, 4, 5])
        data['Time'] += otp.Nano(1)
        data['T'] = data['Time']
        data = data.agg({'Y': func(data['T']), 'Z': otp.agg.min(data['X'])})

        assert set(data.schema.keys()) == {'Y', 'Z'}
        assert data.schema['Y'] is otp.nsectime
        assert data.schema['Z'] is int
        df = otp.run(data)

        assert set(df.columns) == {'Time', 'Y', 'Z'}

        res_t = df['Y'][0]
        exp_t = otp.dt(2003, 12, 1, 0, 0, 0, exp, 1)
        assert str(res_t) == str(exp_t)
        assert res_t.timestamp() == exp_t.timestamp()
        assert res_t.nanosecond == exp_t.nanosecond
        assert all(df['Z'] == [1])

    @pytest.mark.parametrize('func,exp_coeff', [(otp.agg.min, 0), (otp.agg.max, 1)], ids=['min', 'max'])
    def test_all_fields(self, func, exp_coeff):
        data = otp.Ticks(X=[1, 2, 3, 4, 5])
        data['Time'] += otp.Nano(1)
        data['T'] = data['Time']
        data = data.agg({'Y': func(data['T']), 'Z': otp.agg.min(data['X'])},
                        running=True, all_fields=True)

        assert data.schema == {'X': int, 'Y': otp.nsectime, 'Z': int, 'T': otp.nsectime}

        df = otp.run(data)
        assert len(df) == 5
        assert set(df.columns) == {'Time', 'X', 'Y', 'Z', 'T'}
        assert all(df['X'] == [1, 2, 3, 4, 5])
        assert all(df['Z'] == [1] * 5)

        res_t = list(map(str, df['Y']))
        exp_t = [str(otp.dt(2003, 12, 1, 0, 0, 0, x * 1000 * exp_coeff, 1)) for x in range(0, 5)]
        assert exp_t == res_t

    @pytest.mark.parametrize('func,exp_coeff', [(otp.agg.min, 0), (otp.agg.max, 1)], ids=['min', 'max'])
    def test_two_nsectime_fields(self, func, exp_coeff):
        data = otp.Ticks(X=[1, 2, 3, 4, 5])
        data['Time'] += otp.Nano(1)
        data['T'] = data['Time']
        agg_dict = {'A': func(data['T']), 'B': func(data['Time'])}
        data = data.agg(agg_dict, running=True, all_fields=True)
        df = otp.run(data)
        for column in agg_dict:
            res_t = [x.timestamp() for x in df[column]]
            exp_t = [otp.dt(2003, 12, 1, 0, 0, 0, x * 1000 * exp_coeff, 1).timestamp() for x in range(0, 5)]
            assert exp_t == res_t

    def test_two_agg_one_cup(self):
        data = otp.Ticks(X=[1, 2, 3, 4, 5])
        agg_dict = {'A': otp.agg.min(data['Time']), 'B': otp.agg.max(data['Time'])}
        data = data.agg(agg_dict, running=True, all_fields=True)
        df = otp.run(data)
        assert all(df["A"] == otp.config['default_start_time'])
        assert all(df["B"] == [otp.config['default_start_time'] + otp.Milli(i) for i in range(5)])


class TestStddev:
    def test_simple(self):
        data = otp.Ticks(X=[0, 1, 2, 3, 4])
        data = data.agg({"stddev": otp.agg.stddev("X")})
        df = otp.run(data)
        assert all(df["stddev"] == [pytest.approx(1.414214)])

    def test_biased(self):
        data = otp.Ticks(X=[0, 1, 2, 3, 4])
        data = data.agg({"stddev": otp.agg.stddev("X", biased=False)})
        df = otp.run(data)
        assert all(df["stddev"] == [pytest.approx(1.581139)])

    def test_several(self):
        data = otp.Ticks(X=[0, 1, 2, 3, 4])
        data["Y"] = data["X"] + 1
        data = data.agg({"X_STDDEV": otp.agg.stddev("X"), "X_MEAN": otp.agg.average("X"),
                         "Y_STDDEV": otp.agg.stddev("Y"), "Y_MEAN": otp.agg.average("Y")})
        df = otp.run(data)
        assert all(df["X_STDDEV"] == [pytest.approx(1.414214)])
        assert all(df["Y_STDDEV"] == [pytest.approx(1.414214)])
        assert all(df["X_MEAN"] == [pytest.approx(2.0)])
        assert all(df["Y_MEAN"] == [pytest.approx(3.0)])

    def test_time(self):
        data = otp.Ticks(X=[0, 1, 2, 3, 4])
        data = data.agg({"stddev": otp.agg.stddev("X")}, running=True, bucket_interval=2)
        df = otp.run(data)
        assert all(df["stddev"].head(-1) == [pytest.approx(0.000000),
                                             pytest.approx(0.500000),
                                             pytest.approx(0.816497),
                                             pytest.approx(1.118034),
                                             pytest.approx(1.414214),
                                             pytest.approx(1.118034),
                                             pytest.approx(0.816497),
                                             pytest.approx(0.500000),
                                             pytest.approx(0.000000),
                                             ])
        assert math.isnan(df["stddev"].tail(1).iloc[0])


def test_agg_operation_in_group_by():
    data = otp.Ticks(X=[0, 1, 2, 3, 4],
                     Y=[0, 0, 1, 0, 1],
                     Z=[1, 0, 1, 1, 0])
    data['T'] = data['Y'] - data['Z']

    data = data.agg({'X_SUM': otp.agg.sum(data['X']),
                     'T_FST': otp.agg.first(data['T'])},
                    group_by=data['Y'] - data['Z'])

    df = otp.run(data)

    assert all(df['X_SUM'] == [3, 3, 4])
    assert all(df['T_FST'] == [-1, 0, 1])


class TestTwAverage:
    def test_simple(self):
        data = otp.Ticks(X=[2, 1, 2])
        data = data.agg({"twAverage": otp.agg.tw_average("X")}, running=True)
        df = otp.run(data)
        assert all(df["twAverage"] == [pytest.approx(2.0),
                                       pytest.approx(2.0),
                                       pytest.approx(1.5),
                                       ])

    def test_with_bucket_interval(self):
        data = otp.Ticks(X=[1, 2, 3, 4], offset=[1000, 3000, 5000, 6000])
        data = data.agg({"twAverage": otp.agg.tw_average("X")}, running=True, bucket_interval=3)
        df = otp.run(data)
        assert all(df["twAverage"] == [pytest.approx(1.000000),
                                       pytest.approx(1.000000),
                                       pytest.approx(1.333333),
                                       pytest.approx(1.666667),
                                       pytest.approx(2.333333),
                                       pytest.approx(3.666667),
                                       pytest.approx(4.000000)
                                       ])


class TestFirstLastAggregationParams:
    @pytest.mark.skipif(
        not is_first_ep_skip_tick_if_supported(),
        reason="`skip_tick_if` (`SKIP_TICK_IF`) parameter not supported in the FIRST EP on this OneTick version",
    )
    def test_first_tick_skip_tick_if_1(self):
        data = otp.Ticks(X=[1, 2, 3, 4])
        data = data.agg({'Y': otp.agg.first('X', skip_tick_if=1)})
        df = otp.run(data)

        assert list(df['Y']) == [2]

    @pytest.mark.skipif(
        not is_first_ep_skip_tick_if_supported(),
        reason="`skip_tick_if` (`SKIP_TICK_IF`) parameter not supported in the FIRST EP on this OneTick version",
    )
    def test_first_tick_skip_tick_if_2(self):
        data = otp.Ticks(X=[1, 2, 3, 1, 1])
        data = data.agg({'Y': otp.agg.first('X', skip_tick_if=1)}, bucket_interval=2, bucket_units="ticks")
        df = otp.run(data)

        assert list(df['Y']) == [2, 3, 0]

    @pytest.mark.skipif(
        not is_last_ep_fwd_fill_if_supported(),
        reason="`skip_tick_if` (`FWD_FILL_IF`) parameter not supported in the LAST EP on this OneTick version",
    )
    def test_last_tick_skip_tick_if_1(self):
        data = otp.Ticks(X=[1, 2, 3, 4])
        data = data.agg({'Y': otp.agg.last('X', skip_tick_if=4)})
        df = otp.run(data)

        assert list(df['Y']) == [3]

    @pytest.mark.skipif(
        not is_last_ep_fwd_fill_if_supported(),
        reason="`skip_tick_if` (`FWD_FILL_IF`) parameter not supported in the LAST EP on this OneTick version",
    )
    def test_last_tick_skip_tick_if_2(self):
        data = otp.Ticks(X=[1, 2, 2, 3, 2])
        data = data.agg({'Y': otp.agg.last('X', skip_tick_if=2)}, bucket_interval=2, bucket_units="ticks")
        df = otp.run(data)

        assert list(df['Y']) == [1, 3, 0]

    @pytest.mark.skipif(
        not is_first_ep_skip_tick_if_supported(),
        reason="`skip_tick_if` (`SKIP_TICK_IF`) parameter not supported in the FIRST EP on this OneTick version",
    )
    def test_first_skip_tick_if_nan(self):
        data = otp.Ticks(X=[otp.nan, 1, 2, 3])
        data = data.agg({'RESULT': otp.agg.first('X', skip_tick_if=otp.nan)})
        df = otp.run(data)
        assert list(df['RESULT']) == [1]

    @pytest.mark.skipif(
        not is_last_ep_fwd_fill_if_supported(),
        reason="`skip_tick_if` (`FWD_FILL_IF`) parameter not supported in the LAST EP on this OneTick version",
    )
    def test_last_skip_tick_if_nan(self):
        data = otp.Ticks(X=[1, 2, 3, otp.nan])
        data = data.agg({'RESULT': otp.agg.last('X', skip_tick_if=otp.nan)})
        df = otp.run(data)
        assert list(df['RESULT']) == [3]


class TestExpAverageAggregations:
    def test_w_base(self):
        data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
        data = data.agg({'A': otp.agg.exp_w_average('A', decay=2)}, bucket_interval=2, bucket_units='ticks')
        df = otp.run(data)
        exp = [1.880797, 2.984124, 3.880797]
        res = list(df['A'])
        assert all([pytest.approx(res[i]) == exp[i] for i in range(len(res))])

    def test_w_decay_value_type_hl(self):
        data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
        data = data.agg({
            'A': otp.agg.exp_w_average('A', decay=2, decay_value_type='half_life_index'),
        }, bucket_interval=2, bucket_units='ticks')
        df = otp.run(data)
        exp = [1.585786, 2.773459, 3.585786]
        res = list(df['A'])
        assert all([pytest.approx(res[i]) == exp[i] for i in range(len(res))])

    def test_w_time_series_type_event_ts(self):
        data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
        data = data.agg({
            'A': otp.agg.exp_w_average('A', decay=2, time_series_type='event_ts'),
        }, bucket_interval=2, bucket_units='ticks')
        df = otp.run(data)
        exp = [1.880797, 3.0, 4.0]
        res = list(df['A'])
        assert all([pytest.approx(res[i]) == exp[i] for i in range(len(res))])

    def test_w_exceptions(self):
        data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})

        with pytest.raises(ValueError):
            _ = data.agg({
                'A': otp.agg.exp_w_average('A', decay=2, decay_value_type='test'),
            }, bucket_interval=2, bucket_units='ticks')

        with pytest.raises(ValueError):
            _ = data.agg({
                'A': otp.agg.exp_w_average('A', decay=2, time_series_type='test'),
            }, bucket_interval=2, bucket_units='ticks')

    def test_tw_base(self):
        data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
        data = data.agg({'A': otp.agg.exp_tw_average('A', decay=2)}, bucket_interval=2, bucket_units='ticks')
        df = otp.run(data)
        exp = [1.0, 2.500087, 4.0]
        res = list(df['A'])
        assert all([pytest.approx(res[i]) == exp[i] for i in range(len(res))])

    def test_tw_decay_value_type_lambda(self):
        data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
        data = data.agg({
            'A': otp.agg.exp_tw_average('A', decay=2, decay_value_type='lambda'),
        }, bucket_interval=2, bucket_units='ticks')
        df = otp.run(data)
        exp = [1.0, 2.5005, 4.0]
        res = list(df['A'])
        assert all([pytest.approx(res[i]) == exp[i] for i in range(len(res))])

    def test_tw_exceptions(self):
        with pytest.raises(ValueError):
            data = otp.Ticks({'A': [1.0, 2.0, 3.0, 3.0, 4.0]})
            _ = data.agg({
                'A': otp.agg.exp_tw_average('A', decay=2, decay_value_type='test'),
            }, bucket_interval=2, bucket_units='ticks')


@pytest.mark.skipif(
    not is_standardized_moment_supported(), reason='StandardizedMoment is not available on older builds',
)
class TestStandardizedMoment:
    def test_simple(self):
        data = otp.Ticks({'A': [1, 2, 4, 4, 4, 6]})
        data = data.agg({'A': otp.agg.standardized_moment('A')}, bucket_interval=3, bucket_units='ticks')
        df = otp.run(data)
        assert list(df['A']) == [pytest.approx(0.381802), pytest.approx(0.707107)]

    def test_degree(self):
        data = otp.Ticks({'A': [1, 2, 4, 4, 4, 6]})
        data = data.agg({'A': otp.agg.standardized_moment('A', degree=5)}, bucket_interval=3, bucket_units='ticks')
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
        data = otp.Ticks({'PRICE': [12.5, 10, 6.5, 12]})
        data = data.agg({'RESULT': otp.agg.portfolio_price()}, bucket_interval=2, bucket_units='ticks')
        df = otp.run(data)
        assert list(df['RESULT.VALUE']) == [10, 12]
        assert list(df['RESULT.NUM_SYMBOLS']) == [1, 1]

    def test_custom_input_column(self):
        data = otp.Ticks({'TEST': [12.5, 10, 6.5, 12]})
        data = data.agg({'RESULT': otp.agg.portfolio_price(data['TEST'])}, bucket_interval=2, bucket_units='ticks')
        df = otp.run(data)
        assert list(df['RESULT.VALUE']) == [10, 12]
        assert list(df['RESULT.NUM_SYMBOLS']) == [1, 1]

    def test_simple_running(self):
        data = otp.Ticks({'PRICE': [12.5, 10, 6.5, 12]})
        data = data.agg({'RESULT': otp.agg.portfolio_price()}, bucket_interval=2, bucket_units='ticks', running=True)
        df = otp.run(data)
        assert list(df['RESULT.VALUE']) == [12.5, 10, 6.5, 12]

    def test_column_as_weight_field_name(self):
        data = otp.Ticks({'A': [1, 2, 2, 1], 'PRICE': [12.5, 10, 6.5, 12]})
        data = data.agg(
            {'RESULT': otp.agg.portfolio_price(weight_field_name=data['A'])},
            bucket_interval=2, bucket_units='ticks', running=True,
        )
        df = otp.run(data)
        assert list(df['RESULT.VALUE']) == [12.5, 20.0, 13.0, 12.0]

    def test_exceptions(self):
        with pytest.raises(ValueError):
            _ = otp.agg.portfolio_price(weight_type='test')

        with pytest.raises(ValueError):
            _ = otp.agg.portfolio_price(side='test')

        data = otp.Ticks(B=[1, 2, 3], PRICE=[1, 1, 1])

        with pytest.raises(TypeError):
            _ = data.agg({'RESULT': otp.agg.portfolio_price(weight_field_name='A')})

        with pytest.raises(ValueError):
            data = data.agg({'RESULT': otp.agg.portfolio_price(symbols=['S1', 'S2'])})
            otp.run(data)

    @pytest.mark.parametrize('weights,side,match', [
        ([1, 2, 3], 'long', 'SIDE must be set BOTH'),
        ([1, 2, 3], 'short', 'SIDE must be set BOTH'),
        ([-1, 2, 3], 'both', 'Detected negative value of weight'),
    ])
    def test_relative_exceptions(self, weights, side, match):
        data = otp.Ticks({'A': weights, 'PRICE': [1, 2, 3]})
        data = data.agg({'RESULT': otp.agg.portfolio_price(weight_field_name='A', side=side, weight_type='relative')})

        with pytest.raises(Exception, match=match):
            _ = otp.run(data)


class TestMultiPortfolioPrice:
    def test_base(self):
        data = otp.DataSource(db='DEMO_L1', tick_type='TRD', date=otp.datetime(2003, 12, 1))
        with pytest.raises(ValueError):
            _ = data.agg({'RESULT': otp.agg.multi_portfolio_price(portfolios_query='some_path.otq')})


class TestReturnEP:
    def test_base(self):
        data = otp.Ticks(X=[2, 3, 4, 5, 6, 10])
        data = data.agg({'X': otp.agg.return_ep(data['X'])}, bucket_interval=3, bucket_units='ticks')
        df = otp.run(data)
        assert list(df['X']) == [2, 2.5]

    def test_running(self):
        data = otp.Ticks(X=[2, 3, 4, 5, 6, 10], offset=[i * 1000 for i in range(6)])
        data = data.agg({
            'X': otp.agg.return_ep('X'),
        }, bucket_interval=otp.Second(3), running=True)
        df = otp.run(data, start=otp.datetime(2023, 12, 1), end=otp.datetime(2023, 12, 1) + otp.Second(5))
        assert list(df['X']) == [1.0, 1.5, 2.0, 2.5, 2.0, 2.5]


class TestMultiColumnEPs:
    @pytest.fixture
    def data(self):
        return otp.Ticks(A=[1, 2, 3, 4, 5], B=[3, 4, 6, 5, 4])

    @pytest.mark.parametrize('running,bucket_interval,result', [
        (False, 0, [[15], [1], [3]]),
        (True, 0, [[1, 3, 6, 10, 15], [1, 1, 1, 1, 1], [3, 3, 3, 3, 3]]),
        (False, 2, [[3, 7, 5], [1, 3, 5], [3, 6, 4]]),
        (True, 2, [[1, 3, 5, 7, 9], [1, 1, 2, 3, 4], [3, 3, 4, 6, 5]]),
    ])
    def test_base(self, data, running, bucket_interval, result):
        data = data.agg(
            {'C': otp.agg.sum('A'), 'D': otp.agg.first_tick()},
            bucket_interval=bucket_interval,
            bucket_units='ticks' if bucket_interval else None,
            running=running,
        )
        assert data.schema == {'C': int, 'D.A': int, 'D.B': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'C', 'D.A', 'D.B'}
        actual_result = df.to_dict(orient='list')
        del actual_result['Time']
        assert actual_result == {'C': result[0], 'D.A': result[1], 'D.B': result[2]}

    def test_multiple_aggregations(self, data):
        data = data.agg({'C': otp.agg.sum('A'), 'D': otp.agg.first_tick(), 'E': otp.agg.last_tick()})
        assert data.schema == {'C': int, 'D.A': int, 'D.B': int, 'E.A': int, 'E.B': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'C', 'D.A', 'D.B', 'E.A', 'E.B'}
        res = df.to_dict(orient='list')
        del res['Time']
        assert res == {'C': [15], 'D.A': [1], 'D.B': [3], 'E.A': [5], 'E.B': [4]}

    @pytest.mark.parametrize('all_fields,result', [
        ('first', [15, 1, 3, 5, 4]),
        ('last', [15, 5, 4, 5, 4]),
        ('high', [15, 3, 6, 5, 4]),
        ('low', [15, 1, 3, 5, 4]),
    ])
    def test_all_fields_str(self, data, all_fields, result):
        data['PRICE'] = data['B']
        data.drop('B', inplace=True)

        data = data.agg({'C': otp.agg.sum('A'), 'D': otp.agg.last_tick()}, all_fields=all_fields)
        assert data.schema == {'A': int, 'C': int, 'D.A': int, 'D.PRICE': int, 'PRICE': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'A', 'PRICE', 'C', 'D.A', 'D.PRICE'}
        actual_result = df.to_dict(orient='list')
        del actual_result['Time']
        assert actual_result == {
            'C': [result[0]], 'A': [result[1]], 'PRICE': [result[2]], 'D.A': [result[3]], 'D.PRICE': [result[4]],
        }

    def test_all_fields_bool(self, data):
        data = data.agg({'C': otp.agg.sum('A'), 'D': otp.agg.last_tick()}, all_fields=True)
        assert data.schema == {'C': int, 'D.A': int, 'D.B': int, 'A': int, 'B': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'A', 'B', 'C', 'D.A', 'D.B'}
        res = df.to_dict(orient='list')
        del res['Time']
        assert res == {'A': [1], 'B': [3], 'C': [15], 'D.A': [5], 'D.B': [4]}

    @pytest.mark.parametrize('agg_type,result', [('low', [15, 1, 3, 5, 4]), ('high', [15, 3, 6, 5, 4])])
    def test_all_fields_high_low_as_agg(self, data, agg_type, result):
        if agg_type == 'low':
            agg = otp.agg.low_tick(column='B')
        else:
            agg = otp.agg.high_tick(column='B')

        data = data.agg({'C': otp.agg.sum('A'), 'D': otp.agg.last_tick()}, all_fields=agg)
        assert data.schema == {'A': int, 'B': int, 'C': int, 'D.A': int, 'D.B': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'A', 'B', 'C', 'D.A', 'D.B'}
        actual_result = df.to_dict(orient='list')
        del actual_result['Time']
        assert actual_result == {
            'C': [result[0]], 'A': [result[1]], 'B': [result[2]], 'D.A': [result[3]], 'D.B': [result[4]],
        }

    def test_multi_column(self, data):
        data = data.agg({'C': otp.agg.sum('A'), 'D': otp.agg.portfolio_price('B')})
        assert data.schema == {'C': int, 'D.VALUE': float, 'D.NUM_SYMBOLS': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'C', 'D.VALUE', 'D.NUM_SYMBOLS'}
        res = df.to_dict(orient='list')
        del res['Time']
        assert res == {'C': [15], 'D.VALUE': [4.0], 'D.NUM_SYMBOLS': [1]}

    def test_all_fields_str_override(self, data):
        def parse_params(s):
            raw_params = re.match(r'^LOW_TICK\((.*)\)$', s).group(1).split(',')
            result = [list(map(str.strip, sub.split('='))) for sub in raw_params]
            return dict(result)

        agg = otp.agg.low_tick(column='TEST')
        compute = otp.agg.compute(all_fields=agg)
        str_with_override = compute.ep_params['COMPUTE']
        params_with_override = parse_params(str_with_override)
        del params_with_override['KEEP_INITIAL_SCHEMA']

        params_original = parse_params(str(agg))
        assert params_original == params_with_override

    def test_empty(self, data):
        data = data.agg({}, all_fields=True)
        assert data.schema == {'A': int, 'B': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'A', 'B'}
        res = df.to_dict(orient='list')
        del res['Time']
        assert res == {'A': [1], 'B': [3]}

    def test_generic_simple(self, data):
        def agg_fun(source):
            return source.agg({'T': otp.agg.count()})

        data = data.agg({'X': otp.agg.generic(agg_fun)})
        assert set(data.schema.keys()) == {'X.T'}
        df = otp.run(data).to_dict(orient='list')
        assert set(df.keys()) == {'Time', 'X.T'}
        del df['Time']
        assert df == {'X.T': [5]}

    @pytest.mark.skipif(
        not is_multi_column_generic_aggregations_supported(),
        reason='Not supported on this version of OneTick',
    )
    def test_generic_all_columns(self, data):
        def agg_fun(source):
            return source.agg({'T': otp.agg.high_tick('A', 1), 'S': otp.agg.sum('A')})

        data = data.agg({'X': otp.agg.generic(agg_fun)})
        assert set(data.schema.keys()) == {'X.T.A', 'X.T.B', 'X.S'}
        df = otp.run(data).to_dict(orient='list')
        assert set(df.keys()) == {'Time', 'X.T.A', 'X.T.B', 'X.S'}
        del df['Time']
        assert df == {'X.T.A': [5], 'X.T.B': [4], 'X.S': [15]}

    def test_generic_with_other_aggs(self, data):
        def agg_fun(source):
            return source.agg({'E': otp.agg.count(), 'F': otp.agg.sum('A')})

        data = data.agg({'C': otp.agg.sum('A'), 'D': otp.agg.first_tick(), 'X': otp.agg.generic(agg_fun)})
        assert set(data.schema.keys()) == {'C', 'D.A', 'D.B', 'X.E', 'X.F'}
        df = otp.run(data).to_dict(orient='list')
        del df['Time']
        assert df == {'C': [15], 'D.A': [1], 'D.B': [3], 'X.E': [5], 'X.F': [15]}


@pytest.mark.skipif(
    not getattr(otq, 'LinearRegression', False), reason="LinearRegression is not supported on cureent OneTick version",
)
class TestLinearRegression:
    def test_simple(self):
        data = otp.Ticks(X=[1, 2, 2, 1], Y=[5, 4, 3, 4])
        data = data.agg({
            'A': otp.agg.linear_regression(
                dependent_variable_field_name='Y', independent_variable_field_name=data['X'],
            )
        })
        assert data.schema == {'A.SLOPE': float, 'A.INTERCEPT': float}
        df = otp.run(data).to_dict(orient='list')
        assert set(df.keys()) == {'Time', 'A.SLOPE', 'A.INTERCEPT'}
        assert df['A.SLOPE'] == [-1.0]
        assert df['A.INTERCEPT'] == [5.5]
