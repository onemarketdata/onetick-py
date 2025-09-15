from onetick.py.core.column import _Column
from onetick.py.aggregations._base import _Aggregation, _AggregationTSType, _AggregationTSSelection
import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.compatibility import is_all_fields_when_ticks_exit_window_supported

import pytest


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    return m_session


class DummyAgg(_Aggregation):
    NAME = 'DUMMY_AGG'
    EP = otq.Sum


class TestBaseClass:

    def test_str(self):
        a, b = _Column('A'), _Column('B')
        assert str(DummyAgg('A')) == 'DUMMY_AGG(INPUT_FIELD_NAME=A)'
        assert str(DummyAgg('A', running=True)) == 'DUMMY_AGG(INPUT_FIELD_NAME=A,IS_RUNNING_AGGR=true)'
        assert str(DummyAgg('A', all_fields=True, running=True)) == 'DUMMY_AGG(INPUT_FIELD_NAME=A,' \
                                                                    'IS_RUNNING_AGGR=true,ALL_FIELDS_FOR_SLIDING=true)'
        assert str(DummyAgg('A', bucket_interval=10)) == 'DUMMY_AGG(INPUT_FIELD_NAME=A,BUCKET_INTERVAL=10)'
        assert str(DummyAgg('A', bucket_time='start')) == 'DUMMY_AGG(INPUT_FIELD_NAME=A,BUCKET_TIME=BUCKET_START)'
        assert str(DummyAgg('A', bucket_units='days')) == 'DUMMY_AGG(INPUT_FIELD_NAME=A,BUCKET_INTERVAL_UNITS=days)'
        assert str(DummyAgg('A', end_condition_per_group=True)) == 'DUMMY_AGG(INPUT_FIELD_NAME=A,' \
                                                                   'BUCKET_END_PER_GROUP=true)'
        assert str(DummyAgg('A', boundary_tick_bucket="previous")) == 'DUMMY_AGG(INPUT_FIELD_NAME=A,' \
                                                                      'BOUNDARY_TICK_BUCKET=previous)'
        assert str(DummyAgg('A', group_by=['A', 'B'])) == "DUMMY_AGG(INPUT_FIELD_NAME=A,GROUP_BY=A,B)"
        assert str(DummyAgg('A', group_by=[a, b])) == "DUMMY_AGG(INPUT_FIELD_NAME=A,GROUP_BY=A,B)"

    def test_apply(self):
        data = otp.Ticks({'A': [1, 2, 3], 'B': [4, 5, 6]})
        data1 = DummyAgg('A').apply(data)
        data2 = DummyAgg(data['A']).apply(data)
        for data in [data1, data2]:
            assert data.schema == {'A': int}
            df = otp.run(data)
            assert set(df.columns) == {'A', 'Time'}
            assert len(df) == 1
            assert df['A'][0] == 6
            assert len(df.columns) == 2

    def test_apply_float(self):
        data = otp.Ticks({'A': [1., 2, 3]})
        data = DummyAgg('A').apply(data)
        assert data.schema == {'A': float}

    @pytest.mark.parametrize('gb_as_str', [True, False])
    def test_apply_gb(self, gb_as_str):
        data = otp.Ticks({'A': [1, 2, 3, 4], 'B': [1, 1, 2, 2], 'C': ['a', 'b', 'c', 'c'], 'D': range(4)})
        if gb_as_str:
            data = DummyAgg('A', group_by=['B', 'C']).apply(data)
        else:
            data = DummyAgg('A', group_by=[data['B'], data['C']]).apply(data)
        df = otp.run(data)
        assert data.schema == {'A': int, 'B': int, 'C': str}
        assert set(df.columns) == {'Time', 'A', 'B', 'C'}
        assert len(df) == 3
        assert df.shape == (3, 4)
        df = df.set_index(['B', 'C'])
        assert df['A'][1]['a'] == 1
        assert df['A'][1]['b'] == 2
        assert df['A'][2]['c'] == 7

    def test_operation_gb_1(self):
        data = otp.Ticks({'A': [1, 2, 3], 'GB1': 'asd', 'GB2': 'qwe', 'GB3': 'zxc'})
        agg = DummyAgg('A', group_by=[data['GB1'] + data['GB2'], data['GB1'] + data['GB3']])
        data = agg.apply(data)
        assert data.schema == {'A': int, 'GROUP_0': str, 'GROUP_1': str}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'A', 'GROUP_0', 'GROUP_1'}
        assert len(df) == 3

    def test_operation_gb_2(self):
        data = otp.Ticks({'A': [1, 2, 3], 'GB1': 'aad', 'GB2': 'aae'})
        agg = DummyAgg('A', group_by=[data['GB1'] + data['GB2']])
        data = agg.apply(data)
        assert data.schema == {'A': int, 'GROUP_0': str}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'A', 'GROUP_0'}
        assert len(df) == 2

    def test_operation_gb_3(self):
        data = otp.Ticks({'A': [1, 2, 3], 'GB1': 'aad', 'GB2': 'aae', 'GB': 'aaa'})
        agg = DummyAgg('A', group_by=[data['GB1'] + data['GB2'], data['GB']])
        data = agg.apply(data)
        assert data.schema == {'A': int, 'GB': str, 'GROUP_0': str}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'A', 'GB', 'GROUP_0'}
        assert len(df) == 2

    def test_missing_gb(self):
        data = otp.Ticks({'A': [1, 2, 3, 4], 'B': [1, 1, 2, 2], 'C': ['a', 'b', 'c', 'c'], 'D': range(4)})
        agg = DummyAgg('A', group_by=['Z'])
        with pytest.raises(KeyError) as e:
            agg.apply(data)
        assert "There is no 'Z' column to group by" in str(e)

    def test_running(self):
        data = otp.Ticks({'A': [1, 2, 3, 4], 'D': range(4)})
        data = DummyAgg('A', running=True).apply(data, 'ASD')
        df = otp.run(data)
        assert data.schema == {'ASD': int}
        assert df.shape == (4, 2)
        assert set(df.columns) == {'ASD', 'Time'}
        assert list(df['ASD']) == [1, 3, 6, 10]

    def test_all_fields(self):
        data = otp.Ticks({'A': [1, 2, 3, 4], 'D': range(4)})
        agg = DummyAgg('A', all_fields=True, running=True)
        data = agg.apply(data, 'B')
        assert data.schema == {'A': int, 'D': int, 'B': int}
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'A', 'B', 'D'}
        assert df.shape == (4, 4)
        assert list(df['B']) == [1, 3, 6, 10]

    def test_all_fields_error(self):
        with pytest.raises(ValueError, match='It is not allowed set all_fields to True for not running aggregation'):
            DummyAgg('A', all_fields=True)

    @pytest.mark.skipif(
        not is_all_fields_when_ticks_exit_window_supported(),
        reason="when_ticks_exit_window in `all_fields` not supported on this OneTick version",
    )
    def test_all_fields_when_ticks_exit_window(self):
        ticks = otp.Ticks({'A': [1, 2, 3, 4], 'D': range(4)})
        agg = DummyAgg('A', all_fields='when_ticks_exit_window', running=True, bucket_interval=2, bucket_units='ticks')
        data = agg.apply(ticks, 'B')
        assert data.schema == {'A': int, 'D': int, 'B': int}

        df = otp.run(data)
        assert set(df.columns) == {'Time', 'A', 'B', 'D'}
        assert df.shape == (3, 4)
        assert list(df['B']) == [3, 5, 7]

    @pytest.mark.skipif(
        not is_all_fields_when_ticks_exit_window_supported(),
        reason="when_ticks_exit_window in `all_fields` not supported on this OneTick version",
    )
    def test_all_fields_when_ticks_exit_window_errors(self):
        with pytest.raises(ValueError, match='running'):
            _ = DummyAgg(
                'A', all_fields='when_ticks_exit_window', running=False, bucket_interval=2, bucket_units='ticks'
            )

        with pytest.raises(ValueError, match='bucket_interval'):
            _ = DummyAgg('A', all_fields='when_ticks_exit_window', running=True, bucket_interval=0)

    def test_running_gb(self):
        data = otp.Ticks({'A': [1, 2, 3, 4], 'C': [1, 2, 1, 2], 'D': range(4)})
        data = DummyAgg('A', running=True, group_by=[data['C']]).apply(data, 'ASD')
        df = otp.run(data)
        assert data.schema == {'C': int, 'ASD': int}
        assert list(df['ASD']) == [1, 2, 4, 6]
        assert set(df.columns) == {'Time', 'C', 'ASD'}
        assert df.shape == (4, 3)

    def test_bucket_time(self):
        data = otp.Ticks({'A': [1, 2, 3, 4]})
        data = DummyAgg('A', bucket_time='start').apply(data)
        df = otp.run(data)
        assert data.schema == {'A': int}
        assert set(df.columns) == {'Time', 'A'}
        assert df['Time'][0] == otp.config['default_start_time']

    def test_sliding_window_1(self):
        data = otp.Ticks({'A': [1, 2, 3, 4],
                          'C': range(4),
                          'offset': [0, 1, 10002, 10003]})
        data = DummyAgg('A', bucket_interval=10, running=True).apply(data)
        df = otp.run(data)
        assert df.shape == (8, 2)
        assert data.schema == {'A': int}
        assert set(df.columns) == {'Time', 'A'}
        assert list(df['A']) == [1, 3, 2, 0, 3, 7, 4, 0]
        for i, offset in enumerate([0, 1, 10000, 10001, 10002, 10003]):
            assert df['Time'][i] == otp.config['default_start_time'] + otp.Milli(offset)

    def test_sliding_window_2(self):
        data = otp.Ticks({'A': [1, 2, 3, 4],
                          'C': range(4),
                          'offset': [0, 1, 10002, 10003]})
        data = DummyAgg('A',
                        bucket_interval=10,
                        running=True,
                        all_fields=True).apply(data, name='ASD')
        df = otp.run(data)
        assert df.shape == (4, 4)
        assert data.schema == {'A': int, 'C': int, 'ASD': int}
        assert set(df.columns) == {'Time', 'A', 'C', 'ASD'}
        assert list(df['ASD']) == [1, 3, 3, 7]
        for i, offset in enumerate([0, 1, 10002, 10003]):
            assert df['Time'][i] == otp.config['default_start_time'] + otp.Milli(offset)

    def test_sliding_window_corner_case(self):
        data = otp.Ticks({'A': [2, 3],
                          'C': range(2),
                          'offset': [1, 10001]})
        data = DummyAgg('A', bucket_interval=10, running=True).apply(data)
        df = otp.run(data)
        assert data.schema == {'A': int}
        assert set(df.columns) == {'Time', 'A'}
        assert df.shape == (3, 2)
        assert list(df['A']) == [2, 3, 0]

    @pytest.mark.parametrize('interval,result,indexes', [
        (otp.Second(10), [6, 9, 13], [0, 1, 6]),
        (otp.Minute(1), [15, 13], [0, 1]),
    ])
    def test_bucket_interval_date_part(self, interval, result, indexes):
        data = otp.Ticks({'A': [1, 2, 3, 4, 5, 6, 7], 'offset': [
            0, otp.Second(1), otp.Second(2), otp.Second(10), otp.Second(11), otp.Second(61), otp.Second(62)
        ]})
        data = DummyAgg('A', bucket_interval=interval).apply(data)
        df = otp.run(data)

        df_array = df['A'].array
        test_result = [df_array[i] for i in indexes]
        assert result == test_result

    def test_bucket_interval_milli(self):
        data = otp.Ticks({'A': [1, 2, 3, 4, 5, 6, 7]})
        data = DummyAgg('A', bucket_interval=otp.Milli(2)).apply(data)
        df = otp.run(data, start=otp.config.default_start_time, end=otp.config.default_start_time + otp.Milli(10))
        assert list(df['A']) == [3, 7, 11, 7, 0]

    def test_bucket_interval_float(self):
        data = otp.Ticks({'A': [1, 2, 3, 4, 5, 6, 7]})
        with pytest.raises(ValueError, match='Float values for bucket_interval are only supported for seconds.'):
            _ = DummyAgg('A', bucket_interval=0.002, bucket_units='ticks').apply(data)
        with pytest.raises(ValueError,
                           match="Float values for bucket_interval less than 0.001 are not supported."):
            _ = DummyAgg('A', bucket_interval=0.00002).apply(data)
        data = DummyAgg('A', bucket_interval=0.002).apply(data)
        df = otp.run(data, start=otp.config.default_start_time, end=otp.config.default_start_time + otp.Milli(10))
        assert list(df['A']) == [3, 7, 11, 7, 0]

    def test_bucket_interval_date_part_unsupported(self):
        data = otp.Ticks({'A': [1, 2, 3], 'offset': [0, 1, 2]})
        with pytest.raises(ValueError, match="Unsupported DatePart passed to bucket_interval"):
            data = DummyAgg('A', bucket_interval=otp.Year(1)).apply(data)

    def test_bucket_interval_date_part_negative(self):
        data = otp.Ticks({'A': [1, 2, 3], 'offset': [0, 1, 2]})
        with pytest.raises(ValueError, match="Negative DateParts aren't allowed for bucket_interval"):
            data = DummyAgg('A', bucket_interval=otp.Second(-10)).apply(data)

    def test_bucket_interval_date_part_data_expression(self):
        data = otp.Ticks({'A': [1, 2, 3], 'B': [3, 1, 2], 'offset': [0, 1, 2]})
        with pytest.raises(ValueError, match='Operation'):
            data = DummyAgg('A', bucket_interval=otp.Second(data['A'] - data['B'])).apply(data)

    def test_bucket_interval_as_end_condition(self):
        data = otp.Ticks({'A': [1, 2, 3, 4, 5, 6, 7]})
        data = DummyAgg('A', bucket_interval=data['A'] > 4).apply(data)
        df = otp.run(data)
        assert df["A"].to_list() == [10, 5, 6, 7]

        with pytest.raises(ValueError, match="bucket_end_condition can be used only with 'flexible' bucket_units"):
            DummyAgg('A', bucket_interval=data['A'] > 4, bucket_units="seconds").apply(data)

        with pytest.raises(ValueError, match="Bucket end condition passed"):
            DummyAgg('A', bucket_interval=data['A'] > 4, bucket_end_condition=data['A'] < 4).apply(data)

        with pytest.raises(ValueError, match="Bucket interval can only be boolean otp.Operation or"):
            DummyAgg('A', bucket_interval=data['A'] + 4).apply(data)

    def test_bucket_interval_as_symbol_parameter(self):
        data = otp.Ticks({'A': [1, 2, 3, 4, 5, 6, 7]})
        data = DummyAgg('A', bucket_interval=data.Symbol['X', int], bucket_units='ticks').apply(data)
        df = otp.run(data, symbols=otq.Symbol(f'{otp.config.default_db}::', params={'X': 3}))
        assert df["A"].to_list() == [6, 15, 7]

    def test_bucket_unints(self):
        data = otp.Ticks({'A': [1, 2, 3, 4], 'offset': [0, 1, 2, 1000]})
        data = DummyAgg('A', bucket_units='ticks', bucket_interval=2).apply(data)
        df = otp.run(data)
        assert data.schema == {'A': int}
        assert set(df.columns) == {'Time', 'A'}
        assert df.shape == (2, 2)
        assert list(df['A']) == [3, 7]

    @pytest.mark.parametrize('all_fields', [True, False])
    def test_bucket_units_sliding(self, all_fields):
        data = otp.Ticks({'A': [1, 2, 3, 4], 'offset': [0, 1, 2, 1000], 'C': range(4)})
        data = DummyAgg('A',
                        bucket_units='ticks',
                        bucket_interval=2,
                        running=True,
                        all_fields=all_fields).apply(data, name='ASD')
        df = otp.run(data)
        schema = {'ASD': int}
        if all_fields:
            schema['A'] = int
            schema['C'] = int
        assert data.schema == schema
        columns = {'Time'}
        columns.update(set(schema.keys()))
        assert set(df.columns) == columns
        assert df.shape == (4, 4) if all_fields else (4, 2)
        assert list(df['ASD']) == [1, 3, 5, 7]

    @pytest.mark.parametrize('end_condition_per_group', [True, False])
    def test_bucket_condition(self, end_condition_per_group):
        data = otp.Ticks({'A': [1, 2, 3, 4], 'offset': [0, 1, 2, 1000], 'FLAG': [1, 1, 1, 2]})
        data = DummyAgg('A',
                        end_condition_per_group=end_condition_per_group,    # has no effect if group_by not set
                        bucket_units='flexible',
                        bucket_end_condition=(data['FLAG'] != data['FLAG'][-1]) & (data['FLAG'][-1] != 0)).apply(data)
        df = otp.run(data)
        assert data.schema == {'A': int}
        assert set(df.columns) == {'Time', 'A'}
        assert df.shape == (2, 2)
        assert list(df['A']) == [6, 4]

    def test_bucket_condition_without_units(self):
        data = otp.Ticks({'A': [1, 2, 3, 4], 'FLAG': [1, 1, 1, 2]})
        data = DummyAgg(
            'A', bucket_end_condition=(data['FLAG'] != data['FLAG'][-1]) & (data['FLAG'][-1] != 0)
        ).apply(data)
        df = otp.run(data)
        assert data.schema == {'A': int}
        assert set(df.columns) == {'Time', 'A'}
        assert list(df['A']) == [6, 4]

    @pytest.mark.parametrize('end_condition_per_group', [True, False])
    def test_bucket_condition_gb(self, end_condition_per_group):
        data = otp.Ticks({'A': [1, 2, 3, 4],
                          'offset': [0, 1, 2, 1000],
                          'FLAG': [1, 1, 1, 2],
                          'GB': [1, 2, 1, 2]})
        data = DummyAgg('A',
                        end_condition_per_group=end_condition_per_group,
                        bucket_units='flexible',
                        bucket_end_condition=(data['FLAG'] != data['FLAG'][-1]) & (data['FLAG'][-1] != 0),
                        group_by=[data['GB']]).apply(data)
        df = otp.run(data)
        assert data.schema == {'A': int, 'GB': int}
        assert set(df.columns) == {'Time', 'A', 'GB'}
        assert df.shape == (3, 3) if end_condition_per_group else (4, 3)
        df = df.set_index(['GB'])
        if end_condition_per_group:
            assert df['A'][1] == 4
        else:
            assert list(df['A'][1]) == [4, 0]
        assert list(df['A'][2]) == [2, 4] if end_condition_per_group else [0, 4]

    def test_boundary_tick_bucket(self):
        data = otp.Ticks({'A': [1, 2, 3, 4], 'offset': [0, 1, 2, 1000], 'FLAG': [1, 1, 1, 2]})
        data = DummyAgg('A',
                        bucket_units='flexible',
                        bucket_end_condition=(data['FLAG'] != data['FLAG'][-1]) & (data['FLAG'][-1] != 0),
                        boundary_tick_bucket="previous").apply(data)
        df = otp.run(data)
        assert data.schema == {'A': int}
        assert set(df.columns) == {'Time', 'A'}
        assert df.shape == (2, 2)
        assert list(df['A']) == [10, 0]

    def test_param_validation(self):
        with pytest.raises(TypeError):
            DummyAgg()      # pylint: disable=E1120

        with pytest.raises(ValueError) as e:
            DummyAgg('A', bucket_time='zxc')
        assert "'start' or 'end'" in str(e)

        with pytest.raises(ValueError) as e:
            DummyAgg('A', bucket_units='zxc')
        assert "'bucket_units' can be one of the following" in str(e)

        with pytest.raises(ValueError) as e:
            DummyAgg('A', boundary_tick_bucket='zxc')
        assert "'boundary_tick_bucket' can be one of the following" in str(e)

        with pytest.raises(ValueError) as e:
            DummyAgg('A', running=True, bucket_time='start')
        assert "It is not allowed to set up running=True and bucket_time='start'" in str(e)

        with pytest.raises(ValueError) as e:
            DummyAgg('A', bucket_units="flexible")
        assert "bucket_units is set to 'flexible' but bucket_end_condition is not specified" in str(e)

        data = otp.Ticks({'B': [1]})
        agg = DummyAgg('A')
        with pytest.raises(TypeError, match='uses column'):
            agg.apply(data)

    @pytest.mark.parametrize('value', [1, 1.0, otp.config['default_start_time'], 'a'])
    def test_output_type_flexible(self, value):
        """testing aggregation with self.output_type = None"""
        data = otp.Ticks({'A': [1]})
        agg = DummyAgg('A')
        data1 = agg.apply(data)
        assert data1.schema['A'] == data.schema['A']

    @pytest.mark.parametrize('value', [1, 1.0, otp.config['default_start_time'], 'a'])
    def test_output_type_strong(self, value):
        """testing aggregation with self.output_type != None"""

        class Agg(_Aggregation):
            EP = otq.Sum
            NAME = 'DUMMY'

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.output_field_type = float

        data = otp.Ticks({'A': [1]})
        agg = Agg('A')
        data = agg.apply(data)
        assert data.schema['A'] == float

    @pytest.mark.parametrize('value', [1, 1.0, otp.config['default_start_time'], 'a'])
    def test_request_type(self, value):
        """testing aggregation with self.output_type != None"""

        class Agg(_Aggregation):
            EP = otq.Sum
            NAME = 'DUMMY'

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.require_type = (float,)

        data = otp.Ticks({'A': [value]})
        agg = Agg('A')
        if isinstance(value, float):
            agg.apply(data)     # not raising error
        else:
            with pytest.raises(TypeError, match="require"):
                agg.apply(data)

    def test_flexible_schema(self):
        """Test checks that if field not in schema it won't be dropped"""
        t = otp.Ticks({'A': [1, 2, 3], 'B': [4, 5, 6]})
        t.sink(otq.AddField(field='NOT_IN_SCHEMA', value=1))
        schema_before_agg = t.schema.copy()
        agg = DummyAgg('A', all_fields=True, running=True)
        t = agg.apply(t, name='C')
        schema_before_agg['C'] = int
        assert t.schema == schema_before_agg
        assert 'NOT_IN_SCHEMA' not in schema_before_agg
        df = otp.run(t)
        assert 'NOT_IN_SCHEMA' in df

    @pytest.mark.parametrize('inplace', (False, True))
    def test_apply_inplace(self, inplace):
        t = otp.Ticks({'A': [1, 2, 3]})
        res = DummyAgg('A').apply(t, inplace=inplace)
        assert (res is t) == inplace
        df = otp.run(t)
        if inplace:
            assert list(df['A']) == [6]
        else:
            assert list(df['A']) == [1, 2, 3]
        df = otp.run(res)
        assert list(df['A']) == [6]

    @pytest.mark.parametrize('overwrite_output_field', (False, True))
    def test_overwrite_output_field(self, overwrite_output_field):
        t = otp.Ticks({'A': [1, 2, 3], 'B': [1, 2, 3]})
        if not overwrite_output_field:
            with pytest.raises(ValueError):
                t = DummyAgg('A',
                             running=True,
                             all_fields=True,
                             overwrite_output_field=overwrite_output_field).apply(t)
            return
        t = DummyAgg('A',
                     running=True,
                     all_fields=True,
                     overwrite_output_field=overwrite_output_field).apply(t)
        df = otp.run(t)
        assert list(df['A']) == [1, 3, 6]
        assert list(df['B']) == [1, 2, 3]


class TestTsTypeAgg:

    @pytest.fixture()
    def aggr(self):
        class Aggr(_AggregationTSType):
            NAME = 'SOME_AGGR'
            EP = otq.Sum
        return Aggr

    def test_str(self, aggr):
        assert str(aggr('A')) == f'{aggr.NAME}(INPUT_FIELD_NAME=A)'
        assert str(aggr('A', time_series_type='event_ts')) == f'{aggr.NAME}(INPUT_FIELD_NAME=A)'
        assert str(aggr('A', time_series_type='state_ts')) == f'{aggr.NAME}(INPUT_FIELD_NAME=A,' \
                                                              'TIME_SERIES_TYPE=state_ts)'
        with pytest.raises(ValueError) as e:
            aggr('A', time_series_type='asd')
        assert 'time_series_type argument must be "event_ts" or "state_ts"' in str(e)


class TestTsSelectionAgg:

    @pytest.fixture()
    def aggr(self):
        class Aggr(_AggregationTSSelection):
            NAME = 'SOME_AGGR'
            EP = otq.Sum

        return Aggr

    def test_str(self, aggr):
        assert str(aggr('A')) == f'{aggr.NAME}(INPUT_FIELD_NAME=A)'
        assert str(aggr('A', selection='first')) == f'{aggr.NAME}(INPUT_FIELD_NAME=A)'
        assert str(aggr('A', selection='last')) == f'{aggr.NAME}(INPUT_FIELD_NAME=A,SELECTION=last)'
        with pytest.raises(ValueError) as e:
            aggr('A', selection='asd')
        assert 'selection argument must be "first" or "last"' in str(e)
