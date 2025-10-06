# pylama:ignore=W0612
import pytest
import random
import pandas as pd
import onetick.py as otp

from onetick.py.core._internal._state_objects import TickList, TickSet, TickSetUnordered, TickDeque


zero_time = pd.Timestamp(0, tz=otp.config['tz']).replace(tzinfo=None)


class TestTickList:

    def test_dtype(self, session):
        assert otp.state.tick_list().dtype is TickList
        assert isinstance(otp.state.tick_list(), TickList)

    def test_creation(self, session):
        data = otp.Ticks(dict(X=[1, 2, 3]))
        data.state_vars['list'] = otp.state.tick_list()
        otp.run(data)

    def test_exception(self):
        data = otp.Tick(A=1)
        with pytest.raises(ValueError):
            data.state_vars['list'] = otp.state.tick_list(12345)

    def test_dump_inplace(self, session):
        def another_query():
            return otp.Ticks(A=[2, 3])

        data = otp.Tick(A=1)
        data.state_vars['list'] = otp.state.tick_list(otp.eval(another_query))
        data.state_vars['list'].dump(inplace=True)
        df = otp.run(data)
        assert list(df['A']) == [2, 3]

    def test_dump_copy(self, session):
        def another_query():
            return otp.Ticks(A=[2, 3])

        data = otp.Tick(A=1)
        data.state_vars['list'] = otp.state.tick_list(otp.eval(another_query))
        df = otp.run(data)
        data_copy = data.state_vars['list'].dump()
        copy_df = otp.run(data_copy)
        assert list(df['A']) == [1]
        assert list(copy_df['A']) == [2, 3]

    def test_dump_empty(self, session):
        data = otp.Tick(A=1)
        data.state_vars['list'] = otp.state.tick_list()
        data = data.state_vars['list'].dump()
        df = otp.run(data)
        assert df.empty

    def test_dump_first_tick(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2],
                'offset': [1, 2],
            })

        data = otp.Ticks({
            'A': [3, 4],
            'offset': [3, 4],
        })
        data.state_vars['list'] = otp.state.tick_list(otp.eval(another_query))
        data = data.state_vars['list'].dump(propagate_input_ticks=True, when_to_dump='FIRST_TICK')
        df = otp.run(data)
        assert list(df['A']) == [1, 2, 3, 4]

    def test_dump_before_tick(self, session):
        data = otp.Ticks({
            'A': [3, 4],
            'offset': [3, 4],
        })
        data.state_vars['list'] = otp.state.tick_list()
        data = data.state_vars['list'].dump(propagate_input_ticks=True, when_to_dump='BEFORE_TICK')
        df = otp.run(data)
        assert list(df['A']) == [3, 4]

    @pytest.mark.parametrize('propagate_input_ticks', (False, True))
    def test_dump_schema(self, session, propagate_input_ticks):
        def another_query():
            return otp.Tick(B=2)

        data = otp.Tick(A=1)
        data.state_vars['list'] = otp.state.tick_list(otp.eval(another_query))
        data = data.state_vars['list'].dump(propagate_input_ticks=propagate_input_ticks)
        df = otp.run(data)
        assert 'B' in data.schema
        if propagate_input_ticks:
            assert 'A' in data.schema
            assert 'A' in df
            assert list(df['B']) == [2, 0]
            assert list(df['A']) == [0, 1]
        else:
            assert 'A' not in data.schema
            assert 'A' not in df
            assert list(df['B']) == [2]

    @pytest.mark.parametrize('delimiter,added_field_name_suffix', [
        ('tick', '_XXX'),
        ('flag', None),
        (None, None),
    ])
    def test_dump_delimiter(self, session, delimiter, added_field_name_suffix):
        def another_query():
            return otp.Ticks({
                'A': [1, 2],
                'offset': [1, 2],
            })

        data = otp.Ticks({
            'A': [3, 4],
            'offset': [3, 4],
        })
        data.state_vars['list'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.state_vars['list'].dump(when_to_dump='blabla')
        with pytest.raises(ValueError):
            data.state_vars['list'].dump(delimiter='blabla')
        data = data.state_vars['list'].dump(propagate_input_ticks=True,
                                            when_to_dump='first_tick',
                                            delimiter=delimiter,
                                            added_field_name_suffix=added_field_name_suffix)
        df = otp.run(data)
        delimiter_field = 'DELIMITER' + (added_field_name_suffix or '')
        if delimiter == 'tick':
            assert delimiter_field in df
            assert list(df['A']) == [1, 2, 0, 3, 4]
            assert list(df[delimiter_field]) == ['', '', 'D', '', '']
        elif delimiter == 'flag':
            assert delimiter_field in df
            assert list(df['A']) == [1, 2, 3, 4]
            assert list(df[delimiter_field]) == ['', 'D', '', '']
        elif delimiter is None:
            assert delimiter_field not in df
            assert list(df['A']) == [1, 2, 3, 4]

    def test_push_back(self, session):
        def fun(tick):
            if tick['A'] % 2 == 0:
                tick.state_vars['list'].push_back(tick)

        data = otp.Ticks({'A': [0, 1, 2, 3, 4, 5]})
        data.state_vars['list'] = otp.state.tick_list()
        with pytest.raises(ValueError):
            data.state_vars['list'].push_back(None)
        data = data.script(fun)
        data = data.first()
        data = data.state_vars['list'].dump()
        df = otp.run(data)
        assert list(df['A']) == [0, 2, 4]

    def test_push_back_different_schema(self, session):
        def fun(tick):
            t1 = otp.dynamic_tick()
            t1['X'] = 1
            t2 = otp.dynamic_tick()
            t2['Y'] = 2
            tick.state_vars['list'].push_back(t1)
            tick.state_vars['list'].push_back(t2)

        data = otp.Tick(A=1)
        data.state_vars['list'] = otp.state.tick_list()
        data = data.script(fun)
        assert data.state_vars['list'].schema == dict(X=int, Y=int)
        data = data.state_vars['list'].dump()
        df = otp.run(data)
        assert list(df['X']) == [1, 0]
        assert list(df['Y']) == [0, 2]

    def test_get_size(self, session):
        def fun(tick):
            if tick['A'] % 2 == 1:
                tick.state_vars['list'].push_back(tick)
                tick['B'] = tick.state_vars['list'].get_size()
                return True

        data = otp.Ticks({'A': [0, 1, 2, 3, 4, 5]})
        data.state_vars['list'] = otp.state.tick_list()
        data = data.script(fun)
        assert 'B' not in data.state_vars['list'].schema
        data['C'] = data.state_vars['list'].get_size() * 10
        df = otp.run(data)
        assert list(df['A']) == [1, 3, 5]
        assert list(df['B']) == [1, 2, 3]
        assert list(df['C']) == [10, 20, 30]

    def test_clear_inplace(self, session):
        def another_query():
            return otp.Tick(A=2)

        data = otp.Tick(A=1)
        data.state_vars['list'] = otp.state.tick_list(otp.eval(another_query))
        data.state_vars['list'].clear(inplace=True)
        data = data.state_vars['list'].dump()
        df = otp.run(data)
        assert df.empty

    def test_clear_copy(self, session):
        def another_query():
            return otp.Tick(A=2)

        data = otp.Tick(A=1)
        data.state_vars['list'] = otp.state.tick_list(otp.eval(another_query))
        data_copy = data.state_vars['list'].clear()
        data = data.state_vars['list'].dump()
        data_copy = data_copy.state_vars['list'].dump()
        df = otp.run(data)
        copy_df = otp.run(data_copy)
        assert copy_df.empty
        assert list(df['A']) == [2]

    def test_clear_in_script(self, session):
        def another_query():
            return otp.Tick(A=2)

        def fun(tick):
            tick.state_vars['list'].clear()

        data = otp.Tick(A=1)
        data.state_vars['list'] = otp.state.tick_list(otp.eval(another_query))
        data = data.script(fun)
        data = data.state_vars['list'].dump()
        df = otp.run(data)
        assert df.empty

    def test_for(self, session):
        def another_query():
            return otp.Ticks(X=[1, 2, 3])

        def fun(tick):
            tick['SUM'] = 0
            for t in tick.state_vars['list']:
                tick['SUM'] += t.get_long_value('X')
                tick['TS'] = t.get_timestamp()

        data = otp.Tick(A=1)
        data.state_vars['list'] = otp.state.tick_list(otp.eval(another_query))
        data = data.script(fun)
        assert 'SUM' in data.schema
        assert 'TS' in data.schema
        df = otp.run(data)
        assert df['SUM'][0] == 6

    def test_next(self, session):
        def fun(tick):
            tick['X'] = ''
            for t in tick.state_vars['list']:
                tick['X'] += t['A'].apply(str)
                t.next()
                tick['X'] += t['A'].apply(str)
                t.next()
                tick['X'] += t['A'].apply(str)
                t.prev()
                tick['X'] += t['A'].apply(str)
                t.next()
                tick['X'] += t['A'].apply(str)
                t.next()
                if t.is_end():
                    tick['X'] += ':END:'

        data = otp.Tick(A=1)
        data.state_vars['list'] = otp.state.tick_list(otp.eval(otp.Ticks(A=[1, 2, 3])))
        data = data.script(fun)
        assert 'X' in data.schema
        df = otp.run(data)
        assert df['X'][0] == '12323:END:'

    def test_sort_empty_list(self, session):
        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list()
        data = data.state_vars['LIST'].sort('VALUE', int)
        data = data.state_vars['LIST'].dump()
        res = otp.run(data)
        assert len(res) == 0

    def test_sort_1(self, session):
        def fun(tick):
            tick.state_vars['LIST'].push_back(tick)
        data = otp.Ticks([
            ['offset', 'VALUE'],
            [0, 2],
            [0, 4],
            [0, 3],
            [0, 5],
            [0, 1],
        ])
        data.state_vars['LIST'] = otp.state.tick_list()
        data = data.script(fun)
        data = data.agg(dict(NUM_TICKS=otp.agg.count()), bucket_time='end')
        data = data.state_vars['LIST'].sort('VALUE', field_type=int)
        data = data.state_vars['LIST'].dump()
        res = otp.run(data)
        assert len(res) == 5
        assert res['VALUE'][0] == 1
        assert res['VALUE'][1] == 2
        assert res['VALUE'][2] == 3
        assert res['VALUE'][3] == 4
        assert res['VALUE'][4] == 5

    @pytest.mark.parametrize(
        'n_items', list(range(1, 80))  # as we covered a number of 2^n ranges, this should be good enough
    )
    def test_sort_2(self, session, n_items):

        def fun(tick):
            tick.state_vars['LIST'].push_back(tick)

        # we want test results to be stable, thus we seed the random number generator predictably
        random.seed(a=f'tick_list_test_{n_items}', version=2)
        original_list = [(random.randrange(10_000), i) for i in range(0, n_items)]

        data = otp.Ticks(offset=[0] * n_items,
                         VALUE=list(map(lambda x: x[0], original_list)),
                         INDEX=list(map(lambda x: x[1], original_list)))
        # implemented sort is stable, so it's safe to compare exact results with python-sorted list
        sorted_list = sorted(original_list, key=lambda x: x[0])

        data.state_vars['LIST'] = otp.state.tick_list()
        data = data.script(fun)
        data = data.agg(dict(NUM_TICKS=otp.agg.count()), bucket_time='end')
        data = data.state_vars['LIST'].sort('VALUE', int)
        data = data.state_vars['LIST'].dump()
        res = otp.run(data)

        assert len(res) == n_items
        for i in range(0, n_items):
            assert res['VALUE'][i] == sorted_list[i][0]
            assert res['INDEX'][i] == sorted_list[i][1]

    def test_sort_float(self, session):
        def fun(tick):
            tick.state_vars['LIST'].push_back(tick)
        data = otp.Ticks([
            ['offset', 'VALUE'],
            [0, 2.0],
            [0, 4.0],
            [0, 3.0],
            [0, 5.0],
            [0, 1.0],
        ])
        data.state_vars['LIST'] = otp.state.tick_list()
        data = data.script(fun)
        data = data.agg(dict(NUM_TICKS=otp.agg.count()), bucket_time='end')
        data = data.state_vars['LIST'].sort('VALUE', float)
        data = data.state_vars['LIST'].dump()
        res = otp.run(data)
        assert len(res) == 5
        assert res['VALUE'][0] == 1
        assert res['VALUE'][1] == 2
        assert res['VALUE'][2] == 3
        assert res['VALUE'][3] == 4
        assert res['VALUE'][4] == 5

    def test_sort_nsectime(self, session):
        def fun(tick):
            tick.state_vars['LIST'].push_back(tick)
        data = otp.Ticks([
            ['offset', 'VALUE'],
            [0, 2],
            [0, 4],
            [0, 3],
            [0, 5],
            [0, 1],
        ])
        data['VALUE'] = data['_START_TIME'] + otp.Milli(data['VALUE'])
        data.state_vars['LIST'] = otp.state.tick_list()
        data = data.script(fun)
        data = data.agg(dict(NUM_TICKS=otp.agg.count()), bucket_time='end')
        data = data.state_vars['LIST'].sort('VALUE', otp.nsectime)
        data = data.state_vars['LIST'].dump()
        res = otp.run(data)
        assert len(res) == 5
        assert res['VALUE'][0] == otp.config.default_start_time + otp.Milli(1)
        assert res['VALUE'][1] == otp.config.default_start_time + otp.Milli(2)
        assert res['VALUE'][2] == otp.config.default_start_time + otp.Milli(3)
        assert res['VALUE'][3] == otp.config.default_start_time + otp.Milli(4)
        assert res['VALUE'][4] == otp.config.default_start_time + otp.Milli(5)

    def test_erase(self, session):
        def another_query():
            return otp.Ticks(X=[1, 2, 3])

        def func(tick):
            for t in tick.state_vars['LIST']:
                if t['X'] == 2:
                    tick.state_vars['LIST'].erase(t)

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        data = data.script(func)
        data = data.state_vars['LIST'].dump()
        res = otp.run(data)
        assert all(res['X'] == [1, 3])

    @pytest.mark.parametrize("value", [
        otp.string[1100]('it should be a really long string'),
        otp.string[10]('ads'),
        otp.varstring('ferfre'),
        otp.decimal(14.1),
        otp.ulong(5),
        otp.uint(6),
        otp.int(3),
        otp.byte(1),
        otp.short(7),
        otp.date(2007, 1, 1),
        otp.datetime(2007, 1, 1, 1, 1, 1)
    ])
    def test_non_default_types(self, session, value):
        def generate_list(symbol):
            return otp.Tick(B=value)

        def update_from_list(tick):
            for t in tick.state_vars["LIST"]:
                tick["B"] = t["B"]

        data = otp.Tick(A=1)
        data.state_vars["LIST"] = otp.state.tick_list(otp.eval(generate_list))
        if isinstance(value, otp.decimal) or (isinstance(value, otp.varstring) and
                                              not otp.compatibility.is_supported_varstring_in_get_string_value()):
            with pytest.raises(TypeError):
                data.script(update_from_list)
        else:
            data = data.script(update_from_list)
            if isinstance(value, str) and value.length == 10:
                assert data.schema['B'] is str
            elif isinstance(value, (otp.date, otp.datetime)):
                assert data.schema['B'] is otp.nsectime
            else:
                assert data.schema['B'] is type(value)
            df = otp.run(data)
            assert all(df['B'] == [value])

    def test_schema(self, session):
        """
        Checking that schema of tick list can be set
        """
        src = otp.Tick(A=1, B=2.1, C='A', D=otp.datetime(2022, 1, 1))
        src.state_vars['tick_list_1'] = otp.state.tick_list()
        target_schema = dict(A=int, B=float, C=str, D=otp.nsectime)
        for field, type in target_schema.items():
            assert src.state_vars['tick_list_1'].schema[field] == type

        def fun(tick):
            tick.state_vars['tick_list_2'].push_back(tick)

        src.state_vars['tick_list_2'] = otp.state.tick_list(schema=dict(A=int))
        assert src.state_vars['tick_list_2'].schema == dict(A=int)
        src = src.script(fun)
        target_schema = dict(A=int, B=float, C=str, D=otp.nsectime)
        for field, type in target_schema.items():
            assert src.state_vars['tick_list_2'].schema[field] == type

        src.state_vars['tick_list_3'] = otp.state.tick_list(otp.eval(otp.Tick(F=1, G='str')))
        target_schema = dict(F=int, G=str)
        for field, type in target_schema.items():
            assert src.state_vars['tick_list_3'].schema[field] == type

        src.state_vars['tick_list_4'] = otp.state.tick_list(schema=dict(M=int, N=str))
        target_schema = dict(M=int, N=str)
        for field, type in target_schema.items():
            assert src.state_vars['tick_list_4'].schema[field] == type

        src.state_vars['tick_list_5'] = otp.state.tick_list(schema=['A', 'B'])
        assert src.state_vars['tick_list_5'].schema == dict(A=int, B=float)

        src.state_vars['tick_list_6'] = otp.state.tick_list(otp.eval(otp.Tick(H='A', I=1, J=otp.datetime(2021, 1, 1))),
                                                            schema=['I', 'J'])
        assert src.state_vars['tick_list_6'].schema == dict(I=int, J=otp.nsectime)

        src.state_vars['tick_list_7'] = otp.state.tick_list(otp.eval(otp.Tick(H='A', I=1, J=otp.datetime(2021, 1, 1))),
                                                            schema=dict(K=str, L=float))
        assert src.state_vars['tick_list_7'].schema == dict(K=str, L=float)

        with pytest.raises(KeyError):
            src.state_vars['tick_list_8'] = otp.state.tick_list(
                otp.eval(otp.Tick(H='A', I=1, J=otp.datetime(2021, 1, 1))),
                schema=['K', 'L'])
            src.state_vars['tick_list_8'].schema

        with pytest.raises(KeyError):
            src.state_vars['tick_list_9'] = otp.state.tick_list(schema=['K', 'L'])
            src.state_vars['tick_list_9'].schema

    def test_source_as_default_value(self, session):
        data = otp.Tick(A=1)
        data.state_vars['list'] = otp.state.tick_list(otp.Ticks(B=[4, 5, 6]))
        data.state_vars['list'].dump(inplace=True)
        df = otp.run(data)
        assert list(df['B']) == [4, 5, 6]


class TestTickSet:

    def test_dtype(self, session):
        assert otp.state.tick_set('oldest', 'XXX').dtype is TickSet
        assert isinstance(otp.state.tick_set('latest', 'XXX'), TickSet)

    def test_creation(self, session):
        data = otp.Ticks(dict(X=[1, 2, 3]))
        data.state_vars['set_oldest_tick'] = otp.state.tick_set('oldest', 'X')
        data.state_vars['set_latest_tick'] = otp.state.tick_set('latest', 'X')
        otp.run(data)

    def test_exception(self):
        def another_query():
            return otp.Tick(B=2)

        data = otp.Tick(A=1)
        with pytest.raises(ValueError):
            data.state_vars['set'] = otp.state.tick_set('___________', 'A')
        with pytest.raises(ValueError):
            data.state_vars['set'] = otp.state.tick_set('oldest', '____________')
        with pytest.raises(ValueError):
            data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        with pytest.raises(ValueError):
            data.state_vars['set'] = otp.state.tick_set('oldest', 'A', 12345)

    @pytest.mark.parametrize('insertion_policy,result_a,result_b', [
        ('oldest', [1, 2, 3], [1, 1, 1]),
        ('latest', [1, 2, 3], [2, 2, 2]),
    ])
    def test_dump(self, session, insertion_policy, result_a, result_b):
        def another_query():
            return otp.Ticks({
                'A': [1, 2, 1, 3, 2, 3, 3],
                'B': [1, 1, 2, 1, 2, 1, 2],
            })

        data = otp.Tick(X=1)
        data.state_vars['set'] = otp.state.tick_set(insertion_policy, 'A', otp.eval(another_query))
        data = data.state_vars['set'].dump()
        df = otp.run(data)
        assert list(df['A']) == result_a
        assert list(df['B']) == result_b

    def test_dump_empty(self, session):
        data = otp.Tick(A=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A')
        data = data.state_vars['set'].dump()
        df = otp.run(data)
        assert df.empty

    def test_dump_first_tick(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2],
                'offset': [1, 2],
            })

        data = otp.Ticks({
            'A': [3, 4],
            'offset': [3, 4],
        })
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        data = data.state_vars['set'].dump(propagate_input_ticks=True, when_to_dump='FIRST_TICK')
        df = otp.run(data)
        assert list(df['A']) == [1, 2, 3, 4]

    def test_dump_every_tick(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2],
                'offset': [1, 2],
            })

        data = otp.Ticks({
            'A': [3, 4],
            'offset': [3, 4],
        })
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        data = data.state_vars['set'].dump(propagate_input_ticks=True, when_to_dump='EVERY_TICK')
        df = otp.run(data)
        assert list(df['A']) == [1, 2, 3, 1, 2, 4]

    def test_dump_before_tick(self, session):
        data = otp.Ticks({
            'A': [3, 4],
            'offset': [3, 4],
        })
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A')
        data = data.state_vars['set'].dump(propagate_input_ticks=True, when_to_dump='BEFORE_TICK')
        df = otp.run(data)
        assert list(df['A']) == [3, 4]

    @pytest.mark.parametrize('propagate_input_ticks', (False, True))
    def test_dump_schema(self, session, propagate_input_ticks):
        def another_query():
            return otp.Tick(B=2)

        data = otp.Tick(A=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', 'B', otp.eval(another_query))
        data = data.state_vars['set'].dump(propagate_input_ticks=propagate_input_ticks)
        df = otp.run(data)
        assert 'B' in data.schema
        if propagate_input_ticks:
            assert 'A' in data.schema
            assert 'A' in df
            assert list(df['B']) == [2, 0]
            assert list(df['A']) == [0, 1]
        else:
            assert 'A' not in data.schema
            assert 'A' not in df
            assert list(df['B']) == [2]

    def test_for(self, session):
        def another_query():
            return otp.Ticks(X=[1, 2, 3])

        def fun(tick):
            tick['SUM'] = 0
            for t in tick.state_vars['set']:
                tick['SUM'] += t.get_long_value('X')
                tick['TS'] = t.get_timestamp()

        data = otp.Tick(A=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', 'X', otp.eval(another_query))
        data = data.script(fun)
        assert 'SUM' in data.schema
        assert 'TS' in data.schema
        df = otp.run(data)
        assert df['SUM'][0] == 6

    def test_update_inplace(self, session):
        def another_query():
            return otp.Ticks(A=[1, 2, 3])

        data = otp.Ticks({
            'A': [4, 5],
            'offset': [4, 5],
        })
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        data.state_vars['set'].update(inplace=True)
        data = data.state_vars['set'].dump()
        df = otp.run(data)
        assert list(df['A']) == [1, 2, 3, 4, 1, 2, 3, 4, 5]

    def test_update_copy(self, session):
        def another_query():
            return otp.Ticks(A=[1, 2, 3])

        data = otp.Ticks({
            'A': [4, 5],
            'offset': [4, 5],
        })
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        data_copy = data.state_vars['set'].update()
        data = data.state_vars['set'].dump()
        data_copy = data_copy.state_vars['set'].dump()
        df = otp.run(data)
        copy_df = otp.run(data_copy)
        assert list(copy_df['A']) == [1, 2, 3, 4, 1, 2, 3, 4, 5]
        assert list(df['A']) == [1, 2, 3, 1, 2, 3]

    def test_update_where(self, session):
        def another_query():
            return otp.Ticks(A=[1, 2, 3])

        data = otp.Ticks({
            'A': [4, 5],
            'offset': [4, 5],
        })
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        data = data.state_vars['set'].update(where=data['A'] == 4)
        data = data.state_vars['set'].dump()
        df = otp.run(data)
        assert list(df['A']) == [1, 2, 3, 4, 1, 2, 3, 4]

    def test_update_erase_condition(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2, 3],
                'X': [1, 2, 3],
            })

        data = otp.Ticks({
            'A': [4, 5],
            'X': [1, 2],
            'offset': [4, 5],
        })
        data.state_vars['set'] = otp.state.tick_set('oldest', 'X', otp.eval(another_query))
        data = data.state_vars['set'].update(erase_condition=data['A'] == 4)
        data = data.state_vars['set'].dump()
        df = otp.run(data)
        assert list(df['A']) == [2, 3, 2, 3]

    def test_update_value_fields(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2, 3],
                'B': [1, 2, 3],
                'X': [1, 2, 3],
                'Y': [3, 2, 1],
            })

        data = otp.Ticks({
            'A': [4, 5],
            'C': [4, 5],
            'X': [4, 5],
            'offset': [4, 5],
        })
        data.state_vars['set'] = otp.state.tick_set('oldest', ['X', 'Y'], otp.eval(another_query))
        with pytest.raises(ValueError):
            data.state_vars['set'].update(value_fields=['__________'])
        with pytest.raises(ValueError):
            data = data.state_vars['set'].update(value_fields=['X'])
        data['Y'] = 7
        data = data.state_vars['set'].update(value_fields=['C'])
        data = data.state_vars['set'].dump()
        assert {'A', 'B', 'C', 'X', 'Y'}.issubset(data.schema)
        df = otp.run(data)
        # field A in tick set and in source but not specified in update
        assert list(df['A']) == [1, 2, 3, 0, 1, 2, 3, 0, 0]
        # field B in tick set but not in source
        assert list(df['B']) == [1, 2, 3, 0, 1, 2, 3, 0, 0]
        # field C in source only, specified in update, other values are default
        assert list(df['C']) == [0, 0, 0, 4, 0, 0, 0, 4, 5]
        # X and Y are key fields and must be presented both in source and in tick set
        assert list(df['X']) == [1, 2, 3, 4, 1, 2, 3, 4, 5]
        assert list(df['Y']) == [3, 2, 1, 7, 3, 2, 1, 7, 7]

    def test_update_exception(self, session):
        def another_query():
            return otp.Tick(X=1)

        data = otp.Tick(A=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', 'X', otp.eval(another_query))
        with pytest.raises(ValueError):
            data.state_vars['set'].update()

    def test_find(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2, 3],
                'B': [3, 2, 1],
                'TARGET': ['a', 'b', 'c'],
            })

        data = otp.Tick(X=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', ['A', 'B'], otp.eval(another_query))
        with pytest.raises(ValueError):
            data['F'] = data.state_vars['set'].find('______', 'empty')
        with pytest.raises(ValueError):
            data['F'] = data.state_vars['set'].find('TARGET', 'empty')
        with pytest.raises(ValueError):
            data['F'] = data.state_vars['set'].find('TARGET', 'empty', 'WRONG_TYPE', 'WRONG_TYPE')
        with pytest.raises(ValueError):
            data['F'] = data.state_vars['set'].find('TARGET', 123, 1, 3)
        with pytest.raises(ValueError):
            data['F'] = data.state_vars['set'].find('TARGET', 'empty', 1)
        with pytest.raises(ValueError):
            data['NF'] = data.state_vars['set'].find('TARGET', 'empty', A=1)
        with pytest.raises(ValueError):
            data['NF'] = data.state_vars['set'].find('TARGET', 'empty', WRONG_KEY=1, B=3)
        with pytest.raises(ValueError):
            data['NF'] = data.state_vars['set'].find('TARGET', 'empty', A='WRONG_TYPE', B=3)
        with pytest.raises(ValueError):
            data['NF'] = data.state_vars['set'].find('TARGET', 'empty', 1, A=1, B=3)
        data['F'] = data.state_vars['set'].find('TARGET', 'empty', 1, 3)
        data['NF'] = data.state_vars['set'].find('TARGET', 'empty', -1, 3)
        data['NKF'] = data.state_vars['set'].find('TARGET', 'empty', A=1, B=3)
        data['NKNF'] = data.state_vars['set'].find('TARGET', 'empty', A=-1, B=3)
        df = otp.run(data)
        assert list(df['F']) == ['a']
        assert list(df['NF']) == ['empty']
        assert list(df['NKF']) == ['a']
        assert list(df['NKNF']) == ['empty']

    def test_find_with_throw(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2, 3],
                'TARGET': ['a', 'b', 'c'],
            })

        data = otp.Tick(X=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        with pytest.raises(ValueError):
            data['F'] = data.state_vars['set'].find('TARGET')
        with pytest.raises(ValueError):
            data['F'] = data.state_vars['set'].find('TARGET', throw=True, A=1)
        data['F'] = data.state_vars['set'].find('TARGET', 1, throw=True)
        df = otp.run(data)
        assert list(df['F']) == ['a']
        data['F'] = data.state_vars['set'].find('TARGET', -1, throw=True)
        with pytest.raises(Exception):
            otp.run(data)

    def test_find_in_script(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2, 3],
                'TARGET': ['a', 'b', 'c'],
            })

        def fun(tick):
            t = otp.tick_set_tick()
            tick['RES'] = ''
            tick['F'] = tick.state_vars['set'].find('TARGET', 'empty', -1)
            if tick.state_vars['set'].find(t, -1):
                tick['RES'] += '-1'
            if tick.state_vars['set'].find_by_named_keys(t, A=0):
                tick['RES'] += '0'
            if tick.state_vars['set'].find(t, 1):
                tick['RES'] += '1'
            if tick.state_vars['set'].find_by_named_keys(t, A=2):
                tick['RES'] += '2'

        data = otp.Tick(X=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        data = data.script(fun)
        df = otp.run(data)
        assert list(df['F']) == ['empty']
        assert list(df['RES']) == ['12']

    def test_find_with_auto_default(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2, 3],
                'STR': ['a', 'b', 'c'],
                'INT': [1, 2, 3],
                'FLOAT': [1.0, 1.1, 1.2],
                'MSECTIME': [otp.msectime(1), otp.msectime(2), otp.msectime(3)],
                'NSECTIME': [otp.nsectime(1), otp.nsectime(2), otp.nsectime(3)],
            })

        data = otp.Tick(X=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))

        data['TEST_1'] = data.state_vars['set'].find('STR', A=1)
        data['TEST_2'] = data.state_vars['set'].find('STR', A=4)
        data['TEST_3'] = data.state_vars['set'].find('STR', 'test', A=4)
        data['TEST_4'] = data.state_vars['set'].find('INT', A=4)
        data['TEST_5'] = data.state_vars['set'].find('FLOAT', A=4)
        data['TEST_6'] = data.state_vars['set'].find('MSECTIME', A=4)
        data['TEST_7'] = data.state_vars['set'].find('NSECTIME', A=4)

        with pytest.raises(ValueError):
            data['TEST'] = data.state_vars['set'].find('TARGET', A=2, throw=True)

        df = otp.run(data, timezone='GMT')
        test_result = [
            *df['TEST_1'].to_list(), *df['TEST_2'].to_list(), *df['TEST_3'].to_list(),
            *df['TEST_4'].to_list(), *[str(i) for i in df['TEST_5'].to_list()],
            *df['TEST_6'].to_list(), *df['TEST_7'].to_list(),
        ]

        assert test_result == ['a', '', 'test', 0, 'nan', pd.Timestamp(0), pd.Timestamp(0)]

    def test_tick_set_tick_used_after_find_in_script(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2, 3],
                'TARGET': ['a', 'b', 'c'],
            })

        def fun(tick):
            t = otp.tick_set_tick()
            tick['RES'] = ''
            if tick.state_vars['set'].find(t, 1):
                tick['RES'] += t.get_value('TARGET')
            if tick.state_vars['set'].find_by_named_keys(t, A=2):
                tick['RES'] += t.get_value('TARGET')

        data = otp.Tick(X=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        data = data.script(fun)
        df = otp.run(data)
        assert list(df['RES']) == ['ab']

    def test_fixed_length_string(self, session):
        def another_query():
            t = otp.Tick(A='abcdef', X='xyz', F=otp.string('hello'))
            t = t.table(A=otp.string[6], X=otp.string[3], strict=False)
            return t

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        # test named key value
        data['X'] = data.state_vars['LIST'].find('X', 'no', A='abcdef')
        # test key value
        data['Y'] = data.state_vars['LIST'].find('X', 'no', 'abcdefghi')
        # test default value
        with pytest.warns(Warning, match="Value 'not_found' will be truncated to 3 characters"):
            data['Z'] = data.state_vars['LIST'].find('X', 'not_found', 'abcdefghi')
        # test otp.string without length
        data['F'] = data.state_vars['LIST'].find('F', '', 'abcdef')
        df = otp.run(data)
        assert df['X'][0] == 'xyz'
        assert df['Y'][0] == 'no'
        assert df['Z'][0] == 'not'
        assert df['F'][0] == 'hello'

    def test_clear(self, session):
        def another_query():
            return otp.Tick(A=2)

        data = otp.Tick(A=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        data = data.state_vars['set'].clear()
        data = data.state_vars['set'].dump()
        df = otp.run(data)
        assert df.empty

    def test_clear_in_script(self, session):
        def another_query():
            return otp.Tick(A=2)

        def fun(tick):
            tick.state_vars['set'].clear()

        data = otp.Tick(A=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        data = data.script(fun)
        data = data.state_vars['set'].dump()
        df = otp.run(data)
        assert df.empty

    def test_erase(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2, 3, 4],
                'B': [4, 3, 2, 1],
                'TARGET': ['a', 'b', 'c', 'd'],
            })

        data = otp.Tick(X=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', ['A', 'B'], otp.eval(another_query))
        with pytest.raises(ValueError):
            data.state_vars['set'].erase('WRONG_TYPE', 'WRONG_TYPE')
        with pytest.raises(ValueError):
            data.state_vars['set'].erase(1)
        with pytest.raises(ValueError):
            data.state_vars['set'].erase(A=1)
        with pytest.raises(ValueError):
            data.state_vars['set'].erase(WRONG_KEY=1, B=3)
        with pytest.raises(ValueError):
            data.state_vars['set'].erase(A='WRONG_TYPE', B=3)
        with pytest.raises(ValueError):
            data.state_vars['set'].erase(1, A=1, B=3)
        data['F'] = data.state_vars['set'].erase(1, 4)
        data['NF'] = data.state_vars['set'].erase(-1, 4)
        data['NKF'] = data.state_vars['set'].erase(A=2, B=3)
        data['NKNF'] = data.state_vars['set'].erase(A=-1, B=3)
        data = data.execute(data.state_vars['set'].erase(A=3, B=2))
        df = otp.run(data)
        assert list(df['F']) == [1]
        assert list(df['NF']) == [0]
        assert list(df['NKF']) == [1]
        assert list(df['NKNF']) == [0]
        data = data.state_vars['set'].dump()
        df = otp.run(data)
        assert list(df['A']) == [4]
        assert list(df['B']) == [1]
        assert list(df['TARGET']) == ['d']

    def test_erase_in_script(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2, 3],
                'TARGET': ['a', 'b', 'c'],
            })

        def fun(tick):
            tick['RES'] = ''
            for tt in tick.state_vars['set']:
                if tick.state_vars['set'].erase(tt):
                    tick['RES'] += '1'
            tick['LEN'] = tick.state_vars['set'].get_size()

        data = otp.Tick(X=1)
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        data = data.script(fun)
        df = otp.run(data)
        assert list(df['RES']) == ['111']
        assert list(df['LEN']) == [0]

    def test_erase_via_two_tick_set_ticks(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2, 3, 4, 5],
                'TARGET': ['a', 'b', 'c', 'd', 'e'],
            })

        def fun(tick):
            t1 = otp.tick_set_tick()
            t2 = otp.tick_set_tick()
            if tick.state_vars['set'].find(t1, 2) + tick.state_vars['set'].find(t2, 4) == 2:
                tick.state_vars['set'].erase(t1, t2)
            for tt in tick.state_vars['set']:
                tick['RES'] += tt.get_value('TARGET')

        data = otp.Tick(X=1, RES='')
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(another_query))
        data = data.script(fun)
        df = otp.run(data)
        assert list(df['RES']) == ['ade']

    def test_insert(self, session):
        data = otp.Ticks({'A': [0, 1, 2, 3, 4, 5]})
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A')
        data['X'] = data.state_vars['set'].insert()
        data = data.execute(data.state_vars['set'].insert())
        with pytest.raises(ValueError):
            data.state_vars['set'].insert(123)
        df = otp.run(data)
        assert list(df['X']) == [1, 1, 1, 1, 1, 1]
        data = data.first()
        data = data.state_vars['set'].dump()
        df = otp.run(data)
        assert list(df['A']) == [0, 1, 2, 3, 4, 5]

    def test_insert_in_script(self, session):
        def fun(tick):
            if tick['A'] % 2 == 0:
                tick['X'] = tick.state_vars['set'].insert(tick)
                tick.state_vars['set'].insert(tick)
            if tick['A'] == 1:
                tick.state_vars['set'].insert()
                tick.state_vars['set'].insert()

        data = otp.Ticks({'A': [0, 1, 2, 3, 4, 5]})
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A')
        data = data.script(fun)
        df = otp.run(data)
        assert list(df['X']) == [1, 0, 1, 0, 1, 0]
        data = data.first()
        data = data.state_vars['set'].dump()
        df = otp.run(data)
        assert list(df['A']) == [0, 1, 2, 4]

    def test_get_size(self, session):
        def fun(tick):
            if tick['A'] % 2 == 1:
                tick.state_vars['set'].insert(tick)
                tick['B'] = tick.state_vars['set'].get_size()
                return True

        data = otp.Ticks({'A': [0, 1, 2, 3, 4, 5]})
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A')
        data = data.script(fun)
        data['C'] = data.state_vars['set'].get_size() * 10
        df = otp.run(data)
        assert list(df['A']) == [1, 3, 5]
        assert list(df['B']) == [1, 2, 3]
        assert list(df['C']) == [10, 20, 30]

    def test_present(self, session):
        def fun(tick):
            if tick['A'] % 2 == 1:
                tick.state_vars['set'].insert(tick)
                tick['B'] = tick.state_vars['set'].present(5)
                return True

        data = otp.Ticks({'A': [0, 1, 2, 3, 4, 5]})
        data.state_vars['set'] = otp.state.tick_set('oldest', 'A')
        data = data.script(fun)
        with pytest.raises(ValueError):
            data['C'] = data.state_vars['set'].present(1, 0)
        with pytest.raises(ValueError):
            data['C'] = data.state_vars['set'].present('WRONG_TYPE')
        data['C'] = data.state_vars['set'].present(1)
        data['D'] = data.state_vars['set'].present(0)
        df = otp.run(data)
        assert list(df['A']) == [1, 3, 5]
        assert list(df['B']) == [0, 0, 1]
        assert list(df['C']) == [1, 1, 1]
        assert list(df['D']) == [0, 0, 0]

    def test_varstring(self, session):
        data = otp.Ticks({"ID": [0]})
        data.state_vars["LIST"] = otp.state.tick_set(
            "latest",
            "ID",
            otp.eval(otp.Tick(ID=0, VAL=otp.varstring("some string"))),
        )
        data["MY_VAL"] = data.state_vars["LIST"].find("VAL", "")
        assert data.schema["MY_VAL"] == otp.varstring
        df = otp.run(data)
        assert all(df["ID"] == [0])
        assert all(df["MY_VAL"] == ["some string"])

    def test_different_length_strings(self, session):
        data = otp.Tick(EXCHANGE=otp.string[1]("A"))
        data.state_vars["EXCHANGES"] = otp.state.tick_set("latest", "CODE", otp.eval(otp.Tick(CODE="A", NAME="YES")))
        data["EXCHANGE_NAME"] = data.state_vars["EXCHANGES"].find("NAME", "unknown", data["EXCHANGE"])
        df = otp.run(data)
        assert all(df["EXCHANGE"] == ["A"])
        assert all(df["EXCHANGE_NAME"] == ["YES"])

    def test_schema(self, session):
        """
        Checking that schema of tick set can be set
        """
        src = otp.Tick(A=1, B=2.1, C='A', D=otp.datetime(2022, 1, 1))
        src.state_vars['tick_set_1'] = otp.state.tick_set(key_fields='A', insertion_policy='latest')
        target_schema = dict(A=int, B=float, C=str, D=otp.nsectime)
        for field, type in target_schema.items():
            assert src.state_vars['tick_set_1'].schema[field] == type

        src.state_vars['tick_set_2'] = otp.state.tick_set(key_fields='A', insertion_policy='latest',
                                                          schema=dict(A=int))
        assert dict(src.state_vars['tick_set_2'].schema.items()) == dict(A=int)

        src = src.state_vars['tick_set_2'].update()
        # TODO: may check for schema exactly, once hidden columns are removed from tick sequence schemas
        target_schema = dict(A=int, B=float, C=str, D=otp.nsectime)
        for field, type in target_schema.items():
            assert src.state_vars['tick_set_2'].schema[field] == type

        # key value is not necessarily contained in the source schema
        src.state_vars['tick_set_3'] = otp.state.tick_set(key_fields=["K", "L"], insertion_policy='latest',
                                                          schema=dict(K=str, L=int))
        assert dict(src.state_vars['tick_set_3'].schema.items()) == dict(K=str, L=int)

        src.state_vars['tick_set_4'] = otp.state.tick_set(default_value=otp.eval(otp.Tick(F=1, G='str')),
                                                          key_fields='F',
                                                          insertion_policy='latest')
        target_schema = dict(F=int, G=str)
        for field, type in target_schema.items():
            assert src.state_vars['tick_set_4'].schema[field] == type

        src.state_vars['tick_set_5'] = otp.state.tick_set(
            insertion_policy='latest',
            key_fields=['A'],
            schema=['A', 'B'])
        assert src.state_vars['tick_set_5'].schema == dict(A=int, B=float)

        src.state_vars['tick_set_6'] = otp.state.tick_set(
            insertion_policy='latest',
            key_fields=['I'],
            default_value=otp.eval(otp.Tick(H='A', I=1, J=otp.datetime(2021, 1, 1))),
            schema=['I', 'J'])
        assert src.state_vars['tick_set_6'].schema == dict(I=int, J=otp.nsectime)

        src.state_vars['tick_set_7'] = otp.state.tick_set(
            insertion_policy='latest',
            key_fields='K',
            default_value=otp.eval(otp.Tick(H='A', I=1, J=otp.datetime(2021, 1, 1))),
            schema=dict(K=str, L=float))
        assert src.state_vars['tick_set_7'].schema == dict(K=str, L=float)

        with pytest.raises(KeyError):
            src.state_vars['tick_set_8'] = otp.state.tick_set(
                insertion_policy='latest',
                key_fields='K',
                default_value=otp.eval(otp.Tick(H='A', I=1, J=otp.datetime(2021, 1, 1))),
                schema=['K', 'L'])
            src.state_vars['tick_set_8'].schema

        with pytest.raises(KeyError):
            src.state_vars['tick_set_9'] = otp.state.tick_set(
                insertion_policy='latest',
                key_fields='K',
                schema=['K', 'L'])
            src.state_vars['tick_set_9'].schema


class TestTickSetUnordered:

    def test_dtype(self, session):
        assert otp.state.tick_set_unordered('oldest', 'XXX', max_distinct_keys=2).dtype is TickSetUnordered
        assert isinstance(otp.state.tick_set_unordered('latest', 'XXX', max_distinct_keys=2), TickSetUnordered)

    def test_creation(self, session):
        data = otp.Ticks(dict(X=[1, 2, 3]))
        data.state_vars['set'] = otp.state.tick_set_unordered('oldest', 'X', max_distinct_keys=10)
        data.execute(data.state_vars['set'].insert(), inplace=True)
        data = data.first()
        data = data.state_vars['set'].dump()
        df = otp.run(data)
        assert list(df['X']) == [1, 2, 3]

    def test_integer_types(self, session):
        data = otp.Ticks(ORDER_ID=[1, 2, 3], SIZE=[otp.int(1)] * 3)
        data.state_vars['ORDERS'] = otp.state.tick_set_unordered(insertion_policy='latest',
                                                                 key_fields='ORDER_ID',
                                                                 max_distinct_keys=1000)
        data['OLD_SIZE'] = data.state_vars['ORDERS'].find('SIZE', 0, data['ORDER_ID'])


class TestTickDeque:

    def test_dtype(self, session):
        assert otp.state.tick_deque().dtype is TickDeque
        assert isinstance(otp.state.tick_deque(), TickDeque)

    def test_creation(self, session):
        data = otp.Ticks(dict(X=[1, 2, 3]))
        data.state_vars['deque'] = otp.state.tick_deque()

    def test_dump(self, session):
        def another_query():
            return otp.Ticks(A=[2, 3])

        data = otp.Tick(A=1)
        data.state_vars['deque'] = otp.state.tick_deque(otp.eval(another_query))
        data = data.state_vars['deque'].dump()
        df = otp.run(data)
        assert list(df['A']) == [2, 3]

    def test_push_pop_get_size(self, session):
        def fun(tick):
            if tick['A'] % 2 == 0:
                tick.state_vars['deque'].push_back(tick)
                tick.state_vars['deque'].push_back(tick)
                tick.state_vars['deque'].pop_back()
            if tick['A'] == 5:
                tick.state_vars['deque'].pop_front()
            tick['B'] = tick.state_vars['deque'].get_size()

        data = otp.Ticks({'A': [0, 1, 2, 3, 4, 5]})
        data.state_vars['deque'] = otp.state.tick_deque()
        with pytest.raises(ValueError):
            data.state_vars['deque'].push_back(None)
        with pytest.raises(ValueError):
            data.state_vars['deque'].pop_back()
        with pytest.raises(ValueError):
            data.state_vars['deque'].pop_front()
        data = data.script(fun)
        data['C'] = data.state_vars['deque'].get_size()
        df = otp.run(data)
        assert list(df['B']) == [1, 1, 2, 2, 3, 2]
        assert list(df['C']) == [1, 1, 2, 2, 3, 2]
        data = data.first()
        data = data.state_vars['deque'].dump()
        df = otp.run(data)
        assert list(df['A']) == [2, 4]

    def test_sort_tick_deque(self, session):
        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_deque()
        with pytest.raises(NotImplementedError):
            data.state_vars['LIST'].sort('VALUE', int)


class TestTickSequenceTick:

    def test_get_long_value(self, session):
        def another_query():
            return otp.Ticks({
                'X': [1, 2, 3],
                'Y': [4, 5, 6],
                'Z': [7., 8., 9.],
            })

        def fun(tick):
            tick['SUM'] = 0
            for t in tick.state_vars['LIST']:
                tick['SUM'] += t.get_long_value('X') * t.get_long_value('Y')

        def no_fun(tick):
            for t in tick.state_vars['LIST']:
                tick['Z'] = t.get_long_value('NO_SUCH_FIELD')

        def no_fun_2(tick):
            for t in tick.state_vars['LIST']:
                tick['Z'] = t.get_long_value('Z')

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.script(no_fun)
        with pytest.raises(ValueError):
            data.script(no_fun_2)
        data = data.script(fun)
        df = otp.run(data)
        assert df['SUM'][0] == 4 + 10 + 18

    def test_get_double_value(self, session):
        def another_query():
            return otp.Ticks({
                'X': [1., 2., 3.],
                'Y': [4., 5., 6.],
                'Z': [7, 8, 9],
            })

        def fun(tick):
            tick['SUM'] = 0
            for t in tick.state_vars['LIST']:
                tick['SUM'] += t.get_double_value('X') * t.get_double_value('Y')

        def no_fun(tick):
            for t in tick.state_vars['LIST']:
                tick['Z'] = t.get_double_value('NO_SUCH_FIELD')

        def no_fun_2(tick):
            for t in tick.state_vars['LIST']:
                tick['Z'] = t.get_double_value('Z')

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.script(no_fun)
        with pytest.raises(ValueError):
            data.script(no_fun_2)
        data = data.script(fun)
        df = otp.run(data)
        assert df['SUM'][0] == 4. + 10. + 18.

    def test_get_string_value(self, session):
        def another_query():
            return otp.Ticks({
                'X': ['1', '2', '3'],
                'Y': ['4', '5', '6'],
                'Z': [7, 8, 9],
            })

        def fun(tick):
            tick['SUM'] = ''
            for t in tick.state_vars['LIST']:
                tick['SUM'] += t.get_string_value('X') + t.get_string_value('Y')

        def no_fun(tick):
            for t in tick.state_vars['LIST']:
                tick['Z'] = t.get_string_value('NO_SUCH_FIELD')

        def no_fun_2(tick):
            for t in tick.state_vars['LIST']:
                tick['Z'] = t.get_string_value('Z')

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.script(no_fun)
        with pytest.raises(ValueError):
            data.script(no_fun_2)
        data = data.script(fun)
        df = otp.run(data)
        assert df['SUM'][0] == '142536'

    def test_get_datetime_value(self, session):
        def another_query():
            t = otp.Ticks({
                'Z': [7, 8, 9],
            })
            t['TS'] = t['TIMESTAMP']
            t['MTS'] = otp.msectime(2)
            return t

        def fun(tick):
            for t in tick.state_vars['LIST']:
                tick['TS'] = t.get_datetime_value('TS')
                tick['MTS'] = t.get_datetime_value('MTS')

        def no_fun(tick):
            for t in tick.state_vars['LIST']:
                tick['Z'] = t.get_datetime_value('NO_SUCH_FIELD')

        def no_fun_2(tick):
            for t in tick.state_vars['LIST']:
                tick['Z'] = t.get_datetime_value('Z')

        def no_fun_3(tick):
            for t in tick.state_vars['LIST']:
                tick['Z'] = t.get_datetime_value('TIMESTAMP')

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.script(no_fun)
        with pytest.raises(ValueError):
            data.script(no_fun_2)
        with pytest.raises(ValueError):
            data.script(no_fun_3)
        data = data.script(fun)
        df = otp.run(data)
        assert df['TS'][0] == otp.config['default_start_time'] + otp.Milli(2)
        assert df['MTS'][0] == zero_time + otp.Milli(2)

    def test_get_timestamp(self, session):
        def another_query():
            return otp.Ticks({'A': [1, 2, 3]})

        def fun(tick):
            for t in tick.state_vars['LIST']:
                tick['TS'] = t.get_timestamp()

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        data = data.script(fun)
        df = otp.run(data)
        assert df['TS'][0] == otp.config['default_start_time'] + otp.Milli(2)

    def test_get_value(self, session):
        def another_query():
            t = otp.Ticks({
                'A': [1, 2, 3],
                'B': [1.1, 2.2, 3.3],
                'C': ['a', 'b', 'c'],
            })
            t['D'] = t['TIMESTAMP']
            t['E'] = otp.msectime(2)
            return t

        def fun(tick):
            for t in tick.state_vars['LIST']:
                tick['A'] = t.get_value('A')
                tick['B'] = t.get_value('B')
                tick['C'] = t.get_value('C')
                tick['D'] = t.get_value('D')
                tick['E'] = t['E']
                tick['F'] = t.get_value('TIMESTAMP')

        def no_fun(tick):
            for t in tick.state_vars['LIST']:
                tick['A'] = t.get_value('NO_SUCH_FIELD')

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.script(no_fun)
        data = data.script(fun)
        df = otp.run(data)
        assert df['A'][0] == 3
        assert df['B'][0] == 3.3
        assert df['C'][0] == 'c'
        assert df['D'][0] == otp.config['default_start_time'] + otp.Milli(2)
        assert df['E'][0] == zero_time + otp.Milli(2)
        assert df['F'][0] == otp.config['default_start_time'] + otp.Milli(2)

    def test_get_value_with_operation(self, session):
        def another_query():
            t = otp.Ticks({
                'A': [1, 2, 3],
                'B': [1.1, 2.2, 3.3],
                'C': ['a', 'b', 'c'],
            })
            t['D'] = t['TIMESTAMP']
            return t

        def fun(tick):
            long_field = 'A'
            double_field = 'B'
            string_field = 'C'
            datetime_field = 'D'
            for t in tick.state_vars['LIST']:
                tick['TOTAL_INT'] += t.get_long_value(long_field)
                tick['TOTAL_FLOAT'] += t.get_double_value(double_field)
                tick['TOTAL_STRING'] += t.get_string_value(string_field)
                tick['SOME_DATETIME'] = t.get_datetime_value(datetime_field)

        def fun_wrong_type(tick):
            long_field = 1
            for t in tick.state_vars['LIST']:
                tick['TOTAL_INT'] += t.get_long_value(long_field)

        data = otp.Tick(TOTAL_INT=0, TOTAL_FLOAT=0.0, TOTAL_STRING='', SOME_DATETIME=otp.nsectime(0))
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.script(fun_wrong_type)
        data = data.script(fun)
        df = otp.run(data)
        assert df['TOTAL_INT'][0] == 6
        assert df['TOTAL_FLOAT'][0] == 6.6
        assert df['TOTAL_STRING'][0] == 'abc'
        assert df['SOME_DATETIME'][0] == df['Time'][0] + otp.Milli(2)

    def test_inner_for(self, session):
        def another_query_1():
            return otp.Ticks({'A': [1, 2, 3]})

        def another_query_2():
            return otp.Ticks({'B': [4, 5, 6]})

        def fun(tick):
            tick['SUM'] = 0
            for t1 in tick.state_vars['LIST_1']:
                for t2 in tick.state_vars['LIST_2']:
                    tick['SUM'] += t1.get_value('A') * t2.get_value('B')

        data = otp.Tick(A=1)
        data.state_vars['LIST_1'] = otp.state.tick_list(otp.eval(another_query_1))
        data.state_vars['LIST_2'] = otp.state.tick_list(otp.eval(another_query_2))
        data = data.script(fun)
        df = otp.run(data)
        assert df['SUM'][0] == 4 + 5 + 6 + 8 + 10 + 12 + 12 + 15 + 18

    def test_set_long_value(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1, 2, 3],
                'B': [1., 2., 3.],
            })

        def fun(tick):
            for t in tick.state_vars['LIST']:
                t.set_long_value('A', 7)

        def no_fun(tick):
            for t in tick.state_vars['LIST']:
                t.set_long_value('NO_SUCH_FIELD', 1)

        def no_fun_2(tick):
            for t in tick.state_vars['LIST']:
                t.set_long_value('A', 'WRONG_TYPE')

        def no_fun_3(tick):
            for t in tick.state_vars['LIST']:
                t.set_long_value('B', 1)

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.script(no_fun)
        with pytest.raises(ValueError):
            data.script(no_fun_2)
        with pytest.raises(ValueError):
            data.script(no_fun_3)
        data = data.script(fun)
        data = data.state_vars['LIST'].dump()
        df = otp.run(data)
        assert list(df['A']) == [7, 7, 7]

    def test_set_double_value(self, session):
        def another_query():
            return otp.Ticks({
                'A': [1., 2., 3.],
                'B': [1, 2, 3],
            })

        def fun(tick):
            for t in tick.state_vars['LIST']:
                t.set_double_value('A', 7.7)

        def no_fun(tick):
            for t in tick.state_vars['LIST']:
                t.set_double_value('NO_SUCH_FIELD', 1.)

        def no_fun_2(tick):
            for t in tick.state_vars['LIST']:
                t.set_double_value('A', 'WRONG_TYPE')

        def no_fun_3(tick):
            for t in tick.state_vars['LIST']:
                t.set_double_value('B', 1.)

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.script(no_fun)
        with pytest.raises(ValueError):
            data.script(no_fun_2)
        with pytest.raises(ValueError):
            data.script(no_fun_3)
        data = data.script(fun)
        data = data.state_vars['LIST'].dump()
        df = otp.run(data)
        assert list(df['A']) == [7.7, 7.7, 7.7]

    def test_set_string_value(self, session):
        def another_query():
            t = otp.Ticks({
                'A': ['a', 'b', 'c'],
                'B': [1, 2, 3],
                'C': ['a', 'b', 'c'],
            })
            t = t.table(C=otp.string[1], strict=False)
            return t

        def fun(tick):
            for t in tick.state_vars['LIST']:
                t.set_string_value('A', 'X')
                t.set_string_value('C', 'OKAY')

        def no_fun(tick):
            for t in tick.state_vars['LIST']:
                t.set_string_value('NO_SUCH_FIELD', 'X')

        def no_fun_2(tick):
            for t in tick.state_vars['LIST']:
                t.set_string_value('A', 1)

        def no_fun_3(tick):
            for t in tick.state_vars['LIST']:
                t.set_string_value('B', 'X')

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.script(no_fun)
        with pytest.raises(ValueError):
            data.script(no_fun_2)
        with pytest.raises(ValueError):
            data.script(no_fun_3)
        with pytest.warns(Warning, match="Value 'OKAY' will be truncated to 1 characters"):
            data = data.script(fun)
        data = data.state_vars['LIST'].dump()
        df = otp.run(data)
        assert list(df['A']) == ['X', 'X', 'X']
        assert list(df['C']) == ['O', 'O', 'O']

    def test_set_datetime_value(self, session):
        def another_query():
            t = otp.Ticks({'B': [1, 2, 3]})
            t['A'] = t['TIMESTAMP']
            return t

        def fun(tick):
            for t in tick.state_vars['LIST']:
                t.set_datetime_value('A', otp.config['default_start_time'])

        def no_fun(tick):
            for t in tick.state_vars['LIST']:
                t.set_datetime_value('NO_SUCH_FIELD', otp.config['default_start_time'])

        def no_fun_2(tick):
            for t in tick.state_vars['LIST']:
                t.set_datetime_value('A', 1)

        def no_fun_3(tick):
            for t in tick.state_vars['LIST']:
                t.set_datetime_value('B', otp.config['default_start_time'])

        def no_fun_4(tick):
            for t in tick.state_vars['LIST']:
                t.set_datetime_value('TIMESTAMP', otp.config['default_start_time'])

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.script(no_fun)
        with pytest.raises(ValueError):
            data.script(no_fun_2)
        with pytest.raises(ValueError):
            data.script(no_fun_3)
        with pytest.raises(ValueError):
            data.script(no_fun_4)
        data = data.script(fun)
        data = data.state_vars['LIST'].dump()
        df = otp.run(data, timezone='GMT')
        assert list(df['A']) == [otp.config['default_start_time'],
                                 otp.config['default_start_time'],
                                 otp.config['default_start_time']]

    def test_set_value(self, session):
        def another_query():
            t = otp.Ticks({
                'A': [1, 2, 3],
                'B': [1., 2., 3.],
                'C': ['a', 'b', 'c'],
            })
            t['D'] = t['TIMESTAMP']
            return t

        def fun(tick):
            for t in tick.state_vars['LIST']:
                t.set_value('A', 7)
                t.set_value('B', 7.7)
                t.set_value('C', 'X')
                t['D'] = otp.config['default_start_time']

        def no_fun(tick):
            for t in tick.state_vars['LIST']:
                t.set_value('NO_SUCH_FIELD', 1)

        def no_fun_2(tick):
            for t in tick.state_vars['LIST']:
                t.set_value('A', 'WRONG_TYPE')

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.script(no_fun)
        with pytest.raises(ValueError):
            data.script(no_fun_2)
        data = data.script(fun)
        data = data.state_vars['LIST'].dump()
        df = otp.run(data, timezone='GMT')
        assert df['A'][0] == 7
        assert df['B'][0] == 7.7
        assert df['C'][0] == 'X'
        assert df['D'][0] == otp.config['default_start_time']

    def test_set_value_with_operation(self, session):
        def another_query():
            t = otp.Tick(A=0, B=0.0, C='', D=otp.nsectime(0))
            return t

        def fun(tick):
            long_field = 'A'
            long_value = 1
            double_field = 'B'
            double_value = 1.0
            string_field = 'C'
            string_value = 'a'
            datetime_field = 'D'
            datetime_value = tick['TIMESTAMP']
            for t in tick.state_vars['LIST']:
                t.set_long_value(long_field, long_value)
                t.set_double_value(double_field, double_value)
                t.set_string_value(string_field, string_value)
                t.set_datetime_value(datetime_field, datetime_value)

        def fun_wrong_type(tick):
            long_field = 'A'
            long_value = 'a'
            for t in tick.state_vars['LIST']:
                t.set_long_value(long_field, long_value)

        data = otp.Tick(TOTAL_INT=0, TOTAL_FLOAT=0.0, TOTAL_STRING='', SOME_DATETIME=otp.nsectime(0))
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        with pytest.raises(ValueError):
            data.script(fun_wrong_type)
        data = data.script(fun)
        data = data.state_vars['LIST'].dump()
        df = otp.run(data)
        assert df['A'][0] == 1
        assert df['B'][0] == 1.0
        assert df['C'][0] == 'a'
        assert df['D'][0] == df['Time'][0]

    def test_copy_and_get_tick(self, session):
        def fun(tick):
            t1 = otp.tick_deque_tick()
            t2 = otp.tick_deque_tick()
            tick.state_vars['DEQUE'].push_back(tick)
            tick.state_vars['DEQUE'].get_tick(0, t1)
            t1['A'] += 1
            t1.copy(t2)
            t2['A'] += 1
            tick.state_vars['DEQUE'].push_back(t2)

        data = otp.Tick(A=1)
        data.state_vars['DEQUE'] = otp.state.tick_deque()
        data = data.script(fun)
        data = data.state_vars['DEQUE'].dump()
        df = otp.run(data)
        assert list(df['A'] == [3, 3])


class TestDynamicTick:
    def test_fields_access(self, session):
        def fun(tick):
            t = otp.dynamic_tick()
            t['X'] = 12.345
            t['Y'] = 'XD'
            t['Z'] = 0
            if t['Z'] == 0:
                t['Z'] = 777

            tick.state_vars['LIST'].push_back(t)

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list()
        data = data.script(fun)
        assert data.state_vars['LIST'].schema == dict(X=float, Y=str, Z=int)
        data = data.state_vars['LIST'].dump()
        df = otp.run(data)
        assert df['X'][0] == 12.345
        assert df['Y'][0] == 'XD'
        assert df['Z'][0] == 777

    def test_copy_fields(self, session):
        def fun(tick):
            t1 = otp.dynamic_tick()
            t2 = otp.dynamic_tick()
            t1['X'] = 12345
            t1['Y'] = 'abc'
            t2['X'] = 98765
            t2['Y'] = 'xyz'
            t2['Z'] = 1.2345
            t2.copy_fields(t1, ['X'])
            t2['Z'] = 100
            tick.state_vars['LIST'].push_back(t2)

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list()
        data = data.script(fun)
        assert data.state_vars['LIST'].schema == dict(X=int, Z=int)
        data = data.state_vars['LIST'].dump()
        df = otp.run(data)
        assert df['X'][0] == 12345
        assert df['Z'][0] == 100
        assert 'Y' not in df

    def test_attribute(self, session):
        def fun(tick):
            t = otp.dynamic_tick()
            x = t.get_timestamp()  # NOSONAR

        data = otp.Tick(A=1)
        with pytest.raises(AttributeError):
            data.script(fun)

    def test_x(self, session):
        def fun(tick):
            dyn_t = otp.dynamic_tick()
            tick['SUM'] = 0
            for t in tick.state_vars['LIST']:
                tick['SUM'] += t['X']
            dyn_t['X'] = tick['SUM']
            tick.state_vars['LIST'].clear()
            tick.state_vars['LIST'].push_back(dyn_t)

        data = otp.Tick(A=1)
        data.state_vars['LIST'] = otp.state.tick_list(otp.eval(otp.Ticks(X=[1, 2, 3])))
        data = data.script(fun)
        data = data.state_vars['LIST'].dump()
        df = otp.run(data)
        assert df['X'][0] == 6
