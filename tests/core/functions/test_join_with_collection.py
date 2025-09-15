import pytest

import onetick.py as otp
from onetick.py.compatibility import is_supported_nsectime_tick_set_eval


class TestJoinWithCollection:

    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session):
        yield c_session

    def test_inner_outer(self):

        data = otp.Ticks(
            [
                ['A', 'B', 'C'],
                [1, 1, 'A'],
                [1, 2, 'B'],
                [2, 3, 'B'],
                [3, 4, 'C'],
            ]
        )

        def fun(tick):
            tick.state_vars['LIST'].push_back(tick)

        data.state_vars['LIST'] = otp.state.tick_list()
        data = data.script(fun)
        data = data.agg(dict(NUM_TICKS=otp.agg.count()), bucket_time='end')
        data['PARAM'] = 1
        data = data.insert_tick(fields=dict(PARAM=4), insert_before=False)

        def join_func(source, param):
            source, _ = source[source['A'] == param]
            return source

        data_inner = data.join_with_collection(collection_name='LIST', query_func=join_func,
                                               params=dict(param=data['PARAM']), how='inner')

        res_inner = otp.run(data_inner)
        assert len(res_inner) == 2
        assert res_inner['PARAM'][0] == 1
        assert res_inner['A'][0] == 1
        assert res_inner['B'][0] == 1
        assert res_inner['C'][0] == 'A'
        assert res_inner['PARAM'][1] == 1
        assert res_inner['A'][1] == 1
        assert res_inner['B'][1] == 2
        assert res_inner['C'][1] == 'B'

        data_outer = data.join_with_collection(collection_name='LIST', query_func=join_func,
                                               params=dict(param=data['PARAM']), how='outer',
                                               default_fields_for_outer_join=dict(C='DEFAULT'))

        res_outer = otp.run(data_outer)
        assert len(res_outer) == 3
        assert res_outer['PARAM'][0] == 1
        assert res_outer['A'][0] == 1
        assert res_outer['B'][0] == 1
        assert res_outer['C'][0] == 'A'
        assert res_outer['PARAM'][1] == 1
        assert res_outer['A'][1] == 1
        assert res_outer['B'][1] == 2
        assert res_outer['C'][1] == 'B'
        assert res_outer['PARAM'][2] == 4
        assert res_outer['A'][2] == 0
        assert res_outer['B'][2] == 0
        assert res_outer['C'][2] == 'DEFAULT'

    @pytest.mark.parametrize(
        'collection_generator,tick_set',
        [
            (otp.state.tick_set, True),
            (otp.state.tick_set_unordered, True),
            (otp.state.tick_list, False),
            (otp.state.tick_deque, False),
        ]
    )
    def test_collection_types(self, collection_generator, tick_set):

        data = otp.Ticks(
            [
                ['A', 'B', 'C', 'I'],
                [1, 1, 'A', 1],
                [1, 2, 'B', 2],
                [2, 3, 'B', 3],
                [3, 4, 'C', 4],
            ]
        )
        collection_generator_params = {}
        if tick_set:
            collection_generator_params['insertion_policy'] = 'LATEST_TICK'
            collection_generator_params['key_fields'] = 'I'

        if collection_generator is otp.state.tick_set_unordered:
            with pytest.warns(match='expected to be inefficient'):
                data.state_vars['COLL'] = collection_generator(**collection_generator_params)
        else:
            data.state_vars['COLL'] = collection_generator(**collection_generator_params)

        if tick_set:
            data = data.state_vars['COLL'].update()
        else:

            def fun(tick):
                tick.state_vars['COLL'].push_back(tick)

            data = data.script(fun)

        def join_func(source, param):
            source, _ = source[source['B'] == param]
            return source

        data = data.join_with_collection(collection_name='COLL',
                                         query_func=join_func,
                                         prefix='JWC_',
                                         params=dict(param=data['A'])
                                         )
        res = otp.run(data)

        assert len(res) == 4
        assert res['I'][0] == 1
        assert res['JWC_I'][0] == 1
        assert res['I'][1] == 2
        assert res['JWC_I'][1] == 1
        assert res['I'][2] == 3
        assert res['JWC_I'][2] == 2
        assert res['I'][3] == 4
        assert res['JWC_I'][3] == 3

    def test_non_existing_state_var(self):
        src = otp.Empty(schema={'A': int})
        with pytest.raises(KeyError):
            src.join_with_collection(collection_name='NON_EXISTING_VAR')

    def test_non_collection_state_var(self):
        src = otp.Empty(schema={'A': int})
        src.state_vars['ST_VAR'] = otp.state.var(0)
        with pytest.raises(ValueError):
            src.join_with_collection(collection_name='ST_VAR')

    def test_cross_symbol_tick_set(self):
        tick_set_src = otp.Ticks(
            [
                ['I', 'A', 'B'],
                [1, 1, 'A'],
                [2, 1, 'B'],
                [3, 2, 'A'],
                [4, 2, 'A'],
                [5, 2, 'C'],
            ]
        )
        main_src = otp.Tick(K=1, L=2, M=3)
        main_src.state_vars['TICK_SET'] = otp.state.tick_set('LATEST_TICK', 'I', default_value=otp.eval(tick_set_src),
                                                             scope='cross_symbol')

        def join_func_1(source, param_a):
            return source[source['A'] == param_a][0]

        def join_func_2(source, param_b):
            return source[source['B'] == param_b][0]

        main_src_k = main_src.join_with_collection('TICK_SET', join_func_1,
                                                   prefix='JWC_K_', params=dict(param_a=main_src['K']),
                                                   default_fields_for_outer_join=dict(I=-1))
        res_k = otp.run(main_src_k)
        assert len(res_k) == 2
        assert res_k['JWC_K_I'][0] == 1
        assert res_k['JWC_K_I'][1] == 2

        main_src_l = main_src.join_with_collection('TICK_SET', join_func_1,
                                                   prefix='JWC_L_', params=dict(param_a=main_src['L']),
                                                   default_fields_for_outer_join=dict(I=-1))
        res_l = otp.run(main_src_l)
        assert len(res_l) == 3
        assert res_l['JWC_L_I'][0] == 3
        assert res_l['JWC_L_I'][1] == 4
        assert res_l['JWC_L_I'][2] == 5

        main_src_m = main_src.join_with_collection('TICK_SET', join_func_1,
                                                   prefix='JWC_M_', params=dict(param_a=main_src['M']),
                                                   default_fields_for_outer_join=dict(I=-1))
        res_m = otp.run(main_src_m)
        assert len(res_m) == 1
        assert res_m['JWC_M_I'][0] == -1

        main_src_symb = main_src.join_with_collection('TICK_SET', join_func_2,
                                                      prefix='JWC_SYMB_', params=dict(param_b=main_src['_SYMBOL_NAME']),
                                                      default_fields_for_outer_join=dict(I=-1))
        res_symb = otp.run(main_src_symb, symbols=['A', 'B', 'D'])
        assert len(res_symb) == 3

        res_symb_a = res_symb['A']
        assert len(res_symb_a) == 3
        assert res_symb_a['JWC_SYMB_I'][0] == 1
        assert res_symb_a['JWC_SYMB_I'][1] == 3
        assert res_symb_a['JWC_SYMB_I'][2] == 4

        res_symb_b = res_symb['B']
        assert len(res_symb_b) == 1
        assert res_symb_b['JWC_SYMB_I'][0] == 2

        res_symb_d = res_symb['D']
        assert len(res_symb_d) == 1
        assert res_symb_d['JWC_SYMB_I'][0] == -1

    def test_start_end_time(self):
        date = otp.datetime(2023, 6, 7)
        tick_set_src = otp.Ticks(
            [
                ['offset', 'I', 'A', 'B'],
                [1000, 1, 1, 'A'],
                [2000, 2, 1, 'B'],
                [3000, 3, 2, 'A'],
                [4000, 4, 2, 'A'],
                [5000, 5, 2, 'C'],
            ]
        )

        main_src = otp.Tick(X=1)

        main_src.state_vars['TICK_SET'] = otp.state.tick_set('LATEST_TICK', 'I',
                                                             default_value=otp.eval(tick_set_src,
                                                                                    start=date,
                                                                                    end=date + otp.Day(1)),
                                                             scope='cross_symbol')

        main_src_1 = main_src.join_with_collection(collection_name='TICK_SET', how='inner')

        # if start and end time in join_with_collection are empty, then ticks are not selected based on time
        # upon joining
        res_1 = otp.run(main_src_1, start=date, end=date + otp.Day(1))
        assert len(res_1) == 5
        res_2 = otp.run(main_src_1, start=date + otp.Day(1), end=date + otp.Day(2))
        assert len(res_2) == 5
        res_3 = otp.run(main_src_1, start=date + otp.Second(1), end=date + otp.Second(6))
        assert len(res_3) == 5
        res_4 = otp.run(main_src_1, start=date + otp.Second(2), end=date + otp.Second(5))
        assert len(res_4) == 5

        main_src_2 = main_src.join_with_collection(collection_name='TICK_SET', how='inner',
                                                   start=main_src['_START_TIME'], end=main_src['_END_TIME'])

        # if start and end time in join_with_collection are specified, then joined ticks are filtered
        # according to their timestamps
        res_1 = otp.run(main_src_2, start=date, end=date + otp.Day(1))
        assert len(res_1) == 5
        res_2 = otp.run(main_src_2, start=date + otp.Day(1), end=date + otp.Day(2))
        assert len(res_2) == 0
        res_3 = otp.run(main_src_2, start=date + otp.Second(1), end=date + otp.Second(6))
        assert len(res_3) == 5
        res_4 = otp.run(main_src_2, start=date + otp.Second(2), end=date + otp.Second(5))
        assert len(res_4) == 3

        # start and end times can also be taken from tick fields
        main_src_3 = main_src.copy()
        main_src_3['ST'] = main_src_3['_START_TIME'] + otp.Second(2)
        main_src_3['ET'] = main_src_3['_START_TIME'] + otp.Second(5)
        main_src_3 = main_src_3.join_with_collection(collection_name='TICK_SET', how='inner',
                                                     start=main_src_3['ST'], end=main_src_3['ET'])
        res = otp.run(main_src_3, start=date, end=date + otp.Day(1))
        assert len(res) == 3

    @pytest.mark.parametrize(
        'collection_generator,tick_set',
        [
            (otp.state.tick_set, True),
            (otp.state.tick_set_unordered, True),
            (otp.state.tick_list, False),
            (otp.state.tick_deque, False),
        ]
    )
    def test_start_end_nsectime(self, collection_generator, tick_set):
        date = otp.datetime(2023, 6, 7)
        tick_set_src = otp.Ticks(
            [
                ['offset', 'I'],
                [1000, 1],
                [1000, 2],
                [1000, 3],
                [1000, 4],
                [1000, 5],
                [1000, 6],
            ]
        )
        tick_set_src['TIMESTAMP'] = tick_set_src['TIMESTAMP'] + otp.Nano(120) + otp.Nano(tick_set_src['I'])

        main_src = otp.Tick(X=1)

        collection_generator_params = dict(
            default_value=otp.eval(
                tick_set_src,
                start=date,
                end=date + otp.Day(1)),
        )
        if tick_set:
            collection_generator_params['insertion_policy'] = 'LATEST_TICK'
            collection_generator_params['key_fields'] = 'I'

        if collection_generator is otp.state.tick_set_unordered:
            with pytest.warns(match='expected to be inefficient'):
                main_src.state_vars['COLL'] = collection_generator(**collection_generator_params)
        else:
            main_src.state_vars['COLL'] = collection_generator(**collection_generator_params)

        main_src['ST'] = main_src['_START_TIME'] + otp.Second(1) + otp.Nano(123)
        main_src['ET'] = main_src['_START_TIME'] + otp.Second(1) + otp.Nano(126)

        main_src = main_src.join_with_collection(collection_name='COLL', how='inner',
                                                 start=main_src['ST'], end=main_src['ET'])
        res = otp.run(main_src, start=date, end=date + otp.Day(1))
        if tick_set and not is_supported_nsectime_tick_set_eval():
            # BDS-321: nsectime is truncated to msectime for ticksets loaded from eval()
            assert len(res) == 0
        else:
            assert len(res) == 3

    @pytest.mark.parametrize(
        'collection_generator,tick_set',
        [
            (otp.state.tick_set, True),
            (otp.state.tick_set_unordered, True),
            (otp.state.tick_list, False),
            (otp.state.tick_deque, False),
        ]
    )
    def test_nsectime_precision_in_collection(self, collection_generator, tick_set):
        date = otp.datetime(2023, 6, 7)
        src = otp.Tick(A=1)
        src['TIMESTAMP'] = src['TIMESTAMP'] + otp.Nano(123456789)
        collection_generator_params = {}
        if tick_set:
            collection_generator_params['insertion_policy'] = 'LATEST_TICK'
            collection_generator_params['key_fields'] = 'A'

        if collection_generator is otp.state.tick_set_unordered:
            with pytest.warns(match='expected to be inefficient'):
                src.state_vars['COLL'] = collection_generator(**collection_generator_params)
        else:
            src.state_vars['COLL'] = collection_generator(**collection_generator_params)

        if tick_set:
            src = src.state_vars['COLL'].update()
        else:

            def fun(tick):
                tick.state_vars['COLL'].push_back(tick)

            src = src.script(fun)

        def join_func(source):
            source['JWC_TS'] = source['TIMESTAMP']
            source = source[['JWC_TS']]
            return source

        src = src.join_with_collection(collection_name='COLL',
                                       query_func=join_func,
                                       )
        res = otp.run(src, start=date, end=date + otp.Day(1))
        assert len(res) == 1
        assert res['JWC_TS'][0] == date + otp.Nano(123456789)

    @pytest.mark.parametrize(
        'collection_generator,tick_set',
        [
            (otp.state.tick_set, True),
            (otp.state.tick_set_unordered, True),
            (otp.state.tick_list, False),
            (otp.state.tick_deque, False),
        ]
    )
    def test_nsectime_precision_in_collection_with_eval(self, collection_generator, tick_set):
        date = otp.datetime(2023, 6, 7)
        src = otp.Tick(A=1)
        src['TIMESTAMP'] = src['TIMESTAMP'] + otp.Nano(123456789)
        collection_generator_params = dict(
            default_value=otp.eval(src, start=date, end=date + otp.Day(1)),
            scope='cross_symbol',
        )
        if tick_set:
            collection_generator_params['insertion_policy'] = 'LATEST_TICK'
            collection_generator_params['key_fields'] = 'A'

        if collection_generator is otp.state.tick_set_unordered:
            with pytest.warns(match='expected to be inefficient'):
                src.state_vars['COLL'] = collection_generator(**collection_generator_params)
        else:
            src.state_vars['COLL'] = collection_generator(**collection_generator_params)

        def join_func(source):
            source['JWC_TS'] = source['TIMESTAMP']
            source = source[['JWC_TS']]
            return source

        src = src.join_with_collection(collection_name='COLL',
                                       query_func=join_func,
                                       )
        res = otp.run(src, start=date, end=date + otp.Day(1))
        assert len(res) == 1
        if tick_set and not is_supported_nsectime_tick_set_eval():
            # BDS-321: nsectime is truncated to msectime for ticksets loaded from eval()
            assert res['JWC_TS'][0] == date + otp.Milli(123)
        else:
            assert res['JWC_TS'][0] == date + otp.Nano(123456789)

    def test_caching(self):
        """
        Testing that it runs at least
        """
        src = otp.Tick(A=1)
        src.state_vars['TICK_SET'] = otp.state.tick_set('LATEST_TICK', 'A')
        src = src.state_vars['TICK_SET'].update()
        src = src.join_with_collection('TICK_SET', caching='per_symbol', prefix='JWC_')
        res = otp.run(src)
        assert len(res) == 1
        assert res['A'][0] == 1
        assert res['JWC_A'][0] == 1

    def test_keep_time(self):
        src = otp.Tick(A=1)
        src.state_vars['TICK_SET'] = otp.state.tick_set('LATEST_TICK', 'A')
        src = src.state_vars['TICK_SET'].update()

        def join_func(source):
            source['TIMESTAMP'] = source['TIMESTAMP'] + otp.Nano(123)
            return source

        src = src.join_with_collection('TICK_SET', query_func=join_func, prefix='JWC_', keep_time='TS')
        res = otp.run(src)
        assert len(res) == 1
        assert res['A'][0] == 1
        assert res['JWC_A'][0] == 1
        assert res['JWC_TS'][0] == otp.config.default_start_time + otp.Nano(123)
