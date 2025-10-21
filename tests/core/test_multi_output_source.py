import onetick.py as otp
import pytest


class TestMultiOutputSource:

    @pytest.fixture(scope='class', autouse=True)
    def c_session(self, session):
        yield session

    @pytest.mark.parametrize(
        'how_to_run', ['query', 'dict', 'multi_output_source']
    )
    def test_simple(self, how_to_run):
        src = otp.Tick(A=1)
        branch1 = src.copy(0)
        branch1['B'] = 2
        branch2 = src.copy()
        branch2['B'] = 'ABC'
        if how_to_run == 'dict':
            query = dict(BRANCH1=branch1, BRANCH2=branch2)
        elif how_to_run == 'multi_output_source':
            query = otp.MultiOutputSource(dict(BRANCH1=branch1, BRANCH2=branch2))
        elif how_to_run == 'query':
            query = otp.MultiOutputSource(dict(BRANCH1=branch1, BRANCH2=branch2)).to_otq()
        dfs = otp.run(query)
        assert dfs['BRANCH1']['B'][0] == 2
        assert dfs['BRANCH2']['B'][0] == 'ABC'
        dfs = otp.run(query, node_name=['BRANCH1', 'BRANCH2'])
        assert dfs['BRANCH1']['B'][0] == 2
        assert dfs['BRANCH2']['B'][0] == 'ABC'
        df = otp.run(query, node_name='BRANCH1')
        assert df['B'][0] == 2
        df = otp.run(query, node_name='BRANCH2')
        assert df['B'][0] == 'ABC'

    def test_branch_name_not_found(self):
        src = otp.Tick(A=1)
        branch1 = src.copy(0)
        branch1['B'] = 2
        branch2 = src.copy()
        branch2['B'] = 'ABC'
        with pytest.raises(ValueError, match='Branch name "OTHER" not found'):
            otp.MultiOutputSource(dict(BRANCH1=branch1, BRANCH2=branch2),
                                  main_branch_name="OTHER")

    def test_not_enough_branches(self):
        with pytest.raises(ValueError, match='At least one branch should be passed to a MultiOutputSource object'):
            otp.MultiOutputSource({})

    def test_disconnected_outputs(self):
        src1 = otp.Tick(A=1)
        src2 = otp.Tick(A=2)
        with pytest.raises(ValueError, match='Cannot construct a MultiOutputSource object from outputs '
                                             'that are not connected'):
            otp.MultiOutputSource(dict(SOURCE1=src1, SOURCE2=src2))

        branch1 = src1.copy()
        branch1['B'] = 1
        branch2 = src1.copy()
        branch2['B'] = 2
        branch3 = src2.copy()
        branch3['B'] = 1
        branch4 = src2.copy()
        branch4['B'] = 2
        with pytest.raises(ValueError, match='Cannot construct a MultiOutputSource object from outputs '
                                             'that are not connected'):
            otp.MultiOutputSource(dict(BRANCH1=branch1, BRANCH2=branch2, BRANCH3=branch3, BRANCH4=branch4))

    @pytest.mark.parametrize(
        'save_before_running', [True, False]
    )
    @pytest.mark.parametrize('main_branch', ['OUT1', 'OUT2'])
    def test_branch_symbols(self, main_branch, save_before_running):
        """
        We create two branches with bound symbols and attach an unbound branch to one of them,
        then check if the symbol has been applied correctly
        """
        src1 = otp.Tick(A=1, offset=1, symbol='AAPL')
        src1['SN'] = src1['_SYMBOL_NAME']
        src2 = otp.Tick(A=2, offset=2, symbol='MSFT')
        src2['SN'] = src2['_SYMBOL_NAME']
        src = src1 + src2
        branch1 = src.copy()
        branch2 = src.copy()
        src3 = otp.Tick(A=3, offset=3)
        src3['SN'] = src3['_SYMBOL_NAME']
        branch2 += src3
        mbs = otp.MultiOutputSource(dict(OUT2=branch2, OUT1=branch1), main_branch_name=main_branch)
        if save_before_running:
            dfs = otp.run(mbs.to_otq(symbols='TSLA'))
        else:
            dfs = otp.run(mbs, symbols='TSLA')
        df_1 = dfs['OUT1']
        df_2 = dfs['OUT2']
        assert len(df_1) == 2
        assert df_1['SN'][0] == 'AAPL'
        assert df_1['SN'][1] == 'MSFT'
        assert len(df_2) == 3
        assert df_2['SN'][0] == 'AAPL'
        assert df_2['SN'][1] == 'MSFT'
        assert df_2['SN'][2] == 'TSLA'

    @pytest.mark.parametrize(
        'save_before_running', [True, False]
    )
    @pytest.mark.parametrize(
        'main_branch_name', [None, 'PAST', 'PRESENT', 'FUTURE']
    )
    def test_branch_times(self, main_branch_name, save_before_running):
        """
        we create three branches, one of which has time interval shifted to the future and another to the past
        then we check that MultiOutputSource correctly runs
        """
        start_date = otp.datetime(2016, 1, 1)
        end_date = otp.datetime(2016, 1, 2)

        start_past_period = start_date
        end_past_period = start_date + otp.Second(15)
        start_present_period = start_date + otp.Second(15)
        end_present_period = start_date + otp.Second(25)
        start_future_period = start_date + otp.Second(25)
        end_future_period = start_date + otp.Second(30)

        main_source = otp.Tick(A=1, bucket_interval=1, start=start_present_period, end=end_present_period)
        main_branch = main_source.copy()
        left_branch = main_source.copy()
        past_branch = otp.Tick(A=0, bucket_interval=1, start=start_past_period, end=end_past_period)
        left_branch = left_branch + past_branch
        right_branch = main_source.copy()
        future_branch = otp.Tick(A=2, bucket_interval=1, start=start_future_period, end=end_future_period)
        right_branch = right_branch + future_branch

        src = otp.MultiOutputSource(dict(PAST=left_branch,
                                         PRESENT=main_branch,
                                         FUTURE=right_branch),
                                    main_branch_name=main_branch_name)
        if save_before_running:
            dfs = otp.run(src.to_otq(), start=start_date, end=end_date)
        else:
            dfs = otp.run(src)
        assert len(dfs['PAST']) == 25
        assert list(dfs['PAST']['A'])[0:15] == [0] * 15
        assert list(dfs['PAST']['A'])[15:25] == [1] * 10
        assert len(dfs['PRESENT']) == 10
        assert list(dfs['PRESENT']['A']) == [1] * 10
        assert len(dfs['FUTURE']) == 15
        assert list(dfs['FUTURE']['A'])[0:10] == [1] * 10
        assert list(dfs['FUTURE']['A'])[10:15] == [2] * 5

    def test_no_common_root(self):
        source_a = otp.Tick(offset=0, FIELD='A')
        source_b = otp.Tick(offset=1, FIELD='B')
        branch_1 = source_a.copy()
        branch_2 = source_a + source_b
        branch_3 = source_b.copy()
        src = otp.MultiOutputSource(dict(BRANCH1=branch_1, BRANCH2=branch_2, BRANCH3=branch_3))
        res = otp.run(src)
        assert len(res) == 3
        assert len(res['BRANCH1']) == 1
        assert res['BRANCH1']['FIELD'][0] == 'A'
        assert len(res['BRANCH2']) == 2
        assert res['BRANCH2']['FIELD'][0] == 'A'
        assert res['BRANCH2']['FIELD'][1] == 'B'
        assert len(res['BRANCH3']) == 1
        assert res['BRANCH3']['FIELD'][0] == 'B'

    @pytest.mark.parametrize(
        'join_type', ['join', 'same_size', 'join_by_time']
    )
    def test_join(self, join_type):
        source_a = otp.Tick(A_FIELD='A')
        source_b = otp.Tick(B_FIELD='B')
        branch_1 = source_a.copy()
        if join_type == 'join':
            branch_2 = otp.join(source_a, source_b, how='inner', on='all')
        elif join_type == 'same_size':
            branch_2 = otp.join(source_a, source_b, how='inner', on='same_size')
        elif join_type == 'join_by_time':
            branch_2 = otp.join_by_time([source_a, source_b], match_if_identical_times=True)
        src = otp.MultiOutputSource(dict(BRANCH1=branch_1, BRANCH2=branch_2))
        res = otp.run(src)
        assert len(res) == 2
        assert len(res['BRANCH1']) == 1
        assert res['BRANCH1']['A_FIELD'][0] == 'A'
        assert len(res['BRANCH2']) == 1
        assert res['BRANCH2']['A_FIELD'][0] == 'A'
        assert res['BRANCH2']['B_FIELD'][0] == 'B'

    def test_get_branch(self):
        src = otp.Tick(A=1)
        branch1 = src.copy(0)
        branch1['B'] = 2
        branch2 = src.copy()
        branch2['C'] = 2

        query = otp.MultiOutputSource(dict(BRANCH1=branch1, BRANCH2=branch2))

        assert set(query.get_branch('BRANCH1').schema) == {'A', 'B'}
        assert set(query.get_branch('BRANCH2').schema) == {'A', 'C'}

        with pytest.raises(ValueError, match='Branch name "BRANCH3" not found among the outputs!'):
            query.get_branch('BRANCH3')

    def test_main_branch(self):
        src = otp.Tick(A=1)
        branch1 = src.copy(0)
        branch1['B'] = 2
        branch2 = src.copy()
        branch2['C'] = 2

        query = otp.MultiOutputSource(dict(BRANCH1=branch1, BRANCH2=branch2), main_branch_name='BRANCH2')

        assert set(query.main_branch.schema) == {'A', 'C'}
