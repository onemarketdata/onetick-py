import pytest
import tempfile

import onetick.py as otp
from onetick.py import types as ott
from onetick.py.otq import otq

from onetick.py.compatibility import (
    is_save_snapshot_database_parameter_supported,
    is_join_with_snapshot_snapshot_fields_parameter_supported,
)


if (
        not hasattr(otq, "ReadSnapshot") or
        not hasattr(otq, "SaveSnapshot") or
        not is_save_snapshot_database_parameter_supported()
):
    pytest.skip("Current version of OneTick doesn't support basic snapshots related EPs", allow_module_level=True)


@pytest.mark.skipif(
    not hasattr(otq, "ShowSnapshotList"), reason="Current version of OneTick doesn't support SHOW_SNAPSHOT_LIST EP",
)
class TestShowSnapshotList:
    @pytest.fixture(autouse=True, scope='class')
    def db_session(self):
        with otp.Session() as session:
            with tempfile.TemporaryDirectory() as tmpdir:
                db = otp.DB('SNAPSHOT_LIST_DB', db_properties={'SAVE_SNAPSHOT_DIR': tmpdir})
                session.use(db)
                yield session

    @pytest.mark.parametrize('snapshot_storage', [None, 'memory', 'memory_mapped_file', 'all'])
    def test_base(self, snapshot_storage):
        # create snapshots
        data = otp.Ticks(X=[1, 2]).save_snapshot(snapshot_name='test_1', num_ticks=2, keep_snapshot_after_query=True)
        otp.run(data)

        data = otp.Ticks(Y=[1, 2]).save_snapshot(snapshot_name='test_2', num_ticks=2, keep_snapshot_after_query=True)
        otp.run(data)

        data = otp.Ticks(Z=[1, 2]).save_snapshot(
            snapshot_name='test_3', snapshot_storage='memory_mapped_file', num_ticks=2, database='SNAPSHOT_LIST_DB',
        )
        otp.run(data)

        kwargs = {}
        if snapshot_storage:
            kwargs['snapshot_storage'] = snapshot_storage

        src = otp.ShowSnapshotList(**kwargs)
        df = otp.run(src).to_dict(orient='list')
        assert set(df.keys()) == {'Time', 'SNAPSHOT_NAME', 'STORAGE_TYPE', 'DB_NAME'}
        del df['Time']

        result_data = {
            'SNAPSHOT_NAME': ['test_1', 'test_2', 'test_3'],
            'STORAGE_TYPE': ['MEMORY', 'MEMORY', 'MEMORY_MAPPED_FILE'],
        }
        if snapshot_storage is None:
            snapshot_storage = 'all'

        snapshot_storage = snapshot_storage.upper()

        expected_result = {'SNAPSHOT_NAME': [], 'STORAGE_TYPE': []}
        for i in range(len(result_data['STORAGE_TYPE'])):
            if result_data['STORAGE_TYPE'][i] == snapshot_storage or snapshot_storage == 'ALL':
                expected_result['SNAPSHOT_NAME'].append(result_data['SNAPSHOT_NAME'][i])
                expected_result['STORAGE_TYPE'].append(result_data['STORAGE_TYPE'][i])

        if snapshot_storage in ['ALL', 'MEMORY_MAPPED_FILE']:
            assert df['DB_NAME'][-1] == 'SNAPSHOT_LIST_DB'

        del df['DB_NAME']
        assert expected_result == df

    @pytest.mark.filterwarnings("ignore:__call__")
    def test_call(self):
        otp.run(otp.Ticks(X=[1]).save_snapshot(snapshot_name='test_call', keep_snapshot_after_query=True))

        df = otp.ShowSnapshotList()(symbols=f'{otp.config.default_db}::').to_dict(orient='list')
        assert set(df.keys()) == {'Time', 'SNAPSHOT_NAME', 'STORAGE_TYPE', 'DB_NAME'}

        assert all([True for i in range(len(df['Time'])) if df['SNAPSHOT_NAME'][i] == 'test_call'])

    def test_exceptions(self):
        with pytest.raises(ValueError, match='must be one of'):
            _ = otp.ShowSnapshotList(snapshot_storage='TEST')


class TestReadWriteSnapshots:
    @pytest.fixture
    def data(self):
        return otp.Ticks(X=[1, 2, 3, 4, 5], Y=[10, 5, 7, 9, 11])

    @pytest.fixture(autouse=True, scope='class')
    def db_session(self, session):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = otp.DB('SNAPSHOT_DB', db_properties={'SAVE_SNAPSHOT_DIR': tmpdir})
            session.use(db)
            yield session

    def get_result(self, src):
        df = otp.run(src).to_dict(orient='list')
        del df['Time']
        del df['TICK_TIME']

        return df

    def test_base(self, data):
        data = data.save_snapshot(snapshot_name='test', num_ticks=5, keep_snapshot_after_query=True)
        assert data.schema == {}
        otp.run(data)

        snapshot_data = otp.ReadSnapshot(snapshot_name='test')
        df = self.get_result(snapshot_data)

        assert df == {'X': [1, 2, 3, 4, 5], 'Y': [10, 5, 7, 9, 11]}

    def test_write_to_file(self, data):
        snapshot_name = 'test_to_file'
        data = data.save_snapshot(
            snapshot_name=snapshot_name, snapshot_storage='memory_mapped_file', num_ticks=5,
            database='SNAPSHOT_DB',
        )
        otp.run(data)

        snapshot_data = otp.ReadSnapshot(
            snapshot_name=snapshot_name, snapshot_storage='memory_mapped_file', db='SNAPSHOT_DB',
        )
        df = self.get_result(snapshot_data)

        assert df == {'X': [1, 2, 3, 4, 5], 'Y': [10, 5, 7, 9, 11]}

    def test_database(self):
        src = otp.Ticks(X=[1, 2, 3], Y=[4, 5, 6])
        src = src.save_snapshot(
            snapshot_name='test_db', database='SNAPSHOT_DB', num_ticks=5, keep_snapshot_after_query=True,
        )
        otp.run(src)

        snapshot_data = otp.ReadSnapshot(snapshot_name='test_db', db='SNAPSHOT_DB')
        df = self.get_result(snapshot_data)

        assert df == {'X': [1, 2, 3], 'Y': [4, 5, 6]}

        src = otp.ShowSnapshotList()
        df = otp.run(src).to_dict(orient='list')

        assert 'test_db' in df['SNAPSHOT_NAME']
        assert all(
            df['DB_NAME'][i] == 'SNAPSHOT_DB' for i in range(len(df['SNAPSHOT_NAME']))
            if df['SNAPSHOT_NAME'][i] == 'test_db'
        )

    def test_missing_snapshot(self):
        data = otp.ReadSnapshot(snapshot_name='missing', allow_snapshot_absence=True)
        df = otp.run(data)
        assert df.empty

    def test_symbol_name_field(self):
        src = otp.Ticks(X=[1, 2, 3], TEST=['ABC'] * 3)
        src = src.save_snapshot(
            snapshot_name='test_symbol_name', num_ticks=3, symbol_name_field='TEST', keep_snapshot_after_query=True,
        )
        otp.run(src)

        snapshot_data = otp.ReadSnapshot(snapshot_name='test_symbol_name', symbol='ABC')
        df = self.get_result(snapshot_data)
        assert df == {'X': [1, 2, 3]}

    def test_group_by(self):
        src = otp.Ticks(X=[1, 2, 3, 4, 5, 6], Y=[1, 1, 0, 0, 1, 0])
        src = src.save_snapshot(
            snapshot_name='test_group_by', num_ticks=2, keep_snapshot_after_query=True,
            group_by=[src['Y']],
        )
        otp.run(src)

        snapshot_data = otp.ReadSnapshot(snapshot_name='test_group_by')
        df = self.get_result(snapshot_data)
        assert df == {'X': [2, 5, 4, 6], 'Y': [1, 1, 0, 0]}

    def test_group_by_multi(self):
        src = otp.Ticks(A=[1, 2, 3, 4, 5, 6], B=[1, 1, 0, 0, 0, 1], C=[1, 2, 1, 2, 1, 2])
        src = src.save_snapshot(
            snapshot_name='test_group_by', num_ticks=1, keep_snapshot_after_query=True,
            group_by=[src['B'], 'C'],
        )
        otp.run(src)

        snapshot_data = otp.ReadSnapshot(snapshot_name='test_group_by')
        df = self.get_result(snapshot_data)
        assert df == {'A': [1, 6, 5, 4], 'B': [1, 1, 0, 0], 'C': [1, 2, 1, 2]}

    def test_group_by_symbols(self):
        src = otp.Ticks(X=[1, 2, 3, 4, 5, 6], Y=[1, 1, 0, 0, 1, 0], S=['A', 'B', 'A', 'B', 'A', 'B'])
        src = src.save_snapshot(
            snapshot_name='test_group_by', num_ticks=1, keep_snapshot_after_query=True,
            group_by=[src['Y']], symbol_name_field=src['S'],
        )
        otp.run(src)

        snapshot_data = otp.ReadSnapshot(snapshot_name='test_group_by')
        df = otp.run(snapshot_data, symbols=['A', 'B'])

        expected_result = {'A': {'X': [5, 3], 'Y': [1, 0]}, 'B': {'X': [2, 6], 'Y': [1, 0]}}
        assert df.keys() == expected_result.keys()
        for key in df:
            current_df = df[key].to_dict(orient='list')
            del current_df['Time']
            del current_df['TICK_TIME']
            assert current_df == expected_result[key]

    def test_remove_snapshot_upon_start_memory(self):
        snapshot_name = 'test_remove'
        ticks_data = otp.Ticks(X=[1, 2, 3])
        src = ticks_data.copy().save_snapshot(snapshot_name=snapshot_name, num_ticks=10, keep_snapshot_after_query=True)
        otp.run(src)

        # save once again, remove_snapshot_upon_start=True, check if it was overridden
        src = ticks_data.copy().save_snapshot(
            snapshot_name=snapshot_name, num_ticks=10, keep_snapshot_after_query=True, remove_snapshot_upon_start=True,
        )
        otp.run(src)
        snapshot_data = otp.ReadSnapshot(snapshot_name=snapshot_name)
        df = otp.run(snapshot_data)
        assert list(df['X']) == [1, 2, 3]

        # save once again, remove_snapshot_upon_start=None (NOT_SET), check if it was overridden
        src = ticks_data.copy().save_snapshot(
            snapshot_name=snapshot_name, num_ticks=10, keep_snapshot_after_query=True, remove_snapshot_upon_start=None,
        )
        otp.run(src)
        snapshot_data = otp.ReadSnapshot(snapshot_name=snapshot_name)
        df = otp.run(snapshot_data)
        assert list(df['X']) == [1, 2, 3]

        # save once again, remove_snapshot_upon_start=False, check if it appended
        src = ticks_data.copy().save_snapshot(
            snapshot_name=snapshot_name, num_ticks=10, keep_snapshot_after_query=True, remove_snapshot_upon_start=False,
        )
        otp.run(src)
        snapshot_data = otp.ReadSnapshot(snapshot_name=snapshot_name)
        df = otp.run(snapshot_data)
        assert list(df['X']) == [1, 2, 3, 1, 2, 3]

    def test_exceptions(self, data):
        with pytest.raises(ValueError, match='not in schema'):
            _ = data.save_snapshot(snapshot_name='test', symbol_name_field='MISSING')

        with pytest.raises(ValueError, match='not in schema'):
            _ = data.save_snapshot(snapshot_name='test', group_by=['MISSING'])

        with pytest.raises(ValueError, match='must be one of'):
            _ = data.save_snapshot(snapshot_name='test', snapshot_storage='TEST')

        with pytest.raises(ValueError, match='must be a list'):
            _ = data.save_snapshot(snapshot_name='test', group_by=data['X'])

        with pytest.raises(ValueError, match='must be one of'):
            _ = otp.ReadSnapshot(snapshot_name='test', snapshot_storage='TEST')


@pytest.mark.skipif(
    not hasattr(otq, "JoinWithSnapshot"), reason="Current version of OneTick doesn't support JOIN_WITH_SNAPSHOT EP",
)
class TestJoinWithSnapshot:
    @pytest.fixture(scope='class')
    def db_session(self, session):
        db = otp.DB(name='TEST_JOIN_DB')
        session.use(db)
        db.add(otp.Ticks(X=[1, 2, 3], Y=[4, 5, 3]), symbol='AAA')

    @pytest.fixture
    def snapshot(self, db_session):
        data = otp.Ticks(X=[1, 2, 3], Y=[4, 5, 3])
        data = data.save_snapshot(
            snapshot_name='test_join_with_snapshot', snapshot_storage='memory', num_ticks=100,
            keep_snapshot_after_query=True, database='TEST_JOIN_DB',
        )
        otp.run(data)

    def get_result(self, src, prefix=''):
        df = otp.run(src).to_dict(orient='list')
        del df[f'{prefix}TICK_TIME']
        del df['Time']
        return df

    @pytest.mark.parametrize('use_prefix', [True, False])
    def test_base(self, snapshot, use_prefix):
        kwargs = {}
        prefix = ''

        if use_prefix:
            prefix = 'T.'
            kwargs['prefix_for_output_ticks'] = prefix

        data = otp.Ticks(A=[1, 2])
        data = data.join_with_snapshot(
            snapshot_name='test_join_with_snapshot', database='TEST_JOIN_DB', **kwargs,
        )
        df = self.get_result(data, prefix)

        assert df == {'A': [1, 1, 1, 2, 2, 2], f'{prefix}X': [1, 2, 3, 1, 2, 3], f'{prefix}Y': [4, 5, 3, 4, 5, 3]}

    def test_base_ds(self, db_session):
        # create snapshot
        data = otp.DataSource(db='TEST_JOIN_DB', symbol='AAA')
        data = data.save_snapshot(
            snapshot_name='test_join_with_snapshot_ds', snapshot_storage='memory', num_ticks=100,
            keep_snapshot_after_query=True,
        )
        otp.run(data)

        data = otp.Ticks(A=[1, 2], symbol='AAA')
        data = data.join_with_snapshot(
            snapshot_name='test_join_with_snapshot_ds', snapshot_storage='memory', database='TEST_JOIN_DB',
        )
        df = self.get_result(data)

        assert df == {'A': [1, 1, 1, 2, 2, 2], 'X': [1, 2, 3, 1, 2, 3], 'Y': [4, 5, 3, 4, 5, 3]}

    def test_join_missing(self, db_session):
        data = otp.Ticks(A=[1, 2])
        data = data.join_with_snapshot(
            snapshot_name='missing', snapshot_storage='memory', database='TEST_JOIN_DB',
            allow_snapshot_absence=True,
        )
        df = otp.run(data)
        assert df.empty

    @pytest.mark.skipif(
        not is_join_with_snapshot_snapshot_fields_parameter_supported(),
        reason="Current version of OneTick doesn't support parameter `snapshot_fields` in JOIN_WITH_SNAPSHOT EP",
    )
    def test_snapshot_fields(self, snapshot):
        data = otp.Ticks(A=[1, 2])
        data = data.join_with_snapshot(
            snapshot_name='test_join_with_snapshot', snapshot_storage='memory', database='TEST_JOIN_DB',
            snapshot_fields=['Y', 'TICK_TIME'],
        )
        df = self.get_result(data)
        assert df == {'A': [1, 1, 1, 2, 2, 2], 'Y': [4, 5, 3, 4, 5, 3]}

    def test_default_fields_for_outer_join(self, db_session):
        data = otp.Ticks(A=[1, 2])
        data = data.join_with_snapshot(
            snapshot_name='missing_snapshot', snapshot_storage='memory', database='TEST_JOIN_DB',
            allow_snapshot_absence=True,
            default_fields_for_outer_join={
                'A_COPY': data['A'],  # column
                'X': 1,
                'Y': data['A'] * 2,  # operation
            },
        )

        assert data.schema == {'A': int, 'A_COPY': int, 'X': int, 'Y': int}

        df = otp.run(data).to_dict(orient='list')
        del df['Time']

        assert df == {'A': [1, 2], 'X': [1.0, 1.0], 'Y': [2, 4], 'A_COPY': [1, 2]}

    @pytest.mark.parametrize('use_prefix', [True, False])
    def test_default_fields_for_outer_join_with_types(self, db_session, use_prefix):
        prefix = ''
        if use_prefix:
            prefix = 'T.'

        data = otp.Ticks(A=[1, 2])
        data = data.join_with_snapshot(
            snapshot_name='missing_snapshot', snapshot_storage='memory', database='TEST_JOIN_DB',
            allow_snapshot_absence=True,
            default_fields_for_outer_join={
                'TEST1': (ott.long, 1),
                'TEST2': (float, data['A'] * 2),
                'TEST3': 3,
            },
            prefix_for_output_ticks=prefix,
        )

        assert data.schema == {
            'A': int, f'{prefix}TEST1': ott.long, f'{prefix}TEST2': float, f'{prefix}TEST3': int,
        }

        df = otp.run(data).to_dict(orient='list')
        del df['Time']

        assert df == {
            'A': [1, 2], f'{prefix}TEST1': [1, 1], f'{prefix}TEST2': [2.0, 4.0], f'{prefix}TEST3': [3.0, 3.0],
        }

    @pytest.mark.parametrize('param', ['SYM', None, '+'])
    def test_symbol_name_in_snapshot(self, db_session, param):
        db = 'TEST_JOIN_DB'
        # save snapshot
        data = otp.Ticks(X=[1, 2, 1], Y=['A', 'B', 'AA'], db=db)
        data = data.save_snapshot(
            snapshot_name='test_symbol_name_in_snapshot', snapshot_storage='memory', num_ticks=3,
            keep_snapshot_after_query=True, symbol_name_field='Y', database=db,
        )
        otp.run(data)

        data = otp.Ticks(A=[1, 2], SYM=['A'] * 2)

        if param is None:
            param = data['SYM']  # column
        if isinstance(param, str) and param == '+':
            param = data['SYM'] * 2  # operation

        data = data.join_with_snapshot(
            snapshot_name='test_symbol_name_in_snapshot', snapshot_storage='memory', database=db,
            symbol_name_in_snapshot=param,
        )
        df = self.get_result(data)

        assert df == {'A': [1, 2], 'SYM': ['A', 'A'], 'X': [1, 1]}


@pytest.mark.skipif(
    not hasattr(otq, "FindSnapshotSymbols"),
    reason="Current version of OneTick doesn't support FIND_SNAPSHOT_SYMBOLS EP",
)
class TestFindSnapshotSymbols:
    @pytest.fixture(scope='class')
    def snapshots(self, session):
        session.use(otp.DB('S1'))
        otp.run(otp.Ticks(X=[1, 1, 1], S=['TEST1', 'TEST2', 'PREFIX_TEST']).save_snapshot(
            snapshot_name='test_1', database='S1', keep_snapshot_after_query=True, symbol_name_field='S',
        ))

    def test_base(self, snapshots):
        data = otp.FindSnapshotSymbols(snapshot_name='test_1', db='S1')
        df = otp.run(data, symbols='S1::')
        assert set(df.keys()) == {'Time', 'SYMBOL_NAME'}
        assert list(df['SYMBOL_NAME']) == ['S1::TEST1', 'S1::TEST2', 'S1::PREFIX_TEST']

    @pytest.mark.filterwarnings("ignore:__call__")
    def test_no_database_parameter(self, session):
        otp.run(otp.Ticks(X=[1, 1, 1]).save_snapshot(snapshot_name='test_2', keep_snapshot_after_query=True))

        data = otp.FindSnapshotSymbols(snapshot_name='test_2')
        df = data(symbols=f'{otp.config.default_db}::')
        assert set(df.keys()) == {'Time', 'SYMBOL_NAME'}
        assert list(df['SYMBOL_NAME']) == [f'{otp.config.default_db}::{otp.config.default_symbol}']

    def test_pattern(self, snapshots):
        data = otp.FindSnapshotSymbols(snapshot_name='test_1', db='S1', pattern='%TEST')
        df = otp.run(data, symbols='S1::')
        assert set(df.keys()) == {'Time', 'SYMBOL_NAME'}
        assert list(df['SYMBOL_NAME']) == ['S1::PREFIX_TEST']

    def test_pattern_discard_on_match(self, snapshots):
        data = otp.FindSnapshotSymbols(snapshot_name='test_1', db='S1', pattern='%TEST', discard_on_match=True)
        df = otp.run(data, symbols='S1::')
        assert set(df.keys()) == {'Time', 'SYMBOL_NAME'}
        assert list(df['SYMBOL_NAME']) == ['S1::TEST1', 'S1::TEST2']

    def test_exceptions(self):
        with pytest.raises(ValueError, match='must be one of'):
            _ = otp.FindSnapshotSymbols(snapshot_storage='TEST', database='S1')
