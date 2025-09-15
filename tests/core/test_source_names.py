import onetick.py as otp
import os
import pytest

from onetick.py.utils import TMP_CONFIGS_DIR, ONE_TICK_TMP_DIR
from onetick.py.core.query_inspector import get_queries


def reverse_str(s):
    s = list(s)
    s.reverse()
    return str(s)


def list_otqs():
    dir_path = TMP_CONFIGS_DIR()
    if ONE_TICK_TMP_DIR():
        dir_path = ONE_TICK_TMP_DIR()
    if os.getenv('OTP_WEBAPI_TEST_MODE'):
        from onetick.py.otq import _tmp_otq_path
        dir_path = _tmp_otq_path()
    files = []
    for f in os.listdir(dir_path):
        f = os.path.join(dir_path, f)
        if os.path.isfile(f):
            ext = str(f)[-4:]
            if ext == '.otq':
                files.append(str(f))
    return files


def test_creating_named_source(session):
    """
    Named source should always be stored as an .otq file on running, regardless on whether it needs to store
    temporary queries or not.
    We check that it's stored in a file that has file name suffix matching the source name.
    """
    source = otp.Tick(A=1)
    source.set_name('test_source')
    otp.run(source)
    files = list_otqs()
    assert len(files) == 1
    assert files[0].endswith('.test_source.run.otq')
    queries = get_queries(files[0])
    assert len(queries) == 1
    assert queries[0] == 'test_source'


def test_named_source_for_join_with_query(session):
    source1 = otp.Tick(A=1)
    source1.set_name('source_1')
    source2 = otp.Tick(B=1)
    source2.set_name('source_2')
    source2 = source2.join_with_query(source1)
    res = otp.run(source2)
    assert len(res) == 1
    assert res['A'][0] == 1
    assert res['B'][0] == 1
    files = list_otqs()
    assert len(files) == 1
    assert files[0].endswith('.source_2.run.otq')
    queries = get_queries(files[0])
    queries.sort(key=reverse_str)
    assert len(queries) == 2
    assert queries[0] == 'source_2'
    assert queries[1].endswith('__source_1__join_with_query')


def test_named_source_for_process_by_group(session):
    source2 = otp.Tick(FIELD='A')
    source2.set_name('source_2')

    def source_func(source):
        source['FIELD'] = source['FIELD'] + 'B'
        source.set_name('source_1')
        return source

    source2 = source2.process_by_group(source_func)
    res = otp.run(source2)
    assert len(res) == 1
    assert res['FIELD'][0] == 'AB'
    files = list_otqs()
    assert len(files) == 1
    assert files[0].endswith('.source_2.run.otq')
    queries = get_queries(files[0])
    queries.sort(key=reverse_str)
    assert len(queries) == 2
    assert queries[0] == 'source_2'
    assert queries[1].endswith('__source_1__group_by')


def test_named_source_for_eval_same_file(session):
    source1 = otp.Tick(SYMBOL_NAME='AAPL')
    source1.set_name('source_1')
    source2 = otp.Tick(B=1)
    source2.set_name('source_2')
    res = otp.run(source2, symbols=source1)
    assert len(res) == 1
    res = res['AAPL']
    assert len(res) == 1
    assert res['B'][0] == 1
    files = list_otqs()
    assert len(files) == 1
    assert files[0].endswith('.source_2.run.otq')
    queries = get_queries(files[0])
    queries.sort(key=reverse_str)
    assert len(queries) == 2
    assert queries[0] == 'source_2'
    assert queries[1].endswith('__source_1__symbol')


def test_named_source_for_eval_different_files(session):
    source1 = otp.Tick(SYMBOL_NAME='AAPL')
    source1.set_name('source_1')
    source2 = otp.Tick(B=1)
    source2.set_name('source_2')
    res = otp.run(source2.to_otq(), symbols=source1)
    assert len(res) == 1
    res = res['AAPL']
    assert len(res) == 1
    assert res['B'][0] == 1
    files = list_otqs()
    files.sort(key=reverse_str)
    assert len(files) == 2
    assert files[0].endswith('.source_1.symbol.otq')
    assert files[1].endswith('.source_2.to_otq.otq')

    queries = get_queries(files[0])
    assert len(queries) == 1
    assert queries[0] == 'source_1'
    queries = get_queries(files[1])
    assert len(queries) == 1
    assert queries[0] == 'source_2'


def test_source_names_on_join_and_merge(session):
    source1 = otp.Tick(A=1)
    source2 = otp.Tick(B=1)
    source3 = otp.Tick(C=1)
    source1.set_name('source_1')

    s = source1 + source3
    assert s.get_name() == 'source_1'
    s = source2 + source1
    assert s.get_name() == 'source_1'
    s = source2 + source3
    assert s.get_name() is None

    s = otp.join(source1, source2, source1['A'] == source2['B'])
    assert s.get_name() == 'source_1'
    s = otp.join(source2, source1, source2['B'] == source1['A'])
    assert s.get_name() == 'source_1'

    s = otp.join_by_time([source1, source2, source3])
    assert s.get_name() == 'source_1'

    source2.set_name('source_2')
    s1, s2 = source2[source2['B'] == 1]
    s = s1 + s2
    assert s1.get_name() == 'source_2'
    assert s2.get_name() == 'source_2'
    assert s.get_name() == 'source_2'


def test_invalid_source_names(session):
    """
    Some common symbols cannot be used in query names, e.g. "." or "-".
    Such symbols may be present in source names but should not lead to errors on query creation.
    We check that no errors appear when using them in source names.
    """
    source1 = otp.Tick(A=1)
    source1.set_name('source.1')
    source2 = otp.Tick(B=2, SYMBOL_NAME='AAPL')
    source2.set_name('source-2')
    source3 = otp.Tick(C=3)
    source3.set_name('source@3')
    source3 = source3.join_with_query(source1)
    res = otp.run(source3, symbols=source2)
    assert len(res) == 1
    res = res['AAPL']
    assert res['A'][0] == 1
    assert res['C'][0] == 3
    res = otp.run(source3.to_otq(), symbols=source2)
    assert len(res) == 1
    res = res['AAPL']
    assert res['A'][0] == 1
    assert res['C'][0] == 3


def test_empty_or_nonstring_source_name(session):
    source = otp.Tick(A=1)
    source.set_name('_')
    source.set_name('.')
    with pytest.raises(AssertionError):
        source.set_name('')
    with pytest.raises(AssertionError):
        source.set_name(1)
