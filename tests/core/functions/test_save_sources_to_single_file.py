import pytest

import onetick.py as otp


@pytest.mark.parametrize('autogenerate_file', [False, True])
def test_dict_sources(session, autogenerate_file):
    sources = {
        'a': otp.Tick(A=1),
        'b': {
            'source': otp.Tick(B=2),
            'symbols': otp.Tick(SYMBOL_NAME=otp.config['default_db_symbol']),
        },
    }
    file_path = None if autogenerate_file else otp.utils.TmpFile().path
    file_path = otp.functions.save_sources_to_single_file(sources, file_path=file_path)
    queries_names = otp.core.query_inspector.get_queries(file_path)
    assert len(queries_names) == 3
    for name, source in sources.items():
        if isinstance(source, dict):
            source = source['source']
        df = otp.run(otp.query(f'{file_path}::{name}'))
        df2 = otp.run(source)
        assert df.equals(df2)


@pytest.mark.parametrize('autogenerate_file', [False, True])
def test_list_sources(session, autogenerate_file):
    sources = [
        otp.Tick(A=1),
        {
            'source': otp.Tick(B=2),
            'symbols': otp.Tick(SYMBOL_NAME=otp.config['default_db_symbol']),
        }
    ]
    file_path = None if autogenerate_file else otp.utils.TmpFile().path
    query_paths = otp.functions.save_sources_to_single_file(sources, file_path=file_path)
    for query_path, source in zip(query_paths, sources):
        if isinstance(source, dict):
            source = source['source']
        df = otp.run(otp.query(query_path))
        df2 = otp.run(source)
        assert df.equals(df2)


def test_join_with_query(session):
    sources = {
        'ab': otp.Tick(A=1).join_with_query(otp.Tick(B=1)),
        'cd': otp.Tick(C=3).join_with_query(otp.Tick(B=1)),
    }
    file_path = otp.functions.save_sources_to_single_file(sources)
    queries_names = otp.core.query_inspector.get_queries(file_path)
    assert len(queries_names) == 4
    for name, source in sources.items():
        df = otp.run(otp.query(f'{file_path}::{name}'))
        df2 = otp.run(source)
        assert df.equals(df2)


def test_symbols_list(session):
    sources = {
        'a': otp.merge(
            [otp.Tick(A=1)],
            symbols=otp.Tick(SYMBOL_NAME=otp.config['default_db_symbol'])
        ),
        'b': otp.merge(
            [otp.Tick(B=2)],
            symbols=otp.Tick(SYMBOL_NAME=otp.config['default_db_symbol'])
        ),
    }
    file_path = otp.functions.save_sources_to_single_file(sources)
    queries_names = otp.core.query_inspector.get_queries(file_path)
    assert len(queries_names) == 4
    for name, source in sources.items():
        df = otp.run(otp.query(f'{file_path}::{name}'))
        df2 = otp.run(source)
        assert df.equals(df2)


def test_eval(session):
    sources = {
        'a': otp.merge(
            [otp.Tick(A=1)],
            symbols=otp.eval(otp.Tick(SYMBOL_NAME=otp.config['default_db_symbol']))
        ),
        'b': otp.merge(
            [otp.Tick(B=2)],
            symbols=otp.eval(otp.Tick(SYMBOL_NAME=otp.config['default_db_symbol']))
        ),
    }
    file_path = otp.functions.save_sources_to_single_file(sources)
    queries_names = otp.core.query_inspector.get_queries(file_path)
    assert len(queries_names) == 4
    for name, source in sources.items():
        df = otp.run(otp.query(f'{file_path}::{name}'))
        df2 = otp.run(source)
        assert df.equals(df2)
