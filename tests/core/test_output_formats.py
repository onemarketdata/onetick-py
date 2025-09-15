import pytest
import onetick.py as otp
import pandas as pd

DB_NAME = 'TEST_DB'
TICK_TYPE = 'TT'
SYMB = 'AAPL'


@pytest.fixture(scope='module')
def cust_session(session):
    db = otp.DB(DB_NAME, otp.Tick(A=1), tick_type=TICK_TYPE, symbol=SYMB)
    session.use(db)
    yield session


@pytest.fixture(scope='module')
def multi_output_graph():
    branch0 = otp.DataSource(db=DB_NAME, tick_type=TICK_TYPE)
    branch0['TEST_FIELD'] = 2
    branch1 = branch0.copy()
    branch2_1 = branch0.copy()
    branch2_2 = branch0.copy()
    branch0['TEST_FIELD'] = 0
    branch1['TEST_FIELD'] = 1
    branch2_1['TEST_FIELD'] = 2.1
    branch2_2['TEST_FIELD'] = 2.2

    branch0.node().node_name('OUT_0')
    branch0.node()._ep.set_output_pin_name('OUT_0')
    branch1.node().node_name('OUT_1')
    branch1.node()._ep.set_output_pin_name('OUT_1')
    branch2_1.node().node_name('OUT_2')
    branch2_1.node()._ep.set_output_pin_name('OUT2_1')
    branch2_2.node().node_name('OUT_2')
    branch2_2.node()._ep.set_output_pin_name('OUT2_2')

    graph = otp.Source._construct_multi_branch_graph([branch0, branch1, branch2_1, branch2_2])
    yield graph.to_otq(symbols='DUMMY', file_suffix='_multi_output.otq', add_passthrough=False)


# TODO: add also test for "symbol_type='source'" (right now it fails due to incomplete logic)
@pytest.mark.parametrize('run_mode', ['call', 'source', 'query_file'])
@pytest.mark.parametrize('require_dict', [True, False])
@pytest.mark.parametrize('symbol_type', ['string', 'eval'])
def test_save_symbols_to_source(cust_session, run_mode, require_dict, symbol_type):
    symb_query_file = otp.utils.TmpFile(suffix='_symbols.otq')
    main_query_file = otp.utils.TmpFile(suffix='_main.otq')
    if symbol_type == 'source':
        symbol = otp.Tick(SYMBOL_NAME=SYMB)
    elif symbol_type == 'eval':
        symbol_query = otp.Tick(SYMBOL_NAME=SYMB).to_otq(file_name=symb_query_file.path)
        symbol = f'eval("{symbol_query}")'
    elif symbol_type == 'string':
        symbol = SYMB
    else:
        assert False, 'Unknown symbol_type test parameter'

    source = otp.DataSource(symbol=symbol, db=DB_NAME, tick_type=TICK_TYPE)

    if run_mode in ['call', 'source']:
        if require_dict:
            res = otp.run(source, require_dict=require_dict)
        else:
            res = otp.run(source)
    elif run_mode == 'query_file':
        if require_dict:
            res = otp.run(f"'{source.to_otq(file_name=main_query_file.path)}'", require_dict=require_dict)
        else:
            res = otp.run(f"'{source.to_otq(file_name=main_query_file.path)}'")
    else:
        assert False, "Unknown run_mode test parameter"

    # Checking for the new behaviour to match old behaviour specifically
    if (symbol_type == 'eval') and (run_mode == 'query_file') and not require_dict:
        assert isinstance(res, pd.DataFrame)
        assert len(res) == 1
        return

    if require_dict or (symbol_type == 'eval') or (symbol_type == 'source'):
        assert isinstance(res, dict)
        assert len(res) == 1
        assert 'AAPL' in res.keys()
        assert len(res['AAPL'] == 1)
    else:
        assert isinstance(res, pd.DataFrame)
        assert len(res) == 1


@pytest.mark.parametrize('run_mode', ['call', 'source', 'query_file'])
@pytest.mark.parametrize('require_dict', [True, False])
@pytest.mark.parametrize('symbol_type', ['string', 'source', 'eval'])
def test_pass_symbols_external(cust_session, run_mode, require_dict, symbol_type):
    symb_query_file = otp.utils.TmpFile(suffix='_symbols.otq')
    main_query_file = otp.utils.TmpFile(suffix='_main.otq')
    if symbol_type == 'source':
        symbol = otp.Tick(SYMBOL_NAME=SYMB)
    elif symbol_type == 'eval':
        symbol_query = otp.Tick(SYMBOL_NAME=SYMB).to_otq(file_name=symb_query_file.path)
        symbol = f'eval("{symbol_query}")'
    elif symbol_type == 'string':
        symbol = SYMB
    else:
        assert False, 'Unknown symbol_type test parameter'

    source = otp.DataSource(db=DB_NAME, tick_type=TICK_TYPE)

    if run_mode in ['call', 'source']:
        if require_dict:
            res = otp.run(source, symbols=symbol, require_dict=require_dict)
        else:
            res = otp.run(source, symbols=symbol)
    elif run_mode == 'query_file':
        if require_dict:
            res = otp.run(f"'{source.to_otq(file_name=main_query_file.path)}'",
                          symbols=symbol, require_dict=require_dict)
        else:
            res = otp.run(f"'{source.to_otq(file_name=main_query_file.path)}'", symbols=symbol)
    else:
        assert False, "Unknown run_mode test parameter"

    if require_dict or (symbol_type == 'eval') or (symbol_type == 'source'):
        assert isinstance(res, dict)
        assert len(res) == 1
        assert 'AAPL' in res.keys()
        assert len(res['AAPL'] == 1)
    else:
        assert isinstance(res, pd.DataFrame)
        assert len(res) == 1


@pytest.mark.parametrize('symbol_from_source', [True, False])
@pytest.mark.parametrize('run_mode', ['query', 'Query', 'Query_with_call'])
def test_query_from_disk(cust_session, symbol_from_source, run_mode):
    symb_query_file = otp.utils.TmpFile(suffix='_symbols.otq')
    main_query_file = otp.utils.TmpFile(suffix='_main.otq')
    if symbol_from_source:
        symbol_query = otp.Tick(SYMBOL_NAME=SYMB).to_otq(file_name=symb_query_file.path)
        symbol = f'eval("{symbol_query}")'
    else:
        symbol = DB_NAME + "::" + SYMB

    query = otp.query(otp.DataSource(db=DB_NAME, tick_type=TICK_TYPE).to_otq(file_name=main_query_file.path))
    if run_mode == 'query':
        res = otp.run(query, symbols=symbol)
    elif run_mode == 'Query':
        res = otp.run(otp.Query(query), symbols=symbol)
    elif run_mode == 'Query_with_call':
        res = otp.run(otp.Query(query), symbols=symbol)
    else:
        assert False, 'Unknown run_mode parameter!'

    if symbol_from_source:
        assert isinstance(res, dict)
        assert len(res) == 1
        assert 'AAPL' in res.keys()
        assert len(res['AAPL'] == 1)
    else:
        assert isinstance(res, pd.DataFrame)
        assert len(res) == 1


@pytest.mark.parametrize('symbol_from_source', [True, False])
@pytest.mark.parametrize('require_dict', [True, False])
def test_multiple_outputs(cust_session, cur_dir, multi_output_graph, symbol_from_source, require_dict):
    symb_query_file = otp.utils.TmpFile(suffix='_symbols.otq')
    file_name = multi_output_graph
    source = otp.Query(otp.query(file_name), out_pin='OUT_0')

    symb_with_db = DB_NAME + "::" + SYMB

    if symbol_from_source:
        symbol_query = otp.Tick(SYMBOL_NAME=symb_with_db).to_otq(file_name=symb_query_file.path)
        symbol = f'eval("{symbol_query}")'
    else:
        symbol = symb_with_db

    res = otp.run(source, symbols=symbol, require_dict=require_dict)

    if symbol_from_source or require_dict:
        assert isinstance(res, dict)
        assert len(res) == 1
        assert symb_with_db in res.keys()
        assert len(res[symb_with_db]) == 1
        assert res[symb_with_db]['TEST_FIELD'][0] == 0
    else:
        assert isinstance(res, pd.DataFrame)
        assert len(res) == 1
        assert res['TEST_FIELD'][0] == 0


def test_output_format_for_one_pin(cust_session, multi_output_graph, cur_dir):
    file_name = multi_output_graph
    source = otp.Query(otp.query(file_name), out_pin='OUT_0')

    res_list = otp.run(source, symbols=['MSFT', 'AAPL'], output_structure='list')
    assert isinstance(res_list, list)
    assert len(res_list) == 2

    res_dict = otp.run(source, symbols=['MSFT', 'AAPL'])
    assert isinstance(res_dict, dict)
    assert len(res_dict) == 2
    assert 'AAPL' in res_dict.keys()
    assert isinstance(res_dict['AAPL'], pd.DataFrame)


def test_output_format_for_multiple_outputs(cust_session, multi_output_graph, cur_dir):
    # expect return from four nodes
    file_name = multi_output_graph

    res_list = otp.run(file_name, symbols=['MSFT', 'AAPL'], output_structure='list')
    assert isinstance(res_list, list)
    assert len(res_list) == 8

    res_dict = otp.run(file_name, symbols=['MSFT', 'AAPL'])
    assert isinstance(res_dict, dict)
    assert len(res_dict) == 2
    assert 'AAPL' in res_dict.keys()
    res_aapl = res_dict['AAPL']
    assert isinstance(res_aapl, dict)
    # checking returns for individual nodes
    assert len(res_aapl['OUT_0']) == 1
    assert res_aapl['OUT_0']['TEST_FIELD'][0] == 0
    assert len(res_aapl['OUT_1']) == 1
    assert res_aapl['OUT_1']['TEST_FIELD'][0] == 1
    list_out2 = res_aapl['OUT_2']
    assert isinstance(list_out2, list)
    assert len(list_out2) == 2
    assert len(list_out2[0]) == 1
    assert len(list_out2[1]) == 1
    # we do not know in which order results come
    test_values = set([df['TEST_FIELD'][0] for df in list_out2])
    assert test_values == set([2.1, 2.2])


def test_output_format_for_some_outputs(cust_session, multi_output_graph, cur_dir):
    file_name = multi_output_graph

    res_dict = otp.run(file_name, node_name=['OUT_1', 'OUT_2'], symbols=['MSFT', 'AAPL'])
    assert isinstance(res_dict, dict)
    assert len(res_dict) == 2
    assert 'AAPL' in res_dict.keys()
    res_aapl = res_dict['AAPL']
    assert isinstance(res_aapl, dict)
    # checking returns for individual nodes
    assert len(res_aapl['OUT_1']) == 1
    assert res_aapl['OUT_1']['TEST_FIELD'][0] == 1
    list_out2 = res_aapl['OUT_2']
    assert isinstance(list_out2, list)
    assert len(list_out2) == 2
    assert len(list_out2[0]) == 1
    assert len(list_out2[1]) == 1
    # we do not know in which order results come
    test_values = set([df['TEST_FIELD'][0] for df in list_out2])
    assert test_values == set([2.1, 2.2])


def test_output_format_for_one_output(cust_session, multi_output_graph, cur_dir):
    file_name = multi_output_graph

    res_dict = otp.run(file_name, node_name='OUT_1', symbols=['MSFT', 'AAPL'])
    assert isinstance(res_dict, dict)
    assert len(res_dict) == 2
    assert 'AAPL' in res_dict.keys()
    res_aapl = res_dict['AAPL']
    # checking returns for individual nodes
    assert len(res_aapl) == 1
    assert res_aapl['TEST_FIELD'][0] == 1

    res_dict = otp.run(file_name, node_name='OUT_2', symbols=['MSFT', 'AAPL'])
    assert isinstance(res_dict, dict)
    assert len(res_dict) == 2
    assert 'AAPL' in res_dict.keys()
    list_out2 = res_dict['AAPL']
    assert isinstance(list_out2, list)
    assert len(list_out2) == 2
    assert len(list_out2[0]) == 1
    assert len(list_out2[1]) == 1
    # we do not know in which order results come
    test_values = set([df['TEST_FIELD'][0] for df in list_out2])
    assert test_values == set([2.1, 2.2])
