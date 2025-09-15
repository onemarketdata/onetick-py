import onetick.py as otp
from onetick.py.otq import otq
import pytest


def test_jwq_chain(session):
    source_a = otp.Tick(A="A")
    source_b = otp.Tick(B="B")
    source_b = source_b.join_with_query(source_a)
    source_c = otp.Tick(C="C")
    source_c = source_c.join_with_query(source_b)
    source_d = otp.Tick(D="D")
    source_d = source_d.join_with_query(source_c)

    res = otp.run(source_d)
    assert len(res) == 1
    assert res['A'][0] == 'A'
    assert res['B'][0] == 'B'
    assert res['C'][0] == 'C'
    assert res['D'][0] == 'D'


def test_process_by_group_chain(session):
    source = otp.Tick(S='A')

    def source_b_func(source):
        source['S'] = source['S'] + 'B'
        return source

    def source_c_func(source):
        source = source.process_by_group(source_b_func)
        source['S'] = source['S'] + 'C'
        return source

    def source_d_func(source):
        source = source.process_by_group(source_c_func)
        source['S'] = source['S'] + 'D'
        return source
    source = source.process_by_group(source_d_func)

    res = otp.run(source)
    assert len(res) == 1
    assert res['S'][0] == 'ABCD'


# Suppressing deprecation warning for the function we test
@pytest.mark.filterwarnings('ignore:Using .to_graph()')
def test_to_graph_for_complex_queries(session):
    symb = otp.Tick(SYMBOL_NAME=otp.config['default_symbol'])
    source_a = otp.Tick(A="A")
    source_b = otp.Tick(B="B")
    source_b = source_b.join_with_query(source_a)
    per_tick_script = """
    long main() {

    long C = $C;

    return true;
}
    """
    source_b.sink(otq.PerTickScript(script=per_tick_script))
    graph = source_b.to_graph(symbols=symb)
    res = otp.run(graph, symbols=symb, start=otp.config['default_start_time'],
                  end=otp.config['default_start_time'], query_params=dict(C=1))
    assert len(res) == 1
    df = list(res.values())[0]
    assert len(df) == 1
    assert df['A'][0] == 'A'
    assert df['B'][0] == 'B'
    assert df['C'][0] == 1

    kwargs = {}
    res = otp.run(graph,
                  symbols=otp.Source._convert_symbol_to_string(symb),
                  start=otp.config['default_start_time'],
                  end=otp.config['default_end_time'],
                  query_params=dict(C=2),
                  **kwargs)
    assert len(res) == 1
    res_data = res[otp.config['default_symbol']]
    for field, data in res_data.items():
        if field == 'A':
            assert len(data) == 1
            assert data[0] == 'A'
        if field == 'B':
            assert len(data) == 1
            assert data[0] == 'B'
        if field == 'C':
            assert len(data) == 1
            assert data[0] == 2
