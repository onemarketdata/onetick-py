import pytest
import onetick.py as otp
from onetick.py.otq import otq
from datetime import timedelta, datetime
from pathlib import Path


def test_add_field(session):
    t = otp.Tick(A=1)
    t['X'] = otp.param('PARAM', dtype=int)
    df = otp.run(t, query_params={'PARAM': 123})
    assert df['X'][0] == 123


@pytest.mark.xfail(reason='BDS-372: all results should be the same as in test_string_graph', strict=False)
@pytest.mark.parametrize('param,expected', [
    # all test cases taken from QueryDesigner
    # just the string, should be interpreted as a column name
    ('hello', Exception),
    # string with single quotes, should be interpreted as a string hello (without quotes)
    ("'hello'", 'hello'),
    # string with double quotes, should be interpreted as a string hello (without quotes)
    ('"hello"',  'hello'),
    # mixed, should be interpreted as a string 'hello' (with single quotes)
    ('''"'hello'"''',  "'hello'"),
    # mixed, should be interpreted as a string "hello" (with double quotes)
    ("""'"hello"'""",  '"hello"'),
    # string with escaped single quotes
    (r"\'hello\'", '\\hello\\'),
    # string with escaped double quotes
    (r'\"hello\"', '\\hello\\'),
])
def test_string(session, param, expected):
    # with string_literal=False this is practically the same as test_string_otq
    t = otp.Tick(A=otp.param('PARAM', dtype=otp.string[64], string_literal=False))
    if expected is Exception:
        with pytest.raises(Exception):
            otp.run(t, query_params={'PARAM': param})
    else:
        df = otp.run(t, query_params={'PARAM': param})
        assert df['A'][0] == expected


@pytest.mark.parametrize('param,expected', (
    # all test cases taken from QueryDesigner
    # just the string, should be interpreted as a column name
    ('hello', Exception),
    # string with single quotes, should be interpreted as a string hello (without quotes)
    ("'hello'", 'hello'),
    # string with double quotes, should be interpreted as a string hello (without quotes)
    ('"hello"',  'hello'),
    # mixed, should be interpreted as a string 'hello' (with single quotes)
    ('''"'hello'"''',  "'hello'"),
    # mixed, should be interpreted as a string "hello" (with double quotes)
    ("""'"hello"'""",  '"hello"'),
    # string with escaped single quotes
    (r"\'hello\'", Exception if otp.compatibility.is_duplicating_quotes_not_supported() else '\\hello\\'),
    # string with escaped double quotes
    (r'\"hello\"', Exception if otp.compatibility.is_duplicating_quotes_not_supported() else '\\hello\\'),
))
def test_string_graph(session, param, expected):
    run_params = dict(
        symbols='LOCAL::',
        start=datetime(2022, 1, 1),
        end=datetime(2022, 1, 1) + timedelta(days=1),
        timezone='GMT',
    )

    ep = otq.TickGenerator(bucket_interval=0, fields='A=$PARAM')
    graph = otq.GraphQuery(ep)

    if expected is Exception:
        with pytest.raises(Exception):
            otp.run(graph, **run_params, query_params={'PARAM': param})
    else:
        result = otq.run(graph, **run_params, query_params={'PARAM': param})
        assert result['LOCAL::']['A'][0] == expected


@pytest.mark.xfail(reason='BDS-372: all results should be the same as in test_string_graph', strict=False)
@pytest.mark.parametrize('param,expected', (
    # all test cases taken from QueryDesigner
    # just the string, should be interpreted as a column name
    ('hello', Exception),
    # string with single quotes, should be interpreted as a string hello (without quotes)
    ("'hello'", 'hello'),
    # string with double quotes, should be interpreted as a string hello (without quotes)
    ('"hello"',  'hello'),
    # mixed, should be interpreted as a string 'hello' (with single quotes)
    ('''"'hello'"''',  "'hello'"),
    # mixed, should be interpreted as a string "hello" (with double quotes)
    ("""'"hello"'""",  '"hello"'),
    # string with escaped single quotes
    (r"\'hello\'", '\\hello\\'),
    # string with escaped double quotes
    (r'\"hello\"', '\\hello\\'),
))
def test_string_otq(session, param, expected):
    run_params = dict(
        symbols='LOCAL::',
        start=datetime(2022, 1, 1),
        end=datetime(2022, 1, 1) + timedelta(days=1),
        timezone='GMT',
    )

    graph = str(Path(__file__).parent / 'resources' / 'test_params.otq')

    if expected is Exception:
        with pytest.raises(Exception):
            otp.run(graph, **run_params, query_params={'PARAM': param})
    else:
        result = otp.run(graph, **run_params, query_params={'PARAM': param})
        assert result.output('LOCAL::').data['A'][0] == expected


def test_tick(session):
    t = otp.Tick(A=otp.param('PARAM', dtype=int))
    assert t.schema == {'A': int}
    df = otp.run(t, query_params={'PARAM': 123})
    assert df['A'][0] == 123


def test_ticks(session):
    t = otp.Ticks(A=[otp.param('PARAM', dtype=int)] * 2)
    assert t.schema == {'A': int}
    df = otp.run(t, query_params={'PARAM': 123})
    assert list(df['A']) == [123] * 2


def test_script(session):

    def fun(tick):
        tick['X'] = otp.param('PARAM', dtype=int)

    t = otp.Tick(A=1)
    t = t.script(fun)
    assert t.schema['X'] is int
    df = otp.run(t, query_params={'PARAM': 123})
    assert df['X'][0] == 123


@pytest.mark.parametrize('param', (2, 3))
def test_bucket_interval(session, param):
    t = otp.Ticks(A=list(range(6)))
    t = t.agg({'A': otp.agg.sum('A')}, bucket_units='ticks', bucket_interval=otp.param('PARAM', dtype=int))
    df = otp.run(t, query_params={'PARAM': param})
    if param == 3:
        assert list(df['A']) == [3, 12]
    elif param == 2:
        assert list(df['A']) == [1, 5, 9]
