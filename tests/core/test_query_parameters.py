import os
from pathlib import Path

import pytest
import pandas as pd

import onetick.py as otp
from onetick.py.otq import otq

import tests


@pytest.fixture(scope='module')
def session(m_session):
    db = otp.DB('DB')
    db.add(otp.Ticks(A=[1, 2, 3]), tick_type='TT', symbol='SS')
    m_session.use(db)
    yield m_session


def test_source(session):
    ep = otq.TickGenerator(
        fields=('X=GET_QUERY_PROPERTY("ALLOW_GRAPH_REUSE"),'
                'Y=GET_QUERY_PROPERTY("IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE")'),
        bucket_interval=0,
    )
    t = otp.Source(
        ep,
        query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE',
                                                               'IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE': 'TRUE'})
    )
    df = otp.run(t, symbols='LOCAL::')
    assert df['X'][0] == 'TRUE'
    assert df['Y'][0] == 'TRUE'

    # test otp.run override
    df = otp.run(t, symbols='LOCAL::', query_properties={'ALLOW_GRAPH_REUSE': 'FALSE',
                                                         'IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE': 'FALSE'})
    assert df['X'][0] == 'FALSE'
    assert df['Y'][0] == 'FALSE'


@pytest.mark.skipif(not tests.compatibility.is_get_query_property_flag_supported(),
                    reason="Second parameter of GET_QUERY_PROPERTY was not supported before")
def test_override(session):
    ep = otq.TickGenerator(fields='X=GET_QUERY_PROPERTY("MAX_CONCURRENCY", true)', bucket_interval=0)
    t = otp.Source(ep,
                   query_parameters=otp.QueryParameters(concurrency=2))

    # test no override if concurrency is not set
    df = otp.run(t, symbols='LOCAL::', concurrency=None)
    assert df['X'][0] == '2'


def test_to_otq(session):
    t = otp.Tick(X=otp.get_query_property('MAX_CONCURRENCY', True),
                 query_parameters=otp.QueryParameters(concurrency=2))

    query_name = t.to_otq()
    assert 'CPU_NUMBER = 2' in Path(query_name.split('::')[0]).read_text()
    query_name = t.to_otq(concurrency=3)
    assert 'CPU_NUMBER = 3' in Path(query_name.split('::')[0]).read_text()


def test_tick(session):
    t = otp.Tick(
        X=otp.get_query_property('ALLOW_GRAPH_REUSE'),
    )
    df = otp.run(t)
    assert df['X'][0] == ''

    t = otp.Tick(
        X=otp.get_query_property('ALLOW_GRAPH_REUSE'),
        query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'}),
    )
    df = otp.run(t)
    assert df['X'][0] == 'TRUE'

    # test works after copy
    t = t.add_fields({'B': 1})
    df = otp.run(t)
    assert df['X'][0] == 'TRUE'
    assert df['B'][0] == 1


def test_ticks(session):
    t = otp.Ticks(A=[1, 2, 3])
    t['X'] = otp.get_query_property('ALLOW_GRAPH_REUSE')
    df = otp.run(t)
    assert list(df['X']) == ['', '', '']

    t = otp.Ticks(
        A=[1, 2, 3],
        query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'}),
    )
    t['X'] = otp.get_query_property('ALLOW_GRAPH_REUSE')
    df = otp.run(t)
    assert list(df['X']) == ['TRUE'] * 3

    # test works after copy
    t = t.add_fields({'B': 1})
    df = otp.run(t)
    assert list(df['X']) == ['TRUE'] * 3
    assert list(df['B']) == [1, 1, 1]


def test_data_source(session):
    t = otp.DataSource('DB', tick_type='TT', symbols='SS')
    t['X'] = otp.get_query_property('ALLOW_GRAPH_REUSE')
    df = otp.run(t)
    assert list(df['A']) == [1, 2, 3]
    assert list(df['X']) == ['', '', '']

    t = otp.DataSource('DB', tick_type='TT', symbols='SS',
                       query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'}))
    t['X'] = otp.get_query_property('ALLOW_GRAPH_REUSE')
    df = otp.run(t)
    assert list(df['A']) == [1, 2, 3]
    assert list(df['X']) == ['TRUE'] * 3


@pytest.mark.skipif(not tests.compatibility.is_allow_graph_reuse_property_fixed(),
                    reason="Doesn't work on older OneTick versions")
def test_merge_and_join(session):
    t1 = otp.Tick(
        X=otp.get_query_property('ALLOW_GRAPH_REUSE'),
        query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'}),
    )
    t2 = otp.Tick(
        X=otp.get_query_property('ALLOW_GRAPH_REUSE'),
        query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'FALSE'}),
    )
    data = otp.merge([t1, t2])
    df = otp.run(data)
    # when merging, last value will be used
    assert list(df['X']) == ['FALSE', 'FALSE']

    data = otp.join(t1, t2, on='all')
    df = otp.run(data)
    # when joining too
    assert list(df['X']) == ['FALSE']


def test_symbols(session):
    symbols = otp.Symbols('DB', for_tick_type='TT',
                          query_parameters=otp.QueryParameters(query_properties={'IGNORE_REALTIME_DB': 'TRUE'}))
    symbols['X'] = otp.get_query_property('IGNORE_REALTIME_DB')

    main = otp.DataSource(db='DB', tick_type='TT')
    main['X'] = otp.get_query_property('IGNORE_REALTIME_DB')
    main['SYMBOL_PARAM_X'] = main.Symbol.get('X', dtype=otp.string[64])

    df = otp.run(main, symbols=symbols)['SS']
    assert list(df['A']) == [1, 2, 3]
    assert list(df['X']) == ['', '', '']
    assert list(df['SYMBOL_PARAM_X']) == ['TRUE', 'TRUE', 'TRUE']


def test_empty(session):
    t = otp.Empty(
        query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'}),
    )
    t = t.insert_at_end()
    t['X'] = otp.get_query_property('ALLOW_GRAPH_REUSE')
    df = otp.run(t)
    assert list(df['AT_END']) == [1]
    assert list(df['X']) == ['TRUE']

def test_csv(session):
    t = otp.CSV(
        file_contents='#A,B\n1,2\n',
        query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'}),
    )
    t['X'] = otp.get_query_property('ALLOW_GRAPH_REUSE')
    df = otp.run(t)
    assert df['A'][0] == 1
    assert df['B'][0] == 2
    assert df['X'][0] == 'TRUE'


@pytest.mark.skipif(not tests.compatibility.is_allow_graph_reuse_property_fixed(),
                    reason="Doesn't work on older OneTick versions")
def test_join_with_query(session):
    t = otp.Tick(A=1, query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'FALSE'}))
    t['A_X'] = otp.get_query_property('ALLOW_GRAPH_REUSE')
    jwq = otp.Tick(B=2, query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'}))
    jwq['B_X'] = otp.get_query_property('ALLOW_GRAPH_REUSE')

    data = t.join_with_query(jwq)
    df = otp.run(data)
    assert df['A'][0] == 1
    assert df['B'][0] == 2
    assert df['A_X'][0] == 'FALSE'
    assert df['B_X'][0] == 'TRUE'

    t = otp.Tick(A=1)
    jwq = otp.Tick(B=2, query_parameters=otp.QueryParameters(concurrency=3))
    jwq['JWQ_CONCURRENCY'] = otp.get_query_property('MAX_CONCURRENCY', True)
    data = t.join_with_query(jwq, concurrency=5)
    df = otp.run(data)
    assert df['JWQ_CONCURRENCY'][0] == '5'


def test_running_fsq(session):
    fsq = otp.Tick(SYMBOL_NAME='SS',
                   query_parameters=otp.QueryParameters(running=True))
    fsq['X'] = 'ASD'

    t = otp.Tick(A=123)
    t['SYMBOL_PARAM_X'] = t.Symbol.get('X', dtype=otp.string[64])

    df = otp.run(t, symbols=fsq, running=True)[f'{otp.config.default_db}::unresolved']
    assert df['A'][0] == 123
    # symbol is unresolved so getting nothing from fsq
    assert df['SYMBOL_PARAM_X'][0] == ''


def test_query(session):
    t = otp.Tick(A=12345)
    src = otp.Query(t.to_otq(), query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'}))
    src['X'] = otp.get_query_property('ALLOW_GRAPH_REUSE')
    df = otp.run(src)
    assert df['A'][0] == 12345
    assert df['X'][0] == 'TRUE'


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE'),
                    reason='doctest_resources are not available in WebAPI testing mode')
@pytest.mark.skipif(not otp.compatibility._is_data_file_query_supported(),
                    reason="Not supported on some OneTick versions")
def test_data_file(session, pytestconfig):
    data = otp.DataFile(
        f'{pytestconfig.rootdir}/doctest_resources/data.arrow',
        query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'}),
    )
    data['X'] = otp.get_query_property('ALLOW_GRAPH_REUSE')
    df = otp.run(data)
    assert list(df['A']) == [1, 3]
    assert list(df['X']) == ['TRUE', 'TRUE']


def test_load_ticks_from_dataframe(session, pytestconfig):
    data = otp.LoadTicksFromDataFrame(
        pd.DataFrame({'A': [1, 2, 3]}),
        query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'}),
    )
    data['X'] = otp.get_query_property('ALLOW_GRAPH_REUSE')
    df = otp.run(data)
    assert list(df['A']) == [1, 2, 3]
    assert list(df['X']) == ['TRUE'] * 3


@pytest.mark.skipif(not otp.compatibility._is_read_from_dataframe_supported(),
                    reason='Not supported on this OneTick version')
def test_read_from_dataframe(session, pytestconfig):
    data = otp.ReadFromDataFrame(
        pd.DataFrame({'A': [1, 2, 3]}),
        query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'}),
    )
    data['X'] = otp.get_query_property('ALLOW_GRAPH_REUSE')
    df = otp.run(data)
    assert list(df['A']) == [1, 2, 3]
    assert list(df['X']) == ['TRUE'] * 3
