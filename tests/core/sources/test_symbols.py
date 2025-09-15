import pytest
import operator

import onetick.py as otp


@pytest.fixture(scope='module')
def db(session):
    t_db = otp.DB("TEST_SYMBOLS")
    t_db.add(otp.Tick(X=1), symbol='A', tick_type='TA')
    t_db.add(otp.Tick(X=2), symbol='B', tick_type='TA')
    t_db.add(otp.Tick(X=3), symbol='C', tick_type='TB')
    t_db.add(otp.Tick(X=4), symbol='D', tick_type='TB')

    session.use(t_db)

    yield t_db


def test_fetch_all_symbols(db):
    symbols = otp.Symbols(db=db)

    assert 'SYMBOL_NAME' in symbols.schema
    assert symbols['SYMBOL_NAME'].dtype is str

    df = otp.run(symbols)

    assert set(df['SYMBOL_NAME'].to_list()) == {'A', 'B', 'C', 'D'}


@pytest.mark.parametrize('pattern,expected', [
    ('%', {'A', 'B', 'C', 'D'}),
    ('A%', {'A'}),
    ('%A', {'A'}),
    ('C%', {'C'}),
    ('X%', set())
])
def test_pattern(db, pattern, expected):
    symbols = otp.Symbols(db=db, pattern=pattern)

    df = otp.run(symbols)

    assert set(df['SYMBOL_NAME'].to_list()) == expected


@pytest.mark.parametrize('tick_type,expected', [
    (None, {'A', 'B', 'C', 'D'}),
    ('TA', {'A', 'B'}),
    ('TB', {'C', 'D'}),
    ('XY', set())
])
def test_for_tick_type(db, tick_type, expected):
    symbols = otp.Symbols(db=db, for_tick_type=tick_type)

    df = otp.run(symbols)

    assert set(df['SYMBOL_NAME'].to_list()) == expected


@pytest.mark.parametrize('show,op', [
    (True, operator.truth),
    (False, operator.not_)
])
def test_show_tick_type(db, show, op):
    symbols = otp.Symbols(db=db, show_tick_type=show)

    assert op('TICK_TYPE' in symbols.schema)
    if show:
        assert symbols['TICK_TYPE'].dtype is str

    df = otp.run(symbols)

    assert op('TICK_TYPE' in df.columns)


def test_inheritance(db):
    data = otp.Symbols(db)
    for d in [data, data.copy(), data.deepcopy()]:
        assert isinstance(d, otp.Symbols)


def test_properties(db):
    data = otp.Symbols(db)
    properties = set(otp.Symbols._PROPERTIES) - set(otp.Source._PROPERTIES)
    properties = {
        property: getattr(data, property)
        for property in properties
    }
    copy = data.copy()
    for property, value in properties.items():
        assert getattr(copy, property) == value


def test_pattern_as_param_column(session):
    db = otp.DB('TEST_DB_WITH_PARAM')
    db.add(otp.Tick(A=1), symbol='S12345', tick_type='TT')
    db.add(otp.Tick(B=2), symbol='S6789', tick_type='TT')

    session.use(db)

    def query_test(symbol, param):
        q = otp.Symbols('TEST_DB_WITH_PARAM', for_tick_type='TT', pattern=param)
        q['PARAM'] = param
        return q

    t = otp.Tick(X=1)
    t = t.join_with_query(query_test, params={'param': 'S12345'})
    df = otp.run(t)

    assert df['PARAM'].to_list() == ['S12345']


def test_tick_type_warnings(db):
    with pytest.warns(FutureWarning, match="In otp.Symbols parameter 'tick_type' is deprecated."):
        s1 = otp.Symbols(db=db, tick_type='TA')
    s2 = otp.Symbols(db=db, _tick_type='TA')
    s3 = otp.Symbols(db=db, for_tick_type='TA')

    df1 = otp.run(s1)
    df2 = otp.run(s2)
    df3 = otp.run(s3)

    assert list(df1['SYMBOL_NAME']) == ['A', 'B', 'C', 'D']
    assert df1.equals(df2)
    assert list(df3['SYMBOL_NAME']) == ['A', 'B']


def test_find_params_warning(db):
    with pytest.warns(FutureWarning, match="In otp.Symbols parameter 'find_params' is deprecated."):
        s1 = otp.Symbols(db=db, for_tick_type='TA', pattern='A', find_params={'discard_on_match': True})
    s2 = otp.Symbols(db=db, for_tick_type='TA', pattern='A', discard_on_match=True)

    df1 = otp.run(s1)
    df2 = otp.run(s2)

    assert list(df1['SYMBOL_NAME']) == ['B']
    assert df1.equals(df2)


def test_exceptions(db):
    with pytest.raises(ValueError, match="Wrong value for parameter 'cep_method':"):
        otp.Symbols(db=db, for_tick_type='TA', cep_method='WRONG')
    with pytest.raises(ValueError, match="Wrong value for parameter 'symbols_to_return':"):
        otp.Symbols(db=db, for_tick_type='TA', symbols_to_return='WRONG')
