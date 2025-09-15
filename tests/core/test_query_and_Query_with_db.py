import os
import pytest

from onetick import py as otp
from onetick.py import query


@pytest.mark.parametrize('remote', (True, False))
def test_get_symbols(f_session, cur_dir, remote):  # db will be destroyed at the end of the session
    db = otp.DB("MY_DB")

    db.add(otp.Tick(X=1), date=otp.config['default_start_time'], symbol="A", tick_type="TRD")
    db.add(otp.Tick(X=1), date=otp.config['default_start_time'], symbol="B", tick_type="TRD")

    f_session.use(db)

    otq_path = os.path.join(cur_dir, "otqs", "get_symbols.otq")
    if remote and not os.getenv('OTP_WEBAPI_TEST_MODE'):
        otq_path = f"remote://MY_DB::{otq_path}"

    q = query(otq_path, TT="TRD", DB=db.name, PATTERN="%")
    data = otp.Query(q, symbol=None)

    path = data.to_otq().split('::')[0]
    with open(path) as f:
        text = f.read()
    assert otq_path in text

    df = otp.run(data)

    assert len(df) == 2


@pytest.mark.parametrize('remote', (True, False))
def test_query_as_symbols(f_session, cur_dir, remote):
    db = otp.DB("MY_DB")
    db.add(otp.Tick(X=1), symbol="A", tick_type="TRD")
    db.add(otp.Tick(X=2), symbol="B", tick_type="TRD")
    f_session.use(db)

    otq_path = os.path.join(cur_dir, "otqs", "get_symbols.otq")
    if remote:
        otq_path = f"remote://MY_DB::{otq_path}"

    q = query(otq_path, TT="TRD", DB=db.name, PATTERN="%")
    data = otp.DataSource(db=db, symbol=q, identify_input_ts=True)
    df = otp.run(data)
    assert all(df["SYMBOL_NAME"] == ["MY_DB::A", "MY_DB::B"])
    assert all(df["X"] == [1, 2])


class TestExprInEvalStr:
    def test_with_add(self, f_session, cur_dir):
        db = otp.DB("MY_DB")
        db.add(otp.Tick(X=1), symbol="AA", tick_type="TRD")
        db.add(otp.Tick(X=2), symbol="BB", tick_type="TRD")
        f_session.use(db)

        def func(symbol):
            q = query(os.path.join(cur_dir, "otqs", "get_symbols.otq"), TT="TRD", DB=db.name,
                      PATTERN=symbol.name + symbol.param)
            return otp.DataSource(db=db, symbol=q, identify_input_ts=True)

        data = otp.Empty()
        s = otp.Ticks(dict(SYMBOL_NAME=["A", "B"], param=["A", "B"]))
        data = otp.merge([data, func], symbols=s)
        df = otp.run(data)
        assert all(df["SYMBOL_NAME"] == ["MY_DB::AA", "MY_DB::BB"])
        assert all(df["X"] == [1, 2])

    def test_with_complex(self, f_session, cur_dir):
        db = otp.DB("MY_DB")
        db.add(otp.Tick(X=1), symbol="'B'A", tick_type="TRD")
        db.add(otp.Tick(X=2), symbol="'B'AA", tick_type="TRD")
        db.add(otp.Tick(X=3), symbol="'B'AAA", tick_type="TRD")
        f_session.use(db)

        def func(symbol):
            q = query(os.path.join(cur_dir, "otqs", "get_symbols.otq"), TT="TRD", DB=db.name,
                      PATTERN="'" + symbol.prefix + symbol.name * symbol.multiplier)
            return otp.DataSource(db=db, symbol=q, identify_input_ts=True)

        data = otp.Empty()
        s = otp.Ticks(dict(SYMBOL_NAME=["A"] * 3, prefix=["B'"] * 3, multiplier=[1, 2, 3]))
        data = otp.merge([data, func], symbols=s)
        df = otp.run(data)
        assert all(df["SYMBOL_NAME"] == ["MY_DB::'B'A", "MY_DB::'B'AA", "MY_DB::'B'AAA"])
        assert all(df["X"] == [1, 2, 3])


class TestQueryParams:
    def test_expr_with_quote(self, f_session, cur_dir):
        data = otp.Ticks(dict(X=[1, 2, 3], A=["A", "B'", "C"]))
        q = otp.query(
            os.path.join(cur_dir, "otqs", "test_query_params.otq"), COND=(data["X"] > 1) & (data["A"] == "B'")
        )
        res = data.apply(q)
        df = otp.run(res)
        assert all(df["X"] == [2])
        assert all(df["A"] == ["B'"])

    def test_const_expr(self, f_session, cur_dir):
        data = otp.Ticks(dict(X=[1, 2, 3], A=["A", "B'", "C"]))
        q = otp.query(os.path.join(cur_dir, "otqs", "test_query_params.otq"), COND='(X < 3) AND (A = "B\'")')
        res = data.apply(q)
        df = otp.run(res)
        assert all(df["X"] == [2])
        assert all(df["A"] == ["B'"])

    def test_preescaped_and_special_in_the_string(self, f_session, cur_dir):
        data = otp.Ticks(dict(X=[1, 2, 3], A=["A", "B'", ",C"]))
        q = otp.query(
            os.path.join(cur_dir, "otqs", "test_query_params.otq"),
            COND=r"""(X > 1) AND ((A \= "B'") OR (A \= ",C"))""",
        )
        res = data.apply(q)
        df = otp.run(res)
        assert all(df["X"] == [2, 3])
        assert all(df["A"] == ["B'", ",C"])

    def test_slashes_in_the_string(self, f_session, cur_dir):
        data = otp.Ticks(dict(X=[1, 2, 3], A=[r"\A", r"B\'", r"\,C"]))
        q = otp.query(
            os.path.join(cur_dir, "otqs", "test_query_params.otq"),
            COND=r'(A \= "\A") OR (A = "B\'") OR (A \= "\,C")',
        )
        res = data.apply(q)
        df = otp.run(res)
        assert all(df["X"] == [1, 2, 3])
        assert all(df["A"] == [r"\A", r"B\'", r"\,C"])

    def test_fail_on_injection(self, f_session, cur_dir):
        # comma should be replaced with \, to avoid wrong parameter parsing, (one parameter split to two)
        data = otp.Ticks(dict(X=[1, 2, 3], A=["A", "B'", "C"]))
        q = otp.query(os.path.join(cur_dir, "otqs", "test_query_params.otq"), COND="(X < 3), DISCARD_ON_MATCH=true")
        res = data.apply(q)
        with pytest.raises(Exception, match=r"Problem was encountered in expression: \(X < 3\), DISCARD_ON_MATCH=true"):
            otp.run(res)


@pytest.mark.parametrize(
    "s,r",
    [
        (",", r"\,"),
        ("", r""),
        (",=", r"\,\="),
        ('",', r'"\,'),
        ('","', r'","'),
        (',"', r'\,"'),
        ("','", r"','"),
        ('",",', r'","\,'),
        ("\",'\",'", "\",'\"\\,'"),
        ('",",","', r'","\,","'),
        (',",",",",",', r'\,","\,","\,"\,'),
        ('","=","', r'","\=","'),
        ("expr(_SYMBOL_PARAM.prefix + repeat(_SYMBOL_NAME, (atol(_SYMBOL_PARAM.multiplier))))", ) * 2
    ],
)
def test_inner_logic_slashes(s, r):
    assert otp.query._escape_characters_in_query_param(s) == r


def test_quotes(session, cur_dir):
    data = otp.Ticks({'A': ["a"]})

    string1 = """'FIELD1 = "abc", FIELD2 = 2.27'"""
    string2 = '''"FIELD1 = 'abc', FIELD2 = 2.27"'''

    q1 = otp.query(os.path.join(cur_dir, 'test_quotes.otq::test'), ADD=string1)
    q2 = otp.query(os.path.join(cur_dir, 'test_quotes.otq::test'), ADD=string2)
    result1 = data.apply(q1)
    result2 = data.apply(q2)
    df1 = otp.run(result1)
    df2 = otp.run(result2)
    assert df1['FIELD'][0] == string1.strip("'")
    assert df2['FIELD'][0] == string2.strip('"')
