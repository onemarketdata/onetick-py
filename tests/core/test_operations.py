import datetime
import random

import pandas as pd
import pytest

import onetick.py as otp
import onetick.py.types as ott
from onetick.py.core.column_operations import _methods
from onetick.py.core.column_operations.base import _Operation


def _test_numeric_op(make_op_and_check, op_sign, int_op_dtype, float_op_dtype):
    a = make_op_and_check(1, 2, f"(1) {op_sign} (2)", int_op_dtype)
    make_op_and_check(a, 3, f"((1) {op_sign} (2)) {op_sign} (3)", int_op_dtype)
    make_op_and_check(1, a, f"(1) {op_sign} ((1) {op_sign} (2))", int_op_dtype)
    b = make_op_and_check(3, 4, f"(3) {op_sign} (4)", int_op_dtype)
    make_op_and_check(b, a, f"((3) {op_sign} (4)) {op_sign} ((1) {op_sign} (2))", int_op_dtype)
    make_op_and_check(-b, a, f"((-((3) {op_sign} (4)))) {op_sign} ((1) {op_sign} (2))", int_op_dtype)
    make_op_and_check(-1, a, f"(-1) {op_sign} ((1) {op_sign} (2))", int_op_dtype)
    make_op_and_check(-1.0, 2.5, f"(-1.0) {op_sign} (2.5)", float_op_dtype)
    make_op_and_check(-1.0, a, f"(-1.0) {op_sign} ((1) {op_sign} (2))", float_op_dtype)

    class MyInt(int):
        pass

    class MyFloat(float):
        pass

    expected = MyInt if int_op_dtype is int else float  # hack for div operation
    make_op_and_check(MyInt(1), MyInt(2), f"(1) {op_sign} (2)", expected)
    make_op_and_check(MyInt(1), MyFloat(2.5), f"(1) {op_sign} (2.5)", MyFloat)


def _make_one_binary_op_and_check(method):
    def func(a, b, str_op, dtype):
        op = _Operation(method, (a, b))
        assert str(op) == str_op
        assert op.dtype is dtype
        return op

    return func


def _make_one_unary_op_and_check(method):
    def func(a, str_op, dtype):
        op = _Operation(method, (a, ))
        assert str(op) == str_op
        assert op.dtype is dtype
        return op

    return func


def test_add():
    add = _make_one_binary_op_and_check(_methods.add)

    _test_numeric_op(add, "+", int, float)

    add("-1.0", "aaa", '"-1.0" + "aaa"', str)
    add("5", "aaa", '"5" + "aaa"', str)
    a = "5" * 99
    add(a, "aaa", f'"{a}" + "aaa"', ott.string[99])
    add(a, ott.string[100](''), f'"{a}" + ""', ott.string[100])


@pytest.mark.parametrize("a, b", [("a", 50.0), (5, "a")])
def test_add_wrong(a, b):
    # times tested in the separated class
    with pytest.raises(TypeError, match=r"Unsupported operand type\(s\) for \+ operation"):
        _Operation(_methods.add, (a, b))


def test_sub():
    sub = _make_one_binary_op_and_check(_methods.sub)
    # times tested in the separated class
    _test_numeric_op(sub, "-", int, float)


@pytest.mark.parametrize("a, b", [("a", 50.0), (5, "a"), ("a", "b")])
def test_sub_wrong(a, b):
    with pytest.raises(TypeError, match=r"Unsupported operand type\(s\) for - operation"):
        _Operation(_methods.sub, (a, b))


def test_mul():
    mul = _make_one_binary_op_and_check(_methods.mul)

    _test_numeric_op(mul, "*", int, float)
    mul(5, "a", 'repeat("a", (5))', str)
    mul(ott.string[100]("a"), 5, 'repeat("a", (5))', ott.string[100])  # we do not change length for now


@pytest.mark.parametrize("a, b", [("a", 50.0), (ott.string[1]("a"), 50.0), ("a", "b"),
                                  (ott.nsectime(), ott.nsectime())])
def test_mul_wrong(a, b):
    with pytest.raises(TypeError, match=r"Unsupported operand type\(s\) for \* operation"):
        _Operation(_methods.mul, (a, b))


def test_div():
    div = _make_one_binary_op_and_check(_methods.div)
    _test_numeric_op(div, "/", float, float)


@pytest.mark.parametrize("a, b", [("a", 50.0), (5, "a"), ("a", "b"), (ott.nsectime(), ott.nsectime())])
def test_div_wrong(a, b):
    with pytest.raises(TypeError, match=r"Unsupported operand type\(s\) for / operation"):
        _Operation(_methods.div, (a, b))


def test_mod():
    mod = _make_one_binary_op_and_check(_methods.mod)
    a = mod(1, 2, "mod((1), (2))", int)
    mod(a, 3, "mod((mod((1), (2))), (3))", int)
    mod(1, a, "mod((1), (mod((1), (2))))", int)
    b = mod(3, 4, "mod((3), (4))", int)
    mod(b, a, "mod((mod((3), (4))), (mod((1), (2))))", int)
    mod(-b, a, "mod(((-(mod((3), (4))))), (mod((1), (2))))", int)


@pytest.mark.parametrize("a, b", [("a", 50.0), (5, "a"), ("a", "b"), (ott.string("a"), "b"),
                                  (ott.nsectime(), ott.nsectime()), (5.0, 1)])
def test_mod_wrong(a, b):
    with pytest.raises(TypeError, match=r"Unsupported operand type\(s\) for mod operation"):
        _Operation(_methods.mod, (a, b))


def test_abs():
    abs_ = _make_one_unary_op_and_check(_methods.abs)

    a = abs_(-4, "abs(-4)", int)
    abs_(a, "abs(abs(-4))", int)
    abs_(4.0, "abs(4.0)", float)

    class MyInt(int):
        pass
    abs_(MyInt(-4), "abs(-4)", MyInt)

    class MyFloat(float):
        pass
    abs_(MyFloat(1.0), "abs(1.0)", MyFloat)


@pytest.mark.parametrize("a", ["a", ott.string[5]("a"), ("a", "b"), ott.nsectime()])
def test_abs_wrong(a):
    with pytest.raises(TypeError, match="Operation is not supported for type"):
        _Operation(_methods.abs, (a, ))


def _test_pos_neg(make_op_and_check, op_sign):
    make_op_and_check(-4, f"({op_sign}(-4))", int)
    a = _Operation(_methods.add, (5, 4))
    a = make_op_and_check(a, f"({op_sign}((5) + (4)))", int)
    make_op_and_check(a, f"({op_sign}(({op_sign}((5) + (4)))))", int)
    make_op_and_check(9.0, f"({op_sign}(9.0))", float)

    class MyInt(int):
        pass
    make_op_and_check(MyInt(-4), f"({op_sign}(-4))", MyInt)

    class MyFloat(float):
        pass
    make_op_and_check(MyFloat(1.0), f"({op_sign}(1.0))", MyFloat)


def test_pos():
    pos = _make_one_unary_op_and_check(_methods.pos)
    _test_pos_neg(pos, "+")


@pytest.mark.parametrize("a", ["a", ott.string[5]("a"), ("a", "b"), ott.nsectime(), []])
def test_pos_wrong(a):
    with pytest.raises(TypeError, match="Operation is not supported for type"):
        _Operation(_methods.pos, (a,))


def test_neg():
    neg = _make_one_unary_op_and_check(_methods.neg)
    _test_pos_neg(neg, "-")


@pytest.mark.parametrize("a", ["a", ott.string[5]("a"), ("a", "b"), ott.nsectime(), None])
def test_neg_wrong(a):
    with pytest.raises(TypeError, match="Operation is not supported for type"):
        _Operation(_methods.neg, (a,))


def test_mix():
    a = _Operation(_methods.add, (5, 4))
    assert str(a) == "(5) + (4)"
    assert a.dtype is int
    b = _Operation(_methods.mul, (4, a))
    assert str(b) == "(4) * ((5) + (4))"
    assert b.dtype is int
    b = _Operation(_methods.mod, (4, a))
    assert str(b) == "mod((4), ((5) + (4)))"
    assert b.dtype is int
    c = _Operation(_methods.div, (b, -5.4))
    assert str(c) == "(mod((4), ((5) + (4)))) / (-5.4)"
    assert c.dtype is float
    c = _Operation(_methods.abs, (b, ))
    assert str(c) == "abs(mod((4), ((5) + (4))))"
    assert c.dtype is int
    b = _Operation(_methods.neg, (a, ))
    c = _Operation(_methods.mul, (b, 4.9))
    assert str(c) == "((-((5) + (4)))) * (4.9)"
    assert c.dtype is float


def _test_compare_op(make_op_and_check, op_sign):
    make_op_and_check("a", "b", f'"a" {op_sign} "b"', bool)
    a = _Operation(_methods.add, ("a", "b"))
    make_op_and_check(a, ott.string[5]("c"), f'"a" + "b" {op_sign} "c"', bool)
    make_op_and_check(1, 2.0, f"(1) {op_sign} (2.0)", bool)
    a = _Operation(_methods.add, (1, 0))
    make_op_and_check(a, 5, f"((1) + (0)) {op_sign} (5)", bool)


def test_eq():
    eq = _make_one_binary_op_and_check(_methods.eq)
    # times tested in the separated class
    _test_compare_op(eq, "=")


def test_neq():
    ne = _make_one_binary_op_and_check(_methods.ne)
    # times tested in the separated class
    _test_compare_op(ne, "!=")


def test_mix_compare():
    a = _Operation(_methods.ne, (5, 4))
    assert str(a) == "(5) != (4)"
    assert a.dtype is bool
    b = _Operation(_methods.mul, (4, a))
    assert str(b) == "(4) * ((5) != (4))"
    assert b.dtype is int
    a = _Operation(_methods.add, (5, 4))
    b = _Operation(_methods.gt, (4, a))
    assert str(b) == "(4) > ((5) + (4))"
    assert b.dtype is bool
    b = _Operation(_methods.div, (a, -5.4))
    c = _Operation(_methods.gt, (b, a))
    assert str(c) == "(((5) + (4)) / (-5.4)) > ((5) + (4))"
    assert c.dtype is bool
    a = _Operation(_methods.ne, (0, 0))
    b = _Operation(_methods.add, (a, 5))
    assert str(b) == "((0) != (0)) + (5)"
    assert b.dtype is int

    class MyFloat(float):
        pass

    a = _Operation(_methods.eq, (0, 0))
    b = _Operation(_methods.div, (a, MyFloat(2.3)))
    assert str(b) == "((0) = (0)) / (2.3)"
    assert b.dtype is MyFloat


def test_and():
    and_ = _make_one_binary_op_and_check(_methods.and_)
    a = _Operation(_methods.ne, (5, 4))
    b = _Operation(_methods.le, (5, 4))
    and_(a, b, "((5) != (4)) AND ((5) <= (4))", bool)


def test_or():
    or_ = _make_one_binary_op_and_check(_methods.or_)
    a = _Operation(_methods.eq, (5, 4))
    b = _Operation(_methods.ge, (5, 4))
    or_(a, b, "((5) = (4)) OR ((5) >= (4))", bool)


@pytest.mark.parametrize("a, b", [("a", 50.0), (ott.string[1]("a"), 50.0), (_Operation(_methods.ne, (5, 4)), 1.0)])
def test_compare_wrong(a, b):
    method = random.choice((_methods.eq, _methods.ne, _methods.ge, _methods.le, _methods.gt, _methods.lt))
    with pytest.raises(TypeError, match=r"Unsupported operand type\(s\) for (==)|(!=)|(>=)|(<=)|(>)|(<)"):
        _Operation(method, (a, b))


@pytest.mark.parametrize("a, b", [("a", 50.0), (ott.string[1]("a"), 50.0), (_Operation(_methods.ne, (5, 4)), 1.0),
                                  ("a", "b"), (1, 5.0)])
def test_and_or_wrong(a, b):
    method = random.choice((_methods.and_, _methods.or_))
    with pytest.raises(TypeError, match=r"Unsupported operand type\(s\) for (AND)|(OR)"):
        _Operation(method, (a, b))


class TestTimeAddSub:
    # Between datetime (msectime and nsectime) types, only the - operator is allowed.
    # It is also possible to add an integral value to datetime or subtract an integer from it.
    # Any operation between floating-point and datetime types are not allowed in tick script,
    # but supported in EP, so we also allow such operation, but generate warning.
    @pytest.fixture(scope="class")
    def data(self):
        data = otp.Ticks(dict(NSEC=[ott.nsectime(1)], MSEC=[ott.msectime(1)], INT=[1]))
        yield data

    @pytest.mark.parametrize("field, summand, expected_dtype", [["NSEC", 1, ott.nsectime], ["MSEC", 2, ott.msectime],
                                                                ["NSEC", 0, ott.nsectime], ["MSEC", -2, ott.msectime]])
    def test_time_add_correct(self, data, field, summand, expected_dtype):
        op = data[field] + summand
        assert f"({field}) + ({summand})" == str(op)
        assert expected_dtype == op.dtype

    @pytest.mark.parametrize("field, summand", [("NSEC", "string"), ("NSEC", None)])
    def test_time_add_wrong(self, data, field, summand):
        with pytest.raises(TypeError, match=r"Unsupported operand type\(s\) for \+ operation: "):
            _ = data[field] + summand

    @pytest.mark.parametrize("field, summand", [("NSEC", 1.32), ("MSEC", 0.0)])
    def test_time_add_sup_float(self, data, field, summand):
        msg = "^Onetick will shrink the fractional part$"
        with pytest.warns(Warning, match=msg):
            _ = data[field] + summand
        with pytest.warns(Warning, match=msg):
            _ = data[field] - summand

    @pytest.mark.parametrize("field1, field2", [("NSEC", "NSEC"), ("NSEC", "MSEC"), ("MSEC", "MSEC")])
    def test_time_add(self, data, field1, field2):
        with pytest.raises(TypeError, match=r"Unsupported operand type\(s\) for \+ operation: "):
            _ = data[field1] + data[field2]

    def test_time_sub(self, data):
        op = data["NSEC"] - data["MSEC"]
        with pytest.warns(FutureWarning):
            assert str(op) == "(NSEC) - (MSEC)"
        assert op.dtype is int

    def test_compare(self, data):
        op = data["NSEC"] > data["NSEC"]  # NOSONAR
        assert str(op) == "(NSEC) > (NSEC)"
        assert op.dtype is bool

    def test_compare_with_const(self, data):
        op = data["NSEC"] == 0
        assert str(op) == "(NSEC) = (0)"
        assert op.dtype is bool
        op = data["MSEC"] != 0
        assert str(op) == "(MSEC) != (0)"
        assert op.dtype is bool


class TestComparation:
    @pytest.mark.parametrize("date", [
        datetime.datetime(otp.config['default_start_time'].year,
                          otp.config['default_start_time'].month,
                          otp.config['default_start_time'].day).replace(microsecond=2500),
        pd.Timestamp(otp.config['default_start_time']).replace(microsecond=2500),
        otp.datetime(otp.config['default_start_time']).replace(microsecond=2500),
        int(otp.datetime(otp.config['default_start_time']).replace(microsecond=2500).timestamp() * 1000)
    ])
    def test_comparition(self, c_session, date):
        # PY-283
        data = otp.Ticks(X=[1, 2, 3, 4, 5])
        data, _ = data[data["Time"] <= date]
        df = otp.run(data, timezone="GMT")
        assert all(df["X"] == [1, 2, 3])


def test_datetime_sub(session):
    t = otp.Tick(A=1)
    t['N1'] = otp.datetime(2022, 1, 1)
    t['N3'] = otp.datetime(2022, 1, 3)
    t['N2'] = t.mean('N1', 'N3')
    with pytest.warns(match='^Subtracting datetimes without specifying resulted time unit is deprecated.'):
        t['NSUB'] = t['N3'] - t['N1']
    t['NMS'] = otp.Milli(t['N3'] - t['N1'])
    t['NNS'] = otp.Nano(t['N3'] - t['N1'])
    df = otp.run(t)
    assert df['N2'][0] == otp.datetime(2022, 1, 2)
    assert df['NSUB'][0] == 2 * 24 * 60 * 60 * 1000
    assert df['NMS'][0] == 2 * 24 * 60 * 60 * 1000
    assert df['NNS'][0] == 2 * 24 * 60 * 60 * 1_000_000_000


def test_datetime_sub_compare(session):
    t = otp.Ticks({'A': [0, 1, 2]})
    op = otp.Nano(t['TIMESTAMP'] - t['_START_TIME']) >= 1_000_000
    assert str(op) == "(DATEDIFF('nanosecond', (_START_TIME), (TIMESTAMP), _TIMEZONE)) >= (1000000)"
    t, _ = t[op]
    df = otp.run(t)
    assert list(df['A']) == [1, 2]


def test_sub_nano_is_not_milli(session):
    t = otp.Ticks({'A': [0, 1, 2],
                   'N': [otp.dt(otp.config.default_start_time) + otp.Milli(1)] * 3})
    with pytest.warns(match='^Subtracting datetimes without specifying resulted time unit is deprecated.'):
        t['WARNING'] = t['Time'] - t['N']
    t['MILLI'] = otp.Milli(t['Time'] - t['N'])
    t['NANO'] = otp.Nano(t['Time'] - t['N'])
    t['HOUR'] = otp.Hour(t['Time'] - t['N'])
    df = otp.run(t)
    assert list(df['MILLI']) == list(df['WARNING'])
    assert all(df['HOUR'] == 0)


def test_no_recursion(session):
    t = otp.Tick(A=1)
    x = t['A'] + 1
    for _ in range(1, 1000):
        x = x + (t['A'] + 1)
    t['X'] = x
    df = otp.run(t)
    assert df['X'][0] == (1 + 1) * 1000


def test_replace_parameters(session):
    t = otp.Tick(A=1, B=2)

    op = t['A'] + 1

    assert str(op) == '(A) + (1)'

    def fun(param):
        return otp.Column('B', dtype=int)

    new_op = op._replace_parameters(fun)

    assert str(op) == '(A) + (1)'
    assert str(new_op) == '(B) + (B)'


def test_double_join(session):
    d1 = otp.Ticks({'ID': [1, 2, 3], 'A': ['a', 'b', 'c']})
    d2 = otp.Ticks({'ID': [2, 3, 4], 'B': ['q', 'w', 'e']})

    op = (d1['ID'] == d2['ID']) & (d1['ID'] == d2['ID']) & (d1['ID'] == d2['ID'])  # NOSONAR

    assert str(op) == '(((ID) = (ID)) AND ((ID) = (ID))) AND ((ID) = (ID))'

    data = otp.join(d1, d2, on=op, how='left_outer')
    df1 = otp.run(data)

    assert str(op) == '(((ID) = (ID)) AND ((ID) = (ID))) AND ((ID) = (ID))'

    data = otp.join(d1, d2, on=op, how='left_outer')
    df2 = otp.run(data)

    assert str(op) == '(((ID) = (ID)) AND ((ID) = (ID))) AND ((ID) = (ID))'
    assert df2.equals(df1)
