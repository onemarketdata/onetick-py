import pytest

import onetick.py as otp
import numpy as np


Column = otp.core.column._Column
ott = otp.types


def test_types():
    assert str(Column("x")) == "x"
    assert Column("x").dtype is float
    assert Column("x", int).dtype is int
    assert Column("y", str).dtype is str


def test_eq():
    assert str(Column('x') == Column('y')) == "(x) = (y)"
    assert str(Column('x', str) == 'abc') == 'x = "abc"'

    with pytest.raises(TypeError):
        # column type by default is float, but compare with string
        Column("x") == "abc"  # NOSONAR


def test_add():
    assert str(Column("x") + Column("y")) == "(x) + (y)"
    assert str(Column("x") + 1) == "(x) + (1)"
    assert str(1 + Column("x") + 2) == "((1) + (x)) + (2)"
    assert str(Column("x") + 0.5) == "(x) + (0.5)"


def test_mul():
    assert str(-Column("x") * 3) == "((-(x))) * (3)"
    assert str(4 + Column("x") * 0.5) == "(4) + ((x) * (0.5))"


def test_mod():
    assert str(Column("x", int) % 3) == "mod((x), (3))"


class TestIn:
    def test_int(self, session):
        data = otp.Ticks(dict(A=[1, 2, 3, 0, 5], B=[1, 1, -1, 0, 2]))
        yes, _ = data[data["A"].isin(3, 1, 2 * data["B"])]
        df = otp.run(yes)
        assert all(df["A"] == [1, 2, 3, 0])

    def test_str(self, session):
        data = otp.Ticks(dict(A=["ab", "cv", "bc", "a", "d"]))
        yes, _ = data[data["A"].isin("bc", "cv", "d")]
        df = otp.run(yes)
        assert all(df["A"] == ["cv", "bc", "d"])

    def test_str_column(self, session):
        data = otp.Ticks(dict(A=["ab", "cv", "bc", "a", "d"], B=["a", "c", "b", "a", "a"]))
        yes, _ = data[data["A"].isin(data["B"])]
        df = otp.run(yes)
        assert all(df["A"] == ["a"])
        assert all(df["B"] == ["a"])

    def test_str_expr(self, session):
        data = otp.Ticks(dict(A=["ab", "cv", "bc", "a", "d"], B=["a", "c", "b", "a", "a"]))
        yes, _ = data[data["A"].isin(data["B"] + "b")]
        df = otp.run(yes)
        assert all(df["A"] == ["ab"])
        assert all(df["B"] == ["a"])

    def test_assignment(self, session):
        data = otp.Ticks(dict(X=["a", "ab", "bab"]))
        data["Z"] = data["X"].isin("ab")
        df = otp.run(data)
        assert data["Z"].dtype == float
        assert df["Z"].dtype == float
        assert all(df == [0.0, 1.0, 0.0])

    def test_empty(self, session):
        data = otp.Ticks(dict(X=["a", "ab", "bab"]))
        with pytest.raises(ValueError, match=r"Method isin\(\) can't be used without values"):
            _ = data["X"].isin()
        with pytest.raises(ValueError, match=r"Method isin\(\) can't be used without values"):
            _ = data["X"].isin([])

    def test_sequence(self, session):
        data = otp.Ticks(dict(X=["a", "ab", "bab"]))
        with pytest.raises(ValueError, match=r"If the first argument of isin\(\) function is a list"):
            _ = data["X"].isin(["a", "ab"], "c")
        data["Z"] = data["X"].isin(["a", "ab"])
        df = otp.run(data)
        assert all(df == [1.0, 1.0, 0.0])


class TestMap:
    def test_map(self, session):
        data = otp.Ticks(dict(X=["a", "ab", "bab"]))
        data["Z"] = data["X"].map({"a": "A", "bab": "BAB"})
        assert data["Z"].dtype == str
        assert data.schema["Z"] == str
        df = otp.run(data)
        assert df["Z"].dtype == np.dtype('O')
        assert all(df["Z"] == ["A", "", "BAB"])

    def test_map_mixed_value_types(self, session):
        data = otp.Ticks(dict(X=["a"]))
        with pytest.raises(TypeError, match="argument must be a dict with same types for all values"):
            data["Z"] = data["X"].map({"a": "A", "bab": 123})

    def test_map_mixed_key_types(self, session):
        data = otp.Ticks(dict(X=["a"]))
        with pytest.raises(TypeError, match="argument must be a dict with same types for all keys"):
            data["Z"] = data["X"].map({"a": "A", 1: "BAB"})

    def test_map_column_type_compatible(self, session):
        data = otp.Ticks(dict(X=[1.1]))
        with pytest.raises(TypeError, match="must be compatible with column type"):
            data["Z"] = data["X"].map({"1.1": 1.2})

    def test_map_with_na(self, session):
        data = otp.Ticks(dict(X=[1.1, ott.nan]))
        data["Z"] = data["X"].map({1.1: 1.2, otp.nan: 1.3})
        df = otp.run(data)
        assert df["Z"].dtype == float
        assert all(df["Z"] == [1.2, 1.3])

    def test_map_existing_column_type(self, session):
        data = otp.Ticks(dict(X=["a", "ab", "bab"]))
        data["Z"] = 123
        assert data["Z"].dtype == int
        data["Z"] = data["X"].map({"a": "A", "bab": "BAB"})
        assert data["Z"].dtype == str
        df = otp.run(data)
        assert df["Z"].dtype == np.dtype('O')
        assert all(df["Z"] == ["A", "", "BAB"])

    def test_map_int_float_type(self, session):
        data = otp.Ticks(dict(X=[1.1, 2, 3]))
        assert data.schema["X"] == float
        # mix of int and float is ok
        data["Z"] = data["X"].map({1.1: 1.2, 4: 4})
        assert data["Z"].dtype == float
        df = otp.run(data)
        assert df["Z"].dtype == float
        assert all(df["Z"] == [1.2, 0, 0])

    def test_map_default(self, session):
        data = otp.Ticks(dict(A=["a", "ab", "bab"]))
        data["B"] = 12345
        with pytest.raises(TypeError):
            data["A"].map({"a": "A", "bab": "BAB"}, default=12345)
        with pytest.raises(TypeError):
            data["A"].map({"a": "A", "bab": "BAB"}, default=data["B"])
        data["X"] = data["A"].map({"a": "A", "bab": "BAB"}, default="default")
        data["Y"] = data["A"].map({"a": "A", "bab": "BAB"}, default=data["X"])
        data["Z"] = data["A"].map({"a": "A", "bab": "BAB"}, default=data["Y"].str.upper())
        df = otp.run(data)
        assert all(df["X"] == ["A", "default", "BAB"])
        assert all(df["Y"] == ["A", "default", "BAB"])
        assert all(df["Z"] == ["A", "DEFAULT", "BAB"])


def test_if():

    with pytest.raises(TypeError):
        # it is not allowed to use _Column in if-where clause
        if Column("x"):
            pass  # NOSONAR

    with pytest.raises(TypeError):
        # it is not allowed to use _Column in if-where clause
        if (3 != 4) and Column("x"):    # noqa
            pass

    with pytest.raises(TypeError):
        # it is not allowed to us _Column in while clause
        while Column("x"):
            pass  # NOSONAR


def test_for():

    with pytest.raises(TypeError):
        # it is not allowed to use _Column in for clause
        for _ in Column("x"):
            pass  # NOSONAR


def test_init():

    with pytest.raises(TypeError):
        # it is allowed to pass only types as type
        Column("y", 4)

    with pytest.raises(TypeError):

        class MyClass:
            pass

        Column("z", MyClass)

    class MyStr(str):
        pass

    assert Column("z", MyStr).dtype is MyStr

    class MyFloat(float):
        pass

    assert Column("x", MyFloat).dtype is MyFloat

    class MyInt(int):
        pass

    assert Column("x", MyInt).dtype is MyInt
    assert Column("x", ott.string[77]).dtype is ott.string[77]


def test_copy():
    c1 = Column("x", float)
    c2 = c1.copy()

    assert c1.name == c2.name
    assert c1.dtype == c2.dtype


# def


def test_rename():
    c = Column("x")

    c_c = c.copy()

    assert c.name == "x"
    assert c_c.name == "x"

    c.rename("y")

    assert c.name == "y"
    assert c_c.name == "x"


class TestTimeAlias:
    """ test suite validates alias between the Time and TIMESTAMP columns """

    def test_time_alias_1(self, session):
        t = otp.Tick(x=3, end=otp.config['default_start_time'] + otp.Milli(1000))

        t_before = otp.run(t).Time[0]

        t.Time += 500

        t_after = otp.run(t).Time[0]

        assert t_after - t_before == otp.timedelta(milliseconds=500)

    def test_time_alias_1_1(self, session):
        t = otp.Tick(x=3, end=otp.config['default_start_time'] + otp.Milli(1000))

        t_before = otp.run(t).Time[0]

        t.TIMESTAMP += 500

        t_after = otp.run(t).Time[0]

        assert t_after - t_before == otp.timedelta(milliseconds=500)

    def test_time_alias_2(self, session):
        t = otp.Tick(x=3, end=otp.config['default_start_time'] + otp.Milli(1000))

        t_before = otp.run(t).Time[0]

        t["Time"] += 500

        t_after = otp.run(t).Time[0]

        assert t_after - t_before == otp.timedelta(milliseconds=500)

    def test_time_alias_2_1(self, session):
        t = otp.Tick(x=3, end=otp.config['default_start_time'] + otp.Milli(1000))

        t_before = otp.run(t).Time[0]

        t["TIMESTAMP"] += 500

        t_after = otp.run(t).Time[0]

        assert t_after - t_before == otp.timedelta(milliseconds=500)

    def test_time_alias_3(self, session):
        t = otp.Tick(x=3, end=otp.config['default_start_time'] + otp.Milli(1000))

        t_before = otp.run(t).Time[0]

        t["Time"] = t["Time"] + 500

        t_after = otp.run(t).Time[0]

        assert t_after - t_before == otp.timedelta(milliseconds=500)

    def test_time_alias_3_1(self, session):
        t = otp.Tick(x=3, end=otp.config['default_start_time'] + otp.Milli(1000))

        t_before = otp.run(t).Time[0]

        t["Time"] = t["TIMESTAMP"] + 500

        t_after = otp.run(t).Time[0]

        assert t_after - t_before == otp.timedelta(milliseconds=500)

    def test_time_alias_4(self, session):
        t = otp.Tick(x=3, end=otp.config['default_start_time'] + otp.Milli(1000))

        t_before = otp.run(t).Time[0]

        t["Time"] = t.Time.apply(lambda v: v + 500)

        t_after = otp.run(t).Time[0]

        assert t_after - t_before == otp.timedelta(milliseconds=500)


class TestCumsum:
    def test_cumsum(self, session):
        t = otp.Ticks({'A': list(range(10))})
        t['X'] = t['A'].cumsum()
        assert t.schema['X'] is int
        assert t.schema['A'] is int
        df = otp.run(t)
        assert list(df['X']) == [0, 1, 3, 6, 10, 15, 21, 28, 36, 45]

    def test_update(self, session):
        t = otp.Ticks({'A': [1, 2, 3]})
        t['A'] = t['A'].cumsum()
        assert t.schema['A'] is int
        assert list(t.schema) == ['A']
        df = otp.run(t)
        assert list(df['A']) == [1, 3, 6]


@pytest.mark.parametrize(
    'type', ['tick_gen', 'new_field', 'rename_field']
)
@pytest.mark.parametrize(
    'field_name,valid',
    [
        ('A' * 127, True),
        ('A' * 128, False),
        ('ABCabc', True),
        ('AZ123_.', True),
        ('A-b', False),
        ('A,b', False),
        ('A1!', False),
    ]
)
def test_field_name_correctness(session, type, field_name, valid):
    """
    onetick.py should fail on invalid field names
    """
    if not valid:
        with pytest.raises(ValueError) as e:
            if type == 'tick_gen':
                otp.Tick(**{field_name: 1})
            else:
                src = otp.Tick(A=1)
                if type == 'new_field':
                    src[field_name] = 1
                elif type == 'rename_field':
                    src.rename(dict(A=field_name))
        assert 'not a valid field name' in str(e.value)

    else:
        if type == 'tick_gen':
            src = otp.Tick(**{field_name: 1})
        else:
            src = otp.Tick(A=1)
            if type == 'new_field':
                src[field_name] = 1
            elif type == 'rename_field':
                src = src.rename(dict(A=field_name))
        res = otp.run(src)
        assert res[field_name][0] == 1
