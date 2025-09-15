import pytest

import onetick.py as otp


def test_tick_rename_1(session):
    t = otp.Tick(x=99, y=0.35, z="abc")

    assert hasattr(t, "x")
    assert hasattr(t, "y")
    assert hasattr(t, "z")
    df = otp.run(t)
    assert hasattr(df, "x")
    assert hasattr(df, "y")
    assert hasattr(df, "z")

    t.x.rename("F")

    assert not hasattr(t, "x")
    assert hasattr(t, "F")
    assert hasattr(t, "y")
    assert hasattr(t, "z")
    df = otp.run(t)
    assert not hasattr(df, "x")
    assert hasattr(t, "F")
    assert hasattr(t, "y")
    assert hasattr(t, "z")


def test_tick_rename_2(session):
    t = otp.Tick(x=99, y=0.35, z="abc")

    # 'y' column is already exist
    with pytest.raises(AttributeError):
        t.x.rename("y")

    assert hasattr(t, "x")
    assert hasattr(t, "y")
    assert hasattr(t, "z")


def test_tick_rename_3(session):
    t = otp.Tick(x=99, y=0.35, z="abc")
    t_c = t.copy()

    t.x.rename("w")

    assert not hasattr(t, "x")
    assert hasattr(t, "w")
    assert hasattr(t, "y")
    assert hasattr(t, "z")
    df = otp.run(t)
    assert hasattr(df, "w")
    assert hasattr(df, "y")
    assert hasattr(df, "z")
    assert df.w[0] == 99
    assert df.y[0] == 0.35
    assert df.z[0] == "abc"

    assert not hasattr(t_c, "w")
    assert hasattr(t_c, "x")
    assert hasattr(t_c, "y")
    assert hasattr(t_c, "z")
    df_c = otp.run(t_c)
    assert df_c.x[0] == 99
    assert df_c.y[0] == 0.35
    assert df_c.z[0] == "abc"

    t_c.z.rename("str_field")

    assert hasattr(t_c, "str_field")
    assert not hasattr(t_c, "z")
    assert not hasattr(t, "str_field")
    assert hasattr(t, "z")

    assert otp.run(t_c).str_field[0] == df.z[0]


def test_tick_rename_4(session):
    t = otp.Tick(x=99, y=0.35, z="abc")

    # it is not allowed to set field with space
    with pytest.raises(ValueError) as e:
        t.y.rename("my field")
    assert 'not a valid field name' in str(e.value)

    assert hasattr(t, "y")


def test_tick_rename_5(session):
    t = otp.Tick(x=99, y=0.35, z="abc")

    t = t.rename({"x": "X", "y": "Y", "z": "Z"})

    assert not hasattr(t, "x")
    assert not hasattr(t, "y")
    assert not hasattr(t, "z")
    assert hasattr(t, "X")
    assert hasattr(t, "Y")
    assert hasattr(t, "Z")
    df = otp.run(t)
    assert df.X[0] == 99
    assert df.Y[0] == 0.35
    assert df.Z[0] == "abc"


def test_tick_rename_6(session):
    t = otp.Tick(x=99, y=0.35, z="abc")

    t = t.rename({t.x: "X", t.z: "Z"})

    assert not hasattr(t, "x")
    assert not hasattr(t, "z")
    assert hasattr(t, "X")
    assert hasattr(t, "y")
    assert hasattr(t, "Z")
    df = otp.run(t)
    assert df.X[0] == 99
    assert df.y[0] == 0.35
    assert df.Z[0] == "abc"

    t = t.rename({"X": "f1", "y": "f2", t.Z: "f3"})

    df = otp.run(t)
    assert df.f1[0] == 99
    assert df.f2[0] == 0.35
    assert df.f3[0] == "abc"


def test_tick_rename_7(session):
    t = otp.Tick(x=99, y=0.35, z="abc")

    # non-existing field 'field1'
    with pytest.raises(AttributeError):
        t.rename({"x": "f1", "field1": "field2", "y": "f2"})

    assert hasattr(t, "x")
    assert hasattr(t, "y")
    assert hasattr(t, "z")
    assert not hasattr(t, "field1")
    assert not hasattr(t, "f1")
    assert not hasattr(t, "f2")
    df = otp.run(t)
    assert df.x[0] == 99
    assert df.y[0] == 0.35
    assert df.z[0] == "abc"


def test_tick_rename_8(session):
    t = otp.Tick(x=99, y=0.35, z="abc")

    # there is a space in column name 'a b'
    with pytest.raises(ValueError) as e:
        t.rename({"x": "f1", "y": "a b", "z": "f2"})
    assert 'not a valid field name' in str(e.value)


def test_tick_rename_9(session):
    t = otp.Tick(x=99, y=0.35, z="abc")

    # two columns have the same target name
    with pytest.raises(AttributeError):
        t.rename({"x": "f1", "y": "f1"})


class TestRename:
    def test_simple_regex(self, session):
        data = otp.Tick(**{'X.X': 1, 'X.Y': 2})
        data = data.rename({r'X\.(.*)': r'\1'}, use_regex=True)
        assert data.schema == dict(X=int, Y=int)
        df = otp.run(data)
        assert df['X'][0] == 1
        assert df['Y'][0] == 2

    def test_regex_with_fields_to_skip(self, session):
        data = otp.Tick(**{'X.X': 1, 'X.Y': 2})
        data = data.rename({r'X\.(.*)': r'\1'}, use_regex=True, fields_to_skip=['X.Y'])
        assert data.schema == {'X': int, 'X.Y': int}
        df = otp.run(data)
        assert df['X'][0] == 1
        assert df['X.Y'][0] == 2

    @pytest.mark.parametrize('field_to_skip', [r'X\..*', 'X'])
    def test_regex_with_fields_to_skip_uses_regex(self, field_to_skip, session):
        data = otp.Tick(**{'X.X': 1, 'X.Y': 2, 'Y.Z': 3})
        data = data.rename({r'(.*)\.(.*)': r'\2'}, use_regex=True, fields_to_skip=[field_to_skip])
        assert data.schema == {'X.X': int, 'X.Y': int, 'Z': int}
        df = otp.run(data)
        assert df['X.X'][0] == 1
        assert df['X.Y'][0] == 2
        assert df['Z'][0] == 3

    def test_field_not_in_schema(self, session):
        data = otp.Tick(**{'X.X': 1, 'X.Y': 2, 'X.Z': 3})
        data = data.rename({r'X\.(.*)': r'\1'}, use_regex=True)
        assert data.schema == dict(X=int, Y=int, Z=int)
        df = otp.run(data)
        assert df['X'][0] == 1
        assert df['Y'][0] == 2
        assert df['Z'][0] == 3

    def test_pseudo_fields_not_in_schema(self, session):
        data = otp.Tick(A=1)
        data = data.rename({r'^(.*)$': r'__DET_\1'}, use_regex=True)
        assert data.schema == {'__DET_A': int}
        df = otp.run(data)
        assert list(df) == ['Time', '__DET_A']
        assert df['__DET_A'][0] == 1
