import pytest
import datetime
from typing import Type

from contextlib import contextmanager

import pandas
import onetick.py as otp


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    yield m_session


def test_table_1():
    t = otp.Tick(x=3, y=0.5, z="abc")

    assert hasattr(t, "x") and t.x.dtype is int
    assert hasattr(t, "y") and t.y.dtype is float
    assert hasattr(t, "z") and t.z.dtype is str
    df = otp.run(t)
    assert df.x[0] == 3 and df.y[0] == 0.5 and df.z[0] == "abc"

    t_t = t[["x", "z"]]

    assert hasattr(t_t, "x") and t.x.dtype is int
    assert not hasattr(t_t, "y")
    assert hasattr(t_t, "z") and t.z.dtype is str
    df = otp.run(t)
    t_df = otp.run(t_t)
    assert df.x[0] == 3 and df.z[0] == "abc"
    assert not hasattr(t_df, "y")

    assert hasattr(t, "x") and t.x.dtype is int
    assert hasattr(t, "y") and t.y.dtype is float
    assert hasattr(t, "z") and t.z.dtype is str


def test_table_2():
    t = otp.Tick(x=3, y=0.5, z="abc")

    t_t = t[["x", "y", "z"]]

    assert hasattr(t_t, "x") and t_t.x.dtype is int
    assert hasattr(t_t, "y") and t_t.y.dtype is float
    assert hasattr(t_t, "z") and t_t.z.dtype is str

    df = otp.run(t)
    t_df = otp.run(t_t)
    assert t_df.x[0] == df.x[0]
    assert t_df.y[0] == df.y[0]
    assert t_df.z[0] == df.z[0]
    assert t_t != t


def test_table_3():
    t = otp.Tick(x=3, y=0.5, z="abc")

    t_t = t[[t.y, t.z]]

    assert not hasattr(t_t, "x")
    assert hasattr(t_t, "y") and t_t.y.dtype is float
    assert hasattr(t_t, "z") and t_t.z.dtype is str

    df = otp.run(t)
    t_df = otp.run(t_t)
    assert not hasattr(t_df, "x")
    assert t_df.y[0] == df.y[0]
    assert t_df.z[0] == df.z[0]


def test_table_4():
    t = otp.Tick(x=3, y=0.5, z="abc")

    with pytest.raises(ValueError):
        # it is not allowed to access by index
        t[[0, 1]]


def test_table_5():
    t = otp.Tick(x=3, y=0.5)

    with pytest.raises(AttributeError):
        # there is no 'z' column
        t[["z", "x"]]


def test_table_6():
    t = otp.Tick(x=3, y=0.35)

    with pytest.raises(AttributeError):
        # there is no 'z' column
        t[["x", "z"]]


def test_table_7():
    t = otp.Tick(x=3, y=0.35, account="acc")

    with pytest.raises(AttributeError):
        # there is no 'z' column
        t[[t.x, "z", t.account]]


def test_table_concat():
    t = otp.Tick(x=3, y=0.5, z="abc")

    t_t = t[[t.x, t.z]]

    m = otp.merge([t, t_t])
    assert len(otp.run(m)) == 2


class TestStrict:

    def test_negative_1(self):
        ''' Table on subset of columns from the original schema '''
        ticks = otp.Tick(X=3, Y=0.5, Z='a')

        assert len(ticks.schema) == 3

        ticks = ticks.table(**ticks.schema[['X', 'Z']], strict=False)

        assert len(ticks.schema) == 3
        assert set(ticks.schema.keys()) == {'X', 'Y', 'Z'}

        df = otp.run(ticks)
        assert len(df.columns) == 4

    def test_negative_2(self):
        ''' Table that extends existing columns '''
        ticks = otp.Ticks(X=[1])

        assert len(ticks.schema) == 1

        ticks = ticks.table(Y=float, Z=str, X=int, strict=False)

        assert len(ticks.schema) == 3
        assert set(ticks.schema.keys()) == {'X', 'Y', 'Z'}
        assert ticks.schema['X'] is int
        assert ticks.schema['Y'] is float
        assert ticks.schema['Z'] is str

        df = otp.run(ticks)
        assert len(df.columns) == 4

    def test_negative_3(self):
        ''' Table that extends the type '''
        ticks = otp.Ticks(X=[1], Y=[0], Z=[5])

        ticks = ticks.table(Y=float, X=float, strict=False)

        assert len(ticks.schema) == 3
        assert ticks.schema['X'] is float
        assert ticks.schema['Y'] is float
        assert ticks.schema['Z'] is int

    def test_positive_1(self):
        ticks = otp.Tick(X=3, Y=0.5, Z='a')

        ticks = ticks.table(**ticks.schema[['X', 'Z']])

        assert len(ticks.schema) == 2
        assert set(ticks.schema.keys()) == {'X', 'Z'}

        df = otp.run(ticks)
        assert len(df.columns) == 3  # including the 'Time' column
        assert 'X' in df.columns and 'Z' in df.columns

    def test_positive_2(self):
        ''' Table that extends columns '''
        ticks = otp.Ticks(X=[1], Y=[0], Z=[5])

        ticks = ticks.table(X=float, Z=int, W=str, strict=True)

        assert len(ticks.schema) == 3
        assert ticks.schema['X'] is float
        assert ticks.schema['Z'] is int
        assert ticks.schema['W'] is str


@contextmanager
def success():
    yield


class TestColumnDefining:
    @pytest.mark.parametrize("dtype", [int, float, str, otp.nsectime, otp.msectime, otp.string[1099]])
    def test_non_existing(self, dtype):
        t = otp.Empty(schema={"X": int})

        assert "X" in t.columns()
        assert t["X"].dtype is int
        assert "Y" not in t.columns()

        t.schema["Y"] = dtype

        assert "Y" in t.columns()
        assert t["Y"].dtype is dtype

    @pytest.mark.parametrize(
        "base_type,change_type,res_type,expect",
        [
            (int, int, int, success()),
            (int, float, float, success()),
            (int, str, None, pytest.raises(Warning)),
            (int, otp.nsectime, int, success()),
            (int, otp.msectime, int, success()),
            (int, otp.string[99], None, pytest.raises(Warning)),
            (float, otp.string[99], None, pytest.raises(Warning)),
            (float, otp.nsectime, None, pytest.raises(Warning)),
            (float, otp.msectime, None, pytest.raises(Warning)),
            (float, str, None, pytest.raises(Warning)),
            (float, float, float, success()),
            (float, int, float, success()),
            (str, otp.string[99], otp.string[99], success()),
            (str, otp.nsectime, None, pytest.raises(Warning)),
            (str, otp.msectime, None, pytest.raises(Warning)),
            (str, float, None, pytest.raises(Warning)),
            (str, int, None, pytest.raises(Warning)),
            (str, str, str, success()),
            (otp.msectime, otp.string[99], None, pytest.raises(Warning)),
            (otp.msectime, otp.nsectime, otp.nsectime, success()),
            (otp.msectime, otp.msectime, otp.msectime, success()),
            (otp.msectime, int, otp.msectime, success()),
            (otp.msectime, float, None, pytest.raises(Warning)),
            (otp.msectime, str, None, pytest.raises(Warning)),
            (otp.nsectime, otp.string[99], None, pytest.raises(Warning)),
            (otp.nsectime, otp.nsectime, otp.nsectime, success()),
            (otp.nsectime, otp.msectime, otp.nsectime, success()),
            (otp.nsectime, int, otp.nsectime, success()),
            (otp.nsectime, float, None, pytest.raises(Warning)),
            (otp.nsectime, str, None, pytest.raises(Warning)),
        ],
    )
    def test_existing_int(self, base_type, change_type, res_type, expect):
        with expect:
            t = otp.Empty(schema={"X": base_type})

            assert "X" in t.columns()
            assert t["X"].dtype is base_type

            with pytest.warns(DeprecationWarning, match=r'Using _set_field_by_tuple\(\) is not recommended'):
                t._set_field_by_tuple("X", change_type)

            assert "X" in t.columns()
            assert t["X"].dtype is res_type


class TestNativeTable:
    """ Check that the method .table on Source works correctly """

    @pytest.mark.parametrize('inplace', [False, True, list])
    @pytest.mark.parametrize('select', [
        dict(X=int, Y=float),
        dict(X=int, Z=str),
        dict(X=int, U=otp.nsectime),
        dict(Y=float, Z=str, U=otp.nsectime),
        dict(Y=None),
        dict(T='abc'),
        dict(A=1, X=int, B='xx'),
        dict(A=otp.nsectime(0), B=0.5, C=4, Z=str),
        dict(A=otp.nsectime(1641819624975907072), B=otp.msectime(12345678)),
        dict(A=otp.dt(2003, 12, 1), B=otp.dt(2003, 12, 1, 16, 5, 7, 431678, 922))
    ])
    def test_table(self, select, inplace):
        data = otp.Ticks(X=[1], Y=[0.1], Z=['a'], U=[otp.nsectime(0)])

        orig_schema = data.schema.copy()

        if isinstance(inplace, list):
            data = data[select]
        else:
            if inplace:
                data.table(**select, inplace=True)
            else:
                data = data.table(**select)

        df = otp.run(data, timezone='GMT')

        assert len(data.schema) == len(select)
        assert len(df.columns) == len(data.schema) + 1  # + Time
        for name, dtype in select.items():
            assert name in data.schema
            if dtype is not None:
                if isinstance(dtype, Type):
                    assert data.schema[name] is dtype
                else:
                    if not isinstance(dtype, (otp.nsectime, otp.msectime)):
                        assert df[name][0] == dtype
            else:
                assert data.schema[name] is orig_schema[name]
            assert name in df.columns

    def test_string_change_type(self):
        """ Validates that we can change type using the table"""
        data = otp.Tick(X='a' * 101)
        assert data['X'].dtype is otp.string[101]
        data['X'] = 'a' * 200
        assert data['X'].dtype is otp.string[101] and otp.run(data)['X'][0] == 'a' * 101

        # change type and try one more time
        data.table(X=otp.string[1024], inplace=True)
        data['X'] = 'a' * 200
        assert data['X'].dtype is otp.string[1024] and otp.run(data)['X'][0] == 'a' * 200


def test_table_nsectime():
    data = otp.Tick(A=1)
    with pytest.warns(match='milliseconds as nanoseconds'):
        data['T1'] = otp.nsectime(1)
        data = data.table(strict=False, T2=otp.nsectime(1))
    df = otp.run(data, timezone='GMT')
    assert df['T1'][0] == df['T2'][0]
    # TODO: change to nanoseconds after PY-441
    assert df['T1'][0] == pandas.Timestamp(1970, 1, 1, 0, 0, 0, 1000)


@pytest.mark.parametrize('tz', ['GMT', 'EST5EDT'])
@pytest.mark.parametrize('dt', [
    otp.date(2022, 1, 1),
    otp.datetime(2022, 1, 1),
    datetime.datetime(2022, 1, 1),
    datetime.date(2022, 1, 1),
    pandas.Timestamp(2022, 1, 1),
])
def test_table_datetime(dt, tz):
    data = otp.Tick(A=1)
    data['T1'] = dt
    data = data.table(strict=False, T2=dt)
    df = otp.run(data, timezone=tz)
    assert df['T1'][0] == df['T2'][0]
    assert df['T1'][0] == (pandas.Timestamp(dt) if isinstance(dt, datetime.date) else dt)


def test_table_datetime_do_not_override():
    t1, t2 = otp.datetime(2022, 1, 1), otp.datetime(2022, 2, 2)
    data = otp.Tick(A=0)
    data['T1'] = t1
    data = data.table(strict=False, T1=t2, T2=t2, A=1, B=1)
    df = otp.run(data)
    assert df['A'][0] == 0
    assert df['B'][0] == 1
    assert df['T1'][0] == t1
    assert df['T2'][0] == t2


def test_table_strict_true():
    t = otp.Tick(x=3, y=0.5, z="abc")
    t_t = t.table(strict=True, x=int, y=float)
    assert not hasattr(t_t, "z")


def test_table_strict_false():
    t = otp.Tick(x=3, y=0.5, z="abc")
    t_t = t.table(strict=False, x=int, y=float)
    assert hasattr(t_t, "z")


def test_table_slice_list():
    data = otp.Tick(x=3, y=0.5, z="abc")
    t = data[["y", "x"]:]
    assert list(otp.run(t).columns) == ['Time', 'y', 'x', 'z']
    assert hasattr(t, "x") and t.x.dtype is int
    assert hasattr(t, "y") and t.y.dtype is float
    assert hasattr(t, "z") and t.z.dtype is str


@pytest.mark.skip("not implemented yet, maybe even not necessary")
def test_table_slice_list_ending():
    data = otp.Tick(x=3, y=0.5, z="abc")
    t = data[:["y", "x"]]
    assert list(otp.run(t).columns) == ['Time', 'z', 'y', 'x']
    assert hasattr(t, "x") and t.x.dtype is int
    assert hasattr(t, "y") and t.y.dtype is float
    assert hasattr(t, "z") and t.z.dtype is str


def test_table_slice_columns():
    data = otp.Tick(x=3, y=0.5, z="abc")
    t = data[[data.y, data.x]:]
    assert list(otp.run(t).columns) == ['Time', 'y', 'x', 'z']
    assert hasattr(t, "x") and t.x.dtype is int
    assert hasattr(t, "y") and t.y.dtype is float
    assert hasattr(t, "z") and t.z.dtype is str


def test_table_slice_tuple():
    data = otp.Tick(x=3, y=0.5, z="abc")
    t = data[("y", int):]
    assert list(otp.run(t).columns) == ['Time', 'y', 'x', 'z']
    assert hasattr(t, "y") and t.y.dtype is int


def test_table_slice_tuple_list():
    data = otp.Tick(x=3, y=0.5, z="abc")
    t = data[[("y", int), ("z", str), ('x', float)]:]
    assert list(otp.run(t).columns) == ['Time', 'y', 'z', 'x']
    assert hasattr(t, "y") and t.y.dtype is int
    assert hasattr(t, "x") and t.x.dtype is float


def test_table_slice_different_types_error():
    data = otp.Tick(x=3, y=0.5, z="abc")
    with pytest.raises(AttributeError):
        data[[("y", int), "x"]:]


def test_table_slice_step():
    data = otp.Tick(x=3, y=0.5, z="abc")
    with pytest.raises(AttributeError):
        # slice with step makes no sense
        data[::['x']]


def test_table_slice_copy():
    data = otp.Tick(x=3, y=0.5, z="abc")
    t = data[:]
    assert otp.run(t).equals(otp.run(data))


def test_table_meta_fields():
    # testing meta fields not propagated to Table ep
    data = otp.Tick(A=1)
    with pytest.raises(ValueError, match="Can't set meta field TIMESTAMP"):
        data = data.table(**{'TIMESTAMP': otp.nsectime})
    with pytest.raises(ValueError, match="Can't set meta field TIMESTAMP"):
        data = data.table(**{'TIMESTAMP': otp.nsectime, 'A': int})
    with pytest.raises(ValueError, match="Can't set meta field _START_TIME"):
        data = data.table(**{'_START_TIME': otp.nsectime})
    # using this field would have raised an error in OneTick if it was in Table ep
    data['X'] = data['TIMESTAMP'] + 1
    otp.run(data)


def test_table_string_64():
    data = otp.Ticks(X=[1])
    data['Z'] = otp.string[8]('abc')
    data = data.table(Z=otp.string[63])
    data['Z'] = 'x' * 1000
    assert otp.run(data)["Z"][0] == 'x' * 63

    data = otp.Ticks(X=[1])
    data['Z'] = otp.string[8]('abc')
    data = data.table(Z=otp.string[64])
    data['Z'] = 'x' * 1000
    assert otp.run(data)["Z"][0] == 'x' * 64


def test_table_string_small_not_change():
    data = otp.Ticks(X=[1])
    data['Z'] = otp.string[8]('abc')
    # ott.string convertion must keep original string length
    data = data.table(Z=otp.string)
    data['Z'] = 'x' * 100
    assert len(otp.run(data)["Z"][0]) == 8


@pytest.mark.parametrize('dtype,result_dtype,default_value', [
    (int, 'long', 0),
    (float, 'double', float('nan')),
    (otp.decimal, 'decimal128', float('nan')),
    (otp.uint, 'uint', 0),
    (otp.ulong, 'ulong', 0),
    (otp.short, 'short', 0),
    (otp.byte, 'byte', 0),
    (otp.int, 'int', 0),
    (otp.long, 'long', 0),
    (str, 'string[64]', ''),
    (otp.string, 'string[64]', ''),
    (otp.string[123], 'string[123]', ''),
    (otp.varstring, 'varstring', ''),
    (otp.msectime, 'msectime', pandas.Timestamp(0)),
    (otp.nsectime, 'nsectime', pandas.Timestamp(0)),
])
def test_all_types(dtype, result_dtype, default_value):
    t = otp.Tick(A=1)
    t = t.table(**{'X': dtype}, strict=True)
    assert t.schema == {'X': dtype}

    def fun(tick):
        fields = otp.string[100]('')
        dtype = ''
        for field in otp.tick_descriptor_fields():
            dtype = field.get_type()
            fields += field.get_name() + ' ' + field.get_type()
            if dtype == 'string':
                fields += '[' + field.get_size().astype(str) + ']'
        tick['FIELD'] = fields

    t = t.script(fun)
    df = otp.run(t, timezone='GMT')
    assert df['FIELD'][0] == f'X {result_dtype}'
    if pandas.isna(default_value):
        assert pandas.isna(df['X'][0])
    else:
        assert df['X'][0] == default_value
