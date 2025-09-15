import pytest
import numpy as np
import onetick.py as otp


def test_update_fields_1(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.34, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    assert issubclass(data.x.dtype, int)
    assert issubclass(data.y.dtype, float)
    df = otp.run(data)
    assert df.x[0] == 33 and df.y[0] == 0.33
    assert df.x[1] == 34 and df.y[1] == 0.34
    assert df.x[2] == 35 and df.y[2] == 0.35

    data = data.update({data.x: data.x / 3, data.y: data.y / 3}, where=(data.x == 33))

    assert hasattr(data, 'x')
    assert hasattr(data, 'y')
    assert issubclass(data.x.dtype, int)  # NOTE: UPDATE_FIELDS don't change type in opposite to UPDATE_FIELD
    assert issubclass(data.y.dtype, float)

    df = otp.run(data)
    assert not isinstance(df.x[0], np.float64)
    assert df.x[0] == 11 and df.y[0] == 0.11
    assert df.x[1] == 34 and df.y[1] == 0.34
    assert df.x[2] == 35 and df.y[2] == 0.35


def test_update_fields_2(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.34, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    data = data.update({data.x: data.x * 0.1}, {data.x: data.x * 0.2}, data.x == 34)

    assert issubclass(data.x.dtype, int)

    df = otp.run(data)
    assert not isinstance(df.x[0], np.float64)
    assert not isinstance(df.x[1], np.float64)
    assert not isinstance(df.x[2], np.float64)
    assert df.x[0] == 6 and df.y[0] == 0.33
    assert df.x[1] == 3 and df.y[1] == 0.34
    assert df.x[2] == 7 and df.y[2] == 0.35


def test_update_field_3(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    data = data.update({'x': data.x * data.y}, where=data.x != data.y * 100)

    assert issubclass(data.x.dtype, int)

    df = otp.run(data)
    assert df.x[0] == 33 and df.y[0] == 0.33
    assert df.x[1] == int(34 * 0.341) and df.y[1] == 0.341
    assert df.x[2] == 35 and df.y[2] == 0.35


def test_update_field_4(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    data = data.update({'x': data.x * data.y}, {'y': 0}, where=data.x != data.y * 100)

    assert issubclass(data.x.dtype, int)

    df = otp.run(data)
    assert df.x[0] == 33 and df.y[0] == 0
    assert df.x[1] == int(34 * 0.341) and df.y[1] == 0.341
    assert df.x[2] == 35 and df.y[2] == 0


def test_update_field_5(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    # it is allowed to key be either column or column name
    with pytest.raises(AttributeError):
        data.update({1: data.x * data.y}, {'y': 0}, where=data.x != data.y * 100)

    df = otp.run(data)
    assert df.x[0] == 33 and df.y[0] == 0.33
    assert df.x[1] == 34 and df.y[1] == 0.341
    assert df.x[2] == 35 and df.y[2] == 0.35


def test_update_field_6(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    # it is allowed to key be either column or column name
    with pytest.raises(AttributeError):
        data.update({'x': data.x * data.y}, {0: 0}, where=data.x != data.y * 100)

    df = otp.run(data)
    assert df.x[0] == 33 and df.y[0] == 0.33
    assert df.x[1] == 34 and df.y[1] == 0.341
    assert df.x[2] == 35 and df.y[2] == 0.35


def test_update_field_7(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    # it is not allowed to have empty if_set parameter
    with pytest.raises(ValueError):
        data.update({}, {'y': 0}, where=data.x != data.y * 100)

    df = otp.run(data)
    assert df.x[0] == 33 and df.y[0] == 0.33
    assert df.x[1] == 34 and df.y[1] == 0.341
    assert df.x[2] == 35 and df.y[2] == 0.35


def test_update_field_8(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    # 'z' is non-existing column
    with pytest.raises(AttributeError):
        data.update({'x': data.y, 'z': data.x}, where=data.x != data.y * 100)

    df = otp.run(data)
    assert df.x[0] == 33 and df.y[0] == 0.33
    assert df.x[1] == 34 and df.y[1] == 0.341
    assert df.x[2] == 35 and df.y[2] == 0.35


def test_update_field_9(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    # 'z' is non-existing column
    with pytest.raises(AttributeError):
        data.update({'x': data.y}, {data.y: 0, 'z': 'abc'}, where=data.x != data.y * 100)

    df = otp.run(data)
    assert df.x[0] == 33 and df.y[0] == 0.33
    assert df.x[1] == 34 and df.y[1] == 0.341
    assert df.x[2] == 35 and df.y[2] == 0.35


def test_update_field_10(session):
    my_var = 3

    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    # check False
    data = data.update({'x': data.x * data.y}, {'y': 0}, where=my_var != 3)

    assert issubclass(data.x.dtype, int)

    df = otp.run(data)
    assert df.x[0] == 33 and df.y[0] == 0
    assert df.x[1] == 34 and df.y[1] == 0
    assert df.x[2] == 35 and df.y[2] == 0


def test_update_field_11(session):
    my_var = 3

    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    # check True
    data = data.update({'x': data.x * data.y}, {'y': 0}, where=my_var == 3)

    assert issubclass(data.x.dtype, int)

    df = otp.run(data)
    assert df.x[0] == int(33 * 0.33) and df.y[0] == 0.33
    assert df.x[1] == int(34 * 0.341) and df.y[1] == 0.341
    assert df.x[2] == int(35 * 0.35) and df.y[2] == 0.35


def test_update_field_12(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    # it is allowed to pass int value there
    data = data.update({'x': data.x * data.y}, {'y': 0}, where=99)

    assert issubclass(data.x.dtype, int)

    df = otp.run(data)
    assert df.x[0] == int(33 * 0.33) and df.y[0] == 0.33
    assert df.x[1] == int(34 * 0.341) and df.y[1] == 0.341
    assert df.x[2] == int(35 * 0.35) and df.y[2] == 0.35


def test_update_field_13(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    # it is not supported to pass string as a condition
    with pytest.raises(ValueError):
        data.update({'x': data.x * data.y}, {'y': 0}, where='abc')

    data = data.update({'x': data.x * data.y}, {'y': 0}, where='abc' == 'efg')

    assert issubclass(data.x.dtype, int)

    df = otp.run(data)
    assert df.x[0] == 33 and df.y[0] == 0
    assert df.x[1] == 34 and df.y[1] == 0
    assert df.x[2] == 35 and df.y[2] == 0


def test_update_field_14(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    # functions are not supported
    with pytest.raises(TypeError):
        data.update({'x': lambda x: x ** 2}, {'y': 0}, where='abc' == 'efg')

    df = otp.run(data)
    assert df.x[0] == 33 and df.y[0] == 0.33
    assert df.x[1] == 34 and df.y[1] == 0.341
    assert df.x[2] == 35 and df.y[2] == 0.35


def test_update_field_15(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    # functions are not supported
    with pytest.raises(TypeError):
        data.update({'x': 123}, {data.y: lambda: 0}, where='abc' == 'efg')

    df = otp.run(data)
    assert df.x[0] == 33 and df.y[0] == 0.33
    assert df.x[1] == 34 and df.y[1] == 0.341
    assert df.x[2] == 35 and df.y[2] == 0.35


def test_update_field_16(session):
    data = otp.merge([otp.Tick(x=33, y=0.33), otp.Tick(x=34, y=0.341, offset=1), otp.Tick(x=35, y=0.35, offset=2)])

    data = data.update({'x': True, data.y: data.x == 33}, {data.y: data.x == data.y}, where=data.x == data.y * 100)

    df = otp.run(data)
    assert df.x[0] == 1 and df.y[0] == 1
    assert df.x[1] == 34 and df.y[1] == 0
    assert df.x[2] == 1 and df.y[2] == 0


def test_update_string(session):

    data = otp.Ticks(X=['A', 'B'], Y=[0, 1])

    data = data.update({data['X']: 'NONE'},
                       {data['X']: 's'},
                       where=data['Y'] == 0)

    res = otp.run(data)

    assert all(res['X'] == ['NONE', 's'])


def test_bool_type_converts_into_float_2(session):
    t = otp.Tick(x=3, y=-0.4, z=0.2)

    assert t.x.dtype is int
    assert t.y.dtype is float
    assert t.z.dtype is float

    df = otp.run(t)
    assert isinstance(df.x[0], np.integer) and df.x[0] == 3
    assert isinstance(df.y[0], np.float64) and df.y[0] == -0.4
    assert isinstance(df.z[0], np.float64) and df.z[0] == 0.2

    t = t.update({t.x: t.y > 0, t.z: t.y < 0})

    assert t.x.dtype is int
    assert t.y.dtype is float
    assert t.z.dtype is float

    df = otp.run(t)
    assert isinstance(df.x[0], np.integer) and df.x[0] == 0
    assert isinstance(df.y[0], np.float64) and df.y[0] == -0.4
    assert isinstance(df.z[0], np.float64) and df.z[0] == 1.0


def test_time_alias_5(session):
    t = otp.Tick(x=3, end=otp.config['default_start_time'] + otp.Milli(1000))

    # it is not allowed to change Time by design here
    with pytest.raises(ValueError):
        t.update({t.Time: t.Time + 500}, where=True)


def test_time_alias_6(session):
    t = otp.Tick(x=3, end=otp.config['default_start_time'] + otp.Milli(1000))

    # is is not allowed to change Time by design here
    with pytest.raises(ValueError):
        t.update({t["Time"]: t["Time"] + 500}, where=True)


def test_time_alias_7(session):
    t = otp.Tick(x=3, end=otp.config['default_start_time'] + otp.Milli(1000))

    # it is not allowed to change Time by desgin here
    with pytest.raises(ValueError):
        t.update({t.TIMESTAMP: t.TIMESTAMP + 500}, where=True)


class TestColumns:
    def test_same_type(self, session):
        t = otp.Ticks({'A': [1, 2]})
        t = t.update({'A': 22}, where=(t['A'] == 2))
        assert t.schema['A'] is int
        df = otp.run(t)
        assert list(df['A']) == [1, 22]

    def test_incompatible_types(self, session):
        t = otp.Ticks({'A': [1, 2]})
        with pytest.raises(TypeError):
            t.update({'A': 'b'}, where=(t['A'] == 2))


class TestStateVariables:
    def test_same_type(self, session):
        t = otp.Ticks({'A': [1, 2]})
        t.state_vars['X'] = 11
        t = t.update({t.state_vars['X']: 22}, where=(t['A'] == 2))
        t['X'] = t.state_vars['X']
        assert t.schema['X'] is int
        df = otp.run(t)
        assert list(df['X']) == [11, 22]

    def test_incompatible_types(self, session):
        t = otp.Ticks({'A': [1, 2]})
        t.state_vars['X'] = 11
        with pytest.raises(TypeError):
            t.update({t.state_vars['X']: 'b'}, where=(t['A'] == 2))

    def test_update_error(self, session):
        data = otp.Ticks(dict(X=[-15, 1, 2, -3, 0, 5, 10]))
        data.state_vars['M'] = 0
        data = data.update({data.state_vars['M']: data['X']}, where=(data['X'] > data.state_vars['M']))
        data['M'] = data.state_vars['M']
        assert data.schema['M'] is int
        df = otp.run(data)
        assert list(df['M']) == [0, 1, 2, 2, 2, 5, 10]

    def test_strings(self, session):
        t = otp.Tick(A='a' * 40)
        t.state_vars['S32'] = otp.string[32]()
        t.state_vars['S80'] = otp.string[80]()
        t = t.update({
            t.state_vars['S32']: t['A'],
            t.state_vars['S80']: t['A'],
        })
        t['S32'] = t.state_vars['S32']
        t['S80'] = t.state_vars['S80']
        assert t.schema['S32'] is otp.string[32]
        assert t.schema['S80'] is otp.string[80]
        df = otp.run(t)
        assert df['S32'][0] == 'a' * 32
        assert df['S80'][0] == 'a' * 40

    def test_datetime(self, session):
        t = otp.Tick(A=1)
        t.state_vars['DT'] = otp.datetime(2022, 1, 1)
        t = t.update({t.state_vars['DT']: otp.datetime(2023, 1, 1)})
        t['DT'] = t.state_vars['DT']
        assert t.schema['DT'] is otp.nsectime
        df = otp.run(t)
        assert df['DT'][0] == otp.datetime(2023, 1, 1)
