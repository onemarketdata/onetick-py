import pytest
import onetick.py as otp
from onetick.py.otq import otq


def test_join_1(session):
    t1, t2 = otp.Tick(x=3, offset=1), otp.Tick(y=4, x=3, offset=100)
    data = otp.join(t1, t2, t1.x == t2.x)
    df = otp.run(data)

    assert len(df) == 1
    assert df.x[0] == 3
    assert df["RIGHT_x"][0] == 3
    assert df.y[0] == 4


def test_join_2(session):
    t1, t2 = otp.Tick(y=-1, x=3, offset=1), otp.Tick(y=4, x=3, offset=100)
    data = otp.join(t1, t2, t1.y > t2.y, how="inner")
    df = otp.run(data)
    assert len(df) == 0


def test_join_3(session):
    t1, t2 = otp.Tick(y=-1, x=3, offset=1), otp.Tick(y=4, x=3, offset=100)
    data = otp.join(t1, t2, t1.y > t2.y)
    df = otp.run(data)
    assert len(df) == 1
    assert df.x[0] == 3
    assert df.y[0] == -1


def test_join_4(session):
    t1, t2 = otp.Tick(y=-1, x=3, offset=1), otp.Tick(y=4, x=3, offset=100)
    data = otp.join(t1, t2, (t1.y + t2.y == t1.x) & (t2.x == 3))
    df = otp.run(data)
    assert len(df) == 1
    assert df["RIGHT_x"][0] == 3
    assert df.y[0] == -1


def test_join_5(session):
    t1, t2 = otp.Tick(x=3), otp.Tick(x=5)
    data = otp.join(t1, t2, t1.x == t2.x)

    df = otp.run(data)
    assert len(df) == 1
    assert df["RIGHT_x"][0] == 0


def test_join_6(session):
    t1, t2 = otp.Tick(x=3, offset=2), otp.Tick(y=4, offset=1)
    data = otp.join(t1, t2, t1.x == t2.y)
    df = otp.run(data)

    assert len(df) == 1
    assert df.x[0] == 3
    assert hasattr(data, "x") and data.x.dtype is int
    assert hasattr(data, "y")
    assert df.y[0] == 0


def test_join_7(session):
    t1, t2 = otp.Tick(x=3, offset=2), otp.Tick(y=4, offset=1)
    data = otp.join(t1, t2, t1.x == t2.y, how="inner")
    df = otp.run(data)
    assert len(df) == 0


def test_join_8(session):
    t1, t2 = otp.Tick(x=3, y=5, z=6.2, offset=2), otp.Tick(y=3, string_field_name="text", z=21, offset=1221)
    data = otp.join(t1, t2, t1.x == t2.y)
    df = otp.run(data)

    assert hasattr(data, "x") and data.x.dtype is int
    assert df.x[0] == 3
    assert hasattr(data, "y") and data.y.dtype is int
    assert df.y[0] == 5
    assert hasattr(data, "z") and data.z.dtype is float
    assert df.z[0] == pytest.approx(6.2)
    assert hasattr(data, "RIGHT_z") and data.RIGHT_z.dtype is int
    assert df.RIGHT_z[0] == 21
    assert hasattr(data, "RIGHT_y") and data.RIGHT_y.dtype is int
    assert df.RIGHT_y[0] == 3
    assert hasattr(data, "string_field_name") and data.string_field_name.dtype is str
    assert df.string_field_name[0] == "text"


def test_join_9(session):
    """check that object is not changed"""
    t1, t2 = otp.Tick(x=3, y=5, z=6, offset=2), otp.Tick(y=3, field_name="text", z=21, offset=1221)

    t2.node_name("BLA-BLA")
    t1.node_name("TEST")
    old_hash_t1 = t1.__hash__()
    old_hash_t2 = t2.__hash__()
    data = otp.join(t1, t2, t1.x == t2.y)
    df = otp.run(data)
    assert len(df) == 1
    assert hasattr(data, "x") and data.x.dtype is int
    assert df.x[0] == 3
    assert hasattr(data, "y") and data.y.dtype is int
    assert df.y[0] == 5
    assert hasattr(data, "RIGHT_y") and data.RIGHT_y.dtype is int
    assert df.RIGHT_y[0] == 3

    assert t1.node_name() == "TEST"
    assert t2.node_name() == "BLA-BLA"
    assert old_hash_t1 == t1.__hash__()
    assert old_hash_t2 == t2.__hash__()


def test_join_10(session):
    """check that object is not changed"""
    t1, t2 = otp.Tick(x=3, y=5, z=6, offset=2), otp.Tick(y=3, field_name="text", z=21, offset=1221)
    old_hash_t1 = t1.__hash__()
    old_hash_t2 = t2.__hash__()
    data = otp.join(t1, t2, t1.x == t2.y, rprefix="MY_PREFIX")
    df = otp.run(data)

    assert len(df) == 1
    assert hasattr(data, "x") and data.x.dtype is int
    assert df.x[0] == 3
    assert hasattr(data, "y") and data.y.dtype is int
    assert df.y[0] == 5
    assert hasattr(data, "MY_PREFIX_y") and data.MY_PREFIX_y.dtype is int
    assert df.MY_PREFIX_y[0] == 3
    assert old_hash_t1 == t1.__hash__()
    assert old_hash_t2 == t2.__hash__()


def test_wrong_how(session):
    t1, t2 = otp.Tick(x=3, offset=1), otp.Tick(y=4, x=3, offset=100)

    with pytest.raises(ValueError):
        otp.join(t1, t2, t1.x == t2.x, how="blabla")

    with pytest.warns(FutureWarning, match="Value 'outer' for parameter 'how' is deprecated"):
        otp.join(t1, t2, t1.x == t2.x, how="outer")


def test_stack_of_joins_with_rpreffix(session):
    data1 = otp.Ticks(dict(x=[3, 2, 1, 0], y=[1] * 4))
    data2 = otp.Ticks(dict(x=[1, 2, 3, 4], y=[2] * 4))
    result = otp.join(data1, data2, data1["x"] == data2["x"], rprefix="A")
    result = otp.join(result, data2, result["x"] == data2["x"], rprefix="B")
    result = otp.join(result, data2, result["x"] == data2["x"], rprefix="C")
    df = otp.run(result)

    for column in ("x", "A_x", "B_x", "C_x"):
        assert all(df[column] == [3, 2, 1, 0])
    assert all(df["y"] == [1, 1, 1, 1])
    for column in ("A_y", "B_y", "C_y"):
        assert all(df[column] == [2, 2, 2, 0])


def test_same_data_as_left_and_right_param_in_join(session):
    data1 = otp.Ticks(dict(x=[3, 2, 1, 0], y=[1] * 4))
    data2 = otp.Ticks(dict(x=[1, 2, 3, 4], y=[2] * 4))
    data3 = otp.Ticks(dict(x=[2, 4, 6], y=[3] * 3))

    result = otp.join(data1, data2, data1["x"] == data2["x"], rprefix="A")
    df = otp.run(result)
    for column in ("x", "A_x"):
        assert all(df[column] == [3, 2, 1, 0])
    assert all(df["y"] == 1)
    assert all(df["A_y"] == [2, 2, 2, 0])

    result = otp.join(data2, data3, data2["x"] == data3["x"], rprefix="B")
    df = otp.run(result)
    assert all(df["x"] == [1, 2, 3, 4])
    assert all(df["y"] == 2)
    assert all(df["B_x"] == [0, 2, 0, 4])
    assert all(df["B_y"] == [0, 3, 0, 3])

    result = otp.join(data1, result, data1["x"] == result["x"], rprefix="C")
    df = otp.run(result)
    assert all(df["x"] == [3, 2, 1, 0])
    assert all(df["y"] == 1)
    assert all(df["B_x"] == [0, 2, 0, 0])
    assert all(df["B_y"] == [0, 3, 0, 0])
    assert all(df["C_x"] == [3, 2, 1, 0])
    assert all(df["C_y"] == [2, 2, 2, 0])


def test_join_on_all(session):
    data1 = otp.Ticks(dict(x=[1, 2, 3, 4]))
    data2 = otp.Ticks(dict(x=[1, 2, 3, 4]))

    result = otp.join(data1, data2, on="all")
    df = otp.run(result)
    assert len(df) == 16


def test_join_on_all_2(session):
    data1 = otp.Ticks(dict(x=[1, 2, 3, 4]))
    data2 = otp.Ticks(dict(x=[1, 2, 3, 4]))
    data2, _ = data2[data2['x'] == -1]  # expect no ticks propagated

    result = otp.join(data1, data2, on="all", how='inner')
    df = otp.run(result)
    assert len(df) == 0


def test_join_same_size_simple(session):
    left = otp.Ticks(dict(x=[1, 2, 3]))
    right = otp.Ticks(dict(x=[4, 5, 6], y=[7, 8, 9]))
    result = otp.join(left, right, on="same_size")
    df = otp.run(result)
    assert len(df) == 3
    assert all(df["x"] == [1, 2, 3])
    assert all(df["y"] == [7, 8, 9])
    assert all(df["RIGHT_x"] == [4, 5, 6])


def test_join_same_size_with_rprefix(session):
    left = otp.Ticks(dict(x=[1, 2, 3]))
    right = otp.Ticks(dict(x=[4, 5, 6]))
    result = otp.join(left, right, on="same_size", rprefix="A")
    df = otp.run(result)
    assert len(df) == 3
    assert all(df["x"] == [1, 2, 3])
    assert all(df["A_x"] == [4, 5, 6])


def test_join_same_size_exception(session):
    with pytest.raises(Exception):
        left = otp.Ticks(dict(x=[1, 2, 3]))
        right = otp.Ticks(dict(x=[4, 5, 6, 7]))
        data = otp.join(left, right, on="same_size")
        otp.run(data)


@pytest.mark.parametrize('on', ['all', 'same_size', "x"])
def test_join_propagate_fields_not_in_schema(session, on):
    left = otp.Ticks(dict(x=[1, 2, 3]))
    right = otp.Ticks(dict(x=[1, 2, 3]))
    left.sink(otq.AddField(field="A", value='"1"'))
    right.sink(otq.AddField(field="B", value='"2"'))
    if on == 'x':
        on = left['x'] == right['x']
    data = otp.join(left, right, on=on, rprefix="A", keep_fields_not_in_schema=True)
    result = otp.run(data)

    assert set(result.columns) == {'Time', 'x', 'A_x', 'A', 'B'}

    assert result['A'][0] == '1'
    assert result['B'][0] == '2'


def test_output_type(session):
    ticks = otp.Ticks({'A': [1, 2, 3]})
    tick = otp.Tick(B=1)

    result = otp.join(ticks, tick, on='all')
    assert result.__class__ == otp.Source
    tt1 = otp.run(result)

    result = otp.join(ticks, tick, on='all', output_type_index=0)
    assert result.__class__ == ticks.__class__
    tt2 = otp.run(result)
    assert tt2.equals(tt1)

    some_db_1 = otp.DB('SOME_DB_1')
    some_db_1.add(ticks)
    session.use(some_db_1)
    some_db_2 = otp.DB('SOME_DB_2')
    some_db_2.add(ticks)
    session.use(some_db_2)

    custom_1 = otp.DataSource(some_db_1, symbols=otp.config['default_symbol'])
    custom_2 = otp.DataSource(some_db_2, symbols=otp.config['default_symbol'])

    result = otp.join(custom_1, tick, on='all')
    assert result.__class__ == otp.Source
    ct1 = otp.run(result)

    result = otp.join(custom_1, tick, on='all', output_type_index=0)
    assert result.__class__ == otp.DataSource
    assert result.db == custom_1.db
    ct2 = otp.run(result)
    assert ct2.equals(ct1)

    result = otp.join(custom_1, custom_2, on='same_size')
    assert result.__class__ == otp.Source
    cc1 = otp.run(result)

    result = otp.join(custom_1, custom_2, on='same_size', output_type_index=1)
    assert result.__class__ == otp.DataSource
    assert result.db == custom_2.db
    cc2 = otp.run(result)
    assert cc2.equals(cc1)


def test_strange_string_behaviour(session):
    data = otp.join(otp.Tick(A=1), otp.Tick(B=2, F='HELLO'), on='all')
    df = otp.run(data)
    assert df['F'][0] == 'HELLO'
    data = otp.join(otp.Tick(A=1), otp.Tick(B=2, S='HELLO'), on='all')
    df = otp.run(data)
    assert df['S'][0] == 'HELLO'


def test_join_list_one(session):
    left = otp.Ticks(A=[1, 2, 3], B=[4, 6, 7])
    right = otp.Ticks(A=[2, 3, 4], B=[6, 7, 8])
    data = otp.join(left, right, ['A'], how='inner')
    assert 'RIGHT_A' not in data.schema
    df = otp.run(data)
    assert len(df) == 2
    assert all(df['A'] == [2, 3])
    assert all(df['B'] == [6, 7])
    assert all(df['RIGHT_B'] == [6, 7])


def test_join_list_two(session):
    left = otp.Ticks(A=[1, 2, 3], B=[4, 6, 7])
    right = otp.Ticks(A=[2, 3, 4], B=[6, 9, 8], C=[7, 2, 0])
    data = otp.join(left, right, ['A', 'B'], how='inner')
    assert 'RIGHT_A' not in data.schema
    assert 'RIGHT_B' not in data.schema
    df = otp.run(data)
    assert len(df) == 1
    assert all(df['A'] == [2])
    assert all(df['B'] == [6])
    assert all(df['C'] == [7])


def test_join_list_not_existing_filed(session):
    left = otp.Ticks(A=[1, 2, 3], B=[4, 6, 7])
    right = otp.Ticks(A=[2, 3, 4], B=[6, 9, 8], C=[7, 2, 0])
    with pytest.raises(ValueError):
        otp.join(left, right, ['C'], how='inner')


def test_join_empty_on_list(session):
    left = otp.Ticks(A=[1, 2, 3], B=[4, 6, 7])
    right = otp.Ticks(A=[2, 3, 4], B=[6, 9, 8], C=[7, 2, 0])
    with pytest.raises(ValueError):
        otp.join(left, right, [])


def test_join_on_operation(session):
    left = otp.Ticks(A=[1, 2, 3, 4])
    right = otp.Ticks(B=[2, 4, 6])
    data = otp.join(left, right, on=left['A'] == right['B'])
    df = otp.run(data)
    assert list(df['A']) == [1, 2, 3, 4]
    assert list(df['B']) == [0, 2, 0, 4]

    data = otp.join(left, right, on=left['A'].apply(str).str.match(right['B'].apply(str)))
    df = otp.run(data)
    assert list(df['A']) == [1, 2, 3, 4]
    assert list(df['B']) == [0, 2, 0, 4]
