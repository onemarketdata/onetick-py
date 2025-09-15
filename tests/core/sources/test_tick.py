import pytest
import numpy as np
import datetime
import pytz
from onetick.py.core.source import _Source

import onetick.py as otp

Tick = otp.Tick
merge = otp.merge


def tick_and_copies(t: Tick):
    return [t, t.copy(), t.deepcopy()]


@pytest.mark.parametrize("t", tick_and_copies(Tick(A=1)))
def test_copy(session, t):
    assert isinstance(t, otp.Tick)


def test_tick_0(session):
    # it is not allowed to create a tick without fields
    with pytest.raises(ValueError):
        Tick()


def test_tick_1(session):
    t = Tick(x=3, y=0.35, account="my account", offset=1000)

    assert t.x.dtype is int
    assert t.y.dtype is float
    assert t.account.dtype is str

    assert otp.run(t).x[0] == 3
    assert otp.run(t).y[0] == 0.35
    assert otp.run(t).account[0] == "my account"

    t.Z = 333

    assert otp.run(t).Z[0] == 333


def test_tick_2(session):
    # Tick constructor expects values but not types
    with pytest.raises(TypeError):
        Tick(x=int)


def test_tick_3(session):
    t = Tick(x=3)

    t.is_start = t.TIMESTAMP == t._START_TIME
    t.is_end = t.TIMESTAMP == t._END_TIME

    assert issubclass(t.is_start.dtype, float)
    assert issubclass(t.is_end.dtype, float)

    assert len(otp.run(t)) == 1
    assert isinstance(otp.run(t).is_start[0], np.float64)
    assert isinstance(otp.run(t).is_end[0], np.float64)
    assert otp.run(t).is_start[0] == 1
    assert otp.run(t).is_end[0] == 0

    t.is_start = t.TIMESTAMP != t._START_TIME


def test_tick_4(session):
    t = Tick(x=3)

    t.t_diff = t.TIMESTAMP - t._END_TIME

    assert issubclass(t.t_diff.dtype, int)

    assert len(otp.run(t)) == 1
    assert isinstance(otp.run(t).t_diff[0], np.int64)


class TestDrop:
    def test_tick_del_1(self, session):
        # remove a column
        t = Tick(x=3, y=0.78)

        assert hasattr(t, "x")
        assert hasattr(t, "y")
        assert hasattr(otp.run(t), "x")
        assert hasattr(otp.run(t), "y")

        t_c = t.copy()
        del t["x"]
        assert not hasattr(t, "x")
        assert not hasattr(otp.run(t), "x")
        assert hasattr(otp.run(t), "y")

        assert hasattr(otp.run(t_c), "x")
        assert hasattr(otp.run(t_c), "y")

    def test_tick_del_2(self, session):
        # remove several columns
        t = Tick(x=3, y=0.78, z="abc")

        assert hasattr(t, "x")
        assert hasattr(t, "y")
        assert hasattr(t, "z")
        assert hasattr(otp.run(t), "x")
        assert hasattr(otp.run(t), "y")
        assert hasattr(otp.run(t), "z")

        t_c = t.copy()
        del t[["x", "z"]]
        assert not hasattr(t, "x")
        assert not hasattr(t, "z")
        assert not hasattr(otp.run(t), "x")
        assert not hasattr(otp.run(t), "z")
        assert hasattr(otp.run(t), "y")

        assert hasattr(otp.run(t_c), "x")
        assert hasattr(otp.run(t_c), "y")
        assert hasattr(otp.run(t_c), "z")

    def test_tick_del_3(self, session):
        t = Tick(x=99, y=0.35)

        # there is no such column
        with pytest.raises(AttributeError):
            del t["abc"]

        # there is still no 'abc' column
        with pytest.raises(AttributeError):
            del t[["y", "abc"]]

        # 'node_name' is a method, but there is no such column
        with pytest.raises(AttributeError):
            del t["node_name"]

        assert hasattr(t, "y")
        assert hasattr(t, "x")

    def test_tick_del_4(self, session):
        t = Tick(x=99, y=0.35, z="abc", w=7)

        assert hasattr(t, "x")
        assert hasattr(otp.run(t), "x")

        del t[t.x]

        assert not hasattr(t, "x")
        assert not hasattr(otp.run(t), "x")

        # --

        assert hasattr(t, "y")
        assert hasattr(t, "z")
        assert hasattr(otp.run(t), "y")
        assert hasattr(otp.run(t), "z")

        del t[[t.y, t.z]]

        assert not hasattr(t, "y")
        assert not hasattr(t, "z")
        assert not hasattr(otp.run(t), "y")
        assert not hasattr(otp.run(t), "z")

        # that is not allowed to delete by index or something that is not str or _Column
        with pytest.raises(Exception):
            del t[1]

    def test_tick_del_5(self, session):
        t = Tick(x=99, y=0.35, z="abc", w=7)

        assert hasattr(t, "y")
        assert hasattr(t, "w")
        assert hasattr(otp.run(t), "y")
        assert hasattr(otp.run(t), "w")

        del t[t.y, t.w]

        assert not hasattr(t, "y")
        assert not hasattr(t, "w")

    def test_tick_drop_1(self, session):
        # remove several columns
        t = Tick(x=3, y=0.78, z="abc")

        assert hasattr(t, "x")
        assert hasattr(t, "y")
        assert hasattr(t, "z")
        assert hasattr(otp.run(t), "x")
        assert hasattr(otp.run(t), "y")
        assert hasattr(otp.run(t), "z")

        t_c = t.copy()
        t = t.drop(["x", "z"])
        assert not hasattr(t, "x")
        assert not hasattr(t, "z")
        assert not hasattr(otp.run(t), "x")
        assert not hasattr(otp.run(t), "z")
        assert hasattr(otp.run(t), "y")

        assert hasattr(otp.run(t_c), "x")
        assert hasattr(otp.run(t_c), "y")
        assert hasattr(otp.run(t_c), "z")

    def test_tick_drop_2(self, session):
        t = Tick(x=99, y=0.35, z="abc", w=7)

        assert hasattr(t, "x")
        assert hasattr(otp.run(t), "x")

        t = t.drop([t.x])

        assert not hasattr(t, "x")
        assert not hasattr(otp.run(t), "x")

        # --

        assert hasattr(t, "y")
        assert hasattr(t, "z")
        assert hasattr(otp.run(t), "y")
        assert hasattr(otp.run(t), "z")

        t = t.drop([t.y, t.z])

        assert not hasattr(t, "y")
        assert not hasattr(t, "z")
        assert not hasattr(otp.run(t), "y")
        assert not hasattr(otp.run(t), "z")

        # that is not allowed to delete by index or something that is not str or _Column
        with pytest.raises(Exception):
            t.drop([1])

    def test_tick_drop_3(self, session):
        t = Tick(x=99, y=0.35, z="abc", w=7)

        assert hasattr(t, "y")
        assert hasattr(t, "w")
        assert hasattr(otp.run(t), "y")
        assert hasattr(otp.run(t), "w")

        t = t.drop([t.y, t.w])

        assert not hasattr(t, "y")
        assert not hasattr(t, "w")

    def test_unmatched_regex_does_drop_columns(self, session):
        t = Tick(x=99, y=0.35, z="abc", w=7)
        t = t.drop(r"a+")
        otp.run(t)
        assert hasattr(t, "x")
        assert hasattr(t, "y")
        assert hasattr(t, "z")
        assert hasattr(t, "w")

    def test_regex_match_non_column_fields(self, session):
        # methods shouldn't be dropped
        t = Tick(a=99, a1=0.35, a2="abc", aa=7)
        t = t.drop(r"(a.)")
        otp.run(t)
        assert hasattr(t, "a")
        assert hasattr(t, "apply")
        assert hasattr(t, "append")
        assert not hasattr(t, "a1")
        assert not hasattr(t, "a2")
        assert not hasattr(t, "aa")
        assert len(t.columns(skip_meta_fields=True)) == 1

    def test_regex_and_usual_drop(self, session):
        t = Tick(a=99, a1=0.35, a2="abc", b=7)
        t = t.drop([r"a.*", "a"])
        otp.run(t)
        assert hasattr(t, "b")
        assert not hasattr(t, "a")
        assert not hasattr(t, "a1")
        assert not hasattr(t, "a2")
        assert len(t.columns(skip_meta_fields=True)) == 1

    def test_regex_match_special_columns(self, session):
        # META COLUMNS shouldn't be dropped
        t = Tick(a=99)
        t = t.drop(r"_.*")
        otp.run(t)
        assert hasattr(t, "_START_TIME")
        assert hasattr(t, "_END_TIME")
        assert hasattr(t, "__str__")
        assert hasattr(t, "a")
        assert len(t.columns(skip_meta_fields=False)) == len(_Source.meta_fields)

    def test_mixed_regex_and_columns(self, session):
        t = otp.Tick(**{'A': 1, 'AA': 11, 'BB': 22, 'CC': 33, 'D': 4, 'DD': 44, 'X': 10})
        with pytest.raises(AttributeError, match="There is no 'C' column"):
            t = t.drop(['A', '^B', 'C', t['D']])
        t = t.drop(['A', '^B', 'C$', t['D']])
        assert t.schema == {'AA': int, 'DD': int, 'X': int}
        df = otp.run(t)
        assert list(df) == ['Time', 'AA', 'DD', 'X']
        assert df['AA'][0] == 11
        assert df['DD'][0] == 44
        assert df['X'][0] == 10


def test_tick_string_1(session):
    t = Tick(account="x" * 1024)

    assert t.account.dtype is otp.string[1024]
    assert otp.run(t).account[0] == "x" * 1024


def test_tick_string_2(session):
    t = Tick(account=otp.string[1024](""))

    assert t.account.dtype is otp.string[1024]
    assert otp.run(t).account[0] == ""


def test_tick_string_3(session):
    t = Tick(x=0)

    t.account = "x" * 1024
    assert t.account.dtype is otp.string[1024]
    assert otp.run(t).account[0] == "x" * 1024


def test_tick_string_4(session):
    t = Tick(x=0)

    t.account = ("", otp.string[1024])
    assert t.account.dtype is otp.string[1024]
    assert otp.run(t).account[0] == ""


def test_tick_copy(session):
    t1 = Tick(x=3)

    assert otp.run(t1).x[0] == 3

    t2 = t1.copy()

    assert otp.run(t2).x[0] == 3

    t2.x = 7
    t2.Z = 44

    assert otp.run(t1).x[0] == 3
    assert otp.run(t2).x[0] == 7
    assert otp.run(t2).Z[0] == 44


def test_inner_merge_ticks(session):
    # sum of two
    t1 = Tick(x=3, y=0.35)
    t2 = Tick(x=4, y=0.77, offset=1)
    data = t1 + t2

    assert len(otp.run(data)) == 2
    assert otp.run(data).x[0] == 3
    assert otp.run(data).y[0] == 0.35
    assert otp.run(data).x[1] == 4
    assert otp.run(data).y[1] == 0.77

    assert len(otp.run(t1)) == 1
    assert otp.run(t1).x[0] == 3
    assert otp.run(t1).y[0] == 0.35

    assert len(otp.run(t2)) == 1
    assert otp.run(t2).x[0] == 4
    assert otp.run(t2).y[0] == 0.77

    # add one more
    t3 = Tick(x=5, y=0.9, offset=2)
    data2 = data + t3

    assert len(otp.run(data2)) == 3
    assert otp.run(data2).x[0] == 3
    assert otp.run(data2).x[1] == 4
    assert otp.run(data2).x[2] == 5

    assert len(otp.run(t3)) == 1
    assert otp.run(t3).x[0] == 5
    assert otp.run(t3).y[0] == 0.9

    assert len(otp.run(data)) == 2
    assert otp.run(data).x[0] == 3
    assert otp.run(data).y[0] == 0.35
    assert otp.run(data).x[1] == 4
    assert otp.run(data).y[1] == 0.77

    assert len(otp.run(t1)) == 1
    assert otp.run(t1).x[0] == 3
    assert otp.run(t1).y[0] == 0.35

    assert len(otp.run(t2)) == 1
    assert otp.run(t2).x[0] == 4
    assert otp.run(t2).y[0] == 0.77

    # change first ticks and make sure that related objects are not changed
    t1.x = 123
    assert len(otp.run(data2)) == 3
    assert otp.run(data2).x[0] == 3
    assert otp.run(data2).x[1] == 4
    assert otp.run(data2).x[2] == 5

    assert len(otp.run(t3)) == 1
    assert otp.run(t3).x[0] == 5
    assert otp.run(t3).y[0] == 0.9

    assert len(otp.run(data)) == 2
    assert otp.run(data).x[0] == 3
    assert otp.run(data).y[0] == 0.35
    assert otp.run(data).x[1] == 4
    assert otp.run(data).y[1] == 0.77

    assert len(otp.run(t1)) == 1
    assert otp.run(t1).x[0] == 123
    assert otp.run(t1).y[0] == 0.35

    assert len(otp.run(t2)) == 1
    assert otp.run(t2).x[0] == 4
    assert otp.run(t2).y[0] == 0.77

    # add field
    t1.z = t1.x * t1.y

    assert otp.run(t1).z[0] == otp.run(t1).x[0] * otp.run(t1).y[0]

    assert len(otp.run(data2)) == 3
    assert otp.run(data2).x[0] == 3
    assert otp.run(data2).x[1] == 4
    assert otp.run(data2).x[2] == 5

    assert len(otp.run(t3)) == 1
    assert otp.run(t3).x[0] == 5
    assert otp.run(t3).y[0] == 0.9

    assert len(otp.run(data)) == 2
    assert otp.run(data).x[0] == 3
    assert otp.run(data).y[0] == 0.35
    assert otp.run(data).x[1] == 4
    assert otp.run(data).y[1] == 0.77

    assert len(otp.run(t1)) == 1
    assert otp.run(t1).x[0] == 123
    assert otp.run(t1).y[0] == 0.35

    assert len(otp.run(t2)) == 1
    assert otp.run(t2).x[0] == 4
    assert otp.run(t2).y[0] == 0.77


def test_inner_merge_different_schema(session):
    # add 'z' into the first tick
    data = Tick(x=33, y=0.5) + Tick(x=0, y=0.1, z=4, offset=1)
    data.z = 7

    df = otp.run(data)
    assert len(df) == 2
    assert df.z[0] == 7
    assert df.z[1] == 7


def test_inner_merge_int_to_float(session):
    t1 = Tick(x=33)
    t2 = Tick(x=7.3, offset=1)
    data = t1 + t2

    assert type(otp.run(t1).x[0]) is not np.float64
    assert type(otp.run(t2).x[0]) is np.float64

    df = otp.run(data)
    assert type(df.x[0]) is np.float64
    assert type(df.x[1]) is np.float64

    assert len(df) == 2


def test_inner_merge_different_types(session):
    # int and str
    with pytest.raises(ValueError, match="different types"):
        _ = Tick(x=3) + Tick(x="3")

    # float and str
    with pytest.raises(ValueError, match="different types"):
        _ = Tick(x=4.5) + Tick(x="")

    with pytest.raises(ValueError, match="different types"):
        _ = Tick(x=otp.msectime(1000)) + Tick(x="abc")


def test_where_1(session):
    # simple where-clause test
    data = Tick(x=1) + Tick(x=2, offset=1) + Tick(x=3, offset=2) + Tick(x=4, offset=3) + Tick(x=5, offset=4)

    assert len(otp.run(data)) == 5

    left, right = data[data.x >= 3]

    df_l = otp.run(left)
    assert len(df_l) == 3
    assert df_l.x[0] == 3
    assert df_l.x[1] == 4
    assert df_l.x[2] == 5

    df_r = otp.run(right)
    assert len(df_r) == 2
    assert df_r.x[0] == 1
    assert df_r.x[1] == 2

    assert len(otp.run(data)) == 5


def test_where_2(session):
    # check that changing parent object does not affect children
    data = Tick(x=99) + Tick(x=100, offset=1) + Tick(x=101, offset=2)

    left, right = data[data.x < 100]

    assert len(otp.run(left)) == 1
    assert len(otp.run(right)) == 2

    data += Tick(x=102, offset=3) + Tick(x=103, offset=4)

    assert len(otp.run(left)) == 1
    assert len(otp.run(right)) == 2

    left2, right2 = data[data.x < 102]

    assert len(otp.run(data)) == 5

    assert len(otp.run(left)) == 1
    assert otp.run(left).x[0] == 99

    assert len(otp.run(right)) == 2
    assert otp.run(right).x[0] == 100
    assert otp.run(right).x[1] == 101

    assert len(otp.run(left2)) == 3
    assert otp.run(left2).x[0] == 99
    assert otp.run(left2).x[1] == 100
    assert otp.run(left2).x[2] == 101

    assert len(otp.run(right2)) == 2
    assert otp.run(right2).x[0] == 102
    assert otp.run(right2).x[1] == 103


def test_where_inner_merge(session):
    data = Tick(x=99) + Tick(x=100, offset=1) + Tick(x=101, offset=2)
    left, right = data[data.x < 100]
    data2 = left + right

    df = otp.run(data)
    assert len(df) == 3
    assert df.x[0] == 99
    assert df.x[1] == 100
    assert df.x[2] == 101

    assert len(otp.run(left)) == 1
    assert otp.run(left).x[0] == 99

    assert len(otp.run(right)) == 2
    assert otp.run(right).x[0] == 100
    assert otp.run(right).x[1] == 101

    df2 = otp.run(data2)
    assert len(df2) == 3
    assert df2.x[0] == 99
    assert df2.x[1] == 100
    assert df2.x[2] == 101


def test_several_where(session):
    data = Tick(x=99) + Tick(x=100, offset=1) + Tick(x=101, offset=2) + Tick(x=102, offset=3)
    le_100, mt_100 = data[data.x <= 100]
    d_99, d_100 = le_100[le_100.x < 100]
    d_101, d_102 = mt_100[mt_100.x > 101]
    data2 = d_99 + d_100 + d_101 + d_102

    df2 = otp.run(data2)
    assert len(df2) == 4
    assert df2.x[0] == 99
    assert df2.x[1] == 100
    assert df2.x[2] == 101
    assert df2.x[3] == 102


@pytest.mark.parametrize("const", [True, False, 1, 0, 1.5, otp.nan])
def test_wrong_indexes(session, const):
    data = otp.Ticks(dict(X=[1, 2, 3]))
    with pytest.raises(KeyError):
        _ = data[const]


def test_func_merge_ticks_1(session):
    t1, t2 = Tick(x=100), Tick(x=102)

    data = merge([t1, t2])

    df = otp.run(data)
    assert len(df) == 2
    assert df.x[0] == 100
    assert df.x[1] == 102

    data = merge([data, Tick(x=103)])

    df = otp.run(data)
    assert len(df) == 3
    assert df.x[0] == 100
    assert df.x[1] == 102
    assert df.x[2] == 103


def test_func_merge_ticks_2(session):
    data = merge([Tick(x=100), Tick(x=102), Tick(x=103), Tick(x=104)])

    assert len(otp.run(data)) == 4


def test_where_ext_merge(session):
    data = merge([Tick(x=99), Tick(x=100, offset=1), Tick(x=101, offset=2)])
    left, right = data[data.x < 100]
    data2 = merge([left, right])

    df = otp.run(data)
    assert len(df) == 3
    assert df.x[0] == 99
    assert df.x[1] == 100
    assert df.x[2] == 101

    df_l = otp.run(left)
    assert len(df_l) == 1
    assert df_l.x[0] == 99

    df_r = otp.run(right)
    assert len(df_r) == 2
    assert df_r.x[0] == 100
    assert df_r.x[1] == 101

    df2 = otp.run(data2)
    assert len(df2) == 3
    assert df2.x[0] == 99
    assert df2.x[1] == 100
    assert df2.x[2] == 101


def test_several_where_ext_merge(session):
    data = merge([Tick(x=99), Tick(x=100, offset=1), Tick(x=101, offset=2), Tick(x=102, offset=3)])
    le_100, mt_100 = data[data.x <= 100]
    d_99, d_100 = le_100[le_100.x < 100]
    d_101, d_102 = mt_100[mt_100.x > 101]
    data2 = merge([d_99, d_100, d_101, d_102])

    df2 = otp.run(data2)
    assert len(df2) == 4
    assert df2.x[0] == 99
    assert df2.x[1] == 100
    assert df2.x[2] == 101
    assert df2.x[3] == 102


def test_head_1(session):
    data = merge([Tick(x=inx + 100, offset=inx) for inx in range(20)])

    assert len(otp.run(data)) == 20

    assert len(data.head()) == 5

    df = data.head()
    assert df.x[0] == 100
    assert df.x[1] == 101
    assert df.x[2] == 102
    assert df.x[3] == 103
    assert df.x[4] == 104


def test_head_2(session):
    data = merge([Tick(x=inx + 100, offset=inx) for inx in range(20)])

    assert len(otp.run(data)) == 20

    df = data.head(7)
    assert len(df) == 7

    assert df.x[0] == 100
    assert df.x[1] == 101
    assert df.x[2] == 102
    assert df.x[3] == 103
    assert df.x[4] == 104
    assert df.x[5] == 105
    assert df.x[6] == 106


def test_tail_1(session):
    data = merge([Tick(x=inx + 100, offset=inx) for inx in range(20)])

    assert len(otp.run(data)) == 20

    assert len(data.tail()) == 5

    df = data.tail()
    assert df.x[0] == 115
    assert df.x[1] == 116
    assert df.x[2] == 117
    assert df.x[3] == 118
    assert df.x[4] == 119


def test_tail_2(session):
    data = merge([Tick(x=inx + 100, offset=inx) for inx in range(20)])

    assert len(otp.run(data)) == 20

    df = data.tail(7)
    assert len(df) == 7

    assert df.x[0] == 113
    assert df.x[1] == 114
    assert df.x[2] == 115
    assert df.x[3] == 116
    assert df.x[4] == 117
    assert df.x[5] == 118
    assert df.x[6] == 119


def test_bool_type_converts_into_float_1(session):
    t = Tick(x=3, y=-0.4)
    # add
    t.if_cond = t.x * t.y > 0
    t.else_cond = t.x * t.y < 0

    assert t.if_cond.dtype is float
    assert t.else_cond.dtype is float

    df = otp.run(t)
    assert len(df) == 1

    assert isinstance(df.if_cond[0], np.float64) and df.if_cond[0] == 0
    assert isinstance(df.else_cond[0], np.float64) and df.else_cond[0] == 1

    # update
    t.if_cond = t.x * t.y > 0
    t.else_cond = t.x * t.y < 0

    assert t.if_cond.dtype is float
    assert t.else_cond.dtype is float

    df = otp.run(t)
    assert len(df) == 1

    assert isinstance(df.if_cond[0], np.float64) and df.if_cond[0] == 0
    assert isinstance(df.else_cond[0], np.float64) and df.else_cond[0] == 1


def test_force_set_type_when_add_tuple_field_1(session):
    t = Tick(x=3)

    t.y = (0, float)

    assert t.y.dtype is float
    assert isinstance(otp.run(t).y[0], np.float64) and otp.run(t).y[0] == 0

    t.z = 0

    assert t.z.dtype is int
    assert isinstance(otp.run(t).z[0], np.integer) and otp.run(t).z[0] == 0


def test_force_set_type_when_add_tuple_field_2(session):
    t = Tick(x=3)

    t.y = 3.4

    assert t.y.dtype is float
    assert isinstance(otp.run(t).y[0], np.float64) and otp.run(t).y[0] == 3.4

    t.z = (3.4, int)

    assert t.z.dtype is int
    assert isinstance(otp.run(t).z[0], np.integer) and otp.run(t).z[0] == 3


def test_add_string_column_1(session):
    t = Tick(ID="ABC")

    t.MY_ID = t.ID


def test_add_string_column_2(session):
    t = Tick(ID="ABC")

    t.MY_ID = "a x b"


def test_add_bool_column_1(session):
    t = Tick(x=3, end=otp.config['default_start_time'] + otp.Milli(1000))

    t.x = True


def test_add_bool_column_2(session):
    t = Tick(x=3, end=otp.config['default_start_time'] + otp.Milli(1000))

    t.x = False
    # TODO: write more test on bool


def test_date(session):
    date = datetime.datetime(2010, 10, 12, 20, 4, 3)
    tz = pytz.timezone("GMT")
    date = tz.localize(date)

    t = Tick(x=date)

    ts = otp.run(t, timezone="GMT").x[0]

    assert str(ts) == date.strftime("%Y-%m-%d %H:%M:%S")


def test_time_values(session):
    data = Tick(X=1, Y=otp.nsectime(0))

    res = otp.run(data, timezone='GMT')

    assert res['X'][0] == 1
    assert res['Y'][0] == otp.dt(1970, 1, 1)


def test_nsectime(session):
    src = otp.Tick(X=otp.nsectime(1))
    with pytest.warns(match='milliseconds as nanoseconds'):
        df = otp.run(src, timezone='GMT')
    # TODO: change to nanoseconds after PY-441
    assert df['X'][0] == otp.dt(1970, 1, 1, 0, 0, 0, 1000)


def test_num_ticks_per_timestamp(session):
    src = otp.Tick(X=1, num_ticks_per_timestamp=2)
    df = otp.run(src)
    assert df['X'].to_list() == [1, 1]
    assert df['Time'][0] == df['Time'][1]


@pytest.mark.parametrize("offset", [
    otp.Nano(1), otp.Milli(2), otp.Second(3), otp.Minute(4), otp.Hour(5),
    otp.Day(6), otp.Week(7), otp.Month(8), otp.Quarter(2), otp.Year(2),
])
def test_offset_dateparts(session, offset):
    base = otp.datetime(2003, 1, 1)
    end = otp.datetime(2006, 1, 1)
    src = otp.Tick(X=1, offset=offset, start=base, end=end)
    df = otp.run(src)

    datepart = offset.datepart[1:-1]
    diff = df['Time'][0] - base.ts
    if datepart == "week":
        assert diff.days == offset.n * 7
    elif datepart == "month":
        assert diff.days == 243
    elif datepart == "year":
        assert diff.days == 731
    elif datepart == "quarter":
        assert diff.days == 181
    else:
        assert getattr(diff.components, offset.datepart[1:-1] + "s") == offset.n


@pytest.mark.parametrize("offset,expected_type,expected_value", [
    (otp.Milli(2.3), "nanosecond", 2300000), (otp.Second(4.5), "nanosecond", 4500000000),
])
def test_float_offsets(session, offset, expected_type, expected_value):
    assert offset.datepart[1:-1] == expected_type
    assert offset.n == expected_value


def test_offset_dateparts_incorrect(session):
    with pytest.raises(ValueError, match="Negative offset not allowed"):
        otp.Tick(X=1, offset=otp.Nano(-1))

    with pytest.raises(ValueError, match="Unsupported DatePart passed to offset"):
        otp.Tick(X=1, offset=otp.types._construct_dpf(otp.types.offsets.Nano, "Test")(1))


class TestBucketTime:
    @pytest.mark.parametrize("value, expected", [("start", otp.config['default_start_time']),
                                                 ("end", otp.config['default_end_time'])])
    def test_start_end(self, session, value, expected):
        data = otp.Tick(X=1, bucket_time=value)
        df = otp.run(data)
        assert all(df["Time"] == expected)

    @pytest.mark.parametrize("value", ["BUCKET_START", "BUCKET_END"])
    def test_warns(self, session, value):
        with pytest.warns(FutureWarning, match=f"{value} value is deprecated"):
            otp.Tick(X=1, bucket_time=value)

    def test_raises(self, session):
        value = "some_value"
        with pytest.raises(ValueError, match="Only 'start' and 'end' values supported as bucket time"):
            otp.Tick(X=1, bucket_time=value)

    def test_end_with_offset(self, session):
        with pytest.raises(ValueError):
            otp.Tick(X=1, bucket_time="end", offset=1000)


class TestAbsoluteTickTime:

    START_TIME = otp.datetime(2020, 1, 1)
    END_TIME = otp.datetime(2020, 2, 1)

    def test_dt_parts(self, session):
        src = otp.Tick(time=otp.datetime(2020, 1, 2, hour=1, minute=2, second=3,
                                         microsecond=4005, nanosecond=6, tz='GMT'),
                       A=1)
        df = otp.run(src, start=self.START_TIME, end=self.END_TIME, timezone='GMT')
        assert df['Time'][0] == otp.datetime(2020, 1, 2, hour=1, minute=2, second=3,
                                             microsecond=4005, nanosecond=6)

    @pytest.mark.parametrize(
        'set_timezone_in_datetime',
        [True, False]
    )
    @pytest.mark.parametrize(
        'dt_timezone,target_timezone,expected_time_shift',
        [
            ('GMT', 'GMT', otp.Hour(0)),
            ('EST5EDT', 'EST5EDT', otp.Hour(0)),
            ('GMT', 'EST5EDT', otp.Hour(-5)),
            ('EST5EDT', 'GMT', otp.Hour(5)),
            ('America/Chicago', 'Europe/Moscow', otp.Hour(9)),
            ('Europe/Moscow', 'America/Chicago', otp.Hour(-9)),
        ]
    )
    def test_timezones(self, session, set_timezone_in_datetime,
                       dt_timezone, target_timezone, expected_time_shift):
        if set_timezone_in_datetime:
            src = otp.Tick(time=otp.datetime(2020, 1, 2, nanosecond=1, tz=dt_timezone), A=1)
        else:
            src = otp.Tick(time=otp.datetime(2020, 1, 2, nanosecond=1), timezone_for_time=dt_timezone, A=1)
        df = otp.run(src, start=self.START_TIME, end=self.END_TIME, timezone=target_timezone)
        assert df['Time'][0] == otp.datetime(2020, 1, 2, nanosecond=1) + expected_time_shift

    def test_no_tz(self, session):
        # if no TZ, we expect otp to use default tz
        src = otp.Tick(time=otp.datetime(2020, 1, 2, nanosecond=1), A=1)
        df = otp.run(src, start=self.START_TIME, end=self.END_TIME)
        assert df['Time'][0] == otp.datetime(2020, 1, 2, nanosecond=1)

    def test_time_and_offset(self, session):
        with pytest.raises(ValueError):
            otp.Tick(offset=1000, time=otp.datetime(2020, 1, 2), A=1)()

    def test_absolute_time_is_date(self, session):
        src = otp.Tick(time=otp.date(2020, 1, 2), A=1)
        df = otp.run(src, start=self.START_TIME, end=self.END_TIME)
        assert df['Time'][0] == otp.datetime(2020, 1, 2)


def test_ulong(session):
    t = otp.Tick(A=1, B=2**64 - 10)
    df = otp.run(t)
    assert df['A'][0] == 1
    assert df['B'][0] == 2**64 - 10


def test_timedelta_offset(session):
    t = otp.Tick(A=1, offset=otp.timedelta('01:01:01.001001001'))
    df = otp.run(t)
    assert df['Time'][0] == otp.datetime('2003-12-01 01:01:01.001001001')


def test_datetime_offset_bucket_interval(session):
    t = otp.Tick(A=1, bucket_interval=otp.Day(1))
    if not otp.compatibility.is_supported_bucket_units_for_tick_generator():
        with pytest.raises(ValueError,
                           match="Parameter 'bucket_units' in otp.Tick is not supported on this OneTick version"):
            _ = otp.run(t, start=otp.dt(2003, 12, 1), end=otp.dt(2003, 12, 5))
    else:
        df = otp.run(t, start=otp.dt(2003, 12, 1), end=otp.dt(2003, 12, 5))
        assert list(df['Time']) == [otp.dt(2003, 12, i) for i in range(1, 5)], len(df)


def test_empty_time_interval(session):
    t = otp.Tick(A=1, offset=1)
    # start and end time are the same
    df = otp.run(t, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 1))
    assert df['Time'][0] == otp.dt(2022, 1, 1)
