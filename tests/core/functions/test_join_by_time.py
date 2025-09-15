import pytest
import numpy as np

from onetick.py.sources import Tick, Ticks
from onetick.py.functions import join_by_time
import onetick.py.types as ott
import onetick.py as otp
from onetick.py.otq import otq


def test_join_by_time_1(f_session):
    t1, t2 = Tick(x=3, offset=1), Tick(y=4)

    data = join_by_time([t1, t2], how="inner")

    df = otp.run(data)
    assert len(df) == 1
    assert df.x[0] == 3
    assert df.y[0] == 4

    df1 = otp.run(t1)
    assert len(df1) == 1
    assert df1.x[0] == 3
    assert not hasattr(df1, "y")

    df2 = otp.run(t2)
    assert len(df2) == 1
    assert df2.y[0] == 4
    assert not hasattr(df2, "x")


def test_join_by_time_2(f_session):
    # check names
    t1, t2 = Tick(x=3, offset=1), Tick(y=4)

    assert t1.node_name() == ""
    assert t2.node_name() == ""

    data = join_by_time([t1, t2])

    assert data.node_name() == ""
    assert t1.node_name() == ""
    assert t2.node_name() == ""


def test_join_by_time_3(f_session):
    # it is not allowed to join ticks with the same fields
    t1, t2 = Tick(x=3), Tick(x=5)

    with pytest.raises(Exception):
        join_by_time([t1, t2])


def test_check_schema(f_session):
    # https://onemarketdata.atlassian.net/browse/PY-38
    # it is not allowed to join ticks with the same fields
    # test_join_by_time_3 checks that such joins raise an error
    # but after sink column's names can be changed, e.g. NumTicks aggregation, which return only one column `VALUE`
    # by specifying check_schema=False we can avoid false positive exception
    q1 = Ticks({"A": [1, 2]})
    q2 = q1.copy()
    q1.sink(otq.NumTicks())
    join_by_time([q1, q2], check_schema=False)


def test_join_by_time_4(f_session):
    # test three sources
    t1, t2, t3 = Tick(x=3, offset=2), Tick(y=4, offset=1), Tick(z=5, offset=0)
    t2.TS2 = t2.TIMESTAMP
    t3.TS3 = t3.TIMESTAMP

    data = join_by_time([t1, t2, t3], how="inner")

    df = otp.run(data)
    assert len(df) == 1
    assert df.x[0] == 3
    assert df.y[0] == 4
    assert df.z[0] == 5


def test_join_by_time_5(f_session):
    # test inner
    data1 = Ticks(dict(x=[3, 4, 5], offset=[3, 4, 5]))
    data2 = Ticks(dict(y=[0.7, 0.6, 0.9, 0.1], offset=[0, 2, 4.5, 9]))

    assert len(otp.run(data1)) == 3
    assert len(otp.run(data2)) == 4

    data = join_by_time([data2, data1], how="inner")

    df = otp.run(data)
    assert len(df) == 2
    assert df.x[0] == 3
    assert df.y[0] == 0.9
    assert df.x[1] == 5
    assert df.y[1] == 0.1


def test_join_by_time_6(f_session):
    data = Tick(x=100) + Tick(y=101, offset=1) + Tick(z=102, offset=2) + Tick(w=103, offset=3)

    df = otp.run(data)
    assert len(df) == 4
    left, _ = data[data.w == 103]
    right, _ = data[data.x == 100]

    assert list(otp.run(left).w) == [103]

    assert list(otp.run(right).x) == [100]

    assert len(df) == 4

    del left[left.x, left.y, left.z]
    del right[right.w, right.y, right.z]

    joined_data = join_by_time([left, right], how="inner")

    joined_df = otp.run(joined_data)
    assert hasattr(joined_data, "x")
    assert hasattr(joined_data, "w")
    assert not hasattr(joined_data, "y")
    assert not hasattr(joined_data, "z")
    assert joined_df.x[0] == 100
    assert joined_df.w[0] == 103


def test_join_by_time_7(f_session):
    # nothing to join -> 0 records in result
    left = Ticks(dict(x=[100, 102]))
    right = Ticks(dict(y=[103, 104], offset=[2, 3]))

    data = join_by_time([left, right], how="inner")

    df = otp.run(data)
    assert len(df) == 0


def test_join_by_time_8(f_session):
    # matched timestams
    left = Ticks(dict(x=[100, 102], OMDSEQ=[0, 0], offset=[0, 0]))
    right = Ticks(dict(y=[100, 103], OMDSEQ=[0, 0], offset=[0, 0]))

    data = join_by_time([left, right], how="inner")

    df = otp.run(data)
    # 1 looks strange, but OneTick does not guarantee order when timestamps with OMDSEQ match
    assert len(df) == 0 or len(df) == 1


def test_join_by_time_key_1(f_session):
    left = Ticks(dict(x=[100, 103, 199], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], offset=[0, 6]))

    data = join_by_time([left, right], on=left.x, how="inner")

    assert hasattr(data, "x")
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 2
    assert df.x[0] == 100
    assert df.x[1] == 199


def test_join_by_time_key_2(f_session):
    left = Ticks(dict(x=[100, 103, 199], offset=[5, 7, 9]))
    right = Ticks(dict(x=[100, 199], offset=[6, 6]))

    data = join_by_time([left, right], on=left.x, how="inner")

    assert hasattr(data, "x")

    df = otp.run(data)
    assert len(df) == 1
    assert df.x[0] == 199


def test_join_by_time_key_3(f_session):
    left = Ticks(dict(x=[100, 103, 199], offset=[5, 7, 9]))
    right = Ticks(dict(x=[100, 199], offset=[6, 10]))

    data = join_by_time([left, right], on=left.x, how="inner")

    assert hasattr(data, "x")

    df = otp.run(data)
    assert len(df) == 0


def test_join_by_time_key_4(f_session):
    """ multiple keys """
    left = Ticks(dict(x=[100, 103, 199], y=[1, 0, 2], offset=[5, 7, 9]))
    right = Ticks(dict(x=[100, 199], y=[1, 2], offset=[0, 6]))

    data = join_by_time([left, right], on=[left.x, left.y], how="inner")

    assert hasattr(data, "x")
    assert hasattr(data, "y")
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 2
    assert df.x[0] == 100 and df.y[0] == 1
    assert df.x[1] == 199 and df.y[1] == 2


def test_join_by_time_key_5(f_session):
    """ multiple keys """
    left = Ticks(dict(x=[100, 103, 199], y=[2, 0, 2], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], y=[1, 2], offset=[0, 6]))

    data = join_by_time([left, right], on=[left.x, left.y], how="inner")

    assert hasattr(data, "x")
    assert hasattr(data, "y")
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 1
    assert df.x[0] == 199 and df.y[0] == 2


def test_join_by_time_key_6(f_session):
    """ multiple keys """
    left = Ticks(dict(x=[100, 103, 199], y=[2, 0, 3], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], y=[1, 2], offset=[0, 6]))

    data = join_by_time([left, right], on=[left.x, left.y], how="inner")

    assert hasattr(data, "x")
    assert hasattr(data, "y")
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 0


def test_join_by_time_key_7(f_session):
    """ unsupported type """
    left = Ticks(dict(x=[100, 103, 199], y=[2, 0, 3], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], y=[1, 2], offset=[0, 6]))

    with pytest.raises(TypeError):
        join_by_time([left, right], on=left.x == right.y, how="inner")


def test_join_by_time_key_8(f_session):
    """ mutliple keys """
    left = Ticks(dict(x=[100, 103, 199], y=[1, 0, 2], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], y=[1, 2]))

    right.account = "my account"

    data = join_by_time([left, right], on=[left.x, left.y], how="inner")

    assert hasattr(data, "x")
    assert hasattr(data, "y")
    assert hasattr(data, "account") and data.account.dtype is str
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 2
    assert df.x[0] == 100 and df.y[0] == 1 and df.account[0] == "my account"
    assert df.x[1] == 199 and df.y[1] == 2 and df.account[1] == "my account"


def test_join_by_time_key_9(f_session):
    """ multiple keys """
    left = Ticks(dict(x=[100, 103, 199], y=[1, 0, 2], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], y=[1, 2], offset=[0, 6]))

    left.SIZE = 100
    right.account = "my account"

    data = join_by_time([left, right], on=[left.x, left.y], how="inner")

    assert hasattr(data, "x")
    assert hasattr(data, "y")
    assert hasattr(data, "SIZE")
    assert hasattr(data, "account") and data.account.dtype is str
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 2
    assert df.x[0] == 100 and df.y[0] == 1 and df.account[0] == "my account" and df.SIZE[0] == 100
    assert df.x[1] == 199 and df.y[1] == 2 and df.account[1] == "my account" and df.SIZE[1] == 100


def test_join_by_time_key_10(f_session):
    left = Ticks(dict(x=[100, 103, 199], y=[0.35, 0.34, 0.33], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], offset=[0, 6]))

    data = join_by_time([left, right], on=left.x, how="inner")

    assert hasattr(data, "x")
    assert hasattr(data, "y") and data.y.dtype is float
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 2
    assert df.x[0] == 100 and df.y[0] == 0.35
    assert df.x[1] == 199 and df.y[1] == 0.33


def test_join_by_time_key_11(f_session):
    left = Ticks(dict(x=[100, 103, 199], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], y=[0.35, 0.39], offset=[0, 6]))

    data = join_by_time([left, right], on=left.x, how="inner")

    assert hasattr(data, "x")
    assert hasattr(data, "y") and data.y.dtype is float
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 2
    assert df.x[0] == 100 and df.y[0] == 0.35
    assert df.x[1] == 199 and df.y[1] == 0.39


def test_outer_join_by_time_1(f_session):
    t1, t2 = Tick(x=3, offset=1), Tick(y=4)

    data = join_by_time([t1, t2], how="outer")

    assert hasattr(data, "x")
    assert hasattr(data, "y")

    df = otp.run(data)
    assert len(df) == 1
    assert df.x[0] == 3 and df.y[0] == 4


def test_outer_join_by_time_2(f_session):
    """ check int default value """
    left, right = Tick(x=3, offset=1), Tick(y=4, offset=2)

    data = join_by_time([left, right], how="outer")

    assert hasattr(data, "x")
    assert hasattr(data, "y")

    df = otp.run(data)
    assert len(df) == 1
    # tick is not found, but default value for int is 0
    assert df.x[0] == 3 and df.y[0] == 0


def test_outer_join_by_time_3(f_session):
    """ check str default value """
    left, right = Tick(x=3, offset=1), Tick(y="abcdef", offset=2)

    data = join_by_time([left, right], how="outer")

    assert hasattr(data, "x")
    assert hasattr(data, "y")

    df = otp.run(data)
    assert len(df) == 1
    # tick is not found, but default value for str is ''
    assert df.x[0] == 3 and df.y[0] == ""


def test_outer_join_by_time_4(f_session):
    """ check long string default value """
    left, right = Tick(x=3, offset=1), Tick(y="x" * 1025, offset=2)

    data = join_by_time([left, right], how="outer")

    assert hasattr(data, "x")
    assert hasattr(data, "y") and data.y.dtype is ott.string[1025]

    df = otp.run(data)
    assert len(df) == 1
    # tick is not found, but default value for str is ''
    assert df.x[0] == 3 and df.y[0] == ""


def test_outer_join_by_time_5(f_session):
    """ check long string propagation """
    left, right = Tick(x=3, offset=1), Tick(y="x" * 1025)

    data = join_by_time([left, right], how="outer")

    assert hasattr(data, "x")
    assert hasattr(data, "y") and data.y.dtype is ott.string[1025]

    df = otp.run(data)
    assert len(df) == 1
    # tick is not found, but default value for str is ''
    assert df.x[0] == 3 and df.y[0] == "x" * 1025


def test_outer_join_by_time_6(f_session):
    """ check float default value """
    left, right = Tick(x=3, offset=1), Tick(y=17.4, offset=2)

    data = join_by_time([left, right], how="outer")

    assert hasattr(data, "x")
    assert hasattr(data, "y") and data.y.dtype is float

    df = otp.run(data)
    assert len(df) == 1
    # tick is not found, but default value for str is ''
    assert df.x[0] == 3 and np.isnan(df.y[0])


def test_outer_join_by_time_key_1(f_session):
    left = Ticks(dict(x=[100, 103, 199], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], offset=[0, 6]))

    data = join_by_time([left, right], on=left.x, how="outer")

    assert hasattr(data, "x")
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 3
    assert df.x[0] == 100
    assert df.x[1] == 103
    assert df.x[2] == 199


def test_outer_join_by_time_key_2(f_session):
    left = Ticks(dict(x=[100, 103, 199], offset=[5, 7, 9]))
    right = Ticks(dict(x=[100, 199], offset=[6, 6]))

    data = join_by_time([left, right], on=left.x, how="outer")

    assert hasattr(data, "x")

    df = otp.run(data)
    assert len(df) == 3
    assert df.x[0] == 100
    assert df.x[1] == 103
    assert df.x[2] == 199


def test_outer_join_by_time_key_3(f_session):
    left = Ticks(dict(x=[100, 103, 199], offset=[5, 7, 9]))
    right = Ticks(dict(x=[100, 199], offset=[6, 10]))

    data = join_by_time([left, right], on=left.x, how="outer")

    assert hasattr(data, "x")

    df = otp.run(data)
    assert len(df) == 3
    assert df.x[0] == 100
    assert df.x[1] == 103
    assert df.x[2] == 199


def test_outer_join_by_time_key_4(f_session):
    """  multiple keys """
    left = Ticks(dict(x=[100, 103, 199], y=[1, 0, 2], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], y=[1, 2], offset=[0, 6]))

    data = join_by_time([left, right], on=[left.x, left.y], how="outer")

    assert hasattr(data, "x")
    assert hasattr(data, "y")
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 3
    assert df.x[0] == 100 and df.y[0] == 1
    assert df.x[1] == 103 and df.y[1] == 0  # default value added by table
    assert df.x[2] == 199 and df.y[2] == 2


def test_outer_join_by_time_key_5(f_session):
    """ multiple keys """
    left = Ticks(dict(x=[100, 103, 199], y=[2, 0, 2], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], y=[1, 2], offset=[0, 6]))

    data = join_by_time([left, right], on=[left.x, left.y], how="outer")

    assert hasattr(data, "x")
    assert hasattr(data, "y")
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 3
    assert df.x[0] == 100 and df.y[0] == 2  # use leading one
    assert df.x[1] == 103 and df.y[1] == 0  # use default one
    assert df.x[2] == 199 and df.y[2] == 2


def test_outer_join_by_time_key_6(f_session):
    """ multiple keys """
    left = Ticks(dict(x=[100, 103, 199], y=[2, 0, 3], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], y=[1, 2], offset=[0, 6]))

    data = join_by_time([left, right], on=[left.x, left.y], how="outer")

    assert hasattr(data, "x")
    assert hasattr(data, "y")
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 3
    assert df.x[0] == 100 and df.y[0] == 2
    assert df.x[1] == 103 and df.y[1] == 0
    assert df.x[2] == 199 and df.y[2] == 3


def test_outer_join_by_time_key_7(f_session):
    left = Ticks(dict(x=[100, 103, 199], y=[0.35, 0.34, 0.33], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], offset=[0, 6]))

    data = join_by_time([left, right], on=left.x, how="outer")

    assert hasattr(data, "x")
    assert hasattr(data, "y") and data.y.dtype is float
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 3
    assert df.x[0] == 100 and df.y[0] == 0.35
    assert df.x[1] == 103 and df.y[1] == 0.34
    assert df.x[2] == 199 and df.y[2] == 0.33


def test_outer_join_by_time_key_8(f_session):
    left = Ticks(dict(x=[100, 103, 199], offset=[5, 7, 8]))
    right = Ticks(dict(x=[100, 199], y=[0.35, 0.39], offset=[0, 6]))

    data = join_by_time([left, right], on=left.x, how="outer")

    assert hasattr(data, "x")
    assert hasattr(data, "y") and data.y.dtype is float
    assert hasattr(data, "TIMESTAMP")

    df = otp.run(data)
    assert len(df) == 3
    assert df.x[0] == 100 and df.y[0] == 0.35
    assert df.x[1] == 103 and np.isnan(df.y[1])
    assert df.x[2] == 199 and df.y[2] == 0.39


def test_join_by_time_9(f_session):
    def get_md():
        return Ticks([["ASK_PRICE", "BID_PRICE"], [12.7, 11.5], [12.71, 11.52], [12.7, 11.49]])

    def get_orders():
        return Ticks([["PRICE", "offset"], [12.3, 1], [12.5, 2], [12.4, 3]])

    ord_joined = join_by_time([get_orders(), get_md()])

    orders_filters, _ = ord_joined[
        (ord_joined.PRICE <= ord_joined.ASK_PRICE) & (ord_joined.PRICE >= ord_joined.BID_PRICE)
    ]

    df = otp.run(orders_filters)
    assert not df.empty


def test_join_by_time_policy_leader_first(f_session):
    data1 = Ticks({"x": [1, 2], "offset": [0, 1]})
    data2 = Ticks({"y": [1, 2], "offset": [0, 0]})
    result = join_by_time([data1, data2], policy="each_for_leader_with_first")

    assert len(otp.run(result)) == 2
    assert otp.run(result).x[0] == 1 and otp.run(result).y[0] == 1
    assert otp.run(result).x[1] == 2 and otp.run(result).y[1] == 1


def test_join_by_time_policy_leader_latest(f_session):
    data1 = Ticks({"x": [1, 2], "offset": [0, 1]})
    data2 = Ticks({"y": [1, 2], "offset": [0, 0]})
    result = join_by_time([data1, data2], policy="each_for_leader_with_latest")

    assert len(otp.run(result)) == 2
    assert otp.run(result).x[0] == 1 and otp.run(result).y[0] == 2
    assert otp.run(result).x[1] == 2 and otp.run(result).y[1] == 2


def test_join_by_time_policy_arrival_order(f_session):
    data1 = Ticks({"x": [1, 2], "offset": [2, 2]})
    data2 = Ticks({"y": [1, 2], "offset": [0, 0]})
    result = join_by_time([data1, data2], policy="arrival_order")

    assert len(otp.run(result)) == 2
    assert otp.run(result).x[0] == 1 and otp.run(result).y[0] == 2
    assert otp.run(result).x[1] == 2 and otp.run(result).y[1] == 2


def test_join_by_time_policy_latest_ticks(f_session):
    data1 = Ticks({"x": [1, 2], "offset": [2, 2]})
    data2 = Ticks({"y": [1, 2], "offset": [0, 0]})
    result = join_by_time([data1, data2], policy="latest_ticks")

    assert len(otp.run(result)) == 1
    assert otp.run(result).x[0] == 2 and otp.run(result).y[0] == 2


def test_jbt_and_aggr(f_session):
    q1 = Ticks({"A": [1, 2]})
    q2 = q1.copy()

    q1 = q1.agg({"VALUE": otp.agg.count()})
    jbt = join_by_time([q1, q2])

    assert len(otp.run(jbt))


class TestLeading:
    @pytest.fixture(scope="class")
    def data(self):
        q1 = Ticks({"A": [11, 12], "offset": [0, 2]})
        q2 = Ticks({"B": [21, 22], "offset": [1, 3]})
        yield q1, q2

    def check_if_1_is_leading(self, data):
        df = otp.run(data)
        assert all(df["A"] == [11, 12])
        assert all(df["B"] == [00, 21])

    def check_if_2_is_leading(self, data):
        df = otp.run(data)
        assert all(df["A"] == [11, 12])
        assert all(df["B"] == [21, 22])

    def check_if_both_are_leading(self, data):
        df = otp.run(data)
        assert all(df["A"] == [11, 11, 12, 12])
        assert all(df["B"] == [0, 21, 21, 22])

    def test_int(self, data, session):
        data1 = otp.join_by_time(data)
        self.check_if_1_is_leading(data1)
        data2 = otp.join_by_time(data, leading=1)
        self.check_if_2_is_leading(data2)

    def test_negative_int(self, data, session):
        data1 = otp.join_by_time(data, leading=-1)
        self.check_if_2_is_leading(data1)

    def test_wrong_int(self, data, session):
        otp.join_by_time(data)  # the correct join should work
        with pytest.raises(ValueError):
            otp.join_by_time(data, leading=2)
        with pytest.raises(ValueError):
            otp.join_by_time(data, leading=-3)

    def test_source(self, data, session):
        d1, d2 = data
        data1 = otp.join_by_time([d1, d2], leading=d2)
        self.check_if_2_is_leading(data1)

    @pytest.mark.parametrize("leading", ["all", (0, 1), [1, 0]])
    def test_both(self, data, session, leading):
        d1, d2 = data
        data1 = otp.join_by_time([d1, d2], leading="all")
        self.check_if_both_are_leading(data1)

    def test_both_as_source(self, data, session):
        d1, d2 = data
        data1 = otp.join_by_time(data, leading=[d1, d2])
        self.check_if_both_are_leading(data1)
        data2 = otp.join_by_time(data, leading=(d2, d1))
        self.check_if_both_are_leading(data2)

    def test_wrong(self, data, session):
        with pytest.raises(ValueError, match="wrong leading param was specified"):
            data = otp.join_by_time(data, leading=None)


class TestLeadingWithOmdseq:
    # PY-173: JBT does not handle OMDSEQ correctly

    @pytest.fixture(scope="class")
    def data(self):
        q1 = Ticks(X1=[3, 4], Y1=[1] * 2, offset=[0, 1], OMDSEQ=[2, 2])
        q2 = Ticks(X2=[1, 2], Y2=[2] * 2, offset=[0, 1], OMDSEQ=[1, 3])
        yield q1, q2

    def check_if_1_is_leading(self, data):
        df = otp.run(data)
        assert all(df["X1"] == [3, 4])
        assert all(df["X2"] == [1, 1])
        assert all(df["OMDSEQ"] == [2, 2])

    def check_if_2_is_leading(self, data):
        df = otp.run(data)
        assert all(df["X1"] == [0, 4])
        assert all(df["X2"] == [1, 2])
        assert all(df["OMDSEQ"] == [1, 3])

    def test_omdseq_first_leading(self, session, data):
        data1, data2 = data
        result = otp.join_by_time([data1, data2])
        self.check_if_1_is_leading(result)

    def test_omdseq_second_leading(self, session, data):
        data1, data2 = data
        result = otp.join_by_time([data1, data2], leading=1)
        self.check_if_2_is_leading(result)

    def test_omdseq_both_leading_both_with_omdseq(self, session, data):
        data1, data2 = data
        with pytest.raises(
            ValueError,
            match="Several sources was specified as leading and OMDSEQ field is presented in more than one source",
        ):
            otp.join_by_time([data1, data2], leading="all")

    def test_omdseq_both_leading_only_one_with_omdseq(self, session, data):
        data1, data2 = data
        data2.drop("OMDSEQ", inplace=True)
        result = otp.join_by_time([data1, data2], leading="all")
        df = otp.run(result)
        assert all(df["X1"] == [0, 3, 3, 4])
        assert all(df["X2"] == [1, 1, 2, 2])
        assert all(df["OMDSEQ"] == [0, 2, 2, 2])

    def test_three(self, session):
        data1 = Ticks(dict(X1=[3, 4], Y1=[1] * 2, offset=[0, 1], OMDSEQ=[2, 2]))
        data2 = Ticks(dict(X2=[1, 2], Y2=[2] * 2, offset=[0, 1]))
        data3 = Ticks(dict(X3=[1, 2], Y3=[3] * 2, offset=[1, 1]))
        result = otp.join_by_time([data2, data1], leading=1)
        result = otp.join_by_time([data3, result], leading=1)
        df = otp.run(result)
        assert all(df["X1"] == [3, 4])
        assert all(df["X2"] == [1, 2])
        assert all(df["X3"] == [0, 2])
        assert all(df["OMDSEQ"] == [2, 2])


class TestMatchIfIdenticalTimes:

    def test_two(self, session):
        data1 = Ticks(X1=[3, 4], Y1=[1] * 2, offset=[0, 1], OMDSEQ=[2, 2])
        data2 = Ticks(X2=[1, 2], Y2=[2] * 2, offset=[0, 1])
        result = otp.join_by_time([data2, data1], match_if_identical_times=True)
        df = otp.run(result)
        assert all(df["X2"] == [1, 2])
        assert all(df["Y2"] == [2, 2])
        assert all(df["X1"] == [3, 4])
        assert all(df["Y1"] == [1, 1])
        result = otp.join_by_time([data2, data1], match_if_identical_times=False)
        df = otp.run(result)
        assert all(df["X2"] == [1, 2])
        assert all(df["Y2"] == [2, 2])
        assert all(df["X1"] == [0, 3])
        assert all(df["Y1"] == [0, 1])

    def test_three(self, session):
        data1 = Ticks(X1=[3, 4], Y1=[1] * 2, offset=[0, 0], OMDSEQ=[0, 2])
        data2 = Ticks(X2=[1, 2], Y2=[2] * 2, offset=[0, 1])
        data3 = Ticks(X3=[1, 2], Y3=[3] * 2, offset=[1, 1])
        result = otp.join_by_time([data3, data2, data1], match_if_identical_times=True)
        df = otp.run(result)
        assert all(df["X2"] == [2])
        assert all(df["Y2"] == [2])
        assert all(df["X1"] == [0])
        assert all(df["Y1"] == [0])
        result = otp.join_by_time([data3, data2, data1], match_if_identical_times=False)
        df = otp.run(result)
        assert all(df["X2"] == [1, 1])
        assert all(df["Y2"] == [2, 2])
        assert all(df["X1"] == [4, 4])
        assert all(df["Y1"] == [1, 1])

    def test_different_amount(self, session):
        data1 = Ticks(X1=[3, 4, 5, 6], Y1=[1] * 4, offset=[1, 1, 2, 10])
        data2 = Ticks(X2=[1, 2], Y2=[2] * 2, offset=[1, 2])

        result = otp.join_by_time([data2, data1], match_if_identical_times=True)
        df = otp.run(result)
        assert all(df["X2"] == [1, 2])
        assert all(df["Y2"] == [2, 2])
        assert all(df["X1"] == [4, 5])
        assert all(df["Y1"] == [1, 1])
        result = otp.join_by_time([data2, data1], match_if_identical_times=False)
        df = otp.run(result)
        assert all(df["X2"] == [1, 2])
        assert all(df["Y2"] == [2, 2])
        assert all(df["X1"] == [0, 4])
        assert all(df["Y1"] == [0, 1])


def test_output_type(session):
    ticks = otp.Ticks({'A': [1, 2, 3]})
    tick = otp.Tick(B=1)

    result = otp.join_by_time([ticks, tick])
    assert result.__class__ == otp.Source
    tt1 = otp.run(result)

    result = otp.join_by_time([ticks, tick], output_type_index=0)
    assert result.__class__ == ticks.__class__
    tt2 = otp.run(result)
    assert tt2.equals(tt1)

    some_db_1 = otp.DB('SOME_DB_1')
    some_db_1.add(otp.Ticks({'C': [1, 2, 3]}))
    session.use(some_db_1)
    some_db_2 = otp.DB('SOME_DB_2')
    some_db_2.add(otp.Ticks({'D': [1, 2, 3]}))
    session.use(some_db_2)

    custom_1 = otp.DataSource(some_db_1, symbols=otp.config['default_symbol'])
    custom_2 = otp.DataSource(some_db_2, symbols=otp.config['default_symbol'])

    result = otp.join_by_time([custom_1, tick])
    assert result.__class__ == otp.Source
    ct1 = otp.run(result)

    result = otp.join_by_time([custom_1, tick], output_type_index=0)
    assert result.__class__ == otp.DataSource
    assert result.db == custom_1.db
    ct2 = otp.run(result)
    assert ct2.equals(ct1)

    result = otp.join_by_time([custom_1, custom_2])
    assert result.__class__ == otp.Source
    cc1 = otp.run(result)

    result = otp.join_by_time([custom_1, custom_2], output_type_index=1)
    assert result.__class__ == otp.DataSource
    assert result.db == custom_2.db
    cc2 = otp.run(result)
    assert cc2.equals(cc1)


class TestJoinByTimeOnTypes:
    @pytest.mark.parametrize("value1,value2", [
        (1, "1"),
        (1.0, "1"),
        (1.0, 1),
        (1, 1.0),
    ])
    def test_different_types(self, session, value1, value2):
        ticks = otp.Ticks({'A': [value1]})
        tick = otp.Tick(A=value2)
        with pytest.raises(TypeError):
            otp.join_by_time([ticks, tick], on="A", check_schema=True)

    @pytest.mark.parametrize("type1,type2", [
        (str, otp.string[20]),
        (otp.string[10], otp.string[20]),
    ])
    def test_types_string_different(self, session, type1, type2):
        ticks = otp.Ticks({'A': ["1"]}).table(A=type1)
        ticks2 = otp.Ticks({'A': ["1"]}).table(A=type2)
        with pytest.raises(TypeError):
            otp.join_by_time([ticks, ticks2], on="A", check_schema=True)

    @pytest.mark.parametrize("type1,type2", [
        (str, otp.string),
        (str, otp.string[64]),
        (otp.string, otp.string[64]),
        (otp.string[10], otp.string[10]),
    ])
    def test_types_string_similar(self, session, type1, type2):
        ticks = otp.Ticks({'A': ["1"]}).table(A=type1)
        ticks2 = otp.Ticks({'A': ["1"]}).table(A=type2)
        result = otp.join_by_time([ticks, ticks2], on="A", check_schema=True)
        df = otp.run(result)
        assert df['A'][0] == "1"

    @pytest.mark.parametrize("value1,value2", [
        (1, "1"),
        (1.0, "1"),
        (1.0, 1),
        (1, 1.0),
    ])
    def test_check_schema_false(self, session, value1, value2):
        ticks = otp.Ticks({'A': [value1]})
        tick = otp.Tick(A=value2)
        # no exception raised here
        otp.join_by_time([ticks, tick], on="A", check_schema=False)

    def test_empty_sources_example(self, session):
        # if source have no ticks and have no "on" column, OneTick EP JOIN_BY_TIME would not throw an exception:
        # "In JOIN_BY_TIME: join key attribute with name A cannot be found."
        # this is why check_schema also controls type check in join_by_time
        ticks = otp.Ticks({'A': ["1"]})
        ticks2 = otp.Empty()
        result = otp.join_by_time([ticks, ticks2], on="A", leading="all", check_schema=False)
        df = otp.run(result)
        assert df['A'][0] == "1"


class TestJoinByTimeBoundSymbols:
    @pytest.fixture(scope='class', autouse=True)
    def db_setup(self, session):
        db = otp.DB('MULTI_SYMBOL_DB')
        db.add(otp.Ticks(A=[4, 6, 9, 11, 6, 7]), symbol='A', tick_type='TRD')
        db.add(otp.Ticks(A=[5, 4, 5, 7, 6, 7]), symbol='B', tick_type='TRD')
        session.use(db)

    def test_one_symbol(self):
        data = otp.DataSource(db='MULTI_SYMBOL_DB', tick_type='TRD')
        data = data.agg({'last': otp.agg.last(data['A'])}, bucket_interval=2, bucket_units='ticks')
        data = otp.join_by_time([data], symbols=['A'], match_if_identical_times=True)

        df = otp.run(data)

        assert list(df['last']) == [6, 11, 7]
        assert set(df.columns) == {'Time', 'last'}

    def test_multi_symbol(self):
        data = otp.DataSource(db='MULTI_SYMBOL_DB', tick_type='TRD')
        data = data.agg({'last': otp.agg.last(data['A'])}, bucket_interval=2, bucket_units='ticks')
        data = otp.join_by_time([data], symbols=['A', 'B'], match_if_identical_times=True)

        df = otp.run(data)

        assert list(df['A.last'] == [6, 11, 7])
        assert list(df['B.last'] == [4, 7, 7])
        assert set(df.columns) == {'Time', 'A.last', 'B.last'}

    def test_source_as_symbol(self):
        data = otp.DataSource(db='MULTI_SYMBOL_DB', tick_type='TRD')
        data = data.agg({'last': otp.agg.last(data['A'])}, bucket_interval=2, bucket_units='ticks')
        data = otp.join_by_time(
            [data], symbols=otp.Ticks(SYMBOL_NAME=['A', 'B']), match_if_identical_times=True,
        )

        df = otp.run(data)

        assert list(df['A.last'] == [6, 11, 7])
        assert list(df['B.last'] == [4, 7, 7])
        assert set(df.columns) == {'Time', 'A.last', 'B.last'}

    def test_source_as_symbol_single(self):
        data = otp.DataSource(db='MULTI_SYMBOL_DB', tick_type='TRD')
        data = data.agg({'last': otp.agg.last(data['A'])}, bucket_interval=2, bucket_units='ticks')
        data = otp.join_by_time(
            [data], symbols=otp.Ticks(SYMBOL_NAME=['A']), match_if_identical_times=True,
        )

        df = otp.run(data)
        assert list(df['last'] == [6, 11, 7])
        assert set(df.columns) == {'Time', 'last'}

    def test_exceptions(self):
        data_1 = otp.Ticks(X=[1, 2, 3, 4, 5])
        data_2 = otp.Ticks(Y=[5, 4, 3, 2, 1])
        sym = otp.Ticks(SYMBOL_NAME=['A'])

        with pytest.raises(ValueError):
            _ = otp.join_by_time([data_1, data_2], symbols=['A'])

        with pytest.raises(ValueError):
            _ = otp.join_by_time([data_1, data_2], symbols=['A', 'B'])

        with pytest.raises(ValueError):
            _ = otp.join_by_time([data_1, data_2], symbols=sym)
