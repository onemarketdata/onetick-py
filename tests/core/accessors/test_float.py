import random
import pytest

import onetick.py as otp


class TestCommon:
    def test_access(self):
        """ only float type columns have float accessor """

        data = otp.Ticks(dict(x=[0.5, 2.1]))
        assert data.x.dtype is float
        data.x.float

        data = otp.Ticks(dict(x=["a", "b"]))
        assert data.x.dtype is str
        with pytest.raises(TypeError, match="float accessor is available only for float type columns"):
            data.x.float

        data = otp.Ticks(dict(x=[1, 2]))
        assert data.x.dtype is int
        with pytest.raises(TypeError, match="float accessor is available only for float type columns"):
            data.x.float


class TestToStr:
    def test_random_floats(self, m_session):
        data = otp.Ticks(dict(x=[random.random() * 100 * random.choice([1, -1]) for _ in range(random.randint(1, 10))]))
        precision = random.randint(2, 10)
        length = random.randint(precision + 1, 20)
        data["str"] = data["x"].float.str(length, precision)
        data = otp.run(data)
        assert data["str"].dtype == otp.string[length]


class TestCmpAndEq:
    def test_cmp(self, m_session):
        data = otp.Ticks(X=[2.4, 1.3, -3.4, 0.1])
        data["X"] = data["X"].float.cmp(0.0, 1)
        df = otp.run(data)
        assert all(df["X"] == [1.0, 1.0, -1.0, 1.0])

    def test_eq(self, m_session):
        data = otp.Ticks(X=[2.4, 1.3, -0.34, 0.1])
        data["X"] = ~data["X"].float.eq(0.0, 1)
        df = otp.run(data)
        assert all(df["X"] == [1.0, 1.0, 0.0, 0.0])


class TestScientificNotation:
    def test_cmp(self, m_session):
        src = otp.Ticks({'X': [1.1, 2.3, 3.00000001]})
        src['CMP'] = src['X'].float.cmp(10 ** -6, 10 ** -6)
        src['CMP'] = src['X'].float.cmp(src['X'], 10 ** -6)
        df = otp.run(src)
        assert all(df['CMP'] == 0)

    def test_eq(self, m_session):
        src = otp.Ticks({'X': [1.1, 2.3, 3.00000001]})
        src['EQ'] = src['X'].float.eq(10 ** -6, 10 ** -6)
        src['EQ'] = src['X'].float.eq(src['X'], 10 ** -6)
        df = otp.run(src)
        assert all(df['EQ'] == 1)


def test_replace_parameters(m_session):
    from onetick.py.functions import _add_node_name_prefix_to_columns_in_operation
    t = otp.Tick(AA=1.1, BB=2.3, CC=1, DD=0.01)
    t.node_name('PREFIX')

    ops = [
        t['AA'].float.str(t['CC'], t['DD']),
        t['AA'].float.cmp(t['BB'], t['CC']),
        t['AA'].float.eq(t['BB'], t['DD']),
    ]

    for op in ops:
        str_op = str(op)
        str_replaced_op = str_op[:]
        for column in t.schema:
            str_replaced_op = str_replaced_op.replace(column, f'PREFIX.{column}')
        str_node_name_prefix_op = str(_add_node_name_prefix_to_columns_in_operation(op, t))
        assert str_replaced_op == str_node_name_prefix_op
