import pytest
import onetick.py as otp


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    pass


class TestMax:
    def test_two_columns(self):
        data = otp.Ticks(dict(x=[1, 5], y=[4, -1]))

        data.z = otp.math.max(data.x, data.y)

        assert data.z.dtype is int

        df = otp.run(data)

        assert df["z"][0] == 4
        assert df["z"][1] == 5

    def test_three_columns(self):
        data = otp.Ticks(dict(x=[1, 3, 5], y=[4, -1, 2], z=[2, 1, 8]))

        data.u = otp.math.max(data.x, data.y, data.z)

        assert data.z.dtype is int

        df = otp.run(data)

        assert df["u"][0] == 4
        assert df["u"][1] == 3
        assert df["u"][2] == 8

    def test_columns_and_const(self):
        data = otp.Ticks(dict(x=[1, 3, 5], y=[4, -1, 2], z=[2, 1, 8]))

        data.u = otp.math.max(data.x, 6, data.y, data.z)

        assert data.z.dtype is int

        df = otp.run(data)

        assert df["u"][0] == 6
        assert df["u"][1] == 6
        assert df["u"][2] == 8

    def test_script_2_values(self):
        def fun(tick):
            tick['RES'] = otp.math.max(tick['X'], tick['Y'])

        data = otp.Ticks(dict(X=[1, 3, 5], Y=[4, -1, 2]))
        data = data.script(fun)
        df = otp.run(data)

        assert all(df['RES'] == [4, 3, 5])

    def test_script_4_values(self):
        def fun(tick):
            tick['RES'] = otp.math.max(tick['X'], tick['Y'], 6, tick['Z'])

        data = otp.Ticks(dict(X=[1, 3, 5], Y=[4, -1, 2], Z=[2, 1, 8]))
        data = data.script(fun)
        df = otp.run(data)

        assert all(df['RES'] == [6, 6, 8])


class TestMin:
    def test_two_columns(self):
        data = otp.Ticks(dict(x=[1, 5], y=[4, -1]))

        data.z = otp.math.min(data.x, data.y)

        assert data.z.dtype is int

        df = otp.run(data)

        assert df["z"][0] == 1
        assert df["z"][1] == -1

    def test_three_columns(self):
        data = otp.Ticks(dict(x=[1, 3, 5], y=[4, -1, 2], z=[2, 1, 8]))

        data.u = otp.math.min(data.x, data.y, data.z)

        assert data.z.dtype is int

        df = otp.run(data)

        assert df["u"][0] == 1
        assert df["u"][1] == -1
        assert df["u"][2] == 2

    def test_columns_and_const(self):
        data = otp.Ticks(dict(x=[1, 3, 5], y=[4, -1, 3], z=[2, 1, 8]))

        data.u = otp.math.min(data.x, 2, data.y, data.z)

        assert data.z.dtype is int

        df = otp.run(data)

        assert df["u"][0] == 1
        assert df["u"][1] == -1
        assert df["u"][2] == 2

    def test_operations(self):
        data = otp.Ticks(dict(x=[1, 3, 5], y=[4, -1, 2], z=[2, 1, 8]))

        data.u = otp.math.min(data.x + data.y, data.z * data.y)

        df = otp.run(data)

        assert df["u"][0] == 5
        assert df["u"][1] == -1
        assert df["u"][2] == 7

    def test_script_2_values(self):
        def fun(tick):
            tick['RES'] = otp.math.min(tick['X'], tick['Y'])

        data = otp.Ticks(dict(X=[1, 3, 5], Y=[4, -1, 2]))
        data = data.script(fun)
        df = otp.run(data)

        assert all(df['RES'] == [1, -1, 2])

    def test_script_4_values(self):
        def fun(tick):
            tick['RES'] = otp.math.min(tick['X'], tick['Y'], 6, tick['Z'])

        data = otp.Ticks(dict(X=[1, 3, 5], Y=[4, -1, 2], Z=[2, 1, 8]))
        data = data.script(fun)
        df = otp.run(data)

        assert all(df['RES'] == [1, -1, 2])
