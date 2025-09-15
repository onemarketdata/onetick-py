import pytest
import onetick.py as otp
import numpy as np
import pandas as pd


@pytest.fixture(scope='module', autouse=True)
def session(m_session):
    pass


class TestLag:
    @pytest.mark.parametrize(
        "values,default",
        [
            ([1, 2, 3, 4, 5], 0),
            ([0.5, 0.4, -0.1, 19.2], np.nan),
            (["a", "b", "ddd", "e"], ""),
            ([otp.datetime(2019, 4, 5), otp.datetime(2017, 3, 2)], otp.datetime(1969, 12, 31, 19)),
        ],
    )
    def test_add_column(self, values, default):
        data = otp.Ticks(dict(x=values))

        data.y = data.x[-1]
        assert data.y.dtype is data.x.dtype

        df = otp.run(data)

        if data.y.dtype is float:
            assert np.isnan(df["y"][0])
        else:
            assert df["y"][0] == default

        for inx in range(1, len(df)):
            assert df["y"][inx] == df["x"][inx - 1]

    @pytest.mark.parametrize(
        "values,default",
        [
            ([1, 2, 3, 4, 5], 0),
            ([0.5, 0.4, -0.1, 19.2], np.nan),
            (["a", "b", "ddd", "e"], ""),
            ([otp.datetime(2019, 4, 5), otp.datetime(2017, 3, 2)], otp.datetime(1969, 12, 31, 19)),
        ],
    )
    def test_update_column(self, values, default):
        data = otp.Ticks(dict(x=values, y=values))

        data.y = data.x[-1]
        assert data.y.dtype is data.x.dtype

        df = otp.run(data)

        if data.y.dtype is float:
            assert np.isnan(df["y"][0])
        else:
            assert df["y"][0] == default

        for inx in range(1, len(df)):
            assert df["y"][inx] == df["x"][inx - 1]

    def test_where(self):
        data = otp.Ticks(dict(x=[1, 2, 2, 3]))

        data, _ = data[(data.x[-1] != data.x) & (data.x[-1] != 0)]

        df = otp.run(data)
        assert len(df) == 2
        assert df["x"][0] == 2
        assert df["x"][1] == 3

    def test_many(self):
        data = otp.Ticks(dict(x=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))

        data.b1 = data.x[-1]
        data.b2 = data.x[-2]
        data.b3 = data.x[-3]
        data.b4 = data.x[-4]
        data.b5 = data.x[-5]
        data.sum_last_5 = sum([data.x[-j] for j in range(1, 6)])

        df = otp.run(data)

        for inx in range(len(df)):
            for offset in range(1, 6):
                if inx - offset >= 0:
                    assert df[f"b{offset}"][inx] == df["x"][inx - offset]
            if inx - 5 >= 0:
                assert df["sum_last_5"][inx] == sum([df["x"][inx - j] for j in range(1, 6)])


class TestPositiveLag:
    def test_add_field(self):
        data = otp.Ticks(dict(A=[1, 2, 3], B=[0.0, otp.nan, -1.0], C=["A", "B", "C"]))
        for column in ("A", "B", "C"):
            data[f"{column}_NEW"] = data[column][1]
        df = otp.run(data)
        assert all(df["A_NEW"] == [2, 3, 0])
        assert df["B_NEW"][1] == -1.0
        assert all(df["B_NEW"].isna() == [True, False, True])
        assert all(df["C_NEW"] == ["B", "C", ""])

    def test_add_field_with_expr(self):
        data = otp.Ticks(dict(A=list(range(10))))
        data["A_NEW"] = data["A"][2] - 2
        df = otp.run(data)
        assert all(df["A_NEW"] == [0, 1, 2, 3, 4, 5, 6, 7, -2, -2])

    def test_update_field_with_expr(self):
        data = otp.Ticks(dict(A=list(range(10))))
        data["A"] = data["A"][2] - 2
        df = otp.run(data)
        assert all(df["A"] == [0, 1, 2, 3, 4, 5, 6, 7, -2, -2])

    def test_update_field_with_accessor_and_expr(self):
        data = otp.Ticks(dict(A=["AA", "AB", "AB", "BB", "BB", "BC"]))
        data["A"] = ("'" + data["A"][1] + '"').str.find(data["A"]) + 2
        df = otp.run(data)
        assert all(df["A"] == [1, 3, 1, 3, 1, 1])

    def test_update_field_with_several_lags(self):
        data = otp.Ticks(dict(A=[1, 2, 3, 4, 5], B=[1] * 5))
        data["A"] += data["A"][-1] + 2 + data["A"][1] + data["A"][1] - data["A"][2] + data["B"][2]
        df = otp.run(data)
        assert all(df["A"] == [5, 8, 11, 19, 11])


class TestTime:
    def test_time_with_forward_lag(self):
        data = otp.Ticks(dict(A=[1, 2, 3, 4, 5], B=[1] * 5))
        data["DIFF"] = otp.Milli(data["Time"][1] - data["Time"])
        df = otp.run(data)
        assert all(df["DIFF"] == [1, 1, 1, 1, -1070254800004])

    def test_time_with_both_lag_in_datepart(self):
        data = otp.Ticks(dict(A=[1, 2, 3, 4, 5], B=[1] * 5, offset=[0, 0, 1, 5, 10]))
        data["DIFF"] = otp.Milli(data["Time"][-1] - data["Time"][1])
        df = otp.run(data)
        expected = (-1070254800000, -1, -5, -9, 1070254800005)
        assert all(df["DIFF"] == expected)

    def test_time_with_forward_lag_in_datepart(self):
        data = otp.Ticks(dict(A=[1, 2, 3, 4, 5], B=[1] * 5, offset=[0, 0, 1, 5, 10]))
        data["DIFF"] = otp.Milli(data["Time"] - data["Time"][1])
        df = otp.run(data)
        expected = (0, -1, -4, -5, 1070254800010)
        assert all(df["DIFF"] == expected)

    def test_time_with_const(self):
        data = otp.Ticks(dict(A=[1, 2, 3, 4, 5], B=[1] * 5, offset=[0, 0, 1, 5, 10]))
        data["DIFF"] = data["Time"][1] - 1
        df = otp.run(data, timezone="GMT")
        expected = ([otp.config['default_start_time'] + otp.Milli(d) for d in (-1, 0, 4, 9)]
                    + [pd.Timestamp(-1 * 1_000_000)])
        assert all(df["DIFF"] == expected)

    def test_time_with_const_in_datepart(self):
        data = otp.Ticks(dict(A=[1, 2, 3, 4, 5], B=[1] * 5, offset=[0, 0, 1, 5, 10]))
        with pytest.raises(ValueError,
                           match=r"Date arithmetic operations \(except date2-date1, which calculate an amount of "
                                 r"periods between two dates\) are not accepted in TimeOffset constructor"):
            data["DIFF"] = otp.Nano(data["Time"][1] - 1)

    def test_assign_timestamp(self):
        data = otp.Ticks(dict(A=[1, 2, 3, 4, 5], B=[0, 0, 1, 5, 10]))
        data["Time"] += data["B"][1] - data["A"][-1]
        df = otp.run(data)
        assert all(df["Time"] == [otp.config['default_start_time'] + otp.Milli(d) for d in (0, 0, 1, 5, 10)])
        assert all(df["A"] == [1, 5, 2, 3, 4])
        assert all(df["B"] == [0, 10, 0, 1, 5])


class TestWrong:
    def test_types(self):
        """ check that only in is supported """
        data = otp.Ticks(dict(x=[1], y=[2]))

        with pytest.raises(TypeError):
            data.x["-1"]
        with pytest.raises(TypeError):
            data.x[-1.1]
        with pytest.raises(TypeError):
            data.x[data.y]

    def test_value(self):
        data = otp.Ticks(dict(x=[1, 2, 3]))

        data.y = data.x[-1]
        data.z = data.x[0]

        df = otp.run(data)

        assert df["y"].tolist() == [0, 1, 2]
        assert df["z"].tolist() == [1, 2, 3]
