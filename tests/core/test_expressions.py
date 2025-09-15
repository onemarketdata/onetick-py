from contextlib import contextmanager

from pandas import Timestamp

from onetick.py.core._internal._state_objects import _StateColumn

from onetick.py.core.column import _Column

import onetick.py as otp
from onetick.test.fixtures import m_session as session


class TestMethodOnExpressions:
    @contextmanager
    def prepare_data_and_check(self, *, data, expected):
        data = otp.Ticks(data)
        yield data
        df = otp.run(data)
        assert all(df["COLUMN_BASED"] == df["EXPRESSION_BASED"])
        for index, value in enumerate(df["EXPRESSION_BASED"]):
            assert value == expected[index]

    def test_apply(self, session):
        with self.prepare_data_and_check(data=dict(A=[1, 2, 3], B=[0, 0, -1]), expected=[0, 1, 1]) as data:
            data["COLUMN_BASED"] = (data["A"] + data["B"])
            data["COLUMN_BASED"] = data["COLUMN_BASED"].apply(lambda x: x == 2)
            data["EXPRESSION_BASED"] = (data["A"] + data["B"]).apply(lambda x: x == 2)

    def test_astype(self, session):
        with self.prepare_data_and_check(data=dict(A=[1, 2, 3], B=[0, 0, -1]), expected=["1", "2", "2"]) as data:
            data["EXPRESSION_BASED"] = (data["A"] + data["B"]).astype(str)
            data["COLUMN_BASED"] = (data["A"] + data["B"])
            data["COLUMN_BASED"] = data["COLUMN_BASED"].astype(str)

    def test_isin(self, session):
        with self.prepare_data_and_check(data=dict(A=[1, 2, 3], B=[0, 0, -1]), expected=[0, 1, 1]) as data:
            data["COLUMN_BASED"] = (data["A"] + data["B"])
            data["COLUMN_BASED"] = data["COLUMN_BASED"].isin(2)
            data["EXPRESSION_BASED"] = (data["A"] + data["B"]).isin(2)

    def test_fillna(self, session):
        with self.prepare_data_and_check(data=dict(A=[1.0, otp.nan, 3.5], B=[0.0, 1.0, otp.nan]),
                                         expected=[1.0, -100.0, -100.0]) as data:
            data["COLUMN_BASED"] = (data["A"] + data["B"])
            data["COLUMN_BASED"] = data["COLUMN_BASED"].fillna(-100)
            data["EXPRESSION_BASED"] = (data["A"] + data["B"]).fillna(-100)

    def test_chain_the_same_method(self, session):
        with self.prepare_data_and_check(data=dict(A=[1.0, 3.14, -0.49, -0.5, 0.5, 2.71828],
                                                   R=[0, 1, 0, 0, 0, 3]),
                                         expected=[1.0, 3.1, 0.0, 0.0, 1.0, 2.718]) as data:
            data["COLUMN_BASED"] = data["A"].round(data["R"] + 1).round(data["R"])
            data["EXPRESSION_BASED"] = (data["A"] + 0.0).round(data["R"] + 1).round(data["R"])

    def test_str_accessor(self, session):
        with self.prepare_data_and_check(data=dict(A=["ABC", "BC", "C"]),
                                         expected=[1, 0, -1]) as data:
            data["COLUMN_BASED"] = data["A"].str.find("BC")
            data["EXPRESSION_BASED"] = (data["A"] + "1").str.find("BC")
        # test to_datetime
        time = "01.01.1970 09:15:30"
        format_str = "%d.%m.%Y %H:%M:%S"
        with self.prepare_data_and_check(
            data=dict(A=[time] * 2, TZ=["EST5EDT", "UTC"]),
            expected=[
                Timestamp('1970-01-01 09:15:30'),
                Timestamp('1970-01-01 04:15:30'),
            ]
        ) as data:
            data["COLUMN_BASED"] = data["A"].str.to_datetime(format=format_str, timezone=data["TZ"])
            data["EXPRESSION_BASED"] = data["A"].str.to_datetime(format=format_str, timezone=data["TZ"])
        with self.prepare_data_and_check(
            data=dict(A=[time] * 2, TZ=["Chicago", "GMT"]),
            expected=[
                Timestamp('1970-01-01 03:15:30'),
                Timestamp('1970-01-01 03:15:30'),
            ]
        ) as data:
            data["COLUMN_BASED"] = data["A"].str.to_datetime(format=format_str, timezone="Europe/London")
            data["EXPRESSION_BASED"] = data["A"].str.to_datetime(format=format_str, timezone="Europe/London")


class TestStateVar:
    @contextmanager
    def prepare_data_and_check(self, *, data, expected, state_val):
        data = otp.Ticks(data)
        data.state_vars["VAL"] = state_val
        yield data

        df = otp.run(data)
        assert all(df["COLUMN_BASED"] == df["EXPRESSION_BASED"])
        assert all(df["EXPRESSION_BASED"] == expected)

    def test_find_state_expr(self, session):
        with self.prepare_data_and_check(data=dict(A=["ABC", "BC", "C"]), expected=[1, 0, -1], state_val="B") as data:
            data["COLUMN_BASED"] = data["A"].str.find(data.state_vars["VAL"] + "C")
            data["EXPRESSION_BASED"] = (data["A"] + "1").str.find(data.state_vars["VAL"] + "C")

    def test_find_in_state_expr(self, session):
        with self.prepare_data_and_check(data=dict(A=["ABC", "BC", "C"]), expected=[0, 1, 2], state_val="ABC") as data:
            data["COLUMN_BASED"] = data.state_vars["VAL"] + "1"
            data["COLUMN_BASED"] = data["COLUMN_BASED"].str.find(data["A"])
            data["EXPRESSION_BASED"] = (data.state_vars["VAL"] + "1").str.find(data["A"])

    def test_float_expr(self, session):
        with self.prepare_data_and_check(data=dict(A=[1, 2, 3]), expected=["3.1", "3.14", "3.140"],
                                         state_val=2.04) as data:
            data["COLUMN_BASED"] = data.state_vars["VAL"] + 1.1
            data["COLUMN_BASED"] = data["COLUMN_BASED"].float.str(precision=data["A"])
            data["EXPRESSION_BASED"] = (data.state_vars["VAL"] + 1.1).float.str(precision=data["A"])


class TestMetadataIsConsistentAfterMethods:
    def test_name_on_column(self, session):
        t = otp.Tick(X=3.1)
        t["Y"] = t["X"].round()
        assert isinstance(t["Y"], _Column)
        assert t["X"].name == "X"
        assert t["Y"].name == "Y"

    def test_name_on_state_var(self, session):
        t = otp.Tick(X=3.1)
        t.state_vars["Y"] = 1.06
        t["Z"] = t.state_vars["Y"].round()
        assert isinstance(t.state_vars["Y"], _StateColumn)
        assert isinstance(t["Z"], _Column)
        assert t["X"].name == "X"
        assert t.state_vars["Y"].name == "Y"
        assert t["Z"].name == "Z"
