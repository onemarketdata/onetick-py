import itertools
import random
from datetime import datetime

import pytest

import pandas as pd
import numpy as np
from onetick.test.utils import random_string

import onetick.py as otp


class TestCommon:
    def test_declaration_and_update(self, session):
        data = otp.Ticks(dict(X=[1, 2, 3]))
        data.state_vars["S1"] = 1
        with pytest.raises(AssertionError):  # onetick support state var creating with constants only
            data.state_vars["S2"] = data.state_vars["S1"]
        with pytest.raises(AssertionError):  # onetick support state var creating with constants only
            data.state_vars["S3"] = data.state_vars["S1"] + 1
        assert data.state_vars.names == ("S1",)
        data.state_vars["S1"] += 1
        data.state_vars["S1"] += data.state_vars["S1"]
        data.state_vars["S1"] += data.state_vars["S1"] + 1
        data["S"] = data.state_vars["S1"]
        df = otp.run(data)
        assert all(df["S"] == [9, 41, 169])

    def test_assign_to_source(self):
        """ check that we can assign it to the _source """
        data = otp.Ticks(dict(x=[1, 2, 3]))
        data.state_vars["state_column"] = 0

        assert data.state_vars["state_column"].dtype is int

    def test_not_in_columns(self):
        """ check that state variable is not in the columns """
        data = otp.Ticks(dict(x=[1, 2, 3]))

        assert len(data.columns(skip_meta_fields=True)) == 1
        assert "x" in data.columns()

        data.state_vars["state_column"] = 0

        assert len(data.columns(skip_meta_fields=True)) == 1
        assert "x" in data.columns()

    def test_set_to_column(self, session):
        """ check that we could create a column base on state var """
        data = otp.Ticks(dict(x=[1, 2, 3]))
        data.state_vars["state_column"] = 0

        data.y = data.state_vars["state_column"]

        assert data.y.dtype is int

        df = otp.run(data)

        assert "y" in df.columns
        assert df["y"][0] == 0
        assert df["y"][1] == 0
        assert df["y"][2] == 0

    def test_increment(self, session):
        """ check the simple increment based on a state var"""
        data = otp.Ticks(dict(x=[1, 2, 3]))
        data.state_vars["num"] = 0

        data.state_vars["num"] += 1

        data.y = data.state_vars["num"]
        df = otp.run(data)

        assert df["y"][0] == 1
        assert df["y"][1] == 2
        assert df["y"][2] == 3

    def test_column_cum_sum(self, session):
        """ check cum sum calculation based on a column and state var"""
        data = otp.Ticks(dict(x=[1, 2, 3]))
        data.state_vars["state_var"] = 0

        data.state_vars["state_var"] += data["x"]
        data.y = data.state_vars["state_var"]

        df = otp.run(data)

        assert df["y"][0] == 1
        assert df["y"][1] == 1 + 2
        assert df["y"][2] == 1 + 2 + 3

    def test_two_state_variables(self, session):
        """ check that several state variables work simultaneously """
        data = otp.Ticks(dict(x=[1, 2, 3]))
        data.state_vars["var1"] = 2
        data.state_vars["var2"] = 1

        data.col1 = data.state_vars["var1"]
        data.col2 = data.state_vars["var2"]

        data.z = data.state_vars["var1"] + data.state_vars["var2"]
        data.state_vars["var1"] += data.state_vars["var2"]

        df = otp.run(data)

        assert df["z"][0] == 3
        assert df["z"][1] == 4
        assert df["z"][2] == 5

    def test_type_convert(self, session):
        """ check that type conversion works fine"""
        data = otp.Ticks(dict(x=[1, 2, 3]))
        data.state_vars["var"] = 0.0

        data.state_vars["var"] = 1.2 + data.x  # NOSONAR

        data.str_x = data.state_vars["var"].astype(str)
        data.float_x = data.state_vars["var"].astype(float)
        data.int_x = data.state_vars["var"].astype(int)

        df = otp.run(data)

        assert "str_x" in df.columns
        assert pd.api.types.is_string_dtype(df["str_x"])
        assert df["str_x"][0] == "2.2"
        assert df["str_x"][1] == "3.2"
        assert df["str_x"][2] == "4.2"

        assert "float_x" in df.columns
        assert pd.api.types.is_float_dtype(df["float_x"])
        assert df["float_x"][0] == 2.2
        assert df["float_x"][1] == 3.2
        assert df["float_x"][2] == 4.2

        assert "int_x" in df.columns
        assert pd.api.types.is_numeric_dtype(df["int_x"])
        assert df["int_x"][0] == 2
        assert df["int_x"][1] == 3
        assert df["int_x"][2] == 4

    def test_get_all_state_variables(self):
        """ check the _Source method for getting all state variables """
        data = otp.Ticks(dict(a=[1]))

        assert len(data.state_vars.items) == 0
        assert len(data.state_vars.names) == 0

        data.state_vars["var1"], data.state_vars["var2"], data.state_vars["var3"] = 0, 0.0, ""

        assert len(data.state_vars.items) == 3

        names = ["var1", "var2", "var3"]
        types = [int, float, str]
        for expected_name, expected_type, (actual_name, actual_column) in zip(names, types, data.state_vars.items):
            assert expected_name == actual_name
            assert actual_column.name == expected_name
            assert actual_column.dtype is expected_type

    def test_apply_in_result(self, session):
        """ check that apply assigning works for state variables """
        data = otp.Ticks(dict(a=[0, 2, -1, 1]))

        data.state_vars["var"] = -100
        data.state_vars["var"] = data.apply(lambda row: row.a if row.a > 0 else data.state_vars["var"])
        data.z = data.state_vars["var"]

        df = otp.run(data)

        assert df["z"][0] == -100
        assert df["z"][1] == 2
        assert df["z"][2] == 2
        assert df["z"][3] == 1

    def test_apply_str_with_expr(self, session):
        """ check that apply assigning works for state variables """
        data = otp.Ticks(dict(A=["A", "AB", "ABC"]))

        data.state_vars["var"] = otp.string[128]()
        data.state_vars["var"] = data.apply(lambda row: (data.state_vars["var"] + ", " + row["A"]
                                                         if data.state_vars["var"].str.len() > 0 else row["A"]))
        data["Z"] = data.state_vars["var"]

        df = otp.run(data)
        assert all(df["A"] == ["A", "AB", "ABC"])
        assert all(df["Z"] == ["A", "A, AB", "A, AB, ABC"])

    def test_in_update_where(self, session):
        # updating of the state variable will be implemented as column update for readability
        data = otp.Ticks(dict(X=[-15, 1, 2, -3, 0, 5, 10]))
        data["RESULT"] = 0
        data.state_vars["M"] = 0
        data = data.update({data["RESULT"]: data["X"]}, where=(data["X"] > data.state_vars["M"]))
        df = otp.run(data)
        assert all(df["RESULT"] == [0, 1, 2, 0, 0, 5, 10])

    def test_max_apply_state_var_in_condition(self, session):
        data = otp.Ticks(dict(X=[0, 1, -5, 10, 7, 9, 20]))
        data.state_vars["M"] = 0
        data.state_vars["M"] = data.state_vars["M"].apply(
            lambda x: data["X"] if data["X"] > data.state_vars["M"] else data.state_vars["M"]
        )
        data["M"] = data.state_vars["M"]
        df = otp.run(data)
        assert all(df["M"] == [0, 1, 1, 10, 10, 10, 20])

    def test_update(self, session):
        data = otp.Tick(A=1)
        data.state_vars['X'] = 1
        data['X'] = data.state_vars['X']
        df = otp.run(data)
        assert all(df['X'] == [1])
        data.state_vars['X'] = 12345
        data['X'] = data.state_vars['X']
        df = otp.run(data)
        assert all(df['X'] == [12345])

    def test_update_str(self, session):
        data = otp.Tick(A=1)
        data.state_vars['X'] = "some val"
        data['X'] = data.state_vars['X']
        df = otp.run(data)
        assert all(df['X'] == ['some val'])
        data.state_vars['X'] = 'other val'
        data['X'] = data.state_vars['X']
        df = otp.run(data)
        assert all(df['X'] == ['other val'])

    def test_indexing(self, session):
        t = otp.Tick(A=1)
        t.state_vars['X'] = 0
        with pytest.raises(IndexError, match='Indexing is not supported for state variables'):
            t.state_vars['X'][-1]


class TestInt:
    def test_default(self, session):
        """ check that different default values work """

        val1, val2, val3 = (random.randint(-50, 50) for _ in range(3))

        data = otp.Ticks(dict(a=[1]))
        data.state_vars["var1"], data.state_vars["var2"], data.state_vars["var3"] = val1, val2, val3

        data.x = data.state_vars["var1"]
        data.y = data.state_vars["var2"]
        data.z = data.state_vars["var3"]

        df = otp.run(data)

        assert pd.api.types.is_numeric_dtype(df["x"])
        assert df["x"][0] == val1

        assert pd.api.types.is_numeric_dtype(df["y"])
        assert df["y"][0] == val2

        assert pd.api.types.is_numeric_dtype(df["z"])
        assert df["z"][0] == val3

    def test_add_from_float(self, session):
        """
        check that float column values convert into long through
        a long state var when add a new column
        """
        data = otp.Ticks(dict(x=[1.3, -2.5, 3.9, 0.1]))

        data.state_vars["var"] = 0

        data.state_vars["var"] = data["x"]  # NOSONAR
        data.z = data.state_vars["var"]

        assert data.z.dtype is int

        df = otp.run(data)

        assert df["z"][0] == 1
        assert df["z"][1] == -2
        assert df["z"][2] == 3
        assert df["z"][3] == 0

    def test_update_from_float(self, session):
        """
        check that float column values convert into long through
        a long state var when update an existing column
        """
        data = otp.Ticks(dict(x=[1.3, -2.5, 3.9, 0.1], y=[0, 0, 0, 0]))

        data.state_vars["var"] = 0

        data.state_vars["var"] = data["x"]  # NOSONAR
        data.y = data.state_vars["var"]

        assert data.y.dtype is int

        df = otp.run(data)

        assert df["y"][0] == 1
        assert df["y"][1] == -2
        assert df["y"][2] == 3
        assert df["y"][3] == 0


class TestFloat:
    def test_default(self, session):
        """ check different float values """

        val1, val2, val3 = (random.random() - 0.5) * 50, (random.random() - 0.5) * 50, (random.random() - 0.5) * 50

        data = otp.Ticks(dict(a=[1]))
        data.state_vars["var1"], data.state_vars["var2"], data.state_vars["var3"] = val1, val2, val3

        data.x = data.state_vars["var1"]
        data.y = data.state_vars["var2"]
        data.z = data.state_vars["var3"]

        df = otp.run(data)

        assert pd.api.types.is_float_dtype(df["x"])
        assert df["x"][0] == val1

        assert pd.api.types.is_float_dtype(df["y"])
        assert df["y"][0] == val2

        assert pd.api.types.is_float_dtype(df["z"])
        assert df["z"][0] == val3

    def test_from_int(self, session):
        """
        check that int column values converts into float through
        a float state variable
        """
        data = otp.Ticks(dict(x=[5, -4, 0, 1]))

        data.state_vars["var"] = 0.0

        data.state_vars["var"] = data["x"]  # NOSONAR
        data.z = data.state_vars["var"]

        assert data.z.dtype is float

        df = otp.run(data)

        assert pd.api.types.is_float_dtype(df["z"])

    def test_nan(self, session):
        """ check that nan values works with float state variables """
        data = otp.Ticks(dict(x=[1, 2, 3]))

        data.state_vars["var"] = otp.nan

        data.z = data.state_vars["var"]

        assert data.state_vars["var"].dtype is float
        assert data.z.dtype is float

        df = otp.run(data)

        assert pd.api.types.is_float_dtype(df["z"])
        assert np.isnan(df["z"][0])
        assert np.isnan(df["z"][1])
        assert np.isnan(df["z"][2])


class TestString:
    def test_default(self, session):
        """ check different values """

        val1, val2, val3 = [""] + [random_string(max_len=64)] + ["x" * 64]

        data = otp.Ticks(dict(a=[1]))
        data.state_vars["var1"], data.state_vars["var2"], data.state_vars["var3"] = val1, val2, val3

        data.x = data.state_vars["var1"]
        data.y = data.state_vars["var2"]
        data.z = data.state_vars["var3"]

        df = otp.run(data)

        assert pd.api.types.is_string_dtype(df["x"])
        assert df["x"][0] == val1

        assert pd.api.types.is_string_dtype(df["y"])
        assert df["y"][0] == val2

        assert pd.api.types.is_string_dtype(df["z"])
        assert df["z"][0] == val3

    def test_long_string_1(self, session):
        """ check long string, longer than default 64 """
        data = otp.Ticks(dict(a=[1]))

        value = "x" * 88
        data.state_vars["var"] = value

        assert data.state_vars["var"].dtype is otp.string[88]

        data.z = data.state_vars["var"]

        assert data.z.dtype is otp.string[88]

        df = otp.run(data)

        assert df["z"][0] == value

    def test_long_string_2(self, session):
        """ check long string with concatenation """
        data = otp.Ticks(dict(s=["a" * 10, "b" * 40, "c" * 32]))

        data.state_vars["var"] = otp.string[100]()

        assert data.state_vars["var"].dtype is otp.string[100]

        data.state_vars["var"] += data["s"]
        data.z = data.state_vars["var"]

        assert data["z"].dtype is otp.string[100]

        df = otp.run(data)

        assert df["z"][0] == "a" * 10
        assert df["z"][1] == "a" * 10 + "b" * 40
        assert df["z"][2] == "a" * 10 + "b" * 40 + "c" * 32

    def test_short_string(self, session):
        """ check short strings """
        data = otp.Ticks(dict(s=["a" * 10, "b" * 40, "c" * 140]))

        data.state_vars["var"] = otp.string[3]("x" * 10)

        assert data.state_vars["var"].dtype is otp.string[3]

        data.a = data.state_vars["var"]
        data.state_vars["var"] = data.s

        assert data.a.dtype is otp.string[3]

        df = otp.run(data)

        assert df["a"][0] == "x" * 3
        assert df["a"][1] == "a" * 3
        assert df["a"][2] == "b" * 3


class TestBranching:
    def test_simple_copy(self, session):
        data = otp.Ticks(dict(X=[1, 2]))
        data.state_vars["state_column"] = 1

        data2 = data.copy()
        data2["Y"] = data2.state_vars["state_column"] + 1
        data2["Z"] = data2["X"] + data2.state_vars["state_column"]

        df1 = otp.run(data)
        df2 = otp.run(data2)
        assert set(df1.columns) == {"X", "Time"}
        assert all(df1["X"] == [1, 2])

        assert set(df2.columns) == {"X", "Y", "Z", "Time"}
        assert all(df2["X"] == [1, 2])
        assert all(df2["Y"] == [2, 2])
        assert all(df2["Z"] == [2, 3])

    def test_where_clause(self, session):
        data = otp.Ticks(dict(X=[1, 2, 3]))
        data.state_vars["STATE_T"] = 2

        data, _ = data[(data["X"] > 1)]
        data["Y"] = data.state_vars["STATE_T"] * data["X"]

        df = otp.run(data)
        assert all(df["Y"] == [4, 6])

    def test_state_var_in_where_clause_condition(self, session):
        data = otp.Ticks(dict(X=[1, 2, 3]))
        data.state_vars["STATE_T"] = 2

        data, _ = data[(data["X"] > data.state_vars["STATE_T"])]

        df = otp.run(data)
        assert all(df["X"] == [3])

    def test_merge(self, session):
        data1 = otp.Ticks(dict(X=[1, 2]))
        data1.state_vars["STATE1"] = 1

        data2 = otp.Ticks(dict(X=[3, 4], offset=[3, 4]))
        data2.state_vars["STATE2"] = 2

        data = data1 + data2
        data["Y"] = data.state_vars["STATE1"] + data.state_vars["STATE2"]
        df = otp.run(data)

        assert all(df["X"] == [1, 2, 3, 4])
        assert all(df["Y"] == [3, 3, 3, 3])

    def test_apply(self, session):
        data = otp.Ticks(dict(X=[1, 2, 3, 4, 5]))
        data.state_vars["STATE"] = 0

        data.state_vars["STATE"] = data.apply(  # NOSONAR
            lambda row: row["X"] if row.state_vars["STATE"] == 0 and row["X"] > 2 else row.state_vars["STATE"]
        )

        data = data.last()
        data["FIRST"] = data.state_vars["STATE"]
        df = otp.run(data)
        assert all(df["FIRST"] == [3])

    def test_join(self, session):
        data1 = otp.Ticks(dict(X=[1, 2]))
        data2 = otp.Ticks(dict(Y=[3, 4]))
        data2.state_vars["STATE2"] = 2
        result1 = otp.join(data1, data2, (data1["X"] == data2.state_vars["STATE2"]) & (data2["Y"] > 0))
        df = otp.run(result1)
        assert all(df["X"] == [1, 2, 2])
        assert all(df["Y"] == [0, 3, 4])

    def test_as_param_for_join_with_query(self, session):
        def func_symbol(symbol):
            d = otp.Ticks(dict(type=["six"]))
            d["type"] = symbol.start + d["type"] + symbol.finish
            return d

        def func_params(cond, start, finish):
            d = otp.Ticks(dict(type=["six"]))
            d = d.update({'type': "three"}, where=(cond == 3))
            d["type"] = start + d["type"] + finish
            return d

        data = otp.Ticks(dict(a=[1, 2], b=[2, 4]))
        data.state_vars["pre"] = "!"
        data.state_vars["post"] = "."
        res1 = data.join_with_query(
            func_symbol,
            how="inner",
            symbol=(data.a + data.b, dict(start=data.state_vars["pre"], finish=data.state_vars["post"])),
        )
        res2 = data.join_with_query(
            func_params,
            how="inner",
            params=dict(cond=data.a + data.b, start=data.state_vars["pre"], finish=data.state_vars["post"]),
        )
        df = otp.run(res1)
        assert all(df["type"] == ["!six.", "!six."])
        df = otp.run(res2)
        assert all(df["type"] == ["!three.", "!six."])

    def test_join_by_time(self, session):
        data1 = otp.Ticks([["X", "offset"], [3, 3], [4, 4], [5, 5]])
        data1.state_vars["state1"] = 1

        data2 = otp.Ticks([["Y", "offset"], [0.7, 4], [0.6, 6], [0.9, 7], [0.1, 9]])
        data2.state_vars["state2"] = 2

        data = otp.join_by_time([data1, data2], how="outer")
        data["X1"] = data["X"] + data.state_vars["state1"]
        data["X2"] = data["X"] + data.state_vars["state2"]
        df = otp.run(data)
        assert all(df["X1"] == [4, 5, 6])
        assert all(df["X2"] == [5, 6, 7])


class TestTime:
    def test_assign_with_datetime(self, session):
        xs = [-1.14, 3.89, -3.17]
        data = otp.Ticks(dict(X=xs))
        expected = otp.config['default_start_time'] + otp.Hour(1)
        data.state_vars["S"] = otp.datetime(expected)
        data["S"] = data.state_vars["S"]
        df = otp.run(data, timezone="GMT")
        assert all(df["X"] == xs)
        assert all(df["S"] == [expected] * 3)

    def test_assign_with_date(self, session):
        xs = [-1.14, 3.89, -3.17]
        data = otp.Ticks(dict(X=xs))
        expected = otp.date(2011, 1, 1)
        data.state_vars["S"] = expected
        data["S"] = data.state_vars["S"]
        df = otp.run(data, timezone="GMT")
        assert all(df["X"] == xs)
        assert all(df["S"] == [expected] * 3)

    def test_op_with_datetime(self, session):
        xs = [1, 0, 3, -17]
        diffs = itertools.accumulate(xs)
        data = otp.Ticks(dict(X=xs))
        expected = otp.config['default_start_time'] + otp.Hour(1)
        data.state_vars["S"] = otp.datetime(expected)
        data.state_vars["S"] += otp.Second(data["X"])
        data["S"] = data.state_vars["S"]
        df = otp.run(data, timezone="GMT")
        assert all(df["S"] == [expected + otp.Second(d) for d in diffs])

    def test_msectime(self, session):
        """ validate that there is impossible to set msectime type value """
        data = otp.Empty()
        with pytest.raises(TypeError):
            data.state_vars["state"] = otp.msectime()

    def test_nsectime(self, session):
        """ validate that there is impossible to set nsectime type value """
        data = otp.Tick(X=1)

        data.state_vars['state'] = otp.nsectime(0)

        data['Y'] = data.state_vars['state']
        assert data.schema['Y'] is otp.nsectime

        res = otp.run(data, timezone='GMT')

        assert res['Y'][0] == otp.dt(1970, 1, 1)

    def test_time_from_state(self, session):
        # Assignment to const state variable
        data = otp.Ticks({"x": [1, 0], "offset": [0, 1]})

        data.new_time = 0
        data.Time = data.new_time

        df = otp.run(data)
        assert len(df) == 2
        assert set([df.x[0], df.x[1]]) == set([0, 1])

    def test_time_from_state_with_read_data(self, session):
        # Assignment to variable-per-tick state variable
        data = otp.Ticks({"x": [2, -1], "y": [1, 0], "offset": [0, 1]})
        start_ts = datetime(2018, 1, 12)
        finish_ts = datetime(2018, 1, 13)
        test_ts = datetime(2018, 1, 12, 13, 12, 1).timestamp() * 1000

        data.new_time = test_ts
        data.new_time += data.x
        data.Time = data.new_time

        res = otp.run(data, timezone="GMT", start=start_ts, end=finish_ts)
        assert len(res) == 2
        assert res.y[0] == 0 and res.y[1] == 1


class TestAPI:

    def test_state_drop(self, session):
        data = otp.Ticks(dict(X=[1, 2]))
        data.drop("s.*", inplace=True)
        _ = data.state_vars
        with pytest.raises(AttributeError):
            data.drop("state", inplace=True)
        _ = data.state_vars

    def test_state_columns(self, session):
        """ check state state_columns method return expected result before and after join"""
        data = otp.Ticks(dict(X=[1, 2]))
        assert not data.state_vars.items
        data.state_vars["STATE1"] = 1
        assert "STATE1" in data.state_vars.names

        data = otp.join_by_time([data, otp.Empty()])
        assert "STATE1" in data.state_vars.names
        data.state_vars["STATE2"] = "a"
        actual = data.state_vars.names
        assert "STATE2" in actual
        assert "STATE1" in actual
        assert len(actual) == 2
        otp.run(data)

    def test_read_only_properties(self):
        data = otp.Ticks(dict(X=[1, 2]))
        data.state_vars["STATE1"] = 1
        with pytest.raises(AttributeError, match=r"(can't set attribute)|(object has no setter)"):
            data.state_vars.names = "sds"
        with pytest.raises(AttributeError, match=r"(can't set attribute)|(object has no setter)"):
            data.state_vars.items = ("sds", int)
        with pytest.raises(AttributeError, match=r"(can't delete attribute)|(object has no deleter)"):
            del data.state_vars.names
        with pytest.raises(AttributeError, match=r"(can't delete attribute)|(object has no deleter)"):
            del data.state_vars.items


class TestScope:
    def test_default_scope(self, session):
        data = otp.Ticks(dict(X=[1, 2]))
        data.state_vars["S"] = otp.state.var(1)
        assert data.state_vars["S"].scope == "QUERY"
        data = otp.merge([data, otp.Tick(X=3)])
        data.state_vars["S"] = 2
        df = otp.run(data)
        assert list(df['X']) == [1, 3, 2]

    def test_branch_scope(self, session):
        data = otp.Ticks(dict(X=[1, 2]))
        data.state_vars["S"] = otp.state.var(1, "branch")
        assert data.state_vars["S"].scope == "BRANCH"
        data = otp.merge([data, otp.Tick(X=3)])
        # branch ended, will raise an error
        data.state_vars["S"] = 2
        with pytest.raises(Exception, match='State variable STATE::S is not declared'):
            otp.run(data)

    @pytest.mark.parametrize("scope", [1, "abc", None])
    def test_wrong_score(self, session, scope):
        data = otp.Ticks(dict(X=[1, 2]))
        with pytest.raises(ValueError):
            data.state_vars["S"] = otp.state.var(1, scope)


def test_otp_int_with_state_var(session):
    # PY-1449
    t = otp.Tick(SIZE_INT=10, SIZE_OTP_INT=otp.int(100))
    t.state_vars['SUMSIZE'] = 0
    t.state_vars['SUMSIZE'] += t['SIZE_OTP_INT']
    t.state_vars['SUMSIZE'] += t['SIZE_INT']
    t['SUMSIZE'] = t.state_vars['SUMSIZE']
    df = otp.run(t)
    assert df['SUMSIZE'][0] == 110
