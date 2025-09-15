# pylama:ignore=E731

import os
from typing import Any, Dict

import numpy as np
import pandas as pd
import pytest

import onetick.py as otp
from onetick.py.otq import otq


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    pass


class TestSimple:
    def test_one_column(self):
        """ check the trivial case """
        data = otp.Ticks({"x": [1, 2, 3]})

        data.y = data.apply(lambda row: row.x)

        assert hasattr(data, "y") and data.y.dtype is data.x.dtype

        df = otp.run(data)

        assert len(df) == 3
        assert df.x.dtype is df.y.dtype

        for inx in range(3):
            assert df.x[inx] == df.y[inx]

    def test_two_columns(self):
        """ check that several columns works fine """
        data = otp.Ticks(dict(x=[1, 2, 3], y=[0.1, 0.2, 0.3]))

        data.z = data.apply(lambda row: row.x - row.y)

        assert data.z.dtype is float

        df = otp.run(data)

        for inx in range(len(df)):
            assert df["z"][inx] == df["x"][inx] - df["y"][inx]

    def test_consts(self):
        """ check passing constants """
        data = otp.Ticks(dict(x=[1]))

        data["int_c"] = data.apply(lambda row: 3)
        assert data["int_c"].dtype is int
        data["float_c"] = data.apply(lambda row: -1.1)
        assert data["float_c"].dtype is float
        data["str_c"] = data.apply(lambda row: "abc")
        assert data["str_c"].dtype is str
        data["nsec_c"] = data.apply(lambda row: otp.nsectime(0))
        assert data["nsec_c"].dtype is otp.nsectime
        data["msec_c"] = data.apply(lambda row: otp.msectime(0))
        assert data["msec_c"].dtype is otp.msectime

        df = otp.run(data)

        assert df["int_c"][0] == 3
        assert df["float_c"][0] == -1.1
        assert df["str_c"][0] == "abc"

    def test_const_without_param(self):
        """ check that trivial case doesn't require passing parameter """
        data = otp.Ticks(dict(x=[1]))

        data["int_c"] = data.apply(lambda: 3)

        df = otp.run(data)

        assert df["int_c"][0] == 3

    def test_long_string_column(self):
        """
        check that long string type is propagated type information correctly
        """
        data = otp.Ticks(dict(x=["a" * 101]))
        assert data.x.dtype is otp.string[101]

        data.z = data.apply(lambda row: row.x)
        assert data.z.dtype is otp.string[101]

        df = otp.run(data)
        assert df["z"][0] == "a" * 101

    def test_long_string_const(self):
        """
        check that constant long string type is processed properly
        """
        data = otp.Ticks(dict(x=[1, 2, 3]))

        data.z = data.apply(lambda row: "a" * 102)
        assert data.z.dtype is otp.string[102]

        df = otp.run(data)
        assert df["z"][0] == "a" * 102

    def test_long_default(self):
        """ check the corner cases for long strings """
        data = otp.Ticks(dict(x=[1, 2]))

        data.z = data.apply(lambda row: "z" * otp.string.DEFAULT_LENGTH)
        data.u = data.apply(lambda row: "u" * (otp.string.DEFAULT_LENGTH + 1))
        assert data.z.dtype is str
        assert data.u.dtype is otp.string[otp.string.DEFAULT_LENGTH + 1]

        df = otp.run(data)
        assert df["z"][0] == "z" * otp.string.DEFAULT_LENGTH
        assert df["u"][0] == "u" * (otp.string.DEFAULT_LENGTH + 1)

    def test_bool(self):
        """ test boolean operations """
        data = otp.Ticks(dict(x=[1, 2, 3, 2, 4], y=[1, 0, 1, 2, 3]))

        data.z = data.apply(lambda row: row.x == row.y)
        assert data["z"].dtype is float

        df = otp.run(data)

        for inx, value in enumerate([1, 0, 0, 1, 0]):
            assert df["z"][inx] == value

    def test_nsectime(self):
        """ test operation with time """
        data = otp.Ticks(dict(x=[1, 2, 3], y=[1, 0, 1]))

        data.my_t = data._START_TIME
        data.my_t2 = data.apply(lambda row: row.Time if row.x * row.y > 0 else row.my_t)
        assert data.my_t2.dtype is otp.nsectime

        df = otp.run(data)

        for inx in range(len(df)):
            assert (df["my_t2"][inx] == df["Time"][inx] and df["x"][inx] * df["y"][inx] > 0) or (
                df["my_t2"][inx] == df["Time"][0] and df["x"][inx] * df["y"][inx] == 0
            )

    def test_column_condition_int(self):
        data = otp.Ticks(dict(x=[1, 2, 3, 4], y=[1, 0, 1, -1]))

        data.z = data.apply(lambda row: row.x if row.y else -1)
        assert data.z.dtype is int

        df = otp.run(data)
        assert df["z"].tolist() == [1, -1, 3, 4]

    def test_column_condition_float(self):
        data = otp.Ticks(dict(x=[1, 2, 3, 4], y=[0.1, 0, -3.1, 4]))

        data.z = data.apply(lambda row: row.x if row.y else -1)
        assert data.z.dtype is int

        df = otp.run(data)
        assert df["z"].tolist() == [1, -1, 3, 4]

    def test_apply_str_bool(self, session):
        data = otp.Ticks(dict(a=["a", "", "b"]))
        data["b"] = data["a"].apply(lambda x: x if x else "missed")
        df = otp.run(data)
        assert all(df["b"] == ["a", "missed", "b"])


class TestLambdas:
    @pytest.mark.parametrize("n", [8, 10, 12])
    def test_linear_separation(self, n):
        """ check linear separation """
        data = otp.Ticks(dict(x=np.random.randint(-5, 5, n).tolist()))

        data.z = data.apply(lambda row: 1 if row.x > 0 else -1)
        assert data.z.dtype is int

        df = otp.run(data)

        assert len(df) == n

        for inx in range(len(df)):
            assert (df["z"][inx] == 1 and df["x"][inx] > 0) or (df["z"][inx] == -1 and df["x"][inx] <= 0)

    @pytest.mark.parametrize("n", [8, 10, 12])
    def test_stripe(self, n):
        """ check stripe condition """
        data = otp.Ticks(dict(x=np.random.randint(-5, 5, n).tolist()))

        data.z = data.apply(lambda row: "1" if -1 <= row.x <= 2 else "0")
        assert data.z.dtype is str

        df = otp.run(data)

        for inx in range(len(df)):
            assert (df["z"][inx] == "1" and -1 <= df["x"][inx] <= 2) or (
                df["z"][inx] == "0" and (df["x"][inx] > 2 or df["x"][inx] < -1)
            )

    @pytest.mark.parametrize("n", [8, 10, 12])
    def test_triangle(self, n):
        data = dict(x=np.random.randint(0, 10, n).tolist(), y=np.random.randint(0, 10, n).tolist())

        lam = (
            lambda row: 1
            if (row["y"] - 2 * row["x"] <= 0) and (row["y"] - 0.5 * row["x"] >= 0) and (row["y"] + row["x"] - 15 <= 0)
            else 0
        )

        ticks = otp.Ticks(data)

        ticks.z = ticks.apply(lam)
        ot_df = otp.run(ticks)

        pd_df = pd.DataFrame(data)
        pd_df["z"] = pd_df.apply(lam, axis=1)

        for inx in range(len(pd_df)):
            assert ot_df["z"][inx] == pd_df["z"][inx]

    @pytest.mark.parametrize("n", [8, 10, 12])
    def test_areas(self, n):
        data = dict(x=np.random.randint(-5, 5, n).tolist())

        lam = lambda row: 1 if row["x"] < -3 else 2 if row["x"] < 0 else 3 if row["x"] < 2 else 4  # noqa # NOSONAR

        ticks = otp.Ticks(data)
        ticks.z = ticks.apply(lam)
        ot_df = otp.run(ticks)

        pd_df = pd.DataFrame(data)
        pd_df["z"] = pd_df.apply(lam, axis=1)

        for inx in range(len(pd_df)):
            assert ot_df["z"][inx] == pd_df["z"][inx]

    def test_one_return_another(self):
        """ condition on one column but return another """
        data = otp.Ticks(dict(x=[1, 2, 4, 3, 2, 1], y=[1, -1, 1, -1, 1, 1], u=[9, 8, 7, 6, 5, 4]))

        data.z = data.apply(lambda row: row.y if (row.x == 2 or row.x == 3) else row.u)

        assert hasattr(data, "z") and data.z.dtype is int

        df = otp.run(data)

        assert isinstance(df.z[0], np.integer)

        for inx, value in enumerate([9, -1, 7, -1, 1, 4]):
            assert df["z"][inx] == value

    def test_not(self):
        data = otp.Ticks({
            'A': [1, 2, 3],
            'B': [1, 0, 1]
        })
        data['X'] = data.apply(lambda tick: -1 if not tick['B'] else tick['A'])
        data['Y'] = data.apply(lambda tick: -1 if ~tick['B'] else tick['A'])
        data['Z'] = data.apply(
            lambda tick: -1 if not (tick['B'] == 1) or ~(tick['B'] == 0) else tick['A']  # NOSONAR
        )
        df = otp.run(data)
        assert list(df['X']) == [1, -1, 3]
        assert list(df['Y']) == [1, -1, 3]
        assert list(df['Z']) == [-1, -1, -1]

    def test_f_str(self):
        data = otp.Ticks({'A': [1, 0]})
        data['X'] = data.apply(
            lambda tick: f"{tick['A']} {tick['A'] + 1} {3} {'4'}" if tick['A'] == 1 else f'{1 - 1}'  # NOSONAR
        )
        df = otp.run(data)
        assert list(df['X']) == ['1 2 3 4', '0']

    def test_f_str_in_tick(self):
        data = otp.Ticks({
            'A': [1, 0],
            'AA': [0, 2]
        })
        a = 'A'
        data['X'] = data.apply(
            lambda tick: tick[f'{a}'] if tick[a] != 0 else tick[f'{a}{a}']
        )
        df = otp.run(data)
        assert list(df['X']) == [1, 2]

    def test_return_or_and(self):
        data = otp.Ticks({'A': [1, 0]})
        data['B'] = data.apply(lambda tick: tick['A'] or -1)
        data['C'] = data.apply(lambda tick: tick['A'] and -1)
        data['D'] = data.apply(lambda tick: tick['A'] or False or 42)  # NOSONAR
        data['E'] = data.apply(lambda tick: tick['A'] and -1 or 42)
        data['F'] = data.apply(lambda tick: tick['A'] == 1 or tick['A'] == -1 or tick['A'] == 0)
        df = otp.run(data)
        assert list(df['B']) == [1, -1]
        assert list(df['C']) == [-1, 0]
        assert list(df['D']) == [1, 42]
        assert list(df['E']) == [-1, 42]
        assert list(df['F']) == [1, 1]

    def test_in_not_in(self):
        data = otp.Ticks({'A': [0, 1, 2]})
        numbers = [0, 1]
        # test list
        data['X'] = data.apply(lambda tick: 1 if tick['A'] in [1, 2] else 0)
        # test tuple
        data['Y'] = data.apply(lambda tick: 1 if tick['A'] not in [1, 2] else 0)
        # test variable
        data['Z'] = data.apply(lambda tick: 1 if tick['A'] in numbers else 0)
        df = otp.run(data)
        assert list(df['X']) == [0, 1, 1]
        assert list(df['Y']) == [1, 0, 0]
        assert list(df['Z']) == [1, 1, 0]

    def test_in_range(self):
        t = otp.Ticks(A=[1, 2, 3])

        def fun(tick):
            if tick['A'] in range(10):
                return 2
            return 3

        expr = t.apply(fun)
        expr = str(expr)
        assert expr == 'CASE(((A) >= (0)) AND ((A) < (10)), 1, 2, 3)'

    def test_if_else_basic(self):
        data = otp.Ticks(X=[1, 2, 3])
        data['Y'] = data.if_else(data['X'] > 2, 1, 0)
        df = otp.run(data)
        assert df['Y'].array == [0, 0, 1]

    def test_if_else_with_lambda_expr(self):
        data = otp.Ticks(X=[1, 2, 3])
        data['Y'] = data.if_else(data['X'] > 2, data['X'] * 2, 0)
        df = otp.run(data)
        assert df['Y'].array == [0, 0, 6]

    def test_wrong_column_name(self):
        data = otp.Tick(A=1, B=2)
        with pytest.raises(Exception) as excinfo:
            data['C'] = data.apply(
                lambda r: r['A']
                if r['A'] == otp.nan
                else (r['B'] if r['B'] == otp.nan else otp.math.max(r['A'], r['NO_SUCH_COLUMN']))  # NOSONAR
            )
        assert "Column 'NO_SUCH_COLUMN' referenced before assignment" in str(excinfo.value.__cause__)


class TestFunction:
    def test_if_else(self):
        """
        Check if-else construction like:
        if <cond>:
            return <something>
        else:
            return <something>
        """

        def func(row):
            if row.x > 2:
                return 1
            else:
                return 0

        data = otp.Ticks(dict(x=[1, 2, 3]))
        data.z = data.apply(func)
        assert data["z"].dtype is int

        df = otp.run(data)

        for inx, value in enumerate([0, 0, 1]):
            assert df["z"][inx] == value

    def test_if_with_default(self):
        """
        check if construction like:
        if <cond>:
            return <something>

        return <something>
        """

        def func(row):
            if 3 >= row.x > 1:
                return 1

            return -1

        data = otp.Ticks(dict(x=[1, 2, 3, 4, 5]))
        data.z = data.apply(func)
        assert data["z"].dtype is int

        df = otp.run(data)

        for inx, value in enumerate([-1, 1, 1, -1, -1]):
            assert df["z"][inx] == value

    def test_if_without_default(self):
        """
        Check if construction like:
        if <cond>:
            return <something>

        Here is no default branch, but expected
        default value for that case.
        """

        def func(row):
            if 3 >= row.x > 1:
                return 1

        data = otp.Ticks(dict(x=[1, 2, 3, 4, 5]))
        data.z = data.apply(func)
        assert data["z"].dtype is int

        df = otp.run(data)

        for inx, value in enumerate([0, 1, 1, 0, 0]):
            assert df["z"][inx] == value

    def test_elif_else(self):
        """
        Check if-elif-else construction:
        if <cond>:
            return <something>
        elif <cond>:
            return <something>
        else:
            return <something>
        """

        def func(row):
            if row.x > 4:
                return 2
            elif row.x > 2:
                return 1
            else:
                return 0

        data = otp.Ticks(dict(x=[1, 2, 3, 4, 5]))
        data.z = data.apply(func)
        assert data["z"].dtype is int

        df = otp.run(data)

        for inx, value in enumerate([0, 0, 1, 1, 2]):
            assert df["z"][inx] == value

    def test_2elif_with_default(self):
        """ check if-elif-elif with default construction """

        def func(row):
            if row.x > 4:
                return 2
            elif row.x > 2:
                return 1
            elif row.x > 0:
                return 0

            return -1

        data = otp.Ticks(dict(x=[-1, 0, 1, 2, 3, 4, 5]))
        data.z = data.apply(func)

        df = otp.run(data)

        for inx, value in enumerate([-1, -1, 0, 0, 1, 1, 2]):
            assert df["z"][inx] == value

    def test_two_if(self):
        """ check two if constructions """

        def func(row):
            if row.x > 4:
                return 2

            if row.x < 2:
                return 1

        data = otp.Ticks(dict(x=[0, 1, 2, 3, 4, 5]))
        data.z = data.apply(func)

        df = otp.run(data)

        for inx, value in enumerate([1, 1, 0, 0, 0, 2]):
            assert df["z"][inx] == value

    def test_inner_functions(self):
        """ check inner if-else constructions """
        d = {"x": np.arange(0.5, 5, 0.2).tolist()}

        def func(row):
            if row["x"] > 1:
                if 1.5 > row["x"] > 1.2:
                    return 1.1
                elif 1.5 > row["x"] > 1.6:
                    return 1.2
            if 2.5 > row["x"] > 2:
                return 2.1
            if 4 > row["x"] > 3:
                if row["x"] > 3.5:
                    return 3.1
                else:
                    return 3.2

            return 0

        data = otp.Ticks(d)
        data["z"] = data.apply(func)
        ot_df = otp.run(data)

        pd_df = pd.DataFrame(d)
        pd_df["z"] = pd_df.apply(func, axis=1)

        for inx in range(len(ot_df)):
            assert ot_df["x"][inx] == pd_df["x"][inx]

    def test_long_strings(self):
        """ check that resulting type has maximum length """

        def func(row):
            if row["x"] > 1:
                return "a" * 93
            elif 3 > row["x"] > 2:
                if row["y"] > 5:  # NOSONAR
                    return "a" * 104

            return "a" * 97

        data = otp.Ticks(dict(x=[1], y=[2]))

        data["z"] = data.apply(func)
        assert data.z.dtype is otp.string[104]

        data["z"] = "b" * 108

        df = otp.run(data)
        assert df["z"][0] == "b" * 104

    def test_update_time(self):
        """ check upating timestamp """
        d = {"x": [1, 2, 3, 4], "y": [1, -1, 1, -1]}

        data = otp.Ticks(d)

        data.Time = data.apply(lambda row: row._END_TIME if row.y > 0 else row._START_TIME)

        df = otp.run(data)

        assert df["x"][0] == 2
        assert df["x"][1] == 4
        assert df["x"][2] == 1
        assert df["x"][3] == 3

    def test_one_return(self):
        def func():
            return 123

        data = otp.Tick(A=1)
        data.X = data.apply(func)
        assert data['X'].dtype is int
        df = otp.run(data)
        assert df['X'][0] == 123


class TestMix:
    def test_use_loop(self):
        """
        Test function with loop
        """
        d = {"x": [1, 2, 2, 3], "y": [1, 0, 1, 0]}

        def func(row):
            for i in range(3):
                if (row["x"] - i) == 0:
                    return i
                else:
                    continue

            return -1

        data = otp.Ticks(d)
        data.z = data.apply(func)
        ot_df = otp.run(data)

        pd_df = pd.DataFrame(d)
        pd_df["z"] = pd_df.apply(func, axis=1)

        for inx in range(4):
            assert pd_df["z"][inx] == ot_df["z"][inx]

    def test_use_external_objects(self):
        """
        test checks of using external functions and objects
        in the passed to .apply function
        """
        price_buckets = [[0.00, 1.00], [1.01, 25.00], [25.01, 50.00], [50.01, ]]

        def to_bucket_range(t):
            if len(t) == 1:
                return f"{t[0]}-inf"
            else:
                return f"{t[0]}-{t[1]}"

        def func(row):
            for (i, price_tuple) in enumerate(price_buckets):
                if row.X == i:
                    return to_bucket_range(price_tuple)

        data = otp.Ticks(dict(X=[1, 2, 3]))

        data.Y = data.apply(func)

        df = otp.run(data)

        for inx, value in enumerate(["1.01-25.0", "25.01-50.0", "50.01-inf"]):
            assert df["Y"][inx] == value

    def test_use_embedded_functions(self):
        """ check that embedded functions work, for example type casting """

        def func(row):
            if row.x > 3:
                return "+" + row.x.apply(str)

            return "-"

        data = otp.Ticks(dict(x=[1, 2, 3, 4, 5]))
        data.z = data.apply(func)
        assert data.z.dtype is str

        df = otp.run(data)

        for inx, value in enumerate(["-", "-", "-", "+4", "+5"]):
            assert df["z"][inx] == value

    def test_lag_and_min_max(self):
        """ check that lag and min/max operations works fine """

        def func(row):
            if row.x > row.x[-1]:
                return otp.math.max(row.y, 0)
            else:
                return otp.math.min(row.y[-1], row.y[-2])

        data = otp.Ticks(dict(x=[1, 2, 3, 3, 4, 2, 5], y=[-1, 7, -3, -1, -5, 3, 0]))

        data.z = data.apply(func)

        assert data.z.dtype is int

        df = otp.run(data)

        assert df["z"].tolist() == [0, 7, 0, -3, 0, -5, 0]

    def test_break(self):
        bins = [-otp.inf, 1, 20, 100, otp.inf]
        bins_values = ['<1', '1-20', '20-100', '100<']

        def bin(row):
            for i in range(len(bins) - 1):
                if bins[i] <= row['X'] < bins[i + 1]:
                    break
            return bins_values[i]

        data = otp.Ticks(X=[-5, 5, 25, 125, otp.nan])
        data['X_BIN'] = data.apply(bin)
        df = otp.run(data)
        assert list(df['X_BIN']) == ['<1', '1-20', '20-100', '100<', '100<']

    def test_bit_and_or(self):
        data = otp.Ticks({
            'A': [1, 2, 3, 4, 5],
            'B': [5, 4, 3, 2, 1],
        })
        data['X'] = data.apply(
            lambda tick: 1 if ((tick['A'] > 3) & (tick['B'] < 3)) else 0
        )
        data['Y'] = data.apply(
            lambda tick: 1 if ((tick['A'] > 3) | (tick['B'] > 3)) else 0
        )
        data['Z'] = data.apply(lambda tick: (3 | 4) + (8 & 3))
        df = otp.run(data)
        assert list(df['X']) == [0, 0, 0, 1, 1]
        assert list(df['Y']) == [1, 1, 0, 1, 1]
        assert list(df['Z']) == [7, 7, 7, 7, 7]

    @pytest.mark.parametrize('variable', (True, False))
    def test_short_circuit(self, variable):
        data = otp.Ticks(A=[1, 2, 3])
        if variable:
            data['B'] = 2
        data['X'] = data.apply(lambda tick: 1 if variable and tick['A'] == tick['B'] else 0)
        df = otp.run(data)
        if variable:
            assert list(df['X']) == [0, 1, 0]
        else:
            assert list(df['X']) == [0, 0, 0]

    @pytest.mark.parametrize('variable', (False,))
    def test_short_circuit_with_if(self, variable):
        data = otp.Ticks(A=[1, 2, 3])
        if variable:
            data['B'] = 2
        data['X'] = data.apply(lambda tick: tick['B'] if variable else tick['A'])
        df = otp.run(data)
        if variable:
            assert list(df['X']) == [2, 2, 2]
        else:
            assert list(df['X']) == [1, 2, 3]

    def test_inner_if_expr(self):
        data = otp.Ticks(A=[1, 2])
        data['X'] = data.apply(
            lambda r: 'one' if (1 if r['A'] == 1 else 0) == 1 else 'two'  # NOSONAR
        )
        df = otp.run(data)
        assert list(df['X']) == ['one', 'two']

    def test_inner_function_with_if_expr(self):
        def func(row):
            return 1 if row['A'] == 1 else 0
        data = otp.Ticks(A=[1, 2])
        data['X'] = data.apply(
            lambda row: 'one' if func(row) == 1 else 'two'
        )
        df = otp.run(data)
        assert list(df['X']) == ['one', 'two']

    def test_inner_lambda_with_if_expr(self):
        la = lambda row: row['A'] >= 1 and row['A'] == 1 or row['A'] <= 3 and row['A'] == 3
        data = otp.Ticks(A=[1, 2, 3])
        data['X'] = data.apply(
            lambda row: 1 if la(row) else 0
        )
        df = otp.run(data)
        assert list(df['X']) == [1, 0, 1]

    def test_inner_lambda_with_kwargs(self):
        def fun(tick, state='N'):
            return tick['STATE'] == state
        data = otp.Ticks(STATE=['N', 'PF', 'F'])
        data['X'] = data.apply(
            lambda r: 1 if fun(r, state='PF') else 0
        )
        df = otp.run(data)
        assert list(df['X']) == [0, 1, 0]

    def test_inner_lambda_with_kwargs_default(self):
        def fun(tick, state='N'):
            return tick['STATE'] == state
        data = otp.Ticks(STATE=['N', 'PF', 'F'])
        data['X'] = data.apply(
            lambda r: 1 if fun(r) else 0
        )
        df = otp.run(data)
        assert list(df['X']) == [1, 0, 0]

    def test_inner_lambda_with_args(self):
        def fun(tick, states):
            return tick['STATE'] in states

        data = otp.Ticks(STATE=['N', 'PF', 'F'])
        data['X'] = data.apply(
            lambda r: 1 if fun(r, ['PF']) else 0
        )
        df = otp.run(data)
        assert list(df['X']) == [0, 1, 0]

    def test_inner_lambda_with_closure_vars(self):
        def fun(tick, states):
            return tick['STATE'] in states

        data = otp.Ticks(STATE=['N', 'PF', 'F'])
        states = ['PF']
        data['X'] = data.apply(
            lambda r: 1 if fun(r, states) else 0
        )
        df = otp.run(data)
        assert list(df['X']) == [0, 1, 0]

    def test_inner_lambda_with_closure_vars_from_other_context(self):
        def add_apply(data, condition):
            return data.apply(lambda row: 1 if condition(row) else 0)

        data = otp.Ticks(STATE=['N', 'PF', 'F'])
        states = ['PF']
        data['X'] = add_apply(data, lambda r: True if r['STATE'] in states else False)
        df = otp.run(data)
        assert list(df['X']) == [0, 1, 0]

    def test_inner_lambda_with_closure_vars_from_other_context_intersection(self):
        def add_apply(data, condition):
            states = 1
            return data.apply(lambda row: states if condition(row) else 0)

        data = otp.Ticks(STATE=['N', 'PF', 'F'])
        states = ['PF']
        data['X'] = add_apply(data, lambda r: True if r['STATE'] in states else False)
        df = otp.run(data)
        assert list(df['X']) == [0, 1, 0]

    def test_inner_lambda_with_row_arg_somewhere(self):
        def fun(state, tick):
            return tick['STATE'] == state

        data = otp.Ticks(STATE=['N', 'PF', 'F'])
        data['X'] = data.apply(
            lambda r: 1 if fun('PF', r) else 0
        )
        df = otp.run(data)
        assert list(df['X']) == [0, 1, 0]

    def test_inner_builtin(self):
        data = otp.Ticks(A=[1, 2, 3])
        data['X'] = data.apply(lambda r: 1 if abs(r['A']) == 2 else 0)
        df = otp.run(data)
        assert list(df['X']) == [0, 1, 0]

    def test_inner_method(self):
        data = otp.Ticks(A=["A A", "B A", "C A"])
        data['X'] = data.apply(
            lambda r: 1 if r['A'].str.token(' ', 0) in ['A', 'B'] else 0
        )
        df = otp.run(data)
        assert list(df['X']) == [1, 1, 0]

    def is_pf(self, source):
        return 1 if source['STATE'] == 'PF' else 0

    def test_inner_classmethod(self):
        data = otp.Ticks(STATE=['N', 'PF', 'F'])
        data['X'] = data.apply(lambda r: 1 if self.is_pf(r) else 0)
        df = otp.run(data)
        assert list(df['X']) == [0, 1, 0]

    def test_inner_with_operations(self):
        data = otp.Ticks({
            'TRADING_SESSION': ['MORNING', 'CORE', 'CORE', 'CORE', 'CORE', 'LATE'],
            'HALT_TYPE': [0, 0, 23, 0, 0, 0],
        })
        reverted = False

        def _xor1(a, rev):
            return a and (not rev)

        def _xor2(a, rev):
            return a

        def _xor3(a, rev):
            return a & (not rev)

        def _xor4(a, rev):
            return a['HALT_TYPE'] != 0 and (not rev)

        def _xor5(row, a, rev):
            return a and (not rev)

        def _xor6(a, rev):
            if isinstance(rev, otp.Operation):
                return (~rev) & a
            if isinstance(a, otp.Operation):
                return a & (not rev)
            if not isinstance(a, otp.Operation):
                return a and (not rev)

        def halt_effect_on(row, f):
            return row.TRADING_SESSION == 'CORE' and f(row['HALT_TYPE'] != 0, reverted)

        def halt_effect_on_row(row, f):
            return row.TRADING_SESSION == 'CORE' and f(row, reverted)

        def halt_effect_on_ops(row, f):
            return row.TRADING_SESSION == 'CORE' and f(row, row['HALT_TYPE'] != 0, row['TRADING_SESSION'] == 'MORNING')

        def halt_effect_on_full(row, f):
            return (
                row.TRADING_SESSION == 'CORE' and
                f(row['HALT_TYPE'] != 0, reverted) and
                f(not reverted, row['HALT_TYPE'] == 0) and
                f(True, False) and
                f(row['HALT_TYPE'] != 0, row['HALT_TYPE'] == 0)
            )

        def halt_effect_on_full_inversion(row, f):
            return (
                row.TRADING_SESSION != 'CORE' or
                f(row['HALT_TYPE'] == 0, not reverted) or
                f(reverted, row['HALT_TYPE'] != 0) or
                f(False, True) or
                f(row['HALT_TYPE'] == 0, row['HALT_TYPE'] != 0)
            )

        with pytest.warns(UserWarning, match=r"Function '_xor1.+\n?' can't be called in python"):
            data['A'] = data.apply(lambda row: 1 if halt_effect_on(row, _xor1) else 0)
        data['B'] = data.apply(lambda row: 1 if halt_effect_on(row, _xor2) else 0)
        data['C'] = data.apply(lambda row: 1 if halt_effect_on(row, _xor3) else 0)
        data['D'] = data.apply(lambda row: 1 if halt_effect_on_row(row, _xor4) else 0)
        data['E'] = data.apply(lambda row: 1 if halt_effect_on_ops(row, _xor5) else 0)
        data['F'] = data.apply(lambda row: 1 if halt_effect_on(row, _xor6) else 0)
        with pytest.warns(UserWarning, match=r"Function '_xor1.+\n?' can't be called in python"):
            data['G'] = data.apply(lambda row: 1 if halt_effect_on_full(row, _xor1) else 0)
        data['H'] = data.apply(lambda row: 1 if halt_effect_on_full(row, _xor2) else 0)
        data['I'] = data.apply(lambda row: 1 if halt_effect_on_full(row, _xor6) else 0)
        with pytest.warns(UserWarning, match=r"Function '_xor1.+\n?' can't be called in python"):
            data['J'] = data.apply(lambda row: 1 if halt_effect_on_full_inversion(row, _xor1) else 0)
        data['K'] = data.apply(lambda row: 1 if halt_effect_on_full_inversion(row, _xor2) else 0)
        data['L'] = data.apply(lambda row: 1 if halt_effect_on_full_inversion(row, _xor6) else 0)
        df = otp.run(data)
        assert list(df['A']) == [0, 0, 1, 0, 0, 0]
        assert list(df['B']) == [0, 0, 1, 0, 0, 0]
        assert list(df['C']) == [0, 0, 1, 0, 0, 0]
        assert list(df['D']) == [0, 0, 1, 0, 0, 0]
        assert list(df['E']) == [0, 0, 1, 0, 0, 0]
        assert list(df['F']) == [0, 0, 1, 0, 0, 0]
        assert list(df['G']) == [0, 0, 1, 0, 0, 0]
        assert list(df['H']) == [0, 0, 1, 0, 0, 0]
        assert list(df['I']) == [0, 0, 1, 0, 0, 0]
        assert list(df['J']) == [1, 1, 0, 1, 1, 1]
        assert list(df['K']) == [1, 1, 0, 1, 1, 1]
        assert list(df['L']) == [1, 1, 0, 1, 1, 1]

    def test_slice(self):
        data = otp.Ticks(STATE=['N', 'PF', 'F', 'C'])
        states = ['N', 'PF', 'F', 'C']
        data['X'] = data.apply(
            lambda r: 1 if r['STATE'] in states[2:4] else 0
        )
        df = otp.run(data)
        assert list(df['X']) == [0, 0, 1, 1]

    def test_classmethod_as_script(self):
        class A:
            def fun(self, tick):
                tick['X'] = 123
        data = otp.Tick(A=1)
        data = data.script(A().fun)
        df = otp.run(data)
        assert df['X'][0] == 123

    # TODO: test in script
    def test_local_variables(self):
        def size_bin(row):
            bins = (0, 10, 100, 1000, otp.inf)
            for i in range(len(bins) - 1):
                if bins[i] <= row['SIZE'] < bins[i + 1]:
                    return f'{i + 1}: {bins[i]}-{bins[i + 1]}'

        data = otp.Ticks(SIZE=[5, 50, 500, 5000])
        data['SIZE_BIN'] = data.apply(size_bin)
        df = otp.run(data)
        assert list(df['SIZE_BIN']) == ['1: 0-10', '2: 10-100', '3: 100-1000', '4: 1000-INFINITY()']

    def test_if_else_and_if(self):
        def fun(tick):
            if tick['A'] <= 1:
                return 1
            else:
                if tick['A'] > 2:
                    return 3
                else:
                    return 2
            if tick['A'] >= 2:  # NOSONAR
                return 3

        data = otp.Ticks(A=[1, 2, 3])
        data['X'] = data.apply(fun)
        df = otp.run(data)
        assert list(df['X']) == [1, 2, 3]

    def test_if_else_and_return(self):
        def fun(tick):
            if tick['A'] <= 1:
                return 1
            else:
                return 2
            return 3  # NOSONAR

        data = otp.Ticks(A=[1, 2, 3])
        data['X'] = data.apply(fun)
        df = otp.run(data)
        assert list(df['X']) == [1, 2, 2]

    def test_if_if_return(self):
        def fun(tick):
            if tick['A'] > 1:
                if tick['A'] >= 3:  # NOSONAR
                    return 3
            return 1

        data = otp.Ticks(A=[1, 2, 3])
        data['X'] = data.apply(fun)
        df = otp.run(data)
        assert list(df['X']) == [1, 1, 3]

    def test_if_if_return_2(self):
        def fun(tick):
            if tick['A'] > 1:
                if tick['A'] >= 3:  # NOSONAR
                    return 3

        data = otp.Ticks(A=[1, 2, 3])
        data['X'] = data.apply(fun)
        df = otp.run(data)
        assert list(df['X']) == [0, 0, 3]

    def test_if_hard(self):
        def fun(tick):
            if tick['A'] <= 1:
                if tick['A'] > 0:
                    return 1
                else:
                    pass  # NOSONAR
            else:
                if tick['A'] < 3:
                    return 2
            if tick['A'] < 0:
                return -1
            return 0

        data = otp.Ticks(A=[-1, 0, 1, 2, 3])
        data['X'] = data.apply(fun)
        df = otp.run(data)
        assert list(df['X']) == [-1, 0, 1, 2, 0]

    def test_pass(self):
        def fun(tick):  # NOSONAR
            pass

        data = otp.Ticks(A=[1, 2, 3])
        data['X'] = data.apply(fun)
        assert data['X'].dtype is float
        df = otp.run(data)
        assert all(df['X'].isna())

    def test_lag_operator(self):
        data = otp.Ticks(A=[1, 2, 3])
        data['X'] = data.apply(lambda tick: tick['A'][-1])
        df = otp.run(data)
        assert list(df['X']) == [0, 1, 2]

    def test_mod(self):
        data = otp.Ticks(A=[0, 1, 2, 3, 4])
        data['X'] = data.apply(lambda tick: tick['A'] % 2)
        df = otp.run(data)
        assert list(df['X']) == [0, 1, 0, 1, 0]

    def test_range(self):
        def fun(tick):
            if tick['A'] in range(1, 4):
                return tick['A']
            return -123
        data = otp.Ticks(A=[-1, 0, 1, 2, 3, 100])
        data['X'] = data.apply(fun)
        df = otp.run(data)
        assert list(df['X']) == [-123, -123, 1, 2, 3, -123]

    def test_starred(self):
        def custom_sum(*args):
            return sum(args)

        nums = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        data = otp.Tick(A=1)
        data['X'] = data.apply(
            lambda row: sum([*nums]) + custom_sum(*nums)
        )
        df = otp.run(data)
        assert df['X'][0] == 45 + 45

    def test_starred_with_row_fields(self):
        def fun(row):
            return otp.math.max(row['A'], row['B']) + otp.math.min(row['A'], row['B'])

        data = otp.Tick(A=1, B=2)
        data['X'] = data.apply(fun)
        df = otp.run(data)
        assert df['X'][0] == 1 + 2

    def test_fun_with_comments(self):
        def fun(row):
            """comments"""
            if row['A'] > 0:
                return 1
            return -1

        data = otp.Ticks(A=[100, -200])
        data['X'] = data.apply(fun)
        df = otp.run(data)
        assert list(df['X']) == [1, -1]


class TestTypes:
    def test_bool_int(self):
        """ check book-int casting to float """
        data = otp.Ticks(dict(x=[1, 2]))

        data.z = data.apply(lambda row: row.x > 0 if row.x == 1 else 5)
        assert data.z.dtype is float

        df = otp.run(data)

        assert df["z"].dtype == np.float64

    def test_float_int(self):
        """ check float-int casting to float """
        data = otp.Ticks(dict(x=[1]))

        data.z = data.apply(lambda row: 1 if row.x > 0 else 1.5)
        assert data.z.dtype is float

        df = otp.run(data)

        assert df["z"].dtype == np.float64

    def test_msectime(self):
        """ check msectime support """
        data = otp.Ticks(dict(x=[1]))

        data.z = data.apply(lambda row: otp.msectime(0))
        assert data.z.dtype is otp.msectime

    def test_time_and_int(self):
        """ check int-timestamp casting to time """
        data = otp.Ticks(dict(x=[1, -1]))

        data.z = data.apply(lambda row: row.Time if row.x > 0 else 0)
        assert data.z.dtype is otp.nsectime

        df = otp.run(data)

        assert pd.api.types.is_datetime64_any_dtype(df["z"])

    @pytest.mark.parametrize("value", [None, otp.nan])
    def test_nan(self, value):
        """ check None and otp.nan casting to float """
        def fun(q):
            if q['A'] < 0:
                return value
            else:
                return 4

        data = otp.Ticks(A=[-1, 1])
        assert str(data.apply(fun)) == 'CASE((A) < (0), 1, NAN(), 4)'
        data['B'] = data.apply(fun)
        assert data['B'].dtype is float
        df = otp.run(data)
        assert np.isnan(df['B'][0])
        assert df['B'][1] == 4.0


class TestWrong:
    def test_different_types_1(self):
        """ check case when function returns different constant values """
        data = otp.Ticks(dict(x=[1, 2, 3], y=[1, 0, 1]))

        with pytest.raises(TypeError):
            data.z = data.apply(lambda row: "abc" if row.y > 0 else 3.5)

    def test_different_types_2(self):
        """ check case when function returns different column-based values """
        data = otp.Ticks(dict(x=[1, 2, 3], y=[1, 0, 1], z=["a", "b", "c"]))

        with pytest.raises(TypeError):
            data.z = data.apply(lambda row: row.x if row.y > 0 else row.z)
        # with

    def test_str_bool(self):
        """ check incompatible types - bool and string """
        data = otp.Ticks(dict(x=[1]))

        with pytest.raises(TypeError):
            data.apply(lambda row: row.x == 1 if row.x > 0 else "some")

    def test_str_time(self):
        """ check incompatible types - str and time """
        data = otp.Ticks(dict(x=[1]))

        with pytest.raises(TypeError):
            data.apply(lambda row: row.Time if row.x > 0 else "some")

    def test_time_and_float(self):
        """ check incompatible types - float and time """
        data = otp.Ticks(dict(x=[1]))

        with pytest.raises(TypeError):
            data.apply(lambda row: otp.nsectime(0) if data.x > 0 else 0.5)

    def test_many_params(self):
        """
        Check that use could pass only one or zero parameters.
        More - ValueError
        """
        data = otp.Ticks(dict(x=[1]))

        with pytest.raises(ValueError, match=r"either one or zero parameters"):
            data.apply(lambda x, y: 1)

    def test_generator(self):
        """ Check that apply does not support generators """
        t = otp.Ticks(dict(x=[1]))

        with pytest.raises(ValueError):
            t.apply(lambda row: x for x in range(10))  # NOSONAR

    def test_custom_type(self):
        """ does not support custom types"""

        class MyType:
            pass

        t = otp.Ticks(dict(x=[1]))

        with pytest.raises(TypeError):
            t.apply(lambda: MyType())


class TestPerTickScript:
    class TestExistingColumns:
        """ Check reference to existing columns """

        def test_plain_simple(self):
            def func(tick):
                tick['X'] = 1

            data = otp.Ticks(dict(X=[1, 2, 3]))
            res = data.script(func)

            assert 'X' in res.columns()
            assert res['X'].dtype is int

            df = otp.run(res)

            assert all(df['X'] == [1, 1, 1])

        def test_plain_multiple(self):
            def func(tick):
                tick['X'] += 2
                tick['X'] = tick['X'] * tick['X']

            data = otp.Ticks(dict(X=[1, 2, 3]))
            res = data.script(func)

            df = otp.run(res)

            assert all(df['X'] == [9, 16, 25])

        def test_if(self):
            def func(tick):
                if tick['X'] > 1:
                    tick['X'] += 1

            data = otp.Ticks(dict(X=[1, 2, 3]))

            res = data.script(func)
            df = otp.run(res)

            assert all(df['X'] == [1, 3, 4])

        def test_if_else(self):
            def func(tick):
                if tick['X'] <= 1:
                    tick['X'] = 0
                elif 1 < tick['X'] < 5:
                    tick['X'] = 1
                else:
                    tick['X'] = 2

                tick['X'] *= 2

            data = otp.Ticks(dict(X=[0, 1, 2, 3, 4, 5, 6]))

            res = data.script(func)
            df = otp.run(res)

            assert all(df['X'] == [0, 0, 2, 2, 2, 4, 4])

    class TestNewColumns:

        def test_plain(self):
            def func(tick):
                tick['Y'] = tick['X'] * 0.5
                tick['Z'] = tick['X'].apply(str) + 'abc'
                tick['G'] = otp.nan
                tick['T'] = tick['X'] * tick['X']

            data = otp.Ticks(dict(X=[1, 2, 3]))
            data = data.script(func)

            df = otp.run(data)

            assert all(df['Y'] == [0.5, 1.0, 1.5])
            assert all(df['Z'] == ['1abc', '2abc', '3abc'])
            assert np.isnan(df['G'][0])
            assert all(df['T'] == [1, 4, 9])

        def test_if_else(self):
            def func(tick):
                tick['Y'] = 0.0

                if tick['X'] > 1:
                    tick['Y'] = tick['X'] * 0.5
                else:
                    tick['Z'] = -1

                tick['G'] = tick['Z'] * tick['Y']

            data = otp.Ticks(dict(X=[1, 3, 4]))

            res = data.script(func)
            df = otp.run(res)

            assert all(df['X'] == [1, 3, 4])
            assert all(df['Y'] == [0.0, 1.5, 2.0])
            assert all(df['Z'] == [-1, 0, 0])
            assert all(df['G'] == [0.0, 0.0, 0.0])

        def test_nested_if(self):
            def func(tick):
                tick['X'] -= 1

                if tick['X'] > 1:
                    tick['Z'] = 1

                    if tick['X'] < 4:
                        tick['Y'] = 0
                    else:
                        tick['Y'] = 1
                        tick['Z'] = 2

                    tick['Y'] += 1
                else:
                    tick['Y'] = 3
                # if

                tick['X'] += 1

            data = otp.Ticks(dict(X=[1, 2, 3, 4, 5]))
            res = data.script(func)

            df = otp.run(res)

            assert all(df['X'] == [1, 2, 3, 4, 5])
            assert all(df['Y'] == [3, 3, 1, 1, 2])
            assert all(df['Z'] == [0, 0, 1, 1, 2])

        def test_multiple_if_else(self):
            def func(tick):
                tick['X'] -= 1

                if 4 >= tick['X'] > 2:
                    tick['Y'] = 1

                tick['X'] += 1

                if 4 >= tick['X'] > 2:
                    tick['Z'] = 1

                tick['X'] += 1

                if 4 >= tick['X'] > 2:
                    tick['W'] = 1

                tick['X'] -= 1

            data = otp.Ticks(dict(X=[1, 2, 3, 4, 5]))

            data = data.script(func)
            df = otp.run(data)

            assert all(df['X'] == [1, 2, 3, 4, 5])
            assert all(df['Y'] == [0, 0, 0, 1, 1])
            assert all(df['Z'] == [0, 0, 1, 1, 0])
            assert all(df['W'] == [0, 1, 1, 0, 0])

        @pytest.mark.parametrize("value,dtype",
                                 [
                                     (1, int),
                                     (0.1, float),
                                     (otp.nan, float),
                                     (otp.inf, float),
                                     ('', str),
                                     ('a' * 67, otp.string)  # larger than standard 64
                                 ])
        def test_types(self, value, dtype):
            def func(tick):
                tick['Y'] = data['X']

            data = otp.Tick(X=value)

            res = data.script(func)
            assert issubclass(res['X'].dtype, dtype)
            assert issubclass(res['Y'].dtype, dtype)

            df = otp.run(res)

            if value not in (otp.nan, otp.inf):
                assert all(df['Y'] == [value])
            # if

        def test_simple_values(self):
            def fun(tick):
                tick['B'] = 1
                tick['C'] = 1.23
                tick['D'] = 'STRING'

            data = otp.Tick(A=1)
            data = data.script(fun)
            df = otp.run(data)
            assert df['A'][0] == 1
            assert df['B'][0] == 1
            assert df['C'][0] == 1.23
            assert df['D'][0] == 'STRING'

    class TestFilter:
        """ Test filtering use cases """

        def test_bool_1(self):
            def func(tick):
                if tick['X'] > 2:
                    return True

            data = otp.Ticks(dict(X=[0, 1, 2, 3, 4]))

            res = data.script(func)
            df = otp.run(res)

            assert len(df) == 2
            assert all(df['X'] == [3, 4])

        def test_bool_2(self):
            def func(tick):
                if tick['X'] > 2:
                    return False
                else:
                    return True

            data = otp.Ticks(dict(X=[0, 1, 2, 3, 4]))

            res = data.script(func)
            df = otp.run(res)

            assert len(df) == 3
            assert all(df['X'] == [0, 1, 2])

        def test_compare(self):
            def func(tick):
                return tick['X'] == 2

            data = otp.Ticks(dict(X=[0, 1, 2, 3, 4]))

            res = data.script(func)
            df = otp.run(res)

            assert len(df) == 1
            assert all(df['X'] == [2])

        def test_invalid_type(self):
            def func(tick):
                if tick['X'] > 2:
                    return 3

            data = otp.Ticks(dict(X=[1, 2, 3]))

            with pytest.raises(TypeError):
                data.script(func)

        def test_with_preproc(self):
            def func(tick):
                tick['X'] -= 1

                if tick['X'] > 2:
                    return True

            data = otp.Ticks(dict(X=[1, 2, 3, 4, 5]))
            data = data.script(func)

            df = otp.run(data)

            assert len(df) == 2
            assert all(df['X'] == [3, 4])

        def test_nested_if(self):
            def func(tick):
                tick['X'] -= 1

                if tick['X'] < 4:
                    tick['X'] += 1

                    if tick['Y'] > 0:
                        tick['X'] *= 2
                        return True

                    if tick['Z'] != 0:
                        tick['X'] *= -1
                        return True

                tick['X'] -= 1

                if tick['X'] >= 5:
                    if tick['Y'] < 0:  # NOSONAR
                        tick['Z'] = 7
                        return True

                tick['X'] *= 2

            data = otp.Ticks(dict(X=[1, 2, 3, 4, 5, 6, 7],
                                  Y=[0, 1, 0, 1, -1, 1, -1],
                                  Z=[0, 1, 1, 0, 1, 0, 0]))

            data = data.script(func)
            df = otp.run(data)

            assert len(df) == 4
            assert all(df['Y'] == [1, 0, 1, -1])
            assert all(df['Z'] == [1, 1, 0, 7])
            assert all(df['X'] == [4, -3, 8, 5])

    class TestNewColumnsInPerTickScript:
        params: Dict[str, Any] = {}

        @classmethod
        def setup_class(cls):
            folder = os.path.dirname(os.path.abspath(__file__))
            while not folder.endswith("core"):
                folder, _, _ = folder.rpartition(os.sep)
            folder = os.path.join(folder, "per_tick_scripts")
            cls._create_param(folder, "add_values", "add_fields.script")

        @classmethod
        def _create_param(cls, folder, name_prefix, path):
            add_values_path = os.path.join(folder, path)
            cls.params[f"{name_prefix}_path"] = add_values_path
            with open(add_values_path) as file:
                cls.params[f"{name_prefix}_script"] = file.read()

        @pytest.mark.parametrize("script", ["add_values_path", "add_values_script"])
        def test_str(self, script):
            script = self.params[script]
            data = otp.Ticks(X=[1])
            data2 = data.script(script)
            df = otp.run(data)
            assert len(df.columns) == 2  # check script isn't inplace operation
            df2 = otp.run(data2)
            assert len(df2.columns) == 14
            assert data2.schema == dict(X=int,
                                        BENEFITING_ORDER_ID=otp.string[512],
                                        TRADE_ORDER_TYPE=otp.string[10],
                                        VARSTRING1=otp.varstring,
                                        LAYERING_CONDITION_TIME=otp.nsectime,
                                        TRADE_TIME=otp.msectime,
                                        TRADE_PRICE=float,
                                        DEC=otp.decimal,
                                        LAYERING_PRICE_LEVELS_CANCELLED=int,
                                        SOME_SHORT=otp.short,
                                        SOME_UINT=otp.uint,
                                        SOME_BYTE=otp.byte,
                                        SOME_INT=otp.int)


class TestSymbolParams:

    def test_apply(self):
        t = otp.Tick(A=1)
        t['X'] = t.apply(lambda tick: 1 if tick.Symbol['X', str].str.match('a.c') else 0)
        res = otp.run(t, symbols=[
            otq.Symbol(f'{otp.config.default_db}::ABC', {'X': 'abc'}),
            otq.Symbol(f'{otp.config.default_db}::XXX', {'X': 'xxx'}),
        ])
        assert list(res[f'{otp.config.default_db}::ABC']['X']) == [1]
        assert list(res[f'{otp.config.default_db}::XXX']['X']) == [0]

    def test_script(self):
        t = otp.Tick(A=1)

        def fun(tick):
            if tick.Symbol['X', str].str.match('a.c'):
                tick['X'] = 1
            else:
                tick['X'] = 0

        t = t.script(fun)
        res = otp.run(t, symbols=[
            otq.Symbol(f'{otp.config.default_db}::ABC', {'X': 'abc'}),
            otq.Symbol(f'{otp.config.default_db}::XXX', {'X': 'xxx'}),
        ])
        assert list(res[f'{otp.config.default_db}::ABC']['X']) == [1]
        assert list(res[f'{otp.config.default_db}::XXX']['X']) == [0]
