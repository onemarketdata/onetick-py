import pytest
import numpy as np
import pandas as pd
import onetick.py as otp


@pytest.fixture(scope="module")
def symbols():
    data = otp.Ticks(
        [
            ["SYMBOL_NAME", "INT_PARAM", "FLOAT_PARAM", "STR_PARAM"],
            ["A", 1, -1.7, "X"],
            ["B", 2, 0.5, "Y"],
            ["C", -9, otp.nan, "Z"],
        ]
    )

    data.TIME_PARAM = data.Time

    yield data


class TestSymbolParamSource:
    def test_access_symbol_params_source(self, symbols):
        symbol_param = symbols.to_symbol_param()

        assert "INT_PARAM" in symbol_param.__dict__
        assert "FLOAT_PARAM" in symbol_param.__dict__
        assert "STR_PARAM" in symbol_param.__dict__
        assert "TIME_PARAM" in symbol_param.__dict__

        symbol_param.TIME_PARAM
        symbol_param.INT_PARAM
        symbol_param.FLOAT_PARAM
        symbol_param.STR_PARAM

    def test_change_symbol_param_source(self, symbols):
        symbol_param = symbols.to_symbol_param()

        with pytest.raises(Exception):
            symbol_param.INT_PARAM = 3

        with pytest.raises(Exception):
            symbol_param.FLOAT_PARAM = 4.5

        with pytest.raises(Exception):
            symbol_param.STR_PARAM = "abc"


class TestSimpleTypes:
    def test_int_symbol_param(self, session, symbols):
        symbol_param = symbols.to_symbol_param()
        # ----------------
        # main query
        data = otp.Ticks(dict(price=[1.3]))

        data.x = symbol_param.INT_PARAM + 1

        res = otp.run(data, symbols=symbols)

        a_df = res["A"]
        b_df = res["B"]

        assert len(a_df) == 1
        assert len(b_df) == 1

        assert "x" in a_df.columns
        assert pd.api.types.is_numeric_dtype(a_df.x)
        assert "x" in b_df.columns
        assert pd.api.types.is_numeric_dtype(b_df.x.dtype)

        assert a_df.x[0] == 2
        assert b_df.x[0] == 3

    def test_float_symbol_param(self, session, symbols):
        symbol_param = symbols.to_symbol_param()
        # ----------------
        # main query
        data = otp.Ticks(dict(price=[1.3]))

        data.x = symbol_param.FLOAT_PARAM

        res = otp.run(data, symbols=symbols)

        a_df = res["A"]
        b_df = res["B"]
        c_df = res["C"]

        assert len(a_df) == 1
        assert len(b_df) == 1
        assert len(c_df) == 1

        assert "x" in a_df.columns
        assert pd.api.types.is_float_dtype(a_df.x)

        assert "x" in b_df.columns
        assert pd.api.types.is_float_dtype(b_df.x.dtype)

        assert "x" in c_df.columns
        assert pd.api.types.is_float_dtype(c_df.x.dtype)

        assert a_df.x[0] == -1.7
        assert b_df.x[0] == 0.5
        assert np.isnan(c_df.x[0])

    def test_time_symbol_param(self, session, symbols):
        symbol_param = symbols.to_symbol_param()
        # ----------------
        # main query
        data = otp.Ticks(dict(price=[1.3]))

        data.x = symbol_param.TIME_PARAM

        res = otp.run(data, symbols=symbols)

        a_df = res["A"]
        b_df = res["B"]

        assert len(a_df) == 1
        assert len(b_df) == 1

        assert a_df.x[0] == a_df.Time[0]
        assert b_df.x[0] == a_df.Time[0] + otp.Milli(1)

    def test_str_symbol_param(self, session, symbols):
        symbol_param = symbols.to_symbol_param()
        # ----------------
        # main query
        data = otp.Ticks(dict(price=[1.3]))

        data.x = symbol_param.STR_PARAM

        res = otp.run(data, symbols=symbols)

        a_df = res["A"]
        b_df = res["B"]

        assert len(a_df) == 1
        assert len(b_df) == 1

        assert a_df.x[0] == "X"
        assert b_df.x[0] == "Y"


class TestApply:
    def test_apply_column_1(self, session, symbols):
        symbol_param = symbols.to_symbol_param()
        # ----------------
        # main query
        data = otp.Ticks(dict(price=[1.3]))

        data.t = data.price.apply(lambda x: x * symbol_param.FLOAT_PARAM)

        res = otp.run(data, symbols=symbols)

        a_df, b_df, c_df = res["A"], res["B"], res["C"]

        assert a_df.t[0] == 1.3 * (-1.7)
        assert b_df.t[0] == 1.3 * (0.5)
        assert np.isnan(c_df.t[0])

    def test_apply_column_2(self, session, symbols):
        symbol_param = symbols.to_symbol_param()
        # ----------------
        # main query
        data = otp.Ticks(dict(price=[1.3]))

        data.t = data.price.apply(lambda x: x if symbol_param.FLOAT_PARAM > 0 else 0)

        res = otp.run(data, symbols=symbols)

        a_df, b_df, c_df = res["A"], res["B"], res["C"]

        assert a_df.t[0] == 0
        assert b_df.t[0] == 1.3
        assert c_df.t[0] == 0

    def test_apply_source_1(self, session, symbols):
        symbol_param = symbols.to_symbol_param()
        # ----------------
        # main query
        data = otp.Ticks(dict(price=[1.3], qty=[100]))

        data.t = data.apply(lambda row: row.price * row.qty * symbol_param.FLOAT_PARAM)
        res = otp.run(data, symbols=symbols)

        a_df, b_df, c_df = res["A"], res["B"], res["C"]

        assert a_df.t[0] == a_df.price[0] * a_df.qty[0] * -1.7
        assert b_df.t[0] == b_df.price[0] * b_df.qty[0] * 0.5
        assert np.isnan(c_df.t[0])

    def test_apply_source_2(self, session, symbols):
        symbol_param = symbols.to_symbol_param()
        # ----------------
        # main query
        data = otp.Ticks(dict(price=[1.3], qty=[100]))

        def inner_f(row):
            if symbol_param.FLOAT_PARAM > 0:
                return row.price
            else:
                return row.qty

        data.t = data.apply(inner_f)
        res = otp.run(data, symbols=symbols)

        a_df, b_df, c_df = res["A"], res["B"], res["C"]

        assert a_df.t[0] == a_df.qty[0]
        assert b_df.t[0] == b_df.price[0]
        assert c_df.t[0] == c_df.qty[0]
