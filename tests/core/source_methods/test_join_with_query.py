import os
import re
import math
from datetime import datetime
from functools import partial

import dateutil
import numpy as np
import pandas as pd
import pytest
import pytz

import onetick.py as otp
from onetick.py.types import string
from onetick.py.utils import TmpFile


class TestCommon:
    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session):
        yield c_session

    def test_single(self):
        def func():
            return otp.Ticks(dict(y=[5.6]))

        data = otp.Ticks(dict(x=[1]))

        res = data.join_with_query(func)

        df = otp.run(res)

        assert "x" in df.columns
        assert "y" in df.columns

        assert len(df) == 1

        assert df["x"][0] == 1
        assert df["y"][0] == 5.6

    def test_multiple_base(self):
        def func():
            return otp.Ticks(dict(y=[0.3]))

        data = otp.Ticks(dict(x=[2, 5]))

        res = data.join_with_query(func)

        df = otp.run(res)

        assert len(df) == 2

        assert df["x"][0] == 2
        assert df["y"][0] == 0.3

        assert df["x"][1] == 5
        assert df["y"][1] == 0.3

    def test_multiple_joined(self):
        def func():
            return otp.Ticks(dict(y=[0.3, -0.5]))

        data = otp.Ticks(dict(x=[2]))

        res = data.join_with_query(func)

        df = otp.run(res)

        assert len(df) == 2

        assert df["x"][0] == 2
        assert df["y"][0] == 0.3

        assert df["x"][1] == 2
        assert df["y"][1] == -0.5

    def test_with_empty(self):
        def func():
            joined = otp.Ticks(dict(y=[-0.5, -1.2]))
            res, _ = joined[(joined.y > 0)]
            return res

        data = otp.Ticks(dict(x=[1, 2]))

        res = data.join_with_query(func)

        assert "x" in res.columns()
        assert "y" in res.columns()

        df = otp.run(res)

        assert "x" in df.columns
        assert "y" in df.columns

        assert len(df) == 2
        assert df["x"][0] == 1
        assert df["x"][1] == 2


class TestIntParams:
    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session):
        yield c_session

    def test_update_param(self):
        def func(external_x):
            inner = otp.Ticks(dict(y=[-0.5]))
            inner.y *= external_x
            return inner

        data = otp.Ticks(dict(x=[1, -2, 3]))

        res = data.join_with_query(func, params=dict(external_x=data.x))

        df = otp.run(res)

        assert len(df) == 3

        assert df["x"][0] == 1
        assert df["y"][0] == -0.5

        assert df["x"][1] == -2
        assert df["y"][1] == 1

        assert df["x"][2] == 3
        assert df["y"][2] == -1.5

    def test_add_param(self):
        def func(x):
            d = otp.Ticks(dict(y=[-0.5]))
            d.w = x
            return d

        data = otp.Ticks(dict(x=[2, 3]))

        res = data.join_with_query(func, params=dict(x=data.x))

        assert "w" in res.columns()
        assert res["w"].dtype is int
        assert "y" in res.columns()

        res["w"] += 1

        df = otp.run(res)

        assert len(df) == 2
        assert "w" in df.columns
        assert "x" in df.columns
        assert "y" in df.columns
        assert pd.api.types.is_numeric_dtype(df["w"])

        assert df["x"][0] == 2
        assert df["y"][0] == -0.5
        assert df["w"][0] == 3

        assert df["x"][1] == 3
        assert df["y"][0] == -0.5
        assert df["w"][0] == 3

    def test_const_param(self):
        def func(x):
            d = otp.Ticks(dict(y=[-0.5]))
            d.y *= x
            return d

        data = otp.Ticks(dict(x=[3, 4]))

        res = data.join_with_query(func, params=dict(x=3))
        df = otp.run(res)

        assert df["x"][0] == 3
        assert df["y"][0] == -1.5

        assert df["x"][1] == 4
        assert df["y"][1] == -1.5

    def test_multiple_params(self):
        def func(a, b):
            d = otp.Ticks(dict(y=[-1 * a]))
            d.b = b
            return d

        data = otp.Ticks(dict(x=[1, -2]))
        res = data.join_with_query(func, params=dict(a=3, b=5))

        assert res["b"].dtype is int

        df = otp.run(res)

        assert df["y"][0] == -3
        assert df["b"][0] == 5


class TestFloatParams:
    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session):
        yield c_session

    def test_const(self):
        def func(a, b):
            d = otp.Ticks(dict(y=[-4 * b]))
            d.a = a
            return d

        data = otp.Ticks(dict(x=[1, -2]))

        res = data.join_with_query(func, params=dict(a=0.1, b=0.3))

        assert "a" in res.columns()
        assert res["a"].dtype is float
        assert "y" in res.columns()
        assert res["y"].dtype is float

        df = otp.run(res)

        assert df["x"][0] == 1
        assert df["y"][0] == -1.2
        assert df["a"][0] == 0.1

        assert df["x"][1] == -2
        assert df["y"][1] == -1.2
        assert df["a"][1] == 0.1

    def test_from_columns(self):
        def func(a, b):
            d = otp.Ticks(dict(a=[1, a]))
            d.b = b
            return d

        data = otp.Ticks(dict(x=[-1, 2], y=[0.1, 0.2], z=[0.3, 0.4]))

        res = data.join_with_query(func, params=dict(a=data.y, b=data.z))

        assert res["a"].dtype is float
        assert res["b"].dtype is float

        df = otp.run(res)

        assert df["x"][0] == -1 and df["a"][0] == 1 and df["b"][0] == 0.3
        assert df["x"][1] == -1 and df["a"][1] == 0.1 and df["b"][1] == 0.3
        assert df["x"][2] == 2 and df["a"][2] == 1 and df["b"][2] == 0.4
        assert df["x"][3] == 2 and df["a"][3] == 0.2 and df["b"][3] == 0.4

    def test_nan(self):
        def func(a, b):
            d = otp.Ticks(dict(a=[1, a]))
            d.b = b
            return d

        data = otp.Ticks(dict(x=[otp.nan, 2], y=[0.1, otp.nan], z=[0.3, 0.4]))

        res = data.join_with_query(func, params=dict(a=data.x, b=data.y))

        assert res["a"].dtype is float
        assert res["b"].dtype is float

        df = otp.run(res)

        assert np.isnan(df["x"][0]) and df["a"][0] == 1 and df["b"][0] == 0.1
        assert np.isnan(df["x"][1]) and np.isnan(df["a"][1]) and df["b"][0] == 0.1
        assert df["x"][2] == 2 and df["a"][2] == 1 and np.isnan(df["b"][2])


class TestStringParams:
    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session):
        yield c_session

    def test_const(self):
        def func(str_x):
            d = otp.Ticks(dict(y=[1]))
            d.s = str_x + "s"
            return d

        data = otp.Ticks(dict(x=[2, 3], z=["a", "b"]))

        res = data.join_with_query(func, params=dict(str_x="val"))

        assert "s" in res.columns()
        assert res["s"].dtype is str

        df = otp.run(res)

        assert df["x"][0] == 2
        assert df["y"][0] == 1
        assert df["s"][0] == "vals"

        assert df["x"][1] == 3
        assert df["s"][1] == "vals"

    def test_column(self):
        def func(a, b):
            d = otp.Ticks(dict(y=[a]))
            d.b = b
            return d

        data = otp.Ticks(dict(x=[2, 3], z=["a", "b"]))

        res = data.join_with_query(func, params=dict(a="K", b=data.z))

        assert "y" in res.columns()
        assert res["y"].dtype is str

        assert "b" in res.columns()
        assert res["b"].dtype is str

        df = otp.run(res)

        assert df["x"][0] == 2
        assert df["z"][0] == "a"
        assert df["y"][0] == "K"
        assert df["b"][0] == "a"

        assert df["x"][1] == 3
        assert df["z"][1] == "b"
        assert df["y"][1] == "K"
        assert df["b"][1] == "b"

    def test_spaces(self):
        def func(a, b):
            d = otp.Ticks(dict(y=[a]))

            d.b = b + d.y

            return d

        data = otp.Ticks(dict(x=[2, 3], z=["a a", "b b"]))

        res = data.join_with_query(func, params=dict(a="K K", b=data.z))

        df = otp.run(res)

        assert df["x"][0] == 2
        assert df["z"][0] == "a a"
        assert df["y"][0] == "K K"
        assert df["b"][0] == "a aK K"

        assert df["x"][1] == 3
        assert df["z"][1] == "b b"
        assert df["y"][1] == "K K"
        assert df["b"][1] == "b bK K"

    def test_long(self):
        def func(a, b):
            d = otp.Ticks(dict(y=[a]))

            d.b = b

            return d

        data = otp.Ticks(dict(x=[2, 3], z=["a" * 78, "b" * 99]))

        res = data.join_with_query(func, params=dict(a="c" * 133, b=data.z))

        assert res["x"].dtype is int
        assert res["z"].dtype is string[99]
        assert res["y"].dtype is string[133]
        assert res["b"].dtype is string[99]

        df = otp.run(res)

        assert df["x"][0] == 2
        assert df["z"][0] == "a" * 78
        assert df["y"][0] == "c" * 133
        assert df["z"][0] == "a" * 78

        assert df["x"][1] == 3
        assert df["z"][1] == "b" * 99
        assert df["y"][1] == "c" * 133
        assert df["z"][1] == "b" * 99


class TestWrongParams:
    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session):
        yield c_session

    def test_wrong_params(self):
        def func(a, b):
            d = otp.Ticks(dict(y=[a]))

            d.b = b

            return d

        data = otp.Ticks(dict(x=[2, 3], z=["a" * 78, "b" * 99]))

        with pytest.raises(TypeError):
            data.join_with_query(func, params=dict(c=4, d=7))

    def test_not_passed_params(self):
        def func(a, b):
            d = otp.Ticks(dict(y=[a]))

            d.b = b

            return d

        data = otp.Ticks(dict(x=[2, 3], z=["a" * 78, "b" * 99]))

        with pytest.raises(TypeError):
            data.join_with_query(func, params=dict(a=4))

    def test_hybrid_condition(self):
        """ check that parameters cant be used in code branching """

        def func(a):
            d = otp.Ticks(dict(y=[1, 2]))

            if a > 3:
                d.b = a

            return d

        data = otp.Ticks(dict(x=[2, 3]))

        with pytest.raises(TypeError):
            data.join_with_query(func, params=dict(a=2))


class TestInner:
    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session):
        yield c_session

    def test_empty(self):
        """ check inner join with empty result """

        def func():
            joined = otp.Ticks(dict(y=[-0.5, -1.2]))
            res, _ = joined[(joined.y > 0)]
            return res

        data = otp.Ticks(dict(x=[1, 2]))
        res = data.join_with_query(func, how="inner")
        df = otp.run(res)
        assert len(df) == 0

    def test_non_empty(self):
        """ check inner join with non empty result """

        def func():
            return otp.Ticks(dict(y=[-0.5, -1.2]))

        data = otp.Ticks(dict(x=[1, 2]))
        res = data.join_with_query(func, how="inner")
        df = otp.run(res)
        assert len(df) == 4


class TestSymbolParam:
    @pytest.fixture(scope="class")
    def db(self):
        db = otp.DB(name="TESTSYMBOLPARAM")
        db.add(src=otp.Ticks(dict(A=[1, 2, 3, 4], offset=[0, 0, 2, 4])))
        yield db

    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session, db):
        c_session.use(db)
        yield c_session

    def test_simple(self):
        def func(symbol):
            d = otp.Ticks(dict(y=[-0.5, symbol.Y]))
            d.b = symbol.B
            return d

        data = otp.Ticks(dict(x=[1, 2], z=[3, 4]))
        res = data.join_with_query(func, how="inner", symbol=dict(Y=data.x, B=data.z))
        assert res["b"].dtype is int
        assert res["y"].dtype is float
        df = otp.run(res)
        assert df["x"][0] == 1 and df["z"][0] == 3 and df["y"][0] == -0.5 and df["b"][0] == 3

    def test_no_symbol(self):
        d = otp.Ticks(dict(y=[-1]))

        data = otp.Ticks(dict(x=[1, 2], z=[3, 4], s=["a", "b"]))
        res = data.join_with_query(d, how="inner")
        df = otp.run(res)
        assert all(df["y"] == [-1, -1])

    def test_start_time(self, db):
        d = otp.DataSource(db)
        data = otp.Ticks(dict(x=[1, 2]))
        start = datetime(2003, 12, 1, 0, 0, 0, 2000, tzinfo=pytz.timezone("EST5EDT"))
        res = data.join_with_query(d, how="inner", start=start)
        res = otp.run(res)
        assert len(res) == 4  # only tick with 2 and 4 offset is selected

    def test_end_time(self, db):
        d = otp.DataSource(db)
        data = otp.Ticks(dict(x=[1, 2]))
        end = datetime(2003, 12, 1, 0, 0, 0, 3000, tzinfo=pytz.timezone("EST5EDT"))
        res = data.join_with_query(d, how="inner", end=end)
        res = otp.run(res)
        assert len(res) == 6  # only tick with 0 and 2 offset is selected

    @pytest.mark.parametrize('timeclass', [datetime, otp.dt])
    def test_start_and_end_time(self, db, timeclass):
        d = otp.DataSource(db)
        data = otp.Ticks(dict(x=[1, 2]))
        start = timeclass(2003, 12, 1, 0, 0, 0, 1000, tzinfo=pytz.timezone("EST5EDT"))
        end = timeclass(2003, 12, 1, 0, 0, 0, 3000, tzinfo=pytz.timezone("EST5EDT"))
        res = data.join_with_query(d, how="inner", start=start, end=end)
        res = otp.run(res)
        assert len(res) == 2  # only tick with 2 offset is selected

    def test_expression_in_symbol_params(self):
        def func(symbol):
            d = otp.Ticks(dict(x=[-1]))
            d = d.update(dict(x=1), where=(symbol.s % 2 == 1))
            return d

        data = otp.Ticks(dict(a=[1, 2], b=[2, 4]))
        res = data.join_with_query(func, how="inner", symbol=dict(s=data["a"] + data["b"]))
        df = otp.run(res)
        assert all(df["x"] == [1, -1])

    def test_symbol_after_table_wasnot_drop(self):
        d = otp.Ticks(dict(y=[-1]))
        d = d.update(dict(y=1), where=(d.Symbol.name == "a"))
        data = otp.Ticks(dict(x=[1, 2], s=["a", "b"]))
        res = data.join_with_query(d, how="inner", symbol=data.s)
        df = otp.run(res)
        res.Symbol.name  # should exist
        data.Symbol.name
        d.Symbol.name
        otp.run(res)
        otp.run(data)
        otp.run(d)
        assert all(df["y"] == [1, -1])

    def test_params(self):
        def func(symbol, pref, post):
            d = otp.Ticks(dict(type=["six"]))
            d = d.update(dict(type="three"), where=(symbol.name == "3"))  # symbol is always converted to string
            d["type"] = pref + d["type"] + post
            return d

        data = otp.Ticks(dict(a=[1, 2], b=[2, 4], pref="_^"))
        res = data.join_with_query(
            func, how="inner", symbol=(data.a + data.b), params=dict(pref=data["pref"] + ".", post="$")
        )
        df = otp.run(res)
        assert all(df["type"] == ["_.three$", "^.six$"])

    @pytest.mark.parametrize(
        'pass_symbol', [True, False]
    )
    def test_source_as_symbol_param(self, pass_symbol):
        data = otp.Ticks(X=[1, 2], Y=[3, 4], Z=[5, 6])

        def source_func(symbol):
            src = otp.Tick(A=1)
            src['A'] = src['A'] * symbol['X']
            src['B'] = symbol['Y'] - symbol['Z']
            return src

        data = data.join_with_query(
            source_func,
            symbol=('AAPL', data) if pass_symbol else data
        )
        df = otp.run(data)
        assert df['A'][0] == 1
        assert df['A'][1] == 2
        assert df['B'][0] == -2
        assert df['B'][0] == -2

    @pytest.mark.parametrize(
        'pass_symbol', [True, False]
    )
    def test_symbol_as_symbol_param(self, pass_symbol):

        def src_func(symbol):
            src = otp.Tick(C=3)
            src['D'] = symbol['SD']
            return src

        def data_func(symbol):
            data = otp.Tick(A=1, B=2)
            data = data.join_with_query(src_func,
                                        symbol=('AAPL', symbol) if pass_symbol else symbol
                                        )
            return data

        smb = otp.Tick(SD=10, SYMBOL_NAME='AAPL')
        df = otp.run(data_func, symbols=smb)['AAPL']
        assert df['D'][0] == 10

    @pytest.mark.parametrize(
        'symbol_param_source',
        [
            'source',
            'symbol',
        ]
    )
    def test_source_as_symbol_param_types(self, symbol_param_source):
        data = otp.Tick(
            A=1,
            B=1.5,
            C='ABC',
            D='ABC' * 100,
            E=otp.datetime(2020, 1, 1, 2, 3, 4),
            F=otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Milli(123),
            G=otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Nano(1),
            H=otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Nano(123),
            I=otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Nano(999999),
            SYMBOL_NAME='AAPL'
        )
        data['J'] = data['E'], otp.msectime
        data['K'] = data['F'], otp.msectime

        def source_func(symbol):
            src = otp.Tick(DUMMY=1)
            src['JA'] = symbol['A']
            src['JB'] = symbol['B']
            src['JC'] = symbol['C']
            src['JD'] = symbol['D']
            src['JE'] = symbol['E']
            src['JF'] = symbol['F']
            src['JG'] = symbol['G']
            src['JH'] = symbol['H']
            src['JI'] = symbol['I']
            src['JJ'] = symbol['J']
            src['JK'] = symbol['K']
            return src

        if symbol_param_source == 'source':
            data = data.join_with_query(source_func, symbol=data)
            df = otp.run(data)
        elif symbol_param_source == 'symbol':

            def func(symbol):
                src = otp.Tick(DUMMY2=1)
                src = src.join_with_query(source_func, symbol=symbol)
                return src

            df = list(otp.run(func, symbols=data).values())[0]

        else:
            assert False, 'unknown value of "symbol_param_source" test parameter'

        assert df['JA'][0] == 1
        assert df['JB'][0] == 1.5
        assert df['JC'][0] == 'ABC'
        assert df['JD'][0] == 'ABC' * 100
        assert df['JE'][0] == otp.datetime(2020, 1, 1, 2, 3, 4)
        assert df['JF'][0] == otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Milli(123)
        assert df['JG'][0] == otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Nano(1)
        assert df['JH'][0] == otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Nano(123)
        assert df['JI'][0] == otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Nano(999999)
        assert df['JJ'][0] == otp.datetime(2020, 1, 1, 2, 3, 4)
        assert df['JK'][0] == otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Milli(123)

    @pytest.mark.parametrize(
        'symbol_param_source',
        [
            'source',
            'symbol',
        ]
    )
    def test_ignore_special_fields_in_symbol_params(self, symbol_param_source):
        data = otp.Tick(
            A=1,
        )
        data['B'] = data['_START_TIME'] + otp.Nano(data['A'])
        data['_PARAM_START_TIME_NANOS'] = data['_START_TIME'] + otp.Nano(2)
        data['_PARAM_END_TIME_NANOS'] = data['_END_TIME'] + otp.Nano(3)
        data['SYMBOL_NAME'] = 'AAPL'

        def source_func(symbol):
            src = otp.Tick(DUMMY=1)
            src['JOIN_B'] = symbol['B']
            src['JOIN_ST'] = src['_START_TIME']
            src['JOIN_ET'] = src['_END_TIME']
            return src

        if symbol_param_source == 'source':
            with pytest.warns(FutureWarning, match='This parameter would be ignored.'):
                data = data.join_with_query(source_func, symbol=data)
            df = otp.run(data, start=otp.datetime(2022, 1, 2), end=otp.datetime(2022, 2, 3))
        elif symbol_param_source == 'symbol':

            def func(symbol):
                src = otp.Tick(DUMMY2=1)
                # here, _PARAM_START_TIME_NANOS and _PARAM_END_TIME_NANOS in the FSQ
                # will override start time and end time in the main query
                # but we need to check that they will not also override start and end time in the joined query
                # if other values of start/end are explicitly passed
                with pytest.warns(FutureWarning, match='This parameter would be ignored.'):
                    src = src.join_with_query(source_func, symbol=symbol,
                                              start=data['_START_TIME'] - otp.Nano(2),
                                              end=data['_END_TIME'] - otp.Nano(3))
                return src

            df = list(otp.run(func,
                              symbols=data,
                              start=otp.datetime(2022, 1, 2),
                              end=otp.datetime(2022, 2, 3)).values())[0]
        else:
            assert False, 'unknown value of "symbol_param_source" test parameter'

        assert df['JOIN_B'][0] == otp.datetime(2022, 1, 2, nanosecond=1)
        assert df['JOIN_ST'][0] == otp.datetime(2022, 1, 2)
        assert df['JOIN_ET'][0] == otp.datetime(2022, 2, 3)

    def test_time_query_params(self):
        data = otp.Tick(
            E=otp.datetime(2020, 1, 1, 2, 3, 4),
            F=otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Milli(123),
            G=otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Nano(1),
            H=otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Nano(123),
            I=otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Nano(999999),
            SYMBOL_NAME='AAPL'
        )
        data['J'] = data['E'], otp.msectime
        data['K'] = data['F'], otp.msectime

        def source_func(param_e, param_f, param_g, param_h, param_i,
                        param_j, param_k):
            src = otp.Tick(DUMMY=1)
            src['JE'] = param_e
            src['JF'] = param_f
            src['JG'] = param_g
            src['JH'] = param_h
            src['JI'] = param_i
            src['JJ'] = param_j
            src['JK'] = param_k
            return src

        data = data.join_with_query(source_func,
                                    params=dict(
                                        param_e=data['E'],
                                        param_f=data['F'],
                                        param_g=data['G'],
                                        param_h=data['H'],
                                        param_i=data['I'],
                                        param_j=data['J'],
                                        param_k=data['K'],
                                    ))
        df = otp.run(data)
        assert df['JE'][0] == otp.datetime(2020, 1, 1, 2, 3, 4)
        assert df['JF'][0] == otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Milli(123)
        assert df['JG'][0] == otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Nano(1)
        assert df['JH'][0] == otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Nano(123)
        assert df['JI'][0] == otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Nano(999999)
        assert df['JJ'][0] == otp.datetime(2020, 1, 1, 2, 3, 4)
        assert df['JK'][0] == otp.datetime(2020, 1, 1, 2, 3, 4) + otp.Milli(123)

    @pytest.mark.parametrize('symbol_param_form', ['dict', 'source'])
    def test_jwq_on_branch_with_empty_symbol_name(self, symbol_param_form):
        # see BDS-263
        symbol_source = otp.Ticks(SYMBOL_NAME=['AAA', 'BBB'])

        data_source = otp.Tick(A=1)
        data_source = otp.merge([data_source], symbols=symbol_source)
        # after this point, symbol in the data_source graph should be empty
        data_source = data_source.first()

        def jwq_func(symbol):
            src = otp.Tick(C=2)
            src['SP'] = symbol['A']
            return src

        data_source['SN'] = data_source['_SYMBOL_NAME']
        if symbol_param_form == 'dict':
            symbol = dict(A=data_source['A'])
        elif symbol_param_form == 'source':
            symbol = data_source
        data_source = data_source.join_with_query(jwq_func, symbol=symbol)
        df = otp.run(data_source)
        assert len(df) == 1
        assert df['SN'][0] == ''
        assert df['SP'][0] == 1


class TestTimezone:

    @pytest.mark.parametrize(
        'tz_jwq,tz_main,tz_expected',
        [
            (None, 'GMT', 'GMT'),
            (None, 'EST5EDT', 'EST5EDT'),
            (None, 'Europe/Moscow', 'Europe/Moscow'),
            ('GMT', 'EST5EDT', 'GMT'),
            ('Europe/Moscow', 'EST5EDT', 'Europe/Moscow'),
            ('Europe/Moscow', 'GMT', 'Europe/Moscow'),
        ]
    )
    def test_timezone(self, c_session, tz_jwq, tz_main, tz_expected):
        """
        Test that timezone of the main query is used by default
        """
        jwq_src = otp.Tick(B=2)
        jwq_src['JWQ_TZ'] = jwq_src['_TIMEZONE']

        main_src = otp.Tick(A=1)
        main_src = main_src.join_with_query(
            jwq_src,
            timezone=tz_jwq
        )
        df = otp.run(main_src, timezone=tz_main)
        assert df['JWQ_TZ'][0] == tz_expected

    def test_times(self, c_session):
        joined_query = otp.Tick(JOINED_START_TIME=otp.meta_fields.start_time,
                                JOINED_END_TIME=otp.meta_fields.end_time,
                                JOINED_TIMEZONE=otp.meta_fields.timezone)
        main_query = otp.Tick(MAIN_START_TIME=otp.meta_fields.start_time,
                              MAIN_END_TIME=otp.meta_fields.end_time,
                              MAIN_TIMEZONE=otp.meta_fields.timezone)

        # default works
        data = main_query.join_with_query(joined_query)
        df = otp.run(data, start=otp.dt(2020, 1, 1), end=otp.dt(2020, 1, 4), timezone='EST5EDT')
        assert df['JOINED_START_TIME'][0] == otp.dt(2020, 1, 1)
        assert df['JOINED_END_TIME'][0] == otp.dt(2020, 1, 4)
        assert df['JOINED_TIMEZONE'][0] == 'EST5EDT'
        assert df['JOINED_START_TIME'][0] == df['MAIN_START_TIME'][0]
        assert df['JOINED_END_TIME'][0] == df['MAIN_END_TIME'][0]
        assert df['JOINED_TIMEZONE'][0] == df['MAIN_TIMEZONE'][0]

        # works if passed as an otp.Operation
        data = main_query.join_with_query(
            joined_query,
            start=otp.raw('PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J","2020-01-02 00:00:00.000000000",_TIMEZONE)',
                          dtype=otp.nsectime),
            end=otp.raw('PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J","2020-01-03 00:00:00.000000000",_TIMEZONE)',
                        dtype=otp.nsectime),
            timezone='EST5EDT'
        )
        df = otp.run(data, start=otp.dt(2020, 1, 1), end=otp.dt(2020, 1, 4), timezone='EST5EDT')
        assert df['JOINED_START_TIME'][0] == otp.dt(2020, 1, 2)
        assert df['JOINED_END_TIME'][0] == otp.dt(2020, 1, 3)
        assert df['JOINED_TIMEZONE'][0] == 'EST5EDT'
        assert df['MAIN_START_TIME'][0] == otp.dt(2020, 1, 1)
        assert df['MAIN_END_TIME'][0] == otp.dt(2020, 1, 4)
        assert df['JOINED_TIMEZONE'][0] == 'EST5EDT'

        # doesn't work if passed as otp.dt objects
        data = main_query.join_with_query(joined_query,
                                          start=otp.dt(2020, 1, 2), end=otp.dt(2020, 1, 3),
                                          timezone='EST5EDT')
        df = otp.run(data, start=otp.dt(2020, 1, 1), end=otp.dt(2020, 1, 4), timezone='EST5EDT')
        assert df['JOINED_START_TIME'][0] == otp.dt(2020, 1, 2)
        assert df['JOINED_END_TIME'][0] == otp.dt(2020, 1, 3)
        assert df['JOINED_TIMEZONE'][0] == 'EST5EDT'
        assert df['MAIN_START_TIME'][0] == otp.dt(2020, 1, 1)
        assert df['MAIN_END_TIME'][0] == otp.dt(2020, 1, 4)
        assert df['JOINED_TIMEZONE'][0] == 'EST5EDT'


class TestWithBoundUnboundCases:
    @pytest.fixture(scope="class")
    def db_a(self):
        db = otp.DB(name="DB_A")

        db.add(otp.Ticks(dict(X=[1, 2, 3])), symbol="S1", tick_type="TT")
        db.add(otp.Ticks(dict(X=[-3, -2, -1])), symbol="S2", tick_type="TT")
        db.add(otp.Ticks(dict(X=[-3, -2, -1])), symbol="S3", tick_type="TT3")

        yield db

    @pytest.fixture(scope="class")
    def db_b(self):
        db = otp.DB(name="DB_B")
        db.add(otp.Ticks(dict(X=[6, 7, 8])), symbol="S2", tick_type="TT")

        yield db

    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session, db_a, db_b):
        c_session.use(db_a, db_b)
        yield c_session

    def test_bound_only(self):
        def callback():
            data = otp.DataSource(db="DB_A", tick_type="TT")
            return data

        def other_query():
            data = otp.merge([callback], symbols=otp.Symbols("DB_A"))
            return data

        some_data = otp.Ticks(dict(Y=[0.5]))
        some_data = some_data.join_with_query(other_query)

        df = otp.run(some_data)

        assert len(df) == 6

    def test_bound_and_unbound(self):
        def other_query():
            data = otp.DataSource(db="DB_A", tick_type="TT", symbol=otp.Symbols("DB_A"))
            data += otp.Tick(X=77)
            return data

        some_data = otp.Ticks(dict(Y=[0.5]))
        some_data = some_data.join_with_query(other_query)

        df = otp.run(some_data)

        assert len(df) == 7

    def test_db_as_symbol_param(self):
        def other_query(symbol):
            return otp.DataSource(db=symbol.DB, tick_type="TT")

        def logic(symbol):
            some_data = otp.Ticks(dict(Y=[0.5]))
            some_data["DB"] = symbol["DB"]
            some_data["SYMB"] = symbol.name

            some_data = some_data.join_with_query(other_query, symbol=dict(DB=symbol["DB"]))
            return some_data

        symbols = otp.Ticks(dict(SYMBOL_NAME=["S1", "S2"], DB=["DB_A", "DB_B"]))
        dfs = otp.run(logic, symbols=symbols)

        assert set(dfs.keys()) == {"S1", "S2"}
        assert all(dfs["S1"]["X"] == [1, 2, 3])
        assert all(dfs["S2"]["X"] == [6, 7, 8])
        assert all(dfs["S1"]["SYMB"] == "S1")
        assert all(dfs["S2"]["SYMB"] == "S2")
        assert all(dfs["S1"]["DB"] == "DB_A")
        assert all(dfs["S2"]["DB"] == "DB_B")

    def test_tt_as_param(self):
        ''' Check that tick type can be passed as a parameter to the query '''

        def logic(param):
            return otp.DataSource(db='DB_A', symbol='S1', tick_type=param)

        data = otp.Ticks(TICK_TYPE=['TT', 'TX'])

        data = data.join_with_query(logic, params=dict(param=data['TICK_TYPE']))
        df = otp.run(data)
        assert len(df) == 4
        assert all(df['X'] == [1, 2, 3, 0])
        assert all(df['TICK_TYPE'] == ['TT', 'TT', 'TT', 'TX'])

    def test_inside_bound_symbol(self):
        def other_query():
            return otp.DataSource(db="DB_A", tick_type="TT")

        def query(symbol):
            some_data = otp.Ticks(dict(Y=[0.5]))
            some_data["SYMB"] = symbol.name

            some_data = some_data.join_with_query(other_query)
            return some_data

        def main(symbol):
            res = otp.merge([query], symbols=["S1", "S2"])
            res += otp.Tick(X=99)
            res["FS_SYMB"] = symbol.name
            return res

        symbols = otp.Ticks(dict(SYMBOL_NAME=["A", "B"]))
        dfs = otp.run(main, symbols=symbols)

        assert set(dfs.keys()) == {"A", "B"}
        for key, df in dfs.items():
            assert len(df) == 7
            expected = pd.Series([1, 2, 3, -3, -2, -1, 99])
            assert all(df["X"] == expected)
            expected = pd.Series([0.5] * 6 + [None])
            assert all((df["Y"] == expected) | df["Y"].isna() & expected.isna())
            assert all(df["FS_SYMB"] == key)
            expected = pd.Series(["S1"] * 3 + ["S2"] * 3 + [""])
            assert all(df["SYMB"] == expected)


class TestPrefix:
    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session):
        yield c_session

    def test_const_prefix(self):
        data = otp.Ticks(dict(a=["a", "b"]))
        query = otp.Ticks(dict(x=[1]))
        data = data.join_with_query(query, prefix="pref_")
        df = otp.run(data)
        assert all(df["pref_x"] == [1, 1])
        assert "pref_TIMESTAMP" in df

    def test_error_on_not_const(self):
        data = otp.Ticks(dict(a=["a", "b"]))
        query = otp.Ticks(dict(x=[1]))
        with pytest.raises(ValueError):
            data.join_with_query(query, prefix=data["a"])
        with pytest.raises(ValueError):
            data.join_with_query(query, prefix=data["a"] + ".")

    def test_error_on_not_unique_after_prefix(self):
        data = otp.Ticks(dict(ax=["a", "b"]))
        query = otp.Ticks(dict(x=[1]))
        with pytest.raises(ValueError):
            data.join_with_query(query, prefix="a")

    def test_error_on_not_unique_without_prefix(self):
        with pytest.raises(ValueError):
            otp.Tick(A=1).join_with_query(otp.Tick(A=2), prefix=None)

    def test_columns_with_prefix_after_jwq(self):
        assert 'PREFIX.A' in otp.Tick(A=1).join_with_query(otp.Tick(A=2), prefix='PREFIX.').columns()


class TestCachingAndConcurrency:
    @pytest.fixture(scope="class")
    def db(self):
        db = otp.DB(name="TESTCACHINGANDCONCURRENCY")
        db.add(src=otp.Ticks(dict(PRICE=[1, 2, 3, 4])), symbol="A", tick_type="TT")
        db.add(src=otp.Ticks(dict(PRICE=[-1, -2, -3, -4])), symbol="B", tick_type="TT")
        yield db

    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session, db):
        c_session.use(db)
        yield c_session

    @pytest.mark.parametrize("caching", ["per_symbol", "cross_symbol"])
    def test_caching_correct_values(self, caching):
        # just check that query executed successfully
        data = otp.Ticks(dict(a=["a", "b"]))
        query = otp.Ticks(dict(x=[1]))
        data = data.join_with_query(query, caching=caching)
        df = otp.run(data)
        assert len(df) == 2

    @pytest.mark.parametrize("concurrency", [1, 10])
    def test_concurrency_correct_values(self, concurrency):
        # just check that query executed successfully
        data = otp.Ticks(dict(a=["a", "b"]))
        query = otp.Ticks(dict(x=[1]))
        data = data.join_with_query(query, concurrency=concurrency)
        df = otp.run(data)
        assert len(df) == 2

    @pytest.mark.parametrize("concurrency", [-1, 'a'])
    def test_concurrency_incorrect_values(self, concurrency):
        data = otp.Ticks(dict(a=["a", "b"]))
        query = otp.Ticks(dict(x=[1]))
        with pytest.raises(ValueError):
            data.join_with_query(query, concurrency=concurrency)

    @pytest.mark.parametrize("caching", ["symbol", 2])
    def test_caching_incorrect_values(self, caching):
        data = otp.Ticks(dict(a=["a", "b"]))
        query = otp.Ticks(dict(x=[1]))
        with pytest.raises(ValueError):
            data = data.join_with_query(query, caching=caching)

    @pytest.mark.parametrize('caching', (None, 'per_symbol', 'cross_symbol'))
    def test_caching(self, caching):
        t = otp.Ticks(A=['1', '1', '1', '2', '2', '2', '3', '3'])
        other = otp.Tick(B=otp.math.rand(1, 1_000_000_000))
        t = t.join_with_query(other, symbol=t['A'], caching=caching, process_query_async=False)
        df = otp.run(t)
        if not caching:
            assert len(set(df['B'])) == 8
        if caching == 'per_symbol':
            assert len(set(df['B'])) == 3
        if caching == 'cross_symbol':
            assert len(set(df['B'])) == 1


class TestKeepTime:
    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session):
        yield c_session

    def test_drop(self):
        data = otp.Ticks(dict(a=["a", "b"]))
        query = otp.Ticks(dict(x=[1]))
        data = data.join_with_query(query)
        df = otp.run(data)
        assert len(df.columns) == 3
        assert "TIMESTAMP" not in df.columns

    def test_rename(self):
        data = otp.Ticks(dict(a=["a", "b"]))
        query = otp.Ticks(dict(x=[1]))
        data = data.join_with_query(query, keep_time="ORIG_TIME")
        df = otp.run(data)
        assert len(df.columns) == 4
        assert "ORIG_TIME" in df.columns

    def test_error_on_timestamp(self):
        data = otp.Ticks(dict(a=["a", "b"]))
        query = otp.Ticks(dict(x=[1]))
        with pytest.raises(ValueError):
            data.join_with_query(query, keep_time="TIMESTAMP")

    def test_error_on_presented(self):
        data = otp.Ticks(dict(a=["a", "b"]))
        query = otp.Ticks(dict(x=[1]))
        with pytest.raises(ValueError):
            data.join_with_query(query, keep_time="a")


class TestStartEnd:
    def test_expr(self, c_session):
        def query(symbol):
            result = otp.Ticks(dict(X=[1]))
            result["S"] = result._START_TIME
            result["E"] = result._END_TIME
            return result

        create_time = partial(otp.datetime, tzinfo=dateutil.tz.gettz("GMT"))
        time_ts = [create_time(2017, 1, 3, 8), create_time(2019, 1, 3, 9)]
        data = otp.Ticks(dict(A=["a", "b"], TIME_ST=time_ts))
        source = data.join_with_query(query, start=data.TIMESTAMP - otp.Second(10), end=data.TIME_ST)
        df = otp.run(source, timezone="GMT")
        start_ts = otp.config['default_start_time'] - otp.Second(10)
        assert all(df["S"] == [start_ts, start_ts + otp.Milli(1)])
        assert all(a.timestamp() == b.timestamp() for a, b in zip(df["E"], time_ts))


class TestWhere:
    def test_simple_positive(self, c_session):
        def func():
            return otp.Ticks(dict(y=[5.6]))

        data = otp.Ticks(dict(x=[2]))
        source = data.join_with_query(func, where=data['x'] == 2)
        df = otp.run(source)
        assert len(df) == 1
        assert all(df['x'] == [2])
        assert all(df['y'] == [5.6])

    def test_simple_negative(self, c_session):
        def func():
            return otp.Ticks(dict(y=[5.6, 8.9]))

        data = otp.Ticks(dict(x=[2]))
        source = data.join_with_query(func, where=data['x'] == 1)
        df = otp.run(source)
        # only one default tick matched
        assert len(df) == 1
        assert all(df['x'] == [2])
        assert np.isnan(df['y'][0])

    def test_multiple(self, c_session):
        def func():
            return otp.Ticks(y=[1, 2])

        data = otp.Ticks(dict(x=[1, 2]))
        source = data.join_with_query(func, where=data['x'] > 1)
        df = otp.run(source)
        assert len(df) == 3
        assert all(df['y'] == [0, 1, 2])
        assert all(df['x'] == [1, 2, 2])

    def test_not_outer(self, c_session):
        def func():
            return otp.Ticks(y=[1, 2])

        with pytest.raises(ValueError):
            data = otp.Ticks(dict(x=[1, 2]))
            data.join_with_query(func, how='inner', where=data['x'] > 1)


class TestDefaultFields:
    def test_where_int_val(self, c_session):
        main_src = otp.Ticks(A=[1, 2])
        main_src = main_src.join_with_query(
            otp.Tick(B=2),
            where=main_src['A'] == 1,
            default_fields_for_outer_join=dict(B=3)
        )
        df = otp.run(main_src)
        assert df['B'][0] == 2
        assert df['B'][1] == 3

    def test_where_int_expr(self, c_session):
        main_src = otp.Ticks(A=[1, 2])
        main_src = main_src.join_with_query(
            otp.Tick(B=2),
            where=main_src['A'] == 1,
            default_fields_for_outer_join=dict(B=main_src['A'] + 4)
        )
        df = otp.run(main_src)
        assert df['B'][0] == 2
        assert df['B'][1] == 6

    def test_where_str_val(self, c_session):
        main_src = otp.Ticks(A=[1, 2])
        main_src = main_src.join_with_query(
            otp.Tick(B='ABC'),
            where=main_src['A'] == 1,
            default_fields_for_outer_join=dict(B='BCA')
        )
        df = otp.run(main_src)
        assert df['B'][0] == 'ABC'
        assert df['B'][1] == 'BCA'

    def test_where_str_expr(self, c_session):
        main_src = otp.Ticks(A=[1, 2])
        main_src = main_src.join_with_query(
            otp.Tick(B='ABC'),
            where=main_src['A'] == 1,
            default_fields_for_outer_join=dict(B=main_src['A'].apply(str) + 'BCA')
        )
        df = otp.run(main_src)
        assert df['B'][0] == 'ABC'
        assert df['B'][1] == '2BCA'

    def test_where_nsectime_val(self, c_session):
        main_src = otp.Ticks(A=[1, 2])
        main_src = main_src.join_with_query(
            otp.Tick(B=otp.datetime(2022, 1, 2)),
            where=main_src['A'] == 1,
            default_fields_for_outer_join=dict(B=otp.datetime(2022, 2, 3))
        )
        df = otp.run(main_src)
        assert df['B'][0] == otp.datetime(2022, 1, 2)
        assert df['B'][1] == otp.datetime(2022, 2, 3)

    def test_where_nsectime_expr(self, c_session):
        main_src = otp.Ticks(A=[1, 2])
        main_src = main_src.join_with_query(
            otp.Tick(B=otp.datetime(2022, 1, 2)),
            where=main_src['A'] == 1,
            default_fields_for_outer_join=dict(B=main_src['_START_TIME'] + otp.Second(1))
        )
        df = otp.run(main_src)
        assert df['B'][0] == otp.datetime(2022, 1, 2)
        assert df['B'][1] == otp.config.default_start_time + otp.Second(1)

    def test_outer_join_float_val(self, c_session):
        main_src = otp.Tick(A=1)
        main_src = main_src.join_with_query(
            otp.Empty(schema={'B': float}),
            default_fields_for_outer_join=dict(B=12.34)
        )
        df = otp.run(main_src)
        assert df['B'][0] == pytest.approx(12.34)

    def test_outer_join_float_expr(self, c_session):
        main_src = otp.Tick(A=1)
        main_src = main_src.join_with_query(
            otp.Empty(schema={'B': float}),
            default_fields_for_outer_join=dict(B=main_src['A'] + 12.34)
        )
        df = otp.run(main_src)
        assert df['B'][0] == pytest.approx(13.34)

    def test_fail_for_inner_join(self, c_session):
        main_src = otp.Tick(A=1)
        with pytest.raises(ValueError):
            main_src.join_with_query(
                otp.Empty(schema={'B': float}),
                how='inner',
                default_fields_for_outer_join=dict(B=main_src['A'] + 12.34)
            )

    def test_fail_field_not_in_schema(self, c_session):
        main_src = otp.Tick(A=1)
        with pytest.raises(KeyError):
            main_src.join_with_query(
                otp.Empty(schema={'B': float}),
                default_fields_for_outer_join=dict(C=main_src['A'] + 12.34)
            )

    def test_multiple_fields(self, c_session):
        main_src = otp.Ticks(A=[1, 2, 3, 4])

        def join_func(param_a):
            src = otp.Tick(B=1, C=0.5, D='XYZ', E=otp.datetime(2022, 1, 1, nanosecond=1))
            src, _ = src[src['B'] == param_a]
            return src

        main_src = main_src.join_with_query(
            join_func,
            params=dict(param_a=main_src['A']),
            default_fields_for_outer_join=dict(
                B=main_src['A'] + 1,
                C=main_src['A'] + 0.33,
                D=main_src['A'].apply(str) + ' XYZ',
                E=main_src['_START_TIME'] + otp.Nano(main_src['A'])
            )
        )
        df = otp.run(main_src)
        assert len(df) == 4
        assert df['B'][0] == 1
        assert df['C'][0] == pytest.approx(0.5)
        assert df['D'][0] == 'XYZ'
        assert df['E'][0] == otp.datetime(2022, 1, 1, nanosecond=1)
        assert df['B'][1] == 3
        assert df['C'][1] == pytest.approx(2.33)
        assert df['D'][1] == '2 XYZ'
        assert df['E'][1] == otp.config.default_start_time + otp.Nano(2)
        assert df['B'][2] == 4
        assert df['C'][2] == pytest.approx(3.33)
        assert df['D'][2] == '3 XYZ'
        assert df['E'][2] == otp.config.default_start_time + otp.Nano(3)
        assert df['B'][3] == 5
        assert df['C'][3] == pytest.approx(4.33)
        assert df['D'][3] == '4 XYZ'
        assert df['E'][3] == otp.config.default_start_time + otp.Nano(4)

    def test_prefix(self, c_session):
        main_src = otp.Ticks(A=[1, 2])
        main_src = main_src.join_with_query(
            otp.Tick(B=2),
            where=main_src['A'] == 1,
            default_fields_for_outer_join=dict(B=3),
            prefix='P_'
        )
        df = otp.run(main_src)
        assert df['P_B'][0] == 2
        assert df['P_B'][1] == 3


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='Binaries (reference_data_loader.exe) is not available in WebAPI')
class TestSymbolTime:
    base_db_name = 'BASE_DB'
    der_db_name = 'DER_DB'
    ref_db_name = 'REF_DATA'
    base_symbology = 'BASE'
    derived_symbology = 'DER'

    @pytest.fixture(scope='class', autouse=True)
    def symbol_time_session(self):
        with otp.Session(otp.Config()) as session:
            ref_db = otp.RefDB(
                self.ref_db_name,
                db_properties={
                    'symbology': self.base_symbology,
                },
            )

            ref_db.put([
                otp.RefDB.SymbolNameHistory('SYMB||20220501000000|20220530000000', self.base_symbology),
                otp.RefDB.SymbologyMapping('SYMB||20220501000000|20220515000000|SYMB_1|',
                                           source_symbology=self.base_symbology,
                                           dest_symbology=self.derived_symbology),
                otp.RefDB.SymbologyMapping('SYMB||20220515000000|20220530000000|SYMB_2|',
                                           source_symbology=self.base_symbology,
                                           dest_symbology=self.derived_symbology),
            ])
            session.use(ref_db)

            base_db = otp.DB(
                self.base_db_name,
                db_properties={
                    'ref_data_db': self.ref_db_name,
                    'symbology': self.base_symbology,
                },
            )
            der_db = otp.DB(
                self.der_db_name,
                db_properties={
                    'ref_data_db': self.ref_db_name,
                    'symbology': self.derived_symbology,
                },
            )
            session.use(base_db, der_db)

            base_db.add(otp.Ticks(PRICE=[0.0733]),
                        tick_type='TRD',
                        symbol='SYMB',
                        date=otp.dt(2022, 5, 10))
            yield session

    def test_symbol_time_constant(self):
        src = otp.Tick(db=self.der_db_name, DUMMY=1)
        jwq_src = otp.DataSource(db=self.base_db_name, tick_type='TRD', schema_policy='manual', schema={'PRICE': float})
        src = src.join_with_query(
            jwq_src,
            prefix='1_',
            symbol_time=otp.dt(2022, 5, 10)
        )
        src = src.join_with_query(
            jwq_src,
            prefix='2_',
            symbol_time=otp.dt(2022, 5, 20)
        )
        df = otp.run(src, symbols=f'{self.der_db_name}::SYMB_1', start=otp.dt(2022, 5, 1), end=otp.dt(2022, 5, 30))
        assert len(df) == 1
        assert df['1_PRICE'][0] == pytest.approx(0.0733)
        assert math.isnan(df['2_PRICE'][0])

    def test_symbol_time_expr(self):
        src = otp.Tick(db=self.der_db_name, DUMMY=1)
        jwq_src = otp.DataSource(db=self.base_db_name, tick_type='TRD', schema_policy='manual', schema={'PRICE': float})
        src = src.join_with_query(
            jwq_src,
            prefix='1_',
            symbol_time=src['_START_TIME'] + otp.Day(10)
        )
        src = src.join_with_query(
            jwq_src,
            prefix='2_',
            symbol_time=src['_START_TIME'] + otp.Day(20)
        )
        df = otp.run(src, symbols=f'{self.der_db_name}::SYMB_1', start=otp.dt(2022, 5, 1), end=otp.dt(2022, 5, 30))
        assert len(df) == 1
        assert df['1_PRICE'][0] == pytest.approx(0.0733)
        assert math.isnan(df['2_PRICE'][0])

    def test_symbol_time_string(self):
        src = otp.Tick(db=self.der_db_name, DUMMY=1)
        jwq_src = otp.DataSource(db=self.base_db_name, tick_type='TRD', schema_policy='manual', schema={'PRICE': float})
        src = src.join_with_query(
            jwq_src,
            prefix='1_',
            symbol_time=(src['_START_TIME'] + otp.Day(10)).dt.strftime("%Y%m%d%H%M%S.%J")
        )
        src = src.join_with_query(
            jwq_src,
            prefix='2_',
            symbol_time='20220521000000'
        )
        df = otp.run(src, symbols=f'{self.der_db_name}::SYMB_1', start=otp.dt(2022, 5, 1), end=otp.dt(2022, 5, 30))
        assert len(df) == 1
        assert df['1_PRICE'][0] == pytest.approx(0.0733)
        assert math.isnan(df['2_PRICE'][0])

    def test_symbol_time_query_param(self):
        # test that passing _SYMBOL_TIME query parameter manually works also
        # this is deprecated but still used in some code
        src = otp.Tick(db=self.der_db_name, DUMMY=1)

        def jwq_func(_SYMBOL_TIME):  # NOSONAR
            jwq_src = otp.DataSource(
                db=self.base_db_name, tick_type='TRD', schema_policy='manual', schema={'PRICE': float},
            )
            return jwq_src

        with pytest.warns(FutureWarning,
                          match='This is deprecated. Please use a dedicated `symbol_time` parameter'):
            src = src.join_with_query(
                jwq_func,
                prefix='1_',
                params=dict(_SYMBOL_TIME=src['_START_TIME'] + otp.Day(10))
            )
            src = src.join_with_query(
                jwq_func,
                prefix='2_',
                params=dict(_SYMBOL_TIME=src['_START_TIME'] + otp.Day(20))
            )
        df = otp.run(src, symbols=f'{self.der_db_name}::SYMB_1', start=otp.dt(2022, 5, 1), end=otp.dt(2022, 5, 30))
        assert len(df) == 1
        assert df['1_PRICE'][0] == pytest.approx(0.0733)
        assert math.isnan(df['2_PRICE'][0])

    def test_symbol_time_symbol_param(self):
        # test that passing _PARAM_SYMBOL_TIME symbol parameter manually works also
        # this is deprecated but still used in some code
        src = otp.Tick(db=self.der_db_name, DUMMY=1)

        def jwq_func(symbol):
            jwq_src = otp.DataSource(
                db=self.base_db_name, tick_type='TRD', schema_policy='manual', schema={'PRICE': float},
            )
            return jwq_src

        with pytest.warns(FutureWarning, match='This is deprecated - please use symbol_time parameter instead'):
            src = src.join_with_query(
                jwq_func,
                prefix='1_',
                symbol=dict(_PARAM_SYMBOL_TIME=src['_START_TIME'] + otp.Day(10))
            )
            src = src.join_with_query(
                jwq_func,
                prefix='2_',
                symbol=dict(_PARAM_SYMBOL_TIME=src['_START_TIME'] + otp.Day(20))
            )
        df = otp.run(src, symbols=f'{self.der_db_name}::SYMB_1', start=otp.dt(2022, 5, 1), end=otp.dt(2022, 5, 30))
        assert len(df) == 1
        assert df['1_PRICE'][0] == pytest.approx(0.0733)
        assert math.isnan(df['2_PRICE'][0])


def test_symbology_mapping_dest_symbology_onetick_param(f_session):
    def func(symbol, dest_symbology):
        _data = otp.SymbologyMapping(dest_symbology=dest_symbology)
        return _data

    data = otp.Ticks(X=[1, 2, 3])
    data = data.join_with_query(
        func,
        params=dict(dest_symbology='OID'),
    )

    tmp_file = TmpFile(suffix='.otq')
    tmp_path = tmp_file.path
    data.to_otq(tmp_path)

    found = False
    with open(tmp_path, 'r') as f:
        for line in f.readlines():
            if re.match(
                r'^.*SYMBOLOGY_MAPPING.*\(.*DEST_SYMBOLOGY.*=.*[\"\']\$dest_symbology[\"\'].*\).*$',
                line.strip(),
            ):
                found = True
                break

    assert found
