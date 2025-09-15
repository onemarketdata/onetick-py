import datetime
import pytest
import pandas as pd
import numpy as np

import onetick.py as otp
import onetick.test as ott


class TestSameScope:
    @pytest.fixture(autouse=True)
    def default_session(self, c_session):
        yield c_session

    def test_single_symbol(self):
        data = otp.Ticks(dict(x=[1, 2, 3]))

        res = otp.run(data, symbols="ABCD")

        assert len(res) == 3
        assert all(res["x"] == [1, 2, 3])

    def test_several_symbols(self):
        data = otp.Ticks(dict(x=[1, 2, 3, 4]))

        res = otp.run(data, symbols=["A", "B", "C"])

        assert len(res) == 3
        assert "A" in res
        assert "B" in res
        assert "C" in res

        for sym in ["A", "B", "C"]:
            assert all(res[sym]["x"] == [1, 2, 3, 4])

    def test_first_stage_as_query(self):
        # ------------
        # first stage query
        symbols = otp.Ticks([["SYMBOL_NAME", "SOME_PARAM"], ["A", 1], ["B", 2]])

        tmp_otq = ott.TmpFile()

        symbols_query_path = symbols.to_otq(tmp_otq, "get_symbols")

        otp.run(symbols)

        # ----------------
        # main query
        data = otp.Ticks(dict(price=[1.3]))

        res = otp.run(data, symbols=otp.query(symbols_query_path))

        assert len(res) == 2
        assert "A" in res
        assert "B" in res

        assert len(res["A"]) == 1
        assert len(res["B"]) == 1

        assert res["A"].price[0] == 1.3
        assert res["B"].price[0] == 1.3

    def test_first_stage_as_eval(self):
        # ------------
        # first stage query
        symbols = otp.Ticks([["SYMBOL_NAME", "SOME_PARAM"], ["A", 1], ["B", 2]])

        tmp_otq = ott.TmpFile()

        symbols_query_path = symbols.to_otq(tmp_otq, "get_symbols")

        # ----------------
        # main query
        data = otp.Ticks(dict(price=[1.3]))

        res = otp.run(data, symbols='eval("' + symbols_query_path + '")')

        assert len(res) == 2
        assert "A" in res
        assert "B" in res

        assert len(res["A"]) == 1
        assert len(res["B"]) == 1

        assert res["A"].price[0] == 1.3
        assert res["B"].price[0] == 1.3

    def test_symbols_as_a_source(self):
        # ------------
        # first stage query
        symbols = otp.Ticks([["SYMBOL_NAME", "SOME_PARAM"], ["A", 1], ["B", 2]])

        # ----------------
        # main query
        data = otp.Ticks(dict(price=[1.3]))

        res = otp.run(data, symbols=symbols)

        assert len(res) == 2
        assert "A" in res
        assert "B" in res

        assert len(res["A"]) == 1
        assert len(res["B"]) == 1

        assert res["A"].price[0] == 1.3
        assert res["B"].price[0] == 1.3


class TestExternal:
    @pytest.fixture(scope="class", autouse=True)
    def default_session(self, c_session):
        yield c_session

    def test_as_function(self):
        def some_logic(symbol):
            return otp.Ticks(dict(price=[1.3]))

        ticks = otp.Ticks([["SYMBOL_NAME"], ["A"], ["B"]])

        dfs = otp.run(some_logic, symbols=ticks)

        assert len(dfs) == 2
        assert "A" in dfs
        assert "B" in dfs

        assert len(dfs["A"]) == 1
        assert len(dfs["B"]) == 1

        assert dfs["A"].price[0] == 1.3
        assert dfs["B"].price[0] == 1.3

    @pytest.mark.parametrize(
        "params",
        [
            [1, 2, -1, 0],
            ["a", "b", "", "d"],
            [0.1, -1.9, otp.nan, 0],
            ["a" * 1000, "", "x" * 1099, "z" * 7],
            [
                pd.Timestamp("2019/01/02 03:04:05.6789", tz="GMT"),
                pd.Timestamp("2007/09/08 07:06:05.432101234", tz="GMT"),
                pd.Timestamp("2019/05/03", tz="GMT"),
                pd.Timestamp("2001/01/01 12:30:00", tz="GMT"),
            ],
        ],
    )
    def test_param_add_column(self, params):
        """
        Validate that columns with different types
        from the first stage query are passed correctly
        and might be used as symbol param to add field
        """

        def some_logic(symbol):
            data = otp.Ticks(dict(x=[1.3]))
            data.y = symbol["PARAM"]
            return data

        symbols = ["A", "B", "C", "D"]

        ticks = otp.Ticks(dict(SYMBOL_NAME=symbols, PARAM=params))

        res = some_logic(otp.Tick(PARAM=params[0]))
        dfs = otp.run(some_logic, symbols=ticks, timezone="GMT")

        if res["y"].dtype is otp.nsectime:
            assert list(map(lambda x: x.tz_localize("GMT"), map(lambda x: dfs[x].y[0], symbols))) == params
        elif res["y"].dtype is float:
            for et_value, res_value in zip(params, map(lambda x: dfs[x].y[0], symbols)):
                if et_value is otp.nan:
                    assert np.isnan(res_value)
                else:
                    assert et_value == res_value
        else:
            assert list(map(lambda x: dfs[x].y[0], symbols)) == params

    @pytest.mark.parametrize(
        "params",
        [
            [1, 2, -1, 0],
            ["a", "b", "", "d"],
            [0.1, -1.9, otp.nan, 0],
            ["a" * 1000, "", "x" * 1099, "z" * 7],
            [
                pd.Timestamp("2019/01/02 03:04:05.6789", tz="GMT"),
                pd.Timestamp("2007/09/08 07:06:05.432101234", tz="GMT"),
                pd.Timestamp("2019/05/03", tz="GMT"),
                pd.Timestamp("2001/01/01 12:30:00", tz="GMT"),
            ],
        ],
    )
    def test_param_in_ticks(self, params):
        """
        Validate that columns with different types
        from the first stage query are passed correctly
        and might be used as symbol param to add field
        """

        def some_logic(symbol):
            data = otp.Ticks(dict(y=[symbol["PARAM"]]))
            data.x = data.y
            return data

        symbols = ["A", "B", "C", "D"]

        ticks = otp.Ticks(dict(SYMBOL_NAME=symbols, PARAM=params))

        res = some_logic(otp.Tick(PARAM=params[0]))
        dfs = otp.run(some_logic, symbols=ticks, timezone="GMT")

        if res["y"].dtype is otp.nsectime:
            assert list(map(lambda x: x.tz_localize("GMT"), map(lambda x: dfs[x].y[0], symbols))) == params
        elif res["y"].dtype is float:
            for et_value, res_value in zip(params, map(lambda x: dfs[x].y[0], symbols)):
                if et_value is otp.nan:
                    assert np.isnan(res_value)
                else:
                    assert et_value == res_value
        else:
            assert list(map(lambda x: dfs[x].y[0], symbols)) == params

    @pytest.mark.parametrize(
        "sym1_params,sym2_params",
        [
            ([-1, 0, 1, 2, 3], [-5, -3, 0, 3, 5]),
            ([-0.5, -1.953, 0, 3.456, 7.1], [0.2, 0.22, -0.222, 0.4444, -0.6666]),
            (["a", "b" * 93, "", "c", "d"], ["x" * 101, "y" * 33, "w" * 99, "u" * 68, ""]),
        ],
    )
    def test_param_in_join_func(self, sym1_params, sym2_params):
        def func(a, b, symbol):
            data = otp.Tick(A=a, Y=symbol.Y)
            data.B = b
            return data

        def some_logic(symbol):
            data = otp.Tick(X=symbol.X)
            data.Z = symbol.Z
            # TODO: check jwq for nanoseconds
            return data.join_with_query(func, params=dict(a=symbol.A, b=symbol.B), symbol=dict(Y=symbol.Y))

        column_names = ["A", "B", "X", "Y", "Z"]

        ticks = otp.Ticks([["SYMBOL_NAME"] + column_names, ["SYM1"] + sym1_params, ["SYM2"] + sym2_params])

        dfs = otp.run(some_logic, symbols=ticks, timezone="GMT")

        assert list(map(lambda c: dfs["SYM1"][c][0], column_names)) == sym1_params
        assert list(map(lambda c: dfs["SYM2"][c][0], column_names)) == sym2_params

    def test_multiple_same_symbols(self):
        """
        Check case when several same symbols, but with different
        symbol params
        """

        def some_logic(symbol):
            data = otp.Tick(x=1)
            data.y = symbol.PARAM
            return data

        ticks = otp.Ticks([["SYMBOL_NAME", "PARAM"], ["A", 1], ["A", 2]])

        dfs = otp.run(some_logic, symbols=ticks)

        assert len(dfs) == 1
        assert "A" in dfs
        assert len(dfs["A"]) == 2
        assert {dfs["A"][0]["y"][0], dfs["A"][1]["y"][0]} == {1, 2}


class TestDates:
    @pytest.fixture(scope="class")
    def db(self):
        """
        date1:  sym A : 2 ticks
                sym B : 3 ticks
        date2:  sym A : 2 ticks
                sym C : 3 ticks
        date3:  sym B : 2 ticks
                sym C : 2 ticks
        """
        date1 = otp.dt(2007, 12, 3)
        date2 = otp.dt(2007, 12, 5)
        date3 = otp.dt(2007, 12, 7)
        tt = "some"

        db = otp.DB(name="TEST_DATES")
        db.tt = "some"
        db.date1 = date1
        db.date2 = date2
        db.date3 = date3

        db.add(src=otp.Ticks(dict(A=[1, 2])), symbol="A", tick_type=tt, date=date1)
        db.add(src=otp.Ticks(dict(B=[3, 4, 5])), symbol="B", tick_type=tt, date=date1)

        db.add(src=otp.Ticks(dict(A=[6, 7])), symbol="A", tick_type=tt, date=date2)
        db.add(src=otp.Ticks(dict(C=[8, 9, 0])), symbol="C", tick_type=tt, date=date2)

        db.add(src=otp.Ticks(dict(B=[5, 7])), symbol="B", tick_type=tt, date=date3)
        db.add(src=otp.Ticks(dict(C=[4, 6])), symbol="C", tick_type=tt, date=date3)

        yield db

    @pytest.fixture(scope="class", autouse=True)
    def default_session(self, c_session, db):
        c_session.use(db)
        yield c_session

    @pytest.mark.parametrize(
        "date,second_symbol,ret_first,ret_second",
        [(otp.dt(2007, 12, 3), "B", [1, 2], [3, 4, 5]), (otp.dt(2007, 12, 5), "C", [6, 7], [8, 9, 0])],
    )
    def test_single_date(self, date, second_symbol, ret_first, ret_second, default_session, db):
        def some_logic(symbol):
            return otp.DataSource(db, tick_type=db.tt)

        symbols = otp.Symbols(db, for_tick_type=db.tt, date=date)
        dfs = otp.run(some_logic, symbols=symbols, date=date)

        assert len(dfs) == 2
        assert "A" in dfs
        assert second_symbol in dfs

        assert len(dfs["A"]) == 2
        assert all(dfs["A"]["A"] == ret_first)

        assert len(dfs[second_symbol]) == 3
        assert all(dfs[second_symbol][second_symbol] == ret_second)

    def test_two_dates(self, db):
        def some_logic(symbol):
            return otp.DataSource(db, tick_type=db.tt)

        symbols = otp.Symbols(db, for_tick_type=db.tt, start=db.date1.start, end=db.date2.end)

        dfs = otp.run(some_logic, symbols=symbols, start=db.date1.start, end=db.date2.end)

        assert len(dfs) == 3
        assert "A" in dfs
        assert "B" in dfs
        assert "C" in dfs

        assert len(dfs["A"]) == 4
        assert all(dfs["A"]["A"] == [1, 2, 6, 7])

        assert len(dfs["B"]) == 3
        assert all(dfs["B"]["B"] == [3, 4, 5])

        assert len(dfs["C"]) == 3
        assert all(dfs["C"]["C"] == [8, 9, 0])

    @pytest.mark.parametrize(
        "border_date,res_a,res_b,res_c",
        [(otp.dt(2007, 12, 3), [1, 2], [3, 4, 5], []), (otp.dt(2007, 12, 5), [6, 7], [], [8, 9, 0])],
    )
    def test_inner_range_narrower(self, border_date, res_a, res_b, res_c, db):
        """
        Check case where date range for the first stage query
        is wider than _source in the logic
        """

        def some_logic(symbol):
            return otp.DataSource(db, tick_type=db.tt, date=border_date)

        symbols = otp.Symbols(db, for_tick_type=db.tt, start=db.date1.start, end=db.date2.end)

        dfs = otp.run(some_logic, symbols=symbols)

        assert len(dfs) == 3

        assert len(dfs["A"]) == len(res_a)
        assert all(dfs["A"]["A"] == res_a)

        assert len(dfs["B"]) == len(res_b)
        if len(dfs["B"]):
            assert all(dfs["B"]["B"] == res_b)

        assert len(dfs["C"]) == len(res_c)
        if len(dfs["C"]):
            assert all(dfs["C"]["C"] == res_c)

    def test_with_two_inner_sources_narrower_ranges(self, db):
        def some_logic(symbol):
            data1 = otp.DataSource(db, tick_type=db.tt, date=db.date1)
            data1.x = 1

            data2 = otp.DataSource(db, tick_type=db.tt, date=db.date2)
            data2.x = 2
            return data1 + data2

        symbols = otp.Symbols(db, for_tick_type=db.tt, start=db.date1.start, end=db.date2.end)

        dfs = otp.run(some_logic, symbols=symbols)

        assert len(dfs) == 3

        assert all(dfs["A"]["A"] == [1, 2, 6, 7])
        assert all(dfs["A"]["x"] == [1, 1, 2, 2])

        assert all(dfs["B"]["B"] == [3, 4, 5])
        assert all(dfs["B"]["x"] == [1, 1, 1])

        assert all(dfs["C"]["C"] == [8, 9, 0])
        assert all(dfs["C"]["x"] == [2, 2, 2])

    @pytest.mark.parametrize(
        "date,res_a,res_b,res_c",
        [(otp.dt(2007, 12, 3), [1, 2, 6, 7], [3, 4, 5], []), (otp.dt(2007, 12, 5), [1, 2, 6, 7], [], [8, 9, 0])],
    )
    def test_inner_range_wider(self, date, res_a, res_b, res_c, db):
        """
        Check case where date range for the first stage is narrower
        than date in range in the logic.

        Logic under the hood calculates the largest interval, but adds
        ad-hoc MODIFY_QUERY_TIMES EPs
        """

        def some_logic(symbol):
            return otp.DataSource(db, tick_type=db.tt, start=db.date1.start, end=db.date2.end)

        symbols = otp.Symbols(db, for_tick_type=db.tt, date=date)

        dfs = otp.run(some_logic, symbols=symbols)

        assert len(dfs) == 2

        assert len(dfs["A"]) == len(res_a)
        assert all(dfs["A"]["A"] == res_a)

        if len(res_b):
            assert len(dfs["B"]) == len(res_b)
            assert all(dfs["B"]["B"] == res_b)

        if len(res_c):
            assert len(dfs["C"]) == len(res_c)
            assert all(dfs["C"]["C"] == res_c)


class TestSymbolName:
    @pytest.fixture(autouse=True)
    def default_session(self, c_session):
        yield c_session

    def test_add_field(self):
        def some_logic(symbol):
            data = otp.Ticks(dict(x=[1, 2, 3]))

            data.symbol = symbol.name

            return data

        ticks = otp.Ticks(dict(SYMBOL_NAME=["A", "B"]))

        dfs = otp.run(some_logic, symbols=ticks)

        assert all(dfs["A"]["symbol"] == ["A", "A", "A"])
        assert all(dfs["B"]["symbol"] == ["B", "B", "B"])

    def test_update_field(self):
        def some_logic(symbol):
            data = otp.Ticks(dict(x=["a", "b"]))
            data.x += symbol.name
            return data

        ticks = otp.Ticks(dict(SYMBOL_NAME=["A", "B"]))

        dfs = otp.run(some_logic, symbols=ticks)

        assert all(dfs["A"]["x"] == ["aA", "bA"])
        assert all(dfs["B"]["x"] == ["aB", "bB"])

    def test_built_in_functions(self):
        def some_logic(symbol):
            data = otp.Ticks(dict(x=["a", "b"]))
            data.x += symbol.name.str.token("_", -1)
            return data

        ticks = otp.Ticks(dict(SYMBOL_NAME=["SYMBOL_A", "SYMBOL_B"]))

        dfs = otp.run(some_logic, symbols=ticks)

        assert all(dfs["SYMBOL_A"]["x"] == ["aA", "bA"])
        assert all(dfs["SYMBOL_B"]["x"] == ["aB", "bB"])

    def test_in_source(self):
        def some_logic(symbol):
            return otp.Ticks(dict(x=[symbol.name + "1", symbol.name + "2"]))

        ticks = otp.Ticks(dict(SYMBOL_NAME=["A", "B"]))

        dfs = otp.run(some_logic, symbols=ticks)

        assert all(dfs["A"]["x"] == ["A1", "A2"])
        assert all(dfs["B"]["x"] == ["B1", "B2"])


class TestCustomField:
    @pytest.fixture(scope="class", autouse=True)
    def default_session(self, c_session):
        yield c_session

    def test_without_collision(self):
        def some_logic(symbol):
            return otp.Ticks(dict(price=[1.3, 0.9]))

        ticks = otp.Ticks(dict(SYMBOL_NAME=["A", "B"]))
        dfs = otp.run(some_logic, symbols=ticks)

        assert all(dfs["A"]["price"] == [1.3, 0.9])
        assert all(dfs["B"]["price"] == [1.3, 0.9])


class TestUnboundAndBound:
    @pytest.fixture(scope="class")
    def db(self, c_session):
        db = otp.DB("TESTUNBOUNDANDBOUND")

        db.add(src=otp.Ticks(dict(A=[1, 2])), symbol="A", tick_type="tA")
        db.add(src=otp.Ticks(dict(B=[3, 4], offset=[2, 3])), symbol="B", tick_type="tB")
        db.add(src=otp.Ticks(dict(C=[5, 6], offset=[4, 5])), symbol="C", tick_type="tC")
        db.add(src=otp.Ticks(dict(A=[7, 8], offset=[6, 7])), symbol="A", tick_type="tA2")
        db.add(src=otp.Ticks(dict(B=[9, 0], offset=[8, 9])), symbol="B", tick_type="tA")

        yield db

    @pytest.fixture(scope="class", autouse=True)
    def default_session(self, c_session, db):
        c_session.use(db)
        yield c_session

    def test_1_unbound_1_bound(self, db):
        def some_logic(symbol):
            unbound1 = otp.DataSource(db, tick_type="tA")
            bound1 = otp.DataSource(db, tick_type="tB", symbol="B")

            return unbound1 + bound1

        ticks = otp.Ticks(dict(symbol_name=["A", "D"]))
        dfs = otp.run(some_logic, symbols=ticks)

        assert len(dfs["A"]) == 4
        assert len(dfs["D"]) == 2

        assert all(dfs["A"]["A"] == [1, 2, 0, 0])
        assert all(dfs["A"]["B"] == [0, 0, 3, 4])

        assert all(dfs["D"]["B"] == [3, 4])

    def test_2_unbound_1_bond(self, db):
        def some_logic(symbol):
            unbound1 = otp.DataSource(db, tick_type="tA")
            unbound2 = otp.DataSource(db, tick_type="tA2")
            bound = otp.DataSource(db, tick_type="tC", symbol="C")

            return unbound1 + unbound2 + bound

        symbols = otp.Ticks(dict(x=["A", "B"]))
        dfs = otp.run(some_logic, symbols=symbols)

        assert len(dfs["A"]) == 6
        assert len(dfs["B"]) == 4  # tA has two ticks for symbol B

        assert all(dfs["A"]["A"] == [1, 2, 0, 0, 7, 8])
        assert all(dfs["A"]["C"] == [0, 0, 5, 6, 0, 0])

        assert all(dfs["B"]["C"] == [5, 6, 0, 0])
        assert all(dfs["B"]["B"] == [0, 0, 9, 0])

    def test_unbound_and_merge_bound(self, db):
        def callback(symbol):
            data = otp.DataSource(db=db, tick_type="tA")
            return data

        def some_logic(symbol):
            m = otp.merge([callback], symbols=["A", "B"])
            data = otp.DataSource(db=db, tick_type="tC")
            return m + data

        symbols = otp.Ticks(dict(SYMBOL_NAME=["C"]))
        res = otp.run(some_logic, symbols=symbols)

        assert len(res) == 1
        assert len(res["C"]) == 6


class TestUnboundAndBoundMerge:
    @pytest.fixture(scope="class")
    def m1_db(self):
        m1 = otp.DB("MS1")
        m1.add(src=otp.Ticks(dict(PRICE=[295.44, 295.47, 295.43, 295.46])), symbol="AAPL", tick_type="TRD")
        m1.add(src=otp.Ticks(dict(PRICE=[48.1, 48.05])), symbol="AMD", tick_type="TRD")
        return m1

    @pytest.fixture(scope="class")
    def m2_db(self):
        m2 = otp.DB("MS2")
        m2.add(src=otp.Ticks(dict(PRICE=[159.8, 159.83, 159.81, 158.815])), symbol="MSFT", tick_type="TRD")
        m2.add(src=otp.Ticks(dict(PRICE=[48.3, 48.27, 48.31, 48.33])), symbol="AMD", tick_type="TRD")
        return m2

    @pytest.fixture(scope="class")
    def m3_db(self, c_session):
        m3 = otp.DB("MS3")
        m3.add(src=otp.Ticks(dict(PRICE=[158.79, 158.77, 158.81])), symbol="MSFT", tick_type="TRD")
        m3.add(src=otp.Ticks(dict(PRICE=[48.2, 48.24, 48.2, 48.26])), symbol="AMD", tick_type="TRD")
        return m3

    @pytest.fixture(scope="class")
    def orders_db(self, m1_db, m2_db, m3_db):
        db = otp.DB("TEST_ORDERS_DB")

        db.add(
            src=otp.Ticks([["ID", "MD_DB"], [1, m1_db.name], [2, m1_db.name], [3, m1_db.name]]),
            symbol="AAPL",
            tick_type="ORDER",
        )
        db.add(
            src=otp.Ticks([["ID", "MD_DB"], [4, m2_db.name], [5, m2_db.name], [6, m2_db.name]]),
            symbol="MSFT",
            tick_type="ORDER",
        )
        db.add(
            src=otp.Ticks([["ID", "MD_DB"], [7, m3_db.name], [8, m3_db.name], [9, m3_db.name], [0, m3_db.name]]),
            symbol="AMD",
            tick_type="ORDER",
        )

        yield db

    @pytest.fixture(scope="class", autouse=True)
    def default_session(self, c_session, orders_db, m1_db, m2_db, m3_db):
        c_session.use(orders_db)
        c_session.use(m1_db)
        c_session.use(m2_db)
        c_session.use(m3_db)
        return c_session

    def test_unbound_and_callback_bound(self, m1_db, orders_db):
        def get_mds_db(symbol):
            data = otp.DataSource(db=orders_db, tick_type="ORDER")
            data["SYMBOL_NAME"] = symbol.name
            return data.first()

        def cross_trades():
            trd = otp.DataSource(db=m1_db, tick_type="TRD")
            trd["T"] = trd["Time"]
            return trd

        def orders_with_nearest_trade():
            # prices will be replaced by amd data from m1_db, with nan at the beginning because join is outer by default
            orders = otp.DataSource(db=orders_db, tick_type="ORDER")
            trades = otp.merge([cross_trades], symbols=otp.Symbols(db=orders_db, for_tick_type="ORDER"))
            result = otp.join_by_time([orders, trades])
            return result

        first_stage = otp.merge([get_mds_db], symbols=otp.Symbols(db=orders_db, for_tick_type="ORDER"))
        dfs = otp.run(orders_with_nearest_trade(), symbols=first_stage)

        assert set(dfs.keys()) == {"AAPL", "AMD", "MSFT"}
        expected = pd.Series([np.NaN, 48.10, 48.05])
        assert all((dfs["MSFT"]["PRICE"] == expected) | (dfs["MSFT"]["PRICE"].isna() & expected.isna()))
        assert all((dfs["AAPL"]["PRICE"] == expected) | (dfs["AAPL"]["PRICE"].isna() & expected.isna()))
        expected = pd.Series([np.NaN, 48.10, 48.05, 295.43])
        assert all((dfs["AMD"]["PRICE"] == expected) | (dfs["AAPL"]["PRICE"].isna() & expected.isna()))

    def test_mixture(self, default_session, orders_db, m1_db, m2_db, m3_db):
        # merge several market data, find the best price and calculate diff between the best brice and exchange current
        def get_md_db(symbol):
            data = otp.DataSource(db=orders_db, tick_type="ORDER")
            data.SYMBOL_NAME = symbol.name
            return data.first()

        def main_logic(symbol):
            # trades from the main db
            trades = otp.DataSource(db=symbol.MD_DB, tick_type="TRD", schema={'PRICE': float})
            trades.rename(dict(PRICE="MD_PRICE"), inplace=True)

            # best trade over all database
            all_trades = otp.DataSource(
                db=[m1_db, m2_db, m3_db],
                tick_type="TRD",
                symbol=["AAPL", "MSFT", "AMD"],
                identify_input_ts=True,
                schema={'PRICE': float},
            )
            # Attribute TIMESTAMP cann't be used in group by.
            all_trades["T"] = all_trades["Time"]
            all_trades = all_trades.high(all_trades["PRICE"],   # pylint: disable=E1121,E1123
                                         group_by=[all_trades["SYMBOL_NAME"],
                                                   all_trades["T"]])
            all_trades = all_trades[["PRICE", "SYMBOL_NAME"]]
            all_trades, _ = all_trades[(all_trades.SYMBOL_NAME == symbol.name)]

            # orders
            orders = otp.DataSource(db=orders_db, tick_type="ORDER")
            res = otp.join_by_time([orders, trades, all_trades], how="inner")
            res.DIFF = res.PRICE - res.MD_PRICE
            return res

        first_stage = otp.merge([get_md_db], symbols=otp.Symbols(db=orders_db, for_tick_type="ORDER"))
        dfs = otp.run(main_logic, symbols=first_stage)

        assert set(dfs.keys()) == {"AAPL", "AMD", "MSFT"}
        assert all(dfs["AAPL"]["MD_DB"] == "MS1")
        assert all(dfs["AAPL"]["SYMBOL_NAME"] == "AAPL")
        expected = pd.Series([295.44, 295.47])
        assert all(dfs["AAPL"]["MD_PRICE"] == expected)
        assert all(dfs["AAPL"]["PRICE"] == expected)
        assert all(dfs["AAPL"]["DIFF"] == 0.0)

        assert all(dfs["AMD"]["MD_DB"] == "MS3")
        assert all(dfs["AMD"]["SYMBOL_NAME"] == "AMD")
        expected = pd.Series([48.20, 48.24, 48.20])
        assert all(dfs["AMD"]["MD_PRICE"] == expected)
        expected = pd.Series([48.30, 48.27, 48.31])
        assert all(dfs["AMD"]["PRICE"] == expected)
        expected = pd.Series([pytest.approx(0.1), pytest.approx(0.03), pytest.approx(0.11)])
        assert all(dfs["AMD"]["DIFF"] == expected)

        assert all(dfs["MSFT"]["MD_DB"] == "MS2")
        assert all(dfs["MSFT"]["SYMBOL_NAME"] == "MSFT")
        expected = pd.Series([159.80, 159.83])
        assert all(dfs["MSFT"]["MD_PRICE"] == expected)
        assert all(dfs["MSFT"]["PRICE"] == expected)
        assert all(dfs["MSFT"]["DIFF"] == 0.0)


class TestErrors:
    def test_set_symbol_cause_error(self):
        data = otp.Ticks(dict(a=[1, 2], b=[2, 4]))
        data["_SYMBOL_NAME"]  # symbol should be accessible
        with pytest.raises(ValueError):
            data["_SYMBOL_NAME"] = "AA"
        with pytest.raises(ValueError):
            data._SYMBOL_NAME = "AA"

    def test_set_symbol_cause_error_on_alias(self):
        data = otp.Ticks(dict(a=[1, 2], b=[2, 4]))
        data.Symbol  # symbol should be accessible
        with pytest.raises(ValueError):
            data.Symbol = "AA"
