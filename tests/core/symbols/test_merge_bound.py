import pytest
import operator

import onetick.py as otp
from onetick.py.otq import otq


class BaseData:
    @pytest.fixture(scope="class")
    def db_a(self):
        db = otp.DB(name="DB_A")

        db.add(otp.Ticks(dict(X=[1, 2, 3])), symbol="S1", tick_type="TT")
        db.add(otp.Ticks(dict(X=[-3, -2, -1])), symbol="S2", tick_type="TT")
        yield db

    @pytest.fixture(scope="class")
    def db_b(self):
        db = otp.DB(name="DB_B")

        db.add(otp.Ticks(dict(X=[3, 4, 5])), symbol="S4", tick_type="TT")
        db.add(otp.Ticks(dict(Y=[0.1, 0.2, 0.3])), symbol="S4", tick_type="PP")
        db.add(otp.Ticks(dict(X=[6, 7, 8])), symbol="S5", tick_type="TT")
        yield db

    @pytest.fixture(scope="class")
    def db_c(self):
        db = otp.DB(name="DB_C")

        db.add(otp.Ticks(dict(X=[9])), symbol="S6", tick_type="TT")
        db.add(otp.Ticks(dict(X=[0])), symbol="S7", tick_type="TT2")
        yield db

    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session, db_a, db_b, db_c):
        c_session.use(db_a, db_b, db_c)
        yield c_session


class TestMergeAsSource(BaseData):
    @pytest.mark.parametrize(
        "db,symbols,tt,res",
        [
            # test_const
            ("DB_A", ["S1"], "TT", [1, 2, 3]),
            ("DB_A", ["S2"], "TT", [-3, -2, -1]),
            ("DB_A", ["S2"], None, [-3, -2, -1]),
            ("DB_A", ["S2", "S1"], "TT", [-3, 1, -2, 2, -1, 3]),
            (None, ["DB_A::S1"], "TT", [1, 2, 3]),
            (None, ["DB_A::S1", "DB_B::S4"], "TT", [1, 3, 2, 4, 3, 5]),  # TODO: test schema
            (["DB_A", "DB_B"], ["S1", "S5"], "TT", [1, 6, 2, 7, 3, 8]),
            (["DB_A::TT", "DB_C::TT2"], ["S1", "S7"], None, [1, 0, 2, 3]),
            # test_source
            ("DB_A", otp.Ticks(dict(SYMBOL_NAME=["S1", "S2"])), "TT", [1, -3, 2, -2, 3, -1]),
            (None, otp.Ticks(dict(SYMBOL_NAME=["DB_A::S1", "DB_B::S4"])), "TT", [1, 3, 2, 4, 3, 5]),
            (None, otp.Symbols("DB_A", keep_db=True), "TT", [1, -3, 2, -2, 3, -1]),
        ],
    )
    def test_base_parameters(self, db, symbols, tt, res):
        s = otp.DataSource(db=db, symbol=symbols, tick_type=tt)
        df = otp.run(s)
        assert all(df["X"] == res)

    @pytest.mark.parametrize("keep,op", [(True, operator.truth), (False, operator.not_)])
    def test_keep_symbol_tick_type(self, keep, op):
        data = otp.DataSource(db="DB_B", tick_type="TT", symbol=["S4", "S5"], identify_input_ts=keep)

        assert op("SYMBOL_NAME" in data.columns())
        assert op("TICK_TYPE" in data.columns())

        df = otp.run(data)

        assert op("SYMBOL_NAME" in df.columns)
        assert op("TICK_TYPE" in df.columns)

        assert len(df) == 6

    def test_several_sources(self):
        data_1 = otp.DataSource(db="DB_A", tick_type="TT", symbol=otp.Symbols(db="DB_A"))
        data_1["flag"] = 1

        data_2 = otp.DataSource(db="DB_B", tick_type="TT", symbol=otp.Symbols(db="DB_B"))
        data_2["flag"] = 2

        data_3 = otp.Tick(X=99, flag=3)

        res = otp.merge([data_1, data_2, data_3])
        res.sort([res["Time"], res["flag"], res["X"]], inplace=True)
        df = otp.run(res)

        assert all(df["X"] == [-3, 1, 3, 6, 99, -2, 2, 4, 7, -1, 3, 5, 8])

    def test_graph_query(self):
        data = otp.DataSource(db='DB_A', tick_type='TT')
        data = otp.merge([data], symbols=otq.GraphQuery(otq.FindDbSymbols(pattern='%').symbol('DB_A::')))
        df = otp.run(data)
        assert list(df['X']) == [1, -3, 2, -2, 3, -1]

    # TODO:  Test fail cases
    #        - db is not specified at all
    #        - tick type is not specified
    #        - symbol params


class TestWithCallback(BaseData):
    @pytest.mark.parametrize(
        "db,symbols,tt,res",
        [
            ("DB_A", ["S1"], "TT", [2, 3, 4]),
            ("DB_A", ["S2", "S1"], "TT", [2, -2, 3, -1, 4, 0]),
            ("DB_A", ["S2", "S2"], "TT", [-2, -2, -1, -1, 0, 0]),
            (None, ["DB_A::S1", "DB_B::S4"], "TT", [2, 4, 3, 5, 4, 6]),
            (["DB_A", "DB_B"], ["S1", "S5"], "TT", [2, 7, 3, 8, 4, 9]),
            (["DB_A::TT", "DB_C::TT2"], ["S1", "S7"], None, [2, 1, 3, 4]),
        ],
    )
    def test_const(self, db, symbols, tt, res):
        def callback():
            schema = {}
            if db is None or tt is None or isinstance(db, list):
                schema = {'X': int}
            d = otp.DataSource(db=db, tick_type=tt, schema=schema)
            d["X"] += 1
            return d

        data = otp.merge([callback], symbols=symbols, presort=False)
        df = otp.run(data)
        assert all(df["X"] == res)

    @pytest.mark.parametrize(
        "db,symbols,tt,res",
        [
            ("DB_A", otp.Ticks(dict(SYMBOL_NAME=["S1", "S2"])), "TT", [1, -3, 2, -2, 3, -1]),
            (None, otp.Ticks(dict(SYMBOL_NAME=["DB_A::S1", "DB_B::S4"])), "TT", [1, 3, 2, 4, 3, 5]),
            (None, otp.Symbols("DB_A", keep_db=True), "TT", [1, -3, 2, -2, 3, -1]),
        ],
    )
    def test_source(self, db, symbols, tt, res):
        def callback():
            return otp.DataSource(db=db, tick_type=tt)  # TODO: forbid passing symbols in this case

        data = otp.merge([callback], symbols=symbols)
        df = otp.run(data)
        assert all(df["X"] == res)

    @pytest.mark.parametrize(
        "symbols,res1,res2",
        [
            (otp.Ticks(dict(SYMBOL_NAME=["S1"], Y=[0.5])), [0.5, 1.0, 1.5], ["S1"] * 3),
            (
                otp.Ticks(dict(SYMBOL_NAME=["S1", "S2"], Y=[-0.5, 0.5])),
                [-0.5, -1.5, -1.0, -1.0, -1.5, -0.5],
                ["S1", "S2"] * 3,
            ),
            (otp.Ticks(dict(SYMBOL_NAME=["S1", "S1"], Y=[-0.5, 0.5])), [-0.5, 0.5, -1.0, 1.0, -1.5, 1.5], ["S1"] * 6),
        ],
    )
    def test_symbol_params(self, symbols, res1, res2):
        def callback(symbol):
            d = otp.DataSource(db="DB_A", tick_type="TT")
            d["Z"] = d["X"] * symbol["Y"]
            d["S"] = symbol.name
            return d

        data = otp.merge([callback], symbols=symbols)
        df = otp.run(data)
        assert all(df["Z"] == res1)
        assert all(df["S"] == res2)

    def test_multiple_sources(self):
        def callback1():
            return otp.DataSource(db="DB_B", tick_type="TT")

        def callback2():
            return otp.DataSource(db="DB_B", tick_type="PP")

        def callback3():
            return otp.Tick(Z=3)

        data = otp.merge([callback1, callback2, callback3], symbols=["S4", "S5"])
        df = otp.run(data)

        assert len(df) == 11  # 3 from S4|TT + 3 from S4|PP + 3 from S5|TT + 2 generated ticks for S4 and S5

    def test_bound_inside(self):
        def callback():
            data1 = otp.DataSource(db="DB_A", tick_type="TT")
            data1["flag"] = 1
            data2 = otp.DataSource(db="DB_A", symbol="S1", tick_type="TT")
            data2["flag"] = 2

            return data1 + data2

        res = otp.merge([callback], symbols=["S2"])

        df = otp.run(res)

        assert all(df["X"] == [1, -3, 2, -2, 3, -1])
        assert all(df["flag"] == [2, 1] * 3)

    def test_bound_merge_inside_merge(self):
        def db_b_data():
            data_1 = otp.DataSource(db="DB_B", tick_type="TT")
            data_1["flag"] = 1
            data_2 = otp.DataSource(db="DB_B", tick_type="PP", symbol="S4")
            data_2["flag"] = 2
            return data_1 + data_2

        def callback():
            data_1 = otp.DataSource(db="DB_A", tick_type="TT")
            data_1["flag"] = 3
            data_2 = otp.DataSource(db="DB_A", tick_type="TT", symbol="S1")
            data_2["flag"] = 4

            data_b = otp.merge([db_b_data], symbols=["S5"])
            return data_1 + data_2 + data_b

        res = otp.merge([callback], symbols=["S2"])
        res.sort([res["Time"], res["flag"]], inplace=True)

        df = otp.run(res)

        assert all(df["X"] == [6, 0, -3, 1, 7, 0, -2, 2, 8, 0, -1, 3])

    @pytest.mark.parametrize("keep,op", [
        (True, operator.truth),
        (False, operator.not_),
    ])
    def test_keep_symbol_tick_type(self, keep, op):
        def callback1():
            return otp.DataSource(db="DB_B", tick_type="TT")

        def callback2():
            return otp.DataSource(db="DB_B", tick_type="PP")

        data = otp.merge([callback1, callback2], symbols=["S4", "S5"], presort=False, identify_input_ts=keep)

        assert op("SYMBOL_NAME" in data.columns())
        assert op("TICK_TYPE" in data.columns())

        df = otp.run(data)

        assert op("SYMBOL_NAME" in df.columns)
        assert op("TICK_TYPE" in df.columns)

        if keep:
            assert all(df["SYMBOL_NAME"] == ["S4", "S4", "S5"] * 3)
            assert all(df["TICK_TYPE"] == ["TT", "PP", "TT"] * 3)

    def test_db_symbol_param(self):
        def callback(symbol):
            d = otp.DataSource(db=symbol.DB, tick_type="TT", schema={'X': int})
            d["flag"] = d.apply(lambda row: 1 if symbol.name == "S1" else 2)
            return d

        symbols = otp.Ticks(dict(SYMBOL_NAME=["S1", "S6"], DB=["DB_A", "DB_C"]))

        data = otp.merge([callback], symbols=symbols)
        data.sort([data["Time"], data["flag"]], inplace=True)

        df = otp.run(data)

        assert all(df["X"] == [1, 9, 2, 3])

    def test_presort_params(self, monkeypatch):
        """ check that presort params are propagated correctly """
        def callback(symbol):
            return otp.Empty()

        presort = otq.Presort

        class Mock:

            def __init__(self):
                self.init()

            def __call__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs
                self.called = True
                return presort(*args, **kwargs)

            def init(self):
                self.args = ()
                self.kwargs = {}
                self.called = False

            def cleanup(self):
                self.init()

        mock = Mock()
        monkeypatch.setattr('onetick.py.functions.otq.Presort', mock)

        otp.merge([otp.Empty(), otp.Empty()])
        assert not mock.called

        mock.cleanup()
        otp.merge([otp.Empty()], presort=True)
        assert mock.called

        mock.cleanup()
        otp.merge([callback], symbols=otp.Ticks({'SYMBOL_NAME': ['A', 'B', 'C']}))
        # presort is enabled by default when symbols are set
        assert mock.called

        mock.cleanup()
        otp.merge([callback], symbols=['A', 'B'])
        # presort is enabled by default when symbols are set
        assert mock.called

        mock.cleanup()
        otp.merge([callback], symbols=['A', 'B'], presort=False)
        assert not mock.called

        mock.cleanup()
        otp.merge([callback], symbols=['A'], presort=True)
        assert mock.called
        assert not mock.args
        assert mock.kwargs == dict(batch_size=otp.config.default_batch_size,
                                   max_concurrency='')

        mock.cleanup()
        otp.merge([callback], symbols=['A'], presort=True, concurrency=8)
        assert mock.called
        assert not mock.args
        assert mock.kwargs == dict(batch_size=otp.config.default_batch_size,
                                   max_concurrency=8)

        mock.cleanup()
        otp.merge([callback], symbols=['A'], presort=True, batch_size=12, concurrency=16)
        assert mock.called
        assert not mock.args
        assert mock.kwargs == dict(batch_size=12, max_concurrency=16)

        mock.cleanup()
        with pytest.warns(UserWarning, match="the `concurrency` parameter makes effect only"):
            otp.merge([callback], symbols=['A'], concurrency=8, presort=False)
            assert not mock.called

        mock.cleanup()
        with pytest.warns(UserWarning, match="the `batch_size` parameter makes effect only"):
            otp.merge([callback], symbols=['A'], batch_size=32, presort=False)
            assert not mock.called

    @pytest.mark.parametrize('cc', [2, None, 1, 100])
    def test_presort_call(self, cc):
        """ checks that presort works """
        def callback():
            data1 = otp.DataSource(db="DB_A", tick_type="TT")
            data1["flag"] = 1
            data2 = otp.DataSource(db="DB_A", symbol="S1", tick_type="TT")
            data2["flag"] = 2

            return data1 + data2

        res = otp.merge([callback], symbols=["S2"], presort=True, concurrency=2, batch_size=2)

        df = otp.run(res)

        assert all(df["X"] == [1, -3, 2, -2, 3, -1])
        assert all(df["flag"] == [2, 1] * 3)

    def test_several_calls_in_order(self):
        def callback1():
            data = otp.Tick(X=1)
            data['S'] = data.Symbol.name
            return data

        def callback2():
            return otp.merge([callback1], symbols=['S1', 'S2'], presort=True, concurrency=64) + otp.Tick(X=2)

        res = otp.merge([callback2], symbols=['S2'], presort=True)

        df = otp.run(res)

        assert len(df) == 3
