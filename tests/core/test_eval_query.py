import pytest

import onetick.py as otp


class TestWithDB1:
    @pytest.fixture(scope="class")
    def db(self):
        db = otp.DB("MY_DB")
        db.add(otp.Tick(X=1), symbol="AA", tick_type="TRD")
        db.add(otp.Tick(X=2), symbol="BA", tick_type="TRD")
        yield db

    @pytest.fixture(scope="class")
    def session(self, c_session, db):
        c_session.use(db)
        yield c_session

    def test_custom_tick(self, session, db):
        def func1(symbol):
            fsq = otp.Tick(SYMBOL_NAME=symbol.name + "A")
            return otp.DataSource(db, symbol=otp.eval(fsq, symbol=symbol))

        self._check_custom(func1)

    def test_custom_query(self, session, db):
        def func1(symbol):
            fsq = otp.Tick(SYMBOL_NAME=symbol.name + "A")
            fsq = otp.query(fsq.to_otq())
            return otp.DataSource(db, symbol=otp.eval(fsq, symbol=symbol))

        self._check_custom(func1)

    def test_custom_query_source(self, session, db, cur_dir):
        def func1(symbol):
            fsq = otp.Tick(SYMBOL_NAME=symbol.name + "A")
            fsq = otp.query(fsq.to_otq())
            fsq = otp.Query(fsq)
            return otp.DataSource(db, symbol=otp.eval(fsq, symbol=symbol))

        self._check_custom(func1)

    def test_custom_save_to_file(self, session, db, cur_dir):
        otq_name = otp.utils.TmpFile("_test_custom_save_to_file.otq").path

        def func1(symbol):
            fsq = otp.Tick(SYMBOL_NAME=symbol.name + "A")
            fsq.to_graph().save_to_file(otq_name, "query", start=otp.config['default_start_time'],
                                        end=otp.config['default_end_time'])
            fsq = otp.query(otq_name)
            fsq = otp.Query(fsq)
            return otp.DataSource(db, symbol=otp.eval(fsq, symbol=symbol))

        self._check_custom(func1)

    def _check_custom(self, func1):
        data = otp.Tick(SYMBOL_NAME="A")
        dfs = otp.run(func1, symbols=data)
        assert all(dfs["A"]["X"] == [1])

        res = otp.run(func1, symbols="A")
        assert all(res["X"] == [1])

    def test_wrong_arg(self):
        with pytest.raises(ValueError, match="Symbol parameter has wrong type"):
            fsq = otp.Tick(SYMBOL_NAME="A")
            otp.eval(fsq, symbol="AAA")

    def test_class_method_as_param(self, session, db, cur_dir):
        # PY-275: otp.eval does not allow to set method of a class as a first argument
        def return_fsq():
            fsq = otp.Tick(SYMBOL_NAME="AA")
            fsq = otp.query(fsq.to_otq())
            fsq = otp.Query(fsq)
            return fsq

        def func():
            return return_fsq()

        class SomeClass:
            @staticmethod
            def static_method():
                return return_fsq()

            def method(self):
                return return_fsq()

        def with_func(symbol):
            return otp.DataSource(db, symbol=otp.eval(func, symbol=symbol))

        def with_static_method(symbol):
            return otp.DataSource(db, symbol=otp.eval(SomeClass.static_method, symbol=symbol))

        def with_method(symbol):
            clazz = SomeClass()
            return otp.DataSource(db, symbol=otp.eval(clazz.method, symbol=symbol))

        self._check_custom(with_func)
        self._check_custom(with_static_method)
        self._check_custom(with_method)

    def test_merge_symbols_as_callback(self, session, db):
        def get_source(symbol):
            s = otp.DataSource(tick_type='TRD', db='MY_DB')

            def symbols_func(symbol):
                symb = otp.Tick(B=2)
                symb['SYMBOL_NAME'] = symbol['SYMBOL_1']
                return symb

            s = otp.merge([s], symbols=otp.eval(symbols_func, symbol=symbol))
            return s

        dfs = otp.run(get_source, symbols=otp.Tick(SYMBOL_NAME='A', SYMBOL_1='AA', SYMBOL_2='AB'))
        assert len(dfs) == 1
        assert 'A' in dfs.keys()
        df = dfs['A']
        assert len(df) == 1
        assert df['X'][0] == 1


class TestWithDB2:
    @pytest.fixture(scope="class")
    def db(self):
        db = otp.DB("MY_DB")
        db.add(otp.Ticks(dict(X=[1, 2, 3])), symbol="AA", tick_type="TRD")
        db.add(otp.Ticks(dict(X=[1, 2, 3])), symbol="BA", tick_type="TRD")
        db.add(otp.Ticks(dict(X=[1, 2, 3])), symbol="CA", tick_type="TRD")
        yield db

    @pytest.fixture(scope="class")
    def session(self, c_session, db):
        c_session.use(db)
        yield c_session

    def test_symbol_params(self, session, db, cur_dir):
        # execute the query with _PARAM_START_TIME_NANOS and _PARAM_END_TIME_NANOS specified to select only some values
        start = otp.datetime(otp.config['default_start_time'] + otp.Milli(1), tz="EST5EDT")
        end = otp.datetime(otp.config['default_start_time'] + otp.Milli(2), tz="EST5EDT")

        def func1(symbol):
            fsq = otp.Tick(SYMBOL_NAME=symbol.name + "A", _PARAM_START_TIME_NANOS=symbol.START,
                           _PARAM_END_TIME_NANOS=symbol.END)
            return otp.DataSource(db, symbol=otp.eval(fsq, symbol=symbol, start=symbol.START, end=symbol.END))

        data = otp.Tick(SYMBOL_NAME="A", START=start, END=end)
        dfs = otp.run(func1, symbols=data)
        assert set(dfs.keys()) == {"A"}
        assert all(dfs["A"]["X"] == [2])


@pytest.mark.skipif(otp.__build__ < "20200712120000", reason="BDS-179")
class TestWithDB3:
    DB_WITH_SYMBOLS = "DB_WITH_SYMBOLS"
    DB_WITH_DATA = "DATA"

    @pytest.fixture
    def session(self):
        with otp.Session() as s:
            db1 = otp.DB(self.DB_WITH_SYMBOLS)
            db1.add(otp.Ticks(dict(X=["A", "B", "C"])), symbol="S", tick_type="TT", date=otp.datetime(2005, 1, 1))
            db1.add(otp.Ticks(dict(X=["B", "C", "D"])), symbol="S", tick_type="TT", date=otp.datetime(2005, 1, 2))
            db1.add(otp.Ticks(dict(X=["C", "D", "E"])), symbol="S", tick_type="TT", date=otp.datetime(2005, 1, 3))

            db2 = otp.DB(self.DB_WITH_DATA)
            db2.add(otp.Ticks(dict(Y=[1, 2])), symbol="A")
            db2.add(otp.Ticks(dict(Y=[3, 4])), symbol="B")
            db2.add(otp.Ticks(dict(Y=[5, 6])), symbol="C")
            db2.add(otp.Ticks(dict(Y=[6, 8])), symbol="D")
            db2.add(otp.Ticks(dict(Y=[9, 0])), symbol="E")
            s.use(db1, db2)
            yield s

    def test_some(self, session):
        def bound():
            symbols = otp.DataSource(db=self.DB_WITH_SYMBOLS, symbol="S", tick_type="TT")
            symbols["SYMBOL_NAME"] = symbols["X"]
            symbols["S"] = symbols["_START_TIME"]
            symbols["E"] = symbols["_END_TIME"]
            return symbols

        def merge_callback(symbol):
            data = otp.DataSource(db=self.DB_WITH_DATA, schema_policy="manual", schema={'Y': int})
            data["S"] = symbol.S
            data["E"] = symbol.E
            return data

        def main(symbol):
            s1 = otp.Empty()
            s2 = otp.merge([merge_callback], symbols=otp.eval(bound, start=symbol["START"], end=symbol["END"]))
            s2["S_UNBOUND"] = symbol["START"]
            s2["E_UNBOUND"] = symbol["END"]
            return s1 + s2

        starts = [otp.datetime(2005, 1, 1), otp.datetime(2005, 1, 2), otp.datetime(2005, 1, 3)]
        data = otp.Ticks(dict(SYMBOL_NAME=["X", "Y", "Z"],
                              START=starts,
                              END=[s.end for s in starts]))

        dfs = otp.run(main, symbols=data)
        assert all(dfs["X"]["Y"] == [1, 3, 5, 2, 4, 6])
        assert all(dfs["Y"]["Y"] == [3, 5, 6, 4, 6, 8])
        assert all(dfs["Z"]["Y"] == [5, 6, 9, 6, 8, 0])


@pytest.mark.parametrize('eval_as_func', [True, False])
class TestEvalParams:

    @pytest.fixture()
    def session(self):
        s = otp.Session()
        s.use(otp.DB('DB1'))
        s.use(otp.DB('DB2'))
        yield s
        s.close()

    @staticmethod
    def choose_query(func, as_func, **kwargs):
        if as_func:
            return func
        return otp.query(func(**kwargs).to_otq())

    @pytest.mark.skipif(otp.__build__ < "20210223120000",
                        reason="_DBNAME in eval used in ep param supports in 202102 build")
    def test_dbname(self, session, eval_as_func):
        """
        Testing:
            - ability to pass OT meta field _DBNAME to eval query as parameter
            - using eval in filtering
        """
        def eval_func(db):
            res = otp.Ticks({'DB': ['DB1', 'DB2'],
                             'WHERE': ['T=1', 'T=2']})
            res, _ = res[res['DB'] == db]
            return res.drop(['DB'])
        query = self.choose_query(eval_func, eval_as_func, db='$db')

        t = otp.Ticks({'T': [1, 2]}, db=None)
        t, _ = t[otp.eval(query, db=t['_DBNAME'])]
        t = otp.run(t, symbols=['DB1::A', 'DB2::A'])
        assert set(t.keys()) == {'DB1::A', 'DB2::A'}
        assert len(t['DB1::A']) == 1
        assert len(t['DB2::A']) == 1
        assert t['DB1::A']['T'][0] == 1
        assert t['DB2::A']['T'][0] == 2

    def test_tick_dependent(self, session, eval_as_func):
        def eval_func(a):
            return otp.Ticks({'A': [a]})
        query = self.choose_query(eval_func, eval_as_func, a='$a')

        t = otp.Ticks({'A': [1]})
        if eval_as_func:
            with pytest.raises(Exception, match='depend on tick'):
                t, _ = t[otp.eval(query, a=t['A'])]
        else:
            t, _ = t[otp.eval(query, a=t['A'])]
            with pytest.raises(Exception, match='is not constant'):
                otp.run(t)

    @pytest.mark.skipif(otp.__build__ < "20210223120000",
                        reason="_DBNAME in eval used in ep param supports in 202102 build")
    def test_where_multiple_output(self, session, eval_as_func):
        def eval_func(db):
            res = otp.Ticks({'DB': ['DB1'],
                             'WHERE': ['T=1']})
            res, _ = res[res['DB'] == db]
            return res
        query = self.choose_query(eval_func, eval_as_func, db='$db')

        t = otp.Ticks({'T': [1, 2]}, db=None)

        t1 = t.copy()
        t1, _ = t1[otp.eval(query, db=t['_DBNAME'])]
        # check that OT will raise error if more than one field returned in eval query
        with pytest.raises(Exception, match='does not specify attribute name'):
            otp.run(t1, symbols=['DB1::A', 'DB2::A'])
        # check that field to use in WHERE_CLAUSE will be selected
        t, _ = t[otp.eval(query, db=t['_DBNAME'])['WHERE']]
        otp.run(t, symbols=['DB1::A', 'DB2::A'])

    def test_simple_params(self, session, eval_as_func):
        def eval_func(p_str, p_int):
            res = otp.Ticks({'T': [1]})
            res['WHERE_STR'] = "T_STR='" + p_str + "'"
            res['WHERE_INT'] = "T_INT=" + str(p_int)
            return res
        query = self.choose_query(eval_func, eval_as_func, p_str='$p_str', p_int='$p_int')

        t = otp.Ticks({'T_STR': ['1', '2'],
                       'T_INT': [1, 2]})
        t1, _ = t[otp.eval(query, p_str='1', p_int=2)['WHERE_STR']]
        res = otp.run(t1)
        assert len(res) == 1
        assert res['T_STR'][0] == '1'

        t2, _ = t[otp.eval(eval_func, p_str='1', p_int=2)['WHERE_INT']]
        res = otp.run(t2)
        assert len(res) == 1
        assert res['T_INT'][0] == 2

    def test_symbol_param(self, session, eval_as_func):
        def eval_func(p):
            res = otp.Ticks({'T': [1]})
            res['WHERE'] = "T=" + p
            return res
        query = self.choose_query(eval_func, eval_as_func, p='$p')

        t = otp.Ticks({'T': [1, 2, 3]})
        symbols = otp.Ticks({'SYMBOL_NAME': ['a', 'b'],
                             'PARAM': ['1', '2']})
        sym_params = symbols.to_symbol_param()
        e = otp.eval(query, p=sym_params['PARAM'])
        t, _ = t[e['WHERE']]
        res = otp.run(t, symbols=symbols)
        assert len(res['a']) == 1
        assert len(res['b']) == 1
        assert res['a']['T'][0] == 1
        assert res['b']['T'][0] == 2


def test_jwq_param(f_session):
    # PY-895
    db = otp.DB('DB')
    db.add(otp.Ticks({'C': [1, 2, 3]}), symbol='MD_SYMBOL_1', tick_type='TT')
    db.add(otp.Ticks({'C': [4, 5, 6]}), symbol='MD_SYMBOL_2', tick_type='TT')
    f_session.use(db)

    def third_query(symbol, md_symbol):
        return otp.Tick(SYMBOL_NAME=md_symbol)

    def get_influencing_orders(md_symbol):
        data = otp.DataSource(db='DB', tick_type='TT', symbols=otp.eval(third_query, md_symbol=md_symbol))
        data = data.first()
        return data

    t = otp.Ticks({'MD_SYMBOL': ['MD_SYMBOL_1', 'MD_SYMBOL_2']})
    t = t.join_with_query(
        get_influencing_orders,
        symbol=f'{otp.config.default_db}::DUMMY',
        how='inner',
        params={
            'md_symbol': t['MD_SYMBOL']
        },
    )
    df = otp.run(t)
    assert df['MD_SYMBOL'][0] == 'MD_SYMBOL_1'
    assert df['MD_SYMBOL'][1] == 'MD_SYMBOL_2'
    assert df['C'][0] == 1
    assert df['C'][1] == 4


@pytest.mark.skip(reason='Will work after PY-952')
def test_generated_otq(f_session):

    t = otp.Tick(A=1)
    t.state_vars['TL'] = otp.state.tick_list(otp.eval(otp.Tick(B=2)))
    t['X'] = t.state_vars['TL'].size()
    df = otp.run(t)
    print(df)

    gen_query = t.to_otq().split('::')[0]
    queries = otp.core.query_inspector.get_queries(gen_query)
    assert len(queries) == 2
