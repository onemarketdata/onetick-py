import pytest

import onetick.py as otp


class TestEmpty:
    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session):

        db = otp.DB(name="DB_A")
        db.add(otp.Ticks(dict(X=[-1], Y=[0.1], S=["a"])), symbol="S1", tick_type="TT")
        db.add(otp.Ticks(dict(X=["a" * 555], Y=[otp.dt(2019, 12, 1)])), symbol="S2", tick_type="PP")

        c_session.use(db)

        yield c_session

    @pytest.mark.parametrize("t", [otp.Empty(), otp.Empty().copy(), otp.Empty().deepcopy()])
    def test_copy(self, t):
        assert isinstance(t, otp.Empty)

    def test_simple(self):
        data = otp.Empty()

        df = otp.run(data)

        assert len(df) == 0

    def test_with_other(self):
        data = otp.Empty()
        data += otp.Tick(X=1)

        df = otp.run(data)

        assert len(df) == 1
        assert all(df["X"] == [1])

    @pytest.mark.parametrize(
        "tick_type,columns",
        [("TT", dict(X=int, Y=float, S=str)), ("PP", dict(X=otp.string[555], Y=otp.nsectime)), ("GG", {})],
    )
    def test_schema_deduce(self, tick_type, columns):

        if tick_type == 'GG':
            with pytest.warns(match="Can't find not empty day for the last 5 days"):
                data = otp.Empty(db="DB_A", tick_type=tick_type)
        else:
            data = otp.Empty(db="DB_A", tick_type=tick_type)

        assert len(data.schema) == len(columns)

        for name, dtype in columns.items():
            assert name in data.columns()
            assert data[name].dtype is dtype

    @pytest.mark.parametrize(
        "schema", [
            (dict(X=int, Y=float, Z=str)),
            (dict(X=otp.string[1099], Y=otp.nsectime, Z=otp.msectime)),
            ({}),
        ]
    )
    def test_manually_set_schema(self, schema):
        data = otp.Empty(schema=schema)

        assert len(data.columns(skip_meta_fields=True)) == len(schema)

        for name, dtype in schema.items():
            assert name in data.columns()
            assert data[name].dtype is dtype

    def test_join_with_query(self):
        def query_1():
            return otp.Empty(db="DB_A", tick_type="TT")

        def query_2():
            return otp.DataSource(db="DB_A", tick_type="TT")

        data = otp.Tick(U=0)
        res_1 = data.join_with_query(query_1, symbol="S1", how="inner")
        res_2 = data.join_with_query(query_2, symbol="S1", how="inner")

        df_1 = otp.run(res_1)
        df_2 = otp.run(res_2)

        assert len(df_1) == 0

        assert len(df_2) == 1
        assert df_2["X"][0] == -1
        assert df_2["Y"][0] == 0.1
        assert df_2["S"][0] == "a"
        assert df_2["U"][0] == 0

    def test_empty_db(self):
        lh = otp.Tick(db=None, X=0)
        rh = otp.Empty(db=None)

        res = lh + rh

        df = otp.run(res, symbols='DB_A::A')

        assert len(df) == 1
        assert all(df['X'] == [0])
