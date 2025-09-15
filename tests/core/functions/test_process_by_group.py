import os
import pytest
import onetick.py as otp


class TestEmptySession:

    @pytest.fixture(scope="function", autouse=True)
    def session(self, monkeypatch, f_session):
        for option in otp.config.get_changeable_config_options():
            monkeypatch.setattr(otp.config, option, otp.config.default)
        yield f_session

    def test_no_default_symbol(self):
        # PY-1258
        with pytest.raises(ValueError, match='onetick.py.config.default_symbol is not set!'):
            _ = otp.config.default_symbol
        data = otp.Ticks(
            X=[1, 1, 2, 2],
            Y=[1, 2, 3, 4],
            symbol="LOCAL::cat",
            start=otp.dt("2025-01-01"),
            end=otp.dt("2025-01-02"),
        )
        df = otp.run(data)
        assert list(df['Y']) == [1, 2, 3, 4]

        def func(source):
            return source.first()

        data_gr = data.process_by_group(func, group_by=["X"])
        df = otp.run(data_gr)
        assert list(df['Y']) == [1, 3]


class TestBase:
    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session):
        yield c_session

    def small_tick_source(self):
        data = otp.Ticks(dict(x=[1, 2, 2], y=["a", "a", "b"], offset=[1, 2, 3]))
        return data

    def large_tick_source(self):
        data = otp.Ticks(
            dict(
                x=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                y=["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
                a=[1, 1, 1, 1, 1, 2, 2, 2, 2, 2],
                b=["a", "b", "a", "b", "a", "b", "a", "b", "a", "b"],
            )
        )
        return data

    def test_empty(self):
        data = self.small_tick_source()

        def process_func(source):
            return source

        res = data.process_by_group(process_func)
        assert set(res.columns(skip_meta_fields=True)) == {"x", "y"}
        assert res["x"].dtype is int
        assert res["y"].dtype is str

        df = otp.run(res)
        assert df["x"][0] == 1 and df["y"][0] == "a"
        assert df["Time"][0] == otp.config['default_start_time'] + otp.Milli(1)
        assert df["x"][1] == 2 and df["y"][1] == "a"
        assert df["Time"][1] == otp.config['default_start_time'] + otp.Milli(2)
        assert df["x"][2] == 2 and df["y"][2] == "b"
        assert df["Time"][2] == otp.config['default_start_time'] + otp.Milli(3)

    def test_inplace(self):
        data = self.small_tick_source()

        def process_func(source):
            source['z'] = 3
            return source

        data.process_by_group(process_func, inplace=True)

        assert set(data.schema.keys()) == {"x", "y", "z"}
        df = otp.run(data)

        assert all(df['x'] == [1, 2, 2])
        assert all(df['y'] == ['a', 'a', 'b'])
        assert all(df['z'] == [3, 3, 3])

    def test_group_by(self):
        data = self.small_tick_source()

        def process_func(source):
            source = source.first()
            source["z"] = 3
            return source

        res = data.process_by_group(process_func, group_by=["y"])
        assert set(res.columns(skip_meta_fields=True)) == {"x", "y", "z"}
        assert res["x"].dtype is int
        assert res["y"].dtype is str
        assert res["z"].dtype is int

        df = otp.run(res)
        assert df["x"][0] == 1 and df["y"][0] == "a" and df["z"][0] == 3
        assert df["x"][1] == 2 and df["y"][1] == "b" and df["z"][1] == 3

    def test_group_by_2(self):
        data = self.large_tick_source()

        def process_func(source):
            return source.agg({"x_sum": otp.agg.sum(source["x"]), "y_first": otp.agg.first(source["y"])})

        res = data.process_by_group(process_func, group_by=["a", "b"])

        df = otp.run(res)
        assert df["a"][0] == 1 and df["b"][0] == "a" and df["x_sum"][0] == 9 and df["y_first"][0] == "a"
        assert df["a"][1] == 1 and df["b"][1] == "b" and df["x_sum"][1] == 6 and df["y_first"][1] == "b"
        assert df["a"][2] == 2 and df["b"][2] == "b" and df["x_sum"][2] == 24 and df["y_first"][2] == "f"
        assert df["a"][3] == 2 and df["b"][3] == "a" and df["x_sum"][3] == 16 and df["y_first"][3] == "g"

    def test_no_group_by_field(self):
        data = self.small_tick_source()

        def process_func(source):
            return source

        with pytest.raises(ValueError):
            data.process_by_group(process_func, group_by=["z"])

    def test_multiple_sources_empty(self):
        data = self.small_tick_source()

        def process_func(source):
            source1 = source.copy()
            source2 = source.copy()
            return [source1, source2]

        res1, res2 = data.process_by_group(process_func)
        assert set(res1.columns(skip_meta_fields=True)) == {"x", "y"}
        assert res1["x"].dtype is int
        assert res1["y"].dtype is str

        df1 = otp.run(res1)
        assert df1["x"][0] == 1 and df1["y"][0] == "a"
        assert df1["Time"][0] == otp.config['default_start_time'] + otp.Milli(1)
        assert df1["x"][1] == 2 and df1["y"][1] == "a"
        assert df1["Time"][1] == otp.config['default_start_time'] + otp.Milli(2)
        assert df1["x"][2] == 2 and df1["y"][2] == "b"
        assert df1["Time"][2] == otp.config['default_start_time'] + otp.Milli(3)

        df2 = otp.run(res2)
        assert df2["x"][0] == 1 and df2["y"][0] == "a"
        assert df2["Time"][0] == otp.config['default_start_time'] + otp.Milli(1)
        assert df2["x"][1] == 2 and df2["y"][1] == "a"
        assert df2["Time"][1] == otp.config['default_start_time'] + otp.Milli(2)
        assert df2["x"][2] == 2 and df2["y"][2] == "b"
        assert df2["Time"][2] == otp.config['default_start_time'] + otp.Milli(3)

    def test_multiple_sources_different_branches(self):
        data = self.small_tick_source()

        def process_func(source):
            source["z"] = 3
            source2 = source.copy()
            source = source.first()
            source2["xz"] = source2["x"]
            return source, source2

        res1, res2 = data.process_by_group(process_func, group_by=["y"])

        assert set(res1.columns(skip_meta_fields=True)) == {"x", "y", "z"}
        df1 = otp.run(res1)
        assert df1["x"][0] == 1 and df1["y"][0] == "a" and df1["z"][0] == 3
        assert df1["x"][1] == 2 and df1["y"][1] == "b" and df1["z"][1] == 3

        assert set(res2.columns(skip_meta_fields=True)) == {"x", "y", "z", "xz"}
        df2 = otp.run(res2)
        assert df2["x"][0] == 1 and df2["y"][0] == "a" and df2["z"][0] == 3 and df2["xz"][0] == 1
        assert df2["x"][1] == 2 and df2["y"][1] == "a" and df2["z"][1] == 3 and df2["xz"][1] == 2
        assert df2["x"][2] == 2 and df2["y"][2] == "b" and df2["z"][2] == 3 and df2["xz"][2] == 2

    def test_multiple_sources_inplace(self):
        data = self.small_tick_source()

        def process_func(source):
            return source.copy(), source.copy()

        with pytest.raises(ValueError):
            data.process_by_group(process_func, inplace=True)

    def test_single_output_pin(self, par_dir):
        ''' Check that single output does not assign an output pin.
        It could be verified applying a nested query, and if there would be
        a pin, then the whole query would have broken links '''
        data = self.small_tick_source()

        def process_func(source):
            source = source.first()
            source["z"] = 3
            return source

        res = data.process_by_group(process_func, group_by=["y"])
        assert set(res.schema.keys()) == {"x", "y", "z"}
        assert res["x"].dtype is int
        assert res["y"].dtype is str
        assert res["z"].dtype is int

        # makes x = x * 2
        q = otp.query(os.path.join(par_dir, 'otqs', 'update1.otq') + '::update')
        res = res.apply(q)

        df = otp.run(res)

        assert all(df['x'] == [2, 4])
        assert all(df['y'] == ['a', 'b'])
        assert all(df['z'] == [3, 3])

    def test_copy_source(self):
        data = self.small_tick_source()

        def process_func(source):
            source = source.first()
            source["z"] = 3
            return source

        res = data.process_by_group(process_func, group_by=["y"])
        assert set(res.schema.keys()) == {"x", "y", "z"}
        assert res["x"].dtype is int
        assert res["y"].dtype is str
        assert res["z"].dtype is int

        df = otp.run(res)
        assert df["x"][0] == 1 and df["y"][0] == "a" and df["z"][0] == 3
        assert df["x"][1] == 2 and df["y"][1] == "b" and df["z"][1] == 3

        res_copy = res.copy()
        df_copy = otp.run(res_copy)
        assert df_copy["x"][0] == 1 and df_copy["y"][0] == "a" and df_copy["z"][0] == 3
        assert df_copy["x"][1] == 2 and df_copy["y"][1] == "b" and df_copy["z"][1] == 3
