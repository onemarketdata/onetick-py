import os
import datetime
from pathlib import Path
from onetick.py.compatibility import (
    has_max_expected_ticks_per_symbol,
    has_password_param,
    has_timezone_parameter,
    has_query_encoding_parameter,
    is_repeat_with_field_name_works_correctly,
    is_max_concurrency_with_webapi_supported,
    OnetickVersion,
)
from onetick.py.configuration import nothing

from onetick.py.otq import otq, pyomd
import numpy as np
import pandas as pd
import pytest
import re
from io import StringIO

from dateutil.tz import gettz

import onetick.py as otp
from onetick.py.types import time2nsectime
from onetick.py.compatibility import is_supported_uint_numpy_interface
from onetick.py.otq import pyomd

DEFAULT_USE_FT = 'FALSE'


@pytest.fixture(scope='module')
def session(session):
    db = otp.DB('TMP_DB')
    session.use(db)
    db.add(otp.Tick(A=1), tick_type='TT', symbol='S1', date=otp.config.default_start_time)
    db.add(otp.Tick(B=2), tick_type='TT', symbol='S2', date=otp.config.default_start_time)
    db.add(otp.Tick(C=3), tick_type='TT', symbol='S1', date=otp.config.default_start_time + otp.Day(1))
    db.add(otp.Tick(D=4), tick_type='TT', symbol='S2', date=otp.config.default_start_time + otp.Day(1))

    db.add(otp.Tick(A=1), tick_type='TT', symbol='DT1', date=otp.config.default_start_time)
    db.add(otp.Tick(D=otp.dt(2022, 1, 1)),
           tick_type='TT', symbol='DT1', date=otp.config.default_start_time + otp.Day(1))
    yield session


class MonkeyError(Exception):
    def __init__(self, message, *args, **kwargs):
        super().__init__(message)
        self.args = args
        self.kwargs = kwargs


@pytest.fixture(scope="function")
def dummy_run(f_session, monkeypatch):
    def run(*args, **kwargs):
        raise MonkeyError('', *args, **kwargs)
        # due to structure of onetick.py.run and onetick.py.__init__ we cah't use monkeypatch and have to use this hack

    # need it to initialize compatibility module (it executes otq.run once)
    otp.run(otp.Tick(A=1))
    monkeypatch.setattr(otq, 'run', run)


class TestSource:
    def test_source(self, f_session):
        data = otp.Ticks(X=["A", "B"])
        data["S"] = data["_SYMBOL_NAME"]
        result = otp.run(data, symbols="A", start=otp.datetime(2019, 1, 2), end=otp.datetime(2019, 1, 3),
                         timezone="GMT")
        assert all(result["X"] == ["A", "B"])
        assert all(result["S"] == ["A", "A"])
        assert all(result["Time"] == [otp.datetime(2019, 1, 2), otp.datetime(2019, 1, 2, microsecond=1000)])

    @pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                        reason='log_symbol is not supported in WebAPI (callbacks issue)')
    def test_log_symbol(self, f_session, monkeypatch, capsys):
        monkeypatch.setattr(otp.config, 'log_symbol', True)
        data = otp.Ticks(X=["A", "B"])
        data["S"] = data["_SYMBOL_NAME"]
        result = otp.run(data, symbols="A", start=otp.datetime(2019, 1, 2), end=otp.datetime(2019, 1, 3),
                         timezone="GMT")
        assert result is None
        assert len(capsys.readouterr().out.strip('\n').split('\n')) == 2

    @pytest.mark.parametrize('arg,value', [('timezone', 'Europe/London'),
                                           ('context', 1),
                                           ('username', 1),
                                           ('alternative_username', 1),
                                           ('batch_size', 1),
                                           ('query_properties', pyomd.QueryProperties()),
                                           ('concurrency', 1),
                                           ('apply_times_daily', 1),
                                           ('query_params', 1),
                                           ('time_as_nsec', 1),
                                           ('treat_byte_arrays_as_strings', 1),
                                           ('output_matrix_per_field', 1),
                                           ('return_utc_times', 1),
                                           ('connection', 1),
                                           ('callback', 1),
                                           ('svg_path', 1),
                                           ('use_connection_pool', 1)])
    def test_pass_args(self, dummy_run, arg, value):
        if (
            os.getenv('OTP_WEBAPI_TEST_MODE', False) and
            arg == 'concurrency' and not is_max_concurrency_with_webapi_supported()
        ):
            return

        params = {arg: value}

        data = otp.Ticks(X=["A", "B"])
        try:
            otp.run(data,
                    **params)
        except MonkeyError as e:
            if arg == 'concurrency':
                arg = 'max_concurrency'
            elif arg == 'time_as_nsec':
                value = True    # this argument always set to True for otp.Source
            assert e.kwargs[arg] is value

    def test_encoding(self, f_session):
        data = ['AA測試AA']
        source = otp.Ticks({'A': data})

        if has_query_encoding_parameter(throw_warning=True):
            result = otp.run(source, encoding="utf-8")
            assert result["A"][0] == data[0]
        else:
            with pytest.warns(UserWarning, match='which is supported starting from release'):
                otp.run(source, encoding="utf-8")

    def test_start_end_time(self, f_session, monkeypatch):
        monkeypatch.setattr(otp.config.__class__.__dict__.get('default_start_time'), '_set_value', None)
        monkeypatch.setattr(otp.config.__class__.__dict__.get('default_end_time'), '_set_value', None)

        # date spicified on single source
        data = otp.Tick(A=1, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 2), db="LOCAL")
        otp.run(data, symbols='LOCAL::A')

        # test multiple sources
        t1 = otp.Tick(x=1, offset=1)
        t2 = otp.Tick(y=2, start=otp.dt(2022, 1, 1), end=otp.dt(2022, 1, 2))
        data = otp.join_by_time([t1, t2], how="inner")
        otp.run(data)


class TestQuery:
    SYMBOLS = ["DEMO_L1::A", "DEMO_L1::B", "DEMO_L1::C"]

    def test_query_param(self, cur_dir, f_session):
        q = otp.query(cur_dir + "otqs" + "query_with_param.otq::query", TYPE="LONG", PARAM=1)
        result = otp.run(q, output_structure="df")
        for s in self.SYMBOLS:
            assert all(result[s]["X"] == [1])

    def test_run_param(self, cur_dir, f_session):
        q = otp.query(cur_dir + "otqs" + "query_with_param.otq::query")
        result = otp.run(q, query_params=dict(TYPE="LONG", PARAM=2))
        for s in self.SYMBOLS:
            assert all(result[s]["X"] == [2])

    @pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                        reason='log_symbol is not supported in WebAPI (callbacks issue)')
    def test_log_symbol(self, f_session, monkeypatch, capsys, cur_dir):
        monkeypatch.setattr(otp.config, 'log_symbol', True)
        q = otp.query(cur_dir + "otqs" + "query_with_param.otq::query")
        result = otp.run(q, query_params=dict(TYPE="LONG", PARAM=2))
        assert result is None
        assert len(capsys.readouterr().out.strip('\n').split('\n')) == 4

    @pytest.mark.xfail(reason="PY-207", strict=True)
    def test_escaping(self, cur_dir, f_session):
        q = otp.query(cur_dir + "otqs" + "query_with_param.otq::query", TYPE="STRING", PARAM='A"')
        result = otp.run(q)
        for s in self.SYMBOLS:
            assert all(result[s]["X"] == ['A"'])

    def test_param_in_both(self, cur_dir, f_session):
        q = otp.query(cur_dir + "otqs" + "query_with_param.otq::query", PARAM=1)
        with pytest.raises(ValueError, match="please specify parameters in query or in otp.run only"):
            otp.run(q, query_params=dict(PARAM=2))


class TestGraphAndEp:
    @pytest.fixture(scope="class")
    def db(self):
        db = otp.DB("DB")
        db.add(otp.Ticks(X=[1, 2]), symbol="A", tick_type="TRD")
        return db

    @pytest.fixture(scope="class")
    def session(self, db):
        with otp.Session() as session:
            session.use(db)
            yield session

    @pytest.mark.parametrize("graph_func", [otq.GraphQuery, None])
    def test_graph(self, db, session, graph_func):
        graph_func = graph_func if graph_func else (lambda x: x)  # test with graph or raw EP
        symbol = f"{db.name}::A"
        data = graph_func(otq.Passthrough().tick_type("TRD"))
        result = otp.run(data, symbols=symbol, output_structure="map")
        assert all(result[symbol]["X"] == [1, 2])


class TestSymbol:
    @pytest.fixture(scope="class")
    def db(self):
        db = otp.DB("DB")
        db.add(otp.Ticks(X=[1, 2]), symbol="AA", tick_type="TRD")
        db.add(otp.Ticks(X=[1, 2]), symbol="BB", tick_type="TRD")
        return db

    @pytest.fixture(scope="class")
    def session(self, db):
        with otp.Session() as session:
            session.use(db)
            yield session

    def test_otq_symbol_on_graph(self, db, session):
        symbol = otq.Symbol(f"{db.name}::AA", params={"PARAM": "PARAM"})
        data = otq.Passthrough().tick_type("TRD")
        data.sink(otq.AddField("P", "_SYMBOL_PARAM.PARAM"))
        result = otp.run(data, symbols=symbol, output_structure="map")
        assert all(result[symbol]["X"] == [1, 2])
        assert all(result[symbol]["P"] == ["PARAM", "PARAM"])

    def test_otq_symbol_on_source(self, db, session):
        symbol = otq.Symbol(f"{db.name}::AA", params={"PARAM": "PARAM"})
        data = otp.DataSource(tick_type="TRD")
        data["P"] = data.Symbol['PARAM', str]
        result = otp.run(data, symbols=symbol)
        assert all(result["X"] == [1, 2])
        assert all(result["P"] == ["PARAM", "PARAM"])

    @pytest.mark.parametrize("data", (otq.Passthrough().tick_type("TRD"), otp.DataSource(tick_type="TRD")))
    def test_collection(self, db, session, data):
        symbol_names = [f"{db.name}::AA", f"{db.name}::BB"]
        symbols = [f"{db.name}::AA", otq.Symbol(f"{db.name}::BB")]
        result = otp.run(data, symbols=symbols)
        for s in symbol_names:
            assert all(result[s]["X"] == [1, 2])
        result = result if isinstance(result, dict) else result.get_dict()
        assert len(result) == 2

    @pytest.mark.skipif(not is_repeat_with_field_name_works_correctly(), reason="REPEAT EP on fields is broken")
    @pytest.mark.parametrize("data", (otq.Passthrough().tick_type("TRD"), otp.DataSource(tick_type="TRD")))
    def test_source_and_custom(self, db, session, data):
        symbol = otp.Ticks(SYMBOL=["A", "B"])
        symbol["SYMBOL"] *= 2
        symbol["SYMBOL"] = f"{db.name}::" + symbol["SYMBOL"]

        result = otp.run(data, symbols=symbol)
        for s in [f"{db.name}::AA", f"{db.name}::BB"]:
            assert all(result[s]["X"] == [1, 2])
        result = result if isinstance(result, dict) else result.get_dict()
        assert len(result) == 2

    def test_custom_with_otp_symbols(self, db, session):
        symbols = otp.Symbols(db, for_tick_type="TRD")
        df = otp.run(symbols)
        assert all(df["SYMBOL_NAME"] == ["AA", "BB"])
        data = otp.DataSource(db, symbol=symbols)
        df = otp.run(data)
        assert all(df["X"] == [1, 1, 2, 2])

    def test_source_without_symbol_name_as_symbol(self, session):
        source1 = otp.Tick(A=1, SYMBOL_NAME=2)
        sym = source1.to_otq()
        sym = otp.Query(otp.query(sym))
        source2 = otp.Tick(B=2)
        with pytest.warns(
            match=('Using as a symbol list a source without "SYMBOL_NAME" field and with more than one field!'
                   " This won't work unless the schema is incomplete")
        ):
            df = list(otp.run(source2, symbols=sym).values())[0]
        assert len(df) == 1
        assert df['B'][0] == 2


class TestTime:
    MSEC_IN_HOUR = 60 * 60 * 1000

    @pytest.fixture(scope="class")
    def db(self):
        db = otp.DB("DB")
        db.add(otp.Ticks(X=[0, 1, 2, 3, 4], offset=[i * self.MSEC_IN_HOUR for i in range(5)]),
               symbol="A", tick_type="TRD", timezone='EST5EDT')
        return db

    @pytest.fixture(scope="class")
    def session(self, db):
        with otp.Session() as session:
            session.use(db)
            yield session

    @pytest.mark.parametrize("data", (otq.Passthrough().tick_type("TRD"), otp.DataSource(tick_type="TRD")))
    def test_start(self, db, session, data):
        symbol = f"{db.name}::A"
        result = otp.run(data, symbols=symbol, start=otp.config['default_start_time'] + otp.Hour(1), timezone="EST5EDT",
                         output_structure="map")

        assert all(result[symbol]["X"] == [1, 2, 3, 4])

    @pytest.mark.parametrize("data", (otq.Passthrough().tick_type("TRD"), otp.DataSource(tick_type="TRD")))
    def test_end(self, db, session, data):
        symbol = f"{db.name}::A"
        result = otp.run(data, symbols=symbol, end=otp.config['default_start_time'] + otp.Hour(4), timezone="EST5EDT",
                         output_structure="map")
        assert all(result[symbol]["X"] == [0, 1, 2, 3])

    @pytest.mark.parametrize("data", (otq.Passthrough().tick_type("TRD"), otp.DataSource(tick_type="TRD")))
    def test_nano_time(self, db, session, data):
        symbol = f"{db.name}::A"
        start = otp.config['default_start_time'] + otp.Nano(1)
        assert isinstance(start, pd.Timestamp)
        end = otp.config['default_start_time'] + otp.Hour(3) + otp.Nano(1)
        end = otp.datetime(end)
        assert isinstance(end, otp.datetime)
        result = otp.run(data, symbols=symbol, start=start, end=end, timezone="EST5EDT",
                         output_structure="df")
        assert all(result["X"] == [1, 2, 3])

    @pytest.mark.parametrize("date", [datetime.datetime(2019, 1, 1), datetime.date(2019, 1, 1), "20190101",
                                      pd.Timestamp(2019, 1, 1), otp.datetime(2019, 1, 1), otp.date(2019, 1, 1)])
    def test_symbol_date(self, date, session):
        X = otp.Ticks(X=[1])
        otp.run(X, symbol_date=date)  # check there are no errors TODO: actual case

    @pytest.mark.parametrize("tzinfo", [gettz("EST5EDT"), None])
    def test_timezones_with_datetime(self, session, db, tzinfo):
        assert isinstance(otp.config['default_start_time'], datetime.datetime)
        assert isinstance(otp.config['default_end_time'], datetime.datetime)
        self._check_timezones(db, otp.config['default_start_time'], otp.config['default_end_time'], tzinfo)

    @pytest.mark.parametrize("tzinfo", [gettz("EST5EDT"), None])
    @pytest.mark.parametrize("start, end", [(otp.dt(otp.config['default_start_time']),
                                             otp.dt(otp.config['default_end_time'])),
                                            (pd.Timestamp(otp.config['default_start_time']),
                                             pd.Timestamp(otp.config['default_end_time']))])
    def test_timezones_with_other_types(self, session, db, start, end, tzinfo):
        assert isinstance(start, (otp.dt, pd.Timestamp))
        self._check_timezones(db, start, end, tzinfo)

    def _check_timezones(self, db, start, end, tzinfo):
        symbol = f"{db.name}::A"
        start = start.replace(tzinfo=tzinfo)
        end = end.replace(tzinfo=tzinfo)
        graph = otq.GraphQuery(otq.Passthrough().tick_type("TRD"))
        timezone = "Europe/Moscow"
        result = otp.run(graph,
                         start=start, end=end, symbols=symbol, timezone=timezone,
                         output_structure="map")
        assert all(expected == str(actual) for expected, actual in
                   zip((f"2003-12-01T{i:02d}:00:00.000000000" for i in range(8, 13)), result[symbol]["Time"]))
        if not isinstance(start, datetime.datetime):
            start = datetime.datetime(start.year, start.month, start.day, tzinfo=start.tzinfo)
            end = datetime.datetime(end.year, end.month, end.day, end.hour, end.minute, end.second,
                                    tzinfo=end.tzinfo)
        kwargs = {}
        result = otp.run(graph,
                         start=start, end=end, symbols=symbol, timezone=timezone,
                         **kwargs)
        # WebAPI in both output_structures returns Time with nano seconds
        # but onetick.query here returns Time with micro seconds
        assert all(expected in str(actual) for expected, actual in
                   zip((f"2003-12-01 {i:02d}:00:00" for i in range(8, 13)), list(result["Time"])))

    def test_none_value_as_start_end(self, session, cur_dir):
        path = str(cur_dir + "otqs" + "start_end_none.otq")
        df = otp.run(path, start=None, end=None, timezone="GMT")
        assert all(df["X"] == [1])
        assert all(df["Time"] == [otp.dt(2003, 12, 1, 21, 30)])

    def test_start_end_expressions(self, session, cur_dir):
        path = str(cur_dir + "otqs" + "start_end_none.otq")
        end = otp.dt(2020, 12, 2, 23, 30)
        start = otp.dt(2020, 12, 2, 1)
        inc = 1023
        df = otp.run(otp.Query(path),
                     start_time_expression=f"nsectime({time2nsectime(start)} + {inc})",
                     end_time_expression=f"nsectime({time2nsectime(end)} + {inc})",
                     symbols="DB::SYMBOL", timezone="GMT")
        assert all(df["X"] == [1])
        assert all(df["Time"] == [end + otp.Nano(inc)])


class TestOutputStructure:
    def test_on_source(self, f_session):
        X = otp.Ticks(X=[1, 2])
        path_to_otq = X.to_otq(otp.utils.TmpFile(".otq"))
        for X in (X, path_to_otq, otp.Query(path_to_otq), otp.query(path_to_otq)):
            result = otp.run(X, output_structure="list")
            assert isinstance(result, list)
            assert all(result[0][1][1][1] == [1, 2])
            result = otp.run(X, output_structure="map")
            assert isinstance(result, otq.SymbolNumpyResultMap)
            assert all(result[otp.config.default_symbol]["X"] == [1, 2])
            result = otp.run(X, output_structure="df")
            assert isinstance(result, pd.DataFrame)
            assert all(result["X"] == [1, 2])

    def test_on_eps(self, f_session):
        X = (otq.TickGenerator(bucket_interval=0, bucket_time="BUCKET_START", fields="long X = 1").tick_type("TRD")
             >> otq.Merge() <<
             otq.TickGenerator(bucket_interval=0, bucket_time="BUCKET_START", fields="long X = 2").tick_type("TRD"))
        result = otp.run(X, symbols="DEMO_L1::A", output_structure="list")
        assert isinstance(result, list)
        assert all(result[0][1][1][1] == [1, 2])
        result = otp.run(X, symbols="DEMO_L1::A", output_structure="map")
        assert isinstance(result, otq.SymbolNumpyResultMap)
        assert all(result["DEMO_L1::A"]["X"] == [1, 2])
        result = otp.run(X, symbols="DEMO_L1::A", output_structure="df")
        assert isinstance(result, pd.DataFrame)
        assert all(result["X"] == [1, 2])


class TestNodeName:
    # lambda x: x - is for test path to the query as a string
    @pytest.mark.parametrize("func", [lambda x: x, otp.query])
    def test_2_pass(self, session, cur_dir, func):
        path = str(cur_dir + "otqs" + "two_output_nodes.otq")
        query = func(path)
        result = otp.run(query, node_name="node_1", output_structure="df")
        assert all(result["X"] == [1])
        result = otp.run(query, node_name="node_2", output_structure="df")
        assert all(result["X"] == [2])
        with pytest.raises(Exception, match=re.escape("No passed node name(s) were found in the results. "
                                                      "Passed node names were: ['node_3']")):
            otp.run(query, node_name="node_3", output_structure="df")


class TestCallableQuery:

    @pytest.mark.parametrize('method', [False, True])
    @pytest.mark.parametrize('symbols,exp',
                             [(otp.Tick(SYMBOL_NAME='A'), 'A'),
                              ('A', 'A'),
                              (['A'], 'A')])
    def test_function(self, session, method, symbols, exp):

        def f(symbol):
            return otp.Ticks(S=[symbol.name])

        class Some:
            def impl(self, symbol):
                return otp.Ticks(S=[symbol.name])

        obj = Some()

        if method:
            to_check = obj.impl
        else:
            to_check = f

        res = otp.run(to_check, symbols=symbols)

        if 'A' in res:
            res = res['A']

        assert all(res['S'] == exp)


def test_otq_with_timezone_local(session, monkeypatch):
    monkeypatch.setattr(otp.config, 'tz', None)
    t = otp.Tick(A=1)
    t['TZ'] = t['_TIMEZONE']
    tmp_file = otp.utils.TmpFile(suffix='.otq')
    t.to_otq(tmp_file.path)
    with open(tmp_file.path) as f:
        for line in f:
            if line.strip() == 'TZ =':
                break
        else:
            raise AssertionError
    df = otp.run(t)
    assert df['Time'][0] == otp.config['default_start_time']
    if not os.getenv('OTP_WEBAPI', False):
        assert df['TZ'][0] == ''


def test_database_local_timezone(session, monkeypatch):
    monkeypatch.setattr(otp.config, 'tz', None)
    with pytest.warns(match='default timezone is local and is not known at this moment'):
        db = otp.DB('DB_NAME')
    db.add(otp.Tick(A=123), tick_type='TICK_TYPE', symbol='SYMB')
    session.use(db)

    t = otp.DataSource('DB_NAME', tick_type='TICK_TYPE', symbol='SYMB')
    df = otp.run(t)
    assert df['A'][0] == 123


@pytest.mark.parametrize('property_name,property_value,default_use_ft,use_ft_output',
                         [('USE_FT', 'TRUE', None, 'TRUE'),
                          ('USE_FT', 'FALSE', None, 'FALSE'),
                          (None, None, None, DEFAULT_USE_FT),
                          ('CEP_KEEP_ORIGINAL_TIMESTAMP', 'FALSE', None, DEFAULT_USE_FT),
                          ('USE_FT', 'TRUE', 'TRUE', 'TRUE'),
                          ('USE_FT', 'FALSE', 'TRUE', 'FALSE'),
                          (None, None, 'TRUE', 'TRUE'),
                          ('CEP_KEEP_ORIGINAL_TIMESTAMP', 'FALSE', 'TRUE', 'TRUE'),
                          ('USE_FT', 'TRUE', 'FALSE', 'TRUE'),
                          ('USE_FT', 'FALSE', 'FALSE', 'FALSE'),
                          (None, None, 'FALSE', 'FALSE'),
                          ('CEP_KEEP_ORIGINAL_TIMESTAMP', 'FALSE', 'FALSE', 'FALSE'),
                          ])
def test_fault_tolerance(
    session,
    property_name,
    property_value,
    default_use_ft,
    use_ft_output,
    monkeypatch,
):
    if default_use_ft is not None:
        monkeypatch.setattr(otp.config, 'default_fault_tolerance', otp.config.default)
        monkeypatch.setenv('OTP_DEFAULT_FAULT_TOLERANCE', default_use_ft)
    tick = otp.Tick(A=1)
    tick['USE_FT'] = otp.raw('GET_QUERY_PROPERTY("USE_FT")', otp.string[64])
    if property_name is not None:
        qp = pyomd.QueryProperties()
        qp.set_property_value(property_name, property_value)
        res = otp.run(tick, query_properties=qp)
    else:
        res = otp.run(tick)
    assert all(res['USE_FT'] == [use_ft_output])


class TestCallback:
    @pytest.fixture
    def callback_base(self, request):
        yield getattr(request, 'param', otq.CallbackBase)

    @pytest.fixture
    def printing_callback(self, callback_base):
        class PrintingCallback(callback_base):
            def __init__(self):
                otq.CallbackBase.__init__(self)
                self.num_ticks = 0
                self.str_buffer = StringIO()

            def process_symbol_name(self, symbol_name):
                self.str_buffer.write(f'Symbol name: {symbol_name}\n')

            def process_tick_type(self, tick_type):
                self.str_buffer.write(f'Tick type: {tick_type}\n')

            def process_tick(self, tick, time):
                self.num_ticks += 1
                self.str_buffer.write(f'{time} {tick}\n')

            def done(self):
                self.str_buffer.write('Done.\n')
        yield PrintingCallback

    @pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                        reason='WEBAPI is not compatible with callback output_mode')
    def test_onetick_query(self, session, printing_callback):
        t = otp.Tick(A=1)
        otq_file = t.to_otq()
        cb = printing_callback()
        result = otp.run(otq_file,
                         callback=cb)
        assert result is None
        assert cb.num_ticks == 1
        assert cb.str_buffer.getvalue() == (
            "Symbol name: AAPL\n"
            "Tick type: DEMO_L1::ANY\n"
            "2003-12-01 05:00:00 {'A': 1}\n"
            "Done.\n"
        )

    @pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                        reason='WEBAPI is not compatible with callback output_mode')
    @pytest.mark.parametrize('callback_base', (otq.CallbackBase, otp.CallbackBase))
    def test_onetick_py(self, session, callback_base, printing_callback):
        t = otp.Tick(A=1)
        df = otp.run(t)
        assert df['A'][0] == 1
        cb = printing_callback()
        result = otp.run(t, callback=cb)
        assert result is None
        assert cb.num_ticks == 1
        assert cb.str_buffer.getvalue() == (
            "Symbol name: AAPL\n"
            "Tick type: DEMO_L1::ANY\n"
            "2003-12-01 05:00:00 {'A': 1}\n"
            "Done.\n"
        )

    @pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                        reason='WEBAPI is not compatible with callback output_mode')
    def test_running_simulation(self, session):
        # it's best to run this test with pytest -s
        class TimesCallback(otp.CallbackBase):
            def __init__(self):
                self.times = []

            def process_tick(self, tick, time):
                now = datetime.datetime.now()
                self.times.append(now.timestamp())
                print(f'{now}: process_tick callback: {tick}')

        # generate tick every second
        data = otp.Tick(ID=otp.rand(min_value=1, max_value=2),
                        BUY_FLAG=otp.rand(0, 1),
                        PRICE=otp.rand(1, 1000),
                        SIZE=otp.rand(1, 100),
                        bucket_interval=1)
        cb = TimesCallback()
        print()
        print('START')
        otp.run(data,
                running=True,
                callback=cb,
                start=otp.now() - otp.Second(5),
                end=otp.now() + otp.Second(5))
        print('DONE')

        # ticks before now will be generated instantly
        # future ticks will be generated each second
        num_ticks = len(cb.times)
        assert num_ticks in (10, 11)
        border = 5 if num_ticks == 10 else 6
        # first 5 ticks (before start) arrived roughly at the same time
        assert sum(np.diff(cb.times[:border])) < 0.1
        # all other ticks arrived with roughly one second difference
        # (it can be less or more than one second depending on simulation conditions)
        mean = np.mean(np.diff(cb.times[border:]))
        assert 0.5 <= mean < 1.5


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='manual_dataframe_callback is not supported in webapi')
class TestManualDataframeCallback:
    def test_simple(self, session):
        t = otp.Tick(A=1)
        df = otp.run(t)
        manual_df = otp.run(t, manual_dataframe_callback=True)
        assert df.equals(manual_df)

    def test_datetime(self, session):
        t = otp.Ticks(A=[1, 2])
        t['B'] = t.apply(lambda row: row['TIMESTAMP'] if row['A'] == 1 else 0)
        df = otp.run(t)
        manual_df = otp.run(t, manual_dataframe_callback=True)
        assert df.equals(manual_df)

    def test_symbols(self, session):
        t = otp.Tick(A=1)
        t['SYMBOL_NAME'] = t['_SYMBOL_NAME']
        t = t.insert_tick(where=t['_SYMBOL_NAME'] == 'A')
        t = t.insert_tick(where=t['_SYMBOL_NAME'] == 'B', num_ticks_to_insert=2)
        result = otp.run(t, symbols=['A', 'B', 'C', 'D', 'E', 'F'])
        manual_result = otp.run(t, manual_dataframe_callback=True, symbols=['A', 'B', 'C', 'D', 'E', 'F'])
        assert set(manual_result) == set(result)
        for key, df in result.items():
            assert manual_result[key].equals(df)

    def test_schema_change(self, session):
        t = otp.DataSource('TMP_DB', tick_type='TT', symbol='S1')
        df = otp.run(t)
        manual_df = otp.run(t, manual_dataframe_callback=True)
        assert df.equals(manual_df)

    def test_schema_change_datetime(self, session):
        t = otp.DataSource('TMP_DB', tick_type='TT', symbol='DT1')
        df = otp.run(t)
        manual_df = otp.run(t, manual_dataframe_callback=True)
        assert df.equals(manual_df)

    def test_schema_change_symbols(self, session):
        t = otp.DataSource('TMP_DB', tick_type='TT')
        result = otp.run(t, symbols=['S1', 'S2'])
        manual_result = otp.run(t, manual_dataframe_callback=True, symbols=['S1', 'S2'])
        assert set(manual_result) == set(result)
        for key, df in result.items():
            assert manual_result[key].equals(df)


def test_max_expected_ticks_per_symbol(session):
    t = otp.Tick(A=1)
    if not has_max_expected_ticks_per_symbol(throw_warning=True):
        with pytest.warns(UserWarning, match='which is supported starting from release'):
            otp.run(t, max_expected_ticks_per_symbol=1)
    else:
        df = otp.run(t, max_expected_ticks_per_symbol=1)
        assert df['A'][0] == 1


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='Password is not supported in WebAPI (use http_password instead)')
def test_password_param(session):
    t = otp.Tick(A=1)
    if not has_password_param(throw_warning=True):
        with pytest.warns(UserWarning, match='which is supported starting from release'):
            otp.run(t, password='password')
    else:
        df = otp.run(t, password='password')
        assert df['A'][0] == 1


@pytest.mark.parametrize('value', [OnetickVersion(True, '1.22', None, 20230714120000),
                                   OnetickVersion(False, None, 0, 20210714120000)])
def test_has_max_expected_ticks_per_symbol(session, mocker, value):
    mocker.patch('onetick.py.compatibility.get_onetick_version',
                 return_value=value)
    with pytest.warns(UserWarning, match='which is supported starting from release'):
        has_max_expected_ticks_per_symbol(throw_warning=True)


@pytest.mark.parametrize('value', [OnetickVersion(True, '1.22', None, 20230714120000),
                                   OnetickVersion(False, None, 0, 20210714120000)])
def test_has_password_param(session, mocker, value):
    mocker.patch('onetick.py.compatibility.get_onetick_version',
                 return_value=value)
    with pytest.warns(UserWarning, match='which is supported starting from release'):
        has_password_param(throw_warning=True)


@pytest.mark.parametrize('value', [OnetickVersion(True, '1.22', None, 20230714120000),
                                   OnetickVersion(False, None, 0, 20210714120000)])
def test_has_timezone_parameter(session, mocker, value):
    mocker.patch('onetick.py.compatibility.get_onetick_version',
                 return_value=value)
    with pytest.warns(UserWarning, match='which is supported starting from release'):
        has_timezone_parameter(throw_warning=True)


def test_date(session):
    t = otp.Tick(A=1)
    t['START'] = t['_START_TIME']
    t['END'] = t['_END_TIME']
    df = otp.run(t, date=datetime.datetime(2022, 1, 1, 1, 1, 1))
    assert df['START'][0] == pd.Timestamp(2022, 1, 1)
    assert df['END'][0] == pd.Timestamp(2022, 1, 2)
    df = otp.run(t, date=datetime.date(2023, 1, 1))
    assert df['START'][0] == pd.Timestamp(2023, 1, 1)
    assert df['END'][0] == pd.Timestamp(2023, 1, 2)
    df = otp.run(t, date=otp.datetime(2024, 1, 1, 1, 1, 1))
    assert df['START'][0] == pd.Timestamp(2024, 1, 1)
    assert df['END'][0] == pd.Timestamp(2024, 1, 2)
    df = otp.run(t, date=otp.date(2025, 1, 1))
    assert df['START'][0] == pd.Timestamp(2025, 1, 1)
    assert df['END'][0] == pd.Timestamp(2025, 1, 2)
    df = otp.run(t, date=pd.Timestamp(2026, 1, 1))
    assert df['START'][0] == pd.Timestamp(2026, 1, 1)
    assert df['END'][0] == pd.Timestamp(2026, 1, 2)
    with pytest.raises(ValueError):
        otp.run(t, start=otp.datetime(2021, 1, 1), date=otp.date(2022, 1, 1))
    with pytest.raises(ValueError):
        otp.run(t, end=otp.datetime(2021, 1, 1), date=otp.date(2022, 1, 1))
    with pytest.raises(ValueError):
        otp.run(t, start_time_expression='20210101000000', date=otp.date(2022, 1, 1))
    with pytest.raises(ValueError):
        otp.run(t, end_time_expression='20210101000000', date=otp.date(2022, 1, 1))


def test_exception_message(session):
    t = otp.Tick(A=1)
    t['X'] = otp.raw('okay', dtype=otp.string[64])
    with pytest.raises(Exception, match=f'.*onetick-py=={otp.__version__}, OneTick .+'):
        otp.run(t)


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='test mode fails this test, not necessary to run')
def test_main_query_generated_filename(session, monkeypatch):
    t = otp.Tick(A=1)

    # get temporary dir location
    query_file = Path(t.to_otq().split('::')[0])
    tmp_dir = query_file.parent
    assert not [f for f in os.listdir(tmp_dir) if f.endswith('.run.otq')]

    # check file with autogenerated name
    otp.run(t)
    run_files = [f for f in os.listdir(tmp_dir) if f.endswith('.run.otq')]
    assert len(run_files) == 1
    run_file_1 = run_files[0]

    # check file with static name
    monkeypatch.setattr(otp.config, 'main_query_generated_filename', 'hello')
    otp.run(t)
    run_files = [f for f in os.listdir(tmp_dir) if f.endswith('.run.otq')]
    assert len(run_files) == 1 and run_files == [run_file_1]
    run_file_2 = tmp_dir / 'hello.otq'
    assert run_file_2.exists()

    # check rewrite file with static name
    otp.run(t)
    run_files = [f for f in os.listdir(tmp_dir) if f.endswith('.run.otq')]
    assert len(run_files) == 1 and run_files == [run_file_1]

    # check file with static name with suffix
    monkeypatch.setattr(otp.config, 'main_query_generated_filename', 'bye.otq')
    otp.run(t)
    run_files = [f for f in os.listdir(tmp_dir) if f.endswith('.run.otq')]
    assert len(run_files) == 1 and run_files == [run_file_1]
    assert run_file_2.exists()
    run_file_3 = tmp_dir / 'bye.otq'
    assert run_file_3.exists()


class TestEmptyResults:
    @staticmethod
    def get_schema_from_df(df):
        return {k: str(v) for k, v in df.dtypes.to_dict().items()}

    @classmethod
    def get_result_schema(cls, result, output_schema):
        # we take only first node for each symbol
        result_schema = None

        if output_schema == 'list':
            result_schema = {
                s_result[0]: {
                    k: str(v.dtype) if hasattr(v, 'dtype') else otp.types.type2np(type(v[0]))
                    for k, v in s_result[1]
                }
                for s_result in result
            }
        elif output_schema == 'map':
            result_schema = {
                symbol: {
                    k: str(v.dtype) if hasattr(v, 'dtype') else otp.types.type2np(type(v[0]))
                    for k, v in list(s_result.values())[0][0].items()
                } for symbol, s_result in result.get_dict().items()
            }
        elif output_schema == 'df':
            if type(result) is dict:
                result_schema = {k: cls.get_schema_from_df(v) for k, v in result.items()}
            else:
                result_schema = {'DEFAULT': cls.get_schema_from_df(result)}

        return result_schema

    @pytest.mark.parametrize('output_schema', ['list', 'map', 'df'])
    def test_base(self, session, cur_dir, output_schema):
        query = otp.query(cur_dir + "otqs" + "where_clause.otq::condition_source", CONDITION='X = 0')
        source = otp.Query(query, out_pin='IF_OUT')
        source.set_schema(A=int, B=str, C=float, D=otp.types.ulong)

        result = otp.run(source, symbols=['DEMO_L1::A'], output_structure=output_schema)
        result_schema = self.get_result_schema(result, output_schema)
        result_schema = result_schema.get('DEMO_L1::A', result_schema.get('DEFAULT'))

        assert result_schema == {
            'A': 'int64',
            'B': 'object' if output_schema == 'df' else '<U64',
            'C': 'float64',
            'D': 'uint64',
            'Time': 'datetime64[ns]',
        }

    @pytest.mark.parametrize('output_schema', ['list', 'map', 'df'])
    def test_multi_symbol(self, session, cur_dir, output_schema):
        query = otp.query(
            cur_dir + "otqs" + "where_clause.otq::condition_source", CONDITION='_SYMBOL_NAME = \'DEMO_L1::C\''
        )
        source = otp.Query(query, out_pin='IF_OUT')
        source.set_schema(X=int)

        result = otp.run(source, symbols=['DEMO_L1::A', 'DEMO_L1::B', 'DEMO_L1::C'], output_structure=output_schema)
        result_schema = self.get_result_schema(result, output_schema)

        result_c = None
        if output_schema == 'list':
            result_c = [dict(res) for symbol, res, *_ in result if symbol == 'DEMO_L1::C'][0]
        elif output_schema == 'map':
            result_c = result['DEMO_L1::C']
        elif output_schema == 'df':
            result_c = {k: list(v.values()) for k, v in result['DEMO_L1::C'].to_dict().items()}

        # test for not overriding valid results
        assert result_c['X'] == [1]

        assert result_schema == {
            'DEMO_L1::A': {'X': 'int64', 'Time': 'datetime64[ns]'},
            'DEMO_L1::B': {'X': 'int64', 'Time': 'datetime64[ns]'},
            'DEMO_L1::C': {'X': 'int64', 'Time': 'datetime64[ns]'},
        }

    @pytest.mark.parametrize('output_schema', ['list', 'map', 'df'])
    def test_type_conversion(self, session, output_schema):
        data = otp.Tick(
            A1=1, A2=-3, B=2.3, C1='c', C2=otp.string[2]('d'), C3=otp.varstring('test'), C4=otp.string('test'),
            D=otp.short(2), E=otp.byte(1), F=otp.uint(1), G=otp.ulong(1),
            H=otp.msectime(1), I=otp.nsectime(15e12 + 1), J=otp.decimal(1),
            K=otp.datetime(2000, 1, 1, 1, 2, 3, 4, 5),
        )

        base_df = otp.run(data, output_structure=output_schema)

        data, _ = data[data['Time'] != data['Time']]  # NOSONAR
        empty_df = otp.run(data, output_structure=output_schema)

        empty_schema = self.get_result_schema(empty_df, output_schema)
        base_schema = self.get_result_schema(base_df, output_schema)

        if not is_supported_uint_numpy_interface():
            first_empty_response_schema = list(empty_schema.values())[0]
            first_base_response_schema = list(base_schema.values())[0]

            assert first_empty_response_schema['F'] == 'uint32'

            del first_empty_response_schema['F']
            del first_base_response_schema['F']

        if otq.webapi and output_schema != 'df':
            # WebAPI differs this field type on empty results: <U4 turns <U64
            del empty_schema['AAPL']['C3']
            del base_schema['AAPL']['C3']

        assert empty_schema == base_schema


@pytest.mark.parametrize('value', ['start', 'end'])
def test_run_without_start_end_defaults(session, monkeypatch, value):
    monkeypatch.setattr(otp.config, f'default_{value}_time', nothing)
    otp.config.show_stack_info = False
    with pytest.warns(UserWarning, match=f'time is None and default {value} time is not set'):
        try:
            otp.run(otp.Ticks(X=[1]))
        except Exception as e:
            assert 'ERROR:' in str(e)


def test_query_properties(session):
    data = otp.Ticks(X=[1, 2, 3])

    qp = pyomd.QueryProperties()
    qp.set_property_value('FT_PROPERTIES', 'min_same_host_retry_interval_sec=5')

    df = otp.run(data, query_properties=qp)
    assert list(df['X']) == [1, 2, 3]

    df = otp.run(data, query_properties={'FT_PROPERTIES': 'min_same_host_retry_interval_sec=5'})
    assert list(df['X']) == [1, 2, 3]


@pytest.mark.skipif(not otp.compatibility.are_quotes_in_query_params_supported(),
                    reason="Quotes were not supported without escaping before")
@pytest.mark.parametrize('use_file', (True, False))
@pytest.mark.parametrize('escaped', (True, False))
@pytest.mark.parametrize('quotes', ('single', 'double', 'mix'))
def test_query_params(session, use_file, escaped, quotes):
    if quotes == 'single':
        condition = "A = 'x'"
    elif quotes == 'double':
        condition = 'A= "x"'
    else:
        condition = "A = 'x'" + ' or ' + 'A = "x"'

    if escaped:
        condition = otp.query._escape_quotes_in_eval(condition)

    node = otq.TickGenerator(bucket_interval=0, fields='A="x"')
    node = node.sink(otq.WhereClause(where='$CONDITION'))
    graph = otq.GraphQuery(node)
    kwargs = dict(
        start=datetime.datetime(2022, 1, 1),
        end=datetime.datetime(2022, 1, 2),
        timezone='GMT',
        symbols='LOCAL::',
    )
    run_kwargs = dict(**kwargs, query_params={'CONDITION': condition})

    def run():
        if use_file:
            otq_file = otp.utils.TmpFile(suffix='.otq')
            graph.save_to_file(str(otq_file), 'query', **kwargs)
            result = otp.run(f"{otq_file}::query", **run_kwargs)
        else:
            result = otp.run(graph, **run_kwargs)
        return result

    if escaped:
        with pytest.raises(Exception):
            run()
    else:
        result = run()
        assert len(result['A']) == 1
        assert result['A'][0] == 'x'


@pytest.mark.parametrize('run_timezone', [
    otp.utils.default,
    'UTC',
    'EST5EDT',
    'America/New_York'
])
@pytest.mark.parametrize('datetime_with_timezone', [
    otp.dt(2022, 1, 1, tz='UTC'),
    otp.dt(2022, 1, 1, tz='EST5EDT'),
    datetime.datetime(2022, 1, 1, 0, 0, 0, tzinfo=gettz('UTC')),
    datetime.datetime(2022, 1, 1, 0, 0, 0, tzinfo=gettz('EST5EDT')),
    datetime.datetime(2022, 1, 1, 0, 0, 0, tzinfo=gettz('America/New_York')),
])
def test_start_end_timezone(session, run_timezone, datetime_with_timezone):
    tick = otp.Tick(DATE=datetime_with_timezone)
    res = otp.run(tick,
                  start=datetime_with_timezone,
                  end=datetime_with_timezone + otp.Day(1),
                  timezone=run_timezone)
    if run_timezone == otp.utils.default:
        run_timezone = otp.config.tz
    # check that first tick has the exact same time as the datetime_with_timezone
    # but as it is timezone-naive pandas.Timestamp, it will be converted to the run_timezone
    assert otp.dt(res['Time'][0], tz=run_timezone).timestamp() == datetime_with_timezone.timestamp()


@pytest.mark.xfail(os.getenv('OTP_WEBAPI_TEST_MODE'), reason='BDS-469', strict=True)
@pytest.mark.skipif(not otp.compatibility.is_get_query_property_flag_supported(),
                    reason="Second parameter of GET_QUERY_PROPERTY was not supported before")
@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False) and not is_max_concurrency_with_webapi_supported(),
                    reason="Used version of OneTick has bug with setting `concurrency` param with WebAPI.")
def test_concurrency(session):
    t = otp.Tick(A=otp.raw('GET_QUERY_PROPERTY("MAX_CONCURRENCY", true)', dtype=otp.string[64]))
    df = otp.run(t)
    if otp.compatibility.is_zero_concurrency_supported():
        assert df['A'][0] == '0'
    else:
        assert df['A'][0] == '1'
    df = otp.run(t, concurrency=None)
    assert df['A'][0] == '1'
    df = otp.run(t, concurrency=1)
    assert df['A'][0] == '1'
    df = otp.run(t, concurrency=2)
    assert df['A'][0] == '2'
    if otp.compatibility.is_zero_concurrency_supported():
        df = otp.run(t, concurrency=0)
        assert df['A'][0] == '0'
    with pytest.raises(Exception, match=r'Invalid value .+ -1'):
        otp.run(t, concurrency=-1)


@pytest.mark.parametrize('use_db,use_tt', [
    (True, False), (False, True), (True, True),
])
def test_datasource_with_symbol_params(session, use_db, use_tt):
    t = otp.DataSource('TMP_DB', tick_type='TT', symbol='S1')
    df = otp.run(t)

    sym = otp.Ticks({
        'SYMBOL_NAME': ['S1'],
        'DB': ['TMP_DB'],
        'TICK_TYPE': ['TT'],
    })
    sym_p = sym.to_symbol_param()

    db = sym_p['DB'] if use_db else 'TMP_DB'
    tt = sym_p['TICK_TYPE'] if use_tt else 'TT'

    data = otp.DataSource(db=db, tick_type=tt)
    result = otp.run(data, symbols=sym)

    assert result['S1'].equals(df)


def test_datasource_with_symbol_param_as_symbol(session):
    sym = otp.Ticks({
        'PARAM': ['S2'],
        'SYMBOL_NAME': ['S1'],
    })
    sym_p = sym.to_symbol_param()

    t1 = otp.DataSource(db='TMP_DB', symbols=None, tick_type='TT')
    t2 = otp.DataSource(db='TMP_DB', symbol=sym_p['PARAM'], tick_type='TT')

    t = otp.merge([t1, t2], identify_input_ts=True)
    result = otp.run(t, symbols=sym)['S1']

    assert list(result['A']) == [1, 0, 0, 0]
    assert list(result['B']) == [0, 2, 0, 0]
    assert list(result['C']) == [0, 0, 3, 0]
    assert list(result['D']) == [0, 0, 0, 4]


@pytest.mark.parametrize('use_db,use_tt,use_sym', [
    (True, False, False), (True, False, True), (True, True, False),
    (False, True, False), (False, True, True),
    (False, False, True), (True, True, True),
])
def test_datasource_with_query_params(session, use_db, use_tt, use_sym):
    t = otp.DataSource('TMP_DB', tick_type='TT', symbol='S1')
    df = otp.run(t)

    params = {
        'SYM': 'S1',
        'DB': 'TMP_DB',
        'TICK_TYPE': 'TT',
    }

    db = otp.param('DB', dtype=otp.string[64]) if use_db else 'TMP_DB'
    tt = otp.param('TICK_TYPE', dtype=otp.string[64]) if use_tt else 'TT'
    sym = otp.param('SYM', dtype=otp.string[64]) if use_sym else 'S1'

    data = otp.DataSource(db=db, tick_type=tt, symbol=sym)
    result = otp.run(data, query_params=params)

    assert result.equals(df)


@pytest.mark.parametrize('use_db,use_tt', [
    (True, False), (False, True), (True, True),
])
def test_ticks_with_symbol_params(session, use_db, use_tt):
    sym = otp.Ticks({
        'SYMBOL_NAME': ['S1'],
        'DB': ['TMP_DB'],
        'TICK_TYPE': ['TT'],
    })
    sym_p = sym.to_symbol_param()

    db = sym_p['DB'] if use_db else 'TMP_DB'
    tt = sym_p['TICK_TYPE'] if use_tt else 'TT'

    data = otp.Ticks(X=[1], db=db, tick_type=tt)
    data['SYMBOL_NAME'] = data['_SYMBOL_NAME']
    data['TICK_TYPE'] = data['_TICK_TYPE']
    data['DBNAME'] = data['_DBNAME']
    result = otp.run(data, symbols=sym)['S1'].iloc[0].to_dict()

    assert result['SYMBOL_NAME'] == 'S1'
    assert result['TICK_TYPE'] == 'TT'
    assert result['DBNAME'] == 'TMP_DB'


def test_ticks_with_symbol_param_as_symbol(session):
    sym = otp.Ticks({
        'PARAM': ['S2'],
        'SYMBOL_NAME': ['S1'],
    })
    sym_p = sym.to_symbol_param()

    t1 = otp.Tick(A=otp.Source.meta_fields['_SYMBOL_NAME'], db='LOCAL', symbol=None)
    t2 = otp.Tick(A=otp.Source.meta_fields['_SYMBOL_NAME'], db='LOCAL', symbol=sym_p['PARAM'])
    t = otp.merge([t1, t2], identify_input_ts=True)
    result = otp.run(t, symbols=sym)

    assert list(result['S1']['A']) == ['S1', 'S2']


@pytest.mark.parametrize('use_db,use_tt,use_sym', [
    (True, False, False), (True, False, True), (True, True, False),
    (False, True, False), (False, True, True),
    (False, False, True), (True, True, True),
])
def test_ticks_with_query_params(session, use_db, use_tt, use_sym):
    params = {
        'SYM': 'S1',
        'DB': 'TMP_DB',
        'TICK_TYPE': 'TT',
    }

    db = otp.param('DB', dtype=otp.string[64]) if use_db else 'TMP_DB'
    tt = otp.param('TICK_TYPE', dtype=otp.string[64]) if use_tt else 'TT'
    sym = otp.param('SYM', dtype=otp.string[64]) if use_sym else 'S1'

    data = otp.Ticks(X=[1], db=db, tick_type=tt, symbol=sym)
    data['SYMBOL_NAME'] = data['_SYMBOL_NAME']
    data['TICK_TYPE'] = data['_TICK_TYPE']
    data['DBNAME'] = data['_DBNAME']
    result = otp.run(data, query_params=params).iloc[0].to_dict()

    assert result['SYMBOL_NAME'] == 'S1'
    assert result['TICK_TYPE'] == 'TT'
    assert result['DBNAME'] == 'TMP_DB'


@pytest.fixture(scope='module')
def polars_m():
    try:
        import polars  # type: ignore
        return polars
    except ImportError:
        return None


@pytest.mark.skipif(not os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='polars output is only supported in webapi')
class TestPolars:
    def test_simple(self, session, polars_m):
        t = otp.Tick(A=1)
        if polars_m is None:
            with pytest.raises(ValueError):
                otp.run(t, output_structure='polars')
            return

        # test one symbol
        res = otp.run(t, output_structure='polars')
        assert isinstance(res, polars_m.DataFrame)
        assert list(res['A']) == [1]

        # test many symbols
        t = otp.Tick(S=otp.meta_fields.symbol_name)
        res = otp.run(t, output_structure='polars', symbols=['A', 'B'])
        assert isinstance(res, dict)
        assert isinstance(res['A'], polars_m.DataFrame)
        assert isinstance(res['B'], polars_m.DataFrame)
        assert list(res['A']['S']) == ['A']
        assert list(res['B']['S']) == ['B']

        # test empty
        t = otp.Tick(A=1)
        t, _ = t[t['A'] == 2]
        res = otp.run(t, output_structure='polars')
        assert isinstance(res, polars_m.DataFrame)
        assert res.is_empty()
        assert res.schema == {'A': polars_m.Int64, 'Time': polars_m.Datetime(time_unit='ns', time_zone=None)}


def test_run_with_otq_and_start_end_dt_defaults(session, monkeypatch):
    monkeypatch.setattr(otp.config, 'default_start_time', otp.dt(2003, 12, 1))
    monkeypatch.setattr(otp.config, 'default_end_time', otp.dt(2003, 12, 1, 0, 0, 20))
    t = otp.Tick(X=123, S=otp.meta_fields.start, E=otp.meta_fields.end)
    query = t.to_otq()
    df = otp.run(query)
    assert list(df['X']) == [123]
    assert list(df['S']) == [otp.dt(2003, 12, 1)]
    assert list(df['E']) == [otp.dt(2003, 12, 1, 0, 0, 20)]


@pytest.mark.skipif(otq.webapi, reason='pyomd not available on webapi')
def test_otq_run_with_timeval(session):

    node = otq.TickGenerator(bucket_interval=0, fields='long A=0,nsectime S=_START_TIME,nsectime E=_END_TIME')

    res = otq.run(otq.GraphQuery(node),
                  start=datetime.datetime(2003, 12, 1, 1),
                  end=datetime.datetime(2003, 12, 4),
                  timezone='EST5EDT',
                  symbols='LOCAL::')
    res = res['LOCAL::']
    assert list(res['S']) == [pd.Timestamp(2003, 12, 1, 1)]
    assert list(res['E']) == [pd.Timestamp(2003, 12, 4)]

    res = otq.run(otq.GraphQuery(node),
                  start=pyomd.TimeParser('%Y-%m-%d %H:%M:%S.%J', 'EST5EDT').parse_time('2003-12-01 01:00:00.000000000'),
                  end=datetime.datetime(2003, 12, 4),
                  timezone='EST5EDT',
                  symbols='LOCAL::')
    res = res['LOCAL::']
    if otp.compatibility.is_correct_timezone_used_in_otq_run():
        assert list(res['S']) == [pd.Timestamp(2003, 12, 1, 1)]
    else:
        assert list(res['S']) == [pd.Timestamp(2003, 12, 1, 6)]
    assert list(res['E']) == [pd.Timestamp(2003, 12, 4)]
