import os
import sys
import pytest
import numpy as np
import pandas as pd
from datetime import datetime
import onetick.py as otp
from onetick.py.otq import otq
DIR = os.path.dirname(os.path.abspath(__file__))


def test_simple(session):
    data = otp.Ticks({"A": "A"})
    db = otp.DB("TEST_SIMPLE")
    db.add(data, symbol="SN")
    session.use(db)

    syms = otp.Symbols(db, keep_db=True)
    assert list(otp.run(syms)["SYMBOL_NAME"]) == ["TEST_SIMPLE::SN"]


def test_tick_type(session):
    data = otp.Ticks({"A": "A"})
    db = otp.DB("TEST_TT")
    db.add(data, symbol="SN", tick_type="TT1")
    db.add(data, symbol="SN", tick_type="TT2")
    session.use(db)

    syms = otp.Symbols(db, show_tick_type=True, keep_db=True)
    assert list(otp.run(syms)["SYMBOL_NAME"]) == ["TEST_TT::SN", "TEST_TT::SN"]
    assert list(otp.run(syms)["TICK_TYPE"]) == ["TT1", "TT2"]


def test_after_actions(session):
    data = otp.Ticks({"A": "A"})
    db = otp.DB("TEST_AA")
    db.add(data, symbol="SN1", tick_type="TT1")
    db.add(data, symbol="SN2", tick_type="TT2")
    session.use(db)

    syms = otp.Symbols(db, show_tick_type=True, keep_db=True)
    syms1, syms2 = syms.split(syms["TICK_TYPE"], cases=["TT1", "TT2"])
    assert list(otp.run(syms1)["SYMBOL_NAME"]) == ["TEST_AA::SN1"]
    assert list(otp.run(syms2)["SYMBOL_NAME"]) == ["TEST_AA::SN2"]


def test_pattern(session):
    data = otp.Ticks({"A": "A"})
    db = otp.DB("TEST_PAT")
    db.add(data, symbol="SN1", tick_type="TT1")
    db.add(data, symbol="SN2", tick_type="TT2")
    session.use(db)

    syms = otp.Symbols(db, pattern="%2", keep_db=True)
    assert list(otp.run(syms)["SYMBOL_NAME"]) == ["TEST_PAT::SN2"]


def test_db_property_local_db(session):
    data = otp.Ticks({"A": ["A"]})
    db = otp.DB("TEST_DB_PROPERTY_LOCAL_DB")
    assert not db.symbols
    db.add(data, symbol="SN1", tick_type="TT1")
    assert db.symbols == ["SN1"]
    db.add(data, symbol="SN2", tick_type="TT2")
    assert db.symbols == ["SN1", "SN2"]
    session.use(db)
    assert db.symbols == ["SN1", "SN2"]


def test_db_property_local_db_pre_saved(session):
    data = otp.Ticks({"A": ["A"]})
    db = otp.DB("TEST_DB_PROPERTY_LOCAL_DB_PRE_SAVED", data, symbol="SN")
    assert db.symbols == ["SN"]
    db.add(data, symbol="SN1", tick_type="TT1")
    assert db.symbols == ["SN", "SN1"]
    db.add(data, symbol="SN2", tick_type="TT2")
    assert db.symbols == ["SN", "SN1", "SN2"]
    session.use(db)
    assert db.symbols == ["SN", "SN1", "SN2"]


def test_db_property_local_db_pre_saved_empty(session):
    data = otp.Ticks({"A": ["A"]})
    db = otp.DB("TEST_DB_PROPERTY_LOCAL_DB_PRE_SAVED_EMPTY", data, symbol="SN")
    assert db.symbols == ["SN"]

    empty, _ = data.split(data["A"], cases=["XXX"], default=True)
    db.add(empty, symbol="SN1", tick_type="TT1")
    assert db.symbols == ["SN"]
    session.use(db)
    assert db.symbols == ["SN"]


def test_empty(session):
    db = otp.DB("TEST_NOTHING")
    assert not db.symbols
    session.use(db)
    assert not db.symbols


def test_symbols_for_databases(session):
    db1 = otp.DB('DB1')
    db2 = otp.DB('DB2')
    session.use(db1, db2)
    db1.add(otp.Ticks(X=[1]), symbol='A')
    db2.add(otp.Ticks(X=[2]), symbol='B')

    data = otp.merge([otp.Symbols()], symbols=['DB1::', 'DB2::'])
    res = otp.run(data)
    assert all(res['SYMBOL_NAME'] == ['A', 'B'])


def test_symbol_with_colon(session):
    data = otp.Ticks({"A": "A"})
    db = otp.DB("TEST_SYMBOL_WITH_COLON")
    db.add(data, symbol="SN1:SN2 SN3")
    session.use(db)

    syms = otp.Symbols(db, keep_db=False)
    assert list(otp.run(syms)["SYMBOL_NAME"]) == ["SN1:SN2 SN3"]


class TestSymbolNameAndParams:
    def test_name(self, session):
        data = otp.Ticks(X=[1, 2], symbol="AAA")
        data["S"] = 2 * data.Symbol.name.str.len()
        df = otp.run(data)
        assert all(df["S"] == [6, 6])

    def test_param(self, session):
        symbols = otp.Ticks(SYMBOL_NAME=["A", "B"], param=[1, 2])

        data = otp.Ticks(X=[1, 2])
        data["S"] = 2 * data.Symbol.name.str.len() + data.Symbol['param', int]
        dfs = otp.run(data, symbols=symbols)
        assert len(dfs) == 2
        assert all(dfs["A"]["S"] == [3, 3])
        assert all(dfs["B"]["S"] == [4, 4])

    def test_state_var(self, session):
        symbols = otp.Ticks(SYMBOL_NAME=["A"], param=[3])

        data = otp.Ticks(X=[1, 2])
        data.state_vars["S"] = data.Symbol['param', int] + 1
        data["S"] = data.state_vars["S"]
        dfs = otp.run(data, symbols=symbols)
        print(dfs)
        assert len(dfs) == 1
        assert all(dfs["A"]["S"] == [4, 4])


@pytest.fixture(scope='module')
def session_db(session):
    db = otp.DB('DB')
    db.add(otp.Ticks(X=[1, 2]), symbol='AA', tick_type='TRD')
    db.add(otp.Ticks(X=[1, 2]), symbol='BB', tick_type='TRD')
    session.use(db)
    return session


class TestSymbolParamWithDefaults:
    @pytest.mark.parametrize('val,dtype', [
        ('S', str),
        (1, int),
        (1.0, float),
    ])
    def test_param_exists(self, session_db, val, dtype):
        symbol = otq.Symbol('DB::AA', params={'PARAM': val})
        data = otp.DataSource(tick_type='TRD')
        data['P'] = data.Symbol.get(name='PARAM', dtype=dtype, default=val)
        df = otp.run(data, symbols=symbol)
        assert df['P'][0] == val

    @pytest.mark.parametrize('val,dtype', [
        (otp.dt(2022, 1, 1, microsecond=456000), otp.msectime),
        (otp.dt(2022, 1, 1, nanosecond=456), otp.nsectime),
    ])
    def test_param_datetime(self, session_db, val, dtype):
        val = val.timestamp() * 1000  # msec
        symbol = otq.Symbol('DB::AA', params={'PARAM': val})
        data = otp.DataSource(tick_type='TRD')
        data['P'] = data.Symbol.get(name='PARAM', dtype=dtype, default='0')
        df = otp.run(data, symbols=symbol)
        assert otp.dt(df['P'][0], tz=otp.config.tz).timestamp() * 1000 == val

    @pytest.mark.parametrize('dtype,expected', [
        (str, ''),
        (int, 0),
        (float, np.nan),
        (otp.msectime, pd.Timestamp(0)),
        (otp.nsectime, pd.Timestamp(0)),
    ])
    def test_param_dtype_default(self, session_db, dtype, expected):
        symbol = otq.Symbol('DB::AA')
        data = otp.DataSource(tick_type='TRD')
        data['P'] = data.Symbol.get(name='NOT_EXISTS', dtype=dtype)
        df = otp.run(data, symbols=symbol, timezone='GMT')
        if expected is np.nan:
            assert np.isnan(df['P'][0])
        else:
            assert df['P'][0] == expected

    @pytest.mark.parametrize('val,dtype', [
        ('1.0', float),
        ('1', int),
        ('1', float),
        ('1.0', int),
    ])
    def test_value_and_dtype_mixture(self, session_db, val, dtype):
        symbol = otq.Symbol('DB::AA', params={'PARAM': val})
        data = otp.DataSource(tick_type='TRD')
        data['P'] = data.Symbol.get(name='PARAM', dtype=dtype, default=val)
        df = otp.run(data, symbols=symbol)
        assert df['P'][0] == float(val)

    def test_params_in_merge(self, session):
        # make ticks and symbols
        db = otp.DB(name='SOME_DB')
        session.use(db)
        db.add(otp.Ticks(X=[1, 2, 3]), symbol='S1', tick_type='TT')
        db.add(otp.Ticks(X=[-3, -2, -1]), symbol='S2', tick_type='TT')

        # query with merge
        symbols = otp.Ticks(SYMBOL_NAME=['S1', 'S2'], PARAM=['PARAM1', 'PARAM2'])
        ticks = otp.DataSource('SOME_DB', tick_type='TT')
        ticks['SYMBOL_PARAM'] = ticks.Symbol.get(name='PARAM', dtype=str, default='default')
        ticks['PARAM_DEFAULT'] = ticks.Symbol.get(name='NOT_EXISTS', dtype=str, default='default')
        ticks = otp.merge([ticks], symbols=symbols, identify_input_ts=True)
        df = otp.run(ticks)
        assert all(df['SYMBOL_PARAM'] == ['PARAM1', 'PARAM2'] * 3)
        assert all(df['PARAM_DEFAULT'] == ['default'] * 6)

    @pytest.mark.skip('Showcase of _SYMBOL_PARAM.* are always strings')
    def test_params_are_always_strings(self, session_db):
        symbol = otq.Symbol('DB::AA', params={'PARAM': 1})
        data = otp.DataSource(tick_type='TRD')
        data['P'] = otp.raw('_SYMBOL_PARAM.PARAM', dtype=int)
        with pytest.raises(Exception, match='data type of its expression don\'t match'):
            otp.run(data, symbols=symbol)

    def test_params_default_123(self, session_db):
        symbol = otq.Symbol('DB::AA', params={'PARAM': 1})
        symbol2 = otq.Symbol('DB::BB')
        data = otp.DataSource(tick_type='TRD')
        data['P'] = data.Symbol.get(name='PARAM', dtype=int, default=10)
        data = otp.merge([data], symbols=[symbol, symbol2], identify_input_ts=True)
        df = otp.run(data)
        print()
        print(df)

    @pytest.mark.skip('Performance test for CASE+UNDEFINED')
    def test_performance(self, session_db):
        num = 10000
        symbols = [otq.Symbol('DB::S' + str(i), params={'PARAM': 'PARAM' + str(i)}) for i in range(num)]
        start_time = datetime.now()

        data = otp.DataSource(tick_type='TRD')
        data['P'] = otp.raw('CASE(UNDEFINED("_SYMBOL_PARAM.PARAM"), false, _SYMBOL_PARAM.PARAM, "default")',
                            dtype=str)
        df = otp.run(data, symbols=symbols)
        assert len(df) == num
        print(f'Elapsed time for CASE with {num} symbols: {datetime.now() - start_time}')

        start_time = datetime.now()
        data = otp.DataSource(tick_type='TRD')
        data['P'] = otp.raw('_SYMBOL_PARAM.PARAM', dtype=str)
        df = otp.run(data, symbols=symbols)
        assert len(df) == num
        print(f'Elapsed time without CASE with {num} symbols: {datetime.now() - start_time}')
