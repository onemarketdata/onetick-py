import os

import pytest
from onetick import py as otp
from onetick.py.compatibility import is_native_plus_zstd_supported

from onetick.py.otq import otq
import datetime

from multiprocessing import Process
from pathlib import Path


def get_bare_symbols(db_name):
    res = otp.run(
        otq.FindDbSymbols(pattern="%").tick_type("ANY"),
        symbols=db_name + "::",
        start=otp.config['default_start_time'],
        end=otp.config['default_end_time'],
        output_structure="map"
    )
    return [symbol.split("::")[1] for symbol in res[db_name + "::"]["SYMBOL_NAME"]]


class TestDBFromInspection:
    def test_db_tick_types_last_date_without_exception(self, f_session):
        db = otp.DB("S_ORDERS_FIX")
        f_session.use(db)
        tick_types = otp.databases()["S_ORDERS_FIX"].tick_types()
        assert tick_types == []


def test_add_to_db_without_session_1():
    name = "TEST_DB"
    test_db = otp.DB(name)
    raw_data = {"A": [1, 2, 3]}
    data = otp.Ticks(raw_data)
    test_db.add(data)
    assert otp.Session._instance is None
    s = otp.Session()
    try:
        s.use(test_db)
        res_symbols = get_bare_symbols(test_db.name)
        expected_symbol = otp.config.default_symbol
        assert len(res_symbols) == 1 and set(res_symbols) == {expected_symbol}
        res = otp.run(
            otq.Passthrough().tick_type(otp.db.db._tick_type_detector(None, data)),
            symbols=test_db.name + "::" + expected_symbol,
            start=otp.config['default_start_time'],
            end=otp.config['default_end_time'],
            output_structure="map"
        )
        assert "A" in res[name + "::" + expected_symbol]
        assert list(res[name + "::" + expected_symbol]["A"]) == raw_data["A"]
    finally:
        s.close()


def test_add_to_db_with_session_1(f_session):
    """Checks correct adding to locators and correct adding to db when session defined"""
    name = "TEST_DB"
    test_db = otp.DB(name)
    raw_data = {"A": [1, 2, 3]}
    data = otp.Ticks(raw_data)
    dbs_loc_1 = f_session.locator.databases
    dbs_acl_1 = f_session.acl.databases
    test_db.add(data)
    dbs_loc_2 = f_session.locator.databases
    dbs_acl_2 = f_session.acl.databases
    assert len(dbs_loc_1) == len(dbs_loc_2) and set(dbs_loc_1) == set(dbs_loc_2)
    assert len(dbs_acl_1) == len(dbs_acl_2) and set(dbs_acl_1) == set(dbs_acl_2)
    f_session.use(test_db)
    dbs_loc_3 = f_session.locator.databases
    dbs_acl_3 = f_session.acl.databases
    assert len(dbs_loc_2) + 1 == len(dbs_loc_3) and set(dbs_loc_2 + [test_db.name]) == set(dbs_loc_3)
    assert len(dbs_acl_2) + 1 == len(dbs_acl_3) and set(dbs_acl_2 + [test_db.name]) == set(dbs_acl_3)

    res_symbols = get_bare_symbols(test_db.name)
    expexted_symbol = otp.config['default_symbol']
    assert len(res_symbols) == 1
    assert set(res_symbols) == {expexted_symbol}
    res = otp.run(
        otq.Passthrough().tick_type(otp.db.db._tick_type_detector(None, data)),
        symbols=test_db.name + "::" + expexted_symbol,
        start=otp.config['default_start_time'],
        end=otp.config['default_end_time'],
        output_structure="map"
    )

    assert "A" in res[name + "::" + expexted_symbol]
    assert list(res[name + "::" + expexted_symbol]["A"]) == raw_data["A"]


def test_add_to_db_without_session_2():
    """  different symbols """
    name = "TEST_DB"
    test_db = otp.DB(name)
    raw_data = {
        "SYM_1": {"A": [1, 2, 3]},
        "SYM_2": {"B": [4, 5, 6]},
        "SYM_3": {"A": [7, 8, 9], "C": ["a", "b", "c"]},
        "SYM_4": {"A": [1, 2, 3]},
    }
    for symbol, data in raw_data.items():
        test_db.add(otp.Ticks(data), symbol=symbol)
    assert otp.Session._instance is None
    s = otp.Session()
    try:
        s.use(test_db)
        res_symbols = get_bare_symbols(test_db.name)
        assert len(res_symbols) == len(raw_data.keys()) and set(res_symbols) == set(raw_data.keys())
        for symbol, data in raw_data.items():
            tt = otp.db.db._tick_type_detector(None, otp.Ticks(data, symbol=symbol))
            res = otp.run(
                otq.Passthrough().tick_type(tt),
                symbols=test_db.name + "::" + symbol,
                start=otp.config['default_start_time'],
                end=otp.config['default_end_time'],
                output_structure="map"
            )
            for field, values in data.items():
                assert field in res[test_db.name + "::" + symbol]
                assert list(res[test_db.name + "::" + symbol][field]) == values
    finally:
        s.close()


def test_add_to_db_without_session_3():
    """Different tick types"""
    test_db = otp.DB("TEST_DB")
    symbol = "AAPL"
    raw_data = {
        "TT_1": {"A": [1, 2, 3]},
        "TT_2": {"B": [4, 5, 6]},
        "TT_3": {"A": [7, 8, 9], "C": ["a", "b", "c"]},
        "TT_4": {"A": [1, 2, 3]},
    }
    for tt, data in raw_data.items():
        test_db.add(otp.Ticks(data), symbol=symbol, tick_type=tt)
    assert otp.Session._instance is None
    s = otp.Session()
    try:
        s.use(test_db)
        res_symbols = get_bare_symbols(test_db.name)
        assert len(res_symbols) == 1 and set(res_symbols) == {symbol}
        for tt, data in raw_data.items():
            res = otp.run(
                otq.Passthrough().tick_type(tt),
                symbols=test_db.name + "::" + symbol,
                start=otp.config['default_start_time'],
                end=otp.config['default_end_time'],
                output_structure="map"
            )
            for field, values in data.items():
                if field == "offset":  # TODO remove when Ticks wont change _source
                    continue
                assert field in res[test_db.name + "::" + symbol]
                assert list(res[test_db.name + "::" + symbol][field]) == values
    finally:
        s.close()


def test_add_to_db_with_session_2(f_session):
    """Different symbols"""
    name = "TEST_DB"
    test_db = otp.DB(name)
    raw_data = {
        "SYM_1": {"A": [1, 2, 3]},
        "SYM_2": {"B": [4, 5, 6]},
        "SYM_3": {"A": [7, 8, 9], "C": ["a", "b", "c"]},
        "SYM_4": {"A": [1, 2, 3]},
    }
    for symbol, data in raw_data.items():
        test_db.add(otp.Ticks(data), symbol=symbol)
    f_session.use(test_db)
    res_symbols = get_bare_symbols(test_db.name)
    assert len(res_symbols) == len(raw_data.keys()) and set(res_symbols) == set(raw_data.keys())
    for symbol, data in raw_data.items():
        res = otp.run(
            otq.Passthrough().tick_type(otp.db.db._tick_type_detector(None, otp.Ticks(data, symbol=symbol))),
            symbols=test_db.name + "::" + symbol,
            start=otp.config['default_start_time'],
            end=otp.config['default_end_time'],
            output_structure="map"
        )
        for field, values in data.items():
            if field == "offset":  # TODO remove when Ticks wont change _source
                continue
            assert field in res[test_db.name + "::" + symbol]
            assert list(res[test_db.name + "::" + symbol][field]) == values


def test_add_to_db_with_session_3(f_session):
    """Different tick types"""
    test_db = otp.DB("TEST_DB")
    symbol = "AAPL"
    raw_data = {
        "TT_1": {"A": [1, 2, 3]},
        "TT_2": {"B": [4, 5, 6]},
        "TT_3": {"A": [7, 8, 9], "C": ["a", "b", "c"]},
        "TT_4": {"A": [1, 2, 3]},
    }
    for tt, data in raw_data.items():
        test_db.add(otp.Ticks(data), symbol=symbol, tick_type=tt)
    f_session.use(test_db)
    res_symbols = get_bare_symbols(test_db.name)
    assert len(res_symbols) == 1 and set(res_symbols) == {symbol}
    for tt, data in raw_data.items():
        res = otp.run(
            otq.Passthrough().tick_type(tt),
            symbols=test_db.name + "::" + symbol,
            start=otp.config['default_start_time'],
            end=otp.config['default_end_time'],
            output_structure="map"
        )
        for field, values in data.items():
            if field == "offset":  # TODO remove when Ticks wont change _source
                continue
            assert field in res[test_db.name + "::" + symbol]
            assert list(res[test_db.name + "::" + symbol][field]) == values


def test_from_ticks(f_session):
    data = otp.Ticks({"X": [1, 2, 3, 4], "Y": [4, 5, 6, 7]})
    db = otp.DB("S_ORDERS_FIX", data)
    db.add(data)
    f_session.use(db)
    res = otp.DataSource(db, schema={'X': int, 'Y': int})
    assert len(otp.run(res)) == 4


def test_different_symbols(f_session):
    sym_a_data = otp.Ticks({"X": [1, 2, 3, 4], "Y": [4, 5, 6, 7]})
    sym_b_data = otp.Ticks({"Z": [8, -4], "W": [7, 19]})

    db = otp.DB("S_ORDERS_FIX")
    db.add(sym_a_data, symbol="A")
    db.add(sym_b_data, symbol="B")
    f_session.use(db)
    res = otp.DataSource(db, symbol="B") + otp.DataSource(db, symbol="A")
    assert len(otp.run(res)) == 6


def test_different_tick_types(f_session):
    tt_a_data = otp.Ticks({"X": [1, 2, 3, 4], "Y": [4, 5, 6, 7]})
    tt_b_data = otp.Ticks({"Z": [8, -4], "W": [7, 19]})

    db = otp.DB("S_ORDERS_FIX")
    db.add(tt_a_data, symbol="sym", tick_type="A")
    db.add(tt_b_data, symbol="sym", tick_type="B")
    f_session.use(db)

    res = otp.DataSource(db, symbol="sym", tick_type="A") + otp.DataSource(db, symbol="sym", tick_type="B")

    assert len(otp.run(res)) == 6


def test_different_dates(f_session):
    day1 = otp.Ticks({"X": [1, 2, 3, 4], "Y": [4, 5, 6, 7]})
    day2 = otp.Ticks({"Z": [8, -4], "W": [7, 19]})

    db = otp.DB("S_ORDERS_FIX")
    db.add(day1, date=datetime.datetime(2017, 5, 5))
    db.add(day2, date=datetime.datetime(2017, 5, 6))
    f_session.use(db)

    res1 = otp.DataSource(
        db, start=datetime.datetime(2017, 5, 5, 0, 0, 0), end=datetime.datetime(2017, 5, 5, 23, 0, 0)
    )

    df = otp.run(res1)
    assert len(df) == 4
    assert df["X"].iloc[0] == 1

    res2 = otp.DataSource(
        db, start=datetime.datetime(2017, 5, 6, 0, 0, 0), end=datetime.datetime(2017, 5, 6, 23, 0, 0)
    )

    df = otp.run(res2)
    assert len(df) == 2
    assert df["Z"].iloc[0] == 8

    res3 = otp.DataSource(
        db, start=datetime.datetime(2017, 5, 5, 0, 0, 0), end=datetime.datetime(2017, 5, 7, 0, 0, 0)
    )

    assert len(otp.run(res3)) == 6


def test_two_dbs(f_session):
    data1 = {"A": [1, 2, 3]}
    data2 = {"B": [4, 5, 6, 7]}
    symbol, tt = "AAPL", "TT"
    db1 = otp.DB("TEST_DB_1")
    db2 = otp.DB("TEST_DB_2")
    db1.add(otp.Ticks(data1), symbol=symbol, tick_type=tt)
    db2.add(otp.Ticks(data2), symbol=symbol, tick_type=tt)
    f_session.use(db1)
    f_session.use(db2)

    res1 = otp.run(otp.DataSource(db1, symbol=symbol, tick_type=tt))
    res2 = otp.run(otp.DataSource(db2, symbol=symbol, tick_type=tt))

    assert "A" in res1
    assert "B" not in res1
    assert len(res1) == 3

    assert "A" not in res2
    assert "B" in res2
    assert len(res2) == 4


def test_add_to_db_on_creation_1():
    data1 = {"A": [1, 2, 3]}
    ticks1 = otp.Ticks(data1)
    symbol1 = "AAPL1"
    data2 = {"B": [1, 2, 3]}
    ticks2 = otp.Ticks(data2)
    symbol2 = "AAPL2"
    db_name = "TEMP_DB"
    db_ = otp.DB(db_name, src=ticks1, symbol=symbol1)
    db_.add(ticks2, symbol=symbol2)
    with otp.Session() as session:
        session.use(db_)
        res1 = otp.run(otp.DataSource(db_, symbol=symbol1))
        res2 = otp.run(otp.DataSource(db_, symbol=symbol2))
        assert all(res1["A"] == [1, 2, 3])
        assert all(res2["B"] == [1, 2, 3])


def test_add_to_db_on_creation_2(f_session):
    data1 = {"A": [1, 2, 3]}
    ticks1 = otp.Ticks(data1)
    symbol1 = "AAPL1"
    data2 = {"B": [1, 2, 3]}
    ticks2 = otp.Ticks(data2)
    symbol2 = "AAPL2"
    db_name = "TEMP_DB"
    db_ = otp.DB(db_name, src=ticks1, symbol=symbol1)
    db_.add(ticks2, symbol=symbol2)

    f_session.use(db_)
    res1 = otp.run(otp.DataSource(db_, symbol=symbol1))
    res2 = otp.run(otp.DataSource(db_, symbol=symbol2))
    assert all(res1["A"] == [1, 2, 3])
    assert all(res2["B"] == [1, 2, 3])


def test_create_db_with_custom_location():
    location = otp.utils.TmpDir('custom_location')
    db = otp.DB("TEST_DB", db_locations=[dict(location=location)])

    assert db._path is None
    assert "location" in db.locations[0]
    assert "access_method" in db.locations[0]
    assert "start_time" in db.locations[0]
    assert "end_time" in db.locations[0]

    assert not os.listdir(location)
    db.add(otp.Tick(A=1))
    assert sorted(os.listdir(location)) == ['20031201', 'corrections', 'locks']


def test_create_socket_db_without_location(f_session):
    with pytest.raises(ValueError):
        otp.DB("SOCKET_TEST_DB", db_locations=[dict(access_method='socket')])


def test_db_with_destroy(f_session):
    db = otp.DB("TEST_DB", destroy_access=True)
    day1 = otp.Ticks({"A": [1, 2, 3, 4]})
    db.add(day1, date=datetime.datetime(2019, 5, 3, 11, 0, 0))
    f_session.use(db)
    start = datetime.datetime(2019, 5, 3, 10, 0, 0)
    end = datetime.datetime(2019, 5, 3, 12, 0, 0)
    otp.run(otq.DbDestroy("ENTIRE_DB").tick_type("ANY"), symbols="TEST_DB::", start=start, end=end)
    with pytest.raises(Exception, match="No ticks found in"):
        otp.run(otp.DataSource(db, schema={'A': int}, start=start, end=end, schema_policy="fail"))


def _stub_writer(data, db, **kwargs):
    src = otp.Ticks(data)
    src.sink(otq.Pause(delay="2000"))
    otp.db.write_to_db(src, db, otp.config['default_start_time'], src["SN"], "TT", **kwargs)


@pytest.mark.platform("linux")  # known magic problem on windows of using pytest and multiprocessing
@pytest.mark.parametrize("append_mode", [True, False])
def test_write_to_db_append_mode(f_session, append_mode):
    data1 = {"A": [1] * 3, "SN": ["A"] * 3}
    data2 = {"B": [2] * 3, "SN": ["B"] * 3}
    tt = "TT"

    if append_mode:
        db = otp.DB("TEST_DB_A")
    else:
        db = otp.DB("TEST_DB_NA")

    f_session.use(db)

    kwargs = {"append": append_mode}

    p1 = Process(target=_stub_writer, args=(data1, db), kwargs=kwargs)
    p2 = Process(target=_stub_writer, args=(data2, db), kwargs=kwargs)
    p1.start()
    p1.join()
    p2.start()
    p2.join()

    src1 = otp.DataSource(db=db, symbol=data1["SN"][0], tick_type=tt)
    src2 = otp.DataSource(db=db, symbol=data2["SN"][0], tick_type=tt)

    df1 = otp.run(src1)
    df2 = otp.run(src2)
    if append_mode:
        for k, v in data1.items():
            assert list(df1[k]) == v
        for k, v in data2.items():
            assert list(df2[k]) == v
    else:
        assert df1.empty or df2.empty


@pytest.mark.skip(reason="BDS-76: now concurrent write is possible only for remote server")
@pytest.mark.parametrize("concurrent", [True, False])
def test_write_to_db_append_mode_2(f_session, concurrent):
    data1 = {"A": [1] * 3, "SN": ["A"] * 3}
    data2 = {"B": [2] * 3, "SN": ["B"] * 3}
    tt = "TT"

    if concurrent:
        db = otp.DB("TEST_DB_C")
    else:
        db = otp.DB("TEST_DB_NC")

    f_session.use(db)

    kwargs = {"allow_concurrent_write": concurrent}

    p1 = Process(target=_stub_writer, args=(data1, db), kwargs=kwargs)
    p2 = Process(target=_stub_writer, args=(data2, db), kwargs=kwargs)
    p1.start()
    p2.start()
    p1.join()
    p2.join()

    src1 = otp.DataSource(db=db, symbol=data1["SN"][0], tick_type=tt)
    src2 = otp.DataSource(db=db, symbol=data2["SN"][0], tick_type=tt)

    df1 = otp.run(src1)
    df2 = otp.run(src2)
    if concurrent:
        for k, v in data1.items():
            assert list(df1[k]) == v
        for k, v in data2.items():
            assert list(df2[k]) == v
    else:
        assert df1.empty or df2.empty


def test_write_after_session_use():
    with otp.Session() as s:
        db = otp.DB("TEST_DB")
        s.use(db)
        db.add(otp.Ticks({"A": [1, 2, 3]}))


def test_in_locator():
    with otp.Session(override_env=True) as s:
        db = otp.DB("TEMP_DB")
        s.use(db)
        assert str(db) in s.databases
        db.symbols
        assert str(db) in s.databases


def test_empty_name():
    with otp.Session(override_env=True) as s:
        with pytest.raises(TypeError) as e:
            db = otp.DB(otp.Ticks({"X": [1, 2, 3]}))
            s.use(db)
        assert "expected" in str(e.value)


def test_nanoseconds_support(f_session):
    data1 = otp.Ticks(dict(X=[1, 2, 3]))

    data1["Time"] += otp.Nano(2)

    db = otp.DB("TEST_NANOSECONDS")
    f_session.use(db)
    db.add(data1)

    data2 = otp.DataSource(db)

    df1, df2 = otp.run(data1), otp.run(data2)
    assert df1.equals(df2)


@pytest.mark.parametrize(
    "db_locations", [
        (None),
        ([{}]),
        ([dict(timezone='EST5EDT')]),
    ]
)
def test_db_locations(db_locations):
    '''Tests that dbs with derived paths correctly created if db_locations is specified'''
    with otp.Session() as s:
        data1 = otp.Ticks(dict(X=[1]))
        db = otp.DB('LONG//PATH', db_locations=db_locations)
        s.use(db)
        db.add(data1)
        data2 = otp.DataSource(db)
        df1, df2 = otp.run(data1), otp.run(data2)
        assert df1.equals(df2)


@pytest.mark.parametrize('day_boundary_tz,tz,out_of_range_tick_action,len_result', [
    # good cases, timezones are the same
    (None, None, None, 1),
    ('GMT', 'GMT', None, 1),
    ('GMT', 'GMT', 'EXCEPTION', 1),
    ('GMT', 'GMT', 'IGNORE', 1),
    # bad cases, timezones are not the same
    (None, 'GMT', 'EXCEPTION', None),
    (None, 'GMT', 'IGNORE', 0),
    # test default behavior, should raise exception
    (None, 'GMT', None, None),
])
def test_wrong_day_boundary_tz(f_session, day_boundary_tz, tz, out_of_range_tick_action, len_result):
    kwargs = {}
    if day_boundary_tz is not None:
        kwargs['db_locations'] = [{'day_boundary_tz': day_boundary_tz}]
    t_db = otp.DB('DB', **kwargs)

    tick_type, symbol = 'TT', 'AA'
    kwargs = dict(
        symbol=symbol,
        tick_type=tick_type,
        date=otp.date(2022, 1, 1),
    )
    if tz is not None:
        kwargs['timezone'] = tz
    if out_of_range_tick_action is not None:
        kwargs['out_of_range_tick_action'] = out_of_range_tick_action

    if len_result is None:
        with pytest.raises(Exception, match=f'falls outside 20220101 in {tz} timezone'):
            t_db.add(otp.Tick(A=1), **kwargs)
        return

    t_db.add(otp.Tick(A=1), **kwargs)
    f_session.use(t_db)

    if not len_result:
        with pytest.warns(match="Can't find not empty day for the last 5 days"):
            data = otp.DataSource('DB', tick_type=tick_type, symbols=symbol)
    else:
        data = otp.DataSource('DB', tick_type=tick_type, symbols=symbol)

    kwargs = dict(
        start=otp.datetime(2022, 1, 1),
        end=otp.datetime(2022, 1, 2),
    )
    if tz is not None:
        kwargs = dict(
            start=otp.datetime(2022, 1, 1, tz=tz),
            end=otp.datetime(2022, 1, 2, tz=tz),
            timezone=tz,
        )

    df = otp.run(data, **kwargs)
    assert len(df) == len_result


def test_raw(session):
    with pytest.raises(ValueError, match="'prefix' must be specified"):
        otp.DB('A', db_raw_data=[{'id': 'PRIMARY_A'}])
    with pytest.raises(ValueError, match="'mount' must be specified"):
        otp.DB('A', db_raw_data=[{'id': 'PRIMARY_A', 'prefix': 'DATA.'}])
    with pytest.raises(ValueError, match="must be unique for raw databases"):
        otp.DB('A', db_raw_data=[
            {'id': 'PRIMARY_A', 'prefix': 'DATA.', 'locations': [{'mount': 'mount1'}]},
            {'id': 'PRIMARY_A', 'prefix': 'DATA.', 'locations': [{'mount': 'mount1'}]},
        ])
    with pytest.raises(ValueError, match="'type' must be specified"):
        otp.DB(
            'A',
            db_raw_data=[{'id': 'PRIMARY_A', 'prefix': 'DATA.', 'locations': [{'mount': 'mount1'}]}],
            db_feed={'raw_source': 'PRIMARY_A'},
        )
    db = otp.DB(
        'A',
        db_raw_data=[{'id': 'PRIMARY_A',
                      'prefix': 'DATA.',
                      'locations': [{'mount': 'mount1'}]}],
        db_feed={'type': 'rawdb', 'raw_source': 'PRIMARY_A'},
    )
    session.use(db)
    locator = Path(session.locator.path).read_text()
    assert '<RAW_DB id="PRIMARY_A" prefix="DATA." >' in locator
    assert ('<LOCATION mount="mount1" access_method="file" '
            'start_time="20021230000000" end_time="21000101000000" location=') in locator
    assert '<FEED type="rawdb" >' in locator
    assert '<OPTIONS raw_source="PRIMARY_A" format="native" />' in locator
    write_raw = otp.Tick(A=1)
    write_raw.sink(otq.WriteToRaw(database='A', location='PRIMARY_A', mount='mount1', tick_type_name='TT'))
    otp.run(write_raw)
    read_raw = otp.Source(otq.ReadFromRaw(location='PRIMARY_A', mount='mount1').tick_type('TT'))
    df = otp.run(read_raw, symbols='A::')
    assert df['A'][0] == 1


def test_db_properties_lowercase_keys():
    test_db = otp.DB('TEST', db_properties={'DAY_BOUNDARY_TZ': 'GMT', 'ABC': 'TEST'})
    assert 'day_boundary_tz' in test_db.properties
    assert 'abc' in test_db.properties and test_db.properties['abc'] == 'TEST'


@pytest.mark.skipif(not is_native_plus_zstd_supported(), reason='Older versions of OneTick not support ZSTD')
def test_db_properties():
    # moved from doctest due to compatibility issues with older versions of OneTick
    db_properties = otp.DB('X').properties
    assert db_properties == {
        'symbology': 'BZX',
        'archive_compression_type': 'NATIVE_PLUS_ZSTD',
        'tick_timestamp_type': 'NANOS',
    }
