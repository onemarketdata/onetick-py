import itertools
import zoneinfo

from onetick.py.otq import otq
import pytest

import onetick.py as otp

GMT = zoneinfo.ZoneInfo("GMT")
start_ts = otp.config['default_start_time'].replace(tzinfo=GMT).timestamp() * 1000


@pytest.fixture(scope="function", autouse=True)
def timezone(monkeypatch):
    monkeypatch.setenv("TZ", "GMT")


@pytest.fixture(scope="function")
def track_column_creation(monkeypatch):
    class Counter:
        count = 0

    def track_calls(f, *args, **kwargs):
        Counter.count += 1
        f(*args, **kwargs)

    old_add_field_init = otq.AddField.__init__
    monkeypatch.setattr(
        "onetick.py.core.source.otq.AddField.__init__",
        lambda *args, **kwargs: track_calls(old_add_field_init, *args, **kwargs),
    )

    old_add_fields_init = otq.AddField.__init__
    monkeypatch.setattr(
        "onetick.py.core.source.otq.AddFields.__init__",
        lambda *args, **kwargs: track_calls(old_add_fields_init, *args, **kwargs),
    )

    yield Counter


def test_time_1(session):
    # Constant assignment shouldn't change the tick order
    data = otp.Ticks({"x": [0, 1, 2, 3, 4, 5, 6]})
    old_columns = data.columns(skip_meta_fields=True)

    data.Time = 0

    assert data.columns(skip_meta_fields=True) == old_columns
    df = otp.run(data)
    assert len(df) == 7
    for i in range(7):
        assert df.x[i] == i


def test_time_2(session):
    # Assignment to another column
    data = otp.Ticks({"x": [1, 0], "new_offset": [10, 1], "offset": [0, 1]})
    data.new_time = data.Time + data.new_offset
    old_columns = data.columns(skip_meta_fields=True)

    data.Time = data.new_time

    assert data.columns(skip_meta_fields=True) == old_columns
    df = otp.run(data)
    assert len(df) == 2
    assert df.x[0] == 0 and df.x[1] == 1


def test_time_3(session):
    # Assignment to expression (_Arithmetical)
    data = otp.Ticks({"x": [1, 0], "new_offset": [10, 1], "offset": [0, 1]})
    old_columns = data.columns(skip_meta_fields=True)

    data.Time = data.Time + data.new_offset

    assert data.columns(skip_meta_fields=True) == old_columns
    df = otp.run(data)
    assert len(df) == 2
    assert df.x[0] == 0 and df.x[1] == 1


def test_time_4(session):
    # Assignment to expression (_Arithmetical)
    data = otp.Ticks({"x": [1, 0], "new_offset": [10, 1], "offset": [0, 1]})
    old_columns = data.columns(skip_meta_fields=True)

    data.Time += data.new_offset

    assert data.columns(skip_meta_fields=True) == old_columns
    df = otp.run(data)
    assert len(df) == 2
    assert df.x[0] == 0 and df.x[1] == 1


def test_time_5(session):
    # Assignment to a nontrivial expression (_Arithmetical)
    data = otp.Ticks({"a": [2, 2], "b": [2, 3], "c": [1, 4], })
    old_columns = set(data.columns(skip_meta_fields=True))
    old_columns.add("Time")

    data.Time = start_ts + data.a * data.b - data.c

    res = otp.run(data, timezone="GMT")

    assert set(res.columns) == old_columns
    assert len(res) == 2
    assert res.a[0] == 2 and res.b[0] == 3 and res.c[0] == 4
    assert res.a[1] == 2 and res.b[1] == 2 and res.c[1] == 1


def test_time_6(session):
    # Assignment to a lambda (nameless _Column)
    data = otp.Ticks({"id": [0, 1, 2, 3], "first": [1, 1, 2, 2], "second": [4, 4, 3, 3], "which": [1, 2, 1, 2]})
    data.columns(skip_meta_fields=True)
    # Expecting 0, 2, 3, 1

    data.Time = data.apply(lambda row: (start_ts + row.first) if row.which == 1 else (start_ts + row.second))

    res = otp.run(data, timezone="GMT")
    assert len(res) == 4
    assert res.id[0] == 0 and res.id[1] == 2 and res.id[2] == 3 and res.id[3] == 1


def test_time_7(session):
    # Assignment to _Comparable
    data = otp.Ticks({"x": [0, 1]})

    with pytest.raises(Exception):
        data.Time = data.x > 0
    # with


def test_tick_desc_1(session, track_column_creation):
    # Constant assignment shouldn't hit tick descriptor
    data = otp.Ticks({"x": [0], })

    data.Time = 0

    assert track_column_creation.count == 0


def test_tick_desc_2(session, track_column_creation):
    # Assignment to an existing column shouldn't hit tick descriptor
    data = otp.Ticks({"x": [0], })

    data.Time = data.x

    assert track_column_creation.count == 0


def test_time_to_expr(session):
    data = otp.Ticks(X=[1, 2, 3])

    data['S'] = '2003-12-02'

    data['Time'] = data['S'].str.to_datetime('%Y-%m-%d', 'GMT')

    df = otp.run(data, timezone='GMT')

    assert len(df) == 3
    assert all(df['X'] == [1, 2, 3])
    assert all(df['Time'] == [otp.datetime(2003, 12, 2).ts] * 3)


class TestAssignmentWithNoTickFields:
    # We call OrderBy before assignment, because ticks should be ordered
    # But we shouldn't do it in case constant field.

    @pytest.mark.parametrize("field, expected", [("_START_TIME", otp.config.default_start_time),
                                                 ("_END_TIME", otp.config.default_end_time)])
    def test_start_end_time(self, session, field, expected):
        # see PY-253
        data = otp.Ticks(dict(X=[1, 2, 3]))
        data["TIMESTAMP"] = data[field]
        df = otp.run(data)
        assert all(df["Time"] == [expected] * 3)

    def test_start_time_with_modify_query_times(self, session):
        add_milli = 1000
        data = otp.Ticks(dict(X=[1, 2, 3]))
        data["TIMESTAMP"] = data["_START_TIME"]
        data.sink(otq.ModifyQueryTimes(f"_START_TIME + {add_milli}", "_END_TIME", "TIMESTAMP"))
        df = otp.run(data)
        assert all(df["Time"] == [otp.config['default_start_time'] + otp.Milli(add_milli)] * 3)

    def test_start_time_with_modify_query_times_wrong(self, session):
        add_milli = 1000
        data = otp.Ticks(dict(X=[1, 2, 3]))
        data["TIMESTAMP"] = data["_START_TIME"]
        data.sink(otq.ModifyQueryTimes(f"_START_TIME - {add_milli}", "_END_TIME", "TIMESTAMP"))
        with pytest.raises(Exception, match=r"Tick with 20031201045959\.000 timestamp is falling out of initial "
                                            "start/end time range"):
            otp.run(data)

    def test_start_time_with_modify_query_times_and_max(self, session):
        add_milli = 1000
        data = otp.Ticks(dict(X=[1, 2, 3]))
        data["TIMESTAMP"] = data["_START_TIME"]
        data.sink(otq.ModifyQueryTimes(f"_START_TIME - {add_milli}", "_END_TIME", "MAX(TIMESTAMP, _START_TIME)"))
        df = otp.run(data)
        assert all(df["Time"] == [otp.config['default_start_time']] * 3)

    def test_const_state_var(self, session):
        data = otp.Ticks(dict(X=[1, 2, 3]))
        expected = otp.config['default_start_time'] + otp.Hour(8)
        data.state_vars["S"] = otp.datetime(expected)
        data["TIMESTAMP"] = data.state_vars["S"]
        df = otp.run(data, timezone="GMT")
        assert all(df["Time"] == [expected] * 3)

    def test_expr_state_var(self, session):
        xs = [1, 2, 3]
        data = otp.Ticks(dict(X=xs))
        diffs = itertools.accumulate(xs)
        expected = otp.config['default_start_time'] + otp.Hour(8)
        data.state_vars["S"] = otp.datetime(expected)
        data.state_vars["S"] += data["X"]
        data["TIMESTAMP"] = data.state_vars["S"]
        df = otp.run(data, timezone="GMT")
        assert all(df["Time"] == [expected + otp.Milli(d) for d in diffs])
