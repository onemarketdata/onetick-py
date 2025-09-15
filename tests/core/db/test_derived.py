import os
import datetime
import pytest

import onetick.py as otp


def test_write_read(f_session, keep_generated_dir):
    db = otp.DB("DB_A")
    f_session.use(db)

    data = otp.Ticks(dict(A=[1, 2, 3], B=["a", "b", "c"]))

    derived_db = otp.DB("DB_A//DB_D", data)
    f_session.use(derived_db)

    data = otp.DataSource(db=derived_db)
    assert 'A' in data.schema and 'B' in data.schema
    assert len(data.schema) == 2
    data = otp.run(data)

    path = os.path.join(keep_generated_dir, "DB_A", "DERIVED", "DB_D")
    assert os.listdir(os.path.join(keep_generated_dir, "DB_A")) == ["DERIVED"]
    assert os.listdir(os.path.join(keep_generated_dir, "DB_A", "DERIVED")) == ["DB_D"]
    assert os.listdir(path)
    assert all(data["A"] == [1, 2, 3])
    assert all(data["B"] == ["a", "b", "c"])


def test_merge_2_derived_and_parent(f_session, keep_generated_dir):
    db = otp.DB("DB_A")
    db.add(otp.Ticks(dict(A=[1, 2, 3])))
    f_session.use(db)

    derived_db1 = otp.DB("DB_A//DB_D")
    derived_db1.add(otp.Ticks(dict(B=["a", "b", "c"])))
    f_session.use(derived_db1)

    derived_db2 = otp.DB("DB_A//DB_C")
    derived_db2.add(otp.Ticks(dict(B=["a", "b", "c"])))
    f_session.use(derived_db2)

    data = otp.DataSource(db=[db, derived_db1, derived_db2], tick_type='TRD')
    assert 'B' in data.schema
    data = otp.run(data)
    path = os.path.join(keep_generated_dir, "DB_A", "DERIVED")
    assert os.listdir(os.path.join(path, "DB_C"))
    assert os.listdir(os.path.join(path, "DB_D"))
    assert all(data["A"] == [1, 0, 0, 2, 0, 0, 3, 0, 0])
    assert all(data["B"] == ["", "a", "a", "", "b", "b", "", "c", "c"])


def test_merge_simple(f_session):
    db = otp.DB("DB_A")
    db.add(otp.Ticks(dict(A=[1, 2, 3])))
    f_session.use(db)

    derived_db1 = otp.DB("DB_A//DB_D")
    derived_db1.add(otp.Ticks(dict(B=["a", "b", "c"])))
    f_session.use(derived_db1)

    # merge this two databases
    data = otp.DataSource(db=[db, derived_db1], tick_type='TRD')
    data = otp.run(data)
    assert all(data["A"] == [1, 0, 2, 0, 3, 0])


def test_derived_definition_only(f_session, keep_generated_dir):
    data = otp.Ticks(dict(A=[1, 2, 3]))
    derived_db1 = otp.DB("DB_A//DB_D")
    f_session.use(derived_db1)
    derived_db1.add(data)

    derived_db2 = otp.DB("DB_A//DB_D//DB_C")
    derived_db2.add(data)
    f_session.use(derived_db2)

    data = otp.DataSource(db=[derived_db1, derived_db2], tick_type='TRD')
    df = otp.run(data)
    path = os.path.join(keep_generated_dir, "DB_A", "DERIVED", "DB_D")
    assert os.listdir(path)
    assert os.listdir(os.path.join(path, "DERIVED", "DB_C"))
    assert all(df["A"] == [1, 1, 2, 2, 3, 3])

    dbs = otp.databases(derived=True)
    assert 'DB_A//DB_D' in dbs
    assert 'DB_A//DB_D//DB_C' in dbs
    assert dbs['DB_A//DB_D'].tick_types() == ['TRD']
    assert dbs['DB_A//DB_D'].symbols() == [otp.config.default_symbol]
    assert dbs['DB_A//DB_D'].dates() == [datetime.date(2003, 12, 1)]
    assert dbs['DB_A//DB_D'].last_date == datetime.date(2003, 12, 1)
    assert dbs['DB_A//DB_D'].schema() == {'A': int}
    assert list(otp.derived_databases()) == ['DB_A//DB_D', 'DB_A//DB_D//DB_C']
    assert list(
        otp.derived_databases(selection_criteria='direct_children_of_current_db', db='DB_A')
    ) == ['DB_A//DB_D']


@pytest.mark.skipif(not otp.compatibility.is_supported_list_empty_derived_databases(),
                    reason='Raised a segfault on older builds, cannot test')
def test_derived_databases_empty(f_session):
    assert not otp.derived_databases()


def test_add_data_before_use():
    """
    See tasks PY-134, PY-388, BDS-334.
    Was fixed in update1_20231108120000.
    0032118: OneTick processes that refresh their locator may crash
             if they make use databases derived from the dbs in that locator
    """
    with otp.Session() as session:
        derived_db1 = otp.DB("DB_A//DB_D")
        if not otp.compatibility.is_supported_reload_locator_with_derived_db():
            with pytest.raises(Exception, match='use the .use method before adding'):
                derived_db1.add(otp.Ticks(A=[3, 2, 1]))
            return
        derived_db1.add(otp.Ticks(A=[3, 2, 1]), tick_type='TT', symbol='S')
        session.use(derived_db1)
        df = otp.run(otp.DataSource("DB_A//DB_D", tick_type='TT', symbol='S'))
        assert list(df['A']) == [3, 2, 1]


def test_3_level_merge_with_upper(f_session, keep_generated_dir):
    data = otp.Ticks(dict(A=[1, 2, 3]))
    db = otp.DB("DB_A")
    db.add(data)
    f_session.use(db)

    derived_db1 = otp.DB("DB_A//DB_D")
    derived_db1.add(data)
    f_session.use(derived_db1)

    derived_db2 = otp.DB("DB_A//DB_D//DB_C")
    derived_db2.add(data)
    f_session.use(derived_db2)

    data = otp.DataSource(db=[derived_db1, derived_db2], tick_type='TRD')
    df = otp.run(data)
    path = os.path.join(keep_generated_dir, "DB_A", "DERIVED", "DB_D")
    assert os.listdir(path)
    assert os.listdir(os.path.join(path, "DERIVED", "DB_C"))
    assert all(df["A"] == [1, 1, 2, 2, 3, 3])


@pytest.mark.skipif(not otp.compatibility.is_derived_databases_crash_fixed(),
                    reason='Raised a segfault on older builds, cannot test')
@pytest.mark.skipif(os.name == 'nt', reason="Doesn't work on Windows")
@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason="Doesn't work in WebAPI test environment")
def test_derived_databases_with_db_set(monkeypatch):

    monkeypatch.setattr(otp.config, 'default_start_time', otp.datetime(2022, 3, 1))
    monkeypatch.setattr(otp.config, 'default_end_time', otp.datetime(2022, 3, 2))

    db_name = 'SOME_DB'

    with otp.Session(otp.Config(), override_env=True) as session:
        db = otp.DB(
            db_name,
            db_properties={
                'archive_compression_type': 'NATIVE_PLUS_GZIP',
                'tick_search_max_day_boundary_offset_sec': 86399,
                'symbology': 'BZX',
                'tick_timestamp_type': 'NANOS',
            },
            db_locations=[{
                'access_method': otp.core.db_constants.access_method.FILE,
                'day_boundary_tz': 'UTC',
                'start_time': datetime.datetime(year=2022, month=1, day=1),
                'end_time': datetime.datetime(year=2022, month=12, day=31),
            }],
        )
        session.use(db)

        dbs = otp.derived_databases(
            db=db_name,
            selection_criteria='derived_from_current_db',
        )
        assert dbs == {}


def test_show_config(f_session):
    data = otp.Ticks(dict(A=[1, 2, 3]))
    db = otp.DB("DB_A")
    db.add(data)
    f_session.use(db)

    derived_db = otp.DB("DB_A//DB_B")
    derived_db.add(data)
    f_session.use(derived_db)

    db_a = otp.databases()['DB_A']
    db_b = otp.derived_databases()['DB_A//DB_B']
    assert db_a.show_config() == db_b.show_config()
