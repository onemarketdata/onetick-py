import os
import pytest
from functools import partial


from locator_parser.io import LinesReader, FileReader, PrintWriter, FileWriter
from locator_parser.actions import Add, Modify, Delete, Get, GetAll
from locator_parser.locator import parse_locator
from locator_parser.locator import (
    DB,
    RawDB,
    TickServers,
    Location,
    Include,
    Feed,
    FeedOptions,
    ServerLocation,
)
from locator_parser.common import apply_actions


DIR = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def writer():
    return PrintWriter()


def get(w, inx):
    return w.lines[inx].strip()


def test_dbs_add(writer):
    locator = """
    <version info="2"/>
    <databases>
    </databases>
    """

    # ---------------------------------- #
    # add databases
    actions = []
    db1 = Add(DB(id="DB_1", symbology="BZX", archive_compression_type="NATIVE_PLUS_GZIP"))
    db2 = Add(DB(id="DB_2", symbology="TDEQ", db_archive_tmp_dir="/tmp"))

    actions += [db1, db2]

    assert apply_actions(parse_locator, LinesReader(locator), writer, actions)
    assert len(writer.lines[1:-1]) == 15
    assert get(writer, 1) == '<version info="2"/>'
    assert get(writer, 2) == "<databases>"
    assert get(writer, 3) == '<DB id="DB_1" symbology="BZX" archive_compression_type="NATIVE_PLUS_GZIP" >'
    # these empty sections are automatically added
    # since they are belong to the DB, have no properties
    # and consolidates real entities inside
    assert get(writer, 4) == "<LOCATIONS>"
    assert get(writer, 5) == "</LOCATIONS>"
    assert get(writer, 6) == "<RAW_DATA>"
    assert get(writer, 7) == "</RAW_DATA>"
    assert get(writer, 8) == "</DB>"
    assert get(writer, 9) == '<DB id="DB_2" symbology="TDEQ" db_archive_tmp_dir="/tmp" >'
    assert get(writer, 10) == "<LOCATIONS>"
    assert get(writer, 11) == "</LOCATIONS>"
    assert get(writer, 12) == "<RAW_DATA>"
    assert get(writer, 13) == "</RAW_DATA>"
    assert get(writer, 14) == "</DB>"
    assert get(writer, 15) == "</databases>"

    # ------------------------------------ #
    # add location
    actions = []
    location1 = Add(Location(access_method="file", day_boundary_tz="GMT", location="/tmp/location1"))
    location1.add_where(DB, id="DB_1")

    location2 = location1.clone(Location(access_method="file", location="/tmp/location2", day_boundary_tz="EST5EDT"))
    actions += [location1, location2]

    assert apply_actions(parse_locator, LinesReader("\n".join(writer.lines)), writer, actions)
    assert len(writer.lines[1:-1]) == 17
    assert get(writer, 3) == '<DB id="DB_1" symbology="BZX" archive_compression_type="NATIVE_PLUS_GZIP" >'
    assert get(writer, 4) == "<LOCATIONS>"
    assert get(writer, 5) == '<LOCATION access_method="file" day_boundary_tz="GMT" location="/tmp/location1" />'
    assert get(writer, 6) == '<LOCATION access_method="file" location="/tmp/location2" day_boundary_tz="EST5EDT" />'
    assert get(writer, 7) == "</LOCATIONS>"

    # --------------------------------- #
    # add raw db
    actions = []
    raw_db = Add(RawDB(id="PRIMARY"))
    raw_db.add_where(DB, id="DB_2")
    actions.append(raw_db)

    assert apply_actions(parse_locator, LinesReader("\n".join(writer.lines)), writer, actions)
    assert len(writer.lines[1:-1]) == 21
    assert get(writer, 11) == '<DB id="DB_2" symbology="TDEQ" db_archive_tmp_dir="/tmp" >'
    assert get(writer, 12) == "<LOCATIONS>"
    assert get(writer, 13) == "</LOCATIONS>"
    assert get(writer, 14) == "<RAW_DATA>"
    assert get(writer, 15) == '<RAW_DB id="PRIMARY" >'
    assert get(writer, 16) == "<LOCATIONS>"
    assert get(writer, 17) == "</LOCATIONS>"
    assert get(writer, 18) == "</RAW_DB>"
    assert get(writer, 19) == "</RAW_DATA>"
    assert get(writer, 20) == "</DB>"

    # -------------------------------- #
    # add location to the raw db
    location = Add(Location(mount="mount1", location="${MAIN_DATA_DIR}/raw"))
    location.add_where(DB, id="DB_2")
    location.add_where(RawDB, id="PRIMARY")

    assert apply_actions(parse_locator, LinesReader("\n".join(writer.lines)), writer, [location])

    assert len(writer.lines[1:-1]) == 22
    assert get(writer, 15) == '<RAW_DB id="PRIMARY" >'
    assert get(writer, 16) == "<LOCATIONS>"
    assert get(writer, 17) == '<LOCATION mount="mount1" location="${MAIN_DATA_DIR}/raw" />'
    assert get(writer, 18) == "</LOCATIONS>"
    assert get(writer, 19) == "</RAW_DB>"
    assert get(writer, 20) == "</RAW_DATA>"

    # --------------------------------- #
    # add feed
    feed = Add(Feed(type="raw_db"))
    feed.add_where(DB, id="DB_2")

    assert apply_actions(parse_locator, LinesReader("\n".join(writer.lines)), writer, [feed])

    assert len(writer.lines[1:-1]) == 24
    assert get(writer, 20) == "</RAW_DATA>"
    assert get(writer, 21) == '<FEED type="raw_db" >'
    assert get(writer, 22) == "</FEED>"
    assert get(writer, 23) == "</DB>"

    # --------------------------------- #
    # add feed options
    feed_options = Add(FeedOptions(frequency=1000, use_queue="yes"))
    feed_options.add_where(DB, id="DB_2")
    feed_options.add_where(Feed, type="raw_db")

    assert apply_actions(parse_locator, LinesReader("\n".join(writer.lines)), writer, [feed_options])

    assert len(writer.lines[1:-1]) == 25
    assert get(writer, 21) == '<FEED type="raw_db" >'
    assert get(writer, 22) == '<OPTIONS frequency="1000" use_queue="yes" />'
    assert get(writer, 23) == "</FEED>"
    assert get(writer, 24) == "</DB>"


def test_dbs_modify_property(writer):
    locator = """
    <version info="2"/>
    <databases>
        <db id="DB_1" symbology="BZX" archive_compression_type="NATIVE_PLUS_GZIP">
            <location>
            </locations>
            <raw_data>
            </raw_data>
        </db>
        <db id="db_2"
            symbology="TDEQ"
            db_archive_tmp_dir="/tmp" >
        <locations>
        </locations>
        </db>
    </databases>
    """

    db1_action = Modify(symbology="TDEQ")
    db1_action.add_where(DB, id="DB_1")

    db2_action = Modify(db_archive_tmp_dir="/my_dir")
    db2_action.add_where(DB, id="db_2")

    assert apply_actions(parse_locator, LinesReader(locator), writer, [db1_action, db2_action])
    assert len(writer.lines[1:-1]) == 15
    assert get(writer, 1) == '<version info="2"/>'
    assert get(writer, 2) == "<databases>"
    assert get(writer, 3) == '<db id="DB_1" symbology="TDEQ" archive_compression_type="NATIVE_PLUS_GZIP">'
    assert get(writer, 4) == "<location>"
    assert get(writer, 5) == "</locations>"
    assert get(writer, 6) == "<raw_data>"
    assert get(writer, 7) == "</raw_data>"
    assert get(writer, 8) == "</db>"
    assert get(writer, 9) == '<db id="db_2"'
    assert get(writer, 10) == 'symbology="TDEQ"'
    assert get(writer, 11) == 'db_archive_tmp_dir="/my_dir" >'
    assert get(writer, 12) == "<locations>"
    assert get(writer, 13) == "</locations>"
    assert get(writer, 14) == "</db>"
    assert get(writer, 15) == "</databases>"


def test_dbs_delete_property(writer):
    locator = """
    <version info="2"/>
    <databases>
        <db id="DB_1" symbology="BZX" archive_compression_type="NATIVE_PLUS_GZIP">
            <location>
            </locations>
            <raw_data>
            </raw_data>
        </db>
        <db id="db_2"
            symbology="TDEQ"
            db_archive_tmp_dir="/tmp" >
        <locations>
        </locations>
        </db>
    </databases>
    """
    # ------------------------------------- #
    action1 = Modify(symbology=None)
    action1.add_where(DB, id="DB_1")

    action2 = Modify(symbology=None)
    action2.add_where(DB, id="db_2")

    assert apply_actions(parse_locator, LinesReader(locator), writer, [action1, action2])
    assert len(writer.lines[1:-1]) == 15
    assert get(writer, 1) == '<version info="2"/>'
    assert get(writer, 2) == "<databases>"
    assert get(writer, 3) == '<db id="DB_1"  archive_compression_type="NATIVE_PLUS_GZIP">'
    assert get(writer, 4) == "<location>"
    assert get(writer, 5) == "</locations>"
    assert get(writer, 6) == "<raw_data>"
    assert get(writer, 7) == "</raw_data>"
    assert get(writer, 8) == "</db>"
    assert get(writer, 9) == '<db id="db_2"'
    assert get(writer, 10) == ""
    assert get(writer, 11) == 'db_archive_tmp_dir="/tmp" >'
    assert get(writer, 12) == "<locations>"
    assert get(writer, 13) == "</locations>"
    assert get(writer, 14) == "</db>"
    assert get(writer, 15) == "</databases>"

    # --------------------------------------- #
    action1 = Modify(archive_compression_type=None)
    action1.add_where(DB, id="DB_1")

    action2 = Modify(db_archive_tmp_dir=None)
    action2.add_where(DB, id="db_2")

    assert apply_actions(parse_locator, LinesReader("\n".join(writer.lines)), writer, [action1, action2])
    assert len(writer.lines[1:-1]) == 15
    assert get(writer, 1) == '<version info="2"/>'
    assert get(writer, 2) == "<databases>"
    assert get(writer, 3) == '<db id="DB_1"  >'
    assert get(writer, 4) == "<location>"
    assert get(writer, 5) == "</locations>"
    assert get(writer, 6) == "<raw_data>"
    assert get(writer, 7) == "</raw_data>"
    assert get(writer, 8) == "</db>"
    assert get(writer, 9) == '<db id="db_2"'
    assert get(writer, 10) == ""
    assert get(writer, 11) == ">"
    assert get(writer, 12) == "<locations>"
    assert get(writer, 13) == "</locations>"
    assert get(writer, 14) == "</db>"
    assert get(writer, 15) == "</databases>"

    # ---------------------------------------- #
    action1 = Delete()
    action1.add_where(DB, id="DB_1")

    action2 = Modify(xxxx="yyyy")
    action2.add_where(DB, id="db_2")
    assert apply_actions(parse_locator, LinesReader("\n".join(writer.lines)), writer, [action1, action2])
    assert len(writer.lines[1:-1]) == 9
    assert get(writer, 1) == '<version info="2"/>'
    assert get(writer, 2) == "<databases>"
    assert get(writer, 3) == '<db id="db_2"'
    assert get(writer, 4) == ""
    assert get(writer, 5) == 'xxxx="yyyy">'
    assert get(writer, 6) == "<locations>"
    assert get(writer, 7) == "</locations>"
    assert get(writer, 8) == "</db>"
    assert get(writer, 9) == "</databases>"


def test_tick_servers_add_location(writer):
    locator = """
    <version info="2"/>
    <databases>
    </databases>
    <tick_servers>
    </tick_servers>
    <includes>
    </includes>
    """

    location1 = Add(ServerLocation(location="1.2.3.4"))
    location2 = Add(ServerLocation(location="3.4.5.6"))

    assert apply_actions(parse_locator, LinesReader(locator), writer, [location1, location2])
    assert len(writer.lines[1:-1]) == 9
    assert get(writer, 1) == '<version info="2"/>'
    assert get(writer, 2) == "<databases>"
    assert get(writer, 3) == "</databases>"
    assert get(writer, 4) == "<tick_servers>"
    assert get(writer, 5) == '<LOCATION location="1.2.3.4" />'
    assert get(writer, 6) == '<LOCATION location="3.4.5.6" />'
    assert get(writer, 7) == "</tick_servers>"
    assert get(writer, 8) == "<includes>"
    assert get(writer, 9) == "</includes>"


def test_tick_servers_modify_location(writer):
    locator = """
    <version info="2"/>
    <databases>
    </databases>
    <tick_servers>
        <location location="1.2.3.4" />
        <location location="3.4.5.6" />
    </tick_servers>
    <includes>
    </includes>
    """
    action = Modify(location="7.7.7.7")
    action.add_where(ServerLocation, location="1.2.3.4")

    assert apply_actions(parse_locator, LinesReader(locator), writer, [action])
    assert len(writer.lines[1:-1]) == 9
    assert get(writer, 5) == '<location location="7.7.7.7" />'


def test_get_all_remote_locations(writer):
    locator = """
    <version info="2"/>
    <databases>
        <DB ID="SOME_DB_1">
            <locations>
                <location location="/local_location_1" />
            </locations>
        </DB>
        <DB ID="SOME_DB_2">
            <locations>
                <location location="/local_location_2" />
            </locations>
        </DB>
    </databases>
    <tick_servers>
        <location location="1.2.3.4" />
        <location location="3.4.5.6" />
    </tick_servers>
    <includes>
    </includes>
    """
    action = GetAll()
    action.add_where(TickServers)
    action.add_where(ServerLocation)

    apply_actions(parse_locator, LinesReader(locator), writer, [action])

    assert action.result[0].location == "1.2.3.4"
    assert action.result[1].location == "3.4.5.6"


def test_tick_servers_delete_location(writer):
    locator = """
    <version info="2"/>
    <databases>
    </databases>
    <tick_servers>
        <location location="1.2.3.4" />
        <location location="3.4.5.6" />
    </tick_servers>
    <includes>
    </includes>
    """
    action = Delete()
    action.add_where(ServerLocation, location="1.2.3.4")

    assert apply_actions(parse_locator, LinesReader(locator), writer, [action])
    assert len(writer.lines[1:-1]) == 8
    assert get(writer, 4) == "<tick_servers>"
    assert get(writer, 5) == '<location location="3.4.5.6" />'
    assert get(writer, 6) == "</tick_servers>"


def test_includes(writer):
    locator = """
    <version info="2"/>
    <databases>
    </databases>
    <tick_servers>
        <location location="1.2.3.4" />
        <location location="3.4.5.6" />
    </tick_servers>
    <includes>
    </includes>
    """
    include = Add(Include(path="${MAIN_CLIENT_DIR}/locator.server"))

    assert apply_actions(parse_locator, LinesReader(locator), writer, [include])
    assert len(writer.lines[1:-1]) == 10
    assert get(writer, 8) == "<includes>"
    assert get(writer, 9) == '<INCLUDE path="${MAIN_CLIENT_DIR}/locator.server" />'
    assert get(writer, 10) == "</includes>"


def test_from_file(writer):
    # ----------------------------------------------- #
    # Get DB
    action = Get()
    action.add_where(DB, id="LB")

    assert apply_actions(parse_locator, FileReader(os.path.join(DIR, "files", "locator.server")), writer, [action])

    assert action.result.id == "LB"
    assert action.result.symbology == "BZX"
    assert action.result.archive_compression_type == "NATIVE_PLUS_GZIP"
    assert action.result.memory_db_dir == "${MAIN_DATA_DIR}/LB/shmem"
    assert action.result.mmap_db_compression_type == "NATIVE_PLUS_GZIP"
    assert action.result.db_archive_tmp_dir == "${MAIN_DATA_DIR}/tmp/LB"
    assert type(action.result) is DB

    # ------------------------------------------------ #
    # Get location
    action1 = Get()
    action1.add_where(DB, id="LB")
    action1.add_where(Location, location="${MAIN_DATA_DIR}/Anonymized/LB")

    action2 = Get()
    action2.add_where(DB, id="S_ORDERS_FEED")
    action2.add_where(RawDB, id="PRIMARY")
    action2.add_where(Location, mount="mount1")

    assert apply_actions(
        parse_locator, FileReader(os.path.join(DIR, "files", "locator.server")), writer, [action1, action2]
    )

    assert type(action1.result) is Location
    assert action1.result.access_method == "file"
    assert action1.result.day_boundary_tz == "GMT"
    assert action1.result.start_time == "20050101000000"
    assert action1.result.end_time == "20990101000000"

    assert type(action2.result) is Location
    assert action2.result.location == "${MAIN_DATA_DIR}/raw_data/s_orders_feed/primary"
    assert action2.result.start_time == "19930101000000"
    assert action2.result.end_time == "20201231000000"

    # --------------------------------------------------- #
    # Add found location from DB to RAW_DB
    action = Add(action1.result)
    action.add_where(DB, id="S_ORDERS_FEED")
    action.add_where(RawDB, id="PRIMARY")

    action_t = Add(action1.result)
    action_t.add_where(DB, id="S_ORDERS_FEED")
    action_t.add_where(RawDB, id="PRIMARY")

    new_writer = FileWriter(os.path.join(DIR, "files", "locator.server_result"))
    assert apply_actions(
        parse_locator,
        FileReader(os.path.join(DIR, "files", "locator.server")),
        new_writer,
        [action, action_t],
        flush=True,
    )

    assert len(new_writer.lines) == len(writer.lines) + 2
    os.remove(os.path.join(DIR, "files", "locator.server_result"))


def test_get_all(writer):
    action = GetAll()
    action.add_where(DB)

    apply_actions(parse_locator, FileReader(os.path.join(DIR, "files", "locator.server")), writer, [action])

    db_ids = list(map(lambda x: x.id, action.result))

    assert len(db_ids) == 3
    assert "LB" in db_ids
    assert "S_ORDERS_FEED" in db_ids
    assert "S_ORDERS_TEMP" in db_ids

    action = GetAll()
    action.add_where(DB, id="LB")
    action.add_where(Location)

    apply_actions(parse_locator, FileReader(os.path.join(DIR, "files", "locator.server")), writer, [action])

    # one section in RAW_DATA
    assert len(action.result) == 2
    assert action.result[0].access_method == "file"
    assert action.result[1].mount == "mount1"

    # output all file locations
    for db_id in db_ids:
        action = GetAll()
        action.add_where(DB, id=db_id)
        action.add_where(Location)

        apply_actions(parse_locator, FileReader(os.path.join(DIR, "files", "locator.server")), writer, [action])

        print('DB "%s" has the following file locations:' % db_id)
        for loc in action.result:
            if hasattr(loc, "access_method") and loc.access_method == "file":
                print(" - ", loc.location)


def test_get_all_recursively(writer, monkeypatch):
    monkeypatch.setenv("SOME_ENV", os.path.join(DIR, "files"))

    action = GetAll()
    action.add_where(DB)

    def func(*args):
        return parse_locator(*args, recursively=True)

    apply_actions(func, FileReader(os.path.join(DIR, "files", "locator.a")), writer, [action])

    res = set(map(lambda x: x.id, action.result))

    assert res == set(["A", "B", "C"])


def test_condition_shared_between_root_tags(writer):
    # PY-1438

    locator = """
    <VERSION_INFO VERSION="2"/>
    <DATABASES>
    <DB ID="DB1">
      <LOCATIONS>
      </LOCATIONS>
    </DB>
    </DATABASES>
    <TICK_SERVERS>
      <location location="${TEST}" />
    </TICK_SERVERS>
    """
    action = GetAll()
    action.add_where(DB, id="DB1")
    action.add_where(Location)

    apply_actions(partial(parse_locator, recursively=True), LinesReader(locator), writer, [action])

    assert action.result == []
