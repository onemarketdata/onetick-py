import os
import pytest

from locator_parser.io import LinesReader, PrintWriter, FileReader, FileWriter
from locator_parser.actions import Add, Modify, Delete, Get, GetAll
from locator_parser.acl import parse_acl
from locator_parser.acl import EP, Allow, Role, User, DB
from locator_parser.common import apply_actions

DIR = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def writer():
    return PrintWriter()


def get_result(w):
    return "\n".join(x for x in map(lambda x: x.strip(), w.lines) if x != "")


def get(w, inx):
    return w.lines[inx].strip()


def test_simple(writer):
    acl = """
    <roles>
    </roles>
    <databases>
    </databases>
    <event_processors>
    </event_processors>
    """

    parse_acl(LinesReader(acl), writer)

    result = get_result(writer)

    assert "<roles>" in result
    assert "</roles>" in result
    assert "<databases>" in result
    assert "</databases>" in result
    assert "<event_processors>" in result
    assert "</event_processors>" in result


def test_delete_single_role(writer):
    acl = """
    <roles>
    <role name="Admin">
    </role>
    </roles>
    """

    action = Delete()
    action.add_where(Role, name="Admin")

    parse_acl(LinesReader(acl), writer, action)

    result = get_result(writer)

    # test that action was applied
    assert action.executed

    assert "<roles>" in result
    assert "</roles>" in result
    # but!
    assert '<role name="Admin">' not in result
    assert "</role>" not in result

    assert len(writer.lines[1:-1]) == 2
    assert get(writer, 1) == "<roles>"
    assert get(writer, 2) == "</roles>"


def test_delete_first_role_from_two(writer):
    acl = """
    < roles >
      <Role name="Admin">
      </role>
      <role name="Users" >
      </ROLE>
    </RoLeS>
    """

    action = Delete()
    action.add_where(Role, name="Users")

    parse_acl(LinesReader(acl), writer, action)

    result = get_result(writer)

    # test that action was applied
    assert action.executed

    assert "< roles >" in result
    assert '<Role name="Admin">' in result
    assert "</role>" in result
    assert "</RoLeS>" in result
    # but!
    assert '<role name="Users" >' not in result
    assert "</ROLE>" not in result

    assert len(writer.lines[1:-1]) == 4
    assert get(writer, 1) == "< roles >"
    assert get(writer, 2) == '<Role name="Admin">'
    assert get(writer, 3) == "</role>"
    assert get(writer, 4) == "</RoLeS>"


def test_delete_second_role_from_two(writer):
    acl = """
    < roles >
        < Role name="Admin">
        </role>
        <role name="Users" >
        </ROLE>
    </RoLeS>
    """

    action = Delete()
    action.add_where(Role, name="Admin")

    parse_acl(LinesReader(acl), writer, action)

    result = get_result(writer)

    # test that action was applied
    assert action.executed

    assert "< roles >" in result
    assert '<role name="Users" >' in result
    assert "</ROLE>" in result
    assert "</RoLeS>" in result
    # but!
    assert '< Role name="Admin">' not in result
    assert "</role>" not in result

    assert len(writer.lines[1:-1]) == 4
    assert get(writer, 1) == "< roles >"
    assert get(writer, 2) == '<role name="Users" >'
    assert get(writer, 3) == "</ROLE>"
    assert get(writer, 4) == "</RoLeS>"


def test_delete_middle_role_from_three(writer):
    acl = """
    <roles>
        < role name="Admin1" >
        </role>
        <role name="Admin2" >
        </role>
        <role name="Admin3">
        </role>
    </roles>
    """

    action = Delete()
    action.add_where(Role, name="Admin2")

    parse_acl(LinesReader(acl), writer, action)

    _ = get_result(writer)

    # test that action was applied
    assert action.executed

    assert len(writer.lines[1:-1]) == 6
    assert get(writer, 1) == "<roles>"
    assert get(writer, 2) == '< role name="Admin1" >'
    assert get(writer, 3) == "</role>"
    assert get(writer, 4) == '<role name="Admin3">'
    assert get(writer, 5) == "</role>"
    assert get(writer, 6) == "</roles>"


def test_delete_role_with_users(writer):
    acl = """
    <roles>
    <role name="Admin" >
        <user name="onetick" />
        <user name="test" />
        <user name="test2" />
    </role>
    </roles>
    """

    action = Delete()
    action.add_where(Role, name="Admin")

    parse_acl(LinesReader(acl), writer, action)

    # test that action was applied
    assert action.executed

    assert len(writer.lines[1:-1]) == 2
    assert get(writer, 1) == "<roles>"
    assert get(writer, 2) == "</roles>"


def test_delete_user_in_first_role(writer):
    acl = """
    <roles>
    <role name="Admin" >
        <user name="test"/>
        <user name="test2" />
    </role>
    <role name="Users" >
        <user name="test2" />
        <user name="test" />
    </role>
    </roles>
    """

    action = Delete()
    action.add_where(Role, name="Admin")
    action.add_where(User, name="test")

    parse_acl(LinesReader(acl), writer, action)

    # test that action was applied
    assert action.executed

    assert len(writer.lines[1:-1]) == 9
    assert get(writer, 1) == "<roles>"
    assert get(writer, 2) == '<role name="Admin" >'
    assert get(writer, 3) == '<user name="test2" />'
    assert get(writer, 4) == "</role>"
    assert get(writer, 5) == '<role name="Users" >'
    assert get(writer, 6) == '<user name="test2" />'
    assert get(writer, 7) == '<user name="test" />'
    assert get(writer, 8) == "</role>"
    assert get(writer, 9) == "</roles>"


def test_delete_user_in_second_role(writer):
    acl = """
    <roles>
    <role name="Admin">
        <user name="test" />
        <user name="test2" />
    </role>
    <role name="Users">
        <user name="test2" />
        <user name="test"/>
    </role>
    </roles>
    """

    action = Delete()
    action.add_where(Role, name="Users")
    action.add_where(User, name="test")

    parse_acl(LinesReader(acl), writer, action)

    # test that action was applied
    assert action.executed

    assert len(writer.lines[1:-1]) == 9
    assert get(writer, 1) == "<roles>"
    assert get(writer, 2) == '<role name="Admin">'
    assert get(writer, 3) == '<user name="test" />'
    assert get(writer, 4) == '<user name="test2" />'
    assert get(writer, 5) == "</role>"
    assert get(writer, 6) == '<role name="Users">'
    assert get(writer, 7) == '<user name="test2" />'
    assert get(writer, 8) == "</role>"
    assert get(writer, 9) == "</roles>"


def test_delete_all_users_from_role(writer):
    # --------------------------------- #
    # Delete middle
    acl = """
    <roles>

    <role name="Admin">
        <user name="test" />
        <user name="test2" />
        <user name="test3"/>
    </role>
    </roles>
    """

    action = Delete()
    action.add_where(Role, name="Admin")
    action.add_where(User, name="test2")

    parse_acl(LinesReader(acl), writer, action)

    # test that action was applied
    assert action.executed

    assert len(writer.lines[1:-1]) == 7
    assert get(writer, 1) == "<roles>"
    assert get(writer, 2) == ""
    assert get(writer, 3) == '<role name="Admin">'
    assert get(writer, 4) == '<user name="test" />'
    assert get(writer, 5) == '<user name="test3"/>'
    assert get(writer, 6) == "</role>"
    assert get(writer, 7) == "</roles>"

    # ----------------------------------- #
    # Delete second
    action2 = Delete()
    action2.add_where(Role, name="Admin")
    action2.add_where(User, name="test3")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action2)

    # test that action applied
    assert action2.executed

    assert len(writer.lines[1:-1]) == 6
    assert get(writer, 1) == "<roles>"
    assert get(writer, 2) == ""
    assert get(writer, 3) == '<role name="Admin">'
    assert get(writer, 4) == '<user name="test" />'
    assert get(writer, 5) == "</role>"
    assert get(writer, 6) == "</roles>"

    # ----------------------------------- #
    # Delete last
    action3 = Delete()
    action3.add_where(Role, name="Admin")
    action3.add_where(User, name="test")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action3)

    assert action3.executed

    assert len(writer.lines[1:-1]) == 5
    assert get(writer, 1) == "<roles>"
    assert get(writer, 2) == ""
    assert get(writer, 3) == '<role name="Admin">'
    assert get(writer, 4) == "</role>"
    assert get(writer, 5) == "</roles>"


def test_modify_last_field_database(writer):
    acl = """
    <databases>
        <db ID="S_DB_1" read_access="true">
            <allow role="Admin" write_access="true"/>
        </db>
    </databases>
    """

    action = Modify(read_access=False)
    action.add_where(DB, id="S_DB_1")

    parse_acl(LinesReader(acl), writer, action)

    assert action.executed

    assert len(writer.lines[1:-1]) == 5
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db ID="S_DB_1" read_access="False">'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == "</db>"
    assert get(writer, 5) == "</databases>"


def test_modify_key_field_database(writer):
    acl = """
    < DATABASES>
        <DB ID="S_DB_1" read_access="true">
        </DB>
    </databases>
    """

    action = Modify(id="xxx")
    action.add_where(DB, id="S_DB_1")

    parse_acl(LinesReader(acl), writer, action)

    assert action.executed

    assert get(writer, 1) == "< DATABASES>"
    assert get(writer, 2) == '<DB ID="xxx" read_access="true">'
    assert get(writer, 3) == "</DB>"
    assert get(writer, 4) == "</databases>"


def test_modify_allow_last_field(writer):
    acl = """
    <databases>
        <db ID="S_DB_1" read_access="true">
            <allow role="Admin" write_access="true"/>
        </db>
    </databases>
    """

    action = Modify(write_access=False)
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, role="Admin")

    parse_acl(LinesReader(acl), writer, action)

    assert action.executed

    assert len(writer.lines[1:-1]) == 5
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db ID="S_DB_1" read_access="true">'
    assert get(writer, 3) == '<allow role="Admin" write_access="False"/>'
    assert get(writer, 4) == "</db>"
    assert get(writer, 5) == "</databases>"


def test_modify_allow_key_field(writer):
    acl = """
    <databases>
        <db id="S_DB_1" read_access="true">
            < allow ROLE="Admin" write_access="true"/>
        </db>
    </databases>
    """

    action = Modify(role="Users")
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, role="Admin")

    parse_acl(LinesReader(acl), writer, action)

    assert action.executed

    assert len(writer.lines[1:-1]) == 5
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true">'
    assert get(writer, 3) == '< allow ROLE="Users" write_access="true"/>'
    assert get(writer, 4) == "</db>"
    assert get(writer, 5) == "</databases>"


def test_modify_middle_allow(writer):
    acl = """
    <databases>
        <db id="S_DB_1" read_access="true">
            <allow role="Admin" write_access="true"/>
            <allow role="Testers" write_access="true" />
            <allow role="Users" write_access="true"/>
        </db>
    </databases>
    """

    # ------------------------------------- #

    action = Modify(role="Customers")
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, role="Testers")

    parse_acl(LinesReader(acl), writer, action)

    assert action.executed

    assert len(writer.lines[1:-1]) == 7
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true">'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Customers" write_access="true" />'
    assert get(writer, 5) == '<allow role="Users" write_access="true"/>'
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"

    # -------------------------------------- #

    action = Modify(write_access=False)
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, role="Customers")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert len(writer.lines[1:-1]) == 7
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true">'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Customers" write_access="False" />'
    assert get(writer, 5) == '<allow role="Users" write_access="true"/>'
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"

    # -------------------------------------- #
    # Delete it

    action = Delete()
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, role="Customers")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert len(writer.lines[1:-1]) == 6
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true">'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Users" write_access="true"/>'
    assert get(writer, 5) == "</db>"
    assert get(writer, 6) == "</databases>"


def test_add_property(writer):
    acl = """
    <databases>
        <db id="S_DB_1" read_access="true">
            <allow role="Admin" write_access="true"/>
            <allow role="Testers" write_access="true" />
            <allow role="Users" write_access="true"/>
        </db>
    </databases>
    """

    # ---------------------------------------- #
    # add property to the allow section

    action = Modify(read_access=True)
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, role="Testers")

    parse_acl(LinesReader(acl), writer, action)

    assert action.executed

    assert len(writer.lines[1:-1]) == 7
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true">'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Testers" write_access="true"  read_access="True"/>'
    assert get(writer, 5) == '<allow role="Users" write_access="true"/>'
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"

    # -------------------------------------- #
    # add property to the db section

    action = Modify(xxx="yyy")
    action.add_where(DB, id="S_DB_1")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed

    assert len(writer.lines[1:-1]) == 7
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true" xxx="yyy">'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Testers" write_access="true"  read_access="True"/>'
    assert get(writer, 5) == '<allow role="Users" write_access="true"/>'
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"

    # ------------------------------------- #
    # it's not allowed to add properties to sections without properties

    action = Modify(abc="def")
    t_lines = writer.lines

    try:
        parse_acl(LinesReader("\n".join(writer.lines)), writer, action)
        # it raises an exception, because databases is not marked to have properties
        assert False
    except Exception:
        assert True

    writer = PrintWriter()
    writer.lines = t_lines

    # ------------------------------------- #
    # change middle property before added new

    action = Modify(write_access=False)
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, role="Testers")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed

    assert len(writer.lines[1:-1]) == 7
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true" xxx="yyy">'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Testers" write_access="False"  read_access="True"/>'
    assert get(writer, 5) == '<allow role="Users" write_access="true"/>'
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"

    # ------------------------------------ #
    # change already added property

    action = Modify(xxx="zzzz")
    action.add_where(DB, id="S_DB_1")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true" xxx="zzzz">'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Testers" write_access="False"  read_access="True"/>'
    assert get(writer, 5) == '<allow role="Users" write_access="true"/>'
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"

    # ------------------------------------ #
    # add new key to the last allow

    action = Modify(new_key="key")
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, role="Users")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true" xxx="zzzz">'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Testers" write_access="False"  read_access="True"/>'
    assert get(writer, 5) == '<allow role="Users" write_access="true" new_key="key"/>'
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"

    # ------------------------------------- #
    # modify property according to the new added key

    action = Modify(role="Customers")
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, new_key="key")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true" xxx="zzzz">'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Testers" write_access="False"  read_access="True"/>'
    assert get(writer, 5) == '<allow role="Customers" write_access="true" new_key="key"/>'
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"

    # ------------------------------------- #
    # add new property after already added property

    action = Modify(another_key="blabla")
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, new_key="key")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true" xxx="zzzz">'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Testers" write_access="False"  read_access="True"/>'
    assert get(writer, 5) == '<allow role="Customers" write_access="true" new_key="key" another_key="blabla"/>'
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"


def test_delete_property(writer):
    acl = """
    <databases>
        <db id="S_DB_1" read_access="true" xxx="zzzz">
            <allow role="Admin" write_access="true"/>
            <allow role="Testers" write_access="false"  read_access="true"/>
            <allow role="Customers" write_access="true" new_key="key"/>
        </db>
    </databases>
    """

    # ---------------------------------------- #
    # delete property from db
    action = Modify(xxx=None)
    action.add_where(DB, id="S_DB_1")

    parse_acl(LinesReader(acl), writer, action)

    assert action.executed
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true" >'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Testers" write_access="false"  read_access="true"/>'
    assert get(writer, 5) == '<allow role="Customers" write_access="true" new_key="key"/>'
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"

    # ---------------------------------------- #
    # delete property from allow
    action = Modify(new_key=None)
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, role="Customers")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true" >'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Testers" write_access="false"  read_access="true"/>'
    assert get(writer, 5) == '<allow role="Customers" write_access="true" />'
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"

    # -------------------------------------- #
    # delete second property of the allow section
    action = Modify(write_access=None)
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, role="Customers")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true" >'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Testers" write_access="false"  read_access="true"/>'
    assert get(writer, 5) == '<allow role="Customers"  />'
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"

    # --------------------------------------- #
    # delete last property of the allow section
    action = Modify(role=None)
    action.add_where(DB, id="S_DB_1")
    action.add_where(Allow, role="Customers")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == '<db id="S_DB_1" read_access="true" >'
    assert get(writer, 3) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 4) == '<allow role="Testers" write_access="false"  read_access="true"/>'
    # NOTE: you can't access to that section more, because it doesn't have any identificator
    assert get(writer, 5) == "<allow   />"
    assert get(writer, 6) == "</db>"
    assert get(writer, 7) == "</databases>"

    # -------------------------------------- #
    # delete db
    action = Delete()
    action.add_where(DB, id="S_DB_1")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert get(writer, 1) == "<databases>"
    assert get(writer, 2) == "</databases>"


def test_add_ep(writer):
    acl = """
    <event_processors>
    </event_processors>
    """

    # ------------------------------------ #
    # add one entity
    action = Add(EP(id="READ_FROM_RAW"))

    parse_acl(LinesReader(acl), writer, action)

    assert action.executed

    assert len(writer.lines[1:-1]) == 4
    assert get(writer, 1) == "<event_processors>"
    assert get(writer, 2) == '<EP id="READ_FROM_RAW" >'
    assert get(writer, 3) == "</EP>"
    assert get(writer, 4) == "</event_processors>"

    # ------------------------------------ #
    # add new one entity
    action = Add(EP(id="WRITE_TEXT"))

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed

    assert len(writer.lines[1:-1]) == 6
    assert get(writer, 1) == "<event_processors>"
    assert get(writer, 2) == '<EP id="READ_FROM_RAW" >'
    assert get(writer, 3) == "</EP>"
    assert get(writer, 4) == '<EP id="WRITE_TEXT" >'
    assert get(writer, 5) == "</EP>"
    assert get(writer, 6) == "</event_processors>"


def test_add_inner_allows(writer):
    acl = """
    <event_processors>
    </event_processors>
    """

    # ------------------------------------ #
    # add EP
    action = Add(EP(id="WRITE_TEXT"))

    parse_acl(LinesReader(acl), writer, action)

    assert action.executed

    assert len(writer.lines[1:-1]) == 4

    # ------------------------------------ #
    # add allow
    action = Add(Allow(role="Admin"))
    action.add_where(EP, id="WRITE_TEXT")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert len(writer.lines[1:-1]) == 5
    assert get(writer, 1) == "<event_processors>"
    assert get(writer, 2) == '<EP id="WRITE_TEXT" >'
    assert get(writer, 3) == '<ALLOW role="Admin" />'
    assert get(writer, 4) == "</EP>"
    assert get(writer, 5) == "</event_processors>"

    # ----------------------------------- #
    # add one more allow
    action = Add(Allow(role="Users", other=3))
    action.add_where(EP, id="WRITE_TEXT")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert len(writer.lines[1:-1]) == 6
    assert get(writer, 1) == "<event_processors>"
    assert get(writer, 2) == '<EP id="WRITE_TEXT" >'
    assert get(writer, 3) == '<ALLOW role="Admin" />'
    assert get(writer, 4) == '<ALLOW role="Users" other="3" />'
    assert get(writer, 5) == "</EP>"
    assert get(writer, 6) == "</event_processors>"

    # ---------------------------------- #
    # add one more EP
    action = Add(EP(id="READ_FROM_RAW", text="abc"))

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert len(writer.lines[1:-1]) == 8
    assert get(writer, 1) == "<event_processors>"
    assert get(writer, 2) == '<EP id="WRITE_TEXT" >'
    assert get(writer, 3) == '<ALLOW role="Admin" />'
    assert get(writer, 4) == '<ALLOW role="Users" other="3" />'
    assert get(writer, 5) == "</EP>"
    assert get(writer, 6) == '<EP id="READ_FROM_RAW" text="abc" >'
    assert get(writer, 7) == "</EP>"
    assert get(writer, 8) == "</event_processors>"

    # ----------------------------------- #
    # add allow to the new EP
    action = Add(Allow(role="Users", xxx="yyyy"))
    action.add_where(EP, id="READ_FROM_RAW")

    parse_acl(LinesReader("\n".join(writer.lines)), writer, action)

    assert action.executed
    assert len(writer.lines[1:-1]) == 9
    assert get(writer, 1) == "<event_processors>"
    assert get(writer, 2) == '<EP id="WRITE_TEXT" >'
    assert get(writer, 3) == '<ALLOW role="Admin" />'
    assert get(writer, 4) == '<ALLOW role="Users" other="3" />'
    assert get(writer, 5) == "</EP>"
    assert get(writer, 6) == '<EP id="READ_FROM_RAW" text="abc" >'
    assert get(writer, 7) == '<ALLOW role="Users" xxx="yyyy" />'
    assert get(writer, 8) == "</EP>"
    assert get(writer, 9) == "</event_processors>"


def test_multi_actions(writer):
    acl = """
    <roles>
    </roles>
    <databases>
    </databases>
    <event_processors>
    </event_processors>
    """

    actions = []

    # adding roles
    action = Add(Role(name="Admin"))
    actions.append(action)

    action = Add(User(name="onetick"))
    action.add_where(Role, name="Admin")
    actions.append(action)

    actions.append(action.clone(User(name="test")))

    # adding dbs
    action = Add(DB(id="DB_1", read_access=True))
    actions.append(action)

    action = Add(Allow(role="Admin", write_access=False, read_access=True))
    action.add_where(DB, id="DB_1")
    actions.append(action)
    actions.append(action.clone(Allow(role="Users", write_access=True)))

    action = Add(DB(id="DB_2", write_access=True))
    actions.append(action)

    # adding EPs
    action = Add(EP(id="READ_FROM_RAW"))
    actions.append(action)

    action = Add(Allow(role="Admin"))
    action.add_where(EP, id="READ_FROM_RAW")
    actions.append(action)

    # returns True if all actions were executed
    assert apply_actions(parse_acl, LinesReader(acl), writer, actions)

    assert len(writer.lines[1:-1]) == 19
    assert get(writer, 1) == "<roles>"
    assert get(writer, 2) == '<ROLE name="Admin" >'
    assert get(writer, 3) == '<USER name="onetick" />'
    assert get(writer, 4) == '<USER name="test" />'
    assert get(writer, 5) == "</ROLE>"
    assert get(writer, 6) == "</roles>"
    assert get(writer, 7) == "<databases>"
    assert get(writer, 8) == '<DB id="DB_1" read_access="True" >'
    assert get(writer, 9) == '<ALLOW role="Admin" write_access="False" read_access="True" />'
    assert get(writer, 10) == '<ALLOW role="Users" write_access="True" />'
    assert get(writer, 11) == "</DB>"
    assert get(writer, 12) == '<DB id="DB_2" write_access="True" >'
    assert get(writer, 13) == "</DB>"
    assert get(writer, 14) == "</databases>"
    assert get(writer, 15) == "<event_processors>"
    assert get(writer, 16) == '<EP id="READ_FROM_RAW" >'
    assert get(writer, 17) == '<ALLOW role="Admin" />'
    assert get(writer, 18) == "</EP>"
    assert get(writer, 19) == "</event_processors>"


def test_comment(writer):
    acl = """
    # comment 1
    <event_processors>
    # comment 2
    </event_processors>
    # comment 3
    """

    action = Add(EP(id="READ_FROM_RAW"))

    parse_acl(LinesReader(acl), writer, action)

    assert action.executed
    assert len(writer.lines[1:-1]) == 7
    assert get(writer, 1) == "# comment 1"
    assert get(writer, 2) == "<event_processors>"
    assert get(writer, 3) == "# comment 2"
    assert get(writer, 4) == '<EP id="READ_FROM_RAW" >'
    assert get(writer, 5) == "</EP>"
    assert get(writer, 6) == "</event_processors>"
    assert get(writer, 7) == "# comment 3"


def test_read_from_file(writer):
    assert apply_actions(parse_acl, FileReader(os.path.join(DIR, "files", "acl.txt")), writer)
    assert len(writer.lines) == 52

    assert get(writer, 0) == "<roles>"
    assert get(writer, 1) == ""
    assert get(writer, 2) == '<role name="Admin" >'
    assert get(writer, 3) == '<user name="onetick" />'
    assert get(writer, 4) == '<user name="test" />'
    assert get(writer, 5) == "<user name=${LINUX_USERNAME} />"
    assert get(writer, 6) == "</role>"
    assert get(writer, 7) == ""
    assert get(writer, 8) == '<role name="Users" >'
    assert get(writer, 9) == '<user name="onetick" />'
    assert get(writer, 10) == "</role>"
    assert get(writer, 11) == ""
    assert get(writer, 12) == "</roles>"
    assert get(writer, 13) == ""
    assert get(writer, 14) == "<databases>"
    assert get(writer, 15) == ""
    assert get(writer, 16) == '<db ID="S_ORDERS_FEED" read_access="true">'
    assert get(writer, 17) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 18) == "</db>"
    assert get(writer, 19) == ""
    assert get(writer, 20) == '<db ID="S_ORDERS_TEMP" read_access="true">'
    assert get(writer, 21) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 22) == "</db>"
    assert get(writer, 23) == ""
    assert get(writer, 24) == '<db ID="WORKFLOW" read_access="true">'
    assert get(writer, 25) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 26) == "</db>"
    assert get(writer, 27) == ""
    assert get(writer, 28) == '<db ID="S_ORDERS_FIX" read_access="true">'
    assert get(writer, 29) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 30) == "</db>"
    assert get(writer, 31) == ""
    assert get(writer, 32) == "</databases>"
    assert get(writer, 33) == ""
    assert get(writer, 34) == ""
    assert get(writer, 35) == "<event_processors>"
    assert get(writer, 36) == '<ep ID="READ_FROM_RAW">'
    assert get(writer, 37) == '<allow role="Admin" />'
    assert get(writer, 38) == '<allow role="Users" />'
    assert get(writer, 39) == "</ep>"
    assert get(writer, 40) == ""
    assert get(writer, 41) == '<ep ID="WRITE_TEXT">'
    assert get(writer, 42) == '<allow role="Admin" />'
    assert get(writer, 43) == '<allow role="Users" />'
    assert get(writer, 44) == "</ep>"
    assert get(writer, 45) == ""
    assert get(writer, 46) == '<ep ID="COMMAND_EXECUTE">'
    assert get(writer, 47) == '<allow role="Admin" />'
    assert get(writer, 48) == '<allow role="Users" />'
    assert get(writer, 49) == "</ep>"
    assert get(writer, 50) == ""
    assert get(writer, 51) == "</event_processors>"

    # ---------------------------------------------- #
    db = Add(DB(id="S_ORDERS_ENHANCED", read_access=True))

    allow = Add(Allow(role="Admin", write_access=True))
    allow.add_where(DB, id="S_ORDERS_ENHANCED")

    assert apply_actions(parse_acl, FileReader(os.path.join(DIR, "files", "acl.txt")), writer, [db, allow])
    assert len(writer.lines) == 55

    assert get(writer, 28) == '<db ID="S_ORDERS_FIX" read_access="true">'
    assert get(writer, 29) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 30) == "</db>"
    assert get(writer, 31) == ""
    assert get(writer, 32) == '<DB id="S_ORDERS_ENHANCED" read_access="True" >'

    # ----------------------------------------------- #
    delete1 = Delete()
    delete1.add_where(EP, id="READ_FROM_RAW")

    delete2 = Delete()
    delete2.add_where(DB, id="WORKFLOW")

    add = Add(User(name="new_user"))
    add.add_where(Role, name="Admin")

    assert apply_actions(parse_acl, FileReader(os.path.join(DIR, "files", "acl.txt")), writer, [delete1, delete2, add])
    assert len(writer.lines) == 46

    assert get(writer, 5) == "<user name=${LINUX_USERNAME} />"
    assert get(writer, 6) == '<USER name="new_user" />'
    assert get(writer, 7) == "</role>"

    assert get(writer, 21) == '<db ID="S_ORDERS_TEMP" read_access="true">'
    assert get(writer, 22) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 23) == "</db>"
    assert get(writer, 24) == ""
    assert get(writer, 25) == ""
    assert get(writer, 26) == '<db ID="S_ORDERS_FIX" read_access="true">'
    assert get(writer, 27) == '<allow role="Admin" write_access="true"/>'
    assert get(writer, 28) == "</db>"
    assert get(writer, 29) == ""
    assert get(writer, 30) == "</databases>"

    assert get(writer, 33) == "<event_processors>"
    assert get(writer, 34) == ""
    assert get(writer, 35) == '<ep ID="WRITE_TEXT">'
    assert get(writer, 36) == '<allow role="Admin" />'
    assert get(writer, 37) == '<allow role="Users" />'
    assert get(writer, 38) == "</ep>"
    assert get(writer, 39) == ""
    assert get(writer, 40) == '<ep ID="COMMAND_EXECUTE">'
    assert get(writer, 41) == '<allow role="Admin" />'
    assert get(writer, 42) == '<allow role="Users" />'
    assert get(writer, 43) == "</ep>"
    assert get(writer, 44) == ""
    assert get(writer, 45) == "</event_processors>"


def test_write_to_file():
    writer1 = FileWriter(os.path.join(DIR, "files", "result_acl.txt"))
    writer2 = PrintWriter()
    assert apply_actions(parse_acl, FileReader(os.path.join(DIR, "files", "acl.txt")), writer1, flush=True)
    assert apply_actions(parse_acl, FileReader(os.path.join(DIR, "files", "result_acl.txt")), writer2)

    assert len(writer1.lines) == len(writer2.lines)

    for line1, line2 in zip(writer1.lines, writer2.lines):
        assert line1 == line2

    os.remove(os.path.join(DIR, "files", "result_acl.txt"))


def test_get_1():
    action = Get()
    action.add_where(DB, id="S_ORDERS_TEST1")
    action.add_where(Allow, role="OTHER")

    assert apply_actions(parse_acl, FileReader(os.path.join(DIR, "files", "acl2.txt")), PrintWriter(), [action])
    assert action.result.role == "OTHER"
    assert action.result.write_access == "true"
    assert action.result.xxx == "yyy"


def test_get_2():
    action = Get()
    action.add_where(DB, id="S_ORDERS_TEST2")
    action.add_where(Allow, role="OTHER")

    assert apply_actions(parse_acl, FileReader(os.path.join(DIR, "files", "acl2.txt")), PrintWriter(), [action])
    assert action.result.role == "OTHER"
    assert action.result.write_access == "False"
    assert action.result.xxx == "zzz"


def test_get_3():
    action = Get()
    action.add_where(DB, id="S_ORDERS_TEST1")
    action.add_where(Allow, role="Admin")

    assert not apply_actions(parse_acl, FileReader(os.path.join(DIR, "files", "acl2.txt")), PrintWriter(), [action])


def test_get_4():
    action = Get()
    action.add_where(DB, id="S_ORDERS_FEED")
    action.add_where(Allow, role="Admin")

    assert apply_actions(parse_acl, FileReader(os.path.join(DIR, "files", "acl2.txt")), PrintWriter(), [action])
    assert action.result.write_access == "true"
    assert action.result.role == "Admin"


def test_get_5():
    action = Get()
    action.add_where(DB, id="S_ORDERS_FEED")
    action.add_where(Allow, role="OTHER")

    assert not apply_actions(parse_acl, FileReader(os.path.join(DIR, "files", "acl2.txt")), PrintWriter(), [action])


def test_get_all():
    action = GetAll()
    action.add_where(Role, name="Admin")
    action.add_where(User)

    pw = PrintWriter()

    apply_actions(parse_acl, FileReader(os.path.join(DIR, "files", "acl3.txt")), pw, [action])
