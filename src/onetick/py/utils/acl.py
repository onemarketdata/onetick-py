import getpass
import os

from locator_parser.io import PrintWriter, FileReader, FileWriter
from locator_parser.actions import Get, Add, Delete
from locator_parser.acl import DB, User, Role, parse_acl
from locator_parser.common import apply_actions

from .temp import TmpFile
from .types import default


def if_db_in_acl(acl, db_name):
    get_db = Get()
    get_db.add_where(DB, id=db_name)

    return apply_actions(parse_acl, FileReader(acl), PrintWriter(), [get_db], flush=True)


def add_user_to_acl(acl, user):
    add_user = Add(User(name=user))
    add_user.add_where(Role, name="Admin")

    assert apply_actions(parse_acl, FileReader(acl), FileWriter(acl), [add_user], flush=True)


def remove_user_from_acl(acl, user):
    remove_user = Delete()
    remove_user.add_where(Role, name="Admin")
    remove_user.add_where(User, name=user)

    assert apply_actions(parse_acl, FileReader(acl), FileWriter(acl), [remove_user], flush=True)


def if_user_in_acl(acl, user):
    get_user = Get()
    get_user.add_where(Role, name="Admin")
    get_user.add_where(User, name=user)

    return apply_actions(parse_acl, FileReader(acl), PrintWriter(), [get_user], flush=True)


def tmp_acl(clean_up=default):
    data = []
    data.append("<roles>")
    data.append('<role name="Admin">')
    data.append(f'<user name="{getpass.getuser()}" />')
    data.append('<user name="onetick" />')

    ext_users = os.environ.get("TEST_SESSION_ACL_USERS", None)

    if ext_users:
        for usr in ext_users.split(","):
            data.append(f'<user name="{usr}" />')

    data.append("</role>")
    data.append("</roles>")

    data.append("<databases>")
    data.append("</databases>")

    data.append("<event_processors>")
    data.append("")
    data.append('<ep ID="RELOAD_CONFIG">')
    data.append('<allow role="Admin" />')
    data.append("</ep>")
    data.append("")
    data.append('<ep ID="WRITE_TEXT">')
    data.append('<allow role="Admin" />')
    data.append("</ep>")
    data.append("")
    data.append('<ep ID="COMMAND_EXECUTE">')
    data.append('<allow role="Admin" />')
    data.append("</ep>")
    data.append("")
    data.append('<ep ID="READ_FROM_RAW">')
    data.append('<allow role="Admin" />')
    data.append("</ep>")
    data.append("")
    data.append('<ep ID="SAVE_SNAPSHOT">')
    data.append('<allow role="Admin" />')
    data.append("</ep>")
    data.append("</event_processors>")

    if os.getenv('OTP_WEBAPI_TEST_MODE'):
        tmp_file = TmpFile(name="acl.xml", clean_up=clean_up, force=True)
    else:
        tmp_file = TmpFile(suffix=".acl", clean_up=clean_up)

    with open(tmp_file, "w") as fout:
        fout.write("\n".join(data))

    return tmp_file
