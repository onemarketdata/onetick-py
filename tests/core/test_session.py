import os
import re
import sys
import time

import gc
import getpass
import pytest
import pandas as pd
from functools import reduce

from locator_parser.io import FileReader, PrintWriter
from locator_parser.actions import GetAll
import locator_parser.locator as _locator
import locator_parser.acl as _acl
from locator_parser.common import apply_actions
from onetick.py.utils.temp import WEBAPI_TEST_MODE_SHARED_CONFIG

DIR = os.path.dirname(os.path.abspath(__file__))

os.environ["USER"] = getpass.getuser()

if os.name == "nt":
    os.environ["LICENSE_DIR"] = os.path.join("C:\\", "OMD", "client_data", "config", "license_repository")
    os.environ["LICENSE_FILE"] = os.path.join("C:\\", "OMD", "client_data", "config", "license.dat")
else:
    os.environ["LICENSE_DIR"] = "/license"
    os.environ["LICENSE_FILE"] = os.path.join(os.environ["LICENSE_DIR"], "license.dat")


import onetick.py as otp  # noqa
from onetick.py.otq import otq, otli  # noqa


# ------------------------------------- Config tests -------------------------------------
@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='test mode use similar files for all tests')
def test_session_config_1():
    # we don't need to manage deleting generated files
    # because they will be automatically destroyed
    cfg = otp.Config()

    cfg_path = cfg.path
    acl_path = cfg.acl.path
    loc_path = cfg.locator.path

    assert os.path.exists(cfg_path)
    assert os.path.exists(acl_path)
    assert os.path.exists(loc_path)

    with otp.Session(cfg) as session:
        t_cfg_path = session.config.path
        t_acl_path = session.acl.path
        t_loc_path = session.locator.path

        assert t_cfg_path != cfg_path
        assert t_acl_path != acl_path
        assert t_loc_path != loc_path

    # passed externally, and therefore we control config files outside
    assert os.path.exists(cfg_path)
    assert os.path.exists(acl_path)
    assert os.path.exists(loc_path)


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='test mode use similar files for all tests')
def test_session_config_2():
    tmp_cfg = otp.utils.tmp_config()
    cfg = otp.Config()

    assert os.path.exists(tmp_cfg.path)
    assert os.path.exists(cfg.path)
    assert tmp_cfg.path != cfg.path  # copy

    with otp.Session(cfg) as session:
        assert session.config.path != tmp_cfg.path
        assert session.config.path != cfg.path

    assert os.path.exists(tmp_cfg.path)
    assert os.path.exists(cfg.path)


def test_session_config_3():
    cfg = otp.Config()

    assert os.path.exists(cfg.path)

    with otp.Session(cfg, copy=False) as session:
        t_acl_path = session.acl.path
        t_loc_path = session.locator.path

        assert cfg.path == session.config.path

        assert os.path.exists(session.config.path)
        assert os.path.exists(session.locator.path)
        assert os.path.exists(session.acl.path)

    assert os.path.exists(cfg.path)
    assert os.path.exists(t_acl_path)
    assert os.path.exists(t_loc_path)
    # because they belong to cfg
    assert cfg.acl.path == t_acl_path
    assert cfg.locator.path == t_loc_path


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='test mode use similar files for all tests')
def test_session_config_4():
    cfg = otp.Config(otq_path=["/surv/otqs", "/common/otqs"], csv_path=["/surv/csv"])

    with otp.Session(cfg) as session:
        assert cfg.path != session.config.path
        assert cfg.acl.path != session.acl.path
        assert cfg.locator.path != session.locator.path

        t_cfg = session.config
        t_acl = session.acl
        t_locator = session.locator

        assert otp.utils.get_config_param(session.config.path, "OTQ_FILE_PATH") == ",".join(
            ["/surv/otqs", "/common/otqs"]
        )
        assert otp.utils.get_config_param(session.config.path, "CSV_FILE_PATH") == ",".join(["/surv/csv"])

    assert os.path.exists(t_cfg.path)
    assert os.path.exists(t_acl.path)
    assert os.path.exists(t_locator.path)

    # cleanup
    objs = [t_cfg, t_acl, t_locator, cfg, cfg.acl, cfg.locator]
    [os.remove(o.path) for o in objs]

    for obj in objs:
        assert not os.path.exists(obj.path)


def test_session_config_5():
    loc = otp.Locator()
    acl = otp.session.ACL()

    config = otp.Config(acl=acl, locator=loc, copy=False)

    assert loc.path == config.locator.path
    assert acl.path == config.acl.path

    with otp.Session(config, copy=False) as session:
        assert config.path == session.config.path
        assert config.acl.path == session.acl.path
        assert config.locator.path == session.locator.path

        assert os.path.exists(config.path)
        assert os.path.exists(config.acl.path)
        assert os.path.exists(config.locator.path)

    assert os.path.exists(config.path)
    assert os.path.exists(config.acl.path)
    assert os.path.exists(config.locator.path)


def test_session_config_6():
    acl = otp.utils.tmp_acl()

    config = otp.Config(acl=acl, copy=False)

    assert acl.path == config.acl.path
    assert os.path.exists(config.locator.path)

    with otp.Session(config, copy=False) as session:
        assert config.path == session.config.path
        assert config.acl.path == session.acl.path
        assert config.locator.path == session.locator.path

        assert os.path.exists(config.path)
        assert os.path.exists(config.acl.path)
        assert os.path.exists(config.locator.path)

    assert os.path.exists(config.path)
    assert os.path.exists(config.acl.path)
    assert os.path.exists(config.locator.path)

    assert config.path
    assert config.locator.path


def test_session_config_7():
    config = otp.Config(
        variables=dict(
            STRING_PARAM="some_string_param",
            INT_PARAM=5,
            LIST_PARAM=["element_1", 2, "element_3"],
        ),
        copy=False,
    )

    with otp.Session(config, copy=False) as session:
        with open(session.config.path) as config_file:
            config_string = config_file.read()

    assert 'STRING_PARAM="some_string_param"' in config_string
    assert 'INT_PARAM="5"' in config_string
    assert 'LIST_PARAM="element_1,2,element_3"' in config_string


def test_session_config_8():
    with pytest.raises(ValueError):
        otp.Config(
            variables=dict(
                OTQ_FILE_PATH=["/tmp"]
            ),
            copy=False,
        )


# ------------------------------------- Session tests -------------------------------------


def test_session_1():
    with otp.Session() as session:
        session.use(otp.DB('TAQ_NBBO'))
        session.locator.reload()


@pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason="Skip for WebAPI as OneTickLib ")
def test_session_2():
    session = otp.Session()

    session.locator.reload()
    session.use(otp.DB('TAQ_NBBO'))
    session.locator.reload()
    session.close()

    with pytest.raises(AttributeError):
        session.locator.reload()
    with pytest.raises(Exception):
        otp.utils.reload_config()


def test_stubs():
    with otp.Session() as session:
        session.locator.reload()
        assert 'ABC' not in session.databases
        session.use_stub("ABC")
        assert 'ABC' in session.databases


def test_two_sessions_1():
    locator = otp.utils.tmp_locator()
    config = otp.utils.tmp_config(locator.path)

    session = otp.Session(config.path)

    with pytest.raises(Exception):
        # it is not allowed to have two sessions simultaneously
        otp.Session(config.path)

    session.close()


def test_two_sessions_2():
    session = otp.Session()

    with pytest.raises(Exception):
        # it is not allowed to have two sessions simultaneously
        otp.Session()

    session.close()


def test_two_sessions_3():
    locator = otp.utils.tmp_locator()
    config = otp.utils.tmp_config(locator.path)

    with otp.Session(config.path):
        with pytest.raises(Exception):
            # it is not allowed to have two sessions simultaneously
            otp.Session(config.path)


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='test mode use similar files for all tests')
def test_session_files_1():
    """
    When we pass config files externally then should
    be saved even after session destruction.
    All config files should be copeid.
    """
    locator = otp.utils.tmp_locator()
    config = otp.utils.tmp_config(locator.path)

    assert os.path.exists(locator.path)
    assert os.path.exists(config.path)

    # ------------>
    session = otp.Session(config.path)

    assert os.path.exists(locator.path)
    assert os.path.exists(config.path)

    # make sure that locator is copied
    assert locator.path != session.locator.path
    assert config.path != session.config.path

    assert os.path.exists(session.locator.path)
    assert os.path.exists(session.acl.path)
    assert os.path.exists(session.config.path)

    assert session.locator.path
    assert session.acl.path
    assert session.config.path

    session.close()
    # <------------

    assert os.path.exists(locator.path)
    assert os.path.exists(config.path)


def test_session_files_2():
    """
    Check that by default session generates files
    and they are destroyed after session
    """
    session = otp.Session()

    assert session.locator.path
    assert session.acl.path
    assert session.config.path

    assert os.path.exists(session.locator.path)
    assert os.path.exists(session.acl.path)
    assert os.path.exists(session.config.path)

    session.close()


def test_session_files_clean_up_1():
    """
    Check when clean_up=False for generated files
    """
    locator = None
    acl = None
    config = None

    try:
        session = otp.Session(clean_up=False)

        locator = session.locator
        acl = session.acl
        config = session.config

        assert os.path.exists(locator.path)
        assert os.path.exists(acl.path)
        assert os.path.exists(config.path)

        session.close()

        assert os.path.exists(locator.path)
        assert os.path.exists(acl.path)
        assert os.path.exists(config.path)
    finally:
        os.remove(locator.file)
        os.remove(acl.file)
        os.remove(config.file)


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='test mode use similar files for all tests')
def test_session_files_clean_up_2():
    """
    Check when clean_up=False for externally passed configs
    """
    t_locator = None
    t_acl = None
    t_config = None

    try:
        locator = otp.utils.tmp_locator()
        config = otp.utils.tmp_config(locator.path)

        # ----------->
        session = otp.Session(config.path, clean_up=False)

        t_locator = session.locator
        t_acl = session.acl
        t_config = session.config

        assert os.path.exists(t_locator.path)
        assert os.path.exists(t_acl.path)
        assert os.path.exists(t_config.path)

        assert locator.path != session.locator.path
        assert config.path != session.acl.path

        session.close()
        # <----------

        assert os.path.exists(locator.path)
        assert os.path.exists(config.path)
        assert os.path.exists(t_locator.path)
        assert os.path.exists(t_acl.path)
        assert os.path.exists(t_config.path)
    finally:
        os.remove(t_locator.path)
        os.remove(t_acl.path)
        os.remove(t_config.path)


def test_session_files_copy_1():
    """
    Check that flag copy=False do not copy externally passed files
    """

    locator = None
    config = None

    locator = otp.utils.tmp_locator()
    config = otp.utils.tmp_config(locator.path)

    # ------------>
    session = otp.Session(config.path, copy=False)

    t_locator_path = session.locator.path
    t_acl_path = session.acl.path
    t_config_path = session.config.path

    assert t_locator_path == locator.path
    assert t_config_path == config.path

    assert os.path.exists(t_locator_path)
    assert os.path.exists(t_acl_path)
    assert os.path.exists(t_config_path)

    session.close()
    # <------------

    assert os.path.exists(t_locator_path)
    assert os.path.exists(t_acl_path)  # since it is generated
    assert os.path.exists(t_config_path)


def test_session_files_copy_2():
    """
    Check that flag copy=False do not copy externally passed files
    In this test especailly for acl file
    """
    locator = otp.utils.tmp_locator()
    acl = otp.utils.tmp_acl()
    config = otp.utils.tmp_config(locator.path, acl.path)

    # ------------>
    session = otp.Session(config.path, copy=False)

    t_locator_path = session.locator.path
    t_acl_path = session.acl.path
    t_config_path = session.config.path

    assert t_locator_path == locator.path
    assert t_config_path == config.path
    assert t_acl_path == acl.path

    assert os.path.exists(t_locator_path)
    assert os.path.exists(t_acl_path)
    assert os.path.exists(t_config_path)

    session.close()
    # <------------

    assert os.path.exists(t_locator_path)
    assert os.path.exists(t_acl_path)
    assert os.path.exists(t_config_path)


def test_session_files_clean_up_copy():
    acl = None

    try:
        locator = otp.utils.tmp_locator()
        config = otp.utils.tmp_config(locator.path)

        # ---------->
        session = otp.Session(config.path, copy=False, clean_up=False)
        acl = session.acl

        t_locator_path = session.locator.path
        t_acl_path = session.acl.path
        t_config_path = session.config.path

        assert t_locator_path == locator.path
        assert t_config_path == config.path

        session.close()
        # <-----------

        assert os.path.exists(locator.path)
        assert os.path.exists(config.path)
        assert os.path.exists(t_acl_path)
    finally:
        if acl:
            os.remove(acl.path)


def test_session_exception_1():
    with otp.Session() as session:
        t_locator_path = session.locator.path
        t_config_path = session.config.path
        t_acl_path = session.acl.path

        assert os.path.exists(t_locator_path)
        assert os.path.exists(t_config_path)
        assert os.path.exists(t_acl_path)


def test_session_exception_2():
    locator = otp.utils.tmp_locator()
    config = otp.utils.tmp_config(locator.path)

    with otp.Session(config.path, copy=False) as session:
        t_locator_path = session.locator.path
        t_config_path = session.config.path
        t_acl_path = session.acl.path

        assert t_locator_path == locator.path
        assert t_config_path == config.path

        assert os.path.exists(locator.path)
        assert os.path.exists(config.path)
        assert os.path.exists(t_acl_path)


def test_two_sessions_4():
    with otp.Session():
        # it is not allowed to have two sessions simultaneously
        with pytest.raises(Exception):
            otp.Session()


@pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason="Skip for WebAPI as OneTickLib ")
def test_with_existing_config_1():
    old_locator_var = None
    if "DEFAULT_LOCATOR" in os.environ:
        old_locator_var = os.environ["DEFAULT_LOCATOR"]

    os.environ["DEFAULT_LOCATOR"] = os.path.join(DIR, "cfg", "locator.default")
    try:
        session = otp.Session(os.path.join(DIR, "cfg", "onetick.cfg"))

        # existing config does not have COMMON database
        assert "COMMON" not in otp.databases()

        session.use_stub("COMMON")

        assert "COMMON" in otp.databases()

        session.close()
    finally:
        if old_locator_var:
            os.environ["DEFAULT_LOCATOR"] = old_locator_var
        else:
            del os.environ["DEFAULT_LOCATOR"]


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason="in WebAPI session with config is not supported")
def test_with_existing_config_2():
    old_locator_var = None
    if "DEFAULT_LOCATOR" in os.environ:
        old_locator_var = os.environ["DEFAULT_LOCATOR"]

    os.environ["DEFAULT_LOCATOR"] = os.path.join(DIR, "cfg", "locator.default")
    try:
        with otp.Session(os.path.join(DIR, "cfg", "onetick.cfg")) as session:
            # existing config does not have COMMON database
            assert "COMMON" not in session.databases

            session.use_stub("COMMON")
            assert "COMMON" in session.databases

            session.close()
    finally:
        if old_locator_var:
            os.environ["DEFAULT_LOCATOR"] = old_locator_var
        else:
            del os.environ["DEFAULT_LOCATOR"]


def test_config_obj_1():
    session = otp.Session()

    ticks = otp.Ticks({"x": [1, 2, 3]})

    with pytest.raises(Exception):
        # this is query is not in the otq_path
        ticks.apply(otp.query("update2.otq::update"))

    session.close()


@pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False),
                    reason="WebAPI cannot use remote otqs that doesn't exist on the server")
def test_config_obj_2():
    dir_name = os.path.dirname(os.path.abspath(__file__))

    session = otp.Session(otp.Config(otq_path=[os.path.join(dir_name, "otqs")]))

    ticks = otp.Ticks({"x": [1, 2, 3]})

    assert otp.run(ticks)[["x"]].equals(pd.DataFrame({"x": [1, 2, 3]}))

    ticks = ticks.apply(otp.query("update2.otq::update"))

    assert otp.run(ticks)[["x"]].equals(pd.DataFrame({"x": [2, 4, 6]}))

    session.close()


def test_session_direct_access():
    session = otp.Session()

    assert os.path.exists(session.config.path)

    assert os.path.exists(session.config.acl.path)
    assert session.config.acl is session.acl

    assert os.path.exists(session.config.locator.path)
    assert session.config.locator is session.locator

    session.close()


def test_session_clean_up():
    session = otp.Session()

    session.use(otp.DB('TAQ_NBBO'))

    with pytest.warns(Warning):
        # it complains, because there would be two records for the same database
        session.use(otp.DB('TAQ_NBBO'))

    # --------------

    for _ in range(3):
        session.acl.cleanup()
        session.locator.cleanup()

        session.use(otp.DB('TAQ_NBBO'))

    for _ in range(3):
        session.config.cleanup()

        session.use(otp.DB('TAQ_NBBO'))

    session.close()


def test_external_db():
    data = otp.Ticks({"X": [1, 2, 3, 4], "Y": [4, 5, 6, 7]})

    db = otp.DB("S_ORDERS_FIX")
    db.add(data)

    with otp.Session() as session:
        session.use(db)

        res = otp.DataSource(db)

        assert len(otp.run(res)) == 4

    with otp.Session() as session:
        session.use(db)

        res = otp.DataSource(db, schema={'X': int, 'Y': int})
        res, _ = res[res.X >= 2]

        assert len(otp.run(res)) == 3


@pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason="WebAPI has no otq.OneTickLib")
def test_session_with_native_lib():
    tmp_file = otp.utils.tmp_config()

    lib = otq.OneTickLib(tmp_file.path)
    with pytest.raises(Exception):
        s = otp.Session()
        s.close()
    del lib
    gc.collect()


def test_session_with_wrapped_lib():
    tmp_file = otp.utils.tmp_config()

    lib = otli.OneTickLib(tmp_file.path)
    s = otp.Session()
    s.close()
    lib.cleanup()


def test_session_instance_1():
    session = otp.Session()
    session_get = otp.Session._instance
    assert session_get is session
    session.close()


def test_get_session_instance_2():
    session = otp.Session._instance
    assert session is None


def test_session_databases_prop():
    with otp.Session() as session:
        assert len(session.databases) == len(session.locator.databases)
        assert set(session.databases) == set(session.locator.databases)


class TestACL:
    @pytest.fixture(scope="class", autouse=True)
    def session(self):
        s = otp.Session()

        yield s

        s.close()

    def in_writer(self, line, writer):
        return reduce(lambda x, y: x | y, map(lambda x: line in x, writer.lines))

    def get_all(self, type, acl):
        action = GetAll()
        action.add_where(type)

        writer = PrintWriter()

        apply_actions(_acl.parse_acl, FileReader(acl.path), writer, [action])

        return writer

    @pytest.mark.parametrize("value", [None, 3, -7.5, "dsds"])
    def test_add_wrong(self, value):
        """ add entity of unsupported type """
        acl = otp.session.ACL()

        with pytest.raises(TypeError):
            acl.add(value)

    @pytest.mark.parametrize("value", [None, 3, -7.5, "dsds"])
    def test_remove_wrong(self, value):
        """ remove entity of unsupported type """
        acl = otp.session.ACL()

        with pytest.raises(TypeError):
            acl.remove(value)

    def test_add_db(self):
        """ add DB to an ACL using session.ACL """
        acl = otp.session.ACL()

        # empty
        assert not self.in_writer('<DB id="TEST_ACL"', self.get_all(_acl.DB, acl))

        # add DB and validate
        acl.add(otp.DB("TEST_ACL"))
        assert self.in_writer('<DB id="TEST_ACL"', self.get_all(_acl.DB, acl))

    def test_remove_db(self):
        """ remove DB from an ACL """
        db = otp.DB("TEST_ACL")

        acl = otp.session.ACL()
        # add
        acl.add(db)
        assert self.in_writer('<DB id="TEST_ACL"', self.get_all(_acl.DB, acl))

        # remove
        acl.remove(db)
        assert not self.in_writer('<DB id="TEST_ACL"', self.get_all(_acl.DB, acl))

    def test_remove_db_twice(self):
        """ test checks that remove database twice works fine """
        db = otp.DB("TEST_ACL")

        acl = otp.session.ACL()

        acl.add(db)
        acl.remove(db)
        with pytest.raises(ValueError):
            acl.remove(db)

    def test_add_user(self):
        """ add session.ACL.User to an ACL """

        acl = otp.session.ACL()

        # empty
        assert not self.in_writer('<USER name="test_user"', self.get_all(_acl.User, acl))

        # add
        acl.add(otp.session.ACL.User("test_user"))
        assert self.in_writer('<USER name="test_user"', self.get_all(_acl.User, acl))

    def test_remove_user(self):
        """ check removing user from an ACL """
        user = otp.session.ACL.User("test_user")

        acl = otp.session.ACL()
        # add
        acl.add(user)
        assert self.in_writer('<USER name="test_user"', self.get_all(_acl.User, acl))

        # remove
        acl.remove(user)
        assert not self.in_writer('<USER name="test_user"', self.get_all(_acl.User, acl))

    def test_remove_user_twice(self):
        """ check logic when remove twice the same user """
        user = otp.session.ACL.User("test_user")

        acl = otp.session.ACL()

        acl.add(user)
        acl.remove(user)
        with pytest.raises(otp.session.EntityOperationFailed):
            acl.remove(user)
        assert len(acl.databases) == 0

    def test_roll_back_on_add(self):
        """ test roll back login on add """
        db = otp.DB("TEST_ACL")
        acl = otp.session.ACL()

        # validate empty
        assert len(acl.databases) == 0
        assert not self.in_writer('<DB id="TEST_ACL"', self.get_all(_acl.DB, acl))

        # mock reload
        def reload():
            raise ValueError()

        acl.reload = reload

        # try to add
        with pytest.raises(ValueError):
            acl.add(db)

        # validate invariant
        assert len(acl.databases) == 0
        assert not self.in_writer('<DB id="TEST_ACL"', self.get_all(_acl.DB, acl))

    def test_roll_back_on_remove(self):
        """ test roll back logic on remove """
        db = otp.DB("TEST_ACL")
        acl = otp.session.ACL()

        # add
        acl.add(db)
        assert self.in_writer('<DB id="TEST_ACL"', self.get_all(_acl.DB, acl))

        # mock reload
        def reload():
            raise ValueError

        acl.reload = reload

        # try to remove
        with pytest.raises(ValueError):
            acl.remove(db)

        # validate that record is still there
        assert self.in_writer('<DB id="TEST_ACL"', self.get_all(_acl.DB, acl))
        assert len(acl.databases) == 1

    @pytest.mark.parametrize("users", ["user1", "user_A,user_B", "", "A,B,C,D,E,F", None])
    def test_external_users(self, users, monkeypatch):
        """ Test that we have ability to set additional users externally """
        default_dashboard_user = ["onetick"]
        if users:
            monkeypatch.setenv("TEST_SESSION_ACL_USERS", users)

        acl = otp.session.ACL()

        expected = [getpass.getuser()]
        expected += users.split(",") if users else []
        expected += default_dashboard_user
        assert set(expected) == set(acl.users)

    @pytest.mark.xfail
    def test_command_execute(self):
        node = otq.CommandExecute(command="echo", command_args="kitty").tick_type("TEST")
        source = otp.core.source._Source(node=node, _symbols="DEMO_L1::")
        otp.run(source)


def test_get_users():
    with otp.Session() as session:
        assert "new_user" not in session.acl.users

        session.acl.add(otp.session.ACL.User("new_user"))

        assert "new_user" in session.acl.users
        assert "new_user2" not in session.acl.users

        session.acl.add(otp.session.ACL.User("new_user2"))

        assert "new_user" in session.acl.users
        assert "new_user2" in session.acl.users


class TestLocatorSingleSession:
    @pytest.fixture(scope="class", autouse=True)
    def session(self):
        s = otp.Session()

        yield

        s.close()

    def in_writer(self, line, writer):
        return reduce(lambda x, y: x | y, map(lambda x: line in x, writer.lines))

    def get_all(self, cls, acl):
        action = GetAll()

        if isinstance(cls, _locator.ServerLocation):
            action.add_where(_locator.TickServers)

        action.add_where(cls)

        writer = PrintWriter()

        apply_actions(_acl.parse_acl, FileReader(acl.path), writer, [action])

        return writer

    def test_add_wrong(self):
        """ adding entity with unsupported type """
        locator = otp.Locator()

        with pytest.raises(TypeError):
            locator.add(None)
            locator.add(3)
            locator.add(-7.5)
            locator.add("dsdds")

    def test_remove_wrong(self):
        """ removing entity with unsupported type """
        locator = otp.Locator()

        with pytest.raises(TypeError):
            locator.remove(None)
            locator.remove(3)
            locator.remove(-7.5)
            locator.add("dsd")

    def test_add_db(self):
        """ add DB to a locator """
        locator = otp.Locator()

        # empty
        assert not self.in_writer('<DB id="TEST_DB"', self.get_all(_locator.DB, locator))
        assert "TEST_DB" not in locator.databases

        # add
        locator.add(otp.DB("TEST_DB"))
        assert self.in_writer('<DB id="TEST_DB"', self.get_all(_locator.DB, locator))
        assert "TEST_DB" in locator.databases

    def test_remove_db(self):
        """ remove DB from a locator """
        db = otp.DB("TEST_DB")

        locator = otp.Locator()

        # add
        locator.add(db)
        assert self.in_writer('<DB id="TEST_DB"', self.get_all(_locator.DB, locator))
        assert "TEST_DB" in locator.databases

        # remove
        locator.remove(db)
        assert not self.in_writer('<DB id="TEST_DB"', self.get_all(_locator.DB, locator))
        assert "TEST_DB" not in locator.databases

    def test_remove_db_twice(self):
        """ check logic when remove DB twice """
        db = otp.DB("TEST_DB")

        locator = otp.Locator()

        locator.add(db)
        locator.remove(db)
        with pytest.raises(ValueError):
            locator.remove(db)
        assert "TEST_DB" not in locator.databases

    def test_add_ts(self):
        """ add remote tick server to a locator """
        locator = otp.Locator()

        remote_ts = otp.RemoteTS("some_server", "12345")

        # Remove
        assert not self.in_writer(
            '<LOCATION location="some_server:12345"', self.get_all(_locator.ServerLocation, locator)
        )
        assert str(remote_ts) not in locator.tick_servers

        # Add
        locator.add(remote_ts)
        assert self.in_writer('<LOCATION location="some_server:12345"', self.get_all(_locator.ServerLocation, locator))
        assert str(remote_ts) in locator.tick_servers

    def test_remove_ts(self):
        """ remove remote tick server to a locator """
        remote_ts = otp.RemoteTS("some_server", "12345")

        locator = otp.Locator()

        # Add
        locator.add(remote_ts)
        assert self.in_writer('<LOCATION location="some_server:12345"', self.get_all(_locator.ServerLocation, locator))
        assert str(remote_ts) in locator.tick_servers

        # Remove
        locator.remove(remote_ts)
        assert not self.in_writer(
            '<LOCATION location="some_server:12345"', self.get_all(_locator.ServerLocation, locator)
        )
        assert str(remote_ts) not in locator.tick_servers

    def test_remove_ts_twice(self):
        """ check logic when remove ts twice """
        remote_ts = otp.RemoteTS("some_server", "12345")

        locator = otp.Locator()

        locator.add(remote_ts)
        locator.remove(remote_ts)
        with pytest.raises(ValueError):
            locator.remove(remote_ts)
        assert str(remote_ts) not in locator.tick_servers

    def test_roll_back_on_add(self):
        """ test roll back login on add """
        db = otp.DB("TEST_ACL")
        locator = otp.Locator()

        # validate empty
        assert "TEST_ACL" not in locator.databases
        assert not self.in_writer('<DB id="TEST_ACL"', self.get_all(_locator.DB, locator))

        # mock reload
        def reload():
            raise ValueError()

        locator.reload = reload

        # try to add
        with pytest.raises(ValueError):
            locator.add(db)

        # validate invariant
        assert "TEST_ACL" not in locator.databases
        assert not self.in_writer('<DB id="TEST_ACL"', self.get_all(_locator.DB, locator))

    def test_roll_back_on_remove(self):
        """ test roll back logic on remove """
        db = otp.DB("TEST_ACL")
        locator = otp.Locator()

        # add
        locator.add(db)
        assert "TEST_ACL" in locator.databases
        assert self.in_writer('<DB id="TEST_ACL"', self.get_all(_locator.DB, locator))

        # mock reload
        def reload():
            raise ValueError

        locator.reload = reload

        # try to remove
        with pytest.raises(ValueError):
            locator.remove(db)

        # validate that record is still there
        assert self.in_writer('<DB id="TEST_ACL"', self.get_all(_locator.DB, locator))
        assert "TEST_ACL" in locator.databases

    def test_add_locator(self):
        """ add locator reference to a locator """
        locator = otp.Locator()
        yet_another_locator = otp.Locator()

        assert not self.in_writer(
            '<INCLUDE path="' + yet_another_locator.path + '"', self.get_all(_locator.Include, locator)
        )

        locator.add(yet_another_locator)
        assert self.in_writer(
            '<INCLUDE path="' + yet_another_locator.path + '"', self.get_all(_locator.Include, locator)
        )

    @pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason="Skip for WebAPI tests, we use only one locator")
    def test_add_two_locators(self):
        """ add locator references to a locator """
        locator = otp.Locator()
        yet_another_locator = otp.Locator()
        yet_another_locator2 = otp.Locator()

        assert not self.in_writer(
            '<INCLUDE path="' + yet_another_locator.path + '"', self.get_all(_locator.Include, locator)
        )
        assert not self.in_writer(
            '<INCLUDE path="' + yet_another_locator2.path + '"', self.get_all(_locator.Include, locator)
        )

        locator.add(yet_another_locator)
        assert self.in_writer(
            '<INCLUDE path="' + yet_another_locator.path + '"', self.get_all(_locator.Include, locator)
        )
        assert not self.in_writer(
            '<INCLUDE path="' + yet_another_locator2.path + '"', self.get_all(_locator.Include, locator)
        )

        locator.add(yet_another_locator2)
        assert self.in_writer(
            '<INCLUDE path="' + yet_another_locator.path + '"', self.get_all(_locator.Include, locator)
        )
        assert self.in_writer(
            '<INCLUDE path="' + yet_another_locator2.path + '"', self.get_all(_locator.Include, locator)
        )

    def test_remove_locator(self):
        """ remove locator reference from a locator """
        locator = otp.Locator()
        yet_another_locator = otp.Locator()

        assert not self.in_writer(
            '<INCLUDE path="' + yet_another_locator.path + '"', self.get_all(_locator.Include, locator)
        )

        locator.add(yet_another_locator)
        assert self.in_writer(
            '<INCLUDE path="' + yet_another_locator.path + '"', self.get_all(_locator.Include, locator)
        )

        locator.remove(yet_another_locator)
        assert not self.in_writer(
            '<INCLUDE path="' + yet_another_locator.path + '"', self.get_all(_locator.Include, locator)
        )

    def test_empty_locator(self):
        locator = otp.Locator()

        assert "DEMO_L1" in locator.databases
        assert "COMMON" in locator.databases

        yet_another_locator = otp.Locator(empty=True)

        assert len(yet_another_locator.databases) == 0
        assert "DEMO_L1" not in yet_another_locator.databases


class TestLocatorTwoSessions:
    def test_external(self):
        """
        Test validates that we can manage locator
        without session
        """

        locator = otp.Locator()

        assert "TEST_DB" not in locator
        locator.add(otp.DB("TEST_DB"))
        assert "TEST_DB" in locator

        with otp.Session(otp.Config(locator=locator)) as s:
            assert 'TEST_DB1' not in s.databases

        assert "TEST_DB1" not in locator
        locator.add(otp.DB("TEST_DB1"))
        assert "TEST_DB1" in locator

        with otp.Session(otp.Config(locator=locator)) as s:
            assert 'TEST_DB' in s.databases
            assert 'TEST_DB1' in s.databases


class TestOneTickConfigEnv:
    @pytest.fixture(scope="class", autouse=True)
    def session(self):
        if "ONE_TICK_CONFIG" in os.environ:
            del os.environ["ONE_TICK_CONFIG"]

        yield

        if "ONE_TICK_CONFIG" in os.environ:
            del os.environ["ONE_TICK_CONFIG"]

    def test_env_inside_1(self):
        """
        Validate that ONE_TICK_CONFIG is set inside the
        session. It is helpful for some OneTick tools,
        for example ascii_loader that works with ONE_TICK_CONFIG
        environment variable directly and does not have
        an option to point to a locator
        """
        if "ONE_TICK_CONFIG" in os.environ:
            del os.environ["ONE_TICK_CONFIG"]

        assert "ONE_TICK_CONFIG" not in os.environ

        with otp.Session() as s:
            assert "ONE_TICK_CONFIG" in os.environ
            assert os.environ["ONE_TICK_CONFIG"] == s.config.path

        assert "ONE_TICK_CONFIG" not in os.environ

    def test_env_inside_2(self):
        """
        Validate that override_env flag does not affect
        the behaviour
        """
        if "ONE_TICK_CONFIG" in os.environ:
            del os.environ["ONE_TICK_CONFIG"]

        assert "ONE_TICK_CONFIG" not in os.environ

        with otp.Session(override_env=True) as s:
            assert "ONE_TICK_CONFIG" in os.environ
            assert os.environ["ONE_TICK_CONFIG"] == s.config.path

        assert "ONE_TICK_CONFIG" not in os.environ

    def test_env_inside_3(self):
        """
        The same as the first one, but different type of scope
        """
        if "ONE_TICK_CONFIG" in os.environ:
            del os.environ["ONE_TICK_CONFIG"]

        assert "ONE_TICK_CONFIG" not in os.environ

        s = otp.Session()

        assert "ONE_TICK_CONFIG" in os.environ
        assert os.environ["ONE_TICK_CONFIG"] == s.config.path

        s.close()

        assert "ONE_TICK_CONFIG" not in os.environ

    @pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason="WebAPI and otp.Session are not fully compatible")
    @pytest.mark.filterwarnings("ignore:ONE_TICK_CONFIG")
    def test_env_outside_1(self):
        """
        Validate behaviour when ONE_TICK_CONFIG is
        set externally, in particular:
        if ONE_TICK_CONFIG is set externally, then
        inside the session it overriden with the
        local generated config
        """
        if "ONE_TICK_CONFIG" in os.environ:
            del os.environ["ONE_TICK_CONFIG"]

        assert "ONE_TICK_CONFIG" not in os.environ

        locator = otp.Locator()
        locator.add(otp.DB("TEST_DB"))
        config = otp.Config(locator=locator)
        os.environ["ONE_TICK_CONFIG"] = config.path
        assert "ONE_TICK_CONFIG" in os.environ
        assert os.environ["ONE_TICK_CONFIG"] == config.path

        with otp.Session() as s:
            assert 'TEST_DB' not in s.databases
            assert "TEST_DB" not in s.locator
            assert config.path != s.config.path
            assert locator.path != s.locator.path

            assert "ONE_TICK_CONFIG" in os.environ
            # is not overriden
            assert os.environ["ONE_TICK_CONFIG"] == config.path

        assert "ONE_TICK_CONFIG" in os.environ
        assert os.environ["ONE_TICK_CONFIG"] == config.path

    @pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason="WebAPI and otp.Session are not fully compatible")
    def test_env_outside_2(self):
        """
        Validate that override_env flag set to True
        overrides ONE_TICK_CONFIG env variable with
        the local config
        """
        if "ONE_TICK_CONFIG" in os.environ:
            del os.environ["ONE_TICK_CONFIG"]

        assert "ONE_TICK_CONFIG" not in os.environ

        locator = otp.Locator()
        locator.add(otp.DB("TEST_DB"))
        config = otp.Config(locator=locator)
        os.environ["ONE_TICK_CONFIG"] = config.path
        assert "ONE_TICK_CONFIG" in os.environ

        with otp.Session(override_env=True) as s:
            assert "TEST_DB" not in s.databases
            assert "TEST_DB" not in s.locator
            assert config.path != s.config.path

            assert "ONE_TICK_CONFIG" in os.environ
            # overriden
            assert os.environ["ONE_TICK_CONFIG"] == s.config.path

        assert "ONE_TICK_CONFIG" in os.environ
        assert os.environ["ONE_TICK_CONFIG"] == config.path

    @pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason="WebAPI and otp.Session are not fully compatible")
    @pytest.mark.filterwarnings("ignore:ONE_TICK_CONFIG")
    def test_env_outside_3(self):
        """
        Test validates the technique that allows
        to use external defined ONE_TICK_CONFIG
        """
        if "ONE_TICK_CONFIG" in os.environ:
            del os.environ["ONE_TICK_CONFIG"]

        assert "ONE_TICK_CONFIG" not in os.environ

        locator = otp.Locator()
        locator.add(otp.DB("TEST_DB"))
        config = otp.Config(locator=locator)
        os.environ["ONE_TICK_CONFIG"] = config.path
        assert "ONE_TICK_CONFIG" in os.environ

        with otp.Session(config=os.environ["ONE_TICK_CONFIG"], copy=False) as s:
            assert "TEST_DB" in s.databases
            assert "TEST_DB" in s.locator
            assert config.path == s.config.path

            assert "ONE_TICK_CONFIG" in os.environ
            # not overriden, but the same
            assert os.environ["ONE_TICK_CONFIG"] == s.config.path

        assert "ONE_TICK_CONFIG" in os.environ
        assert os.environ["ONE_TICK_CONFIG"] == config.path

    @pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason="WebAPI and otp.Session are not fully compatible")
    def test_env_outside_4(self):
        """
        Test validates the technique that allows
        to use external defined ONE_TICK_CONFIG
        and checks that override_env=True does
        not affect the logic
        """
        if "ONE_TICK_CONFIG" in os.environ:
            del os.environ["ONE_TICK_CONFIG"]

        assert "ONE_TICK_CONFIG" not in os.environ

        locator = otp.Locator()
        locator.add(otp.DB("TEST_DB"))
        config = otp.Config(locator=locator)
        os.environ["ONE_TICK_CONFIG"] = config.path
        assert "ONE_TICK_CONFIG" in os.environ

        with otp.Session(config=os.environ["ONE_TICK_CONFIG"], copy=False, override_env=True) as s:
            assert "TEST_DB" in s.databases
            assert "TEST_DB" in s.locator
            assert config.path == s.config.path

            assert "ONE_TICK_CONFIG" in os.environ
            # overriden, but the same
            assert os.environ["ONE_TICK_CONFIG"] == s.config.path

        assert "ONE_TICK_CONFIG" in os.environ
        assert os.environ["ONE_TICK_CONFIG"] == config.path

    @pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason="WebAPI and otp.Session are not fully compatible")
    def test_change_config(self):
        """
        Check that config might be changed for session
        """
        cfg = otp.Config()
        another_locator = otp.Locator()
        another_locator.add(otp.DB("TEST_DB"))
        another_cfg = otp.Config(locator=another_locator, copy=False)

        with otp.Session(cfg, copy=False, override_env=True) as s:
            assert os.environ["ONE_TICK_CONFIG"] == cfg.path
            assert s.config.path == cfg.path
            assert "TEST_DB" not in s.locator

            otp.run(otp.Tick(x=1))

            s.config = another_cfg
            assert "TEST_DB" in s.locator
            assert "TEST_DB" in s.databases

            otp.run(otp.Tick(x=2))


# --------------------------- #
# Check that config might be used
# in the fixture and modified in
# another fixture
@pytest.fixture
def some_session():
    s = otp.Session()
    yield s
    s.close()


@pytest.fixture
def another_session(some_session):
    some_session.config = otp.Config()
    yield some_session


def test_change_session_as_fixture(another_session):
    pass


# ------------------------ #


def test_two_sessions():
    """
    Validates that we can not have two active sessions simultaniously
    in one process
    """

    with otp.Session():
        with pytest.raises(otp.session.MultipleSessionsException):
            otp.Session()


# ----------------------- #


def test_with_dir_fixtures(cur_dir):
    otq_path = cur_dir + "otqs"
    csv_path = cur_dir + "csvs"

    with otp.Session(otp.Config(otq_path=[otq_path], csv_path=[csv_path])) as s:
        assert otp.utils.get_config_param(s.config.path, "OTQ_FILE_PATH") == otq_path
        assert otp.utils.get_config_param(s.config.path, "CSV_FILE_PATH") == csv_path


@pytest.mark.xfail(sys.platform == "win32", reason="database's directory is lowercase on windows")
def test_use_case_different_dbs():
    with otp.Session() as s:
        test_dbs_number = len(s.databases)
        s.use(otp.DB('A'))
        with pytest.warns(match="Database 'a' is already added to the Locator "
                                "and will not be rewritten with this command. "
                                "Notice that databases' names are case insensitive."):
            s.use(otp.DB('a'))
        assert len(s.databases) == test_dbs_number + 1


@pytest.mark.performance
def test_use_many_dbs():
    with otp.Session() as s:
        dbs = [otp.DB(f'A_{i}') for i in range(100)]
        start = time.time()
        for db in dbs:
            s.use(db)
        duration_consecutive = time.time() - start
    with otp.Session() as s:
        dbs = [otp.DB(f'A_{i}') for i in range(100)]
        start = time.time()
        s.use(*dbs)
        duration_batch = time.time() - start
    assert duration_batch < duration_consecutive
    better = duration_consecutive / duration_batch
    print(f'Better: {better} times')
    assert better > 1.5


class TestConfigParam:
    def test_include(self):
        tmp_cfg = otp.utils.tmp_config()
        with pytest.raises(AttributeError):
            otp.utils.get_config_param(tmp_cfg.path, 'SERVER_LOGGING_LEVEL')

        tmp_cfg_incl = otp.utils.TmpFile()
        with open(tmp_cfg_incl, 'w') as f:
            f.write('SHOW_CONFIG=false\n')
            f.write('SERVER_LOGGING_LEVEL=3\n')
        with open(tmp_cfg, 'a') as f:
            f.write('\nSHOW_CONFIG=true\n')
            f.write(f'INCLUDE "{tmp_cfg_incl.path}"\n')

        assert otp.utils.get_config_param(tmp_cfg.path, 'SHOW_CONFIG') == 'true'
        assert otp.utils.get_config_param(tmp_cfg.path, 'SERVER_LOGGING_LEVEL') == '3'

        otp.utils.modify_config_param(tmp_cfg.path, 'SHOW_CONFIG', 'false')
        assert otp.utils.get_config_param(tmp_cfg.path, 'SHOW_CONFIG') == 'false'
        assert otp.utils.get_config_param(tmp_cfg.path, 'SERVER_LOGGING_LEVEL') == '3'
        with open(tmp_cfg) as f:
            lines = f.readlines()
            assert f'INCLUDE "{tmp_cfg_incl.path}"\n' not in lines
            assert 'SHOW_CONFIG="false"\n' in lines
            assert 'SERVER_LOGGING_LEVEL=3\n' in lines

    def test_include_recursive(self):
        tmp_cfg = otp.utils.tmp_config()

        tmp_cfg_incl = otp.utils.TmpFile()
        with open(tmp_cfg_incl, 'w') as f:
            f.write('SERVER_LOGGING_LEVEL=3\n')
            f.write(f'INCLUDE "{tmp_cfg_incl.path}"\n')
        with open(tmp_cfg, 'a') as f:
            f.write(f'\nINCLUDE "{tmp_cfg_incl.path}"\n')

        # need to escape because windows paths have backslashes
        err_msg = re.escape(f"Path '{tmp_cfg_incl.path}' is included more than once")
        with pytest.raises(RecursionError, match=err_msg):
            _ = otp.utils.get_config_param(tmp_cfg.path, 'SERVER_LOGGING_LEVEL') == '3'

    def test_comment(self):
        tmp_cfg = otp.utils.tmp_config()

        with open(tmp_cfg, 'a') as f:
            f.write('\n# VAR="It is just a comment"\n')

        with pytest.raises(AttributeError):
            otp.utils.get_config_param(tmp_cfg.path, 'VAR')
        with pytest.raises(AttributeError):
            otp.utils.get_config_param(tmp_cfg.path, '# VAR')


@pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason="WebAPI do not support redirect_logs")
@pytest.mark.skipif(os.name == 'nt', reason='PY-873: Unstable on Windows')
@pytest.mark.parametrize('redirect_logs', (True, False))
def test_redirect_logs(redirect_logs, capfd):
    with otp.Session(redirect_logs=redirect_logs) as s:
        t = otp.Tick(A=1)
        t = t.logf('Hello from LOGF', 'INFO')
        otp.run(t)
        time.sleep(1)
        if redirect_logs:
            assert s._log_file is not None
            with open(s._log_file) as f:
                log_line = f.readlines()[-1]
        else:
            log_line = capfd.readouterr().err
        assert 'INFO' in log_line, log_line
        assert 'Hello from LOGF' in log_line, log_line


@pytest.mark.skipif(not otp.compatibility.is_supported_reload_locator_with_derived_db(),
                    reason='skipping old one because we cannot catch segfault')
def test_reload_locator_with_derived_database():
    """
    See tasks PY-388, BDS-334.
    Was fixed in update1_20231108120000.
    0032118: OneTick processes that refresh their locator may crash
             if they make use databases derived from the dbs in that locator
    """
    session = otp.Session()

    db_1 = otp.DB('DB')
    session.use(db_1)

    db_derived1 = otp.DB('DB//X')
    src = otp.Ticks({"X": [1]})

    # comment this to make it work OR move it after .use(SOME_DB) to make it work
    db_derived1.add(src, date=otp.config.default_start_time, tick_type="A", symbol="B")

    market_db = otp.db.DB("SOME_DB")
    # or comment this to make it work
    session.use(market_db)

    # segfault is here (comment this to make it work)
    session.close()


@pytest.mark.skipif(not os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='Tests only for WebAPI related sessions')
def test_session_with_webapi_test_mode():
    with otp.Session() as s:
        assert s
        assert s.config.path == WEBAPI_TEST_MODE_SHARED_CONFIG + '/onetick.cfg'
        assert WEBAPI_TEST_MODE_SHARED_CONFIG in s.config.locator.path
        assert WEBAPI_TEST_MODE_SHARED_CONFIG in s.config.acl.path

    with otp.Session(otp.Config()) as s:
        assert s
        assert s.config.path == WEBAPI_TEST_MODE_SHARED_CONFIG + '/onetick.cfg'
        assert WEBAPI_TEST_MODE_SHARED_CONFIG in s.config.locator.path
        assert WEBAPI_TEST_MODE_SHARED_CONFIG in s.config.acl.path


@pytest.mark.skipif(not os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='Tests only for WebAPI related sessions')
def test_shared_session_otq_files_path():
    with otp.Session():
        data = otp.Tick(X=1)
        path = data.to_otq()
        assert WEBAPI_TEST_MODE_SHARED_CONFIG not in path


class TestPerformanceMetrics:
    def write_log(self, data):
        tmp_log = otp.utils.TmpFile()
        with open(tmp_log, 'w') as log_file:
            log_file.writelines(data)

        return tmp_log

    @pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason="WebAPI do not support this test")
    def test_session_performance_metrics(self):
        metrics_types = {
            "user_time": float,
            "system_time": float,
            "elapsed_time": float,
            "virtual_memory": int,
            "virtual_memory_peak": int,
            "working_set": int,
            "working_set_peak": int,
            "disk_read": int,
            "disk_write": int,
        }

        cfg = otp.Config()
        with otp.Session(cfg, gather_performance_metrics=True) as session:
            data = otp.Tick(X=1)
            data.pause(2000)
            otp.run(data)

        metrics = session.performance_metrics
        assert set(metrics.keys()) == set(metrics_types.keys())
        assert all(['name' in metric and 'value' in metric and 'units' in metric for metric in metrics.values()])
        assert all([type(metric['value']) is metrics_types[key] for key, metric in metrics.items()])
        assert metrics['user_time']['value'] > 1.5

    def test_read_before_close(self):
        with pytest.raises(RuntimeError, match='before closing'):
            cfg = otp.Config()
            with otp.Session(cfg, gather_performance_metrics=True) as session:
                _ = session.performance_metrics

    def test_disabled_metrics_request(self):
        with pytest.raises(RuntimeError, match='gather_performance_metrics'):
            cfg = otp.Config()
            with otp.Session(cfg, gather_performance_metrics=False) as session:
                pass

            _ = session.performance_metrics

    def test_parser(self):
        tmp_log = self.write_log([
            '20250124085917 INFO: 24260 Performance Metrics FINAL User Time (s): 11.59 \n'
            '20250124085917 INFO: 24260 Performance Metrics FINAL System Time (s): 1.01\n'
            '20250124085917 INFO: 24260 Performance Metrics FINAL Elapsed Time: 11.66\n'
            '20250124085917 INFO: 24260 Performance Metrics FINAL Elapsed Time (s): 11.66\n'
            'Performance Metrics FINAL Virtual Memory (bytes): 1775906816\n'
            '20250124085917 INFO: 24260 Performance Metrics FINAL Virtual Memory Peak (bytes): 1963200512\n'
            '20250124085917 INFO: 24260 Performance Metrics FINAL string: 12345\n',
            '20250124085917 INFO: 24260 Performance Metrics FINAL Working Set (bytes): 229048320\n'
            '20250124085917 INFO: 24260 Performance Metrics Other Log Line\n',
            '20250124085917 INFO: 24260 Performance Metrics FINAL Log Line\n',
            '20250124085917 INFO: 24260 Performance Metrics FINAL Working Set Peak (bytes): 229048320\n'
            '20250124085917 INFO: 24260 Performance Metrics FINAL Disk Read (bytes): 0\n'
            '20250124085917 INFO: 24260 Performance Metrics FINAL Disk Write (bytes): 49152\n'
        ])

        parser = otp.session.PerformanceMetricsParser()
        parser.parse(tmp_log.path)
        assert parser.metrics == {
            'user_time': {'name': 'User Time', 'value': 11.59, 'units': 's'},
            'system_time': {'name': 'System Time', 'value': 1.01, 'units': 's'},
            'elapsed_time': {'name': 'Elapsed Time', 'value': 11.66, 'units': 's'},
            'virtual_memory': {'name': 'Virtual Memory', 'value': 1775906816, 'units': 'bytes'},
            'virtual_memory_peak': {'name': 'Virtual Memory Peak', 'value': 1963200512, 'units': 'bytes'},
            'working_set': {'name': 'Working Set', 'value': 229048320, 'units': 'bytes'},
            'working_set_peak': {'name': 'Working Set Peak', 'value': 229048320, 'units': 'bytes'},
            'disk_read': {'name': 'Disk Read', 'value': 0, 'units': 'bytes'},
            'disk_write': {'name': 'Disk Write', 'value': 49152, 'units': 'bytes'},
        }

    def test_parser_duplicate_metrics(self):
        tmp_log = self.write_log([
            '20250124085917 INFO: 24260 Performance Metrics FINAL User Time (s): 11.59 \n',
            '20250124085917 INFO: 24260 Performance Metrics FINAL Disk Write (bytes): 49152\n',
            '20250124085917 INFO: 24260 Performance Metrics FINAL User Time (s): 12.34\n',
        ])

        parser = otp.session.PerformanceMetricsParser()

        with pytest.raises(KeyError, match='user_time'):
            parser.parse(tmp_log.path)

    def test_parser_missing_fields(self):
        tmp_log = self.write_log([
            '20250124085917 INFO: 24260 Performance Metrics FINAL User Time (s): 11.59 \n',
            '20250124085917 INFO: 24260 Performance Metrics FINAL Disk Write (bytes): 49152\n',
            '20250124085917 INFO: 24260 Performance Metrics TEST Elapsed Time (s): 11.59\n',
            '20250124085917 INFO: 24260 Performance Metrics FINAL Elapsed Time (s): 11.59\n',
            '20250124085917 INFO: 24260 Performance Metrics FINAL Virtual Memory Peak (bytes): 34567\n',
        ])

        parser = otp.session.PerformanceMetricsParser()

        with pytest.raises(RuntimeError) as exc:
            parser.parse(tmp_log.path)

        assert set(key.strip() for key in str(exc.value).split(':')[1].split(',')) == {
            'disk_read', 'system_time', 'working_set', 'virtual_memory', 'working_set_peak',
        }

    def test_parser_unknown_metric(self):
        tmp_log = self.write_log([
            '20250124085917 INFO: 24260 Performance Metrics FINAL User Time (s): 11.59 \n',
            '20250124085917 INFO: 24260 Performance Metrics FINAL Disk Write (bytes): 49152\n',
            '20250124085917 INFO: 24260 Performance Metrics FINAL test string (bytes): 12345\n',
            '20250124085917 INFO: 24260 Performance Metrics FINAL Elapsed Time (s): 11.59\n',
        ])

        parser = otp.session.PerformanceMetricsParser()

        with pytest.raises(ValueError, match='Unexpected performance metric `test_string'):
            parser.parse(tmp_log.path)

    def test_parser_units_mismatch(self):
        tmp_log = self.write_log([
            '20250124085917 INFO: 24260 Performance Metrics FINAL Disk Write (kbytes): 1200\n',
        ])

        parser = otp.session.PerformanceMetricsParser()

        with pytest.raises(ValueError, match=r'metric `disk_write`: expected `bytes`, got `kbytes`'):
            parser.parse(tmp_log.path)

    def test_parser_incorrect_type(self):
        tmp_log = self.write_log([
            '20250124085917 INFO: 24260 Performance Metrics FINAL Disk Write (bytes): 12.34\n',
        ])

        parser = otp.session.PerformanceMetricsParser()

        with pytest.raises(ValueError, match=r'metric disk_write from `str` to `int`: "12.34"'):
            parser.parse(tmp_log.path)
