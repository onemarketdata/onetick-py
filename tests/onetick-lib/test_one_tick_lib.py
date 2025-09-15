import pytest
import os

import onetick.lib as otl
import pyomd


CUR_DIR = os.path.dirname(os.path.abspath(__file__))
CFG = os.path.join(CUR_DIR, 'cfg', 'empty.cfg')


@pytest.fixture(scope='function', autouse=True)
def ot_cfg_env():
    ot_cfg = None
    if 'ONE_TICK_CONFIG' in os.environ:
        ot_cfg = os.environ['ONE_TICK_CONFIG']
    os.environ['ONE_TICK_CONFIG'] = CFG
    yield
    if ot_cfg is not None:
        os.environ['ONE_TICK_CONFIG'] = ot_cfg
    else:
        del os.environ['ONE_TICK_CONFIG']


def test_singleton():
    otlib1 = otl.OneTickLib()
    otlib2 = otl.OneTickLib()
    assert otlib1 is otlib2
    otl.OneTickLib().cleanup()


def test_two_inits():
    """
    test to check if singleton works: same instance after 2 initializations
    """
    otlib1 = otl.OneTickLib(None)
    otlib2 = otl.OneTickLib(None)
    otlib3 = otl.OneTickLib()
    assert otlib1 == otlib2
    assert otlib2 == otlib3
    otl.OneTickLib().cleanup()


def test_diff_args():
    """
    it is not possible to have initialized libraries with 2 or more args
    """
    otlib1 = otl.OneTickLib(CFG)
    with pytest.raises(Exception) as e:
        otlib2 = otl.OneTickLib(None)
        assert "instance.OneTickLib was already initialized" in str(e)
        otlib2.cleanup()
    otlib1.cleanup()


def test_default_init():
    """
    test singleton behavior with no initialization
    """
    assert otl.OneTickLib() == otl.OneTickLib()
    assert otl.OneTickLib() is not None
    otl.OneTickLib().cleanup()

    otlib = otl.OneTickLib(CFG)
    assert otlib == otl.OneTickLib()
    otlib.cleanup()


def test_destroy_singleton():
    """
    destroying must destroy hidden otq.instance.OneTickLib
    """
    otliblist = []
    size = 20

    for i in range(size):
        config = None if i % 2 else CFG
        otlib = otl.OneTickLib(config)
        otliblist.append(otlib)
        otlib.cleanup()

    for i in range(1, size):
        assert otliblist[i] == otliblist[i - 1]


class TestLogFileDestruction(object):
    """
    From the user perspective it checks that log file created by OneTick lib can be
    deleted according to BDS-54.
    Under the hood tt checks the OneTickLib.close_log_file_in_destructor() method
    """

    def test_method(self, tmpdir):
        # check that it does not exists
        tmp_path = os.path.join(tmpdir, 'log.file')
        assert not os.path.exists(tmp_path)

        # set log file
        lib = otl.OneTickLib(None)
        lib.set_log_file(tmp_path)
        assert os.path.exists(tmp_path)
        lib.cleanup()

        # try to remove and check that it can be removed
        os.remove(tmp_path)
        assert not os.path.exists(tmp_path)

    def test_constructor(self, tmpdir):
        tmp_path = os.path.join(tmpdir, 'log.file')
        assert not os.path.exists(tmp_path)

        # set log file
        lib = otl.OneTickLib(log_file=tmp_path)
        assert os.path.exists(tmp_path)
        lib.cleanup()

        # try to remove and check that it can be removed
        os.remove(tmp_path)
        assert not os.path.exists(tmp_path)


def test_proxy():
    otlib = otl.OneTickLib()

    # test dynamically added methods
    for attr in (
        'add_dynamic_context',
        'add_external_user_roles_mapping',
        'add_user_with_password',
        'always_send_credentials_during_authentication',
        'close_log_file_in_destructor',
        'get_authentication_token_for_user',
        'get_authentication_username',
        'get_build_number',
        'get_config',
        'get_one_tick_lib',
        'get_password_for_user',
        'get_username_for_authentication',
        'impersonate_client',
        'load_entitlement_modules',
        'log_locator_section_content',
        'log_process_info_on_start',
        'optimize_for_multiple_local_queries',
        'register_http_responce_handler',
        'register_web_server_port',
        'set_default_username_and_password',
        'set_is_tick_server_flag',
        'set_is_web_server_flag',
        'set_multi_user_mode_flag',
        'set_password',
        'set_rolling_log_filename_base',
        'set_username_for_authentication',
        'stop_client_impersonation',
        'use_default_credentials_during_authentication',
    ):
        assert hasattr(otlib, attr)
        assert callable(getattr(otlib, attr))

    # test static method
    assert isinstance(otlib.get_build_number(), int)
    assert isinstance(otl.OneTickLib.get_build_number(), int)

    # test not static method
    assert isinstance(otlib.get_config(), pyomd.OneTickConfig)
    with pytest.raises(TypeError):
        otl.OneTickLib.get_config()

    otlib.cleanup()
