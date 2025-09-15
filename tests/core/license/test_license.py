import os
import pytest
import shutil
import pathlib
import sys

import onetick.py as otp
from onetick.py.otq import otq

LICENSE_MASTER = "lic-srv-a.eng.sol.onetick.com:12345", "lic-srv-b.eng.sol.onetick.com:12345"

if otq.webapi:
    pytest.skip(allow_module_level=True)


@pytest.fixture(scope="module", autouse=True)
def clean():
    if "ONE_TICK_CONFIG" in os.environ:
        del os.environ["ONE_TICK_CONFIG"]
    yield
    if "ONE_TICK_CONFIG" in os.environ:
        del os.environ["ONE_TICK_CONFIG"]


@pytest.fixture(scope="function")
def platform(request, monkeypatch):
    """
    Changes platform using monkeypatcher in scope,
    that allows to return original value back even
    when something fails. Otherwise sys.platform would
    be changed and other code would use wrong not real
    sys.platform.
    """
    orig_platform = sys.platform
    monkeypatch.setattr(sys, "platform", request.param)
    monkeypatch.setattr(os, "pathsep", ";" if sys.platform == "win32" else ":")
    yield
    monkeypatch.setattr(sys, "platform", orig_platform)


class TestDefault:
    def test_default(self):
        with otp.Session() as s:
            assert s.license.dir == otp.configuration.config.default_license_dir
            assert s.license.file == otp.configuration.config.default_license_file
            assert s.config.license is s.license
            assert isinstance(s.config.license, otp.license.Default)

            data = otp.Ticks(dict(x=[1, 2, 3]))

            df = otp.run(data)

            assert len(df) == 3

    @pytest.mark.xfail(sys.platform != "win32", reason="default windows license path does not exist")
    def test_windows(self, monkeypatch):
        """ check default path on windows """
        for option in otp.config.get_changeable_config_options():
            monkeypatch.setattr(otp.config, option, otp.config.default)

        assert "C:\\" in otp.configuration.config.default_license_dir
        assert "C:\\" in otp.configuration.config.default_license_file

        license_ = otp.license.Default()

        assert license_.dir == otp.configuration.config.default_license_dir
        assert license_.file == otp.configuration.config.default_license_file

    @pytest.mark.xfail(sys.platform != "linux", reason="default linux license path does not exist")
    def test_linux(self, monkeypatch):
        """ check default path on linux """
        for option in otp.config.get_changeable_config_options():
            monkeypatch.setattr(otp.config, option, otp.config.default)

        assert sys.platform == "linux"

        assert otp.configuration.config.default_license_dir == '/license'
        assert otp.configuration.config.default_license_file == '/license/license.dat'

        license_ = otp.license.Default()

        assert license_.dir == otp.configuration.config.default_license_dir
        assert license_.file == otp.configuration.config.default_license_file

        monkeypatch.setenv('OTP_DEFAULT_LICENSE_DIR', '/home')
        assert otp.config.default_license_dir == '/home'

        license_ = otp.license.Default()
        assert license_.dir == '/home'
        assert license_.file == '/license/license.dat'


class TestCustom:
    @pytest.fixture(scope="function")
    def valid_license_dir(self, cur_dir):
        parent_dir = otp.utils.TmpDir()
        lic_dir = os.path.join(parent_dir, "license_dir")
        shutil.copytree(otp.configuration.config.default_license_dir, lic_dir)
        yield parent_dir

    @pytest.fixture(scope="function")
    def valid_license_file(self, valid_license_dir):
        lic_file = os.path.join(valid_license_dir, "license.dat")
        shutil.copy(otp.configuration.config.default_license_file, lic_file)
        yield lic_file

    @pytest.fixture(scope="function")
    def invalid_license_file(self, valid_license_dir):
        lic_file = os.path.join(valid_license_dir, "license.dat")
        pathlib.Path(lic_file).touch()
        yield lic_file

    def test_valid(self, valid_license_dir, valid_license_file):
        """
         Tests valid license: copy existing valid license and pass it to the config
        """
        custom_license = otp.license.Custom(valid_license_file, valid_license_dir)
        config = otp.Config(license=custom_license)
        with otp.Session(config):
            data = otp.Ticks(dict(x=[1, 2, 3]))

            df = otp.run(data)

            assert len(df) == 3

    @pytest.mark.skip(
        reason="BDS-91. When OneTickLib init fails, that it is"
        "impossible to re-init it later in the same process."
        "It means that we even can't run this test as xfail,"
        "because it would affect other tests"
    )
    def test_invalid(self, valid_license_dir, invalid_license_file):
        """ Test validates passing invalid license file """
        custom_license = otp.license.Custom(invalid_license_file, valid_license_dir)
        config = otp.Config(license=custom_license)

        with pytest.raises(Exception):
            otp.Session(config)

    def test_not_found(self, cur_dir):
        """ Test validates the files existing validation """
        with pytest.raises(FileNotFoundError):
            otp.Session(config=otp.Config(license=otp.license.Custom(cur_dir + "license.dat", "blabla")))

        with pytest.raises(FileNotFoundError):
            otp.Session(config=otp.Config(license=otp.license.Custom(cur_dir + "license.dat", cur_dir)))

    def test_param_none_file(self):
        """ check passing None as a file """
        with pytest.raises(ValueError):
            otp.license.Custom(None, "blabla")

    def test_param_none_dir(self, valid_license_file):
        """ check passing None as a directory """
        custom_license = otp.license.Custom(valid_license_file, None)
        config = otp.Config(license=custom_license)

        with otp.Session(config):
            data = otp.Ticks(dict(x=[1, 2, 3]))

            df = otp.run(data)
            assert len(df) == 3

    def test_param_default(self, valid_license_file):
        """ check default dir value (None) """
        custom_license = otp.license.Custom(valid_license_file)
        config = otp.Config(license=custom_license)

        with otp.Session(config):
            data = otp.Ticks(dict(x=[1, 2, 3]))

            df = otp.run(data)
            assert len(df) == 3


class TestServer:
    def test_valid_server(self):
        """ Test that everything works with valid server """
        server_license = otp.license.Server(LICENSE_MASTER)

        config = otp.Config(license=server_license)

        with otp.Session(config):
            data = otp.Ticks(dict(x=[1, 2, 3]))

            df = otp.run(data)
            assert len(df) == 3
        # with

    def test_invalid_server(self):
        """ Test invalid server address """
        with pytest.raises(Exception):
            otp.license.Server(["some-server:12345"])

    @pytest.fixture
    def onetick_path(self):
        if sys.platform == "win32":
            separator = ";"
        elif sys.platform == "linux" or sys.platform == "darwin":
            separator = ":"
        omd_prefix = str(os.path.join("one_market_data", "one_tick", "bin"))
        omd_dist_path = None
        for value in os.environ.get("PYTHONPATH", "").split(separator):
            if omd_prefix in value:
                omd_dist_path = value
                break
        if omd_dist_path is None:
            omd_dist_path = os.environ.get('MAIN_ONE_TICK_DIR')

        yield omd_dist_path

    class PythonPathScope:
        def __init__(self, new_pythonpath, new_main_one_tick_dir_path=None):
            self._orig_pythonpath = None
            self._orig_main_one_tick_dir_path = None
            self._new_pythonpath = new_pythonpath
            self._new_main_one_tick_dir_path = new_main_one_tick_dir_path

        def __enter__(self):
            self._orig_pythonpath = os.environ.get("PYTHONPATH", '')
            os.environ["PYTHONPATH"] = self._new_pythonpath
            if self._new_main_one_tick_dir_path is not None:
                self._orig_main_one_tick_dir_path = os.environ.get("MAIN_ONE_TICK_DIR", "")
                os.environ["MAIN_ONE_TICK_DIR"] = self._new_main_one_tick_dir_path

        def __exit__(self, exc_type, exc_val, exc_tb):
            os.environ["PYTHONPATH"] = self._orig_pythonpath
            if self._orig_main_one_tick_dir_path is not None:
                os.environ["MAIN_ONE_TICK_DIR"] = self._orig_main_one_tick_dir_path

    @pytest.mark.parametrize(
        "paths", [(["a", "b", "ONETICK", "c"]), (["ONETICK", "a", "b"]), (["a", "b", "ONETICK"]), (["ONETICK"])]
    )
    @pytest.mark.parametrize("platform", [("linux")], indirect=True)
    def test_omd_path_linux(self, paths, onetick_path, platform):
        """" check that path could be found on linux """
        paths = [onetick_path if x == "ONETICK" else x for x in paths]
        new_pp = ":".join(paths)

        with TestServer.PythonPathScope(new_pp):
            server_license = otp.license.Server(LICENSE_MASTER)
            config = otp.Config(license=server_license)
            with otp.Session(config):
                # checking session initialization
                pass

    @pytest.mark.parametrize("platform", [("win32")], indirect=True)
    def test_omd_path_windows(self, onetick_path, platform):
        """" check that path could be found on windows """
        new_pp = ";".join(["a", "b", onetick_path, "c"])

        with TestServer.PythonPathScope(new_pp):
            server_license = otp.license.Server(LICENSE_MASTER)
            config = otp.Config(license=server_license)
            with otp.Session(config):
                # checking session initialization
                pass

    @pytest.mark.parametrize("platform", [("darwin")], indirect=True)
    def test_omd_path_darwin(self, onetick_path, platform):
        """ check that path could be found on darwin """
        new_pp = ":".join(["a", "b", onetick_path, "c"])

        with TestServer.PythonPathScope(new_pp):
            server_license = otp.license.Server(LICENSE_MASTER)
            config = otp.Config(license=server_license)
            with otp.Session(config):
                # checking session initialization
                pass

    @pytest.mark.parametrize("platform", [("win32")], indirect=True)
    def test_omd_path_not_found(self, platform):
        """" check case where path to onetick distribution can't be found """
        new_pp = ";".join(["a", "b", "c"])

        with TestServer.PythonPathScope(new_pp, new_main_one_tick_dir_path=''):
            with pytest.raises(FileNotFoundError):
                otp.license.Server(LICENSE_MASTER)

    def test_with_env_variable(self):
        """
        check that ONE_TICK_CONFIG env variable does not
        change value
        """
        os.environ["ONE_TICK_CONFIG"] = "blabla"

        server_license = otp.license.Server(LICENSE_MASTER)
        assert os.environ["ONE_TICK_CONFIG"] == "blabla"
        config = otp.Config(license=server_license)
        assert os.environ["ONE_TICK_CONFIG"] == "blabla"

        del os.environ["ONE_TICK_CONFIG"]

        with otp.Session(config):
            data = otp.Ticks(dict(x=[1, 2, 3]))

            df = otp.run(data)
            assert len(df) == 3

    def test_with_custom_license(self):
        """ check that it is possible to reuse """
        server_license = otp.license.Server(LICENSE_MASTER)
        custom_license = otp.license.Custom(server_license.file, server_license.dir)

        with otp.Session(otp.Config(license=custom_license)):
            data = otp.Ticks(dict(x=[1, 2, 3]))

            df = otp.run(data)
            assert len(df) == 3
        # with

    @pytest.fixture
    def custom_file(self):
        file = otp.utils.TmpFile()
        yield file.path

    @pytest.fixture
    def custom_dir(self):
        dir_ = otp.utils.TmpDir()
        yield dir_.path

    def test_custom_file(self, custom_file):
        """
        Check that you could pass custom file location to update using server
        """
        server_license = otp.license.Server(LICENSE_MASTER, file=custom_file)

        assert server_license.file == custom_file

        with otp.Session(otp.Config(license=server_license)):
            # checking session initialization
            pass

    def test_custom_dir(self, custom_dir):
        """
        Check that you could pass custom directory location to update using server
        """
        server_license = otp.license.Server(LICENSE_MASTER, directory=custom_dir)

        assert server_license.dir == custom_dir

        with otp.Session(otp.Config(license=server_license)):
            # checking session initialization
            pass

    @pytest.mark.skip(
        reason="BDS-91. When OneTickLib init fails, that it is"
        "impossible to re-init it later in the same process."
        "It means that we even can't run this test as xfail,"
        "because it would affect other tests"
    )
    def test_not_reload_by_condition(self, custom_file):
        """
        Just created temporary file is not 2 days old, and
        it means license is not reloaded
        """
        server_license = otp.license.Server(LICENSE_MASTER, file=custom_file, reload=otp.Day(2))

        with pytest.raises(Exception):
            otp.Session(otp.Config(license=server_license))

    @pytest.fixture
    def patched_getmtime(self, request, monkeypatch):
        from datetime import datetime, timedelta

        orig_func = os.path.getmtime
        monkeypatch.setattr(os.path, "getmtime", lambda x: (datetime.now() - timedelta(days=request.param)).timestamp())
        yield
        monkeypatch.setattr(os.path, "getmtime", orig_func)

    @pytest.mark.parametrize(
        "patched_getmtime",
        [
            3,
            2.5,
            2,
            # skip, but should be xfail, need to fix after BDS-91
            pytest.param(1.8, marks=pytest.mark.skip),
        ],
        indirect=True,
    )
    def test_reload_by_condition(self, custom_file, patched_getmtime):
        server_license = otp.license.Server(LICENSE_MASTER, file=custom_file, reload=otp.Day(2))

        with otp.Session(otp.Config(license=server_license)):
            # checking session initialization
            pass

    def test_reload_always(self, custom_file):
        """
        0 means that always need to reload. 0 is the default value.
        """
        server_license = otp.license.Server(LICENSE_MASTER, file=custom_file, reload=0)

        with otp.Session(otp.Config(license=server_license)):
            # checking session initialization
            pass

    @pytest.mark.parametrize("reload", [None, "bab", 1, 3.4])
    def test_wrong_value(self, reload, custom_file):
        with pytest.raises(ValueError):
            otp.license.Server(LICENSE_MASTER, file=custom_file, reload=None)
