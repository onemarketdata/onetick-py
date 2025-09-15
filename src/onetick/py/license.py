import os
import abc
import subprocess
import pandas as pd

from datetime import datetime
import onetick.py as otp

from . import utils, configuration


class _LicenseBase(abc.ABC):
    def __init__(self, file, dir):
        self._dir = dir
        self._file = file

        if otp.__webapi__:
            return

        if self._file is None:
            raise ValueError("Something goes wrong, you pass None instead of path to license file")

        if self._dir is not None and not os.path.exists(self._dir):
            raise FileNotFoundError(f'"{self._dir}" license directory does not exist')
        if not os.path.exists(self._file):
            raise FileNotFoundError(f'"{self._file}" license file does not exist')

    @property
    def dir(self):
        return self._dir

    @property
    def file(self):
        return self._file


class Default(_LicenseBase):
    """
    Default license path
    """

    def __init__(self):
        default_file = configuration.config.default_license_file
        default_dir = configuration.config.default_license_dir
        if default_file is None and not otp.__webapi__:
            raise ValueError(
                "I can't understand the default license path :( Please, use "
                "onetick.py.license.Custom to specify a path to license."
            )

        super().__init__(default_file, default_dir)


class Remote(_LicenseBase):

    def __init__(self):
        # remote license doesn't use local files
        pass

    @property
    def dir(self):
        return None

    @property
    def file(self):
        return None


class Custom(_LicenseBase):
    """
    Custom license path
    """

    # it is not useless
    # pylint: disable=useless-parent-delegation
    def __init__(self, file, directory=None):
        """
        Parameters
        ----------
        file: str, os.PathLike
            Path to license file.
        directory: str, os.PathLike, optional
            Path to license directory. Default is None, that means it is not used.
        """
        super().__init__(file, directory)


class Server(_LicenseBase):
    """
    License that is based on license server. It generates new license file
    """

    def __init__(self, addresses=None, file=None, directory=None, reload=0):
        """
        Parameters
        ----------
        addresses: list of strings
            List of ip address or domain names with ports where a license server is running.
            For example: ['license.server.A:123', 'license.server.B:345']
        file: str or PathLike, optional
            License file path. Passed file path should exist. If file is not set or equal to None,
            then temporary file will be used (temporary file lives while an instance lives).
            NOTE: passed license file might be updated according to the 'reload' parameter.
        directory: str or PathLike, optional
            License directory path. Passed directory should exist. If directory is not set or
            equal to None, then temporary directory will be used.
        reload: 0 or offsets from onetick.py, ie onetick.py.Day, Hour, Month etc
            Indicator to reload license from license server. 0 means that reload on creation,
            offset means that if license file modification time more than offset, then it will
            be updated from the server.
        """
        self._generated = False

        if file is None:
            file = utils.TmpFile()
        else:
            if isinstance(reload, pd.tseries.offsets.Tick):
                modify = datetime.fromtimestamp(os.path.getmtime(file))
                current = datetime.now()
                diff_ms = (current - modify).total_seconds() * 1000000
                expire_ms = reload.nanos / 1000

                if diff_ms < expire_ms:
                    self._generated = True
            elif isinstance(reload, int):
                if reload == 0:
                    # always generate
                    self._generated = False
                else:
                    raise ValueError("You've passed not supported value to the 'reload' parameter")
            else:
                raise ValueError("You've passed not supported value to the 'reload' parameter")

        if directory is None:
            directory = utils.TmpDir()

        super().__init__(file, directory)

        self._servers = ",".join(addresses)

        if not self._generated:
            self.__generate()

    @property
    def servers(self):
        return self._servers

    def __generate(self):
        class EnvVarScope:
            def __init__(self, cfg):
                self._env = None
                self._cfg = cfg

            def __enter__(self):
                if "ONE_TICK_CONFIG" in os.environ:
                    self._env = os.environ["ONE_TICK_CONFIG"]
                os.environ["ONE_TICK_CONFIG"] = str(self._cfg)

            def __exit__(self, exc_type, exc_val, exc_tb):
                if self._env is None:
                    if "ONE_TICK_CONFIG" in os.environ:
                        del os.environ["ONE_TICK_CONFIG"]
                else:
                    os.environ["ONE_TICK_CONFIG"] = self._env

        tmp_locator = utils.tmp_locator(empty=True)
        tmp_config = utils.tmp_config(
            locator=tmp_locator, license_path=str(self.file), license_dir=self.dir, license_servers=self.servers
        )

        with EnvVarScope(tmp_config):
            self.__generate_license()

        self._generated = True

    def __generate_license(self):
        lic_generator = self.__license_getter_path(os.path.join(utils.omd_dist_path(), "one_tick", "bin"))

        subprocess.run(
            [lic_generator, "-features", "COLL,QUERY,LOAD"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            check=True,
        )

    @staticmethod
    def __license_getter_path(omd_dist_path):
        license_getter = os.path.join(omd_dist_path, "local_license_getter.exe")
        assert os.path.exists(license_getter), f"license getter is not found by {license_getter}"

        return license_getter
