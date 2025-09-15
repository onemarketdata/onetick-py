import abc
import datetime
import errno
import getpass
import os
import shutil
import tempfile
import weakref
from collections import defaultdict
from typing import Dict, List
from .types import default

import coolname


WEBAPI_TEST_MODE_SHARED_CONFIG = os.getenv('WEBAPI_TEST_MODE_SHARED_CONFIG')
if os.getenv('OTP_WEBAPI_TEST_MODE') and not WEBAPI_TEST_MODE_SHARED_CONFIG:
    raise ValueError("WEBAPI_TEST_MODE_SHARED_CONFIG is not set, but it is required with OTP_WEBAPI_TEST_MODE.")


def default_clean_up(clean_up):
    from ..configuration import config
    if clean_up is default:
        return config.clean_up_tmp_files
    return clean_up


def get_logger(*args):
    from .. import log
    return log.get_logger(__name__, *args)


class CleanUpFinalizer:
    """
    Class that manages destruction of the object,
    setting up proper finalizer callback and clean_up boolean flag.
    """

    def __init__(self, clean_up=default, *args):
        clean_up = default_clean_up(clean_up)
        # we need to use reference type here so finalizer can get the latest clean_up value,
        # even if this value is changed after finalizer is created
        # (because weakref.finalize can't use object's fields)
        self._clean_up_ref = [clean_up]
        self._finalizer = weakref.finalize(self, self._cleanup, self._clean_up_ref, *args)

    @classmethod
    def _cleanup(cls, clean_up_ref, *args):
        raise NotImplementedError()

    @property
    def need_to_cleanup(self):
        return self._clean_up_ref[0]

    @need_to_cleanup.setter
    def need_to_cleanup(self, value):
        self._clean_up_ref[0] = value


class File(abc.ABC):
    def __init__(self, path):
        self._path = path

    @property
    def path(self):
        return self._path

    def __str__(self):
        return self._path

    def __fspath__(self):
        """
        If children inherit os.PathLike, then this method is required and
        makes files behave compatible with 'os' module
        """
        return self._path


def __name_generator():
    """
    Returns tuple with initialized name generator and
    the number of unique names this generator will produce
    before it starts to repeat itself.
    """
    def cool_gen():
        while True:
            yield coolname.generate_slug(2)
    return cool_gen(), coolname.get_combinations_count(2)


def mktemp(fun, dir, prefix='', suffix='', **kwargs):
    names, max_unique_values = __name_generator()
    for _ in range(max_unique_values):
        name = next(names)
        filename = os.path.join(dir, prefix + name + suffix)
        try:
            return fun(filename, **kwargs), filename
        except FileExistsError:
            continue
        except PermissionError:
            # This exception is thrown when a directory with the chosen name already exists on Windows
            if os.name == 'nt' and os.path.isdir(dir) and os.access(dir, os.W_OK):
                continue
            else:
                raise
    raise FileExistsError(errno.EEXIST, 'No usable temporary file name found')


def mkstemp(dir, prefix='', suffix=''):
    flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
    if hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW
    return mktemp(os.open, dir, prefix, suffix, flags=flags, mode=0o600)


def mkdtemp(dir, prefix='', suffix=''):
    _, filename = mktemp(os.mkdir, dir, prefix, suffix, mode=0o700)
    return filename


_TMP_CONFIGS_DIR_BASE = os.environ.get('OTP_BASE_FOLDER_FOR_GENERATED_RESOURCE',
                                       os.path.join(tempfile.gettempdir(), "test_" + getpass.getuser()))


class TmpFile(File, os.PathLike, CleanUpFinalizer):

    ALL: Dict[str, List['TmpFile']] = defaultdict(list)
    keep_everything_generated = False

    def __init__(self, suffix="", name="", clean_up=default, force=False, base_dir=None):
        """
        Class to create a temporary file.
        By default, this file will be deleted automatically after all references to it are gone.
        Base path where temporary files are created could be set using the ``ONE_TICK_TMP_DIR``.
        By default they are created under the ``tempfile.gettempdir()`` folder.


        Parameters
        ----------
        name: str
            name of the temporary file without suffix.
            By default some random name will be generated.
        suffix: str
            suffix of the name of the temporary file.
        clean_up: bool
            Controls whether this temporary file will be deleted automatically
            after all references to it are gone.

            By default,
            :py:attr:`otp.config.clean_up_tmp_files<onetick.py.configuration.Config.clean_up_tmp_files>` is used.
        force: bool
            Rewrite temporary file if it exists and parameter ``name`` is set.
        base_dir: str
            Absolute path of the directory where temporary file will be created.

        See also
        --------

        The testing framework has a ``--keep-generated`` flag that controls clean up for all related instances
        :ref:`onetick py test features`
        """
        clean_up = default_clean_up(clean_up)
        clean_up = clean_up and not TmpFile.keep_everything_generated
        fd, self._path = self._create(clean_up, suffix=suffix, name=name, force=force, base_dir=base_dir)
        # we only needed to create file, so closing opened file descriptor
        os.close(fd)
        self._logger = get_logger(self.__class__.__name__)
        CleanUpFinalizer.__init__(self, clean_up, self.path, self._logger)
        self._logger.debug(f'created {self._path}, clean_up={self.need_to_cleanup}')

    def __repr__(self):
        return f"TmpFile({self.path})"

    @classmethod
    def _cleanup(cls, clean_up_ref, path, logger):
        if clean_up_ref[0] and os.path.exists(path):
            logger.debug(f'removing {path}')
            try:
                os.remove(path)
            except Exception:
                # TODO: remove try-except block when BDS-116 will be fixed
                pass

    def _create(self, clean_up, suffix="", name="", force=False, base_dir=None):

        self._parent_dir = None

        if ONE_TICK_TMP_DIR() and not base_dir:
            # allows to set a test name suffix for when we use it in
            # tests with goal to distinguish tests content (configs, databases, etc)
            # from each other
            dir_path = ONE_TICK_TMP_DIR()
            # save to access them from the teardown
            TmpFile.ALL[dir_path].append(self)

            if not os.path.exists(dir_path):
                # If it does not exist, then lets create it. Note that it goes up
                # recursively until finds an existing folder.
                # We store it as a field of the instance to guarantee that it will
                # be destroyed later than a temporary file.
                self._parent_dir = GeneratedDir(dir_path, clean_up=clean_up)
            elif dir_path in GeneratedDir.ALL:
                self._parent_dir = GeneratedDir.ALL[dir_path]
        else:
            if base_dir and not os.path.exists(base_dir):
                # If it does not exist, then lets create it. Note that it goes up
                # recursively until finds an existing folder.
                # We store it as a field of the instance to guarantee that it will
                # be destroyed later than a temporary file.
                self._parent_dir = GeneratedDir(base_dir, clean_up=clean_up)
            dir_path = base_dir if base_dir else TMP_CONFIGS_DIR()
            TmpFile.ALL[dir_path].append(self)
            if dir_path in GeneratedDir.ALL:
                self._parent_dir = GeneratedDir.ALL[dir_path]

        # let know the parent generated folder to not destroy itself
        if self._parent_dir is not None and not clean_up:
            self._parent_dir.need_to_cleanup = False

        if name:
            path = os.path.normpath(os.path.join(dir_path, name)) + suffix
            flags = os.O_RDWR | os.O_CREAT
            if not force:
                flags = flags | os.O_EXCL
            if hasattr(os, 'O_NOFOLLOW'):
                flags |= os.O_NOFOLLOW
            mode = 0o600
            if os.getenv('OTP_WEBAPI_TEST_MODE'):
                mode = 0o644
            return os.open(path, flags=flags, mode=mode), path
        else:
            return mkstemp(dir=dir_path, suffix=suffix, prefix="")


class TmpDir(os.PathLike, CleanUpFinalizer):

    ALL: Dict[str, List['TmpDir']] = defaultdict(list)
    keep_everything_generated = False

    def __init__(self, rel_path="", *, suffix="", clean_up=default, base_dir=""):
        """
        Class to create a temporary directory.
        By default, this directory will be deleted automatically after all references to it are gone.
        All files and directories under this one will be deleted too.

        Base path where directories are created could be set using the ``ONE_TICK_TMP_DIR``.
        By default they are created under the ``tempfile.gettempdir()`` folder.

        Parameters
        ----------
        rel_path: str
            relative path to the temporary directory.
            If empty, then the name will be auto-generated.
        suffix: str
            suffix of the name of the temporary directory.
        base_dir: str
            relative path of the directory where temporary directory will be created.
        clean_up: bool
            Controls whether this temporary directory will be deleted automatically
            after all references to it are gone.

            By default,
            :py:attr:`otp.config.clean_up_tmp_files<onetick.py.configuration.Config.clean_up_tmp_files>` is used.

        See also
        --------

        The testing framework has a ``--keep-generated`` flag that controls clean up for all related instances
        :ref:`onetick py test features`
        """
        clean_up = default_clean_up(clean_up)
        clean_up = clean_up and not TmpDir.keep_everything_generated
        self._parent_dir = None

        if os.path.isabs(rel_path):
            raise ValueError("Absolute paths are not supported in 'rel_path' parameter.")

        if ONE_TICK_TMP_DIR():
            # allows to set a test name suffix for when we use it in
            # tests with goal to distinguish tests content (configs, databases, etc)
            # from each other
            dir_path = os.path.normpath(os.path.join(ONE_TICK_TMP_DIR(), base_dir))

            if not os.path.exists(dir_path):
                # If it does not exist, then lets create it. Note that it goes up
                # recursively until finds an existing folder.
                # We store it as a field of the instance to guarantee that it will
                # be destroyed later than a temporary file.
                self._parent_dir = GeneratedDir(dir_path, clean_up=clean_up)
            elif not clean_up:
                if dir_path in GeneratedDir.ALL:  # NOSONAR
                    # let know the parent generated folder to not destroy itself
                    GeneratedDir.ALL[dir_path].need_to_cleanup = False

                    self._parent_dir = GeneratedDir.ALL[dir_path]

        else:
            dir_path = os.path.normpath(os.path.join(TMP_CONFIGS_DIR(), base_dir))

        if rel_path:
            path = os.path.normpath(os.path.join(dir_path, rel_path)) + suffix
            # dir_path should be the parent directory of path
            dir_path = os.path.dirname(path)
            for tmp_dir in TmpDir.ALL[dir_path]:
                # check if path already exists
                if tmp_dir.path == path:
                    self.path = tmp_dir.path
                    if not clean_up:
                        tmp_dir.need_to_cleanup = clean_up
                    return

            os.mkdir(path, mode=0o700)
            self.path = path
        else:
            self.path = mkdtemp(dir=dir_path, suffix=suffix, prefix="")

        # save to access them in the teardown
        TmpDir.ALL[dir_path].append(self)

        self._logger = get_logger(self.__class__.__name__)
        CleanUpFinalizer.__init__(self, clean_up, self.path, self._logger)
        self._logger.debug(f'created {self.path}, clean_up={self.need_to_cleanup}')

    @CleanUpFinalizer.need_to_cleanup.setter   # type: ignore
    def need_to_cleanup(self, value):
        super(TmpDir, type(self)).need_to_cleanup.fset(self, value)
        children = self.ALL[self.path]
        for child in children:
            child.need_to_cleanup = value

    @classmethod
    def _cleanup(cls, clean_up_ref, path, logger):
        if clean_up_ref[0]:
            logger.debug(f'removing {path}')
            shutil.rmtree(path, ignore_errors=True)

    def __str__(self):
        return self.path

    def __repr__(self):
        return f"TmpDir({self.path})"

    def __fspath__(self):
        return self.path


class GeneratedDir(os.PathLike, CleanUpFinalizer):

    ALL: Dict[str, 'GeneratedDir'] = {}  # store all created dir with goal to set cleanup=False if it requires
    keep_everything_generated = False

    def __init__(self, dir_path, clean_up=default):
        """
        Class to create generated temporary directory.
        By default, this directory will be deleted automatically after all references to it are gone.
        All files and directories under this one will be deleted too.

        The main difference from TmpDir class is that also this directory's parent directory will be deleted!

        The exact path to this directory will be: ``dir_path``.

        Parameters
        ----------
        dir_path: str
            path to the temporary directory.
        clean_up: bool
            Controls whether this temporary directory will be deleted automatically
            after all references to it are gone.

            By default,
            :py:attr:`otp.config.clean_up_tmp_files<onetick.py.configuration.Config.clean_up_tmp_files>` is used.
        """

        parent = os.path.normpath(os.path.dirname(dir_path))

        clean_up = default_clean_up(clean_up)
        clean_up = clean_up and not GeneratedDir.keep_everything_generated

        self._parent_dir = None
        if not os.path.exists(parent):
            # Do the same thing for the parent folder.
            # Save it as a field to guarantee that child folder is destroyed
            # earlier than parent
            self._parent_dir = GeneratedDir(parent, clean_up=clean_up)
        elif not clean_up:
            if parent in GeneratedDir.ALL:  # NOSONAR
                GeneratedDir.ALL[parent].need_to_cleanup = False

        if self._parent_dir is None and parent in GeneratedDir.ALL:
            self._parent_dir = GeneratedDir.ALL[parent]

        self.path = dir_path

        if os.path.exists(dir_path):
            # Do not create and even handle it, because it has been created externally.
            # It mgiht happen in the concurrent runs.
            return

        # We need to set exist_ok=True, because it still can be created externally in the concurrent runs
        os.makedirs(dir_path, exist_ok=True)

        self._logger = get_logger(self.__class__.__name__)
        CleanUpFinalizer.__init__(self, clean_up, self.path, self._logger)
        self._logger.debug(f'created {self.path}, clean_up={self.need_to_cleanup}')

        GeneratedDir.ALL[dir_path] = self

    @CleanUpFinalizer.need_to_cleanup.setter   # type: ignore
    def need_to_cleanup(self, value):
        super(GeneratedDir, type(self)).need_to_cleanup.fset(self, value)

        if self._parent_dir:
            self._parent_dir.need_to_cleanup = value

    @classmethod
    def _cleanup(cls, clean_up_ref, path, logger):
        if clean_up_ref[0]:
            logger.debug(f'removing {path}')
            shutil.rmtree(path, ignore_errors=True)

    def __str__(self):
        return self.path

    def __repr__(self):
        return f"GeneratedDir({self.path})"

    def __fspath__(self):
        return self.path


# ----------------------------------------------------
# We set TMP_CONFIGS_DIR to a function, that returns
# only the first result.
# We need to support multiprocessing environment, where
# every process should use it's own base temporary folder
# but generate it only when something is required, because
# otherwise parent process could construct it with its pid
# and other processes would use the same location.


def _gen_root_path_():
    generated = []

    def _inner_():
        if not generated:
            generated.append(
                GeneratedDir(
                    os.path.join(
                        _TMP_CONFIGS_DIR_BASE,
                        f'run_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}_{os.getpid()}',
                    )
                ).path
            )
        return generated[0]

    return _inner_


TMP_CONFIGS_DIR = _gen_root_path_()
def ONE_TICK_TMP_DIR():  # noqa # NOSONAR
    if os.getenv('OTP_WEBAPI_TEST_MODE'):
        return WEBAPI_TEST_MODE_SHARED_CONFIG
    if 'ONE_TICK_TMP_DIR' not in os.environ:
        return None
    return os.path.normpath(os.path.join(TMP_CONFIGS_DIR(), os.environ['ONE_TICK_TMP_DIR']))
# ----------------------------------------------------


class PermanentFile(File):
    def copy(self):
        return TmpFile(self._path)
