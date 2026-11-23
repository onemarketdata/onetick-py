# mypy: disable-error-code="attr-defined"
# pylint: disable=import-error,no-name-in-module

import os
import gc
import sys
import ctypes
import inspect
import faulthandler
import secrets
from enum import Enum

if os.getenv('OTP_WEBAPI', default='').lower() not in ('0', 'false', 'no', ''):
    import onetick.query_webapi as otq

    class OneTickLibMock:
        LOGGING_LEVEL_MIN = 0
        LOGGING_LEVEL_LOW = 1
        LOGGING_LEVEL_MEDIUM = 2
        LOGGING_LEVEL_MAX = 3

        def __init__(self, *args, **kwargs):
            pass

        def set_log_file(self, log_file):
            pass

    otq.OneTickLib = OneTickLibMock

else:
    import onetick.query as otq
    import pyomd


class LoggingLevel(Enum):
    MIN = otq.OneTickLib.LOGGING_LEVEL_MIN
    LOW = otq.OneTickLib.LOGGING_LEVEL_LOW
    MEDIUM = otq.OneTickLib.LOGGING_LEVEL_MEDIUM
    MAX = otq.OneTickLib.LOGGING_LEVEL_MAX


# set by OneTickLib.enable_crash_stacks()
_crash_file = _log_link = _signal_stack = None


class _StackT(ctypes.Structure):
    _fields_ = [('ss_sp', ctypes.c_void_p), ('ss_flags', ctypes.c_int), ('ss_size', ctypes.c_size_t)]


def _link_log_file(log_file=None):
    if not _log_link:
        return
    if not log_file:
        try:
            os.remove(_log_link)
        except FileNotFoundError:
            pass
        return
    # create a uniquely named link and rename it over the old one:
    # rename(2) is atomic, so readers never see a missing path
    tmp_link = f'{_log_link}.{os.getpid()}.{secrets.token_hex(4)}.tmp'
    os.symlink(os.path.abspath(log_file), tmp_link)
    try:
        os.replace(tmp_link, _log_link)
    except OSError:
        os.remove(tmp_link)
        raise


class OneTickLib:
    """
    Singleton class for ``otq.OneTickLib`` to initialize it once.

    Returns the same object if it was already initialized.
    """
    __otl_instance = None
    __instance = None
    __args = None

    def __new__(cls, *args, **kwargs):
        if cls.__otl_instance is None:
            cls.__otl_instance = super().__new__(cls)

            def proxy_wrap(attr, static=False):

                if static:
                    def f(*args, **kwargs):  # type: ignore
                        return getattr(OneTickLib.__instance, attr)(*args, **kwargs)
                    return staticmethod(f)
                else:
                    def f(self, *args, **kwargs):  # type: ignore
                        return getattr(OneTickLib.__instance, attr)(*args, **kwargs)
                    return f

            for attr, value in inspect.getmembers(otq.OneTickLib, callable):
                if not attr.startswith('_') and not hasattr(OneTickLib, attr):
                    fun = inspect.getattr_static(otq.OneTickLib, attr)
                    static = isinstance(fun, staticmethod)
                    setattr(OneTickLib, attr, proxy_wrap(attr, static=static))

        return cls.__otl_instance

    def __init__(self, *args, log_file=None):
        if OneTickLib.__instance is None:
            if not args:
                args = (None,)
            OneTickLib.__args = args
            if _crash_file is None:
                OneTickLib.__instance = otq.OneTickLib(*args)
            else:
                # otq.OneTickLib installs OneTick's crash handler and drops the one of faulthandler.
                # disable() restores the handler faulthandler replaced, so it has to be called before that.
                # Enabled again, faulthandler writes the Python traceback and then calls OneTick's handler.
                faulthandler.disable()
                try:
                    OneTickLib.__instance = otq.OneTickLib(*args)
                finally:
                    faulthandler.enable(_crash_file)
            if log_file:
                self.set_log_file(log_file)
        elif args != OneTickLib.__args and args:
            raise ValueError("OneTickLib was already initialized with different "
                             f"parameters: Was: {OneTickLib.__args} Now: {args}")

    def __eq__(self, otl):
        return self.__dict__ == otl.__dict__

    def __ne__(self, otl):
        return self.__dict__ != otl.__dict__

    def __str__(self):
        return f"Instance: {self.__instance.get_one_tick_lib()}"

    def cleanup(self):
        """
        Destroy otq.OneTickLib instance and reset singleton class
        """
        obj = OneTickLib.__instance
        OneTickLib.__instance = None
        OneTickLib.__args = None
        del obj
        gc.collect()
        _link_log_file()

    def set_log_file(self, log_file):
        """
        Set log file for given instance of OneTickLib

        :param log_file: path to log file
        """
        OneTickLib.__instance.set_log_file(str(log_file))
        _link_log_file(log_file)
        if hasattr(OneTickLib.__instance, 'close_log_file_in_destructor'):
            # we need to check it to prevent failing CI on the builds that do not have this feature
            OneTickLib.__instance.close_log_file_in_destructor()

    def set_logging_level(self, lvl: LoggingLevel):
        """
        Logging level can be specified by using LoggingLevel Enum class

        :param lvl: available values are LoggingLevel.MIN, LoggingLevel.LOW, LoggingLevel.MEDIUM or LoggingLevel.MAX
        :return:
        """
        OneTickLib.__instance.set_logging_level(lvl)

    def set_authentication_token(self, auth_token: str):
        """
        Set authentication token for given instance of OneTickLib

        :param auth_token: authentication token
        """
        OneTickLib.__instance.set_authentication_token(auth_token)

    @staticmethod
    def override_config_value(config_parameter_name: str, config_parameter_value):
        """
        Override config value of OneTickConfig

        :param config_parameter_name: param to override (could be both set or not set in OneTickConfig)
        :param config_parameter_value: new value of the param
        """
        if OneTickLib.__instance:
            raise RuntimeError('This method should be called before OneTickLib object is constructed to have effect.')
        try:
            # pylint: disable-next=possibly-used-before-assignment
            pyomd.OneTickLib.override_config_value(config_parameter_name, config_parameter_value)
        except NameError:
            raise RuntimeError('This method is not available in WebAPI mode')

    @staticmethod
    def enable_crash_stacks(file=sys.stderr, log_link=None):
        """
        Report both stacks when the process crashes (SIGSEGV, SIGBUS, SIGABRT) inside OneTick:
        :py:mod:`faulthandler` writes the Python traceback to ``file``,
        OneTick writes the C++ stack to its log file.

        Call it from the main thread before the OneTickLib object is constructed.
        On platforms other than Linux and in WebAPI mode it only enables :py:mod:`faulthandler`.

        :param file: file object or file descriptor for the Python traceback
        :param log_link: path of a symbolic link to keep pointing to the current OneTick log file,
            which for :py:class:`otp.Session <onetick.py.Session>` is a temporary file.
            The link is removed when the OneTickLib object is destroyed, so it is left only if the process dies.
        """
        global _crash_file, _log_link, _signal_stack
        faulthandler.enable(file)
        if sys.platform != 'linux' or 'pyomd' not in globals():
            return
        # OneTick's handler is off by default in API processes on Linux
        OneTickLib.override_config_value('ENABLE_ASYNC_LOGGER_SIGNAL_HANDLERS', 'true')
        # it runs on the alternate signal stack of faulthandler (16 KB) and needs up to 62 KB: set a 1 MB one
        _signal_stack = ctypes.create_string_buffer(1 << 20)
        stack = _StackT(ctypes.addressof(_signal_stack), 0, len(_signal_stack))
        if ctypes.CDLL(None, use_errno=True).sigaltstack(ctypes.byref(stack), None) != 0:
            raise OSError(ctypes.get_errno(), 'sigaltstack failed')
        _crash_file = file
        _log_link = log_link and os.path.abspath(log_link)
        _link_log_file()
