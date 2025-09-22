# mypy: disable-error-code="attr-defined"

import os
import gc
import inspect
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
            cls.__otl_instance = super(OneTickLib, cls).__new__(cls)

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
            OneTickLib.__instance = otq.OneTickLib(*args)
            if log_file:
                self.set_log_file(log_file)
        elif args != OneTickLib.__args and args:
            raise ValueError("OneTickLib was already initialized with different "
                             "parameters: Was: {} Now: {}".format(OneTickLib.__args, args))

    def __eq__(self, otl):
        return self.__dict__ == otl.__dict__

    def __ne__(self, otl):
        return self.__dict__ != otl.__dict__

    def __str__(self):
        return "Instance: {}".format(self.__instance.get_one_tick_lib())

    def cleanup(self):
        """
        Destroy otq.OneTickLib instance and reset singleton class
        """
        del OneTickLib.__instance
        gc.collect()
        OneTickLib.__instance = None
        OneTickLib.__args = None

    def set_log_file(self, log_file):
        """
        Set log file for given instance of OneTickLib

        :param log_file: path to log file
        """
        OneTickLib.__instance.set_log_file(str(log_file))
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
            pyomd.OneTickLib.override_config_value(config_parameter_name, config_parameter_value)  # noqa
        except NameError:
            raise RuntimeError('This method is not available in WebAPI mode')
