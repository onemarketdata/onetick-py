import os
from datetime import datetime
from typing import Iterable, Type, Union, Optional
from contextlib import suppress, contextmanager
from textwrap import dedent

import dotenv
from onetick.py.otq import otq
from .utils import default_license_dir, default_license_file, get_local_number_of_cores
import onetick.py.types as ott

DEFAULT_LICENSE_DIR = default_license_dir()
DEFAULT_LICENSE_FILE = default_license_file()

DATETIME_FORMATS = (
    '%Y/%m/%d %H:%M:%S.%f',
    '%Y/%m/%d %H:%M:%S',
)
DATETIME_DESCRIPTION = (
    "Format of the env variable: "
    f"{', '.join(':code:`{}`'.format(fmt) for fmt in DATETIME_FORMATS)}."
)


def parse_datetime(s):
    for fmt in DATETIME_FORMATS:
        with suppress(ValueError):
            return datetime.strptime(s, fmt)
    raise ValueError(
        f"The datetime pattern is not supported for string '{s}'. "
        f"Available patterns: {DATETIME_FORMATS}"
    )


def parse_bool(value) -> Optional[bool]:
    str_value = str(value).lower()
    if str_value in ('1', 'true', 'yes'):
        return True
    elif str_value in ('0', 'false', 'no'):
        return False
    else:
        return None


def parse_true(value) -> bool:
    return parse_bool(value) is True


def parse_bool_or_string(value) -> Union[bool, str]:
    parsed_value = parse_bool(value)
    if parsed_value is not None:
        return parsed_value
    else:
        return str(value)


def _env_func_concurrency(value: str) -> Optional[int]:
    if not value:
        return None
    if value == 'local_number_of_cores':
        return get_local_number_of_cores()
    return int(value)


def default_query_concurrency():
    concurrency = config.default_concurrency
    if concurrency is None:
        from onetick.py.compatibility import is_zero_concurrency_supported
        # TODO: this logic should be in the default value of otp.config.default_concurrency,
        # but there are complex problems with circular import in this case
        if is_zero_concurrency_supported():
            concurrency = 0
        else:
            concurrency = 1
    return concurrency


def default_presort_concurrency():
    if config.presort_force_default_concurrency:
        return default_query_concurrency()
    else:
        return None


class _nothing(type):
    def __repr__(cls):
        return cls.__name__


class nothing(metaclass=_nothing):
    """
    This is nothing.
    """


class OtpProperty:
    """
    .. attribute:: {name}
       :type: {base_type}
       :value: {base_value}

       {description}

       {env_var_name}

       {env_var_desc}
    """
    def __init__(self, description, base_default, env_var_name=None, env_var_func=None,
                 env_var_desc=None, set_value=nothing, allowed_types: Union[Type, Iterable] = nothing,
                 validator_func=None):
        self._base_default = base_default
        self._env_var_name = env_var_name
        self._env_var_func = env_var_func
        self._env_var_desc = env_var_desc
        self._set_value = set_value
        self._description = description
        if self._base_default is nothing:
            self._allowed_types = []
        else:
            self._allowed_types = [type(self._base_default)]
        if allowed_types is not nothing:
            if isinstance(allowed_types, Iterable):
                self._allowed_types.extend(allowed_types)
            else:
                self._allowed_types.append(allowed_types)
        self._allowed_types = tuple(set(self._allowed_types))  # type: ignore[assignment]
        # will be monkeypatched later
        self._name = None
        if validator_func is None:
            self._validator_func = lambda x: x
        else:
            self._validator_func = validator_func

    def _get_doc(self, name):
        env_var_name = ''
        env_var_desc = ''
        if self._env_var_name is not None:
            env_var_name = f'Can be set using environment variable :envvar:`{self._env_var_name}`.'
            if self._env_var_desc is not None:
                env_var_desc = self._env_var_desc
        return self.__class__.__doc__.format(
            name=name,
            description=self._description,
            base_type=','.join(t.__name__ for t in self._allowed_types),
            base_value=repr(self._base_default),
            env_var_name=env_var_name,
            env_var_desc=env_var_desc
        )

    def __get__(self, obj, objtype=None):
        if self._set_value is not nothing:
            return self._set_value
        if self._env_var_name:
            env_var_value = os.environ.get(self._env_var_name, None)
            if env_var_value is not None:
                if self._env_var_func:
                    return self._env_var_func(env_var_value)
                return env_var_value
        if obj is not None:
            # get value from default config
            if self._env_var_name and self._env_var_name in obj.default_config:
                var_value = obj.default_config[self._env_var_name]
                if self._env_var_func:
                    return self._env_var_func(var_value)
                return var_value
        if self._base_default is nothing:
            raise ValueError(f'onetick.py.config.{self._name} is not set!')
        return self._base_default

    def __set__(self, obj, value):
        # assigning to nothing is permitted
        # assigning to nothing will reset value to default
        if not isinstance(value, self._allowed_types) and value is not nothing:
            raise ValueError(f'Type of passed configuration value "{type(value)}" should be one of '
                             f'the allowed types for this configuration {self._allowed_types}')
        if value is nothing:
            self._set_value = value
        else:
            self._set_value = self._validator_func(value)


class OtpDerivedProperty:
    """
    .. attribute:: {name}
       :type: {base_type}
       :value: {base_value}

       {description}
    """

    def __init__(self, description, definition_function):
        self._description = description
        self.__definition_function = definition_function

    def __get__(self, obj, objtype=None):
        return self.__definition_function(obj)

    def _get_doc(self, name, base_object):
        value = self.__definition_function(base_object, docs=True)
        return self.__doc__.format(
            name=name,
            description=self._description,
            base_type=type(value).__name__,
            base_value=value,
        )

    def __set__(self, obj, value):
        raise AttributeError('It\'s not allowed to change a derived property. Change source properties, and its value '
                             'will be updated automatically.')


class OtpShowStackInfoProperty(OtpProperty):
    """
    .. attribute:: {name}
       :type: {base_type}
       :value: {base_value}

       {description}
    """
    @staticmethod
    def parser(value):
        return parse_true(value)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # set default value on module loading
        self.__set_in_onetick_query__()

    def __get__(self, obj, objtype=None):
        value = super().__get__(obj, objtype)
        return self.parser(value)

    def __set__(self, obj, value):
        super().__set__(obj, value)
        self.__set_in_onetick_query__()

    def __set_in_onetick_query__(self):
        value = 1 if self.__get__(None) else 0
        otq.API_CONFIG['SHOW_STACK_INFO'] = value


def document_config(cls):
    manually_set_properties_doc = ''
    changeable_properties_doc = ''
    derived_properties_doc = ''

    manually_set_options = cls.manually_set_options()
    for b in manually_set_options:
        manually_set_properties_doc += cls.__dict__[b]._get_doc(b)
    for c in cls.get_changeable_config_options():
        cls.__dict__[c]._name = c
        if c not in manually_set_options:
            changeable_properties_doc += cls.__dict__[c]._get_doc(c)
    for d in cls.get_derived_config_options():
        cls.__dict__[d]._name = d
        derived_properties_doc += cls.__dict__[d]._get_doc(d, base_object=cls)

    cls.__doc__ = cls.__doc__.format(
        manually_set_properties=manually_set_properties_doc,
        changeable_properties=changeable_properties_doc,
        derived_properties=derived_properties_doc,
    )
    cls.__doc__ = dedent(cls.__doc__)

    return cls


@document_config
class Config:
    """
    This object is used to access ``onetick.py`` configuration variables.

    Configuration variables may be accessed via :code:`otp.config['...']` syntax, e.g. :code:`otp.config['tz']`.

    Configuration variables may be changed by:

    * during python runtime by modifying properties of object ``otp.config``,
    * by setting environment variables *before* importing ``onetick.py`` module.

    During python runtime you can modify properties of ``otp.config`` object either directly or via context manager:
    ``with otp.config('property', 'value'):``

    Also special environment variable ``OTP_DEFAULT_CONFIG_PATH`` can be used to specify a file,
    from which configuration variables will be taken.
    This file will be read only once on module loading or when getting one of the configuration variables
    when the environment variable is discovered.
    The names of the variables in this file are the same as the names of environment variables.

    In case several methods of setting configuration variables are used,
    the following order of priority is in place:

    1. Value that is set by modifying object ``otp.config``
    2. Value that is set via environment variable
    3. Value that is set in file ``OTP_DEFAULT_CONFIG_PATH``
    4. Default value specified in the source code

    To reset configuration value that has been set by modifying object ``otp.config``,
    special value ``otp.config.default`` should be assigned to it.

    Most of the config vars are optional and have default values,
    but some of them need to be set manually.

    There are also some environment variables that do not have
    corresponding property in ``otp.config`` object:

    * ``OTP_BASE_FOLDER_FOR_GENERATED_RESOURCE``:
      a folder where all intermediate queries, files and databases
      generated by ``onetick-py`` are located.
      The default value is system-dependent, e.g. some generated
      directory with a unique name under a standard directory **/tmp** for Linux.

    **The following properties must be set manually in most cases:**
    {manually_set_properties}

    **The following properties can be changed:**
    {changeable_properties}

    **The following properties are derived and thus read-only:**
    {derived_properties}
    """
    __default_config = None

    @property
    def default_config(self):
        default_config_path = os.environ.get('OTP_DEFAULT_CONFIG_PATH')
        if not default_config_path:
            return {}
        if self.__default_config is None:
            default_config = dotenv.dotenv_values(default_config_path)
            available_option_names = []
            for name, option in self.get_changeable_config_options().items():
                if option._env_var_name:
                    available_option_names.append(option._env_var_name)
            diff = set(default_config).difference(available_option_names)
            if diff:
                raise ValueError(f'Configuration options {diff} from file'
                                 f' OTP_DEFAULT_CONFIG_PATH="{default_config_path}" are not supported.'
                                 f' Available options: {available_option_names}.')
            Config.__default_config = default_config
        return self.__default_config or {}

    def __getitem__(self, item):
        if item not in self.__class__.__dict__.keys():
            raise AttributeError(f'"{item}" is not in the list of onetick.py config options!')
        return self.__class__.__dict__[item].__get__(self)

    def __setitem__(self, item, value):
        if item not in self.__class__.__dict__.keys():
            raise AttributeError(f'"{item}" is not in the list of onetick.py config options!')
        self.__class__.__dict__[item].__set__(self, value)

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except ValueError:
            return default

    def __setattr__(self, item, value):
        """
        To avoid accidental declaration of non-existing properties, e.g. `otp.config.timezone = "GMT"`
        """
        self.__setitem__(item, value)

    @contextmanager
    def __call__(self, name, value):
        old_value = self[name]
        try:
            self[name] = value
            yield
        finally:
            self[name] = old_value

    @classmethod
    def get_changeable_config_options(cls):
        """
        useful for tests where you may want to memorize all existing configuration options before changing them
        """
        return {
            option: value
            for option, value in cls.__dict__.items()
            if isinstance(value, OtpProperty)
        }

    @classmethod
    def get_derived_config_options(cls):
        return {
            option: value
            for option, value in cls.__dict__.items()
            if isinstance(value, OtpDerivedProperty)
        }

    @classmethod
    def manually_set_options(cls):
        return {
            option: value
            for option, value in cls.__dict__.items()
            if isinstance(value, OtpProperty) and value._base_default is nothing
        }

    default = nothing

    tz = OtpProperty(
        description='Default timezone used for running queries and creating databases, '
                    'e.g. with :py:func:`otp.run<onetick.py.run>`. '
                    'Default value is the local timezone of your machine.',
        base_default=None,
        allowed_types=str,
        env_var_name='OTP_DEFAULT_TZ',
    )

    context = OtpProperty(
        description='Default context used for running queries, '
                    'e.g. with :py:func:`otp.run<onetick.py.run>`.'
                    'Note: In WebAPI mode it will have `None` value.',
        base_default='DEFAULT' if not otq.webapi else None,
        allowed_types=[str],
        env_var_name='OTP_CONTEXT',
    )

    default_start_time = OtpProperty(
        description='Default start time used for running queries, '
                    'e.g. with :py:func:`otp.run<onetick.py.run>`.',
        base_default=nothing,
        allowed_types=[datetime, ott.datetime],
        env_var_name='OTP_DEFAULT_START_TIME',
        env_var_func=parse_datetime,
        env_var_desc=DATETIME_DESCRIPTION,
    )

    default_end_time = OtpProperty(
        description='Default end time used for running queries, '
                    'e.g. with :py:func:`otp.run<onetick.py.run>`.',
        base_default=nothing,
        allowed_types=[datetime, ott.datetime],
        env_var_name='OTP_DEFAULT_END_TIME',
        env_var_func=parse_datetime,
        env_var_desc=DATETIME_DESCRIPTION,
    )

    default_db = OtpProperty(
        description='Default database name used for running queries, '
                    'e.g. with :py:func:`otp.run<onetick.py.run>`.',
        base_default=nothing,
        allowed_types=str,
        env_var_name='OTP_DEFAULT_DB',
    )

    default_symbol = OtpProperty(
        description='Default symbol name used for running queries, '
                    'e.g. with :py:func:`otp.run<onetick.py.run>`.',
        base_default=nothing,
        allowed_types=str,
        env_var_name='OTP_DEFAULT_SYMBOL',
    )

    default_symbology = OtpProperty(
        description='Default database symbology.',
        base_default='BZX',
        env_var_name='OTP_DEFAULT_SYMBOLOGY',
    )

    def _default_db_symbol(obj, docs=False):  # noqa
        try:
            return obj.default_db + '::' + obj.default_symbol
        except (ValueError, TypeError):
            if not docs:
                raise

    default_db_symbol = OtpDerivedProperty(
        description='Default symbol with database. '
                    'Defined with :py:attr:`default_db` and :py:attr:`default_symbol` '
                    'as string **default_db::default_symbol**.',
        definition_function=_default_db_symbol,
    )

    def _default_date(obj, docs=False):  # noqa
        try:
            return datetime.combine(obj.default_start_time.date(), datetime.min.time())
        except (ValueError, TypeError, AttributeError):
            if not docs:
                raise

    default_date = OtpDerivedProperty(
        description='Default date. '
                    'Defined as a date part of :py:attr:`default_start_time`.',
        definition_function=_default_date,
    )

    default_concurrency = OtpProperty(
        description='Default concurrency level used for running queries, '
                    'e.g. with :py:func:`otp.run<onetick.py.run>`. '
                    'Default value is ``None`` which means that the value is adaptive '
                    'and is set to 0 (meaning concurrency will be auto-assigned by OneTick server) '
                    'on the latest OneTick versions where it is supported '
                    'or to 1 (meaning no concurrency) on older versions. '
                    'Special value ``local_number_of_cores`` can be used to set concurrency '
                    'to the number of cores of the machine where python code executes '
                    '(this corresponds to the previous default logic still expected by some users).',
        base_default=None,
        allowed_types=[type(None), int, str],
        env_var_name='OTP_DEFAULT_CONCURRENCY',
        env_var_func=_env_func_concurrency,
        validator_func=lambda x: _env_func_concurrency(str(x) if x is not None else '')
    )

    presort_force_default_concurrency = OtpProperty(
        description='By default concurrency value for PRESORT EPs is empty '
                    'and inherited from the concurrency level set in the query where this EP is used. '
                    'However, is some cases it may be desirable '
                    'to force setting default concurrency level for all PRESORT EPs '
                    '(this corresponds to the previous default logic still expected by some users), '
                    'for example when PRESORT is located in the first stage query and cannot inherit '
                    'concurrency from the main query.',
        base_default=False,
        allowed_types=[bool],
        env_var_name='OTP_PRESORT_FORCE_DEFAULT_CONCURRENCY',
        env_var_func=parse_true,
    )

    # default batch size is set to 0, so the number of symbols in batch is not limited
    # it should work better in simple cases, but may use too much memory for complex queries
    default_batch_size = OtpProperty(
        description='Default batch size used for running queries, '
                    'e.g. with :py:func:`otp.run<onetick.py.run>`. '
                    'Batch size is the maximum number of symbols that are processed at once. '
                    'The value of 0 means unlimited -- works faster for simple queries, '
                    'but may consume too much memory for complex queries.',
        base_default=0,
        env_var_name='OTP_DEFAULT_BATCH_SIZE',
        env_var_func=int,
    )

    default_license_dir = OtpProperty(
        description='Default path for license directory. '
                    'Needed for user to be allowed to use OneTick API. '
                    'Default value is system-dependent: '
                    '**/license** for Linux systems and '
                    '**C:/OMD/client_data/config/license_repository** for Windows systems.',
        base_default=DEFAULT_LICENSE_DIR,
        env_var_name='OTP_DEFAULT_LICENSE_DIR',
        allowed_types=str,
    )

    default_license_file = OtpProperty(
        description='Default path for license file. '
                    'Needed for user to be allowed to use OneTick API. '
                    'Default value is system-dependent: '
                    '**/license/license.dat** for Linux systems and '
                    '**C:/OMD/client_data/config/license.dat** for Windows systems.',
        base_default=DEFAULT_LICENSE_FILE,
        env_var_name='OTP_DEFAULT_LICENSE_FILE',
        allowed_types=str,
    )

    default_fault_tolerance = OtpProperty(
        description='Default value for USE_FT query property.',
        base_default='FALSE',
        env_var_name='OTP_DEFAULT_FAULT_TOLERANCE',
        allowed_types=str,
    )

    default_username = OtpProperty(
        description='Default username to call queries. '
                    'By default the name of the owner of the current process is used.',
        base_default=None,
        allowed_types=str,
        env_var_name='OTP_DEFAULT_USERNAME',
    )

    default_auth_username = OtpProperty(
        description='Default username used for authentication.',
        base_default=None,
        allowed_types=str,
        env_var_name='OTP_DEFAULT_AUTH_USERNAME',
    )

    default_password = OtpProperty(
        description='Default password used for authentication.',
        base_default=None,
        allowed_types=str,
        env_var_name='OTP_DEFAULT_PASSWORD',
    )

    http_address = OtpProperty(
        description='Default HTTP server used as WebAPI endpoint.',
        base_default=None,
        allowed_types=str,
        env_var_name='OTP_HTTP_ADDRESS',
    )

    http_username = OtpProperty(
        description='Username used for WebAPI authentication.',
        base_default=None,
        allowed_types=str,
        env_var_name='OTP_HTTP_USERNAME',
    )

    http_password = OtpProperty(
        description='Password used for WebAPI authentication.',
        base_default=None,
        allowed_types=str,
        env_var_name='OTP_HTTP_PASSWORD',
    )

    http_proxy = OtpProperty(
        description='HTTP proxy used for WebAPI requests.',
        base_default=None,
        allowed_types=str,
        env_var_name='HTTP_PROXY',
    )

    https_proxy = OtpProperty(
        description='HTTPS proxy used for WebAPI requests.',
        base_default=None,
        allowed_types=str,
        env_var_name='HTTPS_PROXY',
    )

    access_token = OtpProperty(
        description='SSO access token for WebAPI endpoint.',
        base_default=None,
        allowed_types=str,
        env_var_name='OTP_ACCESS_TOKEN',
    )

    client_id = OtpProperty(
        description='Client ID for obtaining SSO access token.',
        base_default=None,
        allowed_types=str,
        env_var_name='OTP_CLIENT_ID',
    )

    client_secret = OtpProperty(
        description='Client Secret for obtaining SSO access token.',
        base_default=None,
        allowed_types=str,
        env_var_name='OTP_CLIENT_SECRET',
    )

    access_token_url = OtpProperty(
        description='URL for obtaining SSO access token.',
        base_default=None,
        allowed_types=str,
        env_var_name='OTP_ACCESS_TOKEN_URL',
    )

    access_token_scope = OtpProperty(
        description='Scope for obtaining SSO access token.',
        base_default=None,
        allowed_types=str,
        env_var_name='OTP_ACCESS_TOKEN_SCOPE',
    )

    trusted_certificates_file = OtpProperty(
        description='Either a boolean, in which case it controls whether we verify the server TLS certificate '
                    'or a string with the path to the file with list of '
                    'trusted Certificate Authority certificates for WebAPI requests. '
                    'Default behaviour implies verification is enabled.',
        base_default=None,
        allowed_types=(bool, str),
        env_var_name='OTP_SSL_CERT_FILE',
        env_var_func=parse_bool_or_string,
    )

    max_expected_ticks_per_symbol = OtpProperty(
        description='Expected maximum number of ticks per symbol (used for performance optimizations). '
                    'Default is 2000.',
        base_default=None,
        allowed_types=int,
        env_var_name='OTP_MAX_EXPECTED_TICKS_PER_SYMBOL',
    )

    show_stack_info = OtpShowStackInfoProperty(
        description='Show stack info (filename and line or stack trace) in OneTick exceptions.',
        base_default=False,
        allowed_types=(str, bool, int),
        env_var_name='OTP_SHOW_STACK_INFO',
        env_var_func=parse_true,
    )

    log_symbol = OtpProperty(
        description='Log currently executed symbol. Note, this only works with unbound symbols. '
                    'Note, in this case :py:func:`otp.run<onetick.py.run>` does not produce the output '
                    'so it should be used only for debugging purposes.',
        base_default=False,
        allowed_types=(str, bool, int),
        env_var_name='OTP_LOG_SYMBOL',
        env_var_func=parse_true,
    )

    ignore_ticks_in_unentitled_time_range = OtpProperty(
        description='Default value for IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE query property.',
        base_default=False,
        env_var_name='OTP_IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE',
        allowed_types=(str, bool, int),
        env_var_func=parse_true,
    )

    main_query_generated_filename = OtpProperty(
        description='The name of the .otq file with generated main query executed by otp.run.',
        base_default='',
        env_var_name='OTP_MAIN_QUERY_GENERATED_FILENAME',
        allowed_types=str,
    )

    logging = OtpProperty(
        description='The logging level string or path to the file with configuration. '
                    'Check the documentation of python logging module for the configuration formats. '
                    'JSON format (in the file with .json suffix) and python configparser formats are supported.',
        base_default='WARNING',
        env_var_name='OTP_LOGGING',
        allowed_types=str,
    )

    otq_debug_mode = OtpProperty(
        description='Enable .otq files debug mode. '
                    'If set to True, onetick.py will keep all generated otq files and '
                    'log their paths to the console.',
        base_default=False,
        env_var_name='OTP_OTQ_DEBUG_MODE',
        allowed_types=(str, bool, int),
        env_var_func=parse_true,
    )

    allow_lowercase_in_saved_fields = OtpProperty(
        description='Allow using lower case characters in field names that are being stored in Onetick databases. '
                    'If set to False, onetick.py would not allow saving fields with lower case characters '
                    'to a database.',
        base_default=True,
        allowed_types=bool,
    )

    clean_up_tmp_files = OtpProperty(
        description='Control deleting temporary files created by onetick-py. '
                    'Temporary files are OneTick configuration files and generated .otq queries.',
        base_default=True,
        env_var_name='OTP_CLEAN_UP_TMP_FILES',
        allowed_types=(str, bool, int),
        env_var_func=parse_true,
    )

    default_schema_policy = OtpProperty(
        description='Default schema policy when querying onetick database. '
                    'See parameter ``schema_policy`` in :class:`otp.DataSource <onetick.py.DataSource>` '
                    'for the list of supported values.',
        base_default=None,
        env_var_name='OTP_DEFAULT_SCHEMA_POLICY',
        allowed_types=str,
    )

    disable_compatibility_checks = OtpProperty(
        description='Disable compatibility checks when querying OneTick database. '
                    'Using this parameter outside test environment could lead to unexpected errors.',
        base_default=False,
        allowed_types=bool,
        env_var_name='OTP_DISABLE_COMPATIBILITY_CHECKS',
    )


def get_options_table(cls):
    options_table = ('\n'
                     '.. csv-table::\n'
                     '   :header: "Name", "Environment Variable", "Description"\n'
                     '   :widths: auto\n\n')
    for name, prop in cls.get_changeable_config_options().items():
        name = f':py:attr:`otp.config.{name}<onetick.py.configuration.Config.{name}>`'
        options_table += f'   {name},"``{prop._env_var_name}``","{prop._description}"\n'
    for name, prop in cls.get_derived_config_options().items():
        name = f':py:attr:`otp.config.{name}<onetick.py.configuration.Config.{name}>`'
        options_table += f'   {name},"","{prop._description}"\n'
    return options_table


class OptionsTable:
    __doc__ = get_options_table(Config)


config = Config()
