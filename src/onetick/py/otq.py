# This file is used to import the correct query module:
# onetick.query or onetick.query_webapi
# based on the environment variable OTP_WEBAPI
# Re-use this file in your tests to import the correct onetick.query module
# Also it is override pyomd and OneTickLib classes with mock classes

import getpass
import inspect
import os
import tempfile
import warnings
import onetick.py as otp


class OneTickLib:
    # mock class for OneTickLib, used for webapi and onetick-query-stubs
    # maybe it is not right to combine this namesake classes (onetick.lib.OneTickLib and pyomd.OneTickLib)
    # but it is done to simplify the code and it is not affecting anything except tests
    LOGGING_LEVEL_MIN = 0
    LOGGING_LEVEL_LOW = 1
    LOGGING_LEVEL_MEDIUM = 2
    LOGGING_LEVEL_MAX = 3
    set_authentication_token = None

    def __init__(self, *args, **kwargs):  # NOSONAR
        pass

    def set_log_file(self, log_file):  # NOSONAR
        pass

    def cleanup(self):  # NOSONAR
        pass


if os.getenv("OTP_SKIP_OTQ_VALIDATION"):
    import onetick_stubs as otq  # noqa: F401
    import pyomd  # noqa: F401

    class ConfigStub:
        API_CONFIG: dict = {}

    class otli:
        OneTickLib = OneTickLib  # NOSONAR

    for key, value in otq.query.__dict__.items():
        setattr(otq, key, value)
    setattr(otq, 'webapi', False)
    setattr(otq, 'config', ConfigStub)
    setattr(otq, 'graph_components', otq)

elif otp.__webapi__:
    import onetick.query_webapi as otq  # noqa: F401
    setattr(otq, 'webapi', True)

    from onetick.py.pyomd_mock import pyomd

    __original_run = otq.run

    def run(*args, **kwargs):
        from onetick.py import config  # noqa
        from onetick.py.compatibility import (
            is_max_concurrency_with_webapi_supported,
            is_webapi_access_token_scope_supported
        )

        if not config.http_address and 'http_address' not in kwargs:
            raise ValueError('otp.run() http_address keyword param, '
                             'otp.config.http_address or OTP_HTTP_ADDRESS '
                             'environment variable are required '
                             'when using WebAPI mode.')

        # TODO WEBAPI review this
        # if file name is not in single quotes, then put it in single quotes
        query_name = None
        query = kwargs.get('query', None)
        if isinstance(query, str):
            if query[0] == "'" and query[-1] == "'":
                query = query.replace("'", "")
            if '::' in query:
                query_name = query.split('::')[-1]
                query = query.replace(f'::{query_name}', '')
            kwargs['query'] = query
        kwargs['query_name'] = query_name

        del_params = [
            'start_time_expression',
            'end_time_expression',
            'alternative_username',
            'batch_size',
            'treat_byte_arrays_as_strings',
            'output_matrix_per_field',
            'return_utc_times',
            'connection',
            'svg_path',
            'use_connection_pool',
            'time_as_nsec',
            'max_expected_ticks_per_symbol',
        ]
        ignore_deleted_params = [
            'treat_byte_arrays_as_strings',
            'time_as_nsec',
            'alternative_username',
            'max_expected_ticks_per_symbol'
        ]
        for param in del_params:
            if param in kwargs:
                if kwargs[param] and param not in ignore_deleted_params:
                    warnings.warn(f'Parameter {param} is not supported in WebAPI mode and will be ignored.')
                del kwargs[param]

        from onetick.py import config  # noqa
        if 'http_address' not in kwargs:
            kwargs['http_address'] = config.http_address

        if kwargs.get('username'):
            kwargs['http_username'] = kwargs['username']
            del kwargs['username']
        else:
            kwargs['http_username'] = config.http_username

        if kwargs.get('password'):
            kwargs['http_password'] = kwargs['password']
            del kwargs['password']
        else:
            kwargs['http_password'] = config.http_password

        if 'access_token' not in kwargs:
            if config.access_token:
                kwargs['access_token'] = config.access_token
        elif not kwargs['access_token']:
            del kwargs['access_token']

        access_token_url = kwargs.get('access_token_url', config.access_token_url)
        if access_token_url:
            if not hasattr(otq, 'get_access_token'):
                raise RuntimeError('Current `onetick.query_webapi` version doesn\'t have `get_access_token` function')

            if kwargs.get('access_token'):
                raise ValueError('Both `access_token` and `access_token_url` set, instead of only one of them.')

            if 'client_id' in kwargs:
                client_id = kwargs.pop('client_id')
            else:
                client_id = config.client_id

            if 'client_secret' in kwargs:
                client_secret = kwargs.pop('client_secret')
            else:
                client_secret = config.client_secret

            for param_name, param_value in zip(['client_id', 'client_secret'], [client_id, client_secret]):
                if not param_value:
                    raise ValueError(f'`access_token_url` parameter set, however `{param_name}` parameter missing.')

            token_kwargs = {}

            if 'scope' in kwargs:
                scope = kwargs.pop('scope')
            else:
                scope = config.access_token_scope

            if scope:
                if not is_webapi_access_token_scope_supported():
                    raise RuntimeError('Parameter `scope` is not supported on used version of OneTick')

                token_kwargs['scope'] = scope

            kwargs['access_token'] = otq.get_access_token(access_token_url, client_id, client_secret, **token_kwargs)

            if 'access_token_url' in kwargs:
                del kwargs['access_token_url']

        if 'http_proxy' not in kwargs:
            kwargs['http_proxy'] = config.http_proxy

        if 'https_proxy' not in kwargs:
            kwargs['https_proxy'] = config.https_proxy

        webapi_run_parameters = inspect.signature(__original_run).parameters

        trusted_certificate_file_arg = kwargs.pop('trusted_certificates_file',
                                                  kwargs.pop('trusted_certificate_file', None))
        trusted_certificate_file_value = (
            trusted_certificate_file_arg if trusted_certificate_file_arg is not None
            else config.trusted_certificates_file
        )
        if trusted_certificate_file_value is not None:
            trusted_certificates_supported = set(webapi_run_parameters).intersection({'trusted_certificates_file',
                                                                                      'trusted_certificate_file'})
            if not trusted_certificates_supported:
                raise ValueError(
                    "Parameter `trusted_certificates_file` was set,"
                    " however current version of OneTick doesn't support it."
                )
            trusted_certificates_supported_param = list(trusted_certificates_supported)[0]
            kwargs[trusted_certificates_supported_param] = trusted_certificate_file_value

        if 'callback' in kwargs and kwargs['callback'] is not None:
            kwargs['output_mode'] = otq.QueryOutputMode.callback

        if 'max_concurrency' in kwargs and not is_max_concurrency_with_webapi_supported():
            kwargs['max_concurrency'] = None

        return __original_run(*args, **kwargs)

    otq.run = run

    otq.OneTickLib = OneTickLib

    class otli:  # type: ignore # noqa: F401
        OneTickLib = otq.OneTickLib  # NOSONAR

else:
    import onetick.query as otq  # type: ignore # noqa: F401
    import pyomd  # type: ignore # noqa: F401
    import onetick.lib.instance as otli  # type: ignore # noqa: F401
    setattr(otq, 'webapi', False)


def _tmp_otq_path():
    # copied from onetick.test.fixtures _keep_generated_dir() with replacement to /tmp
    # required with OTP_WEBAPI_TEST_MODE to separate otq files from shared dbs+config+locator+acl files
    from onetick.py import utils  # noqa
    res = os.path.join(utils.TMP_CONFIGS_DIR(), os.environ.get("ONE_TICK_TMP_DIR", "otqs"))
    res = res.replace(utils.temp.WEBAPI_TEST_MODE_SHARED_CONFIG,
                      os.path.join(tempfile.gettempdir(), "test_" + getpass.getuser()))

    # % and + is some webapi related path bugs, probably would be fixed someday
    return res.replace("%", "_").replace("+", "_")


__all__ = ['otq', 'pyomd', 'otli', '_tmp_otq_path']
