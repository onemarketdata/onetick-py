import difflib
import os
import sysconfig
import warnings
from collections import OrderedDict

from onetick.py.otq import otq

from .temp import TmpFile, TmpDir, ONE_TICK_TMP_DIR
from ..core import db_constants
from . import types


def reload_config(db=None, config_type='LOCATOR'):
    import onetick.py as otp

    if db is not None:
        warnings.warn("Parameter 'db' is deprecated and has no meaning")

    return otp.run(
        otq.ReloadConfig(config_type=config_type).tick_type('ANY'),
        symbols='LOCAL::',
        # start and end times don't matter for this query, use some constants
        start=db_constants.DEFAULT_START_DATE,
        end=db_constants.DEFAULT_END_DATE,
    )


def _remove_quotes(s):
    return s.replace('"', "").replace("'", "")


def _read_config_params(path, _included_paths=None):
    params = OrderedDict()

    if _included_paths is None:
        _included_paths = set()

    with open(path, "r") as fin:
        for line in fin:
            line = line.strip()

            if line.startswith("#"):
                continue

            if line.startswith("INCLUDE"):
                included_path = os.path.expandvars(_remove_quotes(line.split()[1]))
                if included_path in _included_paths:
                    raise RecursionError(f"Path '{included_path}' is included more than once")
                _included_paths.add(included_path)
                included_params = _read_config_params(included_path, _included_paths)
                params.update({
                    k: v for k, v in included_params.items()
                    if k not in params
                })
                continue

            values = line.split("=")
            key, value = values[0].strip(), os.path.expandvars("".join(values[1:]).strip())

            if key and key not in params:
                params[key] = value.strip()

    return params


def _write_config_params(path, params):
    with open(path, "w") as fout:
        for key, value in params.items():
            fout.write(f"{key}={value}\n")


def _check_param_in_params(param, params, throw_exception=True):
    if param in params:
        return True
    if not throw_exception:
        return False
    closest = difflib.get_close_matches(param, params.keys())
    msg = f"Parameter '{param}' is not in the config file."
    if closest:
        msg = f"{msg} It might be you supposed '{closest[0]}'?"
    raise AttributeError(msg)


def modify_config_param(path, param, value, throw_on_missing=True):
    params = _read_config_params(path)

    _check_param_in_params(param, params, throw_on_missing)

    params[param] = f'"{value}"'

    _write_config_params(path, params)


def get_config_param(path, param, default=None):
    params = _read_config_params(path)

    res = _check_param_in_params(param, params, throw_exception=(default is None))

    if res:
        return _remove_quotes(params[param])
    return default


def is_param_in_config(path, param):
    params = _read_config_params(path)

    return _check_param_in_params(param, params, throw_exception=False)


def tmp_config(
    locator=None,
    acl=None,
    otq_path=None,
    csv_path=None,
    clean_up=types.default,
    license_path=None,
    license_dir=None,
    license_servers=None,
):
    data = []

    if otq_path is None:
        otq_path = []

    if ONE_TICK_TMP_DIR():
        otq_path.append(ONE_TICK_TMP_DIR())

    if csv_path is None:
        csv_path = []

    otq_path = list(map(os.path.normpath, otq_path))
    csv_path = list(map(os.path.normpath, csv_path))

    data.append("ONE_TICK_CONFIG.ALLOW_ENV_VARS=Yes")
    data.append("")
    config_vars = sysconfig.get_config_vars()
    data.append(f"PYTHON_VERSION='{config_vars['py_version_short']}{config_vars['abiflags']}'")

    if locator:
        data.append(f'DB_LOCATOR.DEFAULT="{_remove_quotes(str(locator))}"')

    if acl:
        data.append(f'ACCESS_CONTROL_FILE="{_remove_quotes(str(acl))}"')

    if license_path:
        data.append(f'ONE_TICK_LICENSE_FILE="{_remove_quotes(str(license_path))}"')

    if license_dir:
        data.append(f'LICENSE_REPOSITORY_DIR="{_remove_quotes(str(license_dir))}"')

    if license_servers:
        data.append(f'LICENSE_TICK_SERVERS="{_remove_quotes(str(license_servers))}"')

    if otq_path:
        data.append(f'OTQ_FILE_PATH="{",".join(otq_path)}"')

    if csv_path:
        data.append(f'CSV_FILE_PATH="{",".join(csv_path)}"')

    if os.getenv('OTP_WEBAPI_TEST_MODE'):
        tmp_dir = TmpDir()
        data.append(f'TICK_SERVER_CSV_CACHE_DIR="{tmp_dir}"')
        data.append(f'TICK_SERVER_DATA_CACHE_DIR="{tmp_dir}"')
        data.append(f'TICK_SERVER_OTQ_CACHE_DIR="{tmp_dir}"')

    data.append("")
    data.append("ALLOW_REMOTE_CONTROL=Yes")
    data.append("ALLOW_NO_CERTIFICATE=true")
    # to be able to run otp.ODBC
    data.append("LOAD_ODBC_UDF=true")

    # # let's not strip quotes around params by default, this is the old behaviour
    # new default behaviour breaks backward-compatibility (BEXRTS-1340, BEXRTS-1342)
    # new behaviour can be achieved by setting STRIP_QUOTES_WHEN_ASSIGNING_SYMBOL_AND_OTQ_PARAMS param in JWQ EP
    data.append("COMPATIBILITY.JOIN_WITH_QUERY.STRIP_QUOTES_AROUND_SYMBOL_AND_OTQ_PARAMS=FALSE")

    if os.getenv('OTP_WEBAPI_TEST_MODE'):
        tmp_file = TmpFile(name="onetick.cfg", clean_up=clean_up, force=True)
    else:
        tmp_file = TmpFile(suffix=".cfg", clean_up=clean_up)

    with open(tmp_file, "w") as fout:
        fout.write("\n".join(data))

    return tmp_file
