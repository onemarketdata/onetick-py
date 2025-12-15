# pylama:ignore=E402,W0611
import os
from . import _version
__version__ = _version.VERSION
__webapi__: bool = os.getenv('OTP_WEBAPI', default='').lower() not in ('0', 'false', 'no', '')


def __validate_onetick_query_integration():  # noqa
    """Logic that checks correctness of integration of python with onetick.query and/or onetick.query_webapi.
    One of the modules should be installed and available to import.

    We first try to import onetick.query and NumPy_OneTickQuery, and if it fails,
    before raising an exception, we check if onetick.query_webapi is installed.
    If it is installed, we set OTP_WEBAPI environment variable and avoid raising any exception/warning.
    """
    global __webapi__

    if os.getenv("OTP_SKIP_OTQ_VALIDATION"):
        return

    _otq_import_ex = None

    try:
        # this import will fail if onetick python directory is not in sys.path
        import onetick.query  # noqa
        try:
            # this import will fail if numpy directory is not in sys.path on old OneTick versions
            import NumPy_OneTickQuery
        except ModuleNotFoundError as e:
            _otq_import_ex = e
        except Exception:
            pass
    except ImportError as e:
        _otq_import_ex = e

    import sysconfig

    config_vars = sysconfig.get_config_vars()

    ot_bin_path = os.path.join("bin")
    ot_python_path = os.path.join(ot_bin_path, "python")
    ot_numpy_path = os.path.join(
        ot_bin_path,
        "numpy",
        "python"
        # short python version 27, 36, 37, etc
        + config_vars["py_version_nodot"]
        # suffix at the end, either empty string for python with the standard memory allocator,
        # or 'm' for python with the py-malloc allocator
        + config_vars["abiflags"],
    )

    pythonpath = os.environ.get('PYTHONPATH')

    missed_required = __get_missed_paths(pythonpath, ot_bin_path, ot_python_path)
    missed_optional = __get_missed_paths(pythonpath, ot_numpy_path)

    import warnings
    if len(missed_required + missed_optional) != 3:
        # TODO: move to onetick.py.compatibility
        if _otq_import_ex is None and onetick.query.OneTickLib.get_build_number() >= 20230711120000:
            warnings.warn(
                'Using PYTHONPATH to specify the location of OneTick python libraries is deprecated.'
                ' Starting from OneTick build 20230711 onetick-py is also distributed as the part of the build,'
                ' and it will override onetick-py you may have installed from pip.'
                ' Use MAIN_ONE_TICK_DIR instead.'
            )

    try:
        from .. import __search_main_one_tick_dir
        main_one_tick_dirs = __search_main_one_tick_dir()
    except ImportError:
        # this exception will be raised if PYTHONPATH was used to specify the location of OneTick libraries
        # and onetick-py doesn't exist in the specified OneTick or have older version than these lines
        main_one_tick_dirs = None

    if _otq_import_ex is None:
        return

    # PY-1033: if we can't import onetick.query, but can import onetick.query_webapi
    # then we can just use it and don't need to raise any exception (even if OTP_WEBAPI is not set)
    try:
        import onetick.query_webapi
        __webapi__ = True
        return
    except ImportError:
        print('onetick.query_webapi is not available')

    if isinstance(_otq_import_ex, ImportError):
        raise ImportError(
            'The above exception is probably raised '
            'because your python version is unsupported in this OneTick build'
        ) from _otq_import_ex

    # numpy directory is not needed on the latest onetick versions
    # so checking it only if we couldn't import library
    missed_paths = missed_required or missed_optional

    if missed_paths or not main_one_tick_dirs:

        if not missed_paths:
            missed_paths = [ot_bin_path, ot_python_path, ot_numpy_path]

        missed_paths = ', '.join(
            "'" + os.path.join("<path-to-OneTick-dist>", p) + "'"
            for p in missed_paths
        )
        if __webapi__:
            raise ImportError(
                "Environment variable OTP_WEBAPI is set, but we can't import onetick.query_webapi.\n"
                "because we can't find it and related libraries.\n"
                "Please, make sure you installed it with pip.\n"
                "If you want to use OneTick distributed version\n"
                f", make sure that directories {missed_paths} exist and:\n"
                "- either those directories are specified in PYTHONPATH,\n"
                "- or <path-to-OneTick-dist> directory is specified in MAIN_ONE_TICK_DIR (recommended)."
            )

        raise ImportError(
            "We can't import onetick.query, because we can't find it and related libraries.\n"
            f"Please, make sure that directories {missed_paths} exist and:\n"
            "- either those directories are specified in PYTHONPATH,\n"
            "- or <path-to-OneTick-dist> directory is specified in MAIN_ONE_TICK_DIR (recommended)."
        ) from _otq_import_ex

    raise _otq_import_ex


def __get_missed_paths(env, *paths, found_callback=None):
    from pathlib import Path

    missed_paths = []
    env = env or ''
    global_python_path = env.split(os.pathsep)
    for p in paths:
        match = False
        for gb in global_python_path:
            if Path(gb).match(f"*/{p}"):
                match = True
                if found_callback:
                    found_callback(gb, p)
                break
        if not match:
            missed_paths.append(p)
    return missed_paths


def __set_onetick_path_variables():
    if __webapi__ or os.getenv("OTP_SKIP_OTQ_VALIDATION"):
        return
    import warnings
    import onetick.query as otq
    from pathlib import Path

    onetick_query_init = Path(otq.__file__)

    # pylint: disable=global-variable-undefined
    global __main_one_tick_dir__, __one_tick_bin_dir__, __one_tick_python_dir__
    __one_tick_python_dir__ = str(onetick_query_init.parents[2])
    __one_tick_bin_dir__ = str(onetick_query_init.parents[3])
    __main_one_tick_dir__ = str(onetick_query_init.parents[4])

    if os.name == 'nt' and __one_tick_bin_dir__ not in os.environ.get('PATH', '').split(os.pathsep):
        warnings.warn(
            f'OneTick {__one_tick_bin_dir__} is not specified in PATH environment variable.'
            ' Some functionality, e.g. using per-tick script, may be unavailable in this case.'
        )


__validate_onetick_query_integration()
__set_onetick_path_variables()

# -------------------------------------------- #

from ._stack_info import _modify_stack_info_in_onetick_query
_modify_stack_info_in_onetick_query()

# -------------------------------------------- #

from onetick.py import functions, aggregations
from onetick.py.functions import (
    concat, join, join_by_time, apply_query, apply, cut, qcut, merge, coalesce, corp_actions, format,
    join_with_aggregated_window
)
from onetick.py.types import (
    msectime, nsectime, string, nan, inf, datetime, date, dt, varstring,
    _int as int, uint, long, ulong, byte, short, decimal,
    Year, Quarter, Month, Week, Day, Hour, Minute, Second, Milli, Nano,
    default_by_type, timedelta,
)
from onetick.py.sources import (Tick, TTicks, Ticks, Orders, Trades, NBBO, Quotes, Query, CSV, ReadCache, ReadParquet,
                                Custom, query, Symbols, Empty, DataSource, LocalCSVTicks, SymbologyMapping,
                                ObSnapshot, ObSnapshotWide, ObSnapshotFlat, ObSummary, ObSize, ObVwap, ObNumLevels,
                                by_symbol, ODBC, SplitQueryOutputBySymbol, DataFile, PointInTime,
                                ReadSnapshot, ShowSnapshotList, FindSnapshotSymbols, ReadFromDataFrame)
from onetick.py.utils import adaptive, range, perf
from onetick.py.session import Session, TestSession, Config, Locator, HTTPSession
from onetick.py.servers import RemoteTS, LoadBalancing, FaultTolerance
from onetick.py.db import DB, RefDB
from onetick.py.db._inspection import databases, derived_databases
from onetick.py.cache import create_cache, delete_cache, modify_cache_config
from onetick.py import state
from onetick.py.core.source import Source, MetaFields
from onetick.py.core.multi_output_source import MultiOutputSource
from onetick.py.core.eval_query import eval  # noqa: it is ok to redefine built-in function
from onetick.py.core.per_tick_script import (
    # TODO: wanna access per_tick_script classes as otp.script.static and so on
    Static as static,
    TickDescriptorFields as tick_descriptor_fields,
    tick_list_tick,
    tick_set_tick,
    tick_deque_tick,
    dynamic_tick,
)
from onetick.py.callback import CallbackBase
from onetick.py.sql import SqlQuery
from onetick.py.run import run, run_async
from onetick.py.math import rand, now
from onetick.py.misc import (
    bit_and, bit_or, bit_at, bit_xor, bit_not,
    hash_code,
    get_symbology_mapping,
    get_onetick_version,
    get_username,
)
from onetick.py.core.column import Column
from onetick.py.core.column_operations.base import Operation, Expr as expr, Raw as raw, OnetickParameter as param
from onetick.py.core.per_tick_script import remote, Once, once, logf, throw_exception
from onetick.py.configuration import config
from onetick.py import oqd
from onetick.py.otq import otli
OneTickLib = otli.OneTickLib

try:
    from pandas import date_range
except Exception:
    pass

meta_fields = MetaFields()


def __getattr__(name):
    # actually, these values are not evaluated on module loading anymore,
    # so they can be used without fear
    # but let's raise deprecation warning anyway
    # to encourage users to use otp.config only
    defaults = dict(
        # lambdas are needed in case some config values are not set,
        # so we don't raise exception on getting any attribute
        DEFAULT_START_TIME=lambda: config.default_start_time,
        DEFAULT_END_TIME=lambda: config.default_end_time,
        DEFAULT_TZ=lambda: config.tz,
        DEFAULT_SYMBOL=lambda: config.default_symbol,
        DEFAULT_DB=lambda: config.default_db,
        DEFAULT_DB_SYMBOL=lambda: config.default_db_symbol,
    )
    if name in defaults:
        import warnings
        warnings.warn(
            f'Using otp.{name} is deprecated, use otp.config.* properties instead.',
            FutureWarning
        )
        return defaults[name]()
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


# aliases
funcs = functions  # type: ignore  # noqa
agg = aggregations  # type: ignore  # noqa


# set pandas default pandas options to show all columns
import pandas as _pd

_pd.set_option("display.max_columns", None)
_pd.set_option("display.expand_frame_repr", False)
_pd.set_option("max_colwidth", None)


from onetick.py.otq import otq as _otq
try:
    __build__ = str(_otq.OneTickLib.get_build_number())
except AttributeError:
    __build__ = 'webapi'

# initialize logger module and import public api
from .log import get_logger

del (_version,
     _otq,
     _pd,
     _modify_stack_info_in_onetick_query,
     otli)
