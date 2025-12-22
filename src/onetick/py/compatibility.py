import os
import warnings
from dataclasses import dataclass, astuple
from datetime import datetime
from typing import Optional

import pandas as pd
from packaging.version import parse as parse_version

import onetick.py as otp
from onetick.py.otq import otq, otli
from onetick.py.backports import cache


@dataclass
class OnetickVersion:
    is_release: bool
    release_version: Optional[str]
    update_number: Optional[int]
    build_number: int


@dataclass
class OnetickVersionFromServer(OnetickVersion):
    db: str
    context: str


def _parse_update_info(update_info: str) -> Optional[int]:
    if update_info == 'initial':
        return 0
    if update_info == 'precandidate':
        return None
    prefix = 'update'
    if not update_info.startswith(prefix):
        raise ValueError(f"Unexpected update info format: '{update_info}'")
    update_info = update_info[len(prefix):]
    return int(update_info)


def _compare_build_string_and_number(build_string: str, build_number: int,
                                     release_format_version: int, release_string: str):
    if release_format_version == 2:
        build_string += '120000'
    try:
        release_build_number = int(build_string)
    except Exception:
        raise ValueError(f"Unexpected build number '{build_string}' in release string '{release_string}'")

    if str(release_build_number) != str(build_number):
        raise ValueError(
            f"Different build numbers in OneTick release '{release_string}' and version: '{build_number}'"
        )


def _parse_release_string(release_string: str, build_number: int) -> OnetickVersion:
    # pylint: disable=W0707

    # Known release string formats:
    #  dev_build
    #  rel_1_23_20230605193357
    #  BUILD_initial_20230831120000
    #  BUILD_update1_20230831120000
    #  BUILD_pre_candidate_20240501000000
    #
    #  BUILD_rel_20241018_initial
    #  BUILD_rel_20241018_update3
    #  rel_1_25_initial
    #  rel_1_25_update1

    if release_string == 'dev_build':
        return OnetickVersion(False, None, None, build_number)

    release_type, *release_info, release_suffix = release_string.split('_')

    if not release_info:
        raise ValueError("No release info")

    try:
        update_number = _parse_update_info(release_suffix)
        release_format_version = 2
    except ValueError:
        update_number = None
        release_format_version = 1
        _compare_build_string_and_number(release_suffix, build_number, release_format_version, release_string)

    if release_type == 'rel':
        release_version_string = '.'.join(release_info)
        release_version = parse_version(release_version_string)
        return OnetickVersion(True, str(release_version), update_number, build_number)

    if release_type == 'BUILD':
        if release_format_version == 1:
            update_info = ''.join(release_info)
            update_number = _parse_update_info(update_info)
        if release_format_version == 2:
            assert release_info[0] == 'rel', 'Unknown release type'
            release_info = release_info[1:]
            build_string = ''.join(release_info)
            _compare_build_string_and_number(build_string, build_number, release_format_version, release_string)

        return OnetickVersion(False, None, update_number, build_number)

    raise ValueError(f"Unknown release type '{release_type}' in release string '{release_string}'")


def _get_locator_min_date(db_name, context):
    graph = otq.GraphQuery(otq.DbShowConfiguredTimeRanges(db_name=db_name).tick_type('ANY')
                           >> otq.Table(fields='long START_DATE, long END_DATE'))
    symbols = f'{db_name}::'
    result = otq.run(graph,
                     symbols=symbols,
                     # start and end times don't matter for this query, use some constants
                     start=datetime(2003, 12, 1),
                     end=datetime(2003, 12, 1),
                     # GMT, because start/end timestamp in locator are in GMT
                     timezone='GMT',
                     context=context)
    data = result.output(symbols).data
    first_date = data['START_DATE'][0]
    return datetime.fromtimestamp(first_date / 1000)


def _get_onetick_version(symbols, context, start, end):
    node = otq.TickGenerator(bucket_interval=0,
                             fields='BUILD=GET_ONETICK_VERSION(), RELEASE=GET_ONETICK_RELEASE()')
    graph = otq.GraphQuery(node.tick_type('DUMMY'))
    result = otq.run(graph,
                     symbols=symbols,
                     start=start,
                     end=end,
                     context=context,
                     timezone='UTC')
    return result


@cache
def get_onetick_version(db=None, context=None) -> OnetickVersionFromServer:
    """
    Get OneTick release version, as build number isn't enough
    to determine features available in OneTick.

    Returns tuple with release type, release version, update number and build number.

    Note
    ----
    The version is taken from the server by calling the query against this server.

    The server is specified by two global configuration parameters:
    :py:attr:`otp.config.context<onetick.py.configuration.Config.context>`
    and :py:attr:`otp.config.default_db<onetick.py.configuration.Config.default_db>`.
    By default, 'DEFAULT' context and 'LOCAL' database will be used.

    The check will not be accurate in all cases, as the user may use :func:`otp.run <onetick.py.run>`
    with different context or set symbol with different database in the end.

    Checking version correctly in all cases requires redesigning compatibility check system
    by moving it to the runtime level -- checking version inside the graph.
    But for now this method is the best we can do.
    """
    s = None
    if not os.environ.get('ONE_TICK_CONFIG') and not otq.webapi:
        s = otp.Session()
    else:
        _ = otli.OneTickLib()

    # if otp.config.default_db is set, then we use it to check compatibility
    # otherwise we use LOCAL database available everywhere
    db = db or otp.config.get('default_db', 'LOCAL')
    symbols = f'{db}::'
    context = context or otp.config.context

    try:
        # let's try some default time range first
        start = end = datetime(2003, 12, 1)
        result = _get_onetick_version(symbols, context, start, end)
    except Exception:
        if db != 'LOCAL':
            # for real db we need to set time range correctly
            # otherwise we may get error "Database locator has a gap"
            start = end = _get_locator_min_date(db, context)
            result = _get_onetick_version(symbols, context, start, end)
        else:
            raise
    finally:
        if s:
            s.close()

    build_number = result[symbols]["BUILD"][0]
    release_string = result[symbols]["RELEASE"][0]

    try:
        onetick_version = _parse_release_string(release_string, build_number=build_number)
        return OnetickVersionFromServer(*astuple(onetick_version), db, context)  # type: ignore[call-arg]
    except Exception as err:
        warnings.warn(f"Unknown release format string: '{release_string}'.\n{err}")
        return OnetickVersionFromServer(False, None, None, build_number, db, context)


def _is_min_build_or_version(min_release_version=None,
                             min_release_version_build_number=None,
                             min_build_number=None,
                             min_update_number=None,
                             throw_warning=False,
                             feature_name=None,
                             db=None,
                             context=None):
    """
    Check if current OneTick version is at least min_release_version.
    When using not released version, check if build number is at least min_build_number.
    """
    if not min_build_number:
        raise ValueError("min_build_number parameter is required")

    from onetick.py.configuration import config
    if config.disable_compatibility_checks:
        return True

    onetick_version = get_onetick_version(db=db, context=context)
    if not onetick_version.is_release:
        has = onetick_version.build_number >= min_build_number
        if (
            min_update_number is not None
            and onetick_version.update_number is not None
            and onetick_version.build_number == min_build_number
        ):
            has = has and onetick_version.update_number >= min_update_number
    else:
        if not min_release_version:
            # onetick is on release, but feature is not released yet
            has = False
        else:
            has = parse_version(str(onetick_version.release_version)) >= parse_version(str(min_release_version))
            if min_release_version_build_number:
                has = has and onetick_version.build_number >= min_release_version_build_number

    if not has and throw_warning:
        msg = f"OneTick {onetick_version} does not support {feature_name} which is supported "
        if min_release_version is not None:
            msg += f"starting from release {min_release_version} or "
        msg += f"starting from dev build {min_build_number} "
        if min_update_number is not None:
            msg += f"update {min_update_number}"
        warnings.warn(msg)
    return has


def _add_version_info_to_exception(exc):
    """
    Add onetick-py and onetick version numbers to exception message.
    """
    onetick_version = get_onetick_version()
    if not onetick_version.is_release:
        message = f'OneTick {onetick_version.build_number}'
    else:
        message = f'OneTick {onetick_version.release_version} ({onetick_version.build_number})'
    message = f'onetick-py=={otp.__version__}, {message}'
    if exc.args:
        message = str(exc.args[0]) + os.linesep + message
    exc.args = (message, *exc.args[1:])
    return exc


def has_max_expected_ticks_per_symbol(throw_warning=False):
    """Check if otq.run() has max_expected_ticks_per_symbol parameter.

    20220531: Implemented 0027950: OneTick numpy API and onetick.query python API
    should expose parameter max_expected_ticks_per_symbol
    """
    has = _is_min_build_or_version(1.23, 20221025023710,
                                   20220714120000,
                                   throw_warning=throw_warning,
                                   feature_name="otp.run parameter 'max_expected_ticks_per_symbol'")
    return has


def has_password_param(throw_warning=False):
    """Check if otq.run() has password parameter.

    Implemented 0027216: onetick.query does not expose parameter password
    """
    has = _is_min_build_or_version(1.23, None,
                                   20220327120000,
                                   throw_warning=throw_warning,
                                   feature_name="otp.run parameter 'password'")
    return has


def has_timezone_parameter(throw_warning=False):
    """
    Fixed 0027499: In onetick.query, _convert_time_to_YYYYMMDDhhmmss method should accept timezone parameter
    """
    has = _is_min_build_or_version(1.23, None,
                                   20220519120000,
                                   throw_warning=throw_warning,
                                   feature_name="convert_time_to_YYYYMMDDhhmmss parameter 'timezone'")
    return has


def has_query_encoding_parameter(throw_warning=False):
    """
    0027383: In onetick.query, run method should support parameter "encoding"
    """
    has = _is_min_build_or_version(1.23, None,
                                   20220327120000,
                                   throw_warning=throw_warning,
                                   feature_name="query encoding parameter")
    return has


def is_supported_agg_option_price():
    """0029945: OPTION_PRICE EP produces an exception when used in COMPUTE and explicitly set to its default value
    """
    return _is_min_build_or_version(1.23, 20230314061408,
                                    20230316120000)


def is_supported_otq_run_password():
    """Implemented 0027216: onetick.query does not expose parameter password
    """
    return _is_min_build_or_version(1.23, None,
                                    20220327120000)


def is_supported_stack_info():
    """Fixed 0028824: setting otq.API_CONFIG.SHOW_STACK_INFO=1 does not cause location of an EP in
    python code to be added to the text of exception
    """
    onetick_version = get_onetick_version()
    if onetick_version.build_number == 20240205120000:
        # BDS-345
        return False
    return _is_min_build_or_version(1.24, None,
                                    20221111120000)


def is_supported_num_distinct():
    """???
    """
    return _is_min_build_or_version(1.23, None,
                                    20220913120000)


def is_supported_rename_fields_symbol_change():
    """???
    """
    return _is_min_build_or_version(1.24, 20230316120000,
                                    20230316120000)


def is_supported_new_ob_snapshot_behavior():
    """???
    """
    return _is_min_build_or_version(1.24, 20230711120000,
                                    20230711120000)


def is_supported_where_clause_for_back_ticks():
    """Implemented 0028064: add WHERE_CLAUSE_FOR_BACK_TICKS to PASSTHROUGH EP
    """
    return _is_min_build_or_version(1.23, None,
                                    20220714120000)


def is_supported_bucket_units_for_tick_generator(throw_warning=False):
    """Implemented 0029117: Add BUCKET_INTERVAL_UNITS to TICK_GENERATOR EP
    """
    feature_name = "parameter 'bucket_units' for otp.Tick"
    return _is_min_build_or_version(1.24, None,
                                    20230112120000,
                                    throw_warning=throw_warning, feature_name=feature_name)


def is_supported_varstring_in_get_string_value():
    """Implemented 0030763: GET_STRING_VALUE method on tick objects should support also varstring field types
    """
    return _is_min_build_or_version(1.24, None,
                                    20230711120000)


def is_supported_uint_numpy_interface():
    # 20220216: Fixed 0027130: onetick numpy interface should preserve
    # field type of unsigned fields (currently they become signed)
    return _is_min_build_or_version(1.23, None,
                                    20220327120000)


def is_supported_otq_reference_data_loader():
    """???
    """
    return _is_min_build_or_version(1.23, 20220519120000, 20220519120000)


def is_supported_nsectime_tick_set_eval():
    # BDS-321
    # Fixed 0031588: Ticks in TICK_SET populated by eval , loose nanosecond precision
    return _is_min_build_or_version(1.24, None,
                                    20231108120000)


def is_supported_otq_ob_summary(throw_warning=False):
    """
    20220325: Implemented 0027258: Add EP OB_SUMMARY, which will combine functionality of OB_SIZE, OB_NUM_LEVELS, and
    OB_VWAP, and add new features
    """
    has = _is_min_build_or_version(1.23, None,
                                   20220327120000,
                                   throw_warning=throw_warning,
                                   feature_name="onetick.query OB_SUMMARY support")
    return has


def is_supported_reload_locator_with_derived_db():
    # See tasks PY-388, BDS-334.
    # Was fixed in update1_20231108120000.
    # 0032118: OneTick processes that refresh their locator may crash
    #          if they make use databases derived from the dbs in that locator
    return _is_min_build_or_version(1.24, None,
                                    20231108120000, min_update_number=1)


def is_supported_large_ints_empty_interval():
    # BDS-333
    # Was fixed in update1_20231108120000.
    # 0032093: when EXPECT_LARGE_INTS isn't 'false, HIGH,LOW,FIRST, and LAST EPs should show integer values,
    #          not doubles, when input is empty
    return _is_min_build_or_version(1.24, None,
                                    20231108120000, min_update_number=1)


def is_start_time_as_minimum_start_date_supported():
    # 20220203: Fixed 0027114: Getting error when query start time is equal
    # to <minimum_start_date> parameter in access control file.
    return _is_min_build_or_version(1.23, None,
                                    20220211120000)


def is_supported_list_empty_derived_databases():
    # PY-856, BDS-323
    # Was fixed in BUILD_initial_20240205120000
    # 20240130: Fixed 0031783: onetick.query crashes when a query returned no ticks,
    # but produced a tick descriptor with string fields of 0 size
    return _is_min_build_or_version(1.24, 20240524004422,
                                    20240205120000, min_update_number=0)


def is_odbc_query_supported():
    # no record found in Release Notes
    # but grep shows that it was added in 20231108-0 build and 1.24 release
    return _is_min_build_or_version(1.24, None,
                                    20231108120000)


def is_event_processor_repr_upper():
    if otq.webapi:
        return True
    return _is_min_build_or_version(1.25, None,
                                    20240205120000, min_update_number=0)


def is_date_trunc_fixed():
    # Fixed 0032253: DATE_TRUNC function returns wrong answer in case of daylight saving time
    return _is_min_build_or_version(1.25, None,
                                    20240205120000, min_update_number=0)


def is_supported_end_time_in_modify_state_var_from_query():
    # BDS-335 [onetick 0032075]: End time for the called query in MODIFY_STATE_VAR_FROM_QUERY is set incorrectly
    # Was fixed in update1_20231108120000.
    return _is_min_build_or_version(1.24, None,
                                    20231108120000, min_update_number=1)


def is_supported_modify_state_var_from_query():
    return hasattr(otq, 'ModifyStateVarFromQuery')


def is_sha2_hashing_supported():
    return _is_min_build_or_version(1.23, 20220714120000,
                                    20220714120000)


def is_supported_join_with_aggregated_window():
    return hasattr(otq, 'JoinWithAggregatedWindow')


def is_existing_fields_handling_supported():
    # 20220207: Implemented 0027076:
    # ADD_FIELDS should support parameter EXISTING_FIELDS_HANDLING with values THROW and OVERRIDE
    return _is_min_build_or_version(1.23, None,
                                    20220211120000)


def is_supported_per_cache_otq_params(throw_warning=False):
    return _is_min_build_or_version(1.23, 20220714120000, 20220714120000, throw_warning=throw_warning)


def is_option_price_theta_value_changed():
    # 20240221: Fixed 0032506:
    # Theta value from OPTION_PRICE EP is sometimes wrong.
    return _is_min_build_or_version(1.24, 20240306230425,
                                    20240330120000)


def is_fixed_modify_state_var_from_query():
    # 20230913: Fixed 0031340:
    # MODIFY_STATE_VAR_FROM_QUERY does not properly propagate initialization events
    # which may cause crash in destination EPs
    return _is_min_build_or_version(1.24, None,
                                    20231108120000)


def is_supported_next_in_join_with_aggregated_window(throw_warning=False, feature_name=None):
    # 20231111: Fixed 0031756:
    # Queries with JOIN_WITH_AGGREGATED_WINDOW crash
    # if it is followed by Aggregation EPs referencing fields in PASS_SOURCE
    return _is_min_build_or_version(1.24, None,
                                    20231108120000, min_update_number=1,
                                    throw_warning=throw_warning, feature_name=feature_name)


def is_min_db_start_criteria_works_correctly():
    # Works from 1.23
    # 20220512: Implemented 0027673:
    # SHOW_SYMBOLOGY_LIST should not throw start/end date criteria violation exceptions
    return _is_min_build_or_version(1.23, 20220519120000,
                                    20220519120000)


def is_repeat_with_field_name_works_correctly():
    # Works before 20230522-0, on 20230522-2/4 and after 20230711
    # 20230705: Fixed 0030642:
    # built-in REPEAT function works incorrectly when passed a field name
    # as opposed to the constant string, starting rel_20230522
    onetick_version = get_onetick_version()

    if (
        onetick_version.build_number < 20230522120000 or
        onetick_version.build_number == 20230522120000 and onetick_version.update_number >= 2 or
        onetick_version.build_number >= 20230711120000
    ):
        return True

    return False


def is_duplicating_quotes_not_supported():
    # 20240329: Fixed 0032754:
    # Logical expressions should trigger error when duplicate single(or double) quote
    # is directly followed or preceded by some name
    return _is_min_build_or_version(1.25, None,
                                    20240330120000)


def are_quotes_in_query_params_supported():
    # Fixed 0033318: onetick.query package passes quoted otq parameters without quotes
    return _is_min_build_or_version(None, None, 20240530120000, min_update_number=1)


def is_concurrent_cache_is_fixed():
    # PY-1009, BDS-365
    # 20240802: Fixed 0033806: Dynamic caches created with PER_CACHE_OTQ_PARAMS in READ_CACHE EP
    # still lack synchronization in multi-core environment.
    return _is_min_build_or_version(1.24, 20240806024006,
                                    20240812120000)


def is_apply_rights_supported(throw_warning=False):
    # 20191026: Fixed 0021898: CORP_ACTIONS EP does not expose parameter APPLY_RIGHTS
    return _is_min_build_or_version(1.22, 20220128183755,
                                    20220714120000,
                                    throw_warning=throw_warning)


def is_write_parquet_directories_fixed():
    # 20240609: Fixed 0033342: WRITE_TO_PARQUET EP should not produce directories in non-partitioned mode
    return _is_min_build_or_version(1.25, 20250209162722,
                                    20240530120000, min_update_number=1)


def is_zero_concurrency_supported():
    # 20240312: Implemented 0032157:
    # Add support for automatic assignment of concurrency to the queries, if concurrency is set to special value '0'
    return _is_min_build_or_version(None, None,
                                    20240501000000)


def is_get_query_property_flag_supported():
    # 20231205: Implemented 0031857:
    # create flag for GET_QUERY_PROPERTY and GET_QUERY_PROPERTIES to return also special query properties
    return _is_min_build_or_version(None, None,
                                    20240205120000)


def is_all_fields_when_ticks_exit_window_supported():
    # 20231230: Implemented 0031741:
    # ALL_FIELDS_FOR_SLIDING aggregation parameter should support value WHEN_TICKS_EXIT_WINDOW
    # (check out "Parameters common go generic aggregations" section in OneTick Event Processors' guide).
    return _is_min_build_or_version(1.24, 20240116201311,
                                    20240205120000)


def is_first_ep_skip_tick_if_supported():
    # 20240130: Implemented 0032167: Add SKIP_TICK_IF parameter for FIRST EP
    return _is_min_build_or_version(None, None,
                                    20240205120000)


def is_last_ep_fwd_fill_if_supported():
    # 20220708: Implemented 0028111: LAST EP should have parameter FWD_FILL_IF
    return _is_min_build_or_version(1.23, 20221025023710,
                                    20220714120000)


def is_diff_show_matching_ticks_supported():
    return _is_min_build_or_version(None, None,
                                    20240812120000)


def is_diff_non_decreasing_value_fields_supported():
    # 20240620: Implemmented 0033285: extend DIFF EP to support matching ticks with non-identical primary timestamps
    return _is_min_build_or_version(None, None,
                                    20240812120000)


def is_standardized_moment_supported():
    # 20240513: Implemented 0032822: Add STANDARDIZED_MOMENT EP, to compute STANDARDIZED_MOMENT of Nth degree
    return _is_min_build_or_version(None, None,
                                    20240530120000)


def is_supported_pnl_realized():
    # No info, however onetick.query missing required EP class
    return _is_min_build_or_version(1.24, 20240116201311,
                                    20231108120000)


def is_supported_pnl_realized_buy_sell_flag_bin():
    # 20240429: Implemented 0032683: Enhance PNL_REALIZED EP for BUY_SELL_FLAG field to support also 0 and 1
    return _is_min_build_or_version(None, None,
                                    20240530120000)


def is_data_file_query_supported():
    # 20240311: Implemented 0032631: Implement ARROW_FILE_QUERY EP
    return _is_min_build_or_version(None, None,
                                    20240330120000)


def is_data_file_query_symbology_supported(throw_warning=False, feature_name=None):
    # 20240603: Implemented 0033111: DATA_FILE_QUERY EP should support parameter SYMBOLOGY
    return _is_min_build_or_version(None, None,
                                    20240812120000,
                                    throw_warning=throw_warning, feature_name=feature_name)


def is_supported_point_in_time(throw_warning=False, feature_name=None):
    # 20240323: Implemented 0032255: Add POINT_IN_TIME EP
    # 20240408: Implemented 0032821: enhance POINT_IN_TIME EP to support getting points in time
    # from the input time series, when TIMES parameter is not set.

    # POINT_IN_TIME EP supported since 20240330120000, but it is not very stable in this first version,
    # so we decided to support it since the next version
    return _is_min_build_or_version(1.25, 20241209135932,
                                    20240530120000,
                                    throw_warning=throw_warning, feature_name=feature_name)


def is_find_value_for_percentile_supported():
    # 20240527: Implemented 0032752: Add EP FIND_VALUE_FOR_PERCENTILE
    return _is_min_build_or_version(None, None,
                                    20240530120000)


def is_derived_databases_crash_fixed():
    # 20240130: Fixed 0032118: OneTick processes that refresh their locator
    # may crash if they make use of databases derived from the dbs in that locator
    return _is_min_build_or_version(1.24, 20240524004422,
                                    20240205120000)


def is_character_present_characters_field_fixed():
    # 20230705: Fixed 0030747: CHARACTER_PRESENT EP may produce non-deterministic results when
    # CHARACTERS_FIELD is specified
    # 20230705: Fixed 0030748: CHARACTER_PRESENT EP must ignore 0-bytes in the values of a tick field named
    # by the CHARACTERS_FIELD parameter
    return _is_min_build_or_version(1.24, 20240116201311,
                                    20230711120000)


def is_supported_estimate_ts_delay():
    # 20240924: Implemented 0033286: Add EP ESTIMATE_TS_DELAY
    return _is_min_build_or_version(None, None,
                                    20241002120000)


def is_percentile_bug_fixed():
    # 20241209: Implemented 0034428: In FIND_VALUE_FOR_PERCENTILE EP, rename SHOW_PERCENTILE_AS to COMPUTE_VALUE_AS
    # NOTE: also has fix for FIRST_VALUE_WITH_GE_PERCENTILE and PERCENTILE=100 (was N/A, but must be biggest value)
    return _is_min_build_or_version(None, None,
                                    20241220120000)


def is_limit_ep_supported():
    # Implemented 0034293: LIMIT ep
    return (
        hasattr(otq, 'Limit') and
        _is_min_build_or_version(1.25, 20241229055942,
                                 20241018120000, min_update_number=1)
    )


def is_limit_tick_offset_supported():
    # Implemented OTDEV-37257: LIMIT EP should support TICK_OFFSET parameter
    return (
        is_limit_ep_supported() and
        'tick_offset' in otq.Limit.Parameters.list_parameters() and
        _is_min_build_or_version(None, None,
                                 20251010120000, min_update_number=2)
    )


def is_prefer_speed_over_accuracy_supported(**kwargs):
    return _is_min_build_or_version(1.25, 20241229055942,
                                    20241018120000, min_update_number=3,
                                    **kwargs)


def is_ob_virtual_prl_and_show_full_detail_supported():
    # 20230705: Implemented 0030536: VIRTUAL_OB EP should support PRL output format and should require it
    # for SHOW_FULL_DETAIL case
    return _is_min_build_or_version(1.24, 20240116201311,
                                    20230711120000)


def is_per_tick_script_boolean_problem():
    # strange problem, couldn't reproduce it anywhere except a single onetick release
    version = get_onetick_version()
    return version.release_version == '1.22'


def is_symbol_time_override_fixed():
    # Fixed 0028044: after rel_20220519, symbol_date=0 in the otq file overrides
    # symbol_date expressions and _SYMBOL_TIME otq parameter
    return _is_min_build_or_version(1.23, 20221025023710,
                                    20220714120000)


def is_database_view_schema_supported():
    # Implemented 0034115: DB/SHOW_TICK_TYPES should return non-empty schema
    # for View queries ending in single TABLE EP with type specified for each field
    return _is_min_build_or_version(1.25, 20241229055942,
                                    20241001205534)


def is_native_plus_zstd_supported():
    # 20220204: Implemented 0026827: memory and accelerator databases should support ZSTD, NATIVE_PLUS_ZSTD,
    # and per-tick ZSTD compression
    return _is_min_build_or_version(1.23, 20220913120000,
                                    20220211120000)


def is_save_snapshot_database_parameter_supported():
    # 20220929: Implemented 0028559: Update SAVE_SNAPSHOT to specify output database
    client_support = 'database' in otq.SaveSnapshot.Parameters.list_parameters()
    server_support = _is_min_build_or_version(1.23, 20230605193357,
                                              20221111120000)
    return client_support and server_support


def is_join_with_snapshot_snapshot_fields_parameter_supported():
    # 20240422: Implemented 0032910: add parameter SNAPSHOT_FIELDS to JOIN_WITH_SNAPSHOT EP
    return _is_min_build_or_version(1.25, 20241229055942,
                                    20240530120000)


def is_multi_column_generic_aggregations_supported():
    # Implementation of tick aggregations in COMPUTE requires to use RENAME_FIELDS to make correct output schema.
    # However, if we place it inside generic aggregation inside COMPUTE, next error occur on old OneTick versions:
    # ERR_06708004ERCOM: Event processor RENAME_FIELDS does not currently support dynamic symbol changes.
    return _is_min_build_or_version(1.24, 20240116201311,
                                    20230315095103)


def is_max_concurrency_with_webapi_supported():
    # 0036758: in onetick.query_webapi: max_concurrency is not being saved in otq file when set on otq.Query
    # 0036759: in onetick.query_webapi:
    # it's not possible to pass max_concurrency 0 in method otq.run when using otq file
    return _is_min_build_or_version(None, None,
                                    20250727120000, min_update_number=3)


def is_nanoseconds_fixed_in_run():
    # 0032309: onetick.query_webapi should preserve nanosecond timestamps
    return _is_min_build_or_version(1.24, 20240116201311,
                                    20240205120000)


def is_correct_timezone_used_in_otq_run():
    # Fixed 0027500: In onetick.query, OtqFile.save_to_file method uses incorrect timezone
    return _is_min_build_or_version(1.23, 20221025023710,
                                    20220519120000)


def is_ilike_supported():
    # 20250423: Implemented 0035414: Add support of ILIKE in PER_TICK_SCRIPT EP
    # 20250423: Implemented 0035412: Add support of ILIKE in logical expressions
    # 20250423: Implemented 0035413: Add support of ILIKE in SQL
    return _is_min_build_or_version(None, None,
                                    20250510120000)


def is_include_market_order_ticks_supported(**kwargs):
    # Implemented 0031478: OB_SNAPSHOT... and OB_SUMMARY EPs
    # should support parameter INCLUDE_MARKET_ORDER_TICKS (false by default)
    return _is_min_build_or_version(1.25, 20241229055942,
                                    20240812120000, min_update_number=2,
                                    **kwargs)


def is_join_with_query_symbol_time_otq_supported():
    # 20241209: Fixed 0034770: hours/minutes/seconds part of otq parameter _SYMBOL_TIME, expressed in
    # milliseconds since 1970/01/01 00:00:00 GMT, is ignored
    # 20250219: Implemented 0035092: passing otq param _SYMBOL_TIME should be just like setting symbol_date
    # to the equivalent value, except in YYYYMMDDhhmmss format
    return _is_min_build_or_version(None, None,
                                    20250227120000)


def is_show_db_list_show_description_supported():
    # 20240301: Implemented 0032320: 0032320: SHOW_DB_LIST should have a new EP parameter, SHOW_DESCRIPTION
    # However on 20240330 builds it returns SHOW_DESCRIPTION column instead of DESCRIPTION
    return _is_min_build_or_version(1.25, 20241229055942,
                                    20240501000000)


def is_symbols_prepend_db_name_supported():
    # 20250924: Implemented 0036753: FIND_DB_SYMBOLS should have EP parameter PREPEND_DB_NAME (true by default)
    return hasattr(otq.FindDbSymbols.Parameters, 'prepend_db_name') and _is_min_build_or_version(
        None, None, 20251010120000,
    )


def is_diff_show_all_ticks_supported():
    # 20250919: Implemented 0036784: Add SHOW_ALL_TICKS(false by default) ep parameter to DIFF EP.
    return hasattr(otq.Diff.Parameters, 'show_all_ticks') and _is_min_build_or_version(
        None, None, 20251010120000,
    )


def is_max_spread_supported():
    # 20250819: Implemented 0036522: The book EPs that support parameter MAX_DEPTH_FOR_PRICE
    # should also support parameter MAX_SPREAD
    return _is_min_build_or_version(None, None,
                                    20251010120000)


def is_not_fixed_bds_484():
    # BDS-484: seems like timezone is ignored in otq.run in some cases
    return _is_min_build_or_version(None, None,
                                    20251010120000, min_update_number=2)


def is_webapi_access_token_scope_supported():
    # 20251030: Fixed OTDEV-37063: onetick.query_webapi.get_access_token method must take scope as a parameter
    return _is_min_build_or_version(None, None,
                                    20251010120000, min_update_number=2)
