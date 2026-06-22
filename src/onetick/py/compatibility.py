import os
import inspect
import warnings
from dataclasses import dataclass, astuple
from datetime import datetime, timezone as dt_timezone
from typing import Optional
from functools import cache

from packaging.version import parse as parse_version

import onetick.py as otp
from onetick.py.otq import otq, otli, pyomd


@dataclass
class OnetickVersion:
    release_string: str
    release_version: Optional[str]
    update_number: Optional[int]
    build_number: int

    @property
    def is_release(self):
        return self.release_version is not None

    def get_compact_string(self):
        return f'{self.release_string} ({self.build_number})'


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
        return OnetickVersion(release_string, None, None, build_number)

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
        return OnetickVersion(release_string, str(release_version), update_number, build_number)

    if release_type == 'BUILD':
        if release_format_version == 1:
            update_info = ''.join(release_info)
            update_number = _parse_update_info(update_info)
        if release_format_version == 2:
            assert release_info[0] == 'rel', 'Unknown release type'
            release_info = release_info[1:]
            build_string = ''.join(release_info)
            _compare_build_string_and_number(build_string, build_number, release_format_version, release_string)

        return OnetickVersion(release_string, None, update_number, build_number)

    raise ValueError(f"Unknown release type '{release_type}' in release string '{release_string}'")


def _get_locator_intervals(db_name, context) -> list[tuple[datetime, datetime]]:
    graph = otq.GraphQuery(otq.DbShowConfiguredTimeRanges(db_name=db_name).tick_type('ANY')
                           >> otq.Table(fields='long START_DATE, long END_DATE'))
    symbols = f'{db_name}::'

    # setting this is important so we don't get access error
    qp = pyomd.QueryProperties()
    qp.set_property_value('IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE', 'TRUE')

    result = otq.run(graph,
                     symbols=symbols,
                     # start and end times don't matter for this query, use some constants
                     start=datetime(2003, 12, 1),
                     end=datetime(2003, 12, 1),
                     # timezone is irrelevant, because times are returned as epoch numbers
                     timezone='UTC',
                     query_properties=qp,
                     context=context)
    data = result.output(symbols).data
    if not data:
        raise RuntimeError(f"Database '{db_name}' doesn't have locations")
    return [
        (
            datetime.fromtimestamp(data['START_DATE'][i] / 1000, dt_timezone.utc).replace(tzinfo=None),
            datetime.fromtimestamp(data['END_DATE'][i] / 1000, dt_timezone.utc).replace(tzinfo=None),
        )
        for i in range(len(data['START_DATE']))
    ]


def _get_onetick_version(db_name, context, start, end) -> dict:
    node = otq.TickGenerator(bucket_interval=0,
                             fields='BUILD=GET_ONETICK_VERSION(), RELEASE=GET_ONETICK_RELEASE()')
    graph = otq.GraphQuery(node.tick_type('DUMMY'))
    symbols = f'{db_name}::'

    # setting this is important so we don't get access error
    qp = pyomd.QueryProperties()
    qp.set_property_value('IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE', 'TRUE')

    result = otq.run(graph,
                     symbols=symbols,
                     start=start,
                     end=end,
                     context=context,
                     query_properties=qp,
                     timezone='UTC')
    data = result.output(symbols).data
    if not data:
        raise RuntimeError(f"Can't get OneTick version from database '{db_name}'")
    return data


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
    db_name = db or otp.config.get('default_db', 'LOCAL')
    context = context or otp.config.context

    try:
        if db_name == 'LOCAL':
            # for LOCAL db any date will do
            start = end = datetime(2003, 12, 1)
            result_data = _get_onetick_version(db_name, context, start, end)
        else:
            # for real db we need to set time range correctly
            # otherwise we may get error "Database locator has a gap"
            locator_intervals = _get_locator_intervals(db_name, context)
            for i, (start, end) in enumerate(locator_intervals):
                try:
                    result_data = _get_onetick_version(db_name, context, start, end)
                    break
                except Exception as e:
                    if i < len(locator_intervals) - 1:
                        continue
                    else:
                        raise e
    finally:
        if s:
            s.close()

    build_number = int(result_data["BUILD"][0])
    release_string = result_data["RELEASE"][0]

    try:
        onetick_version = _parse_release_string(release_string, build_number=build_number)
        return OnetickVersionFromServer(*astuple(onetick_version), db_name, context)  # type: ignore[call-arg]
    except Exception as err:
        warnings.warn(f"Unknown release format string: '{release_string}'.\n{err}")
        return OnetickVersionFromServer(release_string, None, None, build_number, db_name, context)


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
    if otp.__webapi__ and hasattr(otq, '__version__'):
        onetick_version = f'{otq.__version__} [webapi]'
    else:
        onetick_version = get_onetick_version(db='LOCAL').get_compact_string()

    message = f'onetick-py=={otp.__version__}, OneTick {onetick_version}'
    if exc.args:
        message = str(exc.args[0]) + os.linesep + message
    exc.args = (message, *exc.args[1:])
    return exc


def _has_max_expected_ticks_per_symbol():
    """
    Check if otq.run() has max_expected_ticks_per_symbol parameter.

    20220531: Implemented 0027950: OneTick numpy API and onetick.query python API
    should expose parameter max_expected_ticks_per_symbol
    """
    return 'max_expected_ticks_per_symbol' in inspect.signature(otq.run).parameters


def _has_password_param():
    """
    Check if otq.run() has password parameter.

    Implemented 0027216: onetick.query does not expose parameter password
    """
    return 'password' in inspect.signature(otq.run).parameters


def _has_query_encoding_parameter():
    """
    0027383: In onetick.query, run method should support parameter "encoding"
    """
    return 'encoding' in inspect.signature(otq.run).parameters


def _is_supported_num_distinct():
    """
    # OneTick build >= 20220913120000
    """
    return hasattr(otq, 'NumDistinct')


def _is_supported_where_clause_for_back_ticks():
    """
    Implemented 0028064: add WHERE_CLAUSE_FOR_BACK_TICKS to PASSTHROUGH EP
    """
    return 'where_clause_for_back_ticks' in otq.Passthrough.Parameters.list_parameters()


def _is_supported_bucket_units_for_tick_generator():
    """
    Implemented 0029117: Add BUCKET_INTERVAL_UNITS to TICK_GENERATOR EP
    """
    return 'bucket_interval_units' in otq.TickGenerator.Parameters.list_parameters()


def _is_supported_otq_ob_summary():
    """
    20220325: Implemented 0027258: Add EP OB_SUMMARY, which will combine functionality of OB_SIZE, OB_NUM_LEVELS, and
    OB_VWAP, and add new features
    """
    return hasattr(otq, 'ObSummary')


def _is_odbc_query_supported():
    # no record found in Release Notes
    # but grep shows that it was added in 20231108-0 build and 1.24 release
    return hasattr(otq, 'Omd_odbcQuery')


def _is_supported_modify_state_var_from_query():
    return hasattr(otq, 'ModifyStateVarFromQuery')


def _is_supported_join_with_aggregated_window():
    return hasattr(otq, 'JoinWithAggregatedWindow')


def _is_existing_fields_handling_supported():
    # 20220207: Implemented 0027076:
    # ADD_FIELDS should support parameter EXISTING_FIELDS_HANDLING with values THROW and OVERRIDE
    return 'existing_fields_handling' in otq.AddFields.Parameters.list_parameters()


def _is_apply_rights_supported():
    # 20191026: Fixed 0021898: CORP_ACTIONS EP does not expose parameter APPLY_RIGHTS
    return 'apply_rights' in otq.CorpActions.Parameters.list_parameters()


# TODO: code and tests
def is_zero_concurrency_supported():
    # 20240312: Implemented 0032157:
    # Add support for automatic assignment of concurrency to the queries, if concurrency is set to special value '0'
    return _is_min_build_or_version(None, None,
                                    20240501000000)


def _is_first_ep_skip_tick_if_supported():
    # 20240130: Implemented 0032167: Add SKIP_TICK_IF parameter for FIRST EP
    return 'skip_tick_if' in otq.First.Parameters.list_parameters()


def _is_last_ep_fwd_fill_if_supported():
    # 20220708: Implemented 0028111: LAST EP should have parameter FWD_FILL_IF
    return 'fwd_fill_if' in otq.Last.Parameters.list_parameters()


def _is_diff_show_matching_ticks_supported():
    return 'show_matching_ticks' in otq.Diff.Parameters.list_parameters()


def _is_diff_non_decreasing_value_fields_supported():
    # 20240620: Implemmented 0033285: extend DIFF EP to support matching ticks with non-identical primary timestamps
    return 'non_decreasing_value_fields' in otq.Diff.Parameters.list_parameters()


def _is_standardized_moment_supported():
    # 20240513: Implemented 0032822: Add STANDARDIZED_MOMENT EP, to compute STANDARDIZED_MOMENT of Nth degree
    return hasattr(otq, 'StandardizedMoment')


def _is_supported_pnl_realized():
    # No info, however onetick.query missing required EP class
    return hasattr(otq, 'PnlRealized')


def _is_data_file_query_supported():
    # 20240311: Implemented 0032631: Implement ARROW_FILE_QUERY EP
    return hasattr(otq, 'DataFileQuery')


def _is_data_file_query_symbology_supported(throw_warning=False, feature_name=None):
    # 20240603: Implemented 0033111: DATA_FILE_QUERY EP should support parameter SYMBOLOGY
    return _is_data_file_query_supported() and 'symbology' in otq.DataFileQuery.Parameters.list_parameters()


def _is_supported_point_in_time():
    # 20240323: Implemented 0032255: Add POINT_IN_TIME EP
    # 20240408: Implemented 0032821: enhance POINT_IN_TIME EP to support getting points in time
    # from the input time series, when TIMES parameter is not set.

    # POINT_IN_TIME EP supported since 20240330120000, but it is not very stable in this first version,
    # so we decided to support it since the next version
    return hasattr(otq, 'PointInTime')


def _is_find_value_for_percentile_supported():
    # 20240527: Implemented 0032752: Add EP FIND_VALUE_FOR_PERCENTILE
    return hasattr(otq, 'FindValueForPercentile')


def _is_supported_estimate_ts_delay():
    # 20240924: Implemented 0033286: Add EP ESTIMATE_TS_DELAY
    return hasattr(otq, 'EstimateTsDelay')


def _is_limit_ep_supported():
    # Implemented 0034293: LIMIT ep
    return hasattr(otq, 'Limit')


def _is_limit_tick_offset_supported():
    # Implemented OTDEV-37257: LIMIT EP should support TICK_OFFSET parameter
    return _is_limit_ep_supported() and 'tick_offset' in otq.Limit.Parameters.list_parameters()


def _is_limit_apply_across_symbols_supported():
    return _is_limit_ep_supported() and 'apply_across_symbols' in otq.Limit.Parameters.list_parameters()


def _is_ob_virtual_prl_and_show_full_detail_supported():
    # 20230705: Implemented 0030536: VIRTUAL_OB EP should support PRL output format and should require it
    # for SHOW_FULL_DETAIL case
    return (
        hasattr(otq, 'VirtualOb') and
        'show_full_detail' in otq.VirtualOb.Parameters.list_parameters() and
        hasattr(otq.VirtualOb, 'OutputBookFormat') and hasattr(otq.VirtualOb.OutputBookFormat, 'OB')
    )


def _is_save_snapshot_database_parameter_supported():
    # 20220929: Implemented 0028559: Update SAVE_SNAPSHOT to specify output database
    return 'database' in otq.SaveSnapshot.Parameters.list_parameters()


def _is_join_with_snapshot_snapshot_fields_parameter_supported():
    # 20240422: Implemented 0032910: add parameter SNAPSHOT_FIELDS to JOIN_WITH_SNAPSHOT EP
    return 'snapshot_fields' in otq.JoinWithSnapshot.Parameters.list_parameters()


# TODO: code and tests
def is_max_concurrency_with_webapi_supported():
    # 0036758: in onetick.query_webapi: max_concurrency is not being saved in otq file when set on otq.Query
    # 0036759: in onetick.query_webapi:
    # it's not possible to pass max_concurrency 0 in method otq.run when using otq file
    return _is_min_build_or_version(None, None,
                                    20250727120000, min_update_number=3)


def _is_include_market_order_ticks_supported(ep_class):
    # Implemented 0031478: OB_SNAPSHOT... and OB_SUMMARY EPs
    # should support parameter INCLUDE_MARKET_ORDER_TICKS (false by default)
    return 'include_market_order_ticks' in ep_class.Parameters.list_parameters()


# TODO: code only
def _is_join_with_query_symbol_time_otq_supported():
    # 20241209: Fixed 0034770: hours/minutes/seconds part of otq parameter _SYMBOL_TIME, expressed in
    # milliseconds since 1970/01/01 00:00:00 GMT, is ignored
    # 20250219: Implemented 0035092: passing otq param _SYMBOL_TIME should be just like setting symbol_date
    # to the equivalent value, except in YYYYMMDDhhmmss format
    return _is_min_build_or_version(None, None,
                                    20250227120000)


def _is_show_db_list_show_description_supported():
    # 20240301: Implemented 0032320: 0032320: SHOW_DB_LIST should have a new EP parameter, SHOW_DESCRIPTION
    # However on 20240330 builds it returns SHOW_DESCRIPTION column instead of DESCRIPTION
    return 'show_description' in otq.ShowDbList.Parameters.list_parameters()


def _is_symbols_prepend_db_name_supported():
    # 20250924: Implemented 0036753: FIND_DB_SYMBOLS should have EP parameter PREPEND_DB_NAME (true by default)
    return 'prepend_db_name' in otq.FindDbSymbols.Parameters.list_parameters()


def _is_diff_show_all_ticks_supported():
    # 20250919: Implemented 0036784: Add SHOW_ALL_TICKS(false by default) ep parameter to DIFF EP.
    return 'show_all_ticks' in otq.Diff.Parameters.list_parameters()


def _is_max_spread_supported():
    # 20250819: Implemented 0036522: The book EPs that support parameter MAX_DEPTH_FOR_PRICE
    # should also support parameter MAX_SPREAD
    return 'max_spread' in otq.ObSnapshot.Parameters.list_parameters()


def _is_expect_decimals_supported(agg_ep):
    # 20251218 build
    # 20251203: Implemented OTDEV-37054: Add parameter EXPECT_DECIMALS to LOW(_TICK), HIGH(_TICK), FIRST, and LAST EPs
    # 20251117: Implemented OTDEV-37055: Add parameter EXPECT_DECIMALS to SUM and MEDIAN EPs

    # 1.26 20251218 and 20251010-3 builds
    # 20251208: Implemented OTDEV-37054: Add parameter EXPECT_DECIMALS to LOW(_TICK), HIGH(_TICK), FIRST, and LAST EPs
    return 'expect_decimals' in agg_ep.Parameters.list_parameters()


def _is_preserve_decimal_flag_supported():
    # OTDEV-37872: Add PRESERVE_DECIMAL_FLAG to onetick.query_webapi run method to return decimal when needed
    # OTDEV-37783: Add PRESERVE_DECIMAL_FLAG to onetick.query run method to return decimal when needed
    return 'preserve_decimal_flag' in inspect.signature(otq.run).parameters


def _is_read_from_dataframe_supported():
    return hasattr(otq, 'ReadFromDataFrame')
