# this file is for compatibility functions used in tests only
# they are allowed to use _is_min_build_or_version function

from onetick.py.compatibility import _is_min_build_or_version, get_onetick_version
from onetick.py.otq import otq


def is_supported_stack_info():
    """
    Fixed 0028824: setting otq.API_CONFIG.SHOW_STACK_INFO=1 does not cause location of an EP in
    python code to be added to the text of exception
    """
    onetick_version = get_onetick_version()
    if onetick_version.build_number == 20240205120000:
        # BDS-345
        return False
    return _is_min_build_or_version(1.24, None,
                                    20221111120000)


def is_supported_rename_fields_symbol_change():
    """
    ???
    """
    return _is_min_build_or_version(1.24, 20230316120000,
                                    20230316120000)


def is_supported_new_ob_snapshot_behavior():
    """
    ???
    """
    return _is_min_build_or_version(1.24, 20230711120000,
                                    20230711120000)


def is_supported_varstring_in_get_string_value():
    """
    Implemented 0030763: GET_STRING_VALUE method on tick objects should support also varstring field types
    """
    return _is_min_build_or_version(1.24, None,
                                    20230711120000)


def is_supported_nsectime_tick_set_eval():
    # BDS-321
    # Fixed 0031588: Ticks in TICK_SET populated by eval , loose nanosecond precision
    return _is_min_build_or_version(1.24, None,
                                    20231108120000)


def is_supported_large_ints_empty_interval():
    # BDS-333
    # 0032093: when EXPECT_LARGE_INTS isn't 'false, HIGH,LOW,FIRST, and LAST EPs should show integer values,
    #          not doubles, when input is empty
    return _is_min_build_or_version(1.24, None,
                                    20231108120000, min_update_number=1)


def is_supported_list_empty_derived_databases():
    # PY-856, BDS-323
    # Was fixed in BUILD_initial_20240205120000
    # 20240130: Fixed 0031783: onetick.query crashes when a query returned no ticks,
    # but produced a tick descriptor with string fields of 0 size
    return _is_min_build_or_version(1.24, 20240524004422,
                                    20240205120000, min_update_number=0)


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


def is_write_parquet_directories_fixed():
    # 20240609: Fixed 0033342: WRITE_TO_PARQUET EP should not produce directories in non-partitioned mode
    return _is_min_build_or_version(1.25, 20250209162722,
                                    20240530120000, min_update_number=1)


def is_get_query_property_flag_supported():
    # 20231205: Implemented 0031857:
    # create flag for GET_QUERY_PROPERTY and GET_QUERY_PROPERTIES to return also special query properties
    return _is_min_build_or_version(1.25, 20241229055942,
                                    20240205120000)


def is_all_fields_when_ticks_exit_window_supported():
    # 20231230: Implemented 0031741:
    # ALL_FIELDS_FOR_SLIDING aggregation parameter should support value WHEN_TICKS_EXIT_WINDOW
    # (check out "Parameters common go generic aggregations" section in OneTick Event Processors' guide).
    return _is_min_build_or_version(1.24, 20240116201311,
                                    20240205120000)


def _is_supported_pnl_realized_buy_sell_flag_bin():
    # 20240429: Implemented 0032683: Enhance PNL_REALIZED EP for BUY_SELL_FLAG field to support also 0 and 1
    return _is_min_build_or_version(None, None,
                                    20240530120000)


def is_derived_databases_crash_fixed():
    # See tasks PY-134, PY-388, BDS-334.
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


def is_percentile_bug_fixed():
    # 20241209: Implemented 0034428: In FIND_VALUE_FOR_PERCENTILE EP, rename SHOW_PERCENTILE_AS to COMPUTE_VALUE_AS
    # NOTE: also has fix for FIRST_VALUE_WITH_GE_PERCENTILE and PERCENTILE=100 (was N/A, but must be biggest value)
    return (hasattr(otq, 'FindValueForPercentile') and
            'compute_value_as' in otq.FindValueForPercentile.Parameters.list_parameters())


def is_database_view_schema_supported():
    # Implemented 0034115: DB/SHOW_TICK_TYPES should return non-empty schema
    # for View queries ending in single TABLE EP with type specified for each field
    return _is_min_build_or_version(1.25, 20241229055942,
                                    20241001205534)


def is_multi_column_generic_aggregations_supported():
    # Implementation of tick aggregations in COMPUTE requires to use RENAME_FIELDS to make correct output schema.
    # However, if we place it inside generic aggregation inside COMPUTE, next error occur on old OneTick versions:
    # ERR_06708004ERCOM: Event processor RENAME_FIELDS does not currently support dynamic symbol changes.
    return _is_min_build_or_version(1.24, 20240116201311,
                                    20230315095103)


def is_timezone_override_fixed():
    # BDS-484, OTDEV-37501: seems like timezone is ignored in otq.run in some cases
    return _is_min_build_or_version(1.26, 20260114173411,
                                    20251218120000, min_update_number=1)


def is_now_in_start_end_time_expressions_fixed():
    # PY-1437, BDS-489, OTDEV-37551
    # Fixed OTDEV-37551: NOW() evaluation causes "Start time of the query can not be greater or equal
    # to the end time" for short (usually <20ms) intervals
    return _is_min_build_or_version(1.26, 20260114173411,
                                    20260216120000)


def is_double_nan_supported_when_the_result_type_is_decimal():
    # OTDEV-37217
    # 20251114: Fixed OTDEV-37217: CASE built-in function produces 0
    # when the result is double NAN and the result type is DECIMAL
    return _is_min_build_or_version(None, None,
                                    20251218120000)


def is_allow_graph_reuse_property_fixed():
    # something out of these
    # Implemented OTDEV-35621: ALLOW_GRAPH_REUSE must be a regular query property
    #                          and "graph_reuse = 0" in an otq file must be treated as if it wasn't there
    # Fixed OTDEV-36580: TIME_SHIFT built-in function might produce incorrect result,
    #                    when "ALLOW_GRAPH_REUSE" query property is set
    return _is_min_build_or_version(1.26, 20251217195357,
                                    20251010120000)


def is_compute_all_fields_fixed():
    # probably 20260104: Fixed OTDEV-37541: COMPUTE EP may cause certain fields to be dropped
    return _is_min_build_or_version(1.26, 20260114173411,
                                    20251218120000, min_update_number=1)
