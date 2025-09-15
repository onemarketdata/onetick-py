import os

import pytest
from datetime import datetime

import onetick.py as otp
from onetick.py.otq import otq

import onetick.py.types as ott
from onetick.py.compatibility import is_supported_per_cache_otq_params


if not is_supported_per_cache_otq_params(throw_warning=True):
    pytest.skip("skipping cache tests for unsupported OneTick version", allow_module_level=True)

cache_name = "test_cache"


@pytest.mark.parametrize(
    "test_type", ["query", "query_func", "otq_file_path"]
)
def test_cache(f_session, cur_dir, test_type):
    """
    Basic test cache creation, querying and deleting
    """
    kwargs = {}

    query = None
    if test_type == "query":
        src = otp.Tick(X=otp.math.rand(0, 10000000), db="LOCAL", tick_type="TRD")
        src.sink(otq.Pause(delay="1000"))
        query = src
    elif test_type == "query_func":
        def query_func():
            _src = otp.Tick(X=otp.math.rand(0, 10000000), db="LOCAL", tick_type="TRD")
            return _src

        query = query_func
    elif test_type == "otq_file_path":
        query = os.path.join(cur_dir, "..", "otqs", "basic_cache.otq::slowquery")

    otp.create_cache(
        cache_name=cache_name,
        query=query,
        tick_type="TRD",
        symbol="SYM",
        db="LOCAL",
    )

    data = otp.ReadCache(
        cache_name=cache_name,
        start=otp.datetime(2022, 6, 1, hour=5),
        end=otp.datetime(2022, 6, 1, hour=12),
        tick_type="TRD",
        db="LOCAL",
        symbol="SYM",
    )
    df1 = otp.run(data)
    df2 = otp.run(data)
    assert df1.to_dict() == df2.to_dict()

    otp.delete_cache(cache_name, tick_type="TRD", symbol="SYM", db="LOCAL")

    data = otp.ReadCache(
        cache_name=cache_name,
        read_mode="cache_only",
        update_cache=False,
        start=otp.datetime(2022, 6, 1, hour=6),
        end=otp.datetime(2022, 6, 1, hour=7, minute=30),
        tick_type="TRD",
        db="LOCAL",
        symbol="SYM",
        **kwargs,
    )
    with pytest.raises(Exception, match="There is no cache named"):
        otp.run(data)


def test_cache_short(f_session):
    query = otp.Tick(X=otp.math.rand(0, 10000000))
    otp.create_cache(cache_name, query=query)

    data = otp.ReadCache(
        cache_name=cache_name,
        start=otp.datetime(2022, 6, 1, hour=6),
        end=otp.datetime(2022, 6, 1, hour=7, minute=30),
    )
    df1 = otp.run(data)
    df2 = otp.run(data)
    assert df1.to_dict() == df2.to_dict()

    otp.delete_cache(cache_name)


@pytest.mark.parametrize("cache_only", [False, True])
def test_inheritability(f_session, cur_dir, cache_only):
    """
    Test caching for overlapping intervals between 2 queries
    Also check read_mode=cache_only
    """
    otp.create_cache(
        cache_name=cache_name,
        query=os.path.join(cur_dir, "..", "otqs", "basic_cache.otq::slowquery"),
        timezone="America/New_York",
        tick_type="TRD",
        db="LOCAL",
    )

    data = otp.ReadCache(
        cache_name=cache_name,
        start=otp.datetime(2022, 6, 1, hour=5),
        end=otp.datetime(2022, 6, 1, hour=6, minute=30),
        tick_type="TRD",
        db="LOCAL",
        symbol="SYM",
    )
    df1 = otp.run(data)

    kwargs = {}
    if cache_only:
        kwargs["read_mode"] = "cache_only"
        kwargs["update_cache"] = False

    data = otp.ReadCache(
        cache_name=cache_name,
        start=otp.datetime(2022, 6, 1, hour=6),
        end=otp.datetime(2022, 6, 1, hour=7, minute=30),
        tick_type="TRD",
        db="LOCAL",
        symbol="SYM",
        **kwargs,
    )
    df2 = otp.run(data)

    assert df1['computed_time'][1] == df2['computed_time'][0]

    if cache_only:
        # where are no 20220601070000 row in cache,
        # because this timestamp not in previous request interval
        assert len(df2) == 1
    else:
        assert len(df2) == 2

    otp.delete_cache(cache_name, tick_type="TRD", db="LOCAL")


def test_cache_time_intervals(f_session, cur_dir):
    """
    Test cache intervals
    """
    otp.create_cache(
        cache_name=cache_name,
        query=os.path.join(cur_dir, "..", "otqs", "basic_cache.otq::slowquery"),
        inheritability=False,
        time_intervals_to_cache=[
            ("20220601050000.000000", "20220601063000.000000"),
            ("20220601073000.000000", "20220601120000.000000"),
        ],
        timezone="America/New_York",
        tick_type="TRD",
        symbol="SYM",
        db="LOCAL",
    )

    # correct interval
    data = otp.ReadCache(
        cache_name=cache_name,
        start=otp.datetime(2022, 6, 1, hour=5),
        end=otp.datetime(2022, 6, 1, hour=6, minute=30),
        tick_type="TRD",
        db="LOCAL",
        symbol="SYM",
    )
    df1 = otp.run(data)
    df2 = otp.run(data)
    assert df1.to_dict() == df2.to_dict()

    # incorrect interval
    data = otp.ReadCache(
        cache_name=cache_name,
        start=otp.datetime(2022, 6, 1, hour=5),
        end=otp.datetime(2022, 6, 1, hour=12),
        tick_type="TRD", db="LOCAL", symbol="SYM",
    )
    with pytest.raises(Exception, match="does not support time interval"):
        otp.run(data)
    otp.delete_cache(cache_name=cache_name)


@pytest.mark.parametrize("units", [
    ("days", 10, otp.datetime(2003, 11, 20), otp.datetime(2003, 12, 20)),
    ("months", 2, otp.datetime(2003, 12, 1), otp.datetime(2004, 2, 1)),
    ("seconds", 30, otp.datetime(2003, 12, 1), otp.datetime(2003, 12, 20, second=30)),
])
def test_time_granularity(f_session, cur_dir, units):
    """
    Test time_granularity param of create_cache
    """
    t_unit, t_value, t_start, t_end = units
    otp.create_cache(
        cache_name=cache_name,
        query=os.path.join(cur_dir, "..", "otqs", "basic_cache.otq::slowquery_multi_symbols"),
        time_granularity=t_value,
        time_granularity_units=t_unit,
        timezone="America/New_York",
        tick_type="TRD",
        db="COMMON",
    )

    # correct interval
    data = otp.ReadCache(
        cache_name=cache_name,
        start=t_start,
        end=t_end,
        tick_type="TRD",
        db="COMMON",
        symbol="SYM_A",
    )
    df1 = otp.run(data)
    df2 = otp.run(data)
    assert df1.to_dict() == df2.to_dict()

    # incorrect interval
    unit_name = t_unit[:-1]
    kwargs = {unit_name: getattr(t_end, unit_name) + 1}
    t_end = t_end.replace(**kwargs)

    data = otp.ReadCache(
        cache_name=cache_name,
        start=t_start,
        end=t_end,
        tick_type="TRD", db="COMMON", symbol="SYM_A",
    )
    with pytest.raises(Exception, match="does not match time granularity constraint for cache"):
        otp.run(data)
    otp.delete_cache(cache_name=cache_name)


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='WebAPI test mode do not clear cache after this test, somehow')
def test_otq_params(f_session, cur_dir):
    """
    Test otq params.
    Setting otq_params in create cache caused ignoring setting otq_params on read.
    """
    otp.create_cache(
        cache_name=cache_name,
        query=os.path.join(cur_dir, "..", "otqs", "basic_cache.otq::slowquery_with_param"),
        timezone="America/New_York",
        tick_type="TRD",
        db="LOCAL",
    )

    data = otp.ReadCache(
        cache_name=cache_name,
        start=otp.datetime(2022, 6, 1, 5),
        end=otp.datetime(2022, 6, 1, 12),
        otq_params={"param": "SYM_A"},
        tick_type="TRD",
        db="LOCAL",
    )
    df_a = otp.run(data, symbols="SYM_A")
    df_b = otp.run(data, symbols="SYM_B")
    assert len(df_a) == 2
    assert len(df_b) == 0

    data = otp.ReadCache(
        cache_name=cache_name,
        start=otp.datetime(2022, 6, 1, 5),
        end=otp.datetime(2022, 6, 1, 12),
        otq_params={"param": "SYM_B"},
        tick_type="TRD",
        db="LOCAL",
    )
    df_a = otp.run(data, symbols="SYM_A")
    df_b = otp.run(data, symbols="SYM_B")
    assert len(df_b) == 2
    assert len(df_a) == 0
    otp.delete_cache(
        cache_name=cache_name,
        tick_type="TRD",
        db="LOCAL",
        per_cache_otq_params={"param": "SYM_A"}
    )


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='WebAPI test mode do not clear cache after this fail')
@pytest.mark.skipif(not otp.compatibility.is_concurrent_cache_is_fixed(), reason='unstable on this onetick version')
def test_otq_params_unstable(f_session):
    """
    Sometimes fails with error `ERR_04697650AWWIW: Attempt to use uninitialized cache`
    Fixed in BDS-365
    """
    otp.create_cache(
        cache_name=cache_name,
        query=otp.Ticks(A=[1, 2]),
    )

    data = otp.ReadCache(
        cache_name=cache_name,
        # comment otq_params to make it work
        otq_params={"any_param": "ANYTHING"},
    )
    res = otp.run(data, symbols=["SYM_A", "SYM_B", 'SYM_C', 'SYM_D', 'SYM_E', 'SYM_F', 'SYM_G', 'SYM_H', 'SYM_I'])
    assert len(res['SYM_A']) == 2
    assert len(res['SYM_B']) == 2


def test_delete_multi_symbol_cache(f_session, cur_dir):
    """
    Test deleting cache for single symbol
    """
    otp.create_cache(
        cache_name=cache_name,
        query=os.path.join(cur_dir, "..", "otqs", "basic_cache.otq::slowquery_multi_symbols"),
        allow_delete_to_everyone=True,
        timezone="America/New_York",
        tick_type="TRD",
        db="COMMON",
    )

    data = otp.ReadCache(
        cache_name=cache_name,
        start=otp.datetime(2003, 12, 1, hour=5),
        end=otp.datetime(2003, 12, 1, hour=12),
        tick_type="TRD",
        db="COMMON",
    )
    df_orig = otp.run(data, symbols=["SYM_A", "SYM_B"])
    otp.delete_cache(cache_name, apply_to_entire_cache=False, tick_type="ANY", db="COMMON", symbol="SYM_A")

    data = otp.ReadCache(
        cache_name=cache_name,
        read_mode="cache_only",
        update_cache=False,
        start=otp.datetime(2003, 12, 1, hour=5),
        end=otp.datetime(2003, 12, 1, hour=12),
        tick_type="TRD",
        db="COMMON",
    )
    df = otp.run(data, symbols=["SYM_A", "SYM_B"])
    assert len(df["SYM_A"]) == 0
    assert len(df["SYM_B"]) == 2
    assert df["SYM_B"].to_dict() == df_orig["SYM_B"].to_dict()
    otp.delete_cache(cache_name=cache_name)


def test_modify_cache_config(f_session, cur_dir):
    otp.create_cache(
        cache_name=cache_name,
        query=os.path.join(cur_dir, "..", "otqs", "basic_cache.otq::slowquery_multi_symbols"),
        time_granularity=10,
        time_granularity_units="days",
        timezone="America/New_York",
        tick_type="TRD",
        db="COMMON",
    )

    data = otp.ReadCache(
        cache_name=cache_name,
        start=otp.datetime(2003, 11, 20),
        end=otp.datetime(2003, 12, 20),
        tick_type="TRD",
        db="COMMON",
        symbol="SYM_A",
    )
    _ = otp.run(data)

    otp.modify_cache_config(cache_name, "TIME_GRANULARITY", "3")

    data = otp.ReadCache(
        cache_name=cache_name,
        start=otp.datetime(2003, 11, 3),
        end=otp.datetime(2003, 12, 6),
        tick_type="TRD",
        db="COMMON",
        symbol="SYM_A",
    )
    _ = otp.run(data)

    with pytest.raises(Exception, match="wrong value was specified for CONFIG_PARAMETER_NAME"):
        otp.modify_cache_config(cache_name, "TEST", "123")

    otp.delete_cache(cache_name=cache_name)


def test_cache_function(f_session):
    src = otp.Tick(X=otp.math.rand(0, 10000000), db="LOCAL", tick_type="TRD")
    data = src.cache(cache_name, db="LOCAL", tick_type="TRD")
    data["TEST"] = 1
    df_1 = otp.run(data)
    df_2 = otp.run(data)

    assert df_1["X"].to_list() == df_2["X"].to_list()
    assert df_1["TEST"].to_list() == df_2["TEST"].to_list() == [1]
    otp.delete_cache(cache_name=cache_name)


@pytest.mark.parametrize("delete_if_exists", [True, False])
def test_cache_function_delete_if_exists(f_session, delete_if_exists):
    src = otp.Tick(X=otp.math.rand(0, 10000000), db="LOCAL", tick_type="TRD")
    data = src.cache(cache_name, db="LOCAL", tick_type="TRD")
    df_1 = otp.run(data)

    data = src.cache(cache_name, delete_if_exists=delete_if_exists, db="LOCAL", tick_type="TRD")
    df_2 = otp.run(data)

    otp.delete_cache(cache_name)

    if delete_if_exists:
        assert df_1["X"].to_list() != df_2["X"].to_list()
    else:
        assert df_1["X"].to_list() == df_2["X"].to_list()


def test_convert_time_intervals_to_cache():
    assert otp.cache._convert_time_intervals(
        [
            (
                otp.cache._convert_dt_to_str("20030102123000.000005"),
                otp.cache._convert_dt_to_str("20030102133000.000005"),
            ),
            (
                datetime(2003, 1, 2, 12, 30, 0, 5),
                datetime(2003, 1, 2, 13, 30, 0, 5),
            ),
            (
                ott.datetime(2003, 1, 2, 12, 30, 0, 5),
                ott.datetime(2003, 1, 2, 13, 30, 0, 5),
            ),
        ]
    ) == "\n".join(["20030102123000.000005,20030102133000.000005"] * 3)
