from typing import Union

import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.core.source import Source

from .. import types as ott
from .. import utils

from .common import update_node_tick_type


def process_otq_params(params: Union[dict, None]) -> str:
    if not params:
        return ""

    return ",".join([f"{k}={ott.value2str(v)}" for k, v in params.items()])


class ReadCache(Source):
    def __init__(
        self,
        cache_name=None,
        db=utils.adaptive_to_default,
        symbol=utils.adaptive,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        read_mode="automatic",
        update_cache=True,
        otq_params=None,
        create_cache_query="",
        schema=None,
        **kwargs,
    ):
        """
        Make cached query

        Cache is initialized on the first read attempt.

        Parameters
        ----------
        cache_name: str
            Name of cache for a query.
        symbol: str, list of str, :class:`Source`, :class:`query`, :py:func:`eval query <onetick.py.eval>`
            Symbol(s) from which data should be taken.
        db: str
            Database to use for tick generation.
        tick_type: str
            Tick type.
            Default: ANY.
        start: :py:class:`otp.datetime <onetick.py.datetime>`
            Start time for tick generation. By default the start time of the query will be used.
        end: :py:class:`otp.datetime <onetick.py.datetime>`
            End time for tick generation. By default the end time of the query will be used.
        read_mode: str
            Mode of querying cache. One of these:

            * ``cache_only`` - only cached results are returned and queries are not performed.
            * ``query_only`` - the query is run irrespective of whether the result is already available in the cache.
            * ``automatic`` (default) - perform the query if the data is not found in the cache.
        update_cache: bool
            If set to ``True``, updates the cached data if ``read_mode=query_only`` or if ``read_mode=automatic`` and
            the result data not found in the cache. Otherwise, the cache remains unchanged.
        otq_params: dict
            OTQ parameters to override ``otq_params`` that are passed during creation of cache.
            Query result will be cached separately for each unique pair of OTQ parameters.
        create_cache_query: str
            If a cache with the given name is not present, the query provided in this param will be invoked,
            which should contain CREATE_CACHE EP to create the corresponding cache.
        schema: Optional, dict
            Dictionary of columns names with their types.
        kwargs:
            Deprecated. Use ``schema`` instead.
            Dictionary of columns names with their types.

        See also
        --------
        | **READ_CACHE** OneTick event processor
        | :py:func:`onetick.py.create_cache`

        Examples
        --------
        Simple cache read:

        >>> def query_func():
        ...    return otp.DataSource("DEMO_L1", tick_type="TRD", symbols="AAPL")
        >>> otp.create_cache(
        ...    cache_name="some_cache", query=query_func, tick_type="TRD", db="DEMO_L1",
        ... )
        >>> src = otp.ReadCache("some_cache")
        >>> otp.run(src)  # doctest: +SKIP

        Make cache query for specific time interval:

        >>> src = otp.ReadCache(
        ...    "some_cache",
        ...    start=otp.datetime(2024, 1, 1, 12, 30),
        ...    end=otp.datetime(2024, 1, 2, 18, 0),
        ... )
        >>> otp.run(src)  # doctest: +SKIP

        Override or set `otq_params` for one query:

        >>> src = otp.ReadCache(
        ...    "some_cache",
        ...    otq_params={"some_param": "test_value"},
        ... )
        >>> otp.run(src)  # doctest: +SKIP
        """
        if self._try_default_constructor(schema=schema, **kwargs):
            return

        if cache_name is None:
            raise ValueError("Missing required parameter `cache_name`")

        if read_mode not in {"automatic", "cache_only", "query_only"}:
            raise ValueError(f"Incorrect value of `read_mode` parameter passed: {read_mode}")

        if update_cache and read_mode == 'cache_only':
            raise ValueError("`update_cache` parameter couldn't be set to True with `read_mode` set to cache_only")

        read_mode = read_mode.upper()

        otq_params = process_otq_params(otq_params)

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=lambda: self.base_ep(
                db=db,
                tick_type=tick_type,
                cache_name=cache_name,
                read_mode=read_mode,
                update_cache=update_cache,
                otq_params=otq_params,
                create_cache_query=create_cache_query,
            ),
            schema=schema,
            **kwargs,
        )

    def base_ep(
        self,
        db=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        cache_name=None,
        read_mode="automatic",
        update_cache=True,
        otq_params="",
        create_cache_query="",
    ):
        src = Source(
            otq.ReadCache(
                cache_name=cache_name,
                per_cache_otq_params=otq_params,
                read_mode=read_mode,
                update_cache=update_cache,
                create_cache_query=create_cache_query,
            )
        )

        update_node_tick_type(src, tick_type, db)

        return src
