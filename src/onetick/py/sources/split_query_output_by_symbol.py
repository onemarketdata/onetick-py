import os

from functools import partial

import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.core.source import Source

from .. import utils

from .common import update_node_tick_type


class SplitQueryOutputBySymbol(Source):
    def __init__(self,
                 query=None,
                 symbol_field=None,
                 single_invocation=False,
                 db=utils.adaptive_to_default,
                 tick_type=utils.adaptive,
                 start=utils.adaptive,
                 end=utils.adaptive,
                 symbols=utils.adaptive,
                 schema=None,
                 **kwargs):
        """
        A data source used to dispatch output ticks,
        resulting after execution of the specified query,
        according to the values of the specified field in those ticks.

        Each replica of this EP, corresponding to a particular bound or unbound symbol, thus,
        propagates resulting ticks of the specified query,
        with values of the specified field in those ticks equal to that symbol.

        Note, that database name part of symbols is not taken into account.

        Parameters
        ----------
        query:
            Specify query to execute.
        symbol_field:
            Specifies the field in the resulting ticks of the underlying query,
            according to values of which those ticks are dispatched.
        single_invocation:
            By default, the underlying query is executed once
            per symbol batch and per execution thread of the containing query.
            If this parameter is set to True, the underlying query is executed once
            regardless of the batch size and the number of CPU cores utilized.

            The former option should be the preferred (hence the default) one, as it reduces memory overhead,
            while the latter one might be chosen to speed-up the overall execution.

            Note, that if the underlying query is a CEP query, than this option has no effect,
            as there is a single batch and a single thread anyway.
        db:
        tick_type:
            Tick type to set on the OneTick's graph node.
            Can be used to specify database name with tick type or tick type only.

            By default setting these parameters is not required, database is usually set
            with parameter ``symbols`` or in :py:func:`otp.run <onetick.py.run>`.
        start:
            Custom start time of the source.
            If set, will override the value specified in :py:func:`otp.run <onetick.py.run>`.
        end:
            Custom end time of the source.
            If set, will override the value specified in :py:func:`otp.run <onetick.py.run>`.
        symbols:
            Symbol(s) from which data should be taken.
            If set, will override the value specified in :py:func:`otp.run <onetick.py.run>`.

        Examples
        --------

        Get only the ticks that have needed symbols specified in field ``TICKER``:

        >>> data = otp.Ticks(X=[1, 2, 3, 4], TICKER=['A', 'B', 'A', 'C'])
        >>> data = otp.SplitQueryOutputBySymbol(data, data['TICKER'])
        >>> res = otp.run(data, symbols=['A', 'B'])
        >>> res['A']
                             Time  X TICKER
        0 2003-12-01 00:00:00.000  1      A
        1 2003-12-01 00:00:00.002  3      A
        >>> res['B']
                             Time  X TICKER
        0 2003-12-01 00:00:00.001  2      B
        """

        if self._try_default_constructor(schema=schema, **kwargs):
            return

        if isinstance(query, Source):
            query = query.copy()
            otq_query = query.to_otq()
            q_start, q_end, _ = query._set_date_range_and_symbols()
            if start is utils.adaptive and end is utils.adaptive:
                start, end = q_start, q_end
        else:
            # TODO: support already existing queries
            raise ValueError('Non supported type of the `query` is specified')

        super().__init__(
            _symbols=symbols,
            _start=start,
            _end=end,
            _base_ep_func=partial(self.build, db, tick_type, symbol_field, otq_query, single_invocation),
            schema=schema,
            **kwargs,
        )

    def build(self, db, tick_type, symbol_field_name, otq_query, single_invocation):
        src = Source(otq.SplitQueryOutputBySymbol(otq_query=otq_query,
                                                  symbol_field_name=str(symbol_field_name),
                                                  ensure_single_invocation=single_invocation))

        update_node_tick_type(src, tick_type, db)

        return src


def by_symbol(src: Source,
              symbol_field,
              single_invocation=False,
              db=utils.adaptive_to_default,
              tick_type=utils.adaptive,
              start=utils.adaptive,
              end=utils.adaptive,
              ) -> Source:
    """
    Create a separate data series for each unique value of ``symbol_field`` in the output of ``src``.
    ``src`` must specify enough parameters to be run (e.g., symbols, query range). A typical use case is to split a
    single data series (e.g., from a CSV file) into separate data series by symbol. This method is a source.

    Parameters
    ----------
    src: Source
        a query which output is to be split by ``symbol_field``
    symbol_field: str
        the name of the field carrying symbol name in the ``src`` query
    single_invocation: bool, optional
        ``True`` means that the ``src`` query is run once and the result stored in memory speeding up the execution.
        ``False`` means that the ``src`` query is run for every symbol of the query saving memory
        but slowing down query execution.
        Default: ``False``
    db: str, optional
        Database for running the query. Doesn't affect the ``src`` query. The default value
        is ``otp.config['default_db']``.
    tick_type: str, optional
        Tick type for the query. Doesn't affect the ``src`` query.
    start: otp.dt, optional
        By default it is taken from the ``src`` start time
    end: otp.dt, optional
        By default it is taken from the ``src`` end time

    See also
    --------
    **SPLIT_QUERY_OUTPUT_BY_SYMBOL** OneTick event processor

    Examples
    --------
    >>> executions = otp.CSV( # doctest: +SKIP
    ...     otp.utils.file(os.path.join(cur_dir, 'data', 'example_events.csv')),
    ...     converters={"time_number": lambda c: c.apply(otp.nsectime)},
    ...     timestamp_name="time_number",
    ...     start=otp.dt(2022, 7, 1),
    ...     end=otp.dt(2022, 7, 2),
    ...     order_ticks=True
    ... )[['stock', 'px']]
    >>> csv = otp.by_symbol(executions, 'stock') # doctest: +SKIP
    >>> trd = otp.DataSource( # doctest: +SKIP
    ...     db='US_COMP',
    ...     tick_type='TRD',
    ...     start=otp.dt(2022, 7, 1),
    ...     end=otp.dt(2022, 7, 2)
    ... )[['PRICE', 'SIZE']]
    >>> data = otp.join_by_time([csv, trd]) # doctest: +SKIP
    >>> result = otp.run(data, symbols=executions.distinct(keys='stock')[['stock']], concurrency=8) # doctest: +SKIP
    >>> result['THG'] # doctest: +SKIP
                               Time stock      px   PRICE  SIZE
    0 2022-07-01 11:37:56.432947200   THG  148.02  146.48     1
    >>> result['TFX'] # doctest: +SKIP
                               Time stock      px   PRICE  SIZE
    0 2022-07-01 11:39:45.882808576   TFX  255.61  251.97     1
    >>> result['BURL'] # doctest: +SKIP
                               Time stock      px   PRICE  SIZE
    0 2022-07-01 11:42:35.125718016  BURL  137.53  135.41     2
    """
    result = SplitQueryOutputBySymbol(src,
                                      symbol_field=symbol_field,
                                      single_invocation=single_invocation,
                                      db=db,
                                      tick_type=tick_type,
                                      start=start,
                                      end=end)

    result.schema.set(**src.schema)
    return result
