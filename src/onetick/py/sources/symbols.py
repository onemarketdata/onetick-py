import warnings

import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.core.source import Source
from onetick.py.core.column_operations.base import Raw, OnetickParameter

from .. import types as ott
from .. import utils

from .common import update_node_tick_type


class Symbols(Source):
    """
    Construct a source that returns ticks with information about symbols in a database.
    The SYMBOL_NAME field is populated with symbol names. The TICK_TYPE field contains
    corresponding tick type (enabled by the ``show_tick_type`` parameter).

    Parameters
    ----------
    db: str
        Name of the database where to search symbols.
        By default the database used by :py:func:`otp.run <onetick.py.run>` will be inherited.
    keep_db: bool
        Flag that indicates whether symbols should have a db prefix.
    pattern: str
        Usual and special characters can be used to search for symbols.
        Special characters are:

        * ``%`` - any number of any characters (zero too)
        * ``_`` - any single character
        * ``\\`` - used to escape special characters

        For example, if you want symbol name starting with ``NQ``, you should write ``NQ%``.
        If you want symbol name to contain literal ``%`` character, you should write ``NQ\\%``.
        ``\\`` is a special character too, so it need to be escaped too
        if you want symbol name to contain literal backslash, e.g. ``NQ\\\\M23``.
        Default is ``%``.

    for_tick_type: str
        Fetch only symbols belong to this tick type, if specified.
        Otherwise fetch symbols for all tick types.
    show_tick_type: bool
        Add the **TICK_TYPE** column with the information about tick type
    symbology: str
        The destination symbology for a symbol name translation.
        Translation is performed, if destination symbology is not empty
        and is different from that of the queried database.
    show_original_symbols: bool
        Switches original symbol name propagation as a tick field ORIGINAL_SYMBOL_NAME
        if symbol name translation is performed (if `symbology` is set).
        Note that if this parameter is set to True,
        database symbols with missing translations are also propagated.
    discard_on_match: bool
        If True, then parameter ``pattern`` filters out symbols to return from the database.
    cep_method: str
        The method to be used for extracting database symbols in CEP mode.
        Possible values are:

            * *use_db*: symbols will be extracted from the database with intervals
              specified by the ``poll_frequency`` parameter, and new symbols will be output.
            * *use_cep_adapter*: CEP adapter will be used to retrieve and propagate the symbols with every heartbeat.
            * Default: None, the EP will work the same way as for historical queries,
              i.e. will query the database for symbols once.
    poll_frequency: int
        Specifies the time interval in *minutes* to check the database for new symbols.
        This parameter can be specified only if ``cep_method`` is set to *use_db*.
        The minimum value is 1 minute.
    symbols_to_return: str
        Indicates whether all symbols must be returned or only those which are in the query time range.
        Possible values are:

            * *all_in_db*: All symbols are returned.
            * *with_tick_in_query_range*: Only the symbols which have ticks in the query time range are returned.
              This option is allowed only when ``cep_method`` is set to *use_cep_adapter*.

    _tick_type: str
        Custom tick type for the node of the graph.
        By default "ANY" tick type will be set.
    tick_type: str
        .. attention::

            This parameter is deprecated, use parameter ``_tick_type`` instead.
            Do not confuse this parameter with ``for_tick_type``.
            This parameter is used for low-level customization of OneTick graph nodes and is rarely needed.

    start: :py:class:`datetime.datetime`, :py:class:`otp.datetime <onetick.py.datetime>`
        Custom start time of the query.
        By default the start time used by :py:func:`otp.run <onetick.py.run>` will be inherited.
    end: :py:class:`datetime.datetime`, :py:class:`otp.datetime <onetick.py.datetime>`
        Custom end time of the query.
        By default the start time used by :py:func:`otp.run <onetick.py.run>` will be inherited.
    date: :py:class:`datetime.date`
        Alternative way of setting instead of ``start``/``end`` times.


    Note
    ----
    Additional fields that can be added to Symbols will be converted to symbol parameters

    See also
    --------
    | :ref:`Symbols guide <static/concepts/symbols:Symbols: bound and unbound>`
    | **FIND_DB_SYMBOLS** OneTick event processor

    Examples
    --------

    This class can be used to get a list of all symbols in the database:

    >>> symbols = otp.Symbols('US_COMP', date=otp.dt(2022, 3, 1))
    >>> otp.run(symbols)
            Time  SYMBOL_NAME
    0 2022-03-01          AAP
    1 2022-03-01         AAPL

    By default database name and time interval will be inherited from :py:func:`otp.run <onetick.py.run>`:

    >>> data = otp.Symbols()
    >>> otp.run(data, symbols='US_COMP::', date=otp.dt(2022, 3, 1))
            Time  SYMBOL_NAME
    0 2022-03-01          AAP
    1 2022-03-01         AAPL

    Parameter ``keep_db`` can be used to show database name in a SYMBOL_NAME field.
    It is useful when querying symbols for many databases:

    >>> data = otp.Symbols(keep_db=True)
    >>> data = otp.merge([data], symbols=['SOME_DB::', 'SOME_DB_2::'])
    >>> otp.run(data, date=otp.config.default_start_time)  # doctest: +ELLIPSIS
            Time    SYMBOL_NAME
    0 2003-12-01    SOME_DB::S1
    1 2003-12-01    SOME_DB::S2
    2 2003-12-01  SOME_DB_2::S1
    3 2003-12-01  SOME_DB_2::S2

    By default symbols for all tick types are returned.
    You can set parameter ``show_tick_type`` to print the tick type for each symbol:

    >>> symbols = otp.Symbols('US_COMP', show_tick_type=True)
    >>> otp.run(symbols, date=otp.dt(2022, 3, 1))
            Time SYMBOL_NAME TICK_TYPE
    0 2022-03-01         AAP       TRD
    1 2022-03-01        AAPL       QTE
    2 2022-03-01        AAPL       TRD

    Parameter ``for_tick_type`` can be used to specify a single tick type for which to return symbols:

    >>> symbols = otp.Symbols('US_COMP', show_tick_type=True, for_tick_type='TRD')
    >>> otp.run(symbols, date=otp.dt(2022, 3, 1))
            Time SYMBOL_NAME TICK_TYPE
    0 2022-03-01         AAP       TRD
    1 2022-03-01        AAPL       TRD

    Parameter ``pattern`` can be used to specify the pattern to filter symbol names:

    >>> symbols = otp.Symbols('US_COMP', show_tick_type=True, for_tick_type='TRD', pattern='AAP_')
    >>> otp.run(symbols, date=otp.dt(2022, 3, 1))
            Time SYMBOL_NAME TICK_TYPE
    0 2022-03-01        AAPL       TRD

    Parameter ``discard_on_match`` can be used to use ``pattern`` to filter out symbols instead:

    >>> symbols = otp.Symbols('US_COMP', show_tick_type=True, for_tick_type='TRD',
    ...                       pattern='AAP_', discard_on_match=True)
    >>> otp.run(symbols, date=otp.dt(2022, 3, 1))
            Time SYMBOL_NAME TICK_TYPE
    0 2022-03-01         AAP       TRD

    ``otp.Symbols`` object can be used to specify symbols for the main query:

    >>> symbols = otp.Symbols('US_COMP')
    >>> data = otp.DataSource('US_COMP', tick_type='TRD')
    >>> result = otp.run(data, symbols=symbols, date=otp.dt(2022, 3, 1))
    >>> result['AAPL']
                         Time  PRICE  SIZE
    0 2022-03-01 00:00:00.000    1.3   100
    1 2022-03-01 00:00:00.001    1.4    10
    2 2022-03-01 00:00:00.002    1.4    50
    >>> result['AAP']
                         Time  PRICE
    0 2022-03-01 00:00:00.000  45.37
    1 2022-03-01 00:00:00.001  45.41

    Additional fields of the ``otp.Symbols`` can be used in the main query as symbol parameters:

    >>> symbols = otp.Symbols('SOME_DB', show_tick_type=True, keep_db=True)
    >>> symbols['PARAM'] = symbols['SYMBOL_NAME'] + '__' + symbols['TICK_TYPE']
    >>> data = otp.DataSource('SOME_DB')
    >>> data['S_PARAM'] = data.Symbol['PARAM', str]
    >>> data = otp.merge([data], symbols=symbols)
    >>> otp.run(data)
                         Time   X          S_PARAM
    0 2003-12-01 00:00:00.000   1  SOME_DB::S1__TT
    1 2003-12-01 00:00:00.000  -3  SOME_DB::S2__TT
    2 2003-12-01 00:00:00.001   2  SOME_DB::S1__TT
    3 2003-12-01 00:00:00.001  -2  SOME_DB::S2__TT
    4 2003-12-01 00:00:00.002   3  SOME_DB::S1__TT
    5 2003-12-01 00:00:00.002  -1  SOME_DB::S2__TT

    **Escaping special characters in the pattern**

    When using patterns with special character, be aware that python strings ``\\`` is a special character too
    and need to be escaped as well:

    >>> print('back\\\\slash')
    back\\slash

    Pattern ``NQ\\\\M23`` in python should be written as ``NQ\\\\\\\\M23``:

    >>> print('NQ\\\\\\\\M23')
    NQ\\\\M23

    Escaping character ``\\`` in python can be avoided with raw strings:

    >>> print(r'NQ\\\\M23')
    NQ\\\\M23
    """

    _PROPERTIES = Source._PROPERTIES + ["_p_db",
                                        "_p_pattern",
                                        "_p_start",
                                        "_p_end",
                                        "_p_for_tick_type",
                                        "_p_keep_db"]

    def __init__(
        self,
        db=None,
        find_params=None,
        keep_db=False,
        pattern='%',
        for_tick_type=None,
        show_tick_type=False,
        symbology='',
        show_original_symbols=False,
        discard_on_match=None,
        cep_method=None,
        poll_frequency=None,
        symbols_to_return=None,
        tick_type=utils.adaptive,
        _tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        date=None,
        schema=None,
        **kwargs,
    ):
        if self._try_default_constructor(schema=schema, **kwargs):
            return

        if isinstance(pattern, OnetickParameter):
            pattern = pattern.parameter_expression

        self._p_db = db
        self._p_pattern = pattern
        self._p_start = start
        self._p_end = end
        self._p_keep_db = keep_db
        self._p_for_tick_type = for_tick_type

        if tick_type is not utils.adaptive:
            warnings.warn("In otp.Symbols parameter 'tick_type' is deprecated."
                          " Previously it was incorrectly interpreted by users as a tick type"
                          " for which symbols in the database will be searched."
                          " Instead right now it sets a tick type for a node in OneTick graph "
                          " (this results in symbols from all tick types returned from this source)."
                          " Use parameter 'for_tick_type' to find the symbols for a particular tick type"
                          " and use parameter '_tick_type' if you want set a tick type for a node in OneTick graph.",
                          FutureWarning, stacklevel=2)

        if date and isinstance(date, (ott.datetime, ott.date)):
            start = date.start
            end = date.end

        _symbol = utils.adaptive
        if db:
            if isinstance(db, list):
                _symbol = [f"{str(_db).split(':')[0]}::" for _db in db] # noqa
            else:
                _symbol = f"{str(db).split(':')[0]}::"  # noqa

        if find_params is not None:
            warnings.warn("In otp.Symbols parameter 'find_params' is deprecated."
                          " Use named parameters instead.",
                          FutureWarning, stacklevel=2)

        _find_params = find_params if find_params is not None else {}

        _find_params.setdefault('pattern', pattern)
        if for_tick_type:
            _find_params['tick_type_field'] = for_tick_type
        _find_params.setdefault('show_tick_type', show_tick_type)

        _find_params.setdefault('symbology', symbology)
        _find_params.setdefault('show_original_symbols', show_original_symbols)

        if discard_on_match is not None:
            _find_params.setdefault('discard_on_match', discard_on_match)
        if cep_method is not None:
            if not isinstance(cep_method, str) or cep_method not in ('use_cep_adapter', 'use_db'):
                raise ValueError(f"Wrong value for parameter 'cep_method': {cep_method}")
            _find_params.setdefault('cep_method', cep_method.upper())
        if poll_frequency is not None:
            _find_params.setdefault('poll_frequency', poll_frequency)
        if symbols_to_return is not None:
            if not isinstance(symbols_to_return, str) or symbols_to_return not in ('all_in_db',
                                                                                   'with_ticks_in_query_range'):
                raise ValueError(f"Wrong value for parameter 'symbols_to_return': {symbols_to_return}")
            _find_params.setdefault('symbols_to_return', symbols_to_return.upper())

        if tick_type is not utils.adaptive and _tick_type is not utils.adaptive:
            raise ValueError("Parameters 'tick_type' and '_tick_type' can't be set simultaneously")
        elif tick_type is not utils.adaptive:
            ep_tick_type = tick_type
        elif _tick_type is not utils.adaptive:
            ep_tick_type = _tick_type
        else:
            ep_tick_type = utils.adaptive

        super().__init__(
            _symbols=_symbol,
            _start=start,
            _end=end,
            _base_ep_func=lambda: self.base_ep(ep_tick_type=ep_tick_type,
                                               keep_db=keep_db, **_find_params),
        )

        self.schema['SYMBOL_NAME'] = str

        if _find_params['show_tick_type']:
            self.schema['TICK_TYPE'] = str

        if _find_params['symbology'] and _find_params['show_original_symbols']:
            self.schema['ORIGINAL_SYMBOL_NAME'] = str

    def base_ep(self, ep_tick_type, keep_db, **params):
        src = Source(otq.FindDbSymbols(**params))

        update_node_tick_type(src, ep_tick_type)
        src.schema['SYMBOL_NAME'] = str

        if not keep_db:
            src["SYMBOL_NAME"] = src["SYMBOL_NAME"].str.regex_replace('.*::', '')

        return src

    @staticmethod
    def duplicate(obj, db=None):
        return Symbols(db=obj._p_db if db is None else db,
                       pattern=obj._p_pattern,
                       start=obj._p_start,
                       end=obj._p_end,
                       keep_db=obj._p_keep_db,
                       for_tick_type=obj._p_for_tick_type)
