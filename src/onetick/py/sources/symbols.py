import warnings

import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.core.source import Source
from onetick.py.core.column_operations.base import Raw, OnetickParameter
from onetick.py.core.eval_query import _QueryEvalWrapper
from onetick.py.core._source.tmp_otq import TmpOtq
from onetick.py.compatibility import is_symbols_prepend_db_name_supported

from .. import types as ott
from .. import utils

from .common import update_node_tick_type


class Symbols(Source):
    """
    Construct a source that returns symbol names from the database.

    The **SYMBOL_NAME** field will contain symbol names.

    Parameters
    ----------
    db: str, :py:func:`eval query <onetick.py.eval>`
        Name of the database where to search symbols.
        By default the database used by :py:func:`otp.run <onetick.py.run>` will be inherited.
    keep_db: bool
        Flag that indicates whether symbols should have a database name prefix in the output.
        If True, symbols are returned in *DB_NAME::SYMBOL_NAME* format.
        Otherwise just symbol names are returned.
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
        Default is ``%`` (any symbol name).

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
        Switches original symbol name propagation as a tick field **ORIGINAL_SYMBOL_NAME**
        if symbol name translation is performed (if parameter ``symbology`` is set).

        .. note::

          If this parameter is set to True, database symbols with missing translations are also propagated.
          In this case **ORIGINAL_SYMBOL_NAME** will be presented, but **SYMBOL_NAME** field will be empty.

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

    >>> data = otp.Symbols('US_COMP_SAMPLE', date=otp.dt(2024, 2, 1))  # doctest: +SKIP
    >>> otp.run(data)                                                  # doctest: +SKIP
              Time  SYMBOL_NAME
    0   2024-02-01            A
    1   2024-02-01          AAL
    2   2024-02-01         AAPL
    3   2024-02-01         ABBV
    4   2024-02-01         ABNB
    ..         ...          ...
    496 2024-02-01          XYL
    497 2024-02-01          YUM
    498 2024-02-01          ZBH
    499 2024-02-01         ZBRA
    500 2024-02-01          ZTS

    By default database name and time interval will be inherited from :py:func:`otp.run <onetick.py.run>`:

    >>> data = otp.Symbols()                                                # doctest: +SKIP
    >>> otp.run(data, symbols='US_COMP_SAMPLE::', date=otp.dt(2024, 2, 1))  # doctest: +SKIP
              Time  SYMBOL_NAME
    0   2024-02-01            A
    1   2024-02-01          AAL
    2   2024-02-01         AAPL
    ..         ...          ...

    Parameter ``keep_db`` can be used to show database name in the output.
    It is useful when querying symbols for many databases:

    >>> data = otp.Symbols(keep_db=True)                                        # doctest: +SKIP
    >>> data = data.first(2)                                                    # doctest: +SKIP
    >>> data = otp.merge([data], symbols=['US_COMP_SAMPLE::', 'CME_SAMPLE::'])  # doctest: +SKIP
    >>> otp.run(data, date=otp.dt(2024, 2, 1))                                  # doctest: +SKIP
            Time          SYMBOL_NAME
    0 2024-02-01    US_COMP_SAMPLE::A
    1 2024-02-01  US_COMP_SAMPLE::AAL
    2 2024-02-01  CME_SAMPLE::CL\\F25
    3 2024-02-01  CME_SAMPLE::CL\\F26
    ..       ...                  ...

    By default symbols for all tick types are returned.
    You can set parameter ``show_tick_type`` to print the tick type for each symbol:

    >>> data = otp.Symbols('US_COMP_SAMPLE', show_tick_type=True)  # doctest: +SKIP
    >>> otp.run(data, date=otp.dt(2024, 2, 1))                     # doctest: +SKIP
               Time  SYMBOL_NAME  TICK_TYPE
    0    2024-02-01            A        DAY
    1    2024-02-01            A       LULD
    2    2024-02-01            A       NBBO
    3    2024-02-01            A        QTE
    4    2024-02-01            A       STAT
    ..          ...          ...        ...

    Parameter ``for_tick_type`` can be used to specify a single tick type for which to return symbols:

    >>> data = otp.Symbols('US_COMP_SAMPLE', show_tick_type=True, for_tick_type='TRD')  # doctest: +SKIP
    >>> otp.run(data, date=otp.dt(2024, 2, 1))                                          # doctest: +SKIP
              Time  SYMBOL_NAME  TICK_TYPE
    0   2024-02-01            A        TRD
    1   2024-02-01          AAL        TRD
    2   2024-02-01         AAPL        TRD
    3   2024-02-01         ABBV        TRD
    4   2024-02-01         ABNB        TRD
    ..         ...          ...        ...

    Parameter ``pattern`` can be used to specify the pattern to filter symbol names:

    >>> data = otp.Symbols('US_COMP_SAMPLE', show_tick_type=True, for_tick_type='TRD',
    ...                    pattern='AAP_')      # doctest: +SKIP
    >>> otp.run(data, date=otp.dt(2024, 2, 1))  # doctest: +SKIP
            Time SYMBOL_NAME TICK_TYPE
    0 2024-02-01        AAPL       TRD

    Parameter ``discard_on_match`` can be used to use ``pattern`` to filter out symbols instead:

    >>> data = otp.Symbols('US_COMP_SAMPLE', show_tick_type=True, for_tick_type='TRD',
    ...                    pattern='AAP_', discard_on_match=True)  # doctest: +SKIP
    >>> otp.run(data, date=otp.dt(2024, 2, 1))                     # doctest: +SKIP
              Time  SYMBOL_NAME  TICK_TYPE
    0   2024-02-01            A        TRD
    1   2024-02-01          AAL        TRD
    2   2024-02-01         ABBV        TRD
    3   2024-02-01         ABNB        TRD
    4   2024-02-01          ABT        TRD
    ..         ...          ...        ...

    ``otp.Symbols`` object can be used to specify symbols for the main query:

    >>> symbols = otp.Symbols('US_COMP_SAMPLE')                           # doctest: +SKIP
    >>> symbols = symbols.first(3)                                        # doctest: +SKIP
    >>> data = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')          # doctest: +SKIP
    >>> result = otp.run(data, symbols=symbols, date=otp.dt(2024, 2, 1))  # doctest: +SKIP
    >>> result['AAPL'][['Time', 'PRICE', 'SIZE']]                         # doctest: +SKIP
                                    Time   PRICE  SIZE
    0      2024-02-01 04:00:00.008283417  186.50     6
    1      2024-02-01 04:00:00.008290927  185.59     1
    2      2024-02-01 04:00:00.008291153  185.49   107
    3      2024-02-01 04:00:00.010381671  185.49     1
    4      2024-02-01 04:00:00.011224206  185.50     2
    ..                               ...     ...   ...

    >>> result['AAL'][['Time', 'PRICE', 'SIZE']]                          # doctest: +SKIP
                                    Time  PRICE  SIZE
    0      2024-02-01 04:00:00.097381367  14.33     1
    1      2024-02-01 04:00:00.138908789  14.37     1
    2      2024-02-01 04:00:00.726613365  14.36    10
    3      2024-02-01 04:00:02.195702506  14.36    73
    4      2024-02-01 04:01:55.268302813  14.39     1
    ..                               ...    ...   ...

    Additional fields of the ``otp.Symbols`` can be used in the main query as symbol parameters:

    >>> symbols = otp.Symbols('US_COMP_SAMPLE', show_tick_type=True, for_tick_type='TRD')  # doctest: +SKIP
    >>> symbols['PARAM'] = symbols['SYMBOL_NAME'] + '__' + symbols['TICK_TYPE']            # doctest: +SKIP
    >>> data = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')                           # doctest: +SKIP
    >>> data = data.first(1)                                                               # doctest: +SKIP
    >>> data['S_PARAM'] = data.Symbol['PARAM', str]                                        # doctest: +SKIP
    >>> data = otp.merge([data], symbols=symbols)                                          # doctest: +SKIP
    >>> data = data[['PRICE', 'SIZE', 'S_PARAM']]                                          # doctest: +SKIP
    >>> otp.run(data, date=otp.dt(2024, 2, 1))                                             # doctest: +SKIP
                                 Time    PRICE  SIZE    S_PARAM
    0   2024-02-01 04:00:00.001974784  193.800     4   HSY__TRD
    1   2024-02-01 04:00:00.003547904   57.810    18   OXY__TRD
    2   2024-02-01 04:00:00.006354688   42.810    30   DVN__TRD
    3   2024-02-01 04:00:00.007310080  165.890     9   WMT__TRD
    4   2024-02-01 04:00:00.007833957   43.170    22  INTC__TRD
    ..                            ...      ...   ...        ...

    Use parameter ``symbology`` to specify different symbology to translate to.
    You can also use parameter ``show_original_symbols`` to print original symbols.
    Note that some symbols may not have a translation in target symbology, so their names will be empty:

    >>> data = otp.Symbols('US_COMP', for_tick_type='TRD',
    ...                    symbology='FGV', show_original_symbols=True)                     # doctest: +SKIP
    >>> otp.run(data, start=otp.dt(2023, 5, 15, 9, 30), end=otp.dt(2023, 5, 15, 9, 30, 1))  # doctest: +SKIP
                         Time   SYMBOL_NAME  ORIGINAL_SYMBOL_NAME
    0     2023-05-15 09:30:00  BBG000C2V3D6            US_COMP::A
    1     2023-05-15 09:30:00  BBG00B3T3HD3           US_COMP::AA
    2     2023-05-15 09:30:00  BBG01B0JRCS6          US_COMP::AAA
    3     2023-05-15 09:30:00  BBG00LPXX872         US_COMP::AAAU
    4     2023-05-15 09:30:00  BBG00YZC2Z91          US_COMP::AAC
    ...                   ...           ...                   ...
    10946 2023-05-15 09:30:00                      US_COMP::ZXIET
    10947 2023-05-15 09:30:00                      US_COMP::ZXZZT
    10948 2023-05-15 09:30:00  BBG019XSYC89         US_COMP::ZYME
    10949 2023-05-15 09:30:00  BBG007BBS8B7         US_COMP::ZYNE
    10950 2023-05-15 09:30:00  BBG000BJBXZ2         US_COMP::ZYXI

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
        _tmp_otq = None
        if db:
            if isinstance(db, list):
                _symbol = [f"{str(_db).split(':')[0]}::" for _db in db] # noqa
            elif isinstance(db, _QueryEvalWrapper):
                _tmp_otq = TmpOtq()
                _symbol = db.to_eval_string(tmp_otq=_tmp_otq)
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

        if 'prepend_db_name' in _find_params:
            raise ValueError('Use parameter `keep_db` instead of passing `prepend_db_name` in `find_params`')

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

        if _tmp_otq:
            self._tmp_otq.merge(_tmp_otq)

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
