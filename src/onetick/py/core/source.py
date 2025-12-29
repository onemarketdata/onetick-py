import os
import re
import uuid
import warnings
from collections import defaultdict
from datetime import datetime, date
import pandas as pd
from typing import Optional, Tuple

from onetick.py.otq import otq

import onetick.py.functions
import onetick.py.sources
from onetick import py as otp
from onetick.py import types as ott
from onetick.py import utils, configuration
from onetick.py.core._internal._manually_bound_value import _ManuallyBoundValue
from onetick.py.core._internal._proxy_node import _ProxyNode
from onetick.py.core._internal._state_vars import StateVars
from onetick.py.core._source._symbol_param import _SymbolParamColumn, _SymbolParamSource
from onetick.py.core._source.schema import Schema
from onetick.py.core._source.symbol import Symbol
from onetick.py.core._source.tmp_otq import TmpOtq
from onetick.py.core.column import _Column
from onetick.py.core.column_operations.base import _Operation, OnetickParameter
from onetick.py.core.query_inspector import get_query_parameter_list
from onetick.py.utils import adaptive, adaptive_to_default, default, render_otq


def _is_dict_required(symbols):
    """
    Depending on symbols, determine if output of otp.run() or Source.__call__() should always be a dictionary
    of {symbol: dataframe} even if only one symbol is present in the results
    """
    if isinstance(symbols, (list, tuple)):
        if len(symbols) == 0:
            return False
        elif len(symbols) > 1:
            return True
        else:
            symbols = symbols[0]

    if isinstance(symbols, otp.Source):
        return True
    if isinstance(symbols, otq.Symbol):
        symbols = symbols.name
    if isinstance(symbols, str) and 'eval' in symbols:
        return True
    return False


class MetaFields:
    """
    OneTick defines several pseudo-columns that can be treated as if they were columns of every tick.

    These columns can be accessed directly via :py:meth:`onetick.py.Source.__getitem__` method.

    But in case they are used in :py:class:`~onetick.py.core.column_operations.base.Expr`
    they can be accessed via ``onetick.py.Source.meta_fields``.

    Examples
    --------

    Accessing pseudo-fields as columns or as class properties

    >>> data = otp.Tick(A=1)
    >>> data['X'] = data['_START_TIME']
    >>> data['Y'] = otp.Source.meta_fields['_TIMEZONE']
    >>> otp.run(data, start=otp.dt(2003, 12, 2), timezone='GMT')
            Time  A          X    Y
    0 2003-12-02  1 2003-12-02  GMT
    """
    def __init__(self):
        self.timestamp = _Column('TIMESTAMP', dtype=ott.nsectime)
        self.time = self.timestamp
        self.start_time = _Column('_START_TIME', dtype=ott.nsectime)
        self.start = self.start_time
        self.end_time = _Column('_END_TIME', dtype=ott.nsectime)
        self.end = self.end_time
        self.timezone = _Column('_TIMEZONE', dtype=str)
        self.db_name = _Column('_DBNAME', dtype=str)
        self.symbol_name = _Column('_SYMBOL_NAME', dtype=str)
        self.tick_type = _Column('_TICK_TYPE', dtype=str)
        self.symbol_time = _Column('_SYMBOL_TIME', dtype=otp.nsectime)
        self.__fields = set(map(str, self.__dict__.values())) | {'Time'}

    def get_onetick_fields_and_types(self):
        return {
            column.name: column.dtype
            for name, column in self.__dict__.items()
            if not name.startswith('_') and name != 'time'
        }

    def __iter__(self):
        yield from self.__fields

    def __contains__(self, item):
        return item in self.__fields

    def __len__(self):
        return len(self.__fields)

    def __getitem__(self, item):
        """
        These fields are available:

        * ``TIMESTAMP`` (or ``Time``)
        * ``START_TIME`` (or ``_START_TIME``)
        * ``END_TIME`` (or ``_END_TIME``)
        * ``TIMEZONE`` (or ``_TIMEZONE``)
        * ``DBNAME`` (or ``_DBNAME``)
        * ``SYMBOL_NAME`` (or ``_SYMBOL_NAME``)
        * ``TICK_TYPE`` (or ``_TICK_TYPE``)
        * ``SYMBOL_TIME`` (or ``_SYMBOL_TIME``)
        """
        return {
            'TIMESTAMP': self.timestamp,
            'Time': self.time,
            'START_TIME': self.start_time,
            '_START_TIME': self.start_time,
            'END_TIME': self.end_time,
            '_END_TIME': self.end_time,
            'TIMEZONE': self.timezone,
            '_TIMEZONE': self.timezone,
            'DB_NAME': self.db_name,
            'DBNAME': self.db_name,
            '_DBNAME': self.db_name,
            'SYMBOL_NAME': self.symbol_name,
            '_SYMBOL_NAME': self.symbol_name,
            'TICK_TYPE': self.tick_type,
            '_TICK_TYPE': self.tick_type,
            'SYMBOL_TIME': self.symbol_time,
            '_SYMBOL_TIME': self.symbol_time,
        }[item]


class Source:
    """
    Base class for representing Onetick execution graph.
    All :ref:`onetick-py sources <api/sources/root:sources>` are derived from this class
    and have access to all its methods.

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> isinstance(data, otp.Source)
    True

    Also this class can be used to initialize raw source
    with the help of ``onetick.query`` classes, but
    it should be done with caution as the user is required to set
    such properties as symbol name and tick type manually.

    >>> data = otp.Source(otq.TickGenerator(bucket_interval=0, fields='long A = 123').tick_type('TT'))
    >>> otp.run(data, symbols='LOCAL::')
            Time    A
    0 2003-12-04  123
    """

    # TODO: need to support transactions for every _source
    # transaction is set of calls between _source creation and call or between two calls
    # if transaction have the same operations, then it seems we should add only one set of operations

    _PROPERTIES = [
        "__node",
        "__hash",
        "__sources_keys_dates",
        "__sources_modify_query_times",
        "__sources_base_ep_func",
        "__sources_symbols",
        "__source_has_output",
        "__name",
        "_tmp_otq"
    ]
    _OT_META_FIELDS = ["_START_TIME", "_END_TIME", "_SYMBOL_NAME", "_DBNAME", "_TICK_TYPE", '_TIMEZONE']
    meta_fields = MetaFields()
    Symbol = Symbol  # NOSONAR

    def __init__(
        self,
        node=None,
        schema=None,
        _symbols=None,
        _start=adaptive,
        _end=adaptive,
        _base_ep_func=None,
        _has_output=True,
        **kwargs,
    ):

        self._tmp_otq = TmpOtq()
        self.__name = None

        if isinstance(_symbols, OnetickParameter):
            _symbols = _symbols.parameter_expression

        schema = self._select_schema(schema, kwargs)

        for key in schema:
            if self._check_key_in_properties(key):
                raise ValueError(f"Can't set class property {key}")
            if self._check_key_is_meta(key):
                if key == 'TIMESTAMP':
                    # for backward-compatibility
                    warnings.warn(f"Setting meta field {key} in schema is not needed", FutureWarning, stacklevel=2)
                else:
                    raise ValueError(f"Can't set meta field {key}")

        schema.update(
            self.meta_fields.get_onetick_fields_and_types()
        )

        for key, value in schema.items():
            # calculate value type
            value_type = ott.get_source_base_type(value)
            self.__dict__[key] = _Column(name=key, dtype=value_type, obj_ref=self)

        # just an alias to Timestamp
        self.__dict__['Time'] = self.__dict__['TIMESTAMP']
        self.__dict__['_state_vars'] = StateVars(self)

        if node is None:
            node = otq.Passthrough()

        if isinstance(_symbols, _SymbolParamColumn):
            symbol_node = otq.ModifySymbolName(str(self.Symbol[_symbols.name, str]))
            node = node.sink(symbol_node)
            _symbols = None

        self.__hash = uuid.uuid4()
        self.__sources_keys_dates = {}
        self.__sources_modify_query_times = {}
        self.__sources_base_ep_func = {}
        self.__sources_symbols = {}
        self.__source_has_output = _has_output

        if isinstance(node, _ProxyNode):
            self.__node = _ProxyNode(*node.copy_graph(), refresh_func=self.__refresh_hash)
        else:
            self.__node = _ProxyNode(*self.__from_ep_to_proxy(node), refresh_func=self.__refresh_hash)
            self.__sources_keys_dates[self.__node.key()] = (_start, _end)
            self.__sources_modify_query_times[self.__node.key()] = False
            self.__sources_base_ep_func[self.__node.key()] = _base_ep_func
            self.__sources_symbols[self.__node.key()] = _symbols

    def _try_default_constructor(self, *args, node=None, schema=None, **kwargs):
        if node is not None:
            # Source.copy() method will use this way
            # all info from original source will be copied by copy() method after
            Source.__init__(self, *args, node=node, schema=schema, **kwargs)
            return True
        return False

    def base_ep(self, **_kwargs):
        # default implementation
        # real implementation should return a Source object
        return None

    def _select_schema(self, schema, kwargs) -> dict:
        """
        Selects schema definition from ``schema`` or ``kwargs`` parameters
        for the time of deprecation of ``kwargs`` parameter.
        """
        if schema is None:
            if kwargs:
                warnings.warn(
                    f'Setting `{self.__class__.__name__}` schema via `**kwargs` is deprecated. '
                    'Please use `schema` parameter for this. '
                    f'Passed kwargs are: {kwargs}.',
                    FutureWarning,
                    stacklevel=2,
                )
                return kwargs
            else:
                return {}
        elif kwargs:
            raise ValueError(
                "Specifying schema through both `**kwargs` and `schema` is prohibited. "
                f"Passed kwargs are: {kwargs}."
            )

        return schema.copy()

    def _clean_sources_dates(self):
        self.__sources_keys_dates = {}
        self.__sources_modify_query_times = {}
        self.__sources_base_ep_func = {}
        self.__sources_symbols = {}

    def _set_sources_dates(self, other, copy_symbols=True):
        self.__sources_keys_dates.update(other._get_sources_dates())
        self.__sources_modify_query_times.update(other._get_sources_modify_query_times())
        self.__sources_base_ep_func.update(other._get_sources_base_ep_func())
        if copy_symbols:
            self.__sources_symbols.update(other._get_sources_symbols())
        else:
            # this branch is applicable for the bound symbols with callbacks,
            # where we drop all adaptive symbols and keep only manually specified
            # symbols
            manually_bound = {
                key: _ManuallyBoundValue(value)
                for key, value in other._get_sources_symbols().items()
                if value is not adaptive and value is not adaptive_to_default
            }
            self.__sources_symbols.update(manually_bound)

        self.__source_has_output = other._get_source_has_output()

    def _change_sources_keys(self, keys: dict):
        """
        Change keys in sources dictionaries.
        Need to do it, for example, after rebuilding the node history with new keys.

        Parameters
        ----------
        keys: dict
            Mapping from old key to new key
        """
        sources = (self.__sources_keys_dates,
                   self.__sources_modify_query_times,
                   self.__sources_base_ep_func,
                   self.__sources_symbols)
        for dictionary in sources:
            for key in list(dictionary):
                dictionary[keys[key]] = dictionary.pop(key)

    def _get_source_has_output(self):
        return self.__source_has_output

    def _get_sources_dates(self):
        return self.__sources_keys_dates

    def _get_sources_modify_query_times(self):
        return self.__sources_modify_query_times

    def _get_sources_base_ep_func(self):
        return self.__sources_base_ep_func

    def _get_sources_symbols(self):
        return self.__sources_symbols

    def _check_key_in_properties(self, key: str) -> bool:
        if key in self.__class__._PROPERTIES:
            return True
        if key.replace('_' + Source.__name__.lstrip('_'), "") in self.__class__._PROPERTIES:
            return True
        if key.replace(self.__class__.__name__, "") in self.__class__._PROPERTIES:
            return True
        return False

    def _check_key_is_meta(self, key: str) -> bool:
        return key in self.__class__.meta_fields

    def _check_key_is_reserved(self, key: str) -> bool:
        return self._check_key_in_properties(key) or self._check_key_is_meta(key)

    def _set_field_by_tuple(self, name, dtype):
        warnings.warn('Using _set_field_by_tuple() is not recommended,'
                      ' change your code to use otp.Source.schema object.', DeprecationWarning)
        if name not in self.__dict__:
            if dtype is None:
                raise KeyError(f'Column name {name} is not in the schema. Please, check that this column '
                               'is in the schema or add it using the .schema property')

            if name in (0, 1):
                raise ValueError(f"constant {name} are not supported for indexing for now, please use otp.Empty")
            if isinstance(name, (int, float)):
                raise ValueError("integer indexes are not supported")
            self.__dict__[name] = _Column(name, dtype, self)
        else:
            if not isinstance(self.__dict__[name], _Column):
                raise AttributeError(f"There is no '{name}' column")

            if dtype:
                type1, type2 = self.__dict__[name].dtype, dtype
                b_type1, b_type2 = ott.get_base_type(type1), ott.get_base_type(type2)

                if b_type1 != b_type2:
                    if {type1, type2} == {int, float}:
                        self.__dict__[name]._dtype = float
                    else:
                        raise Warning(
                            f"Column '{name}' was declared as '{type1}', but you want to change it to '{type2}', "
                            "that is not possible without setting type directly via assigning value"
                        )

                else:
                    if issubclass(b_type1, str):
                        t1_length = ott.string.DEFAULT_LENGTH if type1 is str else type1.length
                        t2_length = ott.string.DEFAULT_LENGTH if type2 is str else type2.length

                        self.__dict__[name]._dtype = type2 if t1_length < t2_length else type1
                    if {type1, type2} == {ott.nsectime, ott.msectime}:
                        self.__dict__[name]._dtype = ott.nsectime
        return self.__dict__[name]

    def _hash(self):
        return self.__hash

    def _merge_tmp_otq(self, source):
        self._tmp_otq.merge(source._tmp_otq)

    def __prepare_graph(self, symbols=None, start=None, end=None, has_output=False):
        # We copy object here, because we will change it according to passed
        # symbols and date ranges. For example, we can add modify_query_times EP
        # if it is necessary

        obj = self.copy()
        if has_output:
            obj.sink(otq.Passthrough())
        start, end, symbols = obj._set_date_range_and_symbols(symbols, start, end)
        if start is adaptive:
            start = None
        if end is adaptive:
            end = None
        if symbols is not None and isinstance(symbols, pd.DataFrame):
            symbols = utils.get_symbol_list_from_df(symbols)
        if symbols is not None and not isinstance(symbols, list):
            symbols = [symbols]
        elif symbols is None:
            symbols = []
        _symbols = []
        for sym in symbols:
            _symbols.append(self._convert_symbol_to_string(sym, tmp_otq=obj._tmp_otq, start=start, end=end))

        return obj, start, end, _symbols

    def to_otq(self, file_name=None, file_suffix=None, query_name=None, symbols=None, start=None, end=None,
               timezone=None, raw=None, add_passthrough=True,
               running=False,
               start_time_expression=None,
               end_time_expression=None,
               symbol_date=None):
        """
        Save :class:`otp.Source <onetick.py.Source>` object to .otq file and return path to the saved file.

        Parameters
        ----------
        file_name: str
            Absolute or relative path to the saved file.
            If ``None``, create temporary file and name it randomly.
        file_suffix: str
            Suffix to add to the saved file name (including extension).
            Can be specified if ``file_name`` is ``None``
            to distinguish between different temporary files.
            Default: ".to_otq.otq"
        query_name: str
            Name of the main query in the created file.
            If ``None``, take name from this Source object.
            If that name is empty, set name to "query".
        symbols: str, list, :pandas:`DataFrame <pandas.DataFrame>`, :class:`Source`
            symbols to save query with
        start: :py:class:`otp.datetime <onetick.py.datetime>`
            start time to save query with
        end: :py:class:`otp.datetime <onetick.py.datetime>`
            end time to save query with
        timezone: str
            timezone to save query with
        raw

            .. deprecated:: 1.4.17

        add_passthrough: bool
            will add :py:class:`onetick.query.Passthrough` event processor at the end of resulting graph
        running: bool
            Indicates whether a query is CEP or not.
        start_time_expression: str, :py:class:`~onetick.py.Operation`, optional
            Start time onetick expression of the query. If specified, it will take precedence over ``start``.
        end_time_expression: str, :py:class:`~onetick.py.Operation`, optional
            End time onetick expression of the query. If specified, it will take precedence over ``end``.
        symbol_date: :py:class:`otp.datetime <onetick.py.datetime>` or :py:class:`datetime.datetime` or int
            Symbol date for the query or integer in the YYYYMMDD format.
            Will be applied only to the main query.

        Returns
        -------
        result: str
            Relative (if ``file_name`` is relative) or absolute path to the created query
            in the format ``file_name::query_name``

        Examples
        --------
        Create the .otq file from a :class:`otp.Source <onetick.py.Source>` object:

        >>> t = otp.Tick(A=1)
        >>> t.to_otq()  # doctest: +SKIP
        '/tmp/test_user/run_20251202_181018_11054/impetuous-bullfrog.to_otq.otq::query'
        """
        if raw is not None:
            warnings.warn('The "raw" flag is deprecated and makes no effect', FutureWarning)

        if timezone is None:
            timezone = configuration.config.tz

        file_path = str(file_name) if file_name is not None else None
        if file_suffix is None:
            file_suffix = self._name_suffix('to_otq.otq')

        if query_name is None:
            query_name = self.get_name(remove_invalid_symbols=True)
        if query_name is None:
            query_name = 'query'

        if isinstance(start, _Operation) and start_time_expression is None:
            start_time_expression = str(start)
            start = utils.adaptive
        if isinstance(end, _Operation) and end_time_expression is None:
            end_time_expression = str(end)
            end = utils.adaptive

        if isinstance(start_time_expression, _Operation):
            start_time_expression = str(start_time_expression)
        if isinstance(end_time_expression, _Operation):
            end_time_expression = str(end_time_expression)

        obj, start, end, symbols = self.__prepare_graph(symbols, start, end)

        graph = obj._to_graph(add_passthrough=add_passthrough)
        graph.set_symbols(symbols)

        return obj._tmp_otq.save_to_file(query=graph, query_name=query_name, file_path=file_path,
                                         file_suffix=file_suffix, start=start, end=end, timezone=timezone,
                                         running_query_flag=running,
                                         start_time_expression=start_time_expression,
                                         end_time_expression=end_time_expression,
                                         symbol_date=symbol_date)

    def _store_in_tmp_otq(self, tmp_otq, operation_suffix="tmp_query", symbols=None, start=None, end=None,
                          raw=None, add_passthrough=True, name=None, timezone=None, symbol_date=None,
                          concurrency=None, batch_size=None):
        """
        Adds this source to the tmp_otq storage

        Parameters
        ----------
        tmp_otq: TmpOtq
            Storage object
        operation_suffix: str
            Suffix string to be added to the autogenerated graph name in the otq file
        name: str, optional
            If specified, this ``name`` will be used to save query
            and ``suffix`` parameter will be ignored.

        Returns
        -------
        result: str
            String with the name of the saved graph (starting with THIS::)
        """
        if raw is not None:
            warnings.warn('The "raw" flag is deprecated and makes no effect', FutureWarning)

        obj, start, end, symbols = self.__prepare_graph(symbols, start, end)
        tmp_otq.merge(obj._tmp_otq)

        if isinstance(start, ott.dt):  # OT save_to_file checks for the datetime time
            start = datetime.fromtimestamp(start.timestamp())
        elif isinstance(start, date):
            start = datetime(start.year, start.month, start.day)
        if isinstance(end, ott.dt):
            end = datetime.fromtimestamp(end.timestamp())
        elif isinstance(end, date):
            end = datetime(end.year, end.month, end.day)

        if timezone is None:
            timezone = configuration.config.tz

        graph = obj._to_graph(add_passthrough=add_passthrough)
        graph.set_start_time(start)
        graph.set_end_time(end)
        graph.set_symbols(symbols)
        if timezone is not None:
            if otq.webapi:
                graph.set_timezone(timezone)
            else:
                graph.time_interval_properties().set_timezone(timezone)

        params = {}
        if symbol_date is not None:
            params['symbol_date'] = symbol_date
        if concurrency is not None:
            params['concurrency'] = concurrency
        if batch_size is not None:
            params['batch_size'] = batch_size
        suffix = self._name_suffix(suffix=operation_suffix, separator='__', remove_invalid_symbols=True)
        return tmp_otq.add_query(graph, suffix=suffix, name=name, params=params)

    def __refresh_hash(self):
        """
        This internal function refreshes hash for every graph modification.
        It is used only in _ProxyNode, because it tracks nodes changes
        """
        self.__hash = uuid.uuid3(uuid.NAMESPACE_DNS, str(self.__hash))

    def _prepare_for_execution(self, symbols=None, start=None, end=None, start_time_expression=None,
                               end_time_expression=None, timezone=None, has_output=None,
                               running_query_flag=None, require_dict=False, node_name=None,
                               symbol_date=None):
        if has_output is None:
            has_output = self.__source_has_output

        if timezone is None:
            timezone = configuration.config.tz

        obj, start, end, symbols = self.__prepare_graph(symbols, start, end, has_output)
        require_dict = require_dict or _is_dict_required(symbols)

        if node_name is None:
            node_name = 'SOURCE_CALL_MAIN_OUT_NODE'
            obj.node().node_name(node_name)

        graph = obj._to_graph(add_passthrough=False)

        graph.set_symbols(symbols)

        # create name and suffix for generated .otq file
        if otp.config.main_query_generated_filename:
            name = otp.config.main_query_generated_filename
            if name.endswith('.otq'):
                suffix = ''
            else:
                suffix = '.otq'
            force = True
        else:
            name = ''
            suffix = self._name_suffix('run.otq')
            force = False

        clean_up = default
        if otp.config.otq_debug_mode:
            clean_up = False
        base_dir = None
        if os.getenv('OTP_WEBAPI_TEST_MODE'):
            from onetick.py.otq import _tmp_otq_path
            base_dir = _tmp_otq_path()
        tmp_file = utils.TmpFile(name=name,
                                 suffix=suffix,
                                 force=force,
                                 base_dir=base_dir,
                                 clean_up=clean_up)

        query_to_run = obj._tmp_otq.save_to_file(query=graph,
                                                 query_name=self.get_name(remove_invalid_symbols=True)
                                                 if self.get_name(remove_invalid_symbols=True) else "main_query",
                                                 file_path=tmp_file.path,
                                                 start=start, end=end,
                                                 running_query_flag=running_query_flag,
                                                 start_time_expression=start_time_expression,
                                                 end_time_expression=end_time_expression,
                                                 timezone=timezone,
                                                 symbol_date=symbol_date)

        # PY-1423: we should set symbol_date in otp.run always
        symbol_date_to_run = None
        if symbol_date is not None:
            symbol_date_to_run = utils.symbol_date_to_str(symbol_date)
        else:
            symbol_dates_in_tmp_otq = obj._tmp_otq._get_symbol_dates()
            if symbol_dates_in_tmp_otq:
                symbol_date_to_run = symbol_dates_in_tmp_otq[0]
                if len(set(symbol_dates_in_tmp_otq)) > 1:
                    warnings.warn(
                        f'There are different symbol dates in resulting .otq file: {set(symbol_dates_in_tmp_otq)}.'
                        'But no symbol date were specified in otp.run.\n'
                        f'In this case the first symbol_date ({symbol_date_to_run}) will be used automatically.'
                    )

        return query_to_run, require_dict, node_name, symbol_date_to_run

    def __call__(self, *args, **kwargs):
        """
        .. deprecated:: 1.48.3
           Use :py:func:`otp.run <onetick.py.run>` instead.
        """
        warnings.warn('__call__() method is deprecated, use otp.run() instead', FutureWarning, stacklevel=2)
        return otp.run(self, *args, **kwargs)

    def to_df(self, symbols=None, **kwargs):
        """
        .. deprecated:: 1.48.3
           Use :py:func:`otp.run <onetick.py.run>` instead.
        """
        warnings.warn('to_df() method is deprecated, use otp.run() instead', FutureWarning, stacklevel=2)
        # For backward compatibility: otp.run() does not accept "symbols" as a non-keyword argument
        if symbols is not None:
            kwargs['symbols'] = symbols
        return otp.run(self, **kwargs)

    to_dataframe = to_df

    def print_api_graph(self):
        self.node().copy_graph(print_out=True)

    def _add_table(self, strict=False):
        table = otq.Table(
            fields=",".join(
                ott.type2str(dtype) + " " + name for name, dtype in self.columns(skip_meta_fields=True).items()
            ),
            keep_input_fields=not strict,
        )
        self.sink(table)

    def _is_unbound_required(self):
        """ Check whether a graph needs unbound symbol or not """

        for symbol in self.__sources_symbols.values():
            if symbol is adaptive or symbol is adaptive_to_default:
                return True
        return False

    def _get_widest_time_range(self):
        """
        Get minimum start time and maximum end time.
        If time is not found, None is returned.
        """
        start_times = []
        end_times = []

        for start, end in self.__sources_keys_dates.values():
            if start is not adaptive:
                start_times.append(start)
            if end is not adaptive:
                end_times.append(end)

        start = min(start_times) if start_times else None
        end = max(end_times) if end_times else None
        return start, end

    def __get_common_symbol(self):
        need_to_bind_symbol = False
        common_symbol = None

        # let's try to understand whether we could use common symbol for all sources
        # or we need to bound symbols instead
        first_symbol = None

        for symbol in self.__sources_symbols.values():
            if first_symbol is None:
                first_symbol = symbol

                if isinstance(first_symbol, _ManuallyBoundValue):
                    # Mark that we need to bound, but keep common_symbol equal to None.
                    # It is applicable for the bound symbols inside the merge with bound
                    # symbols, for example.
                    need_to_bind_symbol = True
                else:
                    common_symbol = symbol

                continue

            if symbol and symbol != first_symbol:
                need_to_bind_symbol = True
                common_symbol = None
                break

        # symbol is specified nowhere - just set unbound to the default one
        if (first_symbol is adaptive or first_symbol is adaptive_to_default) and (
            common_symbol is adaptive or common_symbol is adaptive_to_default
        ):
            common_symbol = configuration.config.default_symbol

        return common_symbol, need_to_bind_symbol

    def __get_modify_query_times(self, key, start, end, sources_start, sources_end):
        # determine whether we have to add modify_query_times to a src

        if self.__sources_modify_query_times[key]:
            return None

        start_date, end_date = self.__sources_keys_dates[key]

        if start_date is adaptive and end_date is adaptive:
            return None

        # if some of the end is specified, then it means
        # we need to check whether it is worth to wrap into the modify_query_times
        if start_date is adaptive:
            if start is None:
                start_date = sources_start
            else:
                start_date = start

        if end_date is adaptive:
            if end is None:
                end_date = sources_end
            else:
                end_date = end

        if start_date is adaptive or end_date is adaptive:
            return None

        # it might happen when either sources_start/end are adaptive
        # or start/end are adaptive
        if (
            (start is None and sources_start is not adaptive and start_date != sources_start)
            or (start is not None and start_date != start)
            or (end is None and sources_end is not adaptive and end_date != sources_end)
            or (end is not None and end_date != end)
        ):
            mqt_format = 'parse_time("%Y-%m-%d %H:%M:%S.%q","{}", _TIMEZONE)'
            mqt_ep = otq.ModifyQueryTimes(
                start_time=mqt_format.format(start_date.strftime("%Y-%m-%d %H:%M:%S.%f")),
                end_time=mqt_format.format(end_date.strftime("%Y-%m-%d %H:%M:%S.%f")),
                output_timestamp="TIMESTAMP",
            )

            return mqt_ep
        return None

    def _set_date_range_and_symbols(self, symbols=None, start=None, end=None):
        # will modify self

        if symbols is None:
            common_symbol, need_to_bind_symbol = self.__get_common_symbol()
        else:
            # when unbound symbols passed
            common_symbol = symbols
            need_to_bind_symbol = True  # use to check all sources whether some has bound symbols

        # Find max and min for _source data ranges
        sources_start, sources_end = self._get_widest_time_range()
        sources_start = sources_start or configuration.config.get('default_start_time', adaptive)
        sources_end = sources_end or configuration.config.get('default_end_time', adaptive)

        for key in self.__sources_keys_dates:

            # find a function that builds _source
            func = self.__sources_base_ep_func[key]
            if not func:
                continue

            src = func()

            mqt_ep = self.__get_modify_query_times(key, start, end, sources_start, sources_end)
            if mqt_ep:
                self.__sources_modify_query_times[key] = True
                src.sink(mqt_ep)

            if need_to_bind_symbol:
                bound = None
                if key in self.__sources_symbols:  # TODO: this is wrong, we need to know about symbols
                    # it happens when we do not copy symbols when apply
                    # merge with bound symbols.
                    # Wrong, in that case merge with bound symbol is
                    # non distinguishable from the manually passed None
                    # for external queries
                    bound = self.__sources_symbols[key]
                    if isinstance(bound, _ManuallyBoundValue):
                        bound = bound.value

                if bound and bound is not adaptive and bound is not adaptive_to_default:
                    src.__node.symbol(bound)
                else:
                    # if key is not in __sources_symbols, then
                    # it means that symbol was not specified, and
                    # therefor use unbound symbol
                    if common_symbol is None:
                        if bound is adaptive_to_default:
                            src.__node.symbol(configuration.config.default_symbol)
                        # TODO: write test validated this
                        # else:
                        #     raise Exception("One of the branch does not have symbol specified")

            # --------------------------
            # glue _source with the main graph
            self.node().add_rules(src.node().copy_rules())
            self.source_by_key(src.node().copy_graph(), key)
            self._merge_tmp_otq(src)

        if start is None:
            start = sources_start

        if end is None:
            end = sources_end

        return start, end, common_symbol

    def _to_graph(self, add_passthrough=True):
        """
        Construct the graph. Only for internal usage.

        It is private, because it constructs the raw graph assuming that a graph
        is already defined, and might confuse an end user, because by default Source
        is not fully defined; it becomes fully defined only when symbols, start and
        end datetime are specified.
        """
        constructed_obj = self.copy()

        # we add it for case when the last EP has a pin output
        if add_passthrough:
            constructed_obj.sink(otq.Passthrough())

        return otq.GraphQuery(constructed_obj.node().get())

    def to_graph(self, raw=None, symbols=None, start=None, end=None, *, add_passthrough=True):
        """
        Construct an :py:class:`onetick.query.GraphQuery` object.

        Parameters
        ----------
        raw:
            .. deprecated:: 1.4.17 has no effect

        symbols:
            symbols query to add to otq.GraphQuery
        start: :py:class:`otp.datetime <onetick.py.datetime>`
            start time of a query
        end: :py:class:`otp.datetime <onetick.py.datetime>`
            end time of a query
        add_passthrough: bool
            add additional :py:class:`onetick.query.Passthrough` event processor to the end of a resulted graph

        Returns
        -------
        otq.GraphQuery

        See Also
        --------
        :meth:`render`
        """

        if raw is not None:
            warnings.warn('The "raw" flag is deprecated and makes not effect', FutureWarning)

        _obj, _start, _end, _symbols = self.__prepare_graph(symbols, start, end)

        if _obj._tmp_otq.queries:
            warnings.warn('Using .to_graph() for a Source object that uses sub-queries! '
                          'This operation is deprecated and is not guaranteed to work as expected. '
                          'Such a Source should be executed using otp.run() or saved to disk using to_otq()',
                          FutureWarning)
            _obj.sink(otq.Passthrough().output_pin_name('OUT_FOR_TO_GRAPH'))
            _graph = _obj._to_graph(add_passthrough=False)
            _graph.set_start_time(_start)
            _graph.set_end_time(_end)
            _graph.set_symbols(_symbols)

            query = _obj._tmp_otq.save_to_file(query=_graph, file_suffix='_to_graph.otq')
            query_path, query_name = query.split('::')
            query_params = get_query_parameter_list(query_path, query_name)

            source_with_nested_query = otp.Query(otp.query(query,
                                                           **{param: f'${param}' for param in query_params}),
                                                 out_pin='OUT_FOR_TO_GRAPH')
            return source_with_nested_query.to_graph(
                symbols=_symbols, start=_start, end=_end,
                add_passthrough=add_passthrough)
        else:
            return _obj._to_graph(add_passthrough=add_passthrough)

    def render(self, **kwargs):
        """
        Renders a calculation graph using the ``graphviz`` library.
        Every node is the onetick query language event processor.
        Nodes in nested queries, first stage queries and eval queries are not shown.
        Could be useful for debugging and in jupyter to learn the underlying graph.

        Note that it's required to have :graphviz:`graphviz <>` package installed.

        Examples
        --------
        >>> data = otp.Tick(X=3)
        >>> data1, data2 = data[(data['X'] > 2)]
        >>> data = otp.merge([data1, data2])
        >>> data.render()  # doctest: +SKIP

        .. graphviz:: ../../static/render_example.dot
        """
        kwargs.setdefault('verbose', True)
        self.to_graph().render(**kwargs)

    def render_otq(
        self,
        image_path: Optional[str] = None,
        output_format: Optional[str] = None,
        load_external_otqs: bool = True,
        view: bool = False,
        line_limit: Optional[Tuple[int, int]] = (10, 30),
        parse_eval_from_params: bool = False,
        render_debug_info: bool = False,
        debug: bool = False,
        graphviz_compat_mode: bool = False,
        **kwargs,
    ):
        """
        Render current :py:class:`~onetick.py.Source` graph.

        Parameters
        ----------
        image_path: str, None
            Path for generated image. If omitted, image will be saved in a temp dir
        output_format: str, None
            `Graphviz` rendering format. Default: `png`.
            If `image_path` contains one of next extensions, `output_format` will be set automatically:
            `png`, `svg`, `dot`.
        load_external_otqs: bool
            If set to `True` (default) dependencies from external .otq files (not listed in ``path`` param)
            will be loaded automatically.
        view: bool
            Defines should generated image be shown after render.
        line_limit: Tuple[int, int], None
            Limit for maximum number of lines and length of some EP parameters strings.
            First param is limit of lines, second - limit of characters in each line.
            If set to None limit disabled.
            If one of tuple values set to zero the corresponding limit disabled.
        parse_eval_from_params: bool
            Enable parsing and printing `eval` sub-queries from EP parameters.
        render_debug_info: bool
            Render additional debug information.
        debug: bool
            Allow to print stdout or stderr from `Graphviz` render.
        graphviz_compat_mode: bool
            Change internal parameters of result graph for better compatibility with old `Graphviz` versions.
            Could produce larger and less readable graphs.
        kwargs:
            Additional arguments to be passed to :py:meth:`onetick.py.Source.to_otq` method (except
            ``file_name``, ``file_suffix`` and ``query_name`` parameters)

        Returns
        -------
        Path to rendered image

        See also
        --------
        :py:func:`render_otq <onetick.py.utils.render_otq>`

        Examples
        --------

        >>> data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAA')  # doctest: +SKIP
        >>> data1, data2 = data[(data['PRICE'] > 50)]  # doctest: +SKIP
        >>> data = otp.merge([data1, data2])  # doctest: +SKIP
        >>> data.render_otq('./path/to/image.png')  # doctest: +SKIP

        .. image:: ../../static/testing/images/render_otq_3.png
        """

        if {'file_name', 'file_suffix', 'query_name'} & kwargs.keys():
            raise ValueError(
                'It\'s not allowed to pass parameters `file_name`, `file_suffix` and `query_name` as `kwargs` '
                'in `render_otq` method.'
            )

        otq_path = self.to_otq(**kwargs)
        return render_otq(
            otq_path, image_path, output_format, load_external_otqs, view, line_limit, parse_eval_from_params,
            render_debug_info, debug, graphviz_compat_mode,
        )

    def copy(self, ep=None, columns=None, deep=False) -> 'Source':
        """
        Build an object with copied calculation graph.

        Every node of the resulting graph has the same id as in the original. It means that
        if the original and copied graphs are merged or joined together further then all common
        nodes (all that created before the .copy() method) will be glued.

        For example, let's imagine that you have the following calculation graph ``G``

        .. graphviz::

           digraph {
             rankdir="LR";
             A -> B;
           }

        where ``A`` is a source and ``B`` is some operation on it.

        Then we copy it to the ``G'`` and assign a new operation there

        .. graphviz::

           digraph {
             rankdir="LR";
             A -> B -> C;
           }

        After that we decided to merge ``G`` and ``G'``. The resulting calculation graph will be:

        .. graphviz::

           digraph {
             rankdir="LR";
             A -> B -> C -> MERGE;
             B -> MERGE;
           }

        Please use the :meth:`Source.deepcopy` if you want to get the following calculation graph after merges and joins

        .. graphviz::

           digraph {
             rankdir="LR";
             A -> B -> C -> MERGE;
             "A'" -> "B'" -> "C'" -> MERGE;
           }

        Returns
        -------
        Source

        See Also
        --------
        Source.deepcopy
        """
        if columns is None:
            columns = self.columns(skip_meta_fields=True)

        if ep:
            result = self.__class__(node=ep, schema=columns)
            result.source(self.node().copy_graph())
            # we need to clean it, because ep is not a _source
            result._clean_sources_dates()
        else:
            result = self.__class__(node=self.node(), schema=columns)

        result.node().add_rules(self.node().copy_rules(deep=deep))
        result._set_sources_dates(self)
        if deep:
            # generating all new uuids for node history and for sources
            # after they were initialized
            keys = defaultdict(uuid.uuid4)  # type: ignore
            result.node().rebuild_graph(keys)
            result._change_sources_keys(keys)

        # add state
        result._copy_state_vars_from(self)

        result._tmp_otq = self._tmp_otq.copy()
        result.__name = self.__name     #noqa

        result._copy_properties_from(self)

        return result

    def deepcopy(self, ep=None, columns=None) -> 'onetick.py.Source':
        """
        Copy all graph and change ids for every node.
        More details could be found in :meth:`Source.copy`

        See Also
        --------
        Source.copy
        """
        return self.copy(ep, columns, deep=True)

    def _copy_properties_from(self, obj):
        # needed if we are doing copy of a child with custom properties
        for attr in set(self.__class__._PROPERTIES) - set(Source._PROPERTIES):
            setattr(self, attr, getattr(obj, attr))

    def _copy_state_vars_from(self, objs):
        self.__dict__["_state_vars"] = StateVars(self, objs)

    def columns(self, skip_meta_fields=False):
        """
        Return columns in data source

        Parameters
        ----------
        skip_meta_fields: bool, default=False
            do not add meta fields
        Returns
        -------
        dict
        """
        result = {}

        for key, value in self.__dict__.items():
            if skip_meta_fields and self._check_key_is_meta(key):
                continue

            if self._check_key_in_properties(key):
                continue

            if isinstance(value, _Column):
                result[value.name] = value.dtype

        return result

    def drop_columns(self):
        """
        Method removes all columns in the python representation, but don't
        drop columns on the data.

        It is used when external query is applied, because we don't know how
        data schema has changed.
        """

        items = []

        for key, value in self.__dict__.items():
            if self._check_key_is_reserved(key):
                continue

            if isinstance(value, _Column):
                items.append(key)

        for item in items:
            del self.__dict__[item]

    def node(self):
        return self.__node

    def tick_type(self, tt):
        self.__node.tick_type(tt)
        return self

    def symbol(self, symbol):  # NOSONAR
        """
        Apply symbol to graph

        .. deprecated:: 1.3.31

        """
        warnings.warn("symbol method is deprecated, please specify symbol during creation", FutureWarning)
        self.__node.symbol(symbol)
        return self

    def node_name(self, name=None, key=None):
        return self.__node.node_name(name, key)

    def _fix_varstrings(self):
        """
        PY-556: converting to varstring results in string with null-characters
        """
        varstring_columns = {
            name: self[name]
            for name, dtype in self.schema.items()
            if dtype is ott.varstring
        }
        # just updating the column removes null-characters
        if varstring_columns:
            self.update(varstring_columns, inplace=True)

    def __from_ep_to_proxy(self, ep):
        in_pin, out_pin = None, None
        if isinstance(ep, otq.graph_components.EpBase.PinnedEp):
            if hasattr(ep, "_output_name"):
                out_pin = getattr(ep, "_output_name")
            else:
                in_pin = getattr(ep, "_input_name")

            ep = getattr(ep, "_ep")

        return ep, uuid.uuid4(), in_pin, out_pin

    def sink(self, ep, out_pin=None, inplace: bool = True):
        """
        Appends ``ep`` node to this source (inplace by default).
        Connects ``out_pin`` of this source to ``ep``.

        Can be used to connect onetick.query objects to :class:`onetick.py.Source`.

        Data schema changes (added or deleted columns) will not be detected automatically
        after applying this function, so the user must change the schema himself
        by updating :meth:`onetick.py.Source.schema` property.

        Parameters
        ----------
        ep: otq.graph_components.EpBase,\
            otq.graph_components.EpBase.PinnedEp,\
            Tuple[otq.graph_components.EpBase, uuid.uuid4, Optional[str], Optional[str]]
            onetick.query EP object to append to source.
        out_pin: Optional[str], default=None
            name of the out pin to connect to ``ep``
        inplace: bool, default=False
            if `True` method will modify current object,
            otherwise it will return modified copy of the object.

        Returns
        ----------
        :class:`Source` or ``None``
            Returns ``None`` if ``inplace=True``.

        See Also
        --------
        onetick.py.Source.schema
        onetick.py.core._source.schema.Schema

        Examples
        --------
        Adding column 'B' directly with onetick.query EP.

        >>> data = otp.Tick(A=1)
        >>> data.sink(otq.AddField(field='B', value=2)) # OTdirective: skip-snippet:;
        >>> otp.run(data) # OTdirective: skip-snippet:;
                Time  A  B
        0 2003-12-01  1  2

        But we can't use this column with `onetick.py` methods yet:

        >>> data['C'] = data['B'] # OTdirective: skip-snippet:; # doctest: +ELLIPSIS
        Traceback (most recent call last):
         ...
        AttributeError: There is no 'B' column

        We should manually change source's schema:

        >>> data.schema.update(B=int) # OTdirective: skip-snippet:;
        >>> data['C'] = data['B']
        >>> otp.run(data)
                Time  A  B  C
        0 2003-12-01  1  2  2

        Use parameter ``inplace=False`` to return modified copy of the source:

        >>> data = otp.Tick(A=1)
        >>> new_data = data.sink(otq.AddField(field='B', value=2), inplace=False)
        >>> otp.run(data)
                Time  A
        0 2003-12-01  1
        >>> otp.run(new_data)
                Time  A  B
        0 2003-12-01  1  2
        """
        if not (
            issubclass(type(ep), otq.graph_components.EpBase)
            or issubclass(type(ep), otq.graph_components.EpBase.PinnedEp)
            or isinstance(ep, tuple)
        ):
            raise TypeError("sinking is allowed only for EpBase instances")

        if inplace:
            obj = self
        else:
            obj = self.copy()

        if isinstance(ep, tuple):
            # for already existed EP fetched from _ProxyNode
            obj.__node.sink(out_pin, *ep)
        else:
            obj.__node.sink(out_pin, *obj.__from_ep_to_proxy(ep))

        if inplace:
            return None
        return obj

    def __rshift__(self, ep):
        """ duplicates sink() method, but returns new object """
        new_source = self.copy()
        new_source.sink(ep)
        return new_source

    def __irshift__(self, ep):
        """ duplicates sink() method, but assigns source new object """
        new_source = self.copy()
        new_source.sink(ep)
        return new_source

    def source(self, ep, in_pin=None):
        """ Add node as source to root node """
        if not (
            issubclass(type(ep), otq.graph_components.EpBase)
            or issubclass(type(ep), otq.graph_components.EpBase.PinnedEp)
            or isinstance(ep, tuple)
        ):
            raise TypeError("sourcing is allowed only for EpBase instances")

        if isinstance(ep, tuple):
            # for already existed EP fetched from _ProxyNode
            return self.__node.source(in_pin, *ep)
        else:
            return self.__node.source(in_pin, *self.__from_ep_to_proxy(ep))

    def source_by_key(self, ep, to_key):
        """ Add node as source to graph node by key"""
        if not (
            issubclass(type(ep), otq.graph_components.EpBase)
            or issubclass(type(ep), otq.graph_components.EpBase.PinnedEp)
            or isinstance(ep, tuple)
        ):
            raise TypeError("sourcing is allowed only for EpBase instances")

        if isinstance(ep, tuple):
            # for already existed EP fetched from _ProxyNode
            return self.__node.source_by_key(to_key, *ep)
        else:
            return self.__node.source_by_key(to_key, *self.__from_ep_to_proxy(ep))

    def to_symbol_param(self):
        """
        Creates a read-only instance with the same columns except Time.
        It is used as a result of a first stage query with symbol params.

        See also
        --------
        :ref:`static/concepts/symbols:Symbol parameters`

        Examples
        --------
        >>> symbols = otp.Ticks({'SYMBOL_NAME': ['S1', 'S2'], 'PARAM': ['A', 'B']})
        >>> symbol_params = symbols.to_symbol_param()
        >>> t = otp.DataSource('SOME_DB', tick_type='TT')
        >>> t['S_PARAM'] = symbol_params['PARAM']
        >>> result = otp.run(t, symbols=symbols)
        >>> result['S1']
                             Time  X S_PARAM
        0 2003-12-01 00:00:00.000  1       A
        1 2003-12-01 00:00:00.001  2       A
        2 2003-12-01 00:00:00.002  3       A
        """
        return _SymbolParamSource(**self.columns())

    @staticmethod
    def _convert_symbol_to_string(symbol, tmp_otq=None, start=None, end=None, timezone=None, symbol_date=None):
        if start is adaptive:
            start = None
        if end is adaptive:
            end = None

        if isinstance(symbol, Source):
            symbol = otp.eval(symbol).to_eval_string(tmp_otq=tmp_otq,
                                                     start=start, end=end, timezone=timezone,
                                                     operation_suffix='symbol',
                                                     query_name=None,
                                                     file_suffix=symbol._name_suffix('symbol.otq'),
                                                     symbol_date=symbol_date)

        if isinstance(symbol, otp.query):
            return symbol.to_eval_string()

        if isinstance(symbol, otq.GraphQuery):
            params = {'symbol_date': symbol_date} if symbol_date is not None else {}
            query_name = tmp_otq.add_query(symbol, suffix='__symbol', params=params)
            return f'eval(THIS::{query_name})'

        return symbol

    @staticmethod
    def _construct_multi_branch_graph(branches):
        # TODO: add various checks, e.g. that branches have common parts
        main = branches[0].copy()
        for branch in branches[1:]:
            main.node().add_rules(branch.node().copy_rules())
            main._merge_tmp_otq(branch)
        return main

    def _apply_side_branches(self, side_branches):
        for side_branch in side_branches:
            self.node().add_rules(side_branch.node().copy_rules())
            self._merge_tmp_otq(side_branch)
            self.__sources_keys_dates.update(side_branch.__sources_keys_dates)
            self.__sources_modify_query_times.update(side_branch.__sources_modify_query_times)
            self.__sources_base_ep_func.update(side_branch.__sources_base_ep_func)
            self.__sources_symbols.update(side_branch.__sources_symbols)

    @property
    def state_vars(self) -> StateVars:
        """
        Provides access to state variables

        Returns
        -------
        State Variables: Dict[str, state variable]
            State variables, you can access one with its name.

        See Also
        --------
        | `State Variables \
         <../../static/getting_started/variables_and_data_structures.html#variables-and-data-structures>`_
        | **DECLARE_STATE_VARIABLES** OneTick event processor

        """
        return self.__dict__['_state_vars']

    # non word characters are not supported
    __invalid_query_name_symbols_regex = re.compile(r'\W')

    def __remove_invalid_symbols(self, s):
        """
        Replaces symbols that cannot be put in query names with '_'
        """
        return self.__invalid_query_name_symbols_regex.sub('_', s)

    def get_name(self, remove_invalid_symbols=False) -> Optional[str]:
        """
        Returns source name.

        Parameters
        ----------
        remove_invalid_symbols: bool
            If True, all characters not supported in query names in `.otq` file will be replaced,
            because only alphanumeric, minus and underscore characters are supported in query names.

        See also
        --------
        :meth:`set_name`
        """
        if remove_invalid_symbols and self.__name:
            return self.__remove_invalid_symbols(self.__name)
        else:
            return self.__name

    def set_name(self, new_name):
        """
        Sets source name.
        It's an internal onetick-py name of the source that is only used
        as a part of the resulting .otq file name and as the name of the query inside this file.

        This method doesn't set the name of the OneTick graph node.

        Parameters
        ----------
        new_name: str
            New name of the source.

            Only alphanumeric, minus and underscore characters are supported.
            All other characters will be replaced in the resulting query name.

        See also
        --------
        :meth:`get_name`

        Examples
        --------
        >>> t = otp.Tick(A=1)

        By default source has no name and some predefined values are used when generating .otq file:

        >>> t.to_otq()  # doctest: +SKIP
        '/tmp/test_user/run_20240126_152546_1391/magnificent-wolverine.to_otq.otq::query'

        Changed name will be used as a part of the resulting .otq file name
        and as the name of the query inside this file:

        >>> t.set_name('main')
        >>> t.to_otq()  # doctest: +SKIP
        '/tmp/test_user/run_20240126_152546_1391/dandelion-angelfish.main.to_otq.otq::main'
        """
        assert isinstance(new_name, str) or new_name is None, "Source name must be a string or None."
        if new_name is not None:
            assert new_name != '', "Source name must be a non-empty string."
        self.__name = new_name

    def _name_suffix(self, suffix, separator='.', remove_invalid_symbols=False):
        if remove_invalid_symbols:
            suffix = self.__remove_invalid_symbols(suffix)
            separator = self.__remove_invalid_symbols(separator)
            name = self.get_name(remove_invalid_symbols=True)
        else:
            name = self.__name
        return f'{separator}{name}{separator}{suffix}' if name else f'{separator}{suffix}'

    @property
    def schema(self) -> Schema:
        """
        Represents actual python data schema in the column-name -> type format.
        For example, could be used after the :meth:`Source.sink` to adjust
        the schema.

        Returns
        -------
        Schema

        See Also
        --------
        Source.sink

        Examples
        --------

        >>> data = otp.Ticks([['X', 'Y',   'Z'],
        ...                   [  1, 0.5, 'abc']])
        >>> data['T'] = data['Time']
        >>> data.schema
        {'X': <class 'int'>, 'Y': <class 'float'>, 'Z': <class 'str'>, 'T': <class 'onetick.py.types.nsectime'>}

        >>> data.schema['X']
        <class 'int'>

        >>> data.schema['X'] = float
        >>> data.schema['X']
        <class 'float'>

        >>> 'W' in data.schema
        False
        >>> data.schema['W'] = otp.nsectime
        >>> 'W' in data.schema
        True
        >>> data.schema['W']
        <class 'onetick.py.types.nsectime'>
        """
        schema = self.columns(skip_meta_fields=True)
        # meta fields will be in schema, but hidden
        hidden_columns = {
            k: v
            for k, v in self.columns(skip_meta_fields=False).items()
            if self._check_key_is_meta(k)
        }
        if 'TIMESTAMP' in hidden_columns:
            hidden_columns['Time'] = hidden_columns['TIMESTAMP']
        return Schema(_base_source=self, _hidden_columns=hidden_columns, **schema)

    def set_schema(self, **kwargs):
        """
        Set schema of the source.
        Note: this method affect python part only and won't make any db queries. It used to set schema after db reading/
        complex query.

        .. deprecated:: 1.14.9

        Please use the :property:`Source.schema` to access and adjust the schema.

        Parameters
        ----------
        kwargs
            schema in the column_name=type format

        Examples
        --------
        Python can't follow low level change of column, e.g. complex query or pertick script can be sink.

        >>> data = otp.Ticks(dict(A=[1, 2], B=["a", "b"]))
        >>> data.sink(otq.AddField(field='Z', value='5'))
        >>> data.columns(skip_meta_fields=True)
        {'A': <class 'int'>, 'B': <class 'str'>}
        >>> # OTdirective: snippet-name: Arrange.schema.set;
        >>> data.set_schema(A=int, B=str, Z=int)
        >>> data.columns(skip_meta_fields=True)
        {'A': <class 'int'>, 'B': <class 'str'>, 'Z': <class 'int'>}
        """
        self.drop_columns()
        for name, dtype in kwargs.items():
            dtype = ott.get_source_base_type(dtype)
            if self._check_key_is_meta(name):
                warnings.warn(f"Setting type in schema for meta field {name}", stacklevel=2)
            if self._check_key_in_properties(name):
                raise ValueError(f"Can't set type in schema for class property {name}")
            self.__dict__[name] = _Column(name, dtype, self)

    def has_start_end_time(self) -> Tuple[bool, bool]:
        """
        Check if at least one of query sources has start and end time
        """
        has_start_time = False
        has_end_time = False

        for start, end in self._get_sources_dates().values():
            if not has_start_time and start is not adaptive and start is not None:
                has_start_time = True

            if not has_end_time and end is not adaptive and end is not None:
                has_end_time = True

        return has_start_time, has_end_time

    from ._source.source_methods.aggregations import (  # type: ignore[misc]
        agg,
        high, low, first, last, distinct, high_time, low_time,
        ob_snapshot, ob_snapshot_wide, ob_snapshot_flat, ob_summary,
        ob_size, ob_vwap, ob_num_levels,
        ranking, percentile, find_value_for_percentile,
        exp_w_average, exp_tw_average, standardized_moment,
        portfolio_price, multi_portfolio_price, return_ep, implied_vol,
        linear_regression,
        process_by_group,
    )
    from ._source.source_methods.joins import (  # type: ignore[misc]
        _process_keep_time_param,
        _get_columns_with_prefix,
        join_with_collection,
        join_with_query,
        point_in_time,
        join_with_snapshot,
    )
    from ._source.source_methods.times import (  # type: ignore[misc]
        update_timestamp,
        modify_query_times,
        time_interval_shift,
        time_interval_change,
    )
    from ._source.source_methods.fields import (  # type: ignore[misc]
        _add_field,
        _update_timestamp,
        _update_field,
        __setattr__,
        __setitem__,
        add_fields,
        table,
        update,
    )
    from ._source.source_methods.filters import (  # type: ignore[misc]
        if_else,
        where_clause,
        where,
        _get_integer_slice,
        __getitem__,
        dropna,
        time_filter,
        skip_bad_tick,
        character_present,
    )
    from ._source.source_methods.drops import (  # type: ignore[misc]
        drop,
        __delitem__,
    )
    from ._source.source_methods.writes import (  # type: ignore[misc]
        write,
        write_parquet,
        save_snapshot,
        write_text,
    )
    from ._source.source_methods.renames import (  # type: ignore[misc]
        _add_prefix_and_suffix,
        add_prefix,
        add_suffix,
        rename,
    )
    from ._source.source_methods.pandases import (  # type: ignore[misc]
        plot,
        count,
        head,
        tail,
    )
    from ._source.source_methods.sorts import (  # type: ignore[misc]
        sort_values,
        sort,
    )
    from ._source.source_methods.debugs import (  # type: ignore[misc]
        dump,
        throw,
        logf,
    )
    from ._source.source_methods.applyers import (  # type: ignore[misc]
        apply_query,
        apply,
        script,
    )
    from ._source.source_methods.symbols import (  # type: ignore[misc]
        show_symbol_name_in_db,
        modify_symbol_name,
    )
    from ._source.source_methods.columns import (  # type: ignore[misc]
        mean,
        unite_columns,
    )
    from ._source.source_methods.switches import (  # type: ignore[misc]
        switch,
        split,
    )
    from ._source.source_methods.misc import (  # type: ignore[misc]
        pause,
        insert_tick,
        insert_at_end,
        transpose,
        cache,
        pnl_realized,
        execute,
        _columns_names_regex,
        fillna,
        mkt_activity,
        book_diff,
        limit,
        virtual_ob,
        corp_actions,
    )
    from ._source.source_methods.merges import (  # type: ignore[misc]
        __add__,
        append,
        diff,
        lee_and_ready,
        estimate_ts_delay,
    )
    from ._source.source_methods.data_quality import (  # type: ignore[misc]
        show_data_quality,
        insert_data_quality_event,
        intercept_data_quality,
        show_symbol_errors,
        intercept_symbol_errors,
    )


_Source = Source    # Backward compatibility
