import inspect
import types
import warnings

from onetick import py as otp
from onetick.py.core._source._symbol_param import _SymbolParamColumn, _SymbolParamSource
from onetick.py.core.column_operations.base import OnetickParameter


class _QueryEvalWrapper:
    def __init__(self, query, params=None, output_field=None, request_substitute_symbol=False,
                 generate_separate_file_only=False):
        self.query = query
        self.params = params
        self.output_field = output_field
        self.request_substitute_symbol = request_substitute_symbol
        self.generate_separate_file_only = generate_separate_file_only
        self._inner_source = None
        if isinstance(query, otp.Source):
            self._inner_source = query
            params = params or {}

            start, end = query._get_widest_time_range()
            if start and end and '_START_TIME' not in params and '_END_TIME' not in params:
                params['_START_TIME'] = start
                params['_END_TIME'] = end

            self.str_params = otp.query._params_to_str(params, with_expr=True)
        elif isinstance(query, otp.query):
            self.path = query.path
            self.str_params = query._params_to_str(params or {}, with_expr=True)
        else:
            raise ValueError("Wrong query parameter, it should be otp.query, otp.Query or function, "
                             "which returns them.")

    def _get_eval_string(self):
        eval_str = f'eval("{self.path}", "{self.str_params}")'
        if self.output_field:
            return f'{eval_str}.{self.output_field}'
        return eval_str

    def to_eval_string(self,
                       tmp_otq=None,
                       operation_suffix='eval',
                       start=None, end=None, timezone=None,
                       file_suffix='_eval_query.otq',
                       query_name='main_eval_query',
                       symbol_date=None) -> str:
        """
        If self._inner_source is not None, then temporary query needs to be saved
        or added to tmp_otq storage
        """
        if self._inner_source is not None:
            # if substitute symbol is requested, then we need to set an unbound symbol for query in eval
            # so that onetick can substitute it with the unbound symbol from the external query
            symbols = None
            if self.request_substitute_symbol:
                symbols = 'SYMBOL_TO_SUBSTITUTE'
            if not self.generate_separate_file_only and tmp_otq is not None:
                tmp_otq.merge(self._inner_source._tmp_otq)
                query_name = self._inner_source._store_in_tmp_otq(tmp_otq, symbols=symbols,
                                                                  start=start, end=end, timezone=timezone,
                                                                  operation_suffix=operation_suffix,
                                                                  symbol_date=symbol_date)
                self.path = f'THIS::{query_name}'
            else:
                self.path = self._inner_source.to_otq(file_suffix=file_suffix,
                                                      symbols=symbols,
                                                      query_name=query_name,
                                                      start=start, end=end, timezone=timezone,
                                                      symbol_date=symbol_date)
        return self._get_eval_string()

    def to_symbol_param(self):
        if self._inner_source:
            return self._inner_source.to_symbol_param()
        else:
            return _SymbolParamSource()

    def __str__(self):
        # TODO: deprecate after PY-952
        # warnings.warn('This method is deprecated, use to_eval_string() instead', FutureWarning)
        return self.to_eval_string()

    def copy(self, output_field=None):
        return _QueryEvalWrapper(query=self.query,
                                 params=self.params,
                                 output_field=output_field,
                                 request_substitute_symbol=self.request_substitute_symbol,
                                 generate_separate_file_only=self.generate_separate_file_only)

    def __getitem__(self, item):
        return self.copy(item)


def eval(query, symbol=None, start=None, end=None,
         generate_separate_file_only=False, **kwargs):
    """
    Creates an object with ``query`` with saved parameters that can be used later.

    It can be used to:

    * return a list of symbols for which the main query
      will be executed (multistage queries).
      Note that in this case ``query`` must return ticks with column **SYMBOL_NAME**.
    * return some value dynamically to be used in other places in the main query.
      Note that in this case ``query`` must return only single tick.

    Note that only constant expressions are allowed in query parameters,
    they must not depend on ticks.

    Parameters
    ----------
    query: :py:class:`onetick.py.Source`, :py:class:`onetick.py.query` or function
        source or query to evaluate.
        If function, then it must return :py:class:`onetick.py.Source` or :py:class:`onetick.py.query`.
        Parameter with name **symbol** and parameters specified in ``kwargs`` will be propagated
        to this function. Parameter from ``kwargs`` *must* be specified in function signature,
        but parameter **symbol** may be omitted if it is not used.
    symbol: :py:class:`~onetick.py.core._source._symbol_param._SymbolParamSource`
        symbol parameter that will be used by ``query`` as a symbol.
        If the function is used as a ``query``, parameter **symbol** can be defined in function
        signature and used in source operations.
    start: meta field (:py:class:`~onetick.py.core.source.MetaFields`) \
           or symbol param (:py:class:`~onetick.py.core._source._symbol_param._SymbolParamColumn`)
        start time with which ``query`` will be executed.
        By default the start time for evaluated query is inherited from the main query.
    end: meta field (:py:class:`~onetick.py.core.source.MetaFields`) \
         or symbol param (:py:class:`~onetick.py.core._source._symbol_param._SymbolParamColumn`)
        end time with which ``query`` will be executed.
        By default the end time for evaluated query is inherited from the main query.
    generate_separate_file_only: bool
        If set, sub-query will be generated in separate file.
        It's needed in some cases, e.g. when generating query for *otq_query_loader_daily.exe*,
        which executes all queries from a file.
    kwargs: str, int, meta fields (:py:class:`~onetick.py.core.source.MetaFields`) \
            or symbol params (:py:class:`~onetick.py.core._source._symbol_param._SymbolParamColumn`)
            or :py:meth:`~onetick.py.core.source.Source.join_with_query` parameters
        parameters that will be passed to ``query``.
        If the function is used as a ``query``, parameters specified in ``kwargs``
        *must* be defined in function signature and can be used in source operations.

    See also
    --------
    :ref:`api/misc/symbol_param:Symbol Parameters Objects`
    :ref:`static/concepts/symbols:Symbol parameters`

    Examples
    --------

    Use ``otp.eval`` to be passed as symbols when running the query:

    >>> def fsq():
    ...     symbols = otp.Ticks(SYMBOL_NAME=['AAPL', 'AAP'])
    ...     return symbols
    >>> main = otp.DataSource(db='US_COMP', tick_type='TRD', date=otp.dt(2022, 3, 1))
    >>> main['SYMBOL_NAME'] = main.Symbol.name
    >>> main = otp.merge([main], symbols=otp.eval(fsq))
    >>> otp.run(main)  # OTdirective: snippet-name: eval with symbols;
                         Time  PRICE  SIZE SYMBOL_NAME
    0 2022-03-01 00:00:00.000   1.30   100        AAPL
    1 2022-03-01 00:00:00.000  45.37     0         AAP
    2 2022-03-01 00:00:00.001   1.40    10        AAPL
    3 2022-03-01 00:00:00.001  45.41     0         AAP
    4 2022-03-01 00:00:00.002   1.40    50        AAPL

    Use ``otp.eval`` as filter:

    >>> def get_filter(a, b):
    ...     return otp.Tick(WHERE=f'X >= {str(a)} and X < {str(b)}', OTHER_FIELD='X')
    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> # note that in this case column WHERE must be specified,
    >>> # because evaluated query returns tick with more than one field
    >>> data = data.where(otp.eval(get_filter, a=0, b=2)['WHERE'])
    >>> otp.run(data)
            Time  X
    0 2003-12-01  1

    Use ``otp.eval`` with meta fields:

    >>> def filter_by_tt(tick_type):
    ...     res = otp.Ticks({
    ...         'TICK_TYPE': ['TRD', 'QTE'],
    ...         'WHERE': ['PRICE>=1.4', 'ASK_PRICE>=1.4']
    ...     })
    ...     res = res.where(res['TICK_TYPE'] == tick_type)
    ...     return res.drop(['TICK_TYPE'])
    >>> t = otp.DataSource('US_COMP::TRD')
    >>> # note that in this case column WHERE do not need to be specified,
    >>> # because evaluated query returns tick with only one field
    >>> t = t.where(otp.eval(filter_by_tt, tick_type=t['_TICK_TYPE']))
    >>> otp.run(t, start=otp.dt(2022, 3, 1), end=otp.dt(2022, 3, 2))
                         Time  PRICE  SIZE
    0 2022-03-01 00:00:00.001    1.4    10
    1 2022-03-01 00:00:00.002    1.4    50
    """
    if isinstance(query, (types.FunctionType, types.LambdaType)) or inspect.ismethod(query):
        params = {}
        params_to_convert = {}
        sig = inspect.signature(query)
        for param in sig.parameters:
            if "symbol" == param:
                if isinstance(symbol, _SymbolParamSource):
                    params['symbol'] = symbol
                else:
                    params["symbol"] = _SymbolParamSource()
            else:
                value = kwargs[param]
                if isinstance(value, otp.Column) and (value.name not in otp.Source.meta_fields
                                                      and not isinstance(value, _SymbolParamColumn)
                                                      and not isinstance(value, OnetickParameter)):
                    raise ValueError('Eval parameters can not depend on tick.')
                params_to_convert[param] = value
        params.update(prepare_params(**params_to_convert))

        query = query(**params)
    params = {}
    request_substitute_symbol = False
    if symbol is not None:
        if not isinstance(symbol, _SymbolParamSource):
            raise ValueError("Symbol parameter has wrong type, are you sure you are using it from function passed "
                             "to merge or join method?")
        params["SYMBOL_NAME"] = symbol.name
        request_substitute_symbol = True

    if start is not None:
        params["_START_TIME"] = start
    if end is not None:
        params["_END_TIME"] = end
    params.update(kwargs)

    return _QueryEvalWrapper(query, params, request_substitute_symbol=request_substitute_symbol,
                             generate_separate_file_only=generate_separate_file_only)


def prepare_params(**kwargs):
    converted_params = {}
    for key, value in kwargs.items():
        dtype = otp.types.get_object_type(value)
        # pylint: disable-next=unidiomatic-typecheck
        if type(value) is str and len(value) > otp.string.DEFAULT_LENGTH:
            dtype = otp.string[len(value)]
        param = OnetickParameter(key, dtype)
        converted_params[key] = param
    return converted_params
