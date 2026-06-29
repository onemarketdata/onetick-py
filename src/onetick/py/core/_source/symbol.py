import warnings

import onetick.py.types as ott
from onetick.py.core._source._symbol_param import _SymbolParamColumn


globals().setdefault("__warningregistry__", {})  # otherwise doctests fails


class SymbolType:
    def __init__(self):
        """
        You can get symbol name and symbol parameters with this class.

        Examples
        --------
        >>> symbols = otp.Symbols('US_COMP_SAMPLE')[:3]
        >>> symbols['PARAM'] = 'PAM'
        >>> ticks = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
        >>> ticks = ticks[['PRICE']][:1]
        >>> ticks['SYMBOL_PARAM'] = ticks.Symbol['PARAM', str]
        >>> ticks['SYMBOL_NAME'] = ticks.Symbol.name
        >>> ticks = otp.merge([ticks], symbols=symbols)
        >>> otp.run(ticks, date=otp.dt(2024, 2, 1))
                                   Time   PRICE SYMBOL_PARAM SYMBOL_NAME
        0 2024-02-01 04:00:00.008283417  186.50          PAM        AAPL
        1 2024-02-01 04:00:00.097381367   14.33          PAM         AAL
        2 2024-02-01 07:56:00.692942080  131.40          PAM           A

        See also
        --------
        | :ref:`Databases, symbols, and tick types <symbols_concept>`
        | :ref:`api/misc/symbol_param:Symbol Parameters Objects`
        """

        self.__name = _SymbolParamColumn("_SYMBOL_NAME", str)

    @property
    def name(self):
        """
        Get symbol name.

        Returns
        -------
        _SymbolParamColumn

        Examples
        --------
        >>> symbols = otp.Symbols('US_COMP_SAMPLE')[:3]
        >>> ticks = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
        >>> ticks = ticks[['PRICE']][:1]
        >>> ticks['SYMBOL_NAME'] = ticks.Symbol.name
        >>> ticks = otp.merge([ticks], symbols=symbols)
        >>> otp.run(ticks, date=otp.dt(2024, 2, 1))
                                   Time   PRICE SYMBOL_NAME
        0 2024-02-01 04:00:00.008283417  186.50        AAPL
        1 2024-02-01 04:00:00.097381367   14.33         AAL
        2 2024-02-01 07:56:00.692942080  131.40           A
        """
        return self.__name

    def get(self, name, dtype, default=None):
        """
        Get symbol parameter by name.

        Parameters
        ----------
        name: str
            The name of the symbol parameter to retrieve.
        dtype: type
            The expected data type of the symbol parameter value.
        default: Any, optional
            The default value to return if the symbol parameter is not found.
            Default is ``None``, which means that default value of dtype will be used.

        Returns
        -------
        Operation

        Examples
        --------
        >>> symbols = otp.Symbols('US_COMP_SAMPLE')[:3]
        >>> symbols['PARAM'] = 'PAM'
        >>> ticks = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
        >>> ticks = ticks[['PRICE']][:1]
        >>> ticks['SYMBOL_PARAM'] = ticks.Symbol.get(name='PARAM', dtype=str, default='default')
        >>> ticks = otp.merge([ticks], symbols=symbols)
        >>> otp.run(ticks, date=otp.dt(2024, 2, 1))
                                   Time   PRICE SYMBOL_PARAM
        0 2024-02-01 04:00:00.008283417  186.50          PAM
        1 2024-02-01 04:00:00.097381367   14.33          PAM
        2 2024-02-01 07:56:00.692942080  131.40          PAM

        >>> symbol1 = otq.Symbol('US_COMP_SAMPLE::AAPL', params={'PARAM': 1})
        >>> symbol2 = otq.Symbol('US_COMP_SAMPLE::MSFT')
        >>> data = otp.DataSource(tick_type='TRD')
        >>> data = data.table(PRICE=float, strict=True)[:1]
        >>> data['P'] = data.Symbol.get(name='PARAM', dtype=int, default=10)
        >>> data = otp.merge([data], symbols=[symbol1, symbol2], identify_input_ts=True)
        >>> otp.run(data, date=otp.dt(2024, 2, 1))
                                   Time   PRICE   P           SYMBOL_NAME TICK_TYPE
        0 2024-02-01 04:00:00.008283417  186.50   1  US_COMP_SAMPLE::AAPL       TRD
        1 2024-02-01 04:00:00.016997102  400.15  10  US_COMP_SAMPLE::MSFT       TRD
        """
        if default is None:
            default = ott.default_by_type(dtype)
        return self._get(item=name, dtype=dtype, default=default)

    @staticmethod
    def _get(item, dtype=str, default=None):
        return _SymbolParamColumn(item, dtype, default=default)

    def __getattr__(self, item):
        """
        Get symbol parameter by name. Notice, that symbol parameter type will be string.

        .. deprecated:: 1.74.0
           Please, use :py:meth:`~onetick.py.core._source.symbol.__getitem__` method.

        Returns
        -------
        _SymbolParamColumn
        """
        if item == '__objclass__':
            # fix for PY-1399 (some inspect functions try to access this special attribute)
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{item}'")
        if not item.startswith('_') and item != 'pytest_mock_example_attribute_that_shouldnt_exist':
            warnings.warn("`__getattr__` method is deprecated. Please, use `__getitem__` method instead.",
                          FutureWarning, stacklevel=2)
        return self._get(item)

    def __getitem__(self, item):
        """
        Get symbol parameter by name.

        Parameters
        ----------
        item: tuple
            The first parameter is string - symbol parameter name. The second one is symbol parameter type.

        Returns
        -------
        _SymbolParamColumn

        Examples
        --------
        The second parameter is symbol parameter's type.

        >>> symbols = otp.Symbols('US_COMP_SAMPLE')[:3]
        >>> symbols['PARAM'] = 5
        >>> ticks = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
        >>> ticks = ticks[['PRICE']][:1]
        >>> ticks['SYMBOL_PARAM'] = ticks.Symbol['PARAM', int] + 1
        >>> ticks['SYMBOL_PARAM'].dtype
        <class 'int'>
        >>> ticks = otp.merge([ticks], symbols=symbols)
        >>> otp.run(ticks, date=otp.dt(2024, 2, 1))
                                   Time   PRICE  SYMBOL_PARAM
        0 2024-02-01 04:00:00.008283417  186.50             6
        1 2024-02-01 04:00:00.097381367   14.33             6
        2 2024-02-01 07:56:00.692942080  131.40             6

        It also works with :py:class:`~onetick.py.msectime` and :py:class:`~onetick.py.nsectime` types:

        >>> symbols = otp.Symbols('US_COMP_SAMPLE')[:3]
        >>> symbols['NSECTIME_PARAM'] = symbols['Time'] + otp.Nano(100)
        >>> symbols['MSECTIME_PARAM'] = symbols['Time'] + otp.Milli(1)
        >>> ticks = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
        >>> ticks = ticks[['PRICE']][:1]
        >>> ticks['NSECTIME_PARAM'] = ticks.Symbol['NSECTIME_PARAM', otp.nsectime] + otp.Nano(1)
        >>> ticks['MSECTIME_PARAM'] = ticks.Symbol['MSECTIME_PARAM', otp.msectime] + otp.Milli(1)
        >>> ticks['NSECTIME_PARAM'].dtype
        <class 'onetick.py.types.nsectime'>
        >>> ticks['MSECTIME_PARAM'].dtype
        <class 'onetick.py.types.msectime'>
        >>> ticks = otp.merge([ticks], symbols=symbols)
        >>> otp.run(ticks, date=otp.dt(2024, 2, 1))
                                   Time   PRICE                NSECTIME_PARAM          MSECTIME_PARAM
        0 2024-02-01 04:00:00.008283417  186.50 2024-02-01 00:00:00.000000101 2024-02-01 00:00:00.002
        1 2024-02-01 04:00:00.097381367   14.33 2024-02-01 00:00:00.000000101 2024-02-01 00:00:00.002
        2 2024-02-01 07:56:00.692942080  131.40 2024-02-01 00:00:00.000000101 2024-02-01 00:00:00.002
        """
        if not isinstance(item, tuple):
            raise ValueError(f"tuple should be passed, but {type(item)} was passed")
        if len(item) != 2:
            raise ValueError(f"tuple's length should be 2 but it is {len(item)}")
        item, dtype = item
        return self._get(item, dtype)


Symbol = SymbolType()
