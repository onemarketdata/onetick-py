import types
from typing import Union

from collections import defaultdict

from ._internal._state_vars import StateVars
from ._internal._state_objects import _StateBase, check_field_name_in_schema
from .column import _Column
from .column_operations.base import _Operation
from .. import types as ott
from ._source.symbol import SymbolType


class _CompareTrackScope:
    """
    We need it to prevent problems when some exceptions are risen, and we left non-default tracking settings.
    This scope guarantee that code works always with the same state of tracker.
    """
    def __init__(self, emulation_enabled: bool = True):
        self.emulation_enabled = emulation_enabled

    def __enter__(self):
        _Operation.emulation_enabled = self.emulation_enabled
        _Column.emulation_enabled = self.emulation_enabled

    def __exit__(self, exc_type, exc_val, exc_tb):
        _Operation.emulation_enabled = not self.emulation_enabled
        _Column.emulation_enabled = not self.emulation_enabled


class _EmulateStateVars(StateVars):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _EmulateStateVars.CHANGED_TICK_LISTS = {}

    def __setitem__(self, key, value):
        if key not in self._columns:
            raise ValueError("Can't declare state variables in per-tick script")

        if isinstance(value, _Operation):
            value = self._set_var_from_operation(key, value)
        elif isinstance(value, _StateBase):
            value = value.copy(obj_ref=self, name=key)

        return f'{str(self._columns[key])} = {ott.value2str(value)};'


class _EmulateInputObject:

    Symbol = SymbolType()  # NOSONAR

    def __init__(self, parent_obj):
        self.__class__._state_vars = _EmulateStateVars(self, parent_obj)

        for name, dtype in parent_obj.columns().items():
            self.__dict__[name] = _Column(name, dtype, self)
            if name == 'TIMESTAMP':
                self.__dict__['Time'] = _Column(name, dtype, self)

        # creating class variables, so they will not go to the self.__dict__
        # they will not be used before object initialization anyway

        # collects new columns with appeared values
        self.__class__.NEW_VALUES = defaultdict(lambda: [])
        self.__class__.LOCAL_VARS_NEW_VALUES = defaultdict(lambda: [])
        # collects local variables in the per-tick-script
        self.__class__.LOCAL_VARS = {}
        self.__class__.STATIC_VARS = {}
        # dictionary with functions used in per-tick script
        self.__class__.FUNCTIONS = {}
        self.__class__.INPUT_SCHEMA = self._get_schema()

    def _get_schema(self):
        return {
            name: value.dtype
            for name, value in self.__dict__.items()
            if isinstance(value, _Column)
        }

    @property
    def schema(self):
        return self.INPUT_SCHEMA

    def __getitem__(self, item):
        if item not in self.__dict__:
            raise NameError(f"Column '{item}' referenced before assignment")

        return self.__getattr__(item)

    def __getattr__(self, item):
        if item not in self.__dict__:
            raise AttributeError(f"There is no '{item}' attribute")

        item = self.__dict__[item]

        if not isinstance(item, _Column):
            return item

        dtype = item.dtype
        name = item.name
        if issubclass(dtype, ott.nsectime):
            return self.get_datetime_value(name)
        if issubclass(dtype, int):
            return self.get_long_value(name)
        if issubclass(dtype, float):
            return self.get_double_value(name)
        if issubclass(dtype, str):
            return self.get_string_value(name)
        raise AttributeError(f'{item} has {dtype} type, so it is not possible to return correct get method')

    @classmethod
    def get_changed_tick_lists(cls):
        return cls._state_vars.CHANGED_TICK_LISTS

    @property
    def state_vars(self):
        return self._state_vars

    def __str__(self):
        return 'LOCAL::INPUT_TICK'

    def copy_tick(self, tick_object):
        """
        Copy fields from ``tick_obj`` to the output tick.
        Will only rewrite fields that are presented in this tick and ``tick_object``,
        will not remove or add any.
        Translated to COPY_TICK() function.
        """
        return f'COPY_TICK({tick_object});'

    def get_long_value(self, field_name: Union[str, _Operation]) -> _Operation:
        """
        Get value of the long ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.

        Examples
        --------
        >>> def fun(tick):
        ...     tick['TOTAL_INT'] = 0
        ...     for field in otp.tick_descriptor_fields():
        ...         if field.get_type() == 'long':
        ...             tick['TOTAL_INT'] += tick.get_long_value(field.get_name())
        >>> t = otp.Tick(INT_1=3, INT_2=5)
        >>> t = t.script(fun)
        >>> otp.run(t)
                Time  INT_1  INT_2  TOTAL_INT
        0 2003-12-01      3      5          8
        """
        schema = self.schema
        check_field_name_in_schema(field_name, int, schema=schema)
        return _Operation(dtype=int,
                          op_str=f'{self}.GET_LONG_VALUE({ott.value2str(field_name)})')

    def get_double_value(self, field_name: Union[str, _Operation]) -> _Operation:
        """
        Get value of the double ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.

        Examples
        --------
        >>> def fun(tick):
        ...     tick['TOTAL_DOUBLE'] = 0.0
        ...     for field in otp.tick_descriptor_fields():
        ...         if field.get_type() == 'double':
        ...             tick['TOTAL_DOUBLE'] += tick.get_double_value(field.get_name())
        >>> t = otp.Tick(DOUBLE_1=3.1, DOUBLE_2=5.2)
        >>> t = t.script(fun)
        >>> otp.run(t)
                Time  DOUBLE_1  DOUBLE_2  TOTAL_DOUBLE
        0 2003-12-01      3.1      5.2          8.3
        """
        check_field_name_in_schema(field_name, float, schema=self.schema)
        return _Operation(dtype=float,
                          op_str=f'{self}.GET_DOUBLE_VALUE({ott.value2str(field_name)})')

    def get_string_value(self, field_name: Union[str, _Operation]) -> _Operation:
        """
        Get value of the string ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.

        Examples
        --------
        >>> def fun(tick):
        ...     tick['TOTAL_STR'] = ""
        ...     for field in otp.tick_descriptor_fields():
        ...         if field.get_type() == 'string':
        ...             tick['TOTAL_STR'] += tick.get_string_value(field.get_name())
        >>> t = otp.Tick(STR_1="1", STR_2="2")
        >>> t = t.script(fun)
        >>> otp.run(t)
                Time  STR_1  STR_2  TOTAL_STR
        0 2003-12-01      1      2          12
        """
        check_field_name_in_schema(field_name, str, schema=self.schema)
        return _Operation(dtype=str,
                          op_str=f'{self}.GET_STRING_VALUE({ott.value2str(field_name)})')

    def get_datetime_value(self, field_name: Union[str, _Operation]) -> _Operation:
        """
        Get value of the datetime ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.

        Examples
        --------
        >>> def fun(tick):
        ...     for field in otp.tick_descriptor_fields():
        ...         if field.get_type() == 'nsectime':
        ...             tick['SOME_DATETIME'] = tick.get_datetime_value(field.get_name())
        >>> t = otp.Tick(DATETIME=otp.datetime(2021, 1, 1))
        >>> t = t.script(fun)
        >>> otp.run(t)
                Time   DATETIME SOME_DATETIME
        0 2003-12-01 2021-01-01    2021-01-01
        """
        check_field_name_in_schema(field_name, ott.nsectime, schema=self.schema)
        return _Operation(dtype=ott.nsectime,  # we don't know if it is msectime
                          op_str=f'{self}.GET_DATETIME_VALUE({ott.value2str(field_name)})')


class _EmulateObject(_EmulateInputObject):
    """
    Instances of this class are proxy to columns of a _Source object,
    and track assignment to construct the correct per-tick-script then
    """
    def __init__(self, parent_obj):
        super().__init__(parent_obj)
        _EmulateObject.input = _EmulateInputObject(parent_obj)

    def __getattr__(self, item):
        if item not in self.__dict__:
            raise AttributeError(f"There is no '{item}' attribute")

        return self.__dict__[item]

    def __setitem__(self, key, value):
        return self.__setattr__(key, value)

    def __setattr__(self, key, value):
        if key in self.__class__.__dict__:
            super().__setattr__(key, value)
        if key not in self.__dict__ or key in _EmulateObject.NEW_VALUES:
            _EmulateObject.NEW_VALUES[key].append(value)
        if key not in self.__dict__:
            self.__dict__[key] = _Column(key, ott.get_object_type(value), self)

    def __str__(self):
        return 'LOCAL::OUTPUT_TICK'

    @property
    def schema(self):
        return self._get_schema()

    @classmethod
    def get_types_of_new_columns(cls):
        """ method calculates types for all tracked new columns """
        return {
            key: ott.get_type_by_objects(values)
            for key, values in _EmulateObject.NEW_VALUES.items()
        }

    def set_long_value(self, field_name: Union[str, _Operation], value: Union[int, _Operation]) -> str:
        """
        Set ``value`` of the long ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        value: int, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            Long value to set or operation which return such value.

        Examples
        --------
        >>> def fun(tick):
        ...     for field in otp.tick_descriptor_fields():
        ...         if field.get_type() == 'long':
        ...             tick.set_long_value(field.get_name(), 5)
        >>> t = otp.Tick(INT_1=3)
        >>> t = t.script(fun)
        >>> otp.run(t)
                Time  INT_1
        0 2003-12-01      5
        """
        check_field_name_in_schema(field_name, int, value, self.schema)
        return f'{self}.SET_LONG_VALUE({ott.value2str(field_name)}, {ott.value2str(value)});'

    def set_double_value(self, field_name: Union[str, _Operation], value: Union[float, _Operation]) -> str:
        """
        Set ``value`` of the double ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        value: float, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            Double value to set or operation which return such value.

        Examples
        --------
        >>> def fun(tick):
        ...     for field in otp.tick_descriptor_fields():
        ...         if field.get_type() == 'double':
        ...             tick.set_double_value(field.get_name(), 5.0)
        >>> t = otp.Tick(DOUBLE_1=3.0)
        >>> t = t.script(fun)
        >>> otp.run(t)
                Time  DOUBLE_1
        0 2003-12-01      5.0
        """
        check_field_name_in_schema(field_name, float, value, self.schema)
        return f'{self}.SET_DOUBLE_VALUE({ott.value2str(field_name)}, {ott.value2str(value)});'

    def set_string_value(self, field_name: Union[str, _Operation], value: Union[str, _Operation]) -> str:
        """
        Set ``value`` of the string ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        value: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String value to set or operation which return such value.

        Examples
        --------
        >>> def fun(tick):
        ...     for field in otp.tick_descriptor_fields():
        ...         if field.get_type() == 'string':
        ...             tick.set_string_value(field.get_name(), '5')
        >>> t = otp.Tick(STR_1='3')
        >>> t = t.script(fun)
        >>> otp.run(t)
                Time  STR_1
        0 2003-12-01      5
        """
        check_field_name_in_schema(field_name, str, value, self.schema)
        return f'{self}.SET_STRING_VALUE({ott.value2str(field_name)}, {ott.value2str(value)});'

    def set_datetime_value(self, field_name: Union[str, _Operation], value: Union[int, _Operation]) -> str:
        """
        Set ``value`` of the datetime ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        value: int, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            Datetime value to set or operation which return such value.

        Examples
        --------
        >>> def fun(tick):
        ...     for field in otp.tick_descriptor_fields():
        ...         if field.get_type() == 'nsectime':
        ...             tick.set_datetime_value(field.get_name(), otp.datetime(2021, 1, 1) - otp.Day(1))
        >>> t = otp.Tick(DATETIME_1=otp.datetime(2021, 1, 1))
        >>> t = t.script(fun)
        >>> otp.run(t)
                Time  DATETIME_1
        0 2003-12-01      2020-12-31
        """
        check_field_name_in_schema(field_name, ott.nsectime, value, self.schema)
        return f'{self}.SET_DATETIME_VALUE({ott.value2str(field_name)}, {ott.value2str(value)});'


def _validate_lambda(lambda_f):
    if not (isinstance(lambda_f, types.LambdaType) and lambda_f.__name__ == '<lambda>'
            or isinstance(lambda_f, (types.FunctionType, types.MethodType))):
        raise ValueError("It is expected to get a function, method or lambda,"
                         f" but got '{lambda_f}' of type '{type(lambda_f)}'")


def apply_script(lambda_f, self_ref):
    """
    In this function we parse python syntax tree for `lambda_f`
    and converting it to OneTick's per-tick script.
    This function returns _Column like function, that
    keeps resulting built expression.
    """
    _validate_lambda(lambda_f)

    with _CompareTrackScope():
        # TODO: remove circular imports
        from .per_tick_script import FunctionParser
        _script = FunctionParser(lambda_f, emulator=self_ref).per_tick_script()
        new_columns_types = _EmulateObject.get_types_of_new_columns()
        return new_columns_types, _script


def apply_lambda(lambda_f, self_ref):
    """
    In this function we parse python syntax tree for `lambda_f`
    and converting it to OneTick's CASE function().
    This function returns _Column like function, that
    keeps resulting built expression.
    """
    _validate_lambda(lambda_f)

    with _CompareTrackScope():
        # TODO: remove circular imports
        from .per_tick_script import FunctionParser
        res, values = FunctionParser(lambda_f, emulator=self_ref).case()
        return _LambdaIfElse(res, ott.get_type_by_objects(values))


class _LambdaIfElse(_Column):
    """
    Behaves like a column and consists information built from the lambda calculation
    """

    def __init__(self, repr, dtype):
        self._dtype = dtype
        self._repr = repr

    def __str__(self):
        return self._repr

    def __len__(self):
        if issubclass(self.dtype, str):
            if self.dtype is str:
                return ott.string.DEFAULT_LENGTH
            else:
                return self.dtype.length

        raise TypeError(f"It is not allowed to call len() method for object of type '{self.dtype}'")
