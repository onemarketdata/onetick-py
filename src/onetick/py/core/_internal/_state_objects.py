import itertools
import warnings
from string import Template
from os import path
from abc import ABC, abstractmethod
from typing import Iterable, Optional, Union, List, Dict
from functools import wraps

from onetick.py.otq import otq
import onetick.py.types as ott
from onetick.py.core.column import _Column
from onetick.py.core.column_operations.base import Operation
from onetick.py.types import (
    string, value2str, nsectime, msectime, get_object_type, type2str, varstring, default_by_type, get_base_type,
)
from onetick.py.core.column_operations._methods.op_types import are_numerics, are_strings
from onetick.py.core.eval_query import _QueryEvalWrapper, prepare_params
from onetick.py.compatibility import (
    is_supported_varstring_in_get_string_value,
    is_supported_modify_state_var_from_query
)


class _StateBase(ABC):

    def __init__(self, name, scope, default_value, obj_ref):
        self.name = name
        self.scope = scope
        self.default_value = default_value
        self.obj_ref = obj_ref

    def __str__(self):
        if not self.name:
            raise ValueError('State variable has no name')
        return f"STATE::{self.name}"

    @abstractmethod
    def copy(self, obj_ref=None, name=None):
        raise NotImplementedError

    def modify_from_query(
        self,
        query,
        symbol=None, start=None, end=None,
        params=None,
        action: str = 'replace',
        where=None,
        output_field_name=None,
    ):
        """
        Modifies a :py:meth:`state variable <onetick.py.Source.state_vars>`
        by assigning it a value that was resulted from a ``query`` evaluation.

        Parameters
        ----------
        query: callable, Source
            Callable ``query`` should return :class:`Source`. This object will be evaluated by OneTick (not python)
            for every tick. Note python code will be executed only once, so all python's conditional expressions
            will be evaluated only once too.

            If ``query`` is a :class:`Source` object then it will be propagated as a query to OneTick.

            If state ``var`` is a primitive (not tick sequence), then ``query`` must return only one tick,
            otherwise exception will be raised.

        symbol: str, Operation, dict, Source, or Tuple[Union[str, Operation], Union[dict, Source]]
            Symbol name to use in ``query``. In addition, symbol params can be passed along with symbol name.

            Symbol name can be passed as a string or as an :class:`Operation`.

            Symbol parameters can be passed as a dictionary. Also, the main :class:`Source` object,
            or the object containing a symbol parameter list, can be used as a list of symbol parameters.

            ``symbol`` will be interpreted as a symbol name or as symbol parameters, depending on its type.
            You can pass both as a tuple.

            If symbol name is not passed, then symbol name from the main source is used.

        start: :py:class:`otp.datetime <onetick.py.datetime>`, :py:class:`otp.Operation <onetick.py.Operation>`
            Start time to run the ``query``.
            By default the start time of the main query is used.

        end: :py:class:`otp.datetime <onetick.py.datetime>`, :py:class:`otp.Operation <onetick.py.Operation>`
            End time to run the ``query``.
            By default the end time of the main query is used.

        params: dict
            Mapping of the parameters' names and their values for the ``query``.
            :py:class:`Columns <onetick.py.Column>` can be used as a value.

        action: str
            Specifies whether all ticks should be erased before the query results are inserted into the tick set.
            Possible values are ``update`` and ``replace``.
            For non-tick-sets, you can set the ``action`` only to ``replace``; otherwise, an error is thrown.

        where: Operation
            Condition to filter ticks for which the result of the ``query`` will be joined.

        output_field_name: str
            Specifies the output field name for state variables of primitive types,
            in case if the query result contains multiple fields.

        Returns
        -------
        :class:`Source`
            Source with joined ticks from ``query``

        See also
        --------
        **MODIFY_STATE_VAR_FROM_QUERY** OneTick event processor

        Examples
        --------

        Update simple state variable from query:

        .. testcode::
           :skipif: not is_supported_modify_state_var_from_query()

           data = otp.Ticks(A=[1, 2, 3])
           data.state_vars['VAR'] = 0
           data.state_vars['VAR'] = 7
           def fun():
               return otp.Tick(X=123, Y=234)
           data = data.state_vars['VAR'].modify_from_query(fun,
                                                           output_field_name='X',
                                                           where=(data['A'] % 2 == 1))
           data['X'] = data.state_vars['VAR']
           df = otp.run(data)
           print(df)

        .. testoutput::

                                Time  A    X
           0 2003-12-01 00:00:00.000  1  123
           1 2003-12-01 00:00:00.001  2  7
           2 2003-12-01 00:00:00.002  3  123

        Update tick sequence from query:

        .. testcode::
           :skipif: not is_supported_modify_state_var_from_query()

           data = otp.Tick(A=1)
           data.state_vars['VAR'] = otp.state.tick_list()
           def fun():
               return otp.Ticks(X=[123, 234])
           data = data.state_vars['VAR'].modify_from_query(fun)
           data = data.state_vars['VAR'].dump()
           df = otp.run(data)
           print(df)

        .. testoutput::

                                Time    X
           0 2003-12-01 00:00:00.000  123
           1 2003-12-01 00:00:00.001  234

        Passing parameters to the ``query``:

        .. testcode::
           :skipif: not is_supported_modify_state_var_from_query()

           data = otp.Tick(A=1)
           data.state_vars['VAR'] = otp.state.tick_list()
           def fun(min_value):
               t = otp.Ticks(X=[123, 234])
               t = t.where(t['X'] > min_value)
               return t
           data = data.state_vars['VAR'].modify_from_query(fun, params={'min_value': 200})
           data = data.state_vars['VAR'].dump()
           df = otp.run(data)
           print(df)

        .. testoutput::

                                Time    X
           0 2003-12-01 00:00:00.001  234
        """
        import onetick.py as otp
        obj_ref: otp.Source = getattr(self.obj_ref, '_owner')

        action = action.lower()
        if action not in {'replace', 'update'}:
            raise ValueError(f"Value '{action}' for parameter 'action' is not supported")

        if isinstance(self, _TickSequence) and output_field_name:
            raise ValueError("Parameter 'output_field_name' can't be set for tick sequences")

        params = params or {}
        converted_params = prepare_params(**params)
        if isinstance(query, otp.Source):
            sub_source = query
        else:
            sub_source = query(**converted_params)
            if not isinstance(sub_source, otp.Source):
                raise ValueError(f"{query} didn't return Source object")

        if not isinstance(self, _TickSequence) and len(sub_source.schema) > 1 and not output_field_name:
            raise ValueError("Parameter 'output_field_name' must be set"
                             " if there is more than one field in query schema")

        if output_field_name and output_field_name not in sub_source.schema:
            raise ValueError(f"There is no field '{output_field_name}' in query schema")

        ep_params = {
            'state_variable': str(self),
        }

        ep_params['action'] = action.upper()

        if output_field_name:
            ep_params['output_field_name'] = ott.value2str(output_field_name)

        if start is None:
            start = otp.meta_fields['_START_TIME']
        if end is None:
            end = otp.meta_fields['_END_TIME']

        from onetick.py.core._source.source_methods.joins import (_process_start_or_end_of_jwq,
                                                                  _columns_to_params_for_joins,
                                                                  _check_and_convert_symbol,
                                                                  _convert_symbol_param_and_columns)
        _process_start_or_end_of_jwq(ep_params, start, 'start_timestamp')
        _process_start_or_end_of_jwq(ep_params, end, 'end_timestamp')

        if params:
            params_str = _columns_to_params_for_joins(params, query_params=True)
            ep_params['otq_query_params'] = params_str

        if where is not None:
            ep_params['where'] = str(where)

        converted_symbol_name, symbol_param = _check_and_convert_symbol(symbol)

        # default symbol name should be this: _SYMBOL_NAME if it is not empty else _NON_EXISTING_SYMBOL_
        # this way we will force JWQ to substitute symbol with any symbol parameters we may have passed
        # otherwise (if an empty symbol name is passed to JWQ), it will not substitute either symbol name
        # or symbol parameters, and so symbol parameters may get lost
        # see BDS-263
        if converted_symbol_name is None:
            converted_symbol_name = "CASE(_SYMBOL_NAME,'','_NON_EXISTING_SYMBOL',_SYMBOL_NAME)"

        converted_symbol_param_columns, converted_symbol_param = _convert_symbol_param_and_columns(symbol_param)
        if converted_symbol_param is None:
            # we couldn't interpret "symbols" as either symbol name or symbol parameters
            raise ValueError('"symbol" parameter has a wrong format! It should be a symbol name, a symbol parameter '
                             'object (dict or Source), or a tuple containing both')

        symbol_params_str = _columns_to_params_for_joins(converted_symbol_param_columns)

        ep_params['symbol_name'] = converted_symbol_name
        ep_params['symbol_params'] = symbol_params_str

        res = obj_ref.copy()

        res._merge_tmp_otq(sub_source)
        query_name = sub_source._store_in_tmp_otq(
            res._tmp_otq,
            symbols='_NON_EXISTING_SYMBOL_',
            operation_suffix='modify_state_var_from_query',
        )
        ep_params['otq_query'] = f'"THIS::{query_name}"'

        res.sink(otq.ModifyStateVarFromQuery(**ep_params))
        return res


class _StateColumn(_StateBase, _Column):

    def __init__(self, name, dtype, obj_ref, default_value, scope):
        _Column.__init__(self, name, dtype, obj_ref)
        _StateBase.__init__(self, name, scope, default_value, obj_ref=obj_ref)

    def __len__(self):
        if issubclass(self.dtype, str):
            if self.dtype is str:
                return string.DEFAULT_LENGTH
            else:
                return self.dtype.length

        else:
            raise NotImplementedError

    def copy(self, obj_ref=None, name=None):
        return _StateColumn(name if name else self.name,
                            self.dtype,
                            obj_ref,
                            self.default_value,
                            self.scope)

    def __getitem__(self, item):
        raise IndexError('Indexing is not supported for state variables')

    def modify_from_query(self, *args, **kwargs):
        if 'action' in kwargs:
            raise ValueError("Parameter 'action' can only be used with tick sequences")
        return super().modify_from_query(*args, **kwargs)


def inplace_operation(method):
    """
    Decorator that adds the `inplace` parameter and logic according to this flag.
    inplace=True means that method modifies an object,
    otherwise it copies the object first, then modifies the copy and returns it.
    Decorator can be used with _TickSequence class methods only.
    """
    @wraps(method)
    def _inner(self, *args, inplace=False, **kwargs):
        if inplace or self._is_used_in_per_tick_script:
            return method(self, *args, **kwargs)
        obj = self._owner.copy()
        return method(obj.state_vars[self.name], *args, **kwargs)

    return _inner


def check_value_dtype(value, dtype, check_string_length=False) -> bool:
    """
    Check if ``value`` is compatible to the type ``dtype``.
    ``value`` can be object or type.
    If ``check_string_length`` is set then warning is raised in case
    string types conversion will result in losing precision.
    """
    value_dtype = value if isinstance(value, type) else get_object_type(value)
    if are_strings(value_dtype, dtype):
        if check_string_length:
            length = string.DEFAULT_LENGTH
            if issubclass(dtype, string) and dtype is not string:
                length = dtype.length
            if dtype is not varstring and len(value) > length:
                warnings.warn(
                    f"Value '{value}' will be truncated to {length} characters, "
                    f"because corresponding type in schema is {dtype}"
                )
        return True
    elif are_numerics(value_dtype, dtype):
        return get_base_type(value_dtype) is get_base_type(dtype)
    else:
        return value_dtype is dtype


class _TickSequence(_StateBase):
    def __init__(self, name, obj_ref, default_value, scope, schema=None, **kwargs):
        if kwargs:
            raise ValueError(f"Unknown parameters for '{self.__class__.__name__}': {list(kwargs)}")
        from onetick.py.core.source import Source
        if default_value is not None and not isinstance(default_value, (_QueryEvalWrapper, Source)):
            raise ValueError('only otp.eval and otp.Source objects can be used as initial value for tick sequences')
        if default_value is not None and schema is not None:
            # TODO: check that the two schemas align or possibly that they are exactly the same
            pass
        super().__init__(name, scope, default_value, obj_ref=obj_ref)
        self.obj_ref = obj_ref
        self.dtype = self.__class__
        self._schema = schema.copy() if schema is not None else {}

    def __iter__(self):
        raise TypeError(f'{self.__class__.__name__} objects can be iterated only in per-tick script')

    def copy(self, obj_ref=None, name=None, **kwargs):
        return self.__class__(name=name if name else self.name,
                              obj_ref=obj_ref,
                              default_value=self.default_value,
                              scope=self.scope,
                              schema=self._schema,
                              **kwargs)

    @property
    def _owner(self):
        """
        Get source that owns this state variable.
        """
        if self.obj_ref is None:
            raise ValueError("Add tick sequence to the state_vars of the Source"
                             " before calling it's methods")
        return self.obj_ref._owner

    @property
    def _is_used_in_per_tick_script(self) -> bool:
        """Check if this variable is used in per-tick script or on Source directly"""
        # TODO: remove circular imports
        from ..lambda_object import _EmulateInputObject
        return isinstance(self._owner, _EmulateInputObject)

    def _default_schema(self):
        return dict(self._owner.schema.items())

    @property
    def schema(self) -> dict:
        """
        Get schema of the tick sequence.
        """
        fields = None
        if isinstance(self._schema, list):
            fields = self._schema
            self._schema = None
        if not self._schema:
            if self.default_value is not None:
                from onetick.py.core.source import Source
                if isinstance(self.default_value, _QueryEvalWrapper):
                    # If tick sequence is initialized from eval,
                    # then we get schema from the Source in eval.
                    self._schema = self.default_value.query.schema.copy()
                elif isinstance(self.default_value, Source):
                    self._schema = self.default_value.schema.copy()
            else:
                # If tick sequence is initialized as empty,
                # then it's schema will be derived from the schema of the parent object (e.g. source)
                self._schema = self._default_schema()
        if fields is not None:
            self._schema = {field: type for field, type in self._schema.items() if field in fields}
            for field in fields:
                if field not in self._schema.keys():
                    raise KeyError(f'Requested field "{field}" is not contained in the base schema!')
        return self._schema

    @property
    def _tick_class(self):
        """Get corresponding class for tick when iterating over tick sequence"""
        return TickSequenceTick

    def _tick_obj(self, name):
        """Get corresponding object for tick when iterating over tick sequence"""
        cls = self._tick_class
        return cls(name, owner=self)

    def _definition(self) -> str:
        """Get OneTick string that constructs tick sequence object"""
        raise NotImplementedError

    @property
    def _dump_ep(self):
        """onetick.query ep that dumps tick sequence"""
        raise NotImplementedError

    @inplace_operation
    def dump(self,
             propagate_input_ticks=False,
             when_to_dump='first_tick',
             delimiter=None,
             added_field_name_suffix=None):
        """
        Propagates all ticks from a given tick sequence upon the arrival of input tick.

        Preserves original timestamps from tick list/deque, if they fall into query start/end time range,
        and for ticks before start time and after end time, timestamps are changed
        to start time and end time correspondingly.

        Parameters
        ----------
        propagate_input_ticks: bool
            Propagate input ticks or not.
        when_to_dump: str

            * `first_tick` - Propagates once before input ticks. There must be at least one input tick.
            * `before_tick` - Propagates once before input ticks.
              Content will be propagated even if there are no input ticks.
        delimiter: 'tick', 'flag or None
            This parameter specifies the policy for adding the delimiter field.
            The name of the additional field is "DELIMITER" + ``added_field_name_suffix``.
            Possible options are:

            * None - No additional field is added to propagated ticks.
            * 'tick' - An extra tick is created after the last tick.
              Also, an additional column is added to output ticks.
              The extra tick has values of all fields set to the defaults (0,NaN,""),
              except the delimiter field, which is set to string "D".
              All other ticks have this field's value set to empty string.
            * 'flag' - The delimiter field is appended to each output tick.
              The field's value is empty for all ticks except the last tick of the tick sequence, which is string "D".
        added_field_name_suffix: str or None
            The suffix to add to the name of the additional field.
        inplace: bool
            If ``True`` current source will be modified else modified copy will be returned

        Returns
        -------
            if ``inplace`` is False then returns :py:class:`~onetick.py.Source` copy.

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(B=[1, 2, 3])
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.state_vars['LIST'].dump()
        >>> otp.run(data)[['B']]
           B
        0  1
        1  2
        2  3
        """
        when_to_dump = when_to_dump.upper()
        if when_to_dump not in ('FIRST_TICK', 'BEFORE_TICK', 'EVERY_TICK'):
            raise ValueError(
                f"Parameter 'when_to_dump' must be one of {('FIRST_TICK', 'BEFORE_TICK', 'EVERY_TICK')}. "
                f"Got {when_to_dump}."
            )
        if delimiter is not None:
            delimiters = {
                'tick': 'TICK_AT_END',
                'tick_at_end': 'TICK_AT_END',
                'flag': 'FLAG_AT_END',
                'flag_at_end': 'FLAG_AT_END',
                'none': 'NONE',
            }
            delimiter = delimiter.lower()
            if delimiter not in delimiters:
                raise ValueError(
                    f"Parameter 'delimiter' must be one of {list(delimiters)}. "
                    f"Got {delimiter}."
                )
            delimiter = delimiters[delimiter]
        else:
            delimiter = 'NONE'

        added_field_name_suffix = added_field_name_suffix or ''

        if self.default_value is not None:
            if not propagate_input_ticks:
                self._owner.schema.set(**self.schema)
            else:
                self._owner.schema.update(**self.schema)

        self._owner.sink(
            self._dump_ep(str(self),
                          propagate_input_ticks=propagate_input_ticks,
                          when_to_dump=when_to_dump,
                          delimiter=delimiter,
                          added_field_name_suffix=added_field_name_suffix or '')
        )
        return self._owner


class TickList(_TickSequence):
    """
    Represents a tick list.
    This class should only be created with :py:func:`onetick.py.state.tick_list` function
    and should be added to the :py:meth:`onetick.py.Source.state_vars` dictionary
    of the :py:class:`onetick.py.Source` and can be accessed only via this dictionary.

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data.state_vars['LIST'] = otp.state.tick_list()
    >>> data = data.state_vars['LIST'].dump()

    See also
    --------
    :py:class:`TickSequenceTick <onetick.py.core._internal._state_objects.TickSequenceTick>`
    """

    def _definition(self) -> str:
        return 'TICK_LIST'

    @property
    def _tick_class(self):
        return _TickListTick

    @property
    def _dump_ep(self):
        return otq.DumpTickList

    @inplace_operation
    def clear(self):
        """
        Clear tick list.
        Can be used in per-tick script or on Source directly.

        inplace: bool
            If ``True`` current source will be modified else modified copy will be returned.
            Makes sense only if used not in per-tick script.

        Returns
        -------
            if ``inplace`` is False and method is not used in per-tick script
            then returns :py:class:`~onetick.py.Source` copy.

        Examples
        --------
        Can be used in per-tick script:

        >>> def fun(tick):
        ...     tick.state_vars['LIST'].clear()
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list()
        >>> data = data.script(fun)

        Can be used in source columns operations:

        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(otp.Tick(A=1)))
        >>> data = data.state_vars['LIST'].clear()

        >>> data = data.state_vars['LIST'].dump()
        >>> otp.run(data)
        Empty DataFrame
        Columns: [A, Time]
        Index: []
        """
        if self._is_used_in_per_tick_script:
            return f'{self}.CLEAR();'
        self._owner.sink(
            otq.ExecuteExpressions(f'{self}.CLEAR()')
        )
        return self._owner

    def push_back(self, tick_object):
        """
        Add `tick_object` to the tick list.
        Can only be used in per-tick script.

        Examples
        --------
        >>> def fun(tick):
        ...     tick.state_vars['LIST'].push_back(tick)
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list()
        >>> data = data.script(fun)

        >>> data = data.state_vars['LIST'].dump()
        >>> otp.run(data)
                Time  A
        0 2003-12-01  1
        """
        if not self._is_used_in_per_tick_script:
            raise ValueError('method .push_back() can only be used in per-tick script')
        self._schema.update(**tick_object.schema)
        self.obj_ref.CHANGED_TICK_LISTS[self.name] = self.schema
        return f'{self}.PUSH_BACK({tick_object});'

    def get_size(self) -> Operation:
        """
        Get size of the tick list.
        Can be used in per-tick script or in Source operations directly.

        Examples
        --------
        Can be used in per-tick script:

        >>> def fun(tick):
        ...     tick['B'] = tick.state_vars['LIST'].get_size()
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list()
        >>> data = data.script(fun)

        Can be used in source columns operations:

        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list()
        >>> data['B'] = data.state_vars['LIST'].get_size()

        >>> otp.run(data)
                Time  A  B
        0 2003-12-01  1  0
        """
        return Operation(dtype=int, op_str=f'{self}.GET_SIZE()')

    def size(self) -> Operation:
        """
        See also
        --------
        get_size
        """
        return self.get_size()

    @inplace_operation
    def sort(self, field_name, field_type=None):
        """
        Sort the tick list in the ascending order over the specified field.
        Integer, float and datetime fields are supported for sorting.
        Implementation is per tick script which does a merge sort algorithm.
        Implemented algorithm is stable: it should not change the order of ticks with the same field value.
        Can only be used Source operations directly.

        Parameters
        ----------
        field_name: str
            Name of the field over which to sort ticks

        field_type: int, float, otp.msectime, otp.nsectime or None (default: None)
            Type of the field_name field. If None, type will be taken from the tick list schema.

        Examples
        --------

        >>> def fun(tick):
        ...     tick.state_vars['LIST'].push_back(tick)
        >>> data = otp.Ticks([
        ...    ['offset', 'VALUE'],
        ...    [0, 2],
        ...    [0, 3],
        ...    [0, 1],
        ... ])
        >>> data.state_vars['LIST'] = otp.state.tick_list()
        >>> data = data.script(fun)
        >>> data = data.agg(dict(NUM_TICKS=otp.agg.count()), bucket_time='end')
        >>> data = data.state_vars['LIST'].sort('VALUE', int)
        >>> data = data.state_vars['LIST'].dump()

        >>> otp.run(data)
                Time  VALUE
        0 2003-12-01  1
        1 2003-12-01  2
        2 2003-12-01  3
        """
        if self._is_used_in_per_tick_script:
            # TODO: implement
            raise NotImplementedError('Sorting tick lists in per tick script is currently not implemented')
        with open(
            path.join(
                path.dirname(path.abspath(__file__)),
                "_per_tick_scripts",
                "tick_list_sort_template.script",
            ),
        ) as script_file:
            sorting_script_template = Template(script_file.read())

        if field_type is None:
            field_type = self.schema[field_name]
        if field_type is int:
            field_access_function = 'GET_LONG_VALUE'
        elif field_type is float:
            field_access_function = 'GET_DOUBLE_VALUE'
        elif field_type is nsectime or field_type is msectime:
            field_access_function = 'GET_DATETIME_VALUE'
        else:
            raise TypeError('Field type {field_type} is not supported for sorting!'
                            'Supported field types: int, float, otp.nsectime, otp.msectime')
        sorting_script = sorting_script_template.substitute(
            tick_list_var=str(self),
            field_name=field_name,
            field_access_function=field_access_function,
        )

        self._owner.sink(
            otq.PerTickScript(
                script=sorting_script
            )
        )
        return self._owner

    def erase(self, tick_object):
        """
        Remove `tick_object` from the tick list.
        Can only be used in per-tick script.

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(X=[1, 2])
        >>> def fun(tick):
        ...     for t in tick.state_vars['LIST']:
        ...         if t['X'] == 1:
        ...             tick.state_vars['LIST'].erase(t)
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)
        >>> data = data.state_vars['LIST'].dump()
        >>> otp.run(data)
                             Time  X
        0 2003-12-01 00:00:00.001  2
        """
        if not self._is_used_in_per_tick_script:
            raise TypeError('erase() method for tick lists is supported only in script')
        return f'{self}.ERASE({tick_object});'


class TickSet(_TickSequence):
    """
    Represents a tick set.
    This class should only be created with :py:func:`onetick.py.state.tick_set` function
    and should be added to the :py:meth:`onetick.py.Source.state_vars` dictionary
    of the :py:class:`onetick.py.Source` and can be accessed only via this dictionary.

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A')
    >>> data = data.state_vars['SET'].dump()

    See also
    --------
    :py:class:`TickSequenceTick <onetick.py.core._internal._state_objects.TickSequenceTick>`
    """

    insertion_policies = {
        'oldest': 'OLDEST_TICK',
        'oldest_tick': 'OLDEST_TICK',
        'latest': 'LATEST_TICK',
        'latest_tick': 'LATEST_TICK',
    }

    def __init__(self, *args, insertion_policy, key_fields, **kwargs):
        insertion_policy = insertion_policy.lower()
        if insertion_policy not in self.insertion_policies:
            raise ValueError(
                f"Parameter 'insertion_policy' must be one of {list(self.insertion_policies)}. "
                f"Got {insertion_policy}."
            )
        self._insertion_policy = insertion_policy
        if isinstance(key_fields, str) or not isinstance(key_fields, Iterable):
            key_fields = [key_fields]
        self._key_fields = list(key_fields)
        super().__init__(*args, **kwargs)

    @property
    def insertion_policy(self):
        return self.insertion_policies[self._insertion_policy]

    @property
    def key_fields(self):
        """Get key fields for this tick set"""
        if not set(self._key_fields).issubset(self.schema):
            x = set(self._key_fields).difference(self.schema)
            raise ValueError(f"Key fields {x} not in tick set schema")
        return self._key_fields

    def copy(self, *args, **kwargs):
        kwargs.setdefault('insertion_policy', self._insertion_policy)
        kwargs.setdefault('key_fields', self._key_fields)
        return super().copy(*args, **kwargs)

    def _definition(self) -> str:
        args = ','.join(map(str, [self.insertion_policy, *self.key_fields]))
        return f'TICK_SET({args})'

    @property
    def _tick_class(self):
        return _TickSetTick

    @property
    def _dump_ep(self):
        return otq.DumpTickSet

    def dump(self, when_to_dump='every_tick', **kwargs):
        """
        Propagates all ticks from a given tick sequence upon the arrival of input tick.
        Timestamps of all propagated ticks are equal to the input tick's TIMESTAMP.

        Parameters
        ----------
        propagate_input_ticks: bool
            Propagate input ticks or not.
        when_to_dump: str

            * `first_tick` - Propagates once before input ticks. There must be at least one input tick.
            * `before_tick` - Propagates once before input ticks.
              Content will be propagated even if there are no input ticks.
            * `every_tick` - Propagates before *each* input tick.
        delimiter: 'tick', 'flag or None
            This parameter specifies the policy for adding the delimiter field.
            The name of the additional field is "DELIMITER" + ``added_field_name_suffix``.
            Possible options are:

            * None - No additional field is added to propagated ticks.
            * 'tick' - An extra tick is created after the last tick.
              Also, an additional column is added to output ticks.
              The extra tick has values of all fields set to the defaults (0,NaN,""),
              except the delimiter field, which is set to string "D".
              All other ticks have this field's value set to empty string.
            * 'flag' - The delimiter field is appended to each output tick.
              The field's value is empty for all ticks except the last tick of the tick sequence, which is string "D".
        added_field_name_suffix: str or None
            The suffix to add to the name of the additional field.
        inplace: bool
            If ``True`` current source will be modified else modified copy will be returned

        Returns
        -------
            if ``inplace`` is False then returns :py:class:`~onetick.py.Source` copy.

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(B=[1, 2, 3])
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'B', otp.eval(another_query))
        >>> data = data.state_vars['SET'].dump()
        >>> otp.run(data)[['B']]
           B
        0  1
        1  2
        2  3
        """
        return super().dump(when_to_dump=when_to_dump, **kwargs)

    @inplace_operation
    def update(self, where=1, value_fields=None, erase_condition=0):
        """
        Insert into or delete ticks from tick set.
        Can be used only on Source directly.

        Parameters
        ----------
        where: :py:class:`~onetick.py.Operation`
            Selection of input ticks that will be inserted into tick set.
            By default, all input ticks are selected.
        value_fields: list of str
             List of value fields to be inserted into tick sets.
             If param is empty, all fields of input tick are inserted.
             Note that this applies only to non-key fields (key-fields are always included).
             If new fields are added to tick set, they will have default values according to their type.
             If some fields are in tick set schema but not added in this method, they will have default values.
        erase_condition: :py:class:`~onetick.py.Operation`
            Selection of input ticks that will be erased from tick set.
            If it is set then ``where`` parameter is not taken into account.
        inplace: bool
            If ``True`` current source will be modified else modified copy will be returned

        Returns
        -------
            if ``inplace`` is False then returns :py:class:`~onetick.py.Source` copy.

        Examples
        --------
        >>> data = otp.Ticks(A=[1, 2, 3], B=[4, 5, 6], C=[7, 8, 9])
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A')
        >>> data = data.state_vars['SET'].update(value_fields=['B'])
        >>> data = data.state_vars['SET'].update(data['A'] == 2)
        >>> data = data.state_vars['SET'].update(erase_condition=data['A'] == 2)

        >>> data = data.first().state_vars['SET'].dump(when_to_dump='first_tick')
        >>> otp.run(data)
                Time  A  B
        0 2003-12-01  1  4
        1 2003-12-01  3  6
        """
        value_fields = value_fields or []
        if not set(value_fields).issubset(self._owner.schema):
            x = set(value_fields).difference(self._owner.schema)
            raise ValueError(f"value_fields {x} not in source schema")
        if not set(self.key_fields).issubset(self._owner.schema):
            x = set(self.key_fields).difference(self._owner.schema)
            raise ValueError(f"Key fields {x} not in source schema")
        for new_field in set(value_fields or self._owner.schema).difference(self.schema):
            self.schema[new_field] = self._owner.schema[new_field]
        value_fields = ','.join(map(str, value_fields))
        self._owner.sink(
            otq.UpdateTickSets(str(self),
                               where=str(where),
                               value_fields=value_fields,
                               erase_condition=str(erase_condition))
        )
        return self._owner

    def _parse_keys(self, key_values=None, named_keys=None) -> list:
        """
        Do some validations for the key values and named keys
        and return list of keys and values to be inserted in OneTick function.
        """
        if key_values and named_keys:
            raise ValueError("Parameters 'key_values' and 'named_keys' can't be used at the same time")
        if key_values:
            if len(key_values) != len(self.key_fields):
                raise ValueError(f"Wrong number of key values specified in parameter 'key_values', "
                                 f"need {len(self.key_fields)} values")
            for key_value, key in zip(key_values, self.key_fields):
                key_value_type = get_object_type(key_value)
                if not check_value_dtype(key_value, self.schema[key]):
                    raise ValueError(f"Key value '{key_value}' is type {key_value_type}, "
                                     f"but the type of key '{key}' is {self.schema[key]}")
            return key_values
        if named_keys:
            if not set(named_keys).issuperset(self.key_fields):
                x = set(self.key_fields).difference(named_keys)
                raise ValueError(f"Not all keys specified in parameter 'named_keys': {x}")
            for key, key_value in named_keys.items():
                if key not in self.key_fields:
                    raise ValueError(f"'{key}' not in tick set's key fields")
                key_value_type = get_object_type(key_value)
                if not check_value_dtype(key_value, self.schema[key]):
                    raise ValueError(f"Key value {key_value} is type {key_value_type}, "
                                     f"but the type of key '{key}' is {self.schema[key]}")
            return list(itertools.chain.from_iterable(named_keys.items()))
        if not set(self.key_fields).intersection(self._owner.schema):
            x = set(self.key_fields).difference(self._owner.schema)
            raise ValueError(f"Key fields {x} not in source schema")
        return []

    def find(
        self,
        field_name: Union[str, 'TickSequenceTick'],
        default_value=None,
        *key_values,
        throw: bool = False,
        **named_keys,
    ) -> Operation:
        """
        Finds a tick in the specified tick set for the given keys.
        If ``field_name`` is a string, it returns the value of the specified field from the found tick.
        If a tick with the given keys is not found, the default value is returned.
        If ``field_name`` is :py:class:`TickSequenceTick <onetick.py.core._internal._state_objects.TickSequenceTick>`,
        the entire tick is returned.

        Parameters
        ----------
        field_name: str, :py:class:`TickSequenceTick <onetick.py.core._internal._state_objects.TickSequenceTick>`
            The field to return the value from or an object for returning an entire tick.
        default_value
            The value to be returned if the key is not found.
            If ``default_value`` omitted and the key is not found,
            default "zero" value for the field type will be returned.
            If ``throw`` is True, the positional argument for ``default_value``
            counts as the first ``key_values`` argument.
        key_values:
            The same number of arguments as the number of keys in the tick set, in the same order as they
            were specified when defining the tick set.
            Values can be specified explicitly or taken from the specified columns. Columns
            must be specified as ``source['col']`` as opposed to just ``'col'``.
            ``key_values`` are optional: if not specified (and ``named_keys`` are not specified either),
            the values for the keys are taken from the tick's columns.
        named_keys:
            Can be used instead of ``key_values`` by specifying the ``key=value`` named arguments.
        throw: bool
            Raise an exception if the key is not found.
            If ``True``, the positional argument for ``default_value`` counts as the first ``key_values`` argument.

        Examples
        --------
        Create a tick set keyed by the values of ``A``. Look up the value of ``B``
        in the tick set for the value of ``A`` in the current tick.

        >>> data = otp.Tick(A=1, B=2)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A', otp.eval(otp.Tick(A=1, B=4)))
        >>> data['B'] = data.state_vars['SET'].find('B', 999)
        >>> otp.run(data)
                Time  A    B
        0 2003-12-01  1    4

        A key can be specified explicitly (i.e., not taken from the key fields of the current tick).

        >>> data = otp.Tick(C=777, B=2)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A', otp.eval(otp.Tick(A=1, B=4)))
        >>> data['B'] = data.state_vars['SET'].find('B', 999, 1)
        >>> otp.run(data)
                Time   C    B
        0 2003-12-01  777   4

        A key can be specified explicitly as a ``key=value`` pair.

        >>> data = otp.Tick(C=777, B=2)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A', otp.eval(otp.Tick(A=1, B=4)))
        >>> data['B'] = data.state_vars['SET'].find('B', 999, A=1)
        >>> otp.run(data)
                Time   C    B
        0 2003-12-01  777   4

        Columns can be used as keys.

        >>> data = otp.Tick(C=1, B=2)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A', otp.eval(otp.Tick(A=1, B=4)))
        >>> data['B'] = data.state_vars['SET'].find('B', 999, data['C'])
        >>> otp.run(data)
                Time   C   B
        0 2003-12-01   1   4

        The ``default_value`` is returned if the key is not found.

        >>> data = otp.Tick(A=555, B=2)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A', otp.eval(otp.Tick(A=1, B=4)))
        >>> data['B'] = data.state_vars['SET'].find('B', 999)
        >>> otp.run(data)
                Time  A    B
        0 2003-12-01 555  999

        Throw an exception if the key is not found (there is no ``default_value`` when ``throw=True``):

        >>> data = otp.Tick(A=555, B=2)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A', otp.eval(otp.Tick(A=1, B=4)))
        >>> data['B'] = data.state_vars['SET'].find('B', throw=True)
        >>> otp.run(data) # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        Exception: 9: ERROR:

        ``find`` can be used in ``otp.Source.script``.

        >>> def fun(tick):
        ...     tick['B'] = tick.state_vars['SET'].find('B', 0, 1)
        >>> data = otp.Tick(A=1, B=2)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A', otp.eval(otp.Tick(A=1, B=4)))
        >>> data = data.script(fun)
        >>> otp.run(data)
                Time  A    B
        0 2003-12-01  1    4

        In ``otp.Source.script`` ``find`` can be used with ``TickSetTick``. It allows looking up a whole tick,
        rather than the value of a single field:

        >>> def fun(tick):
        ...     t = otp.tick_set_tick()
        ...     if tick.state_vars['SET'].find(t, 2):
        ...         tick['B'] = t['B']
        ...         tick['C'] = t['C']
        ...         tick['D'] = t['D']
        >>> def another_query():
        ...     return otp.Ticks(B=[1, 2, 3], C=[4, 5, 6,], D=[7, 8, 9])
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'B', otp.eval(another_query))
        >>> data = data.script(fun)
        >>> otp.run(data)
                Time  A  B  C  D
        0 2003-12-01  1  2  5  8
        """
        if isinstance(field_name, TickSequenceTick):
            return self._find_tick_sequence_tick(
                field_name,
                default_value,
                *key_values,
                **named_keys,
            )
        return self._find_str(
            field_name,
            default_value,
            *key_values,
            throw=throw,
            **named_keys,
        )

    def _find_tick_sequence_tick(
        self,
        tick_sequence_tick: 'TickSequenceTick',
        default_value=None,
        *key_values,
        **named_keys,
    ) -> Operation:
        if default_value is not None:
            key_values = (default_value, *key_values)
        func = 'FIND'
        if named_keys:
            func = 'FIND_BY_NAMED_KEYS'
        args = [self, value2str(tick_sequence_tick)]
        args.extend(map(value2str, self._parse_keys(key_values, named_keys)))
        str_args = ','.join(map(str, args))
        # set new owner since now TickSet defines the tick_set_tick's schema
        tick_sequence_tick._owner = self
        return Operation(dtype=int, op_str=f'{func}({str_args})')

    def _find_str(
        self,
        field_name: str,
        default_value=None,
        *key_values,
        throw=False,
        **named_keys,
    ) -> Operation:
        if field_name not in self.schema:
            raise ValueError(f"field_name '{field_name}' not in tick set schema")
        if default_value is None and not throw:
            default_value = default_by_type(self._schema[field_name])
        field_name_type = self.schema[field_name]
        if throw:
            if named_keys:
                raise ValueError("Parameters 'throw' and 'named_keys' can't be used at the same time")
            key_values = (default_value, *key_values)
            default_value = None
        else:
            default_value_type = get_object_type(default_value)
            if not check_value_dtype(default_value, field_name_type, check_string_length=True):
                raise ValueError(
                    f"default_value '{default_value}' type is {default_value_type!r}, "
                    f"but the type of field '{field_name}' is {field_name_type!r}"
                )

        args = [self, value2str(field_name)]
        if default_value is not None:
            args.append(value2str(default_value))
        if not (key_values and key_values[0] is None):
            args.extend(map(value2str, self._parse_keys(key_values, named_keys)))
        func = 'FIND'
        if throw:
            func = 'FIND_OR_THROW'
        if named_keys:
            func = 'FIND_BY_NAMED_KEYS'
        str_args = ','.join(map(str, args))
        return Operation(dtype=field_name_type, op_str=f'{func}({str_args})')

    def find_by_named_keys(self, field_name, default_value=None, **named_keys) -> Operation:
        """
        Alias for find with restricted set of parameters

        See also
        --------
        find
        """
        return self.find(field_name, default_value, **named_keys)

    def find_or_throw(self, field_name, *key_values) -> Operation:
        """
        Alias for find with restricted set of parameters

        See also
        --------
        find
        """
        return self.find(field_name, None, *key_values, throw=True)

    def erase(self, *key_values: List[Union[str, 'TickSequenceTick']], **named_keys: Dict) -> Operation:
        """
        Erase tick(s) from tick set with keys or through
        :py:class:`TickSequenceTick <onetick.py.core._internal._state_objects.TickSequenceTick>`

        Parameters
        ----------
        key_values: list, optional
            List of tick set's keys values that will be used to find tick.
            If single TickSequenceTick is used, only it is removed.
            If two TickSequenceTicks are used, the whole (excluding right boundary) interval between them is removed.
        named_keys: dict, optional
            Dict of tick set's keys named and values that will be used to find tick.

        Returns
        -------
            :py:class:`~onetick.py.Operation` that evaluates to boolean.
            (1 if tick was erased, and 0 if tick was not in tick set).

        Examples
        --------
        Can be used in per-tick script:

        >>> def fun(tick):
        ...     tick['B'] = tick.state_vars['SET'].erase(1)
        ...     tick.state_vars['SET'].erase(A=1)
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A')
        >>> data = data.script(fun)

        Can be used in source columns operations:

        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'B', otp.eval(otp.Ticks(B=[1, 2, 3])))
        >>> data['C1'] = data.state_vars['SET'].erase(1)
        >>> data['C2'] = data.state_vars['SET'].erase(1)

        >>> otp.run(data)
                Time  A  C1  C2
        0 2003-12-01  1   1   0

        >>> data = data.state_vars['SET'].dump()
        >>> otp.run(data)
                Time  B
        0 2003-12-01  2
        1 2003-12-01  3

        Can be used with :py:meth:`~onetick.py.Source.execute` method
        to do erasing without returning result as :py:class:`~onetick.py.Operation`:

        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'B', otp.eval(otp.Tick(B=2)))
        >>> data = data.execute(data.state_vars['SET'].erase(B=2))

        Example with single TickSetTick:

        >>> def fun(tick):
        ...     tick['RES'] = 0
        ...     for tt in tick.state_vars['set']:
        ...         if tick.state_vars['set'].erase(tt):
        ...             tick['RES'] += 1
        ...     tick['LEN'] = tick.state_vars['set'].get_size()
        >>> data = otp.Tick(X=1)
        >>> data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(otp.Ticks(A=[1, 2, 3])))
        >>> data = data.script(fun)
        >>> otp.run(data)
                Time  X  RES  LEN
        0 2003-12-01  1    3    0

        Example with two TickSetTicks:

        >>> def fun(tick):
        ...     t1 = otp.tick_set_tick()
        ...     t2 = otp.tick_set_tick()
        ...     if tick.state_vars['set'].find(t1, 2) + tick.state_vars['set'].find(t2, 4) == 2:
        ...         tick.state_vars['set'].erase(t1, t2)
        ...     for tt in tick.state_vars['set']:
        ...         tick['RES'] += tt.get_value('A')
        >>> data = otp.Tick(X=1, RES=0)
        >>> data.state_vars['set'] = otp.state.tick_set('oldest', 'A', otp.eval(otp.Ticks(A=[1, 2, 3, 4])))
        >>> data = data.script(fun)
        >>> otp.run(data)
                Time  X  RES
        0 2003-12-01  1    5
        """
        if not named_keys and len(key_values) == 1 and isinstance(key_values[0], TickSequenceTick):
            return self._erase_by_singe_iterator(key_values[0])
        if not named_keys and len(key_values) == 2 and isinstance(key_values[0], TickSequenceTick):
            return self._erase_by_two_iterators(key_values[0], key_values[1])

        return self._erase_by_keys(*key_values, **named_keys)

    def _erase_by_keys(self, *key_values, **named_keys) -> Operation:
        if key_values and isinstance(key_values[0], TickSequenceTick):
            args = [self, *map(value2str, key_values)]
        else:
            args = [self, *map(value2str, self._parse_keys(key_values, named_keys))]
        str_args = ','.join(map(str, args))
        func = 'ERASE_FROM_TICK_SET'
        if named_keys:
            func = 'ERASE_FROM_TICK_SET_BY_NAMED_KEYS'
        op_str = f'{func}({str_args})'
        return Operation(dtype=int, op_str=op_str)

    def _erase_by_singe_iterator(self, tick_sequence_tick: 'TickSequenceTick') -> Optional[Operation]:
        args = [self, value2str(tick_sequence_tick)]
        str_args = ','.join(map(str, args))
        func = 'ERASE_FROM_TICK_SET'
        return Operation(dtype=int, op_str=f'{func}({str_args})')

    def _erase_by_two_iterators(
        self,
        tick_sequence_tick1: 'TickSequenceTick',
        tick_sequence_tick2: 'TickSequenceTick',
    ) -> Optional[Operation]:
        args = [self, value2str(tick_sequence_tick1), value2str(tick_sequence_tick2)]
        str_args = ','.join(map(str, args))
        func = 'ERASE_FROM_TICK_SET'
        return Operation(dtype=int, op_str=f'{func}({str_args})')

    @inplace_operation
    def erase_by_named_keys(self, **named_keys) -> Optional[Operation]:
        """
        Alias for erase with restricted set of parameters

        See also
        --------
        erase
        """
        return self.erase(**named_keys)

    @inplace_operation
    def clear(self):
        """
        Clear tick set.
        Can be used in per-tick script and on Source directly.

        inplace: bool
            If ``True`` current source will be modified else modified copy will be returned.
            Makes sense only if used not in per-tick script.

        Returns
        -------
            if ``inplace`` is False and method is not used in per-tick script
            then returns :py:class:`~onetick.py.Source` copy.

        Examples
        --------
        Can be used in per-tick script:

        >>> def fun(tick):
        ...     tick.state_vars['SET'].clear()
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A')
        >>> data = data.script(fun)

        Can be used in source columns operations:

        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A', otp.eval(otp.Tick(A=1)))
        >>> data = data.state_vars['SET'].clear()

        >>> data = data.state_vars['SET'].dump()
        >>> otp.run(data)
        Empty DataFrame
        Columns: [A, Time]
        Index: []
        """
        if self._is_used_in_per_tick_script:
            return f'CLEAR_TICK_SET({self});'
        self._owner.sink(
            otq.ExecuteExpressions(f'CLEAR_TICK_SET({self})')
        )
        return self._owner

    def insert(self, tick_object=None) -> Optional[Operation]:
        """
        Insert tick into tick set.

        Parameters
        ----------
        tick_object
            Can be set only in per-tick script.
            If not set the current tick is inserted into tick set.

        Returns
        -------
            :py:class:`~onetick.py.Operation` that evaluates to boolean.
            (1 if tick was inserted, and 0 if tick was already presented in tick set).

        Examples
        --------
        Can be used in per-tick script:

        >>> def fun(tick):
        ...     tick.state_vars['SET'].insert(tick)
        ...     tick.state_vars['SET'].insert()
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A')
        >>> data = data.script(fun)

        Can be used in source columns operations:

        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A')
        >>> data['B'] = data.state_vars['SET'].insert()
        >>> data['C'] = data.state_vars['SET'].insert()

        >>> otp.run(data)
                Time  A  B  C
        0 2003-12-01  1  1  0

        >>> data = data.state_vars['SET'].dump()
        >>> otp.run(data)
                Time  A
        0 2003-12-01  1

        Can be used with :py:meth:`~onetick.py.Source.execute` method
        to do insertion without returning result as :py:class:`~onetick.py.Operation`:

        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A')
        >>> data = data.execute(data.state_vars['SET'].insert())

        Inserting current tick in script by passing it to `insert` method apply the changes made to tick:

        >>> def fun(tick):
        ...     tick['A'] = 2
        ...     tick.state_vars['SET'].insert(tick)
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('latest', 'A')
        >>> data = data.script(fun)
        >>> data = data.state_vars['SET'].dump()
        >>> otp.run(data)
                Time  A
        0 2003-12-01  2

        If you want to insert current tick without applied changes use `input` attribute:

        >>> def fun(tick):
        ...     tick['A'] = 2
        ...     tick.state_vars['SET'].insert(tick.input)
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('latest', 'A')
        >>> data = data.script(fun)
        >>> data = data.state_vars['SET'].dump()
        >>> otp.run(data)
                Time  A
        0 2003-12-01  1
        """
        args = [self]
        if tick_object is not None:
            args.append(tick_object)
        str_args = ','.join(map(str, args))
        op_str = f'INSERT_TO_TICK_SET({str_args})'
        if not self._is_used_in_per_tick_script and tick_object is not None:
            raise ValueError("Parameter 'tick_object' can be used only in per-tick script")
        return Operation(dtype=int, op_str=op_str)

    def get_size(self) -> Operation:
        """
        Get size of the tick set.
        Can be used in per-tick script or in Source operations directly.

        Returns
        -------
        :py:class:`~onetick.py.Operation` that evaluates to float value.

        Examples
        --------
        Can be used in per-tick script:

        >>> def fun(tick):
        ...     tick['B'] = tick.state_vars['SET'].get_size()
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A')
        >>> data = data.script(fun)

        Can be used in source columns operations:

        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A')
        >>> data['B'] = data.state_vars['SET'].get_size()

        >>> otp.run(data)
                Time  A  B
        0 2003-12-01  1  0
        """
        return Operation(dtype=int, op_str=f'GET_SIZE_FOR_TICK_SET({self})')

    def size(self) -> Operation:
        """
        See also
        --------
        get_size
        """
        return self.get_size()

    def present(self, *key_values) -> Operation:
        """
        Check if tick with these key values is present in tick set.

        Returns
        -------
        :py:class:`~onetick.py.Operation` that evaluates to boolean (float value 1 or 0).

        Examples
        --------
        Can be used in per-tick script:

        >>> def fun(tick):
        ...     tick['B'] = tick.state_vars['SET'].present(1)
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A')
        >>> data = data.script(fun)

        Can be used in source columns operations:

        >>> data = otp.Tick(A=1)
        >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'A', otp.eval(otp.Tick(A=1)))
        >>> data['B'] = data.state_vars['SET'].present(1)
        >>> data['C'] = data.state_vars['SET'].present(2)

        >>> otp.run(data)
                Time  A  B  C
        0 2003-12-01  1  1  0
        """
        args = [self, *map(value2str, self._parse_keys(key_values))]
        str_args = ','.join(map(str, args))
        return Operation(dtype=int, op_str=f'PRESENT_IN_SET({str_args})')


class TickSetUnordered(TickSet):
    """
    Represents an unordered tick set.
    This class should only be created with :py:func:`onetick.py.state.tick_set_unordered` function
    and should be added to the :py:meth:`onetick.py.Source.state_vars` dictionary
    of the :py:class:`onetick.py.Source` and can be accessed only via this dictionary.

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data.state_vars['SET'] = otp.state.tick_set_unordered('oldest', 'A', max_distinct_keys=10)
    >>> data = data.state_vars['SET'].dump()

    See also
    --------
    :py:class:`TickSequenceTick <onetick.py.core._internal._state_objects.TickSequenceTick>`
    """

    def __init__(self, *args, max_distinct_keys=-1, **kwargs):
        self.max_distinct_keys = max_distinct_keys
        super().__init__(*args, **kwargs)

    def copy(self, *args, **kwargs):
        kwargs.setdefault('max_distinct_keys', self.max_distinct_keys)
        return super().copy(*args, **kwargs)

    def _definition(self) -> str:
        args = ','.join(map(str, [self.insertion_policy, self.max_distinct_keys, *self.key_fields]))
        return f'TICK_SET_UNORDERED({args})'


class TickDeque(TickList):
    """
    Represents a tick deque.
    This class should only be created with :py:func:`onetick.py.state.tick_deque` function
    and should be added to the :py:meth:`onetick.py.Source.state_vars` dictionary
    of the :py:class:`onetick.py.Source` and can be accessed only via this dictionary.

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data.state_vars['DEQUE'] = otp.state.tick_deque()
    >>> data = data.state_vars['DEQUE'].dump()

    See also
    --------
    :py:class:`TickSequenceTick <onetick.py.core._internal._state_objects.TickSequenceTick>`
    """

    def _definition(self) -> str:
        return 'TICK_DEQUE'

    @property
    def _tick_class(self):
        return _TickDequeTick

    @property
    def _dump_ep(self):
        return otq.DumpTickDeque

    def pop_back(self):
        """
        Pop element from back side of deque.
        Can be used only in per-tick script.
        For now returned value can't be assigned to local variable.

        Examples
        --------
        >>> def fun(tick):
        ...     tick.state_vars['DEQUE'].pop_back()
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['DEQUE'] = otp.state.tick_deque()
        >>> data = data.script(fun)
        """
        if not self._is_used_in_per_tick_script:
            raise ValueError('method .pop_back() can only be used in per-tick script')
        return f'{self}.POP_BACK();'

    def pop_front(self):
        """
        Pop element from front side of deque.
        Can be used only in per-tick script.
        For now returned value can't be assigned to local variable.

        Examples
        --------
        >>> def fun(tick):
        ...     tick.state_vars['DEQUE'].pop_front()
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['DEQUE'] = otp.state.tick_deque()
        >>> data = data.script(fun)
        """
        if not self._is_used_in_per_tick_script:
            raise ValueError('method .pop_front() can only be used in per-tick script')
        return f'{self}.POP_FRONT();'

    def get_tick(self, index, tick_object):
        """
        Get tick with this ``index`` from tick set into ``tick_object``.
        Can be used only in per-tick script.
        """
        if not self._is_used_in_per_tick_script:
            raise ValueError('method .get_tick() can only be used in per-tick script')
        if getattr(tick_object, '_owner', None) is None:
            tick_object._owner = self._owner
        return f'{self}.GET_TICK({index},{tick_object});'

    @inplace_operation
    def sort(self, field_name, field_type=None):
        raise NotImplementedError('sort() function is currently not implemented for TickDeque')


class _TickSequenceTickBase(ABC):
    def __init__(self, name, **_):
        self._name = name

    def __str__(self):
        if self._name is None:
            raise ValueError('Trying to use uninitialized tick variable')
        return f'LOCAL::{self._name}'

    @property
    def _definition(self):
        raise NotImplementedError

    @property
    def schema(self) -> dict:
        raise NotImplementedError


class _TickSequenceTickMixin(_TickSequenceTickBase, ABC):
    def get_long_value(self, field_name: Union[str, Operation], dtype=int, check_schema=True) -> Operation:
        """
        Get value of the long ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        dtype: type
            Type to set for output operation. Should be set if value is integer's subclass, e.g. short or byte.
            Default: int
        check_schema: bool
            Check that ``field_name`` exists in tick's schema and its type is int.
            In some cases you may want to disable this behaviour, for example, when tick sequence
            is updated dynamically in different branches of per-tick script. In this case onetick-py
            can't deduce if ``field_name`` exists in schema or has the same type.

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(X=[1, 2, 3])
        >>> def fun(tick):
        ...     tick['SUM'] = 0
        ...     for t in tick.state_vars['LIST']:
        ...         tick['SUM'] += t.get_long_value('X')
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> otp.run(data)
                Time  A  SUM
        0 2003-12-01  1    6
        """
        if not issubclass(dtype, int):
            raise ValueError(f'`dtype` parameter should be one on integer`s subclasses but {dtype} was passed')
        if check_schema:
            check_field_name_in_schema(field_name, dtype=dtype, schema=self.schema)
        return Operation(dtype=dtype,
                         op_str=f'{self}.GET_LONG_VALUE({value2str(field_name)})')

    def set_long_value(self, field_name: Union[str, Operation], value: Union[int, Operation], check_schema=True):
        """
        Set ``value`` of the long ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        value: int, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            Long value to set or operation which return such value.
        check_schema: bool
            Check that ``field_name`` exists in tick's schema and its type is the same as the type of ``value``.
            In some cases you may want to disable this behaviour, for example, when tick sequence
            is updated dynamically in different branches of per-tick script. In this case onetick-py
            can't deduce if ``field_name`` exists in schema or has the same type.

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(A=[1, 2, 3], X=[1, 2, 3])
        >>> def fun(tick):
        ...     for t in tick.state_vars['LIST']:
        ...         t.set_long_value('X', 1)
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> data = data.state_vars['LIST'].dump()
        >>> otp.run(data)
                             Time  A  X
        0 2003-12-01 00:00:00.000  1  1
        1 2003-12-01 00:00:00.001  2  1
        2 2003-12-01 00:00:00.002  3  1
        """
        if check_schema:
            check_field_name_in_schema(field_name, int, value, schema=self.schema)
        return f'{self}.SET_LONG_VALUE({value2str(field_name)}, {value2str(value)});'

    def get_double_value(self, field_name: Union[str, Operation], check_schema=True) -> Operation:
        """
        Get value of the double ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        check_schema: bool
            Check that ``field_name`` exists in tick's schema and its type is float.
            In some cases you may want to disable this behaviour, for example, when tick sequence
            is updated dynamically in different branches of per-tick script. In this case onetick-py
            can't deduce if ``field_name`` exists in schema or has the same type.

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(X=[1.1, 2.2, 3.3])
        >>> def fun(tick):
        ...     tick['SUM'] = 0
        ...     for t in tick.state_vars['LIST']:
        ...         tick['SUM'] += t.get_double_value('X')
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> otp.run(data)
                Time  A  SUM
        0 2003-12-01  1  6.6
        """
        if check_schema:
            check_field_name_in_schema(field_name, dtype=float, schema=self.schema)
        return Operation(dtype=float,
                         op_str=f'{self}.GET_DOUBLE_VALUE({value2str(field_name)})')

    def set_double_value(self, field_name: Union[str, Operation], value: Union[float, Operation], check_schema=True):
        """
        Set ``value`` of the double ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        value: float, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            Double value to set or operation which return such value.
        check_schema: bool
            Check that ``field_name`` exists in tick's schema and its type is the same as the type of ``value``.
            In some cases you may want to disable this behaviour, for example, when tick sequence
            is updated dynamically in different branches of per-tick script. In this case onetick-py
            can't deduce if ``field_name`` exists in schema or has the same type.

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(A=[1, 2, 3], X=[1.1, 2.2, 3.3])
        >>> def fun(tick):
        ...     for t in tick.state_vars['LIST']:
        ...         t.set_double_value('X', 1.1)
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> data = data.state_vars['LIST'].dump()
        >>> otp.run(data)
                             Time  A    X
        0 2003-12-01 00:00:00.000  1  1.1
        1 2003-12-01 00:00:00.001  2  1.1
        2 2003-12-01 00:00:00.002  3  1.1
        """
        if check_schema:
            check_field_name_in_schema(field_name, float, value, schema=self.schema)
        return f'{self}.SET_DOUBLE_VALUE({value2str(field_name)}, {value2str(value)});'

    def get_string_value(self, field_name: Union[str, Operation], dtype=str, check_schema=True) -> Operation:
        """
        Get value of the string ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        dtype: type
            Type to set for output operation. Should be set if value is string's subclass, e.g. otp.varstring.
            Default: str
        check_schema: bool
            Check that ``field_name`` exists in tick's schema and its type is str.
            In some cases you may want to disable this behaviour, for example, when tick sequence
            is updated dynamically in different branches of per-tick script. In this case onetick-py
            can't deduce if ``field_name`` exists in schema or has the same type.

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(X=['a', 'b', 'c'])
        >>> def fun(tick):
        ...     for t in tick.state_vars['LIST']:
        ...         tick['S'] = t.get_string_value('X')
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> otp.run(data)
                Time  A  S
        0 2003-12-01  1  c
        """
        if not issubclass(dtype, str):
            raise ValueError(f'`dtype` parameter should be one on string`s subclasses but {dtype} was passed')
        if check_schema:
            check_field_name_in_schema(field_name, dtype=dtype, schema=self.schema)
        return Operation(dtype=dtype,
                         op_str=f'{self}.GET_STRING_VALUE({value2str(field_name)})')

    def set_string_value(self, field_name: Union[str, Operation], value: Union[str, Operation], check_schema=True):
        """
        Set ``value`` of the string ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        value: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String value to set or operation which return such value.
        check_schema: bool
            Check that ``field_name`` exists in tick's schema and its type is the same as the type of ``value``.
            In some cases you may want to disable this behaviour, for example, when tick sequence
            is updated dynamically in different branches of per-tick script. In this case onetick-py
            can't deduce if ``field_name`` exists in schema or has the same type.

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(A=[1, 2, 3], X=['a', 'b', 'c'])
        >>> def fun(tick):
        ...     for t in tick.state_vars['LIST']:
        ...         t.set_string_value('X', 'a')
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> data = data.state_vars['LIST'].dump()
        >>> otp.run(data)
                             Time  A  X
        0 2003-12-01 00:00:00.000  1  a
        1 2003-12-01 00:00:00.001  2  a
        2 2003-12-01 00:00:00.002  3  a
        """
        if check_schema:
            check_field_name_in_schema(field_name, str, value, schema=self.schema)
        return f'{self}.SET_STRING_VALUE({value2str(field_name)}, {value2str(value)});'

    def get_datetime_value(self, field_name: Union[str, Operation], check_schema=True) -> Operation:
        """
        Get value of the datetime ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        check_schema: bool
            Check that ``field_name`` exists in tick's schema and its type is datetime.
            In some cases you may want to disable this behaviour, for example, when tick sequence
            is updated dynamically in different branches of per-tick script. In this case onetick-py
            can't deduce if ``field_name`` exists in schema or has the same type.

        Examples
        --------
        >>> def another_query():
        ...     t = otp.Ticks(X=['a', 'b', 'c'])
        ...     t['TS'] = t['TIMESTAMP'] + otp.Milli(1)
        ...     return t
        >>> def fun(tick):
        ...     for t in tick.state_vars['LIST']:
        ...         tick['TS'] = t.get_datetime_value('TS')
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> otp.run(data)
                Time  A                      TS
        0 2003-12-01  1 2003-12-01 00:00:00.003
        """
        for i, dtype in enumerate((nsectime, msectime)):
            try:
                if check_schema:
                    check_field_name_in_schema(field_name, dtype=dtype, schema=self.schema)
                break
            except ValueError:
                if i != 1:
                    continue
                else:
                    raise
        return Operation(dtype=dtype,
                         op_str=f'{self}.GET_DATETIME_VALUE({value2str(field_name)})')

    def set_datetime_value(self, field_name: Union[str, Operation], value: Union[int, Operation], check_schema=True):
        """
        Set ``value`` of the datetime ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        value: int, float, str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            Value to set or operation which return such value.
        check_schema: bool
            Check that ``field_name`` exists in tick's schema and its type is the same as the type of ``value``.
            In some cases you may want to disable this behaviour, for example, when tick sequence
            is updated dynamically in different branches of per-tick script. In this case onetick-py
            can't deduce if ``field_name`` exists in schema or has the same type.

        Examples
        --------
        >>> def another_query():
        ...     t = otp.Ticks(X=['a', 'b', 'c'])
        ...     t['TS'] = t['TIMESTAMP'] + otp.Milli(1)
        ...     return t
        >>> def fun(tick):
        ...     for t in tick.state_vars['LIST']:
        ...         t.set_datetime_value('TS', otp.config['default_start_time'])
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> data = data.state_vars['LIST'].dump()
        >>> otp.run(data)
                             Time  X         TS
        0 2003-12-01 00:00:00.000  a 2003-12-01
        1 2003-12-01 00:00:00.001  b 2003-12-01
        2 2003-12-01 00:00:00.002  c 2003-12-01
        """
        if check_schema:
            check_field_name_in_schema(field_name, nsectime, value, schema=self.schema)
        return f'{self}.SET_DATETIME_VALUE({value2str(field_name)}, {value2str(value)});'

    def get_value(self, field_name) -> Operation:
        """
        Get value of the ``field_name`` of the tick.

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(X=[1.1, 2.2, 3.3])
        >>> def fun(tick):
        ...     tick['SUM'] = 0
        ...     for t in tick.state_vars['LIST']:
        ...         tick['SUM'] += t.get_value('X')
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> otp.run(data)
                Time  A  SUM
        0 2003-12-01  1  6.6
        """
        check_field_name_in_schema(field_name, schema=self.schema)
        dtype = self.schema[field_name]
        func_dict = {
            int: self.get_long_value,
            float: self.get_double_value,
            str: self.get_string_value,
            nsectime: self.get_datetime_value,
            msectime: self.get_datetime_value,
        }
        func = func_dict.get(dtype)
        if func is not None:
            return func(field_name)  # type: ignore[operator]
        if issubclass(dtype, int):
            return func_dict[int](field_name, dtype=dtype)  # type: ignore[operator]
        if (issubclass(dtype, str) and
           (dtype is not varstring or is_supported_varstring_in_get_string_value())):
            return func_dict[str](field_name, dtype=dtype)  # type: ignore[operator]
        # decimal is unsupported
        raise TypeError(f'`{dtype}` is unsupported')

    def set_value(self, field_name, value):
        """
        Set ``value`` of the ``field_name`` of the tick.

        Parameters
        ----------
        field_name: str, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            String field name or operation which returns field name.
        value: int, :py:class:`otp.Operation <onetick.py.core.column_operations.base.Operation>`
            Datetime value to set or operation which return such value.

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(A=[1, 2, 3], X=[1.1, 2.2, 3.3])
        >>> def fun(tick):
        ...     for t in tick.state_vars['LIST']:
        ...         t.set_value('X', 0.0)
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> data = data.state_vars['LIST'].dump()
        >>> otp.run(data)
                             Time  A    X
        0 2003-12-01 00:00:00.000  1  0.0
        1 2003-12-01 00:00:00.001  2  0.0
        2 2003-12-01 00:00:00.002  3  0.0
        """
        check_field_name_in_schema(field_name, value=value, schema=self.schema)
        dtype = self.schema[field_name]
        if issubclass(dtype, string):
            dtype = str
        return {
            int: self.set_long_value,
            float: self.set_double_value,
            str: self.set_string_value,
            nsectime: self.set_datetime_value,
        }[dtype](field_name, value)

    def __getitem__(self, field_name):
        """
        Get value of the ``field_name`` of the tick.

        See also
        --------
        get_value

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(X=[1.1, 2.2, 3.3])
        >>> def fun(tick):
        ...     tick['SUM'] = 0
        ...     for t in tick.state_vars['LIST']:
        ...         tick['SUM'] += t['X']
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> otp.run(data)
                Time  A  SUM
        0 2003-12-01  1  6.6
        """
        return self.get_value(field_name)

    def __setitem__(self, field_name, value):
        """
        Set ``value`` of the ``field_name`` of the tick.

        See also
        --------
        set_value

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks(A=[1, 2, 3], X=[1.1, 2.2, 3.3])
        >>> def fun(tick):
        ...     for t in tick.state_vars['LIST']:
        ...         t['X'] = 0.0
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> data = data.state_vars['LIST'].dump()
        >>> otp.run(data)
                             Time  A    X
        0 2003-12-01 00:00:00.000  1  0.0
        1 2003-12-01 00:00:00.001  2  0.0
        2 2003-12-01 00:00:00.002  3  0.0
        """
        return self.set_value(field_name, value)


class TickSequenceTick(_TickSequenceTickMixin, ABC):
    """
    Tick object that can be accessed only in per-tick script.
    This object is a first per-tick script function argument.
    This object is the loop variable of for-cycle in the code of per-tick script
    when for-cycle is used on tick sequence.
    Also tick can be defined as static local variable.

    Examples
    --------
    >>> def another_query():
    ...     return otp.Ticks(X=[1, 2, 3])
    >>> def fun(tick):
    ...     dyn_t = otp.dynamic_tick()
    ...     tick['SUM'] = 0
    ...     for t in tick.state_vars['LIST']:
    ...         tick['SUM'] += t.get_long_value('X')
    ...     dyn_t['X'] = tick['SUM']
    >>> data = otp.Tick(A=1)
    >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
    >>> data = data.script(fun)
    """

    def __init__(self, name, owner=None):
        super().__init__(name)
        self._owner = owner

    @property
    def schema(self) -> dict:
        return self._owner.schema.copy() if self._owner is not None else {}

    def next(self):
        """
        Moves to the next tick in the container.
        """
        return f'{self}.NEXT()'

    def prev(self):
        """
        Moves to the previous tick in the container.
        """
        return f'{self}.PREV()'

    def is_end(self):
        """
        Checks if current tick is valid. If false tick can not be accessed.
        """
        return Operation(dtype=bool, op_str=f'{self}.IS_END()')

    def copy(self, tick_object):
        """
        Makes passed object to point on the same tick (passed object must be of the same type).
        """
        if getattr(tick_object, '_owner', None) is None:
            tick_object._owner = self._owner
        return f'{self}.COPY({tick_object})'

    def get_timestamp(self) -> Operation:
        """
        Get timestamp of the tick.

        Examples
        --------
        >>> def another_query():
        ...     return otp.Ticks({'A': [1, 2, 3]})
        >>> def fun(tick):
        ...    for t in tick.state_vars['LIST']:
        ...        tick['TS'] = t.get_timestamp()
        >>> data = otp.Tick(A=1)
        >>> data.state_vars['LIST'] = otp.state.tick_list(otp.eval(another_query))
        >>> data = data.script(fun)

        >>> otp.run(data)
                Time  A                      TS
        0 2003-12-01  1 2003-12-01 00:00:00.002
        """
        return Operation(dtype=nsectime, op_str=f'{self}.GET_TIMESTAMP()')

    def get_value(self, field_name):
        if field_name in ('TIMESTAMP', 'Time'):
            return self.get_timestamp()
        return super().get_value(field_name)


class _TickListTick(TickSequenceTick):
    _definition = 'TICK_LIST_TICK'


class _TickSetTick(TickSequenceTick):
    _definition = 'TICK_SET_TICK'


class _TickDequeTick(TickSequenceTick):
    _definition = 'TICK_DEQUE_TICK'


class _DynamicTick(_TickSequenceTickMixin):
    """
    This tick type allow to add and update fields dynamically.
    This tick can be inserted to all tick sequences.
    """
    _definition = 'DYNAMIC_TICK'

    def __init__(self, name):
        super().__init__(name)
        self._schema = {}

    @property
    def schema(self):
        return self._schema

    def add_field(self, name, value):
        """
        Add field with `name` and `value` to the tick.
        """
        if name in self.schema:
            raise ValueError(f"Can't add field '{name}' again."
                             " Use set_value or __setitem__ functions to update it.")
        dtype = get_object_type(value)
        self._schema[name] = dtype
        return f'{self}.ADD_FIELD({value2str(name)},{value2str(type2str(dtype))},{value2str(value)});'

    def __setitem__(self, key, value):
        try:
            return self.add_field(key, value)
        except ValueError:
            return super().__setitem__(key, value)

    def copy_fields(self, tick_object, field_names):
        """
        Copy fields with `field_names` from other `tick_object`.
        Fields not listed in the `field_names` argument of the function will be removed from tick.
        """
        if not set(field_names).issubset(tick_object.schema):
            x = set(field_names).difference(tick_object.schema)
            raise ValueError(f"Fields {x} not in tick object schema")
        field_names = ','.join(field_names)
        self._schema = {
            k: v
            for k, v in tick_object.schema.items()
            if k in field_names
        }
        return f'{self}.COPY_FIELDS({tick_object},{value2str(field_names)})'


def _check_field_name(field_name, dtype, value, schema):
    if isinstance(field_name, Operation):
        operation_dtype = field_name.dtype
        if not check_value_dtype(str, operation_dtype):
            raise ValueError(
                f"Can't get value of operation '{str(field_name)}'. "
                f"The type of this operation is '{operation_dtype}'. "
                f"But you are trying to get value with type str."
            )
        return None
    if value is None:
        action = 'get'
        if field_name in ('TIMESTAMP', 'Time'):
            raise ValueError("Please use .get_timestamp() method to get timestamp of the tick")
    else:
        action = 'set'
    schema = schema or {}
    if field_name not in schema:
        raise ValueError(
            f"Field name '{field_name}' not in tick's schema."
            " It may also happen if tick sequence is updated dynamically."
            " In this case to access the value of tick field use functions get_*_value(..., check_schema=False)"
            " and to set the value of tick field use functions set_*_value(..., check_schema=False).")
    field_dtype = schema[field_name]
    if dtype is not None and not check_value_dtype(dtype, field_dtype):
        raise ValueError(
            f"Can't {action} value of field '{field_name}'. "
            f"The type of this field is '{field_dtype}'. "
            f"But you are trying to {action} value with type '{dtype}'."
        )
    return field_dtype


def _check_value(field_dtype, field_name, dtype, value):
    if isinstance(value, Operation):
        value_dtype = value.dtype
    else:
        value_dtype = get_object_type(value)

    error = False
    field_dtype = field_dtype or dtype
    if field_dtype is not None:
        if not isinstance(value, Operation):
            if not check_value_dtype(value, field_dtype, check_string_length=True):
                error = True
        elif not check_value_dtype(value_dtype, field_dtype):
            error = True
    if error:
        raise ValueError(
            f"Can't set value of field '{str(field_name)}'. "
            f"The type of this field is '{field_dtype or dtype}'. "
            f"But you are trying to set value with type '{value_dtype}'."
        )


def check_field_name_in_schema(field_name, dtype=None, value=None, schema=None):
    field_dtype = _check_field_name(field_name, dtype, value, schema)
    if value is not None:
        _check_value(field_dtype, field_name, dtype, value)
