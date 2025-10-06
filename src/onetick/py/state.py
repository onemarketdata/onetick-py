import warnings

from onetick.py import types as ott
from onetick.py.core._internal._state_objects import (
    _StateColumn, TickList, TickSet, TickSetUnordered, TickDeque,
    _TickListTick, _TickSetTick, _TickDequeTick, _DynamicTick,
)


AVAILABLE_SCOPES = (
    "BRANCH",  # A branch-scope state variable is visible to all event processors of the branch its declarator EP
    # belongs to (a branch is a maximal long chain of EPs, each node of which has at most 1 _source and at most 1 sink).
    "ALL_INPUTS",  # An all-inputs-scope state variable is visible to all event processors of the input subgraph of its
    # declarator EP (input subgraph of a particular node is the subset of nodes directly or indirectly feeding it).
    "ALL_OUTPUTS",  # An all-outputs-scope state variable is visible to all event processors of the output subgraph of
    # its declarator EP (output subgraph of a particular node is the subset of nodes directly or indirectly fed by it).
    "QUERY",  # A query scope state variable is visible to all event processors of the graph.
    "CROSS_SYMBOL",  # A cross-symbol scope state variable is visible to all event processors across all unbound
    # symbols. Cross-symbol scope state variables cannot be modified after initialization.
)


def var(default_value, scope="query"):
    """
    Defines a state variable. Supports int, float and string values.

    Parameters
    ----------
    default_value: any
        Default value of the state variable
    scope: str
        Scope for the state variable. Possible values are: query, branch, cross_symbol.
        Default: query

    Returns
    -------
    a state variable that should be assigned to a _source

    Examples
    --------
    >>> data = otp.Ticks(dict(X=[0, 1, 2]))
    >>> data.state_vars['SUM'] = otp.state.var(0)
    >>> data.state_vars['SUM'] += data['X']
    >>> data['SUM'] = data.state_vars['SUM']
    >>> otp.run(data)[['X', 'SUM']]
       X  SUM
    0  0    0
    1  1    1
    2  2    3
    """
    scope = _validate_and_preprocess_scope(scope)

    dtype = ott.get_object_type(default_value)

    if dtype is ott.msectime:
        raise TypeError("State variables do not support msectime type")
    # elif dtype is ott.nsectime:
    #     raise TypeError("State variables do not support nsectime type")
    elif ott.is_time_type(dtype):
        dtype = ott.nsectime

    # pylint: disable-next=unidiomatic-typecheck
    if type(default_value) is str:
        if len(default_value) > ott.string.DEFAULT_LENGTH:
            dtype = ott.string[len(default_value)]

    res = _StateColumn("__STATE_COLUMN", dtype, obj_ref=None, default_value=default_value, scope=scope)

    return res


def tick_list(default_value=None, scope='query', schema=None) -> TickList:
    """
    Defines a state tick list.

    Parameters
    ----------
    default_value: :class:`otp.Source <onetick.py.Source>`, :py:func:`eval query <onetick.py.eval>`
        Evaluated query to initialize tick list from.
    scope: str
        Scope for the state variable.
        Possible values are: query, branch, cross_symbol, all_inputs, all_outputs
    schema: dict, list
        Desired schema for the created tick list. If not passed, schema will be inherited from ``default_value``.
        if ``default_value`` is not passed as well, schema will contain fields of the main Source object.

        If schema is passed as a list, it will select only these fields from the schema of ``default_value``
        or main :class:`Source <onetick.py.Source>` object.

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data.state_vars['LIST'] = otp.state.tick_list(otp.Ticks(B=[1, 2, 3]))
    >>> data = data.state_vars['LIST'].dump()
    >>> otp.run(data)[['B']]
       B
    0  1
    1  2
    2  3
    """
    scope = _validate_and_preprocess_scope(scope)
    return TickList('', obj_ref=None, default_value=default_value, scope=scope, schema=schema)


def tick_set(insertion_policy, key_fields, default_value=None, scope='query', schema=None) -> TickSet:
    """
    Defines a state tick set.

    Parameters
    ----------
    insertion_policy: 'oldest' or 'latest'
        'oldest' specifies not to overwrite ticks with the same keys.
        'latest' makes the last inserted tick overwrite the one with the same keys (if existing).
    key_fields: str, list of str
        The values of the specified fields will be used as keys.
    default_value: :class:`otp.Source <onetick.py.Source>`, :py:func:`eval query <onetick.py.eval>`
        Evaluated query to initialize tick set from.
    scope: str
        Scope for the state variable.
        Possible values are: query, branch, cross_symbol, all_inputs, all_outputs
    schema: dict, list
        Desired schema for the created tick set. If not passed, schema will be inherited from ``default_value``.
        if ``default_value`` is not passed as well, schema will contain fields of the main Source object.

        If schema is passed as a list, it will select only these fields from the schema of ``default_value``
        or main :class:`Source <onetick.py.Source>` object.

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data.state_vars['SET'] = otp.state.tick_set('oldest', 'B', otp.Ticks(B=[1, 1, 2, 2, 3, 3]))
    >>> data = data.state_vars['SET'].dump()
    >>> otp.run(data)[['B']]
       B
    0  1
    1  2
    2  3
    """
    scope = _validate_and_preprocess_scope(scope)
    return TickSet('',
                   insertion_policy=insertion_policy, key_fields=key_fields,
                   obj_ref=None, default_value=default_value, scope=scope,
                   schema=schema)


def tick_set_unordered(insertion_policy,
                       key_fields,
                       default_value=None,
                       max_distinct_keys=-1,
                       scope='query',
                       schema=None) -> TickSetUnordered:
    """
    Defines unordered tick set.

    Parameters
    ----------
    insertion_policy: 'oldest' or 'latest'
        'oldest' specifies not to overwrite ticks with the same keys.
        'latest' makes the last inserted tick overwrite the one with the same keys (if existing).
    key_fields: str, list of str
        The values of the specified fields will be used as keys.
    max_distinct_keys: int
        Expected size of the unordered tick set. It will be used to allocate memory to hold ticks in the tick set.
        It should have the same order as the expected amounts of ticks held in the set. Default value of -1 indicates
        that expected amount of ticks is not known in advance.
        If this value is set correctly, the performance of unordered tick set is expected to be better
        than that of normal tick set.

        .. warning::
            If ``max_distinct_keys`` value is set too low, the performance of unordered tick set
            may be considerably worse than that of normal tick set.
            In particular, **default value of -1 will lead to bad performance and should be avoided**

    default_value: :class:`otp.Source <onetick.py.Source>`, :py:func:`eval query <onetick.py.eval>`
        Evaluated query to initialize unordered tick set from.
    scope: str,
        Scope for the state variable.
        Possible values are: query, branch, cross_symbol, all_inputs, all_outputs
    schema: dict, list
        Desired schema for the created unordered tick set. If not passed,
        schema will be inherited from ``default_value``.
        if ``default_value`` is not passed as well, schema will contain fields of the main Source object.

        If schema is passed as a list, it will select only these fields from the schema of ``default_value``
        or main :class:`Source <onetick.py.Source>` object.

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data.state_vars['SET'] = otp.state.tick_set_unordered('oldest', 'B',
    ...                                                       otp.Ticks(B=[1, 1, 2, 2, 3, 3]),
    ...                                                       max_distinct_keys=5)
    >>> data = data.state_vars['SET'].dump()
    >>> otp.run(data)[['B']]
       B
    0  1
    1  2
    2  3
    """
    scope = _validate_and_preprocess_scope(scope)
    if max_distinct_keys == -1:
        warnings.warn('Unordered tick set with max_distinct_keys == -1 is expected to be inefficient. '
                      'Please consider using regular tick set instead.', stacklevel=2)
    return TickSetUnordered(
        '',
        insertion_policy=insertion_policy, max_distinct_keys=max_distinct_keys, key_fields=key_fields,
        obj_ref=None, default_value=default_value, scope=scope, schema=schema
    )


def tick_deque(default_value=None, scope='query', schema=None) -> TickDeque:
    """
    Defines a state tick deque.

    Parameters
    ----------
    default_value: :class:`otp.Source <onetick.py.Source>`, :py:func:`eval query <onetick.py.eval>`
        Evaluated query to initialize tick deque from.
    scope: str
        Scope for the state variable.
        Possible values are: query, branch, cross_symbol, all_inputs, all_outputs
    schema: dict, list
        Desired schema for the created tick deque. If not passed, schema will be inherited from ``default_value``.
        if ``default_value`` is not passed as well, schema will contain fields of the main Source object.

        If schema is passed as a list, it will select only these fields from the schema of ``default_value``
        or main :class:`Source <onetick.py.Source>` object.

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data.state_vars['DEQUE'] = otp.state.tick_deque(otp.Ticks(B=[1, 2, 3]))
    >>> data = data.state_vars['DEQUE'].dump()
    >>> otp.run(data)[['B']]
       B
    0  1
    1  2
    2  3
    """
    scope = _validate_and_preprocess_scope(scope)
    return TickDeque('', obj_ref=None, default_value=default_value, scope=scope, schema=schema)


def _validate_and_preprocess_scope(scope):
    if isinstance(scope, str):
        scope = scope.upper()
        if scope not in AVAILABLE_SCOPES:
            raise ValueError(f"unknown scope {scope}, please use one of {AVAILABLE_SCOPES}")
    else:
        raise ValueError(f"scope should be one of the following strings: {AVAILABLE_SCOPES}")
    return scope
