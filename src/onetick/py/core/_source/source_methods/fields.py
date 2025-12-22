from typing import TYPE_CHECKING, Optional, Type

import numpy as np

from onetick import py as otp
from onetick.py import types as ott
from onetick.py.compatibility import is_existing_fields_handling_supported
from onetick.py.core._internal._state_objects import _StateColumn
from onetick.py.core.column import _Column, _ColumnAggregation, _LagOperator
from onetick.py.core.column_operations._methods.methods import is_arithmetical, is_compare
from onetick.py.core.column_operations._methods.op_types import are_ints_not_time
from onetick.py.core.column_operations.base import _Operation
from onetick.py.core.cut_builder import _BaseCutBuilder
from onetick.py.core.lambda_object import _LambdaIfElse
from onetick.py.otq import otq

from .misc import inplace_operation

if TYPE_CHECKING:
    from onetick.py.core.source import Source


@inplace_operation
def table(self: 'Source', inplace=False, strict: bool = True, **schema) -> Optional['Source']:
    """
    Set the OneTick and python schemas levels according to the ``schema``
    parameter. The ``schema`` should contain either (field_name -> type) pairs
    or (field_name -> default value) pairs; ``None`` means no specified type, and
    OneTick considers it's as a double type.

    Resulting ticks have the same order as in the ``schema``. If only partial fields
    are specified (i.e. when the ``strict=False``) then fields from the ``schema`` have
    the most left position.

    Parameters
    ----------
    inplace: bool
        The flag controls whether operations should be applied inplace
    strict: bool
        If set to ``False``, all fields present in an input tick will be present in the output tick.
        If ``True``, then only fields specified in the ``schema``.
    schema:
        field_name -> type or field_name -> default value pairs that should be applied on the source.

    Returns
    -------
    :class:`Source` or ``None``

    See Also
    --------
    | :attr:`Source.schema`
    | :meth:`__getitem__`: the table shortcut
    | **TABLE** OneTick event processor

    Examples
    --------

    Selection case

    >>> data = otp.Ticks(X1=[1, 2, 3],
    ...                  X2=[3, 2, 1],
    ...                  A1=["A", "A", "A"])
    >>> data = data.table(X2=int, A1=str)   # OTdirective: snippet-name: Arrange.set schema;
    >>> otp.run(data)
                         Time  X2 A1
    0 2003-12-01 00:00:00.000   3  A
    1 2003-12-01 00:00:00.001   2  A
    2 2003-12-01 00:00:00.002   1  A

    Defining default values case (note the order)

    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> data = data.table(Y=0.5, strict=False)
    >>> otp.run(data)
                         Time   Y   X
    0 2003-12-01 00:00:00.000  0.5  1
    1 2003-12-01 00:00:00.001  0.5  2
    2 2003-12-01 00:00:00.002  0.5  3
    """

    def is_time_type_or_nsectime(obj):
        return ott.is_time_type(obj) or isinstance(obj, ott.nsectime)

    def transformer(name, obj):
        if obj is None:
            return name

        res = f'{ott.type2str(ott.get_object_type(obj))} {name}'

        if isinstance(obj, ott._inner_string):
            res = f'{name} {ott.type2str(ott.get_object_type(obj))}'
        if not isinstance(obj, Type) and not is_time_type_or_nsectime(obj):
            res += f' ({ott.value2str(obj)})'
        return res

    def get_type(value):
        if value is None:
            return float
        return ott.get_object_type(value)

    for c_name in list(schema.keys()):
        if self._check_key_is_meta(c_name):
            # meta fields should not be propagated to Table ep
            # otherwise new user-defined field with the same name will appear in schema
            # and using this field will raise an "ambiguous use" error in OneTick
            raise ValueError(f"Can't set meta field {c_name}")

    if not schema:
        return self

    schema_to_set = {c_name: get_type(c_value) for c_name, c_value in schema.items()}

    if strict:
        self.schema.set(**schema_to_set)
    else:
        self.schema.update(**schema_to_set)

    fields = ','.join([transformer(c_name, c_value) for c_name, c_value in schema.items()])

    self.sink(otq.Table(fields=fields, keep_input_fields=not strict))
    for c_name, c_value in schema.items():
        # datetime and nsetime values require onetick built-in functions to be initialized
        # but built-in functions can't be used in table ep so updating columns after the table
        if is_time_type_or_nsectime(c_value):
            self.update({c_name: c_value}, where=self[c_name] == 0, inplace=True)
    self._fix_varstrings()
    return self


def __add_field_parse_value(value):
    if isinstance(value, tuple):
        value, dtype = value
    else:
        dtype = ott.get_object_type(value)

        # pylint: disable-next=unidiomatic-typecheck
        if type(value) is str and len(value) > ott.string.DEFAULT_LENGTH:
            dtype = ott.string[len(value)]

    if issubclass(dtype, bool):
        # according to OneTick transformations
        dtype = float

    if issubclass(dtype, (ott.datetime, ott.date)):
        # according to OneTick transformations
        dtype = ott.nsectime

    # TODO: shouldn't all such logic be in ott.type2str?
    if np.issubdtype(dtype, np.integer):
        dtype = int
    if np.issubdtype(dtype, np.floating):
        dtype = float

    return dtype, value


def _add_field(self: 'Source', key, value):
    if isinstance(value, _ColumnAggregation):
        value.apply(self, key)
        return

    dtype, value = __add_field_parse_value(value)
    type_str = ott.type2str(dtype)
    str_value = ott.value2str(value)

    self.sink(otq.AddField(field=f'{type_str} {key}', value=str_value))

    self.__dict__[key] = _Column(key, dtype, self)


def _update_timestamp(self: 'Source', key, value, str_value):
    if not hasattr(value, "dtype"):
        # A constant value: no need to pre- or post-sort
        self.sink(otq.UpdateField(field=key, value=str_value))
    elif (
        isinstance(value, _Column)
        and not isinstance(value, _StateColumn)
        and hasattr(value, "name")
        and value.name in self.__dict__
    ):
        # An existing, present column: no need to create a temporary one. See PY-253
        need_to_sort = str_value not in ("_START_TIME", "_END_TIME")
        if need_to_sort:
            self.sort(value, inplace=True)
        self.sink(otq.UpdateField(field=key, value=str_value))
        if need_to_sort:
            self.sort(self['Time'], inplace=True)
    elif not is_compare(value) and isinstance(value, (_Operation, _LambdaIfElse)) or is_arithmetical(value):
        # An expression or a statevar: create a temp column with its value, pre- and post- sort.
        self.sink(otq.AddField(field="__TEMP_TIMESTAMP__", value=str_value))
        self.sink(otq.OrderByEp(order_by="__TEMP_TIMESTAMP__ ASC"))
        self.sink(otq.UpdateField(field=key, value="__TEMP_TIMESTAMP__"))
        self.sink(otq.Passthrough(fields="__TEMP_TIMESTAMP__", drop_fields=True))
        self.sort(self['Time'], inplace=True)
    else:
        raise TypeError(f"Illegal type for timestamp assignment: {value.__class__}")


def _replace_positive_lag_operator_with_tmp_column(value):
    """
    Positive lag operator can't be used in UPDATE_FIELD EP,
    so we are replacing it with temporary column.
    """
    if not isinstance(value, _Operation):
        return value, None

    def fun(operation):
        if isinstance(operation, _LagOperator) and operation.index > 0:
            column = operation._op_params[0]
            name = column.name
            if name.startswith("__"):
                raise ValueError(
                    "Column name started with two underscores should be used by system only, "
                    "please do not use such names."
                )
            name = f"__{name}_{operation.index}_NEW__"
            return _Column(name, column.dtype, column.obj_ref, precision=getattr(column, "_precision", None))
        return None

    op, replace_tuples = value._replace_parameters(fun, return_replace_tuples=True)
    return op, {str(new): old for old, new in replace_tuples}


def _update_field(self: 'Source', field, value):

    if isinstance(value, _ColumnAggregation):
        value.apply(self, str(field))
        return

    value, names_mapping = _replace_positive_lag_operator_with_tmp_column(value)
    if names_mapping:
        self.add_fields(names_mapping, inplace=True)

    if isinstance(value, tuple):
        # support to be compatible with adding fields to get rid of some strange problems
        # but really we do not use passed type, because update field does not support it
        value, _ = value

    convert_to_type = None
    str_value = ott.value2str(value)
    value_dtype = ott.get_object_type(value)
    base_type = ott.get_base_type(value_dtype)

    if base_type is bool:
        # according OneTick
        base_type = float

    type_changes = False  # because mantis 0021194
    if base_type is str:
        # update_field non-string field to string field (of any length) or value
        # changes type to default string
        if not issubclass(field.dtype, str):
            field._dtype = str
            type_changes = True

    else:
        if (
            (issubclass(field.dtype, int) or issubclass(field.dtype, float) or issubclass(field.dtype, str))
            and (issubclass(value_dtype, ott.msectime) or issubclass(value_dtype, ott.nsectime))
            and (str(field) != 'TIMESTAMP')
            and (not isinstance(field, _StateColumn))
        ):
            # in OneTick after updating fields with functions that return datetime values
            # the type of column will not change for long and double columns
            # and will change to long (or double in older versions) when updating string column
            # (see BDS-267)
            # That's why we are explicitly setting type for returned value
            convert_to_type = value_dtype
            if issubclass(field.dtype, str):
                # using update_field only for string because update_fields preserves type
                # by default and raises exception if it can't be done
                type_changes = True
        elif issubclass(field.dtype, float) and base_type is int and not isinstance(field, _StateColumn):
            # PY-574 if field was float and int, then
            # it is still float in onetick, so no need to change type on otp level
            convert_to_type = int
        elif (issubclass(field.dtype, ott.msectime) or issubclass(field.dtype, ott.nsectime)) and base_type is int:
            # if field was time type and we add something to it, then
            # no need to change type
            pass
        elif issubclass(field.dtype, str) and base_type is int:
            type_changes = True
            convert_to_type = int
        elif (
            are_ints_not_time(field.dtype, value_dtype) and
            not issubclass(field.dtype, bool) and not issubclass(value_dtype, bool) and
            value_dtype is not field.dtype
        ):
            # case for converting values between int based types, like long or byte
            convert_to_type = value_dtype
        else:
            if issubclass(value_dtype, bool):
                value_dtype = float

            if isinstance(field, _StateColumn):
                pass  # do nothing
            else:
                field._dtype = value_dtype
                type_changes = True

    # for aliases, TIMESTAMP ~ Time as an example
    key = str(field)

    if key == "TIMESTAMP":
        self._update_timestamp(key, value, str_value)

    elif type_changes:
        if value_dtype in [otp.nsectime, int] and issubclass(field.dtype, str):
            # PY-416 string field changing the type from str to datetime leads to losing nanoseconds
            # BE-142 similar issue with int from string: OneTick convert str to float, and then to int
            # so we lose some precision for big integers
            # work around is: make a new column first, delete accessor column
            # and then recreate it with value from temp column
            self.sink(otq.AddField(field=f"_TMP_{key}", value=str_value))
            self.sink(otq.Passthrough(fields=key, drop_fields=True))
            self.sink(otq.AddField(field=f"{key}", value=f"_TMP_{key}"))
            self.sink(otq.Passthrough(fields=f"_TMP_{key}", drop_fields=True))
        else:
            self.sink(otq.UpdateField(field=key, value=str_value))
    else:
        self.sink(otq.UpdateFields(set=key + "=" + str_value))
    if names_mapping:
        self.drop(list(names_mapping), inplace=True)
    if convert_to_type:
        # manual type conversion after update fields for some cases
        self.table(**{key: convert_to_type}, inplace=True, strict=False)


def _validate_before_setting(key, value):
    if key in ["Symbol", "_SYMBOL_NAME"]:
        raise ValueError("Symbol setting is supported during creation only")
    if key == "_state_vars":
        raise ValueError("state field is necessary for keeping state variables and can't be rewritten")
    if isinstance(value, ott.ExpressionDefinedTimeOffset):
        value = value.n
    if isinstance(value, np.generic):
        value = value.item()
    if not (
        ott.is_type_supported(ott.get_object_type(value))
        or isinstance(value, (_Operation, tuple, _ColumnAggregation))
    ):
        raise TypeError(f'It is not allowed to set objects of "{type(value)}" type')
    return value


def __setattr__(self: 'Source', key, value):
    if self._check_key_in_properties(key):
        self.__dict__[key] = value
        return

    # we only allow TIMESTAMP field to be changed
    if self._check_key_is_meta(key) and key not in {'Time', 'TIMESTAMP'}:
        raise ValueError(f"Can't set meta field {key}")

    if isinstance(value, _BaseCutBuilder):
        value(key)
        return

    value = _validate_before_setting(key, value)
    if key in self.__dict__:
        field = self.__dict__[key]
        if issubclass(type(field), _Column):
            self._update_field(field, value)
        else:
            raise AttributeError(f'Column "{key}" not found')
    else:
        assert not (
            isinstance(value, _StateColumn) and value.obj_ref is None
        ), "State variables should be in `state` field"
        self._add_field(key, value)


def __setitem__(self: 'Source', key, value):
    """
    Add new column to the source or update existing one.

    Parameters
    ----------
    key: str
        The name of the new or existing column.
    value: int, str, float, :py:class:`datetime.datetime`, :py:class:`datetime.date`, \
            :py:class:`~onetick.py.Column`, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.string`, \
            :py:class:`otp.date <onetick.py.date>`, :py:class:`otp.datetime <onetick.py.datetime>`, \
            :py:class:`~onetick.py.nsectime`, :py:class:`~onetick.py.msectime`
        The new value of the column.

    See also
    --------
    | **ADD_FIELD** OneTick event processor
    | **UPDATE_FIELD** OneTick event processor

    Examples
    --------
    >>> data = otp.Tick(A='A')
    >>> data['D'] = otp.datetime(2022, 2, 2)
    >>> data['X'] = 1
    >>> data['Y'] = data['X']
    >>> data['X'] = 12345
    >>> data['Z'] = data['Y'].astype(str) + 'abc'
    >>> otp.run(data)
            Time  A          D      X  Y     Z
    0 2003-12-01  A 2022-02-02  12345  1  1abc
    """

    return self.__setattr__(key, value)


@inplace_operation
def add_fields(self: 'Source', fields: dict, override: bool = False, inplace=False):
    """
    Add new columns to the source.

    Parameters
    ----------
    fields: dict
        The dictionary of the names of the new fields and their values.
        The types of supported values in the dictionary are the same as in :meth:`Source.__setitem__`.
    override: bool
        If *False* then exception will be raised for existing fields.
        If *True* then exception will not be raised and the field will be overridden.
    inplace: bool
        A flag controls whether operation should be applied inplace.
        If ``inplace=True``, then it returns nothing. Otherwise method
        returns a new modified object.

    Returns
    -------
    :class:`Source` or ``None``.

    See also
    --------
    | :meth:`Source.__setitem__`
    | **ADD_FIELDS** OneTick event processor

    Examples
    --------

    Add new fields specified in the dictionary:

    >>> data = otp.Tick(A=1)
    >>> data = data.add_fields({
    ...     'D': otp.datetime(2022, 2, 2),
    ...     'X': 12345,
    ...     'Y': data['A'],
    ...     'Z': data['A'].astype(str) + 'abc',
    ... })
    >>> otp.run(data)
            Time  A           D      X  Y     Z
    0 2003-12-01  1  2022-02-02  12345  1  1abc

    Parameter ``override`` can be used to rewrite existing fields:

    .. testcode::
        :skipif: not is_existing_fields_handling_supported()

        data = otp.Tick(A=1)
        data = data.add_fields({'A': 2, 'B': 'b'}, override=True)
        df = otp.run(data)
        print(df)

    .. testoutput::

                Time  A  B
        0 2003-12-01  2  b
    """
    fields_parsed = {}

    kwargs = {}
    if override:
        if not is_existing_fields_handling_supported():
            raise ValueError("Parameter 'override' is not supported on this OneTick build")
        kwargs['existing_fields_handling'] = 'OVERRIDE'

    for key, value in fields.items():
        if self._check_key_in_properties(key):
            raise ValueError("Class properties can't be set with add_fields method")
        if self._check_key_is_meta(key):
            raise ValueError(f"Can't set meta field {key}")
        if not override and key in self.schema:
            raise ValueError(f"Field '{key}' is already in schema")

        # TODO: _validate_before_setting and __add_field_parse_value both modify value, need to refactor
        value = _validate_before_setting(key, value)
        dtype, value = __add_field_parse_value(value)
        fields_parsed[key] = (_Column(key, dtype, self), f'{ott.type2str(dtype)} {key}={ott.value2str(value)}')

    fields_str = ','.join(field_str for _, field_str in fields_parsed.values())
    self.sink(otq.AddFields(fields=fields_str, **kwargs))

    for key, (column, _) in fields_parsed.items():
        self.__dict__[key] = column

    return self


@inplace_operation
def update(self: 'Source', if_set, else_set=None, where=1, inplace=False) -> 'Source':
    """
    Update field of the Source or state variable.

    Parameters
    ----------
    if_set: dict
        Dictionary <field name>: <expression>.
    else_set: dict, optional
        Dictionary <field name>: <expression>
    where: expression, optional
        Condition of updating.

        If ``where`` is True the fields from ``if_set`` will be updated with corresponding expression.

        If ``where`` is False, the fields from ``else_set`` will be updated with corresponding expression.

    inplace: bool
        A flag controls whether operation should be applied inplace.
        If ``inplace=True``, then it returns nothing. Otherwise method
        returns a new modified object.

    Returns
    -------
    :class:`Source` or ``None``.

    See also
    --------
    **UPDATE_FIELD** and **UPDATE_FIELDS** OneTick event processors

    Examples
    --------

    Columns can be updated with this method:

    >>> # OTdirective: snippet-name: Arrange.conditional update;
    >>> t = otp.Ticks({'X': [1, 2, 3],
    ...                'Y': [4, 5, 6],
    ...                'Z': [1, 0, 1]})
    >>> t = t.update(if_set={'X': t['X'] + t['Y']},
    ...              else_set={'X': t['X'] - t['Y']},
    ...              where=t['Z'] == 1)
    >>> otp.run(t) # OTdirective: snippet-example;
                         Time  X  Y  Z
    0 2003-12-01 00:00:00.000  5  4  1
    1 2003-12-01 00:00:00.001 -3  5  0
    2 2003-12-01 00:00:00.002  9  6  1

    State variables can be updated too:

    >>> t = otp.Ticks({'X': [1, 2, 3],
    ...                'Y': [4, 5, 6],
    ...                'Z': [1, 0, 1]})
    >>> t.state_vars['X'] = 0
    >>> t = t.update(if_set={t.state_vars['X']: t['X'] + t['Y']},
    ...              else_set={t.state_vars['X']: t['X'] - t['Y']},
    ...              where=t['Z'] == 1)
    >>> t['UX'] = t.state_vars['X']
    >>> otp.run(t)
                         Time  X  Y  Z  UX
    0 2003-12-01 00:00:00.000  1  4  1   5
    1 2003-12-01 00:00:00.001  2  5  0  -3
    2 2003-12-01 00:00:00.002  3  6  1   9
    """
    if else_set is None:
        else_set = {}

    if len(if_set) == 0 or not isinstance(if_set, dict):
        raise ValueError(f"'if_set' parameter should be non empty dict, but got '{if_set}' of type '{type(if_set)}'")

    def _prepare(to_prepare):
        result = {}

        for in_obj, out_obj in to_prepare.items():
            if isinstance(in_obj, _StateColumn):
                result[in_obj] = out_obj
            elif isinstance(in_obj, _Column):
                result[in_obj.name] = out_obj
            elif isinstance(in_obj, str):
                result[in_obj.strip()] = out_obj
            else:
                raise AttributeError(f"It is not supported to update item '{in_obj}' of type '{type(in_obj)}'")

        return result

    def _validate(to_validate):
        result = {}
        for in_key, out_obj in to_validate.items():
            if isinstance(in_key, _StateColumn):
                in_dtype = in_key.dtype
            else:
                if not (in_key in self.__dict__ and isinstance(self.__dict__[in_key], _Column)):
                    raise AttributeError(f"There is no '{in_key}' column to update")

                if self._check_key_is_meta(in_key):
                    raise ValueError(f"Can't set meta field {in_key}")

                in_dtype = self.schema[in_key]

            dtype = ott.get_object_type(out_obj)
            if not ott.is_type_supported(dtype):
                raise TypeError(f"Deduced type of object {out_obj} is not supported: {dtype}")
            # will raise exception if types are incompatible
            _ = ott.get_type_by_objects([dtype, in_dtype])

            if isinstance(in_key, _StateColumn):
                in_key = str(in_key)

            if isinstance(out_obj, bool):
                out_obj = int(out_obj)

            result[in_key] = out_obj

        return result

    # prepare and validate
    items = _validate(_prepare(if_set))
    else_items = _validate(_prepare(else_set))

    if isinstance(where, bool):
        where = int(where)

    if not (getattr(where, "dtype", None) is bool or isinstance(where, int)):
        raise ValueError(f"Where has not supported type '{type(where)}'")

    # apply
    set_rules = [f"{key}=({ott.value2str(value)})" for key, value in items.items()]
    else_set_rules = [f"{key}=({ott.value2str(value)})" for key, value in else_items.items()]

    self.sink(otq.UpdateFields(set=",".join(set_rules), else_set=",".join(else_set_rules), where=str(where)))
    return self
