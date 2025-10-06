from onetick.py.otq import otq
from onetick.py import state
from onetick.py.core._internal._op_utils.is_const import is_const

from onetick.py.core.column_operations.base import _Operation
from onetick.py.core._internal._state_objects import _StateColumn, _StateBase, _TickSequence
from onetick.py.types import type2str, value2str


class StateVars:
    def __init__(self, owner, previous=None):
        """ Class for organizing state variables

        Parameters
        ----------
        owner: _Source
            Owner of the object
        previous: collections of dict of _Source, optional
            Dictionaries to copy state variables from
        """
        self._owner = owner
        self._columns = {}
        previous = previous if isinstance(previous, (list, tuple)) else (previous,)
        for p in previous:
            if p:
                for name, column in p.state_vars.items:
                    value = column
                    if isinstance(value, _StateBase):
                        value = value.copy(obj_ref=self, name=name)
                    self._columns[name] = value

    def __contains__(self, item):
        return item in self._columns

    def __getitem__(self, item):
        return self._columns[item]

    def __setitem__(self, key, value):
        if isinstance(value, _Operation):
            value = self._set_var_from_operation(key, value)
        elif isinstance(value, _StateBase):
            value = value.copy(obj_ref=self, name=key)
        elif key not in self._columns:
            value = state.var(value).copy(obj_ref=self, name=key)

        if key in self._columns:
            self._owner._update_field(self._columns[key], value)
        else:
            self._declare_new_var(key, value)
            self._columns[key] = value

    def _set_var_from_operation(self, key, value):
        if isinstance(value, _StateColumn):
            assert not value.obj_ref, "You can't set state variable of one Source to another"
            value.obj_ref = self._owner
            value.name = key
        else:
            if not is_const(value):
                assert (key in self._columns), "OneTick supports state variables creation with constants only"
            else:
                value = state.var(value).copy(obj_ref=self, name=key)
        return value

    @property
    def items(self):
        return tuple(self._columns.items())  # prevent mutation

    @property
    def names(self):
        return tuple(self._columns.keys())

    def _declare_new_var(self, name, value):
        # obj_ref is None means that we need to declare it
        base_type = value.dtype
        str_type = ""

        if isinstance(value, _TickSequence):
            # TODO: maybe create definition() for _StateColumn too and move this logic there
            str_type = value._definition()
        else:
            str_type = type2str(base_type)

        expression = f'{str_type} {name}'

        import onetick.py as otp
        if value.default_value is not None:
            if isinstance(value.default_value, otp.Source):
                value_expression = otp.eval(value.default_value).to_eval_string(self._owner._tmp_otq)
            else:
                # TODO: PY-952: use to_eval_string(self._owner._tmp_otq) here if otp.eval is passed
                value_expression = value2str(value.default_value)
            expression += f' = {value_expression}'
        self._owner.sink(otq.DeclareStateVariables(variables=expression, scope=value.scope))
