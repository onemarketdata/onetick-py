import ast
import inspect
import textwrap
import types
import operator
import tokenize
import warnings
from typing import Callable, Union, Any, Optional, Iterable, Type, Tuple, Dict, List, TypeVar
from copy import deepcopy
from functools import wraps

from onetick.py.backports import astunparse, cached_property

from collections import deque
from contextlib import contextmanager

from .. import types as ott
from .column_operations.base import _Operation
from .column import _Column
from .lambda_object import _EmulateObject, _LambdaIfElse, _EmulateStateVars, _CompareTrackScope
from ._internal._state_objects import (
    _TickSequence, _TickSequenceTickBase, _TickListTick, _TickSetTick, _TickDequeTick, _DynamicTick
)


class Static:
    """
    Class for declaring static local variable in per-tick script.
    Static variables are defined once and save their values between
    arrival of the input ticks.
    """
    def __init__(self, value):
        self.value = value

    # these functions needed mostly for linters
    def __getattr__(self, item):
        return self.value.__getattr__(item)

    def __getitem__(self, item):
        return operator.getitem(self.value, item)

    def __setitem__(self, key, value):
        return operator.setitem(self.value, key, value)


class LocalVariable(_Operation):
    """
    Class for inner representation of local variable in per-tick script.
    Only simple values are supported, tick sequences are represented by another class.
    """
    def __init__(self, name, dtype=None):
        super().__init__(op_str=f'LOCAL::{name}', dtype=dtype)
        self.name = name


class TickDescriptorFields(_TickSequence):
    """
    Class for declaring tick descriptor fields in per-tick script.
    Can only be iterated, doesn't have methods and parameters.

    See also
    --------
    :py:class:`TickDescriptorField <onetick.py.core.per_tick_script.TickDescriptorField>`

    Examples
    --------
    >>> t = otp.Tick(A=1)
    >>> def fun(tick):
    ...     for field in otp.tick_descriptor_fields():
    ...         tick['NAME'] = field.get_name()
    >>> t = t.script(fun)
    >>> otp.run(t)
            Time  A NAME
    0 2003-12-01  1    A
    """
    def __init__(self):
        # we don't want to inherit parent class init
        pass

    def __str__(self):
        return 'LOCAL::INPUT_TICK_DESCRIPTOR_FIELDS'

    @property
    def _tick_class(self):
        return TickDescriptorField


def tick_list_tick():
    """
    Can be used only in per-tick script function
    to define a tick list tick local variable.

    Tick list ticks can be used with some methods
    of tick lists :py:class:`onetick.py.state.tick_list`.

    See also
    --------
    :py:class:`onetick.py.state.tick_list`.

    Returns
    -------
    :py:class:`onetick.py.static` value with tick object.

    Examples
    --------
    >>> def fun(tick):
    ...    t = otp.tick_list_tick()
    ...    tick.state_vars['LIST'].push_back(t)
    """
    return Static(_TickListTick(None))


def tick_set_tick():
    """
    Can be used only in per-tick script function
    to define a tick set tick local variable.

    Tick set ticks can be used with some methods
    of tick sets :py:class:`onetick.py.state.tick_set`.

    See also
    --------
    :py:class:`onetick.py.state.tick_set`.

    Returns
    -------
    :py:class:`onetick.py.static` value with tick object.

    Examples
    --------
    >>> def fun(tick):
    ...    t = otp.tick_set_tick()
    ...    if tick.state_vars['SET'].find(t, -1):
    ...        tick['RES'] = '-1'
    """
    return Static(_TickSetTick(None))


def tick_deque_tick():
    """
    Can be used only in per-tick script function
    to define a tick deque tick local variable.

    Tick deque ticks can be used with some methods
    of tick deques :py:class:`onetick.py.state.tick_deque`.

    See also
    --------
    :py:class:`onetick.py.state.tick_deque`.

    Returns
    -------
    :py:class:`onetick.py.static` value with tick object.

    Examples
    --------
    >>> def fun(tick):
    ...    t = otp.tick_deque_tick()
    ...    tick.state_vars['DEQUE'].get_tick(0, t)
    """
    return Static(_TickDequeTick(None))


def dynamic_tick():
    """
    Can be used only in per-tick script function
    to define a dynamic tick local variable.

    Dynamic ticks can be used with some methods
    of all tick sequences.

    See also
    --------
    :py:class:`onetick.py.state.tick_list`
    :py:class:`onetick.py.state.tick_set`
    :py:class:`onetick.py.state.tick_deque`

    Returns
    -------
    :py:class:`onetick.py.static` value with tick object.

    Examples
    --------
    >>> def fun(tick):
    ...    t = otp.dynamic_tick()
    ...    t['X'] = tick['SUM']
    """
    return Static(_DynamicTick(None))


class TickDescriptorField(_TickSequenceTickBase):
    """
    Tick descriptor field object.
    Can be accessed only while iterating over
    :py:class:`otp.tick_descriptor_fields <onetick.py.core.per_tick_script.TickDescriptorFields>`
    in per-tick script.

    Examples
    --------
    >>> t = otp.Tick(A=2, B='B', C=1.2345)
    >>> def fun(tick):
    ...     tick['NAMES'] = ''
    ...     tick['TYPES'] = ''
    ...     tick['SIZES'] = ''
    ...     for field in otp.tick_descriptor_fields():
    ...         tick['NAMES'] += field.get_name() + ','
    ...         tick['TYPES'] += field.get_type() + ','
    ...         tick['SIZES'] += field.get_size().apply(str) + ','
    >>> t = t.script(fun)
    >>> otp.run(t)
            Time  A  B       C   NAMES                TYPES    SIZES
    0 2003-12-01  2  B  1.2345  A,B,C,  long,string,double,  8,64,8,
    """

    _definition = 'TICK_DESCRIPTOR_FIELD'

    def get_field_name(self):
        """
        Get the name of the field.

        Returns
        -------
        onetick.py.Operation
        """
        return _Operation(op_str=f'{self}.GET_FIELD_NAME()', dtype=str)

    def get_name(self):
        """
        Get the name of the field.

        Returns
        -------
        onetick.py.Operation
        """
        return self.get_field_name()

    def get_size(self):
        """
        Get the size of the type of the field.

        Returns
        -------
        onetick.py.Operation
        """
        return _Operation(op_str=f'{self}.GET_SIZE()', dtype=int)

    def get_type(self):
        """
        Get the name of the type of the field.

        Returns
        -------
        onetick.py.Operation
        """
        return _Operation(op_str=f'{self}.GET_TYPE()', dtype=str)


class Expression:
    """
    Class to save per-tick-script expressions along with their possible values.

    Parameters
    ----------
    expr
        string expression that will be saved to per tick script
    values:
        values that this expression can take.
        For example, bool operation can take many values.
    lhs:
        True if expression is left hand expression.
        In this case value of expression must be callable.
        Calling it with right hand expression value as an argument
        should be the same as execute the whole expression.
    """
    def __init__(self, *values: Any, expr: Optional[str] = None, lhs: bool = False):
        self.values = values
        self._expr = expr
        self.lhs = lhs
        if self.lhs:
            assert callable(self.value)
            assert self.expr

    @property
    def expr(self):
        if self._expr:
            return self._expr
        if self.is_emulator:
            self._expr = 'LOCAL::OUTPUT_TICK'
        elif self.is_column:
            self._expr = str(self.value)
        elif self.values:
            self._expr = self.value_to_onetick(self.value)
        return self._expr

    @property
    def value(self):
        length = len(self.values)
        if length == 0:
            raise ValueError(f"Expression '{self}' doesn't have values.")
        if length > 1:
            raise ValueError(f"Expression '{self}' have more than one value.")
        return self.values[0]

    @cached_property
    def dtype(self):
        return ott.get_type_by_objects(self.values)

    @property
    def is_emulator(self) -> bool:
        try:
            return isinstance(self.value, _EmulateObject)
        except ValueError:
            return False

    @property
    def is_state_vars(self) -> bool:
        try:
            return isinstance(self.value, _EmulateStateVars)
        except ValueError:
            return False

    @property
    def is_static(self) -> bool:
        try:
            return isinstance(self.value, Static)
        except ValueError:
            return False

    @property
    def is_dynamic_tick(self) -> bool:
        try:
            return isinstance(self.value, _DynamicTick)
        except ValueError:
            return False

    @property
    def is_tick(self) -> bool:
        try:
            return isinstance(self.value, _TickSequenceTickBase)
        except ValueError:
            return False

    @property
    def is_column(self) -> bool:
        try:
            return isinstance(self.value, _Column)
        except ValueError:
            return False

    @property
    def is_local_variable(self) -> bool:
        try:
            return isinstance(self.value, LocalVariable)
        except ValueError:
            return False

    @property
    def is_operation(self) -> bool:
        try:
            return isinstance(self.value, _Operation)
        except ValueError:
            return False

    @property
    def predefined(self) -> bool:
        """Check if the value of expression is known before the execution of query"""
        return not self.is_operation

    @property
    def expressible(self) -> bool:
        return bool(self.expr)

    def __str__(self):
        if not self.expressible:
            raise ValueError("This Expression can't be expressed in OneTick or is undefined yet")
        return self.expr

    def convert_to_operation(self):
        """
        Convert otp.Column to otp.Operation.
        Needed to convert expressions like:
            if tick['X']:
        to
            if (X != 0) {
        """
        if self.is_column:
            self.values = [self.value._make_python_way_bool_expression()]
            self._expr = str(self.value)

    @staticmethod
    def value_to_onetick(value: Union[str, int, float, bool, None, _Operation]) -> str:
        """
        Python value will be converted accordingly to OneTick syntax
        (lowercase boolean values, string in quotes, etc.)
        """
        if value is None:
            return str(ott.nan)
        if isinstance(value, bool):
            return str(value).lower()
        return ott.value2str(value)


class CaseOperatorParser:
    """
    Class with methods to convert ast operators to their string or python representations.
    Only ast operators that can be used in OneTick's CASE function are accepted.
    """

    @staticmethod
    def py_operator(op: Union[ast.operator, ast.cmpop, ast.unaryop, ast.boolop]) -> Callable:
        """
        Convert ast operator to python function for this operator.

        Parameters
        ----------
        op
            ast operator object
        """
        return {
            # binary
            ast.Add: operator.add,
            ast.Sub: operator.sub,
            ast.Mult: operator.mul,
            ast.Div: operator.truediv,
            ast.BitAnd: operator.and_,
            ast.BitOr: operator.or_,
            ast.Mod: operator.mod,
            # unary
            ast.UAdd: operator.pos,
            ast.USub: operator.neg,
            ast.Not: operator.not_,
            ast.Invert: operator.invert,
            # compare
            ast.Lt: operator.lt,
            ast.LtE: operator.le,
            ast.Gt: operator.gt,
            ast.GtE: operator.ge,
            ast.Eq: operator.eq,
            ast.NotEq: operator.ne,
            # bool
            ast.And: lambda x, y: x and y,
            ast.Or: lambda x, y: x or y,
        }[type(op)]  # type: ignore[return-value]

    @staticmethod
    def operator(op: Union[ast.operator, ast.cmpop, ast.unaryop, ast.boolop]) -> str:
        """
        Convert ast operator to OneTick's string representation.

        Parameters
        ----------
        op
            ast operator object
        """
        return {
            # binary
            ast.Add: '+',
            ast.Sub: '-',
            ast.Mult: '*',
            ast.Div: '/',
            ast.Mod: '%',
            # unary
            ast.UAdd: '+',
            ast.USub: '-',
            # compare
            ast.Lt: '<',
            ast.LtE: '<=',
            ast.Gt: '>',
            ast.GtE: '>=',
            ast.Eq: '=',
            ast.NotEq: '!=',
            # bool
            ast.And: 'AND',
            ast.Or: 'OR',
        }[type(op)]


class OperatorParser(CaseOperatorParser):
    """
    Class with methods to convert ast operators to their string or python representations.
    Only ast operators that can be used in OneTick's per tick script are accepted.
    """

    @staticmethod
    def py_operator(op: Union[ast.operator, ast.cmpop, ast.unaryop, ast.boolop],
                    aug: bool = False, **kwargs) -> Callable:
        """
        Convert ast operator to python function for this operator.

        Parameters
        ----------
        op
            ast operator object
        aug
            ast don't have separate inplace operators (+=, -=, etc.)
            If this parameter is True then operator is inplace and otherwise if False.
        """
        if aug:
            assert isinstance(op, ast.operator)
            return {
                ast.Add: operator.iadd,
                ast.Sub: operator.isub,
                ast.Mult: operator.imul,
                ast.Div: operator.itruediv,
            }[type(op)]
        return CaseOperatorParser.py_operator(op, **kwargs)

    @staticmethod
    def operator(op: Union[ast.operator, ast.cmpop, ast.unaryop, ast.boolop],
                 aug: bool = False) -> str:
        """
        Convert ast operator to its string representation.

        Parameters
        ----------
        op
            ast operator object
        aug
            ast don't have separate inplace binary operators (+=, -=, etc.)
            If this parameter is True then parameter is inplace and otherwise if False.
        """
        if aug:
            assert isinstance(op, ast.operator)
            return {
                ast.Add: '+=',
                ast.Sub: '-=',
                ast.Mult: '*=',
                ast.Div: '/=',
            }[type(op)]
        try:
            return {
                ast.Eq: '==',
                ast.And: '&&',
                ast.Or: '||',
            }[type(op)]
        except KeyError:
            return CaseOperatorParser.operator(op)


class ExpressionParser:
    """
    Class with methods to convert ast expressions to OneTick's script or function syntax.
    """
    def __init__(self, fun: 'FunctionParser'):
        self.fun = fun
        self.operator_parser = OperatorParser()

    @contextmanager
    def _replace_context(self, closure_vars: inspect.ClosureVars):
        """
        Temporarily change closure variables in self.fun.
        Variables will be replaced with those from closure_vars parameter.
        """
        nonlocals, globals_, *_ = closure_vars
        assert isinstance(self.fun.closure_vars.globals, dict)
        assert isinstance(self.fun.closure_vars.nonlocals, dict)
        saved_globals = self.fun.closure_vars.globals.copy()
        saved_nonlocals = self.fun.closure_vars.nonlocals.copy()
        self.fun.closure_vars.globals.update(globals_)
        self.fun.closure_vars.nonlocals.update(nonlocals)
        yield
        self.fun.closure_vars.globals.update(saved_globals)
        self.fun.closure_vars.nonlocals.update(saved_nonlocals)

    def constant(self, expr: ast.Constant) -> Expression:
        """Some basic constant value: string, integer, float."""
        return Expression(expr.value)

    def string(self, expr: "ast.Str") -> Expression:
        """String (for backward compatibility with Python 3.7)."""
        return Expression(expr.s)

    def number(self, expr: "ast.Str") -> Expression:
        """Number (for backward compatibility with Python 3.7)."""
        return Expression(expr.n)

    def name(self, expr: ast.Name) -> Expression:
        """
        Name of the variable.
        Every variable in per-tick script function, if defined correctly,
        is considered to be local per-tick script variable.
        If variable with this name is not found it will be captured from function context.
        """
        if self.fun.arg_name and expr.id == self.fun.arg_name:
            value = self.fun.emulator if self.fun.emulator is not None else expr.id
            return Expression(value)

        if not isinstance(expr.ctx, ast.Load):
            # local variable, left-hand side
            return Expression(LocalVariable(expr.id))

        for dict_name in ('LOCAL_VARS', 'STATIC_VARS'):
            # local or static variable, right-hand side
            variables = getattr(self.fun.emulator, dict_name, {})
            if expr.id in variables:
                dtype = ott.get_type_by_objects([variables[expr.id]])
                if issubclass(dtype, _TickSequenceTickBase):
                    # ticks have schema, owner and methods, so using saved value
                    return Expression(variables[expr.id])
                return Expression(LocalVariable(expr.id, dtype))

        if not self.fun._from_args_annotations and expr.id in self.fun.args_annotations:
            # parameter of the per-tick script function
            return Expression(_Operation(op_str=expr.id, dtype=self.fun.args_annotations[expr.id]))

        # pylint: disable-next=eval-used
        value = eval(expr.id, self.fun.closure_vars.globals, self.fun.closure_vars.nonlocals)  # type: ignore[arg-type]
        return Expression(value)

    def index(self, expr: ast.Index) -> Expression:
        """Proxy object in ast.Subscript in python < 3.9"""
        return self.expression(expr.value)  # type: ignore[attr-defined]

    def slice(self, expr: ast.Slice) -> Expression:
        """
        Slice of the list.
        For example:
            a = [1, 2, 3, 4]
            a[2:4]
        Here, 2:4 is the slice.
        """
        lower = self.expression(expr.lower).value if expr.lower else None
        upper = self.expression(expr.upper).value if expr.upper else None
        step = self.expression(expr.step).value if expr.step else None
        return Expression(slice(lower, upper, step))

    def subscript(self, expr: ast.Subscript) -> Expression:
        """
        Expression like: tick['X'].
        Setting items of ticks and state variables is supported.
        Getting items supported for any captured variable.
        """
        val = self.expression(expr.value)
        item = self.expression(expr.slice)

        if isinstance(expr.ctx, ast.Load):
            v = val.value[item.value]
            return Expression(v)

        # index of per tick script function parameter or tick sequence tick is column name
        if not (val.is_emulator or val.is_tick or val.is_state_vars):
            raise ValueError(f"Setting items supported only for "
                             f"'{self.fun.arg_name}' function argument, "
                             f"tick sequences' ticks and state variables object")

        return Expression(
            lambda rhs: val.value.__setitem__(item.value, rhs),
            expr=item.value,
            lhs=True,
        )

    def attribute(self, expr: ast.Attribute) -> Expression:
        """
        Expression like: tick.X
        For now we only support setting attributes of first function parameter.
        Getting attributes supported for any captured variable.
        """
        val = self.expression(expr.value)
        attr = expr.attr

        if isinstance(expr.ctx, ast.Load):
            v = getattr(val.value, attr)
            return Expression(v)

        # attribute of per tick script function parameter or tick sequence tick is column name
        if not (val.is_emulator or val.is_tick):
            raise ValueError(f"Setting attributes supported only for "
                             f"'{self.fun.arg_name}' function argument and tick sequences' ticks")

        return Expression(
            lambda rhs: val.value.__setattr__(attr, rhs),
            expr=attr,
            lhs=True,
        )

    def bin_op(self, expr: ast.BinOp) -> Expression:
        """
        Binary operation expression: 2 + 2, tick['X'] * 2, etc.
        """
        left = self.expression(expr.left)
        py_op = self.operator_parser.py_operator(expr.op)
        right = self.expression(expr.right)
        value = py_op(left.value, right.value)
        return Expression(value)

    def unary_op(self, expr: ast.UnaryOp) -> Expression:
        """
        Unary operation expression: -1, -tick['X'], not tick['X'], ~tick['X'], etc.
        """
        py_op = self.operator_parser.py_operator(expr.op)
        operand = self.expression(expr.operand)
        if operand.is_operation:
            # special case for negative otp.Columns and otp.Operations
            if isinstance(expr.op, (ast.Not, ast.Invert)):
                operand.convert_to_operation()
            if isinstance(expr.op, ast.Not):
                py_op = self.operator_parser.py_operator(ast.Invert())
        value = py_op(operand.value)
        return Expression(value)

    def bool_op(self, expr: ast.BoolOp) -> Expression:
        """
        Bool operation expression: True and tick['X'], etc.
        Note that
            * all python values will be checked inplace and will not be written to the script
            * short-circuit logic will work for python values
        For example:
            True   and  0     and tick['X'] == 1  -------> false
            'true' or   False or  tick['X'] == 1  -------> true
            True   and  True  and tick['X'] == 1  -------> X == 1
        """
        value = None
        for e in expr.values:
            expression = self.expression(e)
            expression.convert_to_operation()
            v = expression.value

            if not expression.is_operation:
                # short-circuit logic, return as early as possible
                if isinstance(expr.op, ast.And) and not v:
                    # TODO: return v, not True or False
                    # TODO: there can be many values if operations are present
                    value = False
                    break
                if isinstance(expr.op, ast.Or) and v:
                    value = True
                    break
                continue

            if value is None:
                value = v
                continue

            if isinstance(value, _Operation) or expression.is_operation:
                # change operator for operations
                py_op = self.operator_parser.py_operator({
                    ast.And: ast.BitAnd(),
                    ast.Or: ast.BitOr(),
                }[type(expr.op)])
            else:
                py_op = self.operator_parser.py_operator(expr.op)

            value = py_op(value, v)
        return Expression(value)

    def _convert_in_to_bool_op(self, expr: ast.Compare) -> Union[ast.Compare, ast.BoolOp]:
        """
        Convert expressions like:
            tick['X']     in [1, 2]   ----->   tick['X'] == 1  or tick['X'] == 2
            tick['X'] not in [1, 2]   ----->   tick['X'] != 1 and tick['X'] != 2
        """
        left, op, right = expr.left, expr.ops[0], expr.comparators[0]
        if not isinstance(op, (ast.In, ast.NotIn)):
            return expr

        assert len(expr.ops) == 1
        assert len(expr.comparators) == 1

        right_value = self.expression(right).value
        if isinstance(right_value, range) and right_value.step == 1:
            # replace tick['X'] in range(5, 10) to X >=5 AND X < 10
            return ast.BoolOp(
                op=ast.And(),
                values=[
                    ast.Compare(left=left, ops=[ast.GtE()], comparators=[ast.Constant(right_value.start)]),
                    ast.Compare(left=left, ops=[ast.Lt()], comparators=[ast.Constant(right_value.stop)]),
                ]
            )

        right_values = [ast.Constant(r) for r in right_value]

        bool_op: ast.boolop
        compare_op: ast.cmpop
        if isinstance(op, ast.In):
            bool_op, compare_op = ast.Or(), ast.Eq()
        else:
            bool_op, compare_op = ast.And(), ast.NotEq()

        values: List[ast.expr] = [
            ast.Compare(left=left, ops=[compare_op], comparators=[r])
            for r in right_values
        ]
        return ast.BoolOp(op=bool_op, values=values)

    def _convert_many_comparators_to_bool_op(self, expr: ast.Compare) -> Union[ast.Compare, ast.BoolOp]:
        """
        OneTick don't support compare expressions with many comparators
        so replacing them with several simple expressions.
        For example:
            1 < tick['X'] < 3,   ----->   tick['X'] > 1 AND tick['X'] < 3
        """
        if len(expr.comparators) == 1 and len(expr.ops) == 1:
            return expr

        comparators = []
        comparators.append(expr.left)
        ops = []
        for op, right in zip(expr.ops, expr.comparators):
            ops.append(op)
            comparators.append(right)

        bool_operands: List[ast.expr] = []
        for i in range(len(comparators) - 1):
            left, op, right = comparators[i], ops[i], comparators[i + 1]
            bool_operands.append(
                ast.Compare(left=left, ops=[op], comparators=[right])
            )
        return ast.BoolOp(op=ast.And(), values=bool_operands)

    def compare(self, expr: ast.Compare) -> Expression:
        """
        Compare operation expression: tick['X'] > 1, 1 < 2 < 3, tick['X'] in [1, 2] etc.
        """
        if len(expr.ops) > 1:
            return self.expression(
                self._convert_many_comparators_to_bool_op(expr)
            )

        op = expr.ops[0]
        if isinstance(op, (ast.In, ast.NotIn)):
            return self.expression(
                self._convert_in_to_bool_op(expr)
            )

        left = self.expression(expr.left)
        right = self.expression(expr.comparators[0])

        py_op = self.operator_parser.py_operator(op)
        value = py_op(left.value, right.value)
        return Expression(value)

    def keyword(self, expr: ast.keyword) -> Tuple[str, Any]:
        """
        Keyword argument expression from function call: func(key=value).
        Not converted to per tick script in any way, needed only in self.call() function.
        """
        arg = expr.arg
        val = self.expression(expr.value)
        assert arg is not None
        return arg, val.value

    def call(self, expr: ast.Call) -> Expression:
        """
        Any call expression, like otp.nsectime().
        The returned value of the call will be inserted in script.
        """
        func = self.expression(expr.func)

        # TODO: refactor, this code is copy-pasted from CaseExpressionParser.call()
        need_to_parse = False
        if not isinstance(func.value, types.BuiltinMethodType) and expr.args:
            node = expr.args[0]
            if isinstance(node, ast.Name) and node.id == self.fun.arg_name:
                # we will parse inner function call to OneTick per-tick script function
                # only if one of the function call arguments is 'tick' parameter of the original function
                need_to_parse = True

        if need_to_parse:
            err = None
            func_name = func.value.__name__
            if str(func.value) not in self.fun.emulator.FUNCTIONS:
                try:
                    fp = FunctionParser(func.value, emulator=self.fun.emulator,
                                        check_arg_name=self.fun.arg_name, inner_function=True)
                except Exception as e:
                    err = e
                else:
                    value = fp.per_tick_script()
                    self.fun.emulator.FUNCTIONS[str(func.value)] = (value, fp.args_annotations, fp.return_annotation)
            if err is None:
                _, args_annotations, return_type = self.fun.emulator.FUNCTIONS[str(func.value)]
                if expr.keywords:
                    raise ValueError(
                        f"Passing keyword parameters to per-tick script function '{func_name}' is not supported"
                    )
                args = []
                for arg, (name, dtype) in zip(expr.args[1:], args_annotations.items()):
                    if isinstance(arg, ast.Starred):
                        raise ValueError(
                            f"Passing starred parameter to per-tick script function '{func_name}' is not supported"
                        )
                    arg_expr = self.expression(arg)
                    msg = (f"In function '{func_name}' parameter '{name}'"
                           f" has type annotation '{dtype.__name__}',"
                           f" but the type of passed argument is '{arg_expr.dtype.__name__}'")
                    try:
                        widest_type = ott.get_type_by_objects([arg_expr.dtype, dtype])
                    except TypeError as e:
                        raise TypeError(msg) from e
                    if widest_type is not arg_expr.dtype:
                        raise TypeError(msg)
                    args.append(arg_expr)
                str_args = ', '.join(map(str, args))
                return Expression(_Operation(op_str=f'{func_name}({str_args})', dtype=return_type))

        args = []
        for arg in expr.args:
            # TODO: support starred in CaseExpressionParser.call()
            if isinstance(arg, ast.Starred):
                args.extend(self.expression(arg.value).value)
            else:
                args.append(self.expression(arg).value)
        keywords = dict(self.keyword(keyword) for keyword in expr.keywords)
        value = func.value(*args, **keywords)
        return Expression(value)

    def formatted_value(self, expr: ast.FormattedValue) -> Expression:
        """
        Block from the f-string in curly brackets, e.g.
            {tick['A']} and {123} in f"{tick['A']} {123}"
        """
        return self.expression(expr.value)

    def joined_str(self, expr: ast.JoinedStr) -> Expression:
        """
        F-string expression, like: f"{tick['A']} {123}"
        """
        expressions = [self.expression(value) for value in expr.values]
        value = None
        for expression in expressions:
            v = expression.value
            if expression.is_operation:
                v = v.apply(str)
            else:
                v = str(v)
            if value is None:
                value = v
            else:
                value = value + v
        return Expression(value)

    def list(self, expr: ast.List) -> Expression:
        """
        List expression, like: [1, 2, 3, 4, 5]
        """
        value = []
        for e in expr.elts:
            if isinstance(e, ast.Starred):
                value.extend(self.expression(e.value).value)
            else:
                value.append(self.expression(e).value)
        return Expression(value, expr=None)

    def tuple(self, expr: ast.Tuple) -> Expression:
        """
        Tuple expression, like: (1, 2, 3, 4, 5)
        """
        expression = self.list(expr)  # type: ignore[arg-type]
        expression.values = (tuple(expression.value),)
        return expression

    @property
    def _expression(self) -> dict:
        """Mapping from ast expression to parser functions"""
        mapping = {
            ast.Constant: self.constant,
            ast.Name: self.name,
            ast.Attribute: self.attribute,
            ast.Index: self.index,
            ast.Subscript: self.subscript,
            ast.BinOp: self.bin_op,
            ast.UnaryOp: self.unary_op,
            ast.BoolOp: self.bool_op,
            ast.Compare: self.compare,
            ast.Call: self.call,
            ast.FormattedValue: self.formatted_value,
            ast.JoinedStr: self.joined_str,
            ast.List: self.list,
            ast.Tuple: self.tuple,
            ast.Slice: self.slice,
        }
        deprecated = {
            'NameConstant': self.constant,
            'Str': self.string,
            'Num': self.number,
        }
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            for name, callback in deprecated.items():
                if hasattr(ast, name):
                    mapping[getattr(ast, name)] = callback
        return mapping

    def expression(self, expr: ast.expr) -> Expression:
        """Return parsed expression according to its type."""
        return self._expression[type(expr)](expr)


class CaseExpressionParser(ExpressionParser):
    """
    Class with methods to convert ast expressions to CASE function.
    """
    def __init__(self, fun: 'FunctionParser'):
        super().__init__(fun)
        self.operator_parser = CaseOperatorParser()  # type: ignore[assignment]

    def _convert_bool_op_to_if_expr(self, expr: ast.expr) -> ast.expr:
        """
        Special case to convert bool operation to if expression.
        For example:
            lambda row: row['A'] or -1
        will be converted to:
            CASE(A != 0, 1, A, -1)
        """
        if not isinstance(expr, ast.BoolOp):
            return expr

        def get_if_expr(first, second):
            if isinstance(expr.op, ast.Or):
                return ast.IfExp(test=first, body=first, orelse=second)
            else:
                return ast.IfExp(test=first, body=second, orelse=first)

        first = None
        for i in range(len(expr.values) - 1):
            if first is None:
                first = expr.values[i]
                first = self._convert_bool_op_to_if_expr(first)
            second = expr.values[i + 1]
            second = self._convert_bool_op_to_if_expr(second)
            first = get_if_expr(first, second)
        assert first is not None
        return first

    def if_expr(self, expr: ast.IfExp) -> Expression:
        """
        If expression: 'A' if tick['X'] > 0 else 'B'.
        Do not confuse with if statement.
        Will be converted to OneTick case function: CASE(X > 0, 1, 'A', 'B').
        If condition value can be deduced before execution of script,
        then if or else value will be returned without using CASE() function.
        For example:
            tick['A'] if False else 3  -----------> 3
        """
        test = self.expression(expr.test)
        if test.predefined:
            # we can remove unnecessary branch if condition value is already known
            if test.value:
                return self.expression(expr.body)
            return self.expression(expr.orelse)
        body = self.expression(expr.body)
        orelse = self.expression(expr.orelse)
        test.convert_to_operation()

        str_expr = f'CASE({test}, 1, {body}, {orelse})'
        value = _LambdaIfElse(str_expr, ott.get_type_by_objects([*body.values, *orelse.values]))
        return Expression(value, expr=str_expr)

    def call(self, expr: ast.Call) -> Expression:
        """
        For CASE() function we support using inner functions that return valid case expression.
        """
        func = self.expression(expr.func)

        need_to_parse = False
        if not isinstance(func.value, types.BuiltinMethodType):
            for node in expr.args + [kw.value for kw in expr.keywords]:
                if isinstance(node, ast.Name) and node.id == self.fun.arg_name:
                    # we will parse inner function call to OneTick expression
                    # only if one of the function call arguments is
                    # 'tick' or 'row' parameter of the original function
                    need_to_parse = True
                    break
        orig_err = None
        if not need_to_parse:
            with _CompareTrackScope(emulation_enabled=False):
                try:
                    return super().call(expr)
                except Exception as err:
                    orig_err = err
                    uw = UserWarning(
                        f"Function '{astunparse(expr)}' can't be called in python, "
                        "will try to parse it to OneTick expression. "
                        f"Use '{self.fun.arg_name}' in function's signature to indicate "
                        "that this function can be parsed to OneTick expression."
                    )
                    uw.__cause__ = err
                    warnings.warn(uw)

        fp = FunctionParser(func.value, check_arg_name=False)

        kwargs = {}
        args = fp.ast_node.args.args
        if fp.is_method:
            args = args[1:]
        for arg, default in zip(reversed(args), reversed(fp.ast_node.args.defaults)):
            kwargs[arg.arg] = default
        kwargs.update({keyword.arg: keyword.value for keyword in expr.keywords})  # type: ignore[misc]
        for arg, arg_value in zip(args, expr.args):
            kwargs[arg.arg] = arg_value

        try:
            value = fp.compress()
        except Exception as err:
            try:
                return super().call(expr)
            except Exception:
                if orig_err is not None:
                    raise err from orig_err
                raise ValueError(
                    f"Can't convert function '{astunparse(expr)}' to CASE() expression."
                ) from err
        try:
            with self._replace_context(fp.closure_vars):
                # replace function parameters with calculated values
                value = fp.case_statement_parser._replace_nodes(value, replace_name=kwargs)
                return self.expression(value)
        except Exception as err:
            if orig_err is not None:
                raise err from orig_err
            raise err

    @property
    def _expression(self) -> dict:
        return dict(super()._expression.items() | {
            ast.IfExp: self.if_expr,
        }.items())


class CaseStatementParser:
    """
    Class with methods to convert ast statements to CASE function.
    """
    def __init__(self, fun: 'FunctionParser'):
        self.fun = fun
        self.expression_parser = CaseExpressionParser(fun=fun)
        self.operator_parser = CaseOperatorParser()

    T = TypeVar('T', bound=ast.AST)

    @staticmethod
    def _replace_nodes(node: T,
                       replace_name: Optional[Dict[str, ast.expr]] = None,
                       replace_break: Union[ast.stmt, Exception, Type[Exception], None] = None,
                       inplace: bool = False) -> T:
        """
        Function to replace expressions and statements inside ast.For node.

        Parameters
        ----------
        node
            ast node in which expressions and statements will be replaced
        inplace
            if True `node` object will be modified else it will be copied and the copy will be returned
        replace_name
            mapping from ast.Name ids to ast expressions.
            ast.Name nodes with these ids will be replaced with corresponding expressions.
        replace_break
            replace break statement with another statement.
            We can't execute for loop on real data here so we can't allow break statements at all.
            So we will replace them with statements from code after the for loop.
            If replace_break is Exception then exception will be raised when visiting ast.Break nodes.
        """
        class RewriteName(ast.NodeTransformer):
            def visit_Name(self, n: ast.Name):
                return (replace_name or {}).get(n.id) or n

            def visit_Continue(self, n: ast.Continue):
                # TODO: pass is not continue, we must allow only bodies with one statement in this case
                return ast.Pass()

            def visit_Break(self, n: ast.Break):
                if replace_break is None:
                    return n
                if inspect.isclass(replace_break) and issubclass(replace_break, Exception):
                    raise replace_break("Break is found in for loop and replacer is not provided")
                if isinstance(replace_break, Exception):
                    raise replace_break
                assert isinstance(replace_break, ast.stmt)
                return CaseStatementParser._replace_nodes(replace_break, replace_name=replace_name)

        if not inplace:
            node = deepcopy(node)
        RewriteName().visit(node)
        return node

    def _flatten_for_stmt(self,
                          stmt: ast.For,
                          replace_break: Union[ast.stmt, Exception, Type[Exception], None] = None,
                          stmt_after_for: Optional[ast.stmt] = None) -> List[ast.stmt]:
        """
        Convert for statement to list of copy-pasted statements from the body for each iteration.
        """
        stmts = []
        target = stmt.target
        assert isinstance(target, (ast.Name, ast.Tuple)), (
            f"Unsupported expression '{astunparse(target)}' is used in for statement."
            " Please, use variable or tuple of variables instead."
        )
        if isinstance(target, ast.Tuple):
            for t in target.elts:
                assert isinstance(t, ast.Name)
            targets = target.elts
        else:
            targets = [target]
        replace_name = {}
        iter_ = self.expression_parser.expression(stmt.iter)
        for iter_value in iter_.value:
            if not isinstance(iter_value, Iterable) or isinstance(iter_value, str):
                iter_value = [iter_value]
            replace_name = {
                target.id: ast.Constant(value)  # type: ignore[attr-defined]
                for target, value in zip(targets, iter_value)
            }
            for s in stmt.body:
                stmts.append(
                    self._replace_nodes(s,
                                        replace_name=replace_name,  # type: ignore[arg-type]
                                        replace_break=replace_break)
                )
        if stmt_after_for and replace_name:
            stmts.append(self._replace_nodes(stmt_after_for, replace_name=replace_name))  # type: ignore[arg-type]
        return stmts

    def _flatten_for_stmts(self, stmts: List[ast.stmt]) -> List[Union[ast.If, ast.Return, ast.Pass]]:
        """
        Find ast.For statements in list of statements and flatten them.
        Return list of statements without ast.For.
        Additionally raise exception if unsupported statement is found.
        """
        # TODO: support ast.For statements on deeper levels
        res_stmts = []
        for i, stmt in enumerate(stmts):
            if not isinstance(stmt, (ast.If, ast.Return, ast.For, ast.Pass)):
                raise ValueError(
                    "this function can't be converted to CASE function, "
                    "only for, if, return and pass statements are allowed"
                )
            if isinstance(stmt, ast.For):
                try:
                    res_stmts.extend(self._flatten_for_stmt(stmt, replace_break=ValueError))
                except ValueError:
                    stmts_left = len(stmts[i + 1:])
                    assert stmts_left in (0, 1), "Can't be more than one statement after break"
                    replace_break: Union[ast.Return, ast.Pass]
                    if stmts_left == 0:
                        stmt_after_for = None
                        replace_break = ast.Pass()
                    else:
                        stmt_after_for = stmts[i + 1]
                        assert isinstance(stmt_after_for, (ast.Return, ast.Pass)), (
                            'Can only use pass and return statements after for loop with break'
                        )
                        replace_break = stmt_after_for
                    res_stmts.extend(self._flatten_for_stmt(stmt,
                                                            replace_break=replace_break,
                                                            stmt_after_for=stmt_after_for))
                    break
            else:
                res_stmts.append(stmt)
        return res_stmts  # type: ignore[return-value]

    def _compress_stmts_to_one_stmt(self, stmts: List[ast.stmt], filler=None) -> Union[ast.If, ast.Return]:
        """
        List of if statements will be converted to one if statement.
        For example:
            if tick['X'] <= 1:
                if tick['X'] > 0:
                    return 1
                else:
                    pass
            else:
                if tick['X'] < 3:
                    return 2
            if tick['X'] <= 3:
                return 3
            return 4
        will be converted to:
            if tick['X'] <= 1:
                if tick['X'] > 0:
                    return 1
                else:
                    if tick['X'] <= 3:
                        return 3
                    else:
                        return 4
            else:
                if tick['X'] < 3:
                    return 2
                else:
                    if tick['X'] <= 3:
                        return 3
                    else:
                        return 4
        """
        filler = filler or ast.Pass()
        if not stmts:
            return filler
        stmt, *others = stmts
        if isinstance(stmt, ast.Return):
            return stmt
        if isinstance(stmt, ast.Pass):
            return filler
        filler = self._compress_stmts_to_one_stmt(others, filler=filler)
        if isinstance(stmt, ast.If):
            stmt.body = [self._compress_stmts_to_one_stmt(stmt.body, filler=filler)]
            if stmt.orelse:
                stmt.orelse = [self._compress_stmts_to_one_stmt(stmt.orelse, filler=filler)]
            elif filler:
                stmt.orelse = [filler]
            assert stmt.orelse
            return stmt
        raise ValueError(
            "this function can't be converted to CASE function, "
            "only for, if, return and pass statements are allowed"
        )

    def _replace_local_variables(self, stmts: List[ast.stmt]) -> List[ast.stmt]:
        """
        We support local variables only by calculating their value and
        replacing all it's occurrences in the code after variable definition.
        For example:
            a = 12345
            if a:
                return a
            return 0
        will be converted to:
            if 12345:
                return 12345
            return 0
        """
        replace_name = {}
        res_stmts = []
        for stmt in stmts:
            if isinstance(stmt, ast.Assign):
                assert len(stmt.targets) == 1, 'Unpacking local variables is not supported yet'
                var, val = stmt.targets[0], stmt.value
                assert isinstance(var, ast.Name)
                replace_name[var.id] = val
                continue
            res_stmts.append(
                self._replace_nodes(stmt, replace_name=replace_name)
            )
        return res_stmts

    def if_stmt(self, stmt: ast.If) -> ast.IfExp:
        """
        Classic if statement with limited set of allowed statements in the body:
          * only one statement in the body
          * statement can be return or another if statement with same rules as above

        For example:
            if tick['X'] > 0:
                return 'POS'
            elif tick['X'] == 0:
                return 'ZERO'
            else:
                return 'NEG'
        will be converted to OneTick's CASE function:
            CASE(X > 0, 1, 'POS', CASE(X = 0, 1, 'ZERO', 'NEG'))
        """
        # TODO: support many statements in body
        if len(stmt.body) != 1:
            raise ValueError("this function can't be converted to CASE function, "
                             "too many statements in if body")
        body = self.statement(stmt.body[0])

        if len(stmt.orelse) > 1:
            raise ValueError("this function can't be converted to CASE function, "
                             "too many statements in else body")
        if stmt.orelse and not isinstance(stmt.orelse[0], ast.Pass):
            orelse = self.statement(stmt.orelse[0])
        else:
            e = self.expression_parser.expression(body)
            orelse = ast.Constant(ott.default_by_type(ott.get_type_by_objects(e.values)))
        return ast.IfExp(test=stmt.test, body=body, orelse=orelse)

    def return_stmt(self, stmt: ast.Return) -> ast.expr:
        """
        Return statement.
        Will be converted to value according to OneTick's syntax.
        """
        if stmt.value is None:
            raise ValueError('return statement must have value when converting to CASE function')
        return stmt.value

    def pass_stmt(self, _stmt: ast.Pass) -> ast.Constant:
        """
        Pass statement.
        Will be converted to None according to OneTick's syntax.
        """
        return ast.Constant(None)

    def compress(self, stmts: List[ast.stmt]) -> Union[ast.If, ast.Return]:
        """
        Compress list of statements to single statement.
        This is possible only if simple if and return statements are used.
        """
        stmts = self._replace_local_variables(stmts)
        flat_stmts = self._flatten_for_stmts(stmts)
        stmt = self._compress_stmts_to_one_stmt(flat_stmts)  # type: ignore[arg-type]
        return stmt

    def statement(self, stmt: ast.stmt) -> ast.expr:
        """Return statement converted to expression."""
        return {
            ast.If: self.if_stmt,
            ast.Return: self.return_stmt,
            ast.Pass: self.pass_stmt,
        }[type(stmt)](stmt)  # type: ignore[operator]


class StatementParser(CaseStatementParser):
    """
    Class with methods to convert ast statements to per tick script lines.
    """

    def __init__(self, fun: 'FunctionParser'):
        super().__init__(fun)
        self.expression_parser = ExpressionParser(fun=fun)  # type: ignore[assignment]
        self.operator_parser = OperatorParser()
        self._for_counter = 0

    @staticmethod
    def _transform_if_expr_to_if_stmt(stmt: Union[ast.Assign, ast.AugAssign]) -> ast.If:
        """
        Per tick script do not support if expressions, so converting it to if statement.
        For example:
            tick['X'] = 'A' if tick['S'] > 0 else 'B'
        will be converted to:
            if (S > 0) {
                X = 'A';
            }
            else {
                X = 'B';
            }
        """
        if not isinstance(stmt.value, ast.IfExp):
            raise ValueError()
        if_expr: ast.IfExp = stmt.value
        body, orelse = deepcopy(stmt), deepcopy(stmt)
        body.value = if_expr.body
        orelse.value = if_expr.orelse

        return ast.If(
            test=if_expr.test,
            body=[body],
            orelse=[orelse],
        )

    def assign(self, stmt: ast.Assign) -> str:
        """
        Assign statement: tick['X'] = 1
        Will be converted to OneTick syntax: X = 1;
        """
        assert len(stmt.targets) == 1, 'Unpacking variables is not yet supported'

        if isinstance(stmt.value, ast.IfExp):
            if_stmt = self._transform_if_expr_to_if_stmt(stmt)
            return self.statement(if_stmt)

        var = self.expression_parser.expression(stmt.targets[0])
        val = self.expression_parser.expression(stmt.value)

        default_expr = f'{var} = {val};'

        if var.lhs:
            expr = var.value(val.value)
            return expr or default_expr

        if var.is_local_variable:
            var_name = var.value.name
            if val.is_static:
                val = Expression(val.value.value)
                if var_name in self.fun.emulator.STATIC_VARS:
                    raise ValueError(f"Trying to define static variable '{var_name}' more than once")
                if var_name in self.fun.emulator.LOCAL_VARS:
                    raise ValueError(f"Can't redefine variable '{var_name}' as static")

                if self.fun.emulator.NEW_VALUES:
                    raise ValueError('Mixed definition of static variables and new columns is not supported')

                if val.is_tick:
                    # recreating tick object here, because it doesn't have name yet
                    self.fun.emulator.STATIC_VARS[var_name] = val.dtype(var_name)
                    return f'static {val.value._definition} {var};'
                self.fun.emulator.STATIC_VARS[var_name] = val.value
                return f'static {ott.type2str(val.dtype)} {var} = {val};'

            variables = None
            if var_name in self.fun.emulator.STATIC_VARS:
                variables = self.fun.emulator.STATIC_VARS
            elif var_name in self.fun.emulator.LOCAL_VARS:
                variables = self.fun.emulator.LOCAL_VARS

            if variables is None:
                if val.is_tick:
                    raise ValueError('Only primitive types are allowed for non static local variables.')
                if self.fun.emulator.NEW_VALUES:
                    raise ValueError('Mixed definition of local variables and new columns is not supported')
                self.fun.emulator.LOCAL_VARS[var_name] = val.value
                return f'{ott.type2str(val.dtype)} {var} = {val};'

            dtype = ott.get_type_by_objects([variables[var_name]])
            if val.dtype != dtype:
                raise ValueError(f"Wrong type for variable '{var_name}': should be {dtype}, got {val.dtype}")

        return default_expr

    def aug_assign(self, stmt: ast.AugAssign) -> str:
        """
        Assign with inplace operation statement: tick['X'] += 1.
        Will be converted to OneTick syntax: X = X + 1;
        """
        target = deepcopy(stmt.target)
        target.ctx = ast.Load()
        return self.assign(
            ast.Assign(
                targets=[stmt.target],
                value=ast.BinOp(
                    left=target,
                    op=stmt.op,
                    right=stmt.value,
                )
            )
        )

    def if_stmt(self, stmt: ast.If) -> str:  # type: ignore[override]
        """
        Classic if statement:
            if tick['X'] > 0:
                tick['Y'] = 1
            elif tick['X'] == 0:
                tick['Y'] = 0
            else:
                tick['Y'] = -1
        Will be converted to:
            if (X > 0) {
                Y = 1;
            }
            else {
                if (X == 0) {
                    Y = 0;
                }
                else {
                    Y = -1;
                }
            }
        """
        test = self.expression_parser.expression(stmt.test)
        test.convert_to_operation()
        body = [self.statement(s) for s in stmt.body]
        orelse = [self.statement(s) for s in stmt.orelse]
        if test.predefined:
            if test.value:
                return '\n'.join(body)
            return '\n'.join(orelse)
        lines = []
        lines.append('if (%s) {' % test)
        lines.extend(body)
        lines.append('}')
        if orelse:
            lines.append('else {')
            lines.extend(orelse)
            lines.append('}')
        return '\n'.join(lines)

    def return_stmt(self, stmt: ast.Return) -> str:  # type: ignore[override]
        """
        Return statement.
        For now we support returning only boolean values or nothing.
        Will be converted to: return true;
        """
        # if return is empty then it is not filter
        v = stmt.value if stmt.value is not None else ast.Constant(value=True)
        value = self.expression_parser.expression(v)
        dtype = ott.get_object_type(value.value)
        if not self.fun.inner_function:
            if dtype is not bool:
                raise TypeError(f"Not supported return type {dtype}")
            if stmt.value is not None:
                self.fun.returns = True
        else:
            assert isinstance(self.fun.ast_node, ast.FunctionDef)
            msg = (f"Function '{self.fun.ast_node.name}'"
                   f" has return annotation '{self.fun.return_annotation.__name__}',"
                   f" but the type of statement ({astunparse(stmt)}) is '{dtype.__name__}'")
            try:
                widest_type = ott.get_type_by_objects([dtype, self.fun.return_annotation])
            except TypeError as e:
                raise TypeError(msg) from e
            if widest_type is not self.fun.return_annotation:
                raise TypeError(msg)
            self.fun.returns = True
        return f'return {value};'

    @staticmethod
    def _check_break(*nodes) -> bool:
        """
        Check if break statement in the list of nodes (recursively).
        """
        class FoundBreakException(Exception):
            pass

        class FindBreak(ast.NodeVisitor):
            def visit_Break(self, n: ast.Break):
                raise FoundBreakException()

        try:
            for node in nodes:
                FindBreak().visit(node)
            return False
        except FoundBreakException:
            return True

    def while_stmt(self, stmt: ast.While) -> str:
        """
        Classic while statement:
            while tick['X'] > 0:
                tick['Y'] = 1
        Will be converted to:
            while (X > 0) {
                Y = 1;
            }
        """
        test = self.expression_parser.expression(stmt.test)
        test.convert_to_operation()
        body = [self.statement(s) for s in stmt.body]
        if test.predefined:
            is_break_found = self._check_break(*stmt.body)
            if not is_break_found:
                raise ValueError(f'The condition of while statement always evaluates to {bool(test.value)}'
                                 ' and there is no break statement in the loop body.'
                                 ' That will result in infinite loop.'
                                 ' Change condition or add break statements.')
        lines = []
        lines.append('while (%s) {' % test)
        lines.extend(body)
        lines.append('}')
        return '\n'.join(lines)

    def for_stmt(self, stmt: ast.For) -> str:
        """
        For now for statement in most cases will not be converted to per tick script's for statement.
        Instead, the statements from the body of the for statement will be duplicated
        for each iteration.

        For example:
            for i in [1, 2, 3]:
                tick['X'] += i
        will be converted to:
            X += 1;
            X += 2;
            X += 3;

        But simple case with range object will be translated more correctly:
        For example:
            for i in range(1, 4):
                tick['X'] += i
        will be converted to:
            for (LOCAL::i = 1; LOCAL::i < 4; LOCAL::i += 1) {
                X += LOCAL::i;
            }
        """
        lines = []
        iter_ = self.expression_parser.expression(stmt.iter)
        if isinstance(iter_.value, _TickSequence):
            target = stmt.target
            assert isinstance(target, ast.Name), "Tuples can't be used while iterating on tick sequences"
            state_tick = iter_.value._tick_obj(target.id)
            # TODO: ugly
            state_tick_name = f"_______state_tick_{self._for_counter}_______"
            self._for_counter += 1
            ast_tick = ast.Name(state_tick_name, ctx=ast.Load())
            lines.append('for (%s %s : %s) {' % (state_tick._definition, state_tick, iter_.value))
            with self.expression_parser._replace_context(
                inspect.ClosureVars({}, {state_tick_name: state_tick}, {}, set())
            ):
                for s in stmt.body:
                    s = self._replace_nodes(s, replace_name={target.id: ast_tick})
                    lines.append(self.statement(s))
            lines.append('}')
        elif isinstance(iter_.value, range):
            target = stmt.target
            assert isinstance(target, ast.Name), "Tuples can't be used in for loop"
            var_name = target.id
            if var_name not in self.fun.emulator.LOCAL_VARS:
                # initialize counter variable
                self.fun.emulator.LOCAL_VARS[var_name] = int
                self.fun.emulator.LOCAL_VARS_NEW_VALUES[var_name].append(0)
            elif self.fun.emulator.LOCAL_VARS[var_name] is not int:
                raise ValueError(f'Variable {var_name} was declared before with conflicting type.')
            counter_var = LocalVariable(var_name, int)
            counter_var_str = str(counter_var)
            range_obj = iter_.value
            start_expr = f'{counter_var_str} = {range_obj.start}'
            if (range_obj.start < range_obj.stop and range_obj.step <= 0 or
                    range_obj.start > range_obj.stop and range_obj.step >= 0):
                raise ValueError(f'Range object {range_obj} will result in infinite loop')
            if range_obj.start < range_obj.stop:
                condition_expr = f'{counter_var_str} < {range_obj.stop}'
            else:
                condition_expr = f'{counter_var_str} > {range_obj.stop}'
            increment_expr = f'{counter_var_str} += {range_obj.step}'
            lines.append('for (%s; %s; %s) {' % (start_expr, condition_expr, increment_expr))
            try:
                for s in stmt.body:
                    lines.append(self.statement(s))
                lines.append('}')
            except Exception:
                if var_name in self.fun.emulator.LOCAL_VARS_NEW_VALUES:
                    self.fun.emulator.LOCAL_VARS_NEW_VALUES.pop(var_name)
                    self.fun.emulator.LOCAL_VARS.pop(var_name)
                lines = [self.statement(s) for s in self._flatten_for_stmt(stmt)]
        else:
            lines = [self.statement(s) for s in self._flatten_for_stmt(stmt)]
        return '\n'.join(lines)

    def break_stmt(self, _stmt: ast.Break) -> str:
        return 'break;'

    def continue_stmt(self, _stmt: ast.Continue) -> str:
        return 'continue;'

    def pass_stmt(self, stmt: ast.Pass) -> str:  # type: ignore[override]
        """Pass statement is not converted to anything"""
        return ''

    def yield_expr(self, expr: ast.Yield) -> Expression:
        """
        Yield expression, like: yield
        Values for yield are not supported.
        Will be translated to PROPAGATE_TICK() function.
        Can be used only as a statement, so this function is here and not in ExpressionParser.
        """
        if expr.value is not None:
            raise ValueError("Passing value with yield expression is not supported.")
        return Expression('PROPAGATE_TICK();')

    def expression(self, stmt: ast.Expr) -> str:
        """
        Here goes raw strings and yield expression.
        For example:
            if tick['A'] == 0:
                'return 0;'
        Here 'return 0;' is used as a statement and an expression.
        Expression's returned value *must* be a string and
        this string will be injected in per tick script directly.
        """
        if isinstance(stmt.value, ast.Yield):
            expression = self.yield_expr(stmt.value)
        else:
            expression = self.expression_parser.expression(stmt.value)
        assert isinstance(expression.value, (str, _Operation)), (
            f"The statement '{astunparse(stmt)}' can't be used here"
            " because the value of such statement can be string only"
            " as it's value will be injected directly in per tick script."
        )
        value = str(expression.value)
        if value and value[-1] != ';':
            value += ';'
        return value

    def with_stmt(self, stmt: ast.With) -> str:
        """
        Used only with special context managers. Currently only `_ONCE` is supported.
        """
        if len(stmt.items) != 1:
            raise ValueError('Currently it is possible to use only one context manager in single with statement')
        with_item = stmt.items[0]
        if with_item.optional_vars:
            raise ValueError('It is not allowed to use "as" in with statements for per-tick script')
        context_expr = with_item.context_expr
        if isinstance(context_expr, ast.Call):
            expr = self.expression_parser.expression(context_expr.func)
        else:
            raise ValueError(f'{context_expr} is not called')
        if not issubclass(expr.value, once):
            raise ValueError(f'{expr.value} is not supported in per-tick script with statements')
        return expr.value().get_str('\n'.join([self.statement(s) for s in stmt.body]))

    def statement(self, stmt: ast.stmt) -> str:  # type: ignore[override]
        """Return parsed statement according to its type."""
        return {
            ast.Assign: self.assign,
            ast.AugAssign: self.aug_assign,
            ast.If: self.if_stmt,
            ast.Return: self.return_stmt,
            ast.While: self.while_stmt,
            ast.For: self.for_stmt,
            ast.Break: self.break_stmt,
            ast.Continue: self.continue_stmt,
            ast.Pass: self.pass_stmt,
            ast.Expr: self.expression,
            ast.With: self.with_stmt,
        }[type(stmt)](stmt)  # type: ignore[operator]


class EndOfBlock(Exception):
    pass


class LambdaBlockFinder:
    """
    This is simplified version of
    inspect.BlockFinder
    that supports multiline lambdas.
    """

    OPENING_BRACKETS = {
        '[': ']',
        '(': ')',
        '{': '}',
    }
    CLOSING_BRACKETS = {c: o for o, c in OPENING_BRACKETS.items()}
    BRACKETS_MATCHING = dict(OPENING_BRACKETS.items() | CLOSING_BRACKETS.items())

    def __init__(self):
        # current indentation level
        self.indent = 0
        # row and column index for the start of lambda expression
        self.start = None
        # row and column index for the end of lambda expression
        self.end = None
        # stack with brackets
        self.brackets = deque()
        self.prev = None
        self.current = None

    def tokeneater(self, type, token, srowcol, erowcol, line, start_row=0):
        srowcol = (srowcol[0] + start_row, srowcol[1])
        erowcol = (erowcol[0] + start_row, erowcol[1])
        self.prev = self.current
        self.current = tokenize.TokenInfo(type, token, srowcol, erowcol, line)
        self.end = erowcol
        if token == 'lambda':
            self.start = srowcol
        elif type == tokenize.INDENT:
            self.indent += 1
        elif type == tokenize.DEDENT:
            self.indent -= 1
            # the end of matching indent/dedent pairs ends a block
            if self.indent <= 0:
                raise EndOfBlock
        elif not self.start:
            self.indent = 0
        elif type == tokenize.NEWLINE:
            if self.indent == 0 or (
                # if lambda is the argument of the function
                self.prev and self.prev.type == tokenize.OP and self.prev.string == ','
            ):
                raise EndOfBlock
        elif token in self.OPENING_BRACKETS:
            self.brackets.append(token)
        elif token in self.CLOSING_BRACKETS:
            try:
                assert self.brackets.pop() == self.CLOSING_BRACKETS[token]
            except (IndexError, AssertionError):
                self.end = self.prev.end
                raise EndOfBlock  # noqa: W0707


def get_lambda_source(lines):
    """Extract the block of lambda code at the top of the given list of lines."""
    blockfinder = LambdaBlockFinder()
    start_row = 0
    while True:
        try:
            tokens = tokenize.generate_tokens(iter(lines[start_row:]).__next__)
            for _token in tokens:
                blockfinder.tokeneater(*_token, start_row=start_row)
            break
        except IndentationError as e:
            # indentation errors are possible because
            # we started eating tokens from line with lambda
            # not from the start of the statement
            # trying to eat again from the current row in this case
            start_row = e.args[1][1] - 1
            continue
        except EndOfBlock:
            break
    start_row, start_column = blockfinder.start
    end_row, end_column = blockfinder.end
    # crop block to get rid of tokens from the context around lambda
    lines = lines[start_row - 1: end_row]
    lines[-1] = lines[-1][:end_column]
    lines[0] = lines[0][start_column:]
    # add brackets around lambda in case it is multiline lambda
    return ''.join(['(', *lines, ')'])


def is_lambda(lambda_f) -> bool:
    return isinstance(lambda_f, types.LambdaType) and lambda_f.__name__ == '<lambda>'


def get_source(lambda_f) -> str:
    """
    Get source code of the function or lambda.
    """
    if is_lambda(lambda_f):
        # that's a hack for multiline lambdas in brackets
        # inspect.getsource parse them wrong
        source_lines, lineno = inspect.findsource(lambda_f)
        if 'lambda' not in source_lines[lineno]:
            # inspect.findsource fails sometimes too
            lineno = lambda_f.__code__.co_firstlineno + 1
            while 'lambda' not in source_lines[lineno]:
                lineno -= 1
        source = get_lambda_source(source_lines[lineno:])
    else:
        source = inspect.getsource(lambda_f)
    # doing dedent because self.ast_node do not like indented source code
    return textwrap.dedent(source)


class FunctionParser:
    """
    Class to parse callable objects (lambdas and functions) to
    OneTick's per tick script or case functions.
    Only simple functions corresponding to OneTick syntax supported
    (without inner functions, importing modules, etc.)
    You can call simple functions inside,
    do operations with captured variables (without assigning to them),
    but using non-pure functions is not recommended because
    the code in function may not be executed in the order you expect.
    """
    SOURCE_CODE_ATTRIBUTE = '___SOURCE_CODE___'

    def __init__(self, lambda_f, emulator=None, check_arg_name=True, inner_function=False):
        """
        Parameters
        ----------
        emulator
            otp.Source emulator that will be tracking changes made to source
        check_arg_name
            if True, only callables with zero or one parameter will be allowed.
        inner_function
            if True, then function is treated like inner per-tick script function.
            First argument will be checked and more arguments will be allowed too.
        """

        assert isinstance(lambda_f, (types.LambdaType, types.FunctionType, types.MethodType)), (
            f"It is expected to get a function, method or lambda, but got '{type(lambda_f)}'"
        )
        self.lambda_f = lambda_f
        self.emulator = emulator
        self.check_arg_name = check_arg_name
        self.inner_function = inner_function
        if self.inner_function and not self.check_arg_name:
            self.check_arg_name = self.check_arg_name or True
        self.statement_parser = StatementParser(fun=self)
        self.expression_parser = ExpressionParser(fun=self)
        self.case_expression_parser = CaseExpressionParser(fun=self)
        self.case_statement_parser = CaseStatementParser(fun=self)
        # if the function returns some values or not
        self.returns = False
        # will be set to True when args_annotations will be calculated, need it to break recursion
        self._from_args_annotations = False
        # calling property here, so we can raise exception as early as possible
        _ = self.arg_name

    @cached_property
    def is_method(self) -> bool:
        return isinstance(self.lambda_f, types.MethodType)

    @cached_property
    def source_code(self) -> str:
        """
        Get source code of the function or lambda.
        """
        # first try to get code from special attribute else get code the usual way
        return getattr(self.lambda_f, self.SOURCE_CODE_ATTRIBUTE, None) or get_source(self.lambda_f)

    @cached_property
    def closure_vars(self) -> inspect.ClosureVars:
        """
        Get closure variables of the function.
        These are variables that were captured from the context before function definition.
        For example:
            A = 12345
            def a():
                print(A + 1)
        In this function variable A is the captured variable.
        We need closure variables, so we can use them when parsing ast tree.
        """
        return inspect.getclosurevars(self.lambda_f)

    @cached_property
    def ast_node(self) -> Union[ast.FunctionDef, ast.Lambda]:
        """
        Convert function or lambda to ast module statement.
        """
        source_code = self.source_code
        tree = ast.parse(source_code)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.Lambda)):
                if isinstance(node, ast.FunctionDef) and ast.get_docstring(node):
                    # remove comment section from function body
                    node.body.pop(0)
                return node
        raise ValueError("Can't find function or lambda in source code")

    @cached_property
    def arg_name(self) -> Optional[str]:
        """Get name of the first function or lambda argument."""
        node = self.ast_node
        argv = list(node.args.args)
        argc = len(argv)
        if argc > 1 and argv[0].arg == 'self' and self.is_method:
            argv.pop(0)
            argc -= 1
        if self.check_arg_name and argc > 1 and not self.inner_function:
            raise ValueError(
                "It is allowed to pass only functions or lambdas that take either one or"
                f" zero parameters, but got {argc}"
            )
        arg_name = argv[0].arg if argv else None
        if isinstance(self.check_arg_name, str) and arg_name != self.check_arg_name:
            assert isinstance(node, ast.FunctionDef)
            msg = f"Function '{node.name}' is expected to have first argument named '{self.check_arg_name}'"
            if arg_name is None:
                msg += ', but no argument is found'
            else:
                msg += f", but argument with name '{arg_name}' is found"
            raise ValueError(msg)
        return arg_name

    @cached_property
    def args_annotations(self) -> dict:
        node = self.ast_node
        if not isinstance(node, ast.FunctionDef):
            return {}
        argv = list(node.args.args)
        if argv and argv[0].arg == 'self' and self.is_method:
            argv.pop(0)
        if argv and argv[0].arg == self.arg_name:
            argv.pop(0)
        if node.args.defaults:
            raise ValueError("Default values for arguments are not supported"
                             f" in per-tick script function '{node.name}'")
        annotations = {}
        for arg in argv:
            name = arg.arg
            annotation = getattr(arg, 'annotation', None)
            if not annotation:
                raise ValueError(f"Parameter '{name}' in function '{node.name}' doesn't have type annotation")
            # TODO: remove hacking
            self._from_args_annotations = True
            dtype = self.expression_parser.expression(annotation).value
            self._from_args_annotations = False
            if not ott.is_type_basic(dtype):
                raise ValueError(f"Parameter '{name}' in function '{node.name}' has unsupported type: {dtype}")
            annotations[name] = dtype

        return annotations

    @cached_property
    def return_annotation(self):
        node = self.ast_node
        annotation = getattr(node, 'returns')
        if not annotation:
            raise ValueError(f"Function '{node.name}' doesn't have return type annotation")
        dtype = self.expression_parser.expression(annotation).value
        if not ott.is_type_basic(dtype):
            raise ValueError(f"Function '{node.name}' has unsupported return type: {dtype}")
        return dtype

    def per_tick_script(self) -> str:
        """
        Convert function to OneTick's per tick script.
        """
        node = self.ast_node

        lines = []

        assert isinstance(node, ast.FunctionDef), 'lambdas are not supported in per-tick-script yet'
        function_def: ast.FunctionDef = node

        for stmt in function_def.body:
            line = self.statement_parser.statement(stmt)
            if line:
                lines.append(line)

        if not self.inner_function and self.returns:
            # if there were return statement anywhere in the code
            # then we add default return at the end
            # TODO: but the default behaviour in OneTick is to propagate all ticks?
            #       changing that will break backward-compatibility
            lines.append(self.statement_parser.statement(ast.Return(ast.Constant(False))))

        if self.emulator is not None and not self.inner_function:
            # per tick script syntax demand that we declare variables before using them
            # so we get all new variables from emulator and declare them.

            def var_definition(key, values):
                dtype = ott.get_type_by_objects(values)
                return f'{ott.type2str(dtype)} {str(key)} = {ott.value2str(ott.default_by_type(dtype))};'

            new_columns = []
            for key, values in self.emulator.NEW_VALUES.items():
                new_columns.append(var_definition(key, values))
            new_local_vars = []
            for key, values in self.emulator.LOCAL_VARS_NEW_VALUES.items():
                new_local_vars.append(var_definition(LocalVariable(key), values))
            lines = new_columns + new_local_vars + lines

        if not lines:
            raise ValueError("The resulted body of PER TICK SCRIPT is empty")

        if not self.inner_function:
            lines = ['long main() {'] + lines + ['}']
            for function, *_ in self.emulator.FUNCTIONS.values():
                lines.append(function)
        else:
            if not self.returns:
                raise ValueError(f"Function '{node.name}' must return values")
            return_type = ott.type2str(self.return_annotation)
            args = [
                f'{ott.type2str(dtype)} {name}'
                for name, dtype in self.args_annotations.items()
            ]
            args: str = ', '.join(args)  # type: ignore[no-redef]
            lines = [f'{return_type} {node.name}({args})' + ' {'] + lines + ['}']

        return '\n'.join(lines) + '\n'

    def compress(self) -> ast.expr:
        """
        Convert lambda or function to AST expression.
        """
        node = self.ast_node
        if isinstance(node, ast.Lambda):
            return node.body
        stmt = self.case_statement_parser.compress(node.body)
        return self.case_statement_parser.statement(stmt)

    def case(self) -> Tuple[str, List[Any]]:
        """
        Convert lambda or function to OneTick's CASE() function.
        """
        expr = self.compress()
        expr = self.case_expression_parser._convert_bool_op_to_if_expr(expr)
        expression = self.case_expression_parser.expression(expr)
        # this will raise type error if type of the expression is not supported
        ott.default_by_type(ott.get_type_by_objects(expression.values))
        return str(expression), expression.values


def remote(fun):
    """
    This decorator is needed in case function ``fun``
    is used in :py:meth:`~onetick.py.Source.apply` method in a `Remote OTP with Ray` context.

    We want to get source code of the function locally
    because we will not be able to get source code on the remote server.

    See also
    --------
    :ref:`Remote OTP with Ray <ray-remote>`.
    """
    # see PY-424
    @wraps(fun)
    def wrapper(*args, **kwargs):
        return fun(*args, **kwargs)
    setattr(wrapper, FunctionParser.SOURCE_CODE_ATTRIBUTE, get_source(fun))
    return wrapper


class once:
    """
    Used with a statement or a code block to make it run only once (the first time control reaches to the statement).
    """
    def __enter__(self):
        # __enter__ and __exit__ methods are only used to express syntax for per-tick script, thus no implementation
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        # __enter__ and __exit__ methods are only used to express syntax for per-tick script, thus no implementation
        pass

    def get_str(self, string: str) -> str:
        return f"_ONCE\n{{\n{string}\n}}"


class Once(once):
    def __init__(self):
        warnings.warn('Using `otp.Once` is deprecated, please, use `otp.once` instead', FutureWarning)
        super().__init__()


def logf(message, severity, *args) -> str:
    """
    Call built-in OneTick ``LOGF`` function from per-tick script.

    Parameters
    ----------
    message: str
        Log message/format string. The underlying formatting engine is the Boost Format Library:
        https://www.boost.org/doc/libs/1_53_0/libs/format/doc/format.html

    severity: str
        Severity of message. Supported values: ``ERROR``, ``WARNING`` and ``INFO``.

    args: list
        Parameters for format string (optional).

    Returns
    -------
    str

    Examples
    --------
    >>> t = otp.Ticks({'X': [1, 2, 3]})

    >>> def test_script(tick):
    ...     otp.logf("Tick with value X=%1% processed", "INFO", tick["X"])

    >>> t = t.script(test_script)

    See also
    --------
    :ref:`Per-Tick Script Guide <python callable parser>`
    """

    if severity not in {"ERROR", "WARNING", "INFO"}:
        raise ValueError(f"Param severity expected to be one of ERROR, WARNING or INFO. Got \"{severity}\"")

    message = ott.value2str(message)
    severity = ott.value2str(severity)

    if args:
        params = ", ".join([ott.value2str(arg) for arg in args])
        return f"LOGF({message}, {severity}, {params});"
    else:
        return f"LOGF({message}, {severity});"


def throw_exception(message: str) -> str:
    """
    Call built-in OneTick ``THROW_EXCEPTION`` function from per-tick script.

    Parameters
    ----------
    message: str
        Message string that defines the error message to be thrown.

    Returns
    -------
    str

    Examples
    --------
    >>> t = otp.Ticks({'X': [1, -2, 6]})

    >>> def test_script(tick):
    ...     if tick["X"] <= 0:
    ...         otp.throw_exception("Tick column X should be greater than zero.")

    >>> t = t.script(test_script)

    See also
    --------
    :ref:`Per-Tick Script Guide <python callable parser>`
    """
    message = ott.value2str(message)
    return f"THROW_EXCEPTION({message});"
