# pylama:ignore=E731,E131,R1727,C3001,W0612,E501

import ast
import os
import time
import re

import pytest
import pandas as pd
import numpy as np

import onetick.py as otp
from onetick.py.core.per_tick_script import FunctionParser
from onetick.py.core.lambda_object import _CompareTrackScope


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    pass


def zero_indent_function():
    return 2 ** 3


class TestFunctionAstParse:
    def test_zero_indent_function(self):
        assert FunctionParser(zero_indent_function).source_code.rstrip() == """
def zero_indent_function():
    return 2 ** 3
        """.strip()

    def test_indent_function(self):
        def indent_function():
            for i in range(1, 2):
                yield i

        assert FunctionParser(indent_function).source_code.rstrip() == """
def indent_function():
    for i in range(1, 2):
        yield i
        """.strip()

    def test_in_place_lambda(self):
        assert FunctionParser(lambda x: x ** x).source_code.rstrip() == '(lambda x: x ** x)'

    def test_lambda_from_variable(self):
        la = lambda: 2 ** 10
        assert FunctionParser(la).source_code.rstrip() == '(lambda: 2 ** 10\n)'

    def test_ast_parse(self):
        def a(tick):
            tick['X'] = 1
        assert isinstance(FunctionParser(a).ast_node, ast.FunctionDef)
        assert isinstance(FunctionParser(lambda x: x + 1).ast_node, ast.Lambda)


class TestMultilineLambda:

    def test_0(self):
        la = lambda row: 1 if True else 2
        assert FunctionParser(la).source_code.strip() == r"""
(lambda row: 1 if True else 2
)""".strip()
        assert isinstance(FunctionParser(la).ast_node, ast.Lambda)

    def test_1(self):
        la = (
            lambda row: 1
            if True
            else 2
        )
        assert FunctionParser(la).source_code.strip() == """
(lambda row: 1
            if True
            else 2
        )
""".strip()
        assert isinstance(FunctionParser(la).ast_node, ast.Lambda)

    def test_2(self):
        la = (

            lambda

            row:

            1

            if

            True

            else

            2
        )
        assert FunctionParser(la).source_code.strip() == """
(lambda

            row:

            1

            if

            True

            else

            2
        )
""".strip()
        assert isinstance(FunctionParser(la).ast_node, ast.Lambda)

    def test_3(self):
        la = lambda row: 1 \
        \
        \
        \
            if True\
            else \
            2
        assert FunctionParser(la).source_code.strip() == r"""
(lambda row: 1 \
        \
        \
        \
            if True\
            else \
            2
)
""".strip()
        assert isinstance(FunctionParser(la).ast_node, ast.Lambda)

    def test_4(self):
        fp = FunctionParser(
            # it is important for this comment and lambda to be in this order
            lambda tick: 12345 if True else 2)
        assert isinstance(fp.ast_node, ast.Lambda)

    def test_5(self):
        def case(_, func):
            return str(FunctionParser(func).case()[0])

        # this code formatting is required for test
        assert case(None,
                    lambda tick: 12345 if True else 2) == '12345'

    def test_6(self):
        data = otp.Tick(A=1)
        data[
            'X'
        ] = data.apply(lambda row: row['A'])
        df = otp.run(data)
        assert df['X'][0] == 1

    def test_7(self):
        data = otp.Tick(A=1)
        data[
            'X'
        ] = data.apply(
            lambda row: 0
            if False
            else 1
        )
        df = otp.run(data)
        assert df['X'][0] == 1

    def test_8(self):
        data = otp.Tick(A=1)
        data = data.update(
            if_set={
                'A': data['A'].apply(
                    lambda x:
                    x + 1
                )
            }
        )
        df = otp.run(data)
        assert df['A'][0] == 2

    def test_9(self):
        data = otp.Tick(A=1)
        data['X'] = data.apply(
            lambda row: 0 if (False
                              and False)
                              and False
                        else 1)
        df = otp.run(data)
        assert df['X'][0] == 1


def script_text(src, func, script=True):
    from onetick.py.core.lambda_object import apply_lambda, apply_script, _EmulateObject
    if script:
        return apply_script(func, _EmulateObject(src))[1]
    else:
        return str(apply_lambda(func, _EmulateObject(src)))


class TestCommon:

    @pytest.fixture
    def data(self):
        import onetick.py as otp
        return otp.Tick(A=1, B='2B')

    def test_not_callable_exception(self, data):
        with pytest.raises(ValueError) as err:
            script_text(data, 'not_callable_object')
        assert str(err.value) == (
            "It is expected to get a function, method or lambda, "
            "but got 'not_callable_object' of type '<class 'str'>'"
        )

    def test_return_value_not_bool(self, data):
        def func(tick):
            return 'not_bool'

        with pytest.raises(TypeError) as err:
            script_text(data, func)
        assert str(err.value) == "Not supported return type <class 'str'>"

    def test_default_values(self, data):
        def func(tick):
            tick['X'] = 1
        assert script_text(data, func).strip() == """
long main() {
long X = 0;
X = 1;
}
""".strip()

    def test_string_value(self, data):
        def func(tick):
            tick['X'] = 'aaa'

        data = data.script(func)
        assert 'X' in data.schema
        df = otp.run(data)
        assert df['X'][0] == 'aaa'

    @pytest.mark.skip('TODO')
    def test_schema_change_type(self, data):
        def func(tick):
            tick['B'] = 1

        with pytest.raises(TypeError):
            data.script(func)

    def test_schema(self, data):
        def func(tick):
            tick['X'] = 1

        data = data.script(func)
        assert 'X' in data.schema

    def test_referenced_before_assignment(self, data):
        def func(tick):
            tick['X'] += 1
        with pytest.raises(NameError) as err:
            script_text(data, func)
        assert str(err.value) == "Column 'X' referenced before assignment"

    def test_if_expr_to_if_stmt(self, data):
        def func(tick):
            tick['X'] = 1 if tick['A'] else 2
        assert script_text(data, func).strip() == """
long main() {
long X = 0;
if ((A) != (0)) {
X = 1;
}
else {
X = 2;
}
}
        """.strip()

    def test_twice(self, data):
        def func_1(tick):
            tick['X'] = 1

        def func_2(tick):
            tick['Y'] = 1

        assert script_text(data, func_1).strip() == """
long main() {
long X = 0;
X = 1;
}
""".strip()
        assert script_text(data, func_2).strip() == """
long main() {
long Y = 0;
Y = 1;
}
""".strip()

    def test_if_expr_in_script(self, data):
        def func(tick):
            tick['X'] = 10 if tick['A'] == 1 else tick['A']

        data = data.script(func)
        df = otp.run(data)
        assert df['X'][0] == 10

    def test_if_bool(self, data):
        def func(tick):
            if True:
                tick['X'] = 1
            else:
                tick['X'] = 2

        data = data.script(func)
        df = otp.run(data)
        assert df['X'][0] == 1

    def test_conflicting_types(self, data):
        def func(tick):
            if tick['A'] == 1:
                tick['X'] = '10'
            else:
                tick['X'] = 10

        with pytest.raises(TypeError):
            data.script(func)

    def test_conflicting_types_2(self, data):
        def func(tick):
            tick['X'] = '10'
            tick['X'] = 10

        with pytest.raises(TypeError):
            data.script(func)

    def test_bool_op(self):
        def func(tick):
            if 10 < tick['A'] and tick['A'] < 100:
                tick['B'] = 1
            elif tick['A'] or tick['A']:
                tick['B'] = 2
            else:
                tick['B'] = 3

        data = otp.Tick(A=1)
        df = otp.run(data.script(func))
        assert df['A'][0] == 1
        assert df['B'][0] == 2
        assert script_text(data, func).strip() == """
long main() {
long B = 0;
if (((A) > (10)) AND ((A) < (100))) {
B = 1;
}
else {
if (((A) != (0)) OR ((A) != (0))) {
B = 2;
}
else {
B = 3;
}
}
}
""".strip()

    def test_bin_op(self):
        def func(tick):
            if tick['A'] + tick['B'] == 1 - 2:
                tick['C'] = tick['A'] + tick['B']
            elif tick['A'] + 1 > 2:
                tick['C'] = 1
            elif 10 < tick['A'] < 100:
                tick['C'] = 2
            elif tick['A'] > 2:
                tick['C'] = 3
            else:
                tick['C'] = (tick['A'] + 1) - (tick['B'] + -2 - 3)

        data = otp.Tick(A=1, B=2)
        df = otp.run(data.script(func))
        assert df['A'][0] == 1
        assert df['B'][0] == 2
        assert df['C'][0] == 5
        assert script_text(data, func).strip() == """
long main() {
long C = 0;
if (((A) + (B)) = (-1)) {
C = (A) + (B);
}
else {
if (((A) + (1)) > (2)) {
C = 1;
}
else {
if (((A) > (10)) AND ((A) < (100))) {
C = 2;
}
else {
if ((A) > (2)) {
C = 3;
}
else {
C = ((A) + (1)) - (((B) + (-2)) - (3));
}
}
}
}
}

""".strip()

    def test_attribute(self):
        class A:
            a = 123

        class C:
            b = A()

        def join_kwargs(**kwargs):
            return ', '.join(f'{k}={v}' for k, v in kwargs.items())

        def func(tick):
            tick['X'] = C.b.a
            tick['Y'] = otp.nsectime(0)
            tick['Z'] = join_kwargs(a=1, b=2)

        data = otp.Tick(A=1)
        df = otp.run(data.script(func))
        assert df['X'][0] == 123
        assert df['Z'][0] == 'a=1, b=2'
        assert script_text(otp.Tick(A=1), func).strip() == """
long main() {
long X = 0;
nsectime Y = NSECTIME(0);
string Z = "";
X = 123;
Y = NSECTIME(0);
Z = "a=1, b=2";
}
""".strip()

    def test_if_comparators(self):
        def func(tick):
            if 0 < tick['A'] < 2:
                tick['B'] = 1
            if 0 < tick['A'] and tick['A'] < 2:
                tick['B'] = 2
            if tick['A']:
                tick['B'] = 3

        data = otp.Tick(A=1)
        df = otp.run(data.script(func))
        assert df['B'][0] == 3
        assert script_text(data, func).strip() == """
long main() {
long B = 0;
if (((A) > (0)) AND ((A) < (2))) {
B = 1;
}
if (((A) > (0)) AND ((A) < (2))) {
B = 2;
}
if ((A) != (0)) {
B = 3;
}
}
""".strip()

    def test_parenthesis_are_not_allowed_in_string(self):
        def func(tick):
            tick['B'] = tick['A'].apply(str) + 'abc'
            tick['C'] = 1 + 2
        data = otp.Tick(A=1)
        df = otp.run(data.script(func))
        assert df['B'][0] == '1abc'
        assert df['C'][0] == 3
        assert script_text(data, func).strip() == """
long main() {
string B = "";
long C = 0;
B = tostring(A) + "abc";
C = 3;
}
""".strip()

    def test_for_loop(self):
        def func(tick):
            tick['X'] = 0
            for i in [0, 1, 2]:
                tick['X'] += (i + 1)

        data = otp.Tick(A=1)
        df = otp.run(data.script(func))
        assert df['X'][0] == 6
        assert script_text(data, func).strip() == """
long main() {
long X = 0;
X = 0;
X = (X) + (1);
X = (X) + (2);
X = (X) + (3);
}
""".strip()

    def test_while_loop(self):
        x = 1

        def no_fun(tick):
            while x > 0:
                tick['A'] += 1

        def func(tick):
            tick['X'] = 0
            while True and tick['X'] < 5:  # noqa
                tick['X'] += 1

        data = otp.Tick(A=1)
        with pytest.raises(ValueError):
            data.script(no_fun)
        df = otp.run(data.script(func))
        assert df['X'][0] == 5
        assert script_text(data, func).strip() == """
long main() {
long X = 0;
X = 0;
while ((X) < (5)) {
X = (X) + (1);
}
}
""".strip()

    def test_while_true(self):

        def no_fun(tick):
            tick['X'] = 0
            while True:  # noqa
                tick['X'] += 1

        def fun(tick):
            tick['X'] = 0
            while True:  # noqa
                tick['X'] += 1
                break

        data = otp.Tick(A=1)
        with pytest.raises(ValueError, match='The condition of while statement always evaluates to True'):
            otp.run(data.script(no_fun))
        df = otp.run(data.script(fun))
        assert df['X'][0] == 1
        assert script_text(data, fun).strip() == """
long main() {
long X = 0;
X = 0;
while (true) {
X = (X) + (1);
break;
}
}
""".strip()

    def test_break_in_range(self):
        data = otp.Tick(PARTICIPANT_ID="4351461722440ec6|f49e10c4e47e393f")

        def no_fun(tick):
            for i in range(50, 10):
                tick['X'] += i

        with pytest.raises(ValueError, match=re.escape('Range object range(50, 10) will result in infinite loop')):
            otp.run(data.script(no_fun))

        def no_fun_2(tick):
            i = ''
            for i in range(3):
                tick['X'] += i

        with pytest.raises(ValueError, match=re.escape('Variable i was declared before with conflicting type.')):
            otp.run(data.script(no_fun_2))

        def fun(tick):
            part = ''
            smbl = ''
            tick['SOME_FIELD'] = 0
            for i in range(50):
                smbl = tick['PARTICIPANT_ID'].str.get(i)
                if smbl == '':
                    break
                if smbl == '|':
                    tick['PARTICIPANT_ID_SPLITTED'] = part
                    yield
                    part = ''
                else:
                    part += smbl
            tick['PARTICIPANT_ID_SPLITTED'] = part

        df = otp.run(data.script(fun))
        assert list(df['PARTICIPANT_ID_SPLITTED']) == ['4351461722440ec6', 'f49e10c4e47e393f']

        assert script_text(data, fun).strip() == """
long main() {
long SOME_FIELD = 0;
string PARTICIPANT_ID_SPLITTED = "";
long LOCAL::i = 0;
string LOCAL::part = "";
string LOCAL::smbl = "";
SOME_FIELD = 0;
for (LOCAL::i = 0; LOCAL::i < 50; LOCAL::i += 1) {
LOCAL::smbl = CASE(BYTE_AT(PARTICIPANT_ID, LOCAL::i),-1,"",CHAR(BYTE_AT(PARTICIPANT_ID, LOCAL::i)));
if (LOCAL::smbl = "") {
break;
}
if (LOCAL::smbl = "|") {
PARTICIPANT_ID_SPLITTED = LOCAL::part;
PROPAGATE_TICK();
LOCAL::part = "";
}
else {
LOCAL::part = LOCAL::part + LOCAL::smbl;
}
}
PARTICIPANT_ID_SPLITTED = LOCAL::part;
}
""".strip()

    def test_range_with_index_f_string(self):
        t = otp.Tick(A=1)

        def fun(tick):
            for i in range(2):
                tick[f'A{i}'] = i

        df = otp.run(t.script(fun))
        assert list(df['A0']) == [0]
        assert list(df['A1']) == [1]

        assert script_text(t, fun).strip() == """
long main() {
long A0 = 0;
long A1 = 0;
A0 = 0;
A1 = 1;
}
""".strip()

    def test_pass(self):
        def func(tick):
            pass

        def func_2(tick):
            pass
            tick['X'] = 1

        data = otp.Tick(A=1)
        with pytest.raises(ValueError):
            data.script(func)
        df = otp.run(data.script(func_2))
        assert df['X'][0] == 1
        assert script_text(data, func_2).strip() == """
long main() {
long X = 0;
X = 1;
}
""".strip()

    def test_big(self):
        def func(tick):
            tick['FIELD'] = 1
            tick['FIELD'] += 2
            tick['FIELD'] = tick['FIELD'] * tick['FIELD']
            if tick['FIELD'] <= 1:
                tick['FIELD'] = 0
                return True
            elif 1 < tick['FIELD'] < 5:
                tick['FIELD'] = 1
                return False
            else:
                tick['FIELD'] = 2
                return True
            for i in [1, 2, 3]:
                tick['STR_FIELD'] = str(i)

        data = otp.Tick(A=1)
        assert script_text(data, func).strip() == """
long main() {
long FIELD = 0;
string STR_FIELD = "";
FIELD = 1;
FIELD = (FIELD) + (2);
FIELD = (FIELD) * (FIELD);
if ((FIELD) <= (1)) {
FIELD = 0;
return true;
}
else {
if (((FIELD) > (1)) AND ((FIELD) < (5))) {
FIELD = 1;
return false;
}
else {
FIELD = 2;
return true;
}
}
STR_FIELD = "1";
STR_FIELD = "2";
STR_FIELD = "3";
return false;
}
""".strip()

    def test_predefined_condiditons(self, data):
        def func(tick):
            if True:
                tick['X'] = 1
            else:
                tick['X'] = 2

        assert script_text(data, func).strip() == """
long main() {
long X = 0;
X = 1;
}
""".strip()
        assert script_text(data,
                           # it is important for lambda to be here
                           lambda tick: 12345 if True else 2,
                           script=False) == '12345'

    def test_case(self):
        def func(tick):
            if 0 < tick['A'] < 4:
                if tick['A'] > 2:
                    return 333
                else:
                    return 222
            return 111

        data = otp.Tick(A=1)
        assert script_text(data, func, script=False) == (
            'CASE(((A) > (0)) AND ((A) < (4)), 1, CASE((A) > (2), 1, 333, 222), 111)'
        )

    def test_statement_expression(self):
        return_1 = lambda: 'return 1;'

        def fun(tick):
            if tick['A'] == 0:
                'return 0;'
            return_1()

        data = otp.Ticks(A=[0, 1])
        assert script_text(data, fun).strip() == """
long main() {
if ((A) = (0)) {
return 0;
}
return 1;
}
""".strip()
        df = otp.run(data.script(fun))
        assert list(df['A']) == [1]

    def test_statement_expression_exception(self):
        x = []

        def fun(tick):
            if tick['A'] == 0:
                x.append(1)

        data = otp.Ticks(A=[0, 1])
        with pytest.raises(AssertionError):
            data.script(fun)

    def test_empty_return_and_pass(self):
        def fun(tick):
            tick['B'] = -1
            if tick['A'] == -123:
                return
            if tick['A'] < 0:
                pass
            tick['B'] = 0
            if tick['A'] > 0:
                tick['B'] = 1

        data = otp.Ticks(A=[-123, -1, 0, 1, 2, 3])
        assert script_text(data, fun).strip() == """
long main() {
long B = 0;
B = -1;
if ((A) = (-123)) {
return true;
}
if ((A) < (0)) {

}
B = 0;
if ((A) > (0)) {
B = 1;
}
}
""".strip()
        df = otp.run(data.script(fun))
        assert list(df['B']) == [-1, 0, 0, 1, 1, 1]

    def test_update_state_vars(self):
        def fun(tick):
            tick.state_vars['X'] = 12345

        data = otp.Tick(A=1)
        data.state_vars['X'] = 0
        data = data.script(fun)
        data['X'] = data.state_vars['X']
        df = otp.run(data)
        assert df['X'][0] == 12345

    def test_apply_and_case(self):
        t = otp.Tick(A=1, B=2.2, C='3', D='2022-01-02 12:13:14.123456789')

        def fun(tick):
            tick['X'] = tick['A'].apply(str)
            tick['Y'] = tick['B'].apply(int)
            tick['Z'] = tick['C'].apply(int)
            tick['_'] = tick['D'].apply(otp.nsectime)
            tick['__'] = tick['_'].dt.day_of_week()

        t = t.script(fun)
        df = otp.run(t)
        assert df['X'][0] == '1'
        assert df['Y'][0] == 2
        assert df['Z'][0] == 3
        assert df['_'][0] == pd.Timestamp('2022-01-02 12:13:14.123456789')
        assert df['__'][0] == 7

    def test_time_alias(self):
        t = otp.Tick(A=1)

        def fun(tick):
            tick['X'] = tick['Time']

        t = t.script(fun)
        df = otp.run(t)
        print(df)
        assert df['X'][0] == df['Time'][0]


class TestLocalVariables:
    def test_simple(self):
        def fun(tick):
            x = 123
            tick['X'] = x

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
long X = 0;
long LOCAL::x = 123;
X = LOCAL::x;
}
""".strip()
        t = t.script(fun)
        assert 'X' in t.schema
        df = otp.run(t)
        assert df['X'][0] == 123

    def test_hard(self):
        def fun(tick):
            x = 1
            for t in tick.state_vars['list']:
                x += t['A']
            tick['X'] = x

        data = otp.Tick(A=1)
        data.state_vars['list'] = otp.state.tick_list(otp.eval(otp.Ticks(A=[2, 3, 4])))
        assert script_text(data, fun).strip() == """
long main() {
long X = 0;
long LOCAL::x = 1;
for (TICK_LIST_TICK LOCAL::t : STATE::list) {
LOCAL::x = (LOCAL::x) + (LOCAL::t.GET_LONG_VALUE("A"));
}
X = LOCAL::x;
}
""".strip()
        data = data.script(fun)
        df = otp.run(data)
        assert df['X'][0] == 1 + 2 + 3 + 4

    def test_static(self):
        def fun(tick):
            x1 = 1
            x2 = otp.static(1)
            x1 += 1
            x2 += 1
            tick['X1'] = x1
            tick['X2'] = x2

        t = otp.Ticks(A=[1, 2, 3])
        assert script_text(t, fun).strip() == """
long main() {
long X1 = 0;
long X2 = 0;
long LOCAL::x1 = 1;
static long LOCAL::x2 = 1;
LOCAL::x1 = (LOCAL::x1) + (1);
LOCAL::x2 = (LOCAL::x2) + (1);
X1 = LOCAL::x1;
X2 = LOCAL::x2;
}
""".strip()
        t = t.script(fun)
        assert 'X1' in t.schema
        assert 'X2' in t.schema
        df = otp.run(t)
        assert list(df['X1']) == [2, 2, 2]
        assert list(df['X2']) == [2, 3, 4]

    def test_errors(self):
        def fun_1(tick):
            x1 = 1
            x1 = 'a'

        def fun_2(tick):
            x1 = 1
            x1 = otp.static(1)

        def fun_3(tick):
            x1 = otp.static(1)
            x1 = otp.static(2)

        t = otp.Tick(A=1)
        for fun in (fun_1, fun_2, fun_3):
            with pytest.raises(ValueError):
                t.script(fun)

    def test_not_defined_in_the_beginning(self):
        def fun(tick):
            tick['X1'] = 1
            x2 = 2

        t = otp.Tick(A=1)
        with pytest.raises(ValueError):
            t.script(fun)

    def test_mixed_definition(self):
        def fun(tick):
            x2 = 1
            x3 = otp.static(x2)
            tick['X2'] = x2
            tick['X3'] = x3

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
long X2 = 0;
long X3 = 0;
long LOCAL::x2 = 1;
static long LOCAL::x3 = LOCAL::x2;
X2 = LOCAL::x2;
X3 = LOCAL::x3;
}
""".strip()
        t = t.script(fun)
        df = otp.run(t)
        assert df['X2'][0] == 1
        assert df['X3'][0] == 1

    def test_tick_descriptor_fields(self):
        def fun(tick):
            x = 0
            name = ''
            for field in otp.tick_descriptor_fields():
                name = field.get_name()
                if name.str.contains('A'):
                    x += 1
                tick['NAME'] = name
                tick['SIZE'] = field.get_size()
                tick['TYPE'] = field.get_type()
            tick['X'] = x

        t = otp.Tick(A=1, B=2, AB=3, AC=4, C=5)
        assert script_text(t, fun).strip() == """
long main() {
string NAME = "";
long SIZE = 0;
string TYPE = "";
long X = 0;
long LOCAL::x = 0;
string LOCAL::name = "";
for (TICK_DESCRIPTOR_FIELD LOCAL::field : LOCAL::INPUT_TICK_DESCRIPTOR_FIELDS) {
LOCAL::name = LOCAL::field.GET_FIELD_NAME();
if (instr(LOCAL::name, "A") > -1) {
LOCAL::x = (LOCAL::x) + (1);
}
NAME = LOCAL::name;
SIZE = LOCAL::field.GET_SIZE();
TYPE = LOCAL::field.GET_TYPE();
}
X = LOCAL::x;
}
""".strip()
        t = t.script(fun)
        assert 'X' in t.schema
        df = otp.run(t)
        assert df['X'][0] == 3
        assert df['NAME'][0] == 'C'
        assert df['SIZE'][0] == 8
        assert df['TYPE'][0] == 'long'

    def test_operation_in_get_value(self):
        def fun(tick):
            tick['TOTAL_INT'] = 0
            tick['TOTAL_FLOAT'] = 0.0
            tick['TOTAL_STRING'] = ''
            for field in otp.tick_descriptor_fields():
                if field.get_type() == 'long':
                    tick['TOTAL_INT'] += tick.get_long_value(field.get_name())
                if field.get_type() == 'double':
                    tick['TOTAL_FLOAT'] += tick.get_double_value(field.get_name())
                if field.get_type() == 'string':
                    tick['TOTAL_STRING'] += tick.get_string_value(field.get_name())
                if field.get_type() == 'nsectime':
                    tick['SOME_DATETIME'] = tick.get_datetime_value(field.get_name())

        now = otp.datetime.now()
        t = otp.Tick(
            INT_1=3,
            INT_2=5,
            FLOAT_1=2.5,
            FLOAT_2=2.7,
            STRING_1='string_1',
            STRING_2='string_2',
            DATETIME_FIELD=now,
        )
        t = t.script(fun)
        assert script_text(t, fun).strip() == """
long main() {
TOTAL_INT = 0;
TOTAL_FLOAT = 0.0;
TOTAL_STRING = "";
for (TICK_DESCRIPTOR_FIELD LOCAL::field : LOCAL::INPUT_TICK_DESCRIPTOR_FIELDS) {
if (LOCAL::field.GET_TYPE() = "long") {
TOTAL_INT = (TOTAL_INT) + (LOCAL::OUTPUT_TICK.GET_LONG_VALUE(LOCAL::field.GET_FIELD_NAME()));
}
if (LOCAL::field.GET_TYPE() = "double") {
TOTAL_FLOAT = (TOTAL_FLOAT) + (LOCAL::OUTPUT_TICK.GET_DOUBLE_VALUE(LOCAL::field.GET_FIELD_NAME()));
}
if (LOCAL::field.GET_TYPE() = "string") {
TOTAL_STRING = TOTAL_STRING + LOCAL::OUTPUT_TICK.GET_STRING_VALUE(LOCAL::field.GET_FIELD_NAME());
}
if (LOCAL::field.GET_TYPE() = "nsectime") {
SOME_DATETIME = LOCAL::OUTPUT_TICK.GET_DATETIME_VALUE(LOCAL::field.GET_FIELD_NAME());
}
}
}
""".strip()
        df = otp.run(t)
        assert df['TOTAL_INT'][0] == 8
        assert df['TOTAL_FLOAT'][0] == 5.2
        assert df['TOTAL_STRING'][0] == 'string_1string_2'
        assert df['SOME_DATETIME'][0] == now

    def test_operation_in_set_field_name(self):
        def fun(tick):
            for field in otp.tick_descriptor_fields():
                if field.get_type() == 'long':
                    tick.set_long_value(field.get_name(), 5)
                if field.get_type() == 'double':
                    tick.set_double_value(field.get_name(), 5.0)
                if field.get_type() == 'string':
                    tick.set_string_value(field.get_name(), '5')
                if field.get_type() == 'nsectime':
                    tick.set_datetime_value(field.get_name(), otp.datetime(2021, 1, 1))

        now = otp.datetime.now()
        t = otp.Tick(
            INT_1=3,
            FLOAT_1=2.5,
            STRING_1='string_1',
            DATETIME_1=now,
        )
        t = t.script(fun)
        assert script_text(t, fun).strip() == """
long main() {
for (TICK_DESCRIPTOR_FIELD LOCAL::field : LOCAL::INPUT_TICK_DESCRIPTOR_FIELDS) {
if (LOCAL::field.GET_TYPE() = "long") {
LOCAL::OUTPUT_TICK.SET_LONG_VALUE(LOCAL::field.GET_FIELD_NAME(), 5);
}
if (LOCAL::field.GET_TYPE() = "double") {
LOCAL::OUTPUT_TICK.SET_DOUBLE_VALUE(LOCAL::field.GET_FIELD_NAME(), 5.0);
}
if (LOCAL::field.GET_TYPE() = "string") {
LOCAL::OUTPUT_TICK.SET_STRING_VALUE(LOCAL::field.GET_FIELD_NAME(), "5");
}
if (LOCAL::field.GET_TYPE() = "nsectime") {
LOCAL::OUTPUT_TICK.SET_DATETIME_VALUE(LOCAL::field.GET_FIELD_NAME(), PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J", "2021-01-01 00:00:00.000000000", _TIMEZONE));
}
}
}
""".strip()
        df = otp.run(t)
        assert df['INT_1'][0] == 5
        assert df['FLOAT_1'][0] == 5.0
        assert df['STRING_1'][0] == '5'
        assert df['DATETIME_1'][0] == otp.datetime(2021, 1, 1)

    def test_operation_in_set_value(self):
        def fun(tick):
            int_val = 5
            double_val = 5.0
            string_val = '5'
            datetime_val = otp.datetime(2021, 1, 1)
            tick.set_long_value('INT_1', int_val)
            tick.set_double_value('FLOAT_1', double_val)
            tick.set_string_value('STRING_1', string_val)
            tick.set_datetime_value('DATETIME_1', datetime_val)

        now = otp.datetime.now()
        t = otp.Tick(
            INT_1=3,
            FLOAT_1=2.5,
            STRING_1='string_1',
            DATETIME_1=now,
        )
        t = t.script(fun)
        assert script_text(t, fun).strip() == """
long main() {
long LOCAL::int_val = 5;
double LOCAL::double_val = 5.0;
string LOCAL::string_val = "5";
nsectime LOCAL::datetime_val = PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J", "2021-01-01 00:00:00.000000000", _TIMEZONE);
LOCAL::OUTPUT_TICK.SET_LONG_VALUE("INT_1", LOCAL::int_val);
LOCAL::OUTPUT_TICK.SET_DOUBLE_VALUE("FLOAT_1", LOCAL::double_val);
LOCAL::OUTPUT_TICK.SET_STRING_VALUE("STRING_1", LOCAL::string_val);
LOCAL::OUTPUT_TICK.SET_DATETIME_VALUE("DATETIME_1", LOCAL::datetime_val);
}
""".strip()
        df = otp.run(t)
        assert df['INT_1'][0] == 5
        assert df['FLOAT_1'][0] == 5.0
        assert df['STRING_1'][0] == '5'
        assert df['DATETIME_1'][0] == otp.datetime(2021, 1, 1)

    def test_str_accessor(self):
        def fun(tick):
            str_var = 'abc'
            b_len = 0
            x = 6 - tick['S'].str.len()
            y = 6 - tick.state_vars['STATE_X'].str.len()
            tick['B'] = str_var.str.repeat(3)
            b_len = tick['B'].str.len()
            tick['C'] = b_len
            tick['X'] = x
            tick['Y'] = y

        t = otp.Tick(A=1, S='hello')
        t.state_vars['STATE_X'] = 'state_var'
        t = t.script(fun)
        assert t.schema['B'] is str
        assert t.schema['C'] is int
        df = otp.run(t)
        assert df['B'][0] == 'abc' * 3
        assert df['C'][0] == 9
        assert df['X'][0] == 1
        assert df['Y'][0] == -3


class TestYield:

    def test_simple(self):
        def fun(tick):
            yield
            tick['A'] += 1
            yield
            tick['A'] += 1
            yield
            tick['A'] += 1

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
PROPAGATE_TICK();
A = (A) + (1);
PROPAGATE_TICK();
A = (A) + (1);
PROPAGATE_TICK();
A = (A) + (1);
}
""".strip()
        t = t.script(fun)
        df = otp.run(t)
        assert len(df) == 4
        assert list(df['A']) == [1, 2, 3, 4]

    def test_error(self):
        def fun(tick):
            yield 'forbidden'

        t = otp.Tick(A=1)
        with pytest.raises(ValueError):
            t.script(fun)


class TestCopyTick:
    def test_simple_input(self):
        def fun(tick):
            tick['A'] = 2
            yield
            tick['A'] = 3
            yield
            tick.copy_tick(tick.input)

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
A = 2;
PROPAGATE_TICK();
A = 3;
PROPAGATE_TICK();
COPY_TICK(LOCAL::INPUT_TICK);
}
""".strip()
        t = t.script(fun)
        df = otp.run(t)
        assert len(df) == 3
        assert list(df['A']) == [2, 3, 1]

    def test_simple_output(self):
        def fun(tick):
            tick['A'] = 2
            yield
            tick['A'] = 3
            yield
            tick.copy_tick(tick)

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
A = 2;
PROPAGATE_TICK();
A = 3;
PROPAGATE_TICK();
COPY_TICK(LOCAL::OUTPUT_TICK);
}
""".strip()
        t = t.script(fun)
        df = otp.run(t)
        assert len(df) == 3
        assert list(df['A']) == [2, 3, 3]

    def test_dynamic_tick(self):
        def fun(tick):
            t = otp.dynamic_tick()
            t['A'] = 12345
            t['X'] = 12345
            tick.copy_tick(t)

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
static DYNAMIC_TICK LOCAL::t;
LOCAL::t.ADD_FIELD("A","long",12345);
LOCAL::t.ADD_FIELD("X","long",12345);
COPY_TICK(LOCAL::t);
}
""".strip()
        t = t.script(fun)
        assert 'X' not in t.schema
        df = otp.run(t)
        assert 'X' not in df
        assert len(df) == 1
        assert list(df['A']) == [12345]


class TestComplexity:
    def test_recursion(self):
        def func(tick):
            for i in list(range(2, 1000)):
                if tick['A'] == i:
                    tick['B'] = 1
        data = otp.Tick(A=1)
        df = otp.run(data.script(func))
        assert df['B'][0] == 0

    def test_time(self):
        def func(tick):
            for i in range(15):
                if tick['A'] == i:
                    tick['B'] = 1

        data = otp.Tick(A=1)

        start_time = time.time()
        data.script(func)
        end_time = time.time()
        assert end_time - start_time < 0.5

    @pytest.mark.skip('TODO')
    def test_apply_recursion(self):
        def fun(tick):
            for i in range(2, 1000):
                if tick['A'] == i:
                    return i
        data = otp.Tick(A=1)
        data.apply(fun)

    @pytest.mark.skip('PY-411')
    def test_apply_in(self):
        def fun(tick):
            if tick['A'] in range(2, 1000):
                return tick['A']
            return -1
        data = otp.Ticks(A=[1, 2])
        data['X'] = data.apply(fun)
        df = otp.run(data)
        assert list(df['X']) == [-1, 2]


class TestRemote:
    def test_source_code(self):
        def fun(row):
            return 1
        setattr(fun, FunctionParser.SOURCE_CODE_ATTRIBUTE, 'def no_fun(row): pass')
        assert FunctionParser(fun).source_code == 'def no_fun(row): pass'

    def test_empty_source_code(self):
        def fun(row):
            return 1
        setattr(fun, FunctionParser.SOURCE_CODE_ATTRIBUTE, '')
        assert FunctionParser(fun).source_code.strip() == """
def fun(row):
    return 1
""".strip()

    def test_remote_fun(self):
        @otp.remote
        def remote_fun(row):
            if row['A'].str.contains('0'):
                return 1
            return 0
        assert getattr(remote_fun, FunctionParser.SOURCE_CODE_ATTRIBUTE).strip() == """
@otp.remote
def remote_fun(row):
    if row['A'].str.contains('0'):
        return 1
    return 0
""".strip()
        data = otp.Ticks(A=['0', '1', '2', '10', '20', '22'])
        data['X'] = data.apply(remote_fun)
        df = otp.run(data)
        assert list(df['X']) == [1, 0, 0, 1, 1, 0]

    def test_remote_lambda(self):
        remote_lambda = lambda row: 1 if row['A'].str.contains('0') else 0
        remote_lambda = otp.remote(remote_lambda)
        assert getattr(remote_lambda, FunctionParser.SOURCE_CODE_ATTRIBUTE).strip() == """
(lambda row: 1 if row['A'].str.contains('0') else 0
)
""".strip()
        data = otp.Ticks(A=['0', '1', '2', '10', '20', '22'])
        data['X'] = data.apply(remote_lambda)
        df = otp.run(data)
        assert list(df['X']) == [1, 0, 0, 1, 1, 0]


def test_compare_track_scope():
    t = otp.Tick(A=1)
    with pytest.raises(TypeError):
        if t['A']:
            pass
    with pytest.raises(TypeError):
        if t['A'] == 1:
            pass
    with _CompareTrackScope():
        if t['A']:
            pass
        if t['A'] == 1:
            pass
    with pytest.raises(TypeError):
        if t['A']:
            pass
    with pytest.raises(TypeError):
        if t['A'] == 1:
            pass


def test_iterating():
    def fun1(tick):
        for field in ['A']:
            tick['B1'] = tick[field]

    def fun2(tick):
        for field in ['ABC']:
            tick['B2'] = tick[field]

    def fun3(tick):
        for field in ['A']:
            tick['B3'] = tick[f'{field}']

    def fun4(tick):
        for field in ['ABC']:
            tick['B4'] = tick[f'{field}']

    def fun5(tick):
        for field in ['']:
            tick['B5'] = tick[f'{field}A']

    def fun6(tick):
        for field in ['BC']:
            tick['B6'] = tick[f'A{field}']

    data = otp.Tick(A=1, ABC=2)
    data = data.script(fun1)
    data = data.script(fun2)
    data = data.script(fun3)
    data = data.script(fun4)
    data = data.script(fun5)
    data = data.script(fun6)
    df = otp.run(data)
    assert df['B1'][0] == 1
    assert df['B2'][0] == 2
    assert df['B3'][0] == 1
    assert df['B4'][0] == 2
    assert df['B5'][0] == 1
    assert df['B6'][0] == 2


class TestContextManagers:
    def test_once_full(self):
        def fun(tick):
            with otp.once():
                tick['A'] = tick['B']
                tick['B'] = tick['C']

        data = otp.Ticks(A=[1, 2, 3], B=[4, 5, 6], C=[7, 8, 9])
        assert script_text(data, fun).strip() == """
long main() {
_ONCE
{
A = B;
B = C;
}
}
""".strip()
        data = data.script(fun)
        df = otp.run(data)
        assert all(df['A'] == [4, 2, 3])
        assert all(df['B'] == [7, 5, 6])
        assert all(df['C'] == [7, 8, 9])

    def test_once_part(self):
        def fun(tick):
            tick['A'] = tick['B']
            with otp.once():
                tick['B'] = tick['C']

        data = otp.Ticks(A=[1, 2, 3], B=[4, 5, 6], C=[7, 8, 9])
        assert script_text(data, fun).strip() == """
long main() {
A = B;
_ONCE
{
B = C;
}
}
""".strip()
        data = data.script(fun)
        df = otp.run(data)
        assert all(df['A'] == [4, 5, 6])
        assert all(df['B'] == [7, 5, 6])
        assert all(df['C'] == [7, 8, 9])


class TestInputOutputTick:
    def test_get_new_field_output(self):
        def fun(tick):
            tick['B'] = 2
            tick['C'] = tick.get_long_value('B')

        data = otp.Tick(A=1)
        assert script_text(data, fun).strip() == """
long main() {
long B = 0;
long C = 0;
B = 2;
C = LOCAL::OUTPUT_TICK.GET_LONG_VALUE("B");
}
""".strip()
        data = data.script(fun)
        df = otp.run(data)
        assert df['A'][0] == 1
        assert df['B'][0] == 2
        assert df['C'][0] == 2

    def test_get_new_field_input(self):
        def fun(tick):
            tick['B'] = 2
            tick['C'] = tick.input.get_long_value('B')

        data = otp.Tick(A=1)
        with pytest.raises(ValueError):
            data.script(fun)

    def test_new_field_input(self):
        def fun(tick):
            tick['B'] = 2
            tick['C'] = tick.input['B']

        data = otp.Tick(A=1)
        with pytest.raises(NameError):
            data.script(fun)

    def test_get_updated_field_output(self):
        def fun(tick):
            tick['A'] = 2
            tick['B'] = tick.get_long_value('A')

        data = otp.Tick(A=1)
        assert script_text(data, fun).strip() == """
long main() {
long B = 0;
A = 2;
B = LOCAL::OUTPUT_TICK.GET_LONG_VALUE("A");
}
""".strip()
        data = data.script(fun)
        df = otp.run(data)
        assert df['A'][0] == 2
        assert df['B'][0] == 2

    def test_get_updated_field_input(self):
        def fun(tick):
            tick['A'] = 2
            tick['B'] = tick.input.get_long_value('A')

        data = otp.Tick(A=1)
        assert script_text(data, fun).strip() == """
long main() {
long B = 0;
A = 2;
B = LOCAL::INPUT_TICK.GET_LONG_VALUE("A");
}
""".strip()
        data = data.script(fun)
        df = otp.run(data)
        assert df['A'][0] == 2
        assert df['B'][0] == 1

    def test_updated_field_input(self):
        def fun(tick):
            tick['A'] = 2
            tick['B'] = tick.input['A']

        data = otp.Tick(A=1)
        assert script_text(data, fun).strip() == """
long main() {
long B = 0;
A = 2;
B = LOCAL::INPUT_TICK.GET_LONG_VALUE("A");
}
""".strip()
        data = data.script(fun)
        df = otp.run(data)
        assert df['A'][0] == 2
        assert df['B'][0] == 1

    def test_set_input(self):
        def fun(tick):
            tick.input['A'] = 2

        data = otp.Tick(A=1)
        with pytest.raises(ValueError):
            data.script(fun)

    def test_set_long_input(self):
        def fun(tick):
            tick.input.set_long_value('A', 2)

        data = otp.Tick(A=1)
        with pytest.raises(AttributeError):
            data.script(fun)


class TestInnerFunctions:

    def test_simple(self):

        def inner_fun(tick) -> int:
            if tick['A'] > 0:
                return 1
            return 0

        def fun(tick):
            tick['B'] = inner_fun(tick)
            tick['C'] = -2 * tick['B'] + 1

        t = otp.Ticks({'A': [1, -1]})
        assert script_text(t, fun).strip() == """
long main() {
long B = 0;
long C = 0;
B = inner_fun();
C = ((-2) * (B)) + (1);
}
long inner_fun() {
if ((A) > (0)) {
return 1;
}
return 0;
}
""".strip()
        t = t.script(fun)
        assert t.schema['B'] is int
        assert t.schema['C'] is int
        df = otp.run(t)
        assert list(df['A']) == [1, -1]
        assert list(df['B']) == [1, 0]
        assert list(df['C']) == [-1, 1]

    def test_void(self):

        def inner_fun(tick) -> int:  # type: ignore
            tick['B'] = 0

        def fun(tick):
            inner_fun(tick)

        t = otp.Tick(A=1)
        with pytest.raises(ValueError, match="Function 'inner_fun' must return values"):
            t.script(fun)

    def test_different_return_types(self):
        def inner_fun(tick) -> int:
            if tick['A'] > 0:
                return 1
            return 'a'  # type: ignore

        def fun(tick):
            inner_fun(tick)

        t = otp.Tick(A=1)
        with pytest.raises(TypeError,
                           match=("Function 'inner_fun' has return annotation 'int',"
                                  r" but the type of statement \(\n?return 'a'\n?\) is 'str'")):
            t.script(fun)

    def test_no_annotation(self):
        def inner_fun(tick):
            return 'a'

        def fun(tick):
            inner_fun(tick)

        t = otp.Tick(A=1)
        with pytest.raises(ValueError, match="Function 'inner_fun' doesn't have return type annotation"):
            t.script(fun)

    def test_wrong_annotation(self):
        def inner_fun(tick) -> str:
            return 1  # type: ignore

        def fun(tick):
            inner_fun(tick)

        t = otp.Tick(A=1)
        with pytest.raises(TypeError,
                           match=("Function 'inner_fun' has return annotation 'str',"
                                  r" but the type of statement \(\n?return 1\n?\) is 'int'")):
            t.script(fun)

    def test_return_string(self):

        def inner_fun(tick) -> str:
            return 'a'

        def fun(tick):
            tick['B'] = inner_fun(tick)

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
string B = "";
B = inner_fun();
}
string inner_fun() {
return "a";
}
""".strip()
        t = t.script(fun)
        assert t.schema['B'] is str
        df = otp.run(t)
        assert list(df['A']) == [1]
        assert list(df['B']) == ['a']

    def test_access_tick_fields(self):

        def inner_fun(tick) -> int:
            tick['A'] += 1
            tick['B'] = 1
            return 1

        def fun(tick):
            inner_fun(tick)

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
long B = 0;
inner_fun();
}
long inner_fun() {
A = (A) + (1);
B = 1;
return 1;
}
""".strip()
        t = t.script(fun)
        assert t.schema['B'] is int
        df = otp.run(t)
        assert list(df['A']) == [2]
        assert list(df['B']) == [1]

    def test_calling_twice(self):
        def inner_fun(tick) -> int:
            tick['A'] += 1
            return tick['A']

        def fun(tick):
            tick['B'] = inner_fun(tick)
            tick['B'] += inner_fun(tick)

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
long B = 0;
B = inner_fun();
B = (B) + (inner_fun());
}
long inner_fun() {
A = (A) + (1);
return A;
}
""".strip()

        t = t.script(fun)
        assert t.schema['B'] is int
        df = otp.run(t)
        assert list(df['A']) == [3]
        assert list(df['B']) == [5]

    def test_parameters_no_annotation(self):
        def inner_fun(tick, a) -> int:
            return a + 1

        def fun(tick):
            tick['B'] = inner_fun(tick, tick['A'])

        t = otp.Tick(A=1)
        with pytest.raises(ValueError, match="Parameter 'a' in function 'inner_fun' doesn't have type annotation"):
            t.script(fun)

    def test_parameters_wrong_type(self):
        def inner_fun(tick, a: int) -> int:
            return a + 1

        def fun(tick):
            tick['B'] = inner_fun(tick, "bad parameter")

        t = otp.Tick(A=1)
        with pytest.raises(TypeError,
                           match=("In function 'inner_fun' parameter 'a' has type annotation 'int',"
                                  " but the type of passed argument is 'str'")):
            t.script(fun)

    def test_parameters_defaults(self):
        def inner_fun(tick, a: int = 0) -> int:
            return a + 1

        def fun(tick):
            tick['B'] = inner_fun(tick, 1)

        t = otp.Tick(A=1)
        with pytest.raises(
            ValueError,
            match="Default values for arguments are not supported in per-tick script function 'inner_fun'"
        ):
            t.script(fun)

    def test_parameters(self):
        def inner_fun(tick, a: int) -> int:
            return a + 1

        def fun(tick):
            tick['B'] = inner_fun(tick, tick['A'])
            tick['B'] += inner_fun(tick, 1)

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
long B = 0;
B = inner_fun(A);
B = (B) + (inner_fun(1));
}
long inner_fun(long a) {
return (a) + (1);
}
""".strip()

        t = t.script(fun)
        assert t.schema['B'] is int
        df = otp.run(t)
        assert list(df['A']) == [1]
        assert list(df['B']) == [4]

    def test_different_local_variables(self):
        def inner_fun(tick, a: int) -> int:
            b = 1
            return a + b

        def fun(tick):
            b = "okay"
            tick['B'] = inner_fun(tick, 1)

        t = otp.Tick(A=1)
        with pytest.raises(ValueError, match="Wrong type for variable 'b': should be <class 'str'>, got <class 'int'>"):
            t.script(fun)

    def test_local_variables(self):
        def inner_fun(tick, a: int) -> int:
            b = 1
            return a + b

        def fun(tick):
            tick['B'] = inner_fun(tick, 1)

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
long B = 0;
B = inner_fun(1);
}
long inner_fun(long a) {
long LOCAL::b = 1;
return (a) + (LOCAL::b);
}
""".strip()

        t = t.script(fun)
        assert t.schema['B'] is int
        df = otp.run(t)
        assert list(df['A']) == [1]
        assert list(df['B']) == [2]

    def test_local_variables_with_parameters(self):
        def inner_fun(tick, a: int) -> int:
            a = 123
            return a

        def fun(tick):
            tick['B'] = inner_fun(tick, 1)

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
long B = 0;
B = inner_fun(1);
}
long inner_fun(long a) {
long LOCAL::a = 123;
return LOCAL::a;
}
""".strip()

        t = t.script(fun)
        assert t.schema['B'] is int
        df = otp.run(t)
        assert list(df['A']) == [1]
        assert list(df['B']) == [123]

    def test_closure_variables(self):

        a = 999

        def inner_fun(tick) -> int:
            return a

        def fun(tick):
            tick['B'] = inner_fun(tick)

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
long B = 0;
B = inner_fun();
}
long inner_fun() {
return 999;
}
""".strip()

        t = t.script(fun)
        assert t.schema['B'] is int
        df = otp.run(t)
        assert list(df['A']) == [1]
        assert list(df['B']) == [999]

    def test_closure_variables_and_parameters(self):

        a = 999

        def inner_fun(tick, a: int) -> int:
            return a

        def fun(tick):
            tick['B'] = inner_fun(tick, 2)

        t = otp.Tick(A=1)
        assert script_text(t, fun).strip() == """
long main() {
long B = 0;
B = inner_fun(2);
}
long inner_fun(long a) {
return a;
}
""".strip()

        t = t.script(fun)
        assert t.schema['B'] is int
        df = otp.run(t)
        assert list(df['A']) == [1]
        assert list(df['B']) == [2]


class TestTickSequenceDynamicSchema:

    def test_getitem(self, session):
        def fun(tick):
            dynamic_tick = otp.dynamic_tick()
            tick_set_tick = otp.tick_set_tick()
            x = ''

            FOUND_FLAG = tick.state_vars['TICK_SET'].find(tick_set_tick, 1)

            if FOUND_FLAG == 0:
                dynamic_tick['A'] = 1
                dynamic_tick['B'] = 'example'
                tick.state_vars['TICK_SET'].insert(dynamic_tick)
            else:
                # accessing not key field that will be added dynamically
                x = tick_set_tick['B']

        t = otp.Ticks({'A': [1, 2]})
        t.state_vars['TICK_SET'] = otp.state.tick_set('latest', ['A'])
        with pytest.raises(ValueError, match=('.*It may also happen if tick sequence is updated dynamically.'
                                              ' In this case .* use function.*')):
            t.script(fun)

    def test_setitem(self, session):
        def fun(tick):
            dynamic_tick = otp.dynamic_tick()
            tick_set_tick = otp.tick_set_tick()

            FOUND_FLAG = tick.state_vars['TICK_SET'].find(tick_set_tick, 1)

            if FOUND_FLAG == 0:
                dynamic_tick['A'] = 1
                dynamic_tick['B'] = 'example'
                tick.state_vars['TICK_SET'].insert(dynamic_tick)
            else:
                # setting not key field that will be added dynamically
                tick_set_tick['B'] = '_updated'

        t = otp.Ticks({'A': [1, 2]})
        t.state_vars['TICK_SET'] = otp.state.tick_set('latest', ['A'])
        with pytest.raises(ValueError, match=('.*It may also happen if tick sequence is updated dynamically.'
                                              ' In this case .* use function.*')):
            t.script(fun)

    def test_setitem_fixed_length_string(self, session):
        # PY-1165
        def store_ids(tick):
            t = otp.dynamic_tick()
            t['ORDER_IDS'] = otp.string[1024]('')
            t['ORDER_IDS'] = 'abc' * 100  # NOSONAR
            tick['X'] = t['ORDER_IDS']

        data = otp.Tick(A=1)
        data = data.script(store_ids)
        assert data.schema['X'] is otp.string[1024]
        df = otp.run(data)
        assert df['X'][0] == 'abc' * 100

    def test_ok(self, session):
        def fun(tick):
            dynamic_tick = otp.dynamic_tick()
            tick_set_tick = otp.tick_set_tick()

            FOUND_FLAG = tick.state_vars['TICK_SET'].find(tick_set_tick, 1)

            if FOUND_FLAG == 0:
                dynamic_tick['A'] = 1
                dynamic_tick['B'] = 'example'
                tick.state_vars['TICK_SET'].insert(dynamic_tick)
            else:
                tick_set_tick.set_string_value('B',
                                               tick_set_tick.get_string_value('B', check_schema=False) + '_updated',
                                               check_schema=False)

        t = otp.Ticks({'A': [1, 2]})
        t.state_vars['TICK_SET'] = otp.state.tick_set('latest', ['A'])
        t = t.script(fun)
        t = t.state_vars['TICK_SET'].dump()
        df = otp.run(t)
        assert list(df['B']) == ['example', 'example_updated']


class TestAggInfluencingSequence:
    """
    Copy-pasted from here.
    Maybe this testing is not so needed and we should remove it in the future.
    """

    @staticmethod
    def keep_total_and_current(tick):
        dynamic_tick_total = otp.dynamic_tick()
        dynamic_tick_current = otp.dynamic_tick()
        it = otp.tick_set_tick()
        FOUND_FLAG = -1

        with otp.once():
            dynamic_tick_total['BUY_FLAG'] = -1
            dynamic_tick_total['OWNER_ID'] = ''
            dynamic_tick_total['OWNER_ENTITY_ID'] = ''
            dynamic_tick_total['INF_SEQ_NUM'] = -1

            dynamic_tick_total['FIRST_SPOOFING_TIME'] = tick['_END_TIME']
            dynamic_tick_total['ASK_PRICE_BOOK_LIMIT'] = 0.0
            dynamic_tick_total['BID_PRICE_BOOK_LIMIT'] = 0.0
            dynamic_tick_total['FIRST_ASK_PRICE'] = 0.0
            dynamic_tick_total['FIRST_BID_PRICE'] = 0.0
            dynamic_tick_total['ASK_BOOK_TOTAL_BEFORE'] = 0.0
            dynamic_tick_total['BID_BOOK_TOTAL_BEFORE'] = 0.0

            dynamic_tick_total['FINAL_SPOOFING_TIME'] = tick['_START_TIME']
            dynamic_tick_total['ASK_BOOK_PARTICIPANT_AFTER'] = 0.0
            dynamic_tick_total['BID_BOOK_PARTICIPANT_AFTER'] = 0.0
            dynamic_tick_total['ASK_BOOK_TOTAL_AFTER'] = 0.0
            dynamic_tick_total['BID_BOOK_TOTAL_AFTER'] = 0.0
            dynamic_tick_total['FINAL_CANCEL_SPOOFING_TIME'] = tick['_START_TIME']

            dynamic_tick_current['BUY_FLAG'] = -1
            dynamic_tick_current['OWNER_ID'] = ''
            dynamic_tick_current['OWNER_ENTITY_ID'] = ''
            dynamic_tick_current['INF_SEQ_NUM'] = -1
            dynamic_tick_current['INITIAL_QTY'] = -1
            dynamic_tick_current['CANCEL_TIME'] = tick['_START_TIME']
            dynamic_tick_current['CANCEL_LONG'] = 0

        FOUND_FLAG = tick.state_vars['TOTAL_AGG_VALUES'].find(
            it, tick['BUY_FLAG'], tick['OWNER_ID'], tick['OWNER_ENTITY_ID'], tick['INF_SEQ_NUM'],
        )
        dynamic_tick_total['BUY_FLAG'] = tick['BUY_FLAG']
        dynamic_tick_total['INF_SEQ_NUM'] = tick['INF_SEQ_NUM']
        dynamic_tick_total['OWNER_ID'] = tick['OWNER_ID']
        dynamic_tick_total['OWNER_ENTITY_ID'] = tick['OWNER_ENTITY_ID']

        if FOUND_FLAG == 0:
            # insert base value min
            dynamic_tick_total['FIRST_SPOOFING_TIME'] = tick['PLACEMENT_TIME']
            dynamic_tick_total['ASK_PRICE_BOOK_LIMIT'] = tick['SEQ_START_ASK_PRICE_BOOK_LIMIT']
            dynamic_tick_total['BID_PRICE_BOOK_LIMIT'] = tick['SEQ_START_BID_PRICE_BOOK_LIMIT']
            dynamic_tick_total['FIRST_ASK_PRICE'] = tick['ASK_PRICE']
            dynamic_tick_total['FIRST_BID_PRICE'] = tick['BID_PRICE']
            dynamic_tick_total['ASK_BOOK_TOTAL_BEFORE'] = tick['ASK_BOOK_TOTAL_BEFORE']
            dynamic_tick_total['BID_BOOK_TOTAL_BEFORE'] = tick['BID_BOOK_TOTAL_BEFORE']
            # insert base value max
            dynamic_tick_total['FINAL_SPOOFING_TIME'] = tick['PLACEMENT_TIME']
            dynamic_tick_total['ASK_BOOK_PARTICIPANT_AFTER'] = tick['ASK_BOOK_PARTICIPANT_AFTER']
            dynamic_tick_total['BID_BOOK_PARTICIPANT_AFTER'] = tick['BID_BOOK_PARTICIPANT_AFTER']
            dynamic_tick_total['ASK_BOOK_TOTAL_AFTER'] = tick['ASK_BOOK_TOTAL_AFTER']
            dynamic_tick_total['BID_BOOK_TOTAL_AFTER'] = tick['BID_BOOK_TOTAL_AFTER']

            tick.state_vars['TOTAL_AGG_VALUES'].insert(dynamic_tick_total)
        else:
            if tick['PLACEMENT_TIME'] < it.get_datetime_value('FIRST_SPOOFING_TIME', check_schema=False):
                it.set_datetime_value('FIRST_SPOOFING_TIME', tick['PLACEMENT_TIME'], check_schema=False)
                it.set_double_value('ASK_PRICE_BOOK_LIMIT', tick['SEQ_START_ASK_PRICE_BOOK_LIMIT'], check_schema=False)
                it.set_double_value('BID_PRICE_BOOK_LIMIT', tick['SEQ_START_BID_PRICE_BOOK_LIMIT'], check_schema=False)
                it.set_double_value('FIRST_ASK_PRICE', tick['ASK_PRICE'], check_schema=False)
                it.set_double_value('FIRST_BID_PRICE', tick['BID_PRICE'], check_schema=False)
                it['ASK_BOOK_TOTAL_BEFORE'] = tick['ASK_BOOK_TOTAL_BEFORE']
                it['BID_BOOK_TOTAL_BEFORE'] = tick['BID_BOOK_TOTAL_BEFORE']
            if tick['PLACEMENT_TIME'] > it.get_datetime_value('FINAL_SPOOFING_TIME', check_schema=False):
                it.set_datetime_value('FINAL_SPOOFING_TIME', tick['PLACEMENT_TIME'], check_schema=False)
                it['ASK_BOOK_PARTICIPANT_AFTER'] = tick['ASK_BOOK_PARTICIPANT_AFTER']
                it['BID_BOOK_PARTICIPANT_AFTER'] = tick['BID_BOOK_PARTICIPANT_AFTER']
                it['ASK_BOOK_TOTAL_AFTER'] = tick['ASK_BOOK_TOTAL_AFTER']
                it['BID_BOOK_TOTAL_AFTER'] = tick['BID_BOOK_TOTAL_AFTER']

        dynamic_tick_current['BUY_FLAG'] = tick['BUY_FLAG']
        dynamic_tick_current['INF_SEQ_NUM'] = tick['INF_SEQ_NUM']
        dynamic_tick_current['OWNER_ID'] = tick['OWNER_ID']
        dynamic_tick_current['OWNER_ENTITY_ID'] = tick['OWNER_ENTITY_ID']
        if tick.state_vars['CUR_AGG_VALUES'].find(
            it, tick['BUY_FLAG'], tick['OWNER_ID'], tick['OWNER_ENTITY_ID'], tick['INF_SEQ_NUM'], 0
        ) == 0:
            # add marker value to start iteration over
            dynamic_tick_current['INITIAL_QTY'] = 0
            dynamic_tick_current['CANCEL_TIME'] = tick['_START_TIME']
            dynamic_tick_current['CANCEL_LONG'] = 0
            tick.state_vars['CUR_AGG_VALUES'].insert(dynamic_tick_current)

        FOUND_FLAG = tick.state_vars['CUR_AGG_VALUES'].find(
            it, tick['BUY_FLAG'], tick['OWNER_ID'], tick['OWNER_ENTITY_ID'], tick['INF_SEQ_NUM'],
            tick['CANCEL_TIME'].apply(int)
        )
        if FOUND_FLAG == 0:
            dynamic_tick_current['INITIAL_QTY'] = tick['INITIAL_QTY']
            dynamic_tick_current['CANCEL_TIME'] = tick['CANCEL_TIME']
            dynamic_tick_current['CANCEL_LONG'] = tick['CANCEL_TIME'].apply(int)
            tick.state_vars['CUR_AGG_VALUES'].insert(dynamic_tick_current)
        else:
            it['INITIAL_QTY'] = it['INITIAL_QTY'] + tick['INITIAL_QTY']

    @staticmethod
    def extract_field_from_tick_set(tick):
        it = otp.tick_set_tick()
        if tick.state_vars['TOTAL_AGG_VALUES'].find(
            it, tick['BUY_FLAG'], tick['OWNER_ID'], tick['OWNER_ENTITY_ID'], tick['INF_SEQ_NUM']
        ):
            tick['FIRST_SPOOFING_TIME'] = it.get_datetime_value('FIRST_SPOOFING_TIME', check_schema=False)
            tick['ASK_PRICE_BOOK_LIMIT'] = it.get_double_value('ASK_PRICE_BOOK_LIMIT', check_schema=False)
            tick['BID_PRICE_BOOK_LIMIT'] = it.get_double_value('BID_PRICE_BOOK_LIMIT', check_schema=False)
            tick['ASK_BOOK_TOTAL_BEFORE'] = it['ASK_BOOK_TOTAL_BEFORE']
            tick['BID_BOOK_TOTAL_BEFORE'] = it['BID_BOOK_TOTAL_BEFORE']
            tick['FIRST_ASK_PRICE'] = it.get_double_value('FIRST_ASK_PRICE', check_schema=False)
            tick['FIRST_BID_PRICE'] = it.get_double_value('FIRST_BID_PRICE', check_schema=False)

            tick['FINAL_SPOOFING_TIME'] = it.get_datetime_value('FINAL_SPOOFING_TIME', check_schema=False)
            tick['ASK_BOOK_PARTICIPANT_AFTER'] = it.get_double_value('ASK_BOOK_PARTICIPANT_AFTER', check_schema=False)
            tick['BID_BOOK_PARTICIPANT_AFTER'] = it.get_double_value('BID_BOOK_PARTICIPANT_AFTER', check_schema=False)
            tick['ASK_BOOK_TOTAL_AFTER'] = it['ASK_BOOK_TOTAL_AFTER']
            tick['BID_BOOK_TOTAL_AFTER'] = it['BID_BOOK_TOTAL_AFTER']

            tick.state_vars['TOTAL_AGG_VALUES'].erase(it)

    @staticmethod
    def find_latest_cancelled_tick_for_first_exec(tick):
        it = otp.tick_set_tick()
        X = 0
        _MIN_TIME_INITIAL_QTY = 0
        _MIN_TIME = tick['_END_TIME']
        __PREV_MIN_TIME = otp.nsectime(0)
        dynamic_tick = otp.dynamic_tick()

        with otp.once():
            dynamic_tick['INITIAL_QTY'] = -1
            dynamic_tick['CANCEL_TIME'] = tick['_START_TIME']

        if tick.state_vars['CUR_AGG_VALUES'].find(
            it, tick['BUY_FLAG'], tick['OWNER_ID'], tick['OWNER_ENTITY_ID'], tick['INF_SEQ_NUM'], 0
        ):
            tick.state_vars['CUR_AGG_VALUES'].erase(it)
            it.next()
            if it.is_end():
                return False

            while (
                it['BUY_FLAG'] == tick['BUY_FLAG']
                and it['INF_SEQ_NUM'] == tick['INF_SEQ_NUM']
                and it['OWNER_ENTITY_ID'] == tick['OWNER_ENTITY_ID']
                and it['OWNER_ID'] == tick['OWNER_ID']
            ):
                tick['QTY_SO_FAR'] += it['INITIAL_QTY']
                tick['FIRST_EXEC_TIME_FOR_MIN_CANCEL_QTY_PCT'] = it['CANCEL_TIME']
                if (tick['QTY_SO_FAR'] / tick['SPOOFING_QTY']) * 100 >= tick.state_vars['LEFT_C_QTY_CONST']:
                    break
                tick.state_vars['CUR_AGG_VALUES'].erase(it)
                it.next()
                if it.is_end():
                    break
        if tick['NEXT_TS'] == 0:
            tick.state_vars['CUR_AGG_VALUES'].clear()
            tick.state_vars['TOTAL_AGG_VALUES'].clear()
        return True

    def test_agg_influencing_sequence(self, session):
        min_cancelled_qty_pct = 50
        data = otp.Ticks(A=list(range(1, 11)))
        data['CANCEL_TIME'] = data['TIMESTAMP'] + otp.Milli(100)
        data['BUY_FLAG'] = 1
        data['OWNER_ID'] = data['A'].apply(str)
        data['OWNER_ENTITY_ID'] = data['A'].apply(str)
        data['INF_SEQ_NUM'] = data['A']

        data['SEQ_START_ASK_PRICE_BOOK_LIMIT'] = 10.0
        data['SEQ_START_BID_PRICE_BOOK_LIMIT'] = 10.0
        data['ASK_PRICE'] = data['A'] * 3.0
        data['BID_PRICE'] = data['A'] * 4.0
        data['ASK_BOOK_TOTAL_BEFORE'] = data['ASK_PRICE'] + 1
        data['BID_BOOK_TOTAL_BEFORE'] = data['BID_PRICE'] - 1
        data['ASK_BOOK_TOTAL_AFTER'] = data['ASK_PRICE'] + 1
        data['BID_BOOK_TOTAL_AFTER'] = data['BID_PRICE'] - 1
        data['ASK_BOOK_PARTICIPANT_AFTER'] = data['ASK_PRICE'] + 1
        data['BID_BOOK_PARTICIPANT_AFTER'] = data['BID_PRICE'] - 1
        data['INITIAL_QTY'] = 1000

        group_by = ['BUY_FLAG', 'OWNER_ID', 'OWNER_ENTITY_ID', 'INF_SEQ_NUM']
        data['PLACEMENT_TIME'] = data['TIMESTAMP']
        data['CANCEL_LONG'] = data['CANCEL_TIME'].apply(int)

        data.state_vars['LEFT_C_QTY_CONST'] = otp.state.var(
            100 - min_cancelled_qty_pct, scope='branch')
        data.state_vars['TOTAL_AGG_VALUES'] = otp.state.tick_set_unordered(
            'latest', group_by, max_distinct_keys=100_000, scope='branch'
        )
        data.state_vars['CUR_AGG_VALUES'] = otp.state.tick_set(
            'latest', group_by + ['CANCEL_LONG'], scope='branch'
        )

        data = data.script(self.keep_total_and_current)

        data = data.agg(
            dict(SPOOFING_QTY=otp.agg.sum(data['INITIAL_QTY']),
                 NUM_SPOOF=otp.agg.count(),
                 FINAL_CANCEL_SPOOFING_TIME=otp.agg.max(data['CANCEL_TIME'])),
            group_by=group_by,
        )
        # to remove additional ticks produced by the flexible bucket
        data, _ = data[(data['SPOOFING_QTY'] > 0) & (data['NUM_SPOOF'] != 0)]

        data["FIRST_SPOOFING_TIME"] = otp.nsectime(0)
        data["ASK_PRICE_BOOK_LIMIT"] = 0.0
        data["BID_PRICE_BOOK_LIMIT"] = 0.0
        data["ASK_BOOK_TOTAL_BEFORE"] = 0.0
        data["BID_BOOK_TOTAL_BEFORE"] = 0.0
        data["FIRST_ASK_PRICE"] = 0.0
        data["FIRST_BID_PRICE"] = 0.0
        data["FINAL_SPOOFING_TIME"] = otp.nsectime(0)
        data["ASK_BOOK_PARTICIPANT_AFTER"] = 0.0
        data["BID_BOOK_PARTICIPANT_AFTER"] = 0.0
        data["ASK_BOOK_TOTAL_AFTER"] = 0.0
        data["BID_BOOK_TOTAL_AFTER"] = 0.0

        data = data.script(self.extract_field_from_tick_set)

        data['TOTAL_SIZE_BEFORE'] = data['ASK_BOOK_TOTAL_BEFORE'] + data['BID_BOOK_TOTAL_BEFORE']
        data['SPOOFING_SIZE_BEFORE'] = data.apply(
            lambda r: r['ASK_BOOK_TOTAL_BEFORE'] if r['BUY_FLAG'] == 0 else r['BID_BOOK_TOTAL_BEFORE']
        )
        data['TOTAL_SIZE_AFTER'] = data['ASK_BOOK_TOTAL_AFTER'] + data['BID_BOOK_TOTAL_AFTER']
        data['SPOOFING_SIZE_AFTER'] = data.apply(
            lambda r: r['ASK_BOOK_TOTAL_AFTER'] if r['BUY_FLAG'] == 0 else r['BID_BOOK_TOTAL_AFTER']
        )
        data['SPOOFING_PARTICIPANT_SIZE'] = data.apply(
            lambda r: r['ASK_BOOK_PARTICIPANT_AFTER'] if r['BUY_FLAG'] == 0 else r['BID_BOOK_PARTICIPANT_AFTER']
        )
        # book-related conditions and checks
        data['SPOOFING_IMBALANCE'] = data['SPOOFING_SIZE_AFTER'] / data['TOTAL_SIZE_AFTER']
        data['PARTICIPANT_CONTRIBUTION'] = data['SPOOFING_PARTICIPANT_SIZE'] / data['SPOOFING_SIZE_AFTER']

        data, _ = data[data['SPOOFING_SIZE_BEFORE'] / data['TOTAL_SIZE_BEFORE'] * 100 <= 50]
        data, _ = data[data['SPOOFING_IMBALANCE'] * 100 >= 50]
        data, _ = data[data['PARTICIPANT_CONTRIBUTION'] * 100 >= 50]

        data['FIRST_EXEC_TIME_FOR_MIN_CANCEL_QTY_PCT'] = data['_START_TIME']
        data['QTY_SO_FAR'] = 0.0  # must be double for correct dividing
        data["NEXT_TS"] = data["TIMESTAMP"][+1].apply(int)

        data = data.script(self.find_latest_cancelled_tick_for_first_exec)
        otp.run(data)


@pytest.mark.skipif(os.name != 'nt', reason='Testing on Windows only')
def test_windows(session):
    # PY-831

    path = os.environ.get('PATH')
    paths = path.split(os.pathsep) if path else []

    if otp.__one_tick_bin_dir__ in paths:
        df = otp.run(otp.Tick(A=1).script('long X = 123;'))
        assert df['X'][0] == 123
    else:
        with pytest.raises(Exception, match='Was not able to load JIT compiler'):
            otp.run(otp.Tick(A=1).script('long X = 123;'))


def test_push_back_and_datetime_diff(session, recwarn):
    # PY-829
    md = otp.Ticks({'PRICE': [1, 5, 6, 2, 8, 7, 3]})
    md['IS_MD'] = 1

    tick = otp.Tick(QTY=1000, PRICE=777)
    tick['IS_MD'] = 0

    data = tick + md
    data['MINUTE_PRICE'] = otp.nan

    def func(tick):
        if tick['IS_MD']:
            tick.state_vars['COUNT'] += 1
            for order_tick in tick.state_vars['ORDERS']:
                if (otp.Milli(tick['Time'] - order_tick['Time'])) < 5 * 1_000:
                    order_tick['MINUTE_PRICE'] = tick['PRICE'].astype(float)
            return False

        tick.state_vars['ORDERS'].push_back(tick)

        return True

    data.state_vars['ORDERS'] = otp.state.tick_list()
    data.state_vars['COUNT'] = 0

    data = data.script(func)
    for w in recwarn:
        assert 'Subtracting datetimes' not in str(w.message)

    orders = data.state_vars['ORDERS'].dump()
    otp.run(orders)


def test_store_ids(session):
    # PY-1165
    def store_ids(tick):
        t = otp.dynamic_tick()
        with otp.once():
            t["EMPLOYEE_USERNAME"] = tick["EMPLOYEE_USERNAME"]
            t["ORDER_IDS"] = otp.string[1024]("")
        tick["ORDER_IDS"] = tick.state_vars["SET"].find("ORDER_IDS", "")
        tick["ORDER_IDS"] += "|" + tick["ORDER_ID"] + "|"
        t["EMPLOYEE_USERNAME"] = tick["EMPLOYEE_USERNAME"]
        t["ORDER_IDS"] = tick["ORDER_IDS"]
        tick.state_vars["SET"].insert(t)

    ticks = otp.Ticks(
        EMPLOYEE_USERNAME=["a", "b", "a", "a", "a", "b", "a", "b"],
        ORDER_ID=["1", "2", "3", "1", "2", "3", "4", "4"],
        ANY=["1", "2", "3", "4", "5", "6", "7", "8"]
    )
    ticks = ticks.distinct(["EMPLOYEE_USERNAME", "ORDER_ID"])
    ticks = ticks.table(ORDER_IDS=otp.string[1024], strict=False)
    ticks.state_vars["SET"] = otp.state.tick_set("latest", "EMPLOYEE_USERNAME", schema=dict(EMPLOYEE_USERNAME=str, ORDER_IDS=otp.string[1024]))
    ticks = ticks.script(store_ids)
    df = otp.run(ticks)
    assert str(df) == '\n'.join([
        '        Time     ORDER_IDS EMPLOYEE_USERNAME ORDER_ID',
        '0 2003-12-04           |1|                 a        1',
        '1 2003-12-04           |2|                 b        2',
        '2 2003-12-04        |1||3|                 a        3',
        '3 2003-12-04     |1||3||2|                 a        2',
        '4 2003-12-04        |2||3|                 b        3',
        '5 2003-12-04  |1||3||2||4|                 a        4',
        '6 2003-12-04     |2||3||4|                 b        4',
    ])


class TestDecimal:

    def test_add(self):
        src = otp.Tick(A=1)

        def script(tick):
            tick['B'] = otp.decimal(100) + 5
            tick['C'] = otp.decimal(100) + otp.math.rand(0, 100)
            tick['D'] = otp.decimal(100) + 1.2

        src = src.script(script)
        assert src.schema['B'] is otp.decimal
        assert src.schema['C'] is otp.decimal
        assert src.schema['D'] is otp.decimal
        df = otp.run(src)
        assert df['B'][0] == 105.0
        assert df.dtypes['B'] == np.float64
        assert 200 >= df['C'][0] >= 100
        assert df.dtypes['C'] == np.float64
        assert df['D'][0] == 101.2
        assert df.dtypes['D'] == np.float64

    def test_scientific_notation(self):
        src = otp.Tick(A=0.000000001, B=otp.decimal(0.000000001))

        def script(tick):
            tick['C'] = otp.decimal(0.000000001)
            tick['D'] = otp.decimal(100)
            tick['D'] += otp.decimal(0.000000001)

        src = src.script(script)
        assert src.schema['B'] is otp.decimal
        assert src.schema['C'] is otp.decimal
        assert src.schema['D'] is otp.decimal
        df = otp.run(src)
        assert df['B'][0] == 0.000000001
        assert df.dtypes['B'] == np.float64
        assert df['C'][0] == 0.000000001
        assert df.dtypes['C'] == np.float64
        assert df['D'][0] == 100 + 0.000000001
        assert df.dtypes['D'] == np.float64
