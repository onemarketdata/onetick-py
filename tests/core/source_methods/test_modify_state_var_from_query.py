import pytest

import onetick.py as otp


if not otp.compatibility.is_supported_modify_state_var_from_query():
    pytest.skip("skip tests if ModifyStateVarFromQuery is not supported", allow_module_level=True)


def test_update_simple(session):
    data = otp.Ticks(A=[1, 2, 3])
    data.state_vars['VAR'] = 0
    data.state_vars['VAR'] = 7  # NOSONAR

    def fun():
        return otp.Tick(X=123, Y=234)

    data = data.state_vars['VAR'].modify_from_query(fun, output_field_name='X', where=(data['A'] % 2 == 1))
    data['X'] = data.state_vars['VAR']
    df = otp.run(data)
    assert list(df['X']) == [123, 7, 123]


def test_simple_wrong(session):
    data = otp.Ticks(A=[1, 2, 3])
    data.state_vars['VAR'] = 0

    def fun():
        return otp.Tick(X=123)

    with pytest.raises(ValueError, match="Parameter 'action' can only be used with tick sequences"):
        data.state_vars['VAR'].modify_from_query(fun, action='update')


def update_tick_sequence(session):
    data = otp.Tick(A=1)
    data.state_vars['VAR'] = otp.state.tick_list()

    def fun():
        return otp.Ticks(X=[123, 234])

    data = data.state_vars['VAR'].modify_from_query(fun)
    data = data.state_vars['VAR'].dump()
    df = otp.run(data)
    assert list(df['X']) == [123, 234]


def test_start_end_time(session):
    data = otp.Tick(A=1)
    data['END'] = data['_END_TIME']
    data.state_vars['VAR'] = otp.dt(2022, 1, 1)

    def fun():
        t = otp.Tick(X=123)
        t['S'] = t['_START_TIME']
        t['E'] = t['_END_TIME']
        return t

    data = data.state_vars['VAR'].modify_from_query(fun, output_field_name='S', start=data['_START_TIME'])
    data['S'] = data.state_vars['VAR']
    if otp.compatibility.is_fixed_modify_state_var_from_query():
        data = data.state_vars['VAR'].modify_from_query(fun, output_field_name='E', end=data['_END_TIME'])
        data['E'] = data.state_vars['VAR']
    df = otp.run(data)
    assert df['S'][0] == otp.config.default_start_time
    if otp.compatibility.is_fixed_modify_state_var_from_query():
        if otp.compatibility.is_supported_end_time_in_modify_state_var_from_query():
            assert df['E'][0] == otp.config.default_end_time
        else:
            assert df['E'][0] == otp.config.default_start_time


def test_multiple_output_ticks(session):
    data = otp.Tick(A=1)
    data.state_vars['VAR'] = 7

    def fun():
        return otp.Ticks(X=[123, 234], offset=[0, 0])

    data = data.state_vars['VAR'].modify_from_query(fun)
    data['X'] = data.state_vars['VAR']
    with pytest.raises(Exception, match='resulted in multiple output ticks'):
        otp.run(data)


def test_tick_list(session):
    data = otp.Tick(A=1)
    data.state_vars['VAR'] = otp.state.tick_list()

    def fun():
        return otp.Ticks(X=[123, 234])

    data = data.state_vars['VAR'].modify_from_query(fun)
    data = data.state_vars['VAR'].dump()
    df = otp.run(data)
    assert list(df['X']) == [123, 234]
