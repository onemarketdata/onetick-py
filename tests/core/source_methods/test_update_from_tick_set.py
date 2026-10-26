import pytest

import onetick.py as otp


def get_data():
    data = otp.Ticks(A=[1, 2, 3, 4], B=[10, 20, 30, 40], C=['a', 'b', 'c', 'd'])
    data.state_vars['SET'] = otp.state.tick_set(
        'oldest', 'A', otp.eval(otp.Ticks(A=[1, 3, 5], B=[100, 300, 500], D=[1.5, 2.5, 3.5]))
    )
    return data


def test_simple(session):
    data = get_data()
    data = data.update_from_tick_set(data.state_vars['SET'])
    df = otp.run(data)
    assert list(df['A']) == [1, 2, 3, 4]
    assert list(df['B']) == [100, 20, 300, 40]
    assert list(df['C']) == ['a', 'b', 'c', 'd']
    assert 'D' not in df


def test_string_name(session):
    data = get_data()
    data = data.update_from_tick_set('SET')
    df = otp.run(data)
    assert list(df['B']) == [100, 20, 300, 40]


def test_where(session):
    data = get_data()
    data = data.update_from_tick_set(data.state_vars['SET'], where=data['B'] > 15)
    df = otp.run(data)
    assert list(df['B']) == [10, 20, 300, 40]


def test_wrong_name(session):
    data = get_data()
    with pytest.raises(KeyError, match="State variable 'WRONG' is not in the list"):
        data.update_from_tick_set('WRONG')


@pytest.mark.parametrize('value', [0, otp.state.tick_list(), otp.state.tick_deque()])
def test_not_tick_set_state_var(session, value):
    data = get_data()
    data.state_vars['VAR'] = value
    with pytest.raises(ValueError, match="must be a tick set state variable"):
        data.update_from_tick_set(data.state_vars['VAR'])
    with pytest.raises(ValueError, match="must be a tick set state variable"):
        data.update_from_tick_set('VAR')


def test_not_state_var(session):
    data = get_data()
    with pytest.raises(ValueError, match="must be a tick set state variable"):
        data.update_from_tick_set(data['A'])


def test_tick_set_from_other_source(session):
    data = get_data()
    other = otp.Tick(A=1)
    other.state_vars['OTHER'] = otp.state.tick_set('oldest', 'A')
    with pytest.raises(ValueError, match="Tick set 'OTHER' is not in the list"):
        data.update_from_tick_set(other.state_vars['OTHER'])


def test_missing_key_fields(session):
    data = otp.Ticks(X=[1, 2], B=[10, 20])
    data.state_vars['SET'] = otp.state.tick_set('oldest', 'A', otp.eval(otp.Ticks(A=[1, 3], B=[100, 300])))
    with pytest.raises(ValueError, match="Key fields .* are not in the source's schema"):
        data.update_from_tick_set(data.state_vars['SET'])
