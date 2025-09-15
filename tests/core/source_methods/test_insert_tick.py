import pytest

import onetick.py as otp


@pytest.mark.parametrize('inplace', (True, False, None))
def test_inplace(session, inplace):
    t = otp.Tick(A=1)
    if inplace is None:
        x = t.insert_tick()
        df = otp.run(t)
        assert list(df['A']) == [1]
        df = otp.run(x)
    elif inplace is False:
        x = t.insert_tick(inplace=False)
        df = otp.run(t)
        assert list(df['A']) == [1]
        df = otp.run(x)
    elif inplace is True:
        t.insert_tick(inplace=True)
        df = otp.run(t)
    assert list(df['A']) == [0, 1]


@pytest.mark.parametrize('insert_before', (True, False))
def test_insert_before(session, insert_before):
    t = otp.Tick(A=1)
    t = t.insert_tick(insert_before=insert_before)
    df = otp.run(t)
    if insert_before:
        assert list(df['A']) == [0, 1]
    else:
        assert list(df['A']) == [1, 0]


@pytest.mark.parametrize('num_ticks_to_insert', (1, 2, 0, -1, 'asdads'))
def test_num_ticks_to_insert(session, num_ticks_to_insert):
    t = otp.Tick(A=1)
    if not isinstance(num_ticks_to_insert, int) or num_ticks_to_insert <= 0:
        with pytest.raises(ValueError):
            t.insert_tick(num_ticks_to_insert=num_ticks_to_insert)
        return
    t = t.insert_tick(num_ticks_to_insert=num_ticks_to_insert)
    df = otp.run(t)
    assert list(df['A']) == [0] * num_ticks_to_insert + [1]


@pytest.mark.parametrize('preserve_input_ticks', (True, False))
def test_preserve_input_ticks(session, preserve_input_ticks):
    t = otp.Tick(A=1)
    if not preserve_input_ticks:
        with pytest.raises(ValueError):
            t.insert_tick(preserve_input_ticks=preserve_input_ticks)
        t = t.insert_tick(fields={'B': 'new'}, preserve_input_ticks=False)
    else:
        t = t.insert_tick(fields={'B': 'new'}, preserve_input_ticks=True)
    df = otp.run(t)
    if not preserve_input_ticks:
        assert 'A' not in t.schema
        assert t.schema['B'] is str
        assert 'A' not in df
        assert list(df['B']) == ['new']
    else:
        assert t.schema['A'] is int
        assert t.schema['B'] is str
        assert list(df['A']) == [1, 1]
        assert list(df['B']) == ['new', '']


def test_where(session):
    t = otp.Ticks(A=[1, 2])
    t = t.insert_tick(where=t['A'] == 1)
    df = otp.run(t)
    assert list(df['A']) == [0, 1, 2]


def test_fields(session):
    t = otp.Tick(A=1, B=1)
    with pytest.raises(ValueError):
        t.insert_tick(fields={'A': 'WRONG_TYPE'})

    t = t.insert_tick(fields={'B': 2, 'C': 'c', 'D': float})
    assert t.schema['C'] is str
    assert t.schema['D'] is float
    df = otp.run(t)
    assert list(df['A']) == [1, 1]
    assert list(df['B']) == [2, 1]
    assert list(df['C']) == ['c', '']
    assert all(df['D'].isna())


def test_use_inserted_field(session):
    t = otp.Tick(A=1)
    t = t.insert_tick(fields={'B': 2})
    t['C'] = t['B']
    df = otp.run(t)
    assert list(df['A']) == [1, 1]
    assert list(df['B']) == [2, 0]
    assert list(df['C']) == [2, 0]


def test_fields_vs_no_fields(session):
    t = otp.Tick(A=1)
    t = t.insert_tick()
    df = otp.run(t)
    assert list(df['A']) == [0, 1]

    t = otp.Tick(A=1)
    t = t.insert_tick(fields={'B': 'b'})
    df = otp.run(t)
    assert list(df['A']) == [1, 1]
    assert list(df['B']) == ['b', '']
