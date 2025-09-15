import pytest

import onetick.py as otp


def test_bit_and_base(session):
    t = otp.Tick(X=1)

    t['RES'] = otp.bit_and(23, 13)

    df = otp.run(t)
    assert df['RES'].to_list() == [5]


@pytest.mark.parametrize("ticks_data,x,result", [
    ([0, 1, 2, 3], 1, [0, 1, 0, 1]),
    ([10, 11, 12, 13], 5, [0, 1, 4, 5]),
])
def test_bit_and_column(session, ticks_data, x, result):
    t = otp.Ticks(X=ticks_data)

    t['RES'] = otp.bit_and(t['X'], x)

    df = otp.run(t)
    assert df['RES'].to_list() == result


def test_bit_and_ops(session):
    t = otp.Ticks(X=[0, 1, 2, 3])

    t['RES'] = otp.bit_and(t['X'] * 2, 3)

    df = otp.run(t)
    assert df['RES'].to_list() == [0, 2, 0, 2]


def test_bit_or_base(session):
    t = otp.Tick(X=1)

    t['RES'] = otp.bit_or(2, 1)

    df = otp.run(t)
    assert df['RES'].to_list() == [3]


def test_bit_or_column(session):
    t = otp.Ticks(X=[10, 11, 12, 13])

    t['RES'] = otp.bit_or(t['X'], 5)

    df = otp.run(t)
    assert df['RES'].to_list() == [15, 15, 13, 13]


def test_bit_or_ops(session):
    t = otp.Ticks(X=[0, 1, 2, 3])

    t['RES'] = otp.bit_or(t['X'] * 2, 1)

    df = otp.run(t)
    assert df['RES'].to_list() == [1, 3, 5, 7]


def test_bit_xor(session):
    t = otp.Ticks(X=[0, 1, 2, 3])

    t['RES'] = otp.bit_xor(t['X'], 1)

    df = otp.run(t)
    assert list(df['RES']) == [1, 0, 3, 2]


def test_bit_not(session):
    t = otp.Ticks(X=[0, 1, 2, 3])

    t['RES'] = otp.bit_not(t['X'])

    df = otp.run(t)
    assert list(df['RES']) == [-1, -2, -3, -4]


def test_bit_at(session):
    t = otp.Tick(A=1)
    for i in range(16):
        t[f'A{i}'] = otp.bit_at(0b1000101010111111, i)
    df = otp.run(t)
    result = ''
    for i in reversed(range(16)):
        result += str(df[f'A{i}'][0])
    assert result == '1000101010111111'
