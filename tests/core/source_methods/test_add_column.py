import pytest

import onetick.py as otp


def test_string(session):
    t = otp.Tick(A=1)
    t['B'] = 'B'
    t['C'] = '"C"'
    t['D'] = "'D'"
    t['E'] = """'"E"'"""
    t['F'] = '_TIMEZONE'
    t['G'] = 'a' * 65
    df = otp.run(t)
    assert df['B'][0] == 'B'
    assert df['C'][0] == '"C"'
    assert df['D'][0] == "'D'"
    assert df['E'][0] == """'"E"'"""
    assert df['F'][0] == '_TIMEZONE'
    assert df['G'][0] == 'a' * 65


def test_raw(session):
    t = otp.Tick(A=1)
    t['B'] = otp.raw('A', int)
    t['C'] = otp.raw('A + 999', int)
    with pytest.warns(UserWarning, match='Be careful, default string length'):
        t['D'] = otp.raw('_TIMEZONE', str)
        t['E'] = otp.raw('"' + 'a' * 65 + '"', str)
    t['F'] = otp.raw('"' + 'a' * 65 + '"', otp.string[65])
    df = otp.run(t, timezone='Asia/Yerevan')
    assert df['B'][0] == 1
    assert df['C'][0] == 1000
    assert df['D'][0] == 'Asia/Yerevan'
    assert df['E'][0] == 'a' * 64
    assert df['F'][0] == 'a' * 65


def test_raw_error(session):
    t = otp.Tick(A=1)
    with pytest.warns(UserWarning, match='Be careful, default string length'):
        t['B'] = otp.raw('"', str)
    with pytest.raises(Exception):
        otp.run(t)
