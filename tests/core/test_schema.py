import operator

import pytest

import onetick.py as otp
from onetick.py.otq import otq
from onetick.py.core._source.schema import Schema

Pos = operator.truth
Neg = operator.not_


class TestPureDict:

    @pytest.mark.parametrize("init, len_",
                             [
                                 ({}, 0),
                                 (dict(a=3), 1),
                                 (dict(a=.3, b='a'), 2)
                             ])
    def test_init(self, init, len_):
        s = Schema(**init)
        assert len(s) == len_

    @pytest.mark.parametrize('init,key,op',
                             [
                                 (dict(a=1, b=2), 'a', Pos),
                                 (dict(a=1, b=2), 'c', Neg),
                                 (dict(a=1, b=2), 'b', Pos),
                                 ({}, 'a', Neg),
                                 (dict(a=1, b=2, c=3, d=4), 'c', Pos)
                             ]
                             )
    def test_get_contains_iter(self, init, key, op):
        s = Schema(**init)

        assert op(key in s)
        assert len(init) == len(s)

        for k in s:
            assert init[k] == s[k]
        for k in init:
            assert init[k] == s[k]
        for k, v in s.items():
            assert init[k] == v
        assert init.keys() == s.keys()

    def test_repr(self, capsys):
        """ test validates that __repr__ method works fine """
        d = dict(a=1, b=3)
        s = Schema(**d)

        print(s)
        out, _ = capsys.readouterr()
        assert out.strip() == repr(d)


def test_update(session):
    t = otp.Tick(A=1)
    for dtype in (int, float, str, otp.string[15], otp.varstring, otp.nsectime, otp.msectime):
        t.schema.update(A=dtype)
        assert t.schema['A'] is dtype
    for dtype, result in {otp.datetime: otp.nsectime, bool: float}.items():
        with pytest.warns(FutureWarning, match='complex types is deprecated'):
            t.schema.update(A=dtype)
        assert t.schema['A'] is result
    for dtype, result in {'a': str, 123: int, 1.2345: float}.items():
        with pytest.warns(FutureWarning, match='instance of the class is deprecated'):
            t.schema.update(A=dtype)
        assert t.schema['A'] is result
    with pytest.raises(TypeError):
        with pytest.warns(FutureWarning, match='instance of the class is deprecated'):
            t.schema.update(A=Schema())
