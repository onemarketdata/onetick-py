import pytest

import onetick.py as otp


def test_count(m_session):
    data = otp.Ticks(X=[1, 2, 3])
    assert data.count() == 3

    data = otp.Empty()
    assert data.count() == 0

    data = otp.merge([otp.Ticks(X=[1])], symbols=otp.Ticks(SYMBOL_NAME=['A', 'B']))
    assert data.count() == 2

    with pytest.warns(UserWarning, match='Eval statement returned no symbols'):
        data = otp.merge([otp.Ticks(X=[1])], symbols=otp.Empty())
        assert data.count() == 0
