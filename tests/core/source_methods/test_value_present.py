import pytest

import onetick.py as otp


def test_simple(session):
    data = otp.Ticks(A=[1, 2, 3, 4, 5])
    data = data.value_present(field='A', values=[2, 3])
    df = otp.run(data)
    assert list(df['A']) == [2, 3]


def test_discard_on_match(session):
    data = otp.Ticks(A=[1, 2, 3, 4, 5])
    data = data.value_present(field='A', values=[2, 3], discard_on_match=True)
    df = otp.run(data)
    assert list(df['A']) == [1, 4, 5]


def test_errors(session):
    data = otp.Ticks(A=[1, 2, 3, 4, 5])
    with pytest.raises(ValueError, match="Field 'B' is not in schema"):
        data.value_present(field='B', values=[2, 3])
    with pytest.raises(ValueError, match="Parameter 'values' must be a list or other sequence, got <class 'int'>"):
        data.value_present(field='A', values=123)
    with pytest.raises(ValueError, match="Parameter 'values' must be a list or other sequence, got <class 'str'>"):
        data.value_present(field='A', values='abc')
    with pytest.raises(ValueError,
                       match="Field 'A' and parameter 'values' have different types: <class 'int'> and <class 'str'>"):
        data.value_present(field='A', values=['2', '3'])
    with pytest.raises(ValueError, match="All values must be of the same type in 'values' parameter"):
        data.value_present(field='A', values=[2, '3'])
    with pytest.raises(ValueError, match="Parameter 'values' is empty"):
        data.value_present(field='A', values=[])
