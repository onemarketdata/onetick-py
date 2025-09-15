import pytest
from pandas.core.dtypes.common import is_integer_dtype

import onetick.py as otp


def test_types_and_quote(m_session):
    data = otp.Ticks(X=[1, 2], A=["A", "B"])
    data["A"] = data.unite_columns("' ", apply_str=True)
    assert data.schema["A"] is str
    assert data.schema["X"] is int  # type shouldn't be changed
    df = otp.run(data)
    assert all(df["A"] == ["1' A", "2' B"])
    assert is_integer_dtype(df["X"])


def test_one_column(m_session):
    data = otp.Ticks(A=["A", "B"])
    data["B"] = data.unite_columns("' ", apply_str=True)
    df = otp.run(data)
    assert all(df["B"] == ["A", "B"])


def test_error(m_session):
    data = otp.Ticks(X=[1, 2], A=["A", "B"])
    with pytest.raises(ValueError, match="All joining columns should be strings"):
        data["A"] = data.unite_columns("' ")
