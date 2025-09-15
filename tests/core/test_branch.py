import pytest

from onetick.py.core.column_operations.base import _Operation


def test_if():
    with pytest.raises(TypeError, match="It is not allowed to use compare in if-else and while clauses"):
        if _Operation():
            pass  # NOSONAR


def test_for():
    with pytest.raises(TypeError, match="object is not iterable"):
        for _ in _Operation():  # noqa
            pass
