import pytest

import onetick.py as otp


def test_skip_bad_tick(session):
    data = otp.Ticks(X=[10, 11, 15, 11, 9, 10])
    data = data.skip_bad_tick(field="X", jump_threshold=1.2, num_neighbor_ticks=1)
    result = otp.run(data)
    assert list(result["X"]) == [10, 11, 11, 10]


def test_skip_bad_tick_column(session):
    data = otp.Ticks(X=[10, 11, 15, 11, 9, 10])
    data = data.skip_bad_tick(field=data["X"], jump_threshold=1.2, num_neighbor_ticks=1)
    result = otp.run(data)
    assert list(result["X"]) == [10, 11, 11, 10]


def test_skip_bad_tick_non_existing_field():
    data = otp.Ticks(X=[1, 2, 3])

    with pytest.raises(ValueError, match="not in the schema"):
        data.skip_bad_tick(field="A", jump_threshold=1.5, num_neighbor_ticks=1)


def test_skip_bad_tick_discard_on_match(session):
    data = otp.Ticks(X=[10, 11, 15, 11, 9, 10])
    data = data.skip_bad_tick(field=data["X"], jump_threshold=1.2, num_neighbor_ticks=1, discard_on_match=True)
    result = otp.run(data)
    assert list(result["X"]) == [15, 9]


def test_skip_bad_tick_use_absolute_values(session):
    data = otp.Ticks(X=[10, -11, -15, 11, 9, 10])
    data = data.skip_bad_tick(field=data["X"], jump_threshold=1.2, num_neighbor_ticks=1, use_absolute_values=True)
    result = otp.run(data)
    assert list(result["X"]) == [10, -11, 11, 10]
