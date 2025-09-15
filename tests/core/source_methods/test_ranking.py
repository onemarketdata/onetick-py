import pytest
import onetick.py as otp
from onetick.py.otq import otq


def test_apply(f_session):
    t = otp.Ticks({'A': [1, 2, 3]})
    t = otp.agg.ranking('A').apply(t)
    df = otp.run(t)
    assert t.schema['RANKING'] is int
    assert list(df['RANKING']) == [3, 2, 1]


def test_agg(f_session):
    t = otp.Ticks({'A': [1, 2, 3]})
    with pytest.raises(ValueError):
        t.agg({'X': otp.agg.ranking('A')})


def test_ranking(f_session):
    t = otp.Ticks({'A': [1, 2, 3]})
    t = t.ranking('A')
    df = otp.run(t)
    assert t.schema['RANKING'] is int
    assert list(df['RANKING']) == [3, 2, 1]


@pytest.mark.parametrize('keep_timestamp', (True, False, None))
def test_keep_timestamp(f_session, keep_timestamp):
    t = otp.Ticks({'A': [1, 2, 3]})
    if keep_timestamp is None:
        t = t.ranking('A')
    else:
        t = t.ranking('A', keep_timestamp=keep_timestamp)
    df = otp.run(t)
    if keep_timestamp is False:
        assert 'TICK_TIME' in t.schema
        assert 'TICK_TIME' in df
    else:
        assert 'TICK_TIME' not in t.schema
        assert 'TICK_TIME' not in df

    assert list(df['RANKING']) == [3, 2, 1]


def test_output_field_exists(f_session):
    t = otp.Ticks({'A': [1, 2, 3], 'RANKING': [1, 2, 3]})
    with pytest.raises(ValueError):
        t.ranking('A')


def test_wrong_input_field(f_session):
    t = otp.Ticks({'A': [1, 2, 3]})
    with pytest.raises(TypeError):
        t.ranking('X')


def test_rank_by_many(f_session):
    t = otp.Ticks({'A': [1, 1, 1], 'B': [1, 2, 3]})
    t = t.ranking(['A', 'B'])
    df = otp.run(t)
    assert list(df['RANKING']) == [3, 2, 1]


def test_asc_desc(f_session):
    t = otp.Ticks({'A': [1, 1, 2], 'B': [1, 2, 3]})
    with pytest.raises(ValueError):
        t.ranking({'A': 'kek', 'B': 'lol'})
    t = t.ranking({'A': 'asc', 'B': 'desc'})
    df = otp.run(t)
    assert list(df['RANKING']) == [2, 1, 3]


@pytest.mark.parametrize('include_tick', (True, False))
def test_show_rank_as(f_session, include_tick):
    data = otp.Ticks({'A': [1, 2, 2, 3, 2, 1]})
    with pytest.raises(ValueError):
        data.ranking('A', show_rank_as='lkajsdlasjd')
    t = data.ranking({'A': 'asc'},
                     show_rank_as='percent_lt_values',
                     include_tick=include_tick)
    df = otp.run(t)
    assert t.schema['RANKING'] is float
    if include_tick:
        one = pytest.approx(4 / 6 * 100)
        two = pytest.approx(1 / 6 * 100)
        three = pytest.approx(0 / 6 * 100)
    else:
        one = pytest.approx(4 / 5 * 100)
        two = pytest.approx(1 / 5 * 100)
        three = pytest.approx(0 / 5 * 100)
    assert list(df['RANKING']) == [one, two, two, three, two, one]

    t = data.ranking({'A': 'asc'},
                     show_rank_as='percent_le_values',
                     include_tick=include_tick)
    df = otp.run(t)
    assert t.schema['RANKING'] is float
    if include_tick:
        one = pytest.approx(100)
        two = pytest.approx(2 / 3 * 100)
        three = pytest.approx(1 / 6 * 100)
    else:
        one = pytest.approx(100)
        two = pytest.approx(60)
        three = pytest.approx(0)
    assert list(df['RANKING']) == [one, two, two, three, two, one]

    t = data.ranking({'A': 'asc'},
                     show_rank_as='percentile_standard',
                     include_tick=include_tick)
    df = otp.run(t)
    assert t.schema['RANKING'] is float
    if include_tick:
        one = pytest.approx((4 + 1) / 6 * 100)
        two = pytest.approx((1 + 3 / 2) / 6 * 100)
        three = pytest.approx((0 + 1 / 2) / 6 * 100)
    else:
        one = pytest.approx(5 / 6 * 100)
        two = pytest.approx(5 / 12 * 100)
        three = pytest.approx(1 / 12 * 100)
    assert list(df['RANKING']) == [one, two, two, three, two, one]
