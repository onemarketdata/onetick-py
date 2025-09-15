import pytest
import onetick.py as otp
import numpy as np


if not otp.compatibility.is_supported_join_with_aggregated_window():
    pytest.skip("skip tests if JoinWithAggregatedWindow is not supported", allow_module_level=True)


def test_one_tick(session):
    t1 = otp.Tick(A=1)
    t2 = otp.Tick(B=2)
    data = otp.join_with_aggregated_window(
        t1, t2, {'A': otp.agg.count()},
        boundary_aggr_tick='previous',
    )
    assert dict(data.schema) == {'A': int, 'B': int}
    df = otp.run(data)
    assert df.dtypes['A'] == np.int64
    assert df.dtypes['B'] == np.int64
    assert len(df) == 1
    assert list(df['A']) == [1]
    assert list(df['B']) == [2]


@pytest.mark.parametrize('boundary_aggr_tick', ('previous', 'next'))
def test_boundary_aggr_tick(session, boundary_aggr_tick):

    t1 = otp.Ticks(A=[0, 1, 2, 3, 4, 5, 6])
    t2 = otp.Ticks(B=[1, 3, 5], offset=[1, 3, 5])

    if boundary_aggr_tick == 'next' and not otp.compatibility.is_supported_next_in_join_with_aggregated_window():
        with pytest.warns(match="does not support setting parameter 'boundary_aggr_tick' to 'next'"):
            otp.join_with_aggregated_window(
                t1, t2, {
                    'A': otp.agg.sum('A'),
                    'C': otp.agg.count(),
                },
                boundary_aggr_tick=boundary_aggr_tick,
            )
        return
    else:
        data = otp.join_with_aggregated_window(
            t1, t2, {
                'A': otp.agg.sum('A'),
                'C': otp.agg.count(),
            },
            boundary_aggr_tick=boundary_aggr_tick,
        )

    assert dict(data.schema) == {'A': int, 'B': int, 'C': int}
    df = otp.run(data)
    assert df.dtypes['A'] == np.int64
    assert df.dtypes['B'] == np.int64
    assert df.dtypes['C'] == np.int64
    if boundary_aggr_tick == 'previous':
        assert list(df['A']) == [1, 6, 15]
        assert list(df['C']) == [2, 4, 6]
    else:
        assert list(df['A']) == [0, 3, 10]
        assert list(df['C']) == [1, 3, 5]
    assert list(df['B']) == [1, 3, 5]


@pytest.mark.parametrize('boundary_aggr_tick', ('previous', 'next'))
def test_bucket(session, boundary_aggr_tick):

    if boundary_aggr_tick == 'next' and not otp.compatibility.is_supported_next_in_join_with_aggregated_window():
        pytest.skip('not supported on this onetick build')

    t1 = otp.Ticks(A=[0, 1, 2, 3, 4, 5, 6])
    t2 = otp.Ticks(B=[1, 3, 5], offset=[1, 3, 5])
    data = otp.join_with_aggregated_window(
        t1, t2, {
            'A': otp.agg.sum('A'),
            'C': otp.agg.count(),
        },
        boundary_aggr_tick=boundary_aggr_tick,
        bucket_interval=2,
        bucket_units='ticks',
    )
    assert dict(data.schema) == {'A': int, 'B': int, 'C': int}
    df = otp.run(data)
    assert df.dtypes['A'] == np.int64
    assert df.dtypes['B'] == np.int64
    assert df.dtypes['C'] == np.int64
    if boundary_aggr_tick == 'previous':
        assert list(df['A']) == [1, 5, 9]
        assert list(df['C']) == [2, 2, 2]
    else:
        assert list(df['A']) == [0, 3, 7]
        assert list(df['C']) == [1, 2, 2]
    assert list(df['B']) == [1, 3, 5]


@pytest.mark.parametrize('pass_src_delay_msec', (1, -1))
@pytest.mark.parametrize('boundary_aggr_tick', ('previous', 'next'))
def test_delay(session, pass_src_delay_msec, boundary_aggr_tick):

    if boundary_aggr_tick == 'next' and not otp.compatibility.is_supported_next_in_join_with_aggregated_window():
        pytest.skip('not supported on this onetick build')

    t1 = otp.Ticks(A=[0, 1, 2, 3, 4, 5, 6])
    t2 = otp.Ticks(B=[1, 3, 5], offset=[1, 3, 5])
    data = otp.join_with_aggregated_window(
        t1, t2, {
            'A': otp.agg.sum('A'),
            'C': otp.agg.count(),
        },
        boundary_aggr_tick=boundary_aggr_tick,
        pass_src_delay_msec=pass_src_delay_msec,
    )
    assert dict(data.schema) == {'A': int, 'B': int, 'C': int}
    df = otp.run(data)
    assert df.dtypes['A'] == np.int64
    assert df.dtypes['B'] == np.int64
    assert df.dtypes['C'] == np.int64
    if pass_src_delay_msec == 1:
        if boundary_aggr_tick == 'previous':
            assert list(df['A']) == [0, 3, 10]
            assert list(df['C']) == [1, 3, 5]
        else:
            assert list(df['A']) == [0, 1, 6]
            assert list(df['C']) == [0, 2, 4]
    else:
        if boundary_aggr_tick == 'previous':
            assert list(df['A']) == [3, 10, 21]
            assert list(df['C']) == [3, 5, 7]
        else:
            assert list(df['A']) == [1, 6, 15]
            assert list(df['C']) == [2, 4, 6]
    assert list(df['B']) == [1, 3, 5]
