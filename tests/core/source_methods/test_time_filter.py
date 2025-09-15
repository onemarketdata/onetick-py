import datetime
import pytest

import onetick.py as otp


@pytest.mark.parametrize('start_time,end_time', [
    (100000000, 110000000),
    ('100000000', '110000000'),
    (datetime.time(10, 0), datetime.time(11, 0))])
def test_time_filter_types(session, start_time, end_time):
    data = otp.Ticks(T=range(4),
                     # 9:30, 10:00, 10:30, 11:00
                     offset=[0, 60000 * 30, 60000 * 60, 60000 * 120],
                     start=otp.dt(2022, 1, 1, 9, 30),
                     end=otp.dt(2022, 1, 2))
    data = data.time_filter(start_time=start_time,
                            end_time=end_time)
    df = otp.run(data)
    assert len(df) == 2


@pytest.mark.parametrize('end_time_tick_matches,result', [
    (True, 3),
    (False, 2)])
def test_end_time_tick_matches(session, end_time_tick_matches, result):
    data = otp.Ticks(T=range(4),
                     # 9:30, 10:00, 10:30, 11:00
                     offset=[0, 60000 * 30, 60000 * 60, 60000 * 90],
                     start=otp.dt(2022, 1, 1, 9, 30),
                     end=otp.dt(2022, 1, 2))
    data = data.time_filter(start_time=100000000,
                            end_time=110000000,
                            end_time_tick_matches=end_time_tick_matches)
    df = otp.run(data)
    assert len(df) == result


@pytest.mark.parametrize('discard_on_match,result', [
    (True, 1),
    (False, 3)])
def test_discard_on_match(session, discard_on_match, result):
    data = otp.Ticks(T=range(4),
                     offset=[0, 60000 * 30, 60000 * 60, 60000 * 90],
                     start=otp.dt(2022, 1, 1, 9, 30),
                     end=otp.dt(2022, 1, 2))
    data = data.time_filter(start_time=100000000,
                            end_time=130000000,
                            discard_on_match=discard_on_match)
    df = otp.run(data)
    assert len(df) == result


def test_day_patterns(session):
    data = otp.Ticks(T=range(3),
                     offset=[0, 0, 60000 * 60 * 24],
                     start=otp.dt(2022, 1, 1, 9, 30),
                     end=otp.dt(2022, 1, 2, 11, 0))
    data = data.time_filter(day_patterns="1/2",
                            end_time=240000000,)
    df = otp.run(data)
    assert len(df) == 1
    assert df['T'][0] == 2


@pytest.mark.parametrize('day_patterns', [
    "1/2/3a",
    "1/2/3/4",
    "1.1.1.1",
    "bad",
    ["0.0.1", "0.0.2", "0.0.3", "0.0.4", "0.0.5", "9.9.9"]
])
def test_day_patterns_fail(session, day_patterns):
    data = otp.Ticks(T=range(3),
                     offset=[0, 0, 60000 * 60 * 24],
                     start=otp.dt(2022, 1, 1, 9, 30),
                     end=otp.dt(2022, 1, 2, 11, 0))
    with pytest.raises(ValueError):
        data = data.time_filter(day_patterns=day_patterns,
                                end_time=240000000,)


@pytest.mark.parametrize('day_patterns', [
    "5/1",
    "11.6.4",
    "7.1.1",
    "0.0.06",
    "2021/1/5",
    ["0.0.1", "0.0.2", "0.0.3", "0.0.4", "0.0.5"]
])
def test_day_patterns_good(session, day_patterns):
    data = otp.Ticks(T=range(3),
                     offset=[0, 0, 60000 * 60 * 24],
                     start=otp.dt(2022, 1, 1, 9, 30),
                     end=otp.dt(2022, 1, 2, 11, 0))
    data = data.time_filter(day_patterns=day_patterns,
                            end_time=240000000,)


def test_default_timezone(session, monkeypatch):
    t = otp.Ticks(A=range(24),
                  offset=[i * 60 * 60 * 1000 for i in range(24)],
                  start=otp.dt(2022, 1, 1),
                  end=otp.dt(2022, 1, 2))
    t = t.time_filter(start_time='100000000', end_time='200000000')
    df = otp.run(t, timezone='GMT')
    assert list(df['A']) == [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
