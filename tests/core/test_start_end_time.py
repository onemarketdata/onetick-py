import onetick.py as otp
import pytest
from datetime import datetime, date
import pandas as pd


def format_time_with_nanoseconds(dt):
    s = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    if hasattr(dt, 'nanosecond'):
        s += f'{dt.nanosecond:03}'
    else:
        s += '000'
    return s


@pytest.mark.parametrize("start_time", [datetime(2014, 2, 3, 4, 5, 6, 789012),
                                        otp.datetime(2014, 1, 1, 1, 2, 3, 13, 1),
                                        pd.Timestamp("2014-01-01 15:00:01.000100001"),
                                        datetime(2014, 1, 1),
                                        date(2014, 1, 1),
                                        otp.date(2014, 1, 1)])
@pytest.mark.parametrize("end_time", [datetime(2015, 2, 3, 4, 5, 6, 789012),
                                      otp.datetime(2015, 1, 1, 1, 2, 3, 13, 1),
                                      pd.Timestamp("2015-01-01 15:00:01.000100001"),
                                      datetime(2015, 1, 1),
                                      date(2015, 1, 1),
                                      otp.date(2015, 1, 1)])
def test_nanos(m_session, start_time, end_time):
    source = otp.Tick(A=1)
    source['ST'] = source._START_TIME.dt.strftime("%Y-%m-%d %H:%M:%S.%J", "_TIMEZONE")
    source['ET'] = source._END_TIME.dt.strftime("%Y-%m-%d %H:%M:%S.%J", "_TIMEZONE")
    res = otp.run(source, start=start_time, end=end_time)
    assert res['ST'][0] == format_time_with_nanoseconds(start_time)
    assert res['ET'][0] == format_time_with_nanoseconds(end_time)
