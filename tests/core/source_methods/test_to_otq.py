import os
import pytest
from pathlib import Path

import onetick.py as otp


def test_time_expressions(session):
    t = otp.Tick(A=1)
    query = t.to_otq(start_time_expression=otp.now(),
                     end_time_expression=otp.now() + otp.Second(5),
                     running=True)
    query = query.split('::')[0]
    text = Path(query).read_text()
    assert "start_expression = NOW()" in text
    assert "end_expression = DATEADD('second', (5), (NOW()), _TIMEZONE)" in text
    assert "RunningQuery = 1" in text


def test_query_properties(session):
    t = otp.Tick(A=1)
    query = t.to_otq(query_properties={'MAX_CONCURRENCY': '123'})
    query = query.split('::')[0]
    text = Path(query).read_text()
    assert "CPU_NUMBER = 123" in text or "MAX_CONCURRENCY = 123" in text
