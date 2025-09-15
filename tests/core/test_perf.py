import os
import pytest
from pathlib import Path

from onetick.py.otq import otq
import onetick.py as otp
from onetick.py.compatibility import is_supported_stack_info

if os.getenv('OTP_WEBAPI_TEST_MODE'):
    pytest.skip('otp.perf is not supported in WebAPI mode', allow_module_level=True)


def test_measure_perf(session):
    t = otp.Tick(A=1)
    _, summary_file = otp.perf.measure_perf(t)
    summary_file = Path(summary_file)
    assert summary_file.exists()
    result = otp.perf.PerformanceSummaryFile(summary_file)
    assert result.cep_summary.text is None
    assert result.cep_summary.dataframe.empty
    assert result.presort_summary.text is None
    assert result.presort_summary.dataframe.empty
    df = result.ordinary_summary.dataframe
    assert 'index' in df
    assert 'EP_name' in df
    assert len(df) == 3
    assert 'TICK_GENERATOR' in list(df['EP_name'])
    for e in result.ordinary_summary:
        assert e['index'] is not None
        assert e['EP_name']


@pytest.mark.skipif(not is_supported_stack_info(), reason='stack_info does not work on some OneTick versions')
def test_measure_perf_with_stack_info(session):
    previous = otp.config.show_stack_info
    otp.config.show_stack_info = True

    t = otp.Tick(A=1)
    result = otp.perf.MeasurePerformance(t)
    for e in result.ordinary_summary:
        assert e['stack_info']
        assert e['traceback']

    otp.config.show_stack_info = previous


@pytest.mark.parametrize('stack_info', (False, True))
def test_presort(session, stack_info):

    if stack_info and not is_supported_stack_info():
        return

    previous = otp.config.show_stack_info
    otp.config.show_stack_info = stack_info

    t = otp.Tick(A=1)
    t.sink(otq.Presort())
    result = otp.perf.MeasurePerformance(t)
    assert result.ordinary_summary.text is not None
    assert result.presort_summary.text is not None
    assert result.cep_summary.text is None

    assert len(result.presort_summary.entries) == 1
    entry = result.presort_summary.entries[0]
    assert entry.max_accumulated_ticks_count == 0
    if stack_info:
        assert entry['stack_info']
        assert entry['traceback']
    else:
        assert not entry['stack_info']
        assert not entry['traceback']

    otp.config.show_stack_info = previous
