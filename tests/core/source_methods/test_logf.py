import pytest
import os
import onetick.py as otp


def _load_log_file(session_obj):
    with open(session_obj._log_file, "r") as log_file:
        log_lines = log_file.readlines()

    result = []
    for log_line in log_lines:
        log_line_parts = log_line.split(':')
        severity = log_line_parts[0].split()[1]
        # there are two known log line formats:
        # 20231218145705 WARNING: WARN_00502948AAAAA: test
        if len(log_line_parts) == 3:
            log_message = log_line_parts[2]
        # 20231218145705 WARNING: test
        elif len(log_line_parts) == 2:
            log_message = log_line_parts[1]
        else:
            raise ValueError(f'Unexpected log line: {log_line}')

        result.append((severity.strip(), log_message.strip()))

    return result


@pytest.mark.parametrize("message,severity,params,result", [
    ("test", "WARNING", None, "LOGF(\"test\", \"WARNING\");"),
    ("test %1%", "INFO", ["test_param"], "LOGF(\"test %1%\", \"INFO\", \"test_param\");"),
    ("test %1% %2%", "INFO", ["test_param", "other"], "LOGF(\"test %1% %2%\", \"INFO\", \"test_param\", \"other\");"),
])
def test_logf_str(message, severity, params, result):
    if params:
        assert otp.logf(message, severity, *params) == result
    else:
        assert otp.logf(message, severity) == result


@pytest.mark.parametrize("severity,exception_expected", [
    ("INFO", False), ("WARNING", False), ("ERROR", False), ("TEST", True),
])
def test_logf_severity_values(severity, exception_expected):
    if exception_expected:
        with pytest.raises(ValueError):
            otp.logf("Test", severity)
    else:
        otp.logf("Test", severity)


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='LOGF is not supported in WebAPI')
def test_logf_per_tick(f_session):
    def test_script(tick):
        otp.logf("Tick with value X=%1% processed", "INFO", tick["X"])

    t = otp.Ticks({"X": [1, 2, 3]})
    t = t.script(test_script)
    otp.run(t)

    log_lines = _load_log_file(f_session)

    assert len(log_lines) == 3

    for i in range(3):
        assert log_lines[i] == ("INFO", f"Tick with value X={i + 1} processed")


def _check_results(log_lines, expected_result):
    assert len(log_lines) == len(expected_result)
    assert log_lines == expected_result


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='LOGF is not supported in WebAPI')
def test_source_logf(f_session):
    data = otp.Ticks(X=[1, 2, 3])
    src = data.logf("test", "WARNING")
    otp.run(src)

    log_lines = _load_log_file(f_session)
    _check_results(log_lines, 3 * [("WARNING", "test")])


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='LOGF is not supported in WebAPI')
def test_source_logf_with_args(f_session):
    data = otp.Ticks(X=[1, 2, 3])
    src = data.logf("test %1% %2%", "WARNING", "123", "test")
    otp.run(src)

    log_lines = _load_log_file(f_session)
    _check_results(log_lines, 3 * [("WARNING", "test 123 test")])


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='LOGF is not supported in WebAPI')
def test_source_logf_with_where(f_session):
    data = otp.Ticks(X=[1, 2, 3])
    where = (data["X"] == 2)
    src = data.logf("test", "INFO", where=where)
    otp.run(src)

    log_lines = _load_log_file(f_session)
    _check_results(log_lines, [("INFO", "test")])


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='LOGF is not supported in WebAPI')
def test_source_logf_with_where_and_args(f_session):
    data = otp.Ticks(X=[1, 2, 3])
    where = (data["X"] == 2)
    src = data.logf("test %1% %2%", "INFO", "123", "test", where=where)
    otp.run(src)

    log_lines = _load_log_file(f_session)
    _check_results(log_lines, [("INFO", "test 123 test")])


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='LOGF is not supported in WebAPI')
def test_source_logf_with_all_args(f_session):
    data = otp.Ticks(X=[1, 2, 3])
    where = (data["X"] == 2)
    data.logf("test %1% %2%", "INFO", "123", "test", where=where, inplace=True)
    otp.run(data)

    log_lines = _load_log_file(f_session)
    _check_results(log_lines, [("INFO", "test 123 test")])
