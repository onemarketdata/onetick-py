import pytest
import os
import onetick.py as otp


def _load_log_file(session_obj):
    with open(session_obj._log_file, "r") as log_file:
        return log_file.readlines()


def test_throw_exception_str():
    assert otp.throw_exception("Test message") == "THROW_EXCEPTION(\"Test message\");"


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False), reason='LOGF is not supported')
def test_throw_exception(f_session):
    exception_message = "Tick column X should be greater than zero."

    def test_script(tick):
        if tick["X"] <= 0:
            otp.throw_exception(exception_message)
        else:
            otp.logf(tick["X"].astype(str), "INFO")

    t = otp.Ticks({'X': [1, -2, 6]})

    t = t.script(test_script)
    with pytest.raises(Exception, match=exception_message):
        otp.run(t)

    # check, if exception was thrown on correct tick
    log_lines = _load_log_file(f_session)
    assert len(log_lines) == 1
    assert log_lines[0].strip().split()[-1] == "1"
