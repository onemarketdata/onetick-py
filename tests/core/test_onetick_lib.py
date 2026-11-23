import os
import signal
import subprocess
import sys

import pytest

import onetick.py as otp


class TestAutenticationToken:

    def test_attr(self, f_session):
        otp.OneTickLib().set_authentication_token

    def test_session(self):
        with otp.Session() as session:
            session._lib.set_authentication_token


CRASH_SCRIPT = '''
import ctypes
import resource
import sys
import onetick.py as otp

resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
with open(sys.argv[1], 'w') as python_stack:
    otp.OneTickLib.enable_crash_stacks(file=python_stack, log_link=sys.argv[2])
    with otp.Session():
        if sys.argv[3] == 'crash':
            ctypes.string_at(0)
'''


@pytest.mark.skipif(sys.platform != 'linux' or otp.__webapi__, reason='Linux only, not for WebAPI')
@pytest.mark.parametrize('crash', [True, False])
def test_enable_crash_stacks(tmp_path, crash):
    python_stack, log_link = tmp_path / 'python_stack.log', tmp_path / 'onetick.log'
    res = subprocess.run(
        [sys.executable, '-c', CRASH_SCRIPT, str(python_stack), str(log_link), 'crash' if crash else 'ok'],
        env={**os.environ, 'OTP_BASE_FOLDER_FOR_GENERATED_RESOURCE': str(tmp_path / 'otp')},
        capture_output=True, text=True, timeout=300,
    )
    if crash:
        assert res.returncode == -signal.SIGSEGV, res.stderr
        assert 'Fatal Python error: Segmentation fault' in python_stack.read_text()
        assert 'Dumping call stack trace' in log_link.read_text()
    else:
        assert res.returncode == 0, res.stderr
        assert python_stack.read_text() == ''
        assert not os.path.lexists(log_link)


@pytest.mark.skipif(sys.platform != 'linux' or otp.__webapi__, reason='Linux only, not for WebAPI')
def test_link_log_file(tmp_path, monkeypatch):
    from onetick.lib import instance

    log_link = tmp_path / 'onetick.log'
    monkeypatch.setattr(instance, '_log_link', str(log_link))
    for name in ('first.log', 'second.log'):
        instance._link_log_file(tmp_path / name)
        assert os.readlink(log_link) == str(tmp_path / name)
        assert os.listdir(tmp_path) == ['onetick.log']
    instance._link_log_file()
    instance._link_log_file()
    assert os.listdir(tmp_path) == []
