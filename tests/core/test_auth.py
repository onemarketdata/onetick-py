import os
import sys
import signal
import subprocess
from multiprocessing import Process, Queue

import pytest

import onetick.py as otp
from onetick.py.compatibility import is_supported_otq_run_password


if os.getenv('OTP_WEBAPI_TEST_MODE'):
    pytest.skip(allow_module_level=True,
                reason='Binaries (tick_server.exe) is not available in WebAPI')


@pytest.fixture(scope='module')
def tick_server():
    def run_server(q):
        users_file = otp.utils.TmpFile()
        with open(users_file, 'w') as f:
            f.write('<Version id="1">\n'
                    '  <one_tick_users>\n'
                    '    <user username="user" password="password"/>\n'
                    '  </one_tick_users>\n'
                    '</Version>\n')
        cfg = otp.Config()
        with open(cfg.path, 'a') as f:
            f.write('AUTHENTICATION_SCOPE=EACH_REQUEST\n'
                    'AUTHENTICATION_TYPE=OT\n'
                    'ENCRYPTION_TYPE=openssl\n'
                    f'OT_AUTHENTICATION.PASSWORD_FILE={users_file._path}\n')
        with otp.Session(cfg) as s:
            db = otp.DB('TEST_DB')
            s.use(db)

            ot_path = os.path.join(otp.utils.omd_dist_path(), 'one_tick', 'bin', 'tick_server.exe')
            with subprocess.Popen([ot_path, '-port', '47001'],
                                  env=dict(ONE_TICK_CONFIG=os.environ['ONE_TICK_CONFIG'])) as p:
                q.put(p.pid)
                q.put(os.environ['ONE_TICK_CONFIG'])

    queue = Queue()
    tick_server = Process(target=run_server, args=(queue,))
    tick_server.start()

    tick_server_pid = queue.get()
    tick_server_cfg = queue.get()

    yield tick_server_cfg

    os.kill(tick_server_pid, signal.SIGTERM)


@pytest.mark.skipif(os.name == 'nt', reason='We do not have OneTick server on the windows')
@pytest.mark.skipif(not is_supported_otq_run_password(), reason='password not supported on older OneTick versions')
def test_password(tick_server):
    with otp.Session(otp.Config(locator=otp.RemoteTS('localhost:47001'))):
        t = otp.Tick(A=1)
        t['USERNAME'] = otp.raw('GETUSER()', otp.string[64])
        t['AUTHENTICATED_USERNAME'] = otp.raw('GET_AUTHENTICATED_USERNAME()', otp.string[64])
        df = otp.run(t, alternative_username='user', password='password')
        assert list(df['A']) == [1]
        assert list(df['AUTHENTICATED_USERNAME']) == ['user']
        with pytest.raises(Exception, match='OT authentication failed'):
            otp.run(t, alternative_username='user', password='WRONG')
        with pytest.raises(Exception, match='OT authentication failed'):
            otp.run(t, alternative_username='WRONG', password='WRONG')


@pytest.mark.skipif(os.name == 'nt', reason='We do not have OneTick server on the windows')
@pytest.mark.skipif(not is_supported_otq_run_password(), reason='password not supported on older OneTick versions')
def test_data_source_and_config(tick_server):
    with otp.Session(otp.Config(locator=otp.RemoteTS('localhost:47001'))):
        with pytest.warns(UserWarning, match='OT authentication failed'):
            otp.DataSource('TEST_DB', tick_type='TT', symbols='S')
    with otp.Session(otp.Config(locator=otp.RemoteTS('localhost:47001'))):
        otp.config.default_auth_username = 'user'
        otp.config.default_password = 'password'
        otp.DataSource('TEST_DB', tick_type='TT', symbols='S')
        otp.config.default_auth_username = otp.config.default
        otp.config.default_password = otp.config.default


@pytest.mark.skipif(os.name == 'nt', reason='We do not have OneTick server on the windows')
@pytest.mark.skipif(not is_supported_otq_run_password(), reason='password not supported on older OneTick versions')
def test_data_source_and_env_config(tick_server):
    with otp.Session(otp.Config(locator=otp.RemoteTS('localhost:47001'))):
        with pytest.warns(UserWarning, match='OT authentication failed'):
            otp.DataSource('TEST_DB', tick_type='TT', symbols='S')
    with otp.Session(otp.Config(locator=otp.RemoteTS('localhost:47001'))):
        os.environ['OTP_DEFAULT_AUTH_USERNAME'] = 'user'
        os.environ['OTP_DEFAULT_PASSWORD'] = 'password'
        otp.DataSource('TEST_DB', tick_type='TT', symbols='S')
        del os.environ['OTP_DEFAULT_AUTH_USERNAME']
        del os.environ['OTP_DEFAULT_PASSWORD']
