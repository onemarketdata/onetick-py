import os
import pytest
from datetime import datetime

# do not remove it, it allows to load OneTick dynamic libraries correctly,
# because it seems that OneTick resolves loading path using the order of the sys.modules
import onetick.py as otp
from onetick.py.otq import otq
from onetick.py.utils.temp import WEBAPI_TEST_MODE_SHARED_CONFIG


# TODO: set to True after https://onemarketdata.atlassian.net/browse/BDS-345 is fixed
otp.config.show_stack_info = False

# PY-394: set defaults to previous values to be backward-compatible with old tests
otp.config['tz'] = 'EST5EDT'
otp.config['default_db'] = 'DEMO_L1'
otp.config['default_symbol'] = 'AAPL'
otp.config['default_start_time'] = datetime(2003, 12, 1, 0, 0, 0)
otp.config['default_end_time'] = datetime(2003, 12, 4, 0, 0, 0)

collect_ignore = ['setup.py']
pytest_plugins = ['onetick.test']


if 'ONE_TICK_CONFIG' in os.environ:
    del os.environ['ONE_TICK_CONFIG']


otq.API_CONFIG['SHOW_STACK_WARNING'] = 0


@pytest.fixture(scope='module')
def session(m_session):
    yield m_session


@pytest.hookimpl(hookwrapper=True)
def pytest_sessionstart(session):
    from onetick.test.utils import setup_random_seed
    setup_random_seed()

    # execute all other hooks to obtain the report
    # required, because the hookwrapper is implemented as generator
    _ = yield

    if os.getenv('OTP_WEBAPI_TEST_MODE'):
        os.system(f'rm -rf {WEBAPI_TEST_MODE_SHARED_CONFIG}/*')


@pytest.fixture(scope='session')
def cloud_server():
    yield otp.RemoteTS(
        otp.LoadBalancing(
            "development-queryhost.preprod-solutions.parent.onetick.com:50015",
            "development-queryhost.preprod-solutions.parent.onetick.com:50016",
            "development-queryhost.preprod-solutions.parent.onetick.com:50017",
            "development-queryhost.preprod-solutions.parent.onetick.com:50018",
            "development-queryhost.preprod-solutions.parent.onetick.com:50019",
            "development-queryhost.preprod-solutions.parent.onetick.com:50020",
        )
    )
