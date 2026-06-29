import datetime

import os
import pytest
import onetick.py as otp
from onetick.py.otq import otq


@pytest.fixture(scope='function')
def session(pytestconfig, cloud_server):

    config = otp.Config(
        otq_path=[
            os.path.join(pytestconfig.rootdir, 'doctest_resources')
        ],
    )

    with otp.Session(config) as session_:
        # cloud server is available in doctests
        session_.use(cloud_server)

        session_.use(otp.DB(name='WRITE_DB'))

        yield session_


# pylint: disable=redefined-outer-name
@pytest.fixture(autouse=True)
def add_session(pytestconfig, doctest_namespace, request, session):
    doctest_namespace['otq'] = otq
    doctest_namespace['otp'] = otp
    doctest_namespace['session'] = session
    doctest_namespace['datetime'] = datetime
    doctest_namespace['csv_path'] = os.path.join(pytestconfig.rootdir, 'doctest_resources')
