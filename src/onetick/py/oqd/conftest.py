import pytest
import onetick.py as otp

# do not remove it, it allows to load OneTick dynamic libraries correctly,
from onetick.py.otq import otq


@pytest.fixture(scope="function", autouse=True)
def session(cloud_server, monkeypatch):
    # we assume that cloud server has this database
    # we need to set it to process the queries on cloud server, not proxy
    monkeypatch.setattr(otp.config, 'default_db', 'US_COMP_SAMPLE')

    cfg = otp.Config(locator=cloud_server)

    with otp.Session(cfg):
        yield
