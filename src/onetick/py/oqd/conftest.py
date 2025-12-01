import pytest
import onetick.py as otp

# do not remove it, it allows to load OneTick dynamic libraries correctly,
from onetick.py.otq import otq  # noqa: F401


@pytest.fixture(scope="function", autouse=True)
def session(cloud_server):
    cfg = otp.Config(locator=cloud_server)

    with otp.Session(cfg):
        yield
