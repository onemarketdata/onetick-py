import pytest
import onetick.py as otp

# do not remove it, it allows to load OneTick dynamic libraries correctly,
from onetick.py.otq import otq  # noqa: F401


@pytest.fixture(scope="function", autouse=True)
def session():
    locator = otp.RemoteTS(
        otp.LoadBalancing(
            "development-queryhost.preprod-solutions.parent.onetick.com:50015",
            "development-queryhost-2.preprod-solutions.parent.onetick.com:50015"
        )
    )
    cfg = otp.Config(locator=locator)

    with otp.Session(cfg):
        yield
