''' Test checks that code with remote license works fine '''

import pytest
import onetick.py as otp


@pytest.mark.integration
@pytest.mark.skip(reason='remote server is not working in CI')
def test_remote():
    cfg = otp.Config(locator=otp.RemoteTS('172.16.2.198', 50564))

    with otp.Session(cfg):
        data = otp.Ticks(X=[1, 2, 3])

        print(otp.run(data))
