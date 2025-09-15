''' Test a case when an user points to an exisintg one tick config
with custom (ie not /license) location for license '''

import os
import pytest
import onetick.py as otp


@pytest.mark.skip(reason='It depends on the license location specified in the onetick.cfg')
def test(cur_dir, monkeypatch):

    monkeypatch.setenv('ACL_PATH', os.path.join(cur_dir, 'cfg', 'acl.txt'))
    monkeypatch.setenv('LOCATOR_PATH', os.path.join(cur_dir, 'cfg', 'locator.default'))
    monkeypatch.setenv('CUR_DIR', os.path.join(cur_dir))

    with otp.Session(os.path.join(cur_dir, 'cfg', 'custom-license.cfg')):
        assert len(otp.run(otp.Tick(X=1))) == 1
