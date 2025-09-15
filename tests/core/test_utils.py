import os
import sys
import gc
import tempfile
import getpass
import pytest
import onetick.py as otp
import importlib
import datetime
import pandas as pd
import pytz
import dateutil.tz

from pathlib import Path
from onetick.py.utils import TmpFile
from onetick.py.backports import zoneinfo


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='OneTick distribution is not available in WebAPI mode')
@pytest.mark.platform("linux")
def test_omd_dist_path():
    assert otp.utils.omd_dist_path() == os.path.join("/", "opt", "one_market_data")


class TestAbspathByOtqpath:
    def test_found(self):
        OTQ_PATH1 = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        OTQ_PATH2 = os.path.normpath(os.path.join(OTQ_PATH1, "..", "docs"))
        OTQ_PATH = ",".join([OTQ_PATH2, OTQ_PATH1])

        res = otp.utils.abspath_to_query_by_otq_path(OTQ_PATH, "/otqs/combine.otq")

        assert res == os.path.normpath(os.path.join(OTQ_PATH1, "otqs", "combine.otq"))

    def test_not_found(self):
        OTQ_PATH1 = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        OTQ_PATH2 = os.path.normpath(os.path.join(OTQ_PATH1, "..", "docs"))
        OTQ_PATH = ",".join([OTQ_PATH2, OTQ_PATH1])

        with pytest.raises(FileNotFoundError):
            otp.utils.abspath_to_query_by_otq_path(OTQ_PATH, "/otqs/abc/combine.otq")


# Run it separatetely, because it breaks other tests since our internal mechanic does not assume
# having multiple places, it considers these cases as a concurrent run and handles
# it in a special way
@pytest.mark.skip('run this test separately')
def test_generated_path(cur_dir, monkeypatch):
    tmpdir = otp.utils.TmpDir()

    assert 'OTP_BASE_FOLDER_FOR_GENERATED_RESOURCE' not in os.environ
    assert os.path.join(tempfile.gettempdir(), 'test_' + getpass.getuser()) in tmpdir.path

    monkeypatch.setenv('OTP_BASE_FOLDER_FOR_GENERATED_RESOURCE', str(cur_dir + 'tmp'))

    importlib.reload(otp.utils)
    monkeypatch.setattr(otp.utils, 'TMP_CONFIGS_DIR', lambda: str(cur_dir + 'tmp'))

    tmpdir = otp.utils.TmpDir()

    assert str(cur_dir + 'tmp') in tmpdir.path
    assert os.path.join(tempfile.gettempdir(), 'test_' + getpass.getuser()) not in tmpdir.path

    with otp.Session():
        data = otp.Ticks(X=[1, 2, 3])
        assert data.count() == 3


@pytest.mark.parametrize(
    'obj_class',
    [otp.utils.TmpDir, otp.utils.TmpFile]
)
def test_keep_everything_generated(obj_class):
    obj_class.keep_everything_generated = True
    obj_1 = obj_class(clean_up=True)
    assert obj_1.need_to_cleanup is False
    obj_2 = obj_class(clean_up=False)
    assert obj_2.need_to_cleanup is False

    obj_class.keep_everything_generated = False
    obj_3 = obj_class(clean_up=True)
    assert obj_3.need_to_cleanup is True
    obj_4 = obj_class(clean_up=False)
    assert obj_4.need_to_cleanup is False


@pytest.mark.parametrize('dt,tz', [
    (otp.date(2022, 1, 1), None),
    (datetime.date(2022, 1, 1), None),
    (datetime.datetime(2022, 1, 1), None),
    (datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc), 'UTC'),
    (datetime.datetime(2022, 1, 1, tzinfo=pytz.timezone('Europe/London')), 'Europe/London'),
    (datetime.datetime(2022, 1, 1, tzinfo=dateutil.tz.gettz('Asia/Yerevan')), 'Asia/Yerevan'),
    (datetime.datetime(2022, 1, 1, tzinfo=zoneinfo.ZoneInfo('Europe/Kyiv')), 'Europe/Kyiv'),
    (pd.Timestamp(2022, 1, 1), None),
    (pd.Timestamp(2022, 1, 1).tz_localize('America/New_York'), 'America/New_York'),
    (otp.datetime(2022, 1, 1), None),
    (otp.datetime(2022, 1, 1, tz='Europe/Moscow'), 'Europe/Moscow'),
    (otp.datetime(2022, 1, 1, tz='America/New_York'), 'America/New_York'),
    (otp.datetime(2022, 1, 1, tz='America/Chicago'), 'America/Chicago'),
])
def test_get_timezone_from_datetime(dt, tz):
    assert otp.utils.get_timezone_from_datetime(dt) == tz


@pytest.mark.parametrize('dt,tz', [
    (datetime.datetime(2022, 1, 1, tzinfo=dateutil.tz.gettz('America/New_York')), 'America/New_York'),
    (datetime.datetime(2022, 1, 1, tzinfo=dateutil.tz.gettz('America/Chicago')), 'America/Chicago'),
])
@pytest.mark.xfail(sys.platform == 'win32', reason='May return wrong timezone on Windows')
def test_get_timezone_from_dateutil_tz_on_windows(dt, tz):
    assert otp.utils.get_timezone_from_datetime(dt) == tz


def test_cool_tmp_names(monkeypatch):
    f = otp.utils.TmpFile()
    d = otp.utils.TmpDir()
    assert len(Path(f).name.split('-')) == 2
    assert len(Path(d).name.split('-')) == 2


def test_rel_path():
    f = otp.utils.TmpFile(name='generated_file_name')
    sf = otp.utils.TmpFile(suffix='_suffix', name='generated_file_name')
    d = otp.utils.TmpDir(rel_path='generated_dir_name')
    assert Path(sf).name == 'generated_file_name_suffix'
    assert Path(f).name == 'generated_file_name'
    assert Path(d).name == 'generated_dir_name'
