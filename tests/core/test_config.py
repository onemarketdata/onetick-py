import onetick.py as otp
from onetick.py.compatibility import is_supported_stack_info, is_supported_pnl_realized, OnetickVersionFromServer
from onetick.py.otq import otq
import pytest

from datetime import datetime
from pathlib import Path


@pytest.fixture(scope='function')
def config_preserving_session(session, monkeypatch):
    saved_options = {
        option: otp.config[option]
        for option in otp.config.get_changeable_config_options()
    }
    yield session
    for option, value in saved_options.items():
        otp.config[option] = value


def test_option_access_syntax(config_preserving_session):
    default_tz = otp.config.tz
    assert otp.config.tz == default_tz
    assert otp.config['tz'] == default_tz
    otp.config.tz = 'GMT'
    assert otp.config.tz == 'GMT'
    assert otp.config['tz'] == 'GMT'
    otp.config['tz'] = 'America/Chicago'
    assert otp.config.tz == 'America/Chicago'
    assert otp.config['tz'] == 'America/Chicago'


def test_option_default_assignment(config_preserving_session):
    otp.config.tz = otp.config.default
    default_tz = otp.config.tz
    otp.config.tz = 'non-default tz'
    assert otp.config.tz == 'non-default tz'
    otp.config.tz = otp.config.default
    assert otp.config.tz == default_tz


def test_non_existing_property_assignment(config_preserving_session):
    with pytest.raises(AttributeError):
        otp.config.non_existing_property = 'SOME_VALUE'


def test_derived_properties(config_preserving_session):
    default_db = otp.config.default_db
    default_symbol = otp.config.default_symbol
    assert otp.config.default_db_symbol == f'{default_db}::{default_symbol}'
    otp.config.default_db = 'TEST_DB'
    assert otp.config.default_db_symbol == f'TEST_DB::{default_symbol}'
    otp.config.default_symbol = 'TEST_SYMBOL'
    assert otp.config.default_db_symbol == 'TEST_DB::TEST_SYMBOL'


def test_assigning_derived_properties(config_preserving_session):
    with pytest.raises(AttributeError):
        otp.config.default_db_symbol = 'TEST_DB::TEST_SYMBOL'


def test_assigning_wrong_type(config_preserving_session):
    otp.config.tz = 'GMT'
    with pytest.raises(ValueError):
        otp.config.tz = 123
    otp.config.default_start_time = datetime(2001, 5, 3)
    with pytest.raises(ValueError):
        otp.config.default_start_time = 'today'


def test_env_variables_parsers(config_preserving_session, monkeypatch):
    for option in otp.config.get_changeable_config_options():
        monkeypatch.setattr(otp.config, option, otp.config.default)
    monkeypatch.setenv('OTP_DEFAULT_START_TIME', '2003/12/01 00:00:00')
    assert otp.config.default_start_time == datetime(2003, 12, 1)
    monkeypatch.setenv('OTP_DEFAULT_START_TIME', '2003/12/01 00:00:00.123')
    assert otp.config.default_start_time == datetime(2003, 12, 1, 0, 0, 0, 123000)
    monkeypatch.setenv('OTP_DEFAULT_END_TIME', '2003/12/04 00:00:00')
    assert otp.config.default_end_time == datetime(2003, 12, 4)
    monkeypatch.setenv('OTP_DEFAULT_END_TIME', '2003/12/04 00:00:00.123456')
    assert otp.config.default_end_time == datetime(2003, 12, 4, 0, 0, 0, 123456)


def test_env_variables_parsers_for_license(config_preserving_session, monkeypatch):
    for option in otp.config.get_changeable_config_options():
        monkeypatch.setattr(otp.config, option, otp.config.default)
    monkeypatch.setenv('OTP_DEFAULT_LICENSE_DIR', '/license0')
    assert otp.config.default_license_dir == '/license0'
    monkeypatch.setenv('OTP_DEFAULT_LICENSE_DIR', '/license1')
    assert otp.config.default_license_dir == '/license1'


def test_default_nothing(config_preserving_session, monkeypatch):
    for option in ('default_db', 'default_symbol', 'default_start_time', 'default_end_time'):
        monkeypatch.setattr(otp.config, option, otp.config.default)
        with pytest.raises(ValueError, match=f'onetick.py.config.{option} is not set!'):
            _ = getattr(otp.config, option)


def test_default_config(config_preserving_session, monkeypatch):
    for option in otp.config.get_changeable_config_options():
        monkeypatch.setattr(otp.config, option, otp.config.default)
    tmp_path = otp.utils.TmpFile().path
    monkeypatch.setenv('OTP_DEFAULT_CONFIG_PATH', tmp_path)

    Path(tmp_path).write_text('WRONG_OPTION=12345\n')
    with pytest.raises(ValueError):
        _ = otp.config.default_license_dir

    Path(tmp_path).write_text('OTP_DEFAULT_LICENSE_DIR=/tmp/license\n'
                              'OTP_DEFAULT_END_TIME="2022/01/01 00:00:00.123456"\n'
                              'OTP_DEFAULT_TZ=Europe/Madrid')
    assert otp.config.default_license_dir == '/tmp/license'
    assert otp.config.tz == 'Europe/Madrid'
    assert otp.config.default_end_time == datetime(2022, 1, 1, 0, 0, 0, 123456)


@pytest.mark.skipif(is_supported_stack_info(), reason='stack_info does not work on old versions')
def test_show_stack_info(config_preserving_session, monkeypatch):
    assert 'show_stack_info' in otp.config.get_changeable_config_options()

    def _check_stack_info():
        t = otp.Tick(A=1)
        t.sink(otq.UpdateField('B', 1))
        with pytest.raises(Exception) as e:
            _ = otp.run(t)
        return 'stack_info=' in str(e.value)

    otp.config.show_stack_info = otp.config.default
    assert _check_stack_info() is False

    monkeypatch.setenv('OTP_SHOW_STACK_INFO', 'YES')
    assert otp.config.show_stack_info is True

    otp.config.show_stack_info = False
    assert _check_stack_info() is False


def test_otp_dt_default_times(config_preserving_session):
    data = otp.Tick(A=1)
    otp.config['default_start_time'] = otp.dt(2023, 2, 3)
    otp.config['default_end_time'] = otp.dt(2023, 2, 4)
    df = otp.run(data)
    assert df['Time'][0] == otp.dt(2023, 2, 3)
    data = data.agg({'SUM': otp.agg.sum(data['A'])})
    df = otp.run(data)
    assert df['Time'][0] == otp.dt(2023, 2, 4)


def test_context_manager(config_preserving_session):
    original = otp.config.context
    with otp.config('context', 'TEST_123'):
        assert otp.config.context != original
        assert otp.config.context == 'TEST_123'

    assert otp.config.context == original


def test_context_manager_multiple(config_preserving_session):
    original_context = otp.config.context
    original_default_symbol = otp.config.default_symbol

    with otp.config('context', 'TEST_123'), otp.config('default_symbol', 'TEST_SYMBOL'):
        assert otp.config.context != original_context
        assert otp.config.context == 'TEST_123'

        assert otp.config.default_symbol != original_default_symbol
        assert otp.config.default_symbol == 'TEST_SYMBOL'

    assert otp.config.context == original_context
    assert otp.config.default_symbol == original_default_symbol


@pytest.mark.parametrize('disable_compatibility_checks', [True, False])
def test_disable_compatibility_checks(config_preserving_session, mocker, disable_compatibility_checks):
    otp.config.disable_compatibility_checks = disable_compatibility_checks

    mock = mocker.patch(
        'onetick.py.compatibility.get_onetick_version',
        return_value=OnetickVersionFromServer(False, None, None, 20221111120000, 'LOCAL', otp.config.context),
    )

    if disable_compatibility_checks:
        assert is_supported_pnl_realized()
        mock.assert_not_called()
    else:
        assert not is_supported_pnl_realized()
        mock.assert_called()
