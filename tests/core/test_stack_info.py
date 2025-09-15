import os
import pytest

import onetick.py as otp
from onetick.py.compatibility import is_supported_stack_info
from onetick.py.otq import otq


pytestmark = pytest.mark.skipif(not is_supported_stack_info(),
                                reason='stack_info does not work on old OneTick versions')


class TestStackInfo:
    def test_stack_info(self, session, monkeypatch):
        monkeypatch.setitem(otq.API_CONFIG, 'SHOW_STACK_INFO', 1)

        node = otq.TickGenerator(bucket_interval=0, fields='string A ==== "a"')
        src = otp.Source(node)
        with pytest.raises(Exception) as e:
            otp.run(src, symbols='LOCAL::')

        assert 'stack_info=' in str(e.value)
        if otp.config.show_stack_info:
            assert """node = otq.TickGenerator(bucket_interval=0, fields='string A ==== "a"')""" in str(e.value)

    def test_sink(self, session, monkeypatch):
        monkeypatch.setitem(otq.API_CONFIG, 'SHOW_STACK_INFO', 1)

        src = otp.Tick(A=1)
        src.sink(otq.UpdateField('B', '1'))
        with pytest.raises(Exception) as e:
            otp.run(src)

        assert 'stack_info=' in str(e.value)
        if otp.config.show_stack_info:
            assert "src.sink(otq.UpdateField('B', '1'))" in str(e.value)

    def test_otp_config(self, session, monkeypatch):
        monkeypatch.setattr(otp.config, 'show_stack_info', True)

        src = otp.Tick(A=1)
        src.sink(otq.UpdateField('B', '1'))
        with pytest.raises(Exception) as e:
            otp.run(src)

        assert 'stack_info=' in str(e.value)
        assert "src.sink(otq.UpdateField('B', '1'))" in str(e.value)

    def test_no_stack_info(self, session, monkeypatch):
        monkeypatch.setattr(otp.config, 'show_stack_info', False)

        src = otp.Tick(A=1)
        src.sink(otq.UpdateField('B', '1'))
        with pytest.raises(Exception) as e:
            otp.run(src)

        assert 'stack_info=' not in str(e.value)
        assert "src.sink(otq.UpdateField('B', '1'))" not in str(e.value)

    def test_symbol_name(self, session, monkeypatch):
        monkeypatch.setattr(otp.config, 'show_stack_info', True)
        t = otp.Tick(A=1)
        t.sink(otq.UpdateField('B', '1'))
        with pytest.raises(Exception) as e:
            otp.run(t)
        assert ',symbol_name=' in str(e.value)
        assert "t.sink(otq.UpdateField('B', '1'))" in str(e.value)

    def test_symbol_param(self, session, monkeypatch):
        monkeypatch.setattr(otp.config, 'show_stack_info', True)
        t = otp.Tick(A=1)
        t['B'] = t.Symbol['X', str]
        with pytest.raises(Exception) as e:
            otp.run(t)
        assert 'stack_info=' not in str(e.value)

    @pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                        reason='PY-963: fix this test for webapi, stack_info= somehow is absent here')
    def test_builtin_fun(self, session, monkeypatch):
        monkeypatch.setattr(otp.config, 'show_stack_info', True)
        t = otp.Tick(A=1)
        t['B'] = otp.raw("round('wrong')", int)
        with pytest.raises(Exception) as e:
            otp.run(t)
        assert 'stack_info=' in str(e.value)


@pytest.mark.parametrize('show_stack_info', (False, True))
def test_unique_name(session, show_stack_info, monkeypatch):
    monkeypatch.setattr(otp.config, 'show_stack_info', show_stack_info)

    symbol_query = otq.GraphQuery(
        otq.FindDbSymbols(pattern=otp.config.default_symbol).symbol(f'{otp.config.default_db}::').tick_type('ALL')
    )
    stage1_query = f'eval({symbol_query.unique_name})'

    stage2_query = otp.DataSource(db=otp.config.default_db, tick_type='TRD', schema_policy='manual')
    otp.run(
        stage2_query,
        symbols=stage1_query,
        start=otp.config.default_start_time,
        end=otp.config.default_end_time,
        timezone=otp.config.tz,
    )
