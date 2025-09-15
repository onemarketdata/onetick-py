import logging
import onetick.py as otp


def test_default_logging():
    assert otp.config.logging == 'WARNING'
    logger = logging.getLogger(otp.log.ROOT_LOGGER_NAME)
    assert len(logger.handlers) == 2
    assert isinstance(logger.handlers[0], logging.NullHandler)
    assert isinstance(logger.handlers[1], logging.StreamHandler)
    assert logger.level == logging.WARNING


def test_otq_debug_mode(caplog, session, monkeypatch):
    monkeypatch.setattr(otp.config, 'otq_debug_mode', True)
    assert otp.config.otq_debug_mode
    otp.run(otp.Tick(X=1))
    assert 'otq file saved to' in caplog.text


def test_otq_debug_mode_off(caplog, session, monkeypatch):
    monkeypatch.setattr(otp.config, 'otq_debug_mode', False)
    assert not otp.config.otq_debug_mode
    otp.run(otp.Tick(X=1))
    assert 'otq file saved to' not in caplog.text
