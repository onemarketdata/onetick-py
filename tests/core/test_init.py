from onetick.py import __validate_onetick_query_integration as validate


class TestBinLookup:

    def test_skip_env(self, monkeypatch):
        monkeypatch.setenv("PYTHONPATH", "")
        monkeypatch.setenv("OTP_SKIP_OTQ_VALIDATION", "1")
        validate()
