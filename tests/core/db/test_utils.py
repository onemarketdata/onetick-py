import onetick.py as otp
from onetick.py.db.utils import TmpSession

db_name = "TEST_DB"
symbol = "TEST_SYMBOL"
tick_type = "TEST_TT"
date = otp.config['default_start_time']


class TestTmpSession:
    def test_real_session(self, f_session):
        with TmpSession() as tmpsession:
            assert hasattr(tmpsession, "session")
            assert tmpsession.session == f_session._instance
        assert f_session._instance == f_session

    def test_real_session_unregistered_db_object(self, f_session):
        db = otp.DB("DB")
        with TmpSession() as tmpsession:
            tmpsession.use(db)
            assert "DB" in tmpsession.session.locator.databases
        assert "DB" not in f_session.locator.databases

    def test_real_session_registered_db_object(self, f_session):
        db = otp.DB("DB")
        f_session.use(db)
        with TmpSession() as tmpsession:
            tmpsession.use(db)
            assert "DB" in tmpsession.session.locator.databases
        assert "DB" in f_session.databases and "DB" in f_session.locator.databases

    def test_real_session_registered_db_string(self, f_session):
        db = otp.DB("DB")
        f_session.use(db)
        with TmpSession() as tmpsession:
            tmpsession.use("DB")
            assert "DB" in tmpsession.session.locator.databases
        assert "DB" in f_session.databases and "DB" in f_session.locator.databases

    def test_temp_session(self):
        with TmpSession() as tmpsession:
            assert hasattr(tmpsession, "session")
            assert tmpsession.session is not None
        assert otp.Session._instance is None

    def test_temp_session_db_object(self):
        db = otp.DB("DB")
        with TmpSession() as tmpsession:
            tmpsession.use(db)
            assert "DB" in tmpsession.session.locator.databases

    def test_derived_database_with_session(self, f_session):
        db = otp.DB("DB")
        f_session.use(db)
        with TmpSession() as tmpsession:
            tmpsession.use("DB//ARBITRARY//DERIVED//DATABASE")
