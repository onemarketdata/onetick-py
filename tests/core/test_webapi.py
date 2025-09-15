import os
import pytest
import onetick.py as otp
from onetick.py.otq import otq


@pytest.mark.skipif(not os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='Tests only for WebAPI authorization.')
class TestWebApiAuth:
    def test_tomcat_auth(self, monkeypatch):
        monkeypatch.setattr(otp.config, 'http_username', 'webapi')
        monkeypatch.setattr(otp.config, 'http_password', 'password')
        monkeypatch.setattr(otp.config, 'http_address', 'http://tickserver:8080')
        src = otp.Tick(Y=1)
        df = otp.run(src)
        assert len(df) == 1

    def test_tomcat_auth_param(self, monkeypatch):
        monkeypatch.setattr(otp.config, 'http_address', 'http://tickserver:8080')
        src = otp.Tick(Y=1)
        df = otp.run(src, username="webapi", password="password")
        assert len(df) == 1

    def test_tomcat_not_authorized(self, monkeypatch):
        monkeypatch.setattr(otp.config, 'http_address', 'http://tickserver:8080')
        src = otp.Tick(Y=1)
        with pytest.raises(otq.exception.OneTickException):
            otp.run(src, username="nonexisted", password="password")

    def test_tomcat_not_authorized_with_config(self, monkeypatch):
        monkeypatch.setattr(otp.config, 'http_address', 'http://tickserver:8080')
        monkeypatch.setattr(otp.config, 'http_username', 'nonexisted')
        monkeypatch.setattr(otp.config, 'http_password', 'password')
        src = otp.Tick(Y=1)
        with pytest.raises(otq.exception.OneTickException):
            otp.run(src)

    def test_http_session(self):
        s = otp.HTTPSession(http_address='http://tickserver:8080',
                            http_username="webapi",
                            http_password="password")
        df = otp.run(otp.Tick(Y=1))
        assert len(df) == 1
        s.close()

    def test_http_session_context(self):
        with otp.HTTPSession(http_address='http://tickserver:8080',
                             http_username="webapi",
                             http_password="password"):
            df = otp.run(otp.Tick(Y=1))
            assert len(df) == 1
        # check it is restored well
        assert otp.config.http_address == os.getenv('OTP_HTTP_ADDRESS')
        assert otp.config.http_username is None
        assert otp.config.http_password is None


def test_join_bug(f_session):
    # не получилось воспроизвести
    src1 = otp.Ticks(data=dict(X=[1, 2, 3]), symbol="S1", tick_type="TT")
    src2 = otp.Ticks(data=dict(Y=[1, 2, 3]), symbol="S1", tick_type="TX")
    data = src2.join_with_query(src1, how='inner', )
    df = otp.run(data)
    print(df)


class TestWithBoundUnboundCases:
    @pytest.fixture(scope="class")
    def db_a(self):
        db = otp.DB(name="DB_A")

        db.add(otp.Ticks(dict(X=[1, 2, 3])), symbol="S1", tick_type="TT")
        db.add(otp.Ticks(dict(X=[-3, -2, -1])), symbol="S2", tick_type="TT")
        db.add(otp.Ticks(dict(X=[-3, -2, -1])), symbol="S3", tick_type="TT3")

        yield db

    @pytest.fixture(scope="class")
    def db_b(self):
        db = otp.DB(name="DB_B")
        db.add(otp.Ticks(dict(X=[6, 7, 8])), symbol="S2", tick_type="TT")

        yield db

    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session, db_a, db_b):
        c_session.use(db_a, db_b)
        yield c_session

    def test_tt_as_param(self):
        ''' Check that tick type can be passed as a parameter to the query '''

        def logic(param):
            return otp.DataSource(db='DB_A', symbol='S1', tick_type=param)

        data = otp.Ticks(TICK_TYPE=['TT', 'TX'])

        data = data.join_with_query(logic, params=dict(param=data['TICK_TYPE']))
        df = otp.run(data)
        assert len(df) == 4
        assert all(df['X'] == [1, 2, 3, 0])
        assert all(df['TICK_TYPE'] == ['TT', 'TT', 'TT', 'TX'])


@pytest.mark.skipif(not os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='Test only for WebAPI')
def test_webapi_callback_process_ticks(session):
    class TestCallback(otp.CallbackBase):
        def __init__(self):
            self.process_ticks_result = []
            super().__init__()

        def process_ticks(self, ticks):
            self.process_ticks_result.append(ticks.copy())

    data = otp.Ticks(X=[1, 2, 3, 4, 5], symbol="S1", tick_type="TT")
    cb = TestCallback()

    _ = otp.run(data, callback=cb)

    assert len(cb.process_ticks_result) == 1
    assert cb.process_ticks_result[0]['X'].tolist() == [1, 2, 3, 4, 5]
