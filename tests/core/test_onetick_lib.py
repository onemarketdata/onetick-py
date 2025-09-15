import onetick.py as otp


class TestAutenticationToken:

    def test_attr(self, f_session):
        otp.OneTickLib().set_authentication_token

    def test_session(self):
        with otp.Session() as session:
            session._lib.set_authentication_token
