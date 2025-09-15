import pytest

import onetick.py as otp


class TestTicks:
    @pytest.fixture(scope="class", autouse=True)
    def session(self, c_session):
        yield c_session

    def test_disjoint_intervals(self):
        ticks1 = otp.Ticks(
            dict(x=[1, 2, 3], offset=[0, 25 * 3600 * 1000, 35 * 3600 * 1000]),
            start=otp.dt(2009, 5, 13),
            end=otp.dt(2009, 5, 15),
        )

        ticks1.Time += 380 * 24 * 3600 * 1000

        ticks2 = otp.Ticks(
            dict(y=[-1, -2, -3], offset=[0, 25 * 3600 * 1000, 35 * 3600 * 1000]),
            start=otp.dt(2011, 12, 1),
            end=otp.dt(2011, 12, 5),
        )

        ticks = ticks1 + ticks2

        df = otp.run(ticks)
        assert all(
            df["Time"]
            == [
                otp.dt(2010, 5, 28),
                otp.dt(2010, 5, 29, 1),
                otp.dt(2010, 5, 29, 11),
                otp.dt(2011, 12, 1),
                otp.dt(2011, 12, 2, 1),
                otp.dt(2011, 12, 2, 11),
            ]
        )

    def test_intersect_intervals(self):
        ticks1 = otp.Ticks(
            dict(x=[1, 2, 3], offset=[0, 25 * 3600 * 1000, 35 * 3600 * 1000]),
            start=otp.dt(2009, 5, 13),
            end=otp.dt(2009, 5, 15),
        )

        ticks2 = otp.Ticks(
            dict(y=[-1, -2, -3], offset=[0, 25 * 3600 * 1000, 35 * 3600 * 1000]),
            start=otp.dt(2009, 5, 14),
            end=otp.dt(2009, 5, 16),
        )

        ticks1.Time += 23 * 3600 * 1000

        ticks = ticks1 + ticks2

        df = otp.run(ticks)

        assert all(
            df["Time"]
            == [
                otp.dt(2009, 5, 13, 23),
                otp.dt(2009, 5, 14),
                otp.dt(2009, 5, 15),
                otp.dt(2009, 5, 15, 1),
                otp.dt(2009, 5, 15, 10),
                otp.dt(2009, 5, 15, 11),
            ]
        )
