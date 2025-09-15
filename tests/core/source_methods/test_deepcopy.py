import pytest

import onetick.py as otp
from onetick.py.otq import otq


class TestSimple:

    def test_deep_copy(self, session):
        t = otp.Ticks(A=[1, 2, 3], B=[4, 5, 6]).agg({'A': otp.agg.sum('A')})
        t1 = t.copy(deep=True)
        t2 = t1.copy().copy().copy()
        t3 = t2.deepcopy().deepcopy().deepcopy()
        t4 = t.copy(otq.Table('A, T4 int (4)'), deep=True)
        t5 = t.deepcopy(columns={'A': int})
        t6 = t.deepcopy()
        t6['B'] = 100

        df = otp.run(t1)
        assert df['A'][0] == 6
        df = otp.run(t2)
        assert df['A'][0] == 6
        df = otp.run(t3)
        assert df['A'][0] == 6
        df = otp.run(t4)
        assert df['A'][0] == 6
        assert df['T4'][0] == 4
        df = otp.run(t5)
        assert 'B' not in df
        assert df['A'][0] == 6
        df = otp.run(t6)
        assert df['B'][0] == 100

        for (a, b), length in (((t, t1), 2),
                               ((t, t2), 2),
                               ((t, t3), 2),
                               ((t, t4), 2),
                               ((t, t5), 2),
                               ((t, t6), 2),
                               ((t1, t2), 1),
                               ((t1, t3), 2),
                               ((t1, t4), 2),
                               ((t2, t3), 2),
                               ((t2, t4), 2),
                               ((t3, t4), 2)):
            df = otp.run(otp.merge([a, b]))
            assert len(df) == length
            assert list(df['A']) == [6] * length


class TestDump:

    @pytest.mark.platform("linux")
    def test_sink_dump(self, session):
        orders = otp.Tick(A=1)
        orders.sink(otq.Table(fields='', keep_input_fields=True))
        orders.dump()
        otp.run(orders)

    def test_sink_deepcopy(self, session):
        orders = otp.Tick(A=1)
        orders.sink(otq.Table(fields='', keep_input_fields=True))
        orders = orders.deepcopy()
        otp.run(orders)

    @pytest.mark.platform("linux")
    def test_dump_deepcopy(self, session):
        orders = otp.Tick(A=1)
        orders.dump()
        orders = orders.deepcopy()
        otp.run(orders)

    @pytest.mark.platform("linux")
    def test_sink_dump_deepcopy(self, session):
        orders = otp.Tick(A=1)
        orders.sink(otq.Table(fields='', keep_input_fields=True))
        orders.dump()
        orders = orders.deepcopy()
        otp.run(orders)
