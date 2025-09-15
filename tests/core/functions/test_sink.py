import onetick.py as otp
from onetick.py.otq import otq


def test_sink(session):
    src = otp.Ticks({"A": [1]})
    src.sink(otq.UpdateField(field='A', value='A+1'))
    res = otp.run(src)
    assert res['A'][0] == 2


def test_sink_rshift(session):
    src_old = otp.Ticks({"A": [1]})
    src_new = src_old >> otq.UpdateField(field='A', value='A+1')
    res = otp.run(src_old)
    assert res['A'][0] == 1
    assert id(src_old) != id(src_new)
    res_new = otp.run(src_new)
    assert res_new['A'][0] == 2


def test_sink_irshift(session):
    src = otp.Ticks({"A": [1]})
    id_old = id(src)
    src >>= otq.UpdateField(field='A', value='A+1')
    id_new = id(src)
    assert id_old != id_new
    res = otp.run(src)
    assert res['A'][0] == 2
