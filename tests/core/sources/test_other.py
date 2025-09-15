import onetick.py as otp


def test_trades_copy(session):
    data = otp.Trades()
    for d in [data, data.copy(), data.deepcopy()]:
        assert isinstance(d, otp.Trades)


def test_quotes_copy(session):
    data = otp.Quotes()
    for d in [data, data.copy(), data.deepcopy()]:
        assert isinstance(d, otp.Quotes)


def test_nbbo_copy(session):
    data = otp.NBBO()
    for d in [data, data.copy(), data.deepcopy()]:
        assert isinstance(d, otp.NBBO)


def test_orders_copy(session):
    data = otp.Orders()
    for d in [data, data.copy(), data.deepcopy()]:
        assert isinstance(d, otp.Orders)
