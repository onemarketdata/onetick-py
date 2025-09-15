import pytest

import onetick.py as otp


@pytest.fixture(scope="module")
def session_with_db(session):
    db = otp.DB("MY_DB")
    db.add(otp.Tick(X=1, T="A"), symbol="AA", tick_type="TT")
    db.add(otp.Tick(X=2), symbol="AAA", tick_type="TT")
    db.add(otp.Tick(X=3), symbol="AAB", tick_type="TT")
    session.use(db)


def test_symbol_name_rewrite(session_with_db):
    data = otp.DataSource(db="MY_DB", tick_type="TT", symbols="AA")
    data = data.modify_symbol_name(symbol_name="AAA")
    result = otp.run(data)

    assert result["X"][0] == 2


def test_symbol_name_by_symbol_name(session_with_db):
    data = otp.DataSource(db="MY_DB", tick_type="TT", symbols="AA")
    data = data.modify_symbol_name(symbol_name=data["_SYMBOL_NAME"] + "B")
    result = otp.run(data)

    assert result["X"][0] == 3


def test_symbol_name_by_column(session_with_db):
    with pytest.raises(Exception, match="SYMBOL_NAME parameter must not depend on ticks"):
        data = otp.DataSource(db="MY_DB", tick_type="TT", symbols="AA")
        data = data.modify_symbol_name(symbol_name=data["T"] + "AB")
        otp.run(data)


def test_symbol_name_by_same_symbol_name(session_with_db):
    data = otp.DataSource(db="MY_DB", tick_type="TT", symbols="AA")
    data = data.modify_symbol_name(symbol_name=data["_SYMBOL_NAME"])
    result = otp.run(data)

    assert result["X"][0] == 1
