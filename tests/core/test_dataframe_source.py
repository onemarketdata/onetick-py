import pandas

import onetick.py as otp


def test_dataframe_logic(f_session):
    data = pandas.DataFrame({"A": [1, 2], "B": [3, 4]})
    db = otp.DB(src=data, name="TEST_DATAFRAME_LOGIC")
    f_session.use(db)

    x = otp.DataSource(db)
    res = otp.run(x)

    assert not res.empty
