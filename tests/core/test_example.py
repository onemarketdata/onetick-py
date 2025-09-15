import os
import pytest
import numpy as np
import pandas as pd
import math

import onetick.py as otp

DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="function")
def session():
    s = otp.Session()

    yield s

    s.close()


def test_ex_1(session):
    q = otp.query(os.path.join(DIR, "otqs", "orders.otq") + "::source", DB="S_ORDERS_FIX", TT="MY_TT")

    data = otp.Ticks({"X": [1, 2, 3, 4], "Y": [4, 5, 6, 7]})

    db = otp.DB("S_ORDERS_FIX")
    db.add(data, tick_type="MY_TT")

    session.use(db)

    res = otp.Query(q)

    df = otp.run(res)

    assert df.X[0] == 1 and df.Y[0] == 4
    assert df.X[1] == 2 and df.Y[1] == 5
    assert df.X[2] == 3 and df.Y[2] == 6
    assert df.X[3] == 4 and df.Y[3] == 7


@pytest.mark.xfail(reason="JBT behaviour has changed in the corner case")
def test_ex_2(session):
    q = otp.query(os.path.join(DIR, "otqs", "qte_trd.otq") + "::market_info", MD_DB="MD_DB")

    qte_ticks = otp.Ticks({"ASK_PRICE": [34.5, 34.6, 34.7, 34.6], "BID_PRICE": [34.2, 34.3, 34.6, 34.3]})

    trd_ticks = otp.Ticks({"PRICE": [34.35, 34.4, 34.54, 34.49]})

    ord_tick = otp.Ticks({"ID": [1], "PRICE": [34.8], "offset": [2]})

    db = otp.DB("MD_DB")
    db.add(qte_ticks, tick_type="QTE")
    db.add(trd_ticks, tick_type="TRD")

    session.use(db)

    res = ord_tick.apply(q)

    df = otp.run(res)

    assert df.ASK_PRICE[0] == 34.6 and df.BID_PRICE[0] == 34.3 and df.TRD_PRICE[0] == 34.4


@pytest.mark.xfail(reason="JBT behaviour has changed in the corner case")
def test_ex_5():
    def calc_is(orders_df, quotes_df):
        res = pd.concat([orders_df.set_index("offset"), quotes_df.set_index("offset")], axis=1)

        res["F_ASK_PRICE"] = res["ASK_PRICE"].shift(1)
        res["F_BID_PRICE"] = res["BID_PRICE"].shift(1)

        res.loc[np.isnan(res["ASK_PRICE"]), "ASK_PRICE"] = res["F_ASK_PRICE"]
        res.loc[np.isnan(res["BID_PRICE"]), "BID_PRICE"] = res["F_BID_PRICE"]

        res.dropna(inplace=True)

        res["MID_PRICE"] = (res.ASK_PRICE + res.BID_PRICE) / 2

        res["IS"] = (res.VWAP - res.MID_PRICE) / res.MID_PRICE

        return res

    target_q = otp.query(os.path.join(DIR, "otqs", "symbols.otq") + "::merge", MD_DB="QTE_DB", DB="ORDER_DB")

    qte_data_a = {
        "ASK_PRICE": [34.5, 34.6, 34.7, 34.6],
        "BID_PRICE": [34.2, 34.3, 34.6, 34.3],
        "offset": list(range(4)),
    }
    qte_data_b = {
        "ASK_PRICE": [7.9, 8.0, 8.05, 7.95, 8.0, 7.9],
        "BID_PRICE": [7.8, 7.85, 7.9, 7.9, 7.9, 7.86],
        "offset": list(range(6)),
    }

    order_data_a = {"ID": [1, 2], "VWAP": [34.3859, 34.5888], "offset": [2, 5]}
    order_data_b = {"ID": [3, 4], "VWAP": [7.893, 7.943], "offset": [1, 3]}

    pd_res_a = calc_is(pd.DataFrame(order_data_a), pd.DataFrame(qte_data_a))
    pd_res_b = calc_is(pd.DataFrame(order_data_b), pd.DataFrame(qte_data_b))
    pd_res = pd.concat([pd_res_a, pd_res_b])
    pd_res.sort_index(axis=0, inplace=True)

    qte_db = otp.DB("QTE_DB")
    qte_db.add(otp.Ticks(qte_data_a), tick_type="QTE", symbol="A")
    qte_db.add(otp.Ticks(qte_data_b), tick_type="QTE", symbol="B")

    ord_db = otp.DB("ORDER_DB")
    ord_db.add(otp.Ticks(order_data_a), tick_type="ORDER", symbol="A")
    ord_db.add(otp.Ticks(order_data_b), tick_type="ORDER", symbol="B")

    with otp.Session() as session:
        session.use(qte_db)
        session.use(ord_db)

        res = otp.Query(target_q, symbol=None)

        q_res = otp.run(res)

        assert len(q_res) == len(pd_res)

        for q_row, pd_row in zip(q_res.iterrows(), pd_res.iterrows()):
            _, q_row = q_row
            _, pd_row = pd_row

            assert math.isclose(q_row["IS"], pd_row["IS"])
