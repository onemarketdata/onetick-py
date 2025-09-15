import os
import onetick.py as otp


DIR = os.path.dirname(os.path.abspath(__file__))


def painting_the_tape(orders, window_size_sec=1000, max_trade_ratio=0.7, max_volume_ratio=0.7, min_num_trades=20):
    orders, _ = orders[(orders.STATE == "F") | (orders.STATE != "PF")]

    orders.IS_BUY_TRADE = orders.BUY_FLAG
    orders.IS_SELL_TRADE = 1 - orders.BUY_FLAG
    orders.QTY_FILLED_BUY = 0
    orders.QTY_FILLED_SELL = 0
    orders = orders.update({orders.QTY_FILLED_BUY: orders.QTY_FILLED}, where=(orders.BUY_FLAG == 1))
    orders = orders.update({orders.QTY_FILLED_SELL: orders.QTY_FILLED}, where=(orders.BUY_FLAG == 0))

    agg_ord = orders.agg(
        {
            "NUM_BUY_TRADES": otp.agg.sum(orders.IS_BUY_TRADE),
            "NUM_SELL_TRADES": otp.agg.sum(orders.IS_SELL_TRADE),
            "BUY_VOLUME": otp.agg.sum(orders.QTY_FILLED_BUY),
            "SELL_VOLUME": otp.agg.sum(orders.QTY_FILLED_SELL),
        },
        running=True,
        all_fields=False,
        bucket_interval=window_size_sec,
    )

    agg_ord.VOLUME_RATIO = 0.0
    agg_ord.TRADES_RATIO = 0.0
    agg_ord.NUM_TRADES = agg_ord.NUM_BUY_TRADES + agg_ord.NUM_SELL_TRADES

    agg_ord = agg_ord.update(
        {agg_ord.TRADES_RATIO: agg_ord.NUM_BUY_TRADES / agg_ord.NUM_TRADES},
        else_set={agg_ord.TRADES_RATIO: agg_ord.NUM_SELL_TRADES / agg_ord.NUM_TRADES},
        where=(agg_ord.NUM_BUY_TRADES > agg_ord.NUM_SELL_TRADES),
    )

    agg_ord = agg_ord.update(
        {agg_ord.VOLUME_RATIO: agg_ord.BUY_VOLUME / (agg_ord.BUY_VOLUME + agg_ord.SELL_VOLUME)},
        else_set={agg_ord.VOLUME_RATIO: agg_ord.SELL_VOLUME / (agg_ord.BUY_VOLUME + agg_ord.SELL_VOLUME)},
        where=(agg_ord.BUY_VOLUME > agg_ord.SELL_VOLUME),
    )

    agg_ord, _ = agg_ord[
        (agg_ord.TRADES_RATIO < max_trade_ratio)
        & (agg_ord.VOLUME_RATIO < max_volume_ratio)
        & (agg_ord.NUM_TRADES >= min_num_trades)
    ]

    return agg_ord.high(agg_ord.NUM_TRADES, bucket_interval=window_size_sec)


def quote_stuffing(orders, window_size_sec, min_cancels):
    orders[("ORIGSTATE", str)]
    orders, _ = orders[(orders.ORDTYPE != "LIMIT_ON_CLOSE") | (orders.ORDTYPE != "MARKET_ON_CLOSE")]

    orders.IS_CANCELLED = (orders.STATE == "C") & (orders.ORIGSTATE != "REP")
    orders.IS_NEW = (orders.STATE == "N") & (orders.ORIGSTATE != "REP")
    orders.IS_REJ = orders.STATE == "REJ"
    orders.IS_PFILLED_OR_FILLED = (orders.STATE == "F") & (orders.STATE == "PF")
    orders.IS_REPLACE = (orders.STATE == "N") & (orders.ORIGSTATE == "REP")

    orders.NEW_QTY = 0
    orders = orders.update({orders.NEW_QTY: orders.QTY}, where=(orders.STATE == "N") & (orders.ORIGSTATE != "REP"))

    orders = orders.agg(
        {
            "CANCELLED": otp.agg.sum(orders.IS_CANCELLED),
            "NEW": otp.agg.sum(orders.IS_NEW),
            "REJ": otp.agg.sum(orders.IS_REJ),
            "FILLED_OR_PARTIALLY_FILLED": otp.agg.sum(orders.IS_PFILLED_OR_FILLED),
            "NEWQTY": otp.agg.sum(orders.NEW_QTY),
            "REPLACED": otp.agg.sum(orders.IS_REPLACE),
        },
        running=True,
        all_fields=True,
        bucket_interval=window_size_sec,
    )

    orders, _ = orders[(orders.CANCELLED >= min_cancels) & (orders.FILLED_OR_PARTIALLY_FILLED == 0)]
    orders.drop([orders.FILLED_OR_PARTIALLY_FILLED])
    orders = orders[[orders.CANCELLED, orders.NEW, orders.REJ, orders.NEWQTY, orders.REPLACED]]

    orders = orders.high(orders.CANCELLED, bucket_time="start", bucket_interval=window_size_sec)

    orders.ALERT_FLAG = 1
    orders.ALERT_TS = orders.TIMESTAMP
    orders.ENDTIME = orders.ALERT_TS
    orders.STARTTIME = (orders.ENDTIME - 1000 * window_size_sec, otp.msectime)

    return orders
