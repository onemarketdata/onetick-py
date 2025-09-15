from onetick.py.core.source import Source

from .. import utils

from .common import _common_passthrough_base_ep


class Orders(Source):
    def __init__(
        self, db="S_ORDERS_FIX", symbol=utils.adaptive, start=utils.adaptive, end=utils.adaptive, schema=None, **kwargs,
    ):
        super().__init__(
            _symbols=symbol, _start=start, _end=end, _base_ep_func=lambda: self.base_ep(db), schema=schema, **kwargs,
        )

        self.schema['ID'] = str
        self.schema['BUY_FLAG'] = int
        self.schema['SIDE'] = str
        self.schema['STATE'] = str
        self.schema['ORDTYPE'] = str
        self.schema['PRICE'] = float
        self.schema['PRICE_FILLED'] = float
        self.schema['QTY'] = int
        self.schema['QTY_FILLED'] = int

    def base_ep(self, db):
        return _common_passthrough_base_ep(db, 'ORDER')


class Quotes(Source):
    def __init__(
        self, db=utils.adaptive_to_default, symbol=utils.adaptive, start=utils.adaptive, end=utils.adaptive,
        schema=None, **kwargs,
    ):
        super().__init__(
            _symbols=symbol, _start=start, _end=end, _base_ep_func=lambda: self.base_ep(db), schema=schema, **kwargs,
        )

        self.schema['ASK_PRICE'] = float
        self.schema['BID_PRICE'] = float
        self.schema['ASK_SIZE'] = int
        self.schema['BID_SIZE'] = int

    def base_ep(self, db):
        return _common_passthrough_base_ep(db, 'QTE')


class Trades(Source):
    """
    Trade source object.
    add 'PRICE' and 'SIZE' fields to schema
    """

    def __init__(
        self, db=utils.adaptive_to_default, symbol=utils.adaptive, date=None, start=utils.adaptive, end=utils.adaptive,
        schema=None, **kwargs,
    ):
        if date:
            start, end = date.start, date.end
        super().__init__(
            _symbols=symbol, _start=start, _end=end, _base_ep_func=lambda: self.base_ep(db), schema=schema, **kwargs,
        )

        self.schema['PRICE'] = float
        self.schema['SIZE'] = int

    def base_ep(self, db):
        return _common_passthrough_base_ep(db, 'TRD')


class NBBO(Source):
    def __init__(
        self, db="TAQ_NBBO", symbol=utils.adaptive, start=utils.adaptive, end=utils.adaptive, schema=None, **kwargs,
    ):
        super().__init__(
            _symbols=symbol, _start=start, _end=end, _base_ep_func=lambda: self.base_ep(db), schema=schema, **kwargs,
        )

        self.schema['ASK_PRICE'] = float
        self.schema['BID_PRICE'] = float
        self.schema['ASK_SIZE'] = int
        self.schema['BID_SIZE'] = int

    def base_ep(self, db):
        return _common_passthrough_base_ep(db, 'NBBO')
