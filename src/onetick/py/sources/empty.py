import datetime as dt

import onetick.py as otp
from onetick.py.otq import otq

import onetick.py.core._source
import onetick.py.functions
import onetick.py.db._inspection
from onetick.py.core.source import Source

from .. import utils, configuration

from .common import update_node_tick_type


class Empty(Source):
    """
    Empty data source

    Parameters
    ----------
    db: str
        Name of the database from which to take schema.
    symbol: str, list of str, :class:`Source`, :class:`query`, :py:func:`eval query <onetick.py.eval>`
        Symbol(s) from which data should be taken.
    tick_type: str,
        Name of the tick_type from which to take schema.
    start, end: :py:class:`datetime.datetime`, :py:class:`otp.datetime <onetick.py.datetime>`, \
                    :py:class:`onetick.py.adaptive`
        Time interval from which the data should be taken.
    schema: dict
        Schema to use in case db and/or tick_type are not set.
    kwargs:
        Deprecated. Use ``schema`` instead.
        Schema to use in case db and/or tick_type are not set.

    Examples
    --------
    We can define schema:

    >>> data = otp.Empty(schema={'A': str, 'B': int})
    >>> otp.run(data)
    Empty DataFrame
    Columns: [A, B, Time]
    Index: []
    >>> data.schema
    {'A': <class 'str'>, 'B': <class 'int'>}

    Or we can get schema from the database:

    >>> data = otp.Empty(db='SOME_DB', tick_type='TT')
    >>> data.schema
    {'X': <class 'int'>}
    """

    def __init__(
        self,
        db=utils.adaptive_to_default,
        symbol=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        schema=None,
        **kwargs,
    ):
        if self._try_default_constructor(schema=schema, **kwargs):
            return

        schema = self._select_schema(schema, kwargs)

        columns = {}

        if (tick_type is not utils.adaptive and
                db != configuration.config.get('default_db') and db is not utils.adaptive_to_default):
            try:
                db_obj = onetick.py.db._inspection.DB(db)
                params = {'tick_type': tick_type}
                if end is not utils.adaptive:
                    params['end'] = end
                columns = db_obj.schema(**params)
            except Exception:
                pass  # do not raise an exception if no data found, because it is empty _source and does not matter

        else:
            columns = schema

        super().__init__(
            _symbols=symbol, _start=start, _end=end, _base_ep_func=lambda: self.base_ep(db), schema=columns,
        )

    def base_ep(self, db):
        src = Source(otq.TickGenerator(fields="long ___NOTHING___=0"))
        update_node_tick_type(src, 'TICK_GENERATOR', db)
        return src
