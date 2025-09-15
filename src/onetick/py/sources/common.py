import datetime as dt

from typing import Optional, Union, Type, List

from onetick.py.otq import otq
import pandas as pd

from onetick.py.core._source._symbol_param import _SymbolParamColumn
from onetick.py.core.source import Source
from onetick.py.core.column_operations.base import OnetickParameter

from .. import types as ott
from .. import utils, configuration

AdaptiveTickType = Union[str, OnetickParameter, _SymbolParamColumn, Type[utils.adaptive]]
AdaptiveDBType = Union[str, OnetickParameter, _SymbolParamColumn, Type[utils.adaptive], List[str]]


def get_start_end_by_date(date):
    if isinstance(date, (ott.datetime, ott.date)):
        start = date.start
        end = date.end
    elif isinstance(date, (dt.datetime, dt.date)):
        start = dt.datetime(date.year, date.month, date.day)
        end = start + dt.timedelta(days=1, milliseconds=-1)
    else:
        raise ValueError(f"Unsupported value of parameter 'date': {type(date)}")
    return start, end


def convert_tick_type_to_str(
    tick_type: Optional[AdaptiveTickType],
    db: Optional[AdaptiveDBType] = None,
) -> Optional[str]:
    if not isinstance(db, list) and not isinstance(db, (OnetickParameter, _SymbolParamColumn)):
        if tick_type is utils.adaptive:
            tick_type = 'ANY'

        if isinstance(tick_type, type) or tick_type is None:
            return None

    if db is utils.adaptive_to_default or db is utils.adaptive:
        # if default database is not set, tick type will be set without it
        # and symbols will have to be specified in otp.run
        db = configuration.config.get('default_db')

    str_db = str(db) if db is not None else ''
    str_tt = str(tick_type) if tick_type is not None else ''

    is_db_symbol_param = isinstance(db, _SymbolParamColumn)
    is_tt_symbol_param = isinstance(tick_type, _SymbolParamColumn)

    if isinstance(db, OnetickParameter):
        str_db = db.parameter_expression

    if isinstance(tick_type, OnetickParameter):
        str_tt = tick_type.parameter_expression

    if db is None:
        if is_tt_symbol_param:
            return f"expr({str_tt})"
        return str_tt

    if isinstance(db, list):
        str_db = "+".join(db)

    if tick_type is not None:
        if is_db_symbol_param:
            if is_tt_symbol_param:
                return f"expr({str_db} + '::' + {str_tt})"

            return f"expr({str_db} + '::{str_tt}')"

        if is_tt_symbol_param:
            return f"expr('{str_db}::' + {str_tt})"
        elif "::" not in str_db:
            return str_db + "::" + str_tt
    elif is_db_symbol_param:
        return f"expr({str_db})"

    return str_db


def update_node_tick_type(
    src: "Source",
    tick_type: Optional[AdaptiveTickType],
    db: Optional[AdaptiveDBType] = None,
):
    """
    Update tick type of the node of the source ``src`` according to ``db`` name and ``tick_type``.

    Adaptive tick type means that tick type value doesn't affect query results
    and, thus, we change it to some constant value.

    Parameters
    ----------
    src: Source
        source to set tick type on
    tick_type: Optional[AdaptiveTickType]
        string tick type or :py:class:`onetick.py.adaptive`
    db: Optional[AdaptiveDBType]
        optional db name or list of strings with db names and tick types
    """

    str_db = convert_tick_type_to_str(tick_type, db)
    if str_db is None:
        return

    src.tick_type(str_db)


def _common_passthrough_base_ep(db, tick_type):
    src = Source(otq.Passthrough(fields="SYMBOL_NAME,TICK_TYPE", drop_fields=True))
    update_node_tick_type(src, tick_type, db)
    return src


def default_date_converter(date):
    return pd.to_datetime(date, format='%Y%m%d%H%M%S.%f')


def to_timestamp_nanos(date, date_converter, tz):
    date = date_converter(date)
    if isinstance(date, ott.dt):
        date = date.ts
    else:
        date = pd.to_datetime(date)
    return date.tz_localize(tz)
