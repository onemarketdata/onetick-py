import json
from contextlib import suppress
from datetime import datetime

from onetick.py.otq import otq, pyomd
from .tz import get_local_timezone_name


def get_symbol_list_from_df(df, symbol_name_column='SYMBOL_NAME', timezone=None):
    """
    Creates a onetick.query.Symbol object that may be passed as a symbol list from the dataframe
    with query results. SYMBOL_NAME column is interpreted as symbol names, while other columns are
    interpreted as symbol params.

    Some known OneTick columns are treated specially
    and converted from datetime to number of nanoseconds:
        * _PARAM_START_TIME
        * _PARAM_END_TIME
        * _PARAM_START_TIME_NANOS
        * _PARAM_END_TIME_NANOS

    Timezone-naive values are localized in specified ``timezone`` (None means local timezone),
    while timezone-aware values keep their own timezone.
    """
    if symbol_name_column not in df.columns:
        raise ValueError(f'Dataframe used as symbol list does not contain a {symbol_name_column} column')

    df = df.copy()

    # BDS-511: adding Time column to the query may result in problems with otq.run
    if 'Time' in df.columns:
        df = df.drop(columns=['Time'])

    if timezone is None:
        timezone = get_local_timezone_name()

    # convert special symbol parameters from datetime to number of nanoseconds,
    # timezone-aware values already know their timezone and can't be localized again
    for column in ('_PARAM_START_TIME', '_PARAM_END_TIME', '_PARAM_START_TIME_NANOS', '_PARAM_END_TIME_NANOS'):
        if column in df.columns:
            with suppress(AttributeError):
                df[column] = df[column].apply(lambda x: (x if x.tzinfo else x.tz_localize(timezone)).value)

    def symbol_from_dict(params):
        name = params[symbol_name_column]
        del params[symbol_name_column]
        return otq.Symbol(name=name, params=params)

    symbols = [symbol_from_dict(row) for row in df.to_dict(orient='records')]
    return symbols


def get_symbol_source_from_df(df, symbol_name_column='SYMBOL_NAME'):
    """
    Creates a :class:`onetick.py.Source` that may be passed as an unbound symbol list
    to the functions supporting setting cross-symbol parameters, e.g. :func:`onetick.py.merge`.

    ``SYMBOL_NAME`` column is interpreted as symbol names, while other columns are
    interpreted as symbol params. Special columns ``_PARAM_START_TIME`` and ``_PARAM_END_TIME``
    (and their ``_NANOS`` counterparts) set the query interval per symbol.

    Unlike `get_symbol_list_from_df`, datetime values aren't converted to the number
    of nanoseconds. Timezone-naive values are propagated to the generated query as
    ``PARSE_NSECTIME`` expressions with the ``_TIMEZONE`` placeholder, so their timezone is
    resolved by OneTick at the query runtime. Timezone-aware values keep their own timezone,
    it is not overridden by the ``timezone`` parameter of :func:`onetick.py.run`.
    """
    import onetick.py as otp

    if symbol_name_column not in df.columns:
        raise ValueError(f'Dataframe used as symbol list does not contain a {symbol_name_column} column')

    if symbol_name_column != 'SYMBOL_NAME' and 'SYMBOL_NAME' in df.columns:
        raise ValueError(f"Dataframe used as symbol list contains both '{symbol_name_column}' "
                         "and 'SYMBOL_NAME' columns, only one of them should be passed")

    if df.empty:
        raise ValueError('Dataframe used as symbol list is empty')

    # 'offset' and 'time' are intercepted by otp.Ticks as tick timing parameters,
    # and a 'timestamp' column in any case is silently dropped by CSV_FILE_LISTING
    reserved = [
        column for column in df.columns
        if isinstance(column, str) and (column in ('offset', 'time') or column.lower() == 'timestamp')
    ]
    if reserved:
        raise ValueError(f"Dataframe used as symbol list can't have {reserved} columns, "
                         "'offset', 'time' and 'timestamp' (in any case) are reserved names")

    df = df.copy()

    if 'Time' in df.columns:
        df = df.drop(columns=['Time'])

    if symbol_name_column != 'SYMBOL_NAME':
        df = df.rename(columns={symbol_name_column: 'SYMBOL_NAME'})

    # `offset=None` sets the timestamp of every tick to the start time of the query.
    return otp.Ticks(df.to_dict(orient='list'), offset=None)


class JSONEncoder(json.JSONEncoder):
    """
    onetick.py json encoder that also supports some of the onetick.py objects like otp.adaptive.
    """
    def default(self, o):
        # let's use python str representation by default, all objects in python should have it
        # maybe we will add better representations for some types later
        return str(o)


def json_dumps(obj, **kwargs) -> str:
    """
    Wrapper around json.dumps that also supports some of the onetick.py objects like otp.adaptive.
    ``kwargs`` arguments are propagated to json.dumps function.
    By default ``cls`` parameter is set to otp.utils.JSONEncoder.
    """
    kwargs.setdefault('cls', JSONEncoder)
    return json.dumps(obj, **kwargs)


def query_properties_to_dict(query_properties: pyomd.QueryProperties) -> dict:  # type: ignore[valid-type]
    """
    Convert :py:class:`pyomd.QueryProperties` to dictionary.
    """

    str_qp = query_properties.convert_to_name_value_pairs_string()  # type: ignore[attr-defined]
    if not isinstance(str_qp, str):
        str_qp = str_qp.c_str()
    pairs = str_qp.split(',') if str_qp else []
    return dict(pair.split('=', maxsplit=1) for pair in pairs)


def query_properties_from_dict(query_properties_dict: dict) -> pyomd.QueryProperties:  # type: ignore[valid-type]
    """
    Convert dictionary to :py:class:`pyomd.QueryProperties`.
    """
    query_properties = pyomd.QueryProperties()
    for k, v in query_properties_dict.items():
        query_properties.set_property_value(k, v)
    return query_properties


def symbol_date_to_str(symbol_date) -> str:
    if isinstance(symbol_date, int):
        symbol_date = str(symbol_date)
    if isinstance(symbol_date, str):
        symbol_date = datetime.strptime(symbol_date, '%Y%m%d')
    if hasattr(symbol_date, 'strftime'):
        symbol_date = symbol_date.strftime('%Y%m%d')
    return symbol_date
