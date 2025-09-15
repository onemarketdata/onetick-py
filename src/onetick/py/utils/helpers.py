import json

from onetick.py.otq import otq, pyomd
from datetime import datetime


def get_symbol_list_from_df(df, symbol_name_column='SYMBOL_NAME'):
    """
    Creates a onetick.query.Symbol object that may be passed as a symbol list from the dataframe
    with query results. SYMBOL_NAME column is interpreted as symbol names, while other columns are
    interpreted as symbol params.
    """
    if symbol_name_column not in df.columns:
        raise ValueError(f'Dataframe used as symbol list does not contain a {symbol_name_column} column')

    df = df.drop(columns=['Time'])

    def symbol_from_dict(params):
        name = params[symbol_name_column]
        del params[symbol_name_column]
        return otq.Symbol(name=name, params=params)

    symbols = [symbol_from_dict(row) for row in df.to_dict(orient='records')]
    return symbols


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
