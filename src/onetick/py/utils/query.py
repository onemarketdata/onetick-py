import os

from .config import get_config_param


def abspath_to_query_by_otq_path(otq_path, query_path):
    """
    Function searches absolute path to a query based on the otq_path and
    short query path
    """
    query_path_parts = query_path.split("/")

    for base_path in otq_path.split(","):
        base_path = os.path.abspath(base_path)
        path = os.path.join(base_path, *query_path_parts)
        if os.path.exists(path):
            return path

    raise FileNotFoundError(f'Query "{query_path}" is not found')


def abspath_to_query_by_name(query_path):
    if os.path.isabs(query_path) and os.path.exists(query_path):
        return query_path

    if "ONE_TICK_CONFIG" not in os.environ:
        raise ValueError("ONE_TICK_CONFIG is not set!")

    return abspath_to_query_by_otq_path(
        get_config_param(os.environ["ONE_TICK_CONFIG"], "OTQ_FILE_PATH"), query_path
    )


def query_to_path_and_name(path):
    """
    Split passed OneTick like 'path' to a query ot the
    path and query name
    """
    query_path = None
    query_name = None

    pos1, pos2 = path.rfind(":"), path.find(":")
    if pos1 != pos2 and pos1 > 0 and pos2 > 0:
        _ = path.split(":")
        query_path, query_name = ":".join(_[:-2]), _[-1]
    else:
        query_path = path

    return query_path, query_name
