import pandas as pd
import numpy as np

from .. import types as ott


def _convert_pandas_types(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return int
    elif pd.api.types.is_float_dtype(dtype):
        return float
    elif pd.api.types.is_string_dtype(dtype):
        return str
    elif pd.api.types.is_bool_dtype(dtype):
        return bool
    return None


def inspect_by_pandas(path_to_csv, first_line_is_title=True, names=None, field_delimiter=',', quote_char='"'):
    # read CHUNK_SIZE first lines to determine column types
    CHUNK_SIZE = 300

    header = None

    if first_line_is_title:
        header = 0 if names else "infer"

    with pd.read_csv(
        path_to_csv,
        engine="python",
        iterator=True,
        header=header,
        names=names,
        sep=field_delimiter,
        escapechar='\\',
        quotechar=quote_char,
    ) as reader:
        df = reader.read(CHUNK_SIZE)
        prefix = None if first_line_is_title or names else "COLUMN_"
        if prefix:
            df.columns = [f'{prefix}{col}' for col in df.columns]

    if not first_line_is_title:
        first_column = "COLUMN_0"
        if names:
            first_column = names[0]
        if len(df) > 0 and len(df.columns) > 0 and df.dtypes[first_column] == np.dtype("O"):
            if df[first_column][0].startswith("#"):
                raise ValueError(
                    "If first line of CSV starts with #, you must set first_line_is_title=True, "
                    "because OneTick will forcefully use first line as header.")

    # CSV_FILE_LISTING will ignore FIRST_LINE_IS_TITLE, if first line starts with hash sign #
    forced_title = df.columns.values[0][0] == '#'
    if forced_title:
        # remove hash sign from first column name
        df.rename(columns={df.columns.values[0]: df.columns.values[0][1:]}, inplace=True)

    # check for default types in OneTick format ("columnname type")
    default_types = {}
    rename = {}

    for column in df.columns.values:
        c = column.split()

        if column.startswith('Unnamed: '):
            # OneTick doesn't allow to have empty column name, so we explicitly set it here
            # and in CSV_FILE_LISTING EP later in CSV.base_ep()
            rename[column] = "COLUMN_" + column[9:]

        elif len(c) == 2:
            # format: "type COLUMNNAME"
            # http://solutions.pages.soltest.onetick.com/iac/onetick-server/ep_guide/EP/FieldTypeDeclarations.htm#supported_field_types
            dtype = ott.str2type(c[0])
            if dtype is not None:
                default_types[c[1]] = dtype
                rename[column] = c[1]

    if rename:
        df.rename(columns=rename, inplace=True)

    # convert pandas types to otp
    columns = dict(map(lambda x: (x[0], _convert_pandas_types(x[1])), dict(df.dtypes).items()))

    # explicitly set types for columns having format "type COLUMNNAME"
    for column_name, dtype in default_types.items():
        columns[column_name] = dtype

    # reset default_types if # is not in first line
    if not forced_title:
        default_types = {}

    return columns, default_types, forced_title
