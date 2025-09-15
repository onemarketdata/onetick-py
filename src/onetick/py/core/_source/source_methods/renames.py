import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from onetick.py.core.column import _Column
from onetick.py.otq import otq

from .misc import inplace_operation

if TYPE_CHECKING:
    from onetick.py.core.source import Source


def _add_prefix_and_suffix(
    self: 'Source',
    prefix='',
    suffix='',
    columns=None,
    ignore_columns=None,
) -> Optional['Source']:
    if not prefix and not suffix:
        raise ValueError('Both suffix and prefix are empty')
    if ' ' in prefix:
        raise ValueError(f'There is space in prefix: {prefix}')
    if ' ' in suffix:
        raise ValueError(f'There is space in suffix: {prefix}')
    columns = columns or []
    ignore_columns = ignore_columns or []
    if columns and ignore_columns:
        raise ValueError('It is allowed to use only one of `columns` or `ignore_columns` parameters at a time')
    schema = self.schema
    for column in columns:
        if column not in schema:
            raise AttributeError(f'Column `{column}` does not exist in the schema')
    for column_name in columns or schema:
        if column_name in ignore_columns:
            continue
        new_column_name = f'{prefix}{column_name}{suffix}'
        if new_column_name in self.__dict__:
            if (not columns and not ignore_columns
                    or columns and new_column_name in columns
                    or ignore_columns and new_column_name not in ignore_columns):
                # if the column with the same name already exists, but will be renamed too,
                # then we don't need to raise exception, as it will have a different name after renaming
                continue
            if prefix:
                raise AttributeError(f'Column {new_column_name} already exists, please, use another prefix')
            else:
                raise AttributeError(f'Column {new_column_name} already exists, please, use another suffix')
    for column_name in columns or schema:
        if column_name in ignore_columns:
            continue
        new_column_name = f'{prefix}{column_name}{suffix}'
        self.__dict__[column_name].rename(new_column_name, update_parent_object=False)
        self.__dict__[new_column_name] = self.__dict__[column_name]
        del self.__dict__[column_name]
    if not columns and not ignore_columns:
        self.sink(otq.RenameFieldsEp(rename_fields=f'(.*)={prefix}\\1{suffix}', use_regex=True))
    elif columns:
        renames = [f'{column}={prefix}{column}{suffix}' for column in columns]
        self.sink(otq.RenameFieldsEp(rename_fields=','.join(renames)))
    else:
        renames = [f'{column}={prefix}{column}{suffix}' for column in schema if column not in ignore_columns]
        self.sink(otq.RenameFieldsEp(rename_fields=','.join(renames)))
    return self


def _is_excluded(s: str, not_to_rename: List[str]) -> bool:
    for excluded in not_to_rename:
        if re.match(excluded, s):
            return True
    return False


def _get_columns_names_renaming(schema, rename_dict: Dict[str, str], not_to_rename: List[str]) -> Dict[str, str]:
    """
    We can't be sure python Source has consistent columns cache, because sinking complex event processors
    can change columns unpredictable, so if user will specify regex as a param, we will pass regex
    as an onetick's param, but rename all matched columns from python Source cache.

    Parameters
    ----------
    rename_dict:
        Dict old_name -> new_name. Some of the dictionary's items use regex.

    Returns
    -------
    output_dict:
        Dict old_name -> new_name used for renaming columns in Source. None of this dictionary's items use regex.
    """
    output_dict = {}
    for old_name, new_name in rename_dict.items():
        matching_columns = [col for col in schema if re.match(old_name, col) and not _is_excluded(col, not_to_rename)]
        for col in matching_columns:
            output_dict[col] = re.sub(old_name, new_name, col)
    return output_dict


@inplace_operation
def add_prefix(self: 'Source', prefix, inplace=False, columns=None, ignore_columns=None) -> Optional['Source']:
    """
    Adds prefix to all column names (except **TIMESTAMP** (or **Time**) special column).

    Parameters
    ----------
    prefix : str
        String prefix to add to all columns.
    inplace : bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object.
    columns: List[str], optional
        If set, only selected columns will be updated with prefix. Can't be used with ``ignore_columns`` parameter.
    ignore_columns: List[str], optional
        If set, selected columns won't be updated with prefix. Can't be used with ``columns`` parameter.

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Add prefix *test_* to all columns (note that column **Time** is not renamed):

    >>> data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL')
    >>> data = data.add_prefix('test_')
    >>> otp.run(data, start=otp.dt(2022, 3, 1), end=otp.dt(2022, 3, 2))
                         Time  test_PRICE  test_SIZE
    0 2022-03-01 00:00:00.000         1.3        100
    1 2022-03-01 00:00:00.001         1.4         10
    2 2022-03-01 00:00:00.002         1.4         50

    Parameter ``columns`` specifies columns to be updated with prefix:

    >>> data = otp.Tick(A=1, B=2, C=3, D=4, E=5)
    >>> data = data.add_prefix('test_', columns=['A', 'B', 'C'])
    >>> otp.run(data)
            Time  test_A  test_B  test_C  D  E
    0 2003-12-01       1       2       3  4  5

    Parameter ``ignore_columns`` specifies columns to ignore:

    >>> data = otp.Tick(A=1, B=2, C=3, D=4, E=5)
    >>> data = data.add_prefix('test_', ignore_columns=['A', 'B', 'C'])
    >>> otp.run(data)
            Time  A  B  C  test_D  test_E
    0 2003-12-01  1  2  3       4       5

    Parameters ``columns`` and ``ignore_columns`` can't be used at the same time:

    >>> data = otp.Tick(A=1, B=2, C=3, D=4, E=5)
    >>> data.add_prefix('test_', columns=['B', 'C'], ignore_columns=['A'])
    Traceback (most recent call last):
        ...
    ValueError: It is allowed to use only one of `columns` or `ignore_columns` parameters at a time

    Columns can't be renamed if their resulting name will be equal to existing column name:

    >>> data = otp.Tick(X=1, XX=2)
    >>> data.add_prefix('X', columns=['X'])
    Traceback (most recent call last):
        ...
    AttributeError: Column XX already exists, please, use another prefix

    """
    return self._add_prefix_and_suffix(
        prefix=prefix,
        columns=columns,
        ignore_columns=ignore_columns,
    )


@inplace_operation
def add_suffix(self: 'Source', suffix, inplace=False, columns=None, ignore_columns=None) -> Optional['Source']:
    """
    Adds suffix to all column names (except **TIMESTAMP** (or **Time**) special column).

    Parameters
    ----------
    suffix : str
        String suffix to add to all columns.
    inplace : bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object.
    columns: List[str], optional
        If set, only selected columns will be updated with suffix. Can't be used with ``ignore_columns`` parameter.
    ignore_columns: List[str], optional
        If set, selected columns won't be updated with suffix. Can't be used with ``columns`` parameter.

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    Add suffix *_test* to all columns (note that column **Time** is not renamed):

    >>> data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL')
    >>> data = data.add_suffix('_test')
    >>> otp.run(data, start=otp.dt(2022, 3, 1), end=otp.dt(2022, 3, 2))
                         Time  PRICE_test  SIZE_test
    0 2022-03-01 00:00:00.000         1.3        100
    1 2022-03-01 00:00:00.001         1.4         10
    2 2022-03-01 00:00:00.002         1.4         50

    Parameter ``columns`` specifies columns to be updated with suffix:

    >>> data = otp.Tick(A=1, B=2, C=3, D=4, E=5)
    >>> data = data.add_suffix('_test', columns=['A', 'B', 'C'])
    >>> otp.run(data)
            Time  A_test  B_test  C_test  D  E
    0 2003-12-01       1       2       3  4  5

    Parameter ``ignore_columns`` specifies columns to ignore:

    >>> data = otp.Tick(A=1, B=2, C=3, D=4, E=5)
    >>> data = data.add_suffix('_test', ignore_columns=['A', 'B', 'C'])
    >>> otp.run(data)
            Time  A  B  C  D_test  E_test
    0 2003-12-01  1  2  3       4       5

    Parameters ``columns`` and ``ignore_columns`` can't be used at the same time:

    >>> data = otp.Tick(A=1, B=2, C=3, D=4, E=5)
    >>> data.add_suffix('_test', columns=['B', 'C'], ignore_columns=['A'])
    Traceback (most recent call last):
        ...
    ValueError: It is allowed to use only one of `columns` or `ignore_columns` parameters at a time

    Columns can't be renamed if their resulting name will be equal to existing column name:

    >>> data = otp.Tick(X=1, XX=2)
    >>> data.add_suffix('X', columns=['X'])
    Traceback (most recent call last):
        ...
    AttributeError: Column XX already exists, please, use another suffix
    """
    return self._add_prefix_and_suffix(
        suffix=suffix,
        columns=columns,
        ignore_columns=ignore_columns,
    )


@inplace_operation
def rename(self: 'Source', columns=None, use_regex=False, fields_to_skip=None, inplace=False) -> Optional['Source']:
    r"""
    Rename columns

    Parameters
    ----------
    columns : dict
        Rules how to rename in the following format: {<column> : <new-column-name>},
        where <column> is either existing column name of str type or reference to a column,
        and <new-column-name> a new column name of str type.
    use_regex: bool
        If true, then old-name=new-name pairs in the `columns` parameter are treated as regular expressions.
        This allows bulk renaming for field names. Notice that regular expressions for old names are treated as
        if both their prefix and their suffix are .*, i.e. the prefix and suffix match any substring.
        As a result, old-name *XX* will match all of *aXX*, *aXXB*, and *XXb*, when `use_regex=true`.
        You can have old-name begin from ^ to indicate that .* prefix does not apply,
        and you can have old name end at $ to indicate that .* suffix does not apply.
        Default: false
    fields_to_skip: list of str
        A list of regular expressions for specifying fields that should be skipped
        (i.e., not be renamed). If a field is matched by one of the specified regular expressions,
        it won't be considered for renaming.
        Default: None
    inplace : bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise, method returns a new modified
        object.

    Returns
    -------
    :class:`Source` or ``None``

    See also
    --------
    **RENAME** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(X=[1], Y=[2])
    >>> data = data.rename({'X': 'XX',
    ...                     data['Y']: 'YY'})
    >>> otp.run(data)
            Time  XX  YY
    0 2003-12-01   1   2

    >>> data = otp.Tick(**{'X.X': 1, 'X.Y': 2})
    >>> data = data.rename({r'X\.(.*)': r'\1'}, use_regex=True)
    >>> otp.run(data)
            Time   X   Y
    0 2003-12-01   1   2

    >>> data = otp.Tick(**{'X.X': 1, 'X.Y': 2})
    >>> data = data.rename({r'X\.(.*)': r'\1'}, use_regex=True, fields_to_skip=['X.Y'])
    >>> otp.run(data)
            Time   X X.Y
    0 2003-12-01   1   2
    """
    if columns is None:
        columns = {}

    # prepare
    items = {}
    out_names = set()
    fields_to_skip = fields_to_skip or []

    for in_obj, out_obj in columns.items():
        if isinstance(in_obj, _Column):
            items[in_obj.name.strip()] = out_obj.strip()
        elif isinstance(in_obj, str):
            items[in_obj.strip()] = out_obj.strip()
        else:
            raise TypeError(f"It is not supported to rename item '{in_obj}' of type {type(in_obj)}'")

        if out_obj in out_names:
            raise AttributeError(
                f"You want to rename '{in_obj}' into '{out_obj}', "
                f"but also want to rename another column into '{out_obj}'"
            )

        out_names.add(out_obj)

    schema_update_dict = items
    if use_regex:
        schema_update_dict = _get_columns_names_renaming(self.schema, items, fields_to_skip)

    # validate
    for in_key, out_key in schema_update_dict.items():
        if in_key not in self.__dict__ or not isinstance(self.__dict__[in_key], _Column):
            raise AttributeError(f"There is no '{in_key}' column to rename")

        if out_key in self.__dict__ and isinstance(self.__dict__[out_key], _Column):
            raise AttributeError(f"Column '{out_key}' already exists")

    # apply
    for in_key, out_key in schema_update_dict.items():
        self.__dict__[in_key].rename(out_key, update_parent_object=False)
        self.__dict__[out_key] = self.__dict__[in_key]
        del self.__dict__[in_key]

    rename_rules = [key + "=" + value for key, value in items.items()]
    kwargs: Dict[str, Any] = dict(rename_fields=",".join(rename_rules))
    if use_regex:
        kwargs['use_regex'] = True
    if fields_to_skip:
        kwargs['fields_to_skip'] = ",".join(fields_to_skip)

    self.sink(otq.RenameFieldsEp(**kwargs))

    return self
