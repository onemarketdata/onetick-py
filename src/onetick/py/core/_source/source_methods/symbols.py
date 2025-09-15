from typing import TYPE_CHECKING, Optional, Union

from onetick.py import types as ott
from onetick.py.core.column_operations.base import _Operation
from onetick.py.otq import otq

from .misc import inplace_operation

if TYPE_CHECKING:
    import onetick.py as otp
    from onetick.py.core.source import Source


@inplace_operation
def show_symbol_name_in_db(self: 'Source', inplace=False) -> Optional['Source']:
    """
    Adds the **SYMBOL_NAME_IN_DB** field to input ticks,
    indicating the symbol name of the tick in the database.

    Parameters
    ----------
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise method returns a new modified object.

    See also
    --------
    **SHOW_SYMBOL_NAME_IN_DB** OneTick event processor

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    For example, it can be used to display
    the actual symbol name for the contract (e.g., **ESM23**)
    instead of the artificial continuous name **ES_r_tdi**.
    Notice how actual symbol name can be different for each tick,
    e.g. in this case it is different for each month.

    >>> data = otp.DataSource('TDI_FUT', tick_type='TRD')  # doctest: +SKIP
    >>> data = data[['PRICE']]  # doctest: +SKIP
    >>> data = data.first(bucket_interval=31, bucket_units='days')  # doctest: +SKIP
    >>> data['SYMBOL_NAME'] = data.Symbol.name  # doctest: +SKIP
    >>> data = data.show_symbol_name_in_db()  # doctest: +SKIP
    >>> otp.run(data,  # doctest: +SKIP
    ...         symbols='ES_r_tdi', symbol_date=otp.dt(2023, 3, 1),
    ...         start=otp.dt(2023, 3, 1), end=otp.dt(2023, 5, 1))
                         Time    PRICE SYMBOL_NAME SYMBOL_NAME_IN_DB
    0 2023-03-01 00:00:00.549  3976.75    ES_r_tdi             ESH23
    1 2023-04-02 18:00:00.039  4127.00    ES_r_tdi             ESM23
    """
    if 'SYMBOL_NAME_IN_DB' in self.schema:
        raise ValueError("Column 'SYMBOL_NAME_IN_DB' already exists.")
    self.sink(otq.ShowSymbolNameInDb())
    self.schema['SYMBOL_NAME_IN_DB'] = str
    return self


@inplace_operation
def modify_symbol_name(
    self: 'Source', symbol_name: Union[str, 'otp.Operation', 'otp.Column'], inplace=False
) -> Optional['Source']:
    """
    Modifies the name of the symbol that provides input ticks for this node.
    Uses MODIFY_SYMBOL_NAME EP.

    Parameters
    ----------
    symbol_name: str, :py:class:`~onetick.py.Column`, :py:class:`~onetick.py.Operation`
        String or expression with new `SYMBOL_NAME` value.
        New `SYMBOL_NAME` must not depend on ticks, if set via expression.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise method returns a new modified object.

    See also
    --------
    **MODIFY_SYMBOL_NAME** OneTick event processor

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------
    Replacing with static string:

    >>> data = otp.DataSource('SOME_DB', symbol='S1', tick_type='TT')
    >>> data = data.modify_symbol_name(symbol_name='S2')
    >>> otp.run(data)
                         Time   X
    0 2003-12-01 00:00:00.000  -3
    1 2003-12-01 00:00:00.001  -2
    2 2003-12-01 00:00:00.002  -1

    Replacing with expression:

    >>> data = otp.DataSource('SOME_DB', symbol='S2', tick_type='TT')
    >>> data = data.modify_symbol_name(symbol_name=data['_SYMBOL_NAME'].str.replace('2', '1'))
    >>> otp.run(data)
                         Time  X
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2
    2 2003-12-01 00:00:00.002  3

    """
    if not isinstance(symbol_name, (str, _Operation)):
        raise ValueError("Unsupported symbol_name argument value type")

    self.sink(otq.ModifySymbolName(symbol_name=ott.value2str(symbol_name)))

    return self
