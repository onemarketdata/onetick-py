import typing
from typing import TYPE_CHECKING, Optional, Union

from onetick.py.otq import otq
from onetick.py.core.column_operations.base import _Operation
from onetick.py.backports import Literal
from .misc import inplace_operation

if TYPE_CHECKING:
    from onetick.py.core.source import Source


DATA_QUALITY_EVENTS = Literal[
    'COLLECTOR_FAILURE',
    'EMPTY',
    'MISSING',
    'MOUNT_BAD',
    'OK',
    'PATCHED',
    'QUALITY_DELAY_STITCHING_WITH_RT',
    'QUALITY_OK_STITCHING_WITH_RT',
    'STALE',
]


@inplace_operation
def show_data_quality(self: 'Source', inplace=False) -> Optional['Source']:
    """
    This method shows data quality events within the interval of the query.

    Parameters
    ----------
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    | **SHOW_DATA_QUALITY** OneTick event processor
    | :py:meth:`insert_data_quality_event`
    | :py:meth:`intercept_data_quality`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    By default data quality events are not showed, use this method to see them:

    >>> data = otp.Ticks({'A': [1, 2, 3]})
    >>> data = data.insert_data_quality_event('OK')
    >>> data = data.show_data_quality()
    >>> otp.run(data)
                         Time  DATA_QUALITY_TYPE DATA_QUALITY_NAME
    0 2003-12-01 00:00:00.000                  0                OK
    1 2003-12-01 00:00:00.001                  0                OK
    2 2003-12-01 00:00:00.002                  0                OK
    """

    self.sink(
        otq.ShowDataQuality()
    )
    self.schema.set(**{'DATA_QUALITY_TYPE': int, 'DATA_QUALITY_NAME': str})
    return self


@inplace_operation
def insert_data_quality_event(
    self: 'Source',
    data_quality: DATA_QUALITY_EVENTS,
    where: Optional[Union[str, _Operation]] = None,
    insert_before: bool = True,
    inplace=False,
) -> Optional['Source']:
    """
    Insert data quality events into the data flow.

    Parameters
    ----------
    data_quality: str
        The type of data quality event.
        Must be one of the supported data quality event types.
    where: str or :class:`Operation`
        Specifies a criterion for the selection of ticks whose arrival results in generation of a data quality event.
        If this expression returns True, a data quality event will be inserted into a time series.
        The expression can also be empty (default), in which case each input tick generates a data quality event.
    insert_before: bool
        If True (default),
        generated data quality event tick will be inserted before the input tick that triggered its creation,
        otherwise generated data quality event will be inserted after that input tick.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    | **INSERT_DATA_QUALITY_EVENT** OneTick event processor
    | :py:meth:`show_data_quality`
    | :py:meth:`intercept_data_quality`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------
    Insert OK data quality event before each tick:

    >>> data = otp.Ticks({'A': [1, 2, 3]})
    >>> data = data.insert_data_quality_event('OK')
    >>> otp.run(data)
                         Time  A
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2
    2 2003-12-01 00:00:00.002  3

    By default data quality events are not showed,
    use :py:meth:`show_data_quality` method to see them:

    >>> data = otp.Ticks({'A': [1, 2, 3]})
    >>> data = data.insert_data_quality_event('OK')
    >>> data = data.show_data_quality()
    >>> otp.run(data)
                         Time  DATA_QUALITY_TYPE DATA_QUALITY_NAME
    0 2003-12-01 00:00:00.000                  0                OK
    1 2003-12-01 00:00:00.001                  0                OK
    2 2003-12-01 00:00:00.002                  0                OK

    Use ``where`` parameter to specify the condition when to insert ticks:

    >>> data = otp.Ticks({'SIZE': [1, 200, 3]})
    >>> data = data.insert_data_quality_event('MISSING', where=data['SIZE'] > 100)
    >>> data = data.show_data_quality()
    >>> otp.run(data)
                         Time  DATA_QUALITY_TYPE DATA_QUALITY_NAME
    0 2003-12-01 00:00:00.001                  2           MISSING
    """
    if data_quality not in typing.get_args(DATA_QUALITY_EVENTS):
        raise ValueError(f"Parameter 'data_quality' doesn't support value: {data_quality}")

    self.sink(
        otq.InsertDataQualityEvent(
            data_quality=data_quality,
            where=str(where) if where is not None else '',
            insert_before=insert_before,
        )
    )

    return self


@inplace_operation
def intercept_data_quality(self: 'Source', inplace=False) -> Optional['Source']:
    """
    This method removes data quality messages, thus preventing them from delivery to the client application.

    Parameters
    ----------
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    | **INTERCEPT_DATA_QUALITY** OneTick event processor
    | :py:meth:`insert_data_quality_event`
    | :py:meth:`show_data_quality`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    By default data quality events are not showed, use this method to see them:

    >>> data = otp.Ticks({'A': [1, 2, 3]})
    >>> data = data.insert_data_quality_event('OK')
    >>> data = data.show_data_quality()
    >>> otp.run(data)
                         Time  DATA_QUALITY_TYPE DATA_QUALITY_NAME
    0 2003-12-01 00:00:00.000                  0                OK
    1 2003-12-01 00:00:00.001                  0                OK
    2 2003-12-01 00:00:00.002                  0                OK

    Intercepting data quality events will remove them from the data flow:

    >>> data = otp.Ticks({'A': [1, 2, 3]})
    >>> data = data.insert_data_quality_event('OK')
    >>> data = data.intercept_data_quality()
    >>> data = data.show_data_quality()
    >>> otp.run(data)  # doctest: +ELLIPSIS
    Empty DataFrame
    ...
    """
    self.sink(
        otq.InterceptDataQuality()
    )
    return self


@inplace_operation
def show_symbol_errors(self: 'Source', inplace=False) -> Optional['Source']:
    """
    This method propagates a tick representing per-symbol error.

    Parameters
    ----------
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    | **SHOW_SYMBOL_ERRORS** OneTick event processor
    | :py:meth:`intercept_symbol_errors`
    | :py:meth:`onetick.py.Source.throw`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    By default symbol errors are not showed, use this method to see them:

    >>> data = otp.Ticks({'A': [1, 2, 3]})
    >>> data = data.throw('WRONG', scope='symbol')
    >>> data = data.show_symbol_errors()
    >>> otp.run(data)
            Time  ERROR_CODE ERROR_MESSAGE
    0 2003-12-01           1         WRONG
    1 2003-12-01           1         WRONG
    2 2003-12-01           1         WRONG
    """

    self.sink(
        otq.ShowSymbolErrors()
    )
    self.schema.set(**{'ERROR_CODE': int, 'ERROR_MESSAGE': str})
    return self


@inplace_operation
def intercept_symbol_errors(self: 'Source', inplace=False) -> Optional['Source']:
    """
    This method removes removes per-symbol errors, thus preventing them from delivery to the client application.

    Parameters
    ----------
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.

    See also
    --------
    | **INTERCEPT_SYMBOL_ERRORS** OneTick event processor
    | :py:meth:`show_symbol_errors`
    | :py:meth:`onetick.py.Source.throw`

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    By default data quality events are not showed, use this method to see them:

    >>> data = otp.Ticks({'A': [1, 2, 3]})
    >>> data = data.throw('WRONG', scope='symbol')
    >>> data = data.show_symbol_errors()
    >>> otp.run(data)
            Time  ERROR_CODE ERROR_MESSAGE
    0 2003-12-01           1         WRONG
    1 2003-12-01           1         WRONG
    2 2003-12-01           1         WRONG

    Intercepting data quality events will remove them from the data flow:

    >>> data = otp.Ticks({'A': [1, 2, 3]})
    >>> data = data.throw('WRONG', scope='symbol')
    >>> data = data.intercept_symbol_errors()
    >>> data = data.show_symbol_errors()
    >>> otp.run(data)  # doctest: +ELLIPSIS
    Empty DataFrame
    ...
    """
    self.sink(
        otq.InterceptSymbolErrors()
    )
    return self
