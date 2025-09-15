from typing import TYPE_CHECKING, Callable, List, Optional, Tuple, Union

from onetick import py as otp
from onetick.py import configuration
from onetick.py.otq import otq

from .misc import inplace_operation

if TYPE_CHECKING:
    from onetick.py.core.source import Source


@inplace_operation
def throw(
    self: 'Source', message='', where=1, scope='query', error_code=1, throw_before_query_execution=False, inplace=False
) -> Optional['Source']:
    """
    Propagates error or warning or throws an exception (depending on ``scope`` parameter)
    when condition provided by ``where`` parameter evaluates to True.

    Throwing an exception will abort query execution,
    propagating error will stop tick propagation and
    propagating warning will not change the data processing.

    Parameters
    ----------
    message: str or Operation
        Message that will be thrown in exception or returned in error message.
    where: Operation
        Logical expression that specifies condition when exception or error will be thrown.
    scope: 'query' or 'symbol'
        'query' will throw an exception and 'symbol' will propagate an error or warning
        depending on ``error_code`` parameter.
    error_code: int
        When ``scope='symbol'``, values from interval ``[1, 500]`` indicate warnings
        and values from interval ``[1500, 2000]`` indicate errors.
        Note that tick propagation will not stop when warning is raised,
        and will stop when error is raised.

        This parameter is not used when ``scope='query'``.
    throw_before_query_execution: bool
        If set to ``True``, the exception will be thrown before the execution of the query.
        This option is intended for supplying placeholder queries that must always throw.
    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing.
        Otherwise method returns a new modified object.

    See also
    --------
    **THROW** OneTick event processor

    Returns
    -------
    :class:`Source` or ``None``

    Examples
    --------

    By default, this method will throw an exception on the first tick with empty message:

    >>> t = otp.Tick(A=1)
    >>> t = t.throw()
    >>> otp.run(t)  # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    Exception: ...: In THROW: ...
        ...

    You can specify exception message and condition (note that exception now is raised on second tick):

    >>> t = otp.Ticks(A=[1, 2])
    >>> t = t.throw(message='A is ' + t['A'].apply(str), where=(t['A']==2))
    >>> otp.run(t)  # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    Exception: ...: In THROW: A is 2. ...
        ...

    Note that exception will not be thrown if there are no ticks:

    >>> t = otp.Empty()
    >>> t = t.throw()
    >>> otp.run(t)
    Empty DataFrame
    Columns: []
    Index: []

    If you need exception to be thrown always, you can use ``throw_before_query_execution`` parameter:

    >>> t = otp.Empty()
    >>> t = t.throw(throw_before_query_execution=True)
    >>> otp.run(t)  # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    Exception: ...: In THROW: ...

    You can throw OneTick errors and warnings instead of exceptions.
    Raising error codes from 1 to 500 indicates warnings.
    Raising error codes from 1500 to 2000 indicates errors.
    Tick propagation stops only when error is raised.

    >>> t = otp.Ticks(A=[1, 2, 3, 4])
    >>> t = t.throw(message='warning A=1', scope='symbol', error_code=2, where=(t['A']==1))
    >>> t = t.throw(message='error A=3', scope='symbol', error_code=1502, where=(t['A']==3))
    >>> otp.run(t)
                         Time  A
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2

    Right now the only supported interface to get errors and warnings
    is via ``map`` output structure and its methods:

    >>> otp.run(t, symbols='AAPL', output_structure='map').output('AAPL').error
    [(2, 'warning A=1'), (1502, 'error A=3')]
    """
    if isinstance(message, str):
        treat_message_as_per_tick_expr = False
    else:
        treat_message_as_per_tick_expr = True
    if scope not in {'query', 'symbol'}:
        raise ValueError("Parameter 'scope' can only be set to 'query' or 'symbol'")
    if not (1 <= error_code <= 500 or 1500 <= error_code <= 2000):
        raise ValueError(
            "Parameter 'error_code' can only take values "
            "from interval [1, 500] for warnings and from interval [1500, 2000] for errors"
        )
    self.sink(
        otq.Throw(
            message=str(message),
            treat_message_as_per_tick_expr=treat_message_as_per_tick_expr,
            where=str(where),
            scope=scope.upper(),
            error_code=error_code,
            throw_before_query_execution=throw_before_query_execution,
        )
    )
    return self


@inplace_operation
def logf(
    self: 'Source', message: str, severity: str, *args, where: Optional['otp.Operation'] = None, inplace: bool = False
) -> Optional['Source']:
    """
    Call built-in OneTick ``LOGF`` function.

    Parameters
    ----------
    message: str
        Log message/format string. The underlying formatting engine is the Boost Format Library:
        https://www.boost.org/doc/libs/1_53_0/libs/format/doc/format.html

    severity: str
        Severity of message. Supported values: ``ERROR``, ``WARNING`` and ``INFO``.

    where: Operation
        A condition that allows to filter ticks to call ``LOGF`` on. ``None`` for no filtering.

    inplace: bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object.

    args: list
        Parameters for format string (optional).

    Returns
    -------
    :class:`Source`

    Examples
    --------
    >>> data = otp.Ticks(PRICE=[97, 99, 103])
    >>> data = data.logf("PRICE (%1%) is higher than 100", "WARNING", data["PRICE"], where=(data["PRICE"] > 100))
    """

    where_in_tick = where if where is not None else True

    def per_tick_script(tick):
        if where_in_tick:
            otp.logf(message, severity, *args)

    return self.script(per_tick_script, inplace=inplace)


def dump(
    self: 'Source',
    label: Optional[str] = None,
    where: Optional['otp.Operation'] = None,
    columns: Union[str, Tuple[str], List[str], None] = None,
    callback: Optional[Callable] = None,
):
    """
    Dumps the ``columns`` from ticks into std::out in runtime if they fit the ``where`` condition.
    Every dump has a corresponding header that always includes the TIMESTAMP field. Other fields
    could be configured using the ``columns`` parameter. A header could be augmented with a ``label`` parameter;
    this label is an addition column that helps to distinguish ticks
    from multiple dumps with the same schema, because ticks from different dumps could be mixed.
    It might happen because of the OneTick multithreading, and there is the operating system
    buffer between the OneTick and the actual output.

    This method is helpful for debugging.

    Parameters
    ----------
    label: str
        A label for a dump. It adds a special column **_OUT_LABEL_** for all ticks and set to the
        specified value. It helps to distinguish ticks from multiple dumps, because actual
        output could contain mixed ticks due the concurrency. ``None`` means no label.
    where: Operation
        A condition that allows to filter ticks to dump. ``None`` means no filtering.
    columns: str, tupel or list
        List of columns that should be in the output. ``None`` means dump all columns.
    callback : callable
        Callable, which preprocess source before printing.

    See also
    --------
    **WRITE_TEXT** OneTick event processor

    Examples
    --------
    >>> # OTdirective: skip-snippet:;
    >>> data.dump(label='Debug point', where=data['PRICE'] > 99.3, columns=['PRICE', 'QTY'])    # doctest: +SKIP
    >>> data.dump(columns="X", callback=lambda x: x.first(), label="first")     # doctest: +SKIP
    """

    self_c = self.copy()
    if callback:
        self_c = callback(self_c)
    if where is not None:  # can't be simplified because the _Operation overrides __bool__
        self_c, _ = self_c[(where)]
    if columns:
        self_c = self_c[columns if isinstance(columns, (list, tuple)) else [columns]]
    if label:
        self_c['_OUT_LABEL_'] = label

    self_c.write_text(
        formats_of_fields={'TIMESTAMP': f'%|{configuration.config.tz}|%d-%m-%Y %H:%M:%S.%J'},
        prepend_timestamp=True,
        prepend_symbol_name=False,
        propagate_ticks=True,
        inplace=True,
    )

    # print <no data> in case there are 0 ticks
    if hasattr(otq, 'InsertAtEnd'):
        self_c.sink(otq.InsertAtEnd(delimiter_name='AT_END'))
        self_c.schema['AT_END'] = int
        self_c.state_vars['COUNT'] = otp.state.var(0, scope='branch')
        self_c.state_vars['COUNT'] += 1
        self_c, _ = self_c[(self_c['AT_END'] == 1) & (self_c.state_vars['COUNT'] == 1)]
        self_c['NO_DATA'] = '<no data>'
        self_c = self_c[['NO_DATA']]
        self_c.write_text(output_headers=False, prepend_timestamp=False, prepend_symbol_name=False,
                          propagate_ticks=False, inplace=True)

    # Do not propagate ticks then, because we want just to write them into
    # the std::out. We have to do that, because otherwise these ticks would
    # go to a query output, that would mess real output.
    self_c, _ = self_c[self_c['Time'] != self_c['Time']]  # NOSONAR

    # We have to merge the branch back to the main branch even that these
    # branch does not generate ticks, because we do not introduce one more
    # output point, because the OneTick would add it to the final output
    # datastructure.
    self.sink(otq.Merge(identify_input_ts=False))

    self.source(self_c.node().copy_graph())  # NOSONAR
    self.node().add_rules(self_c.node().copy_rules())
    self._merge_tmp_otq(self_c)
