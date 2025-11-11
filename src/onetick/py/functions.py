import itertools
import warnings
import inspect
import re
import datetime as dt
from collections import defaultdict, Counter
from functools import singledispatch
from itertools import chain, zip_longest, repeat
from typing import List, Union, Type, Optional, Sequence
from enum import Enum

from onetick.py.otq import otq

from onetick.py.configuration import config, default_presort_concurrency
from onetick.py.core.eval_query import _QueryEvalWrapper
from onetick.py.core._source._symbol_param import _SymbolParamSource
from onetick.py.core._source.tmp_otq import TmpOtq
from onetick.py.utils import get_type_that_includes, adaptive, default
import onetick.py.types as ott
from onetick.py.core.column import Column
from onetick.py.core.column_operations.base import Operation
from onetick.py.core.cut_builder import _QCutBuilder, _CutBuilder
from onetick.py.backports import Literal
from onetick.py.compatibility import (
    is_supported_join_with_aggregated_window,
    is_supported_next_in_join_with_aggregated_window,
    is_apply_rights_supported,
)


__all__ = ['merge', 'join', 'join_by_time', 'apply_query', 'apply', 'cut', 'qcut', 'coalesce', 'corp_actions', 'format']


def output_type_by_index(sources, index):
    if index is None:
        from onetick.py.core.source import _Source
        return _Source
    return type(sources[index])


def apply_symbol_to_ep(base_ep, symbol, tmp_otq, symbol_date=None):
    if not symbol:
        return base_ep

    from onetick.py.core.source import _Source
    from onetick.py.sources import query as otp_query

    if isinstance(symbol, _QueryEvalWrapper):
        symbol = symbol.to_eval_string(tmp_otq=tmp_otq, symbol_date=symbol_date)
    elif isinstance(symbol, otp_query):
        if symbol_date is not None:
            raise ValueError("Parameter 'symbol_date' is not supported if symbols are set with otp.query object")
        symbol = symbol.to_eval_string()
    elif isinstance(symbol, (_Source, otq.GraphQuery)):
        symbol = _Source._convert_symbol_to_string(symbol, tmp_otq=tmp_otq, symbol_date=symbol_date,)

    return base_ep.symbols(symbol)


def merge(sources, align_schema=True, symbols=None, identify_input_ts=False,
          presort=adaptive, concurrency=default, batch_size=default, output_type_index=None,
          add_symbol_index: bool = False, separate_db_name: bool = False,
          added_field_name_suffix: str = '', stabilize_schema: Union[Type[adaptive], bool] = adaptive,
          enforce_order: bool = False, symbol_date=None):
    """
    Merges ticks from the ``sources`` into a single output ordered by the timestamp.

    Note
    ----
    If merged ticks have the same timestamp, their order is not guaranteed by default.
    Set parameter ``enforce_order`` to set the order according to parameter ``sources``.

    Parameters
    ----------
    sources : list
        List of sources to merge
    align_schema : bool
        If set to True, then table is added right after merge.
        We recommended to keep True to prevent problems with
        different tick schemas. Default: True
    symbols: str, list of str or functions, :class:`Source`, :py:class:`onetick.query.GraphQuery`
        Symbol(s) to run the query for passed as a string, a list of strings, or as a "symbols" query which results
        include the ``SYMBOL_NAME`` column. The start/end times for the
        symbols query will taken from the :meth:`run` params.
        See :ref:`symbols <static/concepts/symbols:Symbols: bound and unbound>` for more details.
    identify_input_ts: bool
        If set to False, the fields *SYMBOL_NAME* and *TICK_TYPE* are not appended to the output ticks.
    presort: bool
        Add the **PRESORT** EP before merging.
        By default, it is set to True if ``symbols`` are set
        and to False otherwise.
    concurrency: int
        Specifies the number of CPU cores to utilize for the ``presort``.
        By default, the value is inherited from the value of the query where this PRESORT is used.

        For the main query it may be specified in the ``concurrency`` parameter of :meth:`run` method
        (which by default is set to
        :py:attr:`otp.config.default_concurrency<onetick.py.configuration.Config.default_concurrency>`).

        For the auxiliary queries (like first-stage queries) empty value means OneTick's default of 1.
        If :py:attr:`otp.config.presort_force_default_concurrency<onetick.py.configuration.Config.presort_force_default_concurrency>`
        is set then default concurrency value will be set in all PRESORT EPs in all queries.
    batch_size: int
        Specifies the query batch size for the ``presort``.
        By default, the value from
        :py:attr:`otp.config.default_batch_size<onetick.py.configuration.Config.default_batch_size>`
        is used.
    output_type_index: int
        Specifies index of source in ``sources`` from which type and properties of output will be taken.
        Useful when merging sources that inherited from :class:`Source`.
        By default, output object type will be :class:`Source`.
    add_symbol_index: bool
        If set to True, this function adds a field *SYMBOL_INDEX* to each tick,
        with a numeric index (1-based) corresponding to the symbol the tick is for.
    separate_db_name: bool
        If set to True, the security name of the input time series is separated into
        the pure symbol name and the database name parts
        propagated in the *SYMBOL_NAME* and *DB_NAME* fields, respectively.
        Otherwise, the full symbol name is propagated in a single field called *SYMBOL_NAME*.
    added_field_name_suffix: str
        The suffix to add to the names of additional fields
        (that is, *SYMBOL_NAME*, *TICK_TYPE*, *DB_NAME* and *SYMBOL_INDEX*).
    stabilize_schema: bool
        If set to True, any fields that were present on any tick in the input time series
        will be present in the ticks of the output time series.
        New fields will be added to the output tick at the point they are first seen in the input time series.
        If any field already present in the input is not present on a given input tick,
        its type will be determined by the widest encountered type under that field name.
        Incompatible types (for example, int and float) under the same field name will result in an exception.

        Default is False.
    enforce_order: bool
        If merged ticks have the same timestamp, their order is not guaranteed by default.
        Set this parameter to True to set the order according to parameter ``sources``.

        Special OneTick field *OMDSEQ* will be used to order sources.
        If it exists then it will be overwritten and deleted.
    symbol_date: :py:class:`otp.datetime <onetick.py.datetime>` or :py:class:`datetime.datetime` or int
        Symbol date or integer in the YYYYMMDD format.
        Can only be specified if parameters ``symbols`` is set.

    Returns
    -------
    :class:`Source` or same class as ``sources[output_type_index]``
        A time series of ticks.

    See also
    --------
    **MERGE** and **PRESORT** OneTick event processors

    Examples
    --------

    ``merge`` is used to merge different data sources:

    >>> data1 = otp.Ticks(X=[1, 2], Y=['a', 'd'])
    >>> data2 = otp.Ticks(X=[-1, -2], Y=['*', '-'])
    >>> data = otp.merge([data1, data2])      # OTdirective: snippet-name:merge.as list;
    >>> otp.run(data)
                         Time  X  Y
    0 2003-12-01 00:00:00.000  1  a
    1 2003-12-01 00:00:00.000 -1  *
    2 2003-12-01 00:00:00.001  2  d
    3 2003-12-01 00:00:00.001 -2  -

    Merge series from multiple symbols into one series:

    >>> # OTdirective: snippet-name:merge.bound symbols;
    >>> data = otp.Ticks(X=[1])
    >>> data['SYMBOL_NAME'] = data['_SYMBOL_NAME']
    >>> symbols = otp.Ticks(SYMBOL_NAME=['A', 'B'])
    >>> data = otp.merge([data], symbols=symbols)
    >>> otp.run(data)
            Time  X SYMBOL_NAME
    0 2003-12-01  1           A
    1 2003-12-01  1           B

    Use ``identify_input_ts`` and other parameters to add information about symbol to each tick:

    >>> symbols = otp.Ticks(SYMBOL_NAME=['COMMON::S1', 'DEMO_L1::S2'])
    >>> data = otp.Tick(A=1, db=None, tick_type='TT')
    >>> data = otp.merge([data], symbols=symbols, identify_input_ts=True,
    ...                        separate_db_name=True, add_symbol_index=True, added_field_name_suffix='__')
    >>> otp.run(data)
            Time  A SYMBOL_NAME__ DB_NAME__ TICK_TYPE__  SYMBOL_INDEX__
    0 2003-12-01  1            S1    COMMON          TT               1
    1 2003-12-01  1            S2   DEMO_L1          TT               2

    Adding symbol parameters before merge:

    >>> symbols = otp.Ticks(SYMBOL_NAME=['S1', 'S2'], param=[1, -1])
    >>> def func(symbol):
    ...     pre = otp.Ticks(X=[1])
    ...     pre["SYMBOL_NAME"] = symbol.name
    ...     pre["PARAM"] = symbol.param
    ...     return pre
    >>> data = otp.merge([func], symbols=symbols)
    >>> otp.run(data)[['PARAM', 'SYMBOL_NAME']]
       PARAM SYMBOL_NAME
    0      1          S1
    1     -1          S2

    Use parameter ``output_type_index`` to specify which input class to use to create output object.
    It may be useful in case some custom user class was used as input:

    >>> class CustomTick(otp.Tick):
    ...     def custom_method(self):
    ...         return 'custom_result'
    >>> data1 = otp.Tick(A=1)
    >>> data2 = CustomTick(B=2)
    >>> data = otp.merge([data1, data2], output_type_index=1)
    >>> type(data)
    <class 'onetick.py.functions.CustomTick'>
    >>> data.custom_method()
    'custom_result'
    >>> otp.run(data)
            Time  A  B
    0 2003-12-01  1  0
    1 2003-12-01  0  2
    """  # noqa: E501
    from onetick.py.core.source import _Source

    if not sources:
        raise ValueError("Merge should have one or more inputs")

    output_type = output_type_by_index(sources, output_type_index)

    if presort is adaptive:
        presort = True if symbols is not None else False

    if concurrency is not default and not presort:
        warnings.warn("Using the `concurrency` parameter makes effect only when "
                      "the `presort` parameter is set to True")
    if batch_size is not default and not presort:
        warnings.warn("Using the `batch_size` parameter makes effect only when "
                      "the `presort` parameter is set to True")

    if concurrency is default:
        concurrency = default_presort_concurrency()
    if concurrency is None:
        # None means inherit concurrency from the query where this EP is used
        # otq.Presort does not support None
        concurrency = ''

    if batch_size is default:
        batch_size = config.default_batch_size

    merge_kwargs = {
        'identify_input_ts': identify_input_ts,
        'add_symbol_index': add_symbol_index,
        'separate_db_name': separate_db_name,
        'added_field_name_suffix': added_field_name_suffix,
    }

    if 'stabilize_schema' in otq.Merge.Parameters.list_parameters():
        if stabilize_schema is adaptive:
            stabilize_schema = False
        merge_kwargs['stabilize_schema'] = stabilize_schema
    elif stabilize_schema is not adaptive:
        raise ValueError("Parameter 'stabilize_schema' is not supported in this OneTick build")

    if symbol_date is not None:
        if symbols is None:
            raise ValueError("Parameter 'symbol_date' can only be specified together with parameter 'symbols'")
        if isinstance(symbols, (str, list)):
            # this is a hack
            # onetick.query doesn't have an interface to set symbol_date for the EP node
            # so instead of setting symbols for the EP node,
            # we will turn symbol list into the first stage query, and symbol_date will be set for this query
            import onetick.py as otp
            if isinstance(symbols, str):
                symbols = [symbols]
            symbols = otp.Ticks(SYMBOL_NAME=symbols)

    def _base_ep_for_cross_symbol(symbol, tmp_otq, symbol_date=None):
        if presort:
            base_ep = otq.Presort(batch_size=batch_size, max_concurrency=concurrency)
        else:
            base_ep = otq.Merge(**merge_kwargs)

        base_ep = apply_symbol_to_ep(base_ep, symbol, tmp_otq, symbol_date=symbol_date)

        return base_ep

    def _evaluate_functions_in_sources_list(sources, symbols):
        result = []

        if not isinstance(sources, list):
            sources = [sources]

        for s in sources:
            if not isinstance(s, _Source) and callable(s):
                num_params = len(inspect.signature(s).parameters)

                if num_params == 0:
                    s = s()
                elif num_params == 1:
                    s = s(symbols.to_symbol_param() if isinstance(symbols, (_Source, _QueryEvalWrapper))
                          else _SymbolParamSource())
                else:
                    raise ValueError(
                        f"It is expected only one parameter from the callback, but {num_params} passed"
                    )  # TODO: test this case
            if isinstance(s, _Source):
                result.append(s)
            else:
                raise ValueError("Source and functions (returning _source) are expected as preprocessors")
        return result

    sources = _evaluate_functions_in_sources_list(sources, symbols)
    if enforce_order:
        sources = _enforce_order_for_sources(sources)
    need_table = False
    merged_columns, need_table, used_columns = _collect_merged_columns(need_table, sources)
    need_table = _is_table_after_merge_needed(need_table, used_columns)

    # we need to store internal graphs somewhere while we create base ep from eval
    intermediate_tmp_otq = TmpOtq()
    result = output_type(node=_base_ep_for_cross_symbol(symbols, tmp_otq=intermediate_tmp_otq, symbol_date=symbol_date),
                         schema=merged_columns)
    result._tmp_otq.merge(intermediate_tmp_otq)

    __copy_sources_on_merge_or_join(result, sources, symbols, output_type_index=output_type_index)

    if presort:
        result.sink(otq.Merge(**merge_kwargs))

    if enforce_order:
        result.drop('OMDSEQ', inplace=True)
        merged_columns.pop('OMDSEQ')

    if identify_input_ts:
        result.schema['SYMBOL_NAME' + added_field_name_suffix] = str
        result.schema['TICK_TYPE' + added_field_name_suffix] = str
        if separate_db_name:
            result.schema['DB_NAME' + added_field_name_suffix] = str

    if add_symbol_index:
        result.schema['SYMBOL_INDEX' + added_field_name_suffix] = int

    result = _add_table_after_merge(align_schema, merged_columns, need_table, result)
    result._fix_varstrings()
    return result


def _add_table_after_merge(add_table, merged_columns, need_table, result):
    if add_table and need_table:
        # a special case, when the add_table parameter is a list of common columns that should
        # be added to a final table
        # it is used internally
        if isinstance(add_table, list):
            merged_columns = {key: value for key, value in merged_columns.items() if key in add_table}

        if len(merged_columns):
            table = otq.Table(
                fields=",".join(ott.type2str(dtype) + " " + name for name, dtype in merged_columns.items()),
                keep_input_fields=True,
            )
            result.sink(table)
    return result


def __copy_sources_on_merge_or_join(result,
                                    sources,
                                    symbols=None,
                                    names=None,
                                    drop_meta=False,
                                    leading=None,
                                    output_type_index=None,
                                    use_rename_ep=True):
    """ copy columns, state vars and other metadata from joining, merging sources

    Parameters
    ----------
    result: _Source
        Source object constructed as join, merge operation, e.g. result = _Source(otq.Merge(sources))
    sources: list of _Source, tuple of _Source
        Sources were joined, merged
    symbols:
        Symbols to copy
    names: list of str or None, tuple of str or None, bool, optional
        - If collection of string or None than add passthrough eps with such name to `sources` if name is specify
            or do not add anything if corresponding item in names is None.
        - If True, than autogenerate such names in __SRC_{number}__ format
        - If None, False than do not add passthrough eps and do not change node names.
    drop_meta : bool, optional
        If True drop TIMESTAMP and OMDSEQ field
    leading : List of str, Tuple of str, Optional
        List of leading sources names
    output_type_index: int, optional
        Specifies index of source in `sources` from which properties of `result` will be taken.
        Useful when merging sources that inherited from otp.Source.
    use_rename_ep: bool
        Use :py:class:`onetick.query.RenameFields` event processor or not.
        This event processor can't be used in generic aggregation.

    Returns
    -------
        None
        Modify result directly
    """
    from onetick.py.core.source import _Source

    result._copy_state_vars_from(sources)
    result._clean_sources_dates()  # because it is not a real _source

    for source in sources:
        result._merge_tmp_otq(source)
        if source.get_name():
            if not result.get_name():
                result.set_name(source.get_name())
            if result.get_name() != source.get_name():
                warnings.warn(f"Merging/joining sources with different names: '{result.get_name()}' "
                              f"and '{source.get_name()}'. Some of those names will be lost")

    if isinstance(symbols, _Source):
        result._merge_tmp_otq(symbols)

    names = __copy_and_rename_nodes_on_merge_join(result, names, sources, symbols)

    if drop_meta:
        to_drop = list(map(lambda x: x + ".TIMESTAMP", names))
        to_drop += list(map(lambda x: x + ".OMDSEQ", names))
        __rename_leading_omdseq(leading, names, result, sources, use_rename_ep=use_rename_ep)
        result.sink(otq.Passthrough(fields=",".join(to_drop), drop_fields=True))

    if output_type_index is not None:
        result._copy_properties_from(sources[output_type_index])


def __rename_fields(source, mapping, use_rename_ep=True):
    """
    Function to rename fields from ``mapping`` in ``source``.
    Note that it is a low-level function that doesn't change python schema of the ``source``.
    Modifies ``source`` inplace, doesn't return anything.
    If ``use_rename_ep`` is `True`, then :py:class:`onetick.query.RenameFields` event processor will be used.
    """
    if use_rename_ep:
        source.sink(otq.RenameFields(','.join(f'{k}={v}' for k, v in mapping.items())))
        return
    # May be needed, because RenameFields ep is not supported in generic aggregation
    for old, new in mapping.items():
        # RenameFields ignores non-existent fields,
        # all this mess is needed to mimic that logic
        source.sink(otq.WhereClause(where=f'UNDEFINED("{old}")'))
        if_branch_graph = source.node().copy_graph()
        if_branch_rules = source.node().copy_rules()
        source.sink(otq.AddField(new, old), out_pin='ELSE')
        source.sink(otq.Passthrough(old, drop_fields=True))
        source.sink(otq.Merge(identify_input_ts=False))
        source.source(if_branch_graph)
        source.node().add_rules(if_branch_rules)


def __rename_leading_omdseq(leading, names, result, sources, use_rename_ep=True):
    if leading is not None:
        if len(leading) == 1:
            leading = leading.pop()
            __rename_fields(result, {f"{leading}.OMDSEQ": "OMDSEQ"}, use_rename_ep=use_rename_ep)
        else:
            number, indexes = __get_number_and_indexes_of_sources_have_field(sources, "OMDSEQ")
            if number == 1:
                __rename_fields(result, {f"{names[indexes.pop()]}.OMDSEQ": "OMDSEQ"}, use_rename_ep=use_rename_ep)
            elif number:
                raise ValueError(
                    "Several sources was specified as leading and OMDSEQ field is presented in more than "
                    "one source. Resulted OMDSEQ can't be derived in such case."
                )


def __get_number_and_indexes_of_sources_have_field(sources, field):
    number = 0
    indexes = []
    for s in sources:
        if field in s.columns():
            indexes.append(number)
            number += 1
    return number, indexes


def __copy_and_rename_nodes_on_merge_join(result, names, sources, symbols):
    # shared eps between sources
    eps = defaultdict()
    if names is True:
        names = [f"__SRC_{n}__" for n in range(len(sources))]
    if not names:
        names = itertools.repeat(None)
    if sources:
        for name, src in zip(names, sources):
            obj = src
            if name:
                obj = src.copy()
                obj.sink(otq.Passthrough())
                obj.node_name(name)

            result.source(obj.node().copy_graph(eps))
            result.node().add_rules(obj.node().copy_rules())
            result._set_sources_dates(obj, copy_symbols=not bool(symbols))
    return names


def _is_table_after_merge_needed(need_table, used_columns):
    if not need_table:
        for key, value in used_columns.items():
            if not value:
                need_table = True
                break

    return need_table


def _collect_merged_columns(need_table, sources):
    merged_columns = sources[0].columns(skip_meta_fields=True)
    used_columns = {key: False for key in merged_columns.keys()}
    for src in sources[1:]:
        for key, value in src.columns(skip_meta_fields=True).items():
            if key in merged_columns:
                orig_type = merged_columns[key]
                try:
                    merged_dtype, merged_need_table = get_type_that_includes([orig_type, value])
                except ValueError as e:
                    raise ValueError(f"Column '{key}' has different types for "
                                     f"different branches: {orig_type} {value}") from e

                need_table |= merged_need_table
                merged_columns[key] = merged_dtype
            else:
                need_table = True
                merged_columns[key] = value

            if key in used_columns:
                used_columns[key] = True

    return merged_columns, need_table, used_columns


def concat(sources=None, add_table=True, symbols=None):
    """ Deprecated: Merges ticks from the sources into a single output _source ordered by the timestamp

    This function is deprecated due the wrong name notation.
    Use 'merge' instead.

    Parameters
    ----------
    sources : list
        List of sources to merge
    align_schema : bool
        If set to True, then table is added right after merge.
        We recommended to keep True to prevent problems with
        different tick schemas. Default: True

    Returns
    -------
    A new _source that holds a result of the merged sources
    """
    warnings.warn("This function is deprecated due the wrong name notation. Use `merge` instead.", FutureWarning)
    return merge(sources=sources, align_schema=add_table, symbols=symbols)


def _add_node_name_prefix_to_columns_in_operation(op, src):
    """
    Add node name of souce ``src`` as prefix to all columns names in operation ``op``.
    """
    if not isinstance(op, Operation):
        return op

    def fun(operation):
        if isinstance(operation, ott.ExpressionDefinedTimeOffset) and isinstance(operation.n, Operation):
            operation.n = operation.n._replace_parameters(fun)
        if isinstance(operation, Column) and operation.obj_ref is src:
            column = operation
            if not src.node_name().strip():
                raise ValueError('You set to use name for column prefix, but name is empty')
            name = f'{src.node_name()}.{column.name}'
            return Column(name, column.dtype, column.obj_ref, precision=getattr(column, "_precision", None))
        return None

    return op._replace_parameters(fun)


def _enforce_order_for_sources(sources):
    """
    Enforce order of sources by adding/modifying OMDSEQ field.
    """
    result = []
    for i, source in enumerate(sources):
        source = source.copy()
        source = source.table(strict=False, **{'OMDSEQ': int})
        source['OMDSEQ'] = i
        # this update_field is needed to let OneTick know that OMDSEQ was changed
        source.sink(otq.UpdateField(field='TIMESTAMP', value='TIMESTAMP'))
        result.append(source)
    return result


def join(left, right, on, how='left_outer', rprefix='RIGHT', keep_fields_not_in_schema=False, output_type_index=None):
    """
    Joins two sources ``left`` and ``right`` based on ``on`` condition.

    In case you willing to add prefix/suffix to all columns in one of the sources you should use
    :func:`Source.add_prefix` or :func:`Source.add_suffix`

    Parameters
    ----------
    left: :class:`Source`
        left source to join
    right: :class:`Source`
        right source to join
    on: :py:class:`~onetick.py.Operation` or 'all' or 'same_size' or list of strings

        If 'all' joins every tick from ``left`` with every tick from ``right``.

        If 'same_size' and size of sources are same, joins ticks from two sources directly, else raises exception.

        If it is list of strings, then ticks with same ``on`` fields will be joined.

        If :py:class:`~onetick.py.Operation` then only ticks on which the condition evaluates to True will be joined.
    how: 'inner' or 'left_outer'
        Joining type.
        Inner join will only produce ticks that matched the ``on`` condition.
        Left outer join will also produce the ticks from the ``left`` source
        that didn't match the condition.

        Doesn't matter for ``on='same_size'``.
    rprefix: str
        The name of ``right`` data source. It will be added as prefix to overlapping columns arrived
        from right to result
    keep_fields_not_in_schema: bool

        If True - join function will try to preserve any fields of original sources that are not in the source schema,
        propagating them to output. This means a possibility of runtime error if fields are duplicating.

        If False, will remove all fields that are not in schema.
    output_type_index: int
        Specifies index of source in sources from which type and properties of output will be taken.
        Useful when joining sources that inherited from :class:`Source`.
        By default output object type will be :class:`Source`.

    Note
    ----
    ``join`` does some internal optimization in case of using time-based ``on`` conditions. Optimization doesn't apply
    if ``on`` expression has functions in it. So it is recommended to use addition/subtraction number of
    milliseconds (integers).

    See examples for more details.

    Returns
    -------
    :class:`Source` or same class as ``[left, right][output_type_index]``
        joined data

    See also
    --------
    **JOIN** OneTick event processor

    Examples
    --------
    >>> d1 = otp.Ticks({'ID': [1, 2, 3], 'A': ['a', 'b', 'c']})
    >>> d2 = otp.Ticks({'ID': [2, 3, 4], 'B': ['q', 'w', 'e']})

    Outer join:

    >>> data = otp.join(d1, d2, on=d1['ID'] == d2['ID'], how='left_outer')
    >>> otp.run(data)
                         Time  ID  A  RIGHT_ID  B
    0 2003-12-01 00:00:00.000   1  a         0
    1 2003-12-01 00:00:00.001   2  b         2  q
    2 2003-12-01 00:00:00.002   3  c         3  w

    Inner join:

    >>> data = otp.join(d1, d2, on=d1['ID'] == d2['ID'], how='inner')
    >>> otp.run(data)
                         Time  ID  A  RIGHT_ID  B
    0 2003-12-01 00:00:00.001   2  b         2  q
    1 2003-12-01 00:00:00.002   3  c         3  w

    Join all ticks:

    >>> data = otp.join(d1, d2, on='all')
    >>> otp.run(data)
                         Time  ID  A  RIGHT_ID  B
    0 2003-12-01 00:00:00.000   1  a         2  q
    1 2003-12-01 00:00:00.000   1  a         3  w
    2 2003-12-01 00:00:00.000   1  a         4  e
    3 2003-12-01 00:00:00.001   2  b         2  q
    4 2003-12-01 00:00:00.001   2  b         3  w
    5 2003-12-01 00:00:00.001   2  b         4  e
    6 2003-12-01 00:00:00.002   3  c         2  q
    7 2003-12-01 00:00:00.002   3  c         3  w
    8 2003-12-01 00:00:00.002   3  c         4  e

    Join same size sources:

    >>> data = otp.join(d1, d2, on='same_size')
    >>> otp.run(data)
                         Time  ID  A  RIGHT_ID  B
    0 2003-12-01 00:00:00.000   1  a         2  q
    1 2003-12-01 00:00:00.001   2  b         3  w
    2 2003-12-01 00:00:00.002   3  c         4  e

    Adding prefix to the right source for all columns:

    >>> d_right = d2.add_prefix('right_')
    >>> data = otp.join(d1, d_right, on=d1['ID'] == d_right['right_ID'])
    >>> otp.run(data)
                         Time  ID  A  right_ID  right_B
    0 2003-12-01 00:00:00.000   1  a         0
    1 2003-12-01 00:00:00.001   2  b         2        q
    2 2003-12-01 00:00:00.002   3  c         3        w

    This condition will be optimized during run time:

    >>> data = otp.join(d1, d2, on=(d1['ID'] == d2['ID']) & (d1['Time'] >= d2['Time']), how='left_outer')
    >>> otp.run(data)
                         Time  ID  A  RIGHT_ID  B
    0 2003-12-01 00:00:00.000   1  a         0
    1 2003-12-01 00:00:00.001   2  b         2  q
    2 2003-12-01 00:00:00.002   3  c         3  w

    This condition won't be optimized during run time since in transforms addition to time into function.
    So please note, this way of using ``join`` is not recommended.

    >>> data = otp.join(d1, d2, on=(d1['ID'] == d2['ID']) & (d1['Time'] >= d2['Time'] + otp.Milli(1)), how='left_outer')
    >>> otp.run(data)
                         Time  ID  A  RIGHT_ID  B
    0 2003-12-01 00:00:00.000   1  a         0
    1 2003-12-01 00:00:00.001   2  b         2  q
    2 2003-12-01 00:00:00.002   3  c         3  w

    In such cases (adding/subtracting constants to time) adding/subtraction number of milliseconds should be done.
    This example will return exactly the same result as previous one, but it will be optimized, so runtime will be
    shorter.

    >>> data = otp.join(d1, d2, on=(d1['ID'] == d2['ID']) & (d1['Time'] >= d2['Time'] + 1), how='left_outer')
    >>> otp.run(data)
                             Time  ID  A  RIGHT_ID  B
        0 2003-12-01 00:00:00.000   1  a         0
        1 2003-12-01 00:00:00.001   2  b         2  q
        2 2003-12-01 00:00:00.002   3  c         3  w

    ``on`` can be list of strings:

    >>> left = otp.Ticks(A=[1, 2, 3], B=[4, 6, 7])
    >>> right = otp.Ticks(A=[2, 3, 4], B=[6, 9, 8], C=[7, 2, 0])
    >>> data = otp.join(left, right, on=['A', 'B'], how='inner')
    >>> otp.run(data)
                             Time  A  B  C
        0 2003-12-01 00:00:00.001  2  6  7

    Use parameter ``output_type_index`` to specify which input class to use to create output object.
    It may be useful in case some custom user class was used as input:

    >>> class CustomTick(otp.Tick):
    ...     def custom_method(self):
    ...         return 'custom_result'
    >>> data1 = otp.Tick(A=1)
    >>> data2 = CustomTick(B=2)
    >>> data = otp.join(data1, data2, on='same_size', output_type_index=1)
    >>> type(data)
    <class 'onetick.py.functions.CustomTick'>
    >>> data.custom_method()
    'custom_result'
    >>> otp.run(data)
            Time  A  B
    0 2003-12-01  1  2
    """
    output_type = output_type_by_index((left, right), output_type_index)

    on_list = []
    if isinstance(on, list):
        for column in on:
            if column not in left.schema:
                raise ValueError(f'`{column}` column does not exist in the left source.')
            if column not in right.schema:
                raise ValueError(f'`{column}` column does not exist in the right source.')
        if len(on) == 0:
            raise ValueError('`on` parameter can not be empty list.')
        on_list = on
        on = (left[on_list[0]] == right[on_list[0]])
        for column in on_list[1:]:
            on = on & (left[column] == right[column])

    timezone_hack = None
    if re.search(r'\b_TIMEZONE\b', str(on)):
        # join does not support using _TIMEZONE pseudo-field in join_criteria,
        # replacing it with temporary fields in the branches
        timezone_hack = '__TIMEZONE_HACK__'
        left[timezone_hack] = left['_TIMEZONE']
        right[timezone_hack] = right['_TIMEZONE']

    if str(on) == "all":
        on = f'1 = 1 or {rprefix}.TIMESTAMP >= 0'

    _LEFT_NODE_NAME = "__SRC_LEFT__"  # this is internal name
    _RIGHT_NODE_NAME = rprefix

    initial_left_source_node_name = left.node_name()
    initial_right_source_node_name = right.node_name()

    # we have to add _source prefix to all column operations
    # `on` expression is written with right, so we should modify it, we will restore it later
    left.node_name(_LEFT_NODE_NAME)
    right.node_name(_RIGHT_NODE_NAME)

    on = _add_node_name_prefix_to_columns_in_operation(on, left)
    on = _add_node_name_prefix_to_columns_in_operation(on, right)

    columns_name_set = set()
    columns = {}
    fields_to_skip_right_source = {'TIMESTAMP'}
    for name, dtype in chain(left.columns(skip_meta_fields=True).items(), right.columns(skip_meta_fields=True).items()):
        if name in columns_name_set:
            columns[_RIGHT_NODE_NAME + "_" + name] = dtype
            fields_to_skip_right_source.add(name)
        else:
            columns[name] = dtype
            columns_name_set.add(name)

    if how in ("left_outer", "outer"):
        join_type = "LEFT_OUTER"
        if how == "outer":
            warnings.warn("Value 'outer' for parameter 'how' is deprecated. Use 'left_outer' instead.",
                          FutureWarning)
    elif how == "inner":
        join_type = "INNER"
    else:
        raise ValueError("The 'how' parameter has wrong value. Only 'left_outer' and 'inner' are supported")

    if timezone_hack:
        on = re.sub(r'\._TIMEZONE\b', f'.{timezone_hack}', str(on))
        on = re.sub(r'\b_TIMEZONE\b', f'{_LEFT_NODE_NAME}.{timezone_hack}', str(on))

    # ------------------
    # create objects
    params = {"join_criteria": str(on), "join_type": join_type, "left_source": _LEFT_NODE_NAME}

    # return states of sources back
    left.node_name(initial_left_source_node_name)
    right.node_name(initial_right_source_node_name)
    if str(on) == "same_size":
        result = output_type(node=otq.JoinSameSizeTs(), schema=columns)
    else:
        result = output_type(node=otq.Join(**params), schema=columns)

    __copy_sources_on_merge_or_join(result, (left, right),
                                    names=(_LEFT_NODE_NAME, _RIGHT_NODE_NAME),
                                    output_type_index=output_type_index)

    rename_fields_dict = {}
    for lc, rc in zip_longest(left.columns(skip_meta_fields=True), right.columns(skip_meta_fields=True)):
        if lc:
            rename_fields_dict[f"{_LEFT_NODE_NAME}.{lc}"] = lc
        if rc:
            if rc not in fields_to_skip_right_source:
                rename_fields_dict[f"{_RIGHT_NODE_NAME}.{rc}"] = rc
            else:
                rename_fields_dict[f"{_RIGHT_NODE_NAME}.{rc}"] = f"{_RIGHT_NODE_NAME}_{rc}"

    __rename_fields(result, rename_fields_dict)
    result.sink(otq.Passthrough(fields=_LEFT_NODE_NAME + ".TIMESTAMP", drop_fields=True))

    items = []
    for name, dtype in result.columns(skip_meta_fields=True).items():
        items.append(ott.type2str(dtype) + " " + name)

    if keep_fields_not_in_schema:
        # Here we try to preserve fields of original sources that were not in schema
        # in their original form. If there's a duplication of fields or any other problem
        # in runtime, we'll be able to do nothing
        result.sink(otq.Passthrough(fields=_RIGHT_NODE_NAME + ".TIMESTAMP", drop_fields=True))
        result.sink(otq.RenameFieldsEp(rename_fields=rf"{_LEFT_NODE_NAME}\.(.*)=\1,{_RIGHT_NODE_NAME}\.(.*)=\1",
                                       use_regex=True))
        result.sink(otq.Table(fields=",".join(items), keep_input_fields=True))
    else:
        result.sink(otq.Table(fields=",".join(items)))

    if timezone_hack:
        result = result.drop([
            field for field in result.schema
            if field.endswith(timezone_hack)
        ])
        left.drop(timezone_hack, inplace=True)
        right.drop(timezone_hack, inplace=True)

    for column in on_list:
        result.drop(f'{_RIGHT_NODE_NAME}_{column}', inplace=True)

    return result


def join_by_time(sources, how="outer", on=None, policy=None, check_schema=True, leading=0,
                 match_if_identical_times=None, output_type_index=None, use_rename_ep=True,
                 source_fields_order=None, symbols=None):
    """
    Joins ticks from multiple input time series, based on input tick timestamps.

    ``leading`` source tick joined with already arrived ticks from other sources.

    >>> leading = otp.Ticks(A=[1, 2], offset=[1, 3])
    >>> other = otp.Ticks(B=[1], offset=[2])
    >>> otp.run(otp.join_by_time([leading, other]))
                         Time  A  B
    0 2003-12-01 00:00:00.001  1  0
    1 2003-12-01 00:00:00.003  2  1

    Note
    ----
    In case different ``sources`` have matching columns, the exception will be raised.

    To fix this error,
    functions :func:`Source.add_prefix` or :func:`Source.add_suffix` can be used to rename all columns in the source.

    Note that resulting **TIMESTAMP** pseudo-column will be taken from the leading source,
    and timestamps of ticks from non-leading sources will not be added to the output,
    so if you need to save them, you need to copy the timestamp to some other column.

    See examples below.

    Parameters
    ----------
    sources: Collection[:class:`Source`]
        The collection of Source objects which will be joined
    how: 'outer' or 'inner'
        The method of join ("inner" or "outer").
        Inner join logic will propagate ticks only if all sources participated in forming it.
        Outer join will propagate all ticks even if they couldn't be joined with other sources
        (in this case the fields from other sources will have "zero" values depending on the type of the field).
        Default is "outer".
    on: Collection[:class:`Column`]
        ``on`` add an extra check to join - only ticks with same ``on`` fields will be joined

        >>> leading = otp.Ticks(A=[1, 2], offset=[1, 3])
        >>> other = otp.Ticks(A=[2, 2], B=[1, 2], offset=[0, 2])
        >>> otp.run(otp.join_by_time([leading, other], on=['A']))
                             Time  A  B
        0 2003-12-01 00:00:00.001  1  0
        1 2003-12-01 00:00:00.003  2  2

    policy: 'arrival_order', 'latest_ticks', 'each_for_leader_with_first' or 'each_for_leader_with_latest'
        Policy of joining ticks with the same timestamps.
        The default value is "arrival_order" by default, but is set to "latest_ticks"
        if parameter ``match_if_identical_times`` is set to True.

        >>> leading = otp.Ticks(A=[1, 2], offset=[0, 0], OMDSEQ=[0, 3])
        >>> other = otp.Ticks(B=[1, 2], offset=[0, 0], OMDSEQ=[2, 4])

        Note: in the examples below we assume that all ticks have same timestamps, but order of ticks as in example.
        OMDSEQ is a special field that store order of ticks with same timestamp

        - ``arrival_order``
          output tick generated on arrival of ``leading`` source tick

        >>> data = otp.join_by_time([leading, other], policy='arrival_order')
        >>> otp.run(data)[['Time', 'A', 'B']]
                Time  A  B
        0 2003-12-01  1  0
        1 2003-12-01  2  1

        - ``latest_ticks``
          Tick generated at the time of expiration of a particular timestamp (when all ticks from all sources
          for current timestamp arrived). Only latest tick from ``leading`` source will be used.

        >>> data = otp.join_by_time([leading, other], policy='latest_ticks')
        >>> otp.run(data)[['Time', 'A', 'B']]
                Time  A  B
        0 2003-12-01  2  2

        - ``each_for_leader_with_first``
          Each tick from ``leading`` source will be joined with first tick from other sources for current timestamp

        >>> data = otp.join_by_time(
        ...     [leading, other],
        ...     policy='each_for_leader_with_first'
        ... )
        >>> otp.run(data)[['Time', 'A', 'B']]
                Time  A  B
        0 2003-12-01  1  1
        1 2003-12-01  2  1

        - ``each_for_leader_with_latest``
          Each tick from ``leading`` source will be joined with last tick from other sources for current timestamp

        >>> data = otp.join_by_time(
        ...     [leading, other],
        ...     policy='each_for_leader_with_latest'
        ... )
        >>> otp.run(data)[['Time', 'A', 'B']]
                Time  A  B
        0 2003-12-01  1  2
        1 2003-12-01  2  2

    check_schema: bool
        If True onetick.py will check that all columns names are unambiguous
        and columns listed in `on` param are exists in sources schema.
        Which can lead to false positive error
        in case of some event processors were sink to Source. To avoid this set check_scheme to False.
    leading: int, 'all', :class:`Source`, list of int, list of :class:`Source`
        A list of sources or their indexes. If this parameter is 'all', every source is considered to be leading.
        The logic of the leading source depends on ``policy`` parameter.
        The default value is 0, meaning the first specified source will be the leader.

    match_if_identical_times: bool
        A True value of this parameter causes an output tick to be formed from input ticks with identical timestamps
        only.
        If parameter ``how`` is set to 'outer',
        default values of fields (``otp.nan``, 0, empty string) are propagated for
        sources that did not tick at a given timestamp.
        If this parameter is set to True, the default value of ``policy`` parameter is set to 'latest_ticks'.
    output_type_index: int
        Specifies index of source in ``sources`` from which type and properties of output will be taken.
        Useful when joining sources that inherited from :class:`Source`.
        By default output object type will be :class:`Source`.
    use_rename_ep: bool
        This parameter specifies if :py:class:`onetick.query.RenameFields`
        event processor will be used in internal implementation of this function or not.
        This event processor can't be used in generic aggregations, so set this parameter to False
        if ``join_by_time`` is used in generic aggregation logic.
    source_fields_order: list of int, list of :class:`Source`
        Controls the order of fields in output ticks.
        If set, all input sources indexes or objects must be specified.
        By default, the order of the sources is the same as in the ``sources`` list.
    symbols: str, list of str or functions, :class:`Source`, :py:class:`onetick.query.GraphQuery`
        Bound symbol(s) passed as a string, a list of strings, or as a "symbols" query which results
        include the ``SYMBOL_NAME`` column. The start/end times for the
        symbols query will taken from the :meth:`run` params.
        See :ref:`symbols <static/concepts/symbols:Symbols: bound and unbound>` for more details.

        .. warning::
            Passing more than one source for join and setting ``symbols`` parameter at the same time aren't supported

        .. note::
            If bound symbols are specified as :class:`Source` or :py:class:`onetick.query.GraphQuery`,
            you **should** set schema for returned :class:`Source` object manually:
            ``onetick-py`` couldn't determine symbols from sub-query before running the query.

        .. note::
            If bound symbols are specified as :class:`Source` or :py:class:`onetick.query.GraphQuery`,
            and this sub-query returns only one symbol name, output columns wouldn't have a prefix with symbol name.

    See also
    --------
    **JOIN_BY_TIME** OneTick event processor

    Examples
    --------
    >>> d1 = otp.Ticks({'A': [1, 2, 3], 'offset': [1, 2, 3]})
    >>> d2 = otp.Ticks({'B': [1, 2, 4], 'offset': [1, 2, 4]})
    >>> otp.run(d1)
                         Time  A
    0 2003-12-01 00:00:00.001  1
    1 2003-12-01 00:00:00.002  2
    2 2003-12-01 00:00:00.003  3
    >>> otp.run(d2)
                         Time  B
    0 2003-12-01 00:00:00.001  1
    1 2003-12-01 00:00:00.002  2
    2 2003-12-01 00:00:00.004  4

    Default joining logic, outer join with the first source is the leader by default:

    >>> data = otp.join_by_time([d1, d2])
    >>> otp.run(data)
                         Time  A  B
    0 2003-12-01 00:00:00.001  1  0
    1 2003-12-01 00:00:00.002  2  1
    2 2003-12-01 00:00:00.003  3  2

    Leading source can be changed by using parameter ``leading``:

    >>> data = otp.join_by_time([d1, d2], leading=1)
    >>> otp.run(data)
                         Time  A  B
    0 2003-12-01 00:00:00.001  1  1
    1 2003-12-01 00:00:00.002  2  2
    2 2003-12-01 00:00:00.004  3  4

    Note that OneTick's logic is different depending on the order of sources specified,
    so specifying ``leading`` parameter in the previous example is not the same as changing the order of sources here:

    >>> data = otp.join_by_time([d2, d1], leading=0)
    >>> otp.run(data)
                         Time  B  A
    0 2003-12-01 00:00:00.001  1  0
    1 2003-12-01 00:00:00.002  2  1
    2 2003-12-01 00:00:00.004  4  3

    Parameter ``source_fields_order`` can be used to change the order of fields in the output,
    but it also affects the joining logic the same way as changing the order of sources:

    >>> data = otp.join_by_time([d1, d2], leading=1, source_fields_order=[1, 0])
    >>> otp.run(data)
                         Time  B  A
    0 2003-12-01 00:00:00.001  1  0
    1 2003-12-01 00:00:00.002  2  1
    2 2003-12-01 00:00:00.004  4  3

    Parameter ``how`` can be set to "inner".
    In this case only ticks that were successfully joined from all sources will be propagated:

    >>> data = otp.join_by_time([d1, d2], how='inner')
    >>> otp.run(data)
                         Time  A  B
    0 2003-12-01 00:00:00.002  2  1
    1 2003-12-01 00:00:00.003  3  2

    Set parameter ``match_if_identical_times`` to only join ticks with the same timestamps:

    >>> data = otp.join_by_time([d1, d2], how='inner', match_if_identical_times=True)
    >>> otp.run(data)
                         Time  A  B
    0 2003-12-01 00:00:00.001  1  1
    1 2003-12-01 00:00:00.002  2  2

    In case of conflicting names in different sources, exception will be raised:

    >>> d3 = otp.Ticks({'A': [1, 2, 4], 'offset': [1, 2, 4]})
    >>> data = otp.join_by_time([d1, d3])
    Traceback (most recent call last):
        ...
    ValueError: There are matched columns between sources: A

    Adding prefix to right source for all columns will fix this problem:

    >>> data = otp.join_by_time([d1, d3.add_prefix('right_')])
    >>> otp.run(data)
                         Time  A  right_A
    0 2003-12-01 00:00:00.001  1        0
    1 2003-12-01 00:00:00.002  2        1
    2 2003-12-01 00:00:00.003  3        2

    Note that timestamps from the non-leading source are not added to the output.
    You can add them manually in a different field:

    >>> d3['D3_TIMESTAMP'] = d3['TIMESTAMP']
    >>> data = otp.join_by_time([d1, d3.add_prefix('right_')])
    >>> otp.run(data)
                         Time  A  right_A      right_D3_TIMESTAMP
    0 2003-12-01 00:00:00.001  1        0 1969-12-31 19:00:00.000
    1 2003-12-01 00:00:00.002  2        1 2003-12-01 00:00:00.001
    2 2003-12-01 00:00:00.003  3        2 2003-12-01 00:00:00.002

    Use parameter ``output_type_index`` to specify which input class to use to create output object.
    It may be useful in case some custom user class was used as input:

    >>> class CustomTick(otp.Tick):
    ...     def custom_method(self):
    ...         return 'custom_result'
    >>> data1 = otp.Tick(A=1)
    >>> data2 = CustomTick(B=2)
    >>> data = otp.join_by_time([data1, data2], match_if_identical_times=True, output_type_index=1)
    >>> type(data)
    <class 'onetick.py.functions.CustomTick'>
    >>> data.custom_method()
    'custom_result'
    >>> otp.run(data)
            Time  A  B
    0 2003-12-01  1  2

    Use parameter ``source_fields_order`` to specify the order of output fields:

    >>> a = otp.Ticks(A=[1, 2])
    >>> b = otp.Ticks(B=[1, 2])
    >>> c = otp.Ticks(C=[1, 2])
    >>> data = otp.join_by_time([a, b, c], match_if_identical_times=True, source_fields_order=[c, b, a])
    >>> otp.run(data)
                         Time  C  B  A
    0 2003-12-01 00:00:00.000  1  1  1
    1 2003-12-01 00:00:00.001  2  2  2

    Indexes can be used too:

    >>> data = otp.join_by_time([a, b, c], match_if_identical_times=True, source_fields_order=[1, 2, 0])
    >>> otp.run(data)
                         Time  B  C  A
    0 2003-12-01 00:00:00.000  1  1  1
    1 2003-12-01 00:00:00.001  2  2  2

    Use parameter `symbols` to specify bound symbols:

    >>> data = otp.Ticks(X=[1, 2, 3, 4])
    >>> data = otp.join_by_time([data], symbols=['A', 'B'], match_if_identical_times=True)
    >>> otp.run(data)
                         Time  A.X  B.X
    0 2003-12-01 00:00:00.000    1    1
    1 2003-12-01 00:00:00.001    2    2
    2 2003-12-01 00:00:00.002    3    3
    3 2003-12-01 00:00:00.003    4    4

    Returns
    -------
    :class:`Source` or same class as ``sources[output_type_index]``
        A time series of ticks.
    """
    from onetick.py.core.source import _Source

    output_type = output_type_by_index(sources, output_type_index)

    if len(sources) > 1 and symbols:
        raise ValueError(
            'It\'s impossible to use `join_by_time` with multiple sources, '
            'when bound symbols are set via `symbols` parameter.'
        )

    join_str_keys = []

    # if key is set, then generalize it, ie convert into list;
    # then remove keys from 'columns_count' dict to pass validation after
    if on is not None:
        if isinstance(on, list):
            # okay
            pass
        elif isinstance(on, Column):
            on = [on]
        elif isinstance(on, str):
            on = [on]
        else:
            raise TypeError(f"It is not supported to have '{type(on)}' type as a key")

        for join_key in on:
            dtypes = set()
            if check_schema:
                for source in sources:
                    try:
                        key_type = source.schema[str(join_key)]
                    except KeyError as e:
                        raise KeyError(f"Column '{join_key}' not found in source schema {source}") from e
                    type_name = ott.type2str(key_type)
                    if type_name == "string[64]":
                        type_name = "string"
                    dtypes.add(type_name)
                if len(dtypes) > 1:
                    raise TypeError(f"Column '{join_key}' has different types in sources: {dtypes}")

            if isinstance(join_key, Column):
                join_str_keys.append(str(join_key))
            elif isinstance(join_key, str):
                join_str_keys.append(join_key)

    if check_schema:
        _check_schema_for_join_by_time(join_str_keys, sources)

    if how not in ["inner", "outer"]:
        raise ValueError('Wrong value for the "how" parameter. It is allowed to use "inner" or "outer" values')
    join_type = how.upper()

    # ------------------
    # create objects
    params = {"add_source_prefix": False, "join_type": join_type}
    leading = _fill_leading_sources_param(leading, params, sources)
    ordered_sources = _fill_source_fields_order_param(source_fields_order, params, sources)

    if on is not None:
        params["join_keys"] = ",".join(join_str_keys)

    if policy is not None:
        policies = {"arrival_order", "latest_ticks", "each_for_leader_with_first", "each_for_leader_with_latest"}
        if policy.lower() not in policies:
            raise ValueError("Invalid policy. Only the following ones are allowed: " + ", ".join(policies) + ".")
        params["same_timestamp_join_policy"] = policy.upper()

    if match_if_identical_times is not None:
        params["match_if_identical_times"] = match_if_identical_times

    is_bound_multi_symbol = False
    is_source_symbols = isinstance(symbols, (_Source, _QueryEvalWrapper))

    if isinstance(symbols, list) and len(symbols) > 1 or is_source_symbols:
        is_bound_multi_symbol = True
        params['add_source_prefix'] = True

    columns = {name: dtype for src in ordered_sources for name, dtype in src.columns(skip_meta_fields=True).items()}

    tmp_otq = TmpOtq()
    result = output_type(node=apply_symbol_to_ep(otq.JoinByTime(**params), symbols, tmp_otq), schema=columns)
    result._tmp_otq.merge(tmp_otq)

    __copy_sources_on_merge_or_join(result, sources,
                                    symbols=symbols,
                                    names=True,
                                    drop_meta=True,
                                    leading=leading,
                                    output_type_index=output_type_index,
                                    use_rename_ep=use_rename_ep)

    if is_bound_multi_symbol:
        if not is_source_symbols:
            # this isn't supported for symbols defined as otp.Source
            new_columns = {
                f"{sym}.{col}": dtype for col, dtype in columns.items() for sym in symbols
            }
            result.schema.update(**new_columns)

        result = result.drop(columns=list(columns.keys()))

    if is_source_symbols:
        result = result.rename({r'__SRC_0__\.(.*)': r'\1'}, use_regex=True)

    if how == "outer":
        # adding table to convert types in schema, e.g. float to int
        result._add_table(strict=False)

    return result


def _fill_source_fields_order_param(source_fields_order, params, sources):
    if source_fields_order is None:
        return sources
    if not isinstance(source_fields_order, Sequence):
        raise ValueError(f"Wrong type for parameter 'source_fields_order': {type(source_fields_order)}")
    if len(source_fields_order) != len(sources):
        raise ValueError("Wrong number of sources in parameter 'source_fields_order':"
                         f" {len(source_fields_order)} (need {len(sources)})")
    if isinstance(source_fields_order[0], int):
        indexes = source_fields_order
        ordered_sources = [sources[i] for i in indexes]
    else:
        indexes = [__find_by_id(sources, src) for src in source_fields_order]
        ordered_sources = source_fields_order
    params['source_order'] = ','.join(f'__SRC_{i}__' for i in indexes)
    return ordered_sources


@singledispatch
def _fill_leading_sources_param(leading, params, sources):
    from onetick.py.core.source import _Source

    if isinstance(leading, _Source):  # TODO: PY-104 Get rid of circular dependencies in code to avoid local import
        result = f"__SRC_{__find_by_id(sources, leading)}__"
        params["leading_sources"] = result
        result = [result]
    elif leading == "all":  # all sources are leading which is specified by empty string
        params["leading_sources"] = ""
        result = []
    else:
        raise ValueError(
            "wrong leading param was specified, please use any of int, 'all' literal, list of int, list of _Source"
        )
    return result


@_fill_leading_sources_param.register(int)
def _(leading, params, sources):
    if leading < 0:
        leading = len(sources) + leading
    if 0 <= leading < len(sources):
        result = f"__SRC_{leading}__"
        params["leading_sources"] = result
        return [result]
    else:
        raise ValueError(
            f"leading source index should be in range(-len(source), len(source)), but {leading} was specified."
        )


@_fill_leading_sources_param.register(list)  # type: ignore  # _ already defined above
@_fill_leading_sources_param.register(tuple)
def _(leading, params, sources):
    if len(leading) > len(sources):
        raise ValueError("Number of leading sources can't be bigger number of sources")
    if isinstance(leading[0], int):
        result = leading
    else:
        result = [__find_by_id(sources, lead) for lead in leading]
    indexes = ",".join(f"__SRC_{i}__" for i in result)
    params["leading_sources"] = indexes
    return result


def __find_by_id(collection, item):
    for index, s in enumerate(collection):
        if s is item:
            return index
    raise ValueError("The source should be in join sources list")


def _check_schema_for_join_by_time(join_str_keys, sources):
    # check that there aren't matching columns
    columns_count = Counter()
    for src in sources:
        columns_count.update(src.columns(skip_meta_fields=True).keys())
    for join_key in join_str_keys:
        del columns_count[join_key]
    matched = [k for k, value in columns_count.items() if value > 1]
    if "OMDSEQ" in matched:
        # OMDSEQ behaves like the TIMESTAMP field
        matched.remove("OMDSEQ")
    if len(matched):
        raise ValueError(f"There are matched columns between sources: {','.join(matched)}")


def apply_query(query,
                in_sources=None,
                output_pins=None,
                shared_state_variables_list=None,
                output_type_index=None,
                **params):
    from onetick.py.sources import query as otp_query

    output_type = output_type_by_index(in_sources, output_type_index)
    output_pins = output_pins if output_pins else []
    in_sources = in_sources if in_sources else {}
    shared_state_variables_list = shared_state_variables_list if shared_state_variables_list else []
    if isinstance(query, str):
        # it seems that path is passed
        query = otp_query(query, **params)

    elif isinstance(query, otp_query) and params:
        query.update_params(**params)

    columns = {}

    for src in in_sources.values():
        columns.update(src.columns(skip_meta_fields=True))

    str_params = query.str_params

    shared_state_variables = ",".join(shared_state_variables_list)

    inputs_need_unbound_symbols = {in_pin: src._is_unbound_required() for in_pin, src in in_sources.items()}
    if query.graph_info is not None and query.graph_info.has_unbound_if_pinned(inputs_need_unbound_symbols):
        symbol = adaptive
    else:
        symbol = None

    nested_src = output_type(
        node=otq.NestedOtq(query.path, str_params, shared_state_variables=shared_state_variables),
        _has_output=len(output_pins) > 0,
        _symbols=symbol,
        schema=columns,
    )

    eps = defaultdict()

    for in_pin, src in in_sources.items():
        nested_src.source(src.node().copy_graph(eps), in_pin)
        nested_src.node().add_rules(src.node().copy_rules())
        nested_src._set_sources_dates(src)
        nested_src._merge_tmp_otq(src)

    if len(output_pins) == 0:
        return nested_src

    if len(output_pins) > 1:
        result = []

        for out_pin in output_pins:
            res_src = nested_src.copy()
            res_src.node().out_pin(out_pin)
            # NOTE: need to comment out this node
            res_src.sink(otq.Passthrough())

            # apply config customization
            query.config._apply(out_pin, res_src)

            result.append(res_src)

        return tuple(result)
    else:
        # TODO: move setting out_pin on the creating step of nested_src
        # It seems as not working now, because seems .copy() of _Source doesnt
        # copy out_pin reference, need to check
        nested_src.node().out_pin(output_pins[0])

        # apply config customization
        query.config._apply(output_pins[0], nested_src)

        return nested_src


def apply(query, *args, **kwargs):
    return apply_query(query.path, *args, **kwargs, **query.params)


def cut(column: 'Column', bins: Union[int, List[float]], labels: Optional[List[str]] = None):
    """
    Bin values into discrete intervals (mimics :pandas:`pandas.cut`).

    Parameters
    ----------
    column: :py:class:`~onetick.py.Column`
        Column with numeric data used to build bins.
    bins: int or List[float]

        When List[float] - defines the bin edges.

        When int - Defines the number of equal-width bins in the range of x.
    labels: List[str]
        Labels used to name resulting bins.
        If not set, bins are numeric intervals like (5.0000000000, 7.5000000000].

    Returns
    -------
    object that can be set to :py:class:`~onetick.py.Column` via :py:meth:`~onetick.py.Source.__setitem__`

    Examples
    --------
    >>> # OTdirective: snippet-name: Source.functions.cut;
    >>> data = otp.Ticks({"X": [9, 8, 5, 6, 7, 0, ]})
    >>> data['bin'] = otp.cut(data['X'], bins=3, labels=['a', 'b', 'c'])
    >>> otp.run(data)[['X', 'bin']]
       X bin
    0  9   c
    1  8   c
    2  5   b
    3  6   b
    4  7   c
    5  0   a

    """
    src = column.obj_ref
    return _CutBuilder(src, column, bins, labels=labels)


def qcut(column: 'Column', q: Union[int, List[float]], labels: Optional[List[str]] = None):
    """
    Quantile-based discretization function (mimics :pandas:`pandas.qcut`).

    Parameters
    ----------
    column: :py:class:`~onetick.py.Column`
        Column with numeric data used to build bins.
    q: int or List[float]

        When List[float] - array of quantiles, e.g. [0, .25, .5, .75, 1.] for quartiles.

        When int - Number of quantiles. 10 for deciles, 4 for quartiles, etc.
    labels: List[str]
        Labels used to name resulting bins.
        If not set, bins are numeric intervals like (5.0000000000, 7.5000000000].

    Returns
    -------
    object that can be set to :py:class:`~onetick.py.Column` via :py:meth:`~onetick.py.Source.__setitem__`

    Examples
    --------
    >>> # OTdirective: snippet-name: Source.functions.qcut;
    >>> data = otp.Ticks({"X": [10, 3, 5, 6, 7, 1]})
    >>> data['bin'] = otp.qcut(data['X'], q=3, labels=['a', 'b', 'c'])
    >>> otp.run(data)[['X', 'bin']]
        X bin
    0  10   c
    1   3   a
    2   5   b
    3   6   b
    4   7   c
    5   1   a
    """
    # TODO when q is a List[float] like [0, .25, .5, .75, 1.]
    src = column.obj_ref
    return _QCutBuilder(src, column, q, labels=labels)


def coalesce(sources, max_source_delay: float = 0.0, output_type_index: Optional[int] = None):
    """
    Used to fill the gaps in one time series with the ticks from one or several other time series.

    This event processor considers ticks that arrive from several sources at the same time as being the same,
    allowing for possible delay across the sources when determining whether the ticks are the same.
    When the same tick arrives from several sources, it is only propagated from the source
    that has the highest priority among those sources.
    Input ticks do not necessarily have the same structure - they can have different fields.

    In order to distinguish time series the event processor adds the SYMBOL_NAME field.
    Also SOURCE field is added to each tick which lacks it to identify the source from which the tick is coming.
    Hence, one must avoid adding SOURCE field in event processors positioned after COALSECE.

    Parameters
    ----------
    sources: list of :class:`Source`
        List of the sources to coalesce. Also, this list is treated as priority order.
        First member of the list has the highest priority when determining whether ticks are the same.
    max_source_delay: float
        The maximum time in seconds by which a tick from one input time series
        can arrive later than the same tick from another time series.
    output_type_index: int
        Specifies index of source in ``sources`` from which type and properties of output will be taken.
        Useful when merging sources that inherited from :class:`Source`.
        By default, output object type will be :class:`Source`.

    Returns
    -------
    :class:`Source`
        A time series of ticks.

    See also
    --------
    **COALESCE** OneTick event processor

    Examples
    --------
    If ticks from different sources have the same time,
    only the tick from source with the highest priority will be propagated.

    >>> data1 = otp.Ticks(A=[1, 2])
    >>> data2 = otp.Ticks(A=[3, 4])
    >>> data = otp.coalesce([data2, data1])
    >>> otp.run(data)[['Time', 'A']]
                         Time A
    0 2003-12-01 00:00:00.000 3
    1 2003-12-01 00:00:00.001 4

    We can use ``max_source_delay`` parameter to expand time interval in which
    ticks are considered to have the "same time".

    >>> data1 = otp.Ticks({
    ...     'A': [1, 2, 3],
    ...     'offset': [0, 3000, 6000],
    ... })
    >>> data2 = otp.Ticks({
    ...     'A': [4, 5, 6],
    ...     # 4 is delayed by less than one second from 1
    ...     # 5 is delayed by one second from 2
    ...     # 6 is delayed by more than one second from 3
    ...     'offset': [999, 4000, 7001],
    ... })
    >>> data = otp.coalesce([data2, data1], max_source_delay=1)
    >>> otp.run(data)[['Time', 'A']]
                         Time  A
    0 2003-12-01 00:00:00.999  4
    1 2003-12-01 00:00:04.000  5
    2 2003-12-01 00:00:06.000  3
    3 2003-12-01 00:00:07.001  6
    """
    if not sources:
        raise ValueError("Coalesce should have one or more inputs")

    output_type = output_type_by_index(sources, output_type_index)

    # change node names for sources, COALESCE ep needs them
    new_node_names = [
        f'__COALESCE_SRC_{i}__' for i, source in enumerate(sources, start=1)
    ]

    node = otq.Coalesce(
        priority_order=','.join(new_node_names),
        max_source_delay=max_source_delay,
    )

    columns = {
        # these fields will be added by COALESCE ep
        'SYMBOL_NAME': str,
        'TICK_TYPE': str,
    }
    for source in sources:
        for name in ['SYMBOL_NAME', 'TICK_TYPE']:
            if name in source.schema:
                raise ValueError(f"Field with name '{name}' is already present in the source. "
                                 'Please, rename or delete that field prior to invoking coalesce().')
        shared_columns = set(source.schema).intersection(columns)
        for name in shared_columns:
            type_1, type_2 = source.schema[name], columns[name]
            if type_1 != type_2:
                raise ValueError(f"Conflicting types for field '{name}' in different sources: {type_1}, {type_2}")
        columns.update(source.schema)

    # TODO: do we need field SOURCE (especially when node names are auto-generated)?
    # this field will be added by COALESCE if it's not presented in sources
    columns.setdefault('SOURCE', str)

    result = output_type(node, schema=columns)

    __copy_sources_on_merge_or_join(result, sources, names=new_node_names, output_type_index=output_type_index)
    return result


def corp_actions(source,
                 adjustment_date: Union[ott.date, ott.datetime, dt.date, dt.datetime, int, str, None] = None,
                 adjustment_date_tz: Union[str, Type[default]] = default,
                 fields=None,
                 adjust_rule="PRICE",
                 apply_split: bool = True,
                 apply_spinoff: bool = False,
                 apply_rights: Optional[bool] = None,
                 apply_cash_dividend: bool = False,
                 apply_stock_dividend: bool = False,
                 apply_security_splice: bool = False,
                 apply_others: str = "",
                 apply_all: bool = False):
    """
    Adjusts values using corporate actions information loaded into OneTick
    from the reference data file. To use it, location of reference database must
    be specified via OneTick configuration.

    Parameters
    ----------
    source: :py:class:`onetick.py.Source`
        Source object adjusted by corporate actions information.
    adjustment_date : :py:class:`onetick.py.date`, :py:class:`onetick.py.datetime`, int, str, None, optional
        The date as of which the values are adjusted.
        All corporate actions of the types specified in the parameters
        that lie between the tick timestamp and the adjustment date will be applied to each tick.

        This parameter can be either date or datetime .
        `int` and `str` format can be *YYYYMMDD* or *YYYYMMDDhhmmss*.
        When parameter is a date, the time is assumed to be 17:00:00 GMT
        and parameter ``adjustment_date_tz`` is ignored.

        If it is not set, the values are adjusted as of the end date in the query.

        Notice that the ``adjustment date`` is not affected neither by *_SYMBOL_PARAM._PARAM_END_TIME_NANOS*
        nor by the *apply_times_daily* setting in :py:func:`onetick.py.run`.

    adjustment_date_tz : str, optional
        Timezone for ``adjustment date``.

        By default global :py:attr:`tz<onetick.py.configuration.Config.tz>` value is used.
        Local timezone can't be used so in this case parameter is set to GMT.
        When ``adjustment_date`` is in YYYYMMDD format, this parameter is set to GMT.
    fields : str, optional
        A comma-separated list of fields to be adjusted. If this parameter is not set,
        some default adjustments will take place if appropriately named fields exist in the tick:

        - If the ``adjust_rule`` parameter is set to PRICE, and the PRICE field is present,
          it will get adjusted. If the fields ASK_PRICE or BID_PRICE are present, they will get adjusted.
          If fields ASK_VALUE or BID_VALUE are present, they will get adjusted

        - If the ``adjust_rule`` parameter is set to SIZE, and the SIZE field is present,
          it will get adjusted. If the fields ASK_SIZE or BID_SIZE are present, they will get adjusted.
          If fields ASK_VALUE or BID_VALUE are present, they will get adjusted.

    adjust_rule : str, optional
        When set to PRICE, adjustments are applied under the assumption that fields to be adjusted contain prices
        (adjustment direction is determined appropriately).

        When set to SIZE, adjustments are applied under the assumption that fields contain sizes
        (adjustment direction is opposite to that when the parameter's value is PRICE).

        By default the value is PRICE.
    apply_split : bool, optional
        If True, adjustments for splits are applied.
    apply_spinoff : bool, optional
        If True, adjustments for spin-offs are applied.
    apply_cash_dividend : bool, optional
        If True, adjustments for cash dividends are applied.
    apply_stock_dividend : bool, optional
        If True, adjustments for stock dividends are applied.
    apply_security_splice : bool, optional
        If True, adjustments for security splices are applied.
    apply_others : str, optional
        A comma-separated list of names of custom adjustment types to apply.
    apply_all : bool, optional
        If True, applies all types of adjustments, both built-in and custom.

    Returns
    -------
    :py:class:`onetick.py.Source`
        A new source object with applied adjustments.

    See also
    --------
    **CORP_ACTIONS** OneTick event processor

    Examples
    --------
    >>> src = otp.DataSource('US_COMP',
    ...                      tick_type='TRD',
    ...                      start=otp.dt(2022, 5, 20, 9, 30),
    ...                      end=otp.dt(2022, 5, 26, 16))
    >>> df = otp.run(src, symbols='MKD', symbol_date=otp.date(2022, 5, 22))
    >>> df["PRICE"][0]
    0.0911
    >>> src = otp.corp_actions(src,
    ...                        adjustment_date=otp.date(2022, 5, 22),
    ...                        fields="PRICE")
    >>> df = otp.run(src, symbols='MKD', symbol_date=otp.date(2022, 5, 22))
    >>> df["PRICE"][0]
    1.36649931675
    """
    source = source.copy()

    if isinstance(adjustment_date, int):
        adjustment_date = str(adjustment_date)

    is_datetime_param = None

    if adjustment_date is None or isinstance(adjustment_date, str) and adjustment_date == '':
        # default value for otq.CorpActions
        adjustment_date = ''
    elif isinstance(adjustment_date, (ott.datetime, ott.date, dt.datetime, dt.date, str)):
        if isinstance(adjustment_date, str):
            try:
                dt.datetime.strptime(adjustment_date, '%Y%m%d%H%M%S')
                if len(adjustment_date) != 14:
                    # strptime doesn't require leading zeroes for %m%d%H%M%S specificators, but we do
                    raise ValueError()
                is_datetime_param = True
            except ValueError:
                try:
                    dt.datetime.strptime(adjustment_date, '%Y%m%d')
                    if len(adjustment_date) != 8:
                        # strptime doesn't require leading zeroes for %m%d specificators, but we do
                        raise ValueError()
                    is_datetime_param = False
                except ValueError:
                    raise ValueError("Parameter 'adjustment_date' must be in YYYYMMDDhhmmss or YYYYMMDD formats.")
            adjustment_date = int(adjustment_date)
        elif type(adjustment_date) in (ott.datetime, dt.datetime):
            is_datetime_param = True
            adjustment_date = int(adjustment_date.strftime('%Y%m%d%H%M%S'))
        elif type(adjustment_date) in (ott.date, dt.date):
            is_datetime_param = False
            adjustment_date = int(adjustment_date.strftime('%Y%m%d'))
    else:
        raise ValueError("Parameter 'adjustment_date' must be in YYYYMMDDhhmmss or YYYYMMDD formats.")

    adjustment_date_tz_is_default = adjustment_date_tz is default
    if adjustment_date_tz_is_default:
        adjustment_date_tz = config.tz

    if not adjustment_date_tz:
        warnings.warn("Local timezone can't be used in parameter 'adjustment_date_tz', setting to 'GMT'.")
        adjustment_date_tz = 'GMT'

    if is_datetime_param is not None and not is_datetime_param and adjustment_date_tz != 'GMT':
        adjustment_date_tz = 'GMT'
        if not adjustment_date_tz_is_default:
            warnings.warn("`adjustment_date_tz` was changed to 'GMT' since "
                          "it is the only valid value when `adjustment_date` is in YYYYMMDD format.")

    kwargs = {}
    if apply_rights is not None and is_apply_rights_supported(throw_warning=True):
        kwargs['apply_rights'] = apply_rights

    source.sink(otq.CorpActions(
        adjustment_date=adjustment_date,
        adjustment_date_tz=adjustment_date_tz,
        fields=fields or '',
        adjust_rule=adjust_rule,
        apply_split=apply_split,
        apply_spinoff=apply_spinoff,
        apply_cash_dividend=apply_cash_dividend,
        apply_stock_dividend=apply_stock_dividend,
        apply_security_splice=apply_security_splice,
        apply_others=apply_others,
        apply_all=apply_all,
        **kwargs,
    ))
    return source


def save_sources_to_single_file(sources,
                                file_path=None,
                                file_suffix='',
                                start=None,
                                end=None,
                                start_time_expression=None,
                                end_time_expression=None,
                                timezone=None,
                                running_query_flag=None):
    """
    Save onetick.py.Source objects to the single file.

    Parameters
    ----------
    sources: dict or list
        dict of names -> sources or list of sources to merge into single file.
        If it's the list then names will be autogenerated.
        Source can be :class:`otp.Source` object or dictionary with these allowed parameters:
        {
            'source': otp.Source,
            'start': datetime(2022, 1, 1),         # optional
            'end': datetime(2022, 1, 2),           # optional
            'symbols': otp.Source or otp.Symbols,  # optional
        }
    file_path: str, optional
        Path to the file where all sources will be saved.
        If not set, sources will be saved to temporary file and its name will be returned.
    file_suffix: str
        Only used if ``file_path`` is not set.
        This suffix will be added to the name of a generated query file.
    start: datetime, optional
        start time for the resulting query file
    end: datetime, optional
        end time for the resulting query file
    start_time_expression: str, optional
        start time expression for the resulting query file
    end_time_expression: str, optional
        end time expression for the resulting query file
    timezone: str, optional
        timezone for the resulting query file
    running_query_flag: bool, optional
        running query flag for the resulting query file

    Returns
    -------
        If `sources` is list then returns list of full query paths (path_to_file::query_name)
        with autogenerated names corresponding to each source from `sources`.
        If `sources` is dict then the path to the query file is returned.
    """
    if isinstance(sources, dict):
        names = sources.keys()
        sources = sources.values()
        query_names = None
    else:
        names = repeat(None)
        query_names = []
    tmp_otq = TmpOtq()
    for name, source in zip(names, sources):
        query_start = query_end = query_symbols = query_symbol_date = None
        if isinstance(source, dict):
            query_start = source.get('start')
            query_end = source.get('end')
            query_symbols = source.get('symbols')
            query_symbol_date = source.get('symbol_date')
            source = source['source']
        query_name = source._store_in_tmp_otq(tmp_otq,
                                              name=name,
                                              start=query_start,
                                              end=query_end,
                                              symbols=query_symbols,
                                              symbol_date=query_symbol_date)
        if query_names is not None:
            query_names.append(query_name)
    file_path = tmp_otq.save_to_file(
        file_path=file_path,
        file_suffix=file_suffix,
        start=start,
        end=end,
        start_time_expression=start_time_expression,
        end_time_expression=end_time_expression,
        timezone=timezone,
        running_query_flag=running_query_flag,
    )
    if query_names is not None:
        return [f'{file_path}::{query_name}' for query_name in query_names]
    return file_path


class _FormatType(Enum):
    POSITIONAL = 1
    OMITTED_POSITIONAL = 2
    KEY_WORD = 3


def format(format_line: str, *args, **kwargs) -> Operation:
    """
    Perform a string formatting operation.
    Currently, there are only 2 types of formatting available:

    1. Float precision - ``{:.xf}``, where ``x`` is number, e.g. ``{:.5f}``

    2. Time formatting - the same as in ``Source.dt.strftime``

    See examples for more information.

    Parameters
    ----------
    format_line: str
        String which contains literal text or replacement fields delimited by braces {}.
        Currently content of the braces is not supported.
    args
        Values to paste into the line.
    kwargs
        Key-word values to paste into the line.

    Returns
    -------
    :py:class:`~onetick.py.Operation` with type equal to :py:class:`~onetick.py.types.varstring`

    Examples
    --------
    It allows to format :py:class:`~onetick.py.Operation`. For example, :py:class:`~onetick.py.Column`:

    >>> data = otp.Ticks(A=[1, 2], B=['abc', 'def'])
    >>> data['C'] = otp.format('A field value is `{}` and B field value is `{}`', data['A'], data['B'])
    >>> otp.run(data)
                         Time  A    B                                                C
    0 2003-12-01 00:00:00.000  1  abc  A field value is `1` and B field value is `abc`
    1 2003-12-01 00:00:00.001  2  def  A field value is `2` and B field value is `def`

    Formatting can use positional arguments:

    >>> data = otp.Ticks(A=[1, 2], B=['abc', 'def'])
    >>> data['C'] = otp.format('A is `{0}`, B is `{1}`. Also, A is `{0}`', data['A'], data['B'])
    >>> otp.run(data)
                         Time  A    B                                     C
    0 2003-12-01 00:00:00.000  1  abc  A is `1`, B is `abc`. Also, A is `1`
    1 2003-12-01 00:00:00.001  2  def  A is `2`, B is `def`. Also, A is `2`

    Formatting can be used with key-word arguments:

    >>> data = otp.Ticks(A=[1, 2], B=['abc', 'def'])
    >>> data['C'] = otp.format('A is `{a}`, B is `{b}`. Also, A is `{a}`', a=data['A'], b=data['B'])
    >>> otp.run(data)
                         Time  A    B                                     C
    0 2003-12-01 00:00:00.000  1  abc  A is `1`, B is `abc`. Also, A is `1`
    1 2003-12-01 00:00:00.001  2  def  A is `2`, B is `def`. Also, A is `2`

    Float numbers can be formatted:

    >>> data = otp.Ticks(A=[12.3456, 67.8971])
    >>> data['B'] = otp.format('A is about {:.2f}', data['A'])
    >>> otp.run(data)
                         Time        A                 B
    0 2003-12-01 00:00:00.000  12.3456  A is about 12.35
    1 2003-12-01 00:00:00.001  67.8971  A is about 67.90

    Time can be formatted:

    >>> data = otp.Tick(A=otp.datetime(2020, 4, 5, 17, 56, 3, 789123))
    >>> data['B'] = otp.format('A is {:%Y/%m/%d %H:%M:%S.%J}', data['A'])
    >>> otp.run(data)
            Time                          A                                   B
    0 2003-12-01 2020-04-05 17:56:03.789123  A is 2020/04/05 17:56:03.789123000
    """
    _validate_format_line(format_line)
    format_array = re.split('[{}]', format_line)
    format_type = _get_format_type(format_array)
    res = ott.varstring(format_array[0])
    cur_index = 0
    format_spec_array = format_array[1::2]
    regular_string_array = format_array[2::2]
    for format_spec, regular_string in zip(format_spec_array, regular_string_array):
        format_spec_array = format_spec.split(':', 1)
        format_spec_param = format_spec_array[0]
        format_spec_additional = None if len(format_spec_array) == 1 else format_spec_array[1]
        if format_type == _FormatType.POSITIONAL:
            res = _add_element(res, args[int(format_spec_param)], format_spec_additional)
        elif format_type == _FormatType.OMITTED_POSITIONAL:
            res = _add_element(res, args[cur_index], format_spec_additional)
            cur_index += 1
        else:
            res = _add_element(res, kwargs[format_spec_param], format_spec_additional)
        res += regular_string
    return res


def _add_element(cur_res, element, format_spec_additional=None):
    if isinstance(element, Operation):
        if format_spec_additional is None:
            cur_res += element.apply(str)
        elif issubclass(element.dtype, (float, ott.decimal)) and re.fullmatch(r'\.\d+f', format_spec_additional):
            # float has strange behavior when precision=0
            decimal_elem = element.apply(ott.decimal)
            precision_str = re.findall(r'\d+', format_spec_additional)[0]
            try:
                precision = int(precision_str)
            except ValueError as exc:
                raise ValueError('Incorrect value for `precision` for formatting decimal number') from exc

            cur_res += decimal_elem.decimal.str(precision)
        elif issubclass(element.dtype, (ott.nsectime, ott.msectime)):
            cur_res += element.dt.strftime(format_spec_additional)
        else:
            raise ValueError(f'Unsupported formatting `{format_spec_additional}` for field type {element.dtype}')
    else:
        if format_spec_additional is None:
            cur_res += str(element)
        elif isinstance(element, (float, ott.decimal)):
            formatting = f'{{:{format_spec_additional}}}'
            cur_res += formatting.format(element)
        else:
            raise ValueError(f'Unsupported formatting `{format_spec_additional}` for literal {type(element)}')
    return cur_res


def _validate_format_line(format_line: str):
    open_brackets_num = 0
    close_brackets_num = 0
    for symbol in format_line:
        if symbol == '{':
            open_brackets_num += 1
        if symbol == '}':
            close_brackets_num += 1
        if open_brackets_num > close_brackets_num + 1:
            raise ValueError("'{' appeared before previous '{' was closed")
        if open_brackets_num < close_brackets_num:
            raise ValueError("Single '}' encountered in format string")
    if open_brackets_num != close_brackets_num:
        raise ValueError("Single '{' encountered in format string")


def _get_format_type(format_array: List[str]) -> _FormatType:
    if len(format_array) < 2:
        return _FormatType.OMITTED_POSITIONAL
    format_spec_array = format_array[1::2]
    uses_positional = False
    uses_omitted_positional = False
    uses_key_word = False
    for format_spec in format_spec_array:
        format_spec_param = format_spec.split(':')[0]
        if not format_spec_param:
            uses_omitted_positional = True
        elif format_spec_param[0].isdigit():
            if not format_spec_param.isnumeric():
                raise ValueError(f'Incorrect positional argument: `{format_spec_param}`')
            uses_positional = True
        elif format_spec_param[0].isalpha():
            # only word characters are supported
            if not re.fullmatch(r'\w+', format_spec_param):
                raise ValueError(f'Incorrect key word argument: `{format_spec_param}`')
            uses_key_word = True
        else:
            raise ValueError(f'Unrecognised format specification: `{format_spec_param}`')
    if uses_positional and not (uses_omitted_positional or uses_key_word):
        return _FormatType.POSITIONAL
    if uses_omitted_positional and not (uses_positional or uses_key_word):
        return _FormatType.OMITTED_POSITIONAL
    if uses_key_word and not (uses_positional or uses_omitted_positional):
        return _FormatType.KEY_WORD
    raise ValueError("Format string has mixed type of referring to arguments which is not allowed")


def join_with_aggregated_window(
    agg_src, pass_src, aggregation,
    boundary_aggr_tick: str = 'next',
    pass_src_delay_msec: int = 0,
    bucket_interval: int = 0,
    bucket_units: Literal['seconds', 'ticks', 'days', 'months', 'flexible'] = 'seconds',
    output_type_index=None,
):
    """
    Computes one or more aggregations on ``agg_src`` time series
    and joins the result with each incoming tick from ``pass_src`` time series.

    Parameters
    ----------
    agg_src: :py:class:`onetick.py.Source`
        Input time series to which aggregation will be applied.
    pass_src: :py:class:`onetick.py.Source`
        Input time series that will be joined with the aggregation result.
    aggregation: dict
        Dictionary with aggregation output field names and aggregation objects,
        similar to the one passed to :py:meth:`onetick.py.Source.agg` method.
    pass_src_delay_msec: int
        Specifies by how much any incoming tick from the ``pass_src`` is delayed.

        The effective timestamp of a tick from the ``pass_src`` with timestamp ``T`` is ``T - pass_src_delay_msec``.
        This parameter may be negative, in which case ticks from ``pass_src`` will be joined
        with the aggregation result of a later timestamp.
    boundary_aggr_tick: str
        Controls the logic of joining ticks with the same timestamp.

        If set to **next**, ticks from ``agg_src`` with the same timestamp (+ ``pass_src_delay_msec``)
        as the latest ticks from ``pass_src`` will not be included in that tick's joined aggregation.
    bucket_interval: int
        Determines the length of each bucket (units depends on ``bucket_units``).

        When this parameter is set to 0 (by default),
        the computation of the aggregation is performed for all ticks starting from the query's start time
        and until ``pass_src`` effective tick timestamp ``T - pass_src_delay_timestamp``,
        regardless of the value of ``bucket_units``.
    bucket_units: 'seconds', 'ticks', 'days', 'months'
        Set bucket interval units.
    output_type_index: int
        Specifies index of source between ``agg_src`` and ``pass_src``
        from which type and properties of output object will be taken.
        Useful when merging sources that inherited from :class:`Source`.
        By default, output object type will be :class:`Source`.

    Returns
    -------
    :py:class:`onetick.py.Source`

    See also
    --------
    **JOIN_WITH_AGGREGATED_WINDOW** OneTick event processor

    Examples
    --------

    >>> agg_src = otp.Ticks(A=[0, 1, 2, 3, 4, 5, 6])
    >>> pass_src = otp.Ticks(B=[1, 3, 5], offset=[1, 3, 5])
    >>> otp.run(agg_src)
                         Time  A
    0 2003-12-01 00:00:00.000  0
    1 2003-12-01 00:00:00.001  1
    2 2003-12-01 00:00:00.002  2
    3 2003-12-01 00:00:00.003  3
    4 2003-12-01 00:00:00.004  4
    5 2003-12-01 00:00:00.005  5
    6 2003-12-01 00:00:00.006  6
    >>> otp.run(pass_src)
                         Time  B
    0 2003-12-01 00:00:00.001  1
    1 2003-12-01 00:00:00.003  3
    2 2003-12-01 00:00:00.005  5

    By default the aggregation is applied to the ticks from ``agg_src`` in the bucket
    from query start time until (but not including) the *effective* timestamp of the tick from ``pass_src``:

    .. testcode::
       :skipif: not is_supported_join_with_aggregated_window()

       agg_src = otp.Ticks(A=[0, 1, 2, 3, 4, 5, 6])
       pass_src = otp.Ticks(B=[1, 3, 5], offset=[1, 3, 5])
       data = otp.join_with_aggregated_window(
           agg_src, pass_src, {
               'SUM': otp.agg.sum('A'),
               'COUNT': otp.agg.count(),
           }
       )
       df = otp.run(data)
       print(df)

    .. testoutput::

                            Time  SUM  COUNT  B
       0 2003-12-01 00:00:00.001    0      1  1
       1 2003-12-01 00:00:00.003    3      3  3
       2 2003-12-01 00:00:00.005   10      5  5

    If you want ticks from ``agg_src`` with timestamp equal to *effective* timestamp of tick from ``pass_src``
    to be included in bucket, you can set ``boundary_aggr_tick`` to ``previous``:

    .. testcode::
       :skipif: not is_supported_join_with_aggregated_window()

       agg_src = otp.Ticks(A=[0, 1, 2, 3, 4, 5, 6])
       pass_src = otp.Ticks(B=[1, 3, 5], offset=[1, 3, 5])
       data = otp.join_with_aggregated_window(
           agg_src, pass_src, {
               'SUM': otp.agg.sum('A'),
               'COUNT': otp.agg.count(),
           },
           boundary_aggr_tick='previous',
       )
       df = otp.run(data)
       print(df)

    .. testoutput::

                            Time  SUM  COUNT  B
       0 2003-12-01 00:00:00.001    1      2  1
       1 2003-12-01 00:00:00.003    6      4  3
       2 2003-12-01 00:00:00.005   15      6  5

    Set parameters ``bucket_interval`` and ``bucket_units`` to control the size of the aggregation bucket.
    For example, to aggregate buckets of two ticks:

    .. testcode::
       :skipif: not is_supported_join_with_aggregated_window()

       agg_src = otp.Ticks(A=[0, 1, 2, 3, 4, 5, 6])
       pass_src = otp.Ticks(B=[1, 3, 5], offset=[1, 3, 5])
       data = otp.join_with_aggregated_window(
           agg_src, pass_src, {
               'SUM': otp.agg.sum('A'),
               'COUNT': otp.agg.count(),
           },
           boundary_aggr_tick='previous',
           bucket_interval=2,
           bucket_units='ticks',
       )
       df = otp.run(data)
       print(df)

    .. testoutput::

                            Time  SUM  COUNT  B
       0 2003-12-01 00:00:00.001    1      2  1
       1 2003-12-01 00:00:00.003    5      2  3
       2 2003-12-01 00:00:00.005    9      2  5

    By default the *effective* timestamp of the tick from ``pass_src`` is the same as original.
    It can be changed with parameter ``pass_src_delay_msec``.
    The *effective* timestamp of the tick is calculated with ``T - pass_src_delay_msec``,
    and parameter ``pass_src_delay_msec`` can be negative too.
    This allows to shift bucket end boundary like this:

    .. testcode::
       :skipif: not is_supported_join_with_aggregated_window()

       agg_src = otp.Ticks(A=[0, 1, 2, 3, 4, 5, 6])
       pass_src = otp.Ticks(B=[1, 3, 5], offset=[1, 3, 5])
       data = otp.join_with_aggregated_window(
           agg_src, pass_src, {
               'SUM': otp.agg.sum('A'),
               'COUNT': otp.agg.count(),
           },
           boundary_aggr_tick='previous',
           pass_src_delay_msec=-1,
       )
       df = otp.run(data)
       print(df)

    .. testoutput::

                            Time  SUM  COUNT  B
       0 2003-12-01 00:00:00.001    3      3  1
       1 2003-12-01 00:00:00.003   10      5  3
       2 2003-12-01 00:00:00.005   21      7  5

    Use parameter ``output_type_index`` to specify which input class to use to create output object.
    It may be useful in case some custom user class was used as input:

    .. testcode::
       :skipif: not is_supported_join_with_aggregated_window()

       class CustomTick(otp.Tick):
           def custom_method(self):
               return 'custom_result'
       data1 = otp.Tick(A=1)
       data2 = CustomTick(B=2)
       data = otp.join_with_aggregated_window(
           data1, data2, {'A': otp.agg.count()},
           boundary_aggr_tick='previous',
           output_type_index=1,
       )
       print(type(data))
       print(repr(data.custom_method()))
       print(otp.run(data))

    .. testoutput::

       <class 'onetick.py.functions.CustomTick'>
       'custom_result'
               Time  A  B
       0 2003-12-01  1  2

    Use-case: check the volume in the 60 seconds following this trade (not including this trade):

    >>> data = otp.DataSource('US_COMP', tick_type='TRD', symbols='MSFT', date=otp.dt(2022, 3, 3))
    >>> otp.run(data)
                         Time  PRICE  SIZE
    0 2022-03-03 00:00:00.000    1.0   100
    1 2022-03-03 00:00:00.001    1.1   101
    2 2022-03-03 00:00:00.002    1.2   102
    3 2022-03-03 00:01:00.000    2.0   200
    4 2022-03-03 00:01:00.001    2.1   201
    5 2022-03-03 00:01:00.002    2.2   202

    .. testcode::
       :skipif: not is_supported_join_with_aggregated_window()

       data = otp.DataSource('US_COMP', tick_type='TRD', symbols='MSFT', date=otp.dt(2022, 3, 3))
       data = otp.join_with_aggregated_window(
           data, data, {'VOLUME': otp.agg.sum('SIZE')},
           boundary_aggr_tick='next',
           pass_src_delay_msec=-60000,
           bucket_interval=60, bucket_units='seconds',
       )
       df = otp.run(data)
       print(df)

    .. testoutput::

                            Time  VOLUME  PRICE  SIZE
       0 2022-03-03 00:00:00.000     203    1.0   100
       1 2022-03-03 00:00:00.001     302    1.1   101
       2 2022-03-03 00:00:00.002     401    1.2   102
       3 2022-03-03 00:01:00.000     403    2.0   200
       4 2022-03-03 00:01:00.001     202    2.1   201
       5 2022-03-03 00:01:00.002       0    2.2   202
    """
    if not is_supported_join_with_aggregated_window():
        raise RuntimeError('Function join_with_aggregated_window() is not supported on this OneTick build')

    if boundary_aggr_tick not in {'next', 'previous'}:
        raise ValueError(f"Wrong value of 'boundary_aggr_tick' parameter: '{boundary_aggr_tick}'")
    if boundary_aggr_tick == 'next':
        boundary_aggr_tick_behavior = 'NEXT_WINDOW'
        is_supported_next_in_join_with_aggregated_window(
            throw_warning=True,
            feature_name="setting parameter 'boundary_aggr_tick' to 'next' (as this may result in crash)"
        )
    else:
        boundary_aggr_tick_behavior = 'PREV_WINDOW'

    aggregation_str = ','.join([
        str(aggr) + " " + name
        for name, aggr in aggregation.items()
    ])

    params = dict(
        aggregation_source='__AGG_SRC__',
        pass_source='__PASS_SRC__',
        boundary_aggr_tick_behavior=boundary_aggr_tick_behavior,
        append_output_field_name=False,
        aggregation=aggregation_str,
        pass_source_delay_msec=pass_src_delay_msec,
        bucket_interval=bucket_interval,
        bucket_interval_units=bucket_units.upper(),
    )

    output_type = output_type_by_index((agg_src, pass_src), output_type_index)

    agg_src = agg_src.copy()
    pass_src = pass_src.copy()

    agg_src.node_name('__AGG_SRC__')
    pass_src.node_name('__PASS_SRC__')

    columns = {}
    for name, aggr in aggregation.items():
        columns.update(aggr._get_output_schema(agg_src, name=name))
    columns.update(pass_src.schema)
    result = output_type(node=otq.JoinWithAggregatedWindow(**params), schema=columns)

    __copy_sources_on_merge_or_join(result, (agg_src, pass_src),
                                    names=('__AGG_SRC__', '__PASS_SRC__'),
                                    output_type_index=output_type_index)

    # adding table to convert types in schema, e.g. float to int
    result._add_table(strict=False)
    return result
