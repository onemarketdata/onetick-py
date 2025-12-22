import warnings
import datetime
from typing import TYPE_CHECKING, Optional, Set, Type, Union
from onetick.py.backports import Literal

from onetick import py as otp
from onetick.py import configuration
from onetick.py.core.column import _Column, field_name_contains_lowercase
from onetick.py.otq import otq
from onetick.py.utils import adaptive
from onetick.py.compatibility import is_save_snapshot_database_parameter_supported

from .misc import inplace_operation

if TYPE_CHECKING:
    from onetick.py.core.source import Source


@inplace_operation
def write(
    self,
    db: Union[str, 'otp.DB'],
    symbol: Union[str, 'otp.Column', None] = None,
    tick_type: Union[str, 'otp.Column', None] = None,
    date: Union[datetime.date, Type[adaptive], None] = adaptive,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
    append: bool = False,
    keep_symbol_and_tick_type: Union[bool, Type[adaptive]] = adaptive,
    propagate: bool = True,
    out_of_range_tick_action: Literal['exception', 'ignore', 'load'] = 'exception',
    timestamp: Optional['otp.Column'] = None,
    keep_timestamp: bool = True,
    correction_type: Optional['otp.Column'] = None,
    replace_existing_time_series: bool = False,
    allow_concurrent_write: bool = False,
    context: Union[str, Type[adaptive]] = adaptive,
    use_context_of_query: bool = False,
    inplace: bool = False,
    **kwargs,
) -> Optional['Source']:
    """
    Saves data result to OneTick database.

    Note
    ----
    This method does not save anything. It adds instruction in query to save.
    Data will be saved when query will be executed.

    Using ``start``+``end`` parameters instead of single ``date`` have some limitations:

        * ``inplace`` is not supported
        * if ``DAY_BOUNDARY_TZ`` and ``DAY_BOUNDARY_OFFSET`` specified against
            individual locations of database, then day boundary could be calculated incorrectly.
        * ``out_of_range_tick_action`` could be only ``exception`` or ``ignore``

    Parameters
    ----------
    db: str or :py:class:`otp.DB <onetick.py.DB>`
        database name or object.
    symbol: str or Column
        resulting symbol name string or column to get symbol name from.
        If this parameter is not set, then ticks _SYMBOL_NAME pseudo-field is used.
        If it is empty, an attempt is made to retrieve
        the symbol name from the field named SYMBOL_NAME.
    tick_type: str or Column
        resulting tick type string or column to get tick type from.
        If this parameter is not set, the _TICK_TYPE pseudo-field is used.
        If it is empty, an attempt is made to retrieve
        the tick type from the field named TICK_TYPE.
    date: :py:class:`otp.datetime <onetick.py.datetime>` or None
        date where to save data.
        Should be set to `None` if writing to accelerator or memory database.
        By default, it is set to `otp.config.default_date`.
    start_date: :py:class:`otp.datetime <onetick.py.datetime>` or None
        Start date for data to save. It is inclusive.
        Cannot be used with ``date`` parameter.
        Also cannot be used with ``inplace`` set to ``True``.
        Should be set to `None` if writing to accelerator or memory database.
        By default, None.
    end_date: :py:class:`otp.datetime <onetick.py.datetime>` or None
        End date for data to save. It is inclusive.
        Cannot be used with ``date`` parameter.
        Also cannot be used with ``inplace`` set to ``True``.
        Should be set to `None` if writing to accelerator or memory database.
        By default, None.
    append: bool
        If False - data will be rewritten for this ``date``
        or range of dates (from ``start_date`` to ``end_date``),
        otherwise data will be appended: new symbols are added,
        existing symbols can be modified (append new ticks, modify existing ticks).
        This option is not valid for accelerator databases.
    keep_symbol_and_tick_type: bool
        keep fields containing symbol name and tick type when writing ticks
        to the database or propagating them.
        By default, this parameter is adaptive.
        If ``symbol`` or ``tick_type`` are column objects, then it's set to True.
        Otherwise, it's set to False.
    propagate: bool
        Propagate ticks after that event processor or not.
    out_of_range_tick_action: str
        Action to be executed if tick's timestamp's date is not ``date`` or between ``start_date`` or ``end_date``:

            * `exception`: runtime exception will be raised
            * `ignore`: tick will not be written to the database
            * `load`: writes tick to the database anyway.
                Can be used only with ``date``, not with ``start_date``+``end_date``.

        Default: `exception`
    timestamp: Column
        Field that contains the timestamp with which the ticks will be written to the database.
        By default, the TIMESTAMP pseudo-column is used.
    keep_timestamp: bool
        If ``timestamp`` parameter is set and this parameter is set to True,
        then timestamp column is removed.
    correction_type: Column
        The name of the column that contains the correction type.
        This column will be removed.
        If this parameter is not set, no corrections will be submitted.
    replace_existing_time_series: bool
        If ``append`` is set to True, setting this option to True instructs the loader
        to replace existing time series, instead of appending to them.
        Other time series will remain unchanged.
    allow_concurrent_write: bool
        Allows different queries running on the same server to load concurrently into the same database.
    context: str
        The server context used to look up the database.
        By default, `otp.config.context` is used if ``use_context_of_query`` is not set.
    use_context_of_query: bool
        If this parameter is set to True and the ``context`` parameter is not set,
        the context of the query is used instead of the default value of the ``context`` parameter.
    inplace: bool
        A flag controls whether operation should be applied inplace.
        If ``inplace=True``, then it returns nothing.
        Otherwise, method returns a new modified object.
        Cannot be ``True`` if ``start_date`` and ``end_date`` are set.
    kwargs:
        .. deprecated:: 1.21.0

        Use named parameters instead.

    Returns
    -------
    :class:`Source` or None

    See also
    --------
    **WRITE_TO_ONETICK_DB** OneTick event processor

    Examples
    --------
    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> data = data.write('SOME_DB', symbol='S_WRITE', tick_type='T_WRITE')
    >>> otp.run(data)
                         Time  X
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2
    2 2003-12-01 00:00:00.002  3
    >>> data = otp.DataSource('SOME_DB', symbol='S_WRITE', tick_type='T_WRITE')
    >>> otp.run(data)
                         Time  X
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2
    2 2003-12-01 00:00:00.002  3
    """
    if 'append_mode' in kwargs:
        warnings.warn("Parameter 'append_mode' is deprecated, use 'append'", FutureWarning)
        append = kwargs.pop('append_mode')

    if 'timestamp_field' in kwargs:
        warnings.warn("Parameter 'timestamp_field' is deprecated, use 'timestamp'", FutureWarning)
        timestamp = kwargs.pop('timestamp_field')

    if 'keep_timestamp_field' in kwargs:
        warnings.warn("Parameter 'keep_timestamp_field' is deprecated, use 'keep_timestamp'", FutureWarning)
        keep_timestamp = kwargs.pop('keep_timestamp_field')

    if 'start' in kwargs:
        warnings.warn("Parameter 'start' is deprecated, use 'start_date'", FutureWarning)
        start_date = kwargs.pop('start')

    if 'end' in kwargs:
        warnings.warn("Parameter 'end' is deprecated, use 'end_date'", FutureWarning)
        # Parameter 'end' was exclusive. Parameter 'end_date' is inclusive.
        end_date = kwargs.pop('end') - otp.Day(1)

    if kwargs:
        raise TypeError(f'write() got unexpected arguments: {list(kwargs)}')

    kwargs = {}

    # validate field names
    for field_name in self.schema:
        if field_name_contains_lowercase(field_name):
            if otp.config.allow_lowercase_in_saved_fields:
                warnings.warn(
                    f'Field "{field_name}" contains lowercase characters and is being saved'
                    ' to a Onetick database. This field will be converted to uppercase upon saving.'
                )
            else:
                raise ValueError(
                    f'Field "{field_name}" contains lowercase characters and cannot be saved to a Onetick database'
                )

    if date is not adaptive and (start_date or end_date):
        raise ValueError('date cannot be used with start_date+end_date')

    if date is adaptive and (start_date and end_date) and inplace:
        # join_with_query and merge are used for multiple dates, so inplace is not supported
        raise ValueError(
            'cannot run on multiple dates if inplace is True,'
            ' use one value for date instead of start_date+end_date'
        )

    if (start_date and not end_date) or (not start_date and end_date):
        raise ValueError('start_date and end_date should be both specified or both None')

    if date is adaptive:
        date = configuration.config.default_date

    if symbol is not None:
        if isinstance(symbol, _Column):
            kwargs['symbol_name_field'] = str(symbol)
            if keep_symbol_and_tick_type is adaptive:
                keep_symbol_and_tick_type = True
        else:
            kwargs.setdefault('symbol_name_field', '_SYMBOL_NAME_FIELD_')
            self[kwargs['symbol_name_field']] = symbol

    if tick_type is not None:
        if isinstance(tick_type, _Column):
            kwargs['tick_type_field'] = str(tick_type)
            if keep_symbol_and_tick_type is adaptive:
                keep_symbol_and_tick_type = True
        else:
            kwargs.setdefault('tick_type_field', '_TICK_TYPE_FIELD_')
            self[kwargs['tick_type_field']] = tick_type

    if keep_symbol_and_tick_type is adaptive:
        keep_symbol_and_tick_type = False

    if timestamp is not None:
        kwargs['timestamp_field'] = str(timestamp)

    if correction_type is not None:
        kwargs['correction_type_field'] = str(correction_type)

    if context is not adaptive:
        kwargs['context'] = context
    elif not use_context_of_query:
        if otp.config.context is not None:
            kwargs['context'] = otp.config.context

    if out_of_range_tick_action.upper() == 'IGNORE':
        # let's ignore
        pass
    elif out_of_range_tick_action.upper() == 'LOAD':
        if start_date and end_date:
            raise ValueError('LOAD out_of_range_tick_action cannot be used with start_date+end_date, use date instead')
    elif out_of_range_tick_action.upper() == 'EXCEPTION':
        if start_date and end_date:
            end = end_date + otp.Day(1)  # end_date is inclusive

            # WRITE_TO_ONETICK_DB use DAY_BOUNDARY_TZ and DAY_BOUNDARY_OFFSET
            # to check tick timestamp is out of range or not
            # so we mimic it here with THROW event processor
            src = otp.Source(otq.DbShowConfig(str(db), 'DB_TIME_INTERVALS'), schema={
                'DAY_BOUNDARY_TZ': int, 'DAY_BOUNDARY_OFFSET': int, 'START_DATE': int, 'END_DATE': int,
            })

            # Filter not relevant locator time intervals
            src, _ = src[
                (src['START_DATE'].astype(otp.msectime) <= otp.dt(start_date).to_operation()) &
                (src['END_DATE'].astype(otp.msectime) > otp.dt(end).to_operation())
            ]
            src.table(inplace=True, DAY_BOUNDARY_TZ=str, DAY_BOUNDARY_OFFSET=int)
            # DAY_BOUNDARY_OFFSET offset are in seconds
            src['DAY_BOUNDARY_OFFSET'] = src['DAY_BOUNDARY_OFFSET'] * 1000
            src.rename(
                {'DAY_BOUNDARY_TZ': '__DAY_BOUNDARY_TZ', 'DAY_BOUNDARY_OFFSET': '__DAY_BOUNDARY_OFFSET'}, inplace=True
            )

            self = self.join_with_query(src, symbol=f"{str(db)}::DUMMY", caching='per_symbol')
            timezone = self['__DAY_BOUNDARY_TZ']
            offset = self['__DAY_BOUNDARY_OFFSET']
            convert_timestamp = self['TIMESTAMP'].dt.strftime('%Y%m%d%H%M%S.%J', timezone=timezone)

            start_formatted = start_date.strftime('%Y-%m-%d')
            start_op = otp.dt(start_date).to_operation(timezone=timezone) + offset
            self.throw(
                where=(self['TIMESTAMP'] < start_op),
                message=(
                    'Timestamp '
                    + convert_timestamp
                    + ' of a tick, visible or hidden, '
                    + f'earlier than {start_formatted} in timezone '
                    + timezone
                ),
                inplace=True,
            )

            end_formatted = end.strftime('%Y-%m-%d')
            end_op = otp.dt(end).to_operation(timezone=timezone) + offset
            self.throw(
                where=(self['TIMESTAMP'] >= end_op),
                message=(
                    'Timestamp '
                    + convert_timestamp
                    + ' of a tick, visible or hidden, '
                    + f'later than {end_formatted} in timezone '
                    + timezone
                ),
                inplace=True,
            )
            self.drop(['__DAY_BOUNDARY_TZ', '__DAY_BOUNDARY_OFFSET'], inplace=True)
    else:
        raise ValueError(
            f'Unknown out_of_range_tick_action: {out_of_range_tick_action}.'
            ' Possible values are: "ignore", "exception"'
        )

    kwargs = dict(
        **kwargs,
        database=str(db),
        append_mode=append,
        keep_symbol_name_and_tick_type=keep_symbol_and_tick_type,
        keep_timestamp_field=keep_timestamp,
        replace_existing_time_series=replace_existing_time_series,
        allow_concurrent_write=allow_concurrent_write,
        use_context_of_query=use_context_of_query,
    )

    if start_date and end_date:
        days = (end_date - start_date).days
        if days < 0:
            raise ValueError("Parameter 'start_date' must be less than or equal to parameter 'end_date'")
        branches = []
        for i in range(days + 1):
            branch = self.copy()
            branch.sink(
                otq.WriteToOnetickDb(
                    date=(start_date + otp.Day(i)).strftime('%Y%m%d'),
                    propagate_ticks=propagate,
                    out_of_range_tick_action='IGNORE',
                    **kwargs,
                )
            )
            branches.append(branch)
        self = otp.merge(branches)
    else:
        self.sink(
            otq.WriteToOnetickDb(
                date=date.strftime('%Y%m%d') if date else '',  # type: ignore[union-attr]
                propagate_ticks=propagate,
                out_of_range_tick_action=out_of_range_tick_action.upper(),
                **kwargs,
            )
        )

    for col in ('_SYMBOL_NAME_FIELD_', '_TICK_TYPE_FIELD_'):
        if col in self.schema:
            self.drop(col, inplace=True)

    to_drop: Set[str] = set()
    if not keep_symbol_and_tick_type:
        if 'symbol_name_field' in kwargs:
            to_drop.add(kwargs['symbol_name_field'])
        if 'tick_type_field' in kwargs:
            to_drop.add(kwargs['tick_type_field'])
    if not keep_timestamp and timestamp is not None and str(timestamp) not in {'Time', 'TIMESTAMP'}:
        to_drop.add(str(timestamp))
    if correction_type is not None:
        to_drop.add(str(correction_type))
    self.schema.set(**{k: v for k, v in self.schema.items() if k not in to_drop})
    return self


@inplace_operation
def write_parquet(
    self,
    output_path,
    compression_type="snappy",
    num_tick_per_row_group=1000,
    partitioning_keys="",
    propagate_input_ticks=False,
    inplace=False,
):
    """
    Writes the input tick series to parquet data file.

    Input must not have field 'time' as that field will also be added by the EP in the resulting file(s)

    Parameters
    ----------
    output_path: str
        Path for saving ticks to Parquet file.
        Partitioned: Path to the root directory of the parquet files.
        Non-partitioned: Path to the parquet file.
    compression_type: str
        Compression type for parquet files.
        Should be one of these: `gzip`, `lz4`, `none`, `snappy` (default), `zstd`.
    num_tick_per_row_group: int
        Number of rows per row group.
    partitioning_keys: list, str
        List of fields (`list` or comma-separated string) to be used as keys for partitioning.

        Setting this parameter will switch this EP to partitioned mode.

        In non-partitioned mode, if the path points to a file that already exists, it will be overridden.
        When partitioning is active:

        * The target directory must be empty
        * Key fields and their string values will be automatically URL-encoded to avoid conflicts with
            filesystem naming rules.

        Pseudo-fields '_SYMBOL_NAME' and '_TICK_TYPE' may be used as `partitioning_keys` and
        will be added to the schema automatically.
    propagate_input_ticks: bool
        Switches propagation of the ticks. If set to `True`, ticks will be propagated.
    inplace: bool
        A flag controls whether operation should be applied inplace.
        If ``inplace=True``, then it returns nothing. Otherwise method
        returns a new modified object.

    See also
    --------
    | **WRITE_TO_PARQUET** OneTick event processor
    | :py:class:`onetick.py.ReadParquet`

    Examples
    --------
    Simple usage:

    >>> data = otp.Ticks(A=[1, 2, 3])
    >>> data = data.write_parquet("/path/to/parquet/file")  # doctest: +SKIP
    >>> otp.run(data)  # doctest: +SKIP
    """
    if not hasattr(otq, "WriteToParquet"):
        raise RuntimeError("Current version of OneTick don't support WRITE_TO_PARQUET EP")

    if isinstance(partitioning_keys, list):
        partitioning_keys = ",".join(partitioning_keys)

    compression_type = compression_type.upper()

    ep_kwargs = {}
    if 'num_tick_per_row_group' in otq.WriteToParquet.Parameters.list_parameters():
        ep_kwargs['num_tick_per_row_group'] = num_tick_per_row_group
    else:
        ep_kwargs['num_ticks_per_row_group'] = num_tick_per_row_group

    self.sink(
        otq.WriteToParquet(
            output_path=output_path,
            compression_type=compression_type,
            partitioning_keys=partitioning_keys,
            propagate_input_ticks=propagate_input_ticks,
            **ep_kwargs,
        )
    )

    return self


@inplace_operation
def save_snapshot(
    self: 'Source',
    snapshot_name='VALUE',
    snapshot_storage='memory',
    default_db='CEP_SNAPSHOT',
    database='',
    symbol_name_field=None,
    expected_symbols_per_time_series=1000,
    num_ticks=1,
    reread_prevention_level=1,
    group_by=None,
    expected_groups_per_symbol=10,
    keep_snapshot_after_query=False,
    allow_concurrent_writers=False,
    remove_snapshot_upon_start=None,
    inplace=False,
):
    """
    Saves last (at most) `n` ticks of each group of ticks from the input time series in global storage or
    in a memory mapped file under a specified snapshot name.
    Tick descriptor should be the same for all ticks saved into the snapshot.
    These ticks can then be read via :py:class:`ReadSnapshot <onetick.py.ReadSnapshot>` by using the name
    of the snapshot and the same symbol name (``<db_name>::<symbol>``) that were used by this method.

    The event processor cannot be used by default. To enable it, access control should be configured,
    so user could have rights to use **SAVE_SNAPSHOT** EP.

    Parameters
    ----------
    snapshot_name: str
        The name of the snapshot, can be any string which doesn't contain slashes or backslashes.
        Two snapshots can have the same name if they are stored in memory mapped files for different databases. Also,
        they can have the same names if they are stored in the memories of different processes (different tick_servers).
        In all other cases the names should be unique.

        Default: `VALUE`
    snapshot_storage: str
        This parameter specifies the place of storage of the snapshot. Possible options are:

        * `memory` - the snapshot is stored in the dynamic (heap) memory of the process
          that ran (or is still running) the :py:meth:`onetick.py.Source.save_snapshot` for the snapshot.
        * `memory_mapped_file` - the snapshot is stored in a memory mapped file.
          For each symbol to get the location of the snapshot in the file system, ``save_snapshot`` looks at
          the **SAVE_SNAPSHOT_DIR** parameter value in the locator section for the database of the symbol.
          In a specified directory it creates a new directory with the name of the snapshot and keeps
          the memory mapped file and some other helper files there.

        Default: `memory`
    default_db: str
        The ticks with empty symbol names or symbol names with no database name as a prefix are saved as
        if they have symbol names equal to **DEFAULT_DB::SYMBOL_NAME** (where **SYMBOL_NAME** can be empty).
        These kinds of ticks, for example, can appear after merging time series. To save/read these ticks
        to/from storage a dummy database with the specified default name should be configured in the locator.

        Default: `CEP_SNAPSHOT`
    database: str, optional
        Specifies the output database for saving the snapshot.
    symbol_name_field: str, :py:class:`~onetick.py.Column`, optional
        If this parameter is specified, then each input time series is assumed to be a union of several time series and
        the value of the specified attribute of each tick determines to which time series the tick actually belongs.
        These values should be pure symbol names (for instance if the tick belongs to the time series **DEMO_L1::A**,
        then the value of the corresponding attribute should be **A**) and the database name will be taken from
        symbol of the merged time series.
    expected_symbols_per_time_series: int
        This parameter makes sense only when ``symbol_name_field`` is specified.
        It is the number of real symbols that are expected to occur per input time series.
        Bigger numbers may result in larger memory utilization by the query but will make the query faster.

        Default: `1000`
    num_ticks: int
        The number of ticks to be stored for each group per each symbol.

        Default: `1`
    reread_prevention_level: int
        For better performance we do not use synchronization mechanisms between the snapshot writer[s] and reader[s].
        That is why when the writer submits ticks for some symbol very quickly the reader may fail to read
        those ticks, and it will keep trying to reread them until it succeeds.
        The ``reread_prevention_level`` parameter addresses this problem.
        The higher the reread prevention level the higher the chance for the reader to read ticks successfully.
        But high prevention level also means high memory utilization, that is why it is recommended to keep
        the value of this parameter unchanged until you get an error about inability of the reader to read the snapshot
        due to fast writer.

        Default: `1`
    group_by: list of str, :py:class:`~onetick.py.Column`, optional
        When specified, the EP will keep the last **n** ticks of each group for each symbol;
        otherwise it will just keep the last **n** ticks of the input time series.
        The group is a list of input ticks with the same values in the specified fields.
    expected_groups_per_symbol: int
        The number of expected groups of ticks for each time series.
        The specified value is used only when ``group_by`` fields are specified,
        otherwise it is ignored, and we assume that the number of expected groups is 1.
        The number hints the EP to allocate memory for such number of tick groups each time
        a new group of ticks is going to be created and no free memory is left.

        Default: `10`
    keep_snapshot_after_query: bool
        If the snapshot is saved in process memory and this parameter is set, the saved snapshot continues to live
        after the query ends. If this parameter is not set, the snapshot is removed as soon as the query finishes and
        its name is released for saving new snapshots with the same name.
        This parameter is ignored if the snapshot is saved in the memory mapped file.

        Default: `False`
    allow_concurrent_writers: bool
        If this parameter is ``True`` multiple saver queries can write to the same snapshot contemporaneously.
        But different writers should write to different time series.
        Also, saver queries should run inside the same process (i.e., different tick servers or loaders with otq
        transformers cannot write to the same ``memory_mapped_file`` snapshot concurrently).

        Default: `False`
    remove_snapshot_upon_start: bool, optional
        If this parameter is ``True`` the snapshot will be removed at the beginning of the query the next time
        ``save_snapshot`` is called for the same snapshot. If the parameter is ``False`` the snapshot
        with the specified name will be appended to upon the next run of ``save_snapshot``.

        If you'll leave this parameter as ``None``, it will be equal to setting this parameter to ``NOT_SET`` in EP.
        ``NOT_SET`` option operates in the same way as ``True`` for ``memory`` snapshots or ``False``
        for ``memory_mapped_file`` snapshots.

        Default: None (``NOT_SET``)
    inplace: bool
        A flag controls whether operation should be applied inplace.
        If ``inplace=True``, then it returns nothing. Otherwise method
        returns a new modified object.

    See also
    --------
    | **SAVE_SNAPSHOT** OneTick event processor
    | :py:class:`onetick.py.ReadSnapshot`
    | :py:class:`onetick.py.ShowSnapshotList`
    | :py:class:`onetick.py.FindSnapshotSymbols`
    | :py:meth:`onetick.py.Source.join_with_snapshot`

    Examples
    --------
    Save ticks to a snapshot in a memory:

    >>> src = otp.Ticks(X=[1, 2, 3, 4, 5])
    >>> src = src.save_snapshot(snapshot_name='some_snapshot')  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP

    If you want to use snapshot, stored in memory, after query, use parameter ``keep_snapshot_after_query``:

    >>> src = src.save_snapshot(snapshot_name='some_snapshot', keep_snapshot_after_query=True)  # doctest: +SKIP

    Snapshot will be associated with default database. You can set database via ``database`` parameter:

    >>> src = src.save_snapshot(
    ...     snapshot_name='some_snapshot', database='SOME_DATABASE', keep_snapshot_after_query=True
    ... )  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP
    >>>
    >>> src = otp.ShowSnapshotList()  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP
            Time  SNAPSHOT_NAME STORAGE_TYPE        DB_NAME
    0 2003-12-01  some_snapshot       MEMORY  SOME_DATABASE

    By default, only one last tick per group, if it set, or from all ticks per symbol is saved.
    You can change this number by setting ``num_ticks`` parameter:

    >>> src = src.save_snapshot(snapshot_name='some_snapshot', num_ticks=100)  # doctest: +SKIP

    Setting symbol name for every tick in snapshot from source field:

    >>> src = otp.Ticks(X=[1, 2, 3], SYMBOL_FIELD=['A', 'B', 'C'])
    >>> src = src.save_snapshot(
    ...     snapshot_name='some_snapshot', symbol_name_field='SYMBOL_FIELD', keep_snapshot_after_query=True,
    ... )  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP
    >>>
    >>> src = otp.FindSnapshotSymbols(snapshot_name='some_snapshot')  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP
            Time SYMBOL_NAME
    0 2003-12-01  DEMO_L1::A
    1 2003-12-01  DEMO_L1::B
    2 2003-12-01  DEMO_L1::C

    Group ticks by column ``X`` and keep last 2 ticks from each group:

    >>> src = otp.Ticks(X=[0, 0, 0, 1, 1, 1], Y=[1, 2, 3, 4, 5, 6])
    >>> src = src.save_snapshot(
    ...     snapshot_name='some_snapshot', group_by=[src['X']], num_ticks=2, keep_snapshot_after_query=True,
    ... )  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP
    >>>
    >>> src = otp.ReadSnapshot(snapshot_name='some_snapshot')  # doctest: +SKIP
    >>> otp.run(src)  # doctest: +SKIP
            Time  X  Y               TICK_TIME
    0 2003-12-01  0  2 2003-12-01 00:00:00.001
    1 2003-12-01  0  3 2003-12-01 00:00:00.002
    2 2003-12-01  1  5 2003-12-01 00:00:00.004
    3 2003-12-01  1  6 2003-12-01 00:00:00.005

    """
    kwargs = {}

    if not hasattr(otq, "SaveSnapshot"):
        raise RuntimeError("Current version of OneTick doesn't support SAVE_SNAPSHOT EP")

    if snapshot_storage not in ['memory', 'memory_mapped_file']:
        raise ValueError('`snapshot_storage` must be one of "memory", "memory_mapped_file"')

    if isinstance(symbol_name_field, _Column):
        symbol_name_field = str(symbol_name_field)
    if symbol_name_field and symbol_name_field not in self.schema:
        raise ValueError(f'Field "{symbol_name_field}" passed as `symbol_name_field` parameter is not in schema.')

    is_database_param_supported = is_save_snapshot_database_parameter_supported()

    if database:
        if not is_database_param_supported:
            raise RuntimeError("Current version of OneTick doesn't support `database` parameter on SAVE_SNAPSHOT EP")

        kwargs['database'] = database

    if symbol_name_field is None:
        symbol_name_field = ''

    if group_by is None:
        group_by = []

    if not isinstance(group_by, list):
        raise ValueError('`group_by` must be a list')

    result_group_by = []

    for column in group_by:
        item = column
        if isinstance(column, _Column):
            item = str(column)

        if item not in self.schema:
            raise ValueError(f'Field "{item}" passed as `group_by` parameter is not in schema.')

        result_group_by.append(item)

    snapshot_storage = snapshot_storage.upper()

    if remove_snapshot_upon_start is None:
        remove_snapshot_upon_start = 'NOT_SET'

    # clear schema
    self.schema.set()

    self.sink(
        otq.SaveSnapshot(
            snapshot_name=snapshot_name,
            snapshot_storage=snapshot_storage,
            default_db=default_db,
            symbol_name_field=symbol_name_field,
            expected_symbols_per_time_series=expected_symbols_per_time_series,
            num_ticks=num_ticks,
            reread_prevention_level=reread_prevention_level,
            group_by=','.join(result_group_by),
            expected_groups_per_symbol=expected_groups_per_symbol,
            keep_snapshot_after_query=keep_snapshot_after_query,
            allow_concurrent_writers=allow_concurrent_writers,
            remove_snapshot_upon_start=remove_snapshot_upon_start,
            **kwargs,
        )
    )

    return self


@inplace_operation
def write_text(
    self: 'Source',
    *,
    propagate_ticks=True,
    output_headers=True,
    output_types_in_headers=False,
    order=None,
    prepend_symbol_name=True,
    prepended_symbol_name_size=0,
    prepend_timestamp=True,
    separator=',',
    formats_of_fields=None,
    double_format='%f',
    output_dir=None,
    output_file=None,
    error_file=None,
    warning_file=None,
    data_quality_file=None,
    treat_input_as_binary=False,
    flush=True,
    append=False,
    allow_concurrent_write=False,
    inplace=False,
):
    r"""
    Writes the input tick series to a text file or standard output.

    Parameters
    ----------
    propagate_ticks: bool
        If True (default) then ticks will be propagated after this method, otherwise this method won't return ticks.
    output_headers: bool
        Switches the output of the headers.
        If True (default), a tick descriptor line appears in the output before the very first tick for that query.
        If the structure of the output tick changes, another tick descriptor line appears before the first changed tick.
        The header line starts with **#**.
        The field names are ordered as mandated by the ``order`` parameter or,
        if it is empty, in the order of appearance in the tick descriptor.
        Fields that are not specified in the ``order`` parameter
        will appear after specified ones in the order of their appearance in the tick descriptor.
    output_types_in_headers: bool
        Switches the output of field types in the header lines.
        ``output_types_in_headers`` can be set only when ``output_headers`` is set too.
    order: list
        The field appearance order in the output.
        If all or some fields are not specified,
        those fields will be written in the order of their appearance in the tick descriptor.

        Field **SYMBOL_NAME** may be specified if parameter ``prepend_symbol_name`` is set.

        Field **TIMESTAMP** may be specified if parameter ``prepend_timestamp`` is set.
    prepend_symbol_name: bool
        If True (default), prepends symbol name before other fields as a new field named **SYMBOL_NAME** in the header
        (if ``output_headers`` is set).
    prepended_symbol_name_size: int
        When ``prepend_symbol_name`` is set, symbol will be adjusted to this size.
        If set to 0 (default), no adjustment will be done.
    prepend_timestamp: bool
        If set (default), tick timestamps, formatted as *YYYYMMDDhhmmss.qqqqqq* in the GMT time zone,
        will be prepended to the output lines.
        Header lines, if present, will have **TIMESTAMP** as the first field name.
        The default output format for tick timestamps can be specified in the ``formats_of_fields`` parameter.
    separator: str
        The delimiter string. This doesn't have to be a single character.
        Escape sequences are allowed for **\\t** (tab), **\\\\** (\\ character) and **\\xHH** (hex codes).
        By default "," (comma) will be used.
    formats_of_fields: dict
        The dictionary of field names and their formatting specifications.
        The formatting specification is the same as in the standard C
        `printf <https://pubs.opengroup.org/onlinepubs/009695399/functions/printf.html>`_ function.

        For float and decimal fields **%f** and **%.[<precision>]f** formats are only supported,
        first one being the default an outputting 6 decimal digits.

        Also if the field format starts with **%|**,
        it means that this is a timestamp field and should be in the format **%|tz|time_format_spec**,
        where the *tz* is the time zone name (if not specified GMT will be used),
        and *time_format_spec* is a custom time format specification,
        which is the same as the one used by the
        `strftime <https://pubs.opengroup.org/onlinepubs/009695399/functions/strftime.html>`_ function.

        In addition, you can also use **%q** , **%Q** , **%k** and **%J** placeholders,
        which will be replaced by 3 and 2 sign milliseconds, 6 sign microseconds and 9 sign nanoseconds, respectively.

        **%#**, **%-**, **%U**, **%N** placeholders will be replaced by Unix timestamp, Unix timestamp in milliseconds,
        microseconds and nanoseconds, respectively.

        **%+** and **%~** placeholders will be replaced by milliseconds and nanoseconds passed since midnight.
    double_format: str
        This format will be used for fields that are holding double values
        if they are not specified in ``formats_of_fields``.
    output_dir: str
        If specified, all output (output, warning, error, and data quality) files will be redirected to it.
        If this directory does not exist, it will get created.
        By default, the current directory is used.
    output_file: str
        The output file name for generated text data.
        If not set, the standard output will be used.
        It is also possible to add symbol name, database name, tick type,
        date of tick and query start time to the file name.
        For this special placeholders should be used, which will be replaced with the appropriate values:

            * **%SYMBOL%** - will be replaced with symbol name,
            * **%DBNAME%** - with database name,
            * **%TICKTYPE%** - with tick type,
            * **%DATE%** - with date of tick,
            * **%STARTTIME%** - with start time of the query.

        .. note::
            In case of using placeholders the output of the data may be split into different files.
            For example when querying several days of data and using **%DATE%** placeholder,
            the file will be created for every day of the interval.

        This format is also available for ``error_file``, ``warning_file`` and ``data_quality_file`` input parameters.
    error_file: str
        The file name where all error messages are directed.
        If not set the standard error will be used.
    warning_file: str
        The file name where all warning messages are directed.
        If not set the standard error will be used.
    data_quality_file: str
        The file name where all data quality messages are directed.
        If not set the standard error will be used.
    treat_input_as_binary: bool
        Opens output file in binary mode to not modify content of ticks when printing them to the file.
        Also in this mode method prints no new line to the file after every tick write.
    flush: bool
        If True (default) then the output will be flushed to disk after every tick.

        .. note::
            Notice that while this setting makes results of the query recorded into a file without delay,
            making them immediately available to applications that read this file,
            it may slow down the query significantly.

    append: bool
        If set to True, will try to append data to files (output, error, warning, data_quality), instead of overwriting.
    allow_concurrent_write: bool
        Allows different queries running on the same server to write concurrently to the same files
        (output, error, warning, data_quality).
    inplace: bool
        A flag controls whether operation should be applied inplace.
        If ``inplace=True``, then it returns nothing. Otherwise method
        returns a new modified object.

    See also
    --------
    | **WRITE_TEXT** OneTick event processor
    | :py:meth:`onetick.py.Source.dump`

    Examples
    --------

    By default the text is written to the standard output:

    >>> data = otp.Ticks(A=[1, 2, 3])
    >>> write = data.write_text()
    >>> _ = otp.run(write)  # doctest: +SKIP
    #SYMBOL_NAME,TIMESTAMP,A
    AAPL,20031201050000.000000,1
    AAPL,20031201050000.001000,2
    AAPL,20031201050000.002000,3

    Output file can also be specified:

    >>> write = data.write_text(output_file='result.csv')
    >>> _ = otp.run(write)  # doctest: +SKIP
    >>> with open('result.csv') as f:  # doctest: +SKIP
    ...     print(f.read())  # doctest: +SKIP
    #SYMBOL_NAME,TIMESTAMP,A
    AAPL,20031201050000.000000,1
    AAPL,20031201050000.001000,2
    AAPL,20031201050000.002000,3

    Symbol name, timestamp of the tick and can be removed from the output:

    >>> write = data.write_text(prepend_timestamp=False,
    ...                         prepend_symbol_name=False)
    >>> _ = otp.run(write)  # doctest: +SKIP
    #A
    1
    2
    3

    The header can also be removed from the output:

    >>> write = data.write_text(output_headers=False)
    >>> _ = otp.run(write)  # doctest: +SKIP
    AAPL,20031201050000.000000,1
    AAPL,20031201050000.001000,2
    AAPL,20031201050000.002000,3

    The order of fields and separator character can be specified:

    >>> write = data.write_text(order=['A', 'TIMESTAMP'],
    ...                         separator='\t',
    ...                         prepend_symbol_name=False)
    >>> _ = otp.run(write)  # doctest: +SKIP
    #A  TIMESTAMP
    1   20031201050000.000000
    2   20031201050000.001000
    3   20031201050000.002000

    The formatting can be specified for each field:

    >>> write = data.write_text(formats_of_fields={
    ...     'TIMESTAMP': '%|GMT|%Y-%m-%d %H:%M:%S.%q',
    ...     'A': '%3d'
    ... })
    >>> _ = otp.run(write)  # doctest: +SKIP
    #SYMBOL_NAME,TIMESTAMP,A
    AAPL,2003-12-01 05:00:00.000,  1
    AAPL,2003-12-01 05:00:00.001,  2
    AAPL,2003-12-01 05:00:00.002,  3
    """
    if output_types_in_headers and not output_headers:
        raise ValueError("Parameter 'output_types_in_headers' can only be set together with 'output_headers'")

    order = order or []
    formats_of_fields = formats_of_fields or {}
    for field in list(order) + list(formats_of_fields):
        if prepend_symbol_name and field == 'SYMBOL_NAME':
            continue
        if not prepend_symbol_name and field == 'SYMBOL_NAME' and field not in self.schema:
            raise ValueError(
                "Field 'SYMBOL_NAME' can't be specified in 'order' parameter if 'prepend_symbol_name' is not set"
            )
        if not prepend_timestamp and field == 'TIMESTAMP':
            raise ValueError(
                "Field 'TIMESTAMP' can't be specified in 'order' parameter if 'prepend_timestamp' is not set"
            )
        if field not in self.schema:
            raise ValueError(f"Field '{field}' is not in schema")

    kwargs = dict(
        propagate_ticks=propagate_ticks,
        output_headers=output_headers,
        output_types_in_headers=output_types_in_headers,
        order='|'.join(order),
        prepend_symbol_name=prepend_symbol_name,
        prepended_symbol_name_size=prepended_symbol_name_size,
        prepend_timestamp=prepend_timestamp,
        # OneTick uses \ as an escape character,
        # so replacing a single \ character with two \\ characters to escape it in OneTick
        separator=separator.replace('\\', r'\\'),
        formats_of_fields='\n'.join(f'{k}={v}' for k, v in formats_of_fields.items()),
        double_format=double_format,
        output_dir=output_dir,
        output_file=output_file,
        error_file=error_file,
        warning_file=warning_file,
        data_quality_file=data_quality_file,
        treat_input_as_binary=treat_input_as_binary,
        flush=flush,
        append=append,
        allow_concurrent_write=allow_concurrent_write,
    )
    for k, v in kwargs.items():
        if v is None:
            # None values may not be supported by onetick.query
            kwargs[k] = ''

    self.sink(otq.WriteText(**kwargs))
    if not propagate_ticks:
        self.schema.set(**{})
    return self
