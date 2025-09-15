import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.core.source import Source

from .. import utils

from .common import update_node_tick_type


class ReadSnapshot(Source):
    def __init__(
        self,
        snapshot_name='VALUE',
        snapshot_storage='memory',
        allow_snapshot_absence=False,
        symbol=utils.adaptive,
        db=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        schema=None,
        **kwargs,
    ):
        """
        Reads ticks for a specified symbol name and snapshot name from global memory storage or
        from a memory mapped file.

        These ticks should be written there by the :py:meth:`onetick.py.Source.save_snapshot` event processor.
        Ticks with an empty symbol name (for example, those after merging the time series of different symbols)
        are saved under **CEP_SNAPSHOT::** symbol name, so for reading such ticks a dummy database
        with the name **CEP_SNAPSHOT** must be configured in the database locator file.

        Parameters
        ----------
        snapshot_name: str
            The name that was specified in :py:meth:`onetick.py.Source.save_snapshot` as a ``snapshot_name``
            during saving.

            Default: ``VALUE``
        snapshot_storage: 'memory' or 'memory_mapped_file'
            This parameter specifies the place of storage of the snapshot. Possible options are:

            * `memory` - the snapshot is stored in the dynamic (heap) memory of the process
              that ran (or is still running) the :py:meth:`onetick.py.Source.save_snapshot` for the snapshot.
            * `memory_mapped_file` - the snapshot is stored in a memory mapped file.
              For each symbol to get the location of the snapshot in the file system, ``ReadSnapshot`` looks at
              the **SAVE_SNAPSHOT_DIR** parameter value in the locator section for the database of the symbol.

            Default: `memory`
        allow_snapshot_absence: bool
            If specified, the EP does not display an error about missing snapshot
            if the snapshot has not been saved or is still being saved.

            Default: `False`
        symbol: str, list of str, :class:`Source`, :class:`query`, :py:func:`eval query <onetick.py.eval>`
            Symbol(s) from which data should be taken.
        tick_type: str
            Tick type.
            Default: ANY.
        start: :py:class:`otp.datetime <onetick.py.datetime>`
            Start time for tick generation. By default the start time of the query will be used.
        end: :py:class:`otp.datetime <onetick.py.datetime>`
            End time for tick generation. By default the end time of the query will be used.
        schema: dict
            Dictionary of columns names with their types.

            .. warning::
                You should set schema manually, if you want to use fields in `onetick-py` query description
                before its execution.

        See also
        --------
        | **READ_SNAPSHOT** OneTick event processor
        | :py:class:`onetick.py.ShowSnapshotList`
        | :py:class:`onetick.py.FindSnapshotSymbols`
        | :py:meth:`onetick.py.Source.save_snapshot`
        | :py:meth:`onetick.py.Source.join_with_snapshot`

        Examples
        --------
        Read snapshot from memory:

        >>> src = otp.ReadSnapshot(snapshot_name='some_snapshot')
        >>> otp.run(src)  # doctest: +SKIP
                Time  PRICE  SIZE               TICK_TIME
        0 2003-12-01  100.2   500 2003-12-01 00:00:00.000
        1 2003-12-01   98.3   250 2003-12-01 00:00:00.001
        2 2003-12-01  102.5   400 2003-12-01 00:00:00.002

        You can specify schema manually in order to reference snapshot fields while constructing query via `onetick-py`:

        >>> src = otp.ReadSnapshot(
        ...     snapshot_name='some_snapshot', schema={'PRICE': float, 'SIZE': int},
        ... )
        >>> src['VOLUME'] = src['PRICE'] * src['SIZE']

        Read snapshot for specified database and symbol name:

        >>> src = otp.ReadSnapshot(snapshot_name='some_snapshot', db='DB', symbol='AAA')

        Read snapshot from memory mapped file:

        >>> src = otp.ReadSnapshot(
        ...     snapshot_name='some_snapshot', snapshot_storage='memory_mapped_file', db='DB',
        ... )
        """
        if self._try_default_constructor(schema=schema):
            return

        if not hasattr(otq, "ReadSnapshot"):
            raise RuntimeError("Current version of OneTick don't support READ_SNAPSHOT EP")

        if schema is None:
            schema = {}

        schema.update({'TICK_TIME': otp.nsectime})

        if snapshot_storage not in ['memory', 'memory_mapped_file']:
            raise ValueError('`snapshot_storage` must be one of "memory", "memory_mapped_file"')

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=lambda: self.base_ep(
                db=db,
                tick_type=tick_type,
                snapshot_name=snapshot_name,
                snapshot_storage=snapshot_storage,
                allow_snapshot_absence=allow_snapshot_absence,
            ),
            schema=schema,
            **kwargs,
        )

    def base_ep(
        self,
        snapshot_name='VALUE',
        snapshot_storage='memory',
        allow_snapshot_absence=False,
        db=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
    ):
        snapshot_storage = snapshot_storage.upper()

        src = Source(
            otq.ReadSnapshot(
                snapshot_name=snapshot_name,
                snapshot_storage=snapshot_storage,
                allow_snapshot_absence=allow_snapshot_absence,
            )
        )

        if db or tick_type:
            update_node_tick_type(src, tick_type, db)

        return src


class ShowSnapshotList(Source):
    def __init__(self, snapshot_storage='all', **kwargs):
        """
        Outputs all snapshots names for global memory storage and/or for memory mapped files.
        These snapshots should be written there by the :py:meth:`onetick.py.Source.save_snapshot`.
        Output ticks have ``SNAPSHOT_NAME``, ``STORAGE_TYPE`` and ``DB_NAME`` fields.

        Parameters
        ----------
        snapshot_storage: 'memory' or 'memory_mapped_file'
            This parameter specifies the place of storage of the snapshot. Possible options are:

            * `memory` - the snapshot is stored in the dynamic (heap) memory of the process
              that ran (or is still running) the :py:meth:`onetick.py.Source.save_snapshot` for the snapshot.
            * `memory_mapped_file` - the snapshot is stored in a memory mapped file.
              For each symbol to get the location of the snapshot in the file system, ``ReadSnapshot`` looks at
              the **SAVE_SNAPSHOT_DIR** parameter value in the locator section for the database of the symbol.
            * `all` - shows both, `memory`, `memory_mapped_file` snapshots.

            Default: `all`

        See also
        --------
        | **SHOW_SNAPSHOT_LIST** OneTick event processor
        | :py:class:`onetick.py.ReadSnapshot`
        | :py:class:`onetick.py.FindSnapshotSymbols`
        | :py:meth:`onetick.py.Source.save_snapshot`
        | :py:meth:`onetick.py.Source.join_with_snapshot`

        Examples
        --------
        List snapshots from all snapshot storage types:

        >>> src = otp.ShowSnapshotList(snapshot_storage='all')  # doctest: +SKIP
        >>> otp.run(src)  # doctest: +SKIP
                Time SNAPSHOT_NAME        STORAGE_TYPE        DB_NAME
        0 2003-12-01    snapshot_1              MEMORY        DEMO_L1
        1 2003-12-01    snapshot_2              MEMORY        DEMO_L1
        2 2003-12-01    snapshot_3  MEMORY_MAPPED_FILE  SNAPSHOT_DEMO

        List snapshots from memory:

        >>> src = otp.ShowSnapshotList(snapshot_storage='memory')  # doctest: +SKIP
        >>> otp.run(src)  # doctest: +SKIP
                Time SNAPSHOT_NAME        STORAGE_TYPE        DB_NAME
        0 2003-12-01    snapshot_1              MEMORY        DEMO_L1
        1 2003-12-01    snapshot_2              MEMORY        DEMO_L1

        List snapshots from memory mapped files:

        >>> src = otp.ShowSnapshotList(snapshot_storage='memory_mapped_file')  # doctest: +SKIP
        >>> otp.run(src)  # doctest: +SKIP
                Time SNAPSHOT_NAME        STORAGE_TYPE        DB_NAME
        0 2003-12-01    snapshot_3  MEMORY_MAPPED_FILE  SNAPSHOT_DEMO
        """
        if 'schema' not in kwargs:
            kwargs['schema'] = {'SNAPSHOT_NAME': str, 'STORAGE_TYPE': str, 'DB_NAME': str}

        if self._try_default_constructor(**kwargs):
            return

        if not hasattr(otq, "ShowSnapshotList"):
            raise RuntimeError("Current version of OneTick don't support SHOW_SNAPSHOT_LIST EP")

        if snapshot_storage not in ['memory', 'memory_mapped_file', 'all']:
            raise ValueError('`snapshot_storage` must be one of "memory", "memory_mapped_file", "all"')

        super().__init__(
            _symbols=utils.adaptive,
            _base_ep_func=lambda: self.base_ep(snapshot_storage=snapshot_storage),
            **kwargs,
        )

    def base_ep(self, snapshot_storage='all'):
        snapshot_storage = snapshot_storage.upper()

        src = Source(otq.ShowSnapshotList(snapshot_storage=snapshot_storage))
        update_node_tick_type(src, otp.adaptive, otp.config.get('default_db', 'LOCAL'))

        return src


class FindSnapshotSymbols(Source):
    def __init__(
        self,
        snapshot_name='VALUE',
        snapshot_storage='memory',
        pattern='%',
        symbology=None,
        show_original_symbols=False,
        discard_on_match=False,
        db=None,
        tick_type=None,
        **kwargs,
    ):
        """
        Takes snapshot's name and storage type and produces a union of all symbols in the snapshot.
        It also optionally translates the symbols into a user-specified symbology and
        propagates those that match a specified name pattern as a tick with a single field: **SYMBOL_NAME**.

        If symbol name translation is performed, symbol names in native snapshot can optionally be propagated
        as another tick field: ``ORIGINAL_SYMBOL_NAME``, in which case symbols with missing translations
        are also propagated. Symbol name translation is performed as of the query symbol date and requires
        the reference database to be configured.

        The name of the database to query is extracted from the input symbol.
        For example, if an input symbol is ``TAQ::``, the symbols from the **TAQ** database will be returned.

        The ``pattern`` parameter can contain the special characters ``%`` (matches any number of characters) and
        ``_`` (matches any character).
        For example, the pattern ``I_M%`` returns any symbol beginning with I and has M as its third letter.

        If the ``discard_on_match`` parameter is set to ``True``, the names that do not match the pattern
        will be propagated.

        The timestamps of the ticks created by ``FindSnapshotSymbols`` are set to the start time of the query.

        Parameters
        ----------
        snapshot_name: str
            The name that was specified in :py:meth:`onetick.py.Source.save_snapshot` as a ``snapshot_name``
            during saving.

            Default: `VALUE`
        snapshot_storage: 'memory' or 'memory_mapped_file'
            This parameter specifies the place of storage of the snapshot. Possible options are:

            * `memory` - the snapshot is stored in the dynamic (heap) memory of the process
              that ran (or is still running) the :py:meth:`onetick.py.Source.save_snapshot` for the snapshot.
            * `memory_mapped_file` - the snapshot is stored in a memory mapped file.
              For each symbol to get the location of the snapshot in the file system, ``ReadSnapshot`` looks at
              the **SAVE_SNAPSHOT_DIR** parameter value in the locator section for the database of the symbol.

            Default: `memory`
        pattern: str
            The pattern for symbol selection. It can contain special characters ``%`` (matches any number of characters)
            and ``_`` (matches any character). To avoid this special interpretation of characters ``%`` and ``_``,
            the ``\\`` character must be added in front of them. Note that if symbol name translation is required,
            translated rather than original symbol names are checked to match the name pattern.

             Default: `%`
        symbology: Optional[str]
            The destination symbology for a symbol name translation, the latter being performed,
            if destination symbology is not empty and is different from that of the queried database.
        show_original_symbols: bool
            Switches original symbol name propagation as a tick field after symbol name translation is performed.
            Note that if this parameter is set to true, database symbols with missing translations are also propagated.

            Default: `False`
        discard_on_match: bool
            When set to true only ticks that did not match the filter are propagated,
            otherwise ticks that satisfy the filter condition are propagated.

            Default: `False`
        db: str, optional
            Database to use for snapshots search query.

            It's required to fill either this parameter or explicitly specify
            the ``symbols`` parameter of :py:func:`otp.run <onetick.py.run>`,
            or have bound symbols on any node of your query
        tick_type: str, optional
            Tick type.

        See also
        --------
        | **FIND_SNAPSHOT_SYMBOLS** OneTick event processor
        | :py:class:`onetick.py.ReadSnapshot`
        | :py:class:`onetick.py.ShowSnapshotList`
        | :py:meth:`onetick.py.Source.save_snapshot`
        | :py:meth:`onetick.py.Source.join_with_snapshot`

        Examples
        --------
        Find symbols for snapshot `some_snapshot` in database `S1`:

        >>> src = otp.FindSnapshotSymbols(snapshot_name='some_snapshot', db='S1')
        >>> otp.run(src, symbols='S1::')  # doctest: +SKIP
                Time SYMBOL_NAME
        0 2003-12-01    S1::AAPL
        1 2003-12-01    S1::AAAA
        2 2003-12-01    S1::MSFT

        Use ``pattern`` parameter to filter symbol names:

        >>> src = otp.FindSnapshotSymbols(snapshot_name='some_snapshot', db='S1', pattern='A%')
        >>> otp.run(src, symbols='S1::')  # doctest: +SKIP
                Time SYMBOL_NAME
        0 2003-12-01    S1::AAPL
        1 2003-12-01    S1::AAAA

        Select symbol names not matched by pattern:

        >>> src = otp.FindSnapshotSymbols(
        ...     snapshot_name='some_snapshot', db='S1', pattern='A%', discard_on_match=True,
        ... )
        >>> otp.run(src, symbols='S1::')  # doctest: +SKIP
                Time SYMBOL_NAME
        0 2003-12-01    S1::MSFT
        """
        if 'schema' not in kwargs:
            kwargs['schema'] = {'SYMBOL_NAME': str}

            if show_original_symbols:
                kwargs['schema'].update({'ORIGINAL_SYMBOL_NAME': str})

        if self._try_default_constructor(**kwargs):
            return

        if not hasattr(otq, "FindSnapshotSymbols"):
            raise RuntimeError("Current version of OneTick don't support SHOW_SNAPSHOT_LIST EP")

        if symbology is None:
            symbology = ''

        if snapshot_storage not in ['memory', 'memory_mapped_file']:
            raise ValueError('`snapshot_storage` must be one of "memory", "memory_mapped_file"')

        super().__init__(
            _base_ep_func=lambda: self.base_ep(
                snapshot_name=snapshot_name,
                snapshot_storage=snapshot_storage,
                pattern=pattern,
                symbology=symbology,
                show_original_symbols=show_original_symbols,
                discard_on_match=discard_on_match,
                db=db,
                tick_type=tick_type,
            ),
            **kwargs,
        )

    def base_ep(
        self,
        snapshot_name='VALUE',
        snapshot_storage='memory',
        pattern='%',
        symbology='',
        show_original_symbols=False,
        discard_on_match=False,
        db=None,
        tick_type=None,
    ):
        snapshot_storage = snapshot_storage.upper()

        src = Source(
            otq.FindSnapshotSymbols(
                snapshot_name=snapshot_name,
                snapshot_storage=snapshot_storage,
                pattern=pattern,
                symbology=symbology,
                show_original_symbols=show_original_symbols,
                discard_on_match=discard_on_match,
            )
        )
        update_node_tick_type(src, tick_type, db)

        return src
