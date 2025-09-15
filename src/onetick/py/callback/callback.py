from onetick.py.otq import otq


class CallbackBase(otq.CallbackBase):
    """
    Base class for user-defined callback classes
    for :py:func:`onetick.py.run` callback mode.

    Note
    ----
    Callbacks are executed sequentially, so make sure
    they don't take too much time to execute.

    See also
    --------
    :py:func:`onetick.py.run`

    Examples
    --------
    >>> t = otp.Ticks(A=[1, 2, 3])
    >>> class NumTicksCallback(otp.CallbackBase):
    ...     def __init__(self):
    ...         self.num_ticks = 0
    ...     def process_tick(self, tick, time):
    ...         self.num_ticks += 1
    >>> callback = NumTicksCallback()
    >>> otp.run(t, callback=callback)
    >>> callback.num_ticks
    3
    """

    # pylint: disable-next=useless-parent-delegation
    def __init__(self):
        """
        Method ``__init__()`` can be used for callback initialization.
        Can be used to define some variables for future use
        in callback methods.
        """
        super().__init__()

    def replicate(self):
        """
        Called to replicate the callback object for each output node.
        May also be used for internal copying of callback object.

        Returns
        -------
            By default reference to this callback object
        """
        return self

    def process_callback_label(self, callback_label):
        """
        Called immediately before :meth:`process_symbol_name`
        to supply label assigned to callback.

        Parameters
        ----------
        callback_label: str
            label assigned to this callback object
        """
        pass

    def process_symbol_name(self, symbol_name):
        """
        Invoked to supply the name of the security that produces
        all the ticks that will be delivered to this callback object.
        If these ticks are provided by several securities,
        the ``symbol_name`` parameter is set to empty string.

        Parameters
        ----------
        symbol_name: str
            name of security

        Examples
        --------
        >>> t = otp.Tick(A=1)
        >>> class SymbolNameCallback(otp.CallbackBase):
        ...     def process_symbol_name(self, symbol_name):
        ...         self.symbol_name = symbol_name
        >>> callback = SymbolNameCallback()
        >>> otp.run(t, callback=callback, symbols='DEMO_L1::X')
        >>> callback.symbol_name
        'DEMO_L1::X'
        """
        pass

    def process_symbol_group_name(self, symbol_group_name):
        """
        Called when a named group of securities, i.e. portfolio, is processed.

        Parameters
        ----------
        symbol_group_name: str
            The name of security group.
        """
        pass

    def process_tick_type(self, tick_type):
        """
        Reports the type of the security ticks which are processed by this callback object.
        This method is called before any call to
        :meth:`process_tick_descriptor` or :meth:`process_tick`.
        It is called immediately after :meth:`process_symbol_name`.

        Parameters
        ----------
        tick_type: str
            The name of tick type.

        Examples
        --------
        >>> t = otp.Tick(A=1, symbol='DEMO_L1', tick_type='TT_TT')
        >>> class TickTypeCallback(otp.CallbackBase):
        ...     def process_tick_type(self, tick_type):
        ...         self.tick_type = tick_type
        >>> callback = TickTypeCallback()
        >>> otp.run(t, callback=callback)
        >>> callback.tick_type
        'DEMO_L1::TT_TT'
        """
        pass

    def process_tick_descriptor(self, tick_descriptor):
        """
        This method is invoked before the first call to :meth:`process_tick`
        and every time before tick structure changes.

        Parameters
        ----------
        tick_descriptor: list of tuple
            First element of each tuple is field's name
            and the second one is a dictionary ``{'type': string_field_type}``.

        Examples
        --------
        >>> t = otp.Tick(A=1)
        >>> class TickDescriptorCallback(otp.CallbackBase):
        ...    def process_tick_descriptor(self, tick_descriptor):
        ...        self.tick_descriptor = tick_descriptor
        >>> callback = TickDescriptorCallback()
        >>> otp.run(t, callback=callback)
        >>> callback.tick_descriptor
        [('A', {'type': 'int'})]
        """
        pass

    def process_tick(self, tick, time):
        """
        Called to deliver each tick.

        Note
        ----
        If you are making query through WebAPI mode, use ``process_ticks`` callback method instead.

        Parameters
        ----------
        tick: dict
            mapping of field names to field values
        time: :py:class:`datetime.datetime`
            timestamp of the tick in GMT timezone.

        Examples
        --------
        >>> t = otp.Tick(A=1)
        >>> class ProcessTickCallback(otp.CallbackBase):
        ...     def process_tick(self, tick, time):
        ...         self.result = (tick, time)
        >>> callback = ProcessTickCallback()
        >>> otp.run(t, callback=callback)
        >>> callback.result
        ({'A': 1}, datetime.datetime(2003, 12, 1, 5, 0))
        """
        pass

    def process_ticks(self, ticks):
        """
        Called after getting all ticks for WebAPI queries.

        Due to limitation of WebAPI mode ``process_tick`` callback method, which invoked on each tick, isn't supported.
        Instead, WebAPI supports ``process_ticks`` method which invoked for processing the data
        after the query is finished.
        All ticks are returned on the ``ticks`` variable.

        Parameters
        ----------
        ticks: dict
            mapping of field names to field values

        Examples
        --------
        >>> t = otp.Tick(A=1)
        >>> class ProcessTicksCallback(otp.CallbackBase):
        ...     def __init__(self):
        ...         self.result = {}
        ...
        ...     def process_ticks(self, ticks):
        ...         self.result = ticks
        >>> callback = ProcessTicksCallback()
        >>> otp.run(t, callback=callback)  # doctest: +SKIP
        >>> callback.result  # doctest: +SKIP
        {'Time': array(['2003-12-01T00:00:00.000000000'], dtype='datetime64[ns]'), 'A': array([1])}
        """
        pass

    def process_sorting_order(self, sorted_by_time_flag):
        """
        Informs whether the ticks that will be submitted to this callback object
        will be ordered by time.

        Parameters
        ----------
        sorted_by_time_flag: bool
            Indicates whether the incoming ticks will be sorted by time.
        """
        pass

    def process_data_quality_change(self, symbol_name, data_quality, time):
        """
        Called to report a data quality change, such as collection outage.

        Parameters
        ----------
        symbol_name: str
            Symbol name for each data quality change is propagated.
        data_quality: int
            parameter has following meaning:

            * `QUALITY_UNKNOWN` = -1,
            * `QUALITY_OK`,
            * `QUALITY_STALE` = 1,
            * `QUALITY_MISSING` = 2,
            * `QUALITY_PATCHED` = 4,
            * `QUALITY_MOUNT_BAD` = 9,
            * `QUALITY_DISCONNECT` = 17,
            * `QUALITY_COLLECTOR_FAILURE` = 33,
            * `QUALITY_DELAY_STITCHING_WITH_RT` = 64,
            * `QUALITY_OK_STITCHING_WITH_RT` = 66
        time: :py:class:`datetime.datetime`
            Time of the change in GMT timezone.
        """
        pass

    def process_error(self, error_code, error_msg):
        """
        Called to report a per-security error or per-security warning.

        Parameters
        ----------
        error_code: int
            Values of error code less than 1000 are warnings.
            Warnings signal issues which might not affect results of the query
            and thus could be chosen to be ignored
        error_msg: str
            Error message
        """
        pass

    def done(self):
        """
        Invoked when all the raw or computed ticks for a given request
        were submitted to the callback using the :meth:`process_tick` method.

        Examples
        --------
        >>> t = otp.Tick(A=1)
        >>> class DoneCallback(otp.CallbackBase):
        ...     def done(self):
        ...         self.done = True
        >>> callback = DoneCallback()
        >>> otp.run(t, callback=callback)
        >>> callback.done
        True
        """
        pass
