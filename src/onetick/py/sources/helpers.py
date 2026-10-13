from onetick.py.core.source import Source
from onetick.py.core._source.query_parameters import QueryParameters
from onetick.py.otq import otq
from .common import update_node_tick_type
from .. import utils


class ShowEpList(Source):

    _PROPERTIES = Source._PROPERTIES + ['_level_of_detail', '_show_documentation', '_tick_type']

    def __init__(
        self,
        *,
        level_of_detail: str = 'per_ep',
        show_documentation: bool = False,
        symbol=None,
        tick_type=None,
        start=utils.adaptive,
        end=utils.adaptive,
        query_parameters: QueryParameters | None = None,
        **kwargs,
    ):
        """
        Shows available event processors from all tick servers accessible from the process
        (it could be a client process, or could be a tick server, depending on where a queried symbol resides)
        where the query containing this event processor is executed.

        Parameters
        ----------
        level_of_detail: str
            Specifies the level of detail to show.
            Supported values are ``per_ep_parameter`` and ``per_ep`` (default).
        show_documentation: bool
            When set to True, field **DESCRIPTION** will be added,
            with the contents of the documentation for this EP saved in it.
            Can only be set to True if ``level_of_detail`` is ``per_ep``.
            Default value is False.
        symbol: str
            Bound symbol on the node of the graph.
            Not set by default -- the value from :func:`otp.run <onetick.py.run>` will be used.
        tick_type: str
            Tick type set on the node of the graph.
            Not set by default.
        start, end: :class:`datetime.datetime`, :class:`otp.datetime <onetick.py.datetime>`
            Time interval from which the data should be taken.
            Not set by default -- the values from :func:`otp.run <onetick.py.run>` will be used.
        query_parameters: :class:`otp.QueryParameters <onetick.py.QueryParameters>`
            Additional query properties to be set in the resulting .otq file.
            They will be used if they are not overridden by other parameters or in :func:`otp.run <onetick.py.run>`.

        Examples
        --------

        Get the list of all OneTick event processors:

        >>> data = otp.ShowEpList()
        >>> otp.run(data, symbols='LOCAL::')  # doctest: +ELLIPSIS
                  Time           EVENT_PROCESSOR_NAME       CATEGORY  IS_SOURCE    ORIGIN ...
        0   2003-12-01                    ACCESS_INFO          Other          1  BUILT_IN ...
        1   2003-12-01                        OB_VWAP    Aggregation          0  BUILT_IN ...
        2   2003-12-01                      ADD_FIELD    Transformer          0  BUILT_IN ...
        3   2003-12-01                     ADD_FIELDS    Transformer          0  BUILT_IN ...
        4   2003-12-01  ADD_FIELDS_FROM_SYMBOL_PARAMS    Transformer          0  BUILT_IN ...
        ..         ...                            ...            ...        ...       ... ...

        Get the list of all parameters of all OneTick event processors:

        >>> data = otp.ShowEpList(level_of_detail='per_ep_parameter')
        >>> otp.run(data, symbols='LOCAL::')  # doctest: +ELLIPSIS
                    Time  EVENT_PROCESSOR_NAME  EP_CATEGORY ... HAS_PARAMETERS                 PARAMETER_NAME ...
        0     2003-12-01           ACCESS_INFO        Other ...           true                      INFO_TYPE ...
        1     2003-12-01           ACCESS_INFO        Other ...           true                      DEEP_SCAN ...
        2     2003-12-01           ACCESS_INFO        Other ...           true             SHOW_FOR_ALL_USERS ...
        3     2003-12-01               OB_VWAP  Aggregation ...           true                BUCKET_INTERVAL ...
        4     2003-12-01               OB_VWAP  Aggregation ...           true          BUCKET_INTERVAL_UNITS ...
         ...         ...                   ...          ... ...            ...                            ... ...
        """
        if self._try_default_constructor(**kwargs):
            return

        self._level_of_detail = level_of_detail.lower()
        self._show_documentation = show_documentation

        self._tick_type = tick_type

        if self._level_of_detail == 'per_ep_parameter':
            if self._show_documentation:
                raise ValueError("Parameter 'show_documentation' can only be set to True"
                                 " if parameter 'level_of_detail' is set to 'PER_EP'")
            schema = {
                'EVENT_PROCESSOR_NAME'    : str,
                'EP_CATEGORY'             : str,
                'EP_IS_SOURCE'            : int,
                'EP_ORIGIN'               : str,
                'EP_OUTPUTS_DEPENDENCY'   : str,
                'EP_OUTPUTS'              : str,
                'HAS_PARAMETERS'          : str,
                'PARAMETER_NAME'          : str,
                'PARAMETER_DEFAULT_VALUE' : str,
                'PARAMETER_DESCRIPTION'   : str,
                'PARAMETER_PROPERTY_FLAGS': int,
                'PARAMETER_CHOICES'       : str,
                'IS_ADVANCED'             : int,
                'IS_LOGICAL_EXPRESSION'   : int,
                'DEPENDS_EXPR'            : str,
                'VALIDATOR_EXPR'          : str,
                'TYPE_EXPR'               : str,
            }
        elif self._level_of_detail == 'per_ep':
            schema = {
                'EVENT_PROCESSOR_NAME' : str,
                'CATEGORY'             : str,
                'IS_SOURCE'            : int,
                'ORIGIN'               : str,
                'OUTPUTS_DEPENDENCY'   : str,
                'OUTPUTS'              : str,
            }
            if self._show_documentation:
                schema['DOCUMENTATION'] = str
        else:
            raise ValueError(f"Unsupported value for parameter 'level_of_detail': {self._level_of_detail}")

        super().__init__(
            _base_ep_func=self.base_ep,
            _symbols=symbol,
            _start=start,
            _end=end,
            query_parameters=query_parameters,
            schema=schema,
            **kwargs,
        )

    def base_ep(self):
        ep = otq.ShowEpList(
            level_of_detail=self._level_of_detail.upper(),
            show_documentation=self._show_documentation,
        )
        src = Source(ep)
        update_node_tick_type(src, self._tick_type)
        return src


class ShowEpInfo(Source):
    _PROPERTIES = Source._PROPERTIES + ['_ep_name', '_prefer_local_info', '_tick_type']

    def __init__(
        self,
        ep_name: str | None = None,
        /,
        *,
        prefer_local_info: bool = False,
        symbol=None,
        tick_type=None,
        start=utils.adaptive,
        end=utils.adaptive,
        query_parameters: QueryParameters | None = None,
        **kwargs,
    ):
        """
        Shows description for requested event processor searching all accessible tick servers.

        Parameters
        ----------
        ep_name: str
            Event processor name to retrieve description for.
        prefer_local_info: bool
            If set to True and local description is present,
            no attempt is performed to retrieve description from accessible tick servers.
            Default is False.
        symbol: str
            Bound symbol on the node of the graph.
            Not set by default -- the value from :func:`otp.run <onetick.py.run>` will be used.
        tick_type: str
            Tick type set on the node of the graph.
            Not set by default.
        start, end: :class:`datetime.datetime`, :class:`otp.datetime <onetick.py.datetime>`
            Time interval from which the data should be taken.
            Not set by default -- the values from :func:`otp.run <onetick.py.run>` will be used.
        query_parameters: :class:`otp.QueryParameters <onetick.py.QueryParameters>`
            Additional query properties to be set in the resulting .otq file.
            They will be used if they are not overridden by other parameters or in :func:`otp.run <onetick.py.run>`.

        Examples
        --------

        Show information about SHOW_EP_INFO OneTick event processor:

        >>> data = otp.ShowEpInfo('SHOW_EP_INFO')
        >>> otp.run(data, symbols='LOCAL::')  # doctest: +ELLIPSIS
                Time EVENT_PROCESSOR_NAME CATEGORY  IS_SOURCE    ORIGIN OUTPUTS_DEPENDENCY OUTPUTS       DOCUMENTATION
        0 2003-12-01         SHOW_EP_INFO    Other          0  BUILT_IN   NO_NAMED_OUTPUTS          <!DOCTYPE html>...
        """
        if self._try_default_constructor(**kwargs):
            return

        self._tick_type = tick_type

        if ep_name is None:
            raise TypeError("ShowEpInfo() missing 1 required positional argument: 'ep_name'")
        self._ep_name = ep_name
        self._prefer_local_info = prefer_local_info

        schema = {
            'EVENT_PROCESSOR_NAME': str,
            'CATEGORY'            : str,
            'IS_SOURCE'           : int,
            'ORIGIN'              : str,
            'OUTPUTS_DEPENDENCY'  : str,
            'OUTPUTS'             : str,
            'DOCUMENTATION'       : str,
        }
        super().__init__(
            _base_ep_func=self.base_ep,
            _symbols=symbol,
            _start=start,
            _end=end,
            query_parameters=query_parameters,
            schema=schema,
            **kwargs,
        )

    def base_ep(self):
        ep = otq.ShowEpInfo(
            ep_name=self._ep_name,
            prefer_local_info=self._prefer_local_info,
        )
        src = Source(ep)
        update_node_tick_type(src, self._tick_type)
        return src


class ShowFunctionList(Source):

    _PROPERTIES = Source._PROPERTIES + ['_tick_type']

    def __init__(
        self,
        symbol=None,
        tick_type=None,
        start=utils.adaptive,
        end=utils.adaptive,
        query_parameters: QueryParameters | None = None,
        **kwargs,
    ):
        """
        Returns the list of functions and their parameter.

        One tick is generated for each parameter of each function.

        If a function does not have parameters, only one tick is propagated with field **HAS_PARAMETERS** set to false.
        In this case, numeric fields that are describing parameters, take the value 0
        (except for PARAMETER_TYPE, which takes -1) and string fields take empty string.

        Parameters
        ----------
        symbol: str
            Bound symbol on the node of the graph.
            Not set by default -- the value from :func:`otp.run <onetick.py.run>` will be used.
        tick_type: str
            Tick type set on the node of the graph.
            Not set by default.
        start, end: :class:`datetime.datetime`, :class:`otp.datetime <onetick.py.datetime>`
            Time interval from which the data should be taken.
            Not set by default -- the values from :func:`otp.run <onetick.py.run>` will be used.
        query_parameters: :class:`otp.QueryParameters <onetick.py.QueryParameters>`
            Additional query properties to be set in the resulting .otq file.
            They will be used if they are not overridden by other parameters or in :func:`otp.run <onetick.py.run>`.

        Examples
        --------

        Get the list of all OneTick functions:

        >>> data = otp.ShowFunctionList()
        >>> otp.run(data, symbols='LOCAL::')  # doctest: +ELLIPSIS
                  Time                   FUNCTION_NAME PARAMETER_NAME  ... PARAMETER_TYPE_NAME  ...
        0   2003-12-01                             ABS         NUMBER  ...             DECIMAL  ...
        1   2003-12-01      DYNAMIC_TICK.GET_TIMESTAMP                 ...                      ...
        2   2003-12-01               AS_YYYYMMDDHHMMSS           TIME  ...         TIME_MSEC64  ...
        3   2003-12-01               AS_YYYYMMDDHHMMSS       TIMEZONE  ...              STRING  ...
        4   2003-12-01                        NSECTIME           TIME  ...         TIME_NSEC64  ...
        ..         ...                             ...            ...  ...                 ...  ...
        """
        if self._try_default_constructor(**kwargs):
            return

        self._tick_type = tick_type

        schema = {
            'FUNCTION_NAME'          : str,
            'PARAMETER_NAME'         : str,
            'PARAMETER_TYPE'         : int,
            'PARAMETER_TYPE_NAME'    : str,
            'PARAMETER_DEFAULT_VALUE': str,
            'PARAMETER_DESCRIPTION'  : str,
            'PARAMETER_CHOICES'      : str,
            'HAS_PARAMETERS'         : str,
        }
        super().__init__(
            _base_ep_func=self.base_ep,
            _symbols=symbol,
            _start=start,
            _end=end,
            query_parameters=query_parameters,
            schema=schema,
            **kwargs,
        )

    def base_ep(self):
        src = Source(otq.ShowFunctionList())
        update_node_tick_type(src, self._tick_type)
        return src
