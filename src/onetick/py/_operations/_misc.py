import itertools

import onetick.py.types as ott
from onetick.py.core.column_operations.base import _Operation

__all__ = [
    'basename',
    'dirname',
    'fileproperty',
    'get_ac_var',
    'get_client_ip_address',
    'get_client_port',
    'get_config_var',
    'get_cpu_id',
    'get_current_context_name',
    'get_environment_var',
    'get_field_val_by_name',
    'get_orig_sym_param_val',
    'get_os_info',
    'get_otq_parameters',
    'get_query_invocation_time',
    'get_query_properties',
    'get_server_port',
    'get_symbol_name_in_db',
    'get_symbol_parameters',
    'get_symbol_partition',
    'get_thread_id',
    'get_thread_name',
    'get_tick_server_client_announcements_file',
    'get_top_level_query_id',
    'get_top_level_query_name',
    'get_transformer_query_owner',
    'get_type',
    'get_user_roles',
    'gethostname',
    'map_range',
    'mkt_activity',
    'searched_case',
    'select_matching_fields',
    'tick_byte_sum',
    'tick_tostring',
    'undefined',
    'uuid',
]


def basename(path: str | _Operation):
    """
    Returns file name from the given ``path``.

    Parameters
    ----------
    path: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **BASENAME** OneTick function

    Examples
    --------
    >>> data = otp.Ticks(A=['/dev/null', '/etc/group', '/a/b/c/d/e/f'])
    >>> data['NAME'] = otp.ops.basename(data['A'])
    >>> otp.run(data)
                         Time             A   NAME
    0 2003-12-01 00:00:00.000     /dev/null   null
    1 2003-12-01 00:00:00.001    /etc/group  group
    2 2003-12-01 00:00:00.002  /a/b/c/d/e/f      f
    """
    return _Operation(
        op_func=lambda path: (f'BASENAME({ott.value2str(path)})', str),
        op_params=[path],
    )


def dirname(path: str | _Operation):
    """
    Returns the path to the parent directory of the given ``path``.

    Parameters
    ----------
    path: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **DIRNAME** OneTick function

    Examples
    --------
    >>> data = otp.Ticks(A=['/dev/null', '/etc/group', '/a/b/c/d/e/f'])
    >>> data['DIR'] = otp.ops.dirname(data['A'])
    >>> otp.run(data)
                         Time             A          DIR
    0 2003-12-01 00:00:00.000     /dev/null        /dev/
    1 2003-12-01 00:00:00.001    /etc/group        /etc/
    2 2003-12-01 00:00:00.002  /a/b/c/d/e/f  /a/b/c/d/e/
    """
    return _Operation(
        op_func=lambda path: (f'DIRNAME({ott.value2str(path)})', str),
        op_params=[path],
    )


def fileproperty(path: str | _Operation, property: str | _Operation):
    """
    Returns the specified ``property`` value from file ``path``.

    If specified ``path`` was not found function will return -1.

    Parameters
    ----------
    path: str, :class:`~onetick.py.Operation`
    property: str, :class:`~onetick.py.Operation`
        Supported values for ``property`` are ``IS_READ_ONLY`` and ``BYTES_USED`` (number of bytes used by the file).

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **FILEPROPERTY** OneTick function

    Examples
    --------
    >>> data = otp.Ticks(A=['/dev/null', '/etc/group', '/a/b/c/d/e/f'])
    >>> data['IS_READ_ONLY'] = otp.ops.fileproperty(data['A'], 'IS_READ_ONLY')
    >>> data['BYTES_USED'] = otp.ops.fileproperty(data['A'], 'BYTES_USED')
    >>> otp.run(data)  # doctest: +SKIP
                         Time             A  IS_READ_ONLY  BYTES_USED
    0 2003-12-01 00:00:00.000     /dev/null             0           0
    1 2003-12-01 00:00:00.001    /etc/group             1         810
    2 2003-12-01 00:00:00.002  /a/b/c/d/e/f            -1          -1
    """
    return _Operation(
        op_func=lambda path, property: (f'FILEPROPERTY({ott.value2str(path)}, {ott.value2str(property)})', int),
        op_params=[path, property],
    )


def get_cpu_id():
    """
    Returns the ID of the CPU core that is executing this function.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_CPU_ID** OneTick function

    Examples
    --------
    >>> data = otp.Tick(CPU_ID=otp.ops.get_cpu_id())
    >>> otp.run(data)  # doctest: +SKIP
            Time  CPU_ID
    0 2003-12-01       9
    """
    return _Operation(
        op_str='GET_CPU_ID()',
        dtype=int,
    )


def get_thread_id():
    """
    Returns the ID of the thread that is executing this function.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_THREAD_ID** OneTick function

    Examples
    --------
    >>> data = otp.Tick(THREAD_ID=otp.ops.get_thread_id())
    >>> otp.run(data)  # doctest: +SKIP
            Time        THREAD_ID
    0 2003-12-01  132504766963840
    """
    return _Operation(
        op_str='GET_THREAD_ID()',
        dtype=int,
    )


def get_thread_name():
    """
    Returns the ID of the thread that is executing this function.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_THREAD_NAME** OneTick function

    Examples
    --------
    >>> data = otp.Tick(THREAD_NAME=otp.ops.get_thread_name())
    >>> otp.run(data)  # doctest: +SKIP
            Time  THREAD_NAME
    0 2003-12-01     ipython3
    """
    return _Operation(
        op_str='GET_THREAD_NAME()',
        dtype=str,
    )


def gethostname():
    """
    Returns the name of the host where this function runs.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GETHOSTNAME** OneTick function

    Examples
    --------
    >>> data = otp.Tick(HOST_NAME=otp.ops.gethostname())
    >>> otp.run(data)  # doctest: +SKIP
            Time        HOST_NAME
    0 2003-12-01  DESKTOP-EUI4SLI
    """
    return _Operation(
        op_str='GETHOSTNAME()',
        dtype=str,
    )


def get_server_port():
    """
    Returns the port number of the tick server, where this function is executing.

    If this function runs on the client, 0 is returned.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_SERVER_PORT** OneTick function

    Examples
    --------
    >>> data = otp.Tick(SERVER_PORT=otp.ops.get_server_port())
    >>> otp.run(data)  # doctest: +SKIP
            Time  SERVER_PORT
    0 2003-12-01        40001
    """
    return _Operation(
        op_str='GET_SERVER_PORT()',
        dtype=int,
    )


def get_client_port():
    """
    Returns the port number of the client application that initiated the query.

    If this function runs on the client, 0 is returned.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_CLIENT_PORT** OneTick function

    Examples
    --------
    >>> data = otp.Tick(CLIENT_PORT=otp.ops.get_client_port())
    >>> otp.run(data)  # doctest: +SKIP
            Time  CLIENT_PORT
    0 2003-12-01        52642
    """
    return _Operation(
        op_str='GET_CLIENT_PORT()',
        dtype=int,
    )


def get_client_ip_address():
    """
    Returns the IP address of the client application that initiated the query.

    If this function runs on the client, an empty string is returned.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_CLIENT_IP_ADDRESS** OneTick function

    Examples
    --------
    >>> data = otp.Tick(CLIENT_IP_ADDRESS=otp.ops.get_client_ip_address())
    >>> otp.run(data)  # doctest: +SKIP
            Time  CLIENT_IP_ADDRESS
    0 2003-12-01        10.36.0.231
    """
    return _Operation(
        op_str='GET_CLIENT_IP_ADDRESS()',
        dtype=str,
    )


def get_os_info(property: str | _Operation):
    """
    Returns the value for the specified OS property.

    Parameters
    ----------
    property: str, :class:`~onetick.py.Operation`
        Supported properties are ``name``, ``version``, ``bitness``.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_OS_INFO** OneTick function

    Examples
    --------
    >>> data = otp.Tick(
    ...     NAME=otp.ops.get_os_info('name'),
    ...     VERSION=otp.ops.get_os_info('version'),
    ...     BITNESS=otp.ops.get_os_info('bitness'),
    ... )
    >>> otp.run(data)  # doctest: +SKIP
            Time   NAME                            VERSION BITNESS
    0 2003-12-01  Linux  6.18.54.1-microsoft-standard-WSL2      64
    """
    return _Operation(
        op_func=lambda property: (f'GET_OS_INFO({ott.value2str(property)})', str),
        op_params=[property],
    )


def get_top_level_query_id():
    """
    Returns the unique ID of this query.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_TOP_LEVEL_QUERY_ID** OneTick function

    Examples
    --------
    >>> data = otp.Tick(QUERY_ID=otp.ops.get_top_level_query_id())
    >>> otp.run(data)  # doctest: +SKIP
            Time                                   QUERY_ID
    0 2003-12-01  DESKTOP-EUI4SLI.7246.20261105180423.989.8
    """
    return _Operation(
        op_str='GET_TOP_LEVEL_QUERY_ID()',
        dtype=str,
    )


def get_top_level_query_name():
    """
    Returns the top level name of this query.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_TOP_LEVEL_QUERY_NAME** OneTick function

    Examples
    --------
    >>> data = otp.Tick(QUERY_NAME=otp.ops.get_top_level_query_name())
    >>> otp.run(data)  # doctest: +SKIP
            Time                              QUERY_NAME
    0 2003-12-01  industrious-alpaca.run.otq::main_query
    """
    return _Operation(
        op_str='GET_TOP_LEVEL_QUERY_NAME()',
        dtype=str,
    )


def get_current_context_name():
    """
    Returns currently used context name.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_CURRENT_CONTEXT_NAME** OneTick function

    Examples
    --------
    >>> data = otp.Tick(CONTEXT_NAME=otp.ops.get_current_context_name())
    >>> otp.run(data)  # doctest: +SKIP
            Time CONTEXT_NAME
    0 2003-12-01      DEFAULT
    """
    return _Operation(
        op_str='GET_CURRENT_CONTEXT_NAME()',
        dtype=str,
    )


def get_environment_var(name: str | _Operation):
    """
    Returns the value of the environment variable
    from client (when symbol is ``LOCAL::``) or from server (when symbol is a database configured on server side).

    Returns empty string if such variable is absent.

    Parameters
    ----------
    name: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_ENVIRONMENT_VAR** OneTick function

    Examples
    --------
    >>> data = otp.Tick(
    ...     SHELL=otp.ops.get_environment_var('SHELL'),
    ...     PATH=otp.ops.get_environment_var('PATH').astype(otp.string[100]),
    ...     PWD=otp.ops.get_environment_var('PWD'),
    ... )
    >>> otp.run(data)  # doctest: +SKIP
            Time      SHELL                                          PATH                    PWD
    0 2003-12-01  /bin/bash  /home/user/onetick-py/.venv/bin:/home/use...  /home/user/onetick-py
    """
    return _Operation(
        op_func=lambda name: (f'GET_ENVIRONMENT_VAR({ott.value2str(name)})', str),
        op_params=[name],
    )


def get_config_var(name: str | _Operation):
    """
    Returns the value of the configuration parameter specified in the OneTick configuration file.

    Returns empty string if such a parameter is absent in the OneTick configuration file.

    Parameters
    ----------
    name: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_CONFIG_VAR** OneTick function

    Examples
    --------
    >>> data = otp.Tick(
    ...     ALLOW_ENV_VARS=otp.ops.get_config_var('ONE_TICK_CONFIG.ALLOW_ENV_VARS'),
    ...     DB_LOCATOR_DEFAULT=otp.ops.get_config_var('DB_LOCATOR.DEFAULT'),
    ... )
    >>> otp.run(data)  # doctest: +SKIP
            Time  ALLOW_ENV_VARS                            DB_LOCATOR_DEFAULT
    0 2003-12-01             Yes  /tmp/test_user/run_20261105_191931_10044/...
    """
    return _Operation(
        op_func=lambda name: (f'GET_CONFIG_VAR({ott.value2str(name)})', str),
        op_params=[name],
    )


def get_ac_var(name: str | _Operation):
    """
    Returns the value of the specified access control variable for the current user.

    If the variable is not set, an empty string is returned.

    Parameters
    ----------
    name: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_AC_VAR** OneTick function

    Examples
    --------
    >>> data = otp.Tick(
    ...     SERVER_LOCATION=otp.ops.get_ac_var('SERVER_LOCATION'),
    ... )
    >>> otp.run(data)  # doctest: +SKIP
            Time  SERVER_LOCATION
    0 2003-12-01   localhost:4003
    """
    return _Operation(
        op_func=lambda name: (f'GET_CONFIG_VAR({ott.value2str(name)})', str),
        op_params=[name],
    )


def get_query_invocation_time():
    """
    Returns currently used context name.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_QUERY_INVOCATION_TIME** OneTick function

    Examples
    --------
    >>> data = otp.Tick(QUERY_INVOCATION_TIME=otp.ops.get_query_invocation_time())
    >>> otp.run(data)  # doctest: +SKIP
            Time       QUERY_INVOCATION_TIME
    0 2003-12-01  2026-11-05 13:26:47.334088
    """
    return _Operation(
        op_str='GET_QUERY_INVOCATION_TIME()',
        dtype=ott.nsectime,
    )


def uuid():
    """
    Returns a unique identifier in form: ``[mac_address].[timestamp].[process_id].[seq_num]``, where:

    * ``mac_address`` - is the mac address of the machine
    * ``timestamp`` - the current time expressed as the number of milliseconds since the UNIX epoch
    * ``process_id`` - process ID
    * ``seq_num`` - is a unique to a process integer that is incremented per call to this function

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **UUID** OneTick function

    Examples
    --------
    >>> data = otp.Tick(UUID=otp.ops.uuid())
    >>> otp.run(data)  # doctest: +SKIP
            Time                                UUID
    0 2003-12-01  000000000000.1793903402585.12187.1
    """
    return _Operation(
        op_str='UUID()',
        dtype=str,
    )


def get_otq_parameters():
    """
    Returns a list of the otq parameters in the function's query as a string, separated with ``;``.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_OTQ_PARAMETERS** OneTick function

    Examples
    --------
    >>> data = otp.Tick(OTQ_PARAMETERS=otp.ops.get_otq_parameters())
    >>> otp.run(data, query_params={'A': 1, 'B': 'hello'})
            Time  OTQ_PARAMETERS
    0 2003-12-01     A=1;B=hello
    """
    return _Operation(
        op_str='GET_OTQ_PARAMETERS()',
        dtype=str,
    )


def get_query_properties(show_values_set_via_dedicated_setters: bool = False):
    """
    Returns query properties as a comma separated list of name-value pairs.

    Parameters
    ----------
    show_values_set_via_dedicated_setters: bool
        If set to True, the query properties that have dedicated setters,
        i.e. *APPLY_TIMES_DAILY*, *BATCH_SIZE*, *MAX_CONCURRENCY*, and *PROCESS_DBS_CONCURRENTLY*,
        and the values of which are not equal to the default values for those properties,
        will be included into the result of this function.

        Default is False.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_QUERY_PROPERTIES** OneTick function

    Examples
    --------
    >>> data = otp.Tick(QUERY_PROPERTIES=otp.ops.get_query_properties())
    >>> otp.run(data, query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'})  # doctest: +ELLIPSIS
            Time                            QUERY_PROPERTIES
    0 2003-12-01  ALLOW_GRAPH_REUSE='TRUE',USE_FT='FALSE'...
    """
    return _Operation(
        op_func=lambda param: (f'GET_QUERY_PROPERTIES({ott.value2str(param)})', str),
        op_params=[show_values_set_via_dedicated_setters],
    )


def get_symbol_parameters():
    """
    Returns symbol parameters as a comma separated list of name-value pairs.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_SYMBOL_PARAMETERS** OneTick function

    Examples
    --------
    >>> symbols = otp.Tick(SYMBOL_NAME='AAPL', SYMBOL_PARAM_1=1, SYMBOL_PARAM_2='hello')
    >>> data = otp.Tick(SYMBOL_PARAMETERS=otp.ops.get_symbol_parameters())
    >>> result = otp.run(data, symbols=symbols)
    >>> result['AAPL']
            Time                          SYMBOL_PARAMETERS
    0 2003-12-01  SYMBOL_PARAM_1='1',SYMBOL_PARAM_2='hello'
    """
    return _Operation(
        op_str='GET_SYMBOL_PARAMETERS()',
        dtype=str,
    )


def tick_byte_sum():
    """
    Returns the sum of bytes across all fields of the tick.

    This function is useful when comparing ticks that consist of multiple fields.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **TICK_BYTE_SUM** OneTick function

    Examples
    --------
    >>> data = otp.Ticks(A=[1, 2])
    >>> data['TICK_BYTE_SUM'] = otp.ops.tick_byte_sum()
    >>> otp.run(data)
                         Time  A  TICK_BYTE_SUM
    0 2003-12-01 00:00:00.000  1              1
    1 2003-12-01 00:00:00.001  2              2

    >>> data = otp.Ticks(A=[1, 2], B=['hello', 'world'])
    >>> data['TICK_BYTE_SUM'] = otp.ops.tick_byte_sum()
    >>> otp.run(data)
                         Time  A      B  TICK_BYTE_SUM
    0 2003-12-01 00:00:00.000  1  hello            533
    1 2003-12-01 00:00:00.001  2  world            554
    """
    return _Operation(
        op_str='TICK_BYTE_SUM()',
        dtype=int,
    )


def get_transformer_query_owner():
    """
    Returns an integer indicator of the owner of the transformer query.

    * 0 - not a transformer
    * 1 - memory loader transformer
    * 2 - daily loader transformer
    * 3 - CEP transformer
    * 4 - tick dump writer transformer

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_TRANSFORMER_QUERY_OWNER** OneTick function

    Examples
    --------
    >>> data = otp.Tick(TRANSFORMER_QUERY_OWNER=otp.ops.get_transformer_query_owner())
    >>> otp.run(data)
            Time  TRANSFORMER_QUERY_OWNER
    0 2003-12-01                        0
    """
    return _Operation(
        op_str='GET_TRANSFORMER_QUERY_OWNER()',
        dtype=int,
    )


def get_tick_server_client_announcements_file():
    """
    Returns the content of the client announcements file specified in
    *TICK_SERVER_CLIENT_ANNOUNCEMENTS_FILE* configuration parameter in OneTick configuration file.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_TICK_SERVER_CLIENT_ANNOUNCEMENTS_FILE** OneTick function

    Examples
    --------

    >>> client_announcements_file = otp.utils.TmpFile()                                                 # doctest: +SKIP
    >>> with open(client_announcements_file, 'w') as f:                                                 # doctest: +SKIP
    ...     f.write('Hello!')                                                                           # doctest: +SKIP
    >>> s = otp.Session(                                                                                # doctest: +SKIP
    ...     otp.Config(variables={                                                                      # doctest: +SKIP
    ...         'TICK_SERVER_CLIENT_ANNOUNCEMENTS_FILE': str(client_announcements_file)                 # doctest: +SKIP
    ...     })                                                                                          # doctest: +SKIP
    ... )                                                                                               # doctest: +SKIP
    >>> data = otp.Tick(CLIENT_ANNOUNCEMENTS_FILE=otp.ops.get_tick_server_client_announcements_file())  # doctest: +SKIP
    >>> otp.run(data, symbols='LOCAL::')                                                                # doctest: +SKIP
            Time  CLIENT_ANNOUNCEMENTS_FILE
    0 2003-12-01                     Hello!
    """
    return _Operation(
        op_str='GET_TICK_SERVER_CLIENT_ANNOUNCEMENTS_FILE()',
        dtype=str,
    )


def get_user_roles():
    """
    Returns a comma-separated list of the roles of the user who is executing the query.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_USER_ROLES** OneTick function

    Examples
    --------
    >>> data = otp.Tick(USER_ROLES=otp.ops.get_user_roles())
    >>> otp.run(data)
            Time USER_ROLES
    0 2003-12-01      Admin
    """
    return _Operation(
        op_str='GET_USER_ROLES()',
        dtype=str,
    )


def tick_tostring(
    *,
    separator_char: str = '',
    represent_fractional_val_as_hex: bool = False,
    sort_fields_by_name: bool = False,
    include_field_names: bool = False,
):
    """
    Creates a string from the tick by concatenating all fields of the tick except the TIMESTAMP field.

    Parameters
    ----------
    separator_char: str
        Specifies the separator character to use between field values in the resulting string.
        If specified, it should be either an empty string
        or a string that consists of a single character.

        If separator_char is an empty string, and ``include_field_names`` is set to False, no separator is used.
        If separator_char is an empty string, and ``include_field_names`` is set to True,
        a comma will be used as a separator.

        Default - empty string.
    represent_fractional_val_as_hex: bool
        If set to True, results in all bytes of values of each decimal, double, and float tick field
        to be output in their hexadecimal form,
        with the hex representation of more significant bytes preceding hex representation of less significant bytes.

        If set to False, the usual string representations of these values are output.

        Default: False.
    sort_fields_by_name: bool
        If set to True, the values of the tick fields will be output
        according to the alphabetical order of the field names.

        Otherwise, the values of the tick fields will be output in the order
        in which the fields as specified in the tick descriptor.

        Default: False.
    include_field_names: bool
        If set to True, the names of the tick fields, along with "=",
        will be printed in front of the corresponding values.

        Default: False.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **TICK_TOSTRING** OneTick function

    Examples
    --------
    >>> data = otp.Tick(B=2.2, A=1, C=otp.dt(2024, 1, 1), D='hello world')
    >>> data['TOSTRING'] = otp.ops.tick_tostring(
    ...     separator_char=',',
    ...     sort_fields_by_name=True,
    ...     include_field_names=True,
    ... )
    >>> otp.run(data)
            Time    B  A          C            D                                            TOSTRING
    0 2003-12-01  2.2  1 2024-01-01  hello world  A=1,B=2.200000,C=1704085200000000000,D=hello world
    """
    return _Operation(
        op_func=lambda sep, hex_, sort_, names: (
            f'TICK_TOSTRING({ott.value2str(sep)},{ott.value2str(hex_)},{ott.value2str(sort_)},{ott.value2str(names)})',
            str
        ),
        op_params=[
            separator_char,
            represent_fractional_val_as_hex,
            sort_fields_by_name,
            include_field_names,
        ],
    )

def select_matching_fields(
    regex: str,
    /,
    field_types: type | str | list[str | type] | None = None,
):
    """
    Returns a comma-separated list of tick descriptor field names.

    A field is included in the resulting comma-separated field list if:

    * Its name in the tick descriptor fully matches the ``regex`` parameter,
    * If ``field_types`` parameter is specified, and the type of the field is one of the specified types.

    Parameters
    ----------
    regex: str
        A regular expression to match field names with.
    field_types: type | str | list[str | type] | None
        One of the python classes specified in :ref:`Types <otp types>` or a list of them.
        Or a comma separated string of OneTick data types string names or list of such strings.

        By default all field types are returned.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **SELECT_MATCHING_FIELDS** OneTick function

    Examples
    --------
    >>> data = otp.Tick(ASK_PRICE=1.1, BID_PRICE=2.2, ASK_SIZE=3, BID_SIZE=4)
    >>> data['ASK_FIELDS'] = otp.ops.select_matching_fields('ASK.*')
    >>> data['ASK_FLOATS'] = otp.ops.select_matching_fields('ASK.*', float)
    >>> otp.run(data)
            Time  ASK_PRICE  BID_PRICE  ASK_SIZE  BID_SIZE          ASK_FIELDS  ASK_FLOATS
    0 2003-12-01        1.1        2.2         3         4  ASK_PRICE,ASK_SIZE   ASK_PRICE
    """
    def fun(regex, field_types):
        params_str = f'{ott.value2str(regex)}'
        if field_types is not None:
            if not isinstance(field_types, list):
                field_types = [field_types]
            field_types_list = []
            for t in field_types:
                if not isinstance(t, str):
                    field_types_list.append(ott.type2str(t))
                else:
                    field_types_list.append(t)
            field_types_str = ','.join(field_types_list)
            params_str += f',{ott.value2str(field_types_str)}'
        return f'SELECT_MATCHING_FIELDS({params_str})', str

    return _Operation(
        op_func=fun,
        op_params=[regex, field_types],
    )


def get_type(expression: _Operation):
    """
    Returns the type name of the argument.

    If the argument is a tick attribute or a state variable or the operation using them.

    Parameters
    ----------
    expression: :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_TYPE** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data.state_vars['TICK_LIST'] = otp.state.tick_list()
    >>> data.state_vars['TICK_SET'] = otp.state.tick_set('latest', ['A'])
    >>> data['A_TYPE'] = otp.ops.get_type(data['A'])
    >>> data['A_DIV2_TYPE'] = otp.ops.get_type(data['A'] / 2)
    >>> data['A_GT2_TYPE'] = otp.ops.get_type(data['A'] > 2)
    >>> data['STR_TYPE'] = otp.ops.get_type('hello')
    >>> data['TIMESTAMP_TYPE'] = otp.ops.get_type(data['TIMESTAMP'])
    >>> data['TICK_LIST_TYPE'] = otp.ops.get_type(data.state_vars['TICK_LIST'])
    >>> data['TICK_SET_TYPE'] = otp.ops.get_type(data.state_vars['TICK_SET'])
    >>> otp.run(data)
            Time  A  A_TYPE  A_DIV2_TYPE  A_GT2_TYPE  STR_TYPE  TIMESTAMP_TYPE  TICK_LIST_TYPE  TICK_SET_TYPE
    0 2003-12-01  1    long       double     boolean    string        datetime       TICK_LIST       TICK_SET
    """
    return _Operation(
        op_func=lambda e: (f'GET_TYPE({ott.value2str(e)})', str),
        op_params=[expression],
    )


def map_range(
    value: int | _Operation,
    /,
    ranges: dict[str | int, tuple[int, int]],
    *,
    default: int | str | None = None,
):
    """
    Maps specified numeric ``value`` to one of the ranges specified in ``ranges``
    and returns the label of the mapped range.

    Parameters
    ----------
    value: int | :class:`~onetick.py.Operation`
        Numeric value to map.
    ranges: dict[str | int, tuple[int, int]]
        Ranges to map the ``value``.
        It is a mapping from range labels (numbers or string) to ranges (tuples range start and range end)

        It is not allowed to have mixed types of labels. They must either all be strings or all be integers.

        The ranges must not be overlapping, but do not have to be continuous.
    default: int | str
        This default value will be used if the ``value`` didn't match any range.
        By default the default value by type will be used: 0 for numbers and empty string for strings.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **MAP_RANGE** OneTick function

    Examples
    --------

    Map the value to two ranges and return string labels:

    >>> data = otp.Ticks(HOUR=list(range(0, 25, 3)))
    >>> data['LABEL'] = otp.ops.map_range(data['HOUR'], {'pre-open': (4, 9), 'market': (9, 16)})
    >>> otp.run(data)  # doctest: +SKIP
                         Time  HOUR     LABEL
    0 2003-12-01 00:00:00.000     0
    1 2003-12-01 00:00:00.001     3
    2 2003-12-01 00:00:00.002     6  pre-open
    3 2003-12-01 00:00:00.003     9    market
    4 2003-12-01 00:00:00.004    12    market
    5 2003-12-01 00:00:00.005    15    market
    6 2003-12-01 00:00:00.006    18
    7 2003-12-01 00:00:00.007    21
    8 2003-12-01 00:00:00.008    24

    Setting ``default`` value:

    >>> data = otp.Ticks(HOUR=list(range(0, 25, 3)))
    >>> data['LABEL'] = otp.ops.map_range(data['HOUR'], {'pre-open': (4, 9), 'market': (9, 16)}, default='closed')
    >>> otp.run(data)  # doctest: +SKIP
                         Time  HOUR     LABEL
    0 2003-12-01 00:00:00.000     0    closed
    1 2003-12-01 00:00:00.001     3    closed
    2 2003-12-01 00:00:00.002     6  pre-open
    3 2003-12-01 00:00:00.003     9    market
    4 2003-12-01 00:00:00.004    12    market
    5 2003-12-01 00:00:00.005    15    market
    6 2003-12-01 00:00:00.006    18    closed
    7 2003-12-01 00:00:00.007    21    closed
    8 2003-12-01 00:00:00.008    24    closed
    """
    def fun(value, ranges, default):
        labels = list(ranges.keys())
        ranges_str = ';'.join(f'{start},{end},{label}' for label, (start, end) in ranges.items())

        params_str = f'{ott.value2str(value)},{ott.value2str(ranges_str)}'
        if default is not None:
            params_str += f',{ott.value2str(default)}'
            dtype = ott.get_type_by_objects(labels + [default])
        else:
            dtype = ott.get_type_by_objects(labels)

        return f'MAP_RANGE({params_str})', dtype

    return _Operation(
        op_func=fun,
        op_params=[value, ranges, default],
    )


def get_symbol_partition(
    symbol_name: str | _Operation,
    timestamp: ott.datetime | _Operation,
):
    """
    Returns the physical database to which the ``symbol_name`` belongs, for the ``timestamp`` specified.

    For physical databases returns the database name itself.

    For partitioned databases returns the partition to which the symbol resolves based on the underlying partitioner.
    It returns an empty string if the partition cannot be determined.

    Parameters
    ----------
    symbol_name: str | :class:`~onetick.py.Operation`
        Full symbol name in the form ``<dbname>::<symbol>``.
    timestamp: :class:`otp.datetime <onetick.py.datetime>` | :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_SYMBOL_PARTITION** OneTick function

    Examples
    --------
    >>> data = otp.Symbols('US_COMP_SAMPLE', for_tick_type='TRD', keep_db=True).limit(5)
    >>> data['SYMBOL_PARTITION'] = otp.ops.get_symbol_partition(data['SYMBOL_NAME'], otp.dt(2024, 3, 1, 12))
    >>> otp.run(data, date=otp.dt(2024, 2, 1))
              Time           SYMBOL_NAME SYMBOL_PARTITION
    0   2024-02-01     US_COMP_SAMPLE::A   US_COMP_SAMPLE
    1   2024-02-01   US_COMP_SAMPLE::AAL   US_COMP_SAMPLE
    2   2024-02-01  US_COMP_SAMPLE::AAPL   US_COMP_SAMPLE
    3   2024-02-01  US_COMP_SAMPLE::ABBV   US_COMP_SAMPLE
    4   2024-02-01  US_COMP_SAMPLE::ABNB   US_COMP_SAMPLE
    """
    return _Operation(
        op_func=lambda symbol_name, timestamp: (
            f'GET_SYMBOL_PARTITION({ott.value2str(symbol_name)},{ott.value2str(timestamp)})',
            str
        ),
        op_params=[symbol_name, timestamp],
    )


def undefined(name: str):
    """
    Returns False if a state variable or a field with a given ``name`` was declared, and True otherwise.

    Parameters
    ----------
    name: str or :class:`~onetick.py.Column` or state variable
        If string literal is passed, this parameter is treated as a field name.
        Otherwise, :class:`~onetick.py.Column` or state variable object can be used.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **UNDEFINED** OneTick function

    Examples
    --------
    >>> data = otp.Tick(A=1)
    >>> data.state_vars['B'] = 1
    >>> data['UNDEFINED_A'] = otp.ops.undefined('A')
    >>> data['UNDEFINED_FIELD_A'] = otp.ops.undefined(data['A'])
    >>> data['UNDEFINED_STATE_VAR_B'] = otp.ops.undefined(data.state_vars['B'])
    >>> data['UNDEFINED_C'] = otp.ops.undefined('C')
    >>> otp.run(data)
            Time  A  UNDEFINED_A  UNDEFINED_FIELD_A  UNDEFINED_STATE_VAR_B  UNDEFINED_C
    0 2003-12-01  1          0.0                0.0                    0.0          1.0
    """
    return _Operation(
        op_func=lambda name: (f'UNDEFINED({ott.value2str(str(name))})', bool),
        op_params=[name],
    )


def get_symbol_name_in_db():
    """
    Returns the symbol name of the tick in the database.

    This function is supported only in the **ADD_FIELD**, **ADD_FIELDS** and **WHERE_CLAUSE** event processors.

    This function requires the event processor to have a single tick source.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_SYMBOL_NAME_IN_DB** OneTick function

    Examples
    --------
    >>> data = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD')
    >>> data = data.limit(1)
    >>> data['SYMBOL_NAME'] = otp.ops.get_symbol_name_in_db()
    >>> data = data[['PRICE', 'SIZE', 'SYMBOL_NAME']]
    >>> result = otp.run(data, symbols=['AAPL', 'MSFT'],
    ...                  date=otp.dt(2024, 2, 1), timezone='America/New_York')
    >>> result['AAPL']
                               Time  PRICE  SIZE SYMBOL_NAME
    0 2024-02-01 04:00:00.008283417  186.5     6        AAPL
    """
    return _Operation(
        op_str='GET_SYMBOL_NAME_IN_DB()',
        dtype=str,
    )


def get_field_val_by_name(name: str | _Operation, dtype):
    """
    Returns the value of a field or state variable by its ``name``.

    The ``name`` can be specified statically as a string literal
    or dynamically by using columns or operations.

    The ``dtype`` parameter specifies the type of the evaluation result.

    Parameters
    ----------
    name: str or :class:`~onetick.py.Operation`
    dtype:
        One of the python classes specified in :ref:`Types <otp types>`.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_FIELD_VAL_BY_NAME** OneTick function

    Examples
    --------
    >>> data = otp.Ticks(A=[1, 3], B=[2, 4], FIELD_NAME=['A', 'B'])
    >>> data['FROM_A'] = otp.ops.get_field_val_by_name('A', int)
    >>> data['FROM_FIELD_NAME'] = otp.ops.get_field_val_by_name(data['FIELD_NAME'], int)
    >>> otp.run(data)
                         Time  A  B FIELD_NAME  FROM_A  FROM_FIELD_NAME
    0 2003-12-01 00:00:00.000  1  2          A       1                1
    1 2003-12-01 00:00:00.001  3  4          B       3                4
    """
    def fun(name, dtype):
        params_str = f'{ott.value2str(name)}'
        if not isinstance(name, str):
            params_str += f', {ott.value2str(ott.type2str(dtype))}'

        return f'GET_FIELD_VAL_BY_NAME({params_str})', dtype

    return _Operation(
        op_func=fun,
        op_params=[name, dtype],
    )


def get_orig_sym_param_val(name: str, dtype):
    """
    For multi-stage queries returns the value for the specified symbol parameter preserving original value type.

    Parameters
    ----------
    name: str or :class:`~onetick.py.Operation`
        The name of the symbol parameter.
    dtype:
        One of the python classes specified in :ref:`Types <otp types>`.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **GET_ORIG_SYM_PARAM_VAL** OneTick function

    Examples
    --------
    >>> symbols = otp.Tick(SYMBOL_NAME='AAPL', SYMBOL_PARAM_1=1, SYMBOL_PARAM_2='hello')
    >>> data = otp.Tick(A=1)
    >>> data['SYMBOL_PARAM_1'] = otp.ops.get_orig_sym_param_val('SYMBOL_PARAM_1', int)
    >>> data['SYMBOL_PARAM_2'] = otp.ops.get_orig_sym_param_val('SYMBOL_PARAM_2', str)
    >>> result = otp.run(data, symbols=symbols)
    >>> result['AAPL']
            Time  A  SYMBOL_PARAM_1 SYMBOL_PARAM_2
    0 2003-12-01  1               1          hello
    """
    return _Operation(
        op_func=lambda name, dtype: (f'GET_ORIG_SYM_PARAM_VAL({ott.value2str(name)})', dtype),
        op_params=[name, dtype],
    )


def mkt_activity(calendar_name: str | _Operation):
    """
    Returns a string with a value
    similar to :meth:`onetick.py.Source.mkt_activity` **MKT_ACTIVITY** field
    for the specified ``calendar_name``.

    Parameters
    ----------
    calendar_name: str or :class:`~onetick.py.Operation`
        The calendar name to choose for the respective calendar from the *CALENDAR* sections of the reference database.
        It can be a string constant or the name of the field with per-tick calendar name.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **MKT_ACTIVITY** OneTick function
    | :meth:`onetick.py.Source.mkt_activity`

    Examples
    --------
    >>> data = otp.DataSource('US_COMP_SAMPLE', tick_type='TRD', symbols='AAPL')
    >>> data = data[['PRICE', 'SIZE']].limit(5)
    >>> data['MKT_ACTIVITY'] = otp.ops.mkt_activity('CLOUD_DB_US_COMP')
    >>> otp.run(data, date=otp.date(2024, 2, 1))
                               Time   PRICE  SIZE MKT_ACTIVITY
    0 2024-02-01 04:00:00.008283417  186.50     6           Rb
    1 2024-02-01 04:00:00.008290927  185.59     1           Rb
    2 2024-02-01 04:00:00.008291153  185.49   107           Rb
    3 2024-02-01 04:00:00.010381671  185.49     1           Rb
    4 2024-02-01 04:00:00.011224206  185.50     2           Rb
    """
    return _Operation(
        op_func=lambda calendar_name: (f'MKT_ACTIVITY({ott.value2str(calendar_name)})', str),
        op_params=[calendar_name],
    )


def searched_case(*cases, default: _Operation | None = None):
    """
    Implements the logic of searched case statement in SQL.

    Checks a list of condition expressions and returns matched result expression.

    Parameters
    ----------
    cases: int | float | str | :class:`~onetick.py.Operation`
        A list of condition-result pairs.
        Condition should be an :class:`~onetick.py.Operation` and result can be any supported type.
    default: int | float | str | :class:`~onetick.py.Operation`
        Default result returned when no conditions are met.

        If not specified, default value by type is returned: 0 for numbers, empty strings for strings etc.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **SEARCHED_CASE** OneTick function
    | :meth:`onetick.py.Source.if_else`
    | :meth:`onetick.py.Source.apply`

    Examples
    --------

    Basic example with one condition:

    >>> data = otp.Ticks(X=[-1, 0, 1, 2])
    >>> data['Y'] = otp.ops.searched_case(data['X'] > 0, 'POSITIVE')
    >>> otp.run(data)
                         Time  X         Y
    0 2003-12-01 00:00:00.000 -1
    1 2003-12-01 00:00:00.001  0
    2 2003-12-01 00:00:00.002  1  POSITIVE
    3 2003-12-01 00:00:00.003  2  POSITIVE

    Several conditions and ``default`` value can be specified:

    >>> data = otp.Ticks(X=[-1, 0, 1, 2])
    >>> data['Y'] = otp.ops.searched_case(
    ...     data['X'] > 0, 'POSITIVE',
    ...     data['X'] == 0, 'ZERO',
    ...     default='NEGATIVE',
    ... )
    >>> otp.run(data)
                         Time  X         Y
    0 2003-12-01 00:00:00.000 -1  NEGATIVE
    1 2003-12-01 00:00:00.001  0      ZERO
    2 2003-12-01 00:00:00.002  1  POSITIVE
    3 2003-12-01 00:00:00.003  2  POSITIVE
    """
    if len(cases) == 0:
        raise ValueError('At least one condition needs to be specified in searched_case()')
    if len(cases) % 2 != 0:
        raise ValueError('Different number of conditions and expressions in searched_case()')

    def fun(cases, default):
        whens = [cases[i] for i in range(0, len(cases), 2)]
        thens = [cases[i] for i in range(1, len(cases), 2)]

        if default is not None:
            dtype = ott.get_type_by_objects(thens + [default])
        else:
            dtype = ott.get_type_by_objects(thens)

        whens = [str(when) for when in whens]
        thens = [ott.value2str(then) for then in thens]
        pairs = list(itertools.chain.from_iterable(zip(whens, thens, strict=True)))
        defaults = [ott.value2str(default)] if default is not None else []

        params_str = ', '.join(pairs + defaults)
        return f'SEARCHED_CASE({params_str})', dtype

    return _Operation(
        op_func=fun,
        op_params=[cases, default],
    )
