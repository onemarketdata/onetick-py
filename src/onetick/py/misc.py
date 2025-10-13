from onetick.py.compatibility import is_sha2_hashing_supported
from onetick.py.core.column_operations.base import _Operation
from onetick.py.types import value2str, string


def bit_and(value1, value2):
    """
    Performs the logical AND operation on each pair of corresponding bits of the parameters.

    Parameters
    ----------
    value1: int, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`
    value2: int, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    Basic example:

    >>> data = otp.Tick(A=1)
    >>> data['AND'] = otp.bit_and(2, 3)
    >>> otp.run(data)
            Time  A  AND
    0 2003-12-01  1    2

    You can also pass :py:class:`~onetick.py.Column` as parameter:

    >>> data = otp.Tick(A=1)
    >>> data['AND'] = otp.bit_and(data['A'], 1)
    >>> otp.run(data)
            Time  A  AND
    0 2003-12-01  1    1

    Or use :py:class:`~onetick.py.Operation` as parameter:

    >>> data = otp.Tick(A=1)
    >>> data['AND'] = otp.bit_and(data['A'] * 2, 3)
    >>> otp.run(data)
            Time  A  AND
    0 2003-12-01  1    2
    """
    return _Operation(
        op_func=lambda v1, v2: (f'BIT_AND({value2str(v1)}, {value2str(v2)})', int),
        op_params=[value1, value2],
    )


def bit_or(value1, value2):
    """
    Performs the logical OR operation on each pair of corresponding bits of the parameters.

    Parameters
    ----------
    value1: int, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`
    value2: int, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    Basic example:

    >>> data = otp.Tick(A=1)
    >>> data['OR'] = otp.bit_or(2, 1)
    >>> otp.run(data)
            Time  A  OR
    0 2003-12-01  1   3

    You can also pass :py:class:`~onetick.py.Column` as parameter:

    >>> data = otp.Tick(A=1)
    >>> data['OR'] = otp.bit_or(data['A'], 0)
    >>> otp.run(data)
            Time  A  OR
    0 2003-12-01  1   1

    Or use :py:class:`~onetick.py.Operation` as parameter:

    >>> data = otp.Tick(A=1)
    >>> data['OR'] = otp.bit_or(data['A'] * 2, 3)
    >>> otp.run(data)
            Time  A  OR
    0 2003-12-01  1   3
    """
    return _Operation(
        op_func=lambda v1, v2: (f'BIT_OR({value2str(v1)}, {value2str(v2)})', int),
        op_params=[value1, value2],
    )


def bit_xor(value1, value2):
    """
    Performs the logical XOR operation on each pair of corresponding bits of the parameters.

    Parameters
    ----------
    value1: int, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`
    value2: int, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    Basic example:

    >>> data = otp.Tick(A=1)
    >>> data['XOR'] = otp.bit_xor(0b111, 0b011)
    >>> otp.run(data)
            Time  A  XOR
    0 2003-12-01  1    4

    You can also pass :py:class:`~onetick.py.Column` as parameter:

    >>> data = otp.Tick(A=0b001)
    >>> data['XOR'] = otp.bit_xor(data['A'], 0b011)
    >>> otp.run(data)
            Time  A  XOR
    0 2003-12-01  1    2

    Or use :py:class:`~onetick.py.Operation` as parameter:

    >>> data = otp.Tick(A=0b001)
    >>> data['XOR'] = otp.bit_xor(data['A'] * 2, 0b011)
    >>> otp.run(data)
            Time  A  XOR
    0 2003-12-01  1    1
    """
    return _Operation(
        op_func=lambda v1, v2: (f'BIT_XOR({value2str(v1)}, {value2str(v2)})', int),
        op_params=[value1, value2],
    )


def bit_not(value):
    """
    Performs the logical NOT operation on each bit of the ``value``.

    Parameters
    ----------
    value: int, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    Basic example:

    >>> data = otp.Tick(A=1)
    >>> data['NOT'] = otp.bit_not(1)
    >>> otp.run(data)
            Time  A  NOT
    0 2003-12-01  1   -2

    You can also pass :py:class:`~onetick.py.Column` as parameter:

    >>> data = otp.Tick(A=1)
    >>> data['NOT'] = otp.bit_not(data['A'])
    >>> otp.run(data)
            Time  A  NOT
    0 2003-12-01  1   -2

    Or use :py:class:`~onetick.py.Operation` as parameter:

    >>> data = otp.Tick(A=1)
    >>> data['NOT'] = otp.bit_not(data['A'] * 2)
    >>> otp.run(data)
            Time  A  NOT
    0 2003-12-01  1   -3
    """
    return _Operation(
        op_func=lambda v: (f'BIT_NOT({value2str(v)})', int),
        op_params=[value],
    )


def bit_at(value, index):
    """
    Return bit from ``value`` at ``index`` position from the end (zero-based).

    Parameters
    ----------
    value: int, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`
    index: int, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    Examples
    --------
    Basic example:

    >>> data = otp.Tick(A=1)
    >>> data['AT'] = otp.bit_at(0b0010, 1)
    >>> otp.run(data)
            Time  A  AT
    0 2003-12-01  1   1

    You can also pass :py:class:`~onetick.py.Column` as parameter:

    >>> data = otp.Tick(A=0b0001)
    >>> data['AT'] = otp.bit_at(data['A'], 0)
    >>> otp.run(data)
            Time  A  AT
    0 2003-12-01  1   1

    Or use :py:class:`~onetick.py.Operation` as parameter:

    >>> data = otp.Tick(A=0b0001)
    >>> data['AT'] = otp.bit_at(data['A'] * 2, 0)
    >>> otp.run(data)
            Time  A  AT
    0 2003-12-01  1   0
    """
    return _Operation(
        op_func=lambda v, i: (f'BIT_AT({value2str(v)}, {value2str(i)})', int),
        op_params=[value, index],
    )


class _HashCodeOperator(_Operation):
    HASH_TYPES = {
        'sha_1': string[40],
        'sha_224': string[56],
        'sha_256': string[64],
        'sha_384': string[96],
        'sha_512': string[128],
        'lookup3': string[16],
        'metro_hash_64': string[16],
        'city_hash_64': string[16],
        'murmur_hash_64': string[16],
        'sum_of_bytes': string[16],
        'default': string[16],
    }

    def __init__(self, value, hash_type):
        if hash_type not in self.HASH_TYPES:
            raise ValueError(f'Incorrect hash_type was passed: {hash_type}')

        if hash_type.startswith('sha') and not is_sha2_hashing_supported():
            raise RuntimeError("SHA2 hashing unavailable on current OneTick version")

        dtype = self.HASH_TYPES[hash_type]

        def _repr(_value, _hash_type):
            _value = value2str(_value)
            _hash_type = value2str(_hash_type.upper())

            return f'COMPUTE_HASH_CODE_STR({_value}, {_hash_type})', dtype

        super().__init__(op_func=_repr,
                         op_params=[value, hash_type])


def hash_code(value, hash_type):
    """
    Returns hexadecimal encoded hash code for the specified string with the specified hash function.

    Note
    ----
    Fixed sized string hash result could differ from the same variable length string due to trailing nulls.

    Parameters
    ----------
    value: str, :py:class:`~onetick.py.Operation`, :py:class:`~onetick.py.Column`
        value to calculate hash from
    hash_type: str
        one of following hash types:

        * `sha_1`
        * `sha_224`
        * `sha_256`
        * `sha_384`
        * `sha_512`
        * `lookup3`
        * `metro_hash_64`
        * `city_hash_64`
        * `murmur_hash_64`
        * `sum_of_bytes`
        * `default`

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    See also
    --------
    **COMPUTE_HASH_CODE_STR** OneTick built-in function

    Examples
    --------
    Basic example:

    .. testcode::
        :skipif: not is_sha2_hashing_supported()

        data = otp.Tick(A=1)
        data['HASH'] = otp.hash_code('some_string', 'sha_224')
        df = otp.run(data)
        print(df)

    .. testoutput::

                Time  A                                                      HASH
        0 2003-12-01  1  12d3f96511450121e6343b5ace065ec9de7b2a946b86f7dfab8ac51f

    You can also pass :py:class:`~onetick.py.Operation` as a ``value`` parameter:

    .. testcode::
        :skipif: not is_sha2_hashing_supported()

        data = otp.Tick(A=otp.varstring('some_string'))
        data['HASH'] = otp.hash_code(data['A'], 'sha_224')
        df = otp.run(data)
        print(df)

    .. testoutput::

                Time            A                                                      HASH
        0 2003-12-01  some_string  12d3f96511450121e6343b5ace065ec9de7b2a946b86f7dfab8ac51f

    For the same string stored in strings with different fixed sizes, the hash value may differ:

    .. testcode::
        :skipif: not is_sha2_hashing_supported()

        test_str = 'example'
        data = otp.Tick(A=otp.string[128](test_str))
        data['Fixed'] = otp.hash_code(data['A'], 'sha_1')
        data['Var'] = otp.hash_code(otp.varstring(test_str), 'sha_1')
        df = otp.run(data)
        print(df)

    .. testoutput::

                Time        A                                     Fixed                                       Var
        0 2003-12-01  example  bdab82ec533c09646e45f15dc4e7ad2d2d1a8ff1  c3499c2729730a7f807efb8676a92dcb6f8a3f8f
    """
    return _HashCodeOperator(value, hash_type)


def get_symbology_mapping(dest_symbology, src_symbology=None, symbol=None, timestamp=None):
    """
    Translates and returns the symbol in ``dest_symbology`` using the current timestamp as the symbol date.

    The remaining optional parameters are specified to overwrite
    the symbology of the current symbol, the current symbol, and the timestamp, respectively.

    Parameters
    ----------
    dest_symbology: str, :py:class:`~onetick.py.Operation`
        Symbology to translate the symbol name to.
    src_symbology: str, :py:class:`~onetick.py.Operation`
        Symbology from which the symbol will be translated.
        Will be taken from the input symbol name if it has symbology part in it
        or defaults to the symbology of the input database, which is specified in the locator file.
    symbol: str, :py:class:`~onetick.py.Operation`
        Used to specify the input symbol name.
        By default the symbol name of the query is used.
    timestamp: :py:class:`~onetick.py.Operation`
        They symbol date to use when translating symbol name.
        By default the current timestamp of the tick is used.

    Returns
    -------
        :py:class:`~onetick.py.Operation`

    See also
    --------
    :py:class:`onetick.py.SymbologyMapping`

    Examples
    --------
    Get the symbol name in OID symbology:

    >>> data = otp.Tick(A=1, db=None)
    >>> data['SYMBOLOGY_MAPPING'] = otp.get_symbology_mapping('OID')
    >>> otp.run(data, symbols='TDEQ::US_COMP::AAPL',  # doctest: +SKIP
    ...         date=otp.dt(2022, 1, 3))
            Time  A SYMBOLOGY_MAPPING
    0 2022-01-03  1              9706

    Override source symbology, symbol and symbol date
    (Also note that parameters can be set from columns):

    >>> data = otp.Tick(A=1, db=None, SYMBOL='MSFT')
    >>> data['SYMBOLOGY_MAPPING'] = otp.get_symbology_mapping('OID', 'TDEQ', data['SYMBOL'], otp.dt(2022, 1, 3))
    >>> otp.run(data, symbols='US_COMP::AAPL',  # doctest: +SKIP
    ...         date=otp.dt(2022, 1, 3))
            Time  A SYMBOLOGY_MAPPING
    0 2022-01-03  1            109037
    """
    params_correct = (
        all(v is not None for v in [src_symbology, symbol, timestamp])
        or all(v is not None for v in [src_symbology, symbol])
        or not any(v is not None for v in [src_symbology, symbol, timestamp])
    )
    if not params_correct:
        raise ValueError("Parameters 'src_symbology' and 'symbol' or"
                         " 'src_symbology', 'symbol' and 'timestamp' must be specified together")

    def op_func(dest_symbology, src_symbology, symbol, timestamp):
        params = [value2str(dest_symbology)]
        for param in [src_symbology, symbol, timestamp]:
            if param is not None:
                params.append(value2str(param))
        params_str = ','.join(params)
        return f'GET_SYMBOLOGY_MAPPING({params_str})', str

    return _Operation(
        op_func=op_func,
        op_params=[dest_symbology, src_symbology, symbol, timestamp],
        dtype=str,
    )


def get_onetick_version():
    """
    Returns the string with the build name of OneTick.
    Build string may have different format depending on OneTick version.

    Note
    ----
    The version is returned from the server where the query executes.
    Usually it's the same server where the database specified in :func:`otp.run <onetick.py.run>` resides.

    Returns
    -------
    :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(VERSION=otp.get_onetick_version())
    >>> otp.run(data, symbols='US_COMP::')  # doctest: +SKIP
            Time                                      VERSION
    0 2003-12-01  BUILD_rel_20250727_update2 (20250727120000)
    """
    return _Operation(
        op_func=lambda: ('FORMATMESSAGE("%1% (%2%)", GET_ONETICK_RELEASE(), GET_ONETICK_VERSION())', str),
    )


def get_username():
    """
    Returns the string with the name of the user executing the query
    and authenticated login name of the user.

    Returns
    -------
    :py:class:`~onetick.py.Operation`

    Examples
    --------
    >>> data = otp.Tick(USER=otp.get_username())
    >>> otp.run(data)  # doctest: +SKIP
            Time               USER
    0 2003-12-01  onetick (onetick)
    """
    return _Operation(
        op_func=lambda: ('FORMATMESSAGE("%1% (%2%)", GETUSER(), GET_AUTHENTICATED_USERNAME())', str),
    )
