import onetick.py.types as ott
from onetick.py.core.column_operations.base import _Operation

__all__ = [
    'ascii',
    'base16_decode',
    'base16_encode',
    'base64_decode',
    'base64_encode',
    'bit_length',
    'byte_at',
    'char',
    'declare_string_size',
    'is_character_present',
    'name_value_pair_add',
    'name_value_pair_modify',
    'name_value_pair_remove',
    'set_byte',
    'space',
    'str_to_utf8',
    'strcmp',
    'urldecode',
    'urlencode',
    'utf8_to_str',
]


def ascii(value: str | _Operation) -> _Operation:
    """
    Returns the ASCII code of the first character in the string ``value``.

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **ASCII** OneTick function

    Examples
    --------

    Get the ASCII code of the first character in the string literal:

    >>> data = otp.Tick(A=otp.ops.ascii('hello'))
    >>> otp.run(data)
            Time    A
    0 2003-12-01  104

    Get the ASCII code of the first character in each string of the column:

    >>> data = otp.Ticks(A=['hello', 'world'])
    >>> data['ASCII'] = otp.ops.ascii(data['A'])
    >>> otp.run(data)
                         Time      A  ASCII
    0 2003-12-01 00:00:00.000  hello    104
    1 2003-12-01 00:00:00.001  world    119
    """
    return _Operation(
        op_func=lambda value: (f'ASCII({ott.value2str(value)})', int),
        op_params=[value],
    )


def bit_length(value: str | _Operation) -> _Operation:
    """
    Returns the length of the string ``value`` in bits.

    (If a byte with the value of 0 is present in the string, its position (0-based) multiplied by 8 is returned)

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **BIT_LENGTH** OneTick function

    Examples
    --------

    Get the length of a string literal in bits:

    >>> data = otp.Tick(A=otp.ops.bit_length('hello'))
    >>> otp.run(data)
            Time   A
    0 2003-12-01  40

    Get the length of each string in a column:

    >>> data = otp.Ticks(A=['hello', 'world!'])
    >>> data['BIT_LENGTH'] = otp.ops.bit_length(data['A'])
    >>> otp.run(data)
                         Time       A  BIT_LENGTH
    0 2003-12-01 00:00:00.000   hello          40
    1 2003-12-01 00:00:00.001  world!          48
    """
    return _Operation(
        op_func=lambda value: (f'BIT_LENGTH({ott.value2str(value)})', int),
        op_params=[value],
    )


def char(value: int | _Operation, utf8: bool = False) -> _Operation:
    """
    Returns a 1-character string with the character
    corresponding to the ASCII or UTF-8 code passed as a parameter ``value``.

    Parameters
    ----------
    value: int, :class:`~onetick.py.Operation`
    utf8: bool

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **CHAR** OneTick function
    | **CHAR_UTF8** OneTick function

    Examples
    --------

    Get the string by ASCII code:

    >>> data = otp.Tick(A=otp.ops.char(78))
    >>> otp.run(data)
            Time   A
    0 2003-12-01   N

    Get the string by ASCII code for each value in a column:

    >>> data = otp.Ticks(A=list(range(65, 79)))
    >>> data['CHAR'] = otp.ops.char(data['A'])
    >>> otp.run(data)
                          Time   A  CHAR
    0  2003-12-01 00:00:00.000  65     A
    1  2003-12-01 00:00:00.001  66     B
    2  2003-12-01 00:00:00.002  67     C
    3  2003-12-01 00:00:00.003  68     D
    4  2003-12-01 00:00:00.004  69     E
    5  2003-12-01 00:00:00.005  70     F
    6  2003-12-01 00:00:00.006  71     G
    7  2003-12-01 00:00:00.007  72     H
    8  2003-12-01 00:00:00.008  73     I
    9  2003-12-01 00:00:00.009  74     J
    10 2003-12-01 00:00:00.010  75     K
    11 2003-12-01 00:00:00.011  76     L
    12 2003-12-01 00:00:00.012  77     M
    13 2003-12-01 00:00:00.013  78     N

    Use parameter ``utf8`` to support UTF-8 codes:

    >>> data = otp.Ticks(A=list(range(200, 210)))
    >>> data['CHAR'] = otp.ops.char(data['A'], utf8=True)
    >>> otp.run(data, encoding='utf-8')
                         Time    A  CHAR
    0 2003-12-01 00:00:00.000  200     È
    1 2003-12-01 00:00:00.001  201     É
    2 2003-12-01 00:00:00.002  202     Ê
    3 2003-12-01 00:00:00.003  203     Ë
    4 2003-12-01 00:00:00.004  204     Ì
    5 2003-12-01 00:00:00.005  205     Í
    6 2003-12-01 00:00:00.006  206     Î
    7 2003-12-01 00:00:00.007  207     Ï
    8 2003-12-01 00:00:00.008  208     Ð
    9 2003-12-01 00:00:00.009  209     Ñ
    """
    fun = 'CHAR_UTF8' if utf8 else 'CHAR'

    return _Operation(
        op_func=lambda value: (f'{fun}({ott.value2str(value)})', str),
        op_params=[value],
    )


def declare_string_size(expression: str | _Operation, size: int) -> _Operation:
    """
    This function is used to indicate the maximum ``size`` of the string returned by ``expression``.

    This function is especially useful in SQL queries in which some output fields are expressions that return strings.
    In those SQL queries, use of :func:`declare_string_size` around such expressions is the only way to ensure
    that output string fields will be sufficiently wide for the entire output value.

    Parameters
    ----------
    expression: str, :class:`~onetick.py.Operation`
    size: int

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **DECLARE_STRING_SIZE** OneTick function

    Examples
    --------

    Limit the length of the string expression:

    >>> data = otp.Ticks(A=['hello', 'world!'])
    >>> data['X'] = otp.ops.declare_string_size(data['A'].str.repeat(3), 10)
    >>> otp.run(data)
                         Time       A           X
    0 2003-12-01 00:00:00.000   hello  hellohello
    1 2003-12-01 00:00:00.001  world!  world!worl
    """
    return _Operation(
        op_func=lambda expr_: (f'DECLARE_STRING_SIZE({size}, {ott.value2str(expr_)})', ott.string[size]),
        op_params=[expression],
    )


def is_character_present(value: str | _Operation, characters: str | _Operation) -> _Operation:
    """
    Returns True (1) if the ``value`` contains
    at least one character from the character set specified by ``characters``, otherwise False (0).

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`
    characters: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **IS_CHARACTER_PRESENT** OneTick function
    | :meth:`~onetick.py.Source.character_present`

    Examples
    --------

    Check if characters are present in a string literal:

    >>> data = otp.Tick(A=otp.ops.is_character_present('hello', 'abcd'))
    >>> otp.run(data)
            Time  A
    0 2003-12-01  0

    Check if characters are present in each string of the column:

    >>> data = otp.Ticks(A=['hello', 'world'])
    >>> data['ABCD'] = otp.ops.is_character_present(data['A'], 'abcd')
    >>> otp.run(data)
                         Time      A  ABCD
    0 2003-12-01 00:00:00.000  hello     0
    1 2003-12-01 00:00:00.001  world     1
    """
    return _Operation(
        op_func=lambda value, characters: (f'IS_CHARACTER_PRESENT({ott.value2str(value)},{ott.value2str(characters)})',
                                           int),
        op_params=[value, characters],
    )


def space(count: int | _Operation) -> _Operation:
    """
    Returns a string consisting of ``count`` spaces.

    Parameters
    ----------
    count: int, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **SPACE** OneTick function

    Examples
    --------

    Specify number of spaces as an integer:

    >>> data = otp.Tick(A=otp.ops.space(10) + '!')
    >>> otp.run(data)  # doctest: -NORMALIZE_WHITESPACE
            Time            A
    0 2003-12-01            !

    Get the number of spaces from a column:

    >>> data = otp.Ticks(A=[1, 2, 3, 4, 5])
    >>> data['X'] = '|' + otp.ops.space(data['A']) + '|'
    >>> otp.run(data)  # doctest: -NORMALIZE_WHITESPACE
                         Time  A        X
    0 2003-12-01 00:00:00.000  1      | |
    1 2003-12-01 00:00:00.001  2     |  |
    2 2003-12-01 00:00:00.002  3    |   |
    3 2003-12-01 00:00:00.003  4   |    |
    4 2003-12-01 00:00:00.004  5  |     |
    """
    return _Operation(
        op_func=lambda count: (f'SPACE({ott.value2str(count)})', str),
        op_params=[count],
    )


def set_byte(value: str | _Operation, index: int | _Operation, byte_value: int | _Operation) -> _Operation:
    """
    Returns a string with byte at position ``index`` (0-based) of string ``value`` changed to ``byte_value``.

    If ``index`` is outside of ``value``'s boundaries, returned string is identical to ``value``.

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`
    index: int, :class:`~onetick.py.Operation`
    byte_value: int, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **SET_BYTE** OneTick function

    Examples
    --------

    Change byte in string literal:

    >>> data = otp.Tick(A=otp.ops.set_byte('hello', 2, 88))
    >>> otp.run(data)
            Time      A
    0 2003-12-01  heXlo

    Change byte for each string in a column:

    >>> data = otp.Ticks(A=[0, 1, 2, 3, 4, 5])
    >>> data['X'] = otp.ops.set_byte('hello', data['A'], 88)
    >>> otp.run(data)
                         Time  A      X
    0 2003-12-01 00:00:00.000  0  Xello
    1 2003-12-01 00:00:00.001  1  hXllo
    2 2003-12-01 00:00:00.002  2  heXlo
    3 2003-12-01 00:00:00.003  3  helXo
    4 2003-12-01 00:00:00.004  4  hellX
    5 2003-12-01 00:00:00.005  5  hello
    """
    return _Operation(
        op_func=lambda value, index, byte_value: (
            f'SET_BYTE({ott.value2str(value)},{ott.value2str(index)},{ott.value2str(byte_value)})',
            str
        ),
        op_params=[value, index, byte_value],
    )

def byte_at(value: str | _Operation, index: int | _Operation) -> _Operation:
    """
    Returns the integer byte value from the string ``value`` at the position indicated by the 0-based ``index``.
    Returns -1, if the byte does not exist on this position.

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`
    index: int, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **BYTE_AT** OneTick function

    Examples
    --------

    Get byte in string literal:

    >>> data = otp.Tick(A=otp.ops.byte_at('hello', 1))
    >>> otp.run(data)
            Time    A
    0 2003-12-01  101

    Get the byte for each letter in a string:

    >>> data = otp.Ticks(A=[0, 1, 2, 3, 4, 5])
    >>> data['X'] = otp.ops.byte_at('hello', data['A'])
    >>> otp.run(data)
                         Time  A    X
    0 2003-12-01 00:00:00.000  0  104
    1 2003-12-01 00:00:00.001  1  101
    2 2003-12-01 00:00:00.002  2  108
    3 2003-12-01 00:00:00.003  3  108
    4 2003-12-01 00:00:00.004  4  111
    5 2003-12-01 00:00:00.005  5   -1
    """
    return _Operation(
        op_func=lambda value, index: (f'BYTE_AT({ott.value2str(value)},{ott.value2str(index)})', int),
        op_params=[value, index],
    )

def strcmp(value1: str | _Operation, value2: str | _Operation) -> _Operation:
    """
    Compares the strings ``value1`` and ``value2``.
    Returns 0 if they are equal, -1 if ``value1`` is lexicographically smaller than ``value2``, 1 otherwise.

    Parameters
    ----------
    value1: str, :class:`~onetick.py.Operation`
    value2: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **STRCMP** OneTick function

    Examples
    --------

    Comparing string literals:

    >>> data = otp.Tick(A=otp.ops.strcmp('hello', 'world'))
    >>> otp.run(data)
            Time   A
    0 2003-12-01  -1

    Comparing each string in a column:

    >>> data = otp.Ticks(A=['a', 'b', 'c'], B=['c', 'b', 'a'])
    >>> data['X'] = otp.ops.strcmp(data['A'], data['B'])
    >>> otp.run(data)
                         Time  A  B  X
    0 2003-12-01 00:00:00.000  a  c -1
    1 2003-12-01 00:00:00.001  b  b  0
    2 2003-12-01 00:00:00.002  c  a  1
    """
    return _Operation(
        op_func=lambda value1, value2: (f'STRCMP({ott.value2str(value1)},{ott.value2str(value2)})', int),
        op_params=[value1, value2],
    )


def urlencode(value: str | _Operation) -> _Operation:
    """
    Modifies the string ``value`` to be URI compliant,
    replacing most non-alphanumeric characters with ``%xx`` within it,
    in accordance with RFC 3986.

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **URLENCODE** OneTick function
    | :meth:`urldecode`

    Examples
    --------

    Encoding string literal:

    >>> data = otp.Tick(A=otp.ops.urlencode('hello@gmail.com'))
    >>> otp.run(data)
            Time                  A
    0 2003-12-01  hello%40gmail.com

    Encoding each string in a column:

    >>> data = otp.Ticks(A=['hello@gmail.com', 'world@gmail.com'])
    >>> data['X'] = otp.ops.urlencode(data['A'])
    >>> otp.run(data)
                         Time                A                  X
    0 2003-12-01 00:00:00.000  hello@gmail.com  hello%40gmail.com
    1 2003-12-01 00:00:00.001  world@gmail.com  world%40gmail.com
    """
    return _Operation(
        op_func=lambda value: (f'URLENCODE({ott.value2str(value)})', str),
        op_params=[value],
    )


def urldecode(value: str | _Operation) -> _Operation:
    """
    Modifies the URI compliant string ``value``,
    replacing percent-encoded octets ``%xx`` within it with corresponding characters,
    in accordance with RFC 3986.

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **URLDECODE** OneTick function
    | :meth:`urlencode`

    Examples
    --------

    Decoding string literal:

    >>> data = otp.Tick(A=otp.ops.urldecode('hello%40gmail.com'))
    >>> otp.run(data)
            Time                A
    0 2003-12-01  hello@gmail.com

    Decoding each string in a column:

    >>> data = otp.Ticks(A=['hello%40gmail.com', 'world%40gmail.com'])
    >>> data['X'] = otp.ops.urldecode(data['A'])
    >>> otp.run(data)
                         Time                  A                X
    0 2003-12-01 00:00:00.000  hello%40gmail.com  hello@gmail.com
    1 2003-12-01 00:00:00.001  world%40gmail.com  world@gmail.com
    """
    return _Operation(
        op_func=lambda value: (f'URLDECODE({ott.value2str(value)})', str),
        op_params=[value],
    )


def base64_encode(value: str | _Operation) -> _Operation:
    """
    Encodes the string ``value`` with Base64 algorithm in accordance to RFC 4648.

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **BASE64_ENCODE** OneTick function
    | :meth:`base64_decode`

    Examples
    --------

    Encoding string literal:

    >>> data = otp.Tick(A=otp.ops.base64_encode('hello'))
    >>> otp.run(data)
            Time         A
    0 2003-12-01  aGVsbG8=

    Encoding each string in a column:

    >>> data = otp.Ticks(A=['hello', 'world'])
    >>> data['X'] = otp.ops.base64_encode(data['A'])
    >>> otp.run(data)
                         Time      A                                                                 X
    0 2003-12-01 00:00:00.000  hello  aGVsbG8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
    1 2003-12-01 00:00:00.001  world  d29ybGQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA

    Note that all NULL-characters are also encoded by default,
    they can be removed with
    :meth:`~onetick.py.core.column_operations.accessors.str_accessor._StrAccessor.rtrim`:

    >>> data = otp.Ticks(A=['hello', 'world'])
    >>> data['X'] = otp.ops.base64_encode(data['A'].str.rtrim())
    >>> otp.run(data)
                         Time      A         X
    0 2003-12-01 00:00:00.000  hello  aGVsbG8=
    1 2003-12-01 00:00:00.001  world  d29ybGQ=
    """
    return _Operation(
        op_func=lambda value: (f'BASE64_ENCODE({ott.value2str(value)})', str),
        op_params=[value],
    )


def base64_decode(value: str | _Operation) -> _Operation:
    """
    Decodes the string ``value`` encoded with Base64 algorithm in accordance to RFC 4648.

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **BASE64_DECODE** OneTick function
    | :meth:`base64_encode`

    Examples
    --------

    Decoding string literal:

    >>> data = otp.Tick(A=otp.ops.base64_decode('aGVsbG8='))
    >>> otp.run(data)
            Time      A
    0 2003-12-01  hello

    Decoding each string in a column:

    >>> data = otp.Ticks(A=['aGVsbG8=', 'd29ybGQ='])
    >>> data['X'] = otp.ops.base64_decode(data['A'])
    >>> otp.run(data)
                         Time         A      X
    0 2003-12-01 00:00:00.000  aGVsbG8=  hello
    1 2003-12-01 00:00:00.001  d29ybGQ=  world
    """
    # see OTSS-17333 for RTRIM justification
    return _Operation(
        op_func=lambda value: (f'BASE64_DECODE(RTRIM({ott.value2str(value)}))', str),
        op_params=[value],
    )


def base16_encode(value: str | _Operation) -> _Operation:
    """
    Encodes the string ``value`` with Base16 algorithm in accordance to RFC 4648.

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **BASE16_ENCODE** OneTick function
    | :meth:`base16_decode`

    Examples
    --------

    Encoding string literal:

    >>> data = otp.Tick(A=otp.ops.base16_encode('hello'))
    >>> otp.run(data)
            Time           A
    0 2003-12-01  68656C6C6F

    Encoding each string in a column:

    >>> data = otp.Ticks(A=['hello', 'world'])
    >>> data['X'] = otp.ops.base16_encode(data['A'])
    >>> otp.run(data)
                         Time      A                                                                 X
    0 2003-12-01 00:00:00.000  hello  68656C6C6F000000000000000000000000000000000000000000000000000000
    1 2003-12-01 00:00:00.001  world  776F726C64000000000000000000000000000000000000000000000000000000

    Note that all NULL-characters are also encoded by default,
    they can be removed with
    :meth:`~onetick.py.core.column_operations.accessors.str_accessor._StrAccessor.rtrim`:

    >>> data = otp.Ticks(A=['hello', 'world'])
    >>> data['X'] = otp.ops.base16_encode(data['A'].str.rtrim())
    >>> otp.run(data)
                         Time      A           X
    0 2003-12-01 00:00:00.000  hello  68656C6C6F
    1 2003-12-01 00:00:00.001  world  776F726C64
    """
    return _Operation(
        op_func=lambda value: (f'BASE16_ENCODE({ott.value2str(value)})', str),
        op_params=[value],
    )


def base16_decode(value: str | _Operation) -> _Operation:
    """
    Decodes the string ``value`` encoded with Base16 algorithm in accordance to RFC 4648.

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **BASE16_DECODE** OneTick function
    | :meth:`base16_encode`

    Examples
    --------

    Decoding string literal:

    >>> data = otp.Tick(A=otp.ops.base16_decode('68656C6C6F'))
    >>> otp.run(data)
            Time      A
    0 2003-12-01  hello

    Decoding each string in a column:

    >>> data = otp.Ticks(A=['68656C6C6F', '776F726C64'])
    >>> data['X'] = otp.ops.base16_decode(data['A'])
    >>> otp.run(data)
                         Time           A      X
    0 2003-12-01 00:00:00.000  68656C6C6F  hello
    1 2003-12-01 00:00:00.001  776F726C64  world
    """
    # see OTSS-17333 for RTRIM justification
    return _Operation(
        op_func=lambda value: (f'BASE16_DECODE(RTRIM({ott.value2str(value)}))', str),
        op_params=[value],
    )


def name_value_pair_add(name: str | _Operation,
                        value: str | _Operation,
                        /,
                        pairs: str | _Operation | None = None) -> _Operation:
    """
    Add ``name=value`` pair to a comma-separated list of ``pairs``.

    Parameters
    ----------
    name: str, :class:`~onetick.py.Operation`
    value: str, :class:`~onetick.py.Operation`
    pairs: str, :class:`~onetick.py.Operation`
        If not set, then new string is created.

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **NAME_VALUE_PAIR_ADD** OneTick function
    | :meth:`name_value_pair_modify`
    | :meth:`name_value_pair_remove`

    Examples
    --------

    Creating a list of pairs and adding a new pair:

    >>> data = otp.Tick(A=otp.ops.name_value_pair_add('a', '1'))
    >>> data['B'] = otp.ops.name_value_pair_add('b', '2', data['A'])
    >>> otp.run(data)
            Time    A        B
    0 2003-12-01  a=1  a=1,b=2
    """
    def fun(pairs, name, value):
        pairs = ott.value2str(pairs if pairs is not None else '')
        return f'NAME_VALUE_PAIR_ADD({pairs},{ott.value2str(name)},{ott.value2str(value)})', str

    return _Operation(
        op_func=fun,
        op_params=[pairs, name, value],
    )


def name_value_pair_modify(name: str | _Operation,
                           value: str | _Operation,
                           /,
                           pairs: str | _Operation) -> _Operation:
    """
    Modify ``name=value`` pair in a comma-separated list of ``pairs``.

    Parameters
    ----------
    name: str, :class:`~onetick.py.Operation`
    value: str, :class:`~onetick.py.Operation`
    pairs: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **NAME_VALUE_PAIR_MODIFY** OneTick function
    | :meth:`name_value_pair_add`
    | :meth:`name_value_pair_remove`

    Examples
    --------

    Modifying the pair in a list of pairs:

    >>> data = otp.Tick(A='a=1,b=2')
    >>> data['B'] = otp.ops.name_value_pair_modify('a', '2', data['A'])
    >>> otp.run(data)
            Time        A        B
    0 2003-12-01  a=1,b=2  a=2,b=2
    """
    return _Operation(
        op_func=lambda pairs, name, value: (
            f'NAME_VALUE_PAIR_MODIFY({ott.value2str(pairs)},{ott.value2str(name)},{ott.value2str(value)})',
            str
        ),
        op_params=[pairs, name, value],
    )


def name_value_pair_remove(name: str | _Operation,
                           /,
                           pairs: str | _Operation) -> _Operation:
    """
    Remove ``name=value`` pair from a comma-separated list of ``pairs``.

    Parameters
    ----------
    name: str, :class:`~onetick.py.Operation`
    pairs: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **NAME_VALUE_PAIR_REMOVE** OneTick function
    | :meth:`name_value_pair_add`
    | :meth:`name_value_pair_modify`

    Examples
    --------

    Removing the pair from a list of pairs:

    >>> data = otp.Tick(A='a=1,b=2')
    >>> data['B'] = otp.ops.name_value_pair_remove('a', data['A'])
    >>> otp.run(data)
            Time        A    B
    0 2003-12-01  a=1,b=2  b=2
    """
    return _Operation(
        op_func=lambda pairs, name: (f'NAME_VALUE_PAIR_REMOVE({ott.value2str(pairs)},{ott.value2str(name)})', str),
        op_params=[pairs, name],
    )


def str_to_utf8(value: str | _Operation, encoding: str | _Operation) -> _Operation:
    """
    Returns the specified string ``value`` re-encoded from ``encoding`` to UTF-8.

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`
    encoding: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **STR_TO_UTF8** OneTick function
    | :meth:`utf8_to_str`
    | :meth:`~onetick.py.Source.encoding_converter`

    Examples
    --------

    Encoding string literal:

    >>> data = otp.Tick(A=otp.ops.str_to_utf8('hello', encoding='ascii'))
    >>> otp.run(data)
            Time      A
    0 2003-12-01  hello

    Encoding each string in a column:

    >>> data = otp.Ticks(A=['hello', 'world'])
    >>> data['X'] = otp.ops.str_to_utf8(data['A'], encoding='ascii')
    >>> otp.run(data)
                         Time      A      X
    0 2003-12-01 00:00:00.000  hello  hello
    1 2003-12-01 00:00:00.001  world  world
    """
    return _Operation(
        op_func=lambda value, encoding: (f'STR_TO_UTF8({ott.value2str(encoding)},{ott.value2str(value)})', str),
        op_params=[value, encoding],
    )


def utf8_to_str(value: str | _Operation, encoding: str | _Operation) -> _Operation:
    """
    Returns the specified string ``value`` re-encoded from UTF-8 to ``encoding``.

    Parameters
    ----------
    value: str, :class:`~onetick.py.Operation`
    encoding: str, :class:`~onetick.py.Operation`

    Returns
    -------
    :class:`~onetick.py.Operation`

    See Also
    --------
    | **UTF8_TO_STR** OneTick function
    | :meth:`str_to_utf8`
    | :meth:`~onetick.py.Source.encoding_converter`

    Examples
    --------

    Encoding string literal:

    >>> data = otp.Tick(A=otp.ops.utf8_to_str('español', encoding='ascii'))
    >>> otp.run(data, encoding='utf-8')
            Time        A
    0 2003-12-01  espa\x1aol

    Encoding each string in a column:

    >>> data = otp.Ticks(A=['hello', 'español'])
    >>> data['X'] = otp.ops.utf8_to_str(data['A'], encoding='ascii')
    >>> otp.run(data, encoding='utf-8')
                         Time        A        X
    0 2003-12-01 00:00:00.000    hello    hello
    1 2003-12-01 00:00:00.001  español  espa\x1aol
    """
    return _Operation(
        op_func=lambda value, encoding: (f'UTF8_TO_STR({ott.value2str(encoding)},{ott.value2str(value)})', str),
        op_params=[value, encoding],
    )
