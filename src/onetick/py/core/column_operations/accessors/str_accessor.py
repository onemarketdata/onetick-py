from typing import Optional

from onetick.py import types as ott
from onetick.py import configuration, utils
from onetick.py.core.column_operations.accessors._accessor import _Accessor
from onetick.py.core.column_operations.base import _Operation
from onetick.py.backports import Literal
from onetick.py.docs.utils import alias
from onetick.py.compatibility import is_ilike_supported


def _get_onetick_bool_string(value: bool) -> str:
    if value:
        return '"true"'
    return '"false"'


class _StrAccessor(_Accessor):
    """ Accessor for string functions

    >>> data = otp.Ticks(X=['some string'])
    >>> data["Y"] = data["X"].str.<function_name>() # doctest: +SKIP
    """

    def to_datetime(self,
                    format='%Y/%m/%d %H:%M:%S.%J',
                    timezone=None,
                    unit: Optional[Literal['ms', 'ns']] = None):
        """
        Converts the formatted time to the number of nanoseconds (datetime) since 1970/01/01 GMT.

        Parameters
        ----------
        format: str, Operation, Column
            The format might contain any characters, but the following combinations of
            characters have special meanings

            %Y - Year (4 digits)

            %y - Year (2 digits)

            %m - Month (2 digits)

            %d - Day of month (2 digits)

            %H - Hours (2 digits, 24-hour format)

            %I - Hours (2 digits, 12-hour format)

            %M - Minutes (2 digits)

            %S - Seconds (2 digits)

            %J - Nanoseconds (9 digits)

            %p - AM/PM (2 characters)

        timezone: str | Operation | Column
            Timezone. The timezone of the query will be used if no ``timezone`` was passed.

        unit: str, optional
            If set, `format` and `timezone` are ignored.
            If equals to `ns`, constructs a nanosecond-granularity timestamp from a millisecond-granularity
            string. It has the following format: < milliseconds since 1970/01/01 GMT >.< fraction of a millisecond >.
            The fraction might have at most six digits. If the fraction is equal to zero,
            .< fraction of a millisecond > is optional.
            If equals to `ms`, constructs a millisecond-granularity timestamp from a millisecond-granularity
            string. It has the following format: < milliseconds since 1970/01/01 GMT >.

        Returns
        -------
        Operation
            :py:class:`nsectime <onetick.py.types.nsectime>` Operation obtained from the string

        Examples
        --------
        >>> # OTdirective: snippet-name: string.to timestamp;
        >>> data = otp.Tick(X='5/17/22-11:10:56.123456789')
        >>> data['Y'] = data['X'].str.to_datetime('%m/%d/%y-%H:%M:%S.%J', 'Europe/London')
        >>> otp.run(data)
                Time                           X                             Y
        0 2003-12-01  5/17/22-11:10:56.123456789 2022-05-17 06:10:56.123456789

        >>> data = otp.Ticks(A=['1693825877111.002001', '1693825877112'])
        >>> data['NSECTIME_A'] = data['A'].str.to_datetime(unit='ns')
        >>> otp.run(data)
                             Time                     A                    NSECTIME_A
        0 2003-12-01 00:00:00.000  1693825877111.002001 2023-09-04 07:11:17.111002001
        1 2003-12-01 00:00:00.001         1693825877112 2023-09-04 07:11:17.112000000

        >>> data = otp.Tick(A='1693825877111')
        >>> data['MSECTIME_A'] = data['A'].str.to_datetime(unit='ms')
        >>> otp.run(data)
                Time              A              MSECTIME_A
        0 2003-12-01  1693825877111 2023-09-04 07:11:17.111
        """
        if unit is None:
            if timezone is utils.default:
                timezone = configuration.config.tz

            def formatter(column, fmt, tz):
                column_str = ott.value2str(column)
                tz_str, format_str = self._preprocess_tz_and_format(tz, fmt)
                format_str = format_str.replace('%f', '%J')
                return f'parse_nsectime({format_str},{column_str},{tz_str})'

            return _StrAccessor.Formatter(
                op_params=[self._base_column, format, timezone],
                dtype=ott.nsectime,
                formatter=formatter,
            )
        else:
            if unit == 'ns':
                return _StrAccessor.Formatter(
                    op_params=[self._base_column],
                    dtype=ott.nsectime,
                    formatter=lambda column: f'MSEC_STR_TO_NSECTIME({ott.value2str(column)})',
                )
            if unit == 'ms':
                return _StrAccessor.Formatter(
                    op_params=[self._base_column],
                    dtype=ott.msectime,
                    formatter=lambda column: f'GET_MSECS(MSEC_STR_TO_NSECTIME({ott.value2str(column)}))',
                )
            raise ValueError(f'`{unit}` is unsupported value for `unit` parameter')

    strptime = alias(to_datetime,
                     doc_replacer=lambda doc: doc.replace('to_datetime', 'strptime'))

    def token(self, sep=" ", n=0):
        """
        Breaks the value into tokens based on the delimiter ``sep``
        and returns token at position ``n`` (zero-based).

        If there are not enough tokens to get the one at position ``n``, then empty string is returned.

        Parameters
        ----------
        sep: str or Column or Operation
            The delimiter, which must be a single character used to split the string into tokens.
        n: int, Operation
            Token index to return. For a negative ``n``, count from the end instead of the beginning.
            If index is out of range, then empty string is returned.

        Returns
        -------
        Operation
            token at position ``n`` or empty string.

        Examples
        -------
        >>> # OTdirective: snippet-name: string.token;
        >>> data = otp.Tick(X='US_COMP::TRD')
        >>> data['Y'] = data['X'].str.token(':', -1)
        >>> otp.run(data)
                Time              X    Y
        0 2003-12-01  US_COMP::TRD  TRD

        Other columns can be used as parameters too:

        >>> data = otp.Tick(X='US_COMP::TRD', SEP=':', N=-1)
        >>> data['Y'] = data['X'].str.token(data['SEP'], data['N'])
        >>> otp.run(data)
                Time              X SEP  N    Y
        0 2003-12-01  US_COMP::TRD   : -1  TRD

        If index is out of range, then empty string is returned:

        >>> data = otp.Tick(X='US_COMP::TRD')
        >>> data['Y'] = data['X'].str.token(':', 999)
        >>> otp.run(data)
                Time              X  Y
        0 2003-12-01  US_COMP::TRD
        """
        if isinstance(sep, str) and len(sep) != 1:
            raise ValueError("Function '.str.token()' expects parameter 'sep' to be a single character")
        return _StrAccessor.Formatter(
            op_params=[self._base_column, sep, n],
            dtype=self._base_column.dtype,
            formatter=lambda column, sep, n: f'token({ott.value2str(column)},{ott.value2str(n)},{ott.value2str(sep)})'
        )

    def match(self, pat, case=True):
        r"""
        Match the text against a regular expression specified in the ``pat`` parameter.

        Parameters
        ----------
        pat: str or Column or Operation
            A pattern specified via the POSIX extended regular expression syntax.
        case: bool
            If ``True``, then regular expression is case-sensitive.

        Returns
        -------
        Operation
            ``True`` if the match was successful, ``False`` otherwise.
            Note that boolean Operation is converted to float if added as a column.

        Examples
        --------
        >>> # OTdirective: snippet-name: string.match;
        >>> data = otp.Ticks(X=['hello', 'there were 77 ticks'])
        >>> data['Y'] = data['X'].str.match(r'\d\d')
        >>> otp.run(data)
                             Time                    X    Y
        0 2003-12-01 00:00:00.000                hello  0.0
        1 2003-12-01 00:00:00.001  there were 77 ticks  1.0

        Other columns can be used as parameter ``pat`` too:

        >>> data = otp.Tick(X='OneTick', PAT='onetick')
        >>> data['Y'] = data['X'].str.match(data['PAT'], case=False)
        >>> otp.run(data)
                Time        X      PAT    Y
        0 2003-12-01  OneTick  onetick  1.0

        ``match`` function can also be used as a filter.
        For example, to filter on-exchange continuous trading trades:

        >>> q = otp.DataSource('US_COMP', tick_type='TRD', symbols=['SPY'])  # doctest: +SKIP
        >>> q = q[['PRICE', 'SIZE', 'COND', 'EXCHANGE']]  # doctest: +SKIP
        >>> q = q.where(q['COND'].str.match('^[^O6TUHILNRWZ47QMBCGPV]*$'))  # doctest: +SKIP
        >>> otp.run(q, start=otp.dt(2023, 5, 15, 9, 30), end=otp.dt(2023, 5, 15, 9, 30, 1))  # doctest: +SKIP
                                    Time    PRICE  SIZE  COND EXCHANGE
        0  2023-05-15 09:30:00.000776704  412.220   247              Z
        1  2023-05-15 09:30:00.019069440  412.230   100   F          K
        ..                           ...      ...   ...   ...      ...
        """
        caseless = _get_onetick_bool_string(not case)
        return _StrAccessor.Formatter(
            op_params=[self._base_column, pat],
            dtype=bool,
            formatter=lambda column, pat: f'regex_match({ott.value2str(column)},{ott.value2str(pat)},{caseless})',
        )

    def len(self):
        """
        Get the length of a string.

        Returns
        -------
        Operation
            The length of the string.
            If a null-character (byte with value ``0``) is present in the string,
            its position (0-based) is returned.

        Examples
        --------
        >>> # OTdirective: snippet-name: string.len;
        >>> data = otp.Ticks(X=['hello', 'world!'])
        >>> data['LEN'] = data['X'].str.len()
        >>> otp.run(data)
                             Time       X  LEN
        0 2003-12-01 00:00:00.000   hello    5
        1 2003-12-01 00:00:00.001  world!    6
        """
        return _StrAccessor.Formatter(op_params=[self._base_column],
                                      dtype=int,
                                      formatter=lambda column: f'strlen({ott.value2str(column)})')

    def contains(self, substr):
        """
        Check if the string contains ``substr``.

        Note
        ----
        This function does not support regular expressions.
        Use :func:`match` for this purpose.

        Parameters
        ----------
        substr: str or Column or Operation
            A substring to search for within the string.

        Returns
        -------
        Operation
            ``True`` if the string contains the substring, ``False`` otherwise.
            Note that boolean Operation is converted to float if added as a column.

        Examples
        --------
        >>> # OTdirective: snippet-name: string.contains;
        >>> data = otp.Ticks(X=['hello', 'world!'])
        >>> data['CONTAINS'] = data['X'].str.contains('hel')
        >>> otp.run(data)
                             Time       X  CONTAINS
        0 2003-12-01 00:00:00.000   hello       1.0
        1 2003-12-01 00:00:00.001  world!       0.0

        Other columns can be used as parameter ``substr`` too:

        >>>  # OTdirective: snippet-name: string.contains another field;
        >>> data = otp.Ticks(X=['hello', 'big', 'world!'],
        ...                  Y=['hel', 'wor', 'wor'])
        >>> data['CONTAINS'] = data['X'].str.contains(data['Y'])
        >>> otp.run(data)
                             Time       X    Y  CONTAINS
        0 2003-12-01 00:00:00.000   hello  hel       1.0
        1 2003-12-01 00:00:00.001     big  wor       0.0
        2 2003-12-01 00:00:00.002  world!  wor       1.0

        This method can also be used for filtering:

        >>>  # OTdirective: snippet-name: string.contains as a filter;
        >>> data = otp.Ticks(X=['Hello', 'World'])
        >>> with_substr, wo_substr = data[data['X'].str.contains('Hel')]
        >>> otp.run(with_substr)
                Time      X
        0 2003-12-01  Hello
        """
        return _StrAccessor.Formatter(
            op_params=[self._base_column, substr],
            dtype=bool,
            formatter=lambda column, substr: f'instr({ott.value2str(column)}, {ott.value2str(substr)}) > -1',
        )

    def trim(self):
        """
        Removes white spaces from both sides of the string.

        Returns
        -------
        Operation
            Trimmed string

        See Also
        --------
        :meth:`ltrim`, :meth:`rtrim`

        Examples
        --------
        >>> # OTdirective: snippet-name: string.trim;
        >>> data = otp.Ticks(X=['  Hello', 'World  '])
        >>> data['X'] = data['X'].str.trim()
        >>> otp.run(data)
                             Time      X
        0 2003-12-01 00:00:00.000  Hello
        1 2003-12-01 00:00:00.001  World
        """
        return _StrAccessor.Formatter(op_params=[self._base_column],
                                      dtype=self._base_column.dtype,
                                      formatter=lambda column: f'trim({ott.value2str(column)})')

    def ltrim(self):
        """
        Removes the leading white spaces from a string.

        Returns
        -------
        Operation
            Trimmed string

        See Also
        --------
        :meth:`trim`, :meth:`rtrim`
        """
        return _StrAccessor.Formatter(op_params=[self._base_column],
                                      dtype=self._base_column.dtype,
                                      formatter=lambda column: f'ltrim({ott.value2str(column)})')

    def rtrim(self):
        """
        Removes the trailing white spaces from a string.

        Returns
        -------
        Operation
            Trimmed string

        See Also
        --------
        :meth:`ltrim`, :meth:`trim`
        """
        return _StrAccessor.Formatter(op_params=[self._base_column],
                                      dtype=self._base_column.dtype,
                                      formatter=lambda column: f'rtrim({ott.value2str(column)})')

    def lower(self):
        """
        Convert a string to lower case.

        Returns
        -------
        Operation
            Lowercased string

        Examples
        --------
        >>> # OTdirective: snippet-name: string.lower;
        >>> data = otp.Ticks(X=['HeLlO', 'wOrLd!'])
        >>> data['LOW'] = data['X'].str.lower()
        >>> otp.run(data)
                             Time       X     LOW
        0 2003-12-01 00:00:00.000   HeLlO   hello
        1 2003-12-01 00:00:00.001  wOrLd!  world!
        """
        return _StrAccessor.Formatter(op_params=[self._base_column],
                                      dtype=self._base_column.dtype,
                                      formatter=lambda column: f'lower({ott.value2str(column)})')

    def upper(self):
        """
        Converts a string to upper case.

        Returns
        -------
        Operation
            Uppercased string

        Examples
        --------
        >>> # OTdirective: snippet-name: string.upper;
        >>> data = otp.Ticks(X=['HeLlO', 'wOrLd!'])
        >>> data['UP'] = data['X'].str.upper()
        >>> otp.run(data)
                             Time       X      UP
        0 2003-12-01 00:00:00.000   HeLlO   HELLO
        1 2003-12-01 00:00:00.001  wOrLd!  WORLD!
        """
        return _StrAccessor.Formatter(op_params=[self._base_column],
                                      dtype=self._base_column.dtype,
                                      formatter=lambda column: f'upper({ott.value2str(column)})')

    def replace(self, pat, repl):
        """
        Search for occurrences (case dependent) of ``pat`` and replace with ``repl``.

        Parameters
        ----------
        pat: str or Column or Operation
            Pattern to replace.
        repl: str or Column or Operation
            Replacement string.

        Returns
        -------
        Operation
            String with ``pat`` replaced by ``repl``.

        Examples
        --------
        >>> # OTdirective: snippet-name: string.replace;
        >>> data = otp.Ticks(X=['A Table', 'A Chair', 'An Apple'])
        >>> data['Y'] = data['X'].str.replace('A', 'The')
        >>> otp.run(data)
                             Time         X             Y
        0 2003-12-01 00:00:00.000   A Table     The Table
        1 2003-12-01 00:00:00.001   A Chair     The Chair
        2 2003-12-01 00:00:00.002  An Apple  Then Thepple

        Other columns can be used as parameters too:

        >>> # OTdirective: snippet-name: string.replace from field;
        >>> data = otp.Ticks(X=['A Table', 'A Chair', 'An Apple'],
        ...                  PAT=['A', 'A', 'An'],
        ...                  REPL=['The', 'Their', 'My'])
        >>> data['Y'] = data['X'].str.replace(data['PAT'], data['REPL'])
        >>> otp.run(data)
                             Time         X PAT   REPL            Y
        0 2003-12-01 00:00:00.000   A Table   A    The    The Table
        1 2003-12-01 00:00:00.001   A Chair   A  Their  Their Chair
        2 2003-12-01 00:00:00.002  An Apple  An     My     My Apple
        """
        # see, BDS-112
        if not isinstance(pat, str):
            pat = pat.str.rtrim()
        if not isinstance(repl, str):
            repl = repl.str.rtrim()
        return _StrAccessor.Formatter(
            op_params=[self._base_column, pat, repl],
            dtype=self._base_column.dtype,
            formatter=(
                lambda column, pat, repl:
                f'replace({ott.value2str(column)}, {ott.value2str(pat)}, {ott.value2str(repl)})'
            ),
        )

    def regex_replace(self, pat, repl, *, replace_every=False, caseless=False):
        r"""
        Search for occurrences (case dependent) of ``pat`` and replace with ``repl``.

        Parameters
        ----------
        pat: str or Column or Operation
            Pattern to replace specified via the POSIX extended regular expression syntax.
        repl: str or Column or Operation
            Replacement string. ``\0`` refers to the entire matched text. ``\1`` to ``\9`` refer
            to the text matched by the corresponding parenthesized group in ``pat``.
        replace_every: bool
            If ``replace_every`` flag is set to ``True``, all matches will be replaced, if ``False`` only the first one.
        caseless: bool
            If the ``caseless`` flag is set to ``True``, matching is case-insensitive.

        Returns
        -------
        Operation
            String with pattern ``pat`` replaced by ``repl``.

        See Also
        --------
        :meth:`extract`

        Examples
        --------
        >>> # OTdirective: snippet-name: string.regex replace;
        >>> data = otp.Ticks(X=['A Table', 'A Chair', 'An Apple'])
        >>> data['Y'] = data['X'].str.regex_replace('An? ', 'The ')
        >>> otp.run(data)
                             Time         X          Y
        0 2003-12-01 00:00:00.000   A Table  The Table
        1 2003-12-01 00:00:00.001   A Chair  The Chair
        2 2003-12-01 00:00:00.002  An Apple  The Apple

        Parameter ``replace_every`` will replace all occurrences of ``pat`` in the string:

        >>> # OTdirective: snippet-name: string.regex replace all;
        >>> data = otp.Ticks(X=['A Table, A Chair, An Apple'])
        >>> data['Y'] = data['X'].str.regex_replace('An? ', 'The ', replace_every=True)
        >>> otp.run(data)
                Time                           X                                Y
        0 2003-12-01  A Table, A Chair, An Apple  The Table, The Chair, The Apple

        Capturing groups in regular expressions is supported:

        >>> # OTdirective: snippet-name: string.regex groups;
        >>> data = otp.Ticks(X=['11/12/1992', '9/22/1993', '3/30/1991'])
        >>> data['Y'] = data['X'].str.regex_replace(r'(\d{1,2})/(\d{1,2})/', r'\2.\1.')
        >>> otp.run(data)
                             Time           X           Y
        0 2003-12-01 00:00:00.000  11/12/1992  12.11.1992
        1 2003-12-01 00:00:00.001   9/22/1993   22.9.1993
        2 2003-12-01 00:00:00.002   3/30/1991   30.3.1991
        """
        replace_every = _get_onetick_bool_string(replace_every)
        caseless = _get_onetick_bool_string(caseless)
        return _StrAccessor.Formatter(
            op_params=[self._base_column, pat, repl],
            dtype=self._base_column.dtype,
            formatter=lambda column, pat, repl: f'regex_replace({ott.value2str(column)}, {ott.value2str(pat)},'
                                                f' {ott.value2str(repl)}, {replace_every}, {caseless})',
        )

    def find(self, sub, start=0):
        """
        Find the index of ``sub`` in the string. If not found, returns ``-1``.

        Parameters
        ----------
        sub: str or Column or Operation
            Substring to find.
        start: int or Column or Operation
            Starting position to find.

        Returns
        -------
        Operation
            The starting position of the substring or ``-1`` if it is not found.

        Examples
        --------
        >>> data = otp.Ticks(X=['ananas', 'banana', 'potato'])
        >>> data['Y'] = data['X'].str.find('ana') # OTdirective: snippet-name: string.find;
        >>> otp.run(data)
                             Time       X  Y
        0 2003-12-01 00:00:00.000  ananas  0
        1 2003-12-01 00:00:00.001  banana  1
        2 2003-12-01 00:00:00.002  potato -1

        Other columns can be used as parameter ``sub`` too:

        >>> # OTdirective: snippet-name: string.find field value;
        >>> data = otp.Ticks(X=['Ananas', 'Banana', 'Potato'], sub=['Ana', 'anan', 'ato'])
        >>> data['Y'] = data['X'].str.find(data['sub'])
        >>> otp.run(data)
                             Time       X   sub  Y
        0 2003-12-01 00:00:00.000  Ananas   Ana  0
        1 2003-12-01 00:00:00.001  Banana  anan  1
        2 2003-12-01 00:00:00.002  Potato   ato  3

        Note that empty string will be found at the start of any string:

        >>> data = otp.Ticks(X=['string', ''])
        >>> data['Y'] = data['X'].str.find('')
        >>> otp.run(data)
                             Time       X  Y
        0 2003-12-01 00:00:00.000  string  0
        1 2003-12-01 00:00:00.001          0

        ``start`` parameter is used to find ``sub`` starting from selected position:

        >>> data = otp.Ticks(X=['ababab', 'abbbbb'])
        >>> data['Y'] = data['X'].str.find('ab', 1)
        >>> otp.run(data)
                             Time       X  Y
        0 2003-12-01 00:00:00.000  ababab  2
        1 2003-12-01 00:00:00.001  abbbbb -1
        """
        return _StrAccessor.Formatter(
            op_params=[self._base_column, sub, start],
            dtype=int,
            formatter=(
                lambda column, sub, start:
                f'LOCATE({ott.value2str(sub)}, {ott.value2str(column)}, {ott.value2str(start + 1)})-1'
            ),
        )

    def repeat(self, repeats):
        """
        Duplicate a string ``repeats`` times.

        Note
        ----
        * Alternative for the ``repeat`` function is multiplication.
        * The returned string has the same type and maximum length as the original field.

        Parameters
        ----------
        repeats: int or Column or Operation
            Non-negative number of copies of the string.
            Repeating zero times results in empty string.
            Repeating negative number of times results in exception.

        Returns
        -------
        Operation
            String repeated ``repeats`` times.

        Examples
        --------
        >>> # OTdirective: snippet-name: string.repeat;
        >>> data = otp.Ticks(X=['Banana', 'Ananas', 'Apple'])
        >>> data['X'] = data['X'].str.repeat(3)
        >>> otp.run(data)
                             Time                   X
        0 2003-12-01 00:00:00.000  BananaBananaBanana
        1 2003-12-01 00:00:00.001  AnanasAnanasAnanas
        2 2003-12-01 00:00:00.002     AppleAppleApple

        Other columns can be used as parameter ``repeats`` too:

        # OTdirective: snippet-name: string.repeat from a field;
        >>> data = otp.Ticks(X=['Banana', 'Ananas', 'Apple'], TIMES=[1, 3, 2])
        >>> data['Y'] = data['X'].str.repeat(data['TIMES'])
        >>> otp.run(data)
                             Time       X  TIMES                   Y
        0 2003-12-01 00:00:00.000  Banana      1              Banana
        1 2003-12-01 00:00:00.001  Ananas      3  AnanasAnanasAnanas
        2 2003-12-01 00:00:00.002   Apple      2          AppleApple

        The returned string has the same type and therefore the same maximum length as the original field:

        >>> data = otp.Ticks(X=[otp.string[9]('Banana')])
        >>> data['Y'] = data['X'].str.repeat(3)
        >>> data.schema
        {'X': string[9], 'Y': string[9]}
        >>> otp.run(data)
                Time       X          Y
        0 2003-12-01  Banana  BananaBan

        ``repeat`` does the same thing as multiplication by a non-negative int:

        >>> # OTdirective: snippet-name: string.repeat by multiplication;
        >>> data = otp.Ticks(X=['Banana'], N=[2])
        >>> data['X2'] = data['X'] * data['N']
        >>> data['X3'] = data['X'] * 3
        >>> otp.run(data)
                Time       X  N            X2                  X3
        0 2003-12-01  Banana  2  BananaBanana  BananaBananaBanana

        Multiplying by 0 results in empty string:

        >>> data = otp.Ticks(X=['Banana', 'Apple'])
        >>> data['Y'] = data['X'].str.repeat(0)
        >>> otp.run(data)
                             Time       X Y
        0 2003-12-01 00:00:00.000  Banana
        1 2003-12-01 00:00:00.001   Apple
        """
        return _StrAccessor.Formatter(
            op_params=[self._base_column, repeats],
            dtype=self._base_column.dtype,
            formatter=lambda column, repeats: f'repeat({ott.value2str(column)}, {ott.value2str(repeats)})',
        )

    def extract(self, pat, rewrite=r"\0", caseless=False):
        r"""
        Match the string against a regular expression specified by ``pat`` and return the first match.
        The ``rewrite`` parameter can optionally be used to arrange the matched substrings and embed them within the
        string specified in ``rewrite``.

        Parameters
        ----------
        pat: str or Column or Operation
            Pattern to search for specified via the POSIX extended regular expression syntax.
        rewrite: str or Column or Operation
            A string that specifies how to arrange the matched text. ``\0`` refers to the entire matched text.
            ``\1`` to ``\9`` refer to the text matched by the corresponding parenthesized group in ``pat``.
            ``\u`` and ``\l`` modifiers within the ``rewrite`` string convert the case of the text that
            matches the corresponding parenthesized group (e.g., ``\u1`` converts ``\1`` to uppercase).
        caseless: bool
            If the ``caseless`` flag is set to ``True``, matching is case-insensitive.

        Returns
        -------
        Operation
            String matched by ``pat`` with format specified in ``rewrite``.

        See Also
        --------
        regex_replace

        Examples
        --------
        >>> # OTdirective: snippet-name: string.regex extract;
        >>> data = otp.Ticks(X=['Mr. Smith: +1348 +4781', 'Ms. Smith: +8971'])
        >>> data['TEL'] = data['X'].str.extract(r'\+\d{4}')
        >>> otp.run(data)
                             Time                       X    TEL
        0 2003-12-01 00:00:00.000  Mr. Smith: +1348 +4781  +1348
        1 2003-12-01 00:00:00.001        Ms. Smith: +8971  +8971

        You can specify the group to extract in the ``rewrite`` parameter:

        >>> # OTdirective: snippet-name: string.regex extract group;
        >>> data = otp.Ticks(X=['Mr. Smith: 1992/12/22', 'Ms. Smith: 1989/10/15'])
        >>> data['BIRTH_YEAR'] = data['X'].str.extract(r'(\d{4})/(\d{2})/(\d{2})', rewrite=r'birth year: \1')
        >>> otp.run(data)
                             Time                      X        BIRTH_YEAR
        0 2003-12-01 00:00:00.000  Mr. Smith: 1992/12/22  birth year: 1992
        1 2003-12-01 00:00:00.001  Ms. Smith: 1989/10/15  birth year: 1989

        You can use a column as a ``rewrite`` or ``pat`` parameter:

        >>> # OTdirective: snippet-name: string.regex extract from field;
        >>> data = otp.Ticks(X=['Kelly, Mr. James', 'Wilkes, Mrs. James', 'Connolly, Miss. Kate'],
        ...                  PAT=['(Mrs?)\\.', '(Mrs?)\\.', '(Miss)\\.'],
        ...                  REWRITE=['Title 1: \\1', 'Title 2: \\1', 'Title 3: \\1'])
        >>> data['TITLE'] = data['X'].str.extract(data['PAT'], rewrite=data['REWRITE'])
        >>> otp.run(data)
                             Time                     X       PAT      REWRITE          TITLE
        0 2003-12-01 00:00:00.000      Kelly, Mr. James  (Mrs?)\.  Title 1: \1  Title 1:   Mr
        1 2003-12-01 00:00:00.001    Wilkes, Mrs. James  (Mrs?)\.  Title 2: \1  Title 2:  Mrs
        2 2003-12-01 00:00:00.002  Connolly, Miss. Kate  (Miss)\.  Title 3: \1  Title 3: Miss

        Case of the extracted string can be changed by adding ``l`` and ``u`` to extract group:

        >>> # OTdirective: snippet-name: string.regex extract caseless;
        >>> data = otp.Ticks(NAME=['mr. BroWn', 'Ms. smITh'])
        >>> data['RESULT'] = data['NAME'].str.extract(r'(m)([rs]\. )([a-z])([a-z]*)', r'\u1\l2\u3\l4', caseless=True)
        >>> otp.run(data)
                             Time       NAME     RESULT
        0 2003-12-01 00:00:00.000  mr. BroWn  Mr. Brown
        1 2003-12-01 00:00:00.001  Ms. smITh  Ms. Smith
        """
        caseless = _get_onetick_bool_string(caseless)
        return _StrAccessor.Formatter(
            op_params=[self._base_column, pat, rewrite],
            dtype=self._base_column.dtype,
            formatter=(
                lambda column, pat, rewrite:
                f'regex_extract({ott.value2str(column)}, {ott.value2str(pat)}, {ott.value2str(rewrite)}, {caseless})'
            ),
        )

    def substr(self, start, n_bytes=None, rtrim=False):
        """
        Return ``n_bytes`` characters starting from ``start``.

        For a positive ``start`` return ``num_bytes`` of the string, starting from the position specified by
        ``start`` (0-based).
        For a negative ``start``, the position is counted from the end of the string.
        If the ``n_bytes`` parameter is omitted, returns the part of the input string
        starting at ``start`` till the end of the string.

        Parameters
        ----------
        start: int or Column or Operation
            Index of first symbol in substring
        n_bytes: int or Column or Operation
            Number of bytes in substring
        rtrim: bool
            If set to ``True``, original string will be trimmed from the right side
            before getting the substring, this can be useful with negative ``start`` index.

        Returns
        -------
        Operation
            Substring of string (``n_bytes`` length starting with ``start``).

        Examples
        --------
        >>> # OTdirective: snippet-name: string.substring;
        >>> data = otp.Ticks(X=['abcdef', '12345     '], START_INDEX=[2, 1], N=[2, 3])
        >>> data['FIRST_3'] = data['X'].str.substr(0, 3)
        >>> data['LAST_3'] = data['X'].str.substr(-3, rtrim=True)
        >>> data['CENTER'] = data['X'].str.substr(data['START_INDEX'], data['N'])
        >>> otp.run(data)
                             Time       X  START_INDEX  N FIRST_3 LAST_3 CENTER
        0 2003-12-01 00:00:00.000  abcdef            2  2     abc    def     cd
        1 2003-12-01 00:00:00.001   12345            1  3     123    345    234
        """
        column = self._base_column
        if rtrim:
            column = column.str.rtrim()

        if n_bytes is None:
            return _StrAccessor.Formatter(
                op_params=[column, start],
                dtype=self._base_column.dtype,
                formatter=(
                    lambda column, start:
                    f'substr({ott.value2str(column)}, {ott.value2str(start)})'
                ),
            )
        else:
            return _StrAccessor.Formatter(
                op_params=[column, start, n_bytes],
                dtype=self._base_column.dtype,
                formatter=(
                    lambda column, start, n_bytes:
                    f'substr({ott.value2str(column)}, {ott.value2str(start)}, {ott.value2str(n_bytes)})'
                ),
            )

    def get(self, i):
        """
        Returns the character at the position indicated by the 0-based index; and empty string,
        if position is greater or equal to the length.

        Parameters
        ----------
        i: int or Column or Operation
            Index of the character to find.

        Examples
        --------
        >>> data = otp.Ticks(X=['abcdef', '12345     ', 'qw'], GET_INDEX=[2, 1, 0])
        >>> data['THIRD'] = data['X'].str.get(2)
        >>> data['FROM_INDEX'] = data['X'].str.get(data['GET_INDEX'])
        >>> otp.run(data)
                             Time           X  GET_INDEX THIRD FROM_INDEX
        0 2003-12-01 00:00:00.000      abcdef          2     c          c
        1 2003-12-01 00:00:00.001  12345               1     3          2
        2 2003-12-01 00:00:00.002          qw          0                q

        It is possible to use syntax with indexer to call this method:

        >>> data = otp.Ticks(X=['abcdef', '12345     ', 'qw'])
        >>> data['THIRD'] = data['X'].str[1]
        >>> otp.run(data)
                             Time           X THIRD
        0 2003-12-01 00:00:00.000      abcdef     b
        1 2003-12-01 00:00:00.001  12345          2
        2 2003-12-01 00:00:00.002          qw     w
        """
        return _StrAccessor.Formatter(
            op_params=[self._base_column, i],
            dtype=str,
            formatter=(
                lambda column, i:
                'CASE(BYTE_AT({0}, {1}),-1,"",CHAR(BYTE_AT({0}, {1})))'.format(ott.value2str(column), ott.value2str(i))
            ),
        )

    def concat(self, other):
        """
        Returns a string that is the result of concatenating to ``others``.

        Parameters
        ----------
        other: str or Column or Operation
            String to concatenate with.

        Examples
        --------
        >>> data = otp.Ticks(X=['X1', 'X2', 'X3'], Y=['Y1', 'Y2', 'Y3'])
        >>> data['X_WITH_CONST_SUFFIX'] = data['X'].str.concat('_suffix')
        >>> data['X_WTH_Y'] = data['X'].str.concat(data['Y'])
        >>> otp.run(data)
                             Time   X   Y X_WITH_CONST_SUFFIX X_WTH_Y
        0 2003-12-01 00:00:00.000  X1  Y1           X1_suffix    X1Y1
        1 2003-12-01 00:00:00.001  X2  Y2           X2_suffix    X2Y2
        2 2003-12-01 00:00:00.002  X3  Y3           X3_suffix    X3Y3
        """
        return _StrAccessor.Formatter(
            op_params=[self._base_column, other],
            dtype=self._base_column.dtype,
            formatter=lambda column, other: f'CONCAT({ott.value2str(column)}, {ott.value2str(other)})',
        )

    def insert(self, start, length, value):
        """
        Returns a string where ``length`` characters have been deleted from string,
        beginning at ``start``, and where ``value`` has been inserted into string, beginning at ``start``.

        Parameters
        ----------
        start: int or Column or Operation
            Position to remove from and to insert into.
        length: int or Column or Operation
            Number if characters to remove.
        value: str or Column or Operation
            String to insert.

        Examples
        --------
        >>> data = otp.Ticks(X=['aaaaaaa', 'bbbbb', 'cccc'], Y=['ddd', 'ee', 'f'])
        >>> data['INSERTED_1'] = data['X'].str.insert(3, 1, 'X')
        >>> data['INSERTED_2'] = data['X'].str.insert(3, 2, 'X')
        >>> data['INSERTED_Y'] = data['X'].str.insert(3, 2, data['Y'])
        >>> otp.run(data)
                             Time        X    Y INSERTED_1 INSERTED_2 INSERTED_Y
        0 2003-12-01 00:00:00.000  aaaaaaa  ddd    aaXaaaa     aaXaaa   aadddaaa
        1 2003-12-01 00:00:00.001    bbbbb   ee      bbXbb       bbXb      bbeeb
        2 2003-12-01 00:00:00.002     cccc    f       ccXc        ccX        ccf

        It is possible to insert without removal:

        >>> data = otp.Ticks(X=['aaaaaaa', 'bbbbb', 'cccc'])
        >>> data['INSERTED'] = data['X'].str.insert(3, 0, 'X')
        >>> otp.run(data)
                             Time        X  INSERTED
        0 2003-12-01 00:00:00.000  aaaaaaa  aaXaaaaa
        1 2003-12-01 00:00:00.001    bbbbb    bbXbbb
        2 2003-12-01 00:00:00.002     cccc     ccXcc

        It is possible to remove without insertion:

        >>> data = otp.Ticks(X=['aaaaaaa', 'bbbbb', 'cccc'])
        >>> data['REMOVED'] = data['X'].str.insert(3, 2, '')
        >>> otp.run(data)
                             Time        X REMOVED
        0 2003-12-01 00:00:00.000  aaaaaaa   aaaaa
        1 2003-12-01 00:00:00.001    bbbbb     bbb
        2 2003-12-01 00:00:00.002     cccc      cc
        """
        return _StrAccessor.Formatter(
            op_params=[self._base_column, start, length, value],
            dtype=self._base_column.dtype,
            formatter=(
                lambda column, start, length, value:
                f'INSERT({ott.value2str(column)}, {ott.value2str(start)},'
                f' {ott.value2str(length)}, {ott.value2str(value)})'
            ),
        )

    def first(self, count=1):
        """
        Returns first ``count`` symbols.

        Parameters
        ----------
        count: int or Column or Operation
            Number of first symbols to return. Default: 1

        Examples
        --------
        >>> data = otp.Ticks(X=['abc', 'bac', 'cba'], Y=[3, 1, 10])
        >>> data['FIRST'] = data['X'].str.first()
        >>> data['FIRST_Y'] = data['X'].str.first(data['Y'])
        >>> otp.run(data)
                             Time    X   Y FIRST FIRST_Y
        0 2003-12-01 00:00:00.000  abc   3     a     abc
        1 2003-12-01 00:00:00.001  bac   1     b       b
        2 2003-12-01 00:00:00.002  cba  10     c     cba
        """
        return _StrAccessor.Formatter(
            op_params=[self._base_column, count],
            dtype=str,
            formatter=lambda column, count: f'LEFT({ott.value2str(column)}, {ott.value2str(count)})',
        )

    def last(self, count=1):
        """
        Returns last ``count`` symbols.

        Parameters
        ----------
        count: int or Column or Operation
            Number of last symbols to return. Default: 1

        Examples
        --------
        >>> data = otp.Ticks(X=['abc', 'bac', 'cba'], Y=[3, 1, 9])
        >>> data['LAST'] = data['X'].str.last()
        >>> data['LAST_Y'] = data['X'].str.last(data['Y'])
        >>> otp.run(data)
                             Time    X  Y LAST LAST_Y
        0 2003-12-01 00:00:00.000  abc  3    c    abc
        1 2003-12-01 00:00:00.001  bac  1    c      c
        2 2003-12-01 00:00:00.002  cba  9    a    cba
        """
        # RIGHT function works strange with negative index
        # RIGHT_UTF8 works fine but it is not supported by old builds
        return _StrAccessor.Formatter(
            op_params=[self._base_column, count],
            dtype=self._base_column.dtype,
            formatter=(
                lambda column, count:
                'SUBSTR({0}, MAX(STRLEN({0})-{1}, 0))'.format(ott.value2str(column), ott.value2str(count))
            ),
        )

    def startswith(self, value):
        """
        Checks if the Operation starts with a string.

        Parameters
        ----------
        value: str or Column or Operation
            String to check if starts with it.

        Examples
        --------
        >>> data = otp.Ticks(X=['baaaa', 'bbbbb', 'cbbc'], Y=['ba', 'abb', 'c'])
        >>> data['STARTSWITH_CONST'] = data['X'].str.startswith('bb')
        >>> data['STARTSWITH_Y'] = data['X'].str.startswith(data['Y'])
        >>> otp.run(data)
                             Time      X    Y  STARTSWITH_CONST  STARTSWITH_Y
        0 2003-12-01 00:00:00.000  baaaa   ba               0.0           1.0
        1 2003-12-01 00:00:00.001  bbbbb  abb               1.0           0.0
        2 2003-12-01 00:00:00.002   cbbc    c               0.0           1.0
        """
        return _StrAccessor.Formatter(
            op_params=[self._base_column, value],
            dtype=bool,
            formatter=(
                lambda column, value:
                'LEFT({0}, STRLEN({1}))={1}'.format(ott.value2str(column), ott.value2str(value))
            ),
        )

    def endswith(self, value):
        """
        Checks if the Operation ends with a string.

        Parameters
        ----------
        value: str or Column or Operation
            String to check if starts with it.

        Examples
        --------
        >>> data = otp.Ticks(X=['baaaa', 'bbbbb', 'cbbc', 'c'], Y=['ba', 'bbb', 'c', 'cc'])
        >>> data['ENDSWITH_CONST'] = data['X'].str.endswith('bb')
        >>> data['ENDSWITH_Y'] = data['X'].str.endswith(data['Y'])
        >>> otp.run(data)
                             Time      X    Y  ENDSWITH_CONST  ENDSWITH_Y
        0 2003-12-01 00:00:00.000  baaaa   ba             0.0         0.0
        1 2003-12-01 00:00:00.001  bbbbb  bbb             1.0         1.0
        2 2003-12-01 00:00:00.002   cbbc    c             0.0         1.0
        3 2003-12-01 00:00:00.003      c   cc             0.0         0.0
        """
        # RIGHT function works strange with negative index
        # RIGHT_UTF8 works fine but it is not supported by old builds
        return _StrAccessor.Formatter(
            op_params=[self._base_column, value],
            dtype=bool,
            formatter=(
                lambda column, value:
                'SUBSTR({0}, MAX(STRLEN({0})-STRLEN({1}), 0))={1}'.format(ott.value2str(column), ott.value2str(value))
            ),
        )

    def slice(self, start=None, stop=None):
        """
        Returns slice.

        Parameters
        ----------
        start: int or Column or Operation, optional
            Start position for slice operation.
        stop: int or Column or Operation, optional
            Stop position for slice operation.

        Examples
        --------
        >>> data = otp.Ticks(X=['12345', 'abcde', 'qwerty'], START=[3, 0, 1], STOP=[4, 3, 3])
        >>> data['START_1_SLICE'] = data['X'].str.slice(start=1)
        >>> data['STOP_2_SLICE'] = data['X'].str.slice(stop=2)
        >>> data['SLICE_FROM_COLUMNS'] = data['X'].str.slice(start=data['START'], stop=data['STOP'])
        >>> otp.run(data)
                                     Time       X  START  STOP START_1_SLICE STOP_2_SLICE SLICE_FROM_COLUMNS
        0 2003-12-01 00:00:00.000   12345      3     4          2345           12                  4
        1 2003-12-01 00:00:00.001   abcde      0     3          bcde           ab                abc
        2 2003-12-01 00:00:00.002  qwerty      1     3         werty           qw                 we

        Parameters can be negative:

        >>> data = otp.Ticks(X=['12345', 'abcde', 'qwerty'])
        >>> data['START_SLICE'] = data['X'].str.slice(start=-3)
        >>> data['STOP_SLICE'] = data['X'].str.slice(stop=-1)
        >>> data['START_STOP_SLICE'] = data['X'].str.slice(start=-3, stop=-1)
        >>> otp.run(data)
                             Time       X START_SLICE STOP_SLICE START_STOP_SLICE
        0 2003-12-01 00:00:00.000   12345         345       1234               34
        1 2003-12-01 00:00:00.001   abcde         cde       abcd               cd
        2 2003-12-01 00:00:00.002  qwerty         rty      qwert               rt

        It is possible to use syntax with indexer to call this method:

        >>> data = otp.Ticks(X=['12345', 'abcde', 'qwerty'])
        >>> data['START_SLICE'] = data['X'].str[1:]
        >>> data['STOP_SLICE'] = data['X'].str[:3]
        >>> data['START_STOP_SLICE'] = data['X'].str[1:3]
        >>> otp.run(data)
                             Time       X START_SLICE STOP_SLICE START_STOP_SLICE
        0 2003-12-01 00:00:00.000   12345        2345        123               23
        1 2003-12-01 00:00:00.001   abcde        bcde        abc               bc
        2 2003-12-01 00:00:00.002  qwerty       werty        qwe               we
        """
        if start is None and stop is None:
            raise ValueError("At least one of the `start` or `stop` parameters should be set.")
        if start is None:
            def formatter(x, start, stop):
                x = ott.value2str(x)
                stop_str = ott.value2str(stop)
                len_x = f'STRLEN({x})'
                return (f'CASE({stop_str}>=0,1,'
                        f'SUBSTR({x},0,{stop_str}),'
                        f'SUBSTR({x},0,MAX(0,{len_x}+{stop_str})))')
        elif stop is None:
            def formatter(x, start, stop):
                x = ott.value2str(x)
                len_x = f'STRLEN({x})'
                # we need this workaround because simple RIGHT and SUBSTR with negative start parameter work strange
                # SUBSTR_UTF8 works fine but it is not supported by old builds
                x_corrected = f'LEFT({x},{len_x})'
                # SUBSTR returns '' when ABC(second parameter) >= STRLEN
                return f'SUBSTR({x_corrected},MAX({ott.value2str(start)},-{len_x}))'
        else:
            def formatter(x, start, stop):
                x = ott.value2str(x)
                stop_str = ott.value2str(stop)
                len_x = f'STRLEN({x})'
                # we need this workaround because simple RIGHT and SUBSTR with negative start parameter work strange
                # SUBSTR_UTF8 works fine but it is not supported by old builds
                x_corrected = f'LEFT({x},{len_x})'
                # y is x after cutting the left part (we need to cut the right part of it)
                # SUBSTR returns '' when ABC(second parameter) >= STRLEN
                y = f'SUBSTR({x_corrected},MAX({ott.value2str(start)},-{len_x}))'
                len_y = f'STRLEN({y})'
                len_cut = f'({len_x}-{len_y})'  # length of already cut part (the left one)
                stop_for_y = f'CASE({stop_str}>=0,1,{stop_str}-{len_cut},{stop_str})'
                return (f'CASE({stop_for_y}>=0,1,'
                        f'SUBSTR({y},0,{stop_for_y}),'
                        f'SUBSTR({y},0,MAX(0,{len_y}+{stop_for_y})))')
        return _StrAccessor.Formatter(op_params=[self._base_column, start, stop],
                                      dtype=self._base_column.dtype,
                                      formatter=formatter)

    def __getitem__(self, item):
        if isinstance(item, slice):
            if item.step is not None:
                raise ValueError("`step` parameter is not supported.")
            return self.slice(start=item.start, stop=item.stop)
        return self.get(item)

    def like(self, pattern):
        r"""
        Check if the value is matched with SQL-like ``pattern``.

        Parameters
        ----------
        pattern: str or symbol parameter (:py:class:`~onetick.py.core._source._symbol_param._SymbolParamColumn`)
            Pattern to match the value with.
            The pattern can contain usual text characters and two special ones:

            * ``%`` represents zero or more characters
            * ``_`` represents a single character

            Use backslash ``\`` character to escape these special characters.

        Returns
        -------
        Operation
            ``True`` if the match was successful, ``False`` otherwise.
            Note that boolean Operation is converted to float if added as a column.

        Examples
        --------

        Use ``%`` character to specify any number of characters:

        >>> data = otp.Ticks(X=['a', 'ab', 'b_', 'b%'])
        >>> data['LIKE'] = data['X'].str.like('a%')
        >>> otp.run(data)
                             Time   X  LIKE
        0 2003-12-01 00:00:00.000   a   1.0
        1 2003-12-01 00:00:00.001  ab   1.0
        2 2003-12-01 00:00:00.002  b_   0.0
        3 2003-12-01 00:00:00.003  b%   0.0

        Use ``_`` special character to specify a single character:

        >>> data = otp.Ticks(X=['a', 'ab', 'b_', 'b%'])
        >>> data['LIKE'] = data['X'].str.like('a_')
        >>> otp.run(data)
                             Time   X  LIKE
        0 2003-12-01 00:00:00.000   a   0.0
        1 2003-12-01 00:00:00.001  ab   1.0
        2 2003-12-01 00:00:00.002  b_   0.0
        3 2003-12-01 00:00:00.003  b%   0.0

        Use backslash ``\`` character to escape special characters:

        >>> data = otp.Ticks(X=['a', 'ab', 'b_', 'b%'])
        >>> data['LIKE'] = data['X'].str.like(r'b\_')
        >>> otp.run(data)
                             Time   X  LIKE
        0 2003-12-01 00:00:00.000   a   0.0
        1 2003-12-01 00:00:00.001  ab   0.0
        2 2003-12-01 00:00:00.002  b_   1.0
        3 2003-12-01 00:00:00.003  b%   0.0

        This function can be used to filter out ticks:

        >>> data = otp.Ticks(X=['a', 'ab', 'b_', 'b%'])
        >>> data = data.where(data['X'].str.like('a%'))
        >>> otp.run(data)
                             Time   X
        0 2003-12-01 00:00:00.000   a
        1 2003-12-01 00:00:00.001  ab

        ``pattern`` can only be a constant expression, like string or symbol parameter:

        >>> data = otp.Ticks(X=['a', 'ab', 'b_', 'b%'])
        >>> data['LIKE'] = data['X'].str.like(data.Symbol['PATTERN', str])
        >>> otp.run(data, symbols=otp.Tick(SYMBOL_NAME='COMMON::AAPL', PATTERN='_'))['COMMON::AAPL']
                             Time   X  LIKE
        0 2003-12-01 00:00:00.000   a   1.0
        1 2003-12-01 00:00:00.001  ab   0.0
        2 2003-12-01 00:00:00.002  b_   0.0
        3 2003-12-01 00:00:00.003  b%   0.0
        """
        from onetick.py.core._source._symbol_param import _SymbolParamColumn
        if not isinstance(pattern, (str, _SymbolParamColumn)):
            raise ValueError('like() function expects parameter to be a constant expression')
        return _StrAccessor.Formatter(
            op_params=[self._base_column, pattern],
            dtype=bool,
            formatter=lambda column, pattern: f'{ott.value2str(column)} LIKE {ott.value2str(pattern)}'
        )

    def ilike(self, pattern):
        r"""
        Check if the value is case insensitive matched with SQL-like ``pattern``.

        Parameters
        ----------
        pattern: str or symbol parameter (:py:class:`~onetick.py.core._source._symbol_param._SymbolParamColumn`)
            Pattern to match the value with.
            The pattern can contain usual text characters and two special ones:

            * ``%`` represents zero or more characters
            * ``_`` represents a single character

            Use backslash ``\`` character to escape these special characters.

        Returns
        -------
        Operation
            ``True`` if the match was successful, ``False`` otherwise.
            Note that boolean Operation is converted to float if added as a column.

        Examples
        --------

        Use ``%`` character to specify any number of characters:

        .. testcode::
           :skipif: not is_ilike_supported()

           data = otp.Ticks(X=['a', 'ab', 'Ab', 'b_'])
           data['LIKE'] = data['X'].str.ilike('a%')
           df = otp.run(data)
           print(df)

        .. testoutput::

                                Time   X  LIKE
           0 2003-12-01 00:00:00.000   a   1.0
           1 2003-12-01 00:00:00.001  ab   1.0
           2 2003-12-01 00:00:00.002  Ab   1.0
           3 2003-12-01 00:00:00.003  b_   0.0

        Use ``_`` special character to specify a single character:

        .. testcode::
           :skipif: not is_ilike_supported()

           data = otp.Ticks(X=['a', 'ab', 'Ab', 'b_'])
           data['LIKE'] = data['X'].str.ilike('a_')
           df = otp.run(data)
           print(df)

        .. testoutput::

                                Time   X  LIKE
           0 2003-12-01 00:00:00.000   a   0.0
           1 2003-12-01 00:00:00.001  ab   1.0
           2 2003-12-01 00:00:00.002  Ab   1.0
           3 2003-12-01 00:00:00.003  b_   0.0

        Use backslash ``\`` character to escape special characters:

        .. testcode::
           :skipif: not is_ilike_supported()

           data = otp.Ticks(X=['a', 'ab', 'bb', 'b_'])
           data['LIKE'] = data['X'].str.ilike(r'b\_')
           df = otp.run(data)
           print(df)

        .. testoutput::

                                Time   X  LIKE
           0 2003-12-01 00:00:00.000   a   0.0
           1 2003-12-01 00:00:00.001  ab   0.0
           2 2003-12-01 00:00:00.002  bb   0.0
           3 2003-12-01 00:00:00.003  b_   1.0

        This function can be used to filter out ticks:

        .. testcode::
           :skipif: not is_ilike_supported()

           data = otp.Ticks(X=['a', 'ab', 'Ab', 'b_'])
           data = data.where(data['X'].str.ilike('a%'))
           df = otp.run(data)
           print(df)

        .. testoutput::

                                Time   X
           0 2003-12-01 00:00:00.000   a
           1 2003-12-01 00:00:00.001  ab
           2 2003-12-01 00:00:00.002  Ab

        ``pattern`` can only be a constant expression, like string or symbol parameter:

        .. testcode::
           :skipif: not is_ilike_supported()

           data = otp.Ticks(X=['a', 'ab', 'A', 'b_'])
           data['LIKE'] = data['X'].str.ilike(data.Symbol['PATTERN', str])
           df = otp.run(data, symbols=otp.Tick(SYMBOL_NAME='COMMON::AAPL', PATTERN='_'))['COMMON::AAPL']
           print(df)

        .. testoutput::

                                Time   X  LIKE
           0 2003-12-01 00:00:00.000   a   1.0
           1 2003-12-01 00:00:00.001  ab   0.0
           2 2003-12-01 00:00:00.002   A   1.0
           3 2003-12-01 00:00:00.003  b_   0.0
        """
        from onetick.py.core._source._symbol_param import _SymbolParamColumn
        if not isinstance(pattern, (str, _SymbolParamColumn)):
            raise ValueError('ilike() function expects parameter to be a constant expression')
        return _StrAccessor.Formatter(
            op_params=[self._base_column, pattern],
            dtype=bool,
            formatter=lambda column, pattern: f'{ott.value2str(column)} ILIKE {ott.value2str(pattern)}'
        )
