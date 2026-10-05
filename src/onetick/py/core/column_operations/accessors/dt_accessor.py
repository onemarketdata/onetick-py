from typing import Union, Literal

from onetick.py import configuration, utils
from onetick.py import types as ott
from onetick.py.core.column_operations.accessors._accessor import _Accessor
from onetick.py.types import value2str
from onetick.py.docs.utils import docstring, param_doc
from onetick.py.core.column import _Column
from onetick.py.core.column_operations.base import _Operation


_timezone_doc = param_doc(
    name='timezone',
    str_annotation='str | Operation | Column',
    desc="""
    Name of the timezone, an operation or a column with it.
    By default, the timezone of the query will be used.
    """,
    annotation=Union[str, _Operation, _Column]
)


class _DtAccessor(_Accessor):

    """
    Accessor for datetime functions

    >>> data = otp.Ticks(X=[otp.dt(2019, 1, 1, 1, 1, 1), otp.dt(2019, 2, 2, 2, 2, 2)])
    >>> data["Y"] = data['X'].dt.<function_name>()  # doctest: +SKIP
    """

    @docstring(parameters=[_timezone_doc], add_self=True)
    def strftime(self, format='%Y/%m/%d %H:%M:%S.%J', timezone=None):
        """
        Converts the number of nanoseconds (datetime) since 1970/01/01 GMT into
        the string specified by ``format`` for a specified ``timezone``.

        Parameters
        ----------
        format: str
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

            %q - Milliseconds (3 digits)

            %J - Nanoseconds (9 digits)

            %p - AM/PM (2 characters)

            %% - % character

        Examples
        --------
        >>> data = otp.Ticks(A=[otp.dt(2019, 1, 1, 1, 1, 1),
        ...                     otp.dt(2019, 2, 2, 2, 2, 2)])
        >>> data['B'] = data['A'].dt.strftime('%d.%m.%Y')
        >>> otp.run(data)[['A', 'B']]
                            A           B
        0 2019-01-01 01:01:01  01.01.2019
        1 2019-02-02 02:02:02  02.02.2019
        """
        if timezone is utils.default:
            timezone = configuration.config.tz

        def formatter(column, _format, _timezone):
            column = ott.value2str(column)
            _timezone, _format = self._preprocess_tz_and_format(_timezone, _format)
            return f'nsectime_format({_format},{column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, format, timezone],
            dtype=str,
            formatter=formatter,
        )

    def date(self):
        """
        Return a new :py:class:`onetick.py.nsectime` type operation filled with date only.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2019, 1, 1, 1, 1, 1),
        ...                     otp.dt(2019, 2, 2, 2, 2, 2)])
        >>> data['Y'] = data['X'].dt.date()     # OTdirective: snippet-name: timestamp operations.date;
        >>> df = otp.run(data)
        >>> df[['X', 'Y']]
                            X          Y
        0 2019-01-01 01:01:01 2019-01-01
        1 2019-02-02 02:02:02 2019-02-02
        """
        format_str = "%Y%m%d"
        return self.strftime(format_str, None).str.to_datetime(format_str, None)

    @docstring(parameters=[_timezone_doc], add_self=True)
    def day_of_week(
        self, start_index: Union[int, _Operation] = 1, start_day: Literal['monday', 'sunday'] = 'monday', timezone=None,
    ):
        """
        Return the day of the week.

        Assuming the week starts on ``start_day``, which is denoted by ``start_index``.
        Default: Monday - 1, ..., Sunday - 7; set according to ISO8601

        Parameters
        ----------
        start_index: int or Operation
            The index of the ``start_day``.
        start_day: 'monday' or 'sunday'
            Day that will be denoted with ``start_index``

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, 5, i) for i in range(9, 16)])
        >>> data['DAY_OF_WEEK'] = data['X'].dt.day_of_week()
        >>> otp.run(data)[['X', 'DAY_OF_WEEK']]
                   X  DAY_OF_WEEK
        0 2022-05-09            1
        1 2022-05-10            2
        2 2022-05-11            3
        3 2022-05-12            4
        4 2022-05-13            5
        5 2022-05-14            6
        6 2022-05-15            7
        """

        if start_day not in ['monday', 'sunday']:
            raise ValueError(f"'start_day' parameter ({start_day}) not in ['monday', 'sunday']")

        def formatter(column, _start_index, _start_day, _timezone):
            column = ott.value2str(column)
            _start_index = ott.value2str(_start_index)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            format_ = f'day_of_week({column},{_timezone})'
            if _start_day == 'monday':
                # CASE should be uppercased because it can be used in per-tick script
                format_ = f'CASE({format_}, 0, 7, {format_})-1'
            format_ += f'+{_start_index}'
            return format_

        return _DtAccessor.Formatter(
            op_params=[self._base_column, start_index, start_day, timezone],
            dtype=int,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def day_name(self, timezone=None):
        """
        Returns the name of the weekday.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, 5, i) for i in range(10, 17)])
        >>> data['DAY_NAME'] = data['X'].dt.day_name()
        >>> otp.run(data)[['X', 'DAY_NAME']]
                   X   DAY_NAME
        0 2022-05-10    Tuesday
        1 2022-05-11  Wednesday
        2 2022-05-12   Thursday
        3 2022-05-13     Friday
        4 2022-05-14   Saturday
        5 2022-05-15     Sunday
        6 2022-05-16     Monday
        """
        def formatter(column, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'DAYNAME({column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, timezone],
            dtype=str,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def day_of_month(self, timezone=None):
        """
        Return the day of the month.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, 5, i) for i in range(10, 17)])
        >>> data['DAY_OF_MONTH'] = data['X'].dt.day_of_month()
        >>> otp.run(data)[['X', 'DAY_OF_MONTH']]
                   X  DAY_OF_MONTH
        0 2022-05-10            10
        1 2022-05-11            11
        2 2022-05-12            12
        3 2022-05-13            13
        4 2022-05-14            14
        5 2022-05-15            15
        6 2022-05-16            16
        """
        def formatter(column, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'DAYOFMONTH({column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, timezone],
            dtype=int,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def day_of_year(self, timezone=None):
        """
        Return the day of the year.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, 5, i) for i in range(10, 17)])
        >>> data['DAY_OF_YEAR'] = data['X'].dt.day_of_year()
        >>> otp.run(data)[['X', 'DAY_OF_YEAR']]
                   X  DAY_OF_YEAR
        0 2022-05-10          130
        1 2022-05-11          131
        2 2022-05-12          132
        3 2022-05-13          133
        4 2022-05-14          134
        5 2022-05-15          135
        6 2022-05-16          136
        """
        def formatter(column, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'DAYOFYEAR({column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, timezone],
            dtype=int,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def hour(self, timezone=None):
        """
        Return the hour.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, 5, 1, i, 0, 6) for i in range(10, 17)])
        >>> data['HOUR'] = data['X'].dt.hour()
        >>> otp.run(data)[['X', 'HOUR']]
                            X  HOUR
        0 2022-05-01 10:00:06    10
        1 2022-05-01 11:00:06    11
        2 2022-05-01 12:00:06    12
        3 2022-05-01 13:00:06    13
        4 2022-05-01 14:00:06    14
        5 2022-05-01 15:00:06    15
        6 2022-05-01 16:00:06    16
        """
        def formatter(column, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'HOUR({column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, timezone],
            dtype=int,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def minute(self, timezone=None):
        """
        Return the minute.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, 5, 1, 15, i, 6) for i in range(10, 17)])
        >>> data['MINUTE'] = data['X'].dt.minute()
        >>> otp.run(data)[['X', 'MINUTE']]
                            X  MINUTE
        0 2022-05-01 15:10:06      10
        1 2022-05-01 15:11:06      11
        2 2022-05-01 15:12:06      12
        3 2022-05-01 15:13:06      13
        4 2022-05-01 15:14:06      14
        5 2022-05-01 15:15:06      15
        6 2022-05-01 15:16:06      16
        """
        def formatter(column, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'MINUTE({column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, timezone],
            dtype=int,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def second(self, timezone=None):
        """
        Return the second.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, 5, 1, 15, 11, i) for i in range(10, 17)])
        >>> data['SECOND'] = data['X'].dt.second()
        >>> otp.run(data)[['X', 'SECOND']]
                            X  SECOND
        0 2022-05-01 15:11:10      10
        1 2022-05-01 15:11:11      11
        2 2022-05-01 15:11:12      12
        3 2022-05-01 15:11:13      13
        4 2022-05-01 15:11:14      14
        5 2022-05-01 15:11:15      15
        6 2022-05-01 15:11:16      16
        """
        def formatter(column, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'SECOND({column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, timezone],
            dtype=int,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def month(self, timezone=None):
        """
        Return the month number.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, i, 1) for i in range(1, 13)])
        >>> data['MONTH'] = data['X'].dt.month()
        >>> otp.run(data)[['X', 'MONTH']]
                    X  MONTH
        0  2022-01-01      1
        1  2022-02-01      2
        2  2022-03-01      3
        3  2022-04-01      4
        4  2022-05-01      5
        5  2022-06-01      6
        6  2022-07-01      7
        7  2022-08-01      8
        8  2022-09-01      9
        9  2022-10-01     10
        10 2022-11-01     11
        11 2022-12-01     12
        """
        def formatter(column, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'MONTH({column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, timezone],
            dtype=int,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def month_name(self, timezone=None):
        """
        Return the name of the month.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, i, 1) for i in range(1, 13)])
        >>> data['MONTH_NAME'] = data['X'].dt.month_name()
        >>> otp.run(data)[['X', 'MONTH_NAME']]
                    X MONTH_NAME
        0  2022-01-01        Jan
        1  2022-02-01        Feb
        2  2022-03-01        Mar
        3  2022-04-01        Apr
        4  2022-05-01        May
        5  2022-06-01        Jun
        6  2022-07-01        Jul
        7  2022-08-01        Aug
        8  2022-09-01        Sep
        9  2022-10-01        Oct
        10 2022-11-01        Nov
        11 2022-12-01        Dec
        """
        def formatter(column, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'MONTHNAME({column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, timezone],
            dtype=str,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def quarter(self, timezone=None):
        """
        Return the quarter number.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, i, 1) for i in range(1, 13)])
        >>> data['QUARTER'] = data['X'].dt.quarter()
        >>> otp.run(data)[['X', 'QUARTER']]
                    X  QUARTER
        0  2022-01-01        1
        1  2022-02-01        1
        2  2022-03-01        1
        3  2022-04-01        2
        4  2022-05-01        2
        5  2022-06-01        2
        6  2022-07-01        3
        7  2022-08-01        3
        8  2022-09-01        3
        9  2022-10-01        4
        10 2022-11-01        4
        11 2022-12-01        4
        """
        def formatter(column, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'QUARTER({column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, timezone],
            dtype=int,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def year(self, timezone=None):
        """
        Return the year number.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2020 + i, 3, 1) for i in range(3, 11)])
        >>> data['YEAR'] = data['X'].dt.year()
        >>> otp.run(data)[['X', 'YEAR']]
                   X  YEAR
        0 2023-03-01  2023
        1 2024-03-01  2024
        2 2025-03-01  2025
        3 2026-03-01  2026
        4 2027-03-01  2027
        5 2028-03-01  2028
        6 2029-03-01  2029
        7 2030-03-01  2030
        """
        def formatter(column, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'YEAR({column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, timezone],
            dtype=int,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def date_trunc(self,
                   date_part: Literal['year', 'quarter', 'month', 'week', 'day', 'hour', 'minute', 'second',
                                      'millisecond', 'nanosecond'],
                   timezone=None):
        """
        Truncates to the specified precision.

        Parameters
        ----------
        date_part: str | Operation | Column
            Precision to truncate datetime to. Possible values are 'year', 'quarter', 'month', 'week', 'day', 'hour',
            'minute', 'second', 'millisecond' and 'nanosecond'.
            Notice that beginning of week is considered to be Sunday.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2020, 11, 11, 5, 4, 13, 101737, 879)] * 7,
        ...                  DATE_PART=['year', 'day', 'hour', 'minute', 'second', 'millisecond', 'nanosecond'])
        >>> data['TRUNCATED_X'] = data['X'].dt.date_trunc(data['DATE_PART'])
        >>> otp.run(data)[['X', 'TRUNCATED_X', 'DATE_PART']]
                                      X                   TRUNCATED_X    DATE_PART
        0 2020-11-11 05:04:13.101737879 2020-01-01 00:00:00.000000000         year
        1 2020-11-11 05:04:13.101737879 2020-11-11 00:00:00.000000000          day
        2 2020-11-11 05:04:13.101737879 2020-11-11 05:00:00.000000000         hour
        3 2020-11-11 05:04:13.101737879 2020-11-11 05:04:00.000000000       minute
        4 2020-11-11 05:04:13.101737879 2020-11-11 05:04:13.000000000       second
        5 2020-11-11 05:04:13.101737879 2020-11-11 05:04:13.101000000  millisecond
        6 2020-11-11 05:04:13.101737879 2020-11-11 05:04:13.101737879   nanosecond
        """
        def formatter(column, _date_part, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            _date_part = value2str(_date_part)
            return f'DATE_TRUNC({_date_part},{column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, date_part, timezone],
            dtype=ott.nsectime,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def week(self, timezone=None):
        """
        Returns the week number.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2020, i, 1) for i in range(3, 11)])
        >>> data['WEEK'] = data['X'].dt.week()
        >>> otp.run(data)[['X', 'WEEK']]
                   X  WEEK
        0 2020-03-01    10
        1 2020-04-01    14
        2 2020-05-01    18
        3 2020-06-01    23
        4 2020-07-01    27
        5 2020-08-01    31
        6 2020-09-01    36
        7 2020-10-01    40
        """
        def formatter(column, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'WEEK({column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, timezone],
            dtype=int,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def as_yyyymmddhhmmss(self, timezone=None):
        """
        Converts the timestamp into the integer in the form YYYYMMDDhhmmss.

        See Also
        --------
        | **AS_YYYYMMDDHHMMSS** OneTick function

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2020, i, i, i, i, i, i) for i in range(1, 5)])
        >>> data['Y'] = data['X'].dt.as_yyyymmddhhmmss()
        >>> otp.run(data)[['X', 'Y']]
                                   X               Y
        0 2020-01-01 01:01:01.000001  20200101010101
        1 2020-02-02 02:02:02.000002  20200202020202
        2 2020-03-03 03:03:03.000003  20200303030303
        3 2020-04-04 04:04:04.000004  20200404040404
        """
        def formatter(column, _timezone):
            column = ott.value2str(column)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'AS_YYYYMMDDHHMMSS({column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, timezone],
            dtype=int,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def date_name(self,
                  date_part: Literal['year', 'quarter', 'month',
                                     'week', 'weekday', 'day', 'dayofyear',
                                     'hour', 'minute', 'second',
                                     'millisecond', 'nanosecond'],
                  timezone=None):
        """
        Returns a number representing the specified ``date_part`` of the specified datetime,
        interpreted in the specified ``timezone``.

        Parameters
        ----------
        date_part: str | Operation | Column
            Possible values are:
                'year', 'quarter', 'month',
                'week', 'weekday', 'day', 'dayofyear',
                'hour', 'minute', 'second',
                'millisecond', 'nanosecond'.

        See Also
        --------
        | **DATENAME** OneTick function

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2020, 11, 11, 5, 4, 13, 101737, 879)] * 12,
        ...                  DATE_PART=['year', 'quarter', 'month',
        ...                             'week', 'weekday', 'day', 'dayofyear',
        ...                             'hour', 'minute', 'second',
        ...                             'millisecond', 'nanosecond'])
        >>> data['DATE_NAME'] = data['X'].dt.date_name(data['DATE_PART'])
        >>> otp.run(data)[['X', 'DATE_NAME', 'DATE_PART']]
                                       X  DATE_NAME    DATE_PART
        0  2020-11-11 05:04:13.101737879       2020         year
        1  2020-11-11 05:04:13.101737879          4      quarter
        2  2020-11-11 05:04:13.101737879         11        month
        3  2020-11-11 05:04:13.101737879         46         week
        4  2020-11-11 05:04:13.101737879          3      weekday
        5  2020-11-11 05:04:13.101737879         11          day
        6  2020-11-11 05:04:13.101737879        315    dayofyear
        7  2020-11-11 05:04:13.101737879          5         hour
        8  2020-11-11 05:04:13.101737879          4       minute
        9  2020-11-11 05:04:13.101737879         13       second
        10 2020-11-11 05:04:13.101737879          0  millisecond
        11 2020-11-11 05:04:13.101737879          0   nanosecond
        """
        def formatter(column, _date_part, _timezone):
            column = ott.value2str(column)
            _date_part = ott.value2str(_date_part)
            _timezone, _ = self._preprocess_tz_and_format(_timezone, '')
            return f'DATENAME({_date_part},{column},{_timezone})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, date_part, timezone],
            dtype=int,
            formatter=formatter,
        )

    def get_nanos_within_msec(self):
        """
        Returns number of nanoseconds retained in a millisecond from a timestamp value.

        See Also
        --------
        | **GET_NANOS_WITHIN_MSEC** OneTick function

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, 5, 1, 15, 0, 6, 123, i) for i in range(10, 17)])
        >>> data['Y'] = data['X'].dt.get_nanos_within_msec()
        >>> otp.run(data)[['X', 'Y']]
                                      X       Y
        0 2022-05-01 15:00:06.000123010  123010
        1 2022-05-01 15:00:06.000123011  123011
        2 2022-05-01 15:00:06.000123012  123012
        3 2022-05-01 15:00:06.000123013  123013
        4 2022-05-01 15:00:06.000123014  123014
        5 2022-05-01 15:00:06.000123015  123015
        6 2022-05-01 15:00:06.000123016  123016
        """
        return _DtAccessor.Formatter(
            op_params=[self._base_column],
            dtype=int,
            formatter=lambda column: f'GET_NANOS_WITHIN_MSEC({ott.value2str(column)})',
        )

    def get_msecs(self):
        """
        Return millisecond-precision timestamp from nanosecond-precision timestamp.

        See Also
        --------
        | **GET_MSECS** OneTick function

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, 5, 1, 15, 0, 6, i * 1000, 1) for i in range(10)])
        >>> data['Y'] = data['X'].dt.get_msecs()
        >>> otp.run(data)[['X', 'Y']]
                                      X                       Y
        0 2022-05-01 15:00:06.000000001 2022-05-01 15:00:06.000
        1 2022-05-01 15:00:06.001000001 2022-05-01 15:00:06.001
        2 2022-05-01 15:00:06.002000001 2022-05-01 15:00:06.002
        3 2022-05-01 15:00:06.003000001 2022-05-01 15:00:06.003
        4 2022-05-01 15:00:06.004000001 2022-05-01 15:00:06.004
        5 2022-05-01 15:00:06.005000001 2022-05-01 15:00:06.005
        6 2022-05-01 15:00:06.006000001 2022-05-01 15:00:06.006
        7 2022-05-01 15:00:06.007000001 2022-05-01 15:00:06.007
        8 2022-05-01 15:00:06.008000001 2022-05-01 15:00:06.008
        9 2022-05-01 15:00:06.009000001 2022-05-01 15:00:06.009
        """
        return _DtAccessor.Formatter(
            op_params=[self._base_column],
            dtype=ott.nsectime,
            formatter=lambda column: f'GET_MSECS({ott.value2str(column)})',
        )

    def timestamp_add(self,
                      date_part: Literal['SQL_TSI_YEAR', 'SQL_TSI_QUARTER', 'SQL_TSI_MONTH', 'SQL_TSI_WEEK',
                                         'SQL_TSI_DAY', 'SQL_TSI_HOUR', 'SQL_TSI_MINUTE', 'SQL_TSI_SECOND'],
                      number: int):
        """
        Returns the specified datetime with the specified ``number`` interval (signed integer)
        added to the specified ``date_part`` of that datetime.

        Parameters
        ----------
        number: int | Operation | Column
            Number of intervals to add.
        date_part: str | Operation | Column
            Supported values are:
                'SQL_TSI_YEAR', 'SQL_TSI_QUARTER', 'SQL_TSI_MONTH', 'SQL_TSI_WEEK',
                'SQL_TSI_DAY', 'SQL_TSI_HOUR', 'SQL_TSI_MINUTE', 'SQL_TSI_SECOND'.

        See Also
        --------
        | **TIMESTAMPADD** OneTick function

        Examples
        --------
        Add one interval:

        >>> data = otp.Ticks(
        ...     X=[otp.dt(2020, 11, 12)] * 5,
        ...     DATE_PART=['SQL_TSI_YEAR', 'SQL_TSI_QUARTER', 'SQL_TSI_MONTH', 'SQL_TSI_WEEK', 'SQL_TSI_DAY']
        ... )
        >>> data['Y'] = data['X'].dt.timestamp_add(data['DATE_PART'], 1)
        >>> otp.run(data)[['X', 'DATE_PART', 'Y']]
                   X        DATE_PART           Y
        0 2020-11-12     SQL_TSI_YEAR  2021-11-12
        1 2020-11-12  SQL_TSI_QUARTER  2021-02-10
        2 2020-11-12    SQL_TSI_MONTH  2020-12-12
        3 2020-11-12     SQL_TSI_WEEK  2020-11-19
        4 2020-11-12      SQL_TSI_DAY  2020-11-13

        Subtract one interval:

        >>> data = otp.Ticks(X=[otp.dt(2020, 11, 11, 5, 4, 13)] * 3,
        ...                  DATE_PART=['SQL_TSI_HOUR', 'SQL_TSI_MINUTE', 'SQL_TSI_SECOND'])
        >>> data['Y'] = data['X'].dt.timestamp_add(data['DATE_PART'], -1)
        >>> otp.run(data)[['X', 'DATE_PART', 'Y']]
                            X       DATE_PART                    Y
        0 2020-11-11 05:04:13    SQL_TSI_HOUR  2020-11-11 04:04:13
        1 2020-11-11 05:04:13  SQL_TSI_MINUTE  2020-11-11 05:03:13
        2 2020-11-11 05:04:13  SQL_TSI_SECOND  2020-11-11 05:04:12
        """
        def formatter(column, _date_part, _number):
            column = ott.value2str(column)
            _date_part = ott.value2str(_date_part)
            _number = ott.value2str(_number)
            return f'TIMESTAMPADD({_date_part},{number},{column})'

        return _DtAccessor.Formatter(
            op_params=[self._base_column, date_part, number],
            dtype=ott.nsectime,
            formatter=formatter,
        )
