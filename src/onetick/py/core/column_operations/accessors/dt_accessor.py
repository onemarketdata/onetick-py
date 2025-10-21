from typing import Union

from onetick.py import configuration, utils
from onetick.py import types as ott
from onetick.py.core.column_operations.accessors._accessor import _Accessor
from onetick.py.backports import Literal
from onetick.py.types import datetime, value2str
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
    >>> data["Y"] = data["X"].dt.<function_name>()  # doctest: +SKIP
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
        >>> t = otp.Ticks(A=[otp.dt(2019, 1, 1, 1, 1, 1), otp.dt(2019, 2, 2, 2, 2, 2)])
        >>> t['B'] = t['A'].dt.strftime('%d.%m.%Y')
        >>> otp.run(t)[['A', 'B']]
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
        >>> data = otp.Ticks(X=[otp.dt(2019, 1, 1, 1, 1, 1), otp.dt(2019, 2, 2, 2, 2, 2)])
        >>> data["X"] = data["X"].dt.date()     # OTdirective: snippet-name: timestamp operations.date;
        >>> df = otp.run(data, timezone="GMT")
        >>> df["X"]
        0   2019-01-01
        1   2019-02-02
        Name: X, dtype: datetime64[ns]
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
            Sunday index.
        start_day: 'monday' or 'sunday'
            Day that will be denoted with ``start_index``

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, 5, i) for i in range(10, 17)])
        >>> data['DAY_OF_WEEK'] = data['X'].dt.day_of_week()
        >>> otp.run(data)[['X', 'DAY_OF_WEEK']]
                   X  DAY_OF_WEEK
        0 2022-05-10            2
        1 2022-05-11            3
        2 2022-05-12            4
        3 2022-05-13            5
        4 2022-05-14            6
        5 2022-05-15            7
        6 2022-05-16            1
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
        Return the month.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, i, 1) for i in range(3, 11)])
        >>> data['MONTH'] = data['X'].dt.month()
        >>> otp.run(data)[['X', 'MONTH']]
                   X  MONTH
        0 2022-03-01      3
        1 2022-04-01      4
        2 2022-05-01      5
        3 2022-06-01      6
        4 2022-07-01      7
        5 2022-08-01      8
        6 2022-09-01      9
        7 2022-10-01     10
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
        Return name of the month.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, i, 1) for i in range(3, 11)])
        >>> data['MONTH_NAME'] = data['X'].dt.month_name()
        >>> otp.run(data)[['X', 'MONTH_NAME']]
                   X MONTH_NAME
        0 2022-03-01        Mar
        1 2022-04-01        Apr
        2 2022-05-01        May
        3 2022-06-01        Jun
        4 2022-07-01        Jul
        5 2022-08-01        Aug
        6 2022-09-01        Sep
        7 2022-10-01        Oct
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
        Return the quarter.

        Examples
        --------
        >>> data = otp.Ticks(X=[otp.dt(2022, i, 1) for i in range(3, 11)])
        >>> data['QUARTER'] = data['X'].dt.quarter()
        >>> otp.run(data)[['X', 'QUARTER']]
                   X  QUARTER
        0 2022-03-01        1
        1 2022-04-01        2
        2 2022-05-01        2
        3 2022-06-01        2
        4 2022-07-01        3
        5 2022-08-01        3
        6 2022-09-01        3
        7 2022-10-01        4
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
        Return the year.

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
            dtype=datetime,
            formatter=formatter,
        )

    @docstring(parameters=[_timezone_doc], add_self=True)
    def week(self, timezone=None):
        """
        Returns the week.

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
