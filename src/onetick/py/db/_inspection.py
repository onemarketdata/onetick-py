import itertools
import warnings
from collections import defaultdict
from typing import Union, Iterable, Tuple, Optional, Literal
from datetime import date as dt_date, datetime, timedelta
from functools import wraps

import pandas as pd
from dateutil.tz import gettz

import onetick.py as otp
from onetick.py import configuration, utils
from onetick.py import types as ott
from onetick.py.compatibility import is_native_plus_zstd_supported, is_show_db_list_show_description_supported
from onetick.py.core import db_constants
from onetick.py.otq import otq


def _datetime2date(dt: Union[dt_date, datetime]) -> dt_date:
    """ Convert datetime and date explicitly into the datetime.date """
    return dt_date(dt.year, dt.month, dt.day)


def _method_cache(meth):
    """
    Cache the output of class method.
    Cache is created inside of the self object (in self.__cache property)
    and will be deleted when self object is destroyed.

    This is a rewrite of functools.cache,
    but it doesn't add self argument to cache key and thus doesn't keep reference to self forever.
    """
    @wraps(meth)
    def wrapper(self, *args, **kwargs):

        # cache key is a tuple of all arguments
        key = args
        for kw_tup in kwargs.items():
            key += kw_tup
        key = hash(key)

        if not hasattr(self, '__cache'):
            self.__cache = defaultdict(dict)

        method_cache = self.__cache[meth.__name__]

        miss = object()
        result = method_cache.get(key, miss)
        if result is miss:
            result = meth(self, *args, **kwargs)
            method_cache[key] = result
        return result

    return wrapper


class DB:

    """
    An object of available databases that the :py:func:`otp.databases() <onetick.py.databases>` function returns.
    It helps to make initial analysis on the database level: available tick types,
    dates with data, symbols, tick schema, etc.
    """

    def __init__(self, name, description='', context=utils.default):
        self.name = name
        self.description = description
        if context is utils.default or context is None:
            self.context = otp.config.context
        else:
            self.context = context
        self._locator_date_ranges = None

    def __eq__(self, obj):
        return all((
            self.name == obj.name,
            self.description == obj.description,
            self.context == obj.context,
        ))

    def __lt__(self, obj):
        return str(self) < str(obj)

    def __str__(self):
        return self.name

    @_method_cache
    def access_info(self, deep_scan=False, username=None) -> Union[pd.DataFrame, dict]:
        """
        Get access info for this database and ``username``.

        All dates are returned in GMT timezone.

        Parameters
        ----------
        deep_scan:
            If False (default) then the access fields are returned from the configuration of the database
            (basically the same fields as specified in the locator) and the dictionary is returned.
            If True then access fields are returned for each available remote host and time interval
            and the :pandas:`pandas.DataFrame` object is returned.
        username:
            Can be used to specify the user for which the query will be executed.
            By default the query is executed for the current user.

        See also
        --------
        **ACCESS_INFO** OneTick event processor

        Examples
        --------

        By default access fields from the basic configuration of the database are returned:

        >>> some_db = otp.databases()['SOME_DB']
        >>> some_db.access_info()  # doctest: +SKIP
        {'DB_NAME': 'SOME_DB',
         'READ_ACCESS': 1,
         'WRITE_ACCESS': 1,
         'MIN_AGE_SET': 0,
         'MIN_AGE_MSEC': 0,
         'MAX_AGE_SET': 0,
         'MAX_AGE_MSEC': 0,
         'MIN_START_DATE_SET': 0,
         'MIN_START_DATE_MSEC': Timestamp('1970-01-01 00:00:00'),
         'MAX_END_DATE_SET': 0,
         'MAX_END_DATE_MSEC': Timestamp('1970-01-01 00:00:00'),
         'MIN_AGE_DB_DAYS': 0,
         'MIN_AGE_DB_DAYS_SET': 0,
         'MAX_AGE_DB_DAYS': 0,
         'MAX_AGE_DB_DAYS_SET': 0,
         'CEP_ACCESS': 1,
         'DESTROY_ACCESS': 0,
         'MEMDB_ACCESS': 1}

        Set parameter ``deep_scan`` to True to return access fields from each available host and time interval:

        >>> some_db.access_info(deep_scan=True)  # doctest: +SKIP
           DB_NAME  READ_ACCESS  WRITE_ACCESS  MIN_AGE_SET  MIN_AGE_MSEC  MAX_AGE_SET  MAX_AGE_MSEC\
              MIN_START_DATE_SET MIN_START_DATE_MSEC  MAX_END_DATE_SET MAX_END_DATE_MSEC  MIN_AGE_DB_DAYS\
              MIN_AGE_DB_DAYS_SET  MAX_AGE_DB_DAYS  MAX_AGE_DB_DAYS_SET  CEP_ACCESS  DESTROY_ACCESS  MEMDB_ACCESS\
                SERVER_ADDRESS INTERVAL_START INTERVAL_END
        0  SOME_DB            1             1            0             0            0             0\
                               0          1970-01-01                 0        1970-01-01                0\
                                0                0                    0           1               0             1\
                           ...     2002-12-30   2100-01-01
        """
        # get parent name for derived databases, only parent databases will be listed by AccessInfo
        name, _, _ = self.name.partition('//')
        node = (
            otq.AccessInfo(info_type='DATABASES', show_for_all_users=False, deep_scan=deep_scan)
            >> otq.WhereClause(where=f'DB_NAME = "{name}"')
        )
        graph = otq.GraphQuery(node)

        self._set_intervals()
        # start and end times don't matter, but need to fit in the configured time ranges
        start, end = self._locator_date_ranges[-1]

        df = otp.run(graph,
                     symbols=f'{self.name}::',
                     start=start,
                     end=end,
                     # and timezone is GMT, because timestamp parameters in ACL are in GMT
                     timezone='GMT',
                     # ACCESS_INFO can return ACL violation error if we use database name as symbol
                     query_properties={'IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE': 'TRUE'},
                     username=username,
                     context=self.context)
        if not df.empty:
            df = df.drop(columns='Time')
        if deep_scan:
            return df
        return dict(df.iloc[0] if not df.empty else {})

    def show_config(self, config_type: Literal['locator_entry', 'db_time_intervals'] = 'locator_entry') -> dict:
        """
        Shows the specified configuration for a database.

        Parameters
        ----------
        config_type: str
            If **'locator_entry'** is specified, a string representing db's locator entry along with VDB_FLAG
            (this flag equals 1 when the database is virtual and 0 otherwise) will be returned.

            If **'db_time_intervals'** is specified,
            then time intervals configured in the locator file will be propagated
            including additional information, such as
            LOCATION, ARCHIVE_DURATION, DAY_BOUNDARY_TZ, DAY_BOUNDARY_OFFSET, ALTERNATIVE_LOCATIONS, etc.

        See also
        --------
        **DB/SHOW_CONFIG** OneTick event processor

        Examples
        --------
        .. testcode::
           :skipif: not is_native_plus_zstd_supported()

           some_db = otp.databases()['SOME_DB']
           print(some_db.show_config()['LOCATOR_STRING'])

        .. testoutput::
           :options: +ELLIPSIS

           <DB ARCHIVE_COMPRESSION_TYPE="NATIVE_PLUS_ZSTD" ID="SOME_DB" SYMBOLOGY="BZX" TICK_TIMESTAMP_TYPE="NANOS" >
           <LOCATIONS >
               <LOCATION ACCESS_METHOD="file" DAY_BOUNDARY_TZ="EST5EDT"
                         END_TIME="21000101000000" LOCATION="..." START_TIME="20021230000000" />
           </LOCATIONS>
           <RAW_DATA />
           </DB>

        >>> some_db = otp.databases()['SOME_DB']
        >>> some_db.show_config(config_type='db_time_intervals')  # doctest: +ELLIPSIS
        {'START_DATE': 1041206400000, 'END_DATE': 4102444800000,
         'GROWABLE_ARCHIVE_FLAG': 0, 'ARCHIVE_DURATION': 0,
         'LOCATION': '...', 'DAY_BOUNDARY_TZ': 'EST5EDT', 'DAY_BOUNDARY_OFFSET': 0, 'ALTERNATIVE_LOCATIONS': ''}
        """
        node = otq.DbShowConfig(db_name=self.name, config_type=config_type.upper())
        graph = otq.GraphQuery(node)
        df = otp.run(graph,
                     symbols='LOCAL::',
                     # start and end times don't matter
                     start=db_constants.DEFAULT_START_DATE,
                     end=db_constants.DEFAULT_END_DATE,
                     # and timezone is GMT, because timestamp parameters in ACL are in GMT
                     timezone='GMT',
                     context=self.context)
        if df.empty:
            raise ValueError(f"Can't get config for database '{self.name}'")
        df = df.drop(columns='Time')
        return dict(df.iloc[0])

    @property
    def min_acl_start_date(self) -> Optional[dt_date]:
        """
        Minimum start date set in ACL for current user.
        Returns None if not set.
        """
        access_info = self.access_info()
        if not access_info:
            return None
        if access_info['MIN_START_DATE_SET'] == 0:
            return None
        return _datetime2date(access_info['MIN_START_DATE_MSEC'])

    @property
    def max_acl_end_date(self) -> Optional[dt_date]:
        """
        Maximum end date set in ACL for current user.
        Returns None if not set.
        """
        access_info = self.access_info()
        if not access_info:
            return None
        if access_info['MAX_END_DATE_SET'] == 0:
            return None
        return _datetime2date(access_info['MAX_END_DATE_MSEC'])

    def _fit_time_interval_in_acl(self, start, end, timezone='GMT') -> Tuple[datetime, datetime]:
        """
        Returns the part of time interval between ``start`` and ``end`` that fits ACL start/end time rules.
        ``start`` and ``end`` objects are considered to be timezone-naive and will be localized in ``timezone``.

        If it's not possible to find such interval, raises ValueError.
        """
        # convert to GMT, because ACL timestamps are in GMT
        start = otp.dt(utils.convert_timezone(start, timezone, 'GMT'))
        end = otp.dt(utils.convert_timezone(end, timezone, 'GMT'))

        if self.min_acl_start_date is not None:
            if end < otp.dt(self.min_acl_start_date):
                # fully not intersecting intervals
                raise ValueError(f'Date {start.date()} {timezone} violates ACL rules for the database {self.name}:'
                                 f' minimum start time is {otp.dt(self.min_acl_start_date)} GMT.')
            # partly intersecting intervals, choose the part not violating ACL
            start = max(start, otp.dt(self.min_acl_start_date))

        if self.max_acl_end_date is not None:
            if start >= otp.dt(self.max_acl_end_date):
                # fully not intersecting intervals
                raise ValueError(f'Date {start.date()} {timezone} violates ACL rules for the database {self.name}:'
                                 f' maximum (exclusive) end time is {otp.dt(self.max_acl_end_date)} GMT.')
            # partly intersecting intervals, choose the part not violating ACL
            end = min(end, otp.dt(self.max_acl_end_date))

        # convert back to timezone
        start = utils.convert_timezone(start, 'GMT', timezone)
        end = utils.convert_timezone(end, 'GMT', timezone)
        return start, end

    def _fit_date_in_acl(self, date, timezone='GMT') -> Tuple[datetime, datetime]:
        """
        Returns the part of ``date`` time interval that fits ACL start/end time rules.
        ``date`` object is considered to be timezone-naive and will be localized in ``timezone``.

        If it's not possible to find such interval, raises ValueError.
        """
        date = _datetime2date(date)
        start = otp.dt(date)
        end = start + otp.Day(1)
        return self._fit_time_interval_in_acl(start, end, timezone)

    @_method_cache
    def _show_configured_time_ranges(self):
        graph = otq.GraphQuery(otq.DbShowConfiguredTimeRanges(db_name=self.name).tick_type("ANY")
                               >> otq.Table(fields='long START_DATE, long END_DATE'))
        result = otp.run(graph,
                         symbols=f'{self.name}::',
                         # start and end times don't matter for this query, use some constants
                         start=db_constants.DEFAULT_START_DATE,
                         end=db_constants.DEFAULT_END_DATE,
                         # GMT, because start/end timestamp in locator are in GMT
                         timezone='GMT',
                         context=self.context)
        return result

    def _set_intervals(self):
        """
        Finds all date ranges from locators.
        These intervals are required to find all possible dates with data.
        It is only possible by querying the DB_SHOW_LOADED_TIME_RANGE
        against the largest possible query date range.
        """

        if self._locator_date_ranges is None:
            result = self._show_configured_time_ranges()
            date_ranges = []

            tz_gmt = gettz('GMT')
            for inx in range(len(result)):
                start_date = result['START_DATE'][inx]
                # On Windows datetime.fromtimestamp throws an OSError for negative values
                start_date = max(start_date, 0)
                start = datetime.fromtimestamp(start_date / 1000, tz=tz_gmt)
                start = start.replace(tzinfo=None)
                try:
                    end = datetime.fromtimestamp(result['END_DATE'][inx] / 1000, tz=tz_gmt)
                except (ValueError, OSError):
                    # this may happen if value exceeds 9999-12-31 23:59:59.999999
                    end = datetime.max
                end = end.replace(tzinfo=None)

                date_ranges.append((start, end))

            # merge ranges if necessary to reduce number of queries
            # for `dates` property then
            self._locator_date_ranges = []
            start, end = None, None

            for t_start, t_end in date_ranges:
                if start is None:
                    start = t_start
                if end is None:
                    end = t_end
                else:
                    if t_start == end:
                        end = t_end
                    else:
                        self._locator_date_ranges.append((start, end))
                        start, end = t_start, t_end

            if start and end:
                self._locator_date_ranges.append((start, end))

    def _show_loaded_time_ranges(self, start, end, only_last=False, prefer_speed_over_accuracy=False):
        kwargs = {}
        # PY-1421: we aim to make this query as fast as possible
        # There are two problems with this EP:
        #   1. executing this query without using cache
        #      and/or without setting prefer_speed_over_accuracy parameter
        #      may be very slow for big time range
        #   2. using cache sometimes returns not precise results
        # So in case prefer_speed_over_accuracy parameter is available we are disabling cache.
        if prefer_speed_over_accuracy:
            kwargs['prefer_speed_over_accuracy'] = True
            kwargs['use_cache'] = False
        else:
            kwargs['use_cache'] = True

        eps = otq.DbShowLoadedTimeRanges(**kwargs).tick_type('ANY')
        eps = eps >> otq.WhereClause(where='NUM_LOADED_PARTITIONS > 0')
        if only_last:
            eps = eps >> otq.LastTick()

        graph = otq.GraphQuery(eps)
        result = otp.run(graph,
                         symbols=f'{self.name}::',
                         start=start,
                         end=end,
                         # GMT works properly for locators with gap
                         timezone='GMT',
                         context=self.context)

        dates = []
        # every record contains consequent intervals of data on disk
        for inx in range(len(result)):
            start = datetime.strptime(str(result['START_DATE'][inx]), '%Y%m%d')
            end = datetime.strptime(str(result['END_DATE'][inx]), '%Y%m%d')
            if only_last:
                return [_datetime2date(end)]
            while start <= end:
                dates.append(_datetime2date(start))
                start += timedelta(days=1)

        return dates

    def __split_loaded_time_ranges(self, locator_start, locator_end, only_last):
        # locator date range can be very big, so splitting it in smaller parts
        # (because _show_loaded_time_ranges() can be very slow for big time ranges)
        # it is especially useful when we only need the last date
        dates = []
        start = end = locator_end
        delta = 1 if only_last else 365
        while locator_start < start:
            start = end - timedelta(days=delta)
            start = max(locator_start, start)
            loaded_dates = self._show_loaded_time_ranges(start, end, only_last=only_last)
            if only_last and loaded_dates:
                return [loaded_dates[-1]]
            dates = loaded_dates + dates
            end = start
            # if we are not getting data, then increasing time range to find it faster
            if not loaded_dates:
                delta *= 2
        return dates

    def __get_dates(self, only_last=False, respect_acl=False, check_index_file=utils.adaptive):
        """ Returns list of dates in GMT timezone with data """
        self._set_intervals()

        dates = []
        today = dt_date.today()
        today = datetime(today.year, today.month, today.day)
        # searching in reversed order in case we need only_last date
        for locator_start, locator_end in reversed(self._locator_date_ranges):
            # future is not loaded yet
            if locator_start > today:
                continue
            locator_end = min(locator_end, today)

            if respect_acl:
                try:
                    locator_start, locator_end = self._fit_time_interval_in_acl(locator_start, locator_end)
                except ValueError:
                    # fully not intersecting intervals, trying next locator date range
                    continue

            if check_index_file is utils.adaptive or check_index_file is None:
                prefer_speed_over_accuracy = True
            else:
                prefer_speed_over_accuracy = not check_index_file
            try:
                loaded_dates = self._show_loaded_time_ranges(locator_start, locator_end,
                                                             only_last=only_last,
                                                             prefer_speed_over_accuracy=prefer_speed_over_accuracy)
            except Exception as e:
                # parameter prefer_speed_over_accuracy is not supported on all OneTick versions and servers
                if check_index_file is not utils.adaptive:
                    raise ValueError(
                        "Parameter 'check_index_file' is not supported by the API or OneTick server"
                    ) from e
                # in this case we fall back to splitting the locator range into smaller parts to increase speed
                loaded_dates = self.__split_loaded_time_ranges(locator_start, locator_end, only_last)

            if only_last and loaded_dates:
                return loaded_dates[-1]
            dates = loaded_dates + dates

        if only_last and len(dates) == 0:
            return None  # no data on disk

        return dates

    def dates(self, respect_acl=False, check_index_file=utils.adaptive):
        """
        Returns list of dates in GMT timezone for which data is available.

        Parameters
        ----------
        respect_acl: bool
            If True then only the dates that current user has access to will be returned
        check_index_file: bool
            If True, then file *index* will be searched for to determine if a database is loaded for a date.
            This check may be expensive, in terms of time it takes,
            when the file resides on NFS or on object storage, such as S3.
            If this parameter is set to False, then only the database directory for a date will be searched.
            This will increase performance, but may also return the days that are configured
            but where there is actually no data.
            By default this option is set to False if it is supported by API and the server,
            otherwise it is set to True.

        Returns
        -------
        ``datetime.date`` or ``None``
            Returns ``None`` when there is no data in the database

        Examples
        --------
        >>> some_db = otp.databases()['SOME_DB']
        >>> some_db.dates()
        [datetime.date(2003, 12, 1)]
        """
        return self.__get_dates(respect_acl=respect_acl, check_index_file=check_index_file)

    def last_not_empty_date(self, last_date, days_back, timezone=None, tick_type=None):
        """
        Find first day that has data
        starting from ``last_date`` and going ``days_back`` number of days back.
        """
        min_locator_date = self.min_locator_date()
        for i in range(days_back + 1):
            date = _datetime2date(last_date - timedelta(days=i))
            if date < min_locator_date:
                break
            try:
                tick_types = self.tick_types(date, timezone=timezone)
            except ValueError:
                # acl date violation
                break
            if tick_type is None and tick_types:
                return date
            if tick_type is not None and tick_type in tick_types:
                return date
        return None

    @property
    def last_date(self):
        """
        The latest date on which db has data and the current user has access to.

        Returns
        -------
        ``datetime.date`` or ``None``
            Returns ``None`` when there is no data in the database

        Examples
        --------
        >>> some_db = otp.databases()['SOME_DB']
        >>> some_db.last_date
        datetime.date(2003, 12, 1)
        """
        return self.get_last_date()

    def get_last_date(self, tick_type=None, timezone=None, show_warnings=True, check_index_file=utils.adaptive):
        last_date = self.__get_dates(only_last=True, respect_acl=True, check_index_file=check_index_file)
        if last_date is None:
            return None
        # It might happen that database loading processes is configured
        # to work over weekends and holidays and therefore
        # there are days that are configured but have no data, tick types and schema.
        # We want to find the closest not empty day because
        # we want to expose the most actual schema to end user.
        # For example, this is a case of OneTick Cloud US_COMP database.
        # We only scan 5 previous days to cover weekends + possible conjuncted holidays.
        # According to the official NYSE calendar there are no more than 5 closed days.
        date = self.last_not_empty_date(last_date, days_back=5, tick_type=tick_type, timezone=timezone)
        if date is None:
            if show_warnings:
                warnings.warn(
                    "Can't find not empty day for the last 5 days, using last configured day. "
                    "Try to use .last_not_empty_date() function to find older not empty days."
                )
            return last_date
        return date

    def tick_types(self, date=None, timezone=None) -> list[str]:
        """
        Returns list of tick types for the ``date``.

        Parameters
        ----------
        date: :class:`otp.dt <onetick.py.datetime>`, :py:class:`datetime.datetime`, optional
            Date for the tick types look up. ``None`` means the :attr:`last_date`
        timezone: str, optional
            Timezone for the look up. ``None`` means the default timezone.

        Returns
        -------
        list
            List with string values of available tick types.

        Examples
        --------
        >>> us_comp_db = otp.databases()['US_COMP']
        >>> us_comp_db.tick_types(date=otp.dt(2022, 3, 1))
        ['QTE', 'TRD']
        """
        date = self.last_date if date is None else date

        if timezone is None:
            timezone = configuration.config.tz

        if date is None:
            # in the usual case it would mean that there is no data in the database,
            # but _show_loaded_time_ranges doesn't return dates for database views
            # in this case let's just try to get the database schema with default time range
            start = end = utils.adaptive
            # also it seems that show_schema=True doesn't work for views either
            show_schema = False
        else:
            start, end = self._fit_date_in_acl(date, timezone=timezone)  # type: ignore[assignment]
            show_schema = True

        # PY-458: don't use cache, it can return different result in some cases
        result = self._get_schema(use_cache=False, start=start, end=end, timezone=timezone, show_schema=show_schema)
        if len(result) == 0:
            return []

        return result['TICK_TYPE_NAME'].unique().tolist()

    def min_locator_date(self):
        self._set_intervals()
        min_date = min(obj[0] for obj in self._locator_date_ranges)
        return _datetime2date(min_date)

    @_method_cache
    def _get_schema(self, start, end, timezone, use_cache, show_schema):
        ep = otq.DbShowTickTypes(use_cache=use_cache,
                                 show_schema=show_schema,
                                 include_memdb=True)
        return otp.run(ep,
                       symbols=f'{self.name}::',
                       start=start,
                       end=end,
                       timezone=timezone,
                       context=self.context)

    def schema(self, date=None, tick_type=None, timezone=None, check_index_file=utils.adaptive) -> dict[str, type]:
        """
        Gets the schema of the database.

        Parameters
        ----------
        date: :class:`otp.dt <onetick.py.datetime>`, :py:class:`datetime.datetime`, optional
            Date for the schema. ``None`` means the :attr:`last_date`
        tick_type: str, optional
            Specifies a tick type for schema. ``None`` means use the one available
            tick type, if there are multiple tick types then it raises the ``Exception``.
            It uses the :meth:`tick_types` method.
        timezone: str, optional
            Allows to specify a timezone for searching tick types.
        check_index_file: bool
            If True, then file *index* will be searched for to determine if a database is loaded for a date.
            This check may be expensive, in terms of time it takes,
            when the file resides on NFS or on object storage, such as S3.
            If this parameter is set to False, then only the database directory for a date will be searched.
            This will increase performance, but may also return the days that are configured
            but where there is actually no data.
            By default this option is set to False if it is supported by API and the server,
            otherwise it is set to True.

        Returns
        -------
        dict
            Dict where keys are field names and values are ``onetick.py`` :ref:`types <schema concept>`.
            It's compatible with the :attr:`onetick.py.Source.schema` methods.

        Examples
        --------
        >>> us_comp_db = otp.databases()['US_COMP']
        >>> us_comp_db.schema(tick_type='TRD', date=otp.dt(2022, 3, 1))
        {'PRICE': <class 'float'>, 'SIZE': <class 'int'>}
        """
        orig_date = date

        if date is None:
            date = self.get_last_date(tick_type=tick_type, timezone=timezone, check_index_file=check_index_file)
        if timezone is None:
            timezone = configuration.config.tz
        if tick_type is None:
            tick_types = self.tick_types(date=date, timezone=timezone)
            if len(tick_types) == 0:
                raise ValueError("No tick types has found and specified")
            if len(tick_types) > 1:
                raise ValueError("Database has multiple tick types, please specify using the `tick_type` parameter")

            tick_type = tick_types[0]

        if date is None:
            # it might happen when a database has no data on disks
            return {}

        # Convert explicitly into the datetime.date, because min_date and date
        # could be date or datetime types, and datetime is not comparable with datetime.date
        date = _datetime2date(date)

        start, end = self._fit_date_in_acl(date, timezone=timezone)

        kwargs = dict(
            start=start, end=end, timezone=timezone, show_schema=True,
        )
        # PY-458, BEXRTS-1220, PY-1421
        # the results of the query may vary depending on using use_cache parameter, so we are trying both
        result = self._get_schema(use_cache=False, **kwargs)
        if result.empty:
            result = self._get_schema(use_cache=True, **kwargs)

        fields: Iterable = []
        if len(result):
            result = result[result['TICK_TYPE_NAME'] == tick_type]
            # filter schema by date
            date_to_filter = None
            if orig_date:
                # if date is passed as a parameter -- then use it
                date_to_filter = date
            else:
                # otherwise use the closest date
                date_to_filter = result['Time'].max()

            result = result[(result['Time'] >= pd.Timestamp(date_to_filter))]

            fields = zip(result['FIELD_NAME'].tolist(),
                         result['FIELD_TYPE_NAME'].tolist(),
                         result['FIELD_SIZE'].tolist())

        schema = {}

        for fname, ftype, fsize in fields:
            dtype: type

            if 'UINT32' in ftype:
                dtype = otp.uint
            elif 'UINT64' in ftype:
                dtype = otp.ulong
            elif 'INT32' in ftype:
                dtype = otp.int
            elif 'INT64' in ftype:
                # otp.long can be used too, but we use int for backward compatibility
                dtype = int
            elif 'INT8' in ftype:
                dtype = otp.byte
            elif 'INT16' in ftype:
                dtype = otp.short
            elif 'INT' in ftype:
                dtype = int
            elif 'MSEC' in ftype:
                dtype = otp.msectime
            elif 'NSEC' in ftype:
                dtype = otp.nsectime
            elif 'DOUBLE' in ftype or 'FLOAT' in ftype:
                dtype = float
            elif 'DECIMAL' in ftype:
                dtype = otp.decimal
            elif 'VARSTRING' in ftype:
                dtype = otp.varstring
            elif 'STRING' in ftype:
                if fsize == 64:
                    dtype = str
                else:
                    dtype = otp.string[fsize]
            else:
                warnings.warn(
                    f"Unsupported field type '{ftype}' for field '{fname}'. "
                    "Note that this field will be ignored "
                    "and will not be added to the python schema, "
                    "but will still remain in the OneTick schema."
                )
                continue

            schema[fname] = dtype

        return schema

    def symbols(self, date=None, timezone=None, tick_type=None, pattern='.*') -> list[str]:
        """
        Finds a list of available symbols in the database

        Parameters
        ----------
        date: :class:`otp.dt <onetick.py.datetime>`, :py:class:`datetime.datetime`, optional
            Date for the symbols look up. ``None`` means the :attr:`last_date`
        tick_type: str, optional
            Tick type for symbols. ``None`` means union across all tick types.
        timezone: str, optional
            Timezone for the lookup. ``None`` means the default timezone.
        pattern: str
            Regular expression to select symbols.

        Examples
        --------
        >>> us_comp_db = otp.databases()['US_COMP']
        >>> us_comp_db.symbols(date=otp.dt(2022, 3, 1), tick_type='TRD', pattern='^AAP.*')
        ['AAP', 'AAPL']
        """
        if date is None:
            date = self.last_date
        if timezone is None:
            timezone = configuration.config.tz
        if tick_type is None:
            tick_type = ''

        eps = otq.FindDbSymbols(pattern='%', tick_type_field=tick_type) \
            >> otq.AddField(field='varstring SYMBOL', value='regex_replace(SYMBOL_NAME, ".*::", "")') \
            >> otq.WhereClause(where=f'regex_match(SYMBOL, "{pattern}")') \
            >> otq.Table(fields='SYMBOL')

        result = otp.run(eps,
                         symbols=f'{self.name}::',
                         start=date,
                         end=date + timedelta(days=1),
                         timezone=timezone,
                         context=self.context)

        if len(result) == 0:
            return []

        return result['SYMBOL'].tolist()

    def show_archive_stats(
        self,
        start=utils.adaptive,
        end=utils.adaptive,
        date=None,
        timezone='GMT',
    ) -> pd.DataFrame:
        """
        This method shows various stats about the queried symbol,
        as well as an archive as a whole for each day within the queried interval.

        Accelerator databases are not supported.
        Memory databases will be ignored even within their life hours.

        Archive stats returned:

            * COMPRESSION_TYPE - archive compression type.
              In older archives native compression flag is not stored,
              so for example for gzip compression this field may say "GZIP or NATIVE_PLUS_GZIP".
              The meta_data_upgrader.exe tool can be used to determine and inject that information in such cases
              in order to get a more precise result in this field.
            * TIME_RANGE_VALIDITY - whether lowest and highest loaded timestamps (see below) are known.
              Like native compression flag, this information is missing in older archives
              and can be added using meta_data_upgrader.exe tool.
            * LOWEST_LOADED_DATETIME - the lowest loaded timestamp for the queried interval (across all symbols)
            * HIGHEST_LOADED_DATETIME - the highest loaded timestamp for the queried interval (across all symbols)
            * TOTAL_TICKS - the number of ticks for the queried interval (across all symbols).
              Also missing in older archives and can be added using meta_data_upgrader.exe.
              If not available, -1 will be returned.
            * SYMBOL_DATA_SIZE - the size of the symbol in archive in bytes.
              This information is also missing in older archives, however the other options, it cannot later be added.
              In such cases -1 will be returned.
            * TOTAL_SYMBOLS - the number of symbols for the queried interval
            * TOTAL_SIZE - archive size in bytes for the queried interval
              (including the garbage potentially accumulated during appends).

        Note
        ----
        Fields **LOWEST_LOADED_DATETIME** and **HIGHEST_LOADED_DATETIME** are returned in GMT timezone,
        so the default value of parameter ``timezone`` is GMT too.

        See also
        --------
        **SHOW_ARCHIVE_STATS** OneTick event processor

        Examples
        --------

        Show stats for a particular date for a database SOME_DB:

        .. testcode::
           :skipif: not is_native_plus_zstd_supported()

           db = otp.databases()['SOME_DB']
           stats = db.show_archive_stats(date=otp.dt(2003, 12, 1))
           print(stats)

        .. testoutput::
           :options: +ELLIPSIS

                            Time  COMPRESSION_TYPE TIME_RANGE_VALIDITY LOWEST_LOADED_DATETIME HIGHEST_LOADED_DATETIME...
           0 2003-12-01 05:00:00  NATIVE_PLUS_ZSTD               VALID    2003-12-01 05:00:00 2003-12-01 05:00:00.002...
        """
        node = otq.ShowArchiveStats()
        graph = otq.GraphQuery(node)
        df = otp.run(graph,
                     symbols=f'{self.name}::',
                     start=start,
                     end=end,
                     date=date,
                     timezone=timezone,
                     context=self.context)
        return df

    def ref_data(
        self,
        ref_data_type: str,
        symbol_date=None,
        start=utils.adaptive,
        end=utils.adaptive,
        date=None,
        timezone='GMT',
        symbol: str = '',
    ) -> pd.DataFrame:
        """
        Shows reference data for the specified security and reference data type.

        It can be used to view corporation actions,
        symbol name changes,
        primary exchange info and symbology mapping for a securities,
        as well as the list of symbologies,
        names of custom adjustment types for corporate actions present in a reference database
        as well as names of continuous contracts in database symbology.

        Parameters
        ----------
        ref_data_type: str
            Type of reference data to be queried. Possible values are:

                * corp_actions
                * symbol_name_history
                * primary_exchange
                * symbol_calendar
                * symbol_currency
                * symbology_mapping
                * symbology_list
                * custom_adjustment_type_list
                * all_calendars
                * all_continuous_contract_names
        symbol_date:
            This parameter must be specified for some reference data types to be queried.
        symbol:
            Symbol name for the query (may be useful for some ``ref_data_type``).

        See also
        --------
        **REF_DATA** OneTick event processor

        Examples
        --------

        Show calendars for a database TRAIN_A_PRL_TRD in the given range:

        >>> db = otp.databases()['TRAIN_A_PRL_TRD']  # doctest: +SKIP
        >>> db.ref_data('all_calendars',  # doctest: +SKIP
        ...             start=otp.dt(2018, 2, 1),
        ...             end=otp.dt(2018, 2, 9),
        ...             symbol_date=otp.dt(2018, 2, 1))
                         Time        END_DATETIME CALENDAR_NAME SESSION_NAME SESSION_FLAGS DAY_PATTERN  START_HHMMSS\
          END_HHMMSS TIMEZONE  PRIORITY DESCRIPTION
        0 2018-02-01 00:00:00 2018-02-06 23:59:59          FRED      Regular             R   0.0.12345         93000\
              160000  EST5EDT         0
        1 2018-02-06 23:59:59 2018-02-07 23:59:59          FRED      Holiday             H   0.0.12345         93000\
              160000  EST5EDT         1
        2 2018-02-07 23:59:59 2050-12-31 23:59:59          FRED      Regular             F   0.0.12345         93000\
              160000  EST5EDT         0

        Set symbol name with ``symbol`` parameter:

        >>> db = otp.databases()['US_COMP_SAMPLE']  # doctest: +SKIP
        >>> db.ref_data(ref_data_type='corp_actions',  # doctest: +SKIP
        ...             start=otp.dt(2025, 1, 2),
        ...             end=otp.dt(2025, 7, 2),
        ...             symbol_date=otp.dt(2025, 7, 1),
        ...             symbol='WMT',
        ...             timezone='America/New_York')
                Time  MULTIPLICATIVE_ADJUSTMENT  ADDITIVE_ADJUSTMENT ADJUSTMENT_TYPE
        0 2025-03-21                   1.000000                0.235   CASH_DIVIDEND
        1 2025-03-21                   0.997261                0.000  MULTI_ADJ_CASH
        2 2025-05-09                   1.000000                0.235   CASH_DIVIDEND
        3 2025-05-09                   0.997588                0.000  MULTI_ADJ_CASH
        """
        ref_data_type = ref_data_type.upper()
        node = otq.RefData(ref_data_type=ref_data_type)
        graph = otq.GraphQuery(node)
        df = otp.run(graph,
                     symbols=f'{self.name}::{symbol}',
                     symbol_date=symbol_date,
                     start=start,
                     end=end,
                     date=date,
                     timezone=timezone,
                     context=self.context)
        return df


def databases(
    context=utils.default, derived: bool = False, readable_only: bool = True,
    fetch_description: Optional[bool] = None,
    as_table: bool = False,
) -> Union[dict[str, DB], pd.DataFrame]:
    """
    Gets all available databases in the ``context``.

    Parameters
    ----------
    context: str, optional
        Context to run the query.
        If not set then default :py:attr:`context<onetick.py.configuration.Config.context>` is used.
        See :ref:`guide about switching contexts <switching contexts>` for examples.
    derived: bool, dict
        If False (default) then derived databases are not returned.
        Otherwise derived databases names are added to the result after the non-derived databases.
        If set to dict then its items used as parameters to :py:func:`~onetick.py.derived_databases`.
        If set to True then default parameters for :py:func:`~onetick.py.derived_databases` are used.
    readable_only: bool
        If set to True (default), then return only the databases with read-access for the current user.
        Otherwise return all databases visible from the current process.
    fetch_description: bool
        If set to True, retrieves descriptions for databases and puts them into ``description`` property of
        :py:class:`~onetick.py.DB` objects in a returned dict.
    as_table: bool
        If False (default), this function returns a dictionary of database names and database objects.
        If True, returns a :pandas:`pandas.DataFrame` table where each row contains the info for each database.

    See also
    --------
    | **SHOW_DB_LIST** OneTick event processor
    | **ACCESS_INFO** OneTick event processor
    | :py:func:`derived_databases`

    Returns
    -------
    Dict where keys are database names and values are :class:`DB <onetick.py.db._inspection.DB>` objects
    or :pandas:`pandas.DataFrame` object depending on ``as_table`` parameter.

    Examples
    --------

    Get the dictionary of database names and objects:

    >>> otp.databases()  # doctest: +SKIP
    {'ABU_DHABI': <onetick.py.db._inspection.DB at 0x7f9413a5e8e0>,
     'ABU_DHABI_BARS': <onetick.py.db._inspection.DB at 0x7f9413a5ef40>,
     'ABU_DHABI_DAILY': <onetick.py.db._inspection.DB at 0x7f9413a5eac0>,
     'ALPHA': <onetick.py.db._inspection.DB at 0x7f9413a5e940>,
     'ALPHA_X': <onetick.py.db._inspection.DB at 0x7f9413a5e490>,
     ...
    }

    Get a table with database info:

    >>> otp.databases(as_table=True)  # doctest: +SKIP
               Time            DB_NAME  READ_ACCESS  WRITE_ACCESS         ...
    0    2003-01-01          ABU_DHABI            1             0         ...
    1    2003-01-01     ABU_DHABI_BARS            1             1         ...
    2    2003-01-01    ABU_DHABI_DAILY            1             1         ...
    3    2003-01-01              ALPHA            1             1         ...
    4    2003-01-01            ALPHA_X            1             1         ...
    ...         ...                ...          ...           ...         ...
    """
    show_db_list_kwargs = {}
    if fetch_description is not None and is_show_db_list_show_description_supported() and (
        'show_description' in otq.ShowDbList.Parameters.list_parameters()
    ):
        show_db_list_kwargs['show_description'] = fetch_description

    node = otq.AccessInfo(info_type='DATABASES', show_for_all_users=False, deep_scan=True).tick_type('ANY')
    # for some reason ACCESS_INFO sometimes return several ticks
    # for the same database with different SERVER_ADDRESS values
    # so we get only the first tick
    node = (
        node >> otq.NumTicks(is_running_aggr=True, group_by='DB_NAME',
                             all_fields_for_sliding=False, output_field_name='NUM_TICKS')
        >> otq.WhereClause(where='NUM_TICKS = 1')
        >> otq.Passthrough('NUM_TICKS', drop_fields=True)
    )
    if readable_only:
        node = node >> otq.WhereClause(where='READ_ACCESS = 1')

    left = node.set_node_name('LEFT')
    right = otq.ShowDbList(**show_db_list_kwargs).tick_type('ANY').set_node_name('RIGHT')
    join = otq.Join(
        left_source='LEFT', join_type='INNER', join_criteria='LEFT.DB_NAME = RIGHT.DATABASE_NAME',
        add_source_prefix=False,
    )
    left >> join << right  # pylint: disable=pointless-statement
    node = join >> otq.Passthrough('LEFT.TIMESTAMP,RIGHT.TIMESTAMP,DATABASE_NAME', drop_fields=True)

    # times bigger than datetime.max are not representable in python
    max_dt = ott.value2str(datetime.max)
    node = node >> otq.UpdateFields(set=f'INTERVAL_START={max_dt}', where=f'INTERVAL_START > {max_dt}')
    node = node >> otq.UpdateFields(set=f'INTERVAL_END={max_dt}', where=f'INTERVAL_END > {max_dt}')

    dbs = otp.run(node,
                  symbols='LOCAL::',
                  # start and end times don't matter for this query, use some constants
                  start=db_constants.DEFAULT_START_DATE,
                  end=db_constants.DEFAULT_END_DATE,
                  context=context)

    if as_table:
        return dbs

    # WebAPI returns empty DataFrame (no columns) if there are no databases
    if len(dbs) == 0:
        return {}

    db_list = list(dbs['DB_NAME'])
    db_description_list = dbs['DESCRIPTION'] if 'DESCRIPTION' in dbs else itertools.repeat('')
    merged_db_list = list(zip(db_list, db_description_list))

    db_dict = {
        db_name: DB(db_name, description=db_description, context=context)
        for db_name, db_description in merged_db_list
    }

    if derived:
        kwargs: dict = derived if isinstance(derived, dict) else {}
        kwargs.setdefault('context', context)
        db_dict.update(
            derived_databases(**kwargs)
        )
    return db_dict


def derived_databases(
    context=utils.default,
    start=None, end=None,
    selection_criteria='all',
    db=None,
    db_discovery_scope='query_host_only',
    as_table: bool = False,
) -> dict[str, DB]:
    """
    Gets available derived databases.

    Parameters
    ----------
    context: str, optional
        Context to run the query.
        If not set then default :py:attr:`context<onetick.py.configuration.Config.context>` is used.
        See :ref:`guide about switching contexts <switching contexts>` for examples.
    start: :py:class:`otp.datetime <onetick.py.datetime>`, optional
        If both ``start`` and ``end`` are set, then listing databases in this range only.
        Otherwise list databases from all configured time ranges for databases.

        If ``db`` is set, then
        :py:attr:`otp.config.default_start_time <onetick.py.configuration.Config.default_start_time>`
        is used by default.
    end: :py:class:`otp.datetime <onetick.py.datetime>`, optional
        If both ``start`` and ``end`` are set, then listing databases in this range only.
        Otherwise list databases from all configured time ranges for databases.

        If ``db`` is set, then
        :py:attr:`otp.config.default_end_time <onetick.py.configuration.Config.default_end_time>` is used by default.
    selection_criteria: str
        Possible values: *all*, *derived_from_current_db*, *direct_children_of_current_db*.
    db: str, optional
       Specifies database name if ``selection_criteria`` is set to
       *derived_from_current_db* or *direct_children_of_current_db*.
       Must be set in this case, otherwise does nothing.
    db_discovery_scope: str
        When *query_host_and_all_reachable_hosts* is specified,
        an attempt will be performed to get derived databases from all reachable hosts.
        When *query_host_only* is specified,
        only derived databases from the host on which the query is performed will be returned.
    as_table: bool
        If False (default), this function returns a dictionary of database names and database objects.
        If True, returns a :pandas:`pandas.DataFrame` table where each row contains the info for each database.

    See also
    --------
    **SHOW_DERIVED_DB_LIST** OneTick event processor

    Returns
    -------
    Dict where keys are database names and values are :class:`DB <onetick.py.db._inspection.DB>` objects
    or :pandas:`pandas.DataFrame` object depending on ``as_table`` parameter.
    """
    if start and end:
        time_range = otq.ShowDerivedDbList.TimeRange.QUERY_TIME_INTERVAL
    else:
        if db is None:
            # start and end times don't matter in this case, use some constants
            start = db_constants.DEFAULT_START_DATE
            end = db_constants.DEFAULT_END_DATE
        else:
            start = otp.config.default_start_time
            end = otp.config.default_end_time
        time_range = otq.ShowDerivedDbList.TimeRange.CONFIGURED_TIME_INTERVAL

    selection_criteria = getattr(otq.ShowDerivedDbList.SelectionCriteria, selection_criteria.upper())
    db_discovery_scope = getattr(otq.ShowDerivedDbList.DbDiscoveryScope, db_discovery_scope.upper())

    if selection_criteria != otq.ShowDerivedDbList.SelectionCriteria.ALL and not db:
        raise ValueError(f"Parameter 'db' must be set when parameter 'selection_criteria' is {selection_criteria}")

    ep = otq.ShowDerivedDbList(
        time_range=time_range,
        selection_criteria=selection_criteria,
        db_discovery_scope=db_discovery_scope,
    )
    ep = ep.tick_type('ANY')
    db = db or 'LOCAL'
    dbs = otp.run(ep, symbols=f'{db}::', start=start, end=end, context=context)
    if as_table:
        return dbs
    if len(dbs) == 0:
        return {}
    db_list = list(dbs['DERIVED_DB_NAME'])
    return {db_name: DB(db_name, context=context) for db_name in db_list}
