# pylama:ignore=W0237
import logging
import os
import datetime as dt
import subprocess
import warnings

from datetime import timedelta
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from typing import List, Union, Optional
from uuid import uuid4

from onetick import py as otp
from onetick.py.core import db_constants as constants
from onetick.py.compatibility import is_native_plus_zstd_supported
from onetick.py import utils, sources, session, configuration

import pandas


def _tick_type_detector(tick_type, obj):
    if tick_type is not None:
        return tick_type

    type2traits = {
        "ORDER": ["ID", "BUY_FLAG", "SIDE", "QTY", "QTY_FILLED", "QTY", "ORDTYPE", "PRICE", "PRICE_FILLED"],
        "QTE": ["ASK_PRICE", "BID_PRICE", "ASK_SIZE", "BID_SIZE"],
        "NBBO": ["ASK_PRICE", "BID_PRICE", "ASK_SIZE", "BID_SIZE", "BID_EXCHANGE", "ASK_EXCHANGE"],
        "TRD": ["PRICE", "SIZE"],
    }

    type2count = defaultdict(lambda: 0)
    max_tt = "TRD"
    max_count = 0

    for column in obj.columns():
        for tt, traits in type2traits.items():
            if column in traits:
                type2count[tt] += 1

                if type2count[tt] > max_count:
                    max_count = type2count[tt]
                    max_tt = tt

    return max_tt


def write_to_db(src: 'otp.Source',
                dest: Union[str, 'otp.DB'],
                date: dt.date,
                symbol: Union[str, 'otp.Column'],
                tick_type: Union[str, 'otp.Column'],
                timezone: Optional[str] = None,
                execute: bool = True,
                start: Optional[dt.date] = None,
                end: Optional[dt.date] = None,
                propagate: bool = False,
                append: bool = True,
                **kwargs):
    """
    Writes source to the database.
    The main differences from otp.Source.write() function are
    appending ticks by default, automatic tick_type detection and executing the query right here.

    Parameters
    ----------
    src: :class:`otp.Source`
        source that will be written to the database.
    dest: str or :py:class:`otp.DB <onetick.py.DB>`
        database name or object.
    date: datetime or None
        date where to save data.
        Cannot be used together with `start` and `end` parameters.
    start: datetime or None
        start date of period where to save data.
        Cannot be used together with `date` parameters.
        Default is None.
    end: datetime or None
        end date of period where to save data.
        Cannot be used together with `date` parameters.
        Default is None.
    symbol: str or Column
        resulting symbol name string or column to get symbol name from.
    tick_type: str or Column
        resulting tick type string or column to get tick type from.
        If tick type is None then an attempt will be taken to get
        tick type name automatically based on the ``src`` source's schema.
        (ORDER, QTE, TRD and NBBO tick types are supported).
    timezone: str
        If ``execute`` parameter is set then this timezone
        will be used for running the query.
        By default, it is set to `otp.config.tz`.
    execute: bool
        execute the query right here or not.
        If True, `date` or `start`+`end` parameters are required,
        and then dataframe will be returned.
        (Probably empty, unless 'propagate' parameter is specified).
        If False, modified copy of the source ``src`` will be returned.
    propagate: bool
        Propagate ticks after writing or not.
    append: bool
        Write the data in append mode or not.
    kwargs:
        other arguments that will be passed to :py:meth:`onetick.py.Source.write` function.
    """

    tick_type = _tick_type_detector(tick_type, src)

    if timezone is None:
        timezone = configuration.config.tz
    if date is None or date is otp.adaptive:
        kwargs['start_date'] = start
        kwargs['end_date'] = end
    else:
        kwargs['date'] = date

    writer = src.write(db=dest,
                       symbol=symbol,
                       tick_type=tick_type,
                       propagate=propagate,
                       append=append,
                       **kwargs)

    if execute:
        if not start or not end:
            start = getattr(date, 'ts', date)
            end = start + relativedelta(days=1)
        return otp.run(writer, start=start, end=end, timezone=timezone)
    return writer


class _DB:

    _LOCAL = False
    # this flag means that db section should be added to locator and acl (True)
    # or db locates on some ts (False)

    def __init__(
        self,
        name,
        src=None,
        date=None,
        symbol=None,
        tick_type=None,
        db_properties: Optional[dict] = None,
        db_locations: Optional[list] = None,
        db_raw_data: Optional[dict] = None,
        db_feed: Optional[dict] = None,
        write=True,
        minimum_start_date: Optional[Union[str, dt.date, 'otp.date']] = None,
        maximum_end_date: Optional[Union[str, dt.date, 'otp.date']] = None,
    ):
        # we assume here that db_properties and db_locations fully prepared here or None (in case of remote db)
        self.name = name
        self.id, _, _ = name.partition("//")
        self._db_properties = db_properties
        self._db_locations = db_locations
        self._db_raw_data = db_raw_data
        self._db_feed = db_feed
        self._write = write
        self._minimum_start_date = (
            dt.datetime.strptime(minimum_start_date, '%Y%m%d').date()
            if isinstance(minimum_start_date, str)
            else minimum_start_date
        )
        self._maximum_end_date = (
            dt.datetime.strptime(maximum_end_date, '%Y%m%d').date()
            if isinstance(maximum_end_date, str)
            else maximum_end_date
        )
        if src is not None:
            self.add(src=src, date=date, symbol=symbol, tick_type=tick_type)
        elif any(x is not None for x in (date, symbol, tick_type)):
            warnings.warn(
                "Parameters 'date', 'symbol' and 'tick_type' can only be set when parameter 'src' is specified.",
                FutureWarning,
                stacklevel=3,
            )

    @staticmethod
    def _format_params(params):
        res = {}
        for key, value in params.items():
            if hasattr(value, 'strftime'):
                res[key] = value.strftime("%Y%m%d%H%M%S")
            else:
                res[key] = str(value)
        return res

    # TODO: move this method to DB
    def add(self,
            src,
            date=None,
            start=None,
            end=None,
            symbol=None,
            tick_type=None,
            timezone=None,
            **kwargs):
        """
        Add data to a database.
        If ticks with the same timestamp are already presented in database old values won't be updated.

        Parameters
        ----------
        src: :class:`otp.Source`
            source that will be written to the database.
        date: datetime or None
            date of the day in which the data will be saved.
            The timestamps of the ticks should be between the start and the end of the day.
            Be default, it is set to `otp.config.default_date`.
        start: datetime or None
            start day of period in which the data will be saved.
            The timestamps of the ticks should be between `start` and `end` dates.
            Cannot be used with `date` parameter.
            Be default, None.
        end: datetime or None
            end day of period in which the data will be saved.
            The timestamps of the ticks should be between `start` and `end` dates.
            Cannot be used with `date` parameter.
            Be default, None.
        symbol: str or Column
            resulting symbol name string or column to get symbol name from.
            Be default, it is set to `otp.config.default_db_symbol`.
        tick_type: str or Column
            resulting tick type string or column to get tick type from.
            If tick type is None then an attempt will be taken to get
            tick type name automatically based on the ``src`` source's schema.
            (ORDER, QTE, TRD and NBBO tick types are supported).
        timezone: str
            This timezone will be used for running the query.
            By default, it is set to `otp.config.tz`.
        kwargs:
            other arguments that will be passed to :py:meth:`onetick.py.Source.write` function.

        Examples
        --------

        Data is saved to the specified date, symbol and tick type:
        (note that ``session`` is created before this example)

        >>> db = otp.DB('MYDB2')
        >>> db.add(otp.Ticks(A=[4, 5, 6]), date=otp.dt(2003, 1, 1), symbol='SMB', tick_type='TT')
        >>> session.use(db)

        We can get the same data by specifying the same parameters:

        >>> data = otp.DataSource(db, date=otp.dt(2003, 1, 1), symbols='SMB', tick_type='TT')
        >>> otp.run(data)
                             Time  A
        0 2003-01-01 00:00:00.000  4
        1 2003-01-01 00:00:00.001  5
        2 2003-01-01 00:00:00.002  6
        """
        if timezone is None:
            timezone = configuration.config.tz
        if start and end and date:
            raise ValueError("You can't specify both start/end and date parameters")

        if start and end:
            kwargs['start'] = start
            kwargs['end'] = end
            kwargs['date'] = otp.adaptive
        else:
            kwargs['date'] = date if date is not None else configuration.config.default_start_time

        _symbol = symbol if symbol is not None else configuration.config.default_db_symbol
        kwargs.setdefault('propagate', kwargs.get('propagate_ticks', False))

        res = self._session_handler(write_to_db,
                                    src=src,
                                    dest=self.name,
                                    symbol=_symbol,
                                    tick_type=tick_type,
                                    timezone=timezone,
                                    **kwargs)

        # We need to keep backward-compatibility,
        # because before there was no ability to get written ticks
        if kwargs.get('propagate'):
            return res
        else:
            return None

    @property
    def properties(self):
        """
        Get dict of database properties.

        Returns
        -------
        dict

        Examples
        --------
        >>> otp.DB('X').properties  # doctest: +SKIP
        {'symbology': 'BZX',
         'archive_compression_type': 'NATIVE_PLUS_GZIP',
         'tick_timestamp_type': 'NANOS'}
        """
        return self._db_properties

    @property
    def locations(self):
        """
        Get list of database locations.

        Returns
        -------
        list of dict

        Examples
        --------
        >>> otp.DB('X').locations # doctest:+ELLIPSIS
        [{'access_method': 'file',
          'start_time': '20021230000000',
          'end_time': '21000101000000',
          ...}]
        """
        return self._db_locations

    @property
    def raw_data(self):
        """
        Get dict of database raw configurations.

        Returns
        -------
        dict of dict

        Examples
        --------
        >>> db = otp.DB('RAW_EXAMPLE',
        ...     db_raw_data=[{
        ...         'id': 'PRIMARY_A',
        ...         'prefix': 'DATA.',
        ...         'locations': [
        ...             {'mount': 'mount1'}
        ...         ]
        ...     }]
        ... )
        >>> db.raw_data # doctest:+ELLIPSIS
        [{'id': 'PRIMARY_A', 'prefix': 'DATA.', 'locations': [{'mount': 'mount1', 'access_method': 'file', ...}]}]
        """
        return self._db_raw_data

    @property
    def feed(self):
        """
        Get dict of database feed configuration.

        Returns
        -------
        dict

        Examples
        --------
        >>> db = otp.DB('RAW_EXAMPLE',
        ...     db_raw_data=[{
        ...         'id': 'PRIMARY_A',
        ...         'prefix': 'DATA.',
        ...         'locations': [
        ...             {'mount': 'mount1'}
        ...         ]
        ...     }],
        ...     db_feed={'type': 'rawdb', 'raw_source': 'PRIMARY_A'},
        ... )
        >>> db.feed
        {'type': 'rawdb', 'raw_source': 'PRIMARY_A', 'format': 'native'}
        """
        return self._db_feed

    @property
    def symbols(self):
        result = self._session_handler(self._symbols)
        return result if result else []

    def _session_handler(self, func, *args, **kwargs):
        """
        Handler to check if database is already in locator and
        run function with separate session or using current

        :param func: function to run
        """
        __result = None

        _session = session.Session._instance
        close_session = False
        _remove_from_locator = False
        _remove_from_acl = False
        if _session is None:
            close_session = True
            _session = session.Session()

        try:
            if self._LOCAL:
                if self.id not in _session.locator.databases:
                    if self.id != self.name and not otp.compatibility.is_supported_reload_locator_with_derived_db():
                        # Derived DB
                        raise ValueError(
                            "You need include derived DB into the session use the .use method before adding "
                            " data there."
                        )

                    _remove_from_locator = True
                    _session.locator.add(self)
                if self.id not in _session.acl.databases:
                    _remove_from_acl = True
                    _session.acl.add(self)
            __result = func(*args, **kwargs)
        finally:
            if close_session:
                _session.close()
            else:
                if self._LOCAL:
                    if _remove_from_locator:
                        _session.locator.remove(self)
                    if _remove_from_acl:
                        _session.acl.remove(self)

        return __result

    def _symbols(self):
        src = sources.Symbols(self)
        symbols = otp.run(src)
        result = []
        if symbols.empty:
            return []
        for s in list(symbols["SYMBOL_NAME"]):
            result.append(s.split(":")[-1])
        return result

    def __repr__(self):
        return "DB: " + self.name

    def __str__(self):
        return self.name


class DB(_DB):
    """
    A class representing OneTick databases when configuring
    :py:class:`locators <onetick.py.session.Locator>` and :py:class:`ACL <onetick.py.session.ACL>`.

    A database can point to an existing OneTick archive
    or the temporary directory can be created with the data provided.

    This class object can then be :py:meth:`used <onetick.py.Session.use>`
    in :py:class:`onetick.py.Session`.

    The data to add to the database can be passed into the
    constructor with parameter ``src`` or later with the :py:meth:`add` method.

    Note that after ticks were written to a particular timestamps in the database,
    they can't be updated for the same timestamps.

    Note
    ----
    This class can only be used to create database on the local machine.
    Database is created by using/creating a directory in the local filesystem
    and adding entries to the OneTick locator and ACL local files.
    This class can't be used to manage remote databases.

    Parameters
    ----------
    name : str
        Database name.
        Derived databases are specified in "parent//derived" format.
        A derived database inherits the parent database properties.
    src : :py:class:`~onetick.py.Source`, optional
        Data to add to the database.
    date : :py:class:`onetick.py.date` or :py:class:`datetime.date`, optional
        ``src`` data will be added to this date.
        Can be set only if ``src`` is set.
        Default value is the same as in :py:meth:`add` method.
    symbol : str, optional
        Symbol name to add ``src`` data for.
        Can be set only if ``src`` is set.
        Default value is the same as in :py:meth:`add` method.
    tick_type : str, optional
        Tick type to add ``src`` data for.
        Can be set only if ``src`` is set.
        Default value is the same as in :py:meth:`add` method.
    db_properties : :obj:`dict`, optional
        Properties of the database to add to the locator
    db_locations : :obj:`list` of :obj:`dict`, optional
        A list of locations for the database to add to the locator.
        This parameter is a list, because databases in a locator can have several location sections.
        If not specified, a temporary directory is used as the database location.
    db_raw_data: :obj:`list` of :obj:`dict`, optional
        Raw database configuration.
    db_feed: dict, optional
        Feed configuration.
    write : bool, optional
        Flag that controls access to write to database.
    clean_up : bool, optional
        Flag that controls temporary database cleanup
    destroy_access : bool, optional
        Flag that controls access to destroy the database.
    minimum_start_date: str or :py:class:`datetime.date` or :py:class:`onetick.py.date`
        Specifies the minimum date of the tick that can be served to a user.
        The format for the value is *YYYYMMDD*.
        The OneTick server enforces this permission by verifying that
        the query start time is not less than time of 00:00:00, in GMT timezone, for the specified minimum start date.
    maximum_end_date: str or :py:class:`datetime.date` or :py:class:`onetick.py.date`
        Specifies the date of the tick, starting from which ticks are not allowed to be returned to a user.
        The format for the value is *YYYYMMDD*.
        The OneTick server enforces this permission by verifying that
        the query end time is less than time of 00:00:00, in GMT timezone, for the specified maximum end date.

    Examples
    --------

    A database can be initialized along with data:

    >>> data = otp.Ticks(X=['hello', 'world!'])
    >>> db = otp.DB('MYDB', data)

    Database symbol name, tick type and date to which the data will be written can be changed like this:

    >>> db = otp.DB('MYDB', data, symbol='S_S', tick_type='T_T', date=otp.dt(2003, 12, 1))

    You can specify a derived db by using ``//`` as a separator:

    >>> data = otp.Ticks(X=['parent1', 'parent2'])
    >>> db = otp.DB('DB_A', data)
    >>> db.add(data)

    >>> data_derived = otp.Ticks(X=['derived1', 'derived2'])
    >>> db_derived = otp.DB('DB_A//DB_D')
    >>> session.use(db_derived)
    >>> db_derived.add(data_derived)

    You can add an existing OneTick database to the locator or create a new one:

    >>> existing_db = otp.DB('MY_US_COMP',  # doctest: +SKIP
    ...                      db_locations=[{'location': '/home/user/data/US_COMP',
    ...                                     'start_time': datetime(2003, 1, 1),
    ...                                     'end_time': datetime(2010, 1, 1),
    ...                                     'day_boundary_tz': 'EST5EDT'}])
    >>> session.use(existing_db)  # doctest: +SKIP
    """

    _LOCAL = True

    def __init__(
        self,
        name=None,
        src=None,
        date=None,
        symbol=None,
        tick_type=None,
        kind='archive',
        db_properties=None,
        db_locations=None,
        db_raw_data=None,
        db_feed=None,
        write=True,
        clean_up=utils.default,
        destroy_access=False,
        minimum_start_date=None,
        maximum_end_date=None,
    ):
        if name is not None and not isinstance(name, str):
            message = f"Database name expected to be string got {type(name)}"
            logging.error(message)
            raise TypeError(message)

        self._clean_up = clean_up
        self._destroy_access = destroy_access
        self._path = None
        self._db_suffix = ""

        if name:
            self._db_suffix = name
        else:
            # Mostly for temporary databases
            name = uuid4().hex.upper()
            self._db_suffix = "db_" + name

        db_properties = self._prepare_db_properties(db_properties)
        db_day_boundary_tz_set = 'day_boundary_tz' in db_properties.keys()
        db_locations = self._prepare_db_locations(db_locations,
                                                  db_day_boundary_tz_set=db_day_boundary_tz_set,
                                                  kind=kind)
        db_raw_data = self._prepare_db_raw_data(db_raw_data, db_properties)
        db_feed = self._prepare_db_feed(db_feed)

        if isinstance(src, pandas.DataFrame):
            csv_path = os.path.join(self._tmp_dir.path, uuid4().hex.upper() + ".csv")
            src.to_csv(csv_path, index=False)
            if otp.__webapi__:
                src = sources.CSV(utils.FileBuffer(csv_path))
            else:
                src = sources.CSV(csv_path)

        super().__init__(
            name=name,
            src=src,
            date=date,
            symbol=symbol,
            tick_type=tick_type,
            db_properties=db_properties,
            db_locations=db_locations,
            db_raw_data=db_raw_data,
            db_feed=db_feed,
            write=write,
            minimum_start_date=minimum_start_date,
            maximum_end_date=maximum_end_date,
        )

    def _prepare_db_properties(self, properties):
        if properties is None:
            properties = {}

        # convert all property keys to lowercase
        properties = {k.lower(): v for k, v in properties.items()}

        # set default properties if they are not specified
        properties.setdefault("symbology", configuration.config.default_symbology)
        properties.setdefault("tick_timestamp_type", "NANOS")

        if is_native_plus_zstd_supported():
            properties.setdefault("archive_compression_type", constants.compression_type.NATIVE_PLUS_ZSTD)
        else:
            properties.setdefault("archive_compression_type", constants.compression_type.NATIVE_PLUS_GZIP)

        return self._format_params(properties)

    def _create_db(self):
        logging.debug(f'Creating temporary directory for db "{self._db_suffix}"')

        dirs_list = self._db_suffix.replace("//", " DERIVED ").split()
        dir_name = ''
        base_dir = ""
        if os.getenv('OTP_WEBAPI_TEST_MODE'):
            # copied from onetick.test.fixtures _keep_generated_dir()
            base_dir = os.path.join(otp.utils.TMP_CONFIGS_DIR(), os.environ.get("ONE_TICK_TMP_DIR", "dbs"))
        for cur_dir in dirs_list:
            dir_name = os.path.join(dir_name, cur_dir)
            self._tmp_dir = utils.TmpDir(dir_name, clean_up=self._clean_up, base_dir=base_dir)
            if not self._path:
                self._path = self._tmp_dir.path

    def _prepare_db_locations(self, locations, db_day_boundary_tz_set=None, kind=None, default_location=None):
        if not locations:
            locations = [{}]
        result = []
        # set default properties if they are not specified
        for location in locations:
            location.setdefault("access_method", constants.access_method.FILE)
            location.setdefault("start_time", constants.DEFAULT_START_DATE - timedelta(days=2))
            location.setdefault("end_time", constants.DEFAULT_END_DATE + timedelta(days=1))
            if not db_day_boundary_tz_set and db_day_boundary_tz_set is not None:
                # If the day_boundary_tz is not set database-wide, then we want it to have
                # a default value for each location
                day_boundary_tz = utils.default_day_boundary_tz(self._db_suffix)
                if day_boundary_tz:
                    location.setdefault("day_boundary_tz", day_boundary_tz)

            if 'location' not in location:
                methods = {constants.access_method.SOCKET, constants.access_method.MEMORY}
                if location['access_method'] in methods:
                    raise ValueError("Parameter 'location' must be specified when parameter"
                                     f" 'access_method' is set to {methods}")
                if not default_location:
                    self._create_db()
                    location['location'] = self._path
                else:
                    location['location'] = default_location

            if kind == 'accelerator':
                location.setdefault("archive_duration", "continuous")
                location.setdefault("growable_archive", "true")
            # TODO: think what to do if there will be several locations

            result.append(location)

        return list(map(self._format_params, result))

    def _prepare_db_raw_data(self, raw_data, db_properties):
        if not raw_data:
            return []

        raw_ids = set()
        auto_discover_mounts = db_properties.get('auto_discover_mounts', '').lower() == 'yes'
        default_location = None

        for raw_db in raw_data:
            raw_db.setdefault('id', 'PRIMARY')
            if raw_db['id'] in raw_ids:
                raise ValueError("Parameter 'id' must be set and must be unique for raw databases")
            raw_ids.add(raw_db['id'])

            if 'prefix' not in raw_db:
                raise ValueError("Parameter 'prefix' must be specified for raw database")

            if self._path is not None and default_location is None:
                default_location = utils.TmpDir(rel_path='raw', base_dir=self._path, clean_up=self._clean_up).path
            raw_db['locations'] = self._prepare_db_locations(raw_db.get('locations'),
                                                             db_day_boundary_tz_set=None,
                                                             default_location=default_location)
            if auto_discover_mounts and len(raw_db['locations']) > 1:
                raise ValueError("Only one location must be specified for raw database"
                                 " when parameter 'auto_discover_mounts' is specified for database")
            for location in raw_db['locations']:
                if 'mount' not in location and not auto_discover_mounts:
                    raise ValueError("Parameter 'mount' must be specified for raw database location")
                if 'mount' in location and auto_discover_mounts:
                    raise ValueError("Parameter 'mount' must not be specified for raw database location"
                                     " when parameter 'auto_discover_mounts' is specified for database")
        return raw_data

    def _prepare_db_feed(self, feed):
        if not feed:
            return {}
        if 'type' not in feed:
            raise ValueError("Parameter 'type' must be specified for database feed")
        if feed['type'] == 'rawdb':
            feed.setdefault('format', 'native')
            formats = ('native', 'rt', 'ascii', 'xml')
            if feed['format'] not in formats:
                raise ValueError(f"Parameter 'format' must be one of {formats}")
            feed.setdefault('raw_source', 'PRIMARY')
        return self._format_params(feed)


class RefDB(DB):
    """ Creates reference database object.

    Parameters
    ----------
    name : str
        Database name
    clean_up : bool, optional
        Flag that controls temporary database cleanup
    db_properties : :obj:`dict`, optional
        Properties of database to add to locator
    db_location : :obj:`dict`, optional
        Location of database to add to locator. Reference database must have a single location,
        pointing to a continuous archive database.
    write : bool, optional
        Flag that controls access to write to database
    destroy_access : bool, optional
        Flag that controls access to destroy to database

    Examples
    --------

    >>> properties = {'symbology': 'TICKER'}
    >>> location = {'archive_duration': 'continuous'}
    >>> ref_db = otp.RefDB('REF_DATA_MYDB', db_properties=properties, db_location=location)
    >>> session.use(ref_db)
    >>>
    >>> data = 'A||20100102000000|20100103000000|B||20100103000000|20100104000000|'
    >>> out, err = ref_db.put([otp.RefDB.SymbolNameHistory(data, 'TICKER')])
    >>> b'Total ticks 8' in err and b'Total symbols 6' in err
    True
    >>>
    >>> properties = {'ref_data_db': ref_db.name, 'symbology': 'TICKER'}
    >>> db = otp.DB('MYDB', db_properties=properties)
    >>> session.use(db)
    >>>
    >>> data = otp.Ticks(X=['hello'], start=otp.dt(2010, 1, 2), end=otp.dt(2010, 1, 3))
    >>> data = otp.run(data.write(db.name, 'A', 'MSG', date=otp.dt(2010, 1, 2)))
    >>> data = otp.Ticks(X=['world!'], start=otp.dt(2010, 1, 3), end=otp.dt(2010, 1, 4))
    >>> data = otp.run(data.write(db.name, 'B', 'MSG', date=otp.dt(2010, 1, 3)))
    >>>
    >>> data = otp.DataSource(db.name, tick_type='MSG')
    >>> s_dt, e_dt, symbol_date = otp.dt(2010, 1, 1), otp.dt(2010, 1, 4), otp.dt(2010, 1, 2)
    >>> otp.run(data, symbols='A', start=s_dt, end=e_dt, symbol_date=symbol_date)
            Time       X
    0 2010-01-02   hello
    1 2010-01-03  world!
    """

    def __init__(
        self,
        name=None,
        kind='archive',
        db_properties=None,
        db_location=None,
        write=True,
        clean_up=utils.default,
        destroy_access=False,
    ):
        # ref db must have a single location, pointing to a continuous archive database
        # (its location in the locator file must have archive_duration=continuous set)
        if db_location is None:
            db_location = {}
        db_location.setdefault('archive_duration', 'continuous')
        super().__init__(
            name=name,
            kind=kind,
            db_properties=db_properties,
            db_locations=[db_location],
            write=write,
            clean_up=clean_up,
            destroy_access=destroy_access,
        )

    class Section():
        """ Specification of a reference database section. Section content can be specified as a string or source.
        The format of string and output columns of source must correspond with the section documentation.

        Parameters
        ----------
        name : str
            Section name
        data : str or :class:`otp.Source`
            Content of the section
        attrs : :obj:`dict`, optional
            Attributes of the section

        Examples
        --------

        Data provided as a string:

        >>> data = 'SYM1|20100101093000|20100101110000' + os.linesep
        >>> data += 'SYM2|20100101110000|20100103140000'
        >>> section = otp.RefDB.Section('SECTION_NAME', data, {'ATTR1': 'VAL1', 'ATTR2': 'VAL2'})
        >>> print(section)
        <SECTION_NAME ATTR1="VAL1" ATTR2="VAL2">
        SYM1|20100101093000|20100101110000
        SYM2|20100101110000|20100103140000
        </SECTION_NAME>

        Data provided as a :class:`otp.Source`:

        >>> data = dict()
        >>> data['SYMBOL_NAME'] = ['SYM1', 'SYM2']
        >>> data['START_DATETIME'] = [otp.dt(2010, 1, 1, 9, 30, tz='EST5EDT'), otp.dt(2010, 1, 1, 11, tz='EST5EDT')]
        >>> data['END_DATETIME'] = [otp.dt(2010, 1, 1, 11, tz='EST5EDT'), otp.dt(2010, 1, 3, 14, tz='EST5EDT')]
        >>> ticks = otp.Ticks(**data, offset=[0] * 2, db='LOCAL')
        >>> section = otp.RefDB.Section('SECTION_NAME', ticks, {'ATTR1': 'VAL1', 'ATTR2': 'VAL2'})
        >>> print(section) # doctest:+ELLIPSIS
        <SECTION_NAME ATTR1="VAL1" ATTR2="VAL2" OTQ_QUERY=...>
        </SECTION_NAME>

        where OTQ_QUERY is path to :class:`otp.Source`, dumped to disk as temporary .otq file.
        """
        # Read ref db guide for details on input format of sections
        # http://solutions.pages.soltest.onetick.com/iac/onetick-server/ReferenceDatabaseGuide.html
        def __init__(self, name: str, data: Union[str, 'otp.Source'], attrs: Optional[dict] = None):
            self._name = name
            self._data = data
            self._attrs = ' '.join([f'{name}="{value}"' for name, value in attrs.items()]) if attrs else ''

        def __str__(self):
            if isinstance(self._data, str):
                return f'<{self._name} {self._attrs}>{os.linesep}{self._data}{os.linesep}</{self._name}>'
            otq = self._data.to_otq()
            return f'<{self._name} {self._attrs} OTQ_QUERY={otq}>{os.linesep}</{self._name}>'

    class SymbolNameHistory(Section):
        """ Describes symbol changes for the same security. The continuity can be expressed in terms of any symbol type
        and can be specified on the security level or the security+exchange level (more explicit).

        Examples
        --------

        >>> data = 'CORE_A||20100101093000|20100101110000|CORE_B||20100101110000|20100103140000|'
        >>> section = otp.RefDB.SymbolNameHistory(data, symbology='CORE')
        >>> print(section)
        <SYMBOL_NAME_HISTORY SYMBOLOGY="CORE">
        CORE_A||20100101093000|20100101110000|CORE_B||20100101110000|20100103140000|
        </SYMBOL_NAME_HISTORY>

        Equivalent :class:`otp.Source`:

        >>> data = dict()
        >>> data['SYMBOL_NAME'] = ['CORE_A'] * 2
        >>> data['SYMBOL_NAME_IN_HISTORY'] = ['CORE_A', 'CORE_B']
        >>> data['SYMBOL_START_DATETIME'] = [otp.dt(2010, 1, 2, tz='EST5EDT')] * 2
        >>> data['SYMBOL_END_DATETIME'] = [otp.dt(2010, 1, 5, tz='EST5EDT')] * 2
        >>> data['START_DATETIME'] = [otp.dt(2010, 1, 2, tz='EST5EDT'), otp.dt(2010, 1, 3, tz='EST5EDT')]
        >>> data['END_DATETIME'] = [otp.dt(2010, 1, 3, tz='EST5EDT'), otp.dt(2010, 1, 4, tz='EST5EDT')]
        >>> ticks = otp.Ticks(**data, offset=[0] * 2, db='LOCAL')
        >>> section = otp.RefDB.SymbolNameHistory(ticks, symbology='CORE')
        >>> print(section) # doctest:+ELLIPSIS
        <SYMBOL_NAME_HISTORY SYMBOLOGY="CORE" OTQ_QUERY=...>
        </SYMBOL_NAME_HISTORY>
        """
        def __init__(self, data: Union[str, 'otp.Source'], symbology: str):
            super().__init__('SYMBOL_NAME_HISTORY', data, {'SYMBOLOGY': symbology})

    class SymbologyMapping(Section):
        """ Describes a history of mapping of symbols of one symbology to the symbols of another symbology.

        Examples
        --------

        >>> data = 'A||20100101093000|20100101110000|CORE_A|' + os.linesep
        >>> data += 'B||20100101110000|20100103140000|CORE_B|'
        >>> section = otp.RefDB.SymbologyMapping(data, source_symbology='TICKER', dest_symbology='CORE')
        >>> print(section)
        <SYMBOLOGY_MAPPING SOURCE_SYMBOLOGY="TICKER" DEST_SYMBOLOGY="CORE">
        A||20100101093000|20100101110000|CORE_A|
        B||20100101110000|20100103140000|CORE_B|
        </SYMBOLOGY_MAPPING>

        Equivalent :class:`otp.Source`:

        >>> data = dict()
        >>> data['SYMBOL_NAME'] = ['A', 'B']
        >>> data['MAPPED_SYMBOL_NAME'] = ['CORE_A', 'CORE_B']
        >>> data['START_DATETIME'] = [otp.dt(2010, 1, 2, tz='EST5EDT'), otp.dt(2010, 1, 3, tz='EST5EDT')]
        >>> data['END_DATETIME'] = [otp.dt(2010, 1, 3, tz='EST5EDT'), otp.dt(2010, 1, 4, tz='EST5EDT')]
        >>> ticks = otp.Ticks(**data, offset=[0] * 2, db='LOCAL')
        >>> section = otp.RefDB.SymbologyMapping(ticks, source_symbology='TICKER', dest_symbology='CORE')
        >>> print(section) # doctest:+ELLIPSIS
        <SYMBOLOGY_MAPPING SOURCE_SYMBOLOGY="TICKER" DEST_SYMBOLOGY="CORE" OTQ_QUERY=...>
        </SYMBOLOGY_MAPPING>
        """
        def __init__(self, data: Union[str, 'otp.Source'], source_symbology: str, dest_symbology: str):
            super().__init__('SYMBOLOGY_MAPPING', data, {'SOURCE_SYMBOLOGY': source_symbology,
                                                         'DEST_SYMBOLOGY': dest_symbology})

    class CorpActions(Section):
        """ Describes corporate actions. Used by OneTick to compute prices adjusted for various types of
        corporate actions. Supports both built-in and custom (user-defined) types of corporate actions.

        Examples
        --------

        >>> data = 'CORE_C||20100103180000|0.25|0.0|SPLIT'
        >>> section = otp.RefDB.CorpActions(data, symbology='CORE')
        >>> print(section)
        <CORP_ACTIONS SYMBOLOGY="CORE">
        CORE_C||20100103180000|0.25|0.0|SPLIT
        </CORP_ACTIONS>

        Equivalent :class:`otp.Source`:

        >>> data = dict()
        >>> data['SYMBOL_NAME'] = ['CORE_C']
        >>> data['EFFECTIVE_DATETIME'] = [otp.dt(2010, 1, 3, 18, tz='EST5EDT')]
        >>> data['MULTIPLICATIVE_ADJUSTMENT'] = [0.25]
        >>> data['ADDITIVE_ADJUSTMENT'] = [0.0]
        >>> data['ADJUSTMENT_TYPE_NAME'] = ['SPLIT']
        >>> ticks = otp.Ticks(**data, offset=[0], db='LOCAL')
        >>> section = otp.RefDB.CorpActions(ticks, symbology='CORE')
        >>> print(section) # doctest:+ELLIPSIS
        <CORP_ACTIONS SYMBOLOGY="CORE" OTQ_QUERY=...>
        </CORP_ACTIONS>
        """
        def __init__(self, data: Union[str, 'otp.Source'], symbology: str):
            super().__init__('CORP_ACTIONS', data, {'SYMBOLOGY': symbology})

    class ContinuousContracts(Section):
        """ Describes continuous contracts. Continuity is expressed in terms of stitched history
        of real contracts and rollover adjustments in between them and can be specified
        on the continuous contract level or continuous contract+exchange level (more explicit).

        Examples
        --------

        >>> data = 'CC||CORE_A||20100101093000|20100101110000|0.5|0|CORE_B||20100101110000|20100103140000'
        >>> section = otp.RefDB.ContinuousContracts(data, symbology='CORE')
        >>> print(section)
        <CONTINUOUS_CONTRACTS SYMBOLOGY="CORE">
        CC||CORE_A||20100101093000|20100101110000|0.5|0|CORE_B||20100101110000|20100103140000
        </CONTINUOUS_CONTRACTS>

        Equivalent :class:`otp.Source`:

        >>> data = dict()
        >>> data['CONTINUOUS_CONTRACT_NAME'] = ['CC'] * 2
        >>> data['SYMBOL_NAME'] = ['CORE_A', 'CORE_B']
        >>> data['START_DATETIME'] = [otp.dt(2010, 1, 2, tz='EST5EDT'), otp.dt(2010, 1, 3, tz='EST5EDT')]
        >>> data['END_DATETIME'] = [otp.dt(2010, 1, 3, tz='EST5EDT'), otp.dt(2010, 1, 4, tz='EST5EDT')]
        >>> data['MULTIPLICATIVE_ADJUSTMENT'] = [0.5, None]
        >>> data['ADDITIVE_ADJUSTMENT'] = [3, None]
        >>> ticks = otp.Ticks(**data, offset=[0] * 2, db='LOCAL')
        >>> section = otp.RefDB.ContinuousContracts(ticks, symbology='CORE')
        >>> print(section) # doctest:+ELLIPSIS
        <CONTINUOUS_CONTRACTS SYMBOLOGY="CORE" OTQ_QUERY=...>
        </CONTINUOUS_CONTRACTS>
        """
        def __init__(self, data: Union[str, 'otp.Source'], symbology: str):
            super().__init__('CONTINUOUS_CONTRACTS', data, {'SYMBOLOGY': symbology})

    class SymbolCurrency(Section):
        """ Specifies symbols' currencies in 3-letter ISO codes for currencies. These are used for currency conversion
        (e.g., when calculating portfolio price for a list of securities with different currencies).

        Examples
        --------

        >>> data = 'CORE_A||20100101093000|20100101110000|USD|1.0' + os.linesep
        >>> data += 'CORE_B||20100101110000|20100103140000|RUB|1.8'
        >>> section = otp.RefDB.SymbolCurrency(data, symbology='CORE')
        >>> print(section)
        <SYMBOL_CURRENCY SYMBOLOGY="CORE">
        CORE_A||20100101093000|20100101110000|USD|1.0
        CORE_B||20100101110000|20100103140000|RUB|1.8
        </SYMBOL_CURRENCY>

        Equivalent :class:`otp.Source`:

        >>> data = dict()
        >>> data['SYMBOL_NAME'] = ['CORE_A', 'CORE_B',]
        >>> data['CURRENCY'] = ['USD', 'RUB']
        >>> data['MULTIPLIER'] = [1., 1.8]
        >>> data['START_DATETIME'] = [otp.dt(2010, 1, 1, 9, 30, tz='EST5EDT'), otp.dt(2010, 1, 1, 11, tz='EST5EDT')]
        >>> data['END_DATETIME'] = [otp.dt(2010, 1, 1, 11, tz='EST5EDT'), otp.dt(2010, 1, 3, 14, tz='EST5EDT')]
        >>> ticks = otp.Ticks(**data, offset=[0] * 2, db='LOCAL')
        >>> section = otp.RefDB.SymbolCurrency(ticks, symbology='CORE')
        >>> print(section) # doctest:+ELLIPSIS
        <SYMBOL_CURRENCY SYMBOLOGY="CORE" OTQ_QUERY=...>
        </SYMBOL_CURRENCY>
        """
        def __init__(self, data: Union[str, 'otp.Source'], symbology: str):
            super().__init__('SYMBOL_CURRENCY', data, {'SYMBOLOGY': symbology})

    class Calendar(Section):
        """ Specifies a named calendar. Needed to analyze tick data during specific market time intervals (i.e., during
        normal trading hours). Can either be used directly in queries as described below, or referred to
        from the SYMBOL_CALENDAR and EXCH_CALENDAR sections.

        Examples
        --------

        >>> data = 'CAL1|20100101093000|20100101110000|Regular|R|0.0.12345|093000|160000|GMT|1|DESCRIPTION1'
        >>> data += os.linesep
        >>> data += 'CAL2|20100101110000|20100103140000|Holiday|F|0.0.12345|094000|170000|GMT|0|DESCRIPTION2'
        >>> section = otp.RefDB.Calendar(data)
        >>> print(section)
        <CALENDAR >
        CAL1|20100101093000|20100101110000|Regular|R|0.0.12345|093000|160000|GMT|1|DESCRIPTION1
        CAL2|20100101110000|20100103140000|Holiday|F|0.0.12345|094000|170000|GMT|0|DESCRIPTION2
        </CALENDAR>

        Equivalent :class:`otp.Source`:

        >>> data = dict()
        >>> data['CALENDAR_NAME'] = ['CAL1', 'CAL2']
        >>> data['START_DATETIME'] = [otp.dt(2010, 1, 1, 9, 30, tz='EST5EDT'), otp.dt(2010, 1, 1, 11, tz='EST5EDT')]
        >>> data['END_DATETIME'] = [otp.dt(2010, 1, 1, 11, tz='EST5EDT'), otp.dt(2010, 1, 3, 14, tz='EST5EDT')]
        >>> data['SESSION_NAME'] = ['Regular', 'Holiday']
        >>> data['SESSION_FLAGS'] = ['R', 'H']
        >>> data['DAY_PATTERN'] = ['0.0.12345', '0.0.12345']
        >>> data['START_HHMMSS'] = ['093000', '094000']
        >>> data['END_HHMMSS'] = ['160000', '170000']
        >>> data['TIMEZONE'] = ['GMT', 'GMT']
        >>> data['PRIORITY'] = [1, 0]
        >>> data['DESCRIPTION'] = ['DESCRIPTION1', 'DESCRIPTION2']
        >>> ticks = otp.Ticks(**data, offset=[0] * 2, db='LOCAL')
        >>> section = otp.RefDB.Calendar(ticks)
        >>> print(section) # doctest:+ELLIPSIS
        <CALENDAR  OTQ_QUERY=...>
        </CALENDAR>
        """
        def __init__(self, data: Union[str, 'otp.Source']):
            super().__init__('CALENDAR', data)

    class SymbolCalendar(Section):
        """ Specifies a calendar for a symbol. Needed to analyze tick data during specific market time intervals
        (i.e., during normal trading hours). Can either be specified directly or refer to a named calendar by its name
        (see the CALENDAR section).

        Examples
        --------

        Symbol calendar section, referring to named calendar section:

        >>> data = 'CORE_A|20100101093000|20100101110000|CAL1' + os.linesep
        >>> data += 'CORE_B|20100101110000|20100103140000|CAL2'
        >>> section = otp.RefDB.SymbolCalendar(data, symbology='CORE')
        >>> print(section)
        <SYMBOL_CALENDAR SYMBOLOGY="CORE">
        CORE_A|20100101093000|20100101110000|CAL1
        CORE_B|20100101110000|20100103140000|CAL2
        </SYMBOL_CALENDAR>

        Equivalent :class:`otp.Source`:

        >>> data = dict()
        >>> data['SYMBOL_NAME'] = ['CORE_A', 'CORE_B']
        >>> data['START_DATETIME'] = [otp.dt(2010, 1, 1, 9, 30, tz='EST5EDT'), otp.dt(2010, 1, 1, 11, tz='EST5EDT')]
        >>> data['END_DATETIME'] = [otp.dt(2010, 1, 1, 11, tz='EST5EDT'), otp.dt(2010, 1, 3, 14, tz='EST5EDT')]
        >>> data['CALENDAR_NAME'] = ['CAL1', 'CAL2']
        >>> ticks = otp.Ticks(**data, offset=[0] * 2, db='LOCAL')
        >>> section = otp.RefDB.SymbolCalendar(ticks, symbology='CORE')
        >>> print(section) # doctest:+ELLIPSIS
        <SYMBOL_CALENDAR SYMBOLOGY="CORE" OTQ_QUERY=...>
        </SYMBOL_CALENDAR>

        Symbol calendar section without using named calendar section:

        >>> data = 'CORE_A|20100101093000|20100101110000|Regular|R|0.0.12345|093000|160000|EST5EDT|1|' + os.linesep
        >>> data += 'CORE_B|20100101110000|20100103140000|Regular|F|0.0.12345|093000|160000|EST5EDT|1|'
        >>> section = otp.RefDB.SymbolCalendar(data, symbology='CORE')
        >>> print(section)
        <SYMBOL_CALENDAR SYMBOLOGY="CORE">
        CORE_A|20100101093000|20100101110000|Regular|R|0.0.12345|093000|160000|EST5EDT|1|
        CORE_B|20100101110000|20100103140000|Regular|F|0.0.12345|093000|160000|EST5EDT|1|
        </SYMBOL_CALENDAR>

        Equivalent :class:`otp.Source`:

        >>> data = dict()
        >>> data['SYMBOL_NAME'] = ['CORE_A', 'CORE_B']
        >>> data['START_DATETIME'] = [otp.dt(2010, 1, 1, 9, 30, tz='EST5EDT'), otp.dt(2010, 1, 1, 11, tz='EST5EDT')]
        >>> data['END_DATETIME'] = [otp.dt(2010, 1, 1, 11, tz='EST5EDT'), otp.dt(2010, 1, 3, 14, tz='EST5EDT')]
        >>> data['SESSION_NAME'] = ['Regular', 'Regular']
        >>> data['SESSION_FLAGS'] = ['R', 'F']
        >>> data['DAY_PATTERN'] = ['0.0.12345', '0.0.12345']
        >>> data['START_HHMMSS'] = ['093000', '160000']
        >>> data['END_HHMMSS'] = ['CAL1', 'CAL2']
        >>> data['TIMEZONE'] = ['EST5EDT', 'EST5EDT']
        >>> data['PRIORITY'] = [1, 1]
        >>> data['DESCRIPTION'] = ['', '']
        >>> ticks = otp.Ticks(**data, offset=[0] * 2, db='LOCAL')
        >>> section = otp.RefDB.SymbolCalendar(ticks, symbology='CORE')
        >>> print(section) # doctest:+ELLIPSIS
        <SYMBOL_CALENDAR SYMBOLOGY="CORE" OTQ_QUERY=...>
        </SYMBOL_CALENDAR>
        """
        def __init__(self, data: Union[str, 'otp.Source'], symbology: str):
            super().__init__('SYMBOL_CALENDAR', data, {'SYMBOLOGY': symbology})

    class SectionStr(Section):
        """ Specification of a reference database section that can be specified only as a string.
        Section content still can be provided as a :class:`otp.Source`, but the :class:`otp.Source` is executed and
        result data is used as string in section. It's up to user to provide :class:`otp.Source` with correct number
        and order of columns.

        Examples
        --------

        Data provided as a string returns the same result as :class:`otp.RefDB.Section`.

        Data provided as a :class:`otp.Source`:

        >>> data = dict()
        >>> data['SYMBOL_NAME'] = ['SYM1', 'SYM2']
        >>> data['START_DATETIME'] = [otp.dt(2010, 1, 1, 9, 30, tz='EST5EDT'), otp.dt(2010, 1, 1, 11, tz='EST5EDT')]
        >>> data['END_DATETIME'] = [otp.dt(2010, 1, 1, 11, tz='EST5EDT'), otp.dt(2010, 1, 3, 14, tz='EST5EDT')]
        >>> ticks = otp.Ticks(**data, offset=[0] * 2, db='LOCAL')
        >>> ticks = ticks.table(SYMBOL_NAME=otp.string[128], START_DATETIME=otp.msectime, END_DATETIME=otp.msectime)
        >>> section = otp.RefDB.SectionStr('SECTION_NAME', ticks, {'ATTR1': 'VAL1', 'ATTR2': 'VAL2'})
        >>> print(section) # doctest:+ELLIPSIS
        <SECTION_NAME ATTR1="VAL1" ATTR2="VAL2">
        SYM1|20100101093000|20100101110000
        SYM2|20100101110000|20100103140000
        </SECTION_NAME>

        where OTQ_QUERY is path to :class:`otp.Source`, dumped to disk as temporary .otq file.
        """
        def __init__(self, name: str, data: Union[str, 'otp.Source'], attrs: Optional[dict] = None):
            data_str = data if isinstance(data, str) else self._source_to_str(data)
            super().__init__(name, data_str, attrs)

        def _source_to_str(self, data: 'otp.Source'):
            df = otp.run(data)
            df.drop(columns=['Time'], inplace=True)
            df = df.to_csv(sep='|', header=False, index=False, date_format='%Y%m%d%H%M%S')
            return df

    class PrimaryExchange(SectionStr):
        """ Specifies symbols' primary exchanges. Used to extract and analyze tick data for a security on
        its primary exchange, without having to explicitly specify the name of the primary exchange.

        Examples
        --------

        >>> data = 'A||19991118000000|99999999000000|N|'
        >>> data += os.linesep
        >>> data += 'AA||19991118000000|99999999000000|N|AA.N'
        >>> section = otp.RefDB.PrimaryExchange(data, symbology='TICKER')
        >>> print(section)
        <PRIMARY_EXCHANGE SYMBOLOGY="TICKER">
        A||19991118000000|99999999000000|N|
        AA||19991118000000|99999999000000|N|AA.N
        </PRIMARY_EXCHANGE>

        Equivalent query should return the same data values in the same order. Column names does not matter.
        """
        def __init__(self, data: Union[str, 'otp.Source'], symbology: str):
            super().__init__('PRIMARY_EXCHANGE', data, {'SYMBOLOGY': symbology})

    class ExchCalendar(SectionStr):
        """ Specifies symbols' primary exchanges. Used to extract and analyze tick data for a security on
        its primary exchange, without having to explicitly specify the name of the primary exchange.

        Examples
        --------

        >>> data = 'NYSE||19600101000000|20501231235959|Regular|R|0.0.12345|093000|160000|EST5EDT|'
        >>> data += os.linesep
        >>> data += 'NYSE||19600101000000|20501231235959|Half-day|RL|12/31|093000|130000|EST5EDT|'
        >>> data += os.linesep
        >>> data += 'NYSE||19600101000000|20501231235959|Holiday|H|01/01|000000|240000|EST5EDT|'
        >>> section = otp.RefDB.ExchCalendar(data, symbology='MIC')
        >>> print(section)
        <EXCH_CALENDAR SYMBOLOGY="MIC">
        NYSE||19600101000000|20501231235959|Regular|R|0.0.12345|093000|160000|EST5EDT|
        NYSE||19600101000000|20501231235959|Half-day|RL|12/31|093000|130000|EST5EDT|
        NYSE||19600101000000|20501231235959|Holiday|H|01/01|000000|240000|EST5EDT|
        </EXCH_CALENDAR>

        If a CALENDAR section is used:

        >>> data = 'LSE||19600101000000|20501231235959|WNY'
        >>> section = otp.RefDB.ExchCalendar(data, symbology='MIC')
        >>> print(section)
        <EXCH_CALENDAR SYMBOLOGY="MIC">
        LSE||19600101000000|20501231235959|WNY
        </EXCH_CALENDAR>

        Equivalent query should return the same data values in the same order. Column names does not matter.
        """
        def __init__(self, data: Union[str, 'otp.Source'], symbology: str):
            super().__init__('EXCH_CALENDAR', data, {'SYMBOLOGY': symbology})

    class SymbolExchange(SectionStr):
        """ Specifies the exchange where a security is traded. Needs to be provided for the symbologies where
        the symbol name is unique across all exchanges.

        Examples
        --------

        >>> data = 'IBM.N|19980825000000|20501231235959|NYSE||'
        >>> section = otp.RefDB.SymbolExchange(data, symbology='RIC', exchange_symbology='MIC')
        >>> print(section)
        <SYMBOL_EXCHANGE SYMBOLOGY="RIC" EXCHANGE_SYMBOLOGY="MIC">
        IBM.N|19980825000000|20501231235959|NYSE||
        </SYMBOL_EXCHANGE>

        Equivalent query should return the same data values in the same order. Column names does not matter.
        """
        def __init__(self, data: Union[str, 'otp.Source'], symbology: str, exchange_symbology: str):
            super().__init__('SYMBOL_EXCHANGE', data, {'SYMBOLOGY': symbology,
                                                       'EXCHANGE_SYMBOLOGY': exchange_symbology})

    def put(
        self,
        src: Union[str, List[Section]],
        tickdb_symbology: Optional[List[str]] = None,
        delta_mode: bool = False,
        full_integrity_check: bool = False,
        load_by_sections: bool = True,
    ):
        """
        Loads data in database with reference_data_loader.exe.
        If db properties contain SUPPORT_DELTAS=YES, delta_mode set to True, and proper delta file is used
        then data is loaded in incremental mode (in other words, replace or modification mode).
        If the above conditions are not met, reference database content is entirely rewritten with the new data.

        Parameters
        ----------
        src : str, list of str or otp.RefDB.Section
            Path to data file, or list of data per section in specified format
        tickdb_symbology : list of str, optional
            All symbologies for which the reference data needs to be generated
        delta_mode : bool, default is False
            If set to True loader will perform incremental load. Cannot be used if ``tickdb_symbology`` is specified
        full_integrity_check : bool, default is False
            If set to True loader checks all mappings to symbologies with symbol name history section
            and gives warning if mapped securities do not have symbol name history
        load_by_sections : bool, default is True
            If set to True loader will perform input data file splitting by data types and symbologies
            to load each part separately instead loading the entire file at once
        """
        # More info:
        # http://solutions.pages.soltest.onetick.com/iac/onetick-server/reference_data_loader.html - loader doc
        # https://onemarketdata.atlassian.net/browse/KB-286 - details on delta mode
        return self._session_handler(
            self._put,
            src=src,
            delta_mode=delta_mode,
            full_integrity_check=full_integrity_check,
            load_by_sections=load_by_sections,
            tickdb_symbology=tickdb_symbology,
        )

    def _prepare_data_file(self, src):
        if isinstance(src, str):
            return src

        data = f'<VERSION_INFO VERSION="1">{os.linesep}</VERSION_INFO>'
        for section in src:
            data += f'{os.linesep}{os.linesep}{section}'

        data_file = utils.TmpFile(suffix='.txt')
        with open(data_file.path, 'w') as f:
            f.writelines(data)
        return data_file.path

    def _prepare_loader_args(self, data_file, tickdb_symbology, delta_mode, full_integrity_check, load_by_sections):
        loader_args = ['-dbname', self.name]
        loader_args += ['-data_file', data_file]
        if tickdb_symbology:
            for symbology in tickdb_symbology:
                loader_args += ['-tickdb_symbology', symbology]
        loader_args += ['-delta_mode', 'yes' if delta_mode else 'no']
        loader_args += ['-full_integrity_check', 'yes' if full_integrity_check else 'no']
        loader_args += ['-load_by_sections', 'yes' if load_by_sections else 'no']
        return loader_args

    def _put(self, src, tickdb_symbology, delta_mode, full_integrity_check, load_by_sections):
        if otp.__webapi__:
            raise NotImplementedError("Reference database loader is not supported in WebAPI mode.")
        data_file = self._prepare_data_file(src)
        loader_args = self._prepare_loader_args(
            data_file, tickdb_symbology, delta_mode, full_integrity_check, load_by_sections
        )
        loader_path = os.path.join(utils.omd_dist_path(), 'one_tick', 'bin', 'reference_data_loader.exe')
        p = subprocess.run(
            [loader_path] + loader_args,
            env={
                'ONE_TICK_CONFIG': session.Session._instance.config.path,
                'TZ': os.environ.get('TZ', otp.config['tz']),
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        return p.stdout, p.stderr

    def add(self, *args, **kwargs):
        # this method is not implemented because
        # reference database loader can only rewrite the data, not add new entries
        raise NotImplementedError("Method is not supported for reference databases. Use put instead.")
