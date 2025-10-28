import datetime
import sys
import warnings

from typing import Optional
from contextlib import suppress

import dateutil.tz
import tzlocal

import onetick.py as otp
from onetick.py.backports import zoneinfo


def get_tzfile_by_name(timezone):
    if isinstance(timezone, str):
        try:
            timezone = zoneinfo.ZoneInfo(timezone)
        except zoneinfo.ZoneInfoNotFoundError:
            timezone = dateutil.tz.gettz(timezone)
    return timezone


def get_local_timezone_name():
    tz = tzlocal.get_localzone()
    try:
        return tz.zone  # type: ignore
    except AttributeError:
        return tz.key  # type: ignore


def get_timezone_from_datetime(dt) -> Optional[str]:
    tzinfo = getattr(dt, 'tzinfo', None)
    if tzinfo is None:
        return None
    if tzinfo is datetime.timezone.utc:
        return 'UTC'
    with suppress(ModuleNotFoundError):
        import pytz
        if isinstance(tzinfo, pytz.BaseTzInfo):
            return tzinfo.zone
    if isinstance(tzinfo, zoneinfo.ZoneInfo):
        return tzinfo.key
    if isinstance(tzinfo, dateutil.tz.tzlocal):
        return get_local_timezone_name()
    if isinstance(tzinfo, dateutil.tz.tzstr) and hasattr(tzinfo, '_s'):
        return tzinfo._s
    if isinstance(tzinfo, dateutil.tz.tzfile):
        if sys.platform == 'win32':
            warnings.warn(
                "It's not recommended to use dateutil.tz timezones on Windows platform. "
                "Function 'get_timezone_from_datetime' can't guarantee correct results in this case. "
                "Please, use zoneinfo timezones instead."
            )
        if hasattr(tzinfo, '_filename'):
            if tzinfo._filename == '/etc/localtime':
                return get_local_timezone_name()
            for timezone in zoneinfo.available_timezones():
                if tzinfo._filename.endswith(timezone):
                    return timezone
    if sys.platform == 'win32':
        if isinstance(tzinfo, dateutil.tz.win.tzwin) and hasattr(tzinfo, '_name'):
            return tzinfo._name
    raise ValueError(f"Can't get timezone name from datetime '{dt}' with tzinfo {tzinfo}")


def convert_timezone(dt, src_timezone, dest_timezone) -> datetime.datetime:
    """
    Converting timezone-naive ``dt`` object localized in ``src_timezone`` timezone
    to the specified ``dest_timezone`` and returning also timezone-naive object.
    """
    if src_timezone is None:
        src_timezone = get_local_timezone_name()
    # using pandas, because stdlib datetime has some bug around epoch on Windows
    dt = otp.datetime(dt).ts
    # change timezone-naive to timezone-aware
    dt = dt.tz_localize(src_timezone)
    # convert timezone
    dt = dt.tz_convert(dest_timezone)
    # change timezone-aware to timezone-naive
    dt = dt.tz_localize(None)
    # convert to datetime
    dt = dt.to_pydatetime()
    return dt
