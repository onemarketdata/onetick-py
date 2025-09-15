import os
import sys
import multiprocessing
import warnings


def get_local_number_of_cores():
    try:
        # number of cores allowed for user
        return len(os.sched_getaffinity(0))
    except Exception:
        # number of cores
        return multiprocessing.cpu_count()


def default_license_dir():
    if sys.platform == "linux":
        return os.path.join("/", "license")
    elif sys.platform == "win32":
        return os.path.join("C:\\", "OMD", "client_data", "config", "license_repository")
    elif sys.platform == "darwin":
        return os.path.join("/", "Library", "Application Support", "OneTick")
    return None


def default_license_file():
    if sys.platform == "linux":
        return os.path.join("/", "license", "license.dat")
    elif sys.platform == "win32":
        return os.path.join("C:\\", "OMD", "client_data", "config", "license.dat")
    elif sys.platform == "darwin":
        return os.path.join("/", "Library", "Application Support", "OneTick")
    return None


def default_day_boundary_tz(db_name):
    import onetick.py as otp
    if otp.config.tz:
        return otp.config.tz
    warnings.warn(
        "Database property 'day_boundary_tz' can't be set to default timezone "
        "because default timezone is local and is not known at this moment. "
        "Default value for this property will be 'GMT'. "
        f"It may produce unexpected results "
        f"when reading from or writing to database '{db_name}'. "
        "Set otp.config.tz property to some known value to avoid this situation.",
        stacklevel=2,
    )
    return None
