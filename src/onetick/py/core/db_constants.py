from datetime import datetime


DEFAULT_START_DATE = datetime(year=2003, month=1, day=1)
DEFAULT_END_DATE = datetime(year=2099, month=12, day=31)


class compression_type:
    LZ4 = "LZ4"
    GZIP = "GZIP"
    NATIVE = "NATIVE"
    NATIVE_PLUS_LZ4 = "NATIVE_PLUS_LZ4"
    NATIVE_PLUS_GZIP = "NATIVE_PLUS_GZIP"
    NATIVE_PLUS_ZSTD = "NATIVE_PLUS_ZSTD"


class access_method:
    FILE = "file"
    MEMORY = "memory"
    SOCKET = "socket"
