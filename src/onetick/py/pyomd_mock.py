import datetime
import onetick.py as otp


if otp.__webapi__:
    try:
        from onetick.query_webapi import QueryProperties  # noqa: E0611
    except ImportError as e:
        try:
            import onetick.query_webapi  # noqa
            raise RuntimeError("You're trying to use onetick.query_webapi module, "
                               "that is not compatible with onetick.py. "
                               "Please, use onetick.query module instead (unset OTP_WEBAPI), "
                               "or install onetick.query_webapi==1.24.20240715 or newer. "
                               "Also, check that your PYTHONPATH doesn't have onetick binary path, "
                               "because onetick distribution could have older onetick.query_webapi, "
                               "that mirror your pip-installed version.") from e
        except ImportError as e2:
            raise ImportError(
                "OTP_WEBAPI environment variable is set,"
                " but onetick.query_webapi module is not available."
                " Please, install onetick.query_webapi to avoid import errors"
                " or unset OTP_WEBAPI to use onetick.query module instead."
            ) from e2

    # copied from one_market_data/one_tick/bin/python/onetick/query/_internal_utils.py
    def quoted(str_object):
        if len(str_object) <= 1:
            return str_object
        if str_object[0] == "'" and str_object[-1] == "'" or str_object[0] == '"' and str_object[-1] == '"':
            return str_object
        return f"'{str_object}'"

    class OT_time_nsec_mock:  # NOSONAR
        def __init__(self, number):
            self.number = number

        def __int__(self):
            return int(self.number / 1e9)

    class pyomd:
        timeval_t = datetime.datetime  # type: ignore
        QueryProperties = QueryProperties  # type: ignore # NOSONAR

        @staticmethod
        def OT_time_nsec(number):  # NOSONAR
            return int(number / 1e9)
