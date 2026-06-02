import datetime
import onetick.py as otp


if otp.__webapi__:
    try:
        from onetick.query_webapi import QueryProperties
    except ImportError as e:
        try:
            import onetick.query_webapi
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

    class pyomd:
        timeval_t = datetime.datetime  # type: ignore
        QueryProperties = QueryProperties  # type: ignore # NOSONAR
