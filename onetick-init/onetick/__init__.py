import sys


def __search_main_one_tick_dir():
    try:
        import onetick.query_webapi  # noqa
        # if onetick.query_webapi is imported successfully, do not raise any warnings
        return __search_main_one_tick_dir_catch(show_warnings=False)
    except ImportError:
        return __search_main_one_tick_dir_catch(show_warnings=True)


def __search_main_one_tick_dir_catch(show_warnings=True):  # noqa: C901
    import os
    import warnings
    import sysconfig
    from pathlib import Path

    if os.environ.get('OTP_SKIP_OTQ_VALIDATION', False):
        try:
            import onetick_stubs  # noqa
        except ImportError:
            if show_warnings:
                warnings.warn(
                    "OTP_SKIP_OTQ_VALIDATION environment variable is set,"
                    " but onetick_stubs module is not available."
                    " Please, install onetick-query-stubs to avoid import errors"
                    " or unset OTP_SKIP_OTQ_VALIDATION to use onetick.query module."
                )
        return tuple()

    config_vars = sysconfig.get_config_vars()

    ot_bin_path = os.path.join("bin")
    ot_python_path = os.path.join(ot_bin_path, "python")
    ot_numpy_path = os.path.join(
        ot_bin_path,
        "numpy",
        "python"
        # short python version 27, 36, 37, etc
        + config_vars["py_version_nodot"]
        # suffix at the end, either empty string for python with the standard memory allocator,
        # or 'm' for python with the py-malloc allocator
        + config_vars["abiflags"],
    )

    if os.name == "nt":
        default_main_one_tick_dir = "C:/OMD/one_market_data/one_tick"
    elif os.name == "posix":
        default_main_one_tick_dir = "/opt/one_market_data/one_tick"
    else:
        default_main_one_tick_dir = None

    main_one_tick_dir = os.environ.get("MAIN_ONE_TICK_DIR")

    if not main_one_tick_dir:
        message = (
            "MAIN_ONE_TICK_DIR environment variable is not set."
            " It is a recommended way to let onetick-py know where OneTick python libraries are located."
        )
        if not default_main_one_tick_dir:
            if show_warnings:
                warnings.warn(message)
            return
        message += f" We will try to use default value for your system: {default_main_one_tick_dir}."
        if show_warnings:
            warnings.warn(message)
        main_one_tick_dir = default_main_one_tick_dir

    main_one_tick_dir = Path(main_one_tick_dir)

    if not main_one_tick_dir.is_dir():
        if show_warnings:
            warnings.warn(
                f"MAIN_ONE_TICK_DIR is set to '{main_one_tick_dir}',"
                " but this path is not a directory or doesn't exist."
            )
        return

    ot_bin_path = main_one_tick_dir / ot_bin_path
    ot_python_path = main_one_tick_dir / ot_python_path

    for directory in (ot_python_path, ot_bin_path):
        if not directory.is_dir():
            if show_warnings:
                warnings.warn(
                    f"MAIN_ONE_TICK_DIR is set to '{main_one_tick_dir}',"
                    f" and it must contain '{directory}' directory,"
                    " but this path is not a directory or doesn't exist."
                )
            return

    ot_numpy_path = main_one_tick_dir / ot_numpy_path

    return (ot_bin_path, ot_python_path, ot_numpy_path)


for directory in __search_main_one_tick_dir() or []:
    sys.path.append(str(directory))

__path__ = __import__("pkgutil").extend_path(__path__, __name__)
