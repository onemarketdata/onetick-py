Configuration parameters
========================

WebAPI mode
-----------

Environment variable ``OTP_WEBAPI`` can be set *before* importing ``onetick.py`` module to force using WebAPI mode.

This is needed in case of using ``onetick.py`` on the machine with installed OneTick distribution.
In this case by default ``onetick.py`` will use locally installed OneTick, but setting ``OTP_WEBAPI`` variable
will change the mode to WebAPI.

If ``onetick.py`` is used on the machine without OneTick distribution, then setting this variable is not needed.

Other configuration parameters
------------------------------

See :py:class:`otp.config<onetick.py.configuration.Config>` for details.

.. autodata:: onetick.py.configuration.OptionsTable
   :no-value:
