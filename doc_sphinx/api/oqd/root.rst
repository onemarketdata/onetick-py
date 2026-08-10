.. _OQD:

OneQuantData™ (OQD)
===================

OneQuantData™ is a continuously updated repository of historical reference and
pricing data designed specifically for the global equities market.
OneQuantData™ data sets cover decades of history for exchange-traded equity products,
featuring daily prices, security information, corporate actions, and cross-reference information.

.. note::

    | This is a separate offer.
    | It requires an additional setup of client-side configuration and/or receiving access to the OQD cloud databases.
    | Contact OneTick support for additional info.

OQD consists of two parts:

- client configuration:
    - get the code of custom event processors (EPs) that provide an API to read the data from OQD databases
      (it is done automatically by `onetick.py.oqd` module)
    - setting up locators and access list files to access these databases
      and/or receiving credentials to access already set-up cloud OQD servers
- server configuration:
    - support these custom OQD EPs
    - maintain OQD databases that contain reference and pricing data


``onetick-py`` supports several source classes that use OQD EPs and access OQD databases:

- :py:class:`~onetick.py.oqd.sources.OHLCV`
- :py:class:`~onetick.py.oqd.sources.CorporateActions`
- :py:class:`~onetick.py.oqd.sources.DescriptiveFields`
- :py:class:`~onetick.py.oqd.sources.SharesOutstanding`


Also some OQD databases (with ``OQD_`` prefix) are available for querying directly:

.. doctest::

   >>> [db for db in otp.databases() if db.startswith('OQD_')]  # doctest: +SKIP
   ['OQD_BBGBSYM',
    'OQD_BBGBTKR',
    'OQD_BBGFGC',
    'OQD_BBGFGS',
    'OQD_BBGFGV',
    'OQD_BBGOID',
    'OQD_CACT',
    'OQD_CACT_SAMPLE',
    'OQD_DPRC',
    'OQD_DPRC_SAMPLE',
    'OQD_ETF',
    'OQD_EVENT',
    'OQD_FUT',
    'OQD_INDUSTRY_CLASS',
    'OQD_MKTCAL',
    'OQD_RKEY',
    'OQD_SEC',
    'OQD_SHARE',
    'OQD_XOID',
    'OQD_XOID_SOURCE',
    'OQD_XSYM']

For example, let's get some events from `OQD_EVENT` database, like earnings announcement dates:

.. doctest::

   >>> data = otp.DataSource('OQD_EVENT', tick_type='EVENT_D', schema_policy='manual')
   >>> data = otp.merge([data], symbols=otp.Symbols('OQD_EVENT', for_tick_type='EVENT_D'))
   >>> otp.run(data, date=otp.dt(2026, 6, 23), timezone='GMT')
            Time     OID               EVENT_TYPE EVENT_CONDITION      EVENT_DATETIME DELETED_TIME  TICK_STATUS  OMDSEQ
   0  2026-06-23  207482             EARNING_DATE                 2026-06-23 20:05:00   1970-01-01            0       1
   1  2026-06-23   22857  COMPANY_CONFERENCE_CALL                 2026-06-23 14:00:00   1970-01-01            0       1
   2  2026-06-23   22857             EARNING_DATE                 2026-06-23 13:15:00   1970-01-01            0       1
   3  2026-06-23   60963  COMPANY_CONFERENCE_CALL                 2026-06-23 21:00:00   1970-01-01            0       1
   4  2026-06-23   60963             EARNING_DATE                 2026-06-23 20:02:00   1970-01-01            0       1
   5  2026-06-23  681733  COMPANY_CONFERENCE_CALL                 2026-06-23 12:00:00   1970-01-01            0       1
   6  2026-06-23  681733             EARNING_DATE                 2026-06-23 10:00:00   1970-01-01            0       0
   7  2026-06-23  747325  COMPANY_CONFERENCE_CALL                 2026-06-23 21:00:00   1970-01-01            0       1
   8  2026-06-23  747325             EARNING_DATE                 2026-06-23 20:43:00   1970-01-01            0       1
   9  2026-06-23   81848             EARNING_DATE                 2026-06-23 20:15:00   1970-01-01            0       1
   10 2026-06-23   91603  COMPANY_CONFERENCE_CALL                 2026-06-23 21:00:00   1970-01-01            0       1
   11 2026-06-23   91603             EARNING_DATE                 2026-06-23 20:10:00   1970-01-01            0       1
   12 2026-06-23   94601  COMPANY_CONFERENCE_CALL                 2026-06-23 16:00:00   1970-01-01            0       1
   13 2026-06-23   94601             EARNING_DATE                 2026-06-23 10:45:00   1970-01-01            0       1

Table Of Contents
-----------------

.. tableofcontents::
