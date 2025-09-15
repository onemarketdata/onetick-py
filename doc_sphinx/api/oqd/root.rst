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

- :py:class:`onetick.py.oqd.sources.OHLCV`
- :py:class:`onetick.py.oqd.sources.CorporateActions`
- :py:class:`onetick.py.oqd.sources.DescriptiveFields`
- :py:class:`onetick.py.oqd.sources.SharesOutstanding`
