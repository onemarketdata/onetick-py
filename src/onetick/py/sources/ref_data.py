from onetick.py.otq import otq

from onetick.py import types as ott
from onetick.py.core.source import Source
from onetick.py.core._source.query_parameters import QueryParameters

from .. import utils

from .common import update_node_tick_type


class RefData(Source):
    def __init__(
        self,
        ref_data_type=None,
        symbol=utils.adaptive,
        db=utils.adaptive_to_default,
        start=utils.adaptive,
        end=utils.adaptive,
        query_parameters: QueryParameters = None,
        **kwargs,
    ):
        """
        Shows reference data for the specified security and reference data type.

        It can be used to view corporation actions,
        symbol name changes,
        primary exchange info and symbology mapping for a securities,
        as well as the list of symbologies,
        names of custom adjustment types for corporate actions present in a reference database
        as well as names of continuous contracts in database symbology.

        Parameters
        ----------
        ref_data_type: str
            Type of reference data to be queried. Possible values are:

            * corp_actions
            * symbol_name_history
            * primary_exchange
            * symbol_calendar
            * symbol_currency
            * symbology_mapping
            * symbology_list
            * custom_adjustment_type_list
            * all_calendars
            * all_continuous_contract_names

        symbol: str, list of str, :class:`Source`, :class:`query`, :py:func:`eval query <onetick.py.eval>`
            Symbol(s) from which data should be taken.
        db: str
            Name of the database.
        start: :py:class:`otp.datetime <onetick.py.datetime>`
            Start time for tick generation. By default the start time of the query will be used.
        end: :py:class:`otp.datetime <onetick.py.datetime>`
            End time for tick generation. By default the end time of the query will be used.
        query_parameters: :py:class:`otp.QueryParameters <onetick.py.QueryParameters>`
            Additional query properties to be set in the resulting .otq file.
            They will be used if they are not overridden by other parameters or in :py:func:`otp.run <onetick.py.run>`.

        See also
        --------
        **REF_DATA** OneTick event processor

        Examples
        --------

        Show calendars for a database US_COMP_SAMPLE:

        >>> src = otp.RefData('all_calendars')  # doctest: +SKIP
        >>> otp.run(src, symbols='US_COMP_SAMPLE::AAPL',
        ...         date=otp.dt(2024, 2, 1), symbol_date=otp.dt(2024, 2, 1), timezone='EST5EDT')  # doctest: +SKIP
                 Time END_DATETIME       CALENDAR_NAME SESSION_NAME SESSION_FLAGS DAY_PATTERN \
                    START_HHMMSS  END_HHMMSS          TIMEZONE  PRIORITY                    DESCRIPTION
        0  2024-02-01   2024-03-29  BBG_EQUITY_EXCH_US     DAY_TYPE             R   0.0.12345 \
                               0      240000  America/New_York         0                    @US_DEFAULT
        1  2024-02-01   2024-03-29  BBG_EQUITY_EXCH_US   PRE_MARKET             b   0.0.12345 \
                           40000       93000  America/New_York         0                    @US_DEFAULT
        2  2024-02-01   2024-03-29  BBG_EQUITY_EXCH_US       MARKET             r   0.0.12345 \
                           93000      160000  America/New_York         0                    @US_DEFAULT
        3  2024-02-01   2024-03-29  BBG_EQUITY_EXCH_US  POST_MARKET             a   0.0.12345 \
                          160000      200000  America/New_York         0                    @US_DEFAULT
        4  2024-02-01   2024-03-29  BBG_EQUITY_EXCH_US      HOLIDAY             H       1.3.1 \
                               0      240000  America/New_York         1  MARTIN_LUTHER_KING@US_DEFAULT
        ..        ...          ...                 ...          ...           ...         ... \
                             ...         ...               ...       ...                            ...
        85 2024-02-01   2024-03-29     CLOUD_DB_US_OTC      HOLIDAY             H       1.3.1 \
                               0      240000  America/New_York         1  MARTIN_LUTHER_KING@US_DEFAULT
        86 2024-02-01   2024-03-29     CLOUD_DB_US_OTC      HOLIDAY             H       2.3.1 \
                               0      240000  America/New_York         1      PRESIDENTS_DAY@US_DEFAULT
        87 2024-02-01   2024-03-29     CLOUD_DB_US_OTC      HOLIDAY             H       5.6.1 \
                               0      240000  America/New_York         1        MEMORIAL_DAY@US_DEFAULT
        88 2024-02-01   2024-03-29     CLOUD_DB_US_OTC      HOLIDAY             H       9.1.1 \
                               0      240000  America/New_York         1           LABOR_DAY@US_DEFAULT
        89 2024-02-01   2024-03-29     CLOUD_DB_US_OTC      HOLIDAY             H      11.4.4 \
                               0      240000  America/New_York         1    THANKSGIVING_DAY@US_DEFAULT
        """
        if self._try_default_constructor(**kwargs):
            return

        if ref_data_type is None:
            raise ValueError('Parameter `ref_data_type` was not set')

        if ref_data_type not in [
            'corp_actions', 'symbol_name_history', 'primary_exchange', 'symbol_calendar', 'symbol_currency',
            'symbology_mapping', 'symbology_list', 'custom_adjustment_type_list', 'all_calendars',
            'all_continuous_contract_names',
        ]:
            raise ValueError(f'Incorrect `ref_data_type` value passed: `{ref_data_type}`')

        schema = self._get_schema_for_ref_data_type(ref_data_type)
        ref_data_type = ref_data_type.upper()

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=lambda: self.base_ep(
                ref_data_type=ref_data_type,
                db=db,
            ),
            schema=schema,
            query_parameters=query_parameters,
        )

    @staticmethod
    def _get_schema_for_ref_data_type(ref_data_type: str) -> dict:
        if ref_data_type == 'corp_actions':
            return {
                'MULTIPLICATIVE_ADJUSTMENT': float,
                'ADDITIVE_ADJUSTMENT': float,
                'ADJUSTMENT_TYPE': str,
            }
        elif ref_data_type == 'symbol_name_history':
            return {
                'END_DATETIME': ott.nsectime,
                'SYMBOL_NAME': str,
            }
        elif ref_data_type == 'primary_exchange':
            return {
                'END_DATETIME': ott.nsectime,
                'PRIMARY_EXCHANGE': str,
                'SYMBOL_ON_PRIMARY_EXCHANGE': str,
            }
        elif ref_data_type == 'symbol_calendar':
            return {
                'END_DATETIME': ott.nsectime,
                'START_HHMMSS': int,
                'END_HHMMSS': int,
                'SESSION_NAME': str,
                'TIMEZONE': str,
                'DAY_PATTERN': str,
                'SESSION_FLAGS': str,
                'PRIORITY': ott.byte,
                'DESCRIPTION': str,
            }
        elif ref_data_type == 'symbol_currency':
            return {
                'END_DATETIME': ott.nsectime,
                'MULTIPLIER': float,
                'CURRENCY': str,
            }
        elif ref_data_type == 'symbology_mapping':
            return {
                'END_DATETIME': ott.nsectime,
                'MAPPED_SYMBOL_NAME': str,
            }
        elif ref_data_type == 'symbology_list':
            return {
                'SYMBOLOGY_NAME': str,
            }
        elif ref_data_type == 'custom_adjustment_type_list':
            return {
                'ADJUSTMENT_TYPE': str,
            }
        elif ref_data_type == 'all_calendars':
            return {
                'END_DATETIME': ott.nsectime,
                'CALENDAR_NAME': str,
                'SESSION_NAME': str,
                'SESSION_FLAGS': str,
                'DAY_PATTERN': str,
                'START_HHMMSS': int,
                'END_HHMMSS': int,
                'TIMEZONE': str,
                'PRIORITY': ott.byte,
                'DESCRIPTION': str,
            }
        elif ref_data_type == 'all_continuous_contract_names':
            return {
                'CONTINUOUS_CONTRACT_NAME': str,
            }

        raise ValueError('Couldn\'t detect schema: incorrect `ref_data_type` passed')

    def base_ep(
        self,
        ref_data_type,
        db=utils.adaptive_to_default,
    ):
        src = Source(
            otq.RefData(ref_data_type=ref_data_type)
        )

        if db:
            update_node_tick_type(src, tick_type=utils.adaptive, db=db)

        return src
