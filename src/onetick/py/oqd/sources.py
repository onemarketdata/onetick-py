from functools import partial

import pandas as pd

import onetick.py as otp
from onetick.py.otq import otq

from onetick.py import utils
from onetick.py.sources.data_source import _start_doc, _end_doc, _symbol_doc
from onetick.py.docs.utils import docstring, param_doc


COMMON_SOURCE_DOC_PARAMS = [_start_doc, _end_doc, _symbol_doc]
OQD_TICK_TYPE = 'OQD::*'


def _parse_time(time_expr):
    # get datetime from string in OneTick
    return f'parse_time("%Y%m%d %H:%M:%S.%q", {time_expr}, "GMT")'


def _get_start_time_start_of_day():
    # getting the beggining of the start date day
    # (e.g. from 2025-01-01 14:00:00 to 2025-01-01 00:00:00)
    return _parse_time('time_format("%Y%m%d", _START_TIME, _TIMEZONE) + " 00:00:00.000"')


def _get_end_time_end_of_day():
    # by default we change the end time to the end of the day
    # (e.g. from 2025-01-01 15:00:00 to 2025-01-01 24:00:00)
    end_of_day = _parse_time('time_format("%Y%m%d", _END_TIME, _TIMEZONE) + " 24:00:00.000"')
    # but if we have end time as the start of the next day (e.g. 2025-01-02 00:00:00) then we keep it
    return f'case(time_format("%H%M%S%J", _END_TIME, _TIMEZONE), "000000000000000", _END_TIME, {end_of_day})'


def _modify_query_times(src):
    src.sink(otq.ModifyQueryTimes(
        start_time=_get_start_time_start_of_day(),
        end_time=_get_end_time_end_of_day(),
        output_timestamp='min(max(TIMESTAMP,_START_TIME),_END_TIME)'
    ))


_exch_doc = param_doc(
    name='exch',
    desc="""
    The OneQuantData exchange code for the desired price series. Possible values:

    - 'all'
        return data for all exchanges;

    - 'main'
        return data main pricing exchange;

    - any other string value will treated as exchange name to filter data.

    Default: 'all'.
    """,
    str_annotation="str, 'all', 'main'",
    default='all',
)


class OHLCV(otp.Source):
    @docstring(parameters=[_exch_doc] + COMMON_SOURCE_DOC_PARAMS, add_self=True)
    def __init__(
        self,
        exch='all',
        symbol=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        **kwargs
    ):
        """
        OneQuantData™ source to retrieve a time series of unadjusted
        prices for a symbol for one particular pricing exchange of daily OHLCV data.
        Output ticks have fields: OPEN, HIGH, LOW, CLOSE, VOLUME, CURRENCY, EXCH.

        Examples
        --------
        >>> src = otp.oqd.sources.OHLCV(exch="USPRIM")  # doctest: +SKIP
        >>> otp.run(src,  # doctest: +SKIP
        ...         symbols='BTKR::::GOOGL US',
        ...         start=otp.dt(2018, 8, 1),
        ...         end=otp.dt(2018, 8, 2),
        ...         symbol_date=otp.dt(2018, 8, 1))
                         Time    OID    EXCH CURRENCY     OPEN     HIGH      LOW    CLOSE    VOLUME
        0 2018-08-01 00:00:00  74143  USPRIM      USD  1242.73  1245.72  1225.00  1232.99  605680.0
        1 2018-08-01 20:00:00  74143  USPRIM      USD  1219.69  1244.25  1218.06  1241.13  596960.0
        """
        if self._try_default_constructor(**kwargs):
            return

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=partial(self.build, exch=exch)
        )

        self.schema.set(OID=str,
                        EXCH=str,
                        CURRENCY=str,
                        OPEN=float,
                        HIGH=float,
                        LOW=float,
                        CLOSE=float,
                        VOLUME=float)

    def build(self, exch):
        ep = None
        if exch == 'all':
            ep = otp.oqd.eps.OqdSourceDprcAll()
        elif exch == 'main':
            ep = otp.oqd.eps.OqdSourceDprcMain()
        else:
            ep = otp.oqd.eps.OqdSourceDprcExch(exch=exch)

        src = otp.Source(ep)
        src.tick_type(OQD_TICK_TYPE)
        _modify_query_times(src)
        return src


class CorporateActions(otp.Source):
    """
    OneQuantData™ source EP to retrieve a time series of corporate
    actions for a symbol.

    This source will return all corporate action fields available for a symbol
    with EX-Dates between the query start time and end time (end time is not inclusive).  The
    timestamp of the series is equal to the EX-Date of the corporate
    action with a time of 0:00:00 GMT.

    Examples
    --------
    >>> src = otp.oqd.sources.CorporateActions()  # doctest: +SKIP
    >>> otp.run(src,  # doctest: +SKIP
    ...         symbols='TDEQ::::AAPL',
    ...         start=otp.dt(2021, 1, 1),
    ...         end=otp.dt(2021, 8, 6),
    ...         symbol_date=otp.dt(2021, 2, 18),
    ...         timezone='GMT')
            Time   OID  ACTION_ID    ACTION_TYPE  ACTION_ADJUST ACTION_CURRENCY  ANN_DATE   EX_DATE  PAY_DATE  REC_DATE\
                       TERM_NOTE TERM_RECORD_TYPE ACTION_STATUS
    0 2021-02-05  9706   16799540  CASH_DIVIDEND          0.205             USD  20210127  20210205  20210211  20210208\
          CASH:0.205@USD                         NORMAL
    1 2021-05-07  9706   17098817  CASH_DIVIDEND          0.220             USD  20210428  20210507  20210513  20210510\
           CASH:0.22@USD                         NORMAL
    """

    @docstring(parameters=COMMON_SOURCE_DOC_PARAMS, add_self=True)
    def __init__(
        self,
        symbol=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        **kwargs
    ):
        if self._try_default_constructor(**kwargs):
            return

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=partial(self.build)
        )

        self.schema.set(OID=str,
                        ACTION_ID=int,
                        ACTION_TYPE=str,
                        ACTION_ADJUST=float,
                        ACTION_CURRENCY=str,
                        ANN_DATE=int,
                        EX_DATE=int,
                        PAY_DATE=int,
                        REC_DATE=int,
                        TERM_NOTE=str,
                        TERM_RECORD_TYPE=str,
                        ACTION_STATUS=str)

    def build(self):
        ep = otp.oqd.eps.OqdSourceCacs()
        src = otp.Source(ep)
        src.tick_type(OQD_TICK_TYPE)
        _modify_query_times(src)
        return src


class DescriptiveFields(otp.Source):
    """OneQuantData™ source to retrieve a time series of descriptive fields for a symbol.
    There will only be ticks on days when some field in the descriptive data changes.
    Output ticks will have fields:
    OID, END_DATE, COUNTRY, EXCH, NAME,
    ISSUE_DESC, ISSUE_CLASS, ISSUE_TYPE, ISSUE_STATUS,
    SIC_CODE, IDSYM, TICKER, CALENDAR.

    Note: currently actual fields have 9999 year in END_DATE, but it could not fit the
    nanosecond timestamp, so it is replaced with 2035-01-01 date.

    Examples
    --------
    >>> src = otp.oqd.sources.DescriptiveFields()  # doctest: +SKIP
    >>> otp.run(src,  # doctest: +SKIP
    ...         symbols='1000001589',
    ...         start=otp.dt(2020, 3, 1),
    ...         end=otp.dt(2023, 3, 2),
    ...         timezone='GMT').iloc[:6]
            Time         OID    END_DATE COUNTRY  EXCH                NAME                   ISSUE_DESC\
                 ISSUE_CLASS ISSUE_TYPE ISSUE_STATUS SIC_CODE    IDSYM TICKER CALENDAR
    0 2020-03-01  1000001589  2020-03-23     LUX  EL^X  INVESTEC GLOBAL ST   EUROPEAN HIGH YLD BD INC 2\
                FUND                  NORMAL           B2PT4G9
    1 2020-03-23  1000001589  2020-04-01     LUX  EL^X  NINETY ONE LIMITED   EUROPEAN HIGH YLD BD INC 2\
                FUND                  NORMAL           B2PT4G9
    2 2020-04-01  1000001589  2021-01-01     LUX  EL^X  NINETY ONE LUX S.A   EUROPEAN HIGH YLD BD INC 2\
                FUND                  NORMAL           B2PT4G9
    3 2021-01-01  1000001589  2021-06-18     LUX  EL^X  NINETY ONE LUX S.A   EUROPEAN HIGH YLD BD INC 2\
                FUND                  NORMAL           B2PT4G9
    4 2021-06-18  1000001589  2022-01-01     LUX  EL^X  NINETY ONE LUX S.A  GSF GBL HIGH YLD A2 EUR DIS\
                FUND                  NORMAL           B2PT4G9
    5 2022-01-01  1000001589  2022-01-28     LUX  EL^X  NINETY ONE LUX S.A  GSF GBL HIGH YLD A2 EUR DIS\
                FUND                  NORMAL           B2PT4G9
    """

    @docstring(parameters=COMMON_SOURCE_DOC_PARAMS, add_self=True)
    def __init__(
        self,
        symbol=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        **kwargs
    ):
        if self._try_default_constructor(**kwargs):
            return

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=partial(self.build)
        )

        self.schema.set(
            OID=str,
            END_DATE=otp.nsectime,
            COUNTRY=str,
            EXCH=str,
            NAME=str,
            ISSUE_DESC=str,
            ISSUE_CLASS=str,
            ISSUE_TYPE=str,
            ISSUE_STATUS=str,
            SIC_CODE=str,
            IDSYM=str,
            TICKER=str,
            CALENDAR=str,)

    def build(self):
        ep = otp.oqd.eps.OqdSourceDes()
        src = otp.Source(ep)
        src.tick_type(OQD_TICK_TYPE)
        _modify_query_times(src)

        # work-around to resolve problem with pandas timestamp out of bounds
        pd_max = pd.Timestamp.max.strftime('%Y%m%d%H%M%S')
        src.sink(otq.UpdateFields(
            set='END_DATE=PARSE_NSECTIME("%Y-%m-%d", "2035-01-01", _TIMEZONE)',
            where=f'AS_YYYYMMDDHHMMSS(END_DATE) > {pd_max}'))
        return src


class SharesOutstanding(otp.Source):
    """
    Logic is implemented in OQD_SOURCE_SHO EP to retrieve a time series of shares
    outstanding for a stock.

    The source retrieves a time series of shares outstanding
    for a stock. This source only applies to stocks or securities that have
    published shares outstanding data.

    The series represents total shares outstanding and is not free float
    adjusted.

    Note: currently actual fields have 9999 year in END_DATE, but it could not fit the
    nanosecond timestamp, so it is replaced with 2035-01-01 date.


    Examples
    --------
    >>> src = otp.oqd.sources.SharesOutstanding()  # doctest: +SKIP
    >>> otp.run(src,  # doctest: +SKIP
    ...         symbols='TDEQ::::AAPL',
    ...         start=otp.dt(2021, 1, 1),
    ...         end=otp.dt(2021, 8, 6),
    ...         symbol_date=otp.dt(2021, 2, 18),
    ...         timezone='GMT')
            Time   OID   END_DATE REPORT_MONTH        SHARES
    0 2021-01-01  9706 2021-01-06       202009  1.700180e+10
    1 2021-01-06  9706 2021-01-29       202009  1.682326e+10
    2 2021-01-29  9706 2021-05-03       202012  1.678810e+10
    3 2021-05-03  9706 2021-07-30       202103  1.668763e+10
    4 2021-07-30  9706 2021-10-29       202106  1.653017e+10
    """
    @docstring(parameters=COMMON_SOURCE_DOC_PARAMS, add_self=True)
    def __init__(
        self,
        symbol=otp.utils.adaptive,
        start=otp.utils.adaptive,
        end=otp.utils.adaptive,
        **kwargs
    ):
        if self._try_default_constructor(**kwargs):
            return

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=partial(self.build)
        )

        self.schema.set(OID=str,
                        END_DATE=otp.nsectime,
                        REPORT_MONTH=int,
                        SHARES=int)

    def build(self):
        ep = otp.oqd.eps.OqdSourceSho()
        src = otp.Source(ep)
        src.tick_type(OQD_TICK_TYPE)
        _modify_query_times(src)

        # work-around to resolve problem with pandas timestamp out of bounds
        pd_max = pd.Timestamp.max.strftime('%Y%m%d%H%M%S')
        src.sink(otq.UpdateFields(
            set='END_DATE=PARSE_NSECTIME("%Y-%m-%d", "2035-01-01", _TIMEZONE)',
            where=f'AS_YYYYMMDDHHMMSS(END_DATE) > {pd_max}'))
        return src
