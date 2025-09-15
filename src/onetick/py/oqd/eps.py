import os
from inspect import getframeinfo, stack
from onetick.py.otq import otq

_graph_components = otq.graph_components
_internal_utils = otq._internal_utils


def _get_reference_counted_prefix():
    try:
        return _internal_utils.get_reference_counted_prefix()
    except AttributeError:
        return _internal_utils.get_referance_counted_prefix()


class BaseOqdEp(_graph_components.EpBase):  # type: ignore[name-defined]
    """
    Base class for all OQD EPs
    """
    class Parameters:
        shared_state_variables = "SHARED_STATE_VARIABLES"

        @staticmethod
        def list_parameters():
            list_val = ["shared_state_variables"]
            return list_val

    __slots__ = ["shared_state_variables", "_default_shared_state_variables", "stack_info", "_used_strings"]

    def __init__(self, shared_state_variables=""):
        _graph_components.EpBase.__init__(self, self._get_name())
        self._default_shared_state_variables = ""
        self.shared_state_variables = shared_state_variables
        self._used_strings = {}
        for param_name in type(self).__dict__:
            param_val = getattr(self, param_name, '')
            if (
                isinstance(param_val, str) and
                _get_reference_counted_prefix() in param_val and
                param_val not in self._used_strings
            ):
                _internal_utils.inc_ref_count(param_val)
                self._used_strings[param_val] = 1
        caller = getframeinfo(stack()[1][0])
        self.stack_info = caller.filename + ":" + str(caller.lineno)

    def _get_name(self):
        raise NotImplementedError()

    def set_shared_state_variables(self, value):
        self.shared_state_variables = value
        return self

    def __repr__(self):
        return self._to_string(for_repr=True)

    def __str__(self):
        return self._to_string()

    def __del__(self):
        for param_name in self._used_strings:
            _internal_utils.dec_ref_count(param_name)
            if _internal_utils.get_ref_count(param_name) == 0:
                _internal_utils.remove_from_memory(param_name)

    def _to_string(self, ep_name=None, for_repr=False):
        name = ep_name
        if name is None:
            name = self._get_name()
        desc = name + "("
        py_to_str = repr if for_repr else str
        if self.shared_state_variables:
            desc += "shared_state_variables=" + py_to_str(self.shared_state_variables) + ","
        desc = desc[:-1]
        if desc != name:
            desc += ")"
        if for_repr:
            return desc + '()' if desc == name else desc
        desc += "\n"
        if len(self._symbols) > 0:
            desc += "symbols=[" + ", ".join(self._symbols) + "]\n"
        if len(self._tick_types) > 0:
            desc += "tick_types=[" + ', '.join(self._tick_types) + "]\n"
        if self._process_node_locally:
            desc += "process_node_locally=True\n"
        if self._node_name:
            desc += "node_name=" + self._node_name + "\n"
        return desc


class OqdSourceDprcMain(BaseOqdEp):
    """
    OQD_SOURCE_DPRC_MAIN

    &#160;

    Type: Other

    Description: A
    OneQuantData&#153; source EP to retrieve a time series of unadjusted
    prices for a symbol for the main pricing exchange.

    The OQD_SOURCE_DPRC_MAIN EP retrieves a single daily time series
    corresponding to the main pricing line for a symbol.
    OQD_SOURCE_DPRC_MAIN differs from OQD_SOURCE_DPRC_EXCH in that it
    doesn't require specification of an exchange.  The most appropriate
    exchange will be chosen by the EP.

    The main pricing line is defined as the composite price for countries
    where a composite price is available.  If no composite price is
    available, then the exchange of primary listing is used, except for
    Germany where XETRA is chosen as the main pricing source.

    To adjust the data for corporate actions, use either the
    OQD_CORP_ACTION EP or the CORP_ACTION EP.

    To retrieve a price series for only one particular exchange, see
    OQD_SOURCE_DPRC_EXCH.  For all available price series, see
    OQD_SOURCE_DPRC_ALL.

    Set the tick type for this EP to __OQD__::*
    The correct database and
    tick type will be set by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_DPRC_MAIN.otq"


class OqdCorpAction(BaseOqdEp):
    """
    OQD_CORP_ACTION

    &#160;

    Type: Other

    Description: A
    OneQuantData&#153; source EP to generate a sparse multiplicative
    adjustment series for use in queries requiring explicit corporate
    action adjustments.

    This EP is used for writing queries that require finer control over
    the corporate action adjustment process than is provided by the
    CORP_ACTION EP or for queries that don't set the symbol date and
    utilize the Onetick Reference File.

    For a simpler method of applying corporate action adjustments, see the
    CORP_ACTION EP.

    When adjusting historical data for corporate
    actions using multiplicative factors, each data point is adjusted by
    all future corporate actions. This means that multiplicative corporate
    action adjustments are cumulative going backwards in time.

    The OQD_CORP_ACTION EP generates the correct time series of adjustment factors
    for use in a query utilizing the JOIN_BY_TIME EP.  The output of this
    EP is guaranteed to start at the query start time.  If a symbol has no
    corporate actions over the queried time range, this EP will return a
    single tick at the query start time with a value of 1.0 for the
    adjustment factors.

    Price and volume adjustments are applied inversely.  Each tick from
    the OQD_CORP_ACTION EP will return 2 fields  of adjustment factors: MADJ_PRICE and
    MADJ_VOLUME.

    To adjust prices or other data items stated on a per share
    basis, multiple the data series by the value in the MADJ_PRICE column.

    To adjust volume or other data items stated on a share basis, multiple
    the data series by the MADJ_VOLUME column.  The factors for spinoff are
    not include in the MADJ_VOLUME factor.

    The typical usage of this EP is to pass the output of the EP into a
    JOIN_BY_TIME node to join the adjustment factors with a data series
    that needs adjustments applied, and then perform the adjustments in
    and ADD_FIELD or UPDATE_FIELD EP.

    Because the OQD_CORP_ACTION EP goes directly to the OneQuantData
    databases, it does not require that a Onetick reference file has been
    configured and loaded and may be used even when SYMBOL_DATE is set to 0.

    Set the tick type for this EP to __OQD__::*
    The correct database and
    tick type will be set by the EP.

    Input: None

    Output: A series of ticks.

    Parameters:


    ADJUST_TYPES
            (string)



    A string containing a comma separate list of adjustment types to
    apply.  The types may be any of: SPLIT, STOCK_DIVIDEND, RIGHTS, or
    SPINOFF.  The default is SPLIT,STOCK_DIVIDEND,RIGHTS,SPINOFF.
    """

    class Parameters:
        adjust_types = "ADJUST_TYPES"
        shared_state_variables = "SHARED_STATE_VARIABLES"

        @staticmethod
        def list_parameters():
            list_val = ["adjust_types", "shared_state_variables"]
            return list_val

    __slots__ = ["adjust_types", "_default_adjust_types"]

    def __init__(self, adjust_types="SPLIT,STOCK_DIVIDEND,RIGHTS,SPINOFF", shared_state_variables=""):
        BaseOqdEp.__init__(self, shared_state_variables)
        self._default_adjust_types = "SPLIT,STOCK_DIVIDEND,RIGHTS,SPINOFF"
        self.adjust_types = adjust_types

    def set_adjust_types(self, value):
        self.adjust_types = value
        return self

    def _get_name(self):
        return "OTQ::OQD_CORP_ACTION.otq"

    def _to_string(self, ep_name=None, for_repr=False):
        name = ep_name
        if name is None:
            name = self._get_name()
        desc = name + "("
        py_to_str = repr if for_repr else str
        if self.adjust_types != "SPLIT,STOCK_DIVIDEND,RIGHTS,SPINOFF":
            desc += "adjust_types=" + py_to_str(self.adjust_types) + ","
        py_to_str = repr if for_repr else str
        if self.shared_state_variables:
            desc += "shared_state_variables=" + py_to_str(self.shared_state_variables) + ","
        desc = desc[:-1]
        if desc != name:
            desc += ")"
        if for_repr:
            return desc + '()' if desc == name else desc
        desc += "\n"
        if len(self._symbols) > 0:
            desc += "symbols=[" + ", ".join(self._symbols) + "]\n"
        if len(self._tick_types) > 0:
            desc += "tick_types=[" + ', '.join(self._tick_types) + "]\n"
        if self._process_node_locally:
            desc += "process_node_locally=True\n"
        if self._node_name:
            desc += "node_name=" + self._node_name + "\n"
        return desc


class OqdCorpDailyFactor(BaseOqdEp):
    """
    OQD_CORP_DAILY_FACTOR

    &#160;

    Type: Other

    Description:A
    OneQuantData&#153; source EP to generate a daily series of
    multiplicative corporate action adjustment factors.

    This EP will generate a time series containing the total
    multiplicative adjustment factors for each day over the query range.
    For days on which there were no corporate actions, the
    OQD_CORP_DAILY_FACTOR EP will return values of 1.0.

    For adjusting the entire history of a time series, see the CORP_ACTION
    EP or the OQD_CORP_ACTION EP.

    The OQD_CORP_DAILY_FACTOR EP is used when a time series is needed that
    calculates the daily adjustment factor taking into account the possibility
    of multiple corporate actions on the same day.  This EP only
    accumulates factors per day.  It does not accumulate factors over
    time.

    Set the tick type for this EP to __OQD__::*
    The correct database and
    tick type will be set by the EP.

    Input: None

    Output: A series of ticks.

    Parameters:


    ADJUST_TYPES (string)


    A string containing a comman separate list of adjustment types to
    apply.  The types may be any of: SPLIT, STOCK_DIVIDEND, RIGHTS, or
    SPINOFF.  The default is SPLIT,STOCK_DIVIDEND,RIGHTS,SPINOFF.
    """

    class Parameters:
        action_type = "ACTION_TYPE"
        shared_state_variables = "SHARED_STATE_VARIABLES"

        @staticmethod
        def list_parameters():
            list_val = ["action_type", "shared_state_variables"]
            return list_val

    __slots__ = ["action_type", "_default_action_type"]

    def __init__(self, action_type="SPLIT,STOCK_DIVIDEND,RIGHTS,SPINOFF", shared_state_variables=""):
        BaseOqdEp.__init__(self, shared_state_variables)
        self._default_action_type = "SPLIT,STOCK_DIVIDEND,RIGHTS,SPINOFF"
        self.action_type = action_type

    def set_action_type(self, value):
        self.action_type = value
        return self

    def _get_name(self):
        return "OTQ::OQD_CORP_DAILY_FACTOR.otq"

    def _to_string(self, ep_name=None, for_repr=False):
        name = ep_name
        if name is None:
            name = self._get_name()
        desc = name + "("
        py_to_str = repr if for_repr else str
        if self.action_type != "SPLIT,STOCK_DIVIDEND,RIGHTS,SPINOFF":
            desc += "action_type=" + py_to_str(self.action_type) + ","
        py_to_str = repr if for_repr else str
        if self.shared_state_variables:
            desc += "shared_state_variables=" + py_to_str(self.shared_state_variables) + ","
        desc = desc[:-1]
        if desc != name:
            desc += ")"
        if for_repr:
            return desc + '()' if desc == name else desc
        desc += "\n"
        if len(self._symbols) > 0:
            desc += "symbols=[" + ", ".join(self._symbols) + "]\n"
        if len(self._tick_types) > 0:
            desc += "tick_types=[" + ', '.join(self._tick_types) + "]\n"
        if self._process_node_locally:
            desc += "process_node_locally=True\n"
        if self._node_name:
            desc += "node_name=" + self._node_name + "\n"
        return desc


class OqdSourceBbgbsym(BaseOqdEp):
    """
    OQD_SOURCE_BBGBSYM

    &#160;

    Type: Other

    Description: A OneQuantData&#153; source EP to retrieve OpenFIGI symbology
    using a symbol in the full Bloomberg ticker format, {TICKER} {EXCH_CODE} {Yellow Key} .

    This EP will return a time series that includes the OneMarketData OID, the venue FIGI,
    Bloomberg yellow key, Bloomberg exchange code, Bloomberg ticker, composite FIGI,
    share class FIGI, Bloomberg security type, and Bloomberg security name.

    Coverage for OpenFIGI symbology begins on October 6th, 2014.

    The OQD_SOURCE_BBGBSYM EP performs an automatic lookback in the database
    and is guaranteed to return output at the start time of the query if
    the security existed on that day. After the first tick, there will only be ticks
    on days when some field in the data changes.

    Pass the output of OQD_SOURCE_BBGBSYM through a JOIN_BY_TIME EP or an
    aggregation EP to create a daily series of descriptive data.

    The tick type for this EP may be set to OQD::*
    The correct database and tick type will be supplied by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_BBGBSYM.otq"


class OqdSourceBbgbtkr(BaseOqdEp):
    """
    OQD_SOURCE_BBGBTKR

    &#160;

    Type: Other

    Description: A OneQuantData&#153; source EP to retrieve OpenFIGI symbology using a symbol
    in the short Bloomberg ticker format, {TICKER} {EXCH_CODE} .

    This EP will return a time series that includes the OneMarketData OID, the venue FIGI,
     Bloomberg yellow key, Bloomberg exchange code, Bloomberg ticker, composite FIGI,
     share class FIGI, Bloomberg security type, and Bloomberg security name.

    Coverage for OpenFIGI symbology begins on October 6th, 2014.

    The OQD_SOURCE_BBGBTKR EP performs an automatic lookback in the database
    and is guaranteed to return output at the start time of the query if
    the security existed on that day. After the first tick, there will only be ticks
    on days when some field in the data changes.

    Pass the output of OQD_SOURCE_BBGBTKR through a JOIN_BY_TIME EP or an
    aggregation EP to create a daily series of descriptive data.

    The tick type for this EP may be set to OQD::*
    The correct database and tick type will be supplied by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_BBGBTKR.otq"


class OqdSourceBbgfgc(BaseOqdEp):
    """
    OQD_SOURCE_BBGFGC

    &#160;

    Type: Other

    Description: A OneQuantData&#153; source EP to retrieve OpenFIGI symbology
    using a symbol that is a Composite FIGI.

    Multiple FIGI identifiers may be associated with a single composite FIGI.
    This EP will return multiple ticks per day for each FIGI associated with the composite FIGI.

    This EP will return a time series that includes the OneMarketData OID, the venue FIGI,
    Bloomberg yellow key, Bloomberg exchange code, Bloomberg ticker, composite FIGI,
    share class FIGI, Bloomberg security type, and Bloomberg security name.

    Coverage for OpenFIGI symbology begins on October 6th, 2014.

    The OQD_SOURCE_BBGFGC EP performs an automatic lookback in the database
    and is guaranteed to return output at the start time of the query if
    the security existed on that day. After the first tick, there will only be ticks
    on days when some field in the data changes.

    Pass the output of OQD_SOURCE_BBGFGC through a JOIN_BY_TIME EP or an
    aggregation EP to create a daily series of descriptive data.

    The tick type for this EP may be set to OQD::*
    The correct database and tick type will be supplied by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_BBGFGC.otq"


class OqdSourceBbgfgs(BaseOqdEp):
    """
    OQD_SOURCE_BBGFGS

    &#160;

    Type: Other

    Description: A OneQuantData&#153; source EP to retrieve OpenFIGI symbology
    using a symbol that is a share class FIGI.

    Multiple FIGI identifiers may be associated with a single share class FIGI.
    This EP will return multiple ticks per day for each FIGI associated with the share class FIGI.
    The share class FIGI may be used to find alternate venues where a security trades.

    This EP will return a time series that includes the OneMarketData OID, the venue FIGI,
    Bloomberg yellow key, Bloomberg exchange code, Bloomberg ticker, composite FIGI,
    share class FIGI, Bloomberg security type, and Bloomberg security name.

    Coverage for OpenFIGI symbology begins on October 6th, 2014.

    The OQD_SOURCE_BBGFGS EP performs an automatic lookback in the database
    and is guaranteed to return output at the start time of the query if
    the security existed on that day. After the first tick, there will only be ticks
    on days when some field in the data changes.

    Pass the output of OQD_SOURCE_BBGFGS through a JOIN_BY_TIME EP or an
    aggregation EP to create a daily series of descriptive data.

    The tick type for this EP may be set to OQD::*
    The correct database and tick type will be supplied by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_BBGFGS.otq"


class OqdSourceBbgfgv(BaseOqdEp):
    """
    OQD_SOURCE_BBGFGV

    &#160;

    Type: Other

    Description: A OneQuantData&#153; source EP to retrieve OpenFIGI symbology
    using a symbol that is a venue FIGI, which is the FIGI associated
    with a particular trading line on a particular venue.

    This EP will return a time series that includes the OneMarketData OID, the venue FIGI,
    Bloomberg yellow key, Bloomberg exchange code, Bloomberg ticker, composite FIGI,
    share class FIGI, Bloomberg security type, and Bloomberg security name.

    Coverage for OpenFIGI symbology begins on October 6th, 2014.

    The OQD_SOURCE_BBGFGV EP performs an automatic lookback in the database
    and is guaranteed to return output at the start time of the query if
    the security existed on that day. After the first tick, there will only be ticks
     on days when some field in the data changes.

    Pass the output of OQD_SOURCE_BBGFGV through a JOIN_BY_TIME EP or an
    aggregation EP to create a daily series of descriptive data.

    The tick type for this EP may be set to OQD::*
    The correct database and tick type will be supplied by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_BBGFGV.otq"


class OqdSourceBbgoid(BaseOqdEp):
    """
    OQD_SOURCE_BBGOID

    &#160;

    Type: Other

    Description: A OneQuantData&#153; source EP to retrieve OpenFIGI symbology
    using a symbol that is a OneMarketData OID.

    Multiple FIGI identifiers may map to a single OID.
    This EP may return multiple ticks per day for each FIGI associated with an OID.

    This EP will return a time series that includes the OneMarketData OID, the venue FIGI,
    Bloomberg yellow key, Bloomberg exchange code, Bloomberg ticker, composite FIGI,
    share class FIGI, Bloomberg security type, and Bloomberg security name.

    Coverage for OpenFIGI symbology begins on October 6th, 2014.

    The OQD_SOURCE_BBGOID EP performs an automatic lookback in the database
    and is guaranteed to return output at the start time of the query if
    the security existed on that day. After the first tick, there will only be ticks
     on days when some field in the data changes.

    Pass the output of OQD_SOURCE_BBGOID through a JOIN_BY_TIME EP or an
    aggregation EP to create a daily series of descriptive data.

    The tick type for this EP may be set to OQD::*
    The correct database and tick type will be supplied by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_BBGOID.otq"


class OqdSourceCacs(BaseOqdEp):
    """
    OQD_SOURCE_CACS

    &#160;

    Type: Other

    Description:A
    OneQuantData&#153; source EP to retrieve a time series of corporate
    actions for a symbol.

    This EP will return all corporate action fields available for a symbol
    with EX-Dates between the query start time and end time.  The
    timestamp of the series is equal to the EX-Date of the corporate
    action with a time of 0:00:00 GMT.

    Set the tick type for this EP to __OQD__::*
    The correct database and
    tick type will be set by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_CACS.otq"


class OqdSourceCact(BaseOqdEp):
    """
    OQD_SOURCE_CACT
     &#160;
     Type: Other
     Description: A
    OneQuantData&#153; source EP to retrieve a time series of corporate
    actions for all symbols.

    This EP will return a subset of corporate action fields for all
    symbols.

    The OQD_SOURCE_CACT EP is used in conjunction with the special symbols EX_DATE,
    ANN_DATE, PAY_DATE, and REC_DATE.  It will return all corporate
    actions for all symbols with the specified type of date between the query
    start time and query end time.

    For example, if the special symbol EX_DATE is used, then the timestamp
    of the series is the corporate action EX-date with a time of 0:00:00
    GMT and the EP will return all corporate actions with EX-dates between
    the query start time and the query end time.  If the symbol is
    REC_DATE, then all of the timestamps would correspond to corporate
    action record dates.

    The OQD_SOURCE_CACT EP is typically used in stage 1 queries to quickly
    discover a list of securities involved in corporate actions.  Please
    note that if using one of the special symbols other than EX_DATE, the
    EX-date of the corporate action may fall outside of the query time
    range.

    Set the tick type for this EP to __OQD__::*
    The correct database and
    tick type will be set by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_CACT.otq"


class OqdSourceDes(BaseOqdEp):
    """
    OQD_SOURCE_DES

    &#160;

    Type: Other

    Description: A OneQuantData&#153; source EP to retrieve a time series of
    descriptive fields for a symbol.

    This EP will return the history of descriptive fields for a symbol.
    There will only be ticks on days when some field in the descriptive
    data changes.

    The OQD_SOURCE_DES EP performs an automatic lookback in the database
    and is guaranteed to return output at the start time of the query if
    the security existed on that day.

    Pass the output of OQD_SOURCE_DES through a JOIN_BY_TIME EP or an
    aggregation EP to create a daily series of descriptive data.

    Set the tick type for this EP to __OQD__::*
    The correct database and
    tick type will be set by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_DES.otq"


class OqdSourceDprcAll(BaseOqdEp):
    """
    OQD_SOURCE_DPRC_ALL

    &#160;

    Type: Other

    Description: A
    OneQuantData&#153; source EP to retrieve a time series of unadjusted
    prices for a symbol for all available pricing exchanges.

    The OQD_SOURCE_DPRC_ALL EP retrieves a time series of all available
    daily unadjusted prices for a symbol.  The OneQuantData exchange code for each
    price series is in the EXCH field.

    To adjust the data for corporate actions, use either the
    OQD_CORP_ACTION EP or the CORP_ACTION EP.

    To retrieve a price series for only one particular exchange, see
    OQD_SOURCE_DPRC_EXCH.  For a price series that tracks either the
    composite series or the primary exchange series, see
    OQD_SOURCE_DPRC_MAIN.

    Set the tick type for this EP to __OQD__::*
    The correct database and
    tick type will be set by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_DPRC_ALL.otq"


class OqdSourceDprcExch(BaseOqdEp):
    """
    OQD_SOURCE_DPRC_EXCH

    &#160;

    Type: Other

    Description: A
    OneQuantData&#153; source EP to retrieve a time series of unadjusted
    prices for a symbol for one particular pricing exchange.

    The OQD_SOURCE_DPRC_ALL EP retrieves a time series of daily unadjusted
    prices for a symbol.  A OneQuantData exchange code must be supplied as
    an argument to the EP.

    To adjust the data for corporate actions, use either the
    OQD_CORP_ACTION EP or the CORP_ACTION EP.

    To retrieve a price series for all exchanges available in OneQuantData, see
    OQD_SOURCE_DPRC_ALL.  For a price series that tracks either the
    composite series or the primary exchange series, see
    OQD_SOURCE_DPRC_MAIN.

    Set the tick type for this EP to __OQD__::*
    The correct database and
    tick type will be set by the EP.

    Input: None

    Output: A series of ticks.

    Parameters:


    EXCH (string)


    The OneQuantData exchange code for the desired price series.

    The OneQuantData exchange code is formed from the 2 letter ISO country
    code plus the 4 letter ISO MIC code.  For countries that have a
    composite price series, use COMP for the exchange portion of the
    code.  For example, USCOMP is the code for US composite pricing.
    """

    class Parameters:
        exch = "EXCH"
        shared_state_variables = "SHARED_STATE_VARIABLES"

        @staticmethod
        def list_parameters():
            list_val = ["exch", "shared_state_variables"]
            return list_val

    __slots__ = ["exch", "_default_exch"]

    def __init__(self, exch="USCOMP", shared_state_variables=""):
        BaseOqdEp.__init__(self, shared_state_variables)
        self._default_exch = "USCOMP"
        self.exch = exch

    def set_exch(self, value):
        self.exch = value
        return self

    def _get_name(self):
        return "OTQ::OQD_SOURCE_DPRC_EXCH.otq"

    def _to_string(self, ep_name=None, for_repr=False):
        name = ep_name
        if name is None:
            name = self._get_name()
        desc = name + "("
        py_to_str = repr if for_repr else str
        if self.exch != "USCOMP":
            desc += "exch=" + py_to_str(self.exch) + ","
        py_to_str = repr if for_repr else str
        if self.shared_state_variables:
            desc += "shared_state_variables=" + py_to_str(self.shared_state_variables) + ","
        desc = desc[:-1]
        if desc != name:
            desc += ")"
        if for_repr:
            return desc + '()' if desc == name else desc
        desc += "\n"
        if len(self._symbols) > 0:
            desc += "symbols=[" + ", ".join(self._symbols) + "]\n"
        if len(self._tick_types) > 0:
            desc += "tick_types=[" + ', '.join(self._tick_types) + "]\n"
        if self._process_node_locally:
            desc += "process_node_locally=True\n"
        if self._node_name:
            desc += "node_name=" + self._node_name + "\n"
        return desc


class OqdSourceSho(BaseOqdEp):
    """
    OQD_SOURCE_SHO

    &#160;

    Type: Other

    Description: A
    OneQuantData&#153; source EP to retrieve a time series of shares
    outstanding for a stock.

    The OQD_SOURCE_SHO EP retrieves a time series of shares outstanding
    for a stock.  This EP only applies to stocks or securities that have
    published shares outstanding data.

    The series represents total shares outstanding and is not free float
    adjusted.

    Set the tick type for this EP to __OQD__::*
    The correct database and
    tick type will be set by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_SHO.otq"


class OqdSourceXoid(BaseOqdEp):
    """
    OQD_SOURCE_XOID

    &#160;

    Type: Other

    Description: A
    OneQuantData&#153; source EP to retrieve raw ticks from the OQD_XOID
    database.

    The OQD_XOID database translates internal OneQuantData IDs (OIDs) into
    standard identifiers like CUSIP, SEDOL, or TICKER.

    The OQD_SOURCE_XOID EP will return a consolidated time series of
    symbol translations keyed by exchange code (EXCH) and symbol type (ID_TYPE).  Each
    translation is valid from the timestamp of the tick until the
    timestamp given in field END_DATE.

    This EP performs a lookback to ensure that ticks are available for the
    query start time, if there are valid ticks that cover that time.

    Use the OQD_SOURCE_XOID EP to write custom symbol translation queries.

    Set the tick type for this EP to OQD::*
    The correct database and
    tick type will be set by the EP.

    Input: None

    Output: A series of ticks.

    Parameters:


    FIGI_COMPOSITE_FILTER
            (boolean)



    When set to false, all Bloomberg tickers and FIGIs associated with an OID will be returned.
    When set to true, only the composite Bloomberg ticker and FIGI are returned.
    This option does not have an effect on any other symbol types.  The default is false.
    """

    class Parameters:
        figi_composite_filter = "FIGI_COMPOSITE_FILTER"
        shared_state_variables = "SHARED_STATE_VARIABLES"

        @staticmethod
        def list_parameters():
            list_val = ["figi_composite_filter", "shared_state_variables"]
            return list_val

    __slots__ = ["figi_composite_filter", "_default_figi_composite_filter"]

    def __init__(self, figi_composite_filter=False, shared_state_variables=""):
        BaseOqdEp.__init__(self, shared_state_variables)
        self._default_figi_composite_filter = False
        self.figi_composite_filter = figi_composite_filter

    def set_figi_composite_filter(self, value):
        self.figi_composite_filter = value
        return self

    def _get_name(self):
        return "OTQ::OQD_SOURCE_XOID.otq"

    def _to_string(self, ep_name=None, for_repr=False):
        name = ep_name
        if name is None:
            name = self._get_name()
        desc = name + "("
        py_to_str = repr if for_repr else str
        if self.figi_composite_filter:
            desc += "figi_composite_filter=" + py_to_str(self.figi_composite_filter) + ","
        py_to_str = repr if for_repr else str
        if self.shared_state_variables:
            desc += "shared_state_variables=" + py_to_str(self.shared_state_variables) + ","
        desc = desc[:-1]
        if desc != name:
            desc += ")"
        if for_repr:
            return desc + '()' if desc == name else desc
        desc += "\n"
        if len(self._symbols) > 0:
            desc += "symbols=[" + ", ".join(self._symbols) + "]\n"
        if len(self._tick_types) > 0:
            desc += "tick_types=[" + ', '.join(self._tick_types) + "]\n"
        if self._process_node_locally:
            desc += "process_node_locally=True\n"
        if self._node_name:
            desc += "node_name=" + self._node_name + "\n"
        return desc


class OqdSourceXref(BaseOqdEp):
    """
    OQD_SOURCE_XREF

    &#160;

    Type: Other

    Description: A OneQuantData&#153; source EP to retrieve raw ticks from the XREF database.
    The XREF database contains records used to translate external symbols to the OneQuantData internal identifier (OID).

    The OQD_SOURCE_XREF EP will return a consolidated time series of symbol translations
    keyed by exchange code and symbol type.
    Each translation is valid from the timestamp of the tick until the timestamp given in field END_DATE.
    The symbols for this EP should be of the form SYMBOL^SYMBOL_TYPE
    where SYMBOL is the external symbol and SYMBOL_TYPE is the code for one of of the supported symbol types.

    The currently supported symbol types are TKR, SED, CUS, ISN, and TAQ for ticker,
    SEDOL, CUSIP, ISIN, and TAQ ticker (US only), respectively.

    This EP performs a lookback to ensure that ticks are available for the query start time,
    if there are valid ticks that cover that time.

    Use the OQD_SOURCE_XREF EP to write custom symbol translation queries.

    Set the tick type for this EP to "__OQD__::*".  The correct database and tick type will be set by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_XREF.otq"


class OqdSourceXsym(BaseOqdEp):
    """
    OQD_SOURCE_XSYM

    &#160;

    Type: Other

    Description: A
    OneQuantData&#153; source EP to retrieve raw ticks from the OQD_XSYM
    database.

    The OQD_XSYM database translates external identifiers like CUSIP,
    SEDOL, or TICKER to internal OneQuantData IDs (OIDs).

    The OQD_SOURCE_XSYM EP will return a consolidated time series of
    symbol translations keyed by exchange code and symbol type.  Each
    translation is valid from the timestamp of the tick until the
    timestamp given in field END_DATE.

    The symbols for this EP should be of the form SYMBOL^SYMBOL_TYPE where SYMBOL is the
    external symbol and SYMBOL_TYPE is the code for one of of the
    supported symbol types.  For example, a symbol IBM^TKR would return a
    set of ticks supplying the OID for stock ticker IBM.

    Other symbol types include CUS, SED, or ISN for CUSIP, SEDOL, or ISIN,
    if those symbol types are included in your OneQuantData subscription.

    Use the OQD_SOURCE_XREF EP to write custom symbol translation queries.

    Set the tick type for this EP to __OQD__::*
    The correct database and
    tick type will be set by the EP.

    Input: None

    Output: A series of ticks.

    Parameters: There are no parameters for this EP.
    """

    def _get_name(self):
        return "OTQ::OQD_SOURCE_XSYM.otq"


class OqdTranslate(BaseOqdEp):
    """
    OQD_TRANSLATE

    &#160;

    Type: Other

    Description: A
    OneQuantData&#153; source EP to perform symbol translation.


    This EP is maintained for backwards compatibility and may be deprecated
    in future releases of OneQuantData.

    The OQD_TRANSLATE EP may be used to write custom symbol translation
    queries.

    For access to the raw underlying data used by the OQD_TRANSLATE EP,
    see the OQD_SOURCE_XSYM and OQD_SOURCE_XOID EPs.

    Symbols supplied to the OQD_TRANSLATE EP should be of the form SYMBOL^SYMBOL_TYPE where SYMBOL is the
    external symbol and SYMBOL_TYPE is the code for one of of the
    supported symbol types.  For example, a symbol IBM^TKR would be used
    in the symbol list to translate the stock ticker IBM into another alternate
    symbology like CUSIP.

    The OQD_TRANSLATE EP does not require that a Onetick reference file is
    loaded or configured.  It may be used in queries where symbol date is
    set to 0.

    See also the REF_DATA and SYMBOLOGY_MAPPING EPs, which require that
    a Onetick reference file is configured and loaded.

    Input: None

    Output: A series of ticks.

    Parameters:


    ASOF_DATE (integer)


    The date on which the translation should apply


    TO_ID (string)


    The symbol type that should be returned.
    The type should be specified as a OneQuantData symbol type abbreviation


    EXCH (string)


    The exchange to which the symbol translation applies.
    The exchange code should be specified as a OneQuantData exchange code
    """

    class Parameters:
        asof_date = "ASOF_DATE"
        to_id = "TO_ID"
        exch = "EXCH"
        shared_state_variables = "SHARED_STATE_VARIABLES"

        @staticmethod
        def list_parameters():
            list_val = ["asof_date", "to_id", "exch", "shared_state_variables"]
            return list_val

    __slots__ = ["asof_date", "_default_asof_date", "to_id", "_default_to_id", "exch", "_default_exch"]

    def __init__(self, asof_date="", to_id="OID", exch="USCOMP", shared_state_variables=""):
        BaseOqdEp.__init__(self, shared_state_variables)
        self._default_asof_date = ""
        self.asof_date = asof_date
        self._default_to_id = "OID"
        self.to_id = to_id
        self._default_exch = "USCOMP"
        self.exch = exch

    def set_asof_date(self, value):
        self.asof_date = value
        return self

    def set_to_id(self, value):
        self.to_id = value
        return self

    def set_exch(self, value):
        self.exch = value
        return self

    def _get_name(self):
        return "OTQ::OQD_TRANSLATE.otq"

    def _to_string(self, ep_name=None, for_repr=False):
        name = ep_name
        if name is None:
            name = self._get_name()
        desc = name + "("
        py_to_str = repr if for_repr else str
        if self.asof_date:
            desc += "asof_date=" + py_to_str(self.asof_date) + ","
        py_to_str = repr if for_repr else str
        if self.to_id != "OID":
            desc += "to_id=" + py_to_str(self.to_id) + ","
        py_to_str = repr if for_repr else str
        if self.exch != "USCOMP":
            desc += "exch=" + py_to_str(self.exch) + ","
        py_to_str = repr if for_repr else str
        if self.shared_state_variables:
            desc += "shared_state_variables=" + py_to_str(self.shared_state_variables) + ","
        desc = desc[:-1]
        if desc != name:
            desc += ")"
        if for_repr:
            return desc + '()' if desc == name else desc
        desc += "\n"
        if len(self._symbols) > 0:
            desc += "symbols=[" + ", ".join(self._symbols) + "]\n"
        if len(self._tick_types) > 0:
            desc += "tick_types=[" + ', '.join(self._tick_types) + "]\n"
        if self._process_node_locally:
            desc += "process_node_locally=True\n"
        if self._node_name:
            desc += "node_name=" + self._node_name + "\n"
        return desc
