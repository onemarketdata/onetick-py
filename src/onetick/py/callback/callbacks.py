import datetime

import pandas as pd

from onetick.py.backports import cached_property
from onetick.py import utils
from . import CallbackBase


class LogCallback(CallbackBase):
    def __init__(self, query_name):
        print(f'Running query {query_name}')
        super().__init__()

    def process_symbol_name(self, symbol_name):
        print(f'Processing symbol {symbol_name}')


class ManualDataframeCallback(CallbackBase):
    """
    This callback class can be used to generate the same :pandas:`pandas.DataFrame` result as in otp.run.
    Unlike otp.run, here result is constructed manually, one tick at a time.
    This may lead to lower memory usage in some cases.
    See task PY-863 for details.
    """

    def __init__(self, timezone, callback_objects=None):
        # getting timezone from user as string or from replicating as object
        self._timezone = utils.tz.get_tzfile_by_name(timezone) if isinstance(timezone, str) else timezone
        # list of all ManualDataframeCallback objects, passed when replicating
        self._callback_objects = [] if callback_objects is None else callback_objects
        self._callback_objects.append(self)
        # ManualDataframeCallback is replicated for each symbol
        self._symbol_name = None
        # dict of columns names and lists with their values
        self._columns = None
        self._defaults = None
        self._result = None

    def replicate(self):
        """
        Object is replicated for each symbol.
        Passing timezone and list to save all objects to aggregate them in the end.
        """
        return ManualDataframeCallback(self._timezone, callback_objects=self._callback_objects)

    def process_symbol_name(self, symbol_name):
        """
        Called once for each ManualDataframeCallback object
        """
        self._symbol_name = symbol_name

    def process_tick_descriptor(self, tick_descriptor):
        """
        Called for each change of schema.
        For example, different dates in database may have different schemas.
        In this case we are merging them together.
        """
        if self._columns is None:
            self._columns = {'Time': []}
            self._defaults = {}
        for name, type_dict in tick_descriptor:
            if name not in self._columns:
                dtype_str = type_dict['type']
                default = self._get_default_value_by_type(dtype_str)
                # save default value for column, will be used in other places
                self._defaults[name] = default
                # update previous ticks with default value of the new field
                self._columns[name] = [default] * len(self._columns['Time'])

    def process_tick(self, tick, time):
        """
        Called for each tick of the symbol.
        """
        assert self._columns is not None
        for name in self._columns:
            if name == 'Time':
                value = self._convert_to_timezone(time)
            elif name not in tick:
                value = self._defaults[name]
            else:
                value = tick[name]
                if isinstance(value, datetime.datetime):
                    # datetime values are always in UTC
                    value = self._convert_to_timezone(value)
            self._columns[name].append(value)

    def done(self):
        """
        Called once for each symbol after all ticks are processed.
        """
        self._result = pd.DataFrame(self._columns)

    @cached_property
    def result(self):
        """
        Must be called by the user after callback object is used in otp.run.
        Aggregating results from all ManualDataframeCallback objects
        and returning pandas dataframe or dict of such (same as result of otp.run in 'df' output mode).
        """
        results = {
            callback_object._symbol_name: callback_object._result
            for callback_object in self._callback_objects
            if callback_object._result is not None
        }
        # if we have only one symbol, return dataframe, not dict
        if len(results) == 1:
            return results.popitem()[1]
        return results

    def _convert_to_timezone(self, dt):
        """
        Converting timezone-naive ``dt`` object in UTC timezone
        to the timezone specified by user and returning also timezone-naive object.
        """
        return utils.convert_timezone(dt, 'UTC', self._timezone)

    def _get_default_value_by_type(self, dtype_str):
        """
        Converting type names returned by process_tick_descriptor()
        """
        defaults = {
            'int': 0,
            'float': 0.0,
            'string': '',
            'datetime': self._convert_to_timezone(
                # epoch in timezone specified by user converted to UTC, that's what OneTick would return
                pd.Timestamp(1970, 1, 1).tz_localize(self._timezone).tz_convert('UTC').tz_localize(None)
            )
        }
        return defaults[dtype_str]
