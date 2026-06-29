from typing import Optional

import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.core.source import Source
from onetick.py.core._source.query_parameters import QueryParameters
from onetick.py.core.column_operations.base import OnetickParameter

from .. import types as ott
from .. import utils

from .common import update_node_tick_type, AdaptiveTickType


class SymbologyMapping(Source):
    _PROPERTIES = Source._PROPERTIES + ["_p_dest_symbology"]

    def __init__(
        self,
        dest_symbology: Optional[str] = None,
        tick_type: Optional[AdaptiveTickType] = utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        symbols=utils.adaptive,
        schema=None,
        query_parameters: QueryParameters = None,
        **kwargs,
    ):
        """
        Shows symbology mapping information for specified securities stored in the reference database.

        Input (source) symbology is taken from the input symbol,
        if it has a symbology part in it (e.g., RIC::REUTERS::MSFT),
        or defaults to that of the input database, which is specified in the locator file.

        Parameter ``symbol_date`` must be set in :py:func:`otp.run <onetick.py.run>`
        for this source to work.

        Parameters
        ----------
        dest_symbology: str, :py:class:`otp.param <onetick.py.core.column_operations.base.OnetickParameter>`
            Specifying the destination symbology for symbol translation.
        tick_type: str
            Tick type to set on the OneTick's graph node.
            Can be used to specify database name with tick type or tick type only.

            By default setting this parameter is not required, database is usually set
            with parameter ``symbols`` or in :py:func:`otp.run <onetick.py.run>`.
        start:
            Custom start time of the source.
            If set, will override the value specified in :py:func:`otp.run <onetick.py.run>`.
        end:
            Custom end time of the source.
            If set, will override the value specified in :py:func:`otp.run <onetick.py.run>`.
        symbols:
            Symbol(s) from which data should be taken.
            If set, will override the value specified in :py:func:`otp.run <onetick.py.run>`.
        query_parameters: :py:class:`otp.QueryParameters <onetick.py.QueryParameters>`
            Additional query properties to be set in the resulting .otq file.
            They will be used if they are not overridden by other parameters or in :py:func:`otp.run <onetick.py.run>`.

        Examples
        --------

        Getting mapping for OID symbology for one symbol:

        >>> data = otp.SymbologyMapping(dest_symbology='OID')
        >>> otp.run(data, symbols='US_COMP_SAMPLE::AAPL',  # doctest: +SKIP
        ...         symbol_date=otp.dt(2024, 2, 1),
        ...         date=otp.dt(2024, 2, 1))
                Time END_DATETIME MAPPED_SYMBOL_NAME
        0 2024-02-01   2024-02-02               9706

        Getting mapping for all symbols in `US_COMP_SAMPLE` database in single source:

        >>> data = otp.SymbologyMapping(dest_symbology='OID')
        >>> data = otp.merge([data],
        ...                  symbols=otp.Symbols('US_COMP_SAMPLE', keep_db=True),
        ...                  identify_input_ts=True)
        >>> data = data[['SYMBOL_NAME', 'MAPPED_SYMBOL_NAME']]
        >>> otp.run(data, symbol_date=otp.dt(2024, 2, 1), date=otp.dt(2024, 2, 1))  # doctest: +SKIP
                           Time           SYMBOL_NAME MAPPED_SYMBOL_NAME
        0   2024-02-01 00:00:00     US_COMP_SAMPLE::A               3751
        1   2024-02-01 00:00:00   US_COMP_SAMPLE::AAL             322707
        2   2024-02-01 00:00:00  US_COMP_SAMPLE::AAPL               9706
        3   2024-02-01 00:00:00  US_COMP_SAMPLE::ABBV             289689
        4   2024-02-01 00:00:00  US_COMP_SAMPLE::ABNB             698663
        ..                  ...                   ...                ...
        496 2024-02-01 00:00:00   US_COMP_SAMPLE::YUM             208045
        497 2024-02-01 00:00:00   US_COMP_SAMPLE::ZBH             208268
        498 2024-02-01 00:00:00  US_COMP_SAMPLE::ZBRA             208166
        499 2024-02-01 00:00:00   US_COMP_SAMPLE::ZTS             291586
        500 2024-02-01 06:00:00   US_COMP_SAMPLE::DAY             668642
        """
        if self._try_default_constructor(schema=schema, **kwargs):
            return

        if dest_symbology is None:
            raise TypeError("Missing required argument: 'dest_symbology'")

        if isinstance(dest_symbology, OnetickParameter):
            dest_symbology = dest_symbology.parameter_expression

        self._p_dest_symbology = dest_symbology

        super().__init__(
            _symbols=symbols,
            _start=start,
            _end=end,
            _base_ep_func=lambda: self.base_ep(dest_symbology, tick_type),
            schema=schema,
            query_parameters=query_parameters,
            **kwargs,
        )

        self.schema['MAPPED_SYMBOL_NAME'] = str
        self.schema['END_DATETIME'] = ott.nsectime

    @property
    def dest_symbology(self):
        return self._p_dest_symbology

    def base_ep(self, dest_symbology, tick_type):
        src = Source(otq.SymbologyMapping(dest_symbology=dest_symbology))
        update_node_tick_type(src, tick_type)
        return src
