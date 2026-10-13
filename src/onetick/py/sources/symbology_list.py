import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.core.source import Source
from onetick.py.core._source.query_parameters import QueryParameters

from .. import utils

from .common import update_node_tick_type


class ShowSymbologyList(Source):
    def __init__(
        self,
        symbol=utils.adaptive,
        db=utils.adaptive_to_default,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        query_parameters: QueryParameters = None,
        **kwargs,
    ):
        """
        Lists local symbologies and symbologies from directly or indirectly attached tick servers.

        Output ticks have ``SYMBOLOGY`` field with the name of the symbology.

        Parameters
        ----------
        symbol: str, list of str, :class:`Source`, :class:`query`, :py:func:`eval query <onetick.py.eval>`
            Symbol(s) from which data should be taken.
            If set, will override the value specified in :py:func:`otp.run <onetick.py.run>`.
        db: str
            Database name to set on the OneTick's graph node.
            By default :py:attr:`otp.config.default_db <onetick.py.configuration.Config.default_db>` is used.
        tick_type: str
            Tick type to set on the OneTick's graph node.
            Default: ANY.
        start: :py:class:`otp.datetime <onetick.py.datetime>`
            Custom start time of the source.
            If set, will override the value specified in :py:func:`otp.run <onetick.py.run>`.
        end: :py:class:`otp.datetime <onetick.py.datetime>`
            Custom end time of the source.
            If set, will override the value specified in :py:func:`otp.run <onetick.py.run>`.
        query_parameters: :py:class:`otp.QueryParameters <onetick.py.QueryParameters>`
            Additional query properties to be set in the resulting .otq file.
            They will be used if they are not overridden by other parameters or in :py:func:`otp.run <onetick.py.run>`.

        See also
        --------
        | **SHOW_SYMBOLOGY_LIST** OneTick event processor
        | :py:class:`onetick.py.SymbologyMapping`
        | :py:func:`onetick.py.misc.get_symbology_mapping`

        Examples
        --------
        List all available symbologies:

        >>> src = otp.ShowSymbologyList()
        >>> otp.run(src)  # doctest: +ELLIPSIS
                Time SYMBOLOGY
        0 2003-12-01       BBG
        1 2003-12-01      BSYM
        2 2003-12-01      BTKR
        ...
        """
        if self._try_default_constructor(**kwargs):
            return

        super().__init__(
            _symbols=symbol,
            _start=start,
            _end=end,
            _base_ep_func=lambda: self.base_ep(db=db, tick_type=tick_type),
            schema={'SYMBOLOGY': otp.string[256]},
            query_parameters=query_parameters,
            **kwargs,
        )

    def base_ep(self, db=utils.adaptive_to_default, tick_type=utils.adaptive):
        src = Source(otq.ShowSymbologyList())
        update_node_tick_type(src, tick_type, db)
        return src
