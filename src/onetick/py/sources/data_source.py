import datetime as dt
import inspect
import warnings

from typing import Dict, Iterable, Optional

import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.db import _inspection
from onetick.py.core._source._symbol_param import _SymbolParamColumn
from onetick.py.core._source.tmp_otq import TmpOtq
from onetick.py.core.eval_query import _QueryEvalWrapper
from onetick.py.core.source import Source
from onetick.py.core.column_operations.base import Raw, OnetickParameter

from .. import types as ott
from .. import utils
from ..core.column_operations.base import _Operation
from ..db.db import DB
from ..compatibility import is_supported_where_clause_for_back_ticks

from onetick.py.docs.utils import docstring, param_doc

from .common import convert_tick_type_to_str, get_start_end_by_date
from .symbols import Symbols
from .ticks import Ticks
from .query import query


_db_doc = param_doc(
    name='db',
    desc="""
    Name(s) of the database or the database object(s).
    """,
    str_annotation='str, list of str, :class:`otp.DB <onetick.py.DB>`',
    default=None,
    str_default='None',
)
_symbol_doc = param_doc(
    name='symbol',
    desc="""
    Symbol(s) from which data should be taken.
    """,
    str_annotation='str, list of str, :class:`Source`, :class:`query`, :py:func:`eval query <onetick.py.eval>`',
    default=utils.adaptive,
    str_default=' :py:class:`onetick.py.adaptive`',
)
_symbols_doc = param_doc(
    name='symbols',
    desc="""
    Symbol(s) from which data should be taken.
    Alias for ``symbol`` parameter. Will take precedence over it.
    """,
    str_annotation=('str, list of str, :class:`Source`, :class:`query`, :py:func:`eval query <onetick.py.eval>`, '
                    ':py:class:`onetick.query.GraphQuery`.'),
    default=None,
)
_tick_type_doc = param_doc(
    name='tick_type',
    desc="""
    Tick type of the data.
    If not specified, all ticks from `db` will be taken.
    If ticks can't be found or there are many databases specified in `db` then default is "TRD".
    """,
    str_annotation='str, list of str',
    default=utils.adaptive,
    str_default=' :py:class:`onetick.py.adaptive`',
)
_start_doc = param_doc(
    name='start',
    desc="""
    Start of the interval from which the data should be taken.
    Default is :py:class:`onetick.py.adaptive`, making the final query deduce the time
    limits from the rest of the graph.
    """,
    str_annotation=(
        ':py:class:`datetime.datetime`, :py:class:`otp.datetime <onetick.py.datetime>`,'
        ' :py:class:`onetick.py.adaptive`'
    ),
    default=utils.adaptive,
    str_default=' :py:class:`onetick.py.adaptive`',
)
_end_doc = param_doc(
    name='end',
    desc="""
    End of the interval from which the data should be taken.
    Default is :py:class:`onetick.py.adaptive`, making the final query deduce the time
    limits from the rest of the graph.
    """,
    str_annotation=(
        ':py:class:`datetime.datetime`, :py:class:`otp.datetime <onetick.py.datetime>`,'
        ' :py:class:`onetick.py.adaptive`'
    ),
    default=utils.adaptive,
    str_default=' :py:class:`onetick.py.adaptive`',
)
_date_doc = param_doc(
    name='date',
    desc="""
    Allows to specify a whole day instead of passing explicitly ``start`` and ``end`` parameters.
    If it is set along with the ``start`` and ``end`` parameters then last two are ignored.
    """,
    str_annotation=":class:`datetime.datetime`, :class:`otp.datetime <onetick.py.datetime>`",
    default=None,
)
_schema_policy_doc = param_doc(
    name='schema_policy',
    desc="""
    Schema deduction policy:

    - 'tolerant' (default)
      The resulting schema is a combination of ``schema`` and database schema.
      If the database schema can be deduced,
      it's checked to be type-compatible with a ``schema``,
      and ValueError is raised if checks are failed.
      Also, with this policy database is scanned 5 days back to find the schema.
      It is useful when database is misconfigured or in case of holidays.

    - 'tolerant_strict'
      The resulting schema will be ``schema`` if it's not empty.
      Otherwise, database schema is used.
      If the database schema can be deduced,
      it's checked if it lacks fields from the ``schema``
      and it's checked to be type-compatible with a ``schema``
      and ValueError is raised if checks are failed.
      Also, with this policy database is scanned 5 days back to find the schema.
      It is useful when database is misconfigured or in case of holidays.

    - 'fail'
      The same as 'tolerant', but if the database schema can't be deduced, raises an Exception.

    - 'fail_strict'
      The same as 'tolerant_strict', but if the database schema can't be deduced, raises an Exception.

    - 'manual'
      The resulting schema is a combination of ``schema`` and database schema.
      Compatibility with database schema will not be checked.

    - 'manual_strict'
      The resulting schema will be exactly ``schema``.
      Compatibility with database schema will not be checked.
      If some fields specified in ``schema`` do not exist in the database,
      their values will be set to some default value for a type
      (0 for integers, NaNs for floats, empty string for strings, epoch for datetimes).

    Default value is :py:class:`onetick.py.adaptive` (if deprecated parameter ``guess_schema`` is not set).
    If ``guess_schema`` is set to True then value is 'fail', if False then 'manual'.
    If ``schema_policy`` is set to ``None`` then default value is 'tolerant'.

    Default value can be changed with
    :py:attr:`otp.config.default_schema_policy<onetick.py.configuration.Config.default_schema_policy>`
    configuration parameter.

    If you set schema manually, while creating DataSource instance, and don't set ``schema_policy``,
    it will be automatically set to ``manual``.
    """,
    str_annotation="'tolerant', 'tolerant_strict', 'fail', 'fail_strict', 'manual', 'manual_strict'",
    default=utils.adaptive,
    str_default=' :py:class:`onetick.py.adaptive`',
)
_guess_schema_doc = param_doc(
    name='guess_schema',
    desc="""
    .. deprecated:: 1.3.16

    Use ``schema_policy`` parameter instead.

    If ``guess_schema`` is set to True then ``schema_policy`` value is 'fail', if False then 'manual'.
    """,
    annotation=bool,
    default=None,
)
_identify_input_ts_doc = param_doc(
    name='identify_input_ts',
    desc="""
    If set to False, the fields SYMBOL_NAME and TICK_TYPE are not appended to the output ticks.
    """,
    annotation=bool,
    default=False,
)
_back_to_first_tick_doc = param_doc(
    name='back_to_first_tick',
    desc="""
    Determines how far back to go looking for the latest tick before ``start`` time.
    If one is found, it is inserted into the output time series with the timestamp set to ``start`` time.
    Note: it will be rounded to int, so otp.Millis(999) will be 0 seconds.
    """,
    str_annotation=('int, :ref:`offset <datetime_offsets>`, '
                    ':class:`otp.expr <onetick.py.expr>`, '
                    ':py:class:`~onetick.py.Operation`'),
    default=0,
)
_keep_first_tick_timestamp_doc = param_doc(
    name='keep_first_tick_timestamp',
    desc="""
    If set, new field with this name will be added to source.
    This field contains original timestamp of the tick that was taken from before the start time of the query.
    For all other ticks value in this field will be equal to the value of Time field.
    This parameter is ignored if ``back_to_first_tick`` is not set.
    """,
    annotation=str,
    default=None,
)
_presort_doc = param_doc(
    name='presort',
    desc="""
    Add the presort EP in case of bound symbols.
    Applicable only when ``symbols`` is not None.
    By default, it is set to True if ``symbols`` are set
    and to False otherwise.
    """,
    annotation=bool,
    default=utils.adaptive,
    str_default=' :py:class:`onetick.py.adaptive`',
)
_concurrency_doc = param_doc(
    name='concurrency',
    desc="""
    Specifies the number of CPU cores to utilize for the ``presort``.
    By default, the value is inherited from the value of the query where this PRESORT is used.

    For the main query it may be specified in the ``concurrency`` parameter of :meth:`run` method
    (which by default is set to
    :py:attr:`otp.config.default_concurrency<onetick.py.configuration.Config.default_concurrency>`).

    For the auxiliary queries (like first-stage queries) empty value means OneTick's default of 1.
    If :py:attr:`otp.config.presort_force_default_concurrency<onetick.py.configuration.Config.presort_force_default_concurrency>`
    is set then default concurrency value will be set in all PRESORT EPs in all queries.
    """,  # noqa: E501
    annotation=int,
    default=utils.default,
    str_default=' :py:class:`onetick.py.utils.default`',
)
_batch_size_doc = param_doc(
    name='batch_size',
    desc="""
    Specifies the query batch size for the ``presort``.
    By default, the value from
    :py:attr:`otp.config.default_batch_size<onetick.py.configuration.Config.default_batch_size>` is used.
    """,
    annotation=int,
    default=None,
)
_schema_doc = param_doc(
    name='schema',
    desc="""
    Dict of <column name> -> <column type> pairs that the source is expected to have.
    If the type is irrelevant, provide None as the type in question.
    """,
    annotation=Optional[Dict[str, type]],
    default=None,
)
_desired_schema_doc = param_doc(
    name='kwargs',
    desc="""
    Deprecated. Use ``schema`` instead.
    List of <column name> -> <column type> pairs that the source is expected to have.
    If the type is irrelevant, provide None as the type in question.
    """,
    str_annotation='type[str]',
    kind=inspect.Parameter.VAR_KEYWORD,
)

_max_back_ticks_to_prepend_doc = param_doc(
    name='max_back_ticks_to_prepend',
    desc="""
    When the ``back_to_first_tick`` interval is specified, this parameter determines the maximum number
    of the most recent ticks before start_time that will be prepended to the output time series.
    Their timestamp will be changed to start_time.
    """,
    annotation=int,
    default=1,
)

_where_clause_for_back_ticks_doc = param_doc(
    name='where_clause_for_back_ticks',
    desc="""
    A logical expression that is computed only for the ticks encountered when a query goes back from the start time,
    in search of the ticks to prepend. If it returns false, a tick is ignored.
    """,
    annotation=Raw,
    default=None,
)
_symbol_date_doc = param_doc(
    name='symbol_date',
    desc="""
    Symbol date or integer in the YYYYMMDD format.
    Can only be specified if parameters ``symbols`` is set.
    """,
    str_annotation=':py:class:`otp.datetime <onetick.py.datetime>` or :py:class:`datetime.datetime` or int',
    default=None,
)

DATA_SOURCE_DOC_PARAMS = [
    _db_doc, _symbol_doc, _tick_type_doc,
    _start_doc, _end_doc, _date_doc,
    _schema_policy_doc, _guess_schema_doc,
    _identify_input_ts_doc,
    _back_to_first_tick_doc, _keep_first_tick_timestamp_doc,
    _max_back_ticks_to_prepend_doc,
    _where_clause_for_back_ticks_doc,
    _symbols_doc,
    _presort_doc, _batch_size_doc, _concurrency_doc,
    _schema_doc,
    _symbol_date_doc,
    _desired_schema_doc,
]


class DataSource(Source):

    POLICY_MANUAL = "manual"
    POLICY_MANUAL_STRICT = "manual_strict"
    POLICY_TOLERANT = "tolerant"
    POLICY_TOLERANT_STRICT = "tolerant_strict"
    POLICY_FAIL = "fail"
    POLICY_FAIL_STRICT = "fail_strict"

    _VALID_POLICIES = frozenset([POLICY_MANUAL, POLICY_MANUAL_STRICT,
                                 POLICY_TOLERANT, POLICY_TOLERANT_STRICT,
                                 POLICY_FAIL, POLICY_FAIL_STRICT])
    _PROPERTIES = Source._PROPERTIES + ["_p_db", "_p_strict", "_p_schema", "_schema", "logger"]

    def __get_schema(self, db, start, schema_policy):
        schema = {}

        if start is utils.adaptive:
            start = None  # means that use the last date with data

        if isinstance(db, list):
            # This case of a merge, since we need to get combined schema across different tick types and dbs
            for t_db in db:
                if t_db.startswith('expr('):
                    continue

                _db = t_db.split(':')[0]
                _tt = t_db.split(':')[-1]

                # tick type as parameter
                if _tt.startswith('$'):
                    _tt = None

                db_obj = _inspection.DB(_db)
                if schema_policy == self.POLICY_TOLERANT and start:
                    # repeating the same logic as in db_obj.last_date
                    start = db_obj.last_not_empty_date(start, days_back=5, tick_type=_tt)

                db_schema = {}
                try:
                    db_schema = db_obj.schema(date=start, tick_type=_tt)
                except Exception as e:
                    if _tt is not None:
                        warnings.warn(f"Couldn't get schema from the database {db_obj}:\n{e}.\n\n"
                                      "Set parameter schema_policy='manual' to set the schema manually.")

                schema.update(db_schema)

        if db is None or isinstance(db, _SymbolParamColumn):
            # In this case we can't get schema, because db is calculated dynamically.
            # Set to empty to indicate that in this case we expect the manually set schema.
            schema = {}
        return schema

    def __prepare_schema(self, db, start, schema_policy, guess_schema, schema):
        if guess_schema is not None:
            warnings.warn(
                "guess_schema flag is deprecated; use schema_policy argument instead",
                FutureWarning,
            )
            if schema_policy is not None:
                raise ValueError("guess_schema and schema_policy cannot be set at the same time")
            if guess_schema:
                schema_policy = self.POLICY_FAIL
            else:
                schema_policy = self.POLICY_MANUAL

        if schema_policy is None:
            schema_policy = self.POLICY_TOLERANT
        if schema_policy not in self._VALID_POLICIES:
            raise ValueError(f"Invalid schema_policy; allowed values are: {self._VALID_POLICIES}")

        actual_schema = {}
        if schema_policy not in (self.POLICY_MANUAL, self.POLICY_MANUAL_STRICT):
            actual_schema = self.__get_schema(db, start, schema_policy)
            dbs = ', '.join(db if isinstance(db, list) else [])

            if len(actual_schema) == 0:
                if schema_policy in (self.POLICY_FAIL, self.POLICY_FAIL_STRICT):
                    raise ValueError(f'No ticks found in database(-s) {dbs}')
                # lets try to use at least something
                return schema.copy()

            for k, v in schema.items():
                field_type = actual_schema.get(k, None)
                incompatible_types = False
                if field_type is None:
                    if self._p_strict or schema_policy in (self.POLICY_TOLERANT, self.POLICY_FAIL):
                        raise ValueError(f"Database(-s) {dbs} schema has no {k} field")
                elif issubclass(field_type, str) and issubclass(v, str):
                    field_length = ott.string.DEFAULT_LENGTH
                    if issubclass(field_type, ott.string):
                        field_length = field_type.length
                    v_length = ott.string.DEFAULT_LENGTH
                    if issubclass(v, ott.string):
                        v_length = v.length
                    if issubclass(field_type, ott.varstring):
                        if not issubclass(v, ott.varstring):
                            incompatible_types = True
                    elif not issubclass(v, ott.varstring) and v_length < field_length:
                        incompatible_types = True
                elif not issubclass(field_type, v):
                    incompatible_types = True
                if incompatible_types:
                    error_message = f"Database(-s) {dbs} schema field {k} has type {field_type}, but {v} was requested"
                    if field_type in (str, ott.string) or v in (str, ott.string):
                        error_message = f"{error_message}. Notice, that `str` and `otp.string` lengths are 64"
                    raise ValueError(error_message)
            if not self._p_strict:
                schema.update(actual_schema)

        table_schema = schema.copy()
        if not self._p_strict:
            # in this case we will table only fields specified by user
            table_schema = {
                k: v for k, v in table_schema.items() if k not in actual_schema
            }
        return table_schema

    def __prepare_db_tick_type(self, db, tick_type, start, end):
        if isinstance(db, list):
            # If everything is correct then this branch should leave
            # the `db` var as a list of databases with tick types and the `tick_type` var is None.
            # Valid cases:
            #     - Fully defined case. The `db` parameter has a list of databases where
            #       every database has a tick type, when the `tick_type`
            #       parameter has default value or None (for backward compatibility)
            #     - Partially defined case. The `db` parameter has a list of databases but
            #       not every database has a tick type, and meantime the `tick_type`
            #       is passed to not None value. In that case databases without tick type
            #       are exetended with a tick type from the `tick_type` parameter
            #     - No defined case. The `db` parameter has a list of databases and
            #       every database there has no tick type, and the `tick_type` is
            #       set to not None value. In that case every database is extended with
            #       the tick type from the `tick_type`.

            def db_converter(_db):
                if isinstance(_db, DB):
                    return _db.name
                else:
                    return _db

            db = [db_converter(_db) for _db in db]
            res = all(('::' in _db and _db[-1] != ':' for _db in db))
            if res:
                if tick_type is utils.adaptive or tick_type is None:
                    tick_type = None  # tick types is specified for all databases
                else:
                    raise ValueError('The `tick_type` is set as a parameter '
                                     'and also as a part of the `db` parameter'
                                     'for every database')
            else:
                dbs_without_tt = [_db.split(':')[0] for _db in db
                                  if '::' not in _db or _db[-1] == ':']

                if tick_type is utils.adaptive:
                    tick_type = 'TRD'  # default one for backward compatibility and testing usecase
                if tick_type is None:
                    raise ValueError('The tick type is not set for databases: ' +
                                     ', '.join(dbs_without_tt))
                else:
                    # extend databases with missing tick types from the tick tick parameter
                    dbs_with_tt = [_db for _db in db
                                   if '::' in _db and _db[-1] != ':']

                    db = dbs_with_tt + [_db + '::' + tick_type for _db in dbs_without_tt]
                    tick_type = None

        if isinstance(db, (DB, _inspection.DB)):
            db = db.name  # ... and we go to the next branch

        if isinstance(db, str):
            # The resulting `db` var contains a list with string value, that has the `db`
            # concatenated with the `tick_type`.
            if '::' in db:
                if tick_type is utils.adaptive or tick_type is None:
                    tick_type = db.split(':')[-1]
                    db = db.split('::')[0]
                else:
                    raise ValueError('The `tick_type` is set as a parameter '
                                     'and also as a part of the `db` parameter')
            else:
                if tick_type is utils.adaptive or tick_type is None:
                    db_obj = _inspection.DB(db)

                    # try to find at least one common tick type
                    # through all days
                    tick_types = None

                    if start is utils.adaptive:
                        start = end = db_obj.get_last_date(show_warnings=False)

                    if start and end:  # could be None if there is no data
                        t_start = start
                        while t_start <= end:
                            t_tts = set(db_obj.tick_types(t_start))

                            t_start += dt.timedelta(days=1)

                            if len(t_tts) == 0:
                                continue

                            if tick_types is None:
                                tick_types = t_tts
                            else:
                                tick_types &= t_tts

                            if len(tick_types) == 0:
                                raise ValueError(f'It seems that there is no common '
                                                 f'tick types for dates from {start} '
                                                 f'to {end}. Please specify a tick '
                                                 'type')

                    if tick_types is None:
                        if tick_type is utils.adaptive:
                            tick_types = ['TRD']  # the default one
                        else:
                            raise ValueError(f'Could not find any data in from {start} '
                                             f' to {end}. Could you check that tick type, '
                                             ' database and date range are correct.')

                    if len(tick_types) != 1:
                        raise ValueError('The tick type is not specified, found '
                                         'multiple tick types in the database : ' +
                                         ', '.join(tick_types))

                    tick_type = tick_types.pop()

            if not isinstance(tick_type, str) and isinstance(tick_type, Iterable):
                if isinstance(tick_type, _SymbolParamColumn):
                    db = [f"expr('{db}::' + {str(tick_type)})"]
                else:
                    db = [f'{db}::{tt}' for tt in tick_type]
            else:
                db = [db + '::' + tick_type]
            tick_type = None

        if isinstance(db, _SymbolParamColumn):
            # Do nothing, because we don't know whether db will come with the tick type or not.
            # The only one thing that definetely we know that tick_type can not be utils.adaptive
            if tick_type is utils.adaptive:
                # TODO: need to test this case
                raise ValueError('The `db` is set to the symbol param, in that case '
                                 'the `tick_type` should be set explicitly to some value '
                                 'or to None')

        if db is None:
            # This case means that database comes with the symbol name, then tick type should be defined
            if tick_type is utils.adaptive or tick_type is None:
                raise ValueError('The `db` is not specified that means database is '
                                 'expected to be defined with the symbol name. '
                                 'In that case the `tick_type` should be defined.')
            if not isinstance(tick_type, str) and isinstance(tick_type, Iterable):
                tick_type = '+'.join(tick_type)

        return db, tick_type

    @docstring(parameters=DATA_SOURCE_DOC_PARAMS, add_self=True)
    def __init__(
        self,
        db=None,
        symbol=utils.adaptive,
        tick_type=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        date=None,
        schema=None,
        schema_policy=utils.adaptive,
        guess_schema=None,
        identify_input_ts=False,
        back_to_first_tick=0,
        keep_first_tick_timestamp=None,
        max_back_ticks_to_prepend=1,
        where_clause_for_back_ticks=None,
        symbols=None,
        presort=utils.adaptive,
        batch_size=None,
        concurrency=utils.default,
        symbol_date=None,
        **kwargs,
    ):
        """
        Construct a source providing data from a given ``db``.

        .. warning::

            Default value of the parameter ``schema_policy`` enables automatic deduction
            of the data schema, but it is highly not recommended for production code.
            For details see :ref:`static/concepts/schema:Schema deduction mechanism`.

        Note
        ----
        If interval that was set for :py:class:`~onetick.py.DataSource` via ``start``/``end`` or ``date`` parameters
        does not match intervals in other :py:class:`~onetick.py.Source` objects used in query,
        or does not match the whole query interval, then :py:meth:`~otp.Source.modify_query_times` will be applied
        to this ``DataSource`` with specified interval as start and end times parameters.

        If ``symbols`` parameter is omitted, you need to specify unbound symbols for the query in ``symbols``
        parameter of :py:func:`onetick.py.run` function.

        If ``symbols`` parameter is set, :meth:`otp.merge <onetick.py.merge>` is used to merge all passed bound symbols.
        In this case you don't need to specify unbound symbols in :py:func:`onetick.py.run` call.

        It's not allowed to specify bound and unbound symbols at the same time.

        See also
        --------
        :ref:`static/concepts/start_end:Query start / end flow`
        :ref:`static/concepts/symbols:Symbols: bound and unbound`

        Examples
        --------

        Query a single symbol from a database:

        >>> data = otp.DataSource(db='SOME_DB', tick_type='TT', symbols='S1')
        >>> otp.run(data)
                             Time  X
        0 2003-12-01 00:00:00.000  1
        1 2003-12-01 00:00:00.001  2
        2 2003-12-01 00:00:00.002  3

        Parameter ``symbols`` can be a list.
        In this case specified symbols will be merged into a single data flow:

        >>> # OTdirective: snippet-name:fetch data.simple;
        >>> data = otp.DataSource(db='SOME_DB', tick_type='TT', symbols=['S1', 'S2'])
        >>> otp.run(data)
                             Time  X
        0 2003-12-01 00:00:00.000  1
        1 2003-12-01 00:00:00.000 -3
        2 2003-12-01 00:00:00.001  2
        3 2003-12-01 00:00:00.001 -2
        4 2003-12-01 00:00:00.002  3
        5 2003-12-01 00:00:00.002 -1

        Parameter ``identify_input_ts`` can be used to automatically add field with symbol name for each tick:

        >>> data = otp.DataSource(db='SOME_DB', tick_type='TT', symbols=['S1', 'S2'], identify_input_ts=True)
        >>> otp.run(data)
                             Time SYMBOL_NAME TICK_TYPE  X
        0 2003-12-01 00:00:00.000          S1        TT  1
        1 2003-12-01 00:00:00.000          S2        TT -3
        2 2003-12-01 00:00:00.001          S1        TT  2
        3 2003-12-01 00:00:00.001          S2        TT -2
        4 2003-12-01 00:00:00.002          S1        TT  3
        5 2003-12-01 00:00:00.002          S2        TT -1

        Source also can be passed as symbols, in such case magic named column SYMBOL_NAME will be transform to symbol
        and all other columns will be symbol parameters

        >>> # OTdirective: snippet-name:fetch data.symbols as a source;
        >>> symbols = otp.Ticks(SYMBOL_NAME=['S1', 'S2'])
        >>> data = otp.DataSource(db='SOME_DB', symbols=symbols, tick_type='TT')
        >>> otp.run(data)
                             Time  X
        0 2003-12-01 00:00:00.000  1
        1 2003-12-01 00:00:00.000 -3
        2 2003-12-01 00:00:00.001  2
        3 2003-12-01 00:00:00.001 -2
        4 2003-12-01 00:00:00.002  3
        5 2003-12-01 00:00:00.002 -1

        Default schema policy is **tolerant** (unless you specified ``schema`` parameter and
        left ``schema_policy`` with default value, when it will be set to **manual**).

        >>> data = otp.DataSource(
        ...     db='US_COMP', tick_type='TRD', symbols='AAPL', date=otp.dt(2022, 3, 1),
        ... )
        >>> data.schema
        {'PRICE': <class 'float'>, 'SIZE': <class 'int'>}

        >>> data = otp.DataSource(
        ...     db='US_COMP', tick_type='TRD', symbols='AAPL', schema={'PRICE': int},
        ...     schema_policy='tolerant', date=otp.dt(2022, 3, 1),
        ... )
        Traceback (most recent call last):
          ...
        ValueError: Database(-s) US_COMP::TRD schema field PRICE has type <class 'float'>,
        but <class 'int'> was requested

        Schema policy **manual** uses exactly ``schema``:

        >>> data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL', schema={'PRICE': float},
        ...                       date=otp.dt(2022, 3, 1), schema_policy='manual')
        >>> data.schema
        {'PRICE': <class 'float'>}

        Schema policy **fail** raises an exception if the schema cannot be deduced:

        >>> data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL', date=otp.dt(2021, 3, 1),
        ...                       schema_policy='fail')
        Traceback (most recent call last):
          ...
        ValueError: No ticks found in database(-s) US_COMP::TRD

        ``back_to_first_tick`` sets how far back to go looking for the latest tick before ``start`` time:

        >>> data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL', date=otp.dt(2022, 3, 2),
        ...                       back_to_first_tick=otp.Day(1))
        >>> otp.run(data)
                             Time  PRICE  SIZE
        0 2022-03-02 00:00:00.000    1.4    50
        1 2022-03-02 00:00:00.000    1.0   100
        2 2022-03-02 00:00:00.001    1.1   101
        3 2022-03-02 00:00:00.002    1.2   102

        ``keep_first_tick_timestamp`` allows to show the original timestamp of the tick that was taken from before
        the start time of the query:

        >>> data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL', date=otp.dt(2022, 3, 2),
        ...                       back_to_first_tick=otp.Day(1), keep_first_tick_timestamp='ORIGIN_TIMESTAMP')
        >>> otp.run(data)
                             Time         ORIGIN_TIMESTAMP  PRICE  SIZE
        0 2022-03-02 00:00:00.000  2022-03-01 00:00:00.002    1.4    50
        1 2022-03-02 00:00:00.000  2022-03-02 00:00:00.000    1.0   100
        2 2022-03-02 00:00:00.001  2022-03-02 00:00:00.001    1.1   101
        3 2022-03-02 00:00:00.002  2022-03-02 00:00:00.002    1.2   102

        ``max_back_ticks_to_prepend`` is used with ``back_to_first_tick``
        if more than 1 ticks before start time should be retrieved:

        >>> data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL', date=otp.dt(2022, 3, 2),
        ...                       max_back_ticks_to_prepend=2, back_to_first_tick=otp.Day(1),
        ...                       keep_first_tick_timestamp='ORIGIN_TIMESTAMP')
        >>> otp.run(data)
                             Time         ORIGIN_TIMESTAMP  PRICE  SIZE
        0 2022-03-02 00:00:00.000  2022-03-01 00:00:00.001    1.4    10
        1 2022-03-02 00:00:00.000  2022-03-01 00:00:00.002    1.4    50
        2 2022-03-02 00:00:00.000  2022-03-02 00:00:00.000    1.0   100
        3 2022-03-02 00:00:00.001  2022-03-02 00:00:00.001    1.1   101
        4 2022-03-02 00:00:00.002  2022-03-02 00:00:00.002    1.2   102

        ``where_clause_for_back_ticks`` is used to filter out ticks before the start time:

        .. testcode::
           :skipif: not is_supported_where_clause_for_back_ticks()

           data = otp.DataSource(db='US_COMP', tick_type='TRD', symbols='AAPL', date=otp.dt(2022, 3, 2),
                                 where_clause_for_back_ticks=otp.raw('SIZE>=50', dtype=bool),
                                 back_to_first_tick=otp.Day(1), max_back_ticks_to_prepend=2,
                                 keep_first_tick_timestamp='ORIGIN_TIMESTAMP')
           df = otp.run(data)
           print(df)

        .. testoutput::

                                Time         ORIGIN_TIMESTAMP  PRICE  SIZE
           0 2022-03-02 00:00:00.000  2022-03-01 00:00:00.000    1.3   100
           1 2022-03-02 00:00:00.000  2022-03-01 00:00:00.002    1.4    50
           2 2022-03-02 00:00:00.000  2022-03-02 00:00:00.000    1.0   100
           3 2022-03-02 00:00:00.001  2022-03-02 00:00:00.001    1.1   101
           4 2022-03-02 00:00:00.002  2022-03-02 00:00:00.002    1.2   102
        """

        self.logger = otp.get_logger(__name__, self.__class__.__name__)

        if self._try_default_constructor(schema=schema, **kwargs):
            return

        schema = self._select_schema(schema, kwargs)

        if schema and (not schema_policy or schema_policy is utils.adaptive):
            schema_policy = self.POLICY_MANUAL

        if schema_policy is utils.adaptive:
            schema_policy = otp.config.default_schema_policy

        # for cases when we want to explicitly convert into string,
        # it might be symbol param or join_with_query parameter
        if isinstance(tick_type, OnetickParameter):
            tick_type = tick_type.parameter_expression

        if date:
            # TODO: write a warning in that case
            start, end = get_start_end_by_date(date)

        db, tick_type = self.__prepare_db_tick_type(db,
                                                    tick_type,
                                                    start,
                                                    end)

        self._p_db = db

        if not schema and schema_policy == self.POLICY_MANUAL_STRICT:
            raise ValueError(
                f"'{self.POLICY_MANUAL_STRICT}' schema policy was specified, but no schema has been provided"
            )

        self._p_strict = schema_policy in (self.POLICY_FAIL_STRICT,
                                           self.POLICY_TOLERANT_STRICT,
                                           self.POLICY_MANUAL_STRICT)

        # this is deprecated, but user may have set some complex types or values in schema,
        # let's infer basic onetick-py types from them
        for k, v in schema.items():
            schema[k] = ott.get_source_base_type(v)

        self._p_schema = self.__prepare_schema(db,  # tick type is embedded into the db
                                               start,
                                               schema_policy,
                                               guess_schema,
                                               schema)

        if symbols is not None:
            if symbol is utils.adaptive or symbol is None:
                symbol = symbols
            else:
                # TODO: test it
                raise ValueError('You have set the `symbol` and `symbols` parameters'
                                 'together, it is not allowed. Please, clarify parameters')

        if symbol_date is not None:
            if symbol is utils.adaptive or symbol is None:
                raise ValueError("Parameter 'symbol_date' can only be specified together with parameter 'symbols'")
            if isinstance(symbol, (str, list)):
                # this is a hack
                # onetick.query doesn't have an interface to set symbol_date for the EP node
                # so instead of setting symbols for the EP node,
                # we will turn symbol list into the first stage query, and symbol_date will be set for this query
                if isinstance(symbol, str):
                    symbol = [symbol]
                symbol = Ticks(SYMBOL_NAME=symbol)

        if isinstance(symbol, Symbols) and symbol._p_db is None:
            symbol = Symbols.duplicate(symbol, db=db)

        if identify_input_ts:
            if "SYMBOL_NAME" in schema:
                # TODO: think about how user could workaround it
                raise ValueError("Parameter 'identify_input_ts' is set,"
                                 " but field 'SYMBOL_NAME' is already in the schema")
            schema["SYMBOL_NAME"] = str
            self._p_schema["SYMBOL_NAME"] = str
            if "TICK_TYPE" in schema:
                raise ValueError("Parameter 'identify_input_ts' is set,"
                                 " but field 'TICK_TYPE' is already in the schema")
            schema["TICK_TYPE"] = str
            self._p_schema["TICK_TYPE"] = str

        # unobvious way to convert otp.Minute/Hour/... to number of seconds
        if type(back_to_first_tick).__name__ == '_DatePartCls':
            back_to_first_tick = int((ott.dt(0) + back_to_first_tick).timestamp())

        if isinstance(back_to_first_tick, _Operation):
            back_to_first_tick = otp.expr(back_to_first_tick)

        if back_to_first_tick != 0 and keep_first_tick_timestamp:
            schema[keep_first_tick_timestamp] = ott.nsectime
            self._p_schema[keep_first_tick_timestamp] = ott.nsectime

        if max_back_ticks_to_prepend < 1:
            raise ValueError(f'`max_back_ticks_to_prepend` must be at least 1 '
                             f'but {max_back_ticks_to_prepend} was passed')

        if where_clause_for_back_ticks is not None:
            # TODO: add otp.param here
            if not isinstance(where_clause_for_back_ticks, Raw):
                raise ValueError(f'Currently only otp.raw is supported for `where_clause_for_back_ticks` '
                                 f'but {type(where_clause_for_back_ticks)} was passed')
            if where_clause_for_back_ticks.dtype is not bool:
                raise ValueError(f'Only bool dtype for otp.raw in `where_clause_for_back_ticks` is supported '
                                 f'but {where_clause_for_back_ticks.dtype} was passed')
            where_clause_for_back_ticks = str(where_clause_for_back_ticks)

        self._schema = schema

        if isinstance(symbol, _QueryEvalWrapper):
            symbol_str = repr(symbol)
        else:
            symbol_str = symbol
        self.logger.info(
            otp.utils.json_dumps(dict(db=db, symbol=symbol_str, tick_type=tick_type, start=start, end=end))
        )

        if (
            isinstance(symbol, (Source, query, _QueryEvalWrapper, otq.GraphQuery))
            or hasattr(symbol, "__iter__")
            and not isinstance(symbol, (dict, str, OnetickParameter, _SymbolParamColumn))
        ):
            super().__init__(
                _start=start,
                _end=end,
                _base_ep_func=lambda: self._base_ep_for_cross_symbol(
                    db, symbol, tick_type,
                    identify_input_ts=identify_input_ts,
                    back_to_first_tick=back_to_first_tick,
                    keep_first_tick_timestamp=keep_first_tick_timestamp,
                    presort=presort, batch_size=batch_size, concurrency=concurrency,
                    max_back_ticks_to_prepend=max_back_ticks_to_prepend,
                    where_clause_for_back_ticks=where_clause_for_back_ticks,
                    symbol_date=symbol_date,
                ),
                schema=schema,
            )
        else:
            super().__init__(
                _symbols=symbol,
                _start=start,
                _end=end,
                _base_ep_func=lambda: self.base_ep(
                    db,
                    tick_type,
                    identify_input_ts=identify_input_ts,
                    back_to_first_tick=back_to_first_tick,
                    keep_first_tick_timestamp=keep_first_tick_timestamp,
                    max_back_ticks_to_prepend=max_back_ticks_to_prepend,
                    where_clause_for_back_ticks=where_clause_for_back_ticks,
                ),
                schema=schema,
            )

    @property
    def db(self):
        return self._p_db

    def _create_source(self, passthrough_ep, back_to_first_tick=0, keep_first_tick_timestamp=None):
        """Create graph that save original timestamp of first tick if needed"""
        if back_to_first_tick != 0 and keep_first_tick_timestamp:
            src = Source(otq.Passthrough(), schema=self._schema)
            src.sink(otq.AddField(field=keep_first_tick_timestamp, value='TIMESTAMP'))
            src.sink(passthrough_ep)
            return src
        return Source(passthrough_ep, schema=self._schema)

    def _table_schema(self, src):
        return src.table(**self._p_schema, strict=self._p_strict)

    def base_ep(
        self,
        db,
        tick_type,
        identify_input_ts,
        back_to_first_tick=0,
        keep_first_tick_timestamp=None,
        max_back_ticks_to_prepend=1,
        where_clause_for_back_ticks=None,
    ):
        str_db = convert_tick_type_to_str(tick_type, db)
        params = dict(
            go_back_to_first_tick=back_to_first_tick,
            max_back_ticks_to_prepend=max_back_ticks_to_prepend,
        )

        if where_clause_for_back_ticks is not None:
            params['where_clause_for_back_ticks'] = where_clause_for_back_ticks

        if isinstance(db, (list, _SymbolParamColumn)):
            src = self._create_source(otq.Passthrough(**params),
                                      back_to_first_tick=back_to_first_tick,
                                      keep_first_tick_timestamp=keep_first_tick_timestamp)

            if identify_input_ts or '+' in str_db or str_db.startswith('expr('):
                # PY-941: use MERGE only if we need to identify input or there are many databases,
                # otherwise use PASSTHROUGH, it seems to work faster in some cases
                src.sink(otq.Merge(identify_input_ts=identify_input_ts))
        else:
            if identify_input_ts:
                params["fields"] = "SYMBOL_NAME,TICK_TYPE"
                params["drop_fields"] = True

            src = self._create_source(otq.Passthrough(**params),
                                      back_to_first_tick=back_to_first_tick,
                                      keep_first_tick_timestamp=keep_first_tick_timestamp)
        src.tick_type(str_db)

        src = self._table_schema(src)
        return src

    def _cross_symbol_convert(self, symbol, symbol_date=None):
        tmp_otq = TmpOtq()

        if isinstance(symbol, _QueryEvalWrapper):
            symbol = symbol.to_eval_string(tmp_otq=tmp_otq, symbol_date=symbol_date)
        elif isinstance(symbol, query):
            if symbol_date is not None:
                raise ValueError("Parameter 'symbol_date' is not supported if symbols are set with otp.query object")
            symbol = symbol.to_eval_string()
        elif isinstance(symbol, (Source, otq.GraphQuery)):
            symbol = Source._convert_symbol_to_string(symbol, tmp_otq, symbol_date=symbol_date)

        return symbol, tmp_otq

    def _base_ep_for_cross_symbol(
        self, db, symbol, tick_type, identify_input_ts,
        back_to_first_tick=0, keep_first_tick_timestamp=None,
        presort=utils.adaptive, batch_size=None, concurrency=utils.default,
        max_back_ticks_to_prepend=1,
        where_clause_for_back_ticks=None,
        symbol_date=None,
    ):
        symbol, tmp_otq = self._cross_symbol_convert(symbol, symbol_date)

        self.logger.info(f'symbol={symbol}')

        tick_type = convert_tick_type_to_str(tick_type, db)

        kwargs = dict(
            go_back_to_first_tick=back_to_first_tick,
            max_back_ticks_to_prepend=max_back_ticks_to_prepend,
        )

        if where_clause_for_back_ticks is not None:
            kwargs['where_clause_for_back_ticks'] = where_clause_for_back_ticks

        src = self._create_source(otq.Passthrough(**kwargs),
                                  back_to_first_tick=back_to_first_tick,
                                  keep_first_tick_timestamp=keep_first_tick_timestamp)
        if presort is utils.adaptive:
            presort = True
        if presort:
            if batch_size is None:
                batch_size = otp.config.default_batch_size
            if concurrency is utils.default:
                concurrency = otp.configuration.default_presort_concurrency()
            if concurrency is None:
                # None means inherit concurrency from the query where this EP is used
                # otq.Presort does not support None
                concurrency = ''
            src.sink(
                otq.Presort(batch_size=batch_size, max_concurrency=concurrency).symbols(symbol).tick_type(tick_type)
            )
            src.sink(otq.Merge(identify_input_ts=identify_input_ts))
        else:
            src.sink(
                otq.Merge(identify_input_ts=identify_input_ts).symbols(symbol).tick_type(tick_type)
            )

        src._tmp_otq.merge(tmp_otq)

        src = self._table_schema(src)
        return src


Custom = DataSource  # for backward compatiblity, previously we had only Custom
