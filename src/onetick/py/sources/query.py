import operator
import os

import onetick.py as otp
from onetick.py.otq import otq

from onetick.py.core.source import Source

from .. import types as ott
from .. import utils, configuration
from ..core import query_inspector
from ..core.column_operations.base import _Operation


_QUERY_PARAM_SPECIAL_CHARACTERS = "=,"


class Query(Source):
    def __init__(
        self,
        query_object=None,
        out_pin=utils.adaptive,
        symbol=utils.adaptive,
        start=utils.adaptive,
        end=utils.adaptive,
        params=None,
        schema=None,
        **kwargs,
    ):
        """
        Create data source object from .otq file or query object

        Parameters
        ----------
        query_object: path or :class:`query`
            query to use as a data source
        out_pin: str
             query output pin name
        symbol: None, :py:class:`onetick.py.adaptive`
            Symbol(s) from which data should be taken.
        start, end : :py:class:`datetime.datetime`, :py:class:`otp.datetime <onetick.py.datetime>` or utils.adaptive
            Time interval from which the data should be taken.
        params: dict
            params to pass to query.
            Only applicable to string ``query_object``
        """
        if self._try_default_constructor(schema=schema, **kwargs):
            return

        if params is None:
            params = {}

        # Ignore because of the "Only @runtime_checkable protocols can be used with instance and class checks"
        if isinstance(query_object, (str, os.PathLike)):  # type: ignore
            query_object = query(str(query_object), **params)
        elif isinstance(query_object, query):
            if len(params) > 0:
                raise ValueError("Cannot pass both params and a query() (not str) query_object parameter")
        else:
            raise ValueError("query_object parameter has to be either a str (path to the query) or a query object")

        if symbol == utils.adaptive:
            if query_object.graph_info is None or not query_object.graph_info.has_unbound_sources:
                symbol = None
        elif symbol is not None:
            raise ValueError("symbol parameter should be either None or otp.adaptive")

        super().__init__(
            _symbols=symbol, _start=start, _end=end, _base_ep_func=lambda: self.base_ep(query_object, out_pin),
            schema=schema, **kwargs,
        )

    def base_ep(self, query_object, out_pin):
        nested = otq.NestedOtq(query_object.path, query_object.str_params)
        graph = query_object.graph_info

        if out_pin is utils.adaptive:
            if graph is not None:
                if len(graph.nested_outputs) == 1:
                    return Source(nested[graph.nested_outputs[0].NESTED_OUTPUT])
                if len(graph.nested_outputs) > 1:
                    raise ValueError(
                        f'Query "{query_object.query_name}" has multiple outputs, but you have not '
                        "specified which one should be used. You could specify it"
                        ' using "out_pin" parameter of the Query constructor.'
                    )
            # no output
            return Source(nested, _has_output=False)

        if graph is not None:
            existed_out_pins = set(map(operator.attrgetter("NESTED_OUTPUT"), graph.nested_outputs))
            if out_pin not in existed_out_pins:
                raise ValueError(
                    f'Query "{query_object.query_name}" does not have the "{out_pin}" output, there are only following '
                    f"output pins exist: {','.join(existed_out_pins)}"
                )
        return Source(nested[out_pin])


class query:
    """
    Constructs a query object with a certain path.
    Keyword arguments specify query parameters.

    You also can pass an instance of ``otp.query.config`` class as the second positional argument to
    specify a query.

    Parameters
    ----------

    path : str
        path to an .otq file.
        If path is relative, then it's assumed that file is located in one of the directories
        specified in OneTick ``OTQ_FILE_PATH`` configuration variable.
        If there are more than one query in the file, then its name should be specified
        in the format ``<path>::<query-name>``.

        Also prefix ``remote://<database-name>::`` can be used to specify if query is located
        on the remote server.
        If such path exists locally too, then this file is inspected to get info about query and its pins
        as we can't get this info from remote server.
    config:
        optional ``otp.query.config`` object.
        This object can be used to specify different query options, e.g. output columns.
    params:
        parameters for the query.
        Dictionary if parameters' names and their values.

    Raises
    ------
    ValueError, TypeError

    Examples
    --------

    Adding local query and applying it to the source:

    >>> q = otp.query('/otqs/some.otq::some_query', PARAM1='val1', PARAM2=3.14)  # doctest: +SKIP
    >>> t = otp.Tick(A=1)
    >>> t = t.apply(q)  # doctest: +SKIP

    Adding remote query:

    >>> otp.query('remote://DATABASE::/otqs/some.otq::some_query', PARAM1='val1', PARAM2=3.14)  # doctest: +ELLIPSIS
    <onetick.py.sources.query.query object at ...>

    Creating python wrapper function around query from ``.otq`` file:

    >>> def add_w(src):
    ...     schema = dict(src.schema)
    ...     if 'W' in schema:
    ...         raise ValueError("column 'W' already exists")
    ...     else:
    ...         schema['W'] = str
    ...     q = otp.query('add_w.otq::w', otp.query.config(output_columns=list(schema.items())))
    ...     return src.apply(q)
    >>> t = otp.Tick(A=1)
    >>> t = add_w(t)
    >>> t.schema
    {'A': <class 'int'>, 'W': <class 'str'>}
    >>> t['X'] = t['W'].str.upper()
    >>> otp.run(t)
            Time  A      W      X
    0 2003-12-01  1  hello  HELLO
    """

    class config:
        """
        The config allows to specify different query options.
        """

        _special_values = {"input"}

        def __init__(self, output_columns=None):
            """
            Parameters
            ----------

            output_columns : str, list, dict, optional
                The parameter defines what the outputs columns are.
                Default value is ``None`` that means no output fields after applying query
                for every output pin.

                The ``input`` string value means that output columns are the same as inputs for
                every output pin.

                A list of tuples allows to define output columns with their types;
                for example ``[('x', int), ('y', float), ...]``. Applicable for every output
                pin.

                A dict allows to specify output columns for every output pin.

            Raises
            ------
            TypeError, ValueError

            Examples
            --------
            >>> otp.query('/otqs/some.otq::some_query', otp.query.config(output_columns=[('X': int)]))  # doctest: +SKIP
            """

            if output_columns is not None:
                if isinstance(output_columns, list):
                    self._validate_columns(output_columns)
                elif isinstance(output_columns, dict):
                    for pin, columns in output_columns.items():
                        if not isinstance(pin, str):
                            raise TypeError(f"Name of pin '{type(pin)}' is of non-str type '%s'")
                        else:
                            self._validate_columns(columns)

                elif not isinstance(output_columns, str):
                    raise TypeError(f'"output_columns" does not support value of the "{type(output_columns)}" type')

                if isinstance(output_columns, str):
                    if output_columns not in self._special_values:
                        raise ValueError(f'Config does not support "{output_columns}" value')

            self.output_columns = output_columns

        def _validate_list_item(self, item):
            if isinstance(item, str):
                if item not in self._special_values:
                    raise ValueError(f"Value {item} is not supported.")

            else:
                if not isinstance(item, (tuple, list)) or (len(item) != 2) or not isinstance(item[0], str):
                    raise TypeError("Value %s is not a name-type tuple.")

        def _validate_columns(self, columns):
            if isinstance(columns, str):
                if columns not in self._special_values:
                    raise ValueError(f"A pin has invalid output columns definition: '{columns}'")

            elif isinstance(columns, list):
                if columns.count("input") > 1:
                    raise ValueError(f"More than one 'input' value in {columns}")

                for item in columns:
                    self._validate_list_item(item)

            else:
                raise TypeError(f"A pin's columns definition is of unsupported type '{type(columns)}'")

        def _get_output_columns_for_pin(self, out_pin_name):
            if isinstance(self.output_columns, dict):
                if out_pin_name not in self.output_columns:
                    raise ValueError(f"Pin {out_pin_name} wasn't declared in the config")
                else:
                    return self.output_columns[out_pin_name]

            else:
                return self.output_columns

        def _apply(self, out_pin_name, src):
            """
            Applying specified logic on a certain object. Used internally in the functions.apply_query
            """
            columns_descriptor = self._get_output_columns_for_pin(out_pin_name)
            if columns_descriptor is None:
                # drop columns by default, because we don't know
                # how an external query changes data schema
                src.drop_columns()
            elif columns_descriptor != "input":
                if "input" not in columns_descriptor:
                    src.drop_columns()

                for item in columns_descriptor:
                    if item != "input":
                        name, dtype = item
                        src.schema[name] = dtype

    def __init__(self, path, *config, **params):

        # prepare parameters
        self._str_params = None
        self.params = params
        self.update_params()

        # prepare configs
        if len(config) > 1:
            raise ValueError(f"It is allowed to specify only one config object, but passed {len(config)}")
        elif len(config) == 1:
            if not isinstance(config[0], self.config):
                raise TypeError(
                    f'It is expected to see config of the "query.config" type, but got "{type(config[0])}"'
                )
            self.config = config[0]
        else:
            self.config = self.config()

        # prepare path and query name

        path = str(path)

        remote = path.startswith('remote://')
        if remote:
            self.path = path
            _, path = path.split('::', maxsplit=1)
        else:
            if otp.__webapi__:
                # remote:// used only for remote queries, not for locals, like we do without webapi
                self.path = path
            else:
                self.path = f"remote://{configuration.config.get('default_db', 'LOCAL')}::" + path

        self.query_path, self.query_name = utils.query_to_path_and_name(path)

        # if query_path does not exist, then we try
        # to resolve it with OTQ_FILE_PATH assuming that
        # a relative path is passed
        if not os.path.exists(self.query_path):
            otq_path = utils.get_config_param(os.environ["ONE_TICK_CONFIG"], "OTQ_FILE_PATH", "")
            try:
                self.query_path = utils.abspath_to_query_by_otq_path(otq_path, self.query_path)
            except FileNotFoundError:
                if remote:
                    # TODO: we want to get self.graph_info from remote query somehow, probably will have to download it
                    self.graph_info = None
                    return
                raise

        if self.query_name is None:
            # it seems that query name was not passed, then try to find it
            queries = query_inspector.get_queries(self.query_path)
            if len(queries) > 1:
                raise ValueError(f"{self.query_path} has more than one query, "
                                 f"but you have not specified which one to use.")
            self.query_name = queries[0]

        self.graph_info = query_inspector.get_query_info(self.query_path, self.query_name)

    def __call__(self, *ticks, **pins):
        """
        Return object representing outputs of the query.
        This object can be used to get a specified output pin of the query as a new :py:class:`onetick.py.Source`.

        Examples
        --------
        >>> query = otp.query('/otqs/some.otq::some_query', PARAM1='val1')  # doctest: +SKIP
        >>> query()['OUT']  # doctest: +SKIP,+ELLIPSIS
        <onetick.py.core.source.Source at ...>
        """
        for key, value in pins.items():
            if not isinstance(value, Source):
                raise ValueError(f'Input "{key}" pin does not support "{type(value)}" type')

        if self.graph_info is not None and len(pins) == 0 and len(ticks) == 1:
            if len(self.graph_info.nested_inputs) != 1:
                raise ValueError(
                    f'It is expected the query "{self.query_path}" to have one input, but it'
                    f" has {len(self.graph_info.nested_inputs)}"
                )

            pins[self.graph_info.nested_inputs[0].NESTED_INPUT] = ticks[0]
        elif len(ticks) == 0:
            # it is the valid case, when query has no input pins
            pass
        else:
            raise ValueError("It is allowed to pass only one non-specified input")

        outputs = self._outputs()
        outputs.query = self
        outputs.in_sources = pins

        return outputs

    class _outputs:
        def __getitem__(self, key):
            output_pins = []

            if isinstance(key, tuple):
                output_pins = list(key)
            elif isinstance(key, str):
                output_pins = [key]
            elif key is None:
                # No output
                pass
            else:
                raise ValueError(f'Output pins can not be of "{type(key)}" type')

            return otp.apply_query(
                self.query, in_sources=self.in_sources, output_pins=output_pins, **self.query.params
            )

    def to_eval_string(self):
        """
        Converts query object to OneTick's `eval` string.
        """
        res = '"' + self.path + '"'
        if self.params:
            res += f', "{self._params_to_str(self.params, with_expr=True)}"'
        return "eval(" + res + ")"

    def update_params(self, **new_params):
        """
        Update dictionary of parameters of the query.
        """
        if new_params:
            self.params.update(new_params)

    @property
    def str_params(self):
        """
        Query parameters converted to OneTick string representation.
        """
        if self._str_params is None:
            self._str_params = self._params_to_str(self.params)
        return self._str_params

    @staticmethod
    def _params_to_str(params, *, with_expr=False):
        """ converts param to str

        Parameters
        ----------
        params: dict
            Parameters as dict(name=value)
        with_expr:
            If true return all expression in expr() function

        Returns
        -------
        result: str
            string representation of parameters ready for query evaluation
        """

        def to_str(v):
            if isinstance(v, list):
                return "\\,".join(map(to_str, v))
            else:
                if with_expr:
                    is_dt = ott.is_time_type(v)
                    if is_dt:
                        v = ott.value2str(v)
                    result = query._escape_quotes_in_eval(v)
                    if isinstance(v, _Operation) and getattr(v, "name", None) != "_SYMBOL_NAME" or is_dt:
                        result = f"expr({result})"
                else:
                    result = query._escape_characters_in_query_param(str(v))
                return result

        return ",".join(key + "=" + to_str(value) for key, value in params.items())

    @staticmethod
    def _escape_quotes_in_eval(v):
        return str(v).translate(str.maketrans({"'": r"\'", '"': r'\"'}))

    @staticmethod
    def _escape_characters_in_query_param(result):
        # 0 - no need to add backslash, 1 - need to add
        char_map = [0] * len(result)

        # put 1 between two quotes symbols
        open_char = None
        last_inx = 0
        for inx, c in enumerate(result):
            if open_char == c:
                open_char = None
                continue

            if not open_char and c in ("'", '"'):
                open_char = c
                last_inx = inx + 1
                continue

            if open_char:
                char_map[inx] = 1

        # clean open tail if necessary
        if open_char:
            char_map[last_inx:] = [0] * (len(result) - last_inx)

        # apply mapping
        res = []
        last_esc = False  # do not add esc if the previous one is already esc
        n_brackets_in_expr_block = 0  # do not escape in expr(...)
        for inx, c in enumerate(result):
            if c == "(":
                if n_brackets_in_expr_block:
                    n_brackets_in_expr_block += 1
                elif result[inx - 4:inx] == "expr":
                    n_brackets_in_expr_block = 1
            if c == ")" and n_brackets_in_expr_block:
                n_brackets_in_expr_block -= 1

            if c in _QUERY_PARAM_SPECIAL_CHARACTERS and char_map[inx] == 0:
                if not last_esc and not n_brackets_in_expr_block:
                    c = "\\" + c

            last_esc = c == "\\"

            res.append(c)

        return "".join(res)
