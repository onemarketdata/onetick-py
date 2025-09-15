import os
import re
import warnings
from typing import TYPE_CHECKING, Callable, Optional, Union

from onetick import py as otp
from onetick.py import types as ott
from onetick.py.core import db_constants
from onetick.py.core.lambda_object import _EmulateObject, apply_lambda, apply_script
from onetick.py.core.query_inspector import add_pins, get_query_info
from onetick.py.otq import otq

if TYPE_CHECKING:
    from onetick.py.core.source import Source


def apply_query(self: 'Source', query, in_pin="IN", out_pin="OUT", **params):
    """
    Apply data source to query

    .. deprecated:: 1.3.77

    See Also
    --------
    Source.apply
    """
    warnings.warn(
        'The "apply_query" method is deprecated. Please, use the .apply method instead or '
        "call a reference queries directly.",
        FutureWarning,
        stacklevel=2,
    )
    res = otp.apply_query(query, {in_pin: self}, [out_pin], **params)
    res.node().out_pin(out_pin)

    return res


def apply(self: 'Source', obj) -> Union['otp.Column', 'Source']:
    """
    Apply object to data source.

    Parameters
    ----------
    obj: onetick.py.query, Callable, type, onetick.query.GraphQuery

        - `onetick.py.query` allows to apply external nested query

        - python `Callable` allows to translate python code to similar OneTick's CASE expression.
            There are some limitations to which python operators can be used in this callable.
            See :ref:`Python callables parsing guide <python callable parser>` article for details.
            In :ref:`Remote OTP with Ray<ray-remote>` any `Callable` must be decorated with `@otp.remote` decorator,
            see :ref:`Ray usage examples<apply-remote-context>` for details.

        - `type` allows to apply default type conversion

        - `onetick.query.GraphQuery` allows to apply a build onetick.query.Graph

    Returns
    -------
    Column, Source

    Examples
    --------

    Apply external query to a tick flow. In this case it assumes that query has
    only one input and one output. Check the :class:`query` examples if you
    want to use a query with multiple inputs or outputs.

    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> external_query = otp.query('update.otq')
    >>> data = data.apply(external_query)
    >>> otp.run(data)
                         Time  X
    0 2003-12-01 00:00:00.000  2
    1 2003-12-01 00:00:00.001  4
    2 2003-12-01 00:00:00.002  6

    Apply a predicate to a column / operation.
    In this case value passed to a predicate is column values.
    Result is a column.

    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> data['Y'] = data['X'].apply(lambda x: x * 2)
    >>> otp.run(data)
                         Time  X  Y
    0 2003-12-01 00:00:00.000  1  2
    1 2003-12-01 00:00:00.001  2  4
    2 2003-12-01 00:00:00.002  3  6

    Another example of applying more sophisticated operation

    >>> data = otp.Ticks(X=[1, 2, 3])
    >>> data['Y'] = data['X'].apply(lambda x: 1 if x > 2 else 0)
    >>> otp.run(data)
                         Time  X  Y
    0 2003-12-01 00:00:00.000  1  0
    1 2003-12-01 00:00:00.001  2  0
    2 2003-12-01 00:00:00.002  3  1

    Example of applying a predicate to a Source. In this case value passed
    to a predicate is a whole tick. Result is a column.

    >>> data = otp.Ticks(X=[1, 2, 3], Y=[.5, -0.4, .2])
    >>> data['Z'] = data.apply(lambda tick: 1 if abs(tick['X'] * tick['Y']) > 0.5 else 0)
    >>> otp.run(data)
                         Time  X     Y  Z
    0 2003-12-01 00:00:00.000  1   0.5  0
    1 2003-12-01 00:00:00.001  2  -0.4  1
    2 2003-12-01 00:00:00.002  3   0.2  1

    See Also
    --------
    :py:class:`onetick.py.query`
    :py:meth:`onetick.py.Source.script`
    :ref:`Python callables parsing guide <python callable parser>`

    """
    if isinstance(obj, otp.query):
        graph = obj.graph_info
        if graph is None:
            return obj(IN=self)['OUT']

        if len(graph.nested_inputs) != 1:
            raise ValueError(
                f'It is expected the query "{obj.query_name}" to have one input, but it '
                f"has {len(graph.nested_inputs)}"
            )
        if len(graph.nested_outputs) > 1:
            raise ValueError(
                f'It is expected the query "{obj.query_name}" to have one or no output, but it '
                f"has {len(graph.nested_outputs)}"
            )

        in_pin = graph.nested_inputs[0].NESTED_INPUT

        if len(graph.nested_outputs) == 0:
            out_pin = None  # means no output
        else:
            out_pin = graph.nested_outputs[0].NESTED_OUTPUT

        return obj(**{in_pin: self})[out_pin]
    elif isinstance(obj, otq.GraphQuery):
        base_dir = None
        if os.getenv('OTP_WEBAPI_TEST_MODE'):
            from onetick.py.otq import _tmp_otq_path

            base_dir = _tmp_otq_path()
        tmp_file = otp.utils.TmpFile(suffix=".otq", base_dir=base_dir)

        obj.save_to_file(
            tmp_file.path,
            "graph_query_to_apply",
            # start and end times don't matter for this query, use some constants
            start=db_constants.DEFAULT_START_DATE,
            end=db_constants.DEFAULT_END_DATE,
        )

        query_info = get_query_info(tmp_file.path)

        if len(query_info.sources) != 1:
            raise ValueError(f"It is expected the query to have one input, but it has {len(query_info.sources)}")
        if len(query_info.sinks) != 1:
            raise ValueError(f"It is expected the query to have one output, but it has {len(query_info.sinks)}")

        add_pins(
            tmp_file.path,
            "graph_query_to_apply",
            [(query_info.sources[0], 1, "IN"), (query_info.sinks[0], 0, "OUT")],
        )

        return otp.query(tmp_file.path + "::graph_query_to_apply")(IN=self)["OUT"]

    return apply_lambda(obj, _EmulateObject(self))


def _extract_columns_from_script(script_text):
    result = {}
    supported_types = {
        'byte',
        'short',
        'uint',
        'long',
        'ulong',
        'int',
        'float',
        'double',
        'string',
        'time32',
        'nsectime',
        'msectime',
        'varstring',
        'decimal',
    }
    types = (
        r"(?P<type>varstring|byte|short|uint|int|ulong|long|"
        r"float|double|decimal|string|time32|msectime|nsectime|matrix)"
    )
    length = r"(\[(?P<length>\d+)\])?"
    name = r"\s+(?P<name>\w+)\s*=\s*"
    pattern = re.compile(types + length + name)
    for p in re.finditer(pattern, script_text):
        groupdict = p.groupdict()
        type_ = ott.str2type(groupdict["type"]) if groupdict["type"] in supported_types else None
        if type_:
            length = groupdict["length"]
            if length:
                length = int(length)
                if type_ is str and length != ott.string.DEFAULT_LENGTH:
                    type_ = ott.string[length]
            result[groupdict["name"]] = type_
        else:
            warnings.warn(
                f"{groupdict['type']} isn't supported for now, so field {groupdict['name']} won't "
                f"be added to schema."
            )
    return result


def script(self: 'Source', func: Union[Callable[['Source'], Optional[bool]], str], inplace=False) -> 'Source':
    # TODO: need to narrow Source object to get rid of undesired methods like aggregations
    """
    Implements a script for every tick.

    Allows to pass a ``func`` that will be applied per every tick.
    A ``func`` can be python callable in this case it will be translated to per tick script.
    In order to use it in Remote OTP with Ray, the function should be decorated with ``@otp.remote``,
    see :ref:`Ray usage examples<apply-remote-context>` for details.

    See :ref:`Per Tick Script Guide <python callable parser>` for more detailed description
    of python to OneTick code translation and per-tick script features.

    The script written in per tick script language can be passed itself as a string or path to a file with the code.
    onetick-py doesn't validate the script, but configure the schema accordingly.

    Parameters
    ----------
    func: callable, str or path
        - a callable that takes only one parameter - actual tick that behaves like a `Source` instance

        - or the script on per-tick script language

        - or a path to file with onetick script

    Returns
    -------
    :class:`Source`

    See also
    --------
    **PER_TICK_SCRIPT** OneTick event processor

    Examples
    --------
    >>> t = otp.Ticks({'X': [1, 2, 3], 'Y': [4, 5, 6]})
    >>> def fun(tick):
    ...     tick['Z'] = 0
    ...     if tick['X'] + tick['Y'] == 5:
    ...         tick['Z'] = 1
    ...     elif tick['X'] + tick['Y'] * 2 == 15:
    ...         tick['Z'] = 2
    >>> t = t.script(fun)
    >>> otp.run(t)
                         Time  X  Y  Z
    0 2003-12-01 00:00:00.000  1  4  1
    1 2003-12-01 00:00:00.001  2  5  0
    2 2003-12-01 00:00:00.002  3  6  2

    See also
    --------
    :py:meth:`onetick.py.Source.apply`
    :ref:`Per-Tick Script Guide <python callable parser>`
    """
    res = self if inplace else self.copy()
    changed_tick_lists = {}
    if callable(func):
        _new_columns, _script = apply_script(func, _EmulateObject(self))
        changed_tick_lists = _EmulateObject.get_changed_tick_lists()
    elif isinstance(func, str):
        if os.path.isfile(func):
            # path to the file with script
            with open(func) as file:
                _script = file.read()
        else:
            _script = func
        _new_columns = _extract_columns_from_script(_script)
    else:
        raise ValueError(
            "Wrong argument was specify, please use callable or string with either script on per tick "
            "language or path to it"
        )
    res.sink(otq.PerTickScript(script=_script))
    res.schema.update(**_new_columns)
    for tick_list_name, tick_list_schema in changed_tick_lists.items():
        res.state_vars[tick_list_name]._schema = tick_list_schema
    return res
