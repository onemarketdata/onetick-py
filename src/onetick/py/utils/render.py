import os
import re
import html
import textwrap
import graphviz as gv
from collections import defaultdict, deque
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set, Tuple, Union

from onetick.py.utils import TmpFile


EPS_WITH_QUERIES = {
    "eval": (0, "expression"),
    "eval_expression": (0, "expression"),
    "join_with_query": (0, "otq_query"),
    "nested_otq": (0, "otq_name"),
    "join_with_collection_summary": (1, "otq_query"),
    "modify_state_var_from_query": (1, "otq_query"),
    "create_cache": (1, "otq_file_path"),
    "read_cache": (4, "create_cache_query"),
    "generic_aggregation": (0, "query_name"),
}

IF_ELSE_EPS = {
    "CHARACTER_PRESENT", "CORRECT_TICK_FILTER", "PRIMARY_EXCH", "REGEX_MATCHES", "SKIP_BAD_TICK", "TIME_FILTER",
    "TRD_VS_MID", "TRD_VS_QUOTE", "UPTICK", "VALUE_COMPARE", "VALUE_PRESENT", "VOLUME_LIMIT", "WHERE_CLAUSE",
}


def _parse_table_fields(line: str) -> list:
    result = line.strip().split(',')
    for idx in range(0, len(result) - 1):
        result[idx] = result[idx] + ','

    return result


def _light_function_splitter(line: str, sep=',') -> list:
    lines = []
    current_line: list = []
    parentheses_stack = 0
    quotes_stack = 0
    lead_quote_type = None

    for ch in line:
        if ch == sep and not parentheses_stack and not quotes_stack:
            lines.append(''.join(current_line) + sep)
            current_line = []
            continue

        current_line.append(ch)

        if ch == '(' and not quotes_stack:
            parentheses_stack += 1
            continue

        if ch == ')' and not quotes_stack:
            parentheses_stack -= 1
            if parentheses_stack < 0:
                break

        if ch in ["\"", "'"]:
            if lead_quote_type is None:
                lead_quote_type = ch
                quotes_stack = 1
            elif ch == lead_quote_type:
                lead_quote_type = None
                quotes_stack = 0

    if parentheses_stack != 0:
        raise ValueError(f'Incorrect parentheses count in function: `{line}`')

    if quotes_stack != 0:
        raise ValueError(f'Incorrect quotes count in function: `{line}`')

    lines.append(''.join(current_line))

    return lines


EP_TO_MULTILINE_ATTRS: dict = {
    "ADD_FIELDS": {
        "set": _light_function_splitter,
    },
    "UPDATE_FIELDS": {
        "set": _light_function_splitter,
    },
    "TABLE": {
        "fields": _parse_table_fields,
    },
    "PASSTHROUGH": {
        "fields": _parse_table_fields,
    },
    "COMPUTE": {
        "compute": _light_function_splitter,
    },
    "DECLARE_STATE_VARIABLES": {
        "variables": _light_function_splitter,
    },
    "RENAME_FIELDS": {
        "rename_fields": _parse_table_fields,
    }
}


@dataclass
class NestedQuery:
    name: str
    raw_string: str
    query: Optional[str] = field(default=None)
    expression: Optional[str] = field(default=None)
    file_path: Optional[str] = field(default=None)
    args: list = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)
    is_local: bool = field(default=False)

    def to_string(self):
        if self.is_local:
            if self.file_path:
                raise ValueError("Nested query from file couldn't be local")

            if self.expression:
                return self.expression
            else:
                return self.query
        else:
            return "::".join(i for i in [self.file_path, self.query] if i)


@dataclass
class Config:
    height: int = field(default=0)
    width: int = field(default=0)
    render_debug_info: bool = field(default=False)
    constraint_edges: str = field(default="true")


@dataclass
class EP:
    name: str
    raw_string: str
    args: list = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)


@dataclass
class IfElseEP(EP):
    if_nodes: Set[str] = field(default_factory=set)
    else_nodes: Set[str] = field(default_factory=set)


@dataclass
class Node:
    ep: Union[EP, NestedQuery, None]
    id: str
    query: str
    tick_type: Optional[str] = field(default=None)
    labels: Dict[str, str] = field(default_factory=dict)
    config: dict = field(default_factory=dict)
    params: dict = field(default_factory=dict)
    sinks: List[str] = field(default_factory=list)
    symbols: list = field(default_factory=list)


@dataclass
class Query:
    name: str
    graph: str
    nodes: Dict[str, Node] = field(default_factory=dict)
    roots: list = field(default_factory=list)
    leaves: list = field(default_factory=list)
    symbols: list = field(default_factory=list)
    config: dict = field(default_factory=dict)
    params: dict = field(default_factory=dict)
    depends: Set[Tuple[Optional[str], Optional[str]]] = field(default_factory=set)

    def get_id(self, prefix: Optional[str] = "cluster"):
        if prefix:
            prefix = f"{prefix}__"
        else:
            prefix = ""

        graph = self.graph.replace(":", "_")

        return f"{prefix}{graph}__{self.name}"


@dataclass
class Graph:
    file_path: str
    config: dict = field(default_factory=dict)
    queries: Dict[str, Query] = field(default_factory=dict)

    def has_query(self, query):
        return query in self.queries


class GraphStorage(dict):
    def get_query(self, otq_file: Optional[str], query_name: Optional[str]) -> Optional[Query]:
        if not query_name or not otq_file:
            return None

        if otq_file not in self or query_name not in self[otq_file].queries:
            return None

        return self[otq_file].queries[query_name]

    def get_query_unique_id(self, query: Union[str, Query, NestedQuery], graph: Optional[str] = None) -> Optional[str]:
        query_obj = None
        if isinstance(query, Query):
            query_obj = query
        elif isinstance(query, NestedQuery):
            if not query.is_local or query.expression:
                raise RuntimeError("Couldn't get id for non-local or expression-based NestedQuery")

            query_obj = self.get_query(query.file_path, query.query)
        elif isinstance(query, str):
            if not graph:
                raise ValueError("`graph` with path to otq file is required for `str` query")

            query_obj = self.get_query(graph, query)
        else:
            raise RuntimeError(f"Unsupported query type: {type(query)}")

        if not query_obj:
            return None
        else:
            return query_obj.get_id()


class GVTable:
    def __init__(self, border=0, cellborder=1, cellspacing=0, attrs: Optional[dict] = None, auto_colspan=True):
        """
        Generate HTML tables for Graphviz

        Attributes for each row or cell can be set by passing `attrs` attribute to `GVTable.row` or `GVTable.cell`.
        Accoringly `attrs` params have reverse prioriry: higher for cells, less for rows.

        Parameters
        ----------
        border: int
            Value of `BORDER` attribute of table HTML element
        cellborder: int
            Value of `CELLBORDER` attribute of table HTML element
        cellspacing: int
            Value of `CELLSPACING` attribute of table HTML element
        attrs: dict
            HTML attributes to apply to table element.
        auto_colspan: bool
            If set True, then last cell in each row automaticly fills table width,
            if `colspan` attribute not set for this cell.

        Examples
        --------

        Simple two rows table:

        >>> table = otp.utils.render.GVTable()
        >>> table.row(["cell_1_1", "cell_1_2"])  # doctest: +SKIP
        >>> table.row(["cell_2_1", "cell_2_2"])  # doctest: +SKIP
        >>> table_html = str(table)  # doctest: +SKIP
        """
        self.rows: List[Tuple[List[Tuple[Union[List[str], str], dict]], dict]] = []
        self.attrs = {
            "border": border,
            "cellborder": cellborder,
            "cellspacing": cellspacing,
        }
        if attrs:
            self.attrs.update(attrs)

        self.auto_colspan = auto_colspan
        self.max_cols = 0

    def cell(self, data: list, attrs: Optional[dict] = None):
        """
        Append cell in the last row
        """
        if len(self) == 0:
            raise RuntimeError("No rows in table")

        row, row_attrs = self.rows[-1]

        for cell_data in data:
            cell_attrs = row_attrs.copy()

            if attrs:
                cell_attrs.update(attrs)

            if isinstance(cell_data, tuple):
                cell, _cell_attrs = cell_data
                if _cell_attrs:
                    cell_attrs.update(_cell_attrs)
            else:
                cell = cell_data

            row.append((cell, cell_attrs))

        self.max_cols = max(self.max_cols, len(row))

        return self

    def row(self, data: list, attrs: Optional[dict] = None):
        self.rows.append(([], attrs if attrs else {}))
        self.cell(data)

        return self

    def __len__(self):
        return len(self.rows)

    def __str__(self):
        tags = []

        attrs = " ".join([f"{k.upper()}=\"{v}\"" for k, v in self.attrs.items()])
        tags.append(f"<TABLE {attrs}>")

        for row, row_attrs in self.rows:
            col_count = len(row)
            tags.append("<TR>")

            for i in range(col_count):
                cell, cell_attrs = row[i]

                if (
                    self.auto_colspan and col_count - 1 == i and
                    len(row) < self.max_cols and "colspan" not in cell_attrs
                ):
                    cell_attrs["colspan"] = self.max_cols - col_count + 1

                attrs = " ".join([f"{k.upper()}=\"{v}\"" for k, v in cell_attrs.items()])
                if not isinstance(cell, list):
                    cell = [cell]

                cell_str = "<BR/>".join(cell)
                tags.append(f"<TD {attrs}>{cell_str}</TD>")

            tags.append("</TR>")

        tags.append("</TABLE>")

        return "<" + "".join(tags) + ">"


def _posix_path(path: str):
    return path.replace(os.sep, "/")


def _get_node_unique_id(node: Union[Node, str], query: Query):
    if isinstance(node, str):
        node = query.nodes[node]

    return f"{query.get_id(prefix=None)}__{node.id}"


def _save_param(storage, key, value):
    if key in storage:
        if not isinstance(storage[key], list):
            storage[key] = [storage[key]]

        storage[key].append(value)
    else:
        storage[key] = value


def _is_local_query(f_path: Optional[str]) -> bool:
    return f_path in ["THIS", "___ME___"]


def _parse_function_params(func_params: str) -> Tuple[list, dict]:
    def save_param(_key, _value, _args, _kwargs):
        if _key:
            _kwargs[_key.lower()] = (_key, _value)
        else:
            if _kwargs:
                raise RuntimeError("Positional argument could not be after keyword argument")

            _args.append(_value)

    args: list = []
    kwargs: dict = {}
    key = ""
    str_buffer: list = []
    in_quotes = None
    escape_next = False

    if not func_params:
        return args, kwargs

    for ch in func_params:
        if escape_next:
            escape_next = False
            str_buffer.append(ch)
        elif ch == "\\":
            escape_next = True
        elif in_quotes:
            if ch == in_quotes:
                in_quotes = None
            else:
                str_buffer.append(ch)
        else:
            if ch in "\"\'":
                in_quotes = ch
            elif ch.isspace():
                continue
            elif ch == "=":
                key = "".join(str_buffer)
                str_buffer.clear()
            elif ch == ",":
                save_param(key, "".join(str_buffer), args, kwargs)
                str_buffer.clear()
                key = ""
            else:
                str_buffer.append(ch)

    if in_quotes:
        raise ValueError("`func_params` unclosed quote")

    if str_buffer:
        save_param(key, "".join(str_buffer), args, kwargs)
        str_buffer.clear()

    return args, kwargs


def _parse_function(expression: str, pattern: Optional[str] = None) -> Tuple[Optional[str], list, dict]:
    # EP_NAME(PARAM_NAME=PARAM_VALUE,...)
    # [a-zA-Z_:] is EP_NAME, can contain letters, underscore and colon
    # [\s\S] is any symbol including newline (because . doesn't include newline by default)
    if not pattern:
        pattern = r"^([a-zA-Z_:]*)\s*\(([\s\S]*)\)\s*$"

    m = re.search(pattern, expression)

    if not m:
        return None, [], {}

    ep, params_str = m.groups()
    args, kwargs = _parse_function_params(params_str)

    return ep, args, kwargs


def _get_ep_from_str(ep_string: str) -> Tuple[str, list, dict]:
    ep, args, kwargs = _parse_function(ep_string)

    if not ep:
        ep = ep_string

    return ep, args, kwargs


def _parse_query_path(query_path: str) -> Union[Tuple[str, Optional[str]], List[str]]:
    query_path_splitted = query_path.rsplit("::", maxsplit=1)

    if len(query_path_splitted) == 1:
        return _posix_path(query_path_splitted[0]), None
    else:
        file_path, query = query_path_splitted

        return _posix_path(file_path), query


def _parse_ep(ep_string: str, parse_eval_from_params: bool = False) -> Union[EP, NestedQuery]:
    if ep_string.startswith("NESTED_OTQ"):
        query_path = " ".join(ep_string.split(" ")[1:])

        file_path, query = _parse_query_path(query_path)
        is_local = _is_local_query(file_path)

        return NestedQuery(
            name="NESTED_OTQ", raw_string=ep_string, query=query, file_path=None if is_local else file_path,
            is_local=is_local,
        )

    ep, args, kwargs = _get_ep_from_str(ep_string)

    if parse_eval_from_params:
        for param_name, param_value in kwargs.items():
            m = re.search(r"^(eval\([^)]+\)).*$", param_value[1], re.IGNORECASE)
            if not m:
                continue

            param_ep_str = m.group(1)

            try:
                param_ep = _parse_ep(param_ep_str, parse_eval_from_params=False)
                if isinstance(param_ep, NestedQuery):
                    kwargs[param_name] = (param_value[0], param_ep)
            except Exception:
                pass

    if ep.lower() in EPS_WITH_QUERIES:
        ep_description = EPS_WITH_QUERIES[ep.lower()]
        args_idx, kwargs_key = ep_description

        is_query_found = True

        if kwargs_key in kwargs:
            query_path = kwargs[kwargs_key][1]
        elif 0 <= args_idx < len(args):
            query_path = args[args_idx]
        else:
            # don't do anything, just process as EP
            is_query_found = False

        if is_query_found:
            if query_path[0] in ["\"", "\'"] and query_path[0] == query_path[-1]:
                query_path = query_path[1:-1]

            file_path, query = _parse_query_path(query_path)

            if file_path and query:
                is_local = _is_local_query(file_path)
                return NestedQuery(
                    name=ep, raw_string=ep_string, query=query, file_path=None if is_local else file_path,
                    args=args, kwargs=kwargs, is_local=is_local,
                )
            else:
                return NestedQuery(
                    name=ep, raw_string=ep_string, expression=file_path, args=args, kwargs=kwargs, is_local=True,
                )

    if ep in IF_ELSE_EPS:
        return IfElseEP(name=ep, raw_string=ep_string, args=args, kwargs=kwargs)

    return EP(name=ep, raw_string=ep_string, args=args, kwargs=kwargs)


def _parse_security(value: str) -> Tuple[Union[str, EP, NestedQuery], str, bool]:
    is_security_active = True
    split_value = value.split()

    try:
        int(split_value[-1])
    except ValueError:
        # assume that third value is "No"
        is_security_active = False
        split_value.pop()

    security = " ".join(split_value[:-1])

    try:
        security_ep = _parse_ep(security)
    except ValueError:
        security_ep = None

    return (security_ep if security_ep else security), split_value[-1], is_security_active


def _move_between_dicts(source, output, key, func):
    if not isinstance(source[key], list):
        source[key] = [source[key]]

    output.update([func(k.split()) for k in source[key]])
    del source[key]


def _move_parameters(_from, _to):
    if "PARAMETER" in _from:
        _move_between_dicts(
            _from,
            _to,
            "PARAMETER",
            lambda x: (x[0], " ".join(x[1:]))
        )

    if "PARAMETER_MANDATORY" in _from:
        _move_between_dicts(
            _from,
            _to,
            "PARAMETER_MANDATORY",
            lambda x: (x[0], None)
        )


def _build_query_tree(query: Query):
    roots = {*query.nodes.keys()}
    leaves = {*query.nodes.keys()}

    for node_id, node in query.nodes.items():
        _move_parameters(node.config, node.params)

        # save labels
        if "NESTED_INPUT" in node.config:
            node.labels["IN"] = node.config["NESTED_INPUT"]

        if "NESTED_OUTPUT" in node.config:
            node.labels["OUT"] = node.config["NESTED_OUTPUT"]

        if "SOURCE_DESCRIPTION" in node.config:
            descriptions = node.config["SOURCE_DESCRIPTION"]
            if isinstance(descriptions, str):
                descriptions = [descriptions]
            for description in descriptions:
                description = description.strip().split(" ")
                if len(description) > 1:
                    desc_node = description[0].split(".")[0]
                    labels = description[1].split(".")

                    if labels and desc_node in query.nodes:
                        if labels[0]:
                            query.nodes[desc_node].labels["IN"] = labels[0]

                        if labels[1]:
                            query.nodes[desc_node].labels["OUT"] = labels[1]

        if "SINK_DESCRIPTION" in node.config:
            description_path = node.config["SINK_DESCRIPTION"].strip().split(".")
            if len(description_path) > 1:
                desc_node = description_path[0]
                label = description_path[-1]

                if label and desc_node in query.nodes:
                    query.nodes[desc_node].labels["OUT"] = label

        nodes = []

        if "SINK" in node.config:
            sink_nodes = [(sink, True) for sink in node.config["SINK"].strip().split()]
            if sink_nodes:
                leaves.discard(node_id)

            nodes += sink_nodes
            del node.config["SINK"]

        if "SOURCE" in node.config:
            source_nodes = [(sink, False) for sink in node.config["SOURCE"].strip().split()]
            if source_nodes:
                roots.discard(node_id)

            nodes += source_nodes
            del node.config["SOURCE"]

        for source_node, is_sink_node in nodes:
            # just ignore other nodes in path
            source_node_path = source_node.split(".")
            source_node_id = source_node_path[0]

            if not is_sink_node and source_node_id in leaves:
                leaves.discard(source_node_id)

            if is_sink_node and source_node_id in roots:
                roots.discard(source_node_id)

            if source_node_id not in query.nodes:
                raise RuntimeError(f"Malformed otq file passed: node {source_node_id} not found in {query.name}")

            if is_sink_node:
                query.nodes[node_id].sinks.append(source_node_id)

                if isinstance(node.ep, IfElseEP):
                    if "IF" in source_node_path[1:]:
                        node.ep.if_nodes.add(source_node_id)

                    if "ELSE" in source_node_path[1:]:
                        node.ep.else_nodes.add(source_node_id)
            else:
                source_node = query.nodes[source_node_id]
                source_node.sinks.append(node_id)

                if isinstance(source_node.ep, IfElseEP):
                    if "IF" in source_node_path[1:]:
                        source_node.ep.if_nodes.add(node_id)

                    if "ELSE" in source_node_path[1:]:
                        source_node.ep.else_nodes.add(node_id)

    query.roots = list(roots)
    query.leaves = list(leaves)


def _save_dependency(obj, query: Query):
    if isinstance(obj, (EP, NestedQuery)):
        for kwarg_param in obj.kwargs.values():
            if isinstance(kwarg_param[1], NestedQuery):
                _save_dependency(kwarg_param[1], query)

        if isinstance(obj, NestedQuery) and not obj.expression:
            query.depends.add((obj.file_path, obj.query))


def _finalize_query(query: Query, graph: Graph):
    if not query:
        return

    if query.name == "_meta":
        graph.config = {k.upper(): v for k, v in query.config.items()}
        return

    if "SECURITY" in query.config:
        if not isinstance(query.config["SECURITY"], list):
            query.config["SECURITY"] = [query.config["SECURITY"]]

        for security in query.config["SECURITY"]:
            parsed_security = _parse_security(security)
            query.symbols.append(parsed_security)
            _save_dependency(parsed_security[0], query)

        del query.config["SECURITY"]

    _move_parameters(query.config, query.params)
    _build_query_tree(query)

    graph.queries[query.name] = query


def read_otq(path: str, parse_eval_from_params: bool = False) -> Optional[Graph]:
    if path.startswith("remote://") or not os.path.exists(path):
        return None

    graph = Graph(path)
    current_query = None

    with open(path, "r") as input_otq:
        tmp_line = ""
        for line in input_otq:
            line = line.rstrip()

            if line.endswith("\\"):
                tmp_line += f"{line[:-1]}\n"
                continue
            else:
                tmp_line += line

            line = tmp_line.strip()
            tmp_line = ""

            if not line:
                continue

            # found new query
            m = re.search(r"^\[(.*)\]$", line)
            if m:
                if current_query and current_query.config['TYPE'] == 'GRAPH':
                    _finalize_query(current_query, graph)

                query_name = m.groups()[0]
                current_query = Query(name=query_name, graph=path)
                continue

            if not current_query:
                continue

            if line.startswith("NODE"):
                prefix = r"NODE\D*?(\d+)"
            elif line.startswith("ROOT"):
                prefix = r"ROOT\D*?(\d*)"
            else:
                # other query param
                line_expr = line.split("=")
                param, value = line_expr[0], "=".join(line_expr[1:])
                param = param.strip()
                value = value.strip()

                _save_param(current_query.config, param, value)
                continue

            m = re.search(rf"^({prefix})(_([a-zA-Z_]*[0-9]*))?\s*=\s*([\s\S]*)$", line)
            if m:
                node_id, _, _, node_param, value = m.groups()

                if node_id not in current_query.nodes:
                    current_query.nodes[node_id] = Node(ep=None, id=node_id, query=current_query.name)

                if not node_param:
                    ep = _parse_ep(value, parse_eval_from_params=parse_eval_from_params)
                    _save_dependency(ep, current_query)
                    current_query.nodes[node_id].ep = ep
                elif node_param == "BIND_SECURITY":
                    security = _parse_security(value)
                    current_query.nodes[node_id].symbols.append(security)
                    _save_dependency(security[0], current_query)
                elif node_param == "TICK_TYPE":
                    current_query.nodes[node_id].tick_type = value
                else:
                    _save_param(current_query.nodes[node_id].config, node_param, value)

    if current_query:
        _finalize_query(current_query, graph)

    return graph


def _truncate_param_value(value, height, width):
    lines = [
        line if len(line) <= width or not width else line[:width] + "..."
        for line in value.splitlines()
    ]

    if height and len(lines) > height:
        lines = lines[:height] + ["..."]

    return "\n".join(lines)


def _split_long_value_to_lines(value, height, width, indent=0, escape=False) -> list:
    if len(value) <= width:
        return [value]

    result = []
    lines = value.splitlines()

    # textwrap.wrap replaces newline character to whitespace and brakes multiline strings
    # If replace_whitespace=False, it preserves newline, but not use it for result array line splitting
    for line in lines:
        result.extend(textwrap.wrap(line, width=width, replace_whitespace=False))

    if escape:
        result = [html.escape(s) for s in result]

    if indent:
        indent_str = "&nbsp;" * indent
        for i in range(1, len(result)):
            result[i] = indent_str + result[i]

    if height and len(result) > height:
        result = result[:height] + ['...']
    return result


def transform_param_value(ep: Any, param, value, height, width):
    if isinstance(ep, EP) and (
        ep.name == "PER_TICK_SCRIPT" and param.lower() == "script" or
        ep.name == "CSV_FILE_LISTING" and param.lower() == "file_contents"
    ):
        return _truncate_param_value(value, height, width)

    if not (isinstance(ep, EP) and EP_TO_MULTILINE_ATTRS.get(ep.name, {}).get(param.lower())):
        return "\n".join(_split_long_value_to_lines(value, height, width))

    return value


def build_symbols(
    symbols, gr_nested, gr_static, graphs: GraphStorage, graph_node, config: Config, reverse=False, graph_file=None,
):
    table = GVTable()

    for symbol_data in symbols:
        symbol, _, _ = symbol_data

        if isinstance(symbol, NestedQuery):
            if symbol.query:
                if symbol.is_local:
                    # reversed directions here brakes everything

                    if graph_file is None:
                        raise ValueError('`graph_file` parameter required for this case')

                    nested_cluster_id = graphs.get_query_unique_id(symbol.query, graph_file)

                    gr_nested.edge(
                        f"{nested_cluster_id}__footer",
                        f"{graph_node}:symbols",
                        ltail=f"{nested_cluster_id}",
                        style="dashed", dir="both", constraint=config.constraint_edges,
                    )
                    continue

                nested_cluster_id = graphs.get_query_unique_id(symbol.query, symbol.file_path)

                if nested_cluster_id:
                    gr_nested.edge(
                        f"{nested_cluster_id}__footer",
                        f"{graph_node}:symbols",
                        ltail=nested_cluster_id,
                        style="dashed", dir="both", constraint=config.constraint_edges,
                    )
                    continue

            query = symbol.to_string()
        elif isinstance(symbol, EP):
            query = symbol.raw_string
        else:
            query = symbol

        table.row([query])

    if len(table):
        gr_static.node(f"{graph_node}__symbols", str(table))
        gr_static.edge(
            f"{graph_node}__symbols" if not reverse else f"{graph_node}:symbols",
            f"{graph_node}:symbols" if not reverse else f"{graph_node}__symbols",
            style="dashed", constraint=config.constraint_edges,
        )


def _parse_special_attribute(param_name, param_lines, parser, height, width, cols=4):
    """
    Builds better param representation for selected parameters and EPs
    """
    def generate_row_string(_line: list) -> list:
        sep = "&nbsp;&nbsp;&nbsp;&nbsp;"

        # only in this case line could be longer than width
        if len(_line) == 1 and len(_line[0]) > width:
            _lines = _split_long_value_to_lines(_line[0], height, width, indent=4, escape=True)
        else:
            _lines = [sep.join(html.escape(s) for s in _line)]

        return ["&nbsp;" * 2 + s for s in _lines]

    param_value = ' '.join(param_lines)
    params = parser(param_value)

    params_table = [f"{param_name}:"]
    current_line = []
    current_width = 0

    for param in params:
        if width and current_line and current_width + len(param) >= width or len(current_line) == cols:
            params_table.extend(generate_row_string(current_line))
            current_line = []
            current_width = 0

        current_line.append(param)
        current_width += len(param)

    if current_line:
        params_table.extend(generate_row_string(current_line))

    return [(params_table, {"ALIGN": "LEFT", "BALIGN": "LEFT"})]


def build_node(graphs: GraphStorage, node: Node, config: Config):
    if node.ep is None:
        raise ValueError(f"EP of node {node.id} could not be None")

    table = GVTable()

    if "IN" in node.labels:
        table.row([
            ("<FONT POINT-SIZE=\"10\">" + html.escape(node.labels["IN"]) + "</FONT>", {"port": "in"}),
        ], attrs={
            "border": "1", "fixedsize": "TRUE", "colspan": "3",
        })

    table.row([(node.ep.name, {"port": "ep"})], attrs={"bgcolor": "gray95"})

    if node.tick_type:
        table.cell([node.tick_type])

    if config.render_debug_info:
        table.cell([node.id])

    if node.symbols:
        table.cell([("[■]", {"port": "symbols"})])

    if node.ep and (node.ep.args or node.ep.kwargs):
        params: List[Tuple[Optional[str], Union[str, NestedQuery]]] = \
            [(None, v) for v in node.ep.args] + list(node.ep.kwargs.values())

        param_args_lines = []
        param_kwargs_lines = []
        special_params = []

        for idx, data in enumerate(params):
            k, v = data
            attrs = {"port": k}
            if idx == len(params) - 1:
                attrs["sides"] = "LRB"
            else:
                attrs["sides"] = "LR"

            if isinstance(v, NestedQuery):
                param_value = v.raw_string
            else:
                param_value = v

            is_special_attribute = k and EP_TO_MULTILINE_ATTRS.get(node.ep.name, {}).get(k.lower())

            param_value = transform_param_value(node.ep, k, param_value, config.height, config.width)

            if not is_special_attribute:
                param_value = html.escape(param_value)

            param_value = param_value.replace("\t", "&nbsp;" * 4)
            param_lines = param_value.splitlines()

            # additional k check required by mypy
            if is_special_attribute and k:
                special_params.extend(
                    _parse_special_attribute(
                        k, param_lines, EP_TO_MULTILINE_ATTRS[node.ep.name][k.lower()], config.height, config.width,
                    )
                )
            else:
                if k:
                    if len(param_lines) == 1:
                        param_lines[0] = f"{html.escape(k)}={param_lines[0]}"
                    else:
                        param_lines = [f"{html.escape(k)}:"] + param_lines

                if len(param_lines) > 1:
                    # Add idents disable default horizontal central align
                    # if there are multiline parameter for EP.
                    # Align change affects all parameters for EP.
                    for i in range(len(param_lines)):
                        if i > 0:
                            param_lines[i] = "&nbsp;" * 2 + param_lines[i]

                    attrs.update({"ALIGN": "LEFT", "BALIGN": "LEFT"})

                if k:
                    param_kwargs_lines.append((param_lines, attrs))
                else:
                    param_args_lines.append((param_lines, attrs))

        for param_lines, attrs in param_args_lines + special_params + param_kwargs_lines:
            table.row([param_lines], attrs=attrs)

    if node.params:
        table.row([[
            f"{html.escape(k)}={html.escape(_truncate_param_value(v, config.height, config.width))}"
            for k, v in node.params.items()
        ]])

    if isinstance(node.ep, IfElseEP):
        table.row([
            ("<FONT POINT-SIZE=\"10\">[IF]</FONT>", {"port": "if"}), ("", {"border": "0"}),
            ("<FONT POINT-SIZE=\"10\">[ELSE]</FONT>", {"port": "else"})
        ], attrs={
            "border": "1", "fixedsize": "TRUE", "colspan": "1",
        })
    elif "OUT" in node.labels:
        table.row([
            ("<FONT POINT-SIZE=\"10\">" + html.escape(node.labels["OUT"]) + "</FONT>", {"port": "out"}),
        ], attrs={
            "border": "1", "fixedsize": "TRUE", "colspan": "3",
        })

    return str(table)


def _parse_time(time_str: str) -> str:
    if time_str:
        try:
            time_str = datetime.strptime(time_str, "%Y%m%d%H%M%S%f").strftime("%Y/%m/%d %H:%M:%S.%f"[:-3])
        except ValueError:
            pass
    else:
        time_str = "--"

    return time_str


def _build_time_expr(table: GVTable, name: str, time_expr: str):
    attrs = {}
    time_expr = html.escape(time_expr)

    if len(time_expr) > 60:
        time_expr = f"<FONT POINT-SIZE=\"10\">{time_expr}</FONT>"
        attrs["cellpadding"] = "4"

    table.row([name], attrs={"bgcolor": "gray95"}).row([time_expr], attrs=attrs)


def _get_nested_query(nested_query: NestedQuery, local_graph: Graph, graphs: GraphStorage) -> Optional[Query]:
    if nested_query.query:
        if nested_query.is_local:
            return local_graph.queries[nested_query.query]
        else:
            return graphs.get_query(nested_query.file_path, nested_query.query)

    return None


def _render_graph(
    gr_root, gr, graphs: GraphStorage, graph_name: str, queries: set, config: Config,
):
    graph = graphs[graph_name]

    if not queries or queries == {"*"}:
        queries = set(graph.queries.keys())

    for query_name in queries:
        if query_name not in graph.queries:
            continue

        query = graph.queries[query_name]
        query_id = query.get_id()

        with gr.subgraph(name=query_id, node_attr={"shape": "plaintext"}) as gr_sub:
            gr_sub.attr(label=query_name)

            start_time = _parse_time(query.config.get("START", graph.config.get("START")))
            end_time = _parse_time(query.config.get("END", graph.config.get("END")))

            start_expression = query.config.get("START_EXPRESSION", graph.config.get("START_EXPRESSION"))
            end_expression = query.config.get("END_EXPRESSION", graph.config.get("END_EXPRESSION"))

            tz_data = query.config.get("TZ", graph.config.get("TZ"))
            if not tz_data:
                tz_data = "--"

            table = GVTable().row([
                "START_TIME", "END_TIME", "TZ"
            ], attrs={"bgcolor": "gray95"}).row([
                start_time, end_time, tz_data,
            ])

            if start_expression:
                _build_time_expr(table, "START_EXPRESSION", start_expression)

            if end_expression:
                _build_time_expr(table, "END_EXPRESSION", end_expression)

            table.row([
                ("PARAMETERS", {"port": "params"}),
                ("SYMBOLS", {"port": "symbols"}),
            ], attrs={"bgcolor": "gray95"})

            footer_id = f"{query_id}__footer"
            gr_sub.node(footer_id, str(table), labelloc="c")

            # Put footer to the bottom and, most times, to the center
            for node_id in query.leaves:
                gr_sub.edge(_get_node_unique_id(node_id, query), footer_id, style="invis")

            if query.params:
                gr_sub.node(
                    f"{query_id}__params",
                    str(GVTable().row([[
                        f"{html.escape(k)}" + (f" = {html.escape(v)}" if v else "") for k, v in query.params.items()
                    ]]))
                )

                gr_sub.edge(
                    f"{footer_id}:params", f"{query_id}__params",
                    style="dashed", constraint=config.constraint_edges,
                )

            if query.symbols:
                build_symbols(
                    query.symbols, gr, gr_sub, graphs, f"{query_id}__footer", config,
                    reverse=True, graph_file=graph.file_path,
                )

            for node_id, node in query.nodes.items():
                node_unique_id = _get_node_unique_id(node, query)
                gr_sub.node(node_unique_id, build_node(graphs, node, config), group=query_name)

                for sink in node.sinks:
                    if "OUT" in node.labels:
                        output_port = ":out"
                    else:
                        output_port = ""

                    if isinstance(node.ep, IfElseEP):
                        if sink in node.ep.else_nodes:
                            output_port = ":else"
                        else:
                            output_port = ":if"

                    sink_node = query.nodes[sink]
                    if "IN" in sink_node.labels:
                        sink_port = ":in"
                    else:
                        sink_port = ""

                    gr_sub.edge(
                        f"{node_unique_id}{output_port}", f"{_get_node_unique_id(sink_node, query)}{sink_port}",
                    )

                for param_name, param_value in node.ep.kwargs.items():
                    if isinstance(param_value[1], NestedQuery):
                        nested_cluster = _get_nested_query(param_value[1], graph, graphs)
                        if not nested_cluster:
                            continue

                        gr_root.edge(
                            f"{node_unique_id}:{param_name}",
                            _get_node_unique_id(nested_cluster.roots[0], nested_cluster),
                            lhead=nested_cluster.get_id(),
                            style="dashed", dir="both", constraint=config.constraint_edges,
                        )

                if node.symbols:
                    build_symbols(node.symbols, gr, gr_sub, graphs, node_unique_id, config, graph_file=graph.file_path)

                if isinstance(node.ep, NestedQuery):
                    nested_cluster = _get_nested_query(node.ep, graph, graphs)
                    if not nested_cluster:
                        continue

                    gr_root.edge(
                        node_unique_id,
                        _get_node_unique_id(nested_cluster.roots[0], nested_cluster),
                        lhead=nested_cluster.get_id(),
                        style="dashed", dir="both", constraint=config.constraint_edges,
                    )


def render_otq(
    path: Union[str, List[str]],
    image_path: Optional[str] = None,
    output_format: Optional[str] = None,
    load_external_otqs: bool = True,
    view: bool = False,
    line_limit: Optional[Tuple[int, int]] = (10, 60),
    parse_eval_from_params: bool = False,
    render_debug_info: bool = False,
    debug: bool = False,
    graphviz_compat_mode: bool = False,
) -> str:
    """
    Render queries from .otq files.

    Parameters
    ----------
    path: str, List[str]
        Path to .otq file or list of paths to multiple .otq files.
        Needed to render query could be specified with the next format: `path_to_otq::query_name`
    image_path: str, None
        Path for generated image. If omitted, image will be saved in a temp dir
    output_format: str, None
        `Graphviz` rendering format. Default: `svg`.
        If `image_path` contains one of next extensions, `output_format` will be set automatically: `png`, `svg`, `dot`.
    load_external_otqs: bool
        If set to `True` (default) dependencies from external .otq files (not listed in ``path`` param)
        will be loaded automatically.
    view: bool
        Defines should generated image be shown after render.
    line_limit: Tuple[int, int], None
        Limit for maximum number of lines and length of some EP parameters strings.
        First param is limit of lines, second - limit of characters in each line.
        If set to None limit disabled.
        If one of tuple values set to zero the corresponding limit disabled.
    parse_eval_from_params: bool
        Enable parsing and printing `eval` sub-queries from EP parameters.
    render_debug_info: bool
        Render additional debug information.
    debug: bool
        Allow to print stdout or stderr from `Graphviz` render.
    graphviz_compat_mode: bool
        Change internal parameters of result graph for better compatibility with old `Graphviz` versions.
        Could produce larger and less readable graphs.

    Returns
    -------
    Path to rendered image

    Examples
    --------

    Render single file:

    >>> otp.utils.render_otq("./test.otq")  # doctest: +SKIP

    .. image:: ../../static/testing/images/render_otq_1.png

    Render multiple files:

    >>> otp.utils.render_otq(["./first.otq", "./second.otq"])  # doctest: +SKIP

    .. image:: ../../static/testing/images/render_otq_2.png

    Render specific queries from multiple files:

    >>> otp.utils.render_otq(["./first.otq", "./second.otq::some_query"])  # doctest: +SKIP
    """
    if line_limit is None:
        line_limit = (0, 0)

    height, width = line_limit
    if height < 0 or width < 0:
        raise ValueError("line_limit values should not be negative")

    config_kwargs = {}
    if graphviz_compat_mode:
        config_kwargs["constraint_edges"] = "false"

    config = Config(height=height, width=width, render_debug_info=render_debug_info, **config_kwargs)

    if not isinstance(path, list):
        path = [path]

    path = [_posix_path(p) for p in path]

    graphs = GraphStorage()

    queries_to_render: Dict[str, Set[str]] = defaultdict(set)
    path_files: List[str] = []

    for otq_path in path:
        query_file, query_name = _parse_query_path(otq_path)

        path_files.append(query_file)

        if queries_to_render[query_file] == {"*"}:
            continue

        if query_name:
            queries_to_render[query_file].add(query_name)
        else:
            queries_to_render[query_file] = {"*"}

    otq_files: Deque[str] = deque(path_files)

    while otq_files:
        otq_path = otq_files.popleft()

        graph = read_otq(otq_path, parse_eval_from_params=parse_eval_from_params)

        if not graph:
            continue

        graphs[otq_path] = graph

        for graph_query in graph.queries.values():
            for dep_file, dep_query in graph_query.depends:
                if dep_file is None:
                    dep_file = otq_path

                if dep_file not in graphs and load_external_otqs:
                    otq_files.append(dep_file)

                if queries_to_render[dep_file] == {"*"}:
                    continue

                if (
                    load_external_otqs and dep_file != otq_path or
                    dep_file == otq_path and graph_query.name in queries_to_render[otq_path]
                ):
                    if dep_query:
                        queries_to_render[dep_file].add(dep_query)
                    else:
                        queries_to_render[dep_file] = {"*"}

    if image_path:
        extension = Path(image_path).suffix

        if extension:
            extension = extension[1:]

            if extension == output_format or output_format is None and extension in {"png", "svg", "dot"}:
                image_path = str(Path(image_path).with_suffix(""))

                if output_format is None:
                    output_format = extension

    if not output_format:
        output_format = "svg"

    if not image_path:
        image_path = TmpFile().path

    gr = gv.Digraph(format=output_format, filename=image_path, engine="dot")
    gr.attr("graph", compound="true")
    gr.attr("node", shape="plaintext", margin="0")

    idx = 0
    for otq_path, graph in graphs.items():
        if not queries_to_render[otq_path]:
            continue

        with gr.subgraph(name=f"cluster__graph__{idx}", node_attr={"shape": "plaintext"}) as gr_otq:
            gr_otq.attr(label=otq_path)
            gr_otq.attr(margin="16")
            _render_graph(gr, gr_otq, graphs, otq_path, queries_to_render[otq_path], config)

        idx += 1

    try:
        return gr.render(view=view, quiet=not debug)
    except Exception as exc:
        raise RuntimeError(
            "Graphviz render failed. Try to set parameter `graphviz_compat_mode=True` "
            "for better compatibility if you use old Graphviz version"
        ) from exc
