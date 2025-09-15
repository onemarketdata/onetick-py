import os
import re
import io

from collections import defaultdict

from onetick.py.utils import abspath_to_query_by_name

MULTIPLE_NAMESAKE_PARAMETERS_ALLOWED = frozenset(
    [
        "BIND_SECURITY",
        # "PARAMETER" isn't on the list as it's handled separately
    ]
)


class QueryNotFoundError(FileNotFoundError):
    pass


class UncertainQueryName(ValueError):
    pass


def get_queries(otq_path):
    result = []
    with open(otq_path, "r") as fin:
        for line in fin:
            line = line.strip()

            res = re.match(r"^\[(.*)\]$", line)
            if res:
                name = res.group(1)
                if name != "_meta":
                    result.append(name)

    return result


def is_commented_out(node):
    return hasattr(node, "COMMENTED_OUT") and (node.COMMENTED_OUT == "1")


def get_query_info(otq_path, query_name="", inspected_graphs=None):
    """
    Collects some meta information about an .otq-based query.

    Parameters
    ----------
    otq_path: str
        Absolute path to an OTQ file.
    query_name: str
        Name of the query to analyze; if it's not specified, the .otq file should have only one query.
    inspected_graphs: None or Dict[tuple of (str, str) : Graph]
        Usually should be kept as the default value (None).

    Returns
    -------

    Graph - Instance of an internal class. Contains the following fields:
        nodes: list of Node
            Nodes in the query.
        sources: list of Node
            Nodes without incoming links.
        sinks: list of Node
            Nodes without outcoming links.
        nested_inputs: list of Node
            Nodes with nested inputs.
        nested_outputs: list of Node
            Nodes with nested outputs.
        has_unbound_sources: bool
            Whether the query needs an unbound security list to be provided.
    """
    if inspected_graphs is None:
        inspected_graphs = {}

    if (otq_path, query_name) in inspected_graphs:
        return inspected_graphs[(otq_path, query_name)]

    parsed = False
    found = False

    class Node:
        def __init__(self):
            self.SINK = []
            self.SOURCE = []

        def _get_ep_name_and_stack_info(self):
            if not hasattr(self, 'EP'):
                return None, None
            # self.EP can be multiline, so using re.DOTALL for dot to match newline too
            res = re.match(r"([A-Z_]+?)(\((.+)\))?$", self.EP, flags=re.DOTALL)
            if not res:
                return None, None
            ep_name = res.group(1)
            ep_parameters = res.group(3)
            stack_info = None
            if ep_parameters and 'STACK_INFO' in ep_parameters:
                res = re.match(r'.*STACK_INFO="?(-?\d+)', ep_parameters)
                if res:
                    stack_info = res.group(1)
            return ep_name, stack_info

    graph = defaultdict(Node)

    if not os.path.exists(otq_path):
        raise FileNotFoundError(f'otq "{otq_path}" is not found')

    if not otq_path.endswith('.py'):

        multiline_ep = False
        multiline_value = ''

        with open(otq_path, "r") as fin:
            for line in fin:
                line = line.strip()

                if line.startswith("[") and line.endswith("]"):
                    if ("[" + query_name + "]") == line:
                        found = True
                        continue
                    else:
                        if line != "[_meta]" and query_name == "":
                            if found is True:
                                raise UncertainQueryName(
                                    "No query name was passed, implying the file has only one query; it has more"
                                )
                            else:
                                found = True
                                continue

                if (found and not parsed) or multiline_ep:
                    if line.startswith("TYPE = "):
                        parsed = True
                        continue

                    if not multiline_ep:
                        res = re.match(r"NODE_?(\d+)_?(.*?)\s?=\s?(.*)", line)  # NOSONAR

                        if res:
                            num = int(res.group(1))
                            param = res.group(2)
                            value = res.group(3)
                        else:
                            res = re.match(r"ROOT_?(.*?)\s?=\s?(.*)", line)

                            if res:
                                num = 0
                                param = res.group(1)
                                value = res.group(2)

                        if res and line.endswith('\\'):
                            # first line of multiline
                            multiline_ep = True
                            multiline_value += value.replace('\\', '\n')
                            continue
                    else:
                        # next line of multiline
                        multiline_value += line.replace('\\', '\n')
                        if line.endswith('\\'):
                            continue
                        # last line of multiline
                        value = multiline_value
                        multiline_ep = False
                        multiline_value = ''

                    if res:
                        param, value = param.strip(), value.strip()

                        if param == "PARAMETER":
                            if not hasattr(graph[num], "PARAMETERS"):
                                setattr(graph[num], "PARAMETERS", {})
                            bits = value.split(" ")
                            inner_param_name = bits[0]
                            inner_param_value = " ".join(bits[1:])
                            graph[num].PARAMETERS[inner_param_name] = inner_param_value
                            continue

                        if param in ("SOURCE", "SINK"):
                            sources = []

                            for v in value.split():
                                if v == "ROOT":
                                    sources.append(0)
                                else:
                                    res = re.match(r"NODE_?(\d+)(\.(.*))*", v)
                                    sources.append(int(res.group(1)))

                            value = sources
                        elif param == "":
                            param = "EP"

                        if param in MULTIPLE_NAMESAKE_PARAMETERS_ALLOWED:
                            # A node can have several parameters with the same name
                            if not hasattr(graph[num], param):
                                setattr(graph[num], param, [])

                            getattr(graph[num], param).append(value)
                        else:
                            setattr(graph[num], param, value)

        if not found:
            raise QueryNotFoundError(f'Query "{query_name}" is not found in the {otq_path}')

    for num, node in graph.items():
        setattr(node, "NUM", num)
        setattr(node, "IS_NESTED", False)

        if hasattr(node, "NESTED_INPUT"):
            node.NESTED_INPUT = node.NESTED_INPUT.split()[0]

        if hasattr(node, "NESTED_OUTPUT"):
            node.NESTED_OUTPUT = node.NESTED_OUTPUT.split()[0]

        for src in node.SOURCE:
            if num not in graph[src].SINK:
                graph[src].SINK.append(num)

        for src in node.SINK:
            if num not in graph[src].SOURCE:
                graph[src].SOURCE.append(num)

        if not is_commented_out(node):
            # Commented nodes can affect sources and sinks, but cannot anything else
            if node.EP.startswith("NESTED_OTQ"):
                setattr(node, "IS_NESTED", True)
                if node.EP.strip() == "NESTED_OTQ":
                    if "OTQ_PATH" in node.PARAMETERS:
                        address = node.PARAMETERS["OTQ_PATH"]
                    elif "OTQ_NODE" in node.PARAMETERS:
                        address = node.PARAMETERS["OTQ_NODE"]
                    elif hasattr(node, "PH_PATH"):
                        address = node.PH_PATH
                    else:
                        raise ValueError("Can't get NESTED_OTQ address")

                    node.EP = "NESTED_OTQ " + address

                setattr(node, "NESTED_GRAPH", _load_nested_query(otq_path, node.EP, inspected_graphs))

    class Graph:
        def __init__(self, nodes):
            self.nodes = nodes
            self.sources = [node for _, node in graph.items() if len(node.SOURCE) == 0]
            self.sinks = [node for _, node in graph.items() if len(node.SINK) == 0]

            self.nested_inputs = [node for node in self.sources if hasattr(node, "NESTED_INPUT")]
            self.nested_outputs = [node for node in self.sinks if hasattr(node, "NESTED_OUTPUT")]

            inspected_graphs[(otq_path, query_name)] = self

            self.has_unbound_sources = self._check_for_unbound_sources()

        def _check_for_unbound_sources(self, bound_symbol_info=None):
            if bound_symbol_info is None:
                bound_symbol_info = {}

            for node in self.sources:
                self._search_for_bound_sink(node.NUM, bound_symbol_info)

            has_unbound_sources = any((not bound_symbol_info[node.NUM]) for node in self.sources)
            return has_unbound_sources

        def _search_for_bound_sink(self, root, bound_symbol_info):
            if root in bound_symbol_info:
                return

            if not is_commented_out(self.nodes[root]):
                # A commented node cannot have its own bound symbols, but can have a bound sink
                if hasattr(self.nodes[root], "BIND_SECURITY"):
                    for bound_security in self.nodes[root].BIND_SECURITY:
                        if (
                            ("_SYMBOL_NAME" not in bound_security)
                            and ("_SYMBOL_PARAM" not in bound_security)
                            and (not bound_security.endswith("No"))
                        ):
                            # The first two define dependency on lower-bound symbol;
                            # The last is whether the bound security is not unchecked in the list

                            bound_symbol_info[root] = True
                            break

                if self.nodes[root].IS_NESTED:
                    nested = self.nodes[root].NESTED_GRAPH
                    if not nested.has_unbound_sources:
                        bound_symbol_info[root] = True

            if root not in bound_symbol_info:
                for node in self.nodes[root].SINK:
                    if node not in bound_symbol_info:
                        self._search_for_bound_sink(node, bound_symbol_info)

                    if bound_symbol_info.get(node, None) is True:
                        bound_symbol_info[root] = True
                        break

                if root not in bound_symbol_info:
                    bound_symbol_info[root] = False

        def has_unbound_if_pinned(self, in_pins):
            """
            Parameters
            ----------
            in_pins: dict(str: bool)
                mapping of pins to whether the respective source needs unbound symbols

            Returns
            -------
            True if query with pins bound to graphs with described boundness state would need unbound symbols.
            """

            if not self.has_unbound_sources:
                # All sources are bound inside the query graph.
                return False

            in_pins_to_nodes = {node.NESTED_INPUT: node for node in self.nested_inputs}

            bound_symbol_info = {}

            for pin, needs_unbound in in_pins.items():
                if not needs_unbound:
                    bound_symbol_info[in_pins_to_nodes[pin].NUM] = True

            return self._check_for_unbound_sources(bound_symbol_info)

    return Graph(graph)


def _load_nested_query(nesting_file, nested_address, inspected_graphs):
    nested_address = nested_address.split(" ")[1]
    address_bits = nested_address.split("::")

    if len(address_bits) == 1:
        # Only the path is given, no query name
        # E.g. utils/FindSymbols.otq
        query_file = address_bits[0]
        query = ""
    elif address_bits[-1].endswith(".otq"):
        # remote://DB::utils/FindSymbols.otq
        query_file = address_bits[-1]
        query = ""
    else:
        # remote://DB::utils/FindSymbols.otq::Find
        query_file, query = address_bits[-2:]

    if query_file == "___ME___":
        otq_path = nesting_file
    else:
        otq_path = abspath_to_query_by_name(query_file)

    nested_graph = get_query_info(otq_path, query, inspected_graphs)
    return nested_graph


def add_pins(otq_path, query_name, specification):
    """
    The function takes a query and adds pins to the query according to the
    specification, and saves it back to the original file.

    Parameters
    ----------
    otq_path: str
        Absolute path to an OTQ file.
    query_name: str
        Name of the query to analyze
    specification: List[tuple]
        List of 3-values tuples, where the first value node from the `get_query_info` function,
        the second one is marker, the third one is name.
        A marker values: 1 for input, and 0 for output.

    Returns
    -------
    Nothing

    Raises
    ------
    FileNotFoundError
    """
    if not os.path.exists(otq_path):
        raise FileNotFoundError(f'otq "{otq_path}" is not found')

    found = False
    out = io.StringIO()

    with open(otq_path, "r") as fin:

        multiline_ep = False

        for line in fin:
            line = line.strip()

            if line.startswith("[") and line.endswith("]"):
                if ("[" + query_name + "]") == line:
                    found = True

            out.write(line + "\n")

            if found or multiline_ep:
                if not multiline_ep:
                    res = re.match(r"NODE_?(\d+)_?(.*?)\s?=\s?(.*)", line)  # NOSONAR

                    if res:
                        num = int(res.group(1))
                    else:
                        res = re.match(r"ROOT_?(.*?)\s?=\s?(.*)", line)

                        if res:
                            num = 0

                    if res and line.endswith('\\'):
                        # first line of multiline
                        multiline_ep = True
                        continue
                else:
                    if line.endswith('\\'):
                        continue
                    multiline_ep = False

                if res:
                    for inx, v in enumerate(specification):
                        node, pin_flag, pin_name = v
                        if node.NUM == num:  # noqa
                            if pin_flag is None:
                                continue
                            node_name = "NODE_" + str(num)
                            if num == 0:
                                node_name = "ROOT"

                            pin_id = "NESTED_INPUT" if pin_flag == 1 else "NESTED_OUTPUT"

                            out.write(node_name + "_" + pin_id + " = " + pin_name + "\n")

                            # don't use this specification item anymore
                            specification[inx] = (node, None, None)

    with open(otq_path, "w") as fout:
        fout.write(out.getvalue())


def get_query_parameter_list(otq_path, query_name):
    """Returns a list of query parameter names; can be used for nesting to pass the parameters to the nested query"""
    if not os.path.exists(otq_path):
        raise FileNotFoundError(f'otq "{otq_path}" is not found')

    found = False
    param_list = []

    with open(otq_path, "r") as fin:
        for line in fin:
            line = line.strip()

            if line.startswith("[") and line.endswith("]"):
                if ("[" + query_name + "]") == line:
                    found = True
                else:
                    found = False

            if found:
                params = re.findall(r'\$(\w+|(\{(\w+)\}))', line)
                if params:
                    param_list.extend([p[2] if p[2] != '' else p[0] for p in params])

    param_list.sort()
    return param_list
