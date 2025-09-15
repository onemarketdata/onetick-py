import os
import pytest
from dataclasses import asdict
import xml.etree.ElementTree as ET

from onetick.py.utils import render


class TestGVTable:
    def test_base(self):
        table = render.GVTable()
        table.row(["cell1", "cell2", "cell3"])
        table.cell(["cell4"])
        table.row(["cell5", "cell6"])

        assert table.rows == [
            ([("cell1", {}), ("cell2", {}), ("cell3", {}), ("cell4", {})], {}),
            ([("cell5", {}), ("cell6", {})], {})
        ]

    def test_cell_and_attrs(self):
        table = render.GVTable()
        table.row(["cell1"], attrs={"test": 123})
        table.cell(["cell2", "cell3"])
        table.cell(["cell4"], attrs={"param": 456})

        table.row(["cell5"])
        table.cell(["cell6"])

        assert table.rows == [
            (
                [
                    ("cell1", {"test": 123}),
                    ("cell2", {"test": 123}),
                    ("cell3", {"test": 123}),
                    ("cell4", {"test": 123, "param": 456}),
                ], {"test": 123}
            ),
            ([("cell5", {}), ("cell6", {})], {})
        ]

    def test_cell_on_empty_table(self):
        table = render.GVTable()
        with pytest.raises(RuntimeError, match="No rows in table"):
            table.cell(["data"])

    def test_render(self):
        table = render.GVTable()
        table.row(["cell1", "cell2"])
        table.row(["cell3", "cell4"])
        root = ET.fromstring(str(table)[1:-1])
        assert (
            root.tag == "TABLE" and
            root.find("./TR[1]/TD[1]").text == "cell1" and
            root.find("./TR[1]/TD[2]").text == "cell2" and
            root.find("./TR[2]/TD[1]").text == "cell3" and
            root.find("./TR[2]/TD[2]").text == "cell4"
        )

    def test_render_attrs(self):
        table = render.GVTable()
        table.row(["cell1"], attrs={"TEST": "1"})
        table.cell(["cell2"], attrs={"TEST": "2"})
        table.row(["cell3"])
        table.cell(["cell4"], attrs={"TEST": "1"})

        root = ET.fromstring(str(table)[1:-1])
        cell1 = root.find("./TR[1]/TD[1]")
        cell2 = root.find("./TR[1]/TD[2]")
        cell3 = root.find("./TR[2]/TD[1]")
        cell4 = root.find("./TR[2]/TD[2]")

        assert (
            root.tag == "TABLE" and
            cell1.text == "cell1" and cell1.attrib["TEST"] == "1" and
            cell2.text == "cell2" and cell2.attrib["TEST"] == "2" and
            cell3.text == "cell3" and not len(cell3.attrib) and
            cell4.text == "cell4" and cell4.attrib["TEST"] == "1"
        )

    def test_render_table_attrs(self):
        table = render.GVTable(attrs={"TEST": "0"})
        table.row(["cell1"], attrs={"TEST": "1"})

        root = ET.fromstring(str(table)[1:-1])

        assert (
            root.tag == "TABLE" and root.attrib["TEST"] == "0" and
            root.find("./TR[1]/TD[1]").attrib["TEST"] == "1"
        )

    def test_render_multiline_cell(self):
        table = render.GVTable()
        table.row([["mutiline", "test"]])

        result = str(table)
        assert result[0] == "<" and result[-1] == ">"

        root = ET.fromstring(result[1:-1])
        assert root.tag == "TABLE" and root.find("./TR/TD/BR") is not None

    def test_render_auto_colspan(self):
        # auto_colspan by default True
        table = render.GVTable()
        # 1 2 3 4
        # 5 6
        # 7
        table.row(["cell1", "cell2", "cell3", "cell4"])
        table.row(["cell5"])
        table.cell(["cell6"])
        table.row(["cell7"], attrs={"colspan": 1})
        root = ET.fromstring(str(table)[1:-1])

        assert table.max_cols == 4
        assert (
            root.tag == "TABLE" and
            root.find("./TR[1]/TD[1]").attrib.get("COLSPAN") is None and
            root.find("./TR[1]/TD[2]").attrib.get("COLSPAN") is None and
            root.find("./TR[1]/TD[3]").attrib.get("COLSPAN") is None and
            root.find("./TR[1]/TD[4]").attrib.get("COLSPAN") is None and
            root.find("./TR[2]/TD[1]").attrib.get("COLSPAN") is None and
            root.find("./TR[2]/TD[2]").attrib["COLSPAN"] == "3" and
            root.find("./TR[3]/TD[1]").attrib["COLSPAN"] == "1"
        )

    def test_render_no_auto_colspan(self):
        # auto_colspan by default True
        table = render.GVTable(auto_colspan=False)
        # 1
        # 2 3 4
        table.row(["cell1"])
        table.row(["cell2", "cell3", "cell4"])
        root = ET.fromstring(str(table)[1:-1])

        assert table.max_cols == 3
        assert (
            root.tag == "TABLE" and
            root.find("./TR[1]/TD[1]").attrib.get("COLSPAN") is None and
            root.find("./TR[2]/TD[1]").attrib.get("COLSPAN") is None and
            root.find("./TR[2]/TD[2]").attrib.get("COLSPAN") is None and
            root.find("./TR[2]/TD[3]").attrib.get("COLSPAN") is None
        )

    def test_render_empty_table(self):
        table = render.GVTable()
        root = ET.fromstring(str(table)[1:-1])
        assert root.tag == "TABLE" and root.find("./TR[1]") is None


class TestParseFunction:
    @pytest.mark.parametrize("func,ep,args,kwargs", [
        ("some_random_string", None, [], {}),
        ("function()", "function", [], {}),
        ("function(123, \"456\")", "function", ["123", "456"], {}),
        ("function_test(123, PARAM=\"456\")", "function_test", ["123"], {"param": ("PARAM", "456")}),
    ])
    def test_base(self, func, ep, args, kwargs):
        res_ep, res_args, res_kwargs = render._parse_function(func)
        assert res_ep == ep
        assert res_args == args
        assert res_kwargs == kwargs

    @pytest.mark.parametrize("params_str,args,kwargs,exc", [
        ("\"123\",t=123", ["123"], {"t": ("t", "123")}, None),
        ("\"123\", t=123", ["123"], {"t": ("t", "123")}, None),
        ("\"123\" ,t=123", ["123"], {"t": ("t", "123")}, None),
        ("\"123\", value, t=123, v=456", ["123", "value"], {"t": ("t", "123"), "v": ("v", "456")}, None),
        ("PARAM=123", [], {"param": ("PARAM", "123")}, None),
        ("\"test\\'escaping\", also=\"in\\'kwargs\"", ["test\'escaping"], {"also": ("also", "in\'kwargs")}, None),
        ("\"123'456\",t=123", ["123'456"], {"t": ("t", "123")}, None),
        ("k=1,test", None, None, (RuntimeError, "Positional argument")),
        ("\"some other test", None, None, (ValueError, "unclosed quote")),
    ])
    def test_parse_params(self, params_str, args, kwargs, exc):
        if exc:
            with pytest.raises(exc[0], match=exc[1]):
                render._parse_function_params(params_str)
        else:
            res_args, res_kwargs = render._parse_function_params(params_str)
            assert res_args == args
            assert res_kwargs == kwargs

    def test_parse_query_path(self, monkeypatch):
        monkeypatch.setattr(os, "sep", "\\")
        assert {"file.otq", None} == set(render._parse_query_path("file.otq"))
        assert {"file.otq", "query"} == set(render._parse_query_path("file.otq::query"))
        assert {"remote://some_db::some/path/to.otq", "query"} == set(
            render._parse_query_path("remote://some_db::some/path/to.otq::query")
        )
        assert {"C:/test/file.otq", "query"} == set(render._parse_query_path("C:/test/file.otq::query"))
        assert {"C:/test/file.otq", "query"} == set(render._parse_query_path("C:\\test\\file.otq::query"))
        assert {"C:/test/file.otq", "query"} == set(render._parse_query_path(r"C:\test\file.otq::query"))


class TestEpParsers:
    @pytest.mark.parametrize("security_str,expected_type,value,second_param,is_active", [
        ("AAAA 0", render.EP, "AAAA", "0", True),
        ("AAAA 0 No", render.EP, "AAAA", "0", False),
        ("DUMMY_DB::AAAA 1", render.EP, "DUMMY_DB::AAAA", "1", True),
        ("eval(THIS::some_query) 0", render.NestedQuery, "some_query", "0", True),
    ])
    def test_parse_security(self, security_str, expected_type, value, second_param, is_active):
        security, second_param_res, is_active_res = render._parse_security(security_str)

        assert isinstance(security, expected_type)
        if isinstance(security, render.EP):
            assert security.name == value
        elif isinstance(security, render.NestedQuery):
            assert security.query == value
        else:
            assert security == value

        assert second_param_res == second_param
        assert is_active_res == is_active

    def test_parse_ep(self):
        assert asdict(render._parse_ep("NESTED_OTQ ___ME___::query")) == {
            "name": "NESTED_OTQ", "raw_string": "NESTED_OTQ ___ME___::query", "query": "query", "expression": None,
            "file_path": None, "args": [], "kwargs": {}, "is_local": True,
        }

        assert asdict(render._parse_ep("EVAL(THIS::query)")) == {
            "name": "EVAL", "raw_string": "EVAL(THIS::query)", "query": "query", "expression": None, "file_path": None,
            "args": [], "kwargs": {}, "is_local": True,
        }

        assert asdict(render._parse_ep("EVAL(\"some_code = 123\")")) == {
            "name": "EVAL", "raw_string": "EVAL(\"some_code = 123\")", "query": None, "expression": "some_code = 123",
            "file_path": None, "args": [], "kwargs": {}, "is_local": True,
        }

        assert asdict(render._parse_ep("JOIN_WITH_QUERY(otq_query=some_file.otq::query)")) == {
            "name": "JOIN_WITH_QUERY", "raw_string": "JOIN_WITH_QUERY(otq_query=some_file.otq::query)",
            "query": "query", "expression": None, "file_path": "some_file.otq",
            "args": [], "kwargs": {}, "is_local": False,
        }

        assert asdict(render._parse_ep("OTHER_EP(arg, kwarg=1)")) == {
            "name": "OTHER_EP", "raw_string": "OTHER_EP(arg, kwarg=1)",
            "args": ["arg"], "kwargs": {"kwarg": ("kwarg", "1")},
        }

        assert asdict(render._parse_ep("OTHER_EP")) == {
            "name": "OTHER_EP", "raw_string": "OTHER_EP", "args": [], "kwargs": {},
        }

        assert asdict(render._parse_ep("JOIN_WITH_QUERY(other_param=some_file.otq::query)")) == {
            "name": "JOIN_WITH_QUERY", "raw_string": "JOIN_WITH_QUERY(other_param=some_file.otq::query)",
            "args": [], "kwargs": {"other_param": ("other_param", "some_file.otq::query")},
        }

        assert asdict(render._parse_ep("READ_CACHE(cache_name=cache, read_mode=CACHE_ONLY)")) == {
            "name": "READ_CACHE", "raw_string": "READ_CACHE(cache_name=cache, read_mode=CACHE_ONLY)",
            "args": [], "kwargs": {
                "cache_name": ("cache_name", "cache"),
                "read_mode": ("read_mode", "CACHE_ONLY"),
            },
        }

        assert asdict(render._parse_ep("WHERE_CLAUSE(WHERE=\"1=1\")")) == {
            "name": "WHERE_CLAUSE", "raw_string": "WHERE_CLAUSE(WHERE=\"1=1\")",
            "args": [], "kwargs": {"where": ("WHERE", "1=1")},
            "if_nodes": set(), "else_nodes": set(),
        }

        assert asdict(
            render._parse_ep("WHERE_CLAUSE(WHERE=eval(\"path_to.otq::query\"))", parse_eval_from_params=True)
        ) == {
            "name": "WHERE_CLAUSE", "raw_string": "WHERE_CLAUSE(WHERE=eval(\"path_to.otq::query\"))",
            "args": [], "kwargs": {"where": ("WHERE", {
                "name": "eval", "raw_string": "eval(path_to.otq::query)", "query": "query", "file_path": "path_to.otq",
                "expression": None, "is_local": False, "args": [], "kwargs": {},
            })}, "if_nodes": set(), "else_nodes": set(),
        }

        assert asdict(
            render._parse_ep("WHERE_CLAUSE(WHERE=eval(\"path_to.otq::query\"))", parse_eval_from_params=False)
        ) == {
            "name": "WHERE_CLAUSE", "raw_string": "WHERE_CLAUSE(WHERE=eval(\"path_to.otq::query\"))",
            "args": [], "kwargs": {"where": ("WHERE", "eval(path_to.otq::query)")},
            "if_nodes": set(), "else_nodes": set(),
        }


class TestReadOTQ:
    def test_base(self, cur_dir):
        # check basic parsing and some values for real otq file
        otq_path = str(cur_dir + "otqs" + "merge.otq")
        graph = render.read_otq(otq_path)

        assert graph.config["START"] == "20180607120000000"
        assert graph.config["END"] == "20180607121500000"
        assert graph.config["TZ"] == "EDT5EST"

        assert list(graph.queries.keys()) == ["merge"]
        query = graph.queries["merge"]

        assert set(query.roots) == {"NODE_6", "NODE_8"} and set(query.leaves) == {"ROOT"}
        assert query.config["TYPE"] == "GRAPH"
        assert query.depends == set()
        assert query.params == {}

        assert set(query.nodes.keys()) == {"ROOT", "NODE_6", "NODE_8", "NODE_10"}
        nodes = query.nodes

        values_to_check = {
            "ROOT": {"type": render.EP, "name": "PASSTHROUGH", "sinks": set()},
            "NODE_6": {"type": render.EP, "name": "PASSTHROUGH", "sinks": {"NODE_10"}},
            "NODE_8": {"type": render.EP, "name": "PASSTHROUGH", "sinks": {"NODE_10"}},
            "NODE_10": {
                "type": render.EP, "name": "MERGE", "sinks": {"ROOT"},
                "kwargs": {"identify_input_ts": ("IDENTIFY_INPUT_TS", "false")},
            },
        }

        for node_id, params in values_to_check.items():
            node = nodes[node_id]
            assert node.query == "merge"
            assert isinstance(node.ep, params["type"])
            assert node.ep.name == params["name"]
            assert node.ep.args == []
            assert node.ep.kwargs == params.get("kwargs", {})
            assert node.tick_type is None
            assert node.params == params.get("params", {})
            assert set(node.sinks) == params["sinks"]
            assert node.symbols == []

    def test_multiple_queries_with_deps(self, cur_dir):
        otq_path = str(cur_dir + "otqs" + "combine.otq")
        graph = render.read_otq(otq_path)

        values_to_check = {
            "update": {
                "leaves": {"ROOT"}, "roots": {"NODE_24"}, "symbols": {"AAPL"},
                "nodes": {
                    "NODE_23": {
                        "type": render.EP, "name": "UPDATE_FIELD", "sinks": {"ROOT"},
                        "kwargs": {"field": ("FIELD", "x"), "value": ("VALUE", "x * 2")},
                    },
                    "NODE_24": {"type": render.EP, "name": "PASSTHROUGH", "sinks": {"NODE_23"}},
                    "ROOT": {"type": render.EP, "name": "PASSTHROUGH", "sinks": set()},
                },
            },
            "merge_and_update": {
                "leaves": {"ROOT"}, "roots": {"NODE_24", "NODE_27"}, "symbols": {"AAPL"}, "depends": {(None, "update")},
                "nodes": {
                    "NODE_24": {"type": render.EP, "name": "PASSTHROUGH", "sinks": {"NODE_28"}},
                    "NODE_27": {"type": render.EP, "name": "PASSTHROUGH", "sinks": {"NODE_28"}},
                    "NODE_28": {
                        "type": render.EP, "name": "MERGE", "sinks": {"NODE_29"},
                        "kwargs": {"identify_input_ts": ("IDENTIFY_INPUT_TS", "false")},
                    },
                    "NODE_29": {
                        "type": render.NestedQuery, "name": "NESTED_OTQ",
                        "query": (None, "update", None, True), "sinks": {"ROOT"},
                    },
                    "ROOT": {"type": render.EP, "name": "PASSTHROUGH", "sinks": set()},
                },
            },
            "fill": {
                "leaves": {"ROOT"}, "roots": {"NODE_2"},
                "nodes": {
                    "NODE_2": {
                        "type": render.EP, "name": "TICK_GENERATOR", "sinks": {"ROOT"}, "tick_type": "ANY",
                        "kwargs": {"bucket_interval": ("BUCKET_INTERVAL", "0"), "fields": ("FIELDS", "long x=1")},
                    },
                    "ROOT": {"type": render.EP, "name": "PASSTHROUGH", "sinks": set()},
                },
            },
            "order": {
                "leaves": {"ROOT"}, "roots": {"NODE_2"},
                "nodes": {
                    "NODE_2": {
                        "type": render.EP, "name": "TICK_GENERATOR", "sinks": {"ROOT"}, "tick_type": "ANY",
                        "kwargs": {"bucket_interval": ("BUCKET_INTERVAL", "0"), "fields": ("FIELDS", "long x=2")},
                    },
                    "ROOT": {"type": render.EP, "name": "PASSTHROUGH", "sinks": set()},
                },
            },
        }

        assert set(graph.queries.keys()) == set(values_to_check.keys())

        for query_name, query in graph.queries.items():
            value_to_check = values_to_check[query_name]

            assert set(query.leaves) == value_to_check["leaves"]
            assert set(query.roots) == value_to_check["roots"]
            assert set(
                sym[0].name if isinstance(sym[0], (render.EP, render.NestedQuery)) else sym[0] for sym in query.symbols
            ) == value_to_check.get("symbols", set())

            assert query.depends == value_to_check.get("depends", set())

            for node_id, params in value_to_check["nodes"].items():
                node = query.nodes[node_id]

                assert node.query == query_name

                ep = node.ep

                assert isinstance(ep, params["type"])

                if isinstance(ep, render.NestedQuery):
                    file_path, n_query, expression, is_local = params["query"]
                    assert (
                        (file_path and file_path == ep.file_path or not file_path and ep.file_path is None) and
                        (n_query and n_query == ep.query or not n_query and ep.query is None) and
                        (expression and expression == ep.expression or not expression and ep.expression is None) and
                        is_local == ep.is_local
                    )
                assert ep.name == params["name"]
                assert ep.args == []
                assert ep.kwargs == params.get("kwargs", {})
                assert node.tick_type == params.get("tick_type", node.tick_type)
                assert node.params == params.get("params", {})
                assert set(node.sinks) == params["sinks"]
                assert node.symbols == params.get("symbols", [])

    def test_query_with_param(self, cur_dir):
        # Check only NestedQueries in symbols and params
        otq_path = str(cur_dir + "otqs" + "Layering.otq")
        graph = render.read_otq(otq_path)

        assert set(graph.queries.keys()) == {
            "Impl", "_enrich_with_book_state", "_layering_book_and_alert", "_schema_prepare", "_filter_orders",
            "_calculate_size_price_thresholds", "_add_orig_time", "_monitor_participant_sizes_detect_fills",
            "_cache_big_participants", "_finalize_monitoring_generate_alert", "alert", "FSQ", "DetailsExtractAndSave",
            "________________", "_DEVLayering", "orders",
        }

        query_1_node = graph.queries["Impl"].nodes["NODE_125"]
        assert (
            isinstance(query_1_node.ep, render.NestedQuery) and
            query_1_node.ep.name == "NESTED_OTQ" and query_1_node.ep.query == "_layering_book_and_alert" and
            query_1_node.ep.expression is None and query_1_node.ep.file_path is None and
            query_1_node.ep.is_local and not query_1_node.ep.args and not query_1_node.ep.kwargs and
            query_1_node.params == {
                "MAX_CANCEL_DELAY_SEC": "$MAX_CANCEL_DELAY_SEC", "MAX_FILL_DELAY_SEC": "$MAX_FILL_DELAY_SEC",
                "MIN_CANCEL_PCT": "$MIN_CANCEL_PCT", "SIZE_IDS": "$SIZE_IDS"
            }
        )

        query_2_symbols = graph.queries["_DEVLayering"].symbols
        assert (
            len(query_2_symbols) == 1 and isinstance(query_2_symbols[0][0], render.NestedQuery) and
            query_2_symbols[0][0].name == "eval" and query_2_symbols[0][0].query == "FSQ" and
            query_2_symbols[0][0].expression is None and query_2_symbols[0][0].file_path is None and
            query_2_symbols[0][0].is_local and not query_2_symbols[0][0].args and
            query_2_symbols[0][0].kwargs == {"dbs_ticks": ("DBS_TICKS", "$DB"), "pattern": ("PATTERN", "$PATTERN")}
        )

        # check SINK/SOURCE params
        query_3_node = graph.queries["_calculate_size_price_thresholds"].nodes["NODE6"]
        assert query_3_node.sinks == ["NODE_77", "NODE_58"]

        # check node labels
        query_4 = graph.queries["Impl"]
        assert query_4.nodes["NODE_47"].labels["IN"] == "IN"  # NESTED_INPUT
        assert query_4.nodes["ROOT"].labels["OUT"] == "OUT"  # NESTED_OUTPUT
        assert query_4.nodes["NODE_125"].labels["OUT"] == "OUT_ALERT"  # SOURCE_DESCRIPTION

    def test_query_where_clause(self, cur_dir):
        otq_path = str(cur_dir + "otqs" + "where_clause.otq")
        graph = render.read_otq(otq_path)

        where_clause = graph.queries["condition"].nodes["NODE_3"]
        assert set(where_clause.sinks) == {"NODE_2", "ROOT"}
        assert isinstance(where_clause.ep, render.IfElseEP)
        assert where_clause.ep.if_nodes == {"ROOT"}
        assert where_clause.ep.else_nodes == {"NODE_2"}

    def test_read_not_exists_or_remote(self):
        assert render.read_otq("some_not_existing.otq") is None
        assert render.read_otq("remote://SERVER::some/path/to.otq::query") is None


def test_truncate_param_value():
    test_ep = render.EP("test", "test")
    suitable_ep = render.EP("PER_TICK_SCRIPT", "PER_TICK_SCRIPT")

    # single line
    assert render.truncate_param_value(test_ep, "script", "a" * 30, (1, 20)) == "a" * 30
    assert render.truncate_param_value(suitable_ep, "param", "a" * 30, (1, 20)) == "a" * 30
    assert render.truncate_param_value(suitable_ep, "script", "a" * 30, (1, 20)) == "a" * 20 + "..."

    # multiline
    assert render.truncate_param_value(
        test_ep, "script", "\n".join(["a" * 30] * 3), (2, 20)
    ) == "\n".join(["a" * 30] * 3)
    assert render.truncate_param_value(
        suitable_ep, "param", "\n".join(["a" * 20] + ["b" * 10] + ["c" * 30] * 3), (4, 20)
    ) == "\n".join(["a" * 20] + ["b" * 10] + ["c" * 30] * 3)
    assert render.truncate_param_value(
        suitable_ep, "script", "\n".join(["a" * 20] + ["b" * 10] + ["c" * 30] * 3), (4, 20)
    ) == "\n".join(["a" * 20] + ["b" * 10] + ["c" * 20 + "..."] * 2 + ["..."])

    with pytest.raises(ValueError, match="negative"):
        render.truncate_param_value(test_ep, "param", "value", line_limit=(-1, 20))
        render.truncate_param_value(test_ep, "param", "value", line_limit=(20, -1))
        render.truncate_param_value(test_ep, "param", "value", line_limit=(-1, -1))
