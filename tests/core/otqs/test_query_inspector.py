import os
import pytest
import operator

import onetick.py as otp
from onetick.py.otq import otq
import onetick.py.core.query_inspector as qi

OTQS = os.path.dirname(os.path.abspath(__file__))

scenario = [
    ("order2prl.otq", "MakePRL", {"IN"}, {"OUT"}),
    ("combine.otq", "merge_and_update", {"ORDER", "FILL"}, {"OUT"}),
    ("combine.otq", "update", {"IN"}, {"OUT"}),
    ("combine.otq", "fill", set(), {"OUT"}),
    ("where_clause.otq", "condition", {"IN"}, {"IF_OUT", "ELSE_OUT"}),
    ("symbols.otq", "merge", set(), {"OUT"}),
    ("symbols.otq", "get_symbols", set(), set()),
    ("no_out_pin.otq", "no_out_pin", {"IN"}, set()),
    ("no_in_pin.otq", "no_in_pin", set(), {"OUT"}),
    ("custom_pin_names.otq", "custom_pin_names", {"IN_1"}, {"OUT_2"}),
    ("qte_trd.otq", "market_info", {"IN"}, {"OUT"}),
    ("Layering.otq", "_enrich_with_book_state", {"IN"}, {"OUT"}),
    ("Layering.otq", "_layering_book_and_alert", {"IN"}, {"OUT_ALERT"}),
    ("Layering.otq", "_schema_prepare", {"IN"}, {"OUT"}),
    ("Layering.otq", "_filter_orders", {"IN"}, {"OUT"}),
    ("Layering.otq", "_calculate_size_price_thresholds", {"IN"}, {"OUT_ASK_BID_SIZES", "OUT_PRICES"}),
    ("Layering.otq", "_add_orig_time", {"IN"}, {"OUT"}),
    ("Layering.otq", "_monitor_participant_sizes_detect_fills", {"IN"}, {"PARTICIPANT_SIZE"}),
    ("Layering.otq", "_cache_big_participants", {"IN"}, {"OUT"}),
    ("Layering.otq", "_finalize_monitoring_generate_alert", {"IN"}, {"ALERT"}),
    ("Layering.otq", "alert", {"IN"}, set()),
]


@pytest.fixture(scope="function")
def temp_config(monkeypatch):
    monkeypatch.setenv("MAIN_DIR", OTQS)
    monkeypatch.setenv("ONE_TICK_CONFIG", os.path.join(OTQS, "one_tick_config.txt"))


@pytest.mark.parametrize("otq,query,in_pins,out_pins", scenario)
def test_pins(temp_config, otq, query, in_pins, out_pins):
    graph = qi.get_query_info(os.path.join(OTQS, otq), query)

    assert len(graph.nested_inputs) == len(in_pins)
    assert len(graph.nested_outputs) == len(out_pins)

    assert set(map(operator.attrgetter("NESTED_INPUT"), graph.nested_inputs)) == in_pins
    assert set(map(operator.attrgetter("NESTED_OUTPUT"), graph.nested_outputs)) == out_pins


boundness_scenario = [
    ("simple_unbound", True),
    ("simple_bound", False),
    ("child_is_bound", False),
    ("common_child_is_bound", False),
    ("nesting_bound", False),
    ("nesting_unbound", True),
    ("nesting_both", True),
    ("nesting_both_binding_after", False),
    ("nesting_bound_separate_file", False),
    ("nesting_unbound_separate_file", True),
    ("nesting_bound_separate_dir", False),
    ("nesting_unbound_separate_dir", True),
    ("with_symbol_dependent_fsq", True),
    ("with_symbol_dependent_fsq_bound_after", False),
    ("with_symbol_param_dependent_fsq", True),
    ("with_unchecked_bound_list", True),
    ("with_commented_node_with_bound_list", True),
    ("with_commented_nested_with_bound_list", True),
]


@pytest.mark.parametrize("query,has_unbound", boundness_scenario)
def test_bound_symbols(temp_config, query, has_unbound):
    graph = qi.get_query_info(os.path.join(OTQS, "boundness.otq"), query)

    assert graph.has_unbound_sources == has_unbound


def test_multiple_bound_symbols(temp_config):
    graph = qi.get_query_info(os.path.join(OTQS, "boundness.otq"), "multiple_bound_symbols")
    node_id = list(graph.nodes.keys())[0]
    node = graph.nodes[node_id]

    assert hasattr(node, "BIND_SECURITY")
    assert len(node.BIND_SECURITY) == 2


def test_has_unbound_if_pinned(temp_config):
    graph = qi.get_query_info(os.path.join(OTQS, "merge.otq"), "merge")

    assert graph.has_unbound_sources is True
    assert graph.has_unbound_if_pinned({}) is True
    assert graph.has_unbound_if_pinned({"IN1": True, "IN2": True}) is True
    assert graph.has_unbound_if_pinned({"IN1": True, "IN2": False}) is True
    assert graph.has_unbound_if_pinned({"IN1": False, "IN2": True}) is True
    assert graph.has_unbound_if_pinned({"IN1": False, "IN2": False}) is False


def test_noname_query_when_several_are_present():
    with pytest.raises(qi.UncertainQueryName, match="only one query"):
        qi.get_query_info(os.path.join(OTQS, "dubious_boundness.otq"))
    # with


def test_non_existing_query():
    with pytest.raises(qi.QueryNotFoundError, match='Query "blablabla" is not found'):
        qi.get_query_info(os.path.join(OTQS, "combine.otq"), "blablabla")
    # with


def test_non_existing_otq():
    with pytest.raises(FileNotFoundError):
        qi.get_query_info(os.path.join(OTQS, "non_existing_otq"), "sonme")
    # with


def test_queries():
    res = qi.get_queries(os.path.join(OTQS, "combine.otq"))

    assert set(res) == {"update", "merge_and_update", "fill", "order"}


def test_get_query_parameter_list():
    params = qi.get_query_parameter_list(os.path.join(OTQS, "test_query_parameter_list.otq"), "query")
    assert params == ['PARAM_1', 'PARAM_2', 'PARAM_3', 'PARAM_4', 'PARAM_5', 'PARAM_6']


def test_multiline_ep(session):
    t = otp.Tick(A=1)
    t.sink(otq.PerTickScript('A = A + 1;\nA = A + 2;\nA = A + 3;\n'))
    path = t.to_otq()
    filename, query = path.split('::')
    graph = qi.get_query_info(filename, query)
    for i, node in graph.nodes.items():
        if node.EP.startswith('PER_TICK_SCRIPT'):
            assert 'A = A + 1;' in node.EP
            assert 'A = A + 2;' in node.EP
            assert 'A = A + 3;' in node.EP
