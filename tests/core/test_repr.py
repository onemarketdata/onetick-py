import os

from onetick.py.otq import otq
import pytest
from onetick.test import TmpDir

import onetick.py as otp

OTQS = os.path.join(os.path.dirname(__file__), "otqs")


def test_to_dataframe(session):
    data = otp.Ticks(dict(x=[1, 2, 3]))

    df = otp.run(data)

    assert len(df) == 3
    assert df.x[0] == 1
    assert df.x[1] == 2
    assert df.x[2] == 3


def test_to_df(session):
    data = otp.Ticks(dict(x=[1, 2, 4]))

    df = otp.run(data)

    assert len(df) == 3
    assert df.x[0] == 1
    assert df.x[1] == 2
    assert df.x[2] == 4


@pytest.mark.xfail(reason="it might there is no necessary xdg packages")
def test_render(session):
    """
    Test render feature
    """
    data = otp.Ticks(dict(x=[3, 4, 1]))

    data.render()


def test_to_otq(session):
    """
    Test saving into an otq
    """
    data = otp.Ticks(dict(x=[3, 4, 1]))

    # save to a temporary dir
    tmp_dir = TmpDir()
    path = os.path.join(tmp_dir.path, "test.otq")
    data.to_otq(path)

    assert os.path.exists(path)

    # run the save .otq and get result back
    df = otp.run(path)

    assert len(df) == 3
    assert df.x[0] == 3
    assert df.x[1] == 4
    assert df.x[2] == 1


def test_print_api_graph(session):
    data = otp.Ticks(dict(x=[1, 2, 3]))

    data.print_api_graph()


def test_to_graph(session):
    data = otp.Ticks(dict(X=[1, 2, 3]))
    df = otp.run(data)
    assert all(df["X"] == [1, 2, 3])


@pytest.mark.parametrize("add_passthrough", [True, False])
def test_to_graph_add_passthrough(session, add_passthrough):
    data = otp.Ticks(dict(X=[1, 2, 3]))
    data["X"] += 1

    graph = data.to_graph(add_passthrough=add_passthrough)
    kwargs = {}
    otp.run(graph,
            symbols=otp.config['default_symbol'],
            start=otp.config['default_start_time'],
            end=otp.config['default_end_time'],
            **kwargs)
    _, output_nodes = graph.get_input_and_output_nodes()
    assert isinstance(output_nodes[0], otq.Passthrough) == add_passthrough


def test_to_otq_with_eval(session):
    temp_dir = TmpDir()
    path = os.path.join(temp_dir.path, "test.otq")
    q = otp.query(os.path.join(OTQS, "orders.otq::source"), DB="DB", TT="TT")  # any query with params
    q_full = otp.Query(q)
    q_full.to_otq(path, symbols=q)
    assert os.path.exists(path)
