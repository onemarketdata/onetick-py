import pytest

import onetick.py as otp


query = otp.query


def test_query_apply_1(session, cur_dir):
    """
    Check that columns are dropped by default after applying external
    query
    """
    t1 = otp.Ticks({"x": [3]})
    t2 = otp.Ticks({"x": [7]})

    q = otp.query(cur_dir + "otqs" + "merge.otq::merge")

    res = q(IN1=t1, IN2=t2)["OUT"]

    assert len(otp.run(res)) == 2

    assert "x" in t1.columns()
    assert "x" in t2.columns()
    assert "x" not in res.columns()


def test_query_apply_2(session, cur_dir):
    """
    Set output columns equal to input saves columns after applying query
    """
    t1 = otp.Ticks({"x": [3]})
    t2 = otp.Ticks({"x": [7]})

    q = otp.query(cur_dir + "otqs" + "merge.otq::merge", otp.query.config(output_columns="input"))

    res = q(IN1=t1, IN2=t2)["OUT"]

    assert len(otp.run(res)) == 2

    assert "x" in t1.columns()
    assert "x" in t2.columns()
    assert "x" in res.columns()


def test_query_config_1(cur_dir):
    """
    output_columns as list of columns with type
    """
    query(cur_dir + "otqs" + "merge.otq::merge", query.config(output_columns=[]))


def test_query_config_2(cur_dir):
    """
    output_columns as dict, where keys are outputs and values are config per output pin
    """
    query(cur_dir + "otqs" + "merge.otq::merge", query.config(output_columns={}))


def test_query_config_3(cur_dir):
    config = otp.query.config(output_columns="input")
    query = otp.query(cur_dir + "otqs" + "custom_pin_names.otq::custom_pin_names_without_TT", config)
    data = otp.Tick(x=0, y="y", z=0.1)

    data = data.apply(query)

    out_schema = data.columns(skip_meta_fields=True)
    assert len(out_schema) == 3
    assert "x" in out_schema and out_schema["x"] == int
    assert "y" in out_schema and out_schema["y"] == str
    assert "z" in out_schema and out_schema["z"] == float


def test_query_config_4(cur_dir):
    config = otp.query.config(output_columns=[("x", int), ("y", str), ("z", float)])
    query = otp.query(cur_dir + "otqs" + "custom_pin_names.otq::custom_pin_names_without_TT", config)
    data = otp.Tick(a=0)

    data = data.apply(query)

    out_schema = data.columns(skip_meta_fields=True)
    assert len(out_schema) == 3
    assert "x" in out_schema and out_schema["x"] == int
    assert "y" in out_schema and out_schema["y"] == str
    assert "z" in out_schema and out_schema["z"] == float


def test_query_config_5(cur_dir):
    config = otp.query.config(output_columns=["input", ("x", int), ("y", str), ("z", float)])
    query = otp.query(cur_dir + "otqs" + "custom_pin_names.otq::custom_pin_names_without_TT", config)
    data = otp.Tick(a=0)

    data = data.apply(query)

    out_schema = data.columns(skip_meta_fields=True)
    assert len(out_schema) == 4
    assert "a" in out_schema and out_schema["a"] == int
    assert "x" in out_schema and out_schema["x"] == int
    assert "y" in out_schema and out_schema["y"] == str
    assert "z" in out_schema and out_schema["z"] == float


def test_query_config_6(cur_dir):
    config = otp.query.config(
        output_columns={
            "out_0": "input",
            "out_1": [
                "input", ("x", int)
            ],
            "out_2": [
                ("x", int)
            ],
        }
    )
    query = otp.query(cur_dir + "otqs" + "custom_pin_names.otq::custom_pin_names_without_TT", config)
    data = otp.Tick(a=0)

    res_0 = otp.apply_query(query, {'IN': data}, ["out_0"])
    out_schema_0 = res_0.columns(skip_meta_fields=True)

    res_1 = otp.apply_query(query, {'IN': data}, ["out_1"])
    out_schema_1 = res_1.columns(skip_meta_fields=True)

    res_2 = otp.apply_query(query, {'IN': data}, ["out_2"])
    out_schema_2 = res_2.columns(skip_meta_fields=True)

    assert len(out_schema_0) == 1
    assert "a" in out_schema_0 and out_schema_0["a"] == int

    assert len(out_schema_1) == 2
    assert "a" in out_schema_1 and out_schema_1["a"] == int
    assert "x" in out_schema_1 and out_schema_1["x"] == int

    assert len(out_schema_2) == 1
    assert "x" in out_schema_2 and out_schema_2["x"] == int


def test_query_config_neg_0():
    """
    config supposes to support only list, dict and str types
    """
    with pytest.raises(TypeError, match="type"):
        query.config(output_columns=345)
    with pytest.raises(TypeError, match="type"):
        query.config(output_columns=[345])
    # with
    with pytest.raises(TypeError, match="type"):
        query.config(output_columns={"out_pin": 345})
    # with


def test_query_config_neg_1(cur_dir):
    """
    only 'input' is a valid option for string output_columns
    """
    with pytest.raises(ValueError):
        query(cur_dir + "otqs" + "merge.otq::merge", query.config(output_columns="dsdsddsd"))


def test_query_config_neg_2():
    with pytest.raises(ValueError, match="input"):
        otp.query.config(output_columns=["input", "input"])


def test_query_config_neg_3():
    with pytest.raises(ValueError, match="blep"):
        otp.query.config(output_columns=["blep"])


def test_query_config_neg_4():
    with pytest.raises(ValueError, match="blep"):
        otp.query.config(output_columns={"out_pin": "blep"})


def test_query_config_neg_5():
    with pytest.raises(ValueError, match="blep"):
        otp.query.config(output_columns={"out_pin": ["blep"]})


def test_query_config_neg_6():
    with pytest.raises(ValueError, match="input"):
        otp.query.config(output_columns={"out_pin": ["input", "input"]})


def test_query_apply_3(session, cur_dir):
    where_clause = otp.query(cur_dir + "otqs" + "where_clause.otq::condition", CONDITION="x < 3")
    m = otp.Ticks({"x": [0, 1, 2, 3, 4]})

    res1, res2 = where_clause(m)["IF_OUT", "ELSE_OUT"]

    assert len(otp.run(res1)) == 3
    assert len(otp.run(res2)) == 2
    assert "x" not in res1.columns()
    assert "x" not in res2.columns()


def test_query_apply_4(session, cur_dir):
    where_clause = otp.query(
        cur_dir + "otqs" + "where_clause.otq::condition", query.config(output_columns="input"), CONDITION="x < 3"
    )
    m = otp.Ticks({"x": [0, 1, 2, 3, 4]})

    res1, res2 = where_clause(m)["IF_OUT", "ELSE_OUT"]

    assert len(otp.run(res1)) == 3
    assert len(otp.run(res2)) == 2
    assert "x" in res1.columns()
    assert "x" in res2.columns()


def test_source_apply_query_1(session, cur_dir):
    update_q = otp.query(cur_dir + "otqs" + "update1.otq::update")
    m = otp.Ticks({"x": [0, 1, 2, 3, 4]})

    res = m.apply(update_q)

    assert len(otp.run(res)) == 5
    assert "x" in m.columns()
    assert "x" not in res.columns()


def test_source_apply_query_2(session, cur_dir):
    update_q = otp.query(
        cur_dir + "otqs" + "update1.otq::update", otp.query.config(output_columns="input")
    )
    m = otp.Ticks({"x": [0, 1, 2, 3, 4]})

    res = m.apply(update_q)

    assert len(otp.run(res)) == 5
    assert "x" in m.columns()
    assert "x" in res.columns()


@pytest.mark.parametrize("query_name", ["bound", "unbound"])
def test_query_boundness(session, query_name, cur_dir):
    q = otp.query(cur_dir + "otqs" + f"dubious_boundness.otq::{query_name}")()["OUT"]
    res = otp.run(q)

    assert len(res) == 1
    assert "DUMMY" in res.columns


def test_query_boundness_before_query(session, cur_dir):
    t1 = otp.Ticks({"x": ["a"]}, symbol="A")
    t2 = otp.Ticks({"x": ["b"]}, symbol="B")

    q = otp.query(cur_dir + "otqs" + "merge.otq::merge")

    res = q(IN1=t1, IN2=t2)["OUT"]

    assert not res._is_unbound_required()
    assert len(otp.run(res)) == 2


@pytest.mark.parametrize("query_name", ["bound", "unbound"])
def test_query_boundness_query_path(session, query_name, cur_dir):
    dubiously_bound_query = str(cur_dir + "otqs" + f"dubious_boundness.otq::{query_name}")
    q = otp.Query(dubiously_bound_query)
    res = otp.run(q)

    assert len(res) == 1
    assert "DUMMY" in res.columns


@pytest.mark.parametrize("query_name", ["bound", "unbound"])
def test_query_boundness_query_object(session, query_name, cur_dir):
    dubiously_bound_query = otp.query(cur_dir + "otqs" + f"dubious_boundness.otq::{query_name}")
    q = otp.Query(dubiously_bound_query)
    res = otp.run(q)

    assert len(res) == 1
    assert "DUMMY" in res.columns


def test_query_query_path_with_params(session, cur_dir):
    parametrizeable_query = str(cur_dir + "otqs" + "parametrizeable.otq::parametrizeable")
    q = otp.Query(parametrizeable_query, params={"X": "AAAAAA"})
    res = otp.run(q)

    assert len(res) == 1
    assert "FIELD" in res.columns
    assert res.loc[0, "FIELD"] == "AAAAAA"


def test_query_query_object_with_params(session, cur_dir):
    parametrizeable_query = cur_dir + "otqs" + "parametrizeable.otq::parametrizeable"
    with pytest.raises(ValueError, match="both params and a query"):
        otp.Query(otp.query(parametrizeable_query), params={"X": "AAAAAA"})


def test_query_wrong_parameter(session):
    class StubObject:
        pass

    with pytest.raises(ValueError, match="parameter has to be"):
        otp.Query(StubObject())


def test_query_copy(session, cur_dir):
    parametrizeable_query = str(cur_dir + "otqs" + "parametrizeable.otq::parametrizeable")
    q = otp.Query(parametrizeable_query, params={"X": "AAAAAA"})
    for d in [q, q.copy(), q.deepcopy()]:
        assert isinstance(d, otp.Query)


def test_query_symbol_param(session, cur_dir):
    query_path = cur_dir + "otqs" + "query_with_param.otq::query"

    with pytest.raises(ValueError, match="symbol parameter"):
        otp.Query(otp.query(query_path), symbol="DB::SomeSymbol")

    otp.Query(otp.query(query_path), symbol=None)
    otp.Query(otp.query(query_path), symbol=otp.adaptive)
