import os
import pytest

import onetick.py as otp


DIR = os.path.dirname(os.path.abspath(__file__))

skip = pytest.mark.skip
query = otp.query


@pytest.fixture
def otqs(cur_dir):
    return cur_dir + "otqs"


def test_two_inputs_1(session):
    t1 = otp.Tick(x=3)
    t2 = otp.Tick(x=7, offset=1)

    m = otp.apply_query(
        query(os.path.join(DIR, "otqs", "merge.otq") + "::merge", query.config(output_columns="input")),
        {"IN1": t1, "IN2": t2},
    )

    df = otp.run(m)
    assert list(df.x) == [3, 7]

    m.x = m.x * 2

    df = otp.run(m)
    assert list(df.x) == [6, 14]


def test_two_inputs_2(session):
    t1 = otp.Tick(x=3)
    t2 = otp.Tick(x=7, offset=1)

    m = otp.apply_query(
        query(os.path.join(DIR, "otqs", "merge.otq") + "::merge", query.config(output_columns="input")),
        {"IN1": t2, "IN2": t1},
    )

    df = otp.run(m)
    assert list(df.x) == [3, 7]

    m.x = m.x * 2

    df = otp.run(m)
    assert list(df.x) == [6, 14]


def test_two_inputs_3(session):
    t1 = otp.Tick(x=3)
    t2 = otp.Tick(x=7, offset=1)
    t3 = otp.Tick(x=4, offset=2)

    m = otp.apply_query(
        query(os.path.join(DIR, "otqs", "merge.otq") + "::merge", query.config(output_columns="input")),
        {"IN1": t2, "IN2": t1},
    )

    m2 = otp.apply_query(
        query(os.path.join(DIR, "otqs", "merge.otq") + "::merge", query.config(output_columns="input")),
        {"IN1": m, "IN2": t3},
    )

    df = otp.run(m2)
    assert list(df.x) == [3, 7, 4]

    m2.y = 1
    m2.x = 0

    df = otp.run(m2)
    assert list(df.x) == [0, 0, 0]
    assert list(df.y) == [1, 1, 1]

    df = otp.run(m)
    assert list(df.x) == [3, 7]


def test_two_outputs_one_input_1(session):
    m = otp.Ticks({"x": [0, 1, 2, 3, 4]})

    assert len(otp.run(m)) == 5

    left, right = otp.apply_query(
        os.path.join(DIR, "otqs", "where_clause.otq") + "::condition",
        {"IN": m},
        ["IF_OUT", "ELSE_OUT"],
        keep_columns=True,
        CONDITION="x < 3",
    )

    assert list(otp.run(left).x) == [0, 1, 2]
    assert list(otp.run(right).x) == [3, 4]

    m2 = left + right

    assert len(otp.run(m2)) == 5


def test_two_outputs_one_input_2(session):
    m = otp.Ticks({"x": [0, 1, 2, 3, 4]})

    assert len(otp.run(m)) == 5

    left, right = otp.apply_query(
        os.path.join(DIR, "otqs", "where_clause.otq") + "::condition",
        {"IN": m},
        ["IF_OUT", "ELSE_OUT"],
        CONDITION="x < 3",
    )

    assert list(otp.run(left).x) == [0, 1, 2]
    assert list(otp.run(right).x) == [3, 4]

    m2 = otp.apply_query(os.path.join(DIR, "otqs", "merge.otq") + "::merge", {"IN1": left, "IN2": right})

    assert len(otp.run(m2)) == 5


def test_single_pins_1(session):
    # apply query that has only one input pin and only one output pin - IN and OUT
    # accordingly
    t1 = otp.Tick(x=3)
    t2 = otp.Tick(x=7, offset=1)
    t3 = otp.Tick(x=4, offset=2)

    m = otp.merge([t1, t2, t3])

    m2 = m.apply(otp.query(os.path.join(DIR, "otqs", "update1.otq") + "::update"))

    df = otp.run(m2)
    assert list(df.x) == [6, 14, 8]


@pytest.mark.xfail
def test_single_pins_2(session):
    # apply two nested queries in raw
    t1 = otp.Tick(x=3)
    t2 = otp.Tick(x=7, offset=1)
    t3 = otp.Tick(x=4, offset=2)

    m = otp.merge([t1, t2, t3])

    m2 = m.apply(otp.query(os.path.join(DIR, "otqs", "update1.otq") + "::update"))

    df = otp.run(m2)
    assert list(df.x) == [12, 28, 16]


def test_single_pins_3(session):
    # apply query that has only one input pin and only one output pin - OLOLO and BLABLA
    # accordingly
    t1 = otp.Tick(x=3)
    t2 = otp.Tick(x=7, offset=1)
    t3 = otp.Tick(x=4, offset=2)

    m = otp.merge([t1, t2, t3])

    m2 = m.apply(otp.query(os.path.join(DIR, "otqs", "update2.otq") + "::update"))

    df = otp.run(m2)
    assert list(df.x) == [6, 14, 8]


def test_params_query(session):
    t = otp.Tick(x=0)

    m = otp.apply_query(
        query(os.path.join(DIR, "otqs", "query_with_params.otq") + "::query"),
        {"IN": t},
        PARAM1="param1",
        PARAM2="param2",
    )
    df = otp.run(m)
    assert len(df) == 1
    assert df.PARAM1_FIELD[0] == "param1"
    assert df.PARAM2_FIELD[0] == "param2"


def test_query_with_initial_params(session):
    t = otp.Tick(x=0)
    q = query(os.path.join(DIR, "otqs", "query_with_params.otq") + "::query", PARAM1="param1", PARAM2="param2")
    m = otp.apply_query(q, {"IN": t})
    df = otp.run(m)
    assert len(df) == 1
    assert df.PARAM1_FIELD[0] == "param1"
    assert df.PARAM2_FIELD[0] == "param2"


def test_query_with_initial_params_and_updated_parameter(session):
    t = otp.Tick(x=0)
    q = query(os.path.join(DIR, "otqs", "query_with_params.otq") + "::query", PARAM1="param1", PARAM2="param2")

    m = otp.apply_query(q, {"IN": t}, PARAM2="param2_updated")

    df = otp.run(m)
    assert len(df) == 1
    assert df.PARAM1_FIELD[0] == "param1"
    assert df.PARAM2_FIELD[0] == "param2_updated"


class TestQueryWithoutInputPin:

    def test_source(self, session):
        """
        Check that we can use query without input pins as as _source
        """
        q = query(os.path.join(DIR, "otqs", "no_in_pin.otq") + "::no_in_pin")

        data = otp.Query(q)

        otp.run(data, symbols="DEMO_L1::DUMMY")

    def test_apply(self, session):
        """
        Check that it is impossible to pass a query without input to
        the apply
        """
        q = query(os.path.join(DIR, "otqs", "no_in_pin.otq") + "::no_in_pin")

        data = otp.Ticks(dict(X=[1, 2, 3, 4]))

        with pytest.raises(Exception, match="to have one input"):
            data.apply(q)

    def test_call(self, session):
        """
        Check that it is impossible to call a query without input
        """
        q = query(os.path.join(DIR, "otqs", "no_in_pin.otq") + "::no_in_pin")

        data = otp.Ticks(dict(X=[1, 2, 3, 4]))

        with pytest.raises(Exception, match="to have one input"):
            otp.run(q(data)["OUT"])


class TestQueryWithoutOutputPin:
    def test_source_without_in(self, session):
        """
        The no_out_pin does not have output pins, check that we could
        use it as a _source
        """
        q = query(os.path.join(DIR, "otqs", "no_out_pin.otq") + "::no_out_pin_without_in")

        data = otp.Query(q)

        otp.run(data, symbols="DEMO_L1::DUMMY")

    def test_apply(self, session):
        """
        Check that we can use query without outputs in the apply
        """
        q = query(os.path.join(DIR, "otqs", "no_out_pin.otq") + "::no_out_pin_without_TT")

        data = otp.Ticks(dict(X=[1, 2, 3, 4]))

        res = data.apply(q)

        otp.run(res)

    def test_call(self, session):
        """
        Check that we can call query without output pins
        """
        q = query(os.path.join(DIR, "otqs", "no_out_pin.otq") + "::no_out_pin_without_TT")

        data = otp.Ticks(dict(X=[1, 2, 3, 4]))

        otp.run(q(data)[None])  # no output


class TestQueryWithMultipleOutputs:
    def test_source_not_specified(self, session):
        q = query(os.path.join(DIR, "otqs", "where_clause.otq") + "::condition_source", CONDITION="1")

        data = otp.Query(q)

        with pytest.raises(Exception, match="not specified which one should be used"):
            otp.run(data, symbols="DEMO_L1::DUMMY")

    def test_source_specified(self, session):
        q = query(os.path.join(DIR, "otqs", "where_clause.otq") + "::condition_source", CONDITION="1")

        data_if = otp.Query(q, out_pin="IF_OUT")
        data_else = otp.Query(q, out_pin="ELSE_OUT")

        data = data_if + data_else

        assert len(otp.run(data_if, symbols="DEMO_L1::DUMMY")) == 1
        assert len(otp.run(data, symbols="DEMO_L1::DUMMY")) == 1

    def test_source_specified_not_matching(self, session):
        q = query(os.path.join(DIR, "otqs", "where_clause.otq") + "::condition_source", CONDITION="1")

        data_if = otp.Query(q, out_pin="IF_OUT_SOMETHING")

        with pytest.raises(Exception, match='does not have the "IF_OUT_SOMETHING" output'):
            otp.run(data_if, symbols="DEMO_L1::DUMMY")

    def test_apply(self, session):
        q = query(os.path.join(DIR, "otqs", "where_clause.otq") + "::condition", CONDITION="1")

        data = otp.Ticks(dict(X=[1, 2, 3, 4]))

        with pytest.raises(Exception, match="to have one or no output"):
            data.apply(q)

    def test_call(self, session):
        q = query(os.path.join(DIR, "otqs", "where_clause.otq") + "::condition", CONDITION="1")

        data = otp.Ticks(dict(X=[1, 2, 3, 4]))

        assert len(otp.run(q(data)["IF_OUT"])) == 4


class TestCustomPins:
    def test_source(self, session):
        """
        The custom_pin_names has only one output pin - OUT_2, and we do not need to
        specify the output pin since it is the only one
        """
        q = query(os.path.join(DIR, "otqs", "custom_pin_names.otq") + "::custom_pin_source")

        data = otp.Query(q)

        assert len(otp.run(data, symbols="DEMO_L1::DUMMY")) == 1

    def test_apply(self, session):
        q = query(os.path.join(DIR, "otqs", "custom_pin_names.otq") + "::custom_pin_names_without_TT")

        data = otp.Ticks(dict(X=[1, 2, 3, 4]))

        res = data.apply(q)
        df = otp.run(res)

        assert all(df["X"] == [1, 4, 9, 16])

    def test_call(self, session):
        q = query(os.path.join(DIR, "otqs", "custom_pin_names.otq") + "::custom_pin_names_without_TT")

        data = otp.Ticks(dict(X=[1, 2, 3, 4]))

        res = q(data)["OUT_2"]
        df = otp.run(res)

        assert all(df["X"] == [1, 4, 9, 16])


class TestWithSingleQuery:

    def test_source(self, session):
        q = query(os.path.join(DIR, "otqs", "orders.otq"), TT="SOME", DB="DEMO_L1")

        data = otp.Query(q)

        assert len(otp.run(data, symbols="DEMO_L1::DUMMY")) == 0

    def test_source_multiple(self, session):
        with pytest.raises(Exception, match="has more than one query"):
            query(os.path.join(DIR, "otqs", "combine.otq"))


def test_source_without_pins(session):
    q = query(os.path.join(DIR, "otqs", "without_pins.otq"))
    otp.Query(q)


class TestQueryWithTickType:
    def test_tick_wrong_tick_type(self, session, otqs):
        data = otp.Tick(X=3)

        q = query(otqs + "passthrough_with_tick_type.otq::query")
        res = data.apply(q)

        with pytest.raises(Exception):
            otp.run(res, symbols="DEMO_L1::AAPL")

    def test_tick_manually_set_tick_type(self, session, otqs):
        data = otp.Tick(X=3, db=None, tick_type="SOME")

        q = query(otqs + "passthrough_with_tick_type.otq::query")
        res = data.apply(q)

        df = otp.run(res, symbols="DEMO_L1::AAPL")
        assert len(df) == 1

    @pytest.mark.parametrize("values", [[1, 3], [0.1, otp.nan]])
    def test_ticks_wrong_tick_type(self, session, otqs, values):
        data = otp.Ticks(dict(X=values))

        q = query(otqs + "passthrough_with_tick_type.otq::query")
        res = data.apply(q)

        with pytest.raises(Exception):
            otp.run(res, symbols="DEMO_L1::AAPL")

    @pytest.mark.parametrize("values", [[1, 3], [0.1, otp.nan]])
    def test_ticks_wrong_tick_type_2(self, session, otqs, values):
        data = otp.Ticks(dict(X=values), db=None, tick_type="SOME")

        q = query(otqs + "passthrough_with_tick_type.otq::query")
        res = data.apply(q)

        df = otp.run(res, symbols="DEMO_L1::AAPL")

        assert len(df) == 2
