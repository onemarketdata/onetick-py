import re
import pytest

from onetick.py.otq import otq
import onetick.py as otp


@pytest.fixture(scope="module", autouse=True)
def session():
    with otp.Session() as s:
        yield s


class TestApply:

    @pytest.mark.parametrize(
        "logic,res",
        [
            (otq.Passthrough(), [1, 2, 3]),
            (otq.UpdateField(field="X", value="X*X"), [1, 4, 9]),
            (otq.Passthrough() >> otq.UpdateField(field="X", value="X*X"), [1, 4, 9]),
            (otq.UpdateField(field="X", value="X*X") >> otq.Passthrough(), [1, 4, 9]),
            (otq.WhereClause(where="X<3")["IF"] >> otq.Passthrough(), [1, 2]),
            (otq.WhereClause(where="X<3")["ELSE"] >> otq.Passthrough(), [3]),
        ],
    )
    def test_query_with_single_input_and_output(self, logic, res):
        data = otp.Ticks(dict(X=[1, 2, 3]))

        data = data.apply(otq.GraphQuery(logic))

        df = otp.run(data)

        assert all(df["X"] == res)

    def test_multiple_inputs(self):
        data = otp.Ticks(dict(X=[1, 2, 3]))

        graph = otq.Passthrough() >> otq.Merge() << otq.Passthrough()

        with pytest.raises(Exception, match="have one input"):
            data = data.apply(otq.GraphQuery(graph))

    def test_multiple_outputs(self):
        data = otp.Ticks(dict(X=[1, 2, 3]))

        wc = otq.WhereClause(where="X<3")
        wc["IF"] >> otq.UpdateField()
        else_branch = wc["ELSE"] >> otq.Passthrough()

        with pytest.raises(Exception, match="have one output"):
            data.apply(otq.GraphQuery(else_branch))


def test_ep_repr():
    data = otp.Tick(A=1)
    data.sink(otq.AddField(field='B', value=2))
    ep = data.node().get()
    if otp.compatibility.is_event_processor_repr_upper():
        regex = r"^ADD_FIELD\(FIELD=([\'\"])B([\'\"]),VALUE=2\)$"
    else:
        regex = r"^AddField\(field=([\'\"])B([\'\"]),value=2\)$"

    match = re.match(regex, repr(ep))
    assert match, f'Regex {regex} is not matched with string {repr(ep)} (webapi: {otq.webapi})'
    # check quotes equality
    assert match.group(1) == match.group(2)
