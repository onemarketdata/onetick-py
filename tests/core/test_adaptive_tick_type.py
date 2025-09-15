import pytest
import onetick.py as otp


@pytest.fixture()
def adaptive_tick_type_query(cur_dir):
    """Query containing single Passthrough EP with defined TRD tick type only.
    """
    return otp.query(str(cur_dir + "otqs" + "adaptive_tick_type.otq") + "::query")


def test_none_tick_type(m_session, adaptive_tick_type_query):
    """Ensure it is possible to generate ticks without tick type using None as value.
    """
    s = otp.Ticks({"PRICE": [1, 2, 3, 4]}, tick_type=None)
    res = adaptive_tick_type_query(IN=s)["OUT"]

    assert (otp.run(res, symbols=["DEMO_L1::AAPL"]).PRICE == [1, 2, 3, 4]).all()


def test_none_tick_type_without_symbols(m_session, adaptive_tick_type_query):
    """Ensure it is possible to generate ticks without tick type using None as value.
    """
    s = otp.Ticks({"PRICE": [1, 2, 3, 4]}, tick_type=None)
    res = adaptive_tick_type_query(IN=s)["OUT"]

    error_text = "no symbols were specified, neither bound nor non-bound"
    # `Exception` is an exact exception type raised from onetick.query.query:run_numpy
    with pytest.raises(Exception, match=error_text):
        assert (otp.run(res).PRICE == [1, 2, 3, 4]).all()


def test_backward_compatible_default_behavior(m_session, adaptive_tick_type_query):
    """Ensure exception is raised with the default tick_type value of `utils.adaptive`.
    """
    s = otp.Ticks({"PRICE": [1, 2, 3, 4], })
    res = adaptive_tick_type_query(IN=s)["OUT"]
    error_text = "Tick types bound higher in the graph are overridden in the lower sections in the graph"
    # `Exception` is an exact exception type raised from onetick.query.query:run_numpy
    with pytest.raises(Exception, match=error_text):
        assert (otp.run(res).PRICE == [1, 2, 3, 4]).all()
