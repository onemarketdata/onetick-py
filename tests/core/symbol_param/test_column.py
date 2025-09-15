import onetick.py.types as ott

from onetick.py.core.column import _Column as Column
from onetick.py.core._source._symbol_param import _SymbolParamSource, _SymbolParamColumn


class MockSymbolParamsSource(_SymbolParamSource):
    pass


ref_source = MockSymbolParamsSource()


def sym_column(name, dtype=float, **kwargs):
    global ref_source
    return _SymbolParamColumn(name, obj_ref=ref_source, dtype=dtype, **kwargs)


def test_types():
    assert str(sym_column("x")) == "atof(_SYMBOL_PARAM.x)"
    assert sym_column("x").dtype is float

    assert str(sym_column("x", int)) == "atol(_SYMBOL_PARAM.x)"
    assert sym_column("x", int).dtype is int

    assert str(sym_column("x", str)) == "_SYMBOL_PARAM.x"
    assert sym_column("y", str).dtype is str

    assert str(sym_column("x", ott.msectime)) == "GET_MSECS(MSEC_STR_TO_NSECTIME(_SYMBOL_PARAM.x))"
    assert sym_column("x", ott.msectime).dtype is ott.msectime

    assert str(sym_column("x", ott.nsectime)) == "MSEC_STR_TO_NSECTIME(_SYMBOL_PARAM.x)"
    assert sym_column("x", ott.nsectime).dtype is ott.nsectime


def test_add():
    assert str(Column("x") + sym_column("y")) == "(x) + (atof(_SYMBOL_PARAM.y))"
    assert str(sym_column("x") + Column("y")) == "(atof(_SYMBOL_PARAM.x)) + (y)"
    assert str(sym_column("x") + 1) == "(atof(_SYMBOL_PARAM.x)) + (1)"
    assert str(1 + sym_column("x") + 2) == "((1) + (atof(_SYMBOL_PARAM.x))) + (2)"
    assert str(sym_column("x") + 0.5) == "(atof(_SYMBOL_PARAM.x)) + (0.5)"


def test_mul():
    assert str(-sym_column("x") * 3) == "((-(atof(_SYMBOL_PARAM.x)))) * (3)"
    assert str(4 + sym_column("x") * 0.5) == "(4) + ((atof(_SYMBOL_PARAM.x)) * (0.5))"
