from onetick.py.core._internal._op_utils.every_operand import every_operand
from onetick.py.core._source._symbol_param import _SymbolParamColumn
from onetick.py.core.column import _Column


def is_const(operation):
    for op in every_operand(operation):
        if isinstance(op, _Column) and not isinstance(op, _SymbolParamColumn):
            return False
    return True
