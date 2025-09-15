def every_operand(op):
    operations = [op]
    while operations:
        operation = operations.pop()
        operands = getattr(operation, "_op_params", None)
        if operands:
            operations.extend(operands)
        else:
            yield operation
