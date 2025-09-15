class _ManuallyBoundValue:
    def __init__(self, value):
        if isinstance(value, _ManuallyBoundValue):
            self.value = value.value
        else:
            self.value = value
