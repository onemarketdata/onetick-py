def get_type_that_includes(types):
    import onetick.py.types as ott

    def merge_two(type1, type2):
        type_changed = False

        b_type1, b_type2 = ott.get_base_type(type1), ott.get_base_type(type2)
        if b_type1 != b_type2:
            if {b_type1, b_type2} == {int, float}:
                dtype = float
            elif {b_type1, b_type2} == {ott.decimal, float} or {b_type1, b_type2} == {ott.decimal, int}:
                dtype = ott.decimal
            elif {b_type1, b_type2} == {ott.nsectime, ott.msectime}:
                dtype = ott.nsectime
            else:
                raise ValueError(f"Incompatible types: {type1}, {type2}")

            type_changed = True
        elif issubclass(b_type1, str):
            t1_length = ott.string.DEFAULT_LENGTH if type1 is str or type1.length is None else type1.length
            t2_length = ott.string.DEFAULT_LENGTH if type2 is str or type2.length is None else type2.length

            if t1_length is Ellipsis or t2_length is Ellipsis:
                dtype = ott.varstring
            else:
                dtype = type2 if t1_length < t2_length else type1

            if t1_length != t2_length:
                type_changed = True  # TODO: test

        else:
            dtype = type1

        return dtype, type_changed

    dtype = types[0]
    type_changed = False

    for other_dtype in types[1:]:
        dtype, new_type_change = merge_two(dtype, other_dtype)
        type_changed |= new_type_change

    return dtype, type_changed


class adaptive:
    """
    This class is mostly used as the default value for the functions' parameters
    when the value of ``None`` has some other meaning
    or when the meaning of the parameter depends on the other parameter's values,
    :ref:`otp.config <static/configuration/root:configuration>` options or the context.

    Examples
    --------

    For example, setting :py:class:`~onetick.py.DataSource` ``symbols`` parameter
    to ``otp.adaptive`` allows to set symbols when running the query later.

    >>> data = otp.DataSource('SOME_DB', tick_type='TT', symbols=otp.adaptive)
    >>> otp.run(data, symbols='S1')
                         Time  X
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2
    2 2003-12-01 00:00:00.002  3

    This is the default value of ``symbols`` parameter, so omitting it also works:

    >>> data = otp.DataSource('SOME_DB', tick_type='TT')
    >>> otp.run(data, symbols='S1')
                         Time  X
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2
    2 2003-12-01 00:00:00.002  3
    """


class adaptive_to_default(adaptive):
    """
    If something is not specified and can not be deduced, then use the
    default one
    """


class default:
    """
    Used when you need to specify a default without evaluating it (e.g. default timezone)
    """


class range:
    """
    Class that expresses OneTick ranges.
    For example, if you want to express a range in the .split() method,
    then you can use this range.

    It has start and stop fields that allow you to define a range.

    See also
    --------
    :py:meth:`~onetick.py.Source.split`.

    Examples
    --------
    >>> data = otp.Ticks(X=[0.33, -5.1, otp.nan, 9.4])
    >>> r1, r2, r3 = data.split(data['X'], [otp.nan, otp.range(0, 100)], default=True)
    >>> otp.run(r1)
                         Time   X
    0 2003-12-01 00:00:00.002 NaN
    >>> otp.run(r2)
                         Time     X
    0 2003-12-01 00:00:00.000  0.33
    1 2003-12-01 00:00:00.003  9.40
    >>> otp.run(r3)
                         Time    X
    0 2003-12-01 00:00:00.001 -5.1
    """

    def __init__(self, start, stop):
        self.start = start
        self.stop = stop
