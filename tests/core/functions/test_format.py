import pytest

import onetick.py as otp


@pytest.mark.parametrize("value", [1, True, 'hi', 3.14])
def test_primitive(session, value):
    data = otp.Tick(A=1)
    data['B'] = otp.format('{} is primitive', value)
    data['C'] = otp.format('Hi, {}', value)
    data['D'] = otp.format('Hi, {} which is primitive', value)
    df = otp.run(data)
    assert all(df['B'] == [f'{value} is primitive'])
    assert all(df['C'] == [f'Hi, {value}'])
    assert all(df['D'] == [f'Hi, {value} which is primitive'])


@pytest.mark.parametrize("values", [
    [1, 2],
    [True, False],
    ['abc', 'def'],
    [3.14, 2.71],
    [otp.dt(2021, 3, 4, 15, 17, 38, 157), otp.dt(2022, 5, 14, 5, 55, 18)]])
def test_column(session, values):
    data = otp.Ticks(A=values)
    if values[0] is True:
        values = [1.0, 0.0]
    if isinstance(values[0], otp.dt):
        values = ['2021-03-04 15:17:38.000157000', '2022-05-14 05:55:18.000000000']
    data['B'] = otp.format('{} is primitive', data['A'])
    data['C'] = otp.format('Hi, {}', data['A'])
    data['D'] = otp.format('Hi, {} which is primitive', data['A'])
    df = otp.run(data)
    assert all(df['B'] == [f'{value} is primitive' for value in values])
    assert all(df['C'] == [f'Hi, {value}' for value in values])
    assert all(df['D'] == [f'Hi, {value} which is primitive' for value in values])


@pytest.mark.parametrize("format_line", ['{{}', '{}}', '{{}}'])
def test_fail(session, format_line):
    with pytest.raises(ValueError):
        otp.format(format_line, 1)


@pytest.mark.parametrize("format_spec", ['>1', '0.a', 'asd!q'])
def test_format_spec_error(session, format_spec):
    data = otp.Tick(A='name')
    with pytest.raises(ValueError):
        otp.format('hi, {{{}}}, bye'.format(format_spec), data['A'])


def test_operations(session):
    data = otp.Tick(
        INT_COLUMN=1,
        STR_COLUMN='abc',
        FLOAT_COLUMN=3.14,
        DATETIME_COLUMN=otp.dt(2021, 3, 4, 15, 17, 38),
    )
    format_line = 'this is {} operation'
    data['INT_OP'] = otp.format(format_line, data['INT_COLUMN'] + 5)
    data['STR_OP'] = otp.format(format_line, data['STR_COLUMN'].str.token('b'))
    data['FLOAT_OP'] = otp.format(format_line, data['FLOAT_COLUMN'] / 2)
    data['DATETIME_OP'] = otp.format(format_line, data['DATETIME_COLUMN'] + otp.Day(1))
    df = otp.run(data)
    assert all(df['INT_OP'] == [format_line.format(6)])
    assert all(df['STR_OP'] == [format_line.format('a')])
    assert all(df['FLOAT_OP'] == [format_line.format(1.57)])
    assert all(df['DATETIME_OP'] == [format_line.format('2021-03-05 15:17:38.000000000')])


def test_some_args(session):
    data = otp.Tick(A=1, B=3.14, C='hi')
    format_line = 'A is {}, B is {}, C is {}'
    data['D'] = otp.format(format_line, data['A'], data['B'], data['C'])
    df = otp.run(data)
    assert all(df['D'] == [format_line.format(1, 3.14, 'hi')])


def test_column_and_primitive(session):
    data = otp.Tick(A=1)
    format_line = 'A is {}, A is not {}'
    data['D'] = otp.format(format_line, data['A'], 4)
    df = otp.run(data)
    assert all(df['D'] == [format_line.format(1, 4)])


@pytest.mark.parametrize("num_array", [[0, 1, 2], [1, 2, 0], [0, 0, 1]])
def test_positional_args(session, num_array):
    data = otp.Tick(A='a', B='b', C='c')
    format_line = 'A is {{{}}}, B is {{{}}}, C is {{{}}}'.format(*num_array)
    data['D'] = otp.format(format_line, data['A'], data['B'], data['C'])
    df = otp.run(data)
    assert all(df['D'] == [format_line.format('a', 'b', 'c')])


@pytest.mark.parametrize("key_word_array", [['a', 'b', 'c'], ['b', 'c', 'a'], ['a', 'a', 'b']])
def test_key_word_args(session, key_word_array):
    data = otp.Tick(A='aaaaa', B='bbbb', C='cccc')
    format_line = 'A is {{{}}}, B is {{{}}}, C is {{{}}}'.format(*key_word_array)
    data['D'] = otp.format(format_line, a=data['A'], b=data['B'], c=data['C'])
    df = otp.run(data)
    assert all(df['D'] == [format_line.format(a='aaaaa', b='bbbb', c='cccc')])


@pytest.mark.parametrize("format_line", [
    'A is {}, B is {1}',
    'A is {0}, B is {b}',
    'A is {}, C is {c}',
    'A is {}, B is {1}, C is {c}',
])
def test_fail_mixed_format_types(session, format_line):
    data = otp.Tick(A='aaaaa', B='bbbb', C='cccc')
    with pytest.raises(ValueError):
        otp.format(format_line, data['A'], data['B'], c=data['C'])


@pytest.mark.parametrize("precision", [0, 1, 2, 10])
def test_precision(session, precision):
    value = 34.65879867123
    data = otp.Tick(FLOAT_FIELD=float(value), DECIMAL_FIELD=otp.decimal(value))
    data['RES'] = otp.format(f'Float field is about {{:.{precision}f}} and decimal field is about '
                             f'{{:.{precision}f}}. Also float is about {{:.{precision}f}} and decimal is about '
                             f'{{:.{precision}f}}',
                             data['FLOAT_FIELD'], data['DECIMAL_FIELD'], value, otp.decimal(value))
    df = otp.run(data)
    round_value = 35 if precision == 0 else round(value, precision)
    assert all(df['RES'] == [f'Float field is about {round_value} and decimal field is about {round_value}. '
                             f'Also float is about {round_value} and decimal is about {round_value}'])


@pytest.mark.parametrize("precision", [0, 1, 2, 10])
def test_precision_positional(session, precision):
    value = 34.65879867123
    data = otp.Tick(FLOAT_FIELD=float(value), DECIMAL_FIELD=otp.decimal(value))
    data['RES'] = otp.format(f'Float field is about {{0:.{precision}f}} and decimal field is about '
                             f'{{1:.{precision}f}}. Also float is about {{2:.{precision}f}} and decimal is about '
                             f'{{3:.{precision}f}}',
                             data['FLOAT_FIELD'], data['DECIMAL_FIELD'], value, otp.decimal(value))
    df = otp.run(data)
    round_value = 35 if precision == 0 else round(value, precision)
    assert all(df['RES'] == [f'Float field is about {round_value} and decimal field is about {round_value}. '
                             f'Also float is about {round_value} and decimal is about {round_value}'])


@pytest.mark.parametrize("precision", [0, 1, 2, 10])
def test_precision_key_word(session, precision):
    value = 34.65879867123
    data = otp.Tick(FLOAT_FIELD=float(value), DECIMAL_FIELD=otp.decimal(value))
    data['RES'] = otp.format(f'Float field is about {{ff:.{precision}f}} and decimal field is about '
                             f'{{df:.{precision}f}}. Also float is about {{f:.{precision}f}} and decimal is about '
                             f'{{d:.{precision}f}}',
                             ff=data['FLOAT_FIELD'], df=data['DECIMAL_FIELD'], f=value, d=otp.decimal(value))
    df = otp.run(data)
    round_value = 35 if precision == 0 else round(value, precision)
    assert all(df['RES'] == [f'Float field is about {round_value} and decimal field is about {round_value}. '
                             f'Also float is about {round_value} and decimal is about {round_value}'])


def test_datetime(session):
    base_timestamp = 1695989381
    nsectime = otp.nsectime(base_timestamp * 10**9 + 123456789)
    msectime = otp.msectime(base_timestamp * 10**3 + 321)
    data = otp.Tick(NSECTIME_FIELD=nsectime, MSECTIME_FIELD=msectime)
    date_formate = '%Y/%m/%d and time is %H:%M:%S.%J'
    data['RES'] = otp.format(f'Nsectime field is `{{:{date_formate}}}`, msectime field is `{{:{date_formate}}}`',
                             data['NSECTIME_FIELD'], data['MSECTIME_FIELD'])
    df = otp.run(data)
    assert all(df['RES'] == ['Nsectime field is `2023/09/29 and time is 08:09:41.123456789`, msectime field is '
                             '`2023/09/29 and time is 08:09:41.321000000`'])


def test_datetime_positional(session):
    base_timestamp = 1695989381
    nsectime = otp.nsectime(base_timestamp * 10**9 + 123456789)
    msectime = otp.msectime(base_timestamp * 10**3 + 321)
    data = otp.Tick(NSECTIME_FIELD=nsectime, MSECTIME_FIELD=msectime)
    date_formate = '%Y/%m/%d and time is %H:%M:%S.%J'
    data['RES'] = otp.format(f'Nsectime field is `{{0:{date_formate}}}`, msectime field is `{{1:{date_formate}}}`',
                             data['NSECTIME_FIELD'], data['MSECTIME_FIELD'])
    df = otp.run(data)
    assert all(df['RES'] == ['Nsectime field is `2023/09/29 and time is 08:09:41.123456789`, msectime field is '
                             '`2023/09/29 and time is 08:09:41.321000000`'])


def test_datetime_key_word(session):
    base_timestamp = 1695989381
    nsectime = otp.nsectime(base_timestamp * 10**9 + 123456789)
    msectime = otp.msectime(base_timestamp * 10**3 + 321)
    data = otp.Tick(NSECTIME_FIELD=nsectime, MSECTIME_FIELD=msectime)
    date_formate = '%Y/%m/%d and time is %H:%M:%S.%J'
    data['RES'] = otp.format(f'Nsectime field is `{{nf:{date_formate}}}`, msectime field is `{{mf:{date_formate}}}`',
                             nf=data['NSECTIME_FIELD'], mf=data['MSECTIME_FIELD'])
    df = otp.run(data)
    assert all(df['RES'] == ['Nsectime field is `2023/09/29 and time is 08:09:41.123456789`, msectime field is '
                             '`2023/09/29 and time is 08:09:41.321000000`'])


def test_native_format_exception():
    data = otp.Ticks(A=[1, 2, 3], B=[3, 2, 1])

    with pytest.warns(FutureWarning):
        data['C'] = f"{data['A']} is A"

    with pytest.warns(FutureWarning):
        data['C'] = "{} is A".format(data['A'])

    with pytest.warns(FutureWarning):
        data['C'] = f"{data['A'] + data['B']} is A + B"

    with pytest.warns(FutureWarning):
        data['C'] = "{} is A + B".format(data['A'] + data['B'])
