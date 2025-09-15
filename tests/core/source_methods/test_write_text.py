import os
from pathlib import Path

import pytest

import onetick.py as otp

if os.getenv('OTP_WEBAPI_TEST_MODE'):
    pytest.skip(allow_module_level=True, reason='TextWrite EP is not supported in WebAPI')

if os.name == 'nt':
    pytest.skip(allow_module_level=True, reason='Works unstable on Windows test machine for some reason')


def join_lines(*lines):
    return os.linesep.join(lines) + os.linesep


def test_default(session, capfd):
    data = otp.Ticks(A=[1, 2, 3])
    data = data.write_text()
    df = otp.run(data)
    out, _ = capfd.readouterr()
    assert out == join_lines(
        '#SYMBOL_NAME,TIMESTAMP,A',
        'AAPL,20031201050000.000000,1',
        'AAPL,20031201050000.001000,2',
        'AAPL,20031201050000.002000,3',
    )
    assert list(df['A']) == [1, 2, 3]


def test_propagate_ticks(session, capfd):
    data = otp.Ticks(A=[1, 2, 3])
    data_default = data.write_text()
    data_true = data.write_text(propagate_ticks=True)
    data_false = data.write_text(propagate_ticks=False)
    assert data_default.schema == {'A': int}
    assert data_true.schema == {'A': int}
    assert data_false.schema == {}
    df_default = otp.run(data_default)
    df_true = otp.run(data_true)
    df_false = otp.run(data_false)
    assert list(df_default['A']) == [1, 2, 3]
    assert list(df_true['A']) == [1, 2, 3]
    assert df_false.empty


def test_output_headers(session, capfd):
    data = otp.Ticks(A=[1, 2, 3])
    data = data.write_text(output_headers=False)
    otp.run(data)
    out, _ = capfd.readouterr()
    assert out == join_lines(
        'AAPL,20031201050000.000000,1',
        'AAPL,20031201050000.001000,2',
        'AAPL,20031201050000.002000,3',
    )


def test_output_types_in_headers(session, capfd):
    data = otp.Ticks(A=[1, 2, 3])
    with pytest.raises(
        ValueError, match="Parameter 'output_types_in_headers' can only be set together with 'output_headers'"
    ):
        data.write_text(output_headers=False, output_types_in_headers=True)
    data = data.write_text(output_headers=True, output_types_in_headers=True)
    otp.run(data)
    out, _ = capfd.readouterr()
    assert out == join_lines(
        '#string[4] SYMBOL_NAME,string[21] TIMESTAMP,long A',
        'AAPL,20031201050000.000000,1',
        'AAPL,20031201050000.001000,2',
        'AAPL,20031201050000.002000,3',
    )


def test_order(session, capfd):
    data = otp.Ticks(A=[1, 2, 3])
    with pytest.raises(
        ValueError,
        match="Field 'SYMBOL_NAME' can't be specified in 'order' parameter if 'prepend_symbol_name' is not set",
    ):
        data.write_text(order=['A', 'TIMESTAMP', 'SYMBOL_NAME'], prepend_symbol_name=False)
    with pytest.raises(
        ValueError, match="Field 'TIMESTAMP' can't be specified in 'order' parameter if 'prepend_timestamp' is not set"
    ):
        data.write_text(order=['A', 'TIMESTAMP', 'SYMBOL_NAME'], prepend_timestamp=False)
    data = data.write_text(order=['A', 'TIMESTAMP', 'SYMBOL_NAME'])
    otp.run(data)
    out, _ = capfd.readouterr()
    assert out == join_lines(
        '#A,TIMESTAMP,SYMBOL_NAME',
        '1,20031201050000.000000,AAPL',
        '2,20031201050000.001000,AAPL',
        '3,20031201050000.002000,AAPL',
    )


def test_prepend_symbol_name(session, capfd):
    data = otp.Ticks(A=[1, 2, 3])
    data = data.write_text(prepend_symbol_name=False)
    otp.run(data)
    out, _ = capfd.readouterr()
    assert out == join_lines(
        '#TIMESTAMP,A',
        '20031201050000.000000,1',
        '20031201050000.001000,2',
        '20031201050000.002000,3',
    )


def test_prepended_symbol_name_size(session, capfd):
    data = otp.Ticks(A=[1, 2, 3])
    data = data.write_text(prepend_symbol_name=True, prepended_symbol_name_size=2, output_types_in_headers=True)
    otp.run(data)
    out, _ = capfd.readouterr()
    assert out == join_lines(
        '#string[2] SYMBOL_NAME,string[21] TIMESTAMP,long A',
        'AA,20031201050000.000000,1',
        'AA,20031201050000.001000,2',
        'AA,20031201050000.002000,3',
    )


def test_prepend_timestamp(session, capfd):
    data = otp.Ticks(A=[1, 2, 3])
    data = data.write_text(prepend_timestamp=False)
    otp.run(data)
    out, _ = capfd.readouterr()
    assert out == join_lines(
        '#SYMBOL_NAME,A',
        'AAPL,1',
        'AAPL,2',
        'AAPL,3',
    )


@pytest.mark.parametrize(
    'separator',
    [
        ',',
        '|',
        ' ',
        '\t',
        r'\t',
        '\\',
        r'\\',
        '"',
        "'",
        '<SEPARATOR>',
        '\x09',
        r'\x09',
        '\x0d\x0a' if os.name == 'nt' else '\x0a',
        r'\x0A',
        os.linesep,
        r'\n',
    ],
)
def test_separator(session, capfd, separator):
    data = otp.Ticks(A=[1, 2, 3])
    data = data.write_text(separator=separator)
    otp.run(data)
    out, _ = capfd.readouterr()
    assert out == join_lines(
        '#SYMBOL_NAME{sep}TIMESTAMP{sep}A',
        'AAPL{sep}20031201050000.000000{sep}1',
        'AAPL{sep}20031201050000.001000{sep}2',
        'AAPL{sep}20031201050000.002000{sep}3',
    ).format(sep=separator)


def test_formats_of_fields(session, capfd):
    data = otp.Ticks(A=[1, 2, 3], B=[1.1, 2.2, 3.3], C=['Hello', 'World', '!'])
    data = data.write_text(
        formats_of_fields={
            'A': '%3d',
            'B': '%.2f',
            'C': '%5.4s',
            'TIMESTAMP': '%|EST5EDT|%Y-%m-%d %H:%M:%S.%J',
        }
    )
    otp.run(data, timezone='EST5EDT')
    out, _ = capfd.readouterr()
    assert out == join_lines(
        '#SYMBOL_NAME,TIMESTAMP,A,B,C',
        'AAPL,2003-12-01 00:00:00.000000000,  1,1.10, Hell',
        'AAPL,2003-12-01 00:00:00.001000000,  2,2.20, Worl',
        'AAPL,2003-12-01 00:00:00.002000000,  3,3.30,    !',
    )


@pytest.mark.parametrize(
    'spec,result',
    [
        ('%Y', '2022'),
        ('%m', '01'),
        ('%d', '02'),
        ('%H', '03'),
        ('%M', '04'),
        ('%S', '05'),
        ('%q', '123'),
        ('%Q', '12'),
        ('%k', '123456'),
        ('%J', '123456789'),
        ('%#', '1641092645'),
        ('%-', '1641092645123'),
        ('%U', '1641092645123456'),
        ('%N', '1641092645123456789'),
        ('%+', '11045123'),
        ('%~', '11045123456789'),
    ],
)
def test_timestamp_format_specs(session, spec, result, capfd):
    data = otp.Tick(A=otp.dt(2022, 1, 2, 3, 4, 5, 123456, 789))
    data = data.write_text(
        prepend_timestamp=False,
        prepend_symbol_name=False,
        output_headers=False,
        formats_of_fields={'A': f'%|GMT|{spec}'},
    )
    otp.run(data, timezone='GMT')
    out, _ = capfd.readouterr()
    assert out == join_lines(result)


def test_double_format(session, capfd):
    data = otp.Ticks(A=[1.1, 2.2, 3.3], B=[1.1, 2.2, 3.3])
    data = data.write_text(
        double_format='%.4f', formats_of_fields={'B': '%.1f'}, prepend_timestamp=False, prepend_symbol_name=False
    )
    otp.run(data)
    out, _ = capfd.readouterr()
    assert out == join_lines(
        '#A,B',
        '1.1000,1.1',
        '2.2000,2.2',
        '3.3000,3.3',
    )


def read_and_remove_file(path):
    path = Path(path)
    text = path.read_text()
    path.unlink()
    return text


@pytest.mark.skipif(
    not otp.compatibility.is_supported_bucket_units_for_tick_generator(), reason='not supported on old OneTick versions'
)
def test_output_files(session):
    tmp_dir = otp.utils.TmpDir()
    data = otp.Tick(A=1, bucket_interval=otp.Day(1), tick_type='TT')
    data = data.write_text(output_file='result.csv')
    data = data.write_text(output_file='result.csv', output_dir=tmp_dir)
    data = data.write_text(output_file='result_%SYMBOL%_%DBNAME%_%TICKTYPE%_%DATE%_%STARTTIME%.csv')
    otp.run(data, start=otp.dt(2003, 12, 1, 10, 20, 30), end=otp.dt(2003, 12, 4), timezone='GMT')
    for path in (Path('result.csv'), Path(tmp_dir) / Path('result.csv')):
        result = read_and_remove_file(path)
        assert result == join_lines(
            '#SYMBOL_NAME,TIMESTAMP,A',
            'AAPL,20031201102030.000000,1',
            'AAPL,20031202102030.000000,1',
            'AAPL,20031203102030.000000,1',
        )
    result = read_and_remove_file('result_AAPL__TT_20031201_102030.csv')
    assert result == join_lines(
        '#SYMBOL_NAME,TIMESTAMP,A',
        'AAPL,20031201102030.000000,1',
    )
    result = read_and_remove_file('result_AAPL__TT_20031202_102030.csv')
    assert result == join_lines(
        '#SYMBOL_NAME,TIMESTAMP,A',
        'AAPL,20031202102030.000000,1',
    )
    result = read_and_remove_file('result_AAPL__TT_20031203_102030.csv')
    assert result == join_lines(
        '#SYMBOL_NAME,TIMESTAMP,A',
        'AAPL,20031203102030.000000,1',
    )
