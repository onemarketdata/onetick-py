import pytest

import onetick.py as otp


def test_same_time(session):
    data1 = otp.Ticks(A=[1, 2])
    data2 = otp.Ticks(A=[3, 4])
    data = otp.coalesce([data2, data1])
    df = otp.run(data)
    assert 'A' in data.schema
    assert list(df['A']) == [3, 4]


def test_different_time(session):
    data1 = otp.Ticks({
        'A': [1, 2],
        'offset': [1000, 2000],
    })
    data2 = otp.Ticks({
        'A': [3, 4],
        'offset': [3000, 4000],
    })
    data = otp.coalesce([data2, data1])
    df = otp.run(data)
    assert 'A' in data.schema
    assert list(df['A']) == [1, 2, 3, 4]


def test_with_delay(session):
    data1 = otp.Ticks({
        'A': [1, 2, 3],
        'offset': [0, 3000, 6000],
    })
    data2 = otp.Ticks({
        'A': [4, 5, 6],
        # 4 is delayed by less than one second from 1
        # 5 is delayed by one second from 2
        # 6 is delayed by more than one second from 3
        'offset': [999, 4000, 7001],
    })
    data = otp.coalesce([data2, data1], max_source_delay=1)
    df = otp.run(data)
    print()
    print(df[['Time', 'A']])
    assert 'A' in data.schema
    assert list(df['A']) == [4, 5, 3, 6]


def test_added_fields_are_presented(session):
    data1 = otp.Ticks(A=[1, 2], tick_type='TT_1')
    data2 = otp.Ticks(A=[3, 4], tick_type='TT_2')
    data = otp.coalesce([data2, data1])
    df = otp.run(data)
    assert 'A' in data.schema
    assert 'SYMBOL_NAME' in data.schema
    assert 'TICK_TYPE' in data.schema
    assert 'SOURCE' in data.schema
    assert list(df['A']) == [3, 4]
    assert all(df['SYMBOL_NAME'] == otp.config['default_symbol'])
    assert all(df['TICK_TYPE'] == 'TT_2')
    assert all(df['SOURCE'] == '__COALESCE_SRC_1__')


def test_symbol_name_in_schema(session):
    data1 = otp.Ticks({
        'A': [1],
        'SYMBOL_NAME': ['SYMBOL_1']
    }, tick_type='TT_1')
    data2 = otp.Ticks({
        'A': [3],
        'SYMBOL_NAME': ['SYMBOL_2']
    }, tick_type='TT_2')

    with pytest.raises(ValueError):
        otp.coalesce([data2, data1])


def test_tick_type_in_schema(session):
    data1 = otp.Ticks({
        'A': [1],
        'TICK_TYPE': ['TICK_TYPE_1']
    }, tick_type='TT_1')
    data2 = otp.Ticks({
        'A': [3],
        'TICK_TYPE': ['TICK_TYPE_1']
    }, tick_type='TT_2')

    with pytest.raises(ValueError):
        otp.coalesce([data2, data1])


def test_source_in_schema(session):
    data1 = otp.Ticks({
        'A': [1],
        'SOURCE': ['SOURCE_1']
    }, tick_type='TT_1')
    data2 = otp.Ticks({
        'A': [3],
        'SOURCE': ['SOURCE_2']
    }, tick_type='TT_2')

    data = otp.coalesce([data2, data1])
    df = otp.run(data)
    assert 'SOURCE' in data.schema
    assert df['SOURCE'][0] == 'SOURCE_2'
    assert df['TICK_TYPE'][0] == 'TT_2'
    assert df['A'][0] == 3


def test_conflicting_types(session):
    data1 = otp.Ticks({
        'A': [1, 2, 3],
        'offset': [1000, 2000, 3000],
    })
    data2 = otp.Ticks({
        'A': ['I', 'AM', 'STRING'],
        'offset': [1500, 2500, 3500],
    })

    with pytest.raises(ValueError):
        otp.coalesce([data2, data1])
