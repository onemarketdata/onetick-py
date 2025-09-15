import os
import sys
from datetime import datetime

import onetick.py as otp
import pandas as pd

otp.config.default_start_time = datetime(2003, 12, 1)
otp.config.default_end_time = datetime(2003, 12, 4)
otp.config.default_db = 'LOCAL'
otp.config.default_symbol = 'X'
otp.config.tz = 'GMT'

with otp.Session() as s:
    t1 = otp.Tick(A=1)
    t2 = otp.Tick(B=1)
    t3 = otp.Ticks({'A': [2, 3]})

    t = otp.join(t1, t2, on='same_size')
    t = otp.merge([t, t3])

    assert t.schema['A'] is int
    assert t.schema['B'] is int

    def jwq(symbol, a, b):
        t = otp.Tick(A=a, B=b)
        t['C'] = (t['A'] + t['B']) / 2
        t['PARAM_INT'] = symbol.PARAM_INT
        t['PARAM_FLOAT'] = symbol.PARAM_FLOAT
        t['PARAM_STR'] = symbol.PARAM_STR
        t = t[['C', 'PARAM_INT', 'PARAM_FLOAT', 'PARAM_STR']]
        return t

    t = t.join_with_query(jwq,
                          params={'a': t['A'], 'b': t['B']},
                          symbol=('LOCAL::', {'PARAM_INT': 1,
                                              'PARAM_FLOAT': 2.2,
                                              'PARAM_STR': 'three'}))
    assert t.schema['C'] is float
    assert t.schema['PARAM_INT'] is int
    assert t.schema['PARAM_FLOAT'] is float
    assert t.schema['PARAM_STR'] is str

    t['D'] = t['TIMESTAMP'] + otp.Year(1)
    t['E'] = t['D'].dt.strftime('%Y%m%d%H%M%S.%q')
    t['F'] = t['E'].str.token(sep='.', n=0)
    t['G'] = t['F'].astype(int)

    assert t.schema['D'] is otp.nsectime
    assert t.schema['E'] is str
    assert t.schema['F'] is str
    assert t.schema['G'] is int

    t = t.sort('A')

    def fun(tick):
        tick['H'] = tick['G'] / 1_000_000
        if tick['A'] == 1:
            tick['H'] = 0
        else:
            tick['H'] += tick['A'] - 1

    t = t.script(fun)

    assert t.schema['H'] is float

    t['I'] = t.apply(
        lambda row: row['H'] if row['A'] != 1 else row['G']
    ).astype(int)

    assert t.schema['I'] is int

    t = t.agg({'J': otp.agg.sum('H')}, running=True, all_fields=True)

    assert t.schema['J'] is float

    t.state_vars['K'] = 0.0
    t.state_vars['K'] += t['H']

    t['K'] = t.state_vars['K']

    assert t.schema['K'] is float

    t.state_vars['SET'] = otp.state.tick_set('oldest', ['B'])
    t = t.state_vars['SET'].update()

    t['L'] = t.state_vars['SET'].find('K', default_value=-1.0)

    assert t.schema['L'] is float

    df = otp.run(t, symbols='LOCAL::')

    assert len(df) == 3
    assert list(df['Time']) == [otp.config.default_start_time,
                                otp.config.default_start_time,
                                otp.config.default_start_time + otp.Milli(1)]
    assert list(df['A']) == [1, 2, 3]
    assert list(df['B']) == [1, 0, 0]
    assert list(df['C']) == [1, 1, 1.5]
    assert list(df['D']) == [otp.config.default_start_time + otp.Year(1),
                             otp.config.default_start_time + otp.Year(1),
                             otp.config.default_start_time + otp.Year(1) + otp.Milli(1)]
    assert list(df['E']) == ['20041201000000.000',
                             '20041201000000.000',
                             '20041201000000.001']
    assert list(df['F']) == ['20041201000000',
                             '20041201000000',
                             '20041201000000']
    assert list(df['G']) == [20041201000000,
                             20041201000000,
                             20041201000000]
    assert list(df['H']) == [0,
                             20041202,
                             20041203]
    assert list(df['I']) == [20041201000000,
                             20041202,
                             20041203]
    assert list(df['J']) == [0,
                             20041202,
                             40082405]
    assert list(df['K']) == [0,
                             20041202,
                             40082405]
    assert list(df['L']) == [0,
                             20041202,
                             20041202]
    assert all(df['PARAM_INT'] == 1)
    assert all(df['PARAM_FLOAT'] == 2.2)
    assert all(df['PARAM_STR'] == 'three')

    # test eval and first stage query

    def fsq():
        symbols = otp.Tick(SYMBOL_NAME='LOCAL::')
        return symbols

    t_m = otp.merge([t], symbols=otp.eval(fsq))
    df = otp.run(t_m)

    assert len(df) == 3
    assert list(df['Time']) == [otp.config.default_start_time,
                                otp.config.default_start_time,
                                otp.config.default_start_time + otp.Milli(1)]
    assert list(df['A']) == [1, 2, 3]
    assert list(df['B']) == [1, 0, 0]
    assert list(df['C']) == [1, 1, 1.5]

    # test mapping output structure

    mapping = otp.run(t, symbols='LOCAL::', output_structure='map')
    mapping = mapping['LOCAL::']

    assert list(mapping['A']) == [1, 2, 3]
    assert list(mapping['B']) == [1, 0, 0]
    assert list(mapping['C']) == [1, 1, 1.5]

    # test listing output structure

    listing = otp.run(t, symbols='LOCAL::', output_structure='list')
    assert len(listing) == 1
    symbol_name, listing, _, _ = listing[0]
    assert symbol_name == 'LOCAL::'

    for column_name, values in listing:
        if column_name == 'A':
            assert list(values) == [1, 2, 3]
        elif column_name == 'B':
            assert list(values) == [1, 0, 0]
        elif column_name == 'C':
            assert list(values) == [1, 1, 1.5]
