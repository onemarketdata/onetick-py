import onetick.py as otp
from onetick.py.otq import otq

import pytest
import pandas as pd


@pytest.mark.parametrize('run_mode', ['run', 'call', 'file'])
def test_df_as_symbols(session, run_mode):
    source_1 = otp.Ticks(SYMBOL_NAME=['A', 'B'], PARAM_1=[1, 2], PARAM_2=['C', 'D'])
    df = otp.run(source_1)

    source_2 = otp.Tick(DUMMY=1)
    source_2.sink(otq.AddFieldsFromSymbolParams())
    if run_mode == 'run':
        res_dict = otp.run(source_2, symbols=df)
    elif run_mode == 'call':
        res_dict = otp.run(source_2, symbols=df)
    elif run_mode == 'file':
        res_dict = otp.run(source_2.to_otq(symbols=df))
    else:
        assert False, f'Unknown test run mode: {run_mode}'

    assert len(res_dict) == 2
    assert 'A' in res_dict.keys()
    assert len(res_dict['A']) == 1
    assert res_dict['A']['PARAM_1'][0] == '1'
    assert res_dict['A']['PARAM_2'][0] == 'C'
    assert 'B' in res_dict.keys()
    assert len(res_dict['B']) == 1
    assert res_dict['B']['PARAM_1'][0] == '2'
    assert res_dict['B']['PARAM_2'][0] == 'D'


@pytest.mark.parametrize('run_mode', ['run', 'call', 'file'])
def test_df_as_symbols_no_symbol_name(session, run_mode):
    source_1 = otp.Ticks(NOT_SYMBOL_NAME=['A', 'B'], PARAM_1=[1, 2], PARAM_2=['C', 'D'])
    df = otp.run(source_1)

    source_2 = otp.Tick(DUMMY=1)
    source_2.sink(otq.AddFieldsFromSymbolParams())
    with pytest.raises(ValueError):
        if run_mode == 'run':
            otp.run(source_2, symbols=df)
        elif run_mode == 'call':
            otp.run(source_2, symbols=df)
        elif run_mode == 'file':
            otp.run(source_2.to_otq(symbols=df))


def test_run_with_dataframe_symbols(session):
    # PY-1489
    t = otp.Tick(X=1)
    t['SYMBOL_PARAMS'] = otp.raw('GET_SYMBOL_PARAMETERS()', otp.varstring)
    df = pd.DataFrame({'SYMBOL_NAME': ['A', 'B'],
                       'SYMBOL_PARAM': ['PARAM_A', 'PARAM_B'],
                       'TIME': [pd.Timestamp(2022, 1, 1), pd.Timestamp(2022, 1, 2)]})
    result = otp.run(t, symbols=df)
    assert list(result['A']['X']) == [1]
    assert list(result['A']['SYMBOL_PARAMS']) == ["SYMBOL_PARAM='PARAM_A',TIME='2022-01-01 00:00:00'"]
    assert list(result['B']['X']) == [1]
    assert list(result['B']['SYMBOL_PARAMS']) == ["SYMBOL_PARAM='PARAM_B',TIME='2022-01-02 00:00:00'"]
