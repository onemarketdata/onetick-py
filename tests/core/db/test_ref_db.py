import pytest

import os
import pandas as pd

from onetick.py.otq import otq
from onetick import py as otp
from onetick.py.compatibility import is_supported_otq_reference_data_loader

if os.getenv('OTP_WEBAPI_TEST_MODE'):
    pytest.skip(allow_module_level=True,
                reason='Binaries (reference_data_loader.exe) is not available in WebAPI')


class TestRefDBFromInspection:
    def test_ref_db_tick_types_last_date_without_exception(self, f_session):
        db = otp.db.RefDB("REF_DATA_DB", db_properties={'symbology': 'CORE'})
        f_session.use(db)
        tick_types = otp.databases()["REF_DATA_DB"].tick_types()
        assert tick_types == []


@pytest.fixture(scope="function")
def tz(monkeypatch):
    tz = 'UTC'
    monkeypatch.setenv('TZ', tz)
    return tz


reason_otq_query = 'skip, because otq_query not supported by reference_data_loader.exe yet'


def ticks_symbol_name_history(tz):
    data = otp.Ticks(
        SYMBOL_NAME=['CORE_A'] * 2,
        SYMBOL_NAME_IN_HISTORY=['CORE_A', 'CORE_B'],
        SYMBOL_START_DATETIME=[otp.dt(2010, 1, 2, tz=tz)] * 2,
        SYMBOL_END_DATETIME=[otp.dt(2010, 1, 5, tz=tz)] * 2,
        START_DATETIME=[otp.dt(2010, 1, 2, tz=tz), otp.dt(2010, 1, 3, tz=tz)],
        END_DATETIME=[otp.dt(2010, 1, 3, tz=tz), otp.dt(2010, 1, 4, tz=tz)],
        offset=[0] * 2,
        db='LOCAL',
    )
    data = data.table(
        SYMBOL_NAME=otp.string[128],
        SYMBOL_NAME_IN_HISTORY=otp.string[128],
        SYMBOL_START_DATETIME=otp.msectime,
        SYMBOL_END_DATETIME=otp.msectime,
        START_DATETIME=otp.msectime,
        END_DATETIME=otp.msectime,
    )
    return data


def str_symbol_name_history():
    return 'CORE_A||20100102000000|20100103000000|CORE_B||20100103000000|20100104000000|'


def file_symbol_name_history():
    content = f'''<VERSION_INFO VERSION="1">
</VERSION_INFO>
<SYMBOL_NAME_HISTORY SYMBOLOGY="CORE">
{str_symbol_name_history()}
</SYMBOL_NAME_HISTORY>'''
    data_file = otp.utils.TmpFile(suffix='.txt')
    with open(data_file.path, 'w') as f:
        f.writelines(content)
    return data_file.path


@pytest.mark.parametrize('data', [
    str_symbol_name_history(),  # input as a section (string)
    file_symbol_name_history(),  # input as a xml file (saved on disk)
])
def test_put_source_types(f_session, tz, data):
    ref_db_name = 'REF_DATA_DB'
    symbology = 'CORE'

    ref_db = otp.RefDB(
        ref_db_name,
        db_properties={
            'symbology': symbology,
        },
    )
    f_session.use(ref_db)

    if isinstance(data, str) and os.path.exists(data):
        # input as a xml file (saved on disk)
        ref_db.put(data)
    else:
        # input as a section (otp.Source or string)
        ref_db.put([
            otp.RefDB.SymbolNameHistory(data, symbology),
        ])

    data = otp.DataSource(db=ref_db_name, symbol='SYMBOLOGY_CACHE', tick_type='SYMBOLOGY')
    data = otp.run(data, start=otp.dt(2010, 1, 1, tz=tz), end=otp.dt(2010, 1, 5, tz=tz), timezone=tz)
    assert len(data) == 1 and data.iloc[0]['SYMBOLOGY_NAME'] == symbology

    data = otp.DataSource(db=ref_db_name, symbols=otp.Symbols(db=ref_db_name, for_tick_type='SYM'), tick_type='SYM')
    data = otp.run(data, start=otp.dt(2010, 1, 1, tz=tz), end=otp.dt(2010, 1, 5, tz=tz), timezone=tz)
    assert len(data) == 4 and len(data['SYMBOL_NAME'].unique()) == 2


@pytest.mark.skipif(not is_supported_otq_reference_data_loader(), reason=reason_otq_query)
def test_put_source_types_otq_query(f_session, tz):
    test_put_source_types(f_session, tz, ticks_symbol_name_history(tz))  # input as a section (otp.Source)


@pytest.mark.parametrize('data', [
    str_symbol_name_history(),  # input as a section (string)
])
def test_ref_db_without_session(tz, data):
    ref_db_name = 'REF_DATA_DB'
    symbology = 'CORE'

    ref_db = otp.RefDB(
        ref_db_name,
        db_properties={
            'symbology': symbology,
        },
    )

    ref_db.put([
        otp.RefDB.SymbolNameHistory(data, symbology),
    ])

    assert otp.Session._instance is None
    s = otp.Session()
    try:
        s.use(ref_db)

        data = otp.DataSource(db=ref_db_name, symbol='SYMBOLOGY_CACHE', tick_type='SYMBOLOGY')
        data = otp.run(data, start=otp.dt(2010, 1, 1, tz=tz), end=otp.dt(2010, 1, 5, tz=tz), timezone=tz)
        assert len(data) == 1 and data.iloc[0]['SYMBOLOGY_NAME'] == symbology

        symbols = otp.Symbols(db=ref_db_name, for_tick_type='SYM')
        data = otp.DataSource(db=ref_db_name, symbols=symbols, tick_type='SYM')
        data = otp.run(data, start=otp.dt(2010, 1, 1, tz=tz), end=otp.dt(2010, 1, 5, tz=tz), timezone=tz)
        assert len(data) == 4 and len(data['SYMBOL_NAME'].unique()) == 2
    finally:
        s.close()


@pytest.mark.skipif(not is_supported_otq_reference_data_loader(), reason=reason_otq_query)
def test_ref_db_without_session_otq_query(tz):
    test_ref_db_without_session(tz, ticks_symbol_name_history(tz))


@pytest.mark.parametrize('ref_db_name', [
    'REF_DATA_MYDB',  # valid value
    'REF_DATA',  # valid value
    'REFDB',  # invalid value - does not have REF_DATA prefix
])
def test_ref_db_name(f_session, tz, ref_db_name):
    ''' Test that RefDB does not work if does not have REF_DATA prefix '''
    db_name = 'DB'
    symbology = 'CORE'

    ref_db = otp.RefDB(
        ref_db_name,
        db_properties={
            'symbology': symbology,
        },
    )
    f_session.use(ref_db)

    _, err = ref_db.put([
        otp.RefDB.SymbolNameHistory(str_symbol_name_history(), symbology),
    ])

    db = otp.DB(
        db_name,
        db_properties={
            'ref_data_db': ref_db_name,
            'symbology': symbology,
        },
    )

    data_symb = otp.DataSource(db=ref_db_name, symbol='SYMBOLOGY_CACHE', tick_type='SYMBOLOGY')
    data_symb = otp.run(data_symb, start=otp.dt(2010, 1, 1, tz=tz), end=otp.dt(2010, 1, 5, tz=tz), timezone=tz)

    symbols = otp.Symbols(db=ref_db_name, for_tick_type='SYM')
    data_sym = otp.DataSource(db=ref_db_name, symbols=symbols, tick_type='SYM')

    if ref_db_name.startswith('REF_DATA'):
        data_sym = otp.run(data_sym, start=otp.dt(2010, 1, 1, tz=tz), end=otp.dt(2010, 1, 5, tz=tz), timezone=tz)
        assert b'Total ticks 8' in err and b'Total symbols 6' in err
        assert len(data_symb) == 1
        assert len(data_sym) == 4
        f_session.use(db)
    else:
        with pytest.warns(UserWarning, match='Eval statement returned no symbols'):
            data_sym = otp.run(data_sym, start=otp.dt(2010, 1, 1, tz=tz), end=otp.dt(2010, 1, 5, tz=tz), timezone=tz)
        # loader should return error
        assert b'ERROR' in err and b'Reference database should have REF_DATA prefix' in err
        assert len(data_symb) == 0
        assert len(data_sym) == 0
        # can't be used in ref_data_db property of another database
        with pytest.raises(Exception, match='should have REF_DATA prefix'):
            f_session.use(db)


@pytest.mark.parametrize('location', [
    None, {}, {'archive_duration': 'continuous'},  # valid values
    {'archive_duration': 'year'},  # invalid value - reference db location must point to a continuous archive db
])
def test_ref_db_locations(f_session, tz, location):
    ''' Test that RefDB works only if has single location with archive_duration equal to continuous.
    archive_duration set to continuous by default if either location not provided
    or value of archive_duration is not provided. '''
    db_name = 'DB'
    ref_db_name = f'REF_DATA_{db_name}'
    symbology = 'CORE'

    ref_db = otp.RefDB(
        ref_db_name,
        db_properties={
            'symbology': symbology,
        },
        db_location=location
    )

    if location is not None and location.get('archive_duration', 'continuous') != 'continuous':
        with pytest.raises(Exception, match='Invalid location'):
            f_session.use(ref_db)
        return

    f_session.use(ref_db)

    _, err = ref_db.put([
        otp.RefDB.SymbolNameHistory(str_symbol_name_history(), symbology),
    ])

    db = otp.DB(
        db_name,
        db_properties={
            'ref_data_db': ref_db_name,
            'symbology': symbology,
        },
    )
    f_session.use(db)

    data_symb = otp.DataSource(db=ref_db_name, symbol='SYMBOLOGY_CACHE', tick_type='SYMBOLOGY')
    data_symb = otp.run(data_symb, start=otp.dt(2010, 1, 1, tz=tz), end=otp.dt(2010, 1, 5, tz=tz), timezone=tz)

    symbols = otp.Symbols(db=ref_db_name, for_tick_type='SYM')
    data_sym = otp.DataSource(db=ref_db_name, symbols=symbols, tick_type='SYM')
    data_sym = otp.run(data_sym, start=otp.dt(2010, 1, 1, tz=tz), end=otp.dt(2010, 1, 5, tz=tz), timezone=tz)

    assert len(ref_db.locations) == 1 and ref_db.locations[0]['archive_duration'] == 'continuous'
    assert b'Total ticks 8' in err and b'Total symbols 6' in err
    assert len(data_symb) == 1
    assert len(data_sym) == 4


@pytest.mark.parametrize('data', [
    str_symbol_name_history(),
])
def test_symbol_name_history(f_session, tz, data):
    db_name = 'DB'
    ref_db_name = f'REF_DATA_{db_name}'
    symbology = 'CORE'

    ref_db = otp.RefDB(
        ref_db_name,
        db_properties={
            'symbology': symbology,
        },
    )
    f_session.use(ref_db)

    db = otp.DB(
        db_name,
        db_properties={
            'ref_data_db': ref_db_name,
            'symbology': symbology,
        },
    )
    f_session.use(db)

    ref_db.put([
        otp.RefDB.SymbolNameHistory(data, symbology),
    ])

    trd = otp.Ticks(PRICE=[1], SIZE=[1], start=otp.dt(2010, 1, 2, tz=tz), end=otp.dt(2010, 1, 2, tz=tz) + otp.Day(1))
    otp.db.write_to_db(trd, db_name, symbol='CORE_A', tick_type='TRD', date=otp.dt(2010, 1, 2))

    trd = otp.Ticks(PRICE=[1], SIZE=[2], start=otp.dt(2010, 1, 3, tz=tz), end=otp.dt(2010, 1, 3, tz=tz) + otp.Day(1))
    otp.db.write_to_db(trd, db_name, symbol='CORE_B', tick_type='TRD', date=otp.dt(2010, 1, 3))

    trd = otp.DataSource(db, tick_type='TRD')

    s = otp.dt(2010, 1, 1, tz=tz)
    e = otp.dt(2010, 1, 4, tz=tz)

    # without and with symbology prefix - should not make change for symbols in native symbology
    for prefix in ['', 'CORE::::']:
        symbol_name = f'{prefix}CORE_A'
        for symbol_date, expected_len in [(otp.dt(2010, 1, 1, tz=tz), 1),
                                          (otp.dt(2010, 1, 2, tz=tz), 2),
                                          (otp.dt(2010, 1, 3, tz=tz), 1),
                                          (otp.dt(2010, 1, 4, tz=tz), 1)]:
            data = otp.run(trd, symbols=symbol_name, start=s, end=e, timezone=tz, symbol_date=symbol_date,
                           print_symbol_errors=False)
            assert len(data) == expected_len, f'For symbol_name={symbol_name} and symbol_date={symbol_date}' \
                                              f' expected {expected_len} ticks'

        symbol_name = f'{prefix}CORE_B'
        for symbol_date, expected_len in [(otp.dt(2010, 1, 1, tz=tz), 1),
                                          (otp.dt(2010, 1, 2, tz=tz), 1),
                                          (otp.dt(2010, 1, 3, tz=tz), 2),
                                          (otp.dt(2010, 1, 4, tz=tz), 1)]:
            data = otp.run(trd, symbols=symbol_name, start=s, end=e, timezone=tz, symbol_date=symbol_date,
                           print_symbol_errors=False)
            assert len(data) == expected_len, f'For symbol_name={symbol_name} and symbol_date={symbol_date}' \
                                              f' expected {expected_len} ticks'


@pytest.mark.skipif(not is_supported_otq_reference_data_loader(), reason=reason_otq_query)
def test_symbol_name_history_otq_query(f_session, tz):
    test_symbol_name_history(f_session, tz, ticks_symbol_name_history(tz))


def ticks_symbology_mapping(tz):
    data = otp.Ticks(
        SYMBOL_NAME=['A', 'B'],
        MAPPED_SYMBOL_NAME=['CORE_A', 'CORE_B'],
        START_DATETIME=[otp.dt(2010, 1, 2, tz=tz), otp.dt(2010, 1, 3, tz=tz)],
        END_DATETIME=[otp.dt(2010, 1, 3, tz=tz), otp.dt(2010, 1, 4, tz=tz)],
        offset=[0] * 2,
        db='LOCAL',
    )
    data = data.table(
        SYMBOL_NAME=otp.string[128],
        MAPPED_SYMBOL_NAME=otp.string[128],
        START_DATETIME=otp.msectime,
        END_DATETIME=otp.msectime,
    )
    return data


def str_symbology_mapping():
    return '''A||20100102000000|20100103000000|CORE_A|
B||20100103000000|20100104000000|CORE_B|'''


@pytest.mark.parametrize('data', [
    str_symbology_mapping(),
])
def test_symbology_mapping(f_session, tz, data):
    db_name = 'DB'
    ref_db_name = f'REF_DATA_{db_name}'
    symbology = 'CORE'
    symbology2 = 'TICKER'

    ref_db = otp.RefDB(
        ref_db_name,
        db_properties={
            'symbology': symbology,
        },
    )
    f_session.use(ref_db)

    db = otp.DB(
        db_name,
        db_properties={
            'ref_data_db': ref_db_name,
            'symbology': symbology,
        },
    )
    f_session.use(db)

    ref_db.put([
        otp.RefDB.SymbolNameHistory(str_symbol_name_history(), symbology),
        otp.RefDB.SymbologyMapping(data, symbology2, symbology),
    ])

    trd = otp.Ticks(PRICE=[1], SIZE=[1], start=otp.dt(2010, 1, 2, tz=tz), end=otp.dt(2010, 1, 2, tz=tz) + otp.Day(1))
    otp.db.write_to_db(trd, db_name, symbol='CORE_A', tick_type='TRD', date=otp.dt(2010, 1, 2))

    trd = otp.Ticks(PRICE=[1], SIZE=[2], start=otp.dt(2010, 1, 3, tz=tz), end=otp.dt(2010, 1, 3, tz=tz) + otp.Day(1))
    otp.db.write_to_db(trd, db_name, symbol='CORE_B', tick_type='TRD', date=otp.dt(2010, 1, 3))

    trd = otp.DataSource(db, tick_type='TRD')

    s = otp.dt(2010, 1, 1, tz=tz)
    e = otp.dt(2010, 1, 4, tz=tz)

    # symbols in non-native symbology can be queried only with symbol_date set to the symbol mapped time range
    symbol_name = f'{symbology2}::::A'
    for symbol_date, expected_len in [(otp.dt(2010, 1, 1, tz=tz), 0),
                                      (otp.dt(2010, 1, 2, tz=tz), 2),
                                      (otp.dt(2010, 1, 3, tz=tz), 0),
                                      (otp.dt(2010, 1, 4, tz=tz), 0)]:
        data = otp.run(trd, symbols=symbol_name, start=s, end=e, timezone=tz, symbol_date=symbol_date,
                       print_symbol_errors=False)
        assert len(data) == expected_len, f'For symbol_name={symbol_name} and symbol_date={symbol_date}' \
                                          f' expected {expected_len} ticks'

    symbol_name = f'{symbology2}::::B'
    for symbol_date, expected_len in [(otp.dt(2010, 1, 1, tz=tz), 0),
                                      (otp.dt(2010, 1, 2, tz=tz), 0),
                                      (otp.dt(2010, 1, 3, tz=tz), 2),
                                      (otp.dt(2010, 1, 4, tz=tz), 0)]:
        data = otp.run(trd, symbols=symbol_name, start=s, end=e, timezone=tz, symbol_date=symbol_date,
                       print_symbol_errors=False)
        assert len(data) == expected_len, f'For symbol_name={symbol_name} and symbol_date={symbol_date}' \
                                          f' expected {expected_len} ticks'


@pytest.mark.skipif(not is_supported_otq_reference_data_loader(), reason=reason_otq_query)
def test_symbology_mapping_otq_query(f_session, tz):
    test_symbology_mapping(f_session, tz, ticks_symbology_mapping(tz))


def ticks_corp_actions(tz):
    data = otp.Ticks(
        SYMBOL_NAME=['CORE_B'],
        EFFECTIVE_DATETIME=[otp.dt(2010, 1, 3, 12, tz=tz)],
        MULTIPLICATIVE_ADJUSTMENT=[0.25],
        ADDITIVE_ADJUSTMENT=[0.0],
        ADJUSTMENT_TYPE_NAME=['SPLIT'],
        offset=[0],
        db='LOCAL',
    )
    data = data.table(
        SYMBOL_NAME=otp.string[128],
        EFFECTIVE_DATETIME=otp.msectime,
        MULTIPLICATIVE_ADJUSTMENT=float,
        ADDITIVE_ADJUSTMENT=float,
        ADJUSTMENT_TYPE_NAME=otp.string[128],
    )
    return data


def str_corp_actions():
    return 'CORE_B||20100103120000|0.25|0.0|SPLIT'


@pytest.mark.parametrize('data', [
    str_corp_actions(),
])
def test_corp_actions(f_session, tz, data):
    db_name = 'DB'
    ref_db_name = f'REF_DATA_{db_name}'
    symbology = 'CORE'

    ref_db = otp.RefDB(
        ref_db_name,
        db_properties={
            'symbology': symbology,
        },
    )
    f_session.use(ref_db)

    db = otp.DB(
        db_name,
        db_properties={
            'ref_data_db': ref_db_name,
            'symbology': symbology,
        },
    )
    f_session.use(db)

    ref_db.put([
        otp.RefDB.SymbolNameHistory(str_symbol_name_history(), symbology),
        otp.RefDB.CorpActions(data, symbology),
    ])

    trd = otp.Ticks(PRICE=[4], SIZE=[1], start=otp.dt(2010, 1, 2, tz=tz), end=otp.dt(2010, 1, 2, tz=tz) + otp.Day(1))
    otp.db.write_to_db(trd, db_name, symbol='CORE_A', tick_type='TRD', date=otp.dt(2010, 1, 2))

    trd = otp.Ticks(PRICE=[4], SIZE=[2], start=otp.dt(2010, 1, 3, tz=tz), end=otp.dt(2010, 1, 3, tz=tz) + otp.Day(1))
    otp.db.write_to_db(trd, db_name, symbol='CORE_B', tick_type='TRD', date=otp.dt(2010, 1, 3))

    s = otp.dt(2010, 1, 1, tz=tz)
    e = otp.dt(2010, 1, 4, tz=tz)

    expected = pd.DataFrame({'PRICE': [4., 4.], 'SIZE': [1., 2.]})
    for adj_date in ['20100101', '20100102']:  # dates before the split - split is not applied
        trd = otp.DataSource(db, tick_type='TRD')
        trd = otp.corp_actions(
            source=trd,
            adjustment_date=adj_date,
            fields='PRICE',
            adjust_rule='PRICE',
        )
        trd = otp.corp_actions(
            source=trd,
            adjustment_date=adj_date,
            fields='SIZE',
            adjust_rule='SIZE',
        )

        data_a = otp.run(trd, symbols='CORE_A', start=s, end=e, timezone=tz, symbol_date=otp.dt(2010, 1, 2, tz=tz))
        assert len(data_a) == 2 and data_a[['PRICE', 'SIZE']].equals(expected)

        data_b = otp.run(trd, symbols='CORE_B', start=s, end=e, timezone=tz, symbol_date=otp.dt(2010, 1, 3, tz=tz))
        assert len(data_b) == 2 and data_b[['PRICE', 'SIZE']].equals(expected)

    expected = pd.DataFrame({'PRICE': [1., 1.], 'SIZE': [4., 8.]})  # PRICE * 0.25, SIZE / 0.25
    for adj_date in ['20100103', '20100104']:  # dates after the split - split is applied
        trd = otp.DataSource(db, tick_type='TRD')
        trd = otp.corp_actions(
            source=trd,
            adjustment_date=adj_date,
            fields='PRICE',
            adjust_rule='PRICE',
        )
        trd = otp.corp_actions(
            source=trd,
            adjustment_date=adj_date,
            fields='SIZE',
            adjust_rule='SIZE',
        )

        data_a = otp.run(trd, symbols='CORE_A', start=s, end=e, timezone=tz, symbol_date=otp.dt(2010, 1, 2, tz=tz))
        assert len(data_a) == 2 and data_a[['PRICE', 'SIZE']].equals(expected)

        data_b = otp.run(trd, symbols='CORE_B', start=s, end=e, timezone=tz, symbol_date=otp.dt(2010, 1, 3, tz=tz))
        assert len(data_b) == 2 and data_b[['PRICE', 'SIZE']].equals(expected)


@pytest.mark.skipif(not is_supported_otq_reference_data_loader(), reason=reason_otq_query)
def test_corp_actions_otq_query(f_session, tz):
    test_corp_actions(f_session, tz, ticks_corp_actions(tz))


def ticks_continuous_contracts(tz):
    data = otp.Ticks(
        CONTINUOUS_CONTRACT_NAME=['CORE_ab'] * 2,
        SYMBOL_NAME=['CORE_A', 'CORE_B'],
        START_DATETIME=[otp.dt(2010, 1, 2, tz=tz), otp.dt(2010, 1, 3, tz=tz)],
        END_DATETIME=[otp.dt(2010, 1, 3, tz=tz), otp.dt(2010, 1, 4, tz=tz)],
        MULTIPLICATIVE_ADJUSTMENT=[0.5, None],
        ADDITIVE_ADJUSTMENT=[3, None],
        offset=[0] * 2,
        db='LOCAL',
    )
    data = data.table(
        CONTINUOUS_CONTRACT_NAME=otp.string[128],
        SYMBOL_NAME=otp.string[128],
        START_DATETIME=otp.msectime,
        END_DATETIME=otp.msectime,
        MULTIPLICATIVE_ADJUSTMENT=float,
        ADDITIVE_ADJUSTMENT=float,
    )
    return data


def str_continuous_contracts():
    return 'CORE_ab||CORE_A||20100102000000|20100103000000|0.5|3|CORE_B||20100103000000|20100104000000'


@pytest.mark.parametrize('data', [
    str_continuous_contracts(),
])
def test_continuous_contracts(f_session, tz, data):
    db_name = 'DB'
    ref_db_name = f'REF_DATA_{db_name}'
    symbology = 'CORE'

    ref_db = otp.RefDB(
        ref_db_name,
        db_properties={
            'symbology': symbology,
        },
    )
    f_session.use(ref_db)

    db = otp.DB(
        db_name,
        db_properties={
            'ref_data_db': ref_db_name,
            'symbology': symbology,
        },
    )
    f_session.use(db)

    ref_db.put([
        otp.RefDB.SymbolNameHistory(str_symbol_name_history(), symbology),
        otp.RefDB.ContinuousContracts(data, symbology),
    ])

    trd = otp.Ticks(PRICE=[4], SIZE=[1], start=otp.dt(2010, 1, 2, tz=tz), end=otp.dt(2010, 1, 2, tz=tz) + otp.Day(1))
    otp.db.write_to_db(trd, db_name, symbol='CORE_A', tick_type='TRD', date=otp.dt(2010, 1, 2))

    trd = otp.Ticks(PRICE=[4], SIZE=[2], start=otp.dt(2010, 1, 3, tz=tz), end=otp.dt(2010, 1, 3, tz=tz) + otp.Day(1))
    otp.db.write_to_db(trd, db_name, symbol='CORE_B', tick_type='TRD', date=otp.dt(2010, 1, 3))

    s = otp.dt(2010, 1, 1, tz=tz)
    e = otp.dt(2010, 1, 4, tz=tz)

    expected = pd.DataFrame({'PRICE': [4., 11.], 'SIZE': [1., 1.]})  # CORE_A unchanged; CORE_B PRICE/0.5+3, SIZE*0.5
    for adj_date in ['20100101', '20100102']:  # dates of CORE_A contract - splice is applied to CORE_B
        trd = otp.DataSource(db, tick_type='TRD')
        trd = otp.corp_actions(
            source=trd,
            adjustment_date=adj_date,
            apply_security_splice=True,
            fields='PRICE',
            adjust_rule='PRICE',
        )
        trd = otp.corp_actions(
            source=trd,
            adjustment_date=adj_date,
            apply_security_splice=True,
            fields='SIZE',
            adjust_rule='SIZE',
        )

        data_a = otp.run(trd, symbols='CORE_ab', start=s, end=e, timezone=tz, symbol_date=otp.dt(2010, 1, 2, tz=tz))
        assert len(data_a) == 2 and data_a[['PRICE', 'SIZE']].equals(expected)

        data_b = otp.run(trd, symbols='CORE_ab', start=s, end=e, timezone=tz, symbol_date=otp.dt(2010, 1, 3, tz=tz))
        assert len(data_b) == 2 and data_b[['PRICE', 'SIZE']].equals(expected)

    expected = pd.DataFrame({'PRICE': [0.5, 4.], 'SIZE': [2., 2.]})  # CORE_A (PRICE-3)*0.5, SIZE/0.5; CORE_B unchanged
    for adj_date in ['20100103', '20100104']:  # dates of CORE_B contract - splice is applied to CORE_A
        trd = otp.DataSource(db, tick_type='TRD')
        trd = otp.corp_actions(
            source=trd,
            adjustment_date=adj_date,
            apply_security_splice=True,
            fields='PRICE',
            adjust_rule='PRICE',
        )
        trd = otp.corp_actions(
            source=trd,
            adjustment_date=adj_date,
            apply_security_splice=True,
            fields='SIZE',
            adjust_rule='SIZE',
        )

        data_a = otp.run(trd, symbols='CORE_ab', start=s, end=e, timezone=tz, symbol_date=otp.dt(2010, 1, 2, tz=tz))
        assert len(data_a) == 2 and data_a[['PRICE', 'SIZE']].equals(expected)

        data_b = otp.run(trd, symbols='CORE_ab', start=s, end=e, timezone=tz, symbol_date=otp.dt(2010, 1, 3, tz=tz))
        assert len(data_b) == 2 and data_b[['PRICE', 'SIZE']].equals(expected)

    # test show_symbol_name_in_db
    trd = otp.DataSource(db, tick_type='TRD')
    trd['SYMBOL_NAME'] = trd.Symbol.name
    trd = trd.show_symbol_name_in_db()
    trd['X'] = trd['SYMBOL_NAME_IN_DB'] + '__X'
    df = otp.run(trd, symbols='CORE_ab', start=s, end=e, timezone=tz, symbol_date=otp.dt(2010, 1, 2, tz=tz))
    assert trd.schema['SYMBOL_NAME_IN_DB'] is str
    assert list(df['SYMBOL_NAME']) == ['CORE_ab', 'CORE_ab']
    assert list(df['SYMBOL_NAME_IN_DB']) == ['CORE_A', 'CORE_B']
    assert list(df['X']) == ['CORE_A__X', 'CORE_B__X']


@pytest.mark.skipif(not is_supported_otq_reference_data_loader(), reason=reason_otq_query)
def test_continuous_contracts_otq_query(f_session, tz):
    test_continuous_contracts(f_session, tz, ticks_continuous_contracts(tz))


def test_not_implemented(f_session, tz):
    ref_db = otp.RefDB('REF_DATA_DB', db_properties={'symbology': 'CORE'})

    with pytest.raises(NotImplementedError):
        ref_db.add(str_symbol_name_history())
