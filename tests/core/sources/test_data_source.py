import pytest
from pathlib import Path

import onetick.py as otp
from onetick.py.compatibility import is_supported_where_clause_for_back_ticks


@pytest.fixture(scope='module')
def session():
    with otp.Session() as s:
        db_1 = otp.DB('DB_1')
        db_1.add(otp.Ticks(A=[1, 2, 3, 4, 5, 6]), date=otp.datetime('20220101'))
        db_1.add(otp.Ticks(A=[7, 8]), date=otp.datetime('20220102'))
        s.use(db_1)
        db_2 = otp.DB('DB_2')
        db_2.add(
            otp.Tick(
                STRING='abc',
                STRING_10=otp.string[10]('abc'),
                STRING_100=otp.string[100]('abc'),
                VARSTRING=otp.varstring('abc')
            ),
            date=otp.datetime('20220102'),
        )
        s.use(db_2)
        yield s


@pytest.mark.parametrize("max_back_ticks_to_prepend", [0, 1, 2, 7])
def test_max_back_ticks_to_prepend(session, max_back_ticks_to_prepend):
    if max_back_ticks_to_prepend > 0:
        data = otp.DataSource(
            'DB_1',
            date=otp.datetime('20220102'),
            max_back_ticks_to_prepend=max_back_ticks_to_prepend,
            back_to_first_tick=otp.Day(1),
        )
        df = otp.run(data)
        if max_back_ticks_to_prepend != 7:
            assert all(df['A'] == list(range(7 - max_back_ticks_to_prepend, 9)))
        else:
            assert all(df['A'] == list(range(1, 9)))
    else:
        with pytest.raises(ValueError):
            otp.DataSource(
                'DB_1',
                max_back_ticks_to_prepend=max_back_ticks_to_prepend,
            )


@pytest.mark.skipif(not is_supported_where_clause_for_back_ticks(), reason='parameter was added later')
@pytest.mark.parametrize("where_clause_for_back_ticks,ans", [
    (otp.raw('A=5', dtype=bool), [5, 7, 8]),
    (otp.raw('A=100', dtype=bool), [7, 8]),
    (otp.raw('A>0', dtype=bool), [6, 7, 8]),
])
def test_where_clause_for_back_ticks(session, where_clause_for_back_ticks, ans):
    data = otp.DataSource(
        'DB_1',
        date=otp.datetime('20220102'),
        where_clause_for_back_ticks=where_clause_for_back_ticks,
        back_to_first_tick=otp.Day(1),
    )
    df = otp.run(data)
    assert all(df['A'] == ans)


@pytest.mark.skipif(not is_supported_where_clause_for_back_ticks(), reason='parameter was added later')
def test_where_clause_for_back_ticks_exception(session):
    with pytest.raises(ValueError):
        otp.DataSource(
            'DB_1',
            date=otp.datetime('20220102'),
            where_clause_for_back_ticks=otp.raw('SOMETHING', dtype=otp.string[64]),
            back_to_first_tick=otp.Day(1),
        )


@pytest.mark.parametrize('field_name,field_type,raises,notice_message', [
    ('STRING', str, False, False),
    ('STRING', otp.string[10], True, True),
    ('STRING', otp.string[64], False, False),
    ('STRING', otp.string[100], False, False),
    ('STRING', otp.varstring, False, False),
    ('STRING_10', str, False, False),
    ('STRING_10', otp.string[10], False, False),
    ('STRING_10', otp.string[64], False, False),
    ('STRING_10', otp.string[100], False, False),
    ('STRING_10', otp.varstring, False, False),
    ('STRING_100', str, True, True),
    ('STRING_100', otp.string[10], True, False),
    ('STRING_100', otp.string[64], True, False),
    ('STRING_100', otp.string[100], False, False),
    ('STRING_100', otp.varstring, False, False),
    ('VARSTRING', str, True, True),
    ('VARSTRING', otp.string[10], True, False),
    ('VARSTRING', otp.string[64], True, False),
    ('VARSTRING', otp.string[100], True, False),
    ('VARSTRING', otp.varstring, False, False),
])
def test_string(session, field_name, field_type, raises, notice_message):
    # We're checking raising (or not raising) of specific exceptions, related to mixed case,
    # then schema is partially set by user and obtained from DB.
    # So we need to keep schema_policy='tolerant' in this test, to keep previous behaviour,
    # which was before implementing force set schema_policy to 'manual' if schema is set by user.
    if raises:
        with pytest.raises(ValueError) as e:
            otp.DataSource(
                'DB_2',
                date=otp.datetime('20220102'),
                schema={
                    field_name: field_type,
                },
                schema_policy='tolerant',
            )
        assert ('Notice, that `str` and `otp.string` lengths are 64' in str(e.value)) is notice_message
    else:
        data = otp.DataSource(
            'DB_2',
            date=otp.datetime('20220102'),
            schema={
                field_name: field_type,
            },
            schema_policy='tolerant',
        )
        df = otp.run(data)
        assert all(df[field_name] == ['abc'])


def test_db_as_jwq_parameter(session):
    # PY-891

    input_ticks = otp.Ticks(
        X=[1, 2],
        MD_DB=['DB_1', 'DB_2']
    )

    def func(db):
        d = otp.DataSource(db=db, tick_type='TRD')
        d = d.first()
        return d

    res = input_ticks.join_with_query(func,
                                      params=dict(db=input_ticks['MD_DB']))

    df = otp.run(res, date=otp.dt(2022, 1, 2))
    df = df[['MD_DB', 'A', 'STRING']]
    assert dict(df.iloc[0]) == {'MD_DB': 'DB_1', 'A': 7, 'STRING': ''}
    assert dict(df.iloc[1]) == {'MD_DB': 'DB_2', 'A': 0, 'STRING': 'abc'}


def test_one_symbol_no_merge(session):
    data = otp.DataSource('DB_1', tick_type='TRD')
    assert 'MERGE' not in Path(data.to_otq().split('::')[0]).read_text()
    df = otp.run(data, symbols=otp.config.default_symbol, date=otp.dt(2022, 1, 1))
    assert list(df['A']) == [1, 2, 3, 4, 5, 6]

    data = otp.DataSource('DB_1', tick_type='TRD', symbols=otp.config.default_symbol)
    assert 'MERGE' not in Path(data.to_otq().split('::')[0]).read_text()
    df = otp.run(data, date=otp.dt(2022, 1, 1))
    assert list(df['A']) == [1, 2, 3, 4, 5, 6]


def test_default_schema_policy(session):
    default_schema_policy = otp.config.default_schema_policy

    otp.config.default_schema_policy = 'tolerant'
    assert 'A' in otp.DataSource('DB_1', tick_type='TRD').schema

    otp.config.default_schema_policy = 'manual'
    assert 'A' not in otp.DataSource('DB_1', tick_type='TRD').schema

    otp.config.default_schema_policy = default_schema_policy


def test_manual_strict_schema(session):
    # PY-1109
    with pytest.raises(ValueError,
                       match="'manual_strict' schema policy was specified, but no schema has been provided"):
        otp.DataSource('DB_1', tick_type='TRD', schema_policy='manual_strict')
    data = otp.DataSource('DB_1', tick_type='TRD', schema_policy='manual_strict', schema={'A': int})
    assert data.schema == {'A': int}
    df = otp.run(data, symbols=otp.config.default_symbol, date=otp.dt(2022, 1, 1))
    assert list(df['A']) == [1, 2, 3, 4, 5, 6]

    data = otp.DataSource('DB_1', tick_type='TRD', schema_policy='manual_strict', schema={'A': int, 'B': str})
    assert data.schema == {'A': int, 'B': str}
    df = otp.run(data, symbols=otp.config.default_symbol, date=otp.dt(2022, 1, 1))
    assert list(df['A']) == [1, 2, 3, 4, 5, 6]
    assert list(df['B']) == [''] * 6


@pytest.mark.parametrize(
    'schema_policy', ['tolerant', 'tolerant_strict', 'manual', 'manual_strict'],
)
def test_forced_manual_schema(session, mocker, schema_policy):
    is_schema_sync_expected = schema_policy in {'tolerant', 'tolerant_strict'}
    mocked_schema = None

    if not is_schema_sync_expected:
        mocked_schema = mocker.patch('onetick.py.db._inspection.DB.schema', return_value={})

    _ = otp.DataSource(
        'DB_2',
        date=otp.datetime('20220102'),
        schema_policy=schema_policy,
        schema={'STRING': str},
    )

    if not is_schema_sync_expected:
        mocked_schema.assert_not_called()


def test_ambiguous_type_name_manual_default_schema(session):
    data = otp.DataSource(
        'DB_2',
        date=otp.datetime('20220102'),
        schema={'STRING': otp.string[10]},
    )
    df = otp.run(data)
    assert all(df['STRING'] == ['abc'])


def test_unsupported_schema_type(session):
    with pytest.warns(FutureWarning, match='Setting schema with complex types is deprecated'):
        data = otp.DataSource(db='DB_2', schema=dict(FIELD=otp.datetime))
    assert data.schema == {'FIELD': otp.nsectime}
    df = otp.run(data, date=otp.datetime('20220102'), timezone='GMT')
    assert df['FIELD'][0] == otp.dt(1970, 1, 1)


def test_wrong_kwarg_passed(session):
    with pytest.warns(
        FutureWarning,
        match=(
            r"Setting `DataSource` schema via `\*\*kwargs` is deprecated. "
            "Please use `schema` parameter for this. "
            "Passed kwargs are: {'wrong': <class 'str'>}."
        )
    ):
        _ = otp.DataSource('DB_2', date=otp.datetime('20220102'), wrong=str)

    with pytest.raises(
        ValueError,
        match=(
            r"Specifying schema through both `\*\*kwargs` and `schema` is prohibited. "
            "Passed kwargs are: {'wrong': <class 'str'>}."
        )
    ):
        _ = otp.DataSource('DB_2', date=otp.datetime('20220102'), wrong=str, schema={'CORRECT': int})
