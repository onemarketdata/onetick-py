import os
import pytest
import pandas as pd
import numpy as np

import onetick.py as otp
from onetick.py.otq import otq
from datetime import datetime, timedelta


date = datetime(2010, 1, 1)
delayed_date = date + timedelta(days=2)
decayed_date = date + timedelta(days=7)

default_data = otp.Ticks({
    "X": [1],
    "Y": [0.5],
    "Z": ["blep"],
    "T": [datetime(2003, 1, 1)],
    "A": ["a" * 1000],
})


default_schema = {
    "X": int,
    "Y": float,
    "Z": str,
    "T": otp.nsectime,
    "A": otp.string[1000],
}


@pytest.fixture(scope="module")
def default_db(session):
    db = otp.DB("DEFAULT_DB", src=default_data, symbol="BLEP", tick_type="TT", date=date)
    session.use(db)
    yield db


@pytest.fixture(scope="module")
def mismatched_symbol_db(session):
    db = otp.DB("MISMATCHED_DB")
    db.add(src=default_data, symbol="BLEP", tick_type="TT", date=date)
    db.add(src=otp.Tick(X=3, Y="I"), symbol="SYMB", tick_type="TT", date=date)
    session.use(db)
    yield db


@pytest.fixture(scope="module")
def compatible_symbol_db(session):
    db = otp.DB("COMPATIBLE_DB")
    db.add(src=default_data, symbol="BLEP", tick_type="TT", date=date)
    db.add(src=otp.Tick(X=3, Q=" "), symbol="SYMB", tick_type="TT", date=date)
    session.use(db)
    yield db


@pytest.fixture(scope="module")
def delayed_data_db(session):
    db = otp.DB("DELAYED_DB")
    db.add(src=default_data, symbol="BLEP", tick_type="TT", date=date)
    db.add(src=otp.Tick(X=0), symbol="SYMB", tick_type="AA", date=date + timedelta(days=1))
    session.use(db)
    yield db


@pytest.fixture(scope="module")
def decayed_data_db(session):
    db = otp.DB("DECAYED_DB")
    db.add(src=default_data, symbol="BLEP", tick_type="TT", date=date)
    db.add(src=otp.Tick(X=0), symbol="SYMB", tick_type="AA", date=date + timedelta(days=6))
    session.use(db)
    yield db


@pytest.fixture(scope="module")
def ahead_data_db(session):
    db = otp.DB("AHEAD_DB")
    db.add(src=otp.Ticks({
        "X": [1] * 5,
        "Y": [0.5] * 5,
        "Z": ["blep2"] * 5,
        "T": [datetime(2010, 1, 1)] * 5,
        "offset": list(range(4)) + [500],
    }), symbol="BLEP", tick_type="TT", date=date - timedelta(minutes=1))
    session.use(db)
    yield db


@pytest.fixture(scope='module')
def many_tick_types_db(session):
    db = otp.DB('MANY_TICK_TYPES_DB')
    db.add(default_data, symbol='BLEP', tick_type='TT1', date=date)
    db.add(default_data, symbol='BLEP', tick_type='TT2', date=date)
    session.use(db)
    yield db


@pytest.fixture(scope='module')
def unsupported_schema_db(session):
    db = otp.DB('UNSUPPORTED_SCHEMA_DB')
    data = default_data.copy()
    data.sink(
        otq.Table('DEC decimal (12345)', keep_input_fields=True)
    )
    db.add(data, symbol='S', tick_type='TT', date=date)
    session.use(db)
    yield db


@pytest.fixture(scope='module')
def empty_db(session):
    db = otp.DB('EMPTY_DB')
    session.use(db)
    yield db


@pytest.fixture(scope="module")
def different_tt_different_dates_db(session):
    db = otp.DB("DIFFERENT_TT_DIFFERENT_DATES_DB")
    db.add(src=default_data, symbol="SYMB", tick_type="TT", date=date - timedelta(days=2))
    db.add(src=otp.Tick(X=0), symbol="SYMB", tick_type="AA", date=date - timedelta(days=1))
    session.use(db)
    yield db


def validate_schema_is(source, desired_schema):
    columns = source.columns(skip_meta_fields=True)
    assert len(columns) == len(desired_schema)

    for k, v in desired_schema.items():
        assert k in columns and issubclass(columns[k], v)


class TestCorrectArguments:
    @pytest.mark.parametrize(
        "guess_schema", [True, False],
    )
    def test_guess_schema_deprecated(self, default_db, guess_schema):
        with pytest.warns(FutureWarning, match="schema_policy"):
            otp.DataSource(
                default_db,
                symbol="BLEP",
                tick_type="TT",
                start=date,
                end=date + timedelta(days=1),
                guess_schema=guess_schema,
            )

    @pytest.mark.parametrize(
        "guess_schema", [True, False],
    )
    @pytest.mark.parametrize(
        "policy", ["fail", "manual", "tolerant"],
    )
    @pytest.mark.filterwarnings("ignore:guess_schema")
    def test_guess_schema_policy_incompatible(self, default_db, guess_schema, policy):
        with pytest.raises(ValueError, match="cannot be set at the same time"):
            otp.DataSource(
                default_db,
                symbol="BLEP",
                tick_type="TT",
                start=date,
                end=date + timedelta(days=1),
                guess_schema=guess_schema,
                schema_policy=policy,
            )

    def test_incorrect_policy(self, default_db):
        with pytest.raises(ValueError, match="Invalid schema_policy"):
            otp.DataSource(
                default_db,
                symbol="BLEP",
                tick_type="TT",
                start=date,
                end=date + timedelta(days=1),
                schema_policy="make it up",
            )


class TestSchemaManual:
    policy = "manual"

    def test_empty_schema(self, default_db):
        src = otp.DataSource(
            default_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
        )
        validate_schema_is(src, {})

    def test_subset(self, default_db):
        src = otp.DataSource(
            default_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
            schema={"X": int},
        )
        validate_schema_is(src, {"X": int})

    def test_extra_field(self, default_db):
        src = otp.DataSource(
            default_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
            schema={"Q": str},
        )
        validate_schema_is(src, {"Q": str})

    def test_wrong_type(self, default_db):
        src = otp.DataSource(
            default_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
            schema={"X": str},
        )
        validate_schema_is(src, {"X": str})

    def test_full_schema(self, default_db):
        src = otp.DataSource(
            default_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
            schema=default_schema
        )
        validate_schema_is(src, default_schema)


class TestSchemaTolerant:
    policy = "tolerant"

    def test_empty_schema(self, default_db):
        src = otp.DataSource(
            default_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
        )
        validate_schema_is(src, default_schema)

    def test_subset(self, default_db):
        src = otp.DataSource(
            default_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
            schema={"X": int},
        )
        validate_schema_is(src, default_schema)

    def test_extra_field(self, default_db):
        with pytest.raises(ValueError, match="has no .* field"):
            otp.DataSource(
                default_db,
                symbol="BLEP",
                tick_type="TT",
                start=date,
                end=date + timedelta(days=1),
                schema_policy=self.policy,
                schema={"Q": str},
            )

    def test_wrong_type(self, default_db):
        with pytest.raises(ValueError, match="has type .* but .* was requested"):
            otp.DataSource(
                default_db,
                symbol="BLEP",
                tick_type="TT",
                start=date,
                end=date + timedelta(days=1),
                schema_policy=self.policy,
                schema={"X": str},
            )

    def test_full_schema(self, default_db):
        src = otp.DataSource(
            default_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
            schema=default_schema
        )
        validate_schema_is(src, default_schema)

    def test_cross_symbol_conflict(self, mismatched_symbol_db):
        src = otp.DataSource(
            mismatched_symbol_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
        )
        ''' The database have different schemas for two different symbols for the same
        tick type, however OneTick returns the first written schema. Therefor this
        case could lead to runtime problems, but it seems as misuse '''
        validate_schema_is(src, default_schema)

    def test_cross_symbol_conflict_with_default_schema(self, mismatched_symbol_db):
        with pytest.raises(ValueError, match='schema has no .* field'):
            otp.DataSource(
                mismatched_symbol_db,
                symbol="BLEP",
                tick_type="TT",
                start=date,
                end=date + timedelta(days=1),
                schema_policy=self.policy,
                schema={"X": int, "F": str},  # The `F` field no in the tick schema
            )

    def test_cross_symbol_match(self, compatible_symbol_db):
        ''' Checked that the schema is a union across all symbols '''
        src = otp.DataSource(
            compatible_symbol_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
        )
        validate_schema_is(src,
                           dict(X=int, Y=float, Z=str, T=otp.nsectime,
                                A=otp.string[1000], Q=str))

    def test_no_tick_type_for_latest_date(self, delayed_data_db):
        src = otp.DataSource(
            delayed_data_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=delayed_date,
            schema_policy=self.policy
        )
        validate_schema_is(src, default_schema)

    def test_decayed_data(self, decayed_data_db):
        ''' No data for a chosen date range, and therefore no schema there '''
        with pytest.warns(match="Can't find not empty day"):
            src = otp.DataSource(
                decayed_data_db,
                symbol="BLEP",
                tick_type="TT",
                date=decayed_date,
                schema_policy=self.policy
            )
        validate_schema_is(src, {})


class TestSchemaFail:
    policy = "fail"

    def test_empty_schema(self, default_db):
        src = otp.DataSource(
            default_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
        )
        validate_schema_is(src, default_schema)

    def test_subset(self, default_db):
        src = otp.DataSource(
            default_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
            schema={"X": int},
        )
        validate_schema_is(src, default_schema)

    def test_extra_field(self, default_db):
        with pytest.raises(ValueError, match="has no .* field"):
            otp.DataSource(
                default_db,
                symbol="BLEP",
                tick_type="TT",
                start=date,
                end=date + timedelta(days=1),
                schema_policy=self.policy,
                schema={"Q": str},
            )

    def test_wrong_type(self, default_db):
        with pytest.raises(ValueError, match="has type .* but .* was requested"):
            otp.DataSource(
                default_db,
                symbol="BLEP",
                tick_type="TT",
                start=date,
                end=date + timedelta(days=1),
                schema_policy=self.policy,
                schema={"X": str},
            )

    def test_full_schema(self, default_db):
        src = otp.DataSource(
            default_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
            schema=default_schema
        )
        validate_schema_is(src, default_schema)

    def test_cross_symbol_conflict(self, mismatched_symbol_db):
        src = otp.DataSource(
            mismatched_symbol_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
        )
        validate_schema_is(src, default_schema)

    def test_cross_symbol_match(self, compatible_symbol_db):
        ''' The union of schemas '''
        src = otp.DataSource(
            compatible_symbol_db,
            symbol="BLEP",
            tick_type="TT",
            start=date,
            end=date + timedelta(days=1),
            schema_policy=self.policy,
        )
        resulting_schema = default_schema.copy()
        resulting_schema.update(Q=str)
        validate_schema_is(src, resulting_schema)

    def test_no_tick_type_for_latest_date(self, delayed_data_db):
        src = otp.DataSource(
            delayed_data_db, symbol="BLEP", tick_type="TT", start=date, end=delayed_date, schema_policy=self.policy
        )
        validate_schema_is(src, default_schema)

    def test_decayed_data(self, decayed_data_db):
        with pytest.raises(Exception, match="No ticks found in database"):
            otp.DataSource(
                decayed_data_db,
                symbol="BLEP",
                tick_type="TT",
                date=decayed_date,
                schema_policy=self.policy,
            )


class TestProperties:
    def test_db(self, mismatched_symbol_db):
        src = otp.DataSource(db="MISMATCHED_DB", schema_policy="manual")
        assert src.db == ["MISMATCHED_DB::TT"]

    def test_tick_type(self, mismatched_symbol_db):
        src = otp.DataSource(db=mismatched_symbol_db, tick_type="ORDER", schema_policy="manual")
        assert src.db == ["MISMATCHED_DB::ORDER"]


def test_query_as_symbols(session, par_dir):
    """ validates that otp.query and otp.sources. Query can play
        a role of a first stage query to the merge, see PY-203
    """
    db = otp.DB('MY_DB')
    db.add(otp.Tick(X=1), symbol="A", tick_type="TRD")
    db.add(otp.Tick(X=2), symbol="A'", tick_type="TRD")
    session.use(db)
    q = otp.query(par_dir + "otqs" + "get_symbols.otq",
                  TT="TRD",
                  DB=db.name,
                  PATTERN="A'")
    data1 = otp.DataSource(db=db, symbol=otp.Query(q, symbol=None), identify_input_ts=True)
    data2 = otp.DataSource(db=db, symbol=q, identify_input_ts=True)
    data2.to_otq(otp.utils.TmpFile("_main.otq").path)
    df1 = otp.run(data1)
    df2 = otp.run(data2)
    assert len(df1)
    assert df1.equals(df2)


class TestWithSymbolsObj:

    def test_without_pattern(self, compatible_symbol_db):
        data = otp.DataSource(db=compatible_symbol_db,
                              date=date,
                              symbols=otp.Symbols())

        res = otp.run(data)

        assert len(res) == 2
        assert all(res['X'] == [1, 3])

    @pytest.mark.parametrize('pattern,exp_val', [('B%', 1), ('S%', 3)])
    def test_with_pattern(self, compatible_symbol_db, pattern, exp_val):
        data = otp.DataSource(db=compatible_symbol_db,
                              date=date,
                              symbols=otp.Symbols(pattern=pattern))
        res = otp.run(data)

        assert len(res) == 1
        assert all(res['X'] == [exp_val])

    def test_graph_query(self, compatible_symbol_db):
        graph_query = otq.GraphQuery(otq.FindDbSymbols(pattern='%').symbol(f'{compatible_symbol_db}::'))
        data = otp.DataSource(db=compatible_symbol_db,
                              date=date,
                              symbols=graph_query)

        res = otp.run(data)

        assert len(res) == 2
        assert all(res['X'] == [1, 3])


class TestDbTTSymbolsMix:

    def test_different_schemas(self, mismatched_symbol_db, compatible_symbol_db):
        ''' BLEP and SYMBP has incompatible tick schemas '''
        res = otp.DataSource(db='MISMATCHED_DB::TT',
                             symbol=['BLEP', 'SYMB'],
                             date=date)
        match_string = "Field 'Y' has data type"
        if otq.webapi:
            match_string = "Field Y has incompatible types"
        with pytest.raises(Exception, match=match_string):
            otp.run(res)

    def test_tick_type_and_single_db(self):
        ''' The tick type can't be set in the db and separetely simultaniously '''
        with pytest.raises(Exception, match="The `tick_type` is set as a parameter"):
            otp.DataSource(tick_type='QTE', db='MISMATCHED_DB::TT')

    def test_tick_type_and_multiple_dbs(self):
        with pytest.raises(Exception, match="The `tick_type` is set as a parameter"):
            otp.DataSource(tick_type='QTE', db=['MISMATCHED_DB::TT', 'COMPATIBLE_DB::TT'])

    @pytest.mark.parametrize('tt', [None])  # otp.utils.adaptive - is for default tick type
    def test_multiple_dbs_without_tt(self, tt):
        with pytest.raises(Exception, match="The tick type is not set for databases"):
            otp.DataSource(tick_type=tt, db=['MISMATCHED_DB', 'COMPATIBLE_DB::'])

    def test_multiple_dbs_and_tt_separately(self):
        ''' Check the case when databases don't have specified tick type, but tick type
        is passed as a parameter '''
        data = otp.DataSource(tick_type='TT',
                              db=['MISMATCHED_DB', 'COMPATIBLE_DB::'],
                              symbol='SYMB',
                              date=date)

        df = otp.run(data)
        assert len(df) == 2
        assert all(df['X'] == [3, 3])

    def test_multiple_dbs_with_tick_types_1(self):
        ''' Check with the same tick types '''
        data = otp.DataSource(db=['MISMATCHED_DB::TT', 'COMPATIBLE_DB::TT'],
                              symbol='SYMB',
                              date=date)

        df = otp.run(data)
        assert len(df) == 2
        assert all(df['X'] == [3, 3])

    def test_multile_dbs_with_tick_types_2(self, delayed_data_db):
        ''' Check with different tick types '''
        data = otp.DataSource(db=['DELAYED_DB::AA', 'DELAYED_DB::TT'],
                              symbol=['SYMB', 'BLEP'],
                              start=date,
                              end=date + timedelta(days=2))
        df = otp.run(data)
        assert len(df) == 2
        assert all(df['X'] == [1, 0])

    def test_multile_dbs_with_tick_types_3(self, delayed_data_db):
        ''' Check with different tick types, but when tick_type is set to None '''
        data = otp.DataSource(db=['DELAYED_DB::AA', 'DELAYED_DB::TT'],
                              tick_type=None,
                              symbol=['SYMB', 'BLEP'],
                              start=date,
                              end=date + timedelta(days=2))
        df = otp.run(data)
        assert len(df) == 2
        assert all(df['X'] == [1, 0])

    def test_multiple_dbs_with_partial_tick_types(self):
        data = otp.DataSource(db=['MISMATCHED_DB::TT', 'COMPATIBLE_DB'],
                              tick_type='TT',
                              symbol='SYMB',
                              date=date)

        df = otp.run(data)
        assert len(df) == 2
        assert all(df['X'] == [3, 3])

    def test_single_db_and_positive_auto_tick_type(self):
        data = otp.DataSource(db='MISMATCHED_DB',
                              symbol='SYMB',
                              date=date)

        df = otp.run(data)
        assert len(df) == 1
        assert all(df['X'] == 3)

    def test_single_db_no_tick_type_for_date(self, mismatched_symbol_db):
        ''' Case of no data, but it shouldn't fail due the `tolerant` policy '''
        with pytest.warns(match="Can't find not empty day"):
            res = otp.DataSource(db=mismatched_symbol_db,
                                 symbol='SYMB',
                                 date=date + timedelta(days=1))

        assert otp.run(res).empty

    def test_single_db_multiple_tick_types(self, delayed_data_db):
        with pytest.raises(Exception, match='It seems that there is no common'):
            otp.DataSource(db='DELAYED_DB',
                           symbol=['SYMB', 'BLEP'],
                           start=date,
                           end=date + timedelta(days=1))

    def test_db_comes_with_symbol_invalid(self):
        with pytest.raises(Exception, match='The `db` is not specified that means'):
            otp.DataSource(symbol='MISMATCHED_DB::SYMB',
                           date=date)

    def test_db_comes_with_symbol_valid(self):
        data = otp.DataSource(tick_type='TT',
                              symbol='MISMATCHED_DB::SYMB',
                              date=date,
                              schema_policy='manual')  # because database is not set
        df = otp.run(data)
        assert len(df) == 1


class TestCopy:
    def test_inheritance(self, default_db):
        data = otp.DataSource(default_db)
        for d in [data, data.copy(), data.deepcopy()]:
            assert isinstance(d, otp.DataSource)

    def test_properties(self, default_db):
        data = otp.DataSource(default_db)
        properties = set(otp.DataSource._PROPERTIES) - set(otp.Source._PROPERTIES)
        properties = {
            property: getattr(data, property)
            for property in properties
        }
        copy = data.copy()
        for property, value in properties.items():
            assert getattr(copy, property) == value

    def test_try_default_constructor(self, default_db):
        class MyCustom(otp.DataSource):
            pass

        MyCustom(default_db).copy()


def test_with_symbol_param_multiple_dbs(mismatched_symbol_db, compatible_symbol_db):
    sym = otp.Ticks({'SYMBOL_NAME': ['BLEP'],
                    'DB': ['MISMATCHED_DB::TT+COMPATIBLE_DB::TT']})
    sym_p = sym.to_symbol_param()
    data = otp.DataSource(db=sym_p['DB'], tick_type=None)
    otp.run(data, symbols=sym)


def test_update_schema_with_db_as_symbol_param(compatible_symbol_db):
    sym = otp.Ticks({'SYMBOL_NAME': ['BLEP'], 'DB': ['COMPATIBLE_DB::TT']})
    sym_p = sym.to_symbol_param()
    data = otp.DataSource(db=sym_p['DB'], tick_type=None)
    _ = data.db
    data.schema.update(**{'A': int})
    _ = data.db
    data = data.copy()
    otp.run(data, symbols=sym)


@pytest.mark.parametrize('back_to_first_tick', [
    "expr(case(_TICK_TYPE, 'TT', 60, 0))",
    'expr(%s)' % otp.Empty().apply(lambda row: 60 if row['_TICK_TYPE'] == 'TT' else 0),
    'expr(%s)' % otp.Source.meta_fields.tick_type.apply(lambda tt: 60 if tt == 'TT' else 0),
    otp.Source.meta_fields.tick_type.apply(lambda tt: 60 if tt == 'TT' else 0).expr,
    otp.meta_fields['TICK_TYPE'].apply(lambda tt: 60 if tt == 'TT' else 0),
])
def test_back_to_first_tick_expr(ahead_data_db, back_to_first_tick):
    data = otp.DataSource(
        ahead_data_db,
        symbol="BLEP",
        tick_type="TT",
        start=date,
        end=date + timedelta(days=1),
        back_to_first_tick=back_to_first_tick,
    )
    df = otp.run(data)
    assert len(df) == 1


@pytest.mark.parametrize('keep_first_tick_timestamp', [None, 'FIRST_TICK_TIMESTAMP'])
@pytest.mark.parametrize('value,expected', [
    (60, 1),
    (0, 0),
])
def test_back_to_first_tick(keep_first_tick_timestamp, ahead_data_db, value, expected):
    data = otp.DataSource(
        ahead_data_db,
        symbol="BLEP",
        tick_type="TT",
        start=date,
        end=date + timedelta(days=1),
        back_to_first_tick=value,
        keep_first_tick_timestamp=keep_first_tick_timestamp,
    )
    df = otp.run(data)
    assert len(df) == expected
    if df.empty:
        return
    if keep_first_tick_timestamp and value != 0:
        assert keep_first_tick_timestamp in df
        assert keep_first_tick_timestamp in data.schema
        if len(df) > 0:
            assert df['Time'][0] != df[keep_first_tick_timestamp][0]
    else:
        assert keep_first_tick_timestamp not in df


@pytest.mark.parametrize('keep_first_tick_timestamp', [None, 'FIRST_TICK_TIMESTAMP'])
@pytest.mark.parametrize('value,expected', [
    (0, 1),
    (otp.Second(59), 1),
    (60, 2),
    (otp.Milli(60000), 2),
    (otp.Second(60), 2),
    (otp.Minute(1), 2),
    (otp.Hour(1), 2),
    (otp.Day(1), 2),
])
def test_back_to_first_tick_multidb(keep_first_tick_timestamp, default_db, ahead_data_db, value, expected):
    data = otp.DataSource(
        [default_db, ahead_data_db],
        symbol="BLEP",
        tick_type="TT",
        start=date,
        end=date + timedelta(days=1),
        back_to_first_tick=value,
        keep_first_tick_timestamp=keep_first_tick_timestamp,
    )
    df = otp.run(data)
    assert len(df) == expected
    if df.empty:
        return
    if keep_first_tick_timestamp and value != 0:
        assert keep_first_tick_timestamp in df
        assert keep_first_tick_timestamp in data.schema
        if len(df) > 1:
            assert df['Time'][0] != df[keep_first_tick_timestamp][0]
    else:
        assert keep_first_tick_timestamp not in df


@pytest.mark.parametrize('keep_first_tick_timestamp', [None, 'FIRST_TICK_TIMESTAMP'])
@pytest.mark.parametrize('value,expected', [
    (60, 3),
    (0, 2),
])
def test_back_to_first_tick_multiple_symbol(
    keep_first_tick_timestamp, ahead_data_db, compatible_symbol_db, value, expected
):
    data = otp.DataSource(
        [ahead_data_db, compatible_symbol_db],
        symbols=["BLEP", "SYMB"],
        tick_type="TT",
        start=date,
        end=date + timedelta(days=1),
        back_to_first_tick=value,
        keep_first_tick_timestamp=keep_first_tick_timestamp,
        identify_input_ts=True,
    )
    df = otp.run(data)
    assert len(df) == expected
    if df.empty:
        return
    if keep_first_tick_timestamp and value != 0:
        assert keep_first_tick_timestamp in df
        assert keep_first_tick_timestamp in data.schema
        if len(df) > 2:
            assert df['Time'][0] != df[keep_first_tick_timestamp][0]
    else:
        assert keep_first_tick_timestamp not in df


@pytest.mark.parametrize('keep_first_tick_timestamp', [None, 'FIRST_TICK_TIMESTAMP'])
@pytest.mark.parametrize('value,expected', [
    (0, 0),
    (otp.Second(59), 0),
    (60, 1),
    (otp.Milli(60000), 1),
    (otp.Second(60), 1),
    (otp.Minute(1), 1),
    (otp.Hour(1), 1),
    (otp.Day(1), 1),
])
def test_db_in_symbol_branch(keep_first_tick_timestamp, ahead_data_db, value, expected):
    data = otp.DataSource(tick_type='TT',
                          symbol='AHEAD_DB::BLEP',
                          date=date,
                          end=date + timedelta(days=1),
                          schema_policy='manual',
                          back_to_first_tick=value,
                          keep_first_tick_timestamp=keep_first_tick_timestamp)
    df = otp.run(data)
    assert len(df) == expected
    if df.empty:
        return
    if keep_first_tick_timestamp and value != 0:
        assert keep_first_tick_timestamp in df
        assert keep_first_tick_timestamp in data.schema
        if len(df) > 1:
            assert df['Time'][0] != df[keep_first_tick_timestamp][0]
    else:
        assert keep_first_tick_timestamp not in df


class TestManyTickTypes:
    def test_one_tick_type(self, many_tick_types_db):
        data = otp.DataSource(
            tick_type='TT1', symbol=otp.eval(otp.Symbols(many_tick_types_db, keep_db=True))
        )
        df = otp.run(data, start=date, end=date + otp.Day(1))
        assert isinstance(df, pd.DataFrame)
        assert df[list(default_schema)].equals(otp.run(default_data)[list(default_schema)])

    def test_one_db_none_tick_type(self, many_tick_types_db):
        with pytest.raises(Exception):
            otp.DataSource(db=many_tick_types_db, symbol='BLEP')

    def test_many_db_tick_types(self, many_tick_types_db):
        data = otp.DataSource(
            db=['MANY_TICK_TYPES_DB::TT1', 'MANY_TICK_TYPES_DB::TT2'], symbol='BLEP'
        )
        df = otp.run(data, start=date, end=date + otp.Day(1))
        assert len(df) == 2
        default_df = otp.run(default_data)
        expected_df = pd.concat([default_df, default_df]).reset_index()
        pd.testing.assert_frame_equal(df[list(default_schema)], expected_df[list(default_schema)])

    def test_db_many_tick_types(self, many_tick_types_db):
        data = otp.DataSource(
            db=many_tick_types_db, tick_type=['TT1', 'TT2'], symbol='BLEP'
        )
        df = otp.run(data, start=date, end=date + otp.Day(1))
        assert len(df) == 2
        default_df = otp.run(default_data)
        expected_df = pd.concat([default_df, default_df]).reset_index()
        pd.testing.assert_frame_equal(df[list(default_schema)], expected_df[list(default_schema)])

    def test_many_tick_types(self, many_tick_types_db):
        data = otp.DataSource(
            tick_type=['TT1', 'TT2'], symbol=otp.eval(otp.Symbols(many_tick_types_db, keep_db=True))
        )
        df = otp.run(data, start=date, end=date + otp.Day(1))
        assert len(df) == 2
        default_df = otp.run(default_data)
        expected_df = pd.concat([default_df, default_df]).reset_index()
        pd.testing.assert_frame_equal(df[list(default_schema)], expected_df[list(default_schema)])


class TestEmptyDay:
    def test_schema(self, default_db):
        with pytest.raises(Exception):
            otp.DataSource(default_db, symbol='BLEP', tick_type='TT',
                           schema_policy='fail', date=date + timedelta(days=3))

        data = otp.DataSource(default_db, symbol='BLEP', tick_type='TT',
                              schema_policy='tolerant', date=date + timedelta(days=3))
        assert set(default_schema) == set(data.schema)
        df = otp.run(data)
        assert df.empty

    def test_before_locator(self, empty_db):
        db = otp.databases()[str(empty_db)]
        _ = db.last_not_empty_date(otp.core.db_constants.DEFAULT_START_DATE, days_back=5)
        data = otp.DataSource(empty_db, symbol='BLEP', tick_type='TT',
                              schema_policy='tolerant',
                              date=otp.core.db_constants.DEFAULT_START_DATE)
        df = otp.run(data)
        assert df.empty


class TestModifyQueryTimes:
    @pytest.fixture(scope='module')
    def mqt_db(self, session):
        db = otp.DB('MQT_DB')
        db.add(otp.Ticks(X=[1]), date=otp.dt(2022, 12, 1), symbol='A')
        db.add(otp.Ticks(X=[2]), date=otp.dt(2022, 12, 2), symbol='B')
        db.add(otp.Ticks(X=[3]), date=otp.dt(2022, 12, 3), symbol='C')
        session.use(db)
        yield db

    def test_simple(self, mqt_db):
        symbols = otp.Symbols('MQT_DB', start=otp.dt(2022, 12, 1), end=otp.dt(2022, 12, 4))
        df = otp.run(symbols)
        assert list(df['SYMBOL_NAME']) == ['A', 'B', 'C']

    def test_unbound(self, mqt_db):
        symbols = otp.Symbols('MQT_DB', start=otp.dt(2022, 12, 1), end=otp.dt(2022, 12, 2))

        data = otp.Tick(X=1)
        data['S'] = data.Symbol.name

        result = otp.run(data, symbols=symbols, start=otp.dt(2022, 12, 1), end=otp.dt(2022, 12, 4))
        assert list(result) == ['A']
        df = result['A']
        assert list(df['X']) == [1]
        assert list(df['S']) == ['A']

    def test_merge(self, mqt_db):
        symbols = otp.Symbols('MQT_DB', start=otp.dt(2022, 12, 1), end=otp.dt(2022, 12, 4))

        data = otp.Tick(X=1)
        data['S'] = data.Symbol.name
        res = otp.merge([data], symbols=symbols)

        df = otp.run(res, start=otp.dt(2022, 12, 3), end=otp.dt(2022, 12, 4))
        assert list(df['X']) == [1, 1, 1]
        assert list(df['S']) == ['A', 'B', 'C']

    def test_merge_sub_range(self, mqt_db):
        symbols = otp.Symbols('MQT_DB', start=otp.dt(2022, 12, 2), end=otp.dt(2022, 12, 3))

        data = otp.Tick(X=1)
        data['S'] = data.Symbol.name
        res = otp.merge([data], symbols=symbols)

        df = otp.run(res, start=otp.dt(2022, 12, 1), end=otp.dt(2022, 12, 4))
        assert list(df['X']) == [1]
        assert list(df['S']) == ['B']

    def test_merge_non_overlapping_ranges(self, mqt_db):
        symbols = otp.Symbols('MQT_DB', start=otp.dt(2022, 12, 3), end=otp.dt(2022, 12, 4))

        data = otp.Tick(X=1)
        data['S'] = data.Symbol.name
        res = otp.merge([data], symbols=symbols)

        df = otp.run(res, start=otp.dt(2022, 12, 20), end=otp.dt(2022, 12, 30))
        assert list(df['X']) == [1]
        assert list(df['S']) == ['C']

    def test_multiple_sources_dates(self, mqt_db):
        s1 = otp.Symbols('MQT_DB', start=otp.dt(2022, 12, 1), end=otp.dt(2022, 12, 2))
        s2 = otp.Symbols('MQT_DB', start=otp.dt(2022, 12, 3), end=otp.dt(2022, 12, 4))
        symbols = otp.merge([s1, s2])

        data = otp.Tick(X=1)
        data['S'] = data.Symbol.name
        res = otp.merge([data], symbols=symbols)

        df = otp.run(res, start=otp.dt(2022, 12, 20), end=otp.dt(2022, 12, 30))
        assert list(df['X']) == [1, 1]
        assert list(df['S']) == ['A', 'C']

    def test_eval(self, mqt_db):
        def fsq():
            return otp.Symbols('MQT_DB', start=otp.dt(2022, 12, 1), end=otp.dt(2022, 12, 4))

        data = otp.Tick(X=1)
        data.state_vars['SET'] = otp.state.tick_set('latest', 'SYMBOL_NAME', otp.eval(fsq))
        data = data.state_vars['SET'].dump()
        df = otp.run(data, start=otp.dt(2022, 12, 3), end=otp.dt(2022, 12, 4))
        assert list(df['SYMBOL_NAME']) == ['A', 'B', 'C']


class TestStrict:
    @pytest.mark.parametrize('schema_policy,exception', [
        ('manual', False),
        ('tolerant', True),
        ('fail', True),
        ('manual_strict', False),
        ('tolerant_strict', True),
        ('fail_strict', True),
    ])
    def test_field_not_in_database_exception(self, default_db, schema_policy, exception):
        desired_schema = {
            'X': int,
            'NOT_IN_DB': int,
        }
        if exception:
            with pytest.raises(ValueError):
                otp.DataSource(default_db, symbol='BLEP', tick_type='TT',
                               start=date, end=date + timedelta(days=1),
                               schema_policy=schema_policy,
                               schema=desired_schema)
            return

        data = otp.DataSource(default_db, symbol='BLEP', tick_type='TT',
                              start=date, end=date + timedelta(days=1),
                              schema_policy=schema_policy,
                              schema=desired_schema)
        df = otp.run(data)
        assert data.schema['NOT_IN_DB'] is int
        assert df.dtypes['NOT_IN_DB'] == np.int64
        assert df['NOT_IN_DB'][0] == 0

    @pytest.mark.parametrize('schema_policy', [
        'manual_strict', 'tolerant_strict', 'fail_strict',
    ])
    def test_strict(self, default_db, schema_policy):
        desired_schema = {
            'X': int,
            'Y': float,
        }
        data = otp.DataSource(default_db, symbol='BLEP', tick_type='TT',
                              start=date, end=date + timedelta(days=1),
                              schema_policy=schema_policy,
                              schema=desired_schema)
        df = otp.run(data)
        assert set(desired_schema) == set(data.schema)
        assert data.schema['X'] is int
        assert data.schema['Y'] is float
        assert set(df) - {'Time'} == set(data.schema)
        assert df.dtypes['X'] == np.int64
        assert df.dtypes['Y'] == np.float64
        assert df['X'][0] == 1
        assert df['Y'][0] == 0.5

    @pytest.mark.parametrize('schema_policy', [
        'manual', 'tolerant', 'fail',
    ])
    def test_not_strict(self, default_db, schema_policy):
        desired_schema = {
            'X': int,
            'Y': float,
        }
        data = otp.DataSource(default_db, symbol='BLEP', tick_type='TT',
                              start=date, end=date + timedelta(days=1),
                              schema_policy=schema_policy,
                              schema=desired_schema)
        df = otp.run(data)
        if schema_policy == 'manual':
            # manual policy doesn't check database's schema at all
            assert set(desired_schema) == set(data.schema)
        else:
            assert set(default_schema) == set(data.schema)
        assert data.schema['X'] is int
        assert data.schema['Y'] is float
        assert set(df) - {'Time'} == set(default_schema)
        assert df.dtypes['X'] == np.int64
        assert df.dtypes['Y'] == np.float64
        assert df['X'][0] == 1
        assert df['Y'][0] == 0.5

    def test_last_not_empty_day_is_the_same_tick_type(self, different_tt_different_dates_db):
        data = otp.DataSource(different_tt_different_dates_db, symbols=['SYMB'], tick_type='AA')
        assert data.schema == {'X': int}

    def test_last_not_empty_day_is_another_tick_type(self, different_tt_different_dates_db):
        data = otp.DataSource(different_tt_different_dates_db, symbols=['SYMB'], tick_type='TT')
        assert data.schema == {
            'A': otp.string[1000],
            'T': otp.nsectime,
            'X': int,
            'Y': float,
            'Z': str,
        }

    def test_fields_from_parameters(self, default_db):
        data = otp.DataSource(default_db, symbol='BLEP', tick_type='TT',
                              start=date, end=date + timedelta(days=1),
                              schema_policy='manual_strict',
                              identify_input_ts=True,
                              back_to_first_tick=otp.Day(1),
                              keep_first_tick_timestamp='ORIG_TS',
                              schema={"X": int})
        assert set(data.schema) == {'X', 'SYMBOL_NAME', 'TICK_TYPE', 'ORIG_TS'}
        df = otp.run(data)
        assert 'SYMBOL_NAME' in df
        assert 'TICK_TYPE' in df
        assert 'X' in df
        assert 'ORIG_TS' in df
