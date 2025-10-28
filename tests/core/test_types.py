import os
import pytest
import datetime
import zoneinfo

import numpy as np
import pandas as pd

import onetick.py as otp
from onetick.py.compatibility import has_timezone_parameter
import onetick.py.types as ott


def test_nsectime():
    assert issubclass(ott.nsectime, int)
    assert not issubclass(ott.nsectime, ott.msectime)
    assert str(ott.nsectime) == "nsectime"


def test_msectime():
    assert issubclass(ott.msectime, int)
    assert not issubclass(ott.msectime, ott.nsectime)
    assert str(ott.msectime) == "msectime"


def test_nsectime_as_column(session):
    t = otp.Tick(A=1)
    t['T1'] = otp.msectime(1)
    with pytest.warns(match='milliseconds as nanoseconds'):
        t['T2'] = otp.nsectime(1)
    df = otp.run(t, timezone='GMT')
    assert df['T1'][0] == pd.Timestamp(1970, 1, 1, 0, 0, 0, 1000)
    # TODO: change to nanoseconds after PY-441
    assert df['T2'][0] == pd.Timestamp(1970, 1, 1, 0, 0, 0, 1000)


def test_nsectime_naive(session):
    t = otp.Tick(A=1)
    t['T1'] = otp.msectime(1)
    with pytest.warns(match='milliseconds as nanoseconds'):
        t['T2'] = otp.nsectime(1)
    df = otp.run(t, timezone='EST5EDT')
    assert df['T1'][0] == pd.Timestamp(1969, 12, 31, 19, 0, 0, 1000)
    # TODO: change to nanoseconds after PY-441
    assert df['T2'][0] == pd.Timestamp(1969, 12, 31, 19, 0, 0, 1000)


def test_strings():
    assert issubclass(ott.string[10], str)
    assert issubclass(ott.string[1024], str)

    assert not issubclass(ott.string[10], ott.string[1024])
    assert ott.string[10] is not ott.string[1024]

    assert str(ott.string[10]) == "string[10]"
    assert str(ott.string[1024]) == "string[1024]"
    assert str(ott.string[99]) == "string[99]"
    assert str(ott.string) == "string"

    assert ott.string[10](5) == "5"
    assert ott.string[55](3.14159265) == "3.14159265"
    assert ott.string[1024]("abc Def=?") == "abc Def=?"

    with pytest.raises(TypeError):
        ott.string[0]

    with pytest.raises(TypeError):
        ott.string[-1]

    with pytest.raises(TypeError):
        ott.string["abc"]

    assert len(ott.string[1024]("")) == 1024
    assert len(ott.string[99]("abc")) == 99


class TestScienceFloatNotation:
    # PY-286
    def test_small_float(self, session):
        data = otp.Tick(X=0.000_000_001)
        df = otp.run(data)
        assert all(df["X"] == 1e-9)

    def test_ticks_init(self, session):
        data = otp.Ticks(X=[10 ** -12, 10 ** 12])
        data["Y"] = 10 ** -12
        df = otp.run(data)
        assert all(df["X"] == [1e-12, 1e12])
        assert all(df["Y"] == [1e-12, 1e-12])

    def test_operations(self, session):
        data = otp.Ticks(X=[1.5, 1.5])
        data["Y"] = data["X"] + 1e-12
        data["X"] *= 1e-12
        df = otp.run(data)
        assert all(df["X"] == [1.5 * 10 ** -12, 1.5 * 10 ** -12])
        assert all(df["Y"] == [1.5 + 10 ** -12, 1.5 + 10 ** -12])


class TestInf:
    def test_in_declaration(self, session):
        data = otp.Ticks(dict(X=[1000.0, 1.0, 5.0, 100.0, -100.0, otp.inf]))
        df = otp.run(data)
        assert all(df["X"] == [1000.0, 1.0, 5.0, 100.0, -100.0, float("inf")])
        data = data.high(data["X"], n=2)
        df = otp.run(data)
        assert all(df["X"] == [1000.0, float("inf")])

    def test_neg_inf_in_declaration(self, session):
        data = otp.Ticks(dict(X=[1000.0, 1.0, 5.0, 100.0, -100.0, -otp.inf]))
        df = otp.run(data)
        assert all(df["X"] == [1000.0, 1.0, 5.0, 100.0, -100.0, float("-inf")])
        data = data.low(data["X"], n=2)
        df = otp.run(data)
        assert all(df["X"] == [-100.0, float("-inf")])

    def test_both(self, session):
        data = otp.Ticks(dict(X=[otp.inf, -otp.inf, 5.0, otp.inf, -100.0, -otp.inf]))
        df = otp.run(data)
        assert all(df["X"] == [float("inf"), float("-inf"), 5.0, float("inf"), -100.0, float("-inf")])
        low = data.low(data["X"], n=2)
        df = otp.run(low)
        assert all(df["X"] == [float("-inf"), float("-inf")])
        high = data.high(data["X"], n=2)
        df = otp.run(high)
        assert all(df["X"] == [float("inf"), float("inf")])

    def test_where_eq(self, session):
        data = otp.Ticks(dict(X=["a", "b", "c"], Y=[otp.inf, 1, -otp.inf]))
        data, _ = data[data["Y"] == -otp.inf]
        df = otp.run(data)
        assert all(df["Y"] == [float("-inf")])
        assert all(df["X"] == ["c"])

    def test_schema(self, session):
        t = otp.Tick(INF=otp.inf, NAN=otp.nan)
        t['INF2'] = otp.inf
        t['NAN2'] = otp.nan
        assert t.schema['INF'] is float
        assert t.schema['INF2'] is float
        assert t.schema['NAN'] is float
        assert t.schema['NAN2'] is float


@pytest.mark.parametrize('value, result',
                         [('abc', '"abc"'),
                          ("--> ' <--", '''"--> ' <--"'''),
                          ('--> " <--', '''"--> "+'"'+" <--"'''),
                          (1641819624975907072, '1641819624975907072'),
                          (1641819624975, '1641819624975'),
                          (0.1, '0.1'),
                          (5.3139e+3, '5313.9'),
                          (otp.nan, 'NAN()'),
                          (otp.inf, 'INFINITY()'),
                          (otp.now(), 'NOW()'),
                          (12345, '12345'),
                          (otp.core.column._Column('NAME', int), 'NAME'),
                          (otp.core.column._Column('NAME', float), 'NAME'),
                          (otp.core.column._Column('NAME', str), 'NAME'),
                          (otp.core.column._Column('NAME', otp.msectime), 'NAME'),
                          (otp.core.column._Column('NAME', otp.nsectime), 'NAME')])
def test_value2str(value, result):
    assert ott.value2str(value) == result


dt_pattern = 'PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J", "{}", _TIMEZONE)'


@pytest.mark.filterwarnings("ignore:.*milliseconds as nanoseconds.*")
@pytest.mark.parametrize('value,result', [
    (pd.Timestamp(2003, 12, 1, 16, 5, 7, 431678), dt_pattern.format('2003-12-01 16:05:07.431678000')),
    (datetime.datetime(2003, 12, 1, 16, 5, 7, 431678), dt_pattern.format('2003-12-01 16:05:07.431678000')),
    (datetime.date(2003, 12, 1), dt_pattern.format('2003-12-01 00:00:00.000000000')),
    (ott.dt(2003, 12, 1, 16, 5, 7, 431678, 922), dt_pattern.format('2003-12-01 16:05:07.431678922')),
    (ott.date(2003, 12, 1), dt_pattern.format('2003-12-01 00:00:00.000000000')),
    (otp.nsectime(1641819624975907072), 'NSECTIME(1641819624975907072)'),
    # TODO: change to nanoseconds after PY-441
    (otp.nsectime(1641819624975), '1641819624975'),
    (otp.msectime(123456789), '123456789'),
])
def test_value2str_datetime(value, result):
    assert ott.value2str(value) == result


@pytest.mark.parametrize('dt,tz,tz_naive,result', [
    (otp.datetime(2003, 12, 1, 16, 5, 7, 431678, 922), None, None,
     'PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J", "2003-12-01 16:05:07.431678922", _TIMEZONE)'),
    (otp.datetime(2003, 12, 1, 16, 5, 7, 431678, 922, tz='EST5EDT'), None, None,
     'PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J", "2003-12-01 16:05:07.431678922", "EST5EDT")'),
    (otp.datetime(2003, 12, 1, 16, 5, 7, 431678, 922), 'GMT', None,
     'PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J", "2003-12-01 16:05:07.431678922", "GMT")'),
    (otp.datetime(2003, 12, 1, 16, 5, 7, 431678, 922, tz='EST5EDT'), 'GMT', None,
     'PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J", "2003-12-01 16:05:07.431678922", "GMT")'),
    (otp.datetime(2003, 12, 1, 16, 5, 7, 431678, 922), None, 'GMT',
     'PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J", "2003-12-01 16:05:07.431678922", "GMT")'),
    (otp.datetime(2003, 12, 1, 16, 5, 7, 431678, 922, tz='EST5EDT'), None, 'GMT',
     'PARSE_NSECTIME("%Y-%m-%d %H:%M:%S.%J", "2003-12-01 16:05:07.431678922", "EST5EDT")'),
])
def test_datetime2expr(dt, tz, tz_naive, result):
    assert ott.datetime2expr(dt, timezone=tz, timezone_naive=tz_naive) == result


class TestPandasTimestamp:
    """
    pandas.Timestamp.timestamp() method works strange.
    Use pandas.Timestamp.value instead.
    """
    def test_pandas_timestamp(self):
        t1 = otp.datetime(2005, 1, 1, 23, 59, 59, 999999, 999).ts
        t2 = pd.Timestamp(2005, 1, 2)
        assert str(t1) == '2005-01-01 23:59:59.999999999'
        assert str(t2) == '2005-01-02 00:00:00'
        assert t1.timestamp() == t2.timestamp()  # ¯\_(ツ)_/¯
        assert t1.value != t2.value

    def test_timezone(self):
        t1 = pd.Timestamp(2005, 1, 1)
        t2 = pd.Timestamp(2005, 1, 1).tz_localize('EST5EDT')
        assert str(t1) == '2005-01-01 00:00:00'
        assert str(t2) == '2005-01-01 00:00:00-05:00'
        assert t1.timestamp() != t2.timestamp()
        assert t1.value != t2.value

    def test_time2nsectime(self):
        t1 = otp.datetime(2005, 1, 1, 23, 59, 59, 999999, 999).ts
        t2 = pd.Timestamp(2005, 1, 2)
        assert pd.to_datetime(ott.time2nsectime(t1)) == t1
        assert pd.to_datetime(ott.time2nsectime(t2)) == t2

    def test_end(self, session):
        # no less than two ticks are needed
        starts = [otp.datetime(2005, 1, 1), otp.datetime(2005, 1, 2)]
        ends = [s.end for s in starts]
        data = otp.Ticks({'START': starts, 'END': ends})
        df = otp.run(data)
        assert list(df['START']) == starts
        assert list(df['END']) == ends


hour_ns = 60 * 60 * 1_000_000_000


@pytest.mark.parametrize('dt,tz,ns', [
    (pd.Timestamp(2005, 1, 2), None, 1104624000000000000),
    (pd.Timestamp(2005, 1, 2).tz_localize('EST5EDT'), None, 1104624000000000000 + 5 * hour_ns),
    (pd.Timestamp(2005, 1, 2), 'EST5EDT', 1104624000000000000 + 5 * hour_ns),
    (pd.Timestamp(2005, 1, 1, 19).tz_localize('EST5EDT'), 'GMT', 1104624000000000000),
    (otp.datetime(2005, 1, 1, 23, 59, 59, 999999, 999).ts, None, 1104624000000000000 - 1),
    # ---------------------------
    (otp.datetime(2005, 1, 2), None, 1104624000000000000),
    (otp.datetime(2005, 1, 2, tz='EST5EDT'), None, 1104624000000000000 + 5 * hour_ns),
    (otp.datetime(2005, 1, 2), 'EST5EDT', 1104624000000000000 + 5 * hour_ns),
    (otp.datetime(2005, 1, 1, 19, tz='EST5EDT'), 'GMT', 1104624000000000000),
    (otp.datetime(2005, 1, 1, 23, 59, 59, 999999, 999), None, 1104624000000000000 - 1),
    # ---------------------------
    (otp.date(2005, 1, 2), None, 1104624000000000000),
    (otp.date(2005, 1, 2), 'EST5EDT', 1104624000000000000 + 5 * hour_ns),
    # ---------------------------
    (datetime.datetime(2005, 1, 2), None, 1104624000000000000),
    (datetime.datetime(2005, 1, 2, tzinfo=zoneinfo.ZoneInfo('EST5EDT')), None, 1104624000000000000 + 5 * hour_ns),
    (datetime.datetime(2005, 1, 2), 'EST5EDT', 1104624000000000000 + 5 * hour_ns),
    (datetime.datetime(2005, 1, 1, 19, tzinfo=zoneinfo.ZoneInfo('EST5EDT')), 'GMT', 1104624000000000000),
    (datetime.datetime(2005, 1, 1, 23, 59, 59, 999999), None, 1104624000000000000 - 1000),
    # ---------------------------
    (datetime.date(2005, 1, 2), None, 1104624000000000000),
    (datetime.date(2005, 1, 2), 'EST5EDT', 1104624000000000000 + 5 * hour_ns),
])
@pytest.mark.skipif(not has_timezone_parameter(), reason='SURV-1786')
def test_time2nsectime(dt, tz, ns):
    assert ott.time2nsectime(dt, tz) == ns


class TestVarstring:
    @pytest.mark.parametrize('a,b', [
        ('a', 'b'),
        (otp.string('a'), otp.string('b')),
        ('a', otp.string[5]('b')),
        (otp.string[2]('a'), otp.string[5]('b')),
        ('a', otp.varstring('b')),
        (otp.string[2]('a'), otp.varstring('b')),
        (otp.varstring('a'), otp.varstring('b')),
    ])
    def test_merge(self, session, a, b):
        x1 = otp.Tick(X=a)
        x2 = otp.Tick(X=b)
        data = otp.merge([x1, x2])
        df = otp.run(data)
        assert list(df['X']) == ['a', 'b']

    def test_plus(self, session):
        t = otp.Tick(A=otp.varstring('a'))
        t['B'] = t['A'] + 'b'
        assert t.schema['B'] == otp.varstring
        df = otp.run(t)
        assert df['B'][0] == 'ab'

    def test_convert_to_varstring(self, session):
        # converting to varstring should not add null-characters
        t = otp.Tick(A='a')
        t = t.table(A=otp.varstring)
        t['X'] = otp.varstring('a')
        df = otp.run(t)
        assert df['X'][0] == 'a'
        assert df['A'][0] == 'a'


def test_numpy(session):
    t = otp.Tick(I=1, F=2.2, S='ab')
    t['I'] += np.int64(110)
    t['II'] = np.int64(111)
    t['F'] += np.float64(2.2)
    t['FF'] = np.float64(4.4)
    t['IF'] = t['I'] + np.float64(4.4)
    t['FI'] = t['F'] + np.int64(111)
    t['S'] += np.str_('c')
    t['SS'] = np.str_('abc')
    t['D'] = np.datetime64('2022-01-01')
    df = otp.run(t)
    assert t.schema['I'] is int
    assert t.schema['II'] is int
    assert t.schema['F'] is float
    assert t.schema['FF'] is float
    assert t.schema['IF'] is float
    assert t.schema['FI'] is float
    assert t.schema['S'] is str
    assert t.schema['SS'] is str
    assert t.schema['D'] is otp.nsectime
    assert df['I'][0] == 111
    assert df['II'][0] == 111
    assert df['F'][0] == 4.4
    assert df['FF'][0] == 4.4
    assert df['IF'][0] == 115.4
    assert df['FI'][0] == 115.4
    assert df['S'][0] == 'abc'
    assert df['SS'][0] == 'abc'
    assert df['D'][0] == pd.Timestamp(2022, 1, 1)


@pytest.mark.parametrize('cls', (otp.ulong, otp.uint, otp.byte, otp.short, otp.long, otp.int))
def test_integers(session, cls):

    if cls is otp.int:
        assert ott.type2str(cls) == 'int'
        assert str(cls) == 'int'
    else:
        assert ott.type2str(cls) == cls.__name__
        assert str(cls) == cls.__name__

    t = otp.Tick(A1=cls(1))
    t['A2'] = cls(2)
    t['A3'] = cls(3)
    t['A4'] = (1 + cls(1)) + (cls(5) - 4) * (1 * cls(1)) * 2
    t['A5'] = 5.5
    t = t.table(A5=cls, strict=False)
    t['A6'] = t['A1'] + 5

    df = otp.run(t)

    for i in range(1, 7):
        assert t.schema[f'A{i}'] is cls
        assert df[f'A{i}'][0] == i


@pytest.mark.skipif(os.name == 'nt', reason='may be different sizes on windows')
def test_overflow(session):
    with pytest.raises(ValueError):
        otp.ulong(-1)
    with pytest.raises(ValueError):
        otp.ulong(2 ** 64)
    assert otp.ulong(0) - 1 == 2 ** 64 - 1
    assert otp.ulong(2 ** 64 - 1) == 2 ** 64 - 1
    assert otp.ulong(0) + 2 ** 64 == 0

    with pytest.raises(ValueError):
        otp.uint(-1)
    with pytest.raises(ValueError):
        otp.uint(2 ** 32)
    assert otp.uint(0) - 1 == 2 ** 32 - 1
    assert otp.uint(2 ** 32 - 1) == 2 ** 32 - 1
    assert otp.uint(0) + 2 ** 32 == 0

    with pytest.raises(ValueError):
        otp.byte(-129)
    with pytest.raises(ValueError):
        otp.byte(128)
    assert otp.byte(-128) - 1 == 127
    assert otp.byte(-128) == -128
    assert otp.byte(127) == 127
    assert otp.byte(127) + 1 == -128

    with pytest.raises(ValueError):
        otp.short(-32769)
    with pytest.raises(ValueError):
        otp.short(32768)
    assert otp.short(-32768) - 1 == 32767
    assert otp.short(-32768) == -32768
    assert otp.short(32767) == 32767
    assert otp.short(32767) + 1 == -32768


def test_decimal(session):
    assert ott.type2str(otp.decimal) == 'decimal'
    assert str(otp.decimal) == 'decimal'
    assert ott.value2str(otp.decimal(1)) == 'DECIMAL(1.0)'
    assert str(otp.decimal(1)) == '1.0'

    t = otp.Tick(A1=otp.decimal(1.1))
    t['A2'] = otp.decimal(2.2)
    t['A3'] = otp.decimal(3.3)
    t['A4'] = (1.1 + otp.decimal(1.1)) + (otp.decimal(5.5) - 4.4) * (1 * otp.decimal(1)) * 2
    t['A5'] = 5.5
    t = t.table(A5=otp.decimal, strict=False)
    t['A6'] = t['A1'] + 5.5

    t['DEC'] = otp.decimal(0.3)
    t['DEC_STR'] = t['DEC'].apply(str)
    t['DEC_INT'] = t['DEC'].apply(int)
    t['DEC_FLOAT'] = t['DEC'].apply(float)

    t['FLOAT'] = 0.3
    t['FLOAT_DEC'] = t['FLOAT'].apply(otp.decimal)

    t['STR'] = '0.3'
    t['STR_DEC'] = t['STR'].apply(otp.decimal)

    t['DEC_STR_PRECISION'] = t['DEC'].decimal.str(precision=10)

    t['STR'] = '0.2999999999999999888977697537484346'  # NOSONAR
    t['DEC_FROM_STR'] = t['STR'].apply(otp.decimal).decimal.str(precision=34)
    t['FLOAT_FROM_STR'] = t['STR'].apply(float).float.str(precision=34)

    df = otp.run(t)

    for i, v in enumerate((1.1, 2.2, 3.3, 4.4, 5.5, 6.6), start=1):
        assert t.schema[f'A{i}'] is otp.decimal
        assert df[f'A{i}'][0] == pytest.approx(v)

    assert t.schema['DEC_STR'] is str
    assert t.schema['DEC_INT'] is int
    assert t.schema['DEC_FLOAT'] is float
    assert t.schema['FLOAT_DEC'] is otp.decimal
    assert t.schema['STR_DEC'] is otp.decimal
    assert t.schema['DEC_STR_PRECISION'] is str

    assert df['DEC'][0] == 0.3
    assert df['DEC_STR'][0] == '0.30000000'
    assert df['DEC_INT'][0] == 0
    assert df['DEC_FLOAT'][0] == 0.3
    assert df['FLOAT_DEC'][0] == 0.3
    assert df['STR_DEC'][0] == 0.3
    assert df['DEC_STR_PRECISION'][0] == '0.3000000000'

    assert df['DEC_FROM_STR'][0] == '0.2999999999999999888977697537484346'
    assert df['FLOAT_FROM_STR'][0] == '0.30000000'

    t = otp.Ticks({'A': [otp.decimal(1), otp.decimal(2)]})
    assert t.schema['A'] is otp.decimal
    df = otp.run(t)
    assert list(df['A']) == [1, 2]


@pytest.mark.filterwarnings("ignore:.*milliseconds as nanoseconds.*")
@pytest.mark.parametrize('value,default_value,schema_type,result', [
    (1, 0, int, 0),
    (otp.uint(1), otp.uint(0), otp.uint, 0),
    (otp.ulong(1), otp.ulong(0), otp.ulong, 0),
    (otp.short(1), otp.short(0), otp.short, 0),
    (otp.byte(1), otp.byte(0), otp.byte, 0),
    (1.1, otp.nan, float, float('nan')),
    (otp.decimal(1.1), otp.decimal(0), otp.decimal, 0.0),
    ('string', '', str, ''),
    (otp.string[10]('string'), otp.string[10](''), otp.string[10], ''),
    (otp.varstring('string'), otp.varstring(''), otp.varstring, ''),
    (otp.datetime(2022, 1, 1), otp.nsectime(0), otp.nsectime, pd.Timestamp(1970, 1, 1)),
    (otp.nsectime(1), otp.nsectime(0), otp.nsectime, pd.Timestamp(1970, 1, 1)),
    (otp.msectime(1), otp.msectime(0), otp.msectime, pd.Timestamp(1970, 1, 1)),
])
def test_default_by_type(session, value, default_value, schema_type, result):
    t = otp.Tick(VALUE=value)
    assert t.schema['VALUE'] is schema_type
    assert otp.default_by_type(t.schema['VALUE']) == default_value
    t['VALUE'] = otp.default_by_type(t.schema['VALUE'])
    assert t.schema['VALUE'] is schema_type
    df = otp.run(t, timezone='GMT')
    if pd.isna(result):
        assert pd.isna(df['VALUE'][0])
    else:
        assert df['VALUE'][0] == result


def test_nsectime_very_long(session):
    t = otp.Tick(A=1)
    t['B'] = otp.nsectime(otp.long.MAX - 1)
    t['C'] = otp.nsectime(otp.long.MAX + 1)
    assert t.schema['B'] is otp.nsectime
    assert t.schema['C'] is otp.nsectime

    big_integer = otp.long.MAX - 1
    t = otp.Tick(A=1)
    t['B'] = otp.nsectime(big_integer)
    df = otp.run(t, timezone='GMT')
    assert pd.api.types.is_datetime64_any_dtype(df.dtypes['B'])
    # seems like it's the maximum of OneTick nsectime type
    if big_integer > pd.Timestamp(2200, 1, 1).value:
        assert df['B'][0] == pd.Timestamp(2200, 1, 1)
    elif big_integer <= 15e12:
        # backward compatibility with msectime (value was treated as msectime)
        assert df['B'][0] == pd.Timestamp(big_integer * 1_000_000)
    else:
        assert df['B'][0] == pd.Timestamp(big_integer)


def test_timedelta(session):

    x_datetime = otp.datetime(2022, 1, 1, 1, 1, 1)
    x_date = otp.date(2022, 1, 1)
    delta = otp.timedelta(weeks=1, days=1, hours=1, minutes=1, seconds=1, milliseconds=1, microseconds=1, nanoseconds=1)

    assert repr(delta) == "timedelta('8 days 01:01:01.001001001')"
    assert str(delta) == '8 days 01:01:01.001001001'

    assert x_datetime + delta == (
        x_datetime + otp.Day(7) + otp.Day(1) + otp.Hour(1) + otp.Minute(1) +
        otp.Second(1) + otp.Milli(1) + otp.Nano(1000) + otp.Nano(1)
    )
    assert x_date + delta == (
        x_date + otp.Day(7) + otp.Day(1) + otp.Hour(1) + otp.Minute(1) +
        otp.Second(1) + otp.Milli(1) + otp.Nano(1000) + otp.Nano(1)
    )
