import os
import time
import itertools
from pathlib import Path

import pytest
import numpy as np
import pandas as pd

import onetick.py as otp


@pytest.fixture(scope='module')
def session(m_session):
    db_1 = otp.DB('DB_1')
    db_1.add(otp.Ticks(A=[1, 2, 3, 4, 5]), tick_type='TT', symbol='A')
    db_1.add(otp.Ticks(B=[6, 7, 8, 9]), tick_type='TT', symbol='B')
    db_1.add(otp.Ticks(C=[10, 20, 30, 40]), tick_type='TT', symbol='XYZ')
    m_session.use(db_1)
    yield m_session


def test_merge(session):
    t1 = otp.Tick(x="a")
    t2 = otp.Tick(x=otp.string[1024]("b"))

    data = t1 + t2

    assert data.x.dtype is otp.string[1024]


def test_merge_copy_1(session):
    t = otp.Tick(x=34, y=0.55)

    t_c = t.copy()
    t_c.z = "abc"
    t_c.Time += 1

    merged = otp.merge([t, t_c])

    df = otp.run(merged)
    assert len(df) == 2
    assert hasattr(df, "x")
    assert hasattr(df, "y")
    assert hasattr(df, "z")
    assert df.x[0] == 34
    assert df.x[1] == 34
    assert df.z[0] == ""
    assert df.z[1] == "abc"


def test_merge_copy_2(session):
    m = otp.Ticks({"x": [101, 102, 103]})

    left, right = m[m.x <= 102]
    right_c = right.copy()
    right_c.x += 4

    m = otp.merge([left, right_c])

    df = otp.run(m)
    assert len(df) == 3
    assert df.x[0] == 101
    assert df.x[1] == 102
    assert df.x[2] == 107


def test_different_aligned_schema_1(session):
    t1 = otp.Tick(x=1, y=0.1, z="abc", offset=1)
    t2 = otp.Tick(y=-0.3)

    m = otp.merge([t1, t2])

    assert hasattr(m, "x") and m.x.dtype is int
    assert hasattr(m, "y") and m.y.dtype is float
    assert hasattr(m, "z") and m.z.dtype is str

    df = otp.run(m)
    assert hasattr(df, "x") and df.x[0] == 0
    assert hasattr(df, "y") and df.y[0] == -0.3
    assert hasattr(df, "z") and df.z[0] == ""

    assert df.x[1] == 1
    assert df.y[1] == 0.1
    assert df.z[1] == "abc"


def test_different_aligned_schema_2(session):
    t1 = otp.Tick(x=1, y=0.1, z="abc", offset=1)
    t2 = otp.Tick(y=-0.3)

    m = otp.merge([t2, t1])

    assert hasattr(m, "x") and m.x.dtype is int
    assert hasattr(m, "y") and m.y.dtype is float
    assert hasattr(m, "z") and m.z.dtype is str

    df = otp.run(m)
    assert hasattr(df, "x") and df.x[0] == 0
    assert hasattr(df, "y") and df.y[0] == -0.3
    assert hasattr(df, "z") and df.z[0] == ""

    assert df.x[1] == 1
    assert df.y[1] == 0.1
    assert df.z[1] == "abc"


def test_different_aligned_schema_3(session):
    t1 = otp.Tick(x=1, y=0.1, z="abc", offset=1)
    t2 = otp.Tick(y=-0.3)

    m = otp.merge([t1, t2])

    m.x = -1
    m.y = 0.99
    m.z = "xxx"

    df = otp.run(m)
    assert len(df) == 2

    assert df.x[0] == -1
    assert df.y[0] == 0.99
    assert df.z[0] == "xxx"

    assert df.x[1] == -1
    assert df.y[1] == 0.99
    assert df.z[1] == "xxx"


def test_different_aligned_schema_4(session):
    t1 = otp.Tick(x=1)
    t2 = otp.Tick(x=0.1, offset=1)

    m = otp.merge([t1, t2])

    m.x = m.x.apply(str)

    df = otp.run(m)
    assert df.x[0] == "1.0"
    assert df.x[1] == "0.1"


def test_different_aligned_schema_5(session):
    t1 = otp.Tick(x="abc")
    t2 = otp.Tick(x="x" * 99, offset=1)

    m = otp.merge([t1, t2])

    m.x = "y" * 200

    df = otp.run(m)
    assert df.x[0] == "y" * 99
    assert df.x[1] == "y" * 99


def test_different_schema_strong(session):
    t1 = otp.Tick(x=123)
    t2 = otp.Tick(y="abc")

    # check invalid case
    m = otp.merge([t1, t2], align_schema=False)

    m.x = 4

    with pytest.raises(Exception):
        # because we refer to the tick where is no 'x' column
        otp.run(m)

    # but
    m2 = otp.merge([t1, t2])

    m2.x = 4

    assert len(otp.run(m2)) == 2


def test_merge_align_schema(session):
    t = otp.Tick(x=1)
    m = otp.merge([t, t], align_schema=False)
    m.y = 2
    df = otp.run(m)
    assert df.x[0] == 1


def test_merge_vs_pandas(session):
    d1 = {"x": [1, 2, 3],
          "y": [1.3, otp.nan, -0.2],
          "w": ["", "acd", ""],
          "offset": [0, 1, 2]}
    d2 = {"y": [4.1, 2.2], "z": [-4, 0], "offset": [0, 1]}
    d3 = {"x": [1, -99, 0.3], "w": ["", "", "xd"], "offset": [0, 1, 2]}

    t1, t2, t3 = map(otp.Ticks, [d1, d2, d3])
    df1, df2, df3 = map(pd.DataFrame, [d1, d2, d3])

    ticks = otp.merge([t1, t2, t3])
    df = pd.concat([df1, df2, df3], sort=True)

    ticks = ticks.sort_values([ticks.Time, ticks.x])
    df = df.sort_values(["offset", "x"], na_position="first")

    df.w = df.w.fillna("")
    df.z = df.z.fillna(0).astype(int)

    res = otp.run(ticks)
    for inx in range(3):
        assert (df.x.iloc[inx] == res.x[inx]) or (np.isnan(df.x.iloc[inx]) and np.isnan(res.x[inx]))

        assert (df.y.iloc[inx] == res.y[inx]) or (np.isnan(df.y.iloc[inx]) and np.isnan(res.y[inx]))

        assert df.z.iloc[inx] == res.z[inx]
        assert df.w.iloc[inx] == res.w[inx]


def test_append_vs_pandas_1(session):
    d1 = {"x": [1, 2, 3], "y": [1.3, otp.nan, -0.2], "w": ["", "acd", ""], "offset": [0, 1, 2]}
    d2 = {"y": [4.1, 2.2], "z": [-4, 0], "offset": [0, 1]}

    t1, t2 = map(otp.Ticks, [d1, d2])
    df1, df2 = map(pd.DataFrame, [d1, d2])

    ticks = t1.append(t2)
    df = pd.concat([df1, df2])

    ticks = ticks.sort_values([ticks.Time, ticks.x])
    df = df.sort_values(["offset", "x"], na_position="first")

    df.x = df.x.fillna(0).astype(int)
    df.w = df.w.fillna("")
    df.z = df.z.fillna(0).astype(int)

    res = otp.run(ticks)
    for inx in range(3):
        assert (df.x.iloc[inx] == res.x[inx]) or (np.isnan(df.x.iloc[inx]) and np.isnan(res.x[inx]))

        assert (df.y.iloc[inx] == res.y[inx]) or (np.isnan(df.y.iloc[inx]) and np.isnan(res.y[inx]))

        assert df.z.iloc[inx] == res.z[inx]
        assert df.w.iloc[inx] == res.w[inx]


def test_append_vs_pandas_2(session):
    d1 = {"x": [1, 2, 3], "y": [1.3, otp.nan, -0.2], "w": ["", "acd", ""], "offset": [0, 1, 2]}
    d2 = {"y": [4.1, 2.2], "z": [-4, 0], "offset": [0, 1]}
    d3 = {"x": [1, -99, 0.3], "w": ["", "", "xd"], "offset": [0, 1, 2]}

    t1, t2, t3 = map(otp.Ticks, [d1, d2, d3])
    df1, df2, df3 = map(pd.DataFrame, [d1, d2, d3])

    ticks = t1.append([t2, t3])
    df = pd.concat([df1, df2, df3], sort=True)

    ticks = ticks.sort_values([ticks.Time, ticks.x, "y"])
    df = df.sort_values(["offset", "x", "y"], na_position="first")

    df.w = df.w.fillna("")
    df.z = df.z.fillna(0).astype(int)

    res = otp.run(ticks)
    for inx in range(3):
        assert (df.x.iloc[inx] == res.x[inx]) or (np.isnan(df.x.iloc[inx]) and np.isnan(res.x[inx]))

        assert (df.y.iloc[inx] == res.y[inx]) or (np.isnan(df.y.iloc[inx]) and np.isnan(res.y[inx]))

        assert df.z.iloc[inx] == res.z[inx]
        assert df.w.iloc[inx] == res.w[inx]


def test_output_type(session):
    ticks = otp.Ticks({'A': [1, 2, 3]})
    tick = otp.Tick(B=1)

    result = otp.merge([ticks, tick])
    assert result.__class__ == otp.Source
    tt1 = otp.run(result)

    result = otp.merge([ticks, tick], output_type_index=0)
    assert result.__class__ == ticks.__class__
    tt2 = otp.run(result)
    assert tt2.equals(tt1)

    some_db_1 = otp.DB('SOME_DB_1')
    some_db_1.add(ticks)
    session.use(some_db_1)
    some_db_2 = otp.DB('SOME_DB_2')
    some_db_2.add(ticks)
    session.use(some_db_2)

    custom_1 = otp.DataSource(some_db_1, symbols=otp.config['default_symbol'])
    custom_2 = otp.DataSource(some_db_2, symbols=otp.config['default_symbol'])

    result = otp.merge([custom_1, tick])
    assert result.__class__ == otp.Source
    ct1 = otp.run(result)

    result = otp.merge([custom_1, tick], output_type_index=0)
    assert result.__class__ == otp.DataSource
    assert result.db == custom_1.db
    ct2 = otp.run(result)
    assert ct2.equals(ct1)

    result = otp.merge([custom_1, custom_2])
    assert result.__class__ == otp.Source
    cc1 = otp.run(result)

    result = otp.merge([custom_1, custom_2], output_type_index=1)
    assert result.__class__ == otp.DataSource
    assert result.db == custom_2.db
    cc2 = otp.run(result)
    assert cc2.equals(cc1)


@pytest.mark.skipif(os.name == 'nt', reason='Windows test machine is slow')
def test_merge_diamond_pattern_performance(session):
    # PY-953

    t = otp.Tick(A=1)

    for _ in range(20):
        start = time.time()
        a, b = t[t['A'] == 1]
        t = otp.merge([a, b])
        end = time.time()
        print('merge', f'{end - start} seconds')
        assert end - start < 1

    df = otp.run(t)
    assert len(df) == 1
    assert df['A'][0] == 1


def test_order(session):
    # PY-650
    t0 = otp.Tick(A=0)
    t1 = otp.Tick(A=1)
    t2 = otp.Tick(A=2)
    t3 = otp.Tick(A=3)

    t = otp.merge([t0, t1, t2, t3])
    df = otp.run(t)
    # the order is not guaranteed by default
    assert list(df['A']) != [0, 1, 2, 3]

    t = otp.merge([t0, t1, t2, t3], enforce_order=True)
    assert 'OMDSEQ' not in t.schema
    df = otp.run(t)
    assert 'OMDSEQ' not in df
    assert list(df['A']) == [0, 1, 2, 3]

    sources = [
        otp.Tick(A=0, OMDSEQ=3),
        otp.Tick(A=1, OMDSEQ=2),
        otp.Tick(A=2, OMDSEQ=1),
        otp.Tick(A=3, OMDSEQ=0),
    ]
    t = otp.merge(sources, enforce_order=True)
    assert 'OMDSEQ' not in t.schema
    df = otp.run(t)
    assert 'OMDSEQ' not in df
    assert list(df['A']) == [0, 1, 2, 3]

    indexes = list(range(4))
    sources = [otp.Tick(A=i) for i in indexes]
    for index_combination in itertools.permutations(indexes):
        t = otp.merge([sources[i] for i in index_combination], enforce_order=True)
        assert 'OMDSEQ' not in t.schema
        df = otp.run(t)
        assert 'OMDSEQ' not in df
        assert list(df['A']) == list(index_combination)


def test_presort(session):
    t = otp.Tick(A=1)
    q = otp.Tick(A=2)
    data = otp.merge([t, q])
    assert 'PRESORT' not in Path(data.to_otq().split('::')[0]).read_text()
    df = otp.run(data)
    assert list(df['A']) == [1, 2]

    data = otp.merge([t, q], presort=True)
    text = Path(data.to_otq().split('::')[0]).read_text()
    assert 'PRESORT' in text and 'PRESORT(MAX_CONCURRENCY=' not in text
    df = otp.run(data)
    assert list(df['A']) == [1, 2]


def test_force_presort(session, monkeypatch):
    monkeypatch.setattr(otp.config, 'presort_force_default_concurrency', True)

    t = otp.Tick(A=1)
    q = otp.Tick(A=2)

    data = otp.merge([t, q], presort=True)
    text = Path(data.to_otq().split('::')[0]).read_text()
    query_concurrency = otp.configuration.default_query_concurrency()
    presort_concurrency = otp.configuration.default_presort_concurrency()
    assert query_concurrency == presort_concurrency
    assert f'PRESORT(MAX_CONCURRENCY={presort_concurrency}' in text
    df = otp.run(data)
    assert list(df['A']) == [1, 2]


@pytest.mark.skipif(not otp.compatibility.is_symbol_time_override_fixed(),
                    reason='Not supported on older OneTick versions')
def test_symbol_date(session):

    data = otp.Tick(SYM=otp.meta_fields.symbol_name, SYM_TIME=otp.meta_fields.symbol_time)

    # test default symbol_date with to_otq
    df = otp.run(data, timezone='GMT')
    assert df['SYM_TIME'][0] == otp.dt(1970, 1, 1)

    # test symbol_date without symbols
    with pytest.raises(ValueError,
                       match="Parameter 'symbol_date' can only be specified together with parameter 'symbols'"):
        _ = otp.merge([data], symbol_date=otp.dt(2022, 1, 1))

    # test default merged data without symbols
    merged_data = otp.merge([data])
    df = otp.run(merged_data, timezone='GMT')
    assert list(df['SYM_TIME']) == [otp.dt(1970, 1, 1)]

    # test merged data with otp.Source and symbol_date
    merged_data = otp.merge([data], symbols=otp.Tick(SYMBOL_NAME='A'), symbol_date=otp.dt(2022, 1, 1),
                            identify_input_ts=True)
    df = otp.run(merged_data, timezone='GMT')
    assert list(df['SYM_TIME']) == [otp.dt(2022, 1, 1)]
    assert list(df['SYMBOL_NAME']) == ['A']

    # test merged data with many symbols and symbol_date
    merged_data = otp.merge([data], symbols=['A', 'B'], symbol_date=otp.dt(2022, 1, 1), identify_input_ts=True)
    df = otp.run(merged_data, timezone='GMT')
    assert list(df['SYM_TIME']) == [otp.dt(2022, 1, 1), otp.dt(2022, 1, 1)]
    assert list(df['SYMBOL_NAME']) == ['A', 'B']

    # test merged data with a single symbol and symbol_date
    merged_data = otp.merge([data], symbols='XYZ', symbol_date=otp.dt(2022, 1, 1), identify_input_ts=True)
    df = otp.run(merged_data, timezone='GMT')
    assert list(df['SYM_TIME']) == [otp.dt(2022, 1, 1)]
    assert list(df['SYMBOL_NAME']) == ['XYZ']

    # test otp.DataSource without symbol_date
    data = otp.DataSource('DB_1', tick_type='TT', schema_policy='manual', symbols=['A', 'B'], identify_input_ts=True)
    data['SYM_TIME'] = data['_SYMBOL_TIME']
    df = otp.run(data, timezone='GMT')
    assert list(df['SYM_TIME']) == [otp.dt(1970, 1, 1)] * 9
    assert list(df['A']) == [1, 0, 2, 0, 3, 0, 4, 0, 5]
    assert list(df['B']) == [0, 6, 0, 7, 0, 8, 0, 9, 0]
    assert list(df['SYMBOL_NAME']) == ['A', 'B', 'A', 'B', 'A', 'B', 'A', 'B', 'A']

    # test otp.DataSource with symbol_date and without symbols
    with pytest.raises(ValueError,
                       match="Parameter 'symbol_date' can only be specified together with parameter 'symbols'"):
        _ = otp.DataSource('DB_1', tick_type='TT', schema_policy='manual', symbol_date=otp.dt(2022, 1, 1))

    # test otp.DataSource with symbol_date and otp.Source symbols
    data = otp.DataSource('DB_1', tick_type='TT', schema_policy='manual',
                          symbols=otp.Symbols('DB_1'), symbol_date=otp.dt(2022, 1, 1))
    data['SYM_TIME'] = data['_SYMBOL_TIME']
    df = otp.run(data, timezone='GMT')
    assert list(df['SYM_TIME']) == [otp.dt(2022, 1, 1)] * 13

    # test otp.DataSource with symbol_date and many symbols
    data = otp.DataSource('DB_1', tick_type='TT', schema_policy='manual',
                          symbols=['A', 'B'], symbol_date=otp.dt(2022, 1, 1), identify_input_ts=True)
    data['SYM_TIME'] = data['_SYMBOL_TIME']
    df = otp.run(data, timezone='GMT')
    assert list(df['SYM_TIME']) == [otp.dt(2022, 1, 1)] * 9
    assert list(df['A']) == [1, 0, 2, 0, 3, 0, 4, 0, 5]
    assert list(df['B']) == [0, 6, 0, 7, 0, 8, 0, 9, 0]
    assert list(df['SYMBOL_NAME']) == ['A', 'B', 'A', 'B', 'A', 'B', 'A', 'B', 'A']

    # test otp.DataSource with symbol_date and a single symbol
    data = otp.DataSource('DB_1', tick_type='TT', schema_policy='manual',
                          symbols='XYZ', symbol_date=otp.dt(2022, 1, 1))
    data['SYM_TIME'] = data['_SYMBOL_TIME']
    df = otp.run(data, timezone='GMT')
    assert list(df['SYM_TIME']) == [otp.dt(2022, 1, 1)] * 4
    assert list(df['C']) == [10, 20, 30, 40]
