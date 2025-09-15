import pytest

import onetick.py as otp
from onetick.py.compatibility import is_supported_rename_fields_symbol_change


@pytest.fixture
def data():
    return otp.Ticks({
        'A': [1, 2, 3, 4, 5],
    })


def test_apply(f_session, data):
    def agg_fun(source):
        return source.agg({'X': otp.agg.count()})
    data = otp.agg.generic(agg_fun).apply(data)
    assert 'X' in data.schema
    assert 'A' not in data.schema
    df = otp.run(data)
    assert len(df) == 1
    assert 'X' in df
    assert df['X'][0] == 5


def test_all_fields_error(f_session, data):
    def agg_fun(source):
        return source.agg({'X': otp.agg.count()})
    with pytest.raises(ValueError):
        otp.agg.generic(agg_fun, all_fields=True).apply(data)


def test_attribute_error(f_session, data):
    def agg_fun(source):
        return source.agg({'X': otp.agg.sum('NO_SUCH_COLUMN')})
    with pytest.raises(TypeError):
        otp.agg.generic(agg_fun).apply(data)


def test_bucket_delimiter(f_session, data):
    def agg_fun(source):
        return source.agg({'X': otp.agg.count()})
    data = otp.agg.generic(agg_fun, bucket_delimiter=True).apply(data)
    assert 'DELIMITER' in data.schema
    df = otp.run(data)
    assert len(df) == 2
    assert 'X' in df
    assert 'DELIMITER' in df
    assert list(df['X']) == [5, 0]
    assert list(df['DELIMITER']) == ['', 'D']


def test_get_second_tick_in_bucket(f_session, data):
    def agg_fun(source):
        source = source.agg({'X': otp.agg.count()}, running=True, all_fields=True)
        source, _ = source[source['X'] == 2]
        source = source.drop('X')
        return source

    data = otp.agg.generic(agg_fun,
                           bucket_interval=3,
                           bucket_units='ticks').apply(data)
    df = otp.run(data)
    assert 'X' not in df
    assert list(df['A']) == [2, 5]


def test_complex_generic_agg(f_session, data):
    """This is something like pandas.Series.quantile:
    https://pandas.pydata.org/docs/reference/api/pandas.Series.quantile.html
    But rather its an aggregation for running summarized volume in each bucket,
    returning TOP_TIME of a tick after which the quantile was reached.
    """
    VOLUME_PCT = 0.5

    def agg_fun(source):
        total = source.agg({'X': otp.agg.sum('A')}, bucket_time='start')
        # otp.join(on='all') would be better,
        # but the Join ep is not supported in generic aggregation
        source = otp.merge([total, source])
        source = source.agg({'VOLUME': otp.agg.sum('A'),
                             'TOTAL_VOLUME': otp.agg.sum('X')},
                            running=True, all_fields=True)
        source, _ = source[source['VOLUME'] / source['TOTAL_VOLUME'] <= VOLUME_PCT]
        source = source.last()
        source['TOP_TIME'] = source['TIMESTAMP']
        return source

    data = otp.agg.generic(agg_fun).apply(data)
    df = otp.run(data)
    assert len(df) == 1
    assert df['TOP_TIME'][0] == otp.config.default_start_time + otp.Milli(2)


@pytest.mark.parametrize('use_rename_ep', (True, False))
def test_join_by_time(f_session, data, use_rename_ep):

    def agg_fun(source):
        other = source.copy()
        other['B'] = 3
        other = other[['B']]
        source = otp.join_by_time([source, other], use_rename_ep=use_rename_ep)
        source = source.agg({'A': otp.agg.sum('A'),
                             'B': otp.agg.average('B')})
        return source

    data = otp.agg.generic(agg_fun).apply(data)

    if use_rename_ep and not is_supported_rename_fields_symbol_change():
        with pytest.raises(Exception, match='RENAME_FIELDS does not currently support dynamic symbol changes'):
            otp.run(data)
    else:
        df = otp.run(data)
        assert len(df) == 1
        assert df['A'][0] == 15
        assert df['B'][0] == 2.4


@pytest.mark.parametrize('unsupported_on', ('all', 'same_size'))
def test_unsupported_join(f_session, data, unsupported_on):

    def agg_fun(source):
        other = source.copy()
        other['B'] = 3
        other = other[['B']]
        source = otp.join(source, other, on=unsupported_on)
        source = source.agg({'A': otp.agg.sum('A'),
                             'B': otp.agg.average('B')})
        return source

    data = otp.agg.generic(agg_fun).apply(data)
    with pytest.raises(Exception, match='does not currently support dynamic symbol changes'):
        otp.run(data)


@pytest.mark.skipif(is_supported_rename_fields_symbol_change(), reason='PY-557')
def test_unsupported_rename(f_session, data):
    def agg_fun(source):
        source = source.agg({'A': otp.agg.sum('A')})
        source = source.rename({'A': 'AA'})
        return source

    data = otp.agg.generic(agg_fun).apply(data)
    with pytest.raises(Exception, match='RENAME_FIELDS does not currently support dynamic symbol changes'):
        otp.run(data)


class TestGenericAggParams:
    @pytest.mark.parametrize('val', ('val', 123, 1.23, otp.datetime(2020, 1, 1, 2, 3, 4, tz=otp.config['tz'])))
    def test_apply_param_const(self, f_session, data, val):
        def agg_fun(source, value):
            source = source.agg({'count': otp.agg.count()})
            source.X = value
            return source

        data = otp.agg.generic(agg_fun).apply(data, value=val)
        assert 'X' in data.schema
        assert 'A' not in data.schema
        df = otp.run(data)
        assert len(df) == 1
        assert 'X' in df
        assert df['count'][0] == 5
        if val.__class__ is otp.datetime:
            assert otp.datetime(df['X'][0], tz=otp.config['tz']) == val
        else:
            assert df['X'][0] == val

    def test_apply_param_with_column(self, f_session, data):
        def agg_fun(source, key, column):
            source = source.agg({'count': otp.agg.count(),
                                key: otp.agg.sum(column)})
            return source
        data = otp.agg.generic(agg_fun).apply(data, key='KEY', column=data.A)
        assert 'KEY' in data.schema
        assert 'A' not in data.schema
        df = otp.run(data)
        assert len(df) == 1
        assert df['KEY'][0] == 15
        assert df['count'][0] == 5

    def test_apply_param_math(self, f_session, data):
        def agg_fun(source, val, key):
            source = source.agg({'count': otp.agg.count()})
            source.Y = source.count * val
            source[key] = val
            return source
        data = otp.agg.generic(agg_fun).apply(data, val=3, key="KEY")
        assert 'KEY' in data.schema
        assert 'Y' in data.schema
        assert 'A' not in data.schema
        df = otp.run(data)
        assert len(df) == 1
        assert df['Y'][0] == 5 * 3
        assert df['KEY'][0] == 3

    def test_apply_param_add_column(self, f_session, data):
        def agg_fun(source, val, key):
            source = source.agg({'count': otp.agg.count()})
            source[key] = val
            return source
        data = otp.agg.generic(agg_fun).apply(data, val=3, key="KEY")
        assert 'KEY' in data.schema
        assert 'A' not in data.schema
        df = otp.run(data)
        assert len(df) == 1
        assert df['KEY'][0] == 3

    def test_apply_wrong_param(self, f_session, data):
        def agg_fun(source, key):
            source = source.agg({key: otp.agg.count()})
            return source

        with pytest.raises(TypeError):
            otp.agg.generic(agg_fun).apply(data, wrong_key='KEY')

    def test_not_passed_params(self, f_session, data):
        def agg_fun(source, a, b):
            source = source.agg({a: otp.agg.count()})
            source.b = b
            return source

        with pytest.raises(TypeError):
            otp.agg.generic(agg_fun).apply(data, a=1)

    def test_params_condition(self, f_session, data):
        def agg_fun(source, a):
            source = source.agg({'count': otp.agg.count()})
            if a > 0:
                source.b = a
            return source

        data = otp.agg.generic(agg_fun).apply(data, a=1)
        df = otp.run(data)
        assert len(df) == 1
        assert df['b'][0] == 1

    def test_new_source(self, f_session):
        def count_values(source, value):
            values, _ = source[source['A'] == value]
            return values.agg({'count': otp.agg.count()})
        data = otp.Ticks({'A': [1, 2, 1]})
        data = otp.agg.generic(count_values).apply(data, value=1)
        df = otp.run(data)
        assert len(df) == 1
        assert df['count'][0] == 2
