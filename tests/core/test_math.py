import sys
import pytest
import math
import datetime
from functools import partial

import onetick.py as otp


@pytest.fixture(scope='module', autouse=True)
def session(m_session):
    yield m_session


@pytest.fixture
def data():
    yield [1, 2, 3, 4, 5]


@pytest.mark.parametrize(
    'otp_func,py_func,dtype,max_range',
    [
        (otp.math.sqrt, math.sqrt, float, None),
        (partial(otp.math.pow, 0), partial(math.pow, 0), float, None),
        (partial(otp.math.pow, 5), partial(math.pow, 5), float, None),
        (partial(otp.math.pow, -3.2), partial(math.pow, -3.2), float, None),
        (otp.math.sign, partial(math.copysign, 1), int, None),
        (otp.math.exp, math.exp, float, None),
        (otp.math.log10, math.log10, float, None),
        (otp.math.ln, math.log, float, None),
        (otp.math.sin, math.sin, float, None),
        (otp.math.cos, math.cos, float, None),
        (otp.math.tan, math.tan, float, None),
        (otp.math.cot, lambda x: math.tan(math.pi / 2 - x), float, None),
        (otp.math.asin, math.asin, float, 1),
        (otp.math.acos, math.acos, float, 1),
        (otp.math.atan, math.atan, float, None),
        (otp.math.acot, lambda x: math.pi / 2 - math.atan(x), float, None),
        (otp.math.arcsin, math.asin, float, 1),
        (otp.math.arccos, math.acos, float, 1),
        (otp.math.arctan, math.atan, float, None),
        (otp.math.arccot, lambda x: math.pi / 2 - math.atan(x), float, None),
        # the divmod returns a tuple
        (partial(otp.math.mod, 99), lambda x: divmod(99, x)[1], int, None)
    ]
)
class TestMethod:

    def test_constat(self, otp_func, py_func, dtype, max_range):
        ticks = otp.Tick(X=1)
        val = 5 if max_range is None else max_range * 0.87
        ticks['Y'] = otp_func(val)
        assert ticks['Y'].dtype is dtype
        df = otp.run(ticks)
        assert all(df['Y'] == [pytest.approx(py_func(val), 1e-10)])

    def test_ticks(self, data, otp_func, py_func, dtype, max_range):
        data_limited = list(data) if max_range is None else list(map(lambda x: x / 5 * max_range, data))
        ticks = otp.Ticks(X=data_limited)
        ticks['Y'] = otp_func(ticks['X'])
        assert ticks['Y'].dtype is dtype
        df = otp.run(ticks)

        assert all(df['Y'] == list(map(lambda x: pytest.approx(x, 1e-10), map(py_func, data_limited))))


@pytest.mark.parametrize(
    'otp_func,py_func', [
        (otp.math.max, max),
        (otp.math.min, min),
    ]
)
class TestMinMax:
    def test_two_columns(self, otp_func, py_func):
        t1 = [1, 4]
        t2 = [5, -1]
        data = otp.Ticks([['X', 'Y'], t1, t2])

        data['Z'] = otp_func(data['X'], data['Y'])

        assert data['Z'].dtype is int

        df = otp.run(data)

        assert all(df['Z'] == list(map(py_func, [t1, t2])))

    def test_three_columns(self, otp_func, py_func):
        t1 = [1, 4, 2]
        t2 = [3, -1, 1]
        t3 = [5, 2, 8]
        data = otp.Ticks([['X', 'Y', 'Z'], t1, t2, t3])

        data['U'] = otp_func(data['X'], data['Y'], data['Z'])

        assert data['Z'].dtype is int

        df = otp.run(data)

        assert all(df['U'] == list(map(py_func, [t1, t2, t3])))

    def test_columns_and_const(self, otp_func, py_func):
        t1 = [1, 4, 2]
        t2 = [3, -1, 1]
        t3 = [5, 2, 8]
        data = otp.Ticks([['X', 'Y', 'Z'], t1, t2, t3])

        data['U'] = otp_func(data['X'], 6, data['Y'], data['Z'])

        assert data['Z'].dtype is int

        df = otp.run(data)

        assert all(df['U'] == list(map(py_func, [t1 + [6], t2 + [6], t3 + [6]])))

    def test_two_columns_nsectime(self, otp_func, py_func):
        data = otp.Tick(A=1)
        x_time = otp.datetime(datetime.datetime(2019, 1, 1, 1))
        y_time = x_time + otp.Nano(1)
        data['X'] = x_time
        data['Y'] = y_time

        data['Z'] = otp_func(data['X'], data['Y'])

        assert data['Z'].dtype is otp.nsectime

        df = otp.run(data)

        if py_func == max:
            assert all(df['Z'] == [y_time])
        else:
            assert all(df['Z'] == [x_time])

    def test_three_columns_nsectime(self, otp_func, py_func):
        data = otp.Tick(A=1)
        x_time = otp.datetime(datetime.datetime(2019, 1, 1, 1))
        y_time = x_time + otp.Nano(1)
        z_time = x_time - otp.Nano(1)
        data['X'] = x_time
        data['Y'] = y_time
        data['Z'] = z_time

        data['U'] = otp_func(data['X'], data['Y'], data['Z'])

        assert data['U'].dtype is otp.nsectime

        df = otp.run(data)

        if py_func == max:
            assert all(df['U'] == [y_time])
        else:
            assert all(df['U'] == [z_time])


class TestRandom:

    @pytest.mark.parametrize('seed', [None, 12345])
    def test_const(self, seed):
        N = 1000
        data = otp.Ticks(X=[1] * N)
        data['Y'] = otp.math.rand(0, 100, seed)

        df = otp.run(data)

        assert len(df) == N
        assert df['Y'].min() >= 0
        assert df['Y'].max() <= 100

        all_equal = True

        for i in range(0, N - 1):
            all_equal &= df['Y'][i] == df['Y'][i + 1]

        assert not all_equal

    def test_columns(self):
        data = otp.Ticks(X=[0], Y=[100], Z=[12345])

        data['U'] = otp.math.rand(data['X'], data['Y'], data['Z'])

        df = otp.run(data)
        assert df['U'][0] == 41  # constant because of the feed

    @pytest.mark.parametrize(
        'min_value,max_value,seed,text', [
            (-1, 100, 12345, 'It is not possible to use negative values'),
            (100, 1, 12345, 'parameter should be more than'),
            (50, 50, 12345, 'parameter should be more than'),
        ]
    )
    def test_invalid(self, min_value, max_value, seed, text):
        data = otp.Tick(X=1)
        with pytest.raises(Exception, match=text):
            data['Y'] = otp.math.rand(min_value, max_value, seed)


@pytest.mark.skipif(sys.platform == "win32",
                    reason="current timestamp in OneTick and in python behaves with micro precision")
class TestNow:

    def test_simple(self):
        for _ in range(5):  # check it few times
            timestamp_before = int(datetime.datetime.now().timestamp() * 10**9)

            data = otp.Tick(X=1)
            data['Y'] = otp.math.now().apply(int)
            df = otp.run(data)

            timestamp_after = int(datetime.datetime.now().timestamp() * 10**9)

            assert timestamp_before < df['Y'][0] < timestamp_after

    def test_run(self):
        ''' When we use now() in strt / end '''
        data = otp.Tick(X=1)

        timestamp_before = int(datetime.datetime.now().timestamp() * 10**9)

        df = otp.run(data, start=otp.math.now(), end=otp.math.now() + 5, timezone='GMT')

        timestamp_after = int(datetime.datetime.now().timestamp() * 10**9)

        assert timestamp_before < int(df['Time'][0].timestamp() * 10**9) < timestamp_after


class TestFloatNan:
    def test_1_nan(self):
        data = {
            'MD_NAME': [''],
            'MD_PRICE_MULT': [float('nan')],
        }
        data = otp.Ticks(data)
        df = otp.run(data)
        assert all(df['MD_NAME'] == [''])
        assert df['MD_PRICE_MULT'].isnull().all()

    def test_tick_nan(self):
        data = otp.Tick(MD_PRICE_MULT=float('nan'), MD_NAME='')
        df = otp.run(data)
        assert all(df['MD_NAME'] == [''])
        assert df['MD_PRICE_MULT'].isnull().all()

    def test_2_nan(self):
        data = {
            'MD_NAME': [''] * 2,
            'MD_PRICE_MULT': [float('nan')] * 2,
        }
        data = otp.Ticks(data)
        df = otp.run(data)
        assert all(df['MD_NAME'] == [''] * 2)
        assert df['MD_PRICE_MULT'].isnull().all()

    def test_2_nan_md_dollar(self):
        data = {
            'MD_NAME': ['$5', ''],
            'MD_PRICE_MULT': [float('nan')] * 2,
        }
        data = otp.Ticks(data)
        df = otp.run(data)
        assert all(df['MD_NAME'] == ['$5', ''])
        assert df['MD_PRICE_MULT'].isnull().all()


def test_round():
    t = otp.Ticks(A=[1234.5678, otp.inf, -otp.inf, otp.nan])
    t['B'] = round(t['A'])
    t['C'] = round(t['A'], 2)
    t['D'] = round(t['A'], -2)
    df = otp.run(t)
    assert df['B'][0] == 1235
    assert df['C'][0] == 1234.57
    assert df['D'][0] == 1200.0
    assert df['B'][1] == float('inf')
    assert df['C'][1] == float('inf')
    assert df['D'][1] == float('inf')
    assert df['B'][2] == -float('inf')
    assert df['C'][2] == -float('inf')
    assert df['D'][2] == -float('inf')
    assert math.isnan(df['B'][3])
    assert math.isnan(df['C'][3])
    assert math.isnan(df['D'][3])


class TestFloor:
    @pytest.mark.parametrize("value", [1, 3.74])
    def test_const(self, session, value):
        data = otp.Tick(A=1)
        data['B'] = otp.math.floor(value)
        assert data['B'].dtype is float
        df = otp.run(data)
        assert df['B'][0] == math.floor(value)

    @pytest.mark.parametrize("value", [1, 3.74])
    def test_column(self, session, value):
        data = otp.Tick(A=value)
        data['B'] = otp.math.floor(data['A'])
        assert data['B'].dtype is float
        df = otp.run(data)
        assert df['B'][0] == math.floor(value)

    def test_operation(self, session):
        data = otp.Tick(A=54.1, B=27)
        data['C'] = otp.math.floor(data['A'] / data['B'])
        assert data['C'].dtype is float
        df = otp.run(data)
        assert df['C'][0] == 2
