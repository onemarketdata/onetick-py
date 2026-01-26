import os
from functools import partial

import pytest

import onetick.py as otp

if os.getenv('OTP_WEBAPI_TEST_MODE'):
    pytest.skip(allow_module_level=True,
                reason='TextWrite EP is not supported in WebAPI')


@pytest.mark.platform("linux")
def test_insert_at_end(f_session):
    # PY-1433
    f_session.use(otp.DB('A', otp.Tick(A=1), db_properties={'symbology': 'BZX'}))
    f_session.use(otp.DB('B', otp.Tick(B=2), db_properties={'symbology': 'DTKR'}))

    a = otp.DataSource(db='A', tick_type='TRD', symbols='AAPL')
    b = otp.DataSource(db='B', tick_type='TRD', symbols='AAPL')
    data = otp.merge([a, b])

    df = otp.run(data)
    assert list(df['A']) == [1, 0]
    assert list(df['B']) == [0, 2]

    data.dump()
    df = otp.run(data)
    assert list(df['A']) == [1, 0]
    assert list(df['B']) == [0, 2]


@pytest.mark.platform("linux")
class TestDump:

    @pytest.fixture(autouse=True)
    def session(self):
        with otp.Session():
            yield

    def test_single_tick(self, capfd):
        ''' check the simplest case with only one single tick '''
        data = otp.Ticks(X=[1])

        data.dump()
        otp.run(data)

        captured = capfd.readouterr()

        assert captured.out == '#TIMESTAMP,X\n' \
                               '01-12-2003 00:00:00.000000000,1\n'

    def test_multiple_ticks(self, capfd):
        ''' the the case with multiple ticks '''
        data = otp.Ticks(X=[1, 5, 7])

        data.dump()
        otp.run(data)

        captured = capfd.readouterr()

        assert captured.out == '#TIMESTAMP,X\n' \
                               '01-12-2003 00:00:00.000000000,1\n' \
                               '01-12-2003 00:00:00.001000000,5\n' \
                               '01-12-2003 00:00:00.002000000,7\n'

    def test_two_different_dumps(self, capfd):
        ''' check dump in two different places '''
        data = otp.Ticks(X=[1])

        data.dump()
        data['Y'] = 2
        data.dump()

        otp.run(data)

        captured = capfd.readouterr()

        assert captured.out == '#TIMESTAMP,X,Y\n' \
                               '01-12-2003 00:00:00.000000000,1,2\n' \
                               '#TIMESTAMP,X\n' \
                               '01-12-2003 00:00:00.000000000,1\n'

    def test_where(self, capfd):
        ''' dump following some condition '''
        data = otp.Ticks(X=[1, 3, 1, 5, 7])

        data.dump(where=(data['X'] != 1) & (data['X'] != 7))

        otp.run(data)

        captured = capfd.readouterr()

        assert captured.out == '#TIMESTAMP,X\n' \
                               '01-12-2003 00:00:00.001000000,3\n' \
                               '01-12-2003 00:00:00.003000000,5\n'

    def test_label(self, capfd):
        ''' check adding label on the dump point '''
        data = otp.Ticks(X=[1, 2])

        rhs, lhs = data[(data['X'] == 1)]

        rhs.dump(label='right')
        lhs.dump(label='left')

        res = rhs + lhs

        otp.run(res)

        captured = capfd.readouterr()

        assert '#TIMESTAMP,X,_OUT_LABEL_\n' \
               '01-12-2003 00:00:00.000000000,1,right\n' in captured.out
        assert '#TIMESTAMP,X,_OUT_LABEL_\n' \
               '01-12-2003 00:00:00.001000000,2,left\n' in captured.out

    def test_columns(self, capfd):
        ''' check columns selection '''
        data = otp.Ticks(X=[1], Y=[2], Z=[3], U=[4])

        data.dump(label='A')
        data.dump(label='B', columns=['Y', 'U'])

        otp.run(data)

        captured = capfd.readouterr()

        assert '#TIMESTAMP,X,Y,Z,U,_OUT_LABEL_\n' \
               '01-12-2003 00:00:00.000000000,1,2,3,4,A\n' in captured.out
        assert '#TIMESTAMP,Y,U,_OUT_LABEL_\n' \
               '01-12-2003 00:00:00.000000000,2,4,B' in captured.out

    def test_empty(self, capfd):
        data = otp.Empty()
        data.dump()
        otp.run(data)
        captured = capfd.readouterr()
        assert captured.out == '<no data>\n'


@pytest.mark.platform("linux")
class TestIntegration:
    ''' Check that the `dump` method integrates with other methods '''

    @pytest.fixture(scope='class', autouse=True)
    def session(self):
        with otp.Session() as s:
            yield s

    def test_where(self):
        data = otp.Ticks(X=[1, 2, 3])

        data, _ = data[data['X'] >= 2]
        data.dump()

        res = otp.run(data)

        assert len(res) == 2

    def test_jbt(self):
        data1 = otp.Ticks(X=[1, 2, 3])
        data2 = otp.Ticks(Y=[4, 5, 6])

        data1.dump()

        data = otp.join_by_time([data1, data2])

        res = otp.run(data)
        assert len(res) == 3
        assert all(res['X'] == [1, 2, 3])
        assert all(res['Y'] == [0, 4, 5])


@pytest.mark.platform("linux")
class TestCallback:
    def test_lambda(self, session, capfd):
        data = otp.Ticks(X=[1, 2, 3], Y=[1, 1, 1])
        data.dump(columns="X", callback=lambda x: x.first(), label="first")
        data.dump(columns="X", callback=lambda x: x.last(), label="last")
        otp.run(data)
        captured = capfd.readouterr()
        assert captured.out == '#TIMESTAMP,X,_OUT_LABEL_\n' \
                               '01-12-2003 00:00:00.000000000,1,first\n' \
                               '#TIMESTAMP,X,_OUT_LABEL_\n' \
                               '01-12-2003 00:00:00.002000000,3,last\n'

    def test_func(self, session, capfd):
        def callback(source, column):
            return source.process_by_group(lambda x: x.first(), group_by=column)

        data = otp.Ticks(X=[1, 2, 3, 2], Y=[1, 2, 3, 4])
        data.dump(columns="Y", callback=partial(callback, column=["X"]))
        otp.run(data)
        captured = capfd.readouterr()
        assert captured.out == '#TIMESTAMP,Y\n' \
                               '01-12-2003 00:00:00.000000000,1\n' \
                               '01-12-2003 00:00:00.001000000,2\n' \
                               '01-12-2003 00:00:00.002000000,3\n'


@pytest.mark.platform("linux")
def test_agg(session):
    # PY-1341
    data = otp.Ticks(
        X=[2, 1, 1, 1, 1, 1, 1, 1, 0.5],
        offset=[1000 * i for i in range(1, 10)]
    )
    df_1 = otp.run(
        data.high("X", bucket_interval=4)
    )
    data.dump()
    df_2 = otp.run(
        data.high("X", bucket_interval=4)
    )
    assert df_1.equals(df_2)
