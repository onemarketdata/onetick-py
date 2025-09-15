import os
import pytest

import onetick.py as otp


@pytest.fixture
def session():
    with otp.Session() as s:
        yield s


@pytest.mark.parametrize(
    'symbol,expected', [
        ('C', [4]),
        ('B', [2]),
        ('A', [1, 3]),
    ]
)
def test_unbound(session, symbol, expected):

    data = otp.Ticks(X=[1, 2, 3, 4], TICKER=['A', 'B', 'A', 'C'])

    data = otp.SplitQueryOutputBySymbol(data, data['TICKER'])
    res = otp.run(data, symbols=symbol)

    assert all(res['X'] == expected)


@pytest.mark.parametrize(
    'symbol,expected', [
        ('C', [4]),
        ('B', [2]),
        ('A', [1, 3]),
    ]
)
def test_unbound_by_symbol(session, symbol, expected):

    data = otp.by_symbol(otp.Ticks(X=[1, 2, 3, 4],
                                   TICKER=['A', 'B', 'A', 'C']),
                         'TICKER')

    res = otp.run(data, symbols=symbol)

    assert all(res['X'] == expected)


@pytest.mark.parametrize(
    'symbol,expected', [
        ('C', [4]),
        ('B', [2]),
        ('A', [1, 3])
    ]
)
def test_bound(session, symbol, expected):

    data = otp.Ticks(X=[1, 2, 3, 4], TICKER=['A', 'B', 'A', 'C'])

    data = otp.SplitQueryOutputBySymbol(data, data['TICKER'], symbols=symbol)
    data += otp.Empty()  # to prevent conflict between bound and unbound symbols

    # use unbound symbol here to check that the correct bound is taken from the
    # query with TICKER
    res = otp.run(data, symbols='A')

    assert all(res['X'] == expected)


def test_non_supported_type(session):
    with pytest.raises(Exception):
        otp.by_symbol(otp.query('abc'), 'TICKER')


@pytest.mark.integration
@pytest.mark.skip(reason='integration test with cloud')
def test_csv(cur_dir):
    locator = otp.RemoteTS(
        otp.LoadBalancing(
            "development-queryhost.preprod-solutions.parent.onetick.com:50015",
            "development-queryhost-2.preprod-solutions.parent.onetick.com:50015"
        )
    )
    cfg = otp.Config(locator=locator)

    with otp.Session(cfg):

        otp.config['default_db'] = 'US_COMP'

        def main(symbol):
            csv = otp.by_symbol(
                otp.CSV(
                    otp.utils.file(os.path.join(cur_dir, 'data', 'example_events.csv')),
                    converters={"time_number": lambda c: c.apply(otp.nsectime)},
                    timestamp_name="time_number",
                    start=otp.dt(2022, 7, 1),
                    end=otp.dt(2022, 7, 2),
                    order_ticks=True
                )[['stock', 'px']],
                'stock',
            )

            trades = otp.DataSource(db='US_COMP',
                                    tick_type='TRD',
                                    start=otp.dt(2022, 7, 1),
                                    end=otp.dt(2022, 7, 2))[['PRICE', 'SIZE']]

            data = otp.join_by_time([csv, trades])

            data['SN'] = symbol.name

            return data

        data = otp.merge(main, symbols=otp.Symbols(db='US_COMP'), presort=True, concurrency=16)

        res = otp.run(data)

        import pprint
        pprint.pprint(res)


@pytest.mark.integration
@pytest.mark.skip(reason='integration test with cloud')
def test_csv_as_in_docs(cur_dir):
    locator = otp.RemoteTS(
        otp.LoadBalancing(
            "development-queryhost.preprod-solutions.parent.onetick.com:50015",
            "development-queryhost-2.preprod-solutions.parent.onetick.com:50015"
        )
    )
    cfg = otp.Config(locator=locator)

    with otp.Session(cfg):

        otp.config['default_db'] = 'US_COMP'

        executions = otp.CSV(
            otp.utils.file(os.path.join(cur_dir, 'data', 'example_events.csv')),
            converters={"time_number": lambda c: c.apply(otp.nsectime)},
            timestamp_name="time_number",
            start=otp.dt(2022, 7, 1),
            end=otp.dt(2022, 7, 2),
            order_ticks=True
        )[['stock', 'px']]
        csv = otp.by_symbol(executions, 'stock')
        trd = otp.DataSource(
            db='US_COMP',
            tick_type='TRD',
            start=otp.dt(2022, 7, 1),
            end=otp.dt(2022, 7, 2)
        )[['PRICE', 'SIZE']]
        data = otp.join_by_time([csv, trd])
        res = otp.run(data, symbols=executions.distinct(keys='stock')[['stock']], concurrency=8)
        print(res)
