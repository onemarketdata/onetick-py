from collections import Counter

import math
import pytest

import onetick.py as otp
from onetick.py.core._source.source_methods.misc import _merge_fields_by_regex
from onetick.py.compatibility import is_ob_virtual_prl_and_show_full_detail_supported


class TestVirtualOb:
    @pytest.fixture(scope='class', autouse=True)
    def db_data(self, session):
        db = otp.DB("TEST_DB")
        db.add(otp.Ticks(
            ASK_PRICE=[1.5, 1.4, 1.7, 2.2, 1.4, 1.5, 1.5, 2.5, 2.1],
            ASK_SIZE=[10, 20, 10, 100, 20, 100, 30, 20, 10],
            BID_PRICE=[1.4, 1.3, 1.6, 1.9, 1.3, 1.4, 1.4, 1.8, 2.0],
            BID_SIZE=[40, 50, 30, 150, 50, 300, 80, 100, 40],
            EXCHANGE=['A', 'A', 'B', 'A', 'B', 'B', 'B', 'B', 'A'],
            OTHER=['C', 'D', 'C', 'C', 'D', 'D', 'C', 'D', 'D'],
        ), symbol='TEST', tick_type='QTE', date=otp.date(2003, 12, 1))
        session.use(db)

    @pytest.mark.parametrize('output_book_format', ['ob', 'prl'])
    def test_simple(self, output_book_format):
        if output_book_format == 'prl' and not is_ob_virtual_prl_and_show_full_detail_supported():
            return

        data = otp.DataSource(db='TEST_DB', symbols='TEST', tick_type='QTE', date=otp.date(2003, 12, 1))
        data = data.virtual_ob(output_book_format=output_book_format)
        df = otp.run(data)
        assert set(df.columns) == {'Time', 'PRICE', 'SIZE', 'DELETED_TIME', 'BUY_SELL_FLAG', 'TICK_STATUS', 'SOURCE'}
        assert list(df['PRICE'])[:3] == [1.5, 1.4, 1.4]
        assert list(df['SIZE'])[:3] == [10, 40, 20]
        assert list(df['BUY_SELL_FLAG'])[:3] == [1, 0, 1]
        assert list(df['SOURCE'])[:3] == ['TEST'] * 3

    def test_with_source_field(self):
        data = otp.DataSource(db='TEST_DB', symbols='TEST', tick_type='QTE', date=otp.date(2003, 12, 1))
        data = data.virtual_ob(['EXCHANGE'])
        df = otp.run(data)
        assert set(df['SOURCE']) == {'A', 'B'}

        data = data.ob_snapshot(max_levels=2)
        df = otp.run(data)
        assert list(df['PRICE']) == [2.1, 2.5, 2.0, 1.8]
        assert list(df['SIZE']) == [10, 20, 40, 100]
        assert list(df['LEVEL']) == [1, 2, 1, 2]
        assert list(df['BUY_SELL_FLAG']) == [1, 1, 0, 0]

    def test_with_multiple_source_fields(self):
        data = otp.DataSource(db='TEST_DB', symbols='TEST', tick_type='QTE', date=otp.date(2003, 12, 1))
        data = data.virtual_ob(['EXCHANGE', data['OTHER']])
        df = otp.run(data)
        assert Counter(list(df['SOURCE'])) == {'B/D': 10, 'A/C': 6, 'A/D': 6, 'B/C': 6}

        data = data.ob_snapshot(max_levels=2)
        df = otp.run(data)
        assert list(df['PRICE']) == [1.5, 2.1, 2.0, 1.9]
        assert list(df['SIZE']) == [30, 10, 40, 150]

    @pytest.mark.skipif(
        not is_ob_virtual_prl_and_show_full_detail_supported(),
        reason="not `ob_virtual` not supports `show_full_detail` on this OneTick version",
    )
    def test_show_full_detail(self):
        data = otp.DataSource(db='TEST_DB', symbols='TEST', tick_type='QTE', date=otp.date(2003, 12, 1))
        data['BID_A'] = data['BID_PRICE']
        data['ASK_AB'] = data['ASK_PRICE']
        data = data.virtual_ob(['EXCHANGE'], show_full_detail=True, output_book_format='prl')
        df = otp.run(data)

        assert set(df.columns) == {
            'Time', 'PRICE', 'SIZE', 'DELETED_TIME', 'BUY_SELL_FLAG', 'TICK_STATUS', 'SOURCE', 'A', 'AB', 'OTHER',
        }
        assert list(df['PRICE'])[:4] == [1.5, 1.4, 1.4, 1.5]
        assert list(df['SIZE'])[:4] == [10, 40, 20, 0]
        assert list(df['BUY_SELL_FLAG'])[:4] == [1, 0, 1, 1]
        assert list(df['SOURCE'])[:4] == ['A'] * 4

        assert all(
            (flag == 1 and price == ab and (a == otp.nan or math.isnan(a))) or
            (flag == 0 and price == a and (ab == otp.nan or math.isnan(ab)))
            for flag, a, ab, price in zip(df['BUY_SELL_FLAG'], df['A'], df['AB'], df['PRICE'])
        )

    def test_exceptions(self):
        data = otp.DataSource(db='TEST_DB', symbols='TEST', tick_type='QTE', date=otp.date(2003, 12, 1))

        with pytest.raises(ValueError):
            _ = data.virtual_ob(['FAKE_FIELD'])

        with pytest.raises(ValueError):
            _ = data.virtual_ob(output_book_format='test')

        with pytest.raises(ValueError):
            _ = data.virtual_ob(show_full_detail=True)

        with pytest.raises(ValueError, match=r'((ASK_SIZE|BID_PRICE)(, )?){2}'):
            ticks = otp.Tick(ASK_PRICE=1, BID_SIZE=1)
            _ = ticks.virtual_ob()


class TestFieldsMerge:
    def test_simple(self):
        schema = {
            'A': int,
            'TEST_A_B': int,
            'TEST_B_B': int,
            'TEST_C': str,
            'TEST_A_D': float,
            'F': str,
        }

        _merge_fields_by_regex(schema, r'^(TEST_A|TEST_B)_(.*)$', 2)

        assert schema == {'A': int, 'B': int, 'TEST_C': str, 'D': float, 'F': str}

    def test_type_mismatch(self):
        schema = {
            'A': int,
            'TEST_A_B': int,
            'TEST_B_B': int,
            'TEST_A_C': str,
            'PREFIX_C': str,
            'TEST_B_C': float,
        }

        with pytest.raises(TypeError, match='`C`'):
            _merge_fields_by_regex(schema, r'^(TEST_A|TEST_B|PREFIX)_(.*)$', 2)

    def test_existing_field(self):
        schema = {
            'A': int,
            'TEST_A_B': int,
            'TEST_B_B': int,
            'TEST_A_A': str,
        }

        with pytest.raises(KeyError, match='`TEST_A_A`.*`A`'):
            _merge_fields_by_regex(schema, r'^(TEST_A|TEST_B)_(.*)$', 2)
