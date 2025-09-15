import pytest

import onetick.py as otp


class TestSetSchema:
    def test_empty(self):
        data = otp.Empty()
        data.set_schema(A=int, B=str, Z=str)
        columns = data.columns(skip_meta_fields=True)
        assert columns == {"A": int, "B": str, "Z": str}
        data["A"]
        data["B"]
        data["Z"]

    def test_the_same(self):
        data = otp.Ticks(dict(A=[1], B="A", Z="C"))
        data.set_schema(**data.columns(skip_meta_fields=True))
        columns = data.columns(skip_meta_fields=True)
        assert columns == {"A": int, "B": str, "Z": str}
        data["A"]
        data["B"]
        data["Z"]

    def test_magic_column_correct(self):
        data = otp.Ticks(dict(A=[1.6], Z=[2]))
        with pytest.warns(match='Setting type in schema for meta field'):
            data.set_schema(_SYMBOL_NAME=str, _START_TIME=int)
        assert not data.columns(skip_meta_fields=True)

    def test_magic_column_wrong(self):
        data = otp.Ticks(dict(A=[1.6], Z=[2]))
        with pytest.warns(Warning):
            data.set_schema(_SYMBOL_NAME=int)


class TestSchemaProperty:
    set_plan = [
        {},
        dict(PRICE=float, SIZE=int),
        dict(NT=otp.nsectime, MT=otp.msectime),
        dict(S1=str, S2=otp.string[1024]),
        {}
    ]

    @pytest.mark.parametrize('init', set_plan)
    def test_init(self, init):
        data = otp.Empty(schema=init)

        assert data.schema == dict(**init)
        assert data.columns(skip_meta_fields=True) == data.schema

    @pytest.mark.parametrize('to_set', set_plan)
    def test_set(self, to_set):
        data1 = otp.Empty()
        data2 = otp.Empty()

        assert not data1.schema
        assert not data2.schema

        data1.schema.set(**to_set)
        data2.set_schema(**to_set)

        assert data1.schema == data2.schema == to_set
        assert data1.columns(skip_meta_fields=True) == data2.schema

    @pytest.mark.parametrize('to_init,to_update',
                             [
                                 ({}, dict(PRICE=float)),
                                 (dict(PRICE=float), dict(PRICE=float)),
                                 (dict(PRICE=float), dict(PRICE=float, QTY=int)),
                                 (dict(PRICE=float), dict(QTY=int))
                             ])
    def test_update(self, to_init, to_update):
        data = otp.Empty(schema=to_init)

        assert data.schema == to_init

        data.schema.update(**to_update)

        updated_schema = to_init.copy()
        updated_schema.update(to_update)

        assert data.schema == updated_schema
    # def

    @pytest.mark.parametrize('init', set_plan)
    def test_symbol_param(self, init):
        data = otp.Empty(schema=init)

        symbol = data.to_symbol_param()
        assert symbol.schema == init
        assert symbol.schema == data.schema
    # def

# class


class TestMetaFields:

    @pytest.fixture()
    def session(self):
        s = otp.Session()
        yield s
        s.close()

    def test_symbol_name(self, session):
        t = otp.Ticks({'T': [1]}, tick_type='TT')
        t['SN'] = t['_SYMBOL_NAME']
        t['S'] = t['_START_TIME']
        t['E'] = t['_END_TIME']
        t['DB'] = t['_DBNAME']
        t['TT'] = t['_TICK_TYPE']
        t['TZ'] = t['_TIMEZONE']
        res = otp.run(t)
        assert res['SN'][0] == otp.config['default_symbol']
        assert res['S'][0] == otp.config['default_start_time']
        assert res['E'][0] == otp.config['default_end_time']
        assert res['DB'][0] == otp.config['default_db']
        assert res['TT'][0] == 'TT'
        assert res['TZ'][0] == otp.config['tz']

    def test_meta_fields_not_rewritten(self, session):
        t = otp.Ticks({'META_FIELDS': [1]})
        t.columns(skip_meta_fields=True)
