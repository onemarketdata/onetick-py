import pytest
import onetick.py as otp
from onetick.py.otq import otq


META_FIELDS = (
    'TIMESTAMP',
    '_START_TIME',
    '_END_TIME',
    '_TIMEZONE',
    '_SYMBOL_NAME',
)


@pytest.fixture(scope='module')
def session(session):
    data = otp.Tick(A=1)
    data.sink(otq.AddField('TIMESTAMP', 'nsectime(0)'))
    db = otp.DB('DB')
    db.add(data, tick_type='TT')
    session.use(db)
    yield session


@pytest.mark.parametrize('meta_field', META_FIELDS)
def test_onetick(session, meta_field):
    t = otp.Tick(A=1)
    t.sink(otq.AddField(meta_field, '1'))
    t.sink(otq.AddField('B', meta_field))
    with pytest.raises(Exception):
        otp.run(t)


@pytest.mark.parametrize('meta_field', META_FIELDS)
def test_tick(session, meta_field):
    if meta_field == 'TIMESTAMP':
        with pytest.warns(FutureWarning, match='Setting meta field TIMESTAMP in schema is not needed'):
            otp.Tick(A=1, **{meta_field: otp.msectime(0)})
    else:
        with pytest.raises(ValueError, match=f"Can't set meta field {meta_field}"):
            otp.Tick(A=1, **{meta_field: otp.msectime(0)})


@pytest.mark.parametrize('meta_field', META_FIELDS)
def test_ticks(session, meta_field):
    if meta_field == 'TIMESTAMP':
        with pytest.warns(FutureWarning, match='Setting meta field TIMESTAMP in schema is not needed'):
            otp.Ticks(**{'A': [1, 2], meta_field: [otp.msectime(0), otp.msectime(0)]})
    else:
        with pytest.raises(ValueError, match=f"Can't set meta field {meta_field}"):
            otp.Ticks(**{'A': [1, 2], meta_field: [otp.msectime(0), otp.msectime(0)]})


@pytest.mark.parametrize('meta_field', META_FIELDS)
def test_source_methods(session, meta_field):
    t = otp.Tick(A=1)
    assert meta_field in t.schema
    with pytest.raises(ValueError, match=f"Can't set meta field {meta_field}"):
        t.table(**{meta_field: otp.msectime(0)})
    with pytest.raises(ValueError, match=f"Can't set meta field {meta_field}"):
        t.add_fields({meta_field: otp.msectime(0)})
    with pytest.raises(ValueError, match=f"Can't set meta field {meta_field}"):
        t.update({meta_field: otp.msectime(0)})
    if meta_field != 'TIMESTAMP':
        with pytest.raises(ValueError, match=f"Can't set meta field {meta_field}"):
            t[meta_field] = otp.msectime(0)


def test_database(session):
    with pytest.warns(FutureWarning, match='Setting meta field TIMESTAMP in schema is not needed'):
        otp.DataSource('DB', tick_type='TT', schema_policy='tolerant')
    otp.DataSource('DB', tick_type='TT', schema_policy='manual')


@pytest.mark.parametrize('meta_field', META_FIELDS)
def test_source(session, meta_field):
    if meta_field == 'TIMESTAMP':
        with pytest.warns(FutureWarning, match='Setting meta field TIMESTAMP in schema is not needed'):
            otp.Source(schema={meta_field: otp.msectime})
    else:
        with pytest.raises(ValueError, match=f"Can't set meta field {meta_field}"):
            otp.Source(schema={meta_field: otp.msectime})
