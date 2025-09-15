import pytest

import onetick.py as otp


if not otp.compatibility.is_database_view_schema_supported():
    pytest.skip(allow_module_level=True,
                reason='Getting schema from database view is not supported on this OneTick version')


@pytest.fixture(scope='module')
def session(m_session):
    db_1 = otp.DB('DB')
    db_1.add(otp.Ticks(A=[1, 2, 3, 4, 5]), tick_type='TT', symbol='A')
    db_1.add(otp.Ticks(B=[6, 7, 8, 9]), tick_type='TT', symbol='B')
    m_session.use(db_1)

    # http://solutions.pages.soltest.onetick.com/iac/onetick-server/Views.html

    view = otp.DataSource(tick_type='TT', schema_policy='manual')
    view_query = view.to_otq(otp.utils.TmpFile(), query_name='TT', symbols='DB::', add_passthrough=False)
    db_view = otp.DB('DB_VIEW', db_properties={'BASE_OTQ': view_query.split('::')[0]})
    m_session.use(db_view)

    view_with_table = view.table(A=int, B=int, strict=True)
    # add_passthrough=False is needed, because TABLE must be at the end to get the schema from view
    view_with_table_query = view_with_table.to_otq(otp.utils.TmpFile(),
                                                   query_name='TT', symbols='DB::', add_passthrough=False)
    db_view_with_table = otp.DB('DB_VIEW_WITH_TABLE',
                                db_properties={'BASE_OTQ': view_with_table_query.split('::')[0]})
    m_session.use(db_view_with_table)

    yield m_session


def test_databases(session):
    db = otp.databases()['DB']
    assert db.last_date == otp.config.default_start_time.date()

    assert 'DB_VIEW' in otp.databases()
    db = otp.databases()['DB_VIEW']
    assert db.tick_types() == ['TT']
    # last_date is None, because otq.DbShowLoadedTimeRanges always return NUM_LOADED_PARTITIONS=0 for views
    assert db.last_date is None
    assert db.dates() == []
    # schema is empty, because last date is None
    assert db.schema(tick_type='TT') == {}
    # schema is empty, because there is no TABLE EP at the end of the view query
    assert db.schema(tick_type='TT', date=otp.config.default_start_time) == {}

    assert 'DB_VIEW_WITH_TABLE' in otp.databases()
    db = otp.databases()['DB_VIEW_WITH_TABLE']
    assert db.tick_types() == ['TT']
    # last_date is None, because otq.DbShowLoadedTimeRanges always return NUM_LOADED_PARTITIONS=0 for views
    assert db.last_date is None
    assert db.dates() == []
    # schema is empty, because last date is None
    assert db.schema(tick_type='TT') == {}
    # schema is not empty, because there is a TABLE EP at the end of the view query
    assert db.schema(tick_type='TT', date=otp.config.default_start_time) == {'A': int, 'B': int}


def test_data_source_tolerant(session):
    assert 'DB_VIEW_WITH_TABLE' in otp.databases()
    data = otp.DataSource('DB_VIEW_WITH_TABLE', tick_type='TT', schema_policy='tolerant')
    # schema is empty, because last_date is None
    assert data.schema == {}
    data = otp.DataSource('DB_VIEW_WITH_TABLE', tick_type='TT', schema_policy='tolerant',
                          start=otp.config.default_start_time,
                          end=otp.config.default_end_time)
    # schema is not empty, because the date to get schema is specified with start and end parameters
    assert data.schema == {'A': int, 'B': int}
    # check getting correct data with view
    df1 = otp.run(data, symbols='A')
    assert list(df1['A']) == [1, 2, 3, 4, 5]
    df2 = otp.run(data, symbols='B')
    assert list(df2['B']) == [6, 7, 8, 9]
