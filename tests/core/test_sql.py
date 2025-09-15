import pytest

import onetick.py as otp


@pytest.fixture(scope="module", autouse=True)
def sql_db(session):
    db = otp.DB("TEST_SQL_DB")
    db.add(src=otp.Ticks(X=[1, 2, 3]), symbol="AAA", tick_type="TRD", date=otp.date(2003, 12, 1))
    session.use(db)
    yield db


@pytest.mark.filterwarnings("ignore:.*onetick.query will use 19700101.*")
@pytest.mark.parametrize('use_start_end_time', [True, False])
def test_sql(use_start_end_time, monkeypatch):
    if not use_start_end_time:
        monkeypatch.setattr(otp.config.__class__.__dict__.get('default_start_time'), '_set_value', otp.config.default)
        monkeypatch.setattr(otp.config.__class__.__dict__.get('default_end_time'), '_set_value', otp.config.default)

    sql_statement = """
        SELECT * \
        FROM TEST_SQL_DB.TRD
        WHERE SYMBOL_NAME = 'AAA' and
            TIMESTAMP >= '2003-12-01 00:00:00 GMT' and
            TIMESTAMP < '2003-12-01 12:00:00 GMT'
        LIMIT 2 \
    """

    sql_query = otp.SqlQuery(sql_statement)
    result = otp.run(sql_query).to_dict(orient='list')
    assert result['X'] == [1, 2]
