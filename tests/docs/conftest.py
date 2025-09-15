import pytest


@pytest.fixture(scope="module", autouse=True)
def session(m_session):
    pass
