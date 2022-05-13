import pytest
from prefect import flow
from sqlalchemy.engine import Engine, create_mock_engine
from sqlalchemy.engine.mock import MockConnection
from sqlalchemy.ext.asyncio.engine import AsyncConnection, AsyncEngine

from prefect_sqlalchemy.credentials import AsyncDriver, DatabaseCredentials, SyncDriver


@pytest.mark.parametrize(
    "driver", [AsyncDriver.POSTGRESQL_ASYNCPG, "postgresql+asyncpg"]
)
def test_sqlalchemy_credentials_get_connection_async(monkeypatch, driver):
    @flow
    def test_flow():
        sqlalchemy_credentials = DatabaseCredentials(
            driver, "user", "password", "database"
        )
        expected = "postgresql+asyncpg://user:***@localhost:5432/database"
        assert sqlalchemy_credentials.engine.url.render_as_string() == expected
        assert sqlalchemy_credentials.is_async is True
        assert isinstance(sqlalchemy_credentials.engine, AsyncEngine)
        connection = sqlalchemy_credentials.get_connection()
        assert isinstance(connection, AsyncConnection)

    test_flow().result()


@pytest.mark.parametrize(
    "driver", [SyncDriver.POSTGRESQL_PSYCOPG2, "postgresql+psycopg2"]
)
def test_sqlalchemy_credentials_get_connection_sync(driver):
    @flow
    def test_flow():
        sqlalchemy_credentials = DatabaseCredentials(
            driver, "user", "password", "database"
        )
        expected = "postgresql+psycopg2://user:***@localhost:5432/database"
        assert sqlalchemy_credentials.engine.url.render_as_string() == expected
        assert sqlalchemy_credentials.is_async is False
        # first check if it's the proper engine
        assert isinstance(sqlalchemy_credentials.engine, Engine)
        # can't use monkeypatch or else affects Orion as well
        # so manually replace it with a mock engine
        sqlalchemy_credentials.engine = create_mock_engine(
            sqlalchemy_credentials.engine.url, ""
        )
        connection = sqlalchemy_credentials.get_connection()
        assert isinstance(connection, MockConnection)

    test_flow().result()
