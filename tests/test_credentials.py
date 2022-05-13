import pytest
from prefect import flow
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio.engine import AsyncConnection

from prefect_sqlalchemy.credentials import AsyncDriver, DatabaseCredentials, SyncDriver


@pytest.mark.parametrize(
    "driver", [AsyncDriver.POSTGRESQL_ASYNCPG, "postgresql+asyncpg"]
)
def test_sqlalchemy_credentials_get_connection_async(driver):
    @flow
    def test_flow():
        sqlalchemy_credentials = DatabaseCredentials(
            driver, "user", "password", "database"
        )
        expected = "postgresql+asyncpg://user:***@localhost:5432/database"
        assert sqlalchemy_credentials.engine.url.render_as_string() == expected
        connection = sqlalchemy_credentials.get_connection()
        assert isinstance(connection, AsyncConnection)
        assert sqlalchemy_credentials.is_async is True

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
        connection = sqlalchemy_credentials.get_connection()
        assert isinstance(connection, Connection)
        assert sqlalchemy_credentials.is_async is False

    test_flow().result()
