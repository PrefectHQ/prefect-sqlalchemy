import pytest
from sqlalchemy.ext.asyncio.engine import AsyncConnection

from prefect_sqlalchemy.credentials import AsyncDriver, DatabaseCredentials


@pytest.mark.parametrize(
    "driver", [AsyncDriver.POSTGRESQL_ASYNCPG, "postgresql+asyncpg"]
)
def test_sqlalchemy_credentials_get_connection(driver):
    sqlalchemy_credentials = DatabaseCredentials(driver, "user", "password", "database")
    expected = "postgresql+asyncpg://user:***@localhost:5432/database"
    assert sqlalchemy_credentials.engine.url.render_as_string() == expected
    connection = sqlalchemy_credentials.get_connection()
    assert isinstance(connection, AsyncConnection)
