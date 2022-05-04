from sqlalchemy.ext.asyncio.engine import AsyncConnection

from prefect_sqlalchemy.credentials import DatabaseCredentials


def test_sqlalchemy_credentials_get_connection():
    sqlalchemy_credentials = DatabaseCredentials(
        "postgresql", "user", "password", "database"
    )
    expected = "postgresql+asyncpg://user:***@localhost:5432/database"
    assert sqlalchemy_credentials.engine.url.render_as_string() == expected
    connection = sqlalchemy_credentials.get_connection()
    assert isinstance(connection, AsyncConnection)
