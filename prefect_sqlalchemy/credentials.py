"""Credential classes used to perform authenticated interactions with SQLAlchemy"""

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from sqlalchemy.engine.url import URL
from sqlalchemy.ext.asyncio import create_async_engine

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio.engine import AsyncConnection


class AsyncDriver(Enum):
    """
    Known dialects with their corresponding async drivers.
    """

    POSTGRESQL_ASYNCPG = "postgresql+asyncpg"
    POSTGRESQL_PSYCOPG = "postgresql+psycopg"
    SQLITE_AIOSQLITE = "sqlite+aiosqlite"
    MYSQL_ASYNCMY = "mysql+asyncmy"
    MYSQL_AIOMYSQL = "mysql+aiomysql"


@dataclass
class DatabaseCredentials:
    """
    Dataclass used to manage authentication with SQLAlchemy.
    The engine should be disposed manually at the end of the flow.

    Args:
        driver: The driver name, e.g. "postgresql+asyncpg"
        database: The name of the database to use.
        user: The user name used to authenticate.
        password: The password used to authenticate.
        host: The host address of the database.
        port: The port to connect to the database.
        query: A dictionary of string keys to string values to be passed to
            the dialect and/or the DBAPI upon connect. To specify non-string
            parameters to a Python DBAPI directly, use connect_args.
        connect_args: The options which will be passed
            directly to the DBAPI's connect() method as
            additional keyword arguments.
    """

    driver: Union[AsyncDriver, str]
    user: str
    password: str
    database: str
    host: Optional[str] = "localhost"
    port: Optional[str] = 5432
    query: Optional[Dict[str, str]] = None
    connect_args: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        """
        Initializes the engine.
        """
        if isinstance(self.driver, AsyncDriver):
            drivername = self.driver.value
        else:  # if they specify a novel async driver
            drivername = self.driver

        url = URL.create(
            drivername=drivername,
            username=self.user,
            password=self.password,
            database=self.database,
            host=self.host,
            port=self.port,
            query=self.query,
        )
        connect_args = self.connect_args or {}
        self.engine = create_async_engine(url, connect_args=connect_args)

    def get_connection(self) -> "AsyncConnection":
        """
        Returns an authenticated connection that can be
        used to query from Snowflake databases.

        Returns:
            The authenticated SQLAlchemy AsyncConnection.

        Examples:
            ```python
            from prefect import flow
            from prefect_sqlalchemy import DatabaseCredentials, AsyncDriver

            @flow
            def sqlalchemy_credentials_flow():
                sqlalchemy_credentials = DatabaseCredentials(
                    drivername=AsyncDriver.POSTGRESQL_ASYNCPG,
                    username="prefect",
                    password="prefect_password",
                    database="postgres"
                )
                return sqlalchemy_credentials

            sqlalchemy_credentials_flow()
            ```
        """
        return self.engine.connect()

    async def dispose(self):
        """
        Dispose the engine.
        """
        await self.engine.dispose()
