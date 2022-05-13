"""Credential classes used to perform authenticated interactions with SQLAlchemy"""

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.asyncio import create_async_engine

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.ext.asyncio.engine import AsyncConnection

from prefect.logging import get_run_logger


class AsyncDriver(Enum):
    """
    Known dialects with their corresponding async drivers.
    """

    POSTGRESQL_ASYNCPG = "postgresql+asyncpg"

    SQLITE_AIOSQLITE = "sqlite+aiosqlite"

    MYSQL_ASYNCMY = "mysql+asyncmy"
    MYSQL_AIOMYSQL = "mysql+aiomysql"


class SyncDriver(Enum):
    """
    Known dialects with their corresponding sync drivers.
    """

    POSTGRESQL_PSYCOPG2 = "postgresql+psycopg2"
    POSTGRESQL_PG8000 = "postgresql+pg8000"
    POSTGRESQL_PSYCOPG2CFFI = "postgresql+psycopg2cffi"
    POSTGRESQL_PYPOSTGRESQL = "postgresql+pypostgresql"
    POSTGRESQL_PYGRESQL = "postgresql+pygresql"

    MYSQL_MYSQLDB = "mysql+mysqldb"
    MYSQL_PYMYSQL = "mysql+pymysql"
    MYSQL_MYSQLCONNECTOR = "mysql+mysqlconnector"
    MYSQL_CYMYSQL = "mysql+cymysql"
    MYSQL_OURSQL = "mysql+oursql"
    MYSQL_PYODBC = "mysql+pyodbc"

    SQLITE_PYSQLITE = "sqlite+pysqlite"
    SQLITE_PYSQLCIPHER = "sqlite+pysqlcipher"

    ORACLE_CX_ORACLE = "oracle+cx_oracle"

    MSSQL_PYODBC = "mssql+pyodbc"
    MSSQL_MXODBC = "mssql+mxodbc"
    MSSQL_PYMSSQL = "mssql+pymssql"


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
        logger = get_run_logger()
        if isinstance(self.driver, Enum):
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
        try:
            self.engine = create_async_engine(url, connect_args=connect_args)
            self.is_async = True
        except InvalidRequestError as exc:
            if "is not async" in str(exc):
                self.engine = create_engine(url, connect_args=connect_args)
                self.is_async = False
            else:
                raise exc

        engine_type = "ASYNCHRONOUS" if self.is_async else "SYNCHRONOUS"
        url_repr = url.render_as_string()  # hides the password
        logger.info(
            f"Created a(n) {engine_type} engine successfully "
            f"using the connection string: {url_repr}."
        )

    def get_connection(self) -> Union["Connection", "AsyncConnection"]:
        """
        Returns an authenticated connection that can be
        used to query from databases.

        Returns:
            The authenticated SQLAlchemy Connection / AsyncConnection.

        Examples:
            Create an asynchronous connection.
            ```python
            from prefect import flow
            from prefect_sqlalchemy import DatabaseCredentials, AsyncDriver

            @flow
            def sqlalchemy_credentials_flow():
                sqlalchemy_credentials = DatabaseCredentials(
                    driver=AsyncDriver.POSTGRESQL_ASYNCPG,
                    user="prefect",
                    password="prefect_password",
                    database="postgres"
                )
                print(sqlalchemy_credentials.get_connection())

            sqlalchemy_credentials_flow()
            ```
        """
        return self.engine.connect()

    async def dispose(self):
        """
        Dispose the engine.
        """
        await self.engine.dispose()
